import os
import asyncio
import logging
from enum import Enum
from typing import Optional
import httpx
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
import redis.asyncio as redis

logging.basicConfig(level=os.getenv("AIM_LOG_LEVEL", "INFO"))
logger = logging.getLogger("aim.llm-router")
logging.getLogger("httpx").setLevel(logging.WARNING)  # request URLs must never be logged

from fastapi.middleware.cors import CORSMiddleware

app = FastAPI(title="AIM LLM Router", version="0.1.0")

# -- AIM live-map trace: reports real HTTP traffic to the Research Agent (stdlib only, fire-and-forget) --
import threading as _aim_th, time as _aim_t, json as _aim_j, urllib.request as _aim_ur


def _aim_trace(ev):
    try:
        _aim_ur.urlopen(_aim_ur.Request("http://research-agent:8005/trace", _aim_j.dumps(ev).encode(),
                                        {"Content-Type": "application/json"}), timeout=1).read()
    except Exception:
        pass


# -- AIM origin guard: browsers on other websites must not be able to change data here (CSRF) --
_AIM_ORIGINS = ("http://localhost:8888", "http://127.0.0.1:8888")


@app.middleware("http")
async def _aim_origin_guard(request, call_next):
    if request.method in ("POST", "PUT", "PATCH", "DELETE"):
        o = request.headers.get("origin")
        if o and o not in _AIM_ORIGINS:
            from fastapi.responses import JSONResponse as _J
            return _J({"detail": "cross-site request refused"}, status_code=403)
    return await call_next(request)


@app.middleware("http")
async def _aim_trace_mw(request, call_next):
    p = request.url.path
    if request.method == "OPTIONS" or p == "/health" or p.startswith(("/trace", "/docs", "/openapi")):
        return await call_next(request)
    t0, status = _aim_t.time(), 500
    try:
        resp = await call_next(request)
        status = resp.status_code
        return resp
    finally:
        _aim_th.Thread(target=_aim_trace, daemon=True, args=({
            "dst": "llm-router", "method": request.method, "path": p, "status": status,
            "ms": int((_aim_t.time() - t0) * 1000), "ip": request.client.host if request.client else "",
            "size": int(request.headers.get("content-length") or 0)},)).start()

app.add_middleware(
    CORSMiddleware,
    allow_origins=["http://localhost:8888", "http://127.0.0.1:8888"],
    allow_methods=["*"],
    allow_headers=["*"],
)
# ── Models ────────────────────────────────────────

class TaskType(str, Enum):
    INTENT      = "intent"
    CODE        = "code"
    DEBUG       = "debug"
    VISION      = "vision"
    RESEARCH    = "research"
    EMBED       = "embed"
    PLAN        = "plan"
    FAST        = "fast"
    THINK       = "think"

class RouteRequest(BaseModel):
    task:     TaskType
    prompt:   str
    context:  Optional[str] = None
    image_b64: Optional[str] = None
    offline:  bool = False

class RouteResponse(BaseModel):
    result:   str
    model:    str
    provider: str
    tokens:   Optional[int] = None

# ── Provider config ───────────────────────────────

GROQ_KEY    = os.getenv("GROQ_API_KEY")
GOOGLE_KEY  = os.getenv("GOOGLE_API_KEY")
OLLAMA_HOST = os.getenv("OLLAMA_HOST", "http://host.docker.internal:11434")

ROUTING_TABLE = {
    # offline-capable tasks → local first
    TaskType.VISION:   {"provider": "ollama",  "model": os.getenv("OLLAMA_SCREEN_MODEL",  "gemma3:4b")},
    TaskType.EMBED:    {"provider": "ollama",  "model": os.getenv("OLLAMA_EMBED_MODEL",   "nomic-embed-text")},
    # online tasks → free APIs
    TaskType.INTENT:   {"provider": "groq",    "model": os.getenv("GROQ_BRAIN_MODEL",     "openai/gpt-oss-120b")},
    TaskType.CODE:     {"provider": "groq",    "model": os.getenv("GROQ_CODE_MODEL",      "qwen/qwen3.8-27b")},
    TaskType.DEBUG:    {"provider": "groq",    "model": os.getenv("GROQ_CODE_MODEL",      "qwen/qwen3.8-27b")},
    TaskType.FAST:     {"provider": "groq",    "model": os.getenv("GROQ_FAST_MODEL",      "openai/gpt-oss-20b")},
    TaskType.RESEARCH: {"provider": "google",  "model": os.getenv("GEMINI_RESEARCH_MODEL","gemini-2.5-flash")},
    TaskType.PLAN:     {"provider": "google",  "model": os.getenv("GEMINI_THINK_MODEL",   "gemini-2.5-flash")},
    TaskType.THINK:    {"provider": "google",  "model": os.getenv("GEMINI_THINK_MODEL",   "gemini-2.5-flash")},
}

OFFLINE_FALLBACK = os.getenv("OLLAMA_OFFLINE_MODEL", "qwen3:4b")

# ── Redis client ──────────────────────────────────

redis_client: Optional[redis.Redis] = None

@app.on_event("startup")
async def startup():
    global redis_client
    redis_client = redis.Redis(
        host=os.getenv("REDIS_HOST", "redis"),
        port=int(os.getenv("REDIS_PORT", 6379)),
        password=os.getenv("REDIS_PASSWORD"),
        decode_responses=True
    )
    logger.info("LLM Router started ✅")

@app.on_event("shutdown")
async def shutdown():
    if redis_client:
        await redis_client.aclose()

# ── Providers ─────────────────────────────────────

async def call_groq(model: str, prompt: str, context: Optional[str]) -> RouteResponse:
    messages = []
    if context:
        messages.append({"role": "system", "content": context})
    messages.append({"role": "user", "content": prompt})

    async with httpx.AsyncClient(timeout=60) as client:
        resp = await client.post(
            "https://api.groq.com/openai/v1/chat/completions",
            headers={"Authorization": f"Bearer {GROQ_KEY}"},
            json={"model": model, "messages": messages, "max_tokens": 6000, **({"reasoning_effort": "low"} if "gpt-oss" in model else {})}
        )
        resp.raise_for_status()
        data = resp.json()
        return RouteResponse(
            result=data["choices"][0]["message"]["content"],
            model=model,
            provider="groq",
            tokens=data["usage"]["total_tokens"]
        )

async def call_google(model: str, prompt: str, context: Optional[str], image_b64: Optional[str]) -> RouteResponse:
    parts = []
    if context:
        parts.append({"text": f"Context: {context}\n\n"})
    if image_b64:
        parts.append({"inline_data": {"mime_type": "image/png", "data": image_b64}})
    parts.append({"text": prompt})

    async with httpx.AsyncClient(timeout=60) as client:
        resp = await client.post(
            f"https://generativelanguage.googleapis.com/v1beta/models/{model}:generateContent",
            headers={"x-goog-api-key": GOOGLE_KEY},   # header, never the URL: keys must not reach logs
            json={"contents": [{"parts": parts}]}
        )
        resp.raise_for_status()
        data = resp.json()
        text = data["candidates"][0]["content"]["parts"][0]["text"]
        return RouteResponse(result=text, model=model, provider="google")

async def call_ollama(model: str, prompt: str, image_b64: Optional[str]) -> RouteResponse:
    payload = {"model": model, "prompt": prompt, "stream": False}
    if model.startswith("qwen3"):
        payload["think"] = False  # skip slow chain-of-thought for local replies
    if image_b64:
        payload["images"] = [image_b64]

    async with httpx.AsyncClient(timeout=120) as client:
        resp = await client.post(f"{OLLAMA_HOST}/api/generate", json=payload)
        resp.raise_for_status()
        data = resp.json()
        return RouteResponse(result=data["response"], model=model, provider="ollama")

# ── Router ────────────────────────────────────────

@app.post("/route", response_model=RouteResponse)
async def route(req: RouteRequest):
    route_config = ROUTING_TABLE.get(req.task)
    if not route_config:
        raise HTTPException(status_code=400, detail=f"Unknown task: {req.task}")

    result = None
    errors = []
    use_local = req.offline or route_config["provider"] == "ollama"
    provider = route_config["provider"]
    # No key configured for a cloud provider -> go local instead of failing
    if provider == "groq" and not GROQ_KEY:
        use_local = True
    if provider == "google" and not GOOGLE_KEY:
        use_local = True

    if not use_local:
        try:
            if provider == "groq":
                result = await call_groq(route_config["model"], req.prompt, req.context)
            elif provider == "google":
                result = await call_google(route_config["model"], req.prompt, req.context, req.image_b64)
            else:
                raise HTTPException(status_code=500, detail="Unknown provider")
        except HTTPException:
            raise
        except Exception as e:  # network down, rate limit, bad key -> local fallback
            errors.append(f"{provider}: {e}")
            logger.warning(f"{provider} failed ({e}); falling back to local model")

    if result is None:
        if req.offline or provider != "ollama":
            model = OFFLINE_FALLBACK
        else:
            model = route_config["model"]
        prompt = req.prompt if not req.context else f"{req.context}\n\n{req.prompt}"
        try:
            result = await call_ollama(model, prompt, req.image_b64)
        except Exception as e:
            errors.append(f"ollama: {e}")
            raise HTTPException(status_code=503, detail="; ".join(errors) or "No model available")

    # Publish to event bus
    if redis_client:
        await redis_client.publish("aim.llm.response", f"{req.task}:{result.model}")

    logger.info(f"Routed {req.task} → {result.provider}/{result.model}")
    return result

@app.get("/health")
async def health():
    return {"status": "ok", "service": "llm-router"}

@app.get("/models")
async def models():
    return ROUTING_TABLE