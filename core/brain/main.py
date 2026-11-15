import os
import asyncio
import logging
import json
import httpx
from datetime import datetime
from typing import Optional
from fastapi import FastAPI
from pydantic import BaseModel
import redis.asyncio as redis

logging.basicConfig(level=os.getenv("AIM_LOG_LEVEL", "INFO"))
logger = logging.getLogger("aim.brain")

from fastapi.middleware.cors import CORSMiddleware

app = FastAPI(title="AIM Core Brain", version="0.1.0")

# -- AIM live-map trace: reports real HTTP traffic to the Research Agent (stdlib only, fire-and-forget) --
import threading as _aim_th, time as _aim_t, json as _aim_j, urllib.request as _aim_ur


def _aim_trace(ev):
    try:
        _aim_ur.urlopen(_aim_ur.Request("http://research-agent:8005/trace", _aim_j.dumps(ev).encode(),
                                        {"Content-Type": "application/json"}), timeout=1).read()
    except Exception:
        pass


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
            "dst": "brain", "method": request.method, "path": p, "status": status,
            "ms": int((_aim_t.time() - t0) * 1000), "ip": request.client.host if request.client else "",
            "size": int(request.headers.get("content-length") or 0)},)).start()

app.add_middleware(
    CORSMiddleware,
    allow_origins=["http://localhost:8888", "http://127.0.0.1:8888"],
    allow_methods=["*"],
    allow_headers=["*"],
)
# ── Models ────────────────────────────────────────

class ScreenEvent(BaseModel):
    app: str
    window_title: str
    timestamp: str
    image_b64: Optional[str] = None

class IntentResult(BaseModel):
    intent: str
    project: Optional[str]
    task: Optional[str]
    confidence: float
    should_notify: bool
    action: Optional[str]

# ── Clients ───────────────────────────────────────

redis_client = None
LLM_ROUTER = os.getenv("LLM_ROUTER_URL", "http://llm-router:8001")
MEMORY_URL  = os.getenv("MEMORY_URL", "http://memory:8003")
CONFIDENCE  = float(os.getenv("AIM_PROACTIVE_CONFIDENCE", 0.75))

SYSTEM_PROMPT = """You are AIM, an AI operating environment.
Your job is to understand what the user is doing from context clues.
Always respond in valid JSON with these exact fields:
{
  "intent": "brief description of what user is doing",
  "project": "project name or null",
  "task": "specific task or null",
  "confidence": 0.0-1.0,
  "should_notify": true/false,
  "action": "suggest_resource/store_memory/none"
}
Only set should_notify=true if confidence > 0.75 and you have something genuinely useful to say.
Be concise. No explanations outside the JSON."""

@app.on_event("startup")
async def startup():
    global redis_client
    redis_client = redis.Redis(
        host=os.getenv("REDIS_HOST", "redis"),
        port=int(os.getenv("REDIS_PORT", 6379)),
        password=os.getenv("REDIS_PASSWORD"),
        decode_responses=True
    )
    # Start listening to events
    asyncio.create_task(listen_events())
    logger.info("Core Brain started ✅")

@app.on_event("shutdown")
async def shutdown():
    if redis_client:
        await redis_client.aclose()

# ── LLM calls ─────────────────────────────────────

async def classify_intent(context: str, image_b64: Optional[str] = None) -> dict:
    task = "vision" if image_b64 else "intent"
    async with httpx.AsyncClient(timeout=60) as client:
        resp = await client.post(
            f"{LLM_ROUTER}/route",
            json={
                "task": task,
                "prompt": context,
                "context": SYSTEM_PROMPT,
                "image_b64": image_b64
            }
        )
        resp.raise_for_status()
        result = resp.json()["result"]
        # Clean and parse JSON
        result = result.strip()
        if "```" in result:
            result = result.split("```")[1]
            if result.startswith("json"):
                result = result[4:]
        return json.loads(result.strip())

async def store_memory(content: str, source: str, importance: float):
    async with httpx.AsyncClient(timeout=30) as client:
        await client.post(
            f"{MEMORY_URL}/memories",
            json={
                "content": content,
                "source": source,
                "importance": importance,
                "confidence": 0.8,
                "tags": ["auto"],
                "tier": "short"
            }
        )

async def search_memory(query: str) -> list:
    async with httpx.AsyncClient(timeout=30) as client:
        resp = await client.post(
            f"{MEMORY_URL}/memories/search",
            json={"query": query, "limit": 3}
        )
        return resp.json().get("results", [])

# ── Event listener ────────────────────────────────

async def listen_events():
    await asyncio.sleep(2)  # wait for redis ready
    pubsub = redis_client.pubsub()
    await pubsub.subscribe(
        "aim.ide.file_open",
        "aim.terminal.cmd",
        "aim.browser.tab"
    )
    logger.info("Brain listening to event bus...")

    async for message in pubsub.listen():
        if message["type"] != "message":
            continue
        try:
            channel = message["channel"]
            data    = json.loads(message["data"])
            await process_event(channel, data)
        except Exception as e:
            logger.error(f"Event error: {e}")

async def process_event(channel: str, data: dict):
    logger.info(f"Processing event from {channel}")

    # Build context string
    context = f"""
Channel: {channel}
Time: {datetime.utcnow().isoformat()}
Data: {json.dumps(data, indent=2)}

What is the user doing? Respond in JSON.
"""
    # Classify intent
    image_b64 = data.get("image_b64")
    intent = await classify_intent(context, image_b64)
    logger.info(f"Intent: {intent.get('intent')} (confidence: {intent.get('confidence')})")

    # Search relevant memories
    if intent.get("intent"):
        memories = await search_memory(intent["intent"])
        if memories:
            logger.info(f"Found {len(memories)} relevant memories")

    # Store to memory if important enough
    if intent.get("confidence", 0) > 0.5:
        await store_memory(
            content=f"{intent.get('intent')} — {json.dumps(data)}",
            source=channel,
            importance=intent.get("confidence", 0.5)
        )

    # Publish to timeline
    await redis_client.publish("aim.timeline.event", json.dumps({
        "event_type": channel.replace("aim.", ""),
        "description": intent.get("intent", ""),
        "app": data.get("app", ""),
        "project": intent.get("project"),
        "metadata": data,
        "timestamp": datetime.utcnow().isoformat()
    }))

    # Fire notification if confident enough
    if intent.get("should_notify") and intent.get("confidence", 0) >= CONFIDENCE:
        await redis_client.publish("aim.notification.send", json.dumps({
            "title": "AIM",
            "message": intent.get("action", intent.get("intent")),
            "confidence": intent.get("confidence"),
            "source": channel
        }))

# ── API ───────────────────────────────────────────

@app.post("/process")
async def process(event: ScreenEvent):
    context = f"App: {event.app}\nWindow: {event.window_title}\nTime: {event.timestamp}"
    intent = await classify_intent(context, event.image_b64)
    return intent

@app.get("/health")
async def health():
    return {"status": "ok", "service": "brain"}