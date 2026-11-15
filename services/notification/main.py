import os
import asyncio
import logging
import json
from datetime import datetime
from typing import Optional
from fastapi import FastAPI
from pydantic import BaseModel
import redis.asyncio as redis

logging.basicConfig(level=os.getenv("AIM_LOG_LEVEL", "INFO"))
logger = logging.getLogger("aim.notification")

from fastapi.middleware.cors import CORSMiddleware

app = FastAPI(title="AIM Notification Service", version="0.1.0")

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
            "dst": "notification", "method": request.method, "path": p, "status": status,
            "ms": int((_aim_t.time() - t0) * 1000), "ip": request.client.host if request.client else "",
            "size": int(request.headers.get("content-length") or 0)},)).start()

app.add_middleware(
    CORSMiddleware,
    allow_origins=["http://localhost:8888", "http://127.0.0.1:8888"],
    allow_methods=["*"],
    allow_headers=["*"],
)
# ── Models ────────────────────────────────────────

class Notification(BaseModel):
    title: str
    message: str
    confidence: float = 0.8
    source: str = "aim"
    action_url: Optional[str] = None
    priority: str = "normal"  # low | normal | high

# ── Config ────────────────────────────────────────

redis_client = None
MIN_CONFIDENCE   = float(os.getenv("AIM_PROACTIVE_CONFIDENCE", 0.75))
COOLDOWN_SECONDS = 120  # minimum gap between notifications
last_notified    = {}   # source -> last timestamp

@app.on_event("startup")
async def startup():
    global redis_client
    redis_client = redis.Redis(
        host=os.getenv("REDIS_HOST", "redis"),
        port=int(os.getenv("REDIS_PORT", 6379)),
        password=os.getenv("REDIS_PASSWORD"),
        decode_responses=True
    )
    asyncio.create_task(listen_events())
    logger.info("Notification Service started ✅")

@app.on_event("shutdown")
async def shutdown():
    if redis_client:
        await redis_client.aclose()

# ── Cooldown check ────────────────────────────────

def is_on_cooldown(source: str) -> bool:
    last = last_notified.get(source)
    if not last:
        return False
    elapsed = (datetime.utcnow() - last).total_seconds()
    return elapsed < COOLDOWN_SECONDS

def mark_notified(source: str):
    last_notified[source] = datetime.utcnow()

# ── Send notification ─────────────────────────────

async def send_notification(notif: Notification):
    # Check confidence threshold
    if notif.confidence < MIN_CONFIDENCE:
        logger.info(f"Skipped low confidence notification: {notif.confidence}")
        return

    # Check cooldown
    if is_on_cooldown(notif.source):
        logger.info(f"Skipped cooldown notification from: {notif.source}")
        return

    mark_notified(notif.source)

    # Store in Redis list for dashboard to read
    await redis_client.lpush("aim.notifications.history", json.dumps({
        "title": notif.title,
        "message": notif.message,
        "confidence": notif.confidence,
        "source": notif.source,
        "priority": notif.priority,
        "timestamp": datetime.utcnow().isoformat()
    }))

    # Keep only last 50 notifications
    await redis_client.ltrim("aim.notifications.history", 0, 49)

    # Publish to dashboard websocket channel
    await redis_client.publish("aim.notification.display", json.dumps({
        "title": notif.title,
        "message": notif.message,
        "confidence": notif.confidence,
        "source": notif.source,
        "priority": notif.priority,
        "timestamp": datetime.utcnow().isoformat()
    }))

    logger.info(f"Notification sent: [{notif.priority}] {notif.title} — {notif.message[:60]}")

# ── Event listener ────────────────────────────────

async def listen_events():
    """Keeps the bus listener alive: a dropped Redis connection must never silently stop event handling."""
    while True:
        try:
            await _listen_once()
        except asyncio.CancelledError:
            raise
        except Exception as e:
            logger.warning("event listener restarting after %s", type(e).__name__)
            await asyncio.sleep(2)


async def _listen_once():
    await asyncio.sleep(2)
    pubsub = redis_client.pubsub()
    await pubsub.subscribe(
        "aim.notification.send",
        "aim.research.result"
    )
    logger.info("Notification Service listening...")

    async for message in pubsub.listen():
        if message["type"] != "message":
            continue
        try:
            channel = message["channel"]
            data    = json.loads(message["data"])

            if channel == "aim.notification.send":
                await send_notification(Notification(
                    title=data.get("title", "AIM"),
                    message=data.get("message", ""),
                    confidence=data.get("confidence", 0.8),
                    source=data.get("source", "aim"),
                    priority=data.get("priority", "normal")
                ))

            elif channel == "aim.research.result":
                summary = data.get("summary", "")[:100]
                await send_notification(Notification(
                    title="AIM Research Complete",
                    message=f"{data.get('query', '')[:40]}: {summary}",
                    confidence=0.9,
                    source="research-agent",
                    priority="normal"
                ))

        except Exception as e:
            logger.error(f"Notification event error: {e}")

# ── API ───────────────────────────────────────────

@app.post("/notify")
async def notify(notif: Notification):
    await send_notification(notif)
    return {"status": "sent"}

@app.get("/history")
async def history(limit: int = 20):
    items = await redis_client.lrange("aim.notifications.history", 0, limit - 1)
    return [json.loads(i) for i in items]

@app.delete("/history")
async def clear_history():
    await redis_client.delete("aim.notifications.history")
    return {"status": "cleared"}

@app.delete("/history/test")
async def clear_test_history():
    """Removes only entries created by the automated test suite (marker: aimtest)."""
    items = await redis_client.lrange("aim.notifications.history", 0, -1)
    n = 0
    for raw in items:
        if "aimtest" in raw:
            n += await redis_client.lrem("aim.notifications.history", 0, raw)
    return {"status": "cleared", "removed": n}

@app.get("/health")
async def health():
    return {"status": "ok", "service": "notification"}