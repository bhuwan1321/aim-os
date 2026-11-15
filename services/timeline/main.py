import os
import asyncio
import logging
import json
from datetime import datetime, timedelta
from typing import Optional, List
from uuid import uuid4

import asyncpg
from fastapi import FastAPI, HTTPException, Query
from pydantic import BaseModel, field_validator
import redis.asyncio as redis

logging.basicConfig(level=os.getenv("AIM_LOG_LEVEL", "INFO"))
logger = logging.getLogger("aim.timeline")

from fastapi.middleware.cors import CORSMiddleware

app = FastAPI(title="AIM Timeline Service", version="0.1.0")

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
            "dst": "timeline", "method": request.method, "path": p, "status": status,
            "ms": int((_aim_t.time() - t0) * 1000), "ip": request.client.host if request.client else "",
            "size": int(request.headers.get("content-length") or 0)},)).start()

app.add_middleware(
    CORSMiddleware,
    allow_origins=["http://localhost:8888", "http://127.0.0.1:8888"],
    allow_methods=["*"],
    allow_headers=["*"],
)
import re as _re
_UUID_RX = _re.compile(r"^[0-9a-fA-F]{8}-([0-9a-fA-F]{4}-){3}[0-9a-fA-F]{12}$")

# ── Models ────────────────────────────────────────

class TimelineEvent(BaseModel):
    event_type: str
    description: str
    app: Optional[str] = None
    project_id: Optional[str] = None

    @field_validator("project_id")
    @classmethod
    def _pid_is_uuid(cls, v):
        if v is not None and not _UUID_RX.match(v):
            raise ValueError("project_id must be a UUID")
        return v
    metadata: dict = {}
    focus_score: float = 0.0

class TimelineResponse(BaseModel):
    id: str
    event_type: str
    description: str
    app: Optional[str]
    project_id: Optional[str]
    metadata: dict
    focus_score: float
    started_at: str
    ended_at: Optional[str]

# ── Clients ───────────────────────────────────────

db_pool = None
redis_client = None

@app.on_event("startup")
async def startup():
    global db_pool, redis_client

    db_pool = await asyncpg.create_pool(
        host=os.getenv("POSTGRES_HOST", "postgres"),
        port=int(os.getenv("POSTGRES_PORT", 5432)),
        database=os.getenv("POSTGRES_DB", "aimdb"),
        user=os.getenv("POSTGRES_USER", "aim"),
        password=os.getenv("POSTGRES_PASSWORD"),
        min_size=2, max_size=10
    )

    redis_client = redis.Redis(
        host=os.getenv("REDIS_HOST", "redis"),
        port=int(os.getenv("REDIS_PORT", 6379)),
        password=os.getenv("REDIS_PASSWORD"),
        decode_responses=True
    )

    asyncio.create_task(listen_events())
    logger.info("Timeline Service started ✅")

@app.on_event("shutdown")
async def shutdown():
    if db_pool:
        await db_pool.close()
    if redis_client:
        await redis_client.aclose()

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
    await pubsub.subscribe("aim.timeline.event")
    logger.info("Timeline listening to event bus...")

    async for message in pubsub.listen():
        if message["type"] != "message":
            continue
        try:
            data = json.loads(message["data"])
            await log_event(TimelineEvent(
                event_type=data.get("event_type", "unknown"),
                description=data.get("description", ""),
                app=data.get("app"),
                project_id=data.get("project") if _UUID_RX.match(str(data.get("project") or "")) else None,
                metadata={**(data.get("metadata") or {}), **({"project_name": data.get("project")} if data.get("project") and not _UUID_RX.match(str(data.get("project"))) else {})},
                focus_score=data.get("focus_score", 0.0)
            ))
        except Exception as e:
            logger.error(f"Timeline event error: {e}")

# ── Core ops ──────────────────────────────────────

async def log_event(event: TimelineEvent) -> str:
    event_id = str(uuid4())
    now = datetime.utcnow()

    async with db_pool.acquire() as conn:
        await conn.execute("""
            INSERT INTO timeline_events
            (id, event_type, description, app, project_id,
             metadata, focus_score, started_at)
            VALUES ($1,$2,$3,$4,$5,$6,$7,$8)
        """,
            event_id,
            event.event_type,
            event.description,
            event.app,
            event.project_id,
            json.dumps(event.metadata),
            event.focus_score,
            now
        )

    logger.info(f"Logged: {event.event_type} — {event.description[:50]}")
    return event_id

# ── API ───────────────────────────────────────────

@app.post("/events", response_model=dict)
async def create_event(event: TimelineEvent):
    event_id = await log_event(event)
    return {"id": event_id, "status": "logged"}

@app.get("/events")
async def get_events(
    date: Optional[str] = Query(None, description="YYYY-MM-DD"),
    project_id: Optional[str] = None,
    event_type: Optional[str] = None,
    limit: int = 100
):
    if project_id is not None and not _UUID_RX.match(project_id):
        raise HTTPException(status_code=422, detail="project_id must be a UUID")
    conditions = []
    params = []
    idx = 1

    if date:
        start = datetime.strptime(date, "%Y-%m-%d")
        end   = start + timedelta(days=1)
        conditions.append(f"started_at >= ${idx} AND started_at < ${idx+1}")
        params.extend([start, end])
        idx += 2

    if project_id:
        conditions.append(f"project_id = ${idx}")
        params.append(project_id)
        idx += 1

    if event_type:
        conditions.append(f"event_type = ${idx}")
        params.append(event_type)
        idx += 1

    where = f"WHERE {' AND '.join(conditions)}" if conditions else ""
    query = f"""
        SELECT * FROM timeline_events
        {where}
        ORDER BY started_at DESC
        LIMIT {limit}
    """

    async with db_pool.acquire() as conn:
        rows = await conn.fetch(query, *params)

    return [dict(r) for r in rows]

@app.get("/events/today")
async def get_today():
    today = datetime.utcnow().strftime("%Y-%m-%d")
    return await get_events(date=today, limit=200)

@app.get("/stats")
async def get_stats(date: Optional[str] = None):
    if not date:
        date = datetime.utcnow().strftime("%Y-%m-%d")

    start = datetime.strptime(date, "%Y-%m-%d")
    end   = start + timedelta(days=1)

    async with db_pool.acquire() as conn:
        # Total events
        total = await conn.fetchval(
            "SELECT COUNT(*) FROM timeline_events WHERE started_at >= $1 AND started_at < $2",
            start, end
        )
        # By type
        by_type = await conn.fetch("""
            SELECT event_type, COUNT(*) as count
            FROM timeline_events
            WHERE started_at >= $1 AND started_at < $2
            GROUP BY event_type
            ORDER BY count DESC
        """, start, end)

        # By app
        by_app = await conn.fetch("""
            SELECT app, COUNT(*) as count
            FROM timeline_events
            WHERE started_at >= $1 AND started_at < $2
            AND app IS NOT NULL
            GROUP BY app
            ORDER BY count DESC
            LIMIT 10
        """, start, end)

        # Focus score average
        focus = await conn.fetchval("""
            SELECT AVG(focus_score)
            FROM timeline_events
            WHERE started_at >= $1 AND started_at < $2
        """, start, end)

    return {
        "date": date,
        "total_events": total,
        "avg_focus_score": round(float(focus or 0), 2),
        "by_type": [dict(r) for r in by_type],
        "by_app": [dict(r) for r in by_app]
    }

@app.get("/events/week")
async def get_week():
    today = datetime.utcnow()
    week_start = today - timedelta(days=7)

    async with db_pool.acquire() as conn:
        rows = await conn.fetch("""
            SELECT
                DATE(started_at) as date,
                COUNT(*) as events,
                AVG(focus_score) as avg_focus
            FROM timeline_events
            WHERE started_at >= $1
            GROUP BY DATE(started_at)
            ORDER BY date DESC
        """, week_start)

    return [dict(r) for r in rows]

@app.delete("/events/test")
async def delete_test_events():
    """Removes only rows created by the automated test suite (marker: aimtest)."""
    async with db_pool.acquire() as conn:
        r = await conn.execute(
            "DELETE FROM timeline_events WHERE event_type LIKE 'aimtest%' OR description LIKE '%aimtest%' OR metadata::text LIKE '%aimtest%'")
    return {"status": "deleted", "result": r}

@app.get("/health")
async def health():
    return {"status": "ok", "service": "timeline"}