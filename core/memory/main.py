import os
import asyncio
import logging
import json
from datetime import datetime, timedelta
from typing import Optional, List
from uuid import uuid4

import asyncpg
import httpx
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
import redis.asyncio as redis
from qdrant_client import QdrantClient
from qdrant_client.models import (
    Distance, VectorParams, PointStruct, Filter,
    FieldCondition, MatchValue
)

logging.basicConfig(level=os.getenv("AIM_LOG_LEVEL", "INFO"))
logger = logging.getLogger("aim.memory")

from fastapi.middleware.cors import CORSMiddleware

app = FastAPI(title="AIM Memory Engine", version="0.1.0")

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
            "dst": "memory", "method": request.method, "path": p, "status": status,
            "ms": int((_aim_t.time() - t0) * 1000), "ip": request.client.host if request.client else "",
            "size": int(request.headers.get("content-length") or 0)},)).start()

app.add_middleware(
    CORSMiddleware,
    allow_origins=["http://localhost:8888", "http://127.0.0.1:8888"],
    allow_methods=["*"],
    allow_headers=["*"],
)
# ── Models ────────────────────────────────────────

class MemoryCreate(BaseModel):
    content: str
    source: str
    importance: float = 0.5
    confidence: float = 0.5
    tags: List[str] = []
    project_id: Optional[str] = None
    tier: str = "short"

class MemoryResponse(BaseModel):
    id: str
    content: str
    source: str
    importance: float
    confidence: float
    tags: List[str]
    tier: str
    created_at: str

class SearchRequest(BaseModel):
    query: str
    limit: int = 5
    tier: Optional[str] = None
    project_id: Optional[str] = None

# ── Clients ───────────────────────────────────────

db_pool = None
redis_client = None
qdrant = None

COLLECTION = "aim_memories"
VECTOR_SIZE = 768
OLLAMA_HOST = os.getenv("OLLAMA_HOST", "http://host.docker.internal:11434")
EMBED_MODEL  = os.getenv("OLLAMA_EMBED_MODEL", "nomic-embed-text")

@app.on_event("startup")
async def startup():
    global db_pool, redis_client, qdrant

    # Postgres
    db_pool = await asyncpg.create_pool(
        host=os.getenv("POSTGRES_HOST", "postgres"),
        port=int(os.getenv("POSTGRES_PORT", 5432)),
        database=os.getenv("POSTGRES_DB", "aimdb"),
        user=os.getenv("POSTGRES_USER", "aim"),
        password=os.getenv("POSTGRES_PASSWORD"),
        min_size=2, max_size=10
    )

    # Redis
    redis_client = redis.Redis(
        host=os.getenv("REDIS_HOST", "redis"),
        port=int(os.getenv("REDIS_PORT", 6379)),
        password=os.getenv("REDIS_PASSWORD"),
        decode_responses=True
    )

    # Qdrant
    qdrant = QdrantClient(
        host=os.getenv("QDRANT_HOST", "qdrant"),
        port=int(os.getenv("QDRANT_PORT", 6333))
    )

    # Create collection if not exists
    existing = [c.name for c in qdrant.get_collections().collections]
    if COLLECTION not in existing:
        qdrant.create_collection(
            collection_name=COLLECTION,
            vectors_config=VectorParams(
                size=VECTOR_SIZE,
                distance=Distance.COSINE
            )
        )
        logger.info(f"Created Qdrant collection: {COLLECTION}")

    logger.info("Memory Engine started ✅")

@app.on_event("shutdown")
async def shutdown():
    if db_pool:
        await db_pool.close()
    if redis_client:
        await redis_client.aclose()

# ── Embedding ─────────────────────────────────────

async def get_embedding(text: str) -> List[float]:
    async with httpx.AsyncClient(timeout=30) as client:
        resp = await client.post(
            f"{OLLAMA_HOST}/api/embeddings",
            json={"model": EMBED_MODEL, "prompt": text[:6000]}
        )
        resp.raise_for_status()
        return resp.json()["embedding"]

import re as _re
_UUID_RX = _re.compile(r"^[0-9a-fA-F]{8}-([0-9a-fA-F]{4}-){3}[0-9a-fA-F]{12}$")


def _need_uuid(i: str):
    if not _UUID_RX.match(i or ""):
        raise HTTPException(status_code=404, detail="Not found (invalid id)")


# ── Memory ops ────────────────────────────────────

@app.post("/memories", response_model=MemoryResponse)
async def store_memory(mem: MemoryCreate):
    memory_id = str(uuid4())

    # Get embedding
    embedding = await get_embedding(mem.content)

    # Store in Qdrant
    qdrant.upsert(
        collection_name=COLLECTION,
        points=[PointStruct(
            id=memory_id,
            vector=embedding,
            payload={
                "content": mem.content,
                "source": mem.source,
                "tags": mem.tags,
                "tier": mem.tier,
                "project_id": mem.project_id,
                "importance": mem.importance
            }
        )]
    )

    # Store in Postgres
    now = datetime.utcnow()
    expires = now + timedelta(days=7) if mem.tier == "short" else None

    async with db_pool.acquire() as conn:
        await conn.execute("""
            INSERT INTO memories
            (id, content, tier, importance, confidence, source,
             project_id, tags, embedding_id, created_at, expires_at)
            VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11)
        """,
            memory_id, mem.content, mem.tier,
            mem.importance, mem.confidence, mem.source,
            mem.project_id, mem.tags, memory_id,
            now, expires
        )

    # Publish event
    if redis_client:
        await redis_client.publish("aim.memory.stored", json.dumps({
            "id": memory_id,
            "source": mem.source,
            "tier": mem.tier,
            "importance": mem.importance
        }))

    logger.info(f"Stored memory {memory_id} from {mem.source}")

    return MemoryResponse(
        id=memory_id,
        content=mem.content,
        source=mem.source,
        importance=mem.importance,
        confidence=mem.confidence,
        tags=mem.tags,
        tier=mem.tier,
        created_at=now.isoformat()
    )

@app.post("/memories/search")
async def search_memories(req: SearchRequest):
    # Get query embedding
    embedding = await get_embedding(req.query)

    # Search Qdrant
    results = qdrant.search(
        collection_name=COLLECTION,
        query_vector=embedding,
        limit=req.limit,
        with_payload=True
    )

    memories = []
    for r in results:
        memories.append({
            "id": r.id,
            "content": r.payload["content"],
            "source": r.payload["source"],
            "score": r.score,
            "tags": r.payload.get("tags", []),
            "tier": r.payload.get("tier", "short"),
            "importance": r.payload.get("importance", 0.5)
        })

    return {"query": req.query, "results": memories}

@app.get("/memories/{memory_id}")
async def get_memory(memory_id: str):
    _need_uuid(memory_id)
    async with db_pool.acquire() as conn:
        row = await conn.fetchrow(
            "SELECT * FROM memories WHERE id = $1", memory_id
        )
    if not row:
        raise HTTPException(status_code=404, detail="Memory not found")
    return dict(row)

@app.delete("/memories/{memory_id}")
async def delete_memory(memory_id: str):
    _need_uuid(memory_id)
    async with db_pool.acquire() as conn:
        await conn.execute(
            "DELETE FROM memories WHERE id = $1", memory_id
        )
    qdrant.delete(
        collection_name=COLLECTION,
        points_selector=[memory_id]
    )
    if redis_client:
        await redis_client.publish("aim.memory.deleted", memory_id)
    return {"deleted": memory_id}

@app.get("/memories")
async def list_memories(tier: Optional[str] = None, limit: int = 20):
    query = "SELECT * FROM memories"
    params = []
    if tier:
        query += " WHERE tier = $1"
        params.append(tier)
    query += f" ORDER BY importance DESC, created_at DESC LIMIT {limit}"

    async with db_pool.acquire() as conn:
        rows = await conn.fetch(query, *params)
    return [dict(r) for r in rows]

# ── Memory decay (run periodically) ──────────────

@app.post("/memories/decay")
async def run_decay():
    decay_days = int(os.getenv("AIM_MEMORY_DECAY_DAYS", 30))
    cutoff = datetime.utcnow() - timedelta(days=decay_days)

    async with db_pool.acquire() as conn:
        # Archive old low-importance short-term memories
        archived = await conn.fetch("""
            UPDATE memories
            SET tier = 'archive'
            WHERE tier = 'short'
            AND importance < 0.3
            AND created_at < $1
            RETURNING id
        """, cutoff)

        # Delete expired memories
        deleted = await conn.fetch("""
            DELETE FROM memories
            WHERE expires_at IS NOT NULL
            AND expires_at < NOW()
            RETURNING id
        """)

    logger.info(f"Decay: archived {len(archived)}, deleted {len(deleted)}")
    return {
        "archived": len(archived),
        "deleted": len(deleted)
    }

# ── Research Center projects (persisted in PostgreSQL) ───────────────────

_rp_ready = False


async def _rp_ensure():
    global _rp_ready
    if _rp_ready:
        return
    async with db_pool.acquire() as conn:
        await conn.execute("""
            CREATE TABLE IF NOT EXISTS research_projects (
                id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
                name TEXT NOT NULL,
                kind TEXT NOT NULL DEFAULT 'scanned',
                type TEXT,
                idea TEXT,
                root TEXT,
                graph JSONB NOT NULL DEFAULT '{}'::jsonb,
                overrides JSONB NOT NULL DEFAULT '{}'::jsonb,
                created_at TIMESTAMPTZ DEFAULT NOW(),
                updated_at TIMESTAMPTZ DEFAULT NOW()
            )""")
    _rp_ready = True


class ProjectIn(BaseModel):
    name: str
    kind: str = "scanned"      # scanned | planned
    type: Optional[str] = None
    idea: Optional[str] = None
    root: Optional[str] = None
    graph: dict = {}
    overrides: dict = {}


def _rp_row(r, full=True):
    d = {"id": str(r["id"]), "name": r["name"], "kind": r["kind"], "type": r["type"], "idea": r["idea"], "root": r["root"],
         "created_at": r["created_at"].isoformat(), "updated_at": r["updated_at"].isoformat()}
    if full:
        g = r["graph"]; o = r["overrides"]
        d["graph"] = json.loads(g) if isinstance(g, str) else g
        d["overrides"] = json.loads(o) if isinstance(o, str) else o
    else:
        g = r["graph"]; g = json.loads(g) if isinstance(g, str) else g
        d["nodes"] = len(g.get("nodes", [])); d["edges"] = len(g.get("edges", []))
    return d


@app.get("/research/projects")
async def rp_list():
    await _rp_ensure()
    async with db_pool.acquire() as conn:
        rows = await conn.fetch("SELECT * FROM research_projects ORDER BY updated_at DESC")
    return [_rp_row(r, full=False) for r in rows]


@app.get("/research/projects/{pid}")
async def rp_get(pid: str):
    _need_uuid(pid)
    await _rp_ensure()
    async with db_pool.acquire() as conn:
        r = await conn.fetchrow("SELECT * FROM research_projects WHERE id = $1::uuid", pid)
    if not r:
        raise HTTPException(status_code=404, detail="Project not found")
    return _rp_row(r)


@app.post("/research/projects")
async def rp_create(p: ProjectIn):
    await _rp_ensure()
    p.graph = {"nodes": [], "edges": [], **(p.graph or {})}
    async with db_pool.acquire() as conn:
        r = await conn.fetchrow(
            "INSERT INTO research_projects (name, kind, type, idea, root, graph, overrides) "
            "VALUES ($1,$2,$3,$4,$5,$6::jsonb,$7::jsonb) RETURNING *",
            p.name, p.kind, p.type, p.idea, p.root, json.dumps(p.graph), json.dumps(p.overrides))
    return _rp_row(r)


@app.put("/research/projects/{pid}")
async def rp_update(pid: str, p: ProjectIn):
    _need_uuid(pid)
    await _rp_ensure()
    p.graph = {"nodes": [], "edges": [], **(p.graph or {})}
    async with db_pool.acquire() as conn:
        r = await conn.fetchrow(
            "UPDATE research_projects SET name=$2, kind=$3, type=$4, idea=$5, root=$6, graph=$7::jsonb, overrides=$8::jsonb, "
            "updated_at=NOW() WHERE id=$1::uuid RETURNING *",
            pid, p.name, p.kind, p.type, p.idea, p.root, json.dumps(p.graph), json.dumps(p.overrides))
    if not r:
        raise HTTPException(status_code=404, detail="Project not found")
    return _rp_row(r)


@app.delete("/research/projects/{pid}")
async def rp_delete(pid: str):
    _need_uuid(pid)
    await _rp_ensure()
    async with db_pool.acquire() as conn:
        await conn.execute("DELETE FROM research_projects WHERE id = $1::uuid", pid)
    return {"deleted": pid}


@app.get("/health")
async def health():
    return {"status": "ok", "service": "memory"}