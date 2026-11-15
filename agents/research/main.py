"""
AIM Research Agent
  - /research            web research (Gemini grounded search, falls back to the LLM router)
  - Research Center API: projects (scan / plan), live edge health, real traffic stream, completion tracker, AI help
Live signals only: service probes, HTTP traces reported by the services, Redis bus traffic and Redis subscriber counts.
"""
import asyncio
import hashlib
import json
import logging
import os
import re
import socket
import time
from collections import deque
from datetime import datetime
from typing import Optional, List

import httpx
import redis.asyncio as redis
from fastapi import FastAPI, HTTPException, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import StreamingResponse
from pydantic import BaseModel

import planner
import scanner
import suites_aim
import suites_generic
import tester

logging.basicConfig(level=os.getenv("AIM_LOG_LEVEL", "INFO"))
logger = logging.getLogger("aim.research-agent")
logging.getLogger("httpx").setLevel(logging.WARNING)  # request URLs must never reach the logs

app = FastAPI(title="AIM Research Agent", version="0.2.0")
app.add_middleware(CORSMiddleware, allow_origins=["http://localhost:8888", "http://127.0.0.1:8888"], allow_methods=["*"], allow_headers=["*"])

GOOGLE_KEY = os.getenv("GOOGLE_API_KEY")
MEMORY_URL = os.getenv("MEMORY_URL", "http://memory:8003")
TIMELINE_URL = os.getenv("TIMELINE_URL", "http://timeline:8002")
LLM_ROUTER = os.getenv("LLM_ROUTER_URL", "http://llm-router:8001")
PROJECT_ROOTS = os.getenv("PROJECT_ROOTS", "/projects")
SELF = "research-agent"

redis_client = None


# ═════════════════════════ live telemetry (real signals only) ═════════════════════════
TRACES = deque(maxlen=4000)          # {ts,src,dst,method,path,status,ms,size}
BUS = {}                             # channel -> {count,last_ts,bytes,recent:deque[ts]}
CLIENTS = set()                      # SSE queues
IPMAP = {}                           # container ip -> service name
KNOWN_SVC = set()
PROBE_CACHE = {}
NODE_HIST = {}                       # (pid,nid) -> {last_ok,last_err}
EDGE_HIST = {}
LIVE_CACHE = {}
LAST_PCT = {}
RL_UNTIL = 0.0                       # LLM rate-limit cooldown
SKIP_TRACE = ("/projects", "/stream", "/ai", "/trace", "/folders", "/health", "/docs", "/openapi")


def broadcast(ev: dict):
    for q in list(CLIENTS):
        try:
            q.put_nowait(ev)
        except asyncio.QueueFull:
            pass


def record_trace(ev: dict):
    ip = ev.get("ip", "")
    src = IPMAP.get(ip) or ("host" if ip.endswith(".1") or ip in ("127.0.0.1", "::1") else "")
    out = {"ts": time.time(), "src": src, "dst": ev.get("dst", ""), "method": ev.get("method", "GET"),
           "path": ev.get("path", ""), "status": int(ev.get("status", 0)), "ms": int(ev.get("ms", 0)), "size": int(ev.get("size", 0))}
    TRACES.append(out)
    broadcast({"t": "http", **out})


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
async def _trace_self(request: Request, call_next):
    p = request.url.path
    if request.method == "OPTIONS" or p.startswith(SKIP_TRACE):
        return await call_next(request)
    t0 = time.time()
    resp = await call_next(request)
    record_trace({"dst": SELF, "method": request.method, "path": p, "status": resp.status_code,
                  "ms": int((time.time() - t0) * 1000), "ip": request.client.host if request.client else "",
                  "size": int(request.headers.get("content-length") or 0)})
    return resp


class TraceIn(BaseModel):
    dst: str
    method: str = "GET"
    path: str = "/"
    status: int = 200
    ms: int = 0
    ip: str = ""
    size: int = 0


@app.post("/trace")
async def trace(ev: TraceIn):
    record_trace(ev.model_dump())
    return {"ok": True}


async def bus_monitor():
    await asyncio.sleep(2)
    while True:
        try:
            ps = redis_client.pubsub()
            await ps.psubscribe("*")
            async for m in ps.listen():
                if m["type"] != "pmessage":
                    continue
                ch, data = m["channel"], m["data"]
                st = BUS.setdefault(ch, {"count": 0, "last_ts": 0, "bytes": 0, "recent": deque(maxlen=300)})
                n = len(data) if isinstance(data, (str, bytes)) else 0
                now = time.time()
                st["count"] += 1; st["last_ts"] = now; st["bytes"] += n; st["recent"].append(now)
                broadcast({"t": "bus", "ch": ch, "size": n, "ts": now})
        except Exception as e:
            logger.warning("bus monitor restarting: %s", type(e).__name__)
            await asyncio.sleep(3)


async def ipmap_refresher():
    loop = asyncio.get_running_loop()
    while True:
        for name in list(KNOWN_SVC):
            try:
                ip = await loop.run_in_executor(None, socket.gethostbyname, name)
                IPMAP[ip] = name
            except Exception:
                pass
        await asyncio.sleep(20)


@app.on_event("startup")
async def startup():
    global redis_client
    redis_client = redis.Redis(host=os.getenv("REDIS_HOST", "redis"), port=int(os.getenv("REDIS_PORT", 6379)),
                               password=os.getenv("REDIS_PASSWORD"), decode_responses=True)
    KNOWN_SVC.update({"llm-router", "memory", "brain", "timeline", "notification", "voice-agent", "browser-agent",
                      "research-agent", "dashboard"})
    asyncio.create_task(listen_events())
    asyncio.create_task(bus_monitor())
    asyncio.create_task(ipmap_refresher())
    logger.info("Research Agent started")


@app.on_event("shutdown")
async def shutdown():
    if redis_client:
        await redis_client.aclose()


@app.get("/stream")
async def stream(request: Request):
    q: asyncio.Queue = asyncio.Queue(maxsize=300)
    CLIENTS.add(q)

    async def gen():
        try:
            yield "retry: 3000\n\n"
            while True:
                if await request.is_disconnected():
                    break
                try:
                    ev = await asyncio.wait_for(q.get(), timeout=15)
                    yield f"data: {json.dumps(ev)}\n\n"
                except asyncio.TimeoutError:
                    yield ": keepalive\n\n"
        finally:
            CLIENTS.discard(q)
    return StreamingResponse(gen(), media_type="text/event-stream",
                             headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"})


# ═════════════════════════ probes ═════════════════════════
async def probe_target(p: dict) -> dict:
    key = json.dumps(p, sort_keys=True)
    c = PROBE_CACHE.get(key)
    if c and time.time() - c[0] < 3:
        return c[1]
    mode = p.get("mode", "none")
    cands = []
    if p.get("service") and p.get("port"): cands.append((p["service"], p["port"]))
    if p.get("hostPort"): cands.append(("host.docker.internal", p["hostPort"]))
    if p.get("host") and p.get("port"): cands.append((p["host"], p["port"]))
    res = {"state": "unknown", "ms": None, "error": "no probe available"}
    for host, port in cands:
        t0 = time.time()
        try:
            if mode == "tcp":
                r, w = await asyncio.wait_for(asyncio.open_connection(host, int(port)), timeout=1.5)
                w.close()
                ms = int((time.time() - t0) * 1000)
                res = {"state": "slow" if ms > 800 else "ok", "ms": ms, "error": None}
            else:
                async with httpx.AsyncClient(timeout=2.5) as c2:
                    r = await c2.get(f"http://{host}:{port}{p.get('path') or '/'}")
                ms = int((time.time() - t0) * 1000)
                if r.status_code >= 500:
                    res = {"state": "fail", "ms": ms, "error": f"HTTP {r.status_code} from {p.get('path') or '/'}"}
                else:
                    res = {"state": "slow" if ms > 800 else "ok", "ms": ms, "error": None}
            break
        except Exception as e:
            res = {"state": "fail", "ms": None, "error": f"{type(e).__name__} reaching {host}:{port}"}
    PROBE_CACHE[key] = (time.time(), res)
    return res


# ═════════════════════════ live computation ═════════════════════════
def _svc_id(n):
    return n["id"] if n["kind"] in ("service", "db", "infra", "external", "storage", "api", "ui", "queue") or not n.get("parent") else n["parent"]


def _path_rx(path):
    if not path:
        return None
    return re.compile("^" + re.sub(r"\\\{[^}]*\\\}", "[^/]+", re.escape(path.split("?")[0].replace("{x}", "{x}"))) + "$")


def _edge_traffic(e, nodes, window=600):
    now = time.time()
    rx = _path_rx(e.get("path"))
    fn = nodes.get(e["from"], {})
    out = []
    for t in reversed(TRACES):
        if now - t["ts"] > window:
            break
        if t["dst"] != e.get("toSvc"):
            continue
        if e.get("method") and t["method"] != e["method"]:
            continue
        if rx and not rx.match(t["path"].split("?")[0]):
            continue
        s = t["src"]
        if s == "host":
            if fn.get("layer") != "ui":
                continue
        elif s and e.get("fromSvc") and s != e["fromSvc"]:
            continue
        out.append(t)
    return out


async def compute_live(p: dict) -> dict:
    pid = p["id"]
    c = LIVE_CACHE.get(pid)
    if c and time.time() - c[0] < 2:
        return c[1]
    g = p["graph"] = p.get("graph") or {}
    nodes = {n["id"]: n for n in g.get("nodes", [])}
    edges = g.get("edges", [])
    ov = p.get("overrides", {}) or {}
    planned = p["kind"] == "planned"
    now = time.time()
    for n in nodes.values():
        if n["kind"] == "service" and n["id"].startswith("svc:"):
            KNOWN_SVC.add(n["id"][4:])

    # ── nodes ──
    probes = {nid: n["probe"] for nid, n in nodes.items() if n.get("probe", {}).get("mode") in ("http", "tcp")}
    results = dict(zip(probes.keys(), await asyncio.gather(*[probe_target(pr) for pr in probes.values()]))) if probes else {}
    root = p.get("root")
    nst = {}
    for nid, n in nodes.items():
        if n.get("status") == "planned":
            built = bool(ov.get("items", {}).get(f"{nid}:built"))
            nst[nid] = {"state": "unknown" if built else "planned", "reason": "marked built manually - no live probe" if built else "planned, not built yet"}
        elif nid in results:
            nst[nid] = dict(results[nid])
        elif n["kind"] == "storage":
            ok = bool(root) and os.path.exists(os.path.join(root, n["path"]))
            nst[nid] = {"state": "ok" if ok else "unknown", "reason": "folder exists" if ok else "folder not created yet"}
        elif n["kind"] in ("external",):
            nst[nid] = {"state": "unknown", "reason": "external API - no health probe (see traffic on incoming links)"}
        elif n["kind"] in ("service", "infra", "db") and nid.startswith("svc:"):
            nst[nid] = {"state": "unknown", "reason": "no running endpoint (job or not published)"}
        else:
            nst[nid] = None  # file: inherit below
    for nid, n in nodes.items():
        if nst[nid] is None:
            par = n.get("parent")
            nst[nid] = dict(nst.get(par) or {"state": "ok", "reason": "static file"}) if par else {"state": "ok", "reason": "static file"}
            nst[nid]["inherited"] = bool(par)
        h = NODE_HIST.setdefault((pid, nid), {"last_ok": None, "last_err": None})
        s = nst[nid]
        if s["state"] in ("ok", "slow"): h["last_ok"] = now
        if s["state"] == "fail": h["last_err"] = {"ts": now, "msg": s.get("error") or s.get("reason")}
        s["last_ok"], s["last_err"] = h["last_ok"], h["last_err"]

    # ── edges ──
    chans = sorted({e["channel"] for e in edges if e.get("channel")})
    numsub = {}
    if redis_client and chans:
        try:
            raw = await redis_client.execute_command("PUBSUB", "NUMSUB", *chans)
            numsub = {raw[i]: int(raw[i + 1]) for i in range(0, len(raw), 2)}
        except Exception:
            numsub = {}
    est = {}
    for e in edges:
        a, b = nst.get(_svc_id(nodes[e["from"]]) if e["from"] in nodes else "", {}), nst.get(_svc_id(nodes[e["to"]]) if e["to"] in nodes else "", {})
        a = a or nst.get(e["from"], {"state": "unknown"}); b = b or nst.get(e["to"], {"state": "unknown"})
        a = nst.get(e["from"]) or a; b = nst.get(e["to"]) or b
        r = {"state": "ok", "reason": "", "side": None, "count": 0, "per_min": 0.0, "avg_ms": None, "bytes": 0}
        if planned or a.get("state") == "planned" or b.get("state") == "planned":
            r.update(state="planned", reason="planned - not built yet")
        elif e["kind"] == "http":
            tr = _edge_traffic(e, nodes)
            errs = [t for t in tr if t["status"] >= 500]
            r["count"] = len(tr); r["per_min"] = round(len([t for t in tr if now - t["ts"] < 60]), 1)
            r["avg_ms"] = int(sum(t["ms"] for t in tr) / len(tr)) if tr else None
            r["bytes"] = sum(t["size"] for t in tr)
            if a["state"] == "fail":
                r.update(state="fail", side="source", reason=f"source is down: {a.get('error')}")
            elif b["state"] == "fail":
                r.update(state="fail", side="target", reason=f"target is down: {b.get('error')}")
            elif len(tr) >= 2 and len(errs) / len(tr) >= 0.5:
                r.update(state="fail", side="target", reason=f"{len(errs)}/{len(tr)} recent calls returned HTTP {errs[0]['status']}")
            elif b["state"] == "slow" or (r["avg_ms"] or 0) > (30000 if (e.get("path") or "") in ("/route", "/research", "/api/generate", "/converse/text") or "ai/" in (e.get("path") or "") else 1500):
                r.update(state="slow", side="target", reason=f"slow responses ({r['avg_ms'] or b.get('ms')} ms)")
            elif b["state"] == "unknown" and not tr:
                r.update(state="unknown", reason=b.get("reason", "no signal"))
            else:
                r["reason"] = f"{len(tr)} calls in last 10 min" if tr else "target healthy, no recent calls"
            if errs:
                EDGE_HIST.setdefault((pid, e["id"]), {})["last_err"] = {"ts": errs[0]["ts"], "msg": f"HTTP {errs[0]['status']} on {errs[0]['method']} {errs[0]['path']}"}
            if tr and len(errs) < len(tr):
                EDGE_HIST.setdefault((pid, e["id"]), {})["last_ok"] = max(t["ts"] for t in tr if t["status"] < 500)
        elif e["kind"] == "redis":
            ch = e.get("channel")
            st = BUS.get(ch)
            subs = numsub.get(ch, 0)
            rec = [t for t in (st["recent"] if st else []) if now - t < 600]
            r["count"] = len(rec); r["per_min"] = float(len([t for t in rec if now - t < 60])); r["bytes"] = st["bytes"] if st else 0
            if st: EDGE_HIST.setdefault((pid, e["id"]), {})["last_ok"] = st["last_ts"]
            redis_nodes = [nid for nid, n in nodes.items() if n.get("dbkind") == "redis"]
            bus = nst.get(redis_nodes[0], {"state": "unknown"}) if redis_nodes else {"state": "unknown"}
            if bus["state"] == "fail":
                r.update(state="fail", side="bus", reason="Redis bus is down")
            elif a["state"] == "fail" and not e.get("dangling"):
                r.update(state="fail", side="source", reason=f"publisher is down: {a.get('error')}")
            elif b["state"] == "fail" and not e.get("dangling"):
                r.update(state="fail", side="target", reason=f"subscriber is down: {b.get('error')}")
            elif e.get("dangling") and e["from"] in nodes and nodes[e["from"]].get("dbkind") == "redis":
                if not st:
                    r.update(state="fail", side="source", reason="never receiving data: nothing in this project publishes the channel and no message was seen")
                else:
                    r["reason"] = f"messages seen ({st['count']})"
            elif e.get("dangling"):
                if subs == 0:
                    r.update(state="slow", side="target", reason="published, but no subscriber is listening")
            elif subs == 0:
                r.update(state="fail", side="target", reason="0 subscribers on this channel: the receiver is not listening")
            else:
                r["reason"] = f"{subs} subscriber(s), {st['count']} message(s) seen" if st else f"{subs} subscriber(s), no messages yet"
        elif e["kind"] == "db":
            if b["state"] == "fail":
                r.update(state="fail", side="target", reason=f"database unreachable: {b.get('error')}")
            elif a["state"] == "fail":
                r.update(state="fail", side="source", reason=f"source is down: {a.get('error')}")
            elif b["state"] == "slow":
                r.update(state="slow", side="target", reason=f"slow ({b.get('ms')} ms)")
            elif b["state"] == "unknown":
                r.update(state="unknown", reason=b.get("reason", "no signal"))
            else:
                r["reason"] = f"database answered in {b.get('ms')} ms"
        else:  # import / depends_on / file
            if b["state"] == "fail" and e["kind"] == "import" and e["label"] == "depends_on":
                r.update(state="fail", side="target", reason=f"dependency is down: {b.get('error')}")
            elif e["kind"] == "file":
                r.update(state=("ok" if b["state"] == "ok" else "unknown"), reason=b.get("reason", ""))
            else:
                r["reason"] = "static link"
        h = EDGE_HIST.setdefault((pid, e["id"]), {})
        r["last_ok"], r["last_err"] = h.get("last_ok"), h.get("last_err")
        if r["state"] == "fail" and not r["last_err"]:
            r["last_err"] = {"ts": now, "msg": r["reason"]}
            h["last_err"] = r["last_err"]
        est[e["id"]] = r

    comp = completion(p, nodes, edges, nst, est)
    out = {"ts": now, "nodes": nst, "edges": est, "completion": comp}
    LIVE_CACHE[pid] = (now, out)
    asyncio.create_task(log_progress(p, comp["pct"]))
    return out


def completion(p, nodes, edges, nst, est):
    g = p["graph"]; ov = (p.get("overrides") or {}).get("items", {}); tests = g.get("tests", {})
    planned = p["kind"] == "planned"
    mods = [n for n in nodes.values() if n["kind"] in ("service", "db", "infra", "api", "ui", "queue", "file") and (
        n["id"].startswith("svc:") or planned or not any(m.get("id", "").startswith("svc:") for m in nodes.values()))]
    if not planned and any(n["id"].startswith("svc:") for n in nodes.values()):
        mods = [n for n in nodes.values() if n["id"].startswith("svc:") and n["kind"] in ("service", "db", "infra") and not n.get("job")]
    elif not planned:   # no compose: group files by top folder
        groups = {}
        for n in nodes.values():
            if n["kind"] == "file":
                groups.setdefault(n["path"].split("/")[0] if "/" in n["path"] else "(root)", []).append(n)
        mods = [{"id": f"dir:{k}", "name": k, "kind": "service", "files": [x["id"] for x in v], "lines": sum(x["lines"] for x in v)} for k, v in groups.items()]
    modules, blockers, done_all, total_all = [], [], 0, 0
    for n in mods:
        nid = n["id"]
        in_edges = [e for e in edges if (e["from"] == nid or e["to"] == nid or (n.get("files") and (e["from"] in n["files"] or e["to"] in n["files"])))
                    and e["kind"] in ("http", "redis", "db")]
        sts = [est[e["id"]]["state"] for e in in_edges if e["id"] in est]
        red = [e for e in in_edges if est.get(e["id"], {}).get("state") == "fail"]
        items = []

        def item(key, label, auto, evidence):
            iid = f"{nid}:{key}"
            manual = ov.get(iid)
            done = bool(manual) if manual is not None else bool(auto)
            items.append({"id": iid, "key": key, "label": label, "done": done, "manual": manual is not None, "evidence": evidence})
        if planned:
            item("built", "Built", False, "planned" if n.get("status") == "planned" else "")
        else:
            lines = n.get("lines", 0)
            item("built", "Built", True, f"{lines} lines" if lines else (n.get("path") or "container"))
        if n.get("kind") in ("service",) or planned:
            svcname = nid[4:] if nid.startswith("svc:") else n.get("name")
            tf = tests.get(svcname) or []
            item("tested", "Tested", bool(tf), (", ".join(tf[:2]) if tf else "no test references this module"))
        if sts:
            ok_cnt = sum(1 for s in sts if s in ("ok", "slow"))
            item("connected", "Connected", not red and ok_cnt > 0 and nst.get(nid, {}).get("state") != "fail",
                 f"{ok_cnt}/{len(sts)} links green" + (f", {len(red)} failing" if red else ""))
        else:
            item("connected", "Connected", (not planned) and nst.get(nid, {}).get("state") in ("ok", "slow"),
                 "healthy" if nst.get(nid, {}).get("state") in ("ok", "slow") else "no live signal")
        d = sum(1 for i in items if i["done"])
        done_all += d; total_all += len(items)
        modules.append({"id": nid, "name": n.get("name", nid), "pct": round(100 * d / len(items)), "items": items})
        for e in red:
            blockers.append({"kind": "link", "module": n.get("name"), "edge": e["id"],
                             "text": f"Link {e['label']} ({_nm(nodes, e['from'])} -> {_nm(nodes, e['to'])}) is failing: {est[e['id']]['reason']}"})
        for i in items:
            if not i["done"] and i["key"] != "connected":
                blockers.append({"kind": i["key"], "module": n.get("name"), "item": i["id"], "text": f"{n.get('name')}: not {i['key']}" + (f" ({i['evidence']})" if i["evidence"] and i["key"] == "tested" else "")})
    tasks = []
    for t in g.get("tasks", []):
        auto = t.get("auto")
        if auto == "services_up":
            svcs = [n for n in nodes.values() if n["id"].startswith("svc:") and n["kind"] in ("service", "db", "infra") and not n.get("job")]
            done = bool(svcs) and all((nst.get(n["id"]) or {}).get("state") in ("ok", "slow") for n in svcs)
        elif auto == "tests_pass":
            lt = (p.get("overrides") or {}).get("last_test") or {}
            done = bool(lt) and (lt.get("summary") or {}).get("fail", 1) == 0
        elif auto == "tavily_key":
            done = bool(TAVILY_KEY)
        else:
            done = bool(ov.get(f"task:{t['id']}"))
        tasks.append({"id": f"task:{t['id']}", "label": t["label"], "done": done, "auto": bool(auto)})
        done_all += int(done); total_all += 1
        if not done: blockers.append({"kind": "task", "module": None, "item": f"task:{t['id']}", "text": t["label"]})
    seen, uniq = set(), []
    for b in blockers:
        if b["text"] not in seen: seen.add(b["text"]); uniq.append(b)
    order = {"link": 0, "built": 1, "tested": 2, "task": 3}
    uniq.sort(key=lambda b: order.get(b["kind"], 4))
    return {"pct": round(100 * done_all / total_all) if total_all else 0, "modules": modules, "tasks": tasks,
            "blockers": uniq[:14], "blocker_count": len(uniq)}


def _nm(nodes, i):
    n = nodes.get(i, {})
    return n.get("path") if n.get("kind") == "file" and n.get("path") else n.get("name", i)


async def log_progress(p, pct):
    pid = p["id"]
    last = LAST_PCT.get(pid)
    if last and last[0] == pct and time.time() - last[1] < 900:
        return
    if last and time.time() - last[1] < 60:
        return
    LAST_PCT[pid] = (pct, time.time())
    try:
        async with httpx.AsyncClient(timeout=5) as c:
            await c.post(f"{TIMELINE_URL}/events", json={"event_type": "research.progress", "app": "research",
                         "description": f"{p['name']}: {pct}% complete", "metadata": {"project": pid, "pct": pct}})
    except Exception:
        pass


# ═════════════════════════ projects ═════════════════════════
class OpenReq(BaseModel):
    path: str
    name: Optional[str] = None


class NewReq(BaseModel):
    name: str
    type: str = "webapp"
    idea: str = ""
    approach: Optional[dict] = None      # the option the owner picked after the comparison
    comparison: Optional[dict] = None
    motive: Optional[str] = None         # why the owner wants this (from "think with me")
    novelty: Optional[dict] = None


class OverrideReq(BaseModel):
    key: str
    value: Optional[bool] = None


PCACHE = {}      # last good copy of project reads: the map must keep working (and show memory as RED) while the memory service is down


async def mem(method, path, **kw):
    try:
        async with httpx.AsyncClient(timeout=30) as c:
            r = await c.request(method, f"{MEMORY_URL}{path}", **kw)
    except httpx.HTTPError as e:
        if method == "GET" and path in PCACHE:
            return PCACHE[path]
        raise HTTPException(503, f"memory service unreachable ({type(e).__name__})")
    if r.status_code >= 400:
        raise HTTPException(r.status_code, r.text[:300])
    data = r.json()
    if method == "GET" and path.startswith("/research/projects"):
        PCACHE[path] = data
    return data


def _safe_root(path: str) -> str:
    real = os.path.realpath(path)
    base = os.path.realpath(PROJECT_ROOTS)
    if not (real == base or real.startswith(base + os.sep)) or not os.path.isdir(real):
        raise HTTPException(400, f"Folder must exist under {PROJECT_ROOTS} (mounted: aim-os, workspace)")
    return real


@app.get("/folders")
async def folders():
    out = []
    base = PROJECT_ROOTS
    if os.path.isdir(base):
        for d in sorted(os.listdir(base)):
            full = os.path.join(base, d)
            if os.path.isdir(full):
                out.append({"name": d, "path": full, "stack": scanner.detect_stack(full)})
                if d == "workspace":
                    for s in sorted(os.listdir(full)):
                        sp = os.path.join(full, s)
                        if os.path.isdir(sp) and not s.startswith("."):
                            out.append({"name": f"workspace/{s}", "path": sp, "stack": scanner.detect_stack(sp)})
    return out


@app.get("/projects")
async def projects():
    return await mem("GET", "/research/projects")


KEEP_KEYS = ("understanding", "tasks", "motive", "source", "novelty")


async def _open_root(root: str, name: Optional[str], source: Optional[str] = None):
    loop = asyncio.get_running_loop()
    graph = await loop.run_in_executor(None, scanner.scan, root, name)
    if source:
        graph["source"] = {"git": source}
    existing = next((x for x in await mem("GET", "/research/projects") if x.get("root") == root), None)
    body = {"name": name or graph["project"], "kind": "scanned", "type": graph["type"], "root": root, "graph": graph, "overrides": {}}
    if existing:
        old = await mem("GET", f"/research/projects/{existing['id']}")
        body["overrides"] = old.get("overrides", {})
        for k in KEEP_KEYS:
            if old["graph"].get(k) is not None and graph.get(k) is None:
                graph[k] = old["graph"][k]
        return await mem("PUT", f"/research/projects/{existing['id']}", json=body)
    return await mem("POST", "/research/projects", json=body)


@app.post("/projects/open")
async def open_project(req: OpenReq):
    return await _open_root(_safe_root(req.path), req.name)


# ── clone a public git repo into workspace/, scan it (nothing in it is ever executed) ──
GIT_RX = re.compile(r"^https://(github\.com|gitlab\.com|bitbucket\.org|codeberg\.org)/([\w.-]+)/([\w.-]+?)(?:\.git)?/?$")
MAX_REPO_MB = 600


class CloneReq(BaseModel):
    url: str


async def _git(args, cwd=None, timeout=240):
    env = {"PATH": os.environ.get("PATH", ""), "HOME": "/tmp", "GIT_TERMINAL_PROMPT": "0", "GIT_LFS_SKIP_SMUDGE": "1",
           "GIT_ASKPASS": "/bin/true"}      # minimal env: no API keys reach git
    proc = await asyncio.create_subprocess_exec("git", "-c", "core.hooksPath=/dev/null", "-c", "protocol.allow=never",
                                                "-c", "protocol.https.allow=always", *args, cwd=cwd, env=env,
                                                stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE)
    try:
        _, err = await asyncio.wait_for(proc.communicate(), timeout)
    except asyncio.TimeoutError:
        proc.kill()
        raise HTTPException(504, "git took too long (over 4 minutes)")
    return proc.returncode, err.decode(errors="ignore")[-300:]


def _dir_mb(path, cap):
    total = 0
    for d, _, fs in os.walk(path):
        for f in fs:
            try:
                total += os.path.getsize(os.path.join(d, f))
            except OSError:
                pass
            if total > cap * 1024 * 1024:
                return total // (1024 * 1024)
    return total // (1024 * 1024)


@app.post("/projects/clone")
async def clone_project(req: CloneReq):
    url = req.url.strip()
    m = GIT_RX.match(url)
    if not m:
        raise HTTPException(400, "Paste an https link to a public repo, e.g. https://github.com/owner/repo (GitHub, GitLab, Bitbucket or Codeberg)")
    owner, repo = m.group(2), m.group(3)
    base = os.path.join(PROJECT_ROOTS, "workspace")
    dest = os.path.join(base, f"{owner}__{repo}")
    if not os.path.isdir(base):
        raise HTTPException(500, "workspace folder is not mounted")
    if os.path.isdir(os.path.join(dest, ".git")):
        code, err = await _git(["pull", "--ff-only", "--depth", "1"], cwd=dest)
    else:
        code, err = await _git(["clone", "--depth", "1", "--single-branch", "--no-tags", url, dest])
    if code != 0:
        low = err.lower()
        hint = "Repo not found, or it is private (only public repos work for now)." if ("not found" in low or "authentication" in low or "could not read" in low) else err.strip()[-200:]
        raise HTTPException(400, f"Could not clone: {hint}")
    mb = await asyncio.get_running_loop().run_in_executor(None, _dir_mb, dest, MAX_REPO_MB)
    if mb > MAX_REPO_MB:
        import shutil
        shutil.rmtree(dest, ignore_errors=True)
        raise HTTPException(400, f"Repo is larger than {MAX_REPO_MB} MB - too big to analyse here")
    return await _open_root(_safe_root(dest), repo, source=url)


@app.post("/projects/new")
async def new_project(req: NewReq):
    if not req.name.strip():
        raise HTTPException(400, "Name required")
    graph = planner.plan(req.name.strip(), req.type, req.idea, req.approach, req.comparison, req.motive, req.novelty)
    return await mem("POST", "/research/projects", json={"name": req.name.strip(), "kind": "planned", "type": req.type,
                                                        "idea": req.idea, "graph": graph, "overrides": {}})


@app.get("/projects/{pid}")
async def get_project(pid: str):
    return await mem("GET", f"/research/projects/{pid}")


@app.delete("/projects/{pid}")
async def delete_project(pid: str):
    LIVE_CACHE.pop(pid, None)
    return await mem("DELETE", f"/research/projects/{pid}")


@app.post("/projects/{pid}/rescan")
async def rescan(pid: str):
    p = await mem("GET", f"/research/projects/{pid}")
    if p["kind"] != "scanned" or not p.get("root"):
        raise HTTPException(400, "Only opened folders can be rescanned")
    graph = await asyncio.get_running_loop().run_in_executor(None, scanner.scan, _safe_root(p["root"]), p["name"])
    for k in KEEP_KEYS:
        if p["graph"].get(k) is not None:
            graph[k] = p["graph"][k]
    LIVE_CACHE.pop(pid, None)
    return await mem("PUT", f"/research/projects/{pid}", json={**{k: p[k] for k in ("name", "kind", "type", "idea", "root", "overrides")}, "graph": graph})


@app.get("/projects/{pid}/live")
async def live(pid: str):
    p = await mem("GET", f"/research/projects/{pid}")
    return await compute_live(p)


@app.post("/projects/{pid}/override")
async def override(pid: str, req: OverrideReq):
    p = await mem("GET", f"/research/projects/{pid}")
    ov = p.get("overrides") or {}
    items = ov.setdefault("items", {})
    if req.value is None:
        items.pop(req.key, None)
    else:
        items[req.key] = req.value
    LIVE_CACHE.pop(pid, None)
    await mem("PUT", f"/research/projects/{pid}", json={**{k: p[k] for k in ("name", "kind", "type", "idea", "root", "graph")}, "overrides": ov})
    return {"ok": True}


@app.get("/projects/{pid}/history")
async def history(pid: str):
    await mem("GET", f"/research/projects/{pid}")      # 404 for unknown projects
    try:
        async with httpx.AsyncClient(timeout=10) as c:
            r = await c.get(f"{TIMELINE_URL}/events", params={"event_type": "research.progress", "limit": 500})
        pts = []
        for ev in r.json():
            md = ev.get("metadata")
            md = json.loads(md) if isinstance(md, str) else (md or {})
            if md.get("project") == pid:
                pts.append({"t": ev["started_at"], "pct": md.get("pct", 0)})
        pts.sort(key=lambda x: x["t"])
        return pts
    except Exception:
        return []


# ═════════════════════════ AI help (on click only, cached, rate-limit aware) ═════════════════════════
class ExplainReq(BaseModel):
    project_id: str
    target: str          # node or edge id
    kind: str = "node"   # node | edge


class MissingReq(BaseModel):
    project_id: str


def _snippet(root, rel, n=3500):
    try:
        with open(os.path.join(root, rel), encoding="utf-8", errors="ignore") as f:
            return f.read(n)
    except Exception:
        return ""


async def cache_get(p, key):
    """Exact repeats are free: answers live in the project record (PostgreSQL via the memory service)."""
    return ((p.get("overrides") or {}).get("ai_cache") or {}).get(key)


async def cache_put(p, key, text):
    ov = p.get("overrides") or {}
    ov.setdefault("ai_cache", {})[key] = text
    try:
        await mem("PUT", f"/research/projects/{p['id']}", json={**{k: p[k] for k in ("name", "kind", "type", "idea", "root", "graph")}, "overrides": ov})
        # semantic copy in memory (skipped when a near-duplicate, score >= 0.92, already exists)
        head = text[:300]
        near = (await mem("POST", "/memories/search", json={"query": head, "limit": 1})).get("results", [])
        if not (near and near[0].get("score", 0) >= 0.92):
            await mem("POST", "/memories", json={"content": f"Research Center note ({p['name']}): {text[:1500]}", "source": "research-center",
                                                 "importance": 0.4, "confidence": 0.8, "tags": ["research-cache", key], "tier": "long"})
    except Exception:
        pass


async def ask_llm(task, prompt, system):
    """Calls the router. One backoff retry on 429; sets a short cooldown so repeated clicks stay quiet."""
    global RL_UNTIL
    if time.time() < RL_UNTIL:
        return {"rate_limited": True, "retry_after": int(RL_UNTIL - time.time()) + 1}
    for attempt in (0, 1):
        try:
            async with httpx.AsyncClient(timeout=90) as c:
                r = await c.post(f"{LLM_ROUTER}/route", json={"task": task, "prompt": prompt, "context": system})
            if r.status_code == 429 or "429" in r.text[:200] and r.status_code >= 400:
                if attempt == 0:
                    await asyncio.sleep(3)
                    continue
                RL_UNTIL = time.time() + 30
                return {"rate_limited": True, "retry_after": 30}
            r.raise_for_status()
            return {"text": r.json()["result"], "model": r.json().get("model")}
        except httpx.HTTPError as e:
            return {"error": f"model unavailable ({type(e).__name__})"}
    return {"error": "model unavailable"}


@app.post("/ai/explain")
async def ai_explain(req: ExplainReq):
    p = await mem("GET", f"/research/projects/{req.project_id}")
    g = p["graph"]; nodes = {n["id"]: n for n in g["nodes"]}
    lv = (LIVE_CACHE.get(p["id"]) or (0, {}))[1]
    if req.kind == "edge":
        e = next((x for x in g["edges"] if x["id"] == req.target), None)
        if not e: raise HTTPException(404, "edge not found")
        a, b = nodes.get(e["from"], {}), nodes.get(e["to"], {})
        key = hashlib.sha1(f"{p['id']}|edge|{e['id']}".encode()).hexdigest()[:20]
        ctx = (f"Link kind: {e['kind']}; label: {e['label']}\nFrom: {a.get('name')} ({a.get('path')}) - {a.get('purpose')}\n"
               f"To: {b.get('name')} ({b.get('path')}) - {b.get('purpose')}\n"
               f"Live state: {(lv.get('edges', {}).get(e['id']) or {}).get('state')}: {(lv.get('edges', {}).get(e['id']) or {}).get('reason')}")
        if p.get("root") and a.get("path") and a.get("kind") == "file":
            ctx += "\n\nSource of the caller:\n" + _snippet(p["root"], a["path"], 2500)
        ask = "Explain in 4-6 short sentences what flows over this link, why it exists, and what breaks if it fails."
    else:
        n = nodes.get(req.target)
        if not n: raise HTTPException(404, "node not found")
        key = hashlib.sha1(f"{p['id']}|node|{n['id']}|{n.get('mtime')}|{n.get('size')}".encode()).hexdigest()[:20]
        ctx = (f"{n['kind']}: {n['name']} ({n.get('path')}), {n.get('lines')} lines, layer {n['layer']}\nPurpose hint: {n.get('purpose')}\n"
               f"Routes: {n.get('routes')}\nImports: {n.get('imports')}\nExports: {n.get('exports')}")
        if p.get("root") and n["kind"] == "file":
            ctx += "\n\nSource (start of file):\n" + _snippet(p["root"], n["path"])
        ask = "Explain in 5-7 short sentences what this module does, what it depends on and who uses it. Plain language."
    cached = await cache_get(p, key)
    if cached:
        return {"text": cached, "cached": True}
    out = await ask_llm("fast", f"{ask}\n\n{ctx}", "You are a senior engineer documenting a project for its owner. Be concrete, no fluff.")
    if "text" in out:
        await cache_put(p, key, out["text"])
        out["cached"] = False
    return out


@app.post("/ai/missing")
async def ai_missing(req: MissingReq):
    p = await mem("GET", f"/research/projects/{req.project_id}")
    g = p["graph"]
    lv = await compute_live(p)
    names = [f"{n['name']} [{n['kind']}/{n['layer']}]" for n in g["nodes"] if n["kind"] != "file"][:40]
    undone = [b["text"] for b in lv["completion"]["blockers"]][:10]
    goal = p.get("idea") or f"{p['name']} ({p.get('type')})"
    key = hashlib.sha1(f"{p['id']}|missing|{len(g['nodes'])}|{len(g['edges'])}|{lv['completion']['pct']}".encode()).hexdigest()[:20]
    cached = await cache_get(p, key)
    if cached:
        return {"text": cached, "cached": True}
    prompt = (f"Project goal: {goal}\nCurrent modules: {', '.join(names)}\nCompletion: {lv['completion']['pct']}%\n"
              f"Current blockers: {undone}\n\nWhat is missing to reach the goal? Give at most 6 concrete next tasks, most important first, "
              "as a numbered list, each one line.")
    out = await ask_llm("fast", prompt, "You are a pragmatic tech lead. Compare the project graph with its goal and propose next tasks.")
    if "text" in out:
        await cache_put(p, key, out["text"])
        out["cached"] = False
    return out


# ═════════════════════════ test cycle (Test button): layered, real, self-cleaning ═════════════════════════
RUNS = {}


class TestReq(BaseModel):
    ai: bool = True               # include tests that use external services (LLMs, web, git)
    destructive: bool = False
    approved: bool = False        # uploaded projects: the owner must approve the generated plan first
    mutating: bool = False        # uploaded projects: also call POST/PUT/PATCH/DELETE routes (may change the project's data)


def _public_run(run):
    out = {k: v for k, v in run.items() if k != "layers"}
    out["layers"] = [{**{k: v for k, v in l.items() if k != "tests"},
                      "tests": [{k: v for k, v in t.items() if k != "fn"} for t in l["tests"]]} for l in run["layers"]]
    out["summary"] = tester.summarize(out["layers"])
    return out


def _is_aim(p):
    return bool(p.get("root")) and os.path.exists(os.path.join(p["root"], "agents", "research", "suites_aim.py"))


async def _links_layer(p, since):
    LIVE_CACHE.pop(p["id"], None)
    lv = await compute_live(p)
    nodes = {n["id"]: n for n in p["graph"]["nodes"]}
    tests, proven, total = [], 0, 0
    succ = {}
    for tr in TRACES:
        if tr["ts"] >= since and tr["status"] < 500:
            succ[tr["dst"]] = succ.get(tr["dst"], 0) + 1
    shown = lambda n: (n.get("path") if n.get("kind") == "file" and n.get("path") else n.get("name")) or n.get("id")
    for e in p["graph"]["edges"]:
        if e["kind"] not in ("http", "redis", "db"):
            continue
        est = lv["edges"].get(e["id"]) or {"state": "unknown", "reason": ""}
        total += 1
        if e["kind"] == "http":
            traffic = len(_edge_traffic(e, nodes, window=time.time() - since + 5))
        elif e["kind"] == "redis":
            traffic = len([t for t in (BUS.get(e.get("channel"), {}).get("recent") or []) if t >= since])
        else:
            traffic = 0
        inferred = False
        to_node = nodes.get(e["to"], {})
        if traffic == 0 and (e["kind"] == "db" or to_node.get("kind") in ("external", "infra", "db") or e["to"].startswith(("ext:", "local:"))):
            traffic, inferred = succ.get(e.get("fromSvc") or "", 0), True      # database / outbound API calls are not visible on the wire: infer from the caller serving requests
        name = f"{shown(nodes.get(e['from'], {}))}  ->  {shown(to_node)}   [{e['label']}]"
        t = {"id": f"link:{e['id']}", "layer": "links", "name": name, "ms": 0, "expect": None, "actual": None, "flaky": False}
        st = est["state"]
        gap = e.get("dangling") and nodes.get(e["from"], {}).get("dbkind") == "redis"
        if st == "fail" and gap:
            t["status"], t["detail"] = "warn", "KNOWN GAP: " + est["reason"]
        elif st == "fail":
            t["status"], t["detail"] = "fail", est["reason"]
        elif st == "slow":
            t["status"], t["detail"] = "warn", est["reason"]
        elif st in ("unknown", "planned"):
            t["status"], t["detail"] = "skip", est["reason"] or "no live signal for this link"
        elif traffic > 0:
            t["status"] = "pass"
            t["detail"] = (f"inferred: {e.get('fromSvc')} served {traffic} successful request(s); this link itself cannot be observed directly" if inferred
                           else f"{traffic} real call(s) during this run, no errors")
            proven += 1
        else:
            t["status"], t["detail"] = "warn", "healthy, but no traffic crossed it during this run (path not exercised by any test)"
        tests.append(t)
    return tests, {"proven": proven, "total": total, "pct": round(100 * proven / total) if total else 0}


async def _run_suite(p, run, req):
    pid = p["id"]
    ctx = tester.Ctx(p["root"], ai=req.ai, destructive=req.destructive)
    try:
        aim = _is_aim(p)
        if aim:
            groups = {"static": suites_aim.static_tests(ctx), "smoke": suites_aim.smoke_tests(ctx), "contract": await suites_aim.contract_tests(ctx),
                      "data": suites_aim.data_tests(ctx), "bus": suites_aim.bus_tests(ctx), "flows": suites_aim.flow_tests(ctx),
                      "security": suites_aim.security_tests(ctx), "perf": suites_aim.perf_tests(ctx)}
        else:
            groups = await suites_generic.build(ctx, p["graph"], req.mutating)
        ui = tester.T("ui", "browser harness (clicks every button on the dashboard pages)", None)
        ui["status"], ui["detail"] = "skip", "runs in your browser: press 'Run UI tests' on this page"
        rs = tester.T("resilience", "stop-a-service drills", None)
        rs["status"], rs["detail"] = "skip", "needs the Docker CLI on your computer: python aim.py test --chaos"
        placeholders = {"ui": [ui], "resilience": [rs]}
        layers = []
        for lid, name, desc in tester.LAYERS:
            ts = groups.get(lid) or (placeholders.get(lid, []) if aim else [])
            if ts:
                layers.append({"id": lid, "name": name, "desc": desc, "tests": ts})
        run["layers"] = layers
        flat = [t for l in layers for t in l["tests"] if l["id"] not in ("ui", "links", "resilience")]
        run["total"] = len(flat)

        def progress(t, phase):
            if phase == "start":
                run["current"] = t["name"]
            else:
                run["done"] += 1
        await tester.run_tests(flat, ctx, progress)
        run["current"] = "link proof"
        link_cov = None
        if aim:
            try:
                p = await mem("GET", f"/research/projects/{pid}")      # the scanner flow refreshed the graph during the run
            except HTTPException:
                pass
            link_tests, link_cov = await _links_layer(p, ctx.t0)
            next(l for l in layers if l["id"] == "links")["tests"] = link_tests
        run["coverage"] = {"routes": {"tested": len(ctx.shared.get("routes_tested", ())), "total": len(ctx.shared.get("routes", ())),
                                      "pct": round(100 * len(ctx.shared.get("routes_tested", ())) / max(1, len(ctx.shared.get("routes", ()))))},
                           **({"links": link_cov} if link_cov else {})}
        run["status"] = "done"
    except Exception as e:
        logger.exception("test run crashed")
        run["status"], run["error"] = "error", f"{type(e).__name__}: {e}"
    run["finished"] = time.time()
    run["current"] = ""
    await _persist_run(p, _public_run(run) if run.get("layers") else run)


async def _persist_run(p, pub):
    try:
        cur = await mem("GET", f"/research/projects/{p['id']}")
        ov = cur.get("overrides") or {}
        ov["last_test"] = pub
        hist = ov.get("test_history") or []
        sm = pub.get("summary") or {}
        hist.append({"id": pub["id"], "t": pub.get("finished") or time.time(), "pass": sm.get("pass", 0), "fail": sm.get("fail", 0), "warn": sm.get("warn", 0),
                     "skip": sm.get("skip", 0), "pct": sm.get("pct", 0), "verdict": sm.get("verdict")})
        ov["test_history"] = hist[-20:]
        await mem("PUT", f"/research/projects/{p['id']}", json={**{k: cur[k] for k in ("name", "kind", "type", "idea", "root", "graph")}, "overrides": ov})
    except Exception as e:
        logger.warning("could not persist test run: %s", type(e).__name__)


@app.post("/projects/{pid}/test")
async def start_test(pid: str, req: TestReq):
    p = await mem("GET", f"/research/projects/{pid}")
    if not _is_aim(p):
        if p["kind"] != "scanned" or not p.get("root"):
            raise HTTPException(400, "Only opened or cloned folders can be tested")
        if not req.approved:
            raise HTTPException(400, "Approve the generated test plan first (POST /projects/{id}/test/plan shows it)")
    if pid in RUNS and RUNS[pid].get("status") == "running":
        return {"run_id": RUNS[pid]["id"], "already_running": True}
    run = {"id": uuid_hex(), "project_id": pid, "status": "running", "started": time.time(), "finished": None, "done": 0, "total": 0,
           "current": "preparing", "layers": [], "options": req.model_dump()}
    RUNS[pid] = run
    asyncio.create_task(_run_suite(p, run, req))
    return {"run_id": run["id"]}


def uuid_hex():
    import uuid
    return uuid.uuid4().hex[:10]


@app.post("/projects/{pid}/test/plan")
async def test_plan(pid: str, mutating: bool = False):
    """Shows exactly what the generated tests would do, without running any of them. The owner approves this before a run."""
    p = await mem("GET", f"/research/projects/{pid}")
    if _is_aim(p):
        return {"aim": True, "note": "AIM OS has its own full suite; no approval step needed."}
    if p["kind"] != "scanned" or not p.get("root"):
        raise HTTPException(400, "Only opened or cloned folders can be tested")
    ctx = tester.Ctx(p["root"])
    groups = await suites_generic.build(ctx, p["graph"], mutating)
    layers = []
    for lid, name, desc in tester.LAYERS:
        ts = groups.get(lid) or []
        if ts:
            layers.append({"id": lid, "name": name, "desc": desc, "tests": [{"name": t["name"], "kind": t.get("kind", "")} for t in ts]})
    kinds = {}
    for l in layers:
        for t in l["tests"]:
            kinds[t["kind"]] = kinds.get(t["kind"], 0) + 1
    svcs = [x["name"] for x in suites_generic.services(p["graph"])]
    return {"aim": False, "layers": layers, "kinds": kinds, "total": sum(kinds.values()), "services": svcs,
            "notes": ["Nothing in the project is ever executed; static checks only read its files.",
                      "Live checks only call services that are already running on a published port: " + (", ".join(svcs) if svcs else "none found in the scan (static checks only)"),
                      "Default calls are read-only GETs." + (" Mutating calls are INCLUDED: they may change the project's data." if mutating else " Calls that could change data are excluded unless you opt in.")]}


@app.get("/projects/{pid}/test")
async def get_test(pid: str):
    run = RUNS.get(pid)
    if run and run.get("layers"):
        out = _public_run(run)
        out["history"] = (await _hist(pid)) if run["status"] != "running" else []
        return out
    if run:
        return {k: v for k, v in run.items() if k != "layers"}
    p = await mem("GET", f"/research/projects/{pid}")
    ov = p.get("overrides") or {}
    if ov.get("last_test"):
        return {**ov["last_test"], "history": ov.get("test_history", []), "from_storage": True}
    return {"status": "none", "history": []}


async def _hist(pid):
    try:
        return ((await mem("GET", f"/research/projects/{pid}")).get("overrides") or {}).get("test_history", [])
    except Exception:
        return []


class UiReport(BaseModel):
    tests: list
    coverage: dict = {}


@app.post("/projects/{pid}/test/layer/{lid}")
async def layer_report(pid: str, lid: str, rep: UiReport):
    """A client (the browser harness for 'ui', the host script for 'resilience') posts what it did; that layer of the stored run is replaced."""
    if lid not in ("ui", "resilience"):
        raise HTTPException(400, "only the ui and resilience layers are reported by clients")
    p = await mem("GET", f"/research/projects/{pid}")
    ov = p.get("overrides") or {}
    last = ov.get("last_test")
    if not last or not last.get("layers"):
        raise HTTPException(400, "Run the server-side tests first")
    clean = []
    for i, t in enumerate(rep.tests[:600]):
        st = t.get("status") if t.get("status") in ("pass", "fail", "warn", "skip") else "warn"
        clean.append({"id": f"{lid}:{i}", "layer": lid, "name": str(t.get("name", ""))[:140], "status": st, "ms": int(t.get("ms") or 0),
                      "detail": str(t.get("detail", ""))[:300], "expect": t.get("expect"), "actual": t.get("actual"), "flaky": False})
    for l in last["layers"]:
        if l["id"] == lid:
            l["tests"] = clean
    last["summary"] = tester.summarize(last["layers"])
    last["coverage"] = {**(last.get("coverage") or {}), lid: rep.coverage}
    if lid == "ui":
        try:
            lt, lcov = await _links_layer(p, last.get("started") or (time.time() - 3600))
            for l in last["layers"]:
                if l["id"] == "links":
                    l["tests"] = lt
            last["coverage"]["links"] = lcov
            last["summary"] = tester.summarize(last["layers"])
        except Exception as e:
            logger.warning("link proof refresh failed: %s", type(e).__name__)
    ov["last_test"] = last
    hist = ov.get("test_history") or []
    if hist:
        sm = last["summary"]
        hist[-1].update({"pass": sm["pass"], "fail": sm["fail"], "warn": sm["warn"], "skip": sm["skip"], "pct": sm["pct"], "verdict": sm["verdict"]})
    ov["test_history"] = hist
    await mem("PUT", f"/research/projects/{pid}", json={**{k: p[k] for k in ("name", "kind", "type", "idea", "root", "graph")}, "overrides": ov})
    if pid in RUNS and RUNS[pid].get("layers"):
        for l in RUNS[pid]["layers"]:
            if l["id"] == lid:
                l["tests"] = clean
        RUNS[pid].setdefault("coverage", {})[lid] = rep.coverage
    return {"ok": True, "summary": last["summary"]}


# ═════════════════════════ project editing + dashboard summary ═════════════════════════
AUTO_TASKS = {"services_up", "tests_pass", "tavily_key"}


class TasksReq(BaseModel):
    tasks: list


@app.put("/projects/{pid}/tasks")
async def put_tasks(pid: str, req: TasksReq):
    p = await mem("GET", f"/research/projects/{pid}")
    clean, seen = [], set()
    for i, t in enumerate(req.tasks[:60]):
        if not isinstance(t, dict) or not str(t.get("label", "")).strip():
            continue
        tid = re.sub(r"[^\w-]", "", str(t.get("id") or f"t{i + 1}"))[:30] or f"t{i + 1}"
        while tid in seen:
            tid += "x"
        seen.add(tid)
        item = {"id": tid, "label": str(t["label"]).strip()[:140]}
        if t.get("auto") in AUTO_TASKS:
            item["auto"] = t["auto"]
        clean.append(item)
    g = p["graph"]; g["tasks"] = clean
    ov = p.get("overrides") or {}
    ov["items"] = {k: v for k, v in (ov.get("items") or {}).items() if not k.startswith("task:") or k[5:] in seen}
    LIVE_CACHE.pop(pid, None)
    await mem("PUT", f"/research/projects/{pid}", json={**{k: p[k] for k in ("name", "kind", "type", "idea", "root")}, "graph": g, "overrides": ov})
    return {"ok": True, "tasks": clean}


class ProjectPatch(BaseModel):
    name: Optional[str] = None
    idea: Optional[str] = None
    motive: Optional[str] = None


@app.patch("/projects/{pid}")
async def patch_project(pid: str, req: ProjectPatch):
    p = await mem("GET", f"/research/projects/{pid}")
    g = p["graph"]
    if req.name is not None and req.name.strip():
        p["name"] = req.name.strip()[:80]
    if req.idea is not None:
        p["idea"] = req.idea[:2000]
    if req.motive is not None:
        g["motive"] = req.motive[:500] or None
    LIVE_CACHE.pop(pid, None)
    return await mem("PUT", f"/research/projects/{pid}", json={**{k: p[k] for k in ("name", "kind", "type", "idea", "root")}, "graph": g, "overrides": p.get("overrides") or {}})


@app.get("/dashboard/summary")
async def dashboard_summary():
    """One call for the main dashboard: every project with live completion, service health, link health, tasks and last test."""
    out = []
    for pl in (await mem("GET", "/research/projects"))[:8]:
        try:
            p = await mem("GET", f"/research/projects/{pl['id']}")
            lv = await compute_live(p)
        except HTTPException:
            continue
        nodes = p["graph"].get("nodes", [])
        svcs = [n for n in nodes if n["id"].startswith("svc:") and n["kind"] in ("service", "db", "infra") and not n.get("job")]
        ok = sum(1 for n in svcs if (lv["nodes"].get(n["id"]) or {}).get("state") in ("ok", "slow"))
        links = [e for e in p["graph"].get("edges", []) if e["kind"] in ("http", "redis", "db")]
        st = [(lv["edges"].get(e["id"]) or {}).get("state") for e in links]
        lt = (p.get("overrides") or {}).get("last_test") or {}
        sm = lt.get("summary") or {}
        out.append({"id": p["id"], "name": p["name"], "kind": p["kind"], "type": p.get("type"), "pct": lv["completion"]["pct"],
                    "services": {"ok": ok, "total": len(svcs)}, "links": {"total": len(links), "fail": st.count("fail"), "slow": st.count("slow")},
                    "tasks": lv["completion"]["tasks"], "blockers": lv["completion"]["blocker_count"],
                    "test": {"verdict": sm.get("verdict"), "pass": sm.get("pass"), "warn": sm.get("warn"), "fail": sm.get("fail"), "at": lt.get("finished")} if sm else None})
    return {"projects": out, "ts": time.time()}


# ═════════════════════════ file editor (workspace projects are editable; AIM OS itself is read-only here; secrets are never served) ═════════════════════════
DENY_DIRS = {".git", "data", "runtime", "node_modules", "__pycache__", "venv", ".venv", ".idea"}
SECRET_RX = re.compile(r"^(\.env(\..*)?|.*\.pem|id_rsa.*|id_ed25519.*|.*\.key|.*\.p12|credentials.*|\.npmrc|\.pypirc)$", re.I)
SAFE_ENV = {".env.example", ".env.sample", ".env.template"}
MAX_EDIT = 1_000_000


def _fs(p, rel):
    """Resolve rel inside the project root or raise. Returns (root, full, writable)."""
    if not p.get("root"):
        raise HTTPException(400, "This project has no folder")
    root = os.path.realpath(p["root"])
    rel = (rel or "").replace("\\", "/").strip("/")
    full = os.path.realpath(os.path.join(root, rel))
    if full != root and not full.startswith(root + os.sep):
        raise HTTPException(400, "Path is outside the project folder")
    parts = [x for x in rel.split("/") if x]
    if any(x in DENY_DIRS for x in parts):
        raise HTTPException(403, "This folder is hidden (git internals, data, caches)")
    if parts and SECRET_RX.match(parts[-1]) and parts[-1].lower() not in SAFE_ENV:
        raise HTTPException(403, "Secret files (.env, keys, certificates) are never opened here")
    ws = os.path.realpath(os.path.join(PROJECT_ROOTS, "workspace"))
    return root, full, (root == ws or root.startswith(ws + os.sep))


class FileReq(BaseModel):
    path: str
    content: str = ""


@app.get("/projects/{pid}/tree")
async def file_tree(pid: str, path: str = ""):
    p = await mem("GET", f"/research/projects/{pid}")
    root, full, writable = _fs(p, path)
    if not os.path.isdir(full):
        raise HTTPException(404, "Not a folder")
    items = []
    for name in sorted(os.listdir(full), key=lambda n: (not os.path.isdir(os.path.join(full, n)), n.lower())):
        if name in DENY_DIRS or (SECRET_RX.match(name) and name.lower() not in SAFE_ENV):
            continue
        fp = os.path.join(full, name)
        isd = os.path.isdir(fp)
        items.append({"name": name, "dir": isd, "size": 0 if isd else os.path.getsize(fp)})
    return {"path": path, "writable": writable, "items": items[:500]}


@app.get("/projects/{pid}/file")
async def read_file(pid: str, path: str):
    p = await mem("GET", f"/research/projects/{pid}")
    root, full, writable = _fs(p, path)
    if not os.path.isfile(full):
        raise HTTPException(404, "File not found")
    size = os.path.getsize(full)
    if size > MAX_EDIT:
        raise HTTPException(413, f"File is {size // 1024} KB - the editor opens files up to 1000 KB")
    raw = open(full, "rb").read()
    if b"\0" in raw[:8192]:
        raise HTTPException(415, "Binary file - not shown")
    return {"path": path, "content": raw.decode("utf-8", errors="replace"), "size": size, "writable": writable}


@app.put("/projects/{pid}/file")
async def write_file(pid: str, req: FileReq):
    p = await mem("GET", f"/research/projects/{pid}")
    root, full, writable = _fs(p, req.path)
    if not writable:
        raise HTTPException(403, "Read-only: only projects inside the workspace folder can be edited here")
    if len(req.content.encode("utf-8")) > MAX_EDIT:
        raise HTTPException(413, "File too large for the editor")
    if not os.path.isfile(full):
        raise HTTPException(404, "File not found (use New file to create one)")
    tmp = full + ".aimtmp"
    with open(tmp, "w", encoding="utf-8", newline="") as f:
        f.write(req.content)
    os.replace(tmp, full)
    return {"ok": True, "size": os.path.getsize(full)}


@app.post("/projects/{pid}/file/new")
async def new_file(pid: str, req: FileReq):
    p = await mem("GET", f"/research/projects/{pid}")
    root, full, writable = _fs(p, req.path)
    if not writable:
        raise HTTPException(403, "Read-only: only projects inside the workspace folder can be edited here")
    if os.path.exists(full):
        raise HTTPException(409, "That file already exists")
    os.makedirs(os.path.dirname(full), exist_ok=True)
    with open(full, "w", encoding="utf-8", newline="") as f:
        f.write(req.content or "")
    return {"ok": True}


@app.delete("/projects/{pid}/file")
async def delete_file(pid: str, path: str):
    p = await mem("GET", f"/research/projects/{pid}")
    root, full, writable = _fs(p, path)
    if not writable:
        raise HTTPException(403, "Read-only: only projects inside the workspace folder can be edited here")
    if not os.path.isfile(full):
        raise HTTPException(404, "File not found")
    os.remove(full)
    return {"ok": True}


# ═════════════════════════ understand a repo, then plan how to complete it (plan first, build only on approval) ═════════════════════════
def _read_text(path, n):
    try:
        with open(path, encoding="utf-8", errors="ignore") as f:
            return f.read(n)
    except Exception:
        return ""


@app.post("/projects/{pid}/understand")
async def understand(pid: str):
    p = await mem("GET", f"/research/projects/{pid}")
    if not p.get("root"):
        raise HTTPException(400, "Only opened or cloned folders can be analysed")
    root, g = p["root"], p["graph"]
    readme = ""
    for f in sorted(os.listdir(root)):
        if f.lower().startswith("readme"):
            readme = _read_text(os.path.join(root, f), 5000)
            break
    manifests = "\n".join(f"--- {f} ---\n{_read_text(os.path.join(root, f), 1400)}" for f in
                          ("package.json", "requirements.txt", "pyproject.toml", "docker-compose.yml", "go.mod", "Cargo.toml") if os.path.exists(os.path.join(root, f)))
    files = [n["path"] for n in g["nodes"] if n["kind"] == "file"][:90]
    svcs = [f"{n['name']}({len(n.get('routes', []))} routes)" for n in g["nodes"] if n["kind"] == "service"][:20]
    nl = chr(10)
    prompt = nl.join([
        "You are a senior engineer taking over an unfamiliar project for its owner. Do NOT write code. Be honest and concrete.",
        f"Project: {p['name']}   stack: {g.get('stack')}   type: {g.get('type')}   scanned files: {g.get('files_scanned')}",
        f"Services: {svcs}", f"Has tests: {bool(g.get('tests'))}   Has README: {g.get('has_readme')}",
        "", "README (start):", readme or "(none)", "", "Manifests:", manifests or "(none)", "", "Files:", ", ".join(files), "",
        "Respond ONLY with one JSON object:",
        '{"what": "3-4 sentences: what this project is and who it is for",',
        ' "how": "3-5 sentences: how it works (main parts and how they talk)",',
        ' "state": "2-3 sentences: how complete it looks, what already works",',
        ' "gaps": ["up to 6 concrete things missing or broken"],',
        ' "plan": [ up to 8 objects in the order to do them: {"task":"short imperative","why":"one sentence","effort":"hours/days"} ]}',
    ])
    out = await ask_llm("fast", prompt, "You are a pragmatic tech lead. Answer only with the requested JSON.")
    if out.get("rate_limited"):
        return {"rate_limited": True, "retry_after": out.get("retry_after", 30)}
    if "text" not in out:
        return {"error": out.get("error", "model unavailable")}
    try:
        d = _extract_json(out["text"])
        plan = [x for x in (d.get("plan") or []) if isinstance(x, dict) and x.get("task")][:8]
        u = {"what": d.get("what", ""), "how": d.get("how", ""), "state": d.get("state", ""),
             "gaps": [str(x) for x in (d.get("gaps") or [])][:6], "plan": plan, "model": out.get("model"), "at": int(time.time())}
    except Exception:
        return {"error": "The model's answer could not be read. Try again."}
    g["understanding"] = u
    g["tasks"] = [t for t in g.get("tasks", []) if not str(t["id"]).startswith("u")] + [{"id": f"u{i + 1}", "label": x["task"]} for i, x in enumerate(plan)]
    LIVE_CACHE.pop(pid, None)
    return await mem("PUT", f"/research/projects/{pid}", json={**{k: p[k] for k in ("name", "kind", "type", "idea", "root", "overrides")}, "graph": g})


# ═════════════════════════ think with me: find the motive, encourage, clarify (no building) ═════════════════════════
class ThinkReq(BaseModel):
    name: str = ""
    type: str = "webapp"
    messages: list = []


@app.post("/ai/think")
async def ai_think(req: ThinkReq):
    nl = chr(10)
    convo = nl.join(f"{'OWNER' if m.get('role') == 'user' else 'YOU'}: {str(m.get('content', ''))[:1200]}" for m in req.messages[-14:])
    system = ("You are the owner's thinking partner, in the spirit of JARVIS with Tony Stark: warm, sharp, on their side. "
              "Your job BEFORE anything is built: help them find their real motive (why this matters to them), who it is for, "
              "what 'done' looks like, and the smallest first win. Never write code or a build plan yet. "
              "Each reply: first reflect what you heard and give honest, specific encouragement (1-2 sentences, no flattery clichés), "
              "then ask at most 2 sharp questions. Short, human, direct.")
    prompt = nl.join([
        f"Project name: {req.name or '(not named yet)'}   type: {req.type}", "", "Conversation so far:", convo, "",
        "Respond ONLY with JSON: {\"reply\": \"your message to the owner\", \"motive\": \"their motive in one sentence, or null if unclear yet\", "
        "\"idea\": \"one-paragraph summary of what they want to build, in their words\", "
        "\"ready\": true when motive, audience and a clear first win are known (usually after 3+ owner messages), else false}"])
    out = await ask_llm("fast", prompt, system)
    if out.get("rate_limited"):
        return {"rate_limited": True, "retry_after": out.get("retry_after", 30)}
    if "text" not in out:
        return {"error": out.get("error", "model unavailable")}
    try:
        d = _extract_json(out["text"])
        return {"reply": str(d.get("reply", "")).strip() or "Tell me more.", "motive": d.get("motive") or None,
                "idea": d.get("idea") or "", "ready": bool(d.get("ready"))}
    except Exception:
        return {"reply": out["text"].strip()[:900], "motive": None, "idea": "", "ready": False}


# ═════════════════════════ live web search: Tavily (free tier) -> free LLM writes the answer; Gemini search is the fallback ═════════════════════════
TAVILY_KEY = os.getenv("TAVILY_API_KEY")
TAVILY_URL = os.getenv("TAVILY_API_URL", "https://api.tavily.com/search")
TAV_UNTIL = 0.0
SEARCH_CACHE = {}          # query -> (ts, results): repeats cost no credits


class SearchUnavailable(Exception):
    pass


async def web_search(query: str, n: int = 6, depth: str = "basic"):
    """Tavily search -> [{title, url, content}]. Raises SearchUnavailable when there is no key, the plan limit is hit or the key is rejected."""
    global TAV_UNTIL
    if not TAVILY_KEY:
        raise SearchUnavailable("TAVILY_API_KEY is not set")
    if time.time() < TAV_UNTIL:
        raise SearchUnavailable("Tavily paused after a limit or auth error")
    q = " ".join(query.split())[:380]
    hit = SEARCH_CACHE.get((q, n, depth))
    if hit and time.time() - hit[0] < 3600:
        return hit[1]
    try:
        async with httpx.AsyncClient(timeout=30) as c:
            r = await c.post(TAVILY_URL, headers={"Authorization": f"Bearer {TAVILY_KEY}"},
                             json={"query": q, "search_depth": depth, "max_results": n, "include_answer": False})
    except httpx.HTTPError as e:
        raise SearchUnavailable(f"Tavily unreachable ({type(e).__name__})")
    if r.status_code in (429, 432, 433):
        TAV_UNTIL = time.time() + 900
        raise SearchUnavailable("Tavily limit reached (monthly credits or rate limit)")
    if r.status_code in (401, 403):
        TAV_UNTIL = time.time() + 3600
        raise SearchUnavailable("Tavily rejected the API key")
    r.raise_for_status()
    res = [{"title": x.get("title", ""), "url": x.get("url", ""), "content": (x.get("content") or "")[:700]} for x in r.json().get("results", [])][:n]
    SEARCH_CACHE[(q, n, depth)] = (time.time(), res)
    return res


async def search_grounded(prompt: str, queries):
    """Answer `prompt` using live web results. -> (text, source_urls, how) with how in {'tavily','gemini'}.
    Order: Tavily results + a free LLM (Groq) writing the answer; then Gemini's built-in search. Raises when neither is available."""
    if TAVILY_KEY and time.time() >= TAV_UNTIL:
        try:
            batches = await asyncio.gather(*[web_search(q) for q in queries[:3]])
            seen, results = set(), []
            for b in batches:
                for x in b:
                    if x["url"] not in seen:
                        seen.add(x["url"]); results.append(x)
            results = results[:10]
            if results:
                nl = chr(10)
                ctx = (nl + nl).join(f"[{i + 1}] {x['title']} ({x['url']}){nl}{x['content']}" for i, x in enumerate(results))
                full = prompt + nl + nl + "LIVE WEB SEARCH RESULTS (prefer these facts over memory; do not invent products or numbers that are not supported here):" + nl + ctx
                out = await ask_llm("fast", full, "You are a careful research analyst. Ground every claim in the search results. Answer only in the requested format.")
                if "text" in out:
                    return out["text"], [x["url"] for x in results], "tavily"
        except SearchUnavailable as e:
            logger.info("web search skipped: %s", e)
        except httpx.HTTPError:
            pass
    text, sources = await gemini_grounded(prompt)        # raises GroundingUnavailable when its quota is used up too
    return text, sources, "gemini"


@app.get("/search/status")
async def search_status():
    return {"tavily": bool(TAVILY_KEY), "paused": time.time() < TAV_UNTIL, "gemini_search_paused": time.time() < GROUND_UNTIL}


class SearchReq(BaseModel):
    query: str
    n: int = 6


@app.post("/search")
async def search_endpoint(req: SearchReq):
    try:
        return {"results": await web_search(req.query, min(max(req.n, 1), 10))}
    except SearchUnavailable as e:
        raise HTTPException(503, str(e))


# ═════════════════════════ research comparison: "what are we building and how" (before anything is built) ═════════════════════════
class RateLimited(Exception):
    pass


class GroundingUnavailable(Exception):
    """Google Search grounding has its own (small) free quota; when it is used up we answer without live search."""


GROUND_UNTIL = 0.0


COMPARE_CACHE = {}


SCORE_KEYS = ("speed", "low_effort", "low_cost", "simplicity", "scalability", "low_risk", "fit")


def _clean_scores(sc):
    if not isinstance(sc, dict):
        return None
    out = {}
    for k in SCORE_KEYS:
        try:
            out[k] = max(1, min(10, int(round(float(sc.get(k, 5))))))
        except (TypeError, ValueError):
            out[k] = 5
    return out


def _extract_json(text: str) -> dict:
    a, b = text.find("{"), text.rfind("}")
    if a < 0 or b <= a:
        raise ValueError("no JSON object in answer")
    return json.loads(text[a:b + 1])


async def gemini_grounded(prompt: str):
    """Gemini with Google Search grounding. Quota/429 -> GroundingUnavailable (caller falls back to the plain model)."""
    global GROUND_UNTIL
    if not GOOGLE_KEY:
        raise GroundingUnavailable()
    if time.time() < GROUND_UNTIL:
        raise GroundingUnavailable()
    model = os.getenv("GEMINI_RESEARCH_MODEL", "gemini-3.5-flash")
    async with httpx.AsyncClient(timeout=120) as c:
        r = await c.post(f"https://generativelanguage.googleapis.com/v1beta/models/{model}:generateContent",
                         headers={"x-goog-api-key": GOOGLE_KEY},
                         json={"contents": [{"parts": [{"text": prompt}]}], "tools": [{"google_search": {}}]})
    if r.status_code == 429:
        GROUND_UNTIL = time.time() + (900 if "quota" in r.text.lower() else 60)
        raise GroundingUnavailable()
    r.raise_for_status()
    d = r.json()["candidates"][0]
    text = "".join(p.get("text", "") for p in d["content"]["parts"])
    srcs = [c2["web"]["uri"] for c2 in d.get("groundingMetadata", {}).get("groundingChunks", []) if c2.get("web", {}).get("uri")]
    return text, list(dict.fromkeys(srcs))[:8]


@app.post("/ai/compare")
async def ai_compare(req: NewReq):
    """Briefing before building: what we are building, 3 researched ways to build it, a recommendation, risks, open questions."""
    key = hashlib.sha1(f"{req.name}|{req.type}|{req.idea}".lower().encode()).hexdigest()[:20]
    if key in COMPARE_CACHE:
        return {**COMPARE_CACHE[key], "cached": True}
    prompt = chr(10).join([
        "You are a senior technical advisor briefing the owner BEFORE anything is built. Do NOT write code.",
        f"Project name: {req.name}",
        f"Type: {req.type}",
        f"Idea: {req.idea or '(no description given)'}",
        "",
        "Research what is current and realistic today, then respond ONLY with one JSON object:",
        '{"building": "2-3 plain sentences: what exactly we are building and who it is for",',
        ' "goals": ["3-5 concrete outcomes"],',
        ' "approaches": [ exactly 3 objects: {"id":"a|b|c","name":"short name","how":"2-3 sentences on how it would be built",',
        '   "stack":["main tools/libraries"],"pros":["..."],"cons":["..."],"effort":"rough time","cost":"free/low/medium + why",',
        '   "best_for":"when to choose it",',
        '   "scores":{"speed":1-10,"low_effort":1-10,"low_cost":1-10,"simplicity":1-10,"scalability":1-10,"low_risk":1-10,"fit":1-10}} ],',
        ' (scores: whole numbers, 10 = most favourable on EVERY axis, e.g. low_effort 10 = very little effort, low_risk 10 = very safe; judge honestly and make the options differ)',
        ' "recommendation": {"id":"a|b|c","why":"2 sentences tied to this idea"},',
        ' "risks": ["3-4 things that could go wrong"], "questions": ["2-3 questions the owner must answer first"]}',
    ])
    sources, grounded, how = [], True, None
    idea_q = (req.idea or req.name)[:200]
    try:
        text, sources, how = await search_grounded(prompt, [f"best ways to build {idea_q} tech stack options", f"{idea_q} architecture comparison pros cons"])
    except Exception as e:
        grounded = False
        logger.info("compare without live search (%s)", type(e).__name__)
        out = await ask_llm("fast", prompt, "You are a senior technical advisor. Respond only with the requested JSON.")
        if out.get("rate_limited"):
            return out
        if "text" not in out:
            return {"error": out.get("error", "model unavailable")}
        text = out["text"]
    try:
        d = _extract_json(text)
        apps = d.get("approaches") or []
        if not apps:
            raise ValueError("no approaches")
        for i, a in enumerate(apps[:3]):
            a["id"] = "abc"[i]
            for k2 in ("stack", "pros", "cons"):
                a[k2] = [str(x) for x in (a.get(k2) or [])][:6]
            a["scores"] = _clean_scores(a.get("scores"))
        rec = d.get("recommendation") or {}
        if rec.get("id") not in [a["id"] for a in apps]:
            rec["id"] = apps[0]["id"]
        result = {"building": d.get("building", ""), "goals": d.get("goals", [])[:6], "approaches": apps[:3], "recommendation": rec,
                  "risks": d.get("risks", [])[:5], "questions": d.get("questions", [])[:4], "sources": sources, "grounded": grounded, "search": how}
    except Exception:
        return {"error": "The model's answer could not be read. Try again."}
    COMPARE_CACHE[key] = result
    return {**result, "cached": False}


# ═════════════════════════ novelty check: what already exists, where you could stand out ═════════════════════════
class NoveltyReq(BaseModel):
    name: str = ""
    type: str = ""
    idea: str = ""
    motive: Optional[str] = None
    project_id: Optional[str] = None


NOVELTY_CACHE = {}


def _clamp(v, lo, hi, default):
    try:
        return max(lo, min(hi, int(round(float(v)))))
    except (TypeError, ValueError):
        return default


@app.post("/ai/novelty")
async def ai_novelty(req: NoveltyReq):
    p, nl = None, chr(10)
    if req.project_id:
        p = await mem("GET", f"/research/projects/{req.project_id}")
        u = p["graph"].get("understanding") or {}
        subject = nl.join([f"Existing project: {p['name']}  stack: {p['graph'].get('stack')}", f"What it is: {u.get('what', '(not analysed yet)')}",
                           f"How it works: {u.get('how', '')}", f"Gaps: {u.get('gaps', [])}"])
    else:
        subject = nl.join([f"Idea: {req.name} ({req.type})", req.idea or "(no description)", f"Owner's motive: {req.motive or 'unknown'}"])
    key = hashlib.sha1(subject.lower().encode()).hexdigest()[:20]
    if key in NOVELTY_CACHE:
        return {**NOVELTY_CACHE[key], "cached": True}
    prompt = nl.join([
        "You are a product strategist doing a novelty check. Do NOT write code. Be honest, name only REAL products you are confident exist.",
        subject, "", "Respond ONLY with one JSON object:",
        '{"verdict": "crowded" | "niche" | "open", "summary": "2 sentences on how original this is",',
        ' "existing": [ up to 5 objects: {"name":"real product or project","what":"one line","similarity":0-100,"gap":"where it falls short for this owner"} ],',
        ' "differentiators": [ exactly 3 objects: {"idea":"a concrete way to stand out","why":"one sentence","novelty":1-10,"feasibility":1-10} ],',
        ' "unique_angle": "one sentence tying the best differentiator to the owner\'s motive"}'])
    sources, grounded, how = [], True, None
    subj_q = (req.idea or req.name or (p or {}).get("name", ""))[:200]
    try:
        text, sources, how = await search_grounded(prompt, [f"{subj_q} existing products alternatives competitors", f"apps similar to {subj_q}"])
    except Exception:
        grounded = False
        out = await ask_llm("fast", prompt, "You are a product strategist. Answer only with the requested JSON.")
        if out.get("rate_limited"):
            return out
        if "text" not in out:
            return {"error": out.get("error", "model unavailable")}
        text = out["text"]
    try:
        d = _extract_json(text)
        res = {"verdict": d.get("verdict") if d.get("verdict") in ("crowded", "niche", "open") else "niche", "summary": str(d.get("summary", "")),
               "existing": [{"name": str(x.get("name", ""))[:60], "what": str(x.get("what", ""))[:160], "similarity": _clamp(x.get("similarity"), 0, 100, 50),
                             "gap": str(x.get("gap", ""))[:200]} for x in (d.get("existing") or []) if isinstance(x, dict)][:5],
               "differentiators": [{"idea": str(x.get("idea", ""))[:200], "why": str(x.get("why", ""))[:200], "novelty": _clamp(x.get("novelty"), 1, 10, 5),
                                    "feasibility": _clamp(x.get("feasibility"), 1, 10, 5)} for x in (d.get("differentiators") or []) if isinstance(x, dict)][:4],
               "unique_angle": str(d.get("unique_angle", "")), "sources": sources, "grounded": grounded, "search": how}
    except Exception:
        return {"error": "The model's answer could not be read. Try again."}
    if how == "tavily" and not res["existing"]:     # the model sometimes forgets to list products even though the search results name them: ask once more
        try:
            t2, s2, _ = await search_grounded(prompt + nl + "Your previous answer listed no existing products, but the search results mention some. Name at least 3 real ones in `existing`.",
                                              [f"{subj_q} existing products alternatives competitors", f"apps similar to {subj_q}"])
            d2 = _extract_json(t2)
            ex = [{"name": str(x.get("name", ""))[:60], "what": str(x.get("what", ""))[:160], "similarity": _clamp(x.get("similarity"), 0, 100, 50), "gap": str(x.get("gap", ""))[:200]}
                  for x in (d2.get("existing") or []) if isinstance(x, dict) and x.get("name")][:5]
            if ex:
                res["existing"] = ex
                if d2.get("verdict") in ("crowded", "niche", "open"):
                    res["verdict"] = d2["verdict"]
        except Exception:
            pass
    NOVELTY_CACHE[key] = res
    if p:
        g = p["graph"]; g["novelty"] = res
        await mem("PUT", f"/research/projects/{p['id']}", json={**{k: p[k] for k in ("name", "kind", "type", "idea", "root", "overrides")}, "graph": g})
    return {**res, "cached": False}


# ═════════════════════════ web research (existing behaviour) ═════════════════════════
class ResearchRequest(BaseModel):
    query: str
    context: Optional[str] = None
    depth: str = "normal"
    project_id: Optional[str] = None


class ResearchResult(BaseModel):
    query: str
    summary: str
    key_points: List[str]
    sources: List[str]
    stored_memory_id: Optional[str]
    timestamp: str


def _parse_json_text(text: str) -> dict:
    text = text.strip()
    if "```" in text:
        text = text.split("```")[1]
        if text.startswith("json"):
            text = text[4:]
    return json.loads(text.strip())


async def gemini_search(query: str, context: Optional[str] = None) -> dict:
    nl = chr(10)
    prompt = nl.join([f"Research this topic thoroughly: {query}", f"Additional context: {context}" if context else "", "",
                      'Respond ONLY as JSON: {"summary": str, "key_points": [str], "sources": [str], "confidence": 0.0-1.0}'])
    try:
        text, srcs, how = await search_grounded(prompt, [query])
        result = _parse_json_text(text)
        result["sources"] = srcs or result.get("sources", [])
        result["search"] = how
        return result
    except Exception as e:
        logger.info("web research falling back to the router (%s)", type(e).__name__)
        return await fallback_research(query, context)


async def fallback_research(query: str, context: Optional[str]) -> dict:
    out = await ask_llm("fast", f"Research this topic: {query}\n\nContext: {context or 'none'}",
                        "You are a research assistant. Respond ONLY in JSON with summary, key_points array, and sources array.")
    if "text" in out:
        try:
            return _parse_json_text(out["text"])
        except Exception:
            return {"summary": out["text"][:2000], "key_points": [], "sources": [], "confidence": 0.4}
    return {"summary": f"Research on: {query}", "key_points": [], "sources": [], "confidence": 0.0,
            **({"rate_limited": True} if out.get("rate_limited") else {})}


async def store_research(query: str, result: dict, project_id: Optional[str]) -> Optional[str]:
    try:
        content = (f"Research: {query}\nSummary: {result.get('summary', '')}\n"
                   f"Key Points: {json.dumps(result.get('key_points', []))}\nSources: {json.dumps(result.get('sources', []))}")
        r = await mem("POST", "/memories", json={"content": content, "source": "research-agent", "importance": 0.8,
                                                 "confidence": result.get("confidence", 0.7), "tags": ["research", "auto"], "tier": "long"})
        return r.get("id")
    except Exception as e:
        logger.error("Store research error: %s", type(e).__name__)
        return None


async def already_researched(query: str) -> bool:
    try:
        res = (await mem("POST", "/memories/search", json={"query": query, "limit": 1})).get("results", [])
        return bool(res and res[0]["score"] > 0.92 and "research" in (res[0].get("tags") or []))
    except Exception:
        return False


async def listen_events():
    await asyncio.sleep(2)
    pubsub = redis_client.pubsub()
    await pubsub.subscribe("aim.research.request")
    logger.info("Research Agent listening...")
    async for message in pubsub.listen():
        if message["type"] != "message":
            continue
        try:
            data = json.loads(message["data"])
            if data.get("query"):
                await handle_research(data["query"], data.get("context"), data.get("project_id"))
        except Exception as e:
            logger.error("Research event error: %s", type(e).__name__)


async def handle_research(query, context, project_id):
    if await already_researched(query):
        return
    result = await gemini_search(query, context)
    memory_id = await store_research(query, result, project_id)
    await redis_client.publish("aim.research.result", json.dumps({
        "query": query, "summary": result.get("summary", ""), "key_points": result.get("key_points", []),
        "sources": result.get("sources", []), "memory_id": memory_id, "timestamp": datetime.utcnow().isoformat()}))
    await redis_client.publish("aim.notification.send", json.dumps({
        "title": "AIM Research", "message": f"Found info on: {query[:50]}", "confidence": 0.9, "source": "research-agent"}))


@app.post("/research", response_model=ResearchResult)
async def research(req: ResearchRequest):
    result = await gemini_search(req.query, req.context)
    memory_id = await store_research(req.query, result, None)
    return ResearchResult(query=req.query, summary=result.get("summary", ""), key_points=result.get("key_points", []),
                          sources=result.get("sources", []), stored_memory_id=memory_id, timestamp=datetime.utcnow().isoformat())


@app.get("/health")
async def health():
    return {"status": "ok", "service": "research-agent", "traces": len(TRACES), "channels": len(BUS)}
