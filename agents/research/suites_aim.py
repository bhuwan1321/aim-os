"""AIM OS test suites. Every test touches the real running system; all test data carries the marker `aimtest` and is removed afterwards."""
import asyncio
import base64
import io
import json
import os
import re
import statistics
import time
import uuid
import wave

import redis.asyncio as aioredis
import yaml

from tester import (T, check, Ctx, TestFail, TestSkip, TestWarn, build_request, has_required_body, operations, sample, _resolve)

HOSTS = {"llm-router": ("llm-router", 8001), "timeline": ("timeline", 8002), "memory": ("memory", 8003), "brain": ("brain", 8004),
         "research-agent": ("localhost", 8005), "notification": ("notification", 8006), "browser-agent": ("browser-agent", 8007),
         "voice-agent": ("voice-agent", 8008), "dashboard": ("dashboard", 8888)}
QDRANT, OLLAMA, BRIDGE = "http://qdrant:6333", "http://ollama:11434", "http://host.docker.internal:9999"
MARK = "aimtest"
SECRET_VARS = ("GOOGLE_API_KEY", "GROQ_API_KEY", "REDIS_PASSWORD", "POSTGRES_PASSWORD", "AIM_BRIDGE_TOKEN")
SKIP_DIRS = {"node_modules", ".git", "__pycache__", "venv", ".venv", "dist", "build", "data", "runtime", "target", "workspace", "logs"}
KEY_PATTERNS = [r"AIza[0-9A-Za-z_\-]{30,}", r"gsk_[0-9A-Za-z]{20,}", r"\bAQ\.[0-9A-Za-z_\-]{20,}", r"\bsk-[A-Za-z0-9]{20,}", r"ghp_[0-9A-Za-z]{30,}",
                r"-----BEGIN [A-Z ]*PRIVATE KEY-----"]
UNSAFE = {("memory", "POST", "/memories/decay"), ("notification", "DELETE", "/history")}
AI_ROUTES = {("llm-router", "/route"), ("research-agent", "/research"), ("research-agent", "/ai/compare"), ("research-agent", "/ai/think"),
             ("research-agent", "/ai/novelty"), ("voice-agent", "/converse/text")}
NO_CONTRACT = ("/stream", "/openapi", "/docs", "/redoc")


def U(svc, path=""):
    h, p = HOSTS[svc]
    return f"http://{h}:{p}{path}"


def files(root, exts):
    for d, dirs, fs in os.walk(root):
        dirs[:] = [x for x in dirs if x not in SKIP_DIRS and not x.startswith(".")]
        for f in fs:
            if os.path.splitext(f)[1].lower() in exts:
                yield os.path.join(d, f)


def rd(p):
    try:
        with open(p, encoding="utf-8", errors="ignore") as f:
            return f.read()
    except OSError:
        return ""


def rclient():
    return aioredis.Redis(host=os.getenv("REDIS_HOST", "redis"), port=int(os.getenv("REDIS_PORT", 6379)), password=os.getenv("REDIS_PASSWORD"), decode_responses=True)


async def pub(ch, payload):
    r = rclient()
    try:
        await r.publish(ch, json.dumps(payload))
    finally:
        await r.aclose()


async def until(fn, timeout, interval=0.5):
    end = time.time() + timeout
    while time.time() < end:
        v = await fn()
        if v:
            return v
        await asyncio.sleep(interval)
    return None


def pctl(vals, p):
    s = sorted(vals)
    return s[min(len(s) - 1, int(len(s) * p))]


def wav_info(b64):
    raw = base64.b64decode(b64)
    check(raw[:4] == b"RIFF" and raw[8:12] == b"WAVE", "audio is not a WAV file", "RIFF/WAVE header", raw[:12])
    with wave.open(io.BytesIO(raw)) as w:
        dur = w.getnframes() / float(w.getframerate())
        return dur, w.getframerate(), len(raw)


async def del_memory(ctx, mid):
    await ctx.call("DELETE", U("memory", f"/memories/{mid}"))


def track_memory(ctx, mid):
    ctx.cleanups.append(lambda: del_memory(ctx, mid))


CLOUD_ROUTES = {("research-agent", "/research"), ("research-agent", "/ai/compare"), ("research-agent", "/ai/novelty")}


async def cloud_ai(ctx):
    """True when the cloud research model (Gemini) answers. When its quota is used up the router silently falls back to a slow local model;
    that is an environment limit, not a code defect, so such tests warn instead of failing."""
    if "cloud_ai" not in ctx.shared:
        try:
            r = await ctx.call("POST", U("llm-router", "/route"), json={"task": "research", "prompt": "Reply with the single word OK"}, timeout=60)
            ctx.shared["cloud_ai"] = r.status == 200 and r.json().get("provider") == "google"
        except TestFail:
            ctx.shared["cloud_ai"] = False
    return ctx.shared["cloud_ai"]


NO_CLOUD = "cloud AI (Gemini) unavailable - quota or rate limit; the slow local fallback was answering. Not a code failure."


async def purge_test_rows(ctx):
    await ctx.call("DELETE", U("timeline", "/events/test"))
    await ctx.call("DELETE", U("notification", "/history/test"))
    r = await ctx.call("GET", U("memory", "/memories?limit=200"))
    if r.status == 200:
        for m in r.json():
            if MARK in (m.get("content") or "") or MARK in str(m.get("source", "")):
                await del_memory(ctx, m["id"])


# ═════════════════════════════════ STATIC ═════════════════════════════════
def static_tests(ctx):
    root, out = ctx.root, []
    for p in files(root, {".py"}):
        rel = os.path.relpath(p, root).replace("\\", "/")

        async def fn(ctx, p=p, rel=rel):
            try:
                compile(rd(p), p, "exec")
            except SyntaxError as e:
                raise TestFail(f"syntax error in {rel} line {e.lineno}: {e.msg}", "valid Python", f"line {e.lineno}")
        out.append(T("static", f"compiles: {rel}", fn))
    for p in files(root, {".html"}):
        rel = os.path.relpath(p, root).replace("\\", "/")

        async def fn(ctx, p=p, rel=rel):
            t = rd(p)
            check(len(re.findall(r"<script\b", t)) == len(re.findall(r"</script>", t)), "unbalanced <script> tags")
            check("<title>" in t and "</html>" in t and "<body" in t, "missing <title>, <body> or </html>")
            check(t.count("<div") == t.count("</div>"), f"unbalanced <div> tags ({t.count('<div')} open, {t.count('</div>')} close)")
        out.append(T("static", f"HTML structure: {rel}", fn))

    async def compose(ctx):
        d = yaml.safe_load(rd(os.path.join(root, "docker-compose.yml")))
        svcs = d.get("services", {})
        check(svcs, "no services in compose file")
        missing = [n for n, s in svcs.items() if "container_name" not in s]
        check(not missing, f"services without container_name: {missing}")
        nohc = [n for n, s in svcs.items() if s.get("build") and "healthcheck" not in s and n not in ("dashboard",)]
        if nohc:
            raise TestWarn(f"no healthcheck on: {nohc}")
    out.append(T("static", "docker-compose.yml parses and every service is named", compose))

    async def keyshape(ctx):
        hits = []
        for p in files(root, {".py", ".js", ".html", ".yml", ".yaml", ".json", ".md", ".sh", ".bat", ".txt", ".sql", ".toml", ".example"}):
            if os.path.basename(p) == ".env" or p.endswith("suites_aim.py"):
                continue
            t = rd(p)
            for rx in KEY_PATTERNS:
                if re.search(rx, t):
                    hits.append(os.path.relpath(p, root))
        check(not hits, f"key-shaped strings in source files: {sorted(set(hits))}", "none", hits)
    out.append(T("static", "no API-key-shaped strings in source files", keyshape))

    async def livesecrets(ctx):
        vals = {v: os.getenv(v) for v in SECRET_VARS if len(os.getenv(v) or "") >= 8}
        hits = []
        for p in files(root, {".py", ".js", ".html", ".yml", ".yaml", ".json", ".md", ".sh", ".bat", ".txt", ".sql", ".toml"}):
            t = rd(p)
            for name, val in vals.items():
                if val in t:
                    hits.append(f"{name} found in {os.path.relpath(p, root)}")
        check(not hits, f"live secret values present in source files (names only shown): {hits}", "none", hits)
        ctx.note(f"checked {len(vals)} live secrets against all source files")
    out.append(T("static", "no live secret value is hard-coded in any source file", livesecrets))

    async def urlkey(ctx):
        hits = [os.path.relpath(p, root) for p in files(root, {".py", ".js", ".html"}) if re.search(r"[?&]key=", rd(p)) and not p.endswith("suites_aim.py")]
        check(not hits, f"API key passed in a URL query string in: {hits}", "key sent in a header", hits)
    out.append(T("static", "no API key is ever put in a URL (it would end up in logs)", urlkey))

    async def reqs(ctx):
        miss = []
        for rq in files(root, {".txt"}):
            if os.path.basename(rq) != "requirements.txt":
                continue
            for line in rd(rq).splitlines():
                line = line.strip()
                if line and not line.startswith("#") and "==" not in line and "<" not in line and ">" not in line:
                    miss.append(f"{os.path.relpath(rq, root)}: {line}")
        if miss:
            raise TestWarn(f"unpinned dependencies (builds not reproducible): {miss}")
    out.append(T("static", "dependencies are pinned", reqs))
    return out


# ═════════════════════════════════ SMOKE ═════════════════════════════════
def smoke_tests(ctx):
    out = []
    for svc in HOSTS:
        if svc == "dashboard":
            continue

        async def fn(ctx, svc=svc):
            r = await ctx.call("GET", U(svc, "/health"))
            check(r.status == 200, f"/health returned {r.status}", 200, r.status)
            d = r.json()
            check(d.get("status") == "ok", f"health body says {d}", {"status": "ok"}, d)
            ctx.note(f"{r.ms} ms")
            if r.ms > 800:
                raise TestWarn(f"slow health response ({r.ms} ms)")
        out.append(T("smoke", f"{svc} /health", fn))

    async def qd(ctx):
        r = await ctx.call("GET", QDRANT + "/readyz")
        check(r.status == 200, f"Qdrant not ready ({r.status})", 200, r.status)
    out.append(T("smoke", "qdrant is ready", qd))

    async def models(ctx):
        r = await ctx.call("GET", OLLAMA + "/api/tags")
        check(r.status == 200, f"Ollama returned {r.status}")
        have = {m["name"] for m in r.json().get("models", [])}
        need = {os.getenv("OLLAMA_EMBED_MODEL", "nomic-embed-text") + ":latest", os.getenv("OLLAMA_OFFLINE_MODEL", "qwen3:4b"), os.getenv("OLLAMA_SCREEN_MODEL", "gemma3:4b")}
        miss = [n for n in need if n not in have and n.replace(":latest", "") not in have]
        check(not miss, f"models missing from Ollama: {miss}", sorted(need), sorted(have))
    out.append(T("smoke", "ollama has the embedding and fallback models", models))

    async def redis_ping(ctx):
        r = rclient()
        try:
            t = time.time()
            check(await r.ping(), "redis PING failed")
            ctx.note(f"{int((time.time() - t) * 1000)} ms")
        finally:
            await r.aclose()
    out.append(T("smoke", "redis answers PING", redis_ping))

    async def pg(ctx):
        try:
            rd_, wr = await asyncio.wait_for(asyncio.open_connection("postgres", 5432), 3)
            wr.close()
        except Exception as e:
            raise TestFail(f"postgres not reachable: {type(e).__name__}")
        r = await ctx.call("GET", U("memory", "/memories?limit=1"))
        check(r.status == 200, f"memory service could not read PostgreSQL ({r.status})", 200, r.status)
    out.append(T("smoke", "postgres is reachable and readable through the memory service", pg))

    for page in ("index.html", "research.html", "timer.html"):
        async def fn(ctx, page=page):
            r = await ctx.call("GET", U("dashboard", "/" + page))
            check(r.status == 200, f"{page} returned {r.status}", 200, r.status)
            check("text/html" in r.headers.get("content-type", ""), "not served as HTML", "text/html", r.headers.get("content-type"))
            check(len(r.text) > 2000, f"{page} suspiciously small ({len(r.text)} bytes)")
        out.append(T("smoke", f"dashboard serves {page}", fn))

    async def bridge(ctx):
        r = await ctx.call("GET", BRIDGE + "/health")
        check(r.status == 200, f"host bridge returned {r.status}", 200, r.status)
        r2 = await ctx.call("GET", BRIDGE + "/system-stats")
        check(r2.status == 200 and isinstance(r2.json(), dict), "host bridge /system-stats failed")
    out.append(T("smoke", "host bridge answers (health + system stats)", bridge))
    return out


# ═════════════════════════════════ CONTRACT (from each service's OpenAPI) ═════════════════════════════════
async def contract_tests(ctx):
    out = []
    ctx.shared["routes"] = set()
    ctx.shared["routes_tested"] = set()
    for svc in HOSTS:
        if svc == "dashboard":
            continue
        try:
            r = await ctx.call("GET", U(svc, "/openapi.json"), timeout=10)
            spec = r.json()
        except Exception as e:
            async def bad(ctx, svc=svc, e=e):
                raise TestFail(f"{svc}: OpenAPI document unavailable ({e})")
            out.append(T("contract", f"{svc} publishes /openapi.json", bad))
            continue

        async def miss404(ctx, svc=svc):
            r = await ctx.call("GET", U(svc, "/__aimtest_missing__"))
            check(r.status == 404, f"unknown route should be 404", 404, r.status)
        out.append(T("contract", f"{svc} unknown route -> 404", miss404))

        for path, method, op in operations(spec):
            if any(path.startswith(x) for x in NO_CONTRACT):
                continue
            ctx.shared["routes"].add((svc, method, path))
            unsafe = (svc, method, path) in UNSAFE
            ai = (svc, path) in AI_ROUTES

            async def pos(ctx, svc=svc, method=method, path=path, op=op, spec=spec):
                p, q, body, multipart, has_pp = build_request(spec, path, op)
                if multipart:
                    raise TestSkip("multipart file upload - exercised in the end-to-end flows")
                if (svc, path) in CLOUD_ROUTES and not await cloud_ai(ctx):
                    raise TestWarn(NO_CLOUD)
                kw = {}
                if q: kw["params"] = q
                if body is not None: kw["json"] = body
                r = await ctx.call(method, U(svc, p), timeout=90, **kw)
                ctx.shared["routes_tested"].add((svc, method, path))
                check(r.status < 500, f"server error {r.status}", "status < 500", f"{r.status}: {r.text[:140]}")
                if has_pp:
                    okc = (400, 404, 422) + ((200, 204) if method == "DELETE" else ())   # DELETE of an unknown id may be idempotent
                    check(r.status in okc, f"unknown id should be 404/422, got {r.status}", "404 or 422", r.status)
                elif method == "GET":
                    check(r.status < 400, f"GET with valid input returned {r.status}", "2xx", f"{r.status}: {r.text[:120]}")
                if r.status < 300 and method == "POST" and "json" in r.headers.get("content-type", ""):
                    try:
                        d = r.json()
                    except Exception:
                        d = None
                    if isinstance(d, dict) and d.get("id") and svc == "memory" and path == "/memories":
                        track_memory(ctx, d["id"])
                    if isinstance(d, dict) and d.get("id") and svc == "memory" and path == "/research/projects":
                        ctx.cleanups.append(lambda i=d["id"]: ctx.call("DELETE", U("memory", f"/research/projects/{i}")))
                    if isinstance(d, dict) and d.get("id") and svc == "research-agent" and path == "/projects/new":
                        ctx.cleanups.append(lambda i=d["id"]: ctx.call("DELETE", U("research-agent", f"/projects/{i}")))
                ok = (op.get("responses", {}).get("200", {}).get("content", {}).get("application/json", {}) or {}).get("schema")
                if ok and r.status == 200:
                    sch = _resolve(spec, ok)
                    if sch.get("required") and isinstance(r.json(), dict):
                        miss = [k for k in sch["required"] if k not in r.json()]
                        check(not miss, f"response missing required fields {miss}", sch["required"], list(r.json()))
                ctx.note(f"{r.status} in {r.ms} ms")
                if r.ms > (40000 if (svc, path) in AI_ROUTES else 5000):
                    raise TestWarn(f"slow ({r.ms} ms)")
            out.append(T("contract", f"{method} {svc}{path}", pos, ai=ai, unsafe=unsafe, timeout=120))

            if method in ("POST", "PUT", "PATCH") and has_required_body(spec, op) and not unsafe and not ai:
                async def neg(ctx, svc=svc, method=method, path=path, op=op, spec=spec):
                    p, _, _, _, _ = build_request(spec, path, op)
                    r = await ctx.call(method, U(svc, p), json={})
                    check(r.status in (400, 404, 422), f"empty body should be rejected with 422, got {r.status}", "422", f"{r.status}: {r.text[:100]}")
                    r2 = await ctx.call(method, U(svc, p), content=b"{not json", headers={"Content-Type": "application/json"})
                    check(r2.status in (400, 404, 422), f"malformed JSON should be rejected, got {r2.status}", "422", r2.status)
                out.append(T("contract", f"{method} {svc}{path} rejects bad input", neg))
    return out


# ═════════════════════════════════ DATA ═════════════════════════════════
def data_tests(ctx):
    out = []
    TEXT = f"{MARK}: the quick brown axolotl files its taxes every Tuesday"

    async def collection(ctx):
        r = await ctx.call("GET", QDRANT + "/collections/aim_memories")
        check(r.status == 200, "Qdrant collection aim_memories missing", 200, r.status)
        cfg = r.json()["result"]["config"]["params"]["vectors"]
        check(cfg["size"] == 768, f"vector size {cfg['size']} != 768 (embedding model mismatch)", 768, cfg["size"])
        check(cfg["distance"] == "Cosine", "distance should be Cosine", "Cosine", cfg["distance"])
    out.append(T("data", "qdrant collection exists with 768-dim cosine vectors", collection))

    async def create(ctx):
        r = await ctx.call("POST", U("memory", "/memories"), json={"content": TEXT, "source": MARK, "tier": "short", "importance": 0.1, "tags": [MARK]}, timeout=60)
        check(r.status == 200, f"create returned {r.status}: {r.text[:120]}", 200, r.status)
        ctx.shared["mid"] = r.json()["id"]
        track_memory(ctx, ctx.shared["mid"])
        ctx.note(f"stored + embedded in {r.ms} ms")
    out.append(T("data", "memory round trip 1/6: create (PostgreSQL row + embedding + Qdrant point)", create))

    async def get(ctx):
        mid = ctx.shared.get("mid") or (_ for _ in ()).throw(TestSkip("create step failed"))
        r = await ctx.call("GET", U("memory", f"/memories/{mid}"))
        check(r.status == 200 and r.json().get("content") == TEXT, "PostgreSQL did not return what was stored", TEXT, r.text[:100])
    out.append(T("data", "memory round trip 2/6: read back from PostgreSQL", get))

    async def search(ctx):
        mid = ctx.shared.get("mid") or (_ for _ in ()).throw(TestSkip("create step failed"))
        r = await ctx.call("POST", U("memory", "/memories/search"), json={"query": "axolotl taxes Tuesday", "limit": 5}, timeout=60)
        res = r.json().get("results", [])
        hit = next((x for x in res if x["id"] == mid), None)
        check(hit, "semantic search did not find the memory just stored", mid, [x["id"] for x in res])
        check(hit["score"] > 0.6, f"similarity too low ({hit['score']:.2f})", "> 0.6", round(hit["score"], 2))
        ctx.note(f"score {hit['score']:.2f}")
    out.append(T("data", "memory round trip 3/6: semantic search finds it (Qdrant + embeddings)", search))

    async def qpoint(ctx):
        mid = ctx.shared.get("mid") or (_ for _ in ()).throw(TestSkip("create step failed"))
        r = await ctx.call("POST", QDRANT + "/collections/aim_memories/points", json={"ids": [mid]})
        check(r.status == 200 and len(r.json()["result"]) == 1, "Qdrant has no point for this memory", 1, r.text[:100])
    out.append(T("data", "memory round trip 4/6: the vector point exists in Qdrant", qpoint))

    async def delete(ctx):
        mid = ctx.shared.get("mid") or (_ for _ in ()).throw(TestSkip("create step failed"))
        r = await ctx.call("DELETE", U("memory", f"/memories/{mid}"))
        check(r.status == 200, f"delete returned {r.status}", 200, r.status)
        r2 = await ctx.call("GET", U("memory", f"/memories/{mid}"))
        check(r2.status == 404, "row still in PostgreSQL after delete", 404, r2.status)
    out.append(T("data", "memory round trip 5/6: delete removes the PostgreSQL row", delete))

    async def gone(ctx):
        mid = ctx.shared.get("mid") or (_ for _ in ()).throw(TestSkip("create step failed"))
        r = await ctx.call("POST", QDRANT + "/collections/aim_memories/points", json={"ids": [mid]})
        check(len(r.json()["result"]) == 0, "vector point still in Qdrant after delete (orphan)", 0, len(r.json()["result"]))
        s = await ctx.call("POST", U("memory", "/memories/search"), json={"query": "axolotl taxes Tuesday", "limit": 5}, timeout=60)
        check(mid not in [x["id"] for x in s.json().get("results", [])], "deleted memory still returned by search")
    out.append(T("data", "memory round trip 6/6: delete removes the vector too (no orphans)", gone))

    async def pubsub(ctx):
        ch, got = f"aimtest.{uuid.uuid4().hex[:6]}", []
        r = rclient()
        ps = r.pubsub()
        try:
            await ps.subscribe(ch)
            await ps.get_message(timeout=1)
            await r.publish(ch, "ping")
            m = await until(lambda: ps.get_message(ignore_subscribe_messages=True, timeout=1), 3)
            check(m and m["data"] == "ping", "published message was not delivered to a subscriber")
        finally:
            await ps.aclose(); await r.aclose()
    out.append(T("data", "redis pub/sub delivers a message", pubsub))

    async def timeline_rt(ctx):
        ctx.cleanups.append(lambda: ctx.call("DELETE", U("timeline", "/events/test")))
        r = await ctx.call("POST", U("timeline", "/events"), json={"event_type": "aimtest.data", "description": f"{MARK} roundtrip"})
        check(r.status == 200, f"create event returned {r.status}", 200, r.status)
        g = await ctx.call("GET", U("timeline", "/events?event_type=aimtest.data&limit=5"))
        check(any(e["description"].endswith("roundtrip") for e in g.json()), "event not returned after being written to PostgreSQL")
    out.append(T("data", "timeline writes to and reads from PostgreSQL", timeline_rt))

    async def project_rt(ctx):
        r = await ctx.call("POST", U("memory", "/research/projects"), json={"name": f"{MARK} proj", "kind": "planned", "graph": {"nodes": [], "edges": []}})
        check(r.status == 200, f"create project {r.status}: {r.text[:100]}")
        pid = r.json()["id"]
        try:
            u = await ctx.call("PUT", U("memory", f"/research/projects/{pid}"), json={"name": f"{MARK} proj2", "kind": "planned", "graph": {"nodes": [], "edges": []}, "overrides": {"x": 1}})
            check(u.json()["name"] == f"{MARK} proj2" and u.json()["overrides"] == {"x": 1}, "update did not persist")
        finally:
            await ctx.call("DELETE", U("memory", f"/research/projects/{pid}"))
        g = await ctx.call("GET", U("memory", f"/research/projects/{pid}"))
        check(g.status == 404, "project still there after delete", 404, g.status)
    out.append(T("data", "research project store: create, update, delete in PostgreSQL", project_rt))

    async def consistency(ctx):
        a = await ctx.call("GET", U("memory", "/memories?limit=1000"))
        b = await ctx.call("POST", QDRANT + "/collections/aim_memories/points/count", json={"exact": True})
        pg, qd = len(a.json()), b.json()["result"]["count"]
        ctx.note(f"PostgreSQL {pg} rows, Qdrant {qd} vectors")
        if pg != qd:
            raise TestWarn(f"PostgreSQL has {pg} memories but Qdrant has {qd} vectors (out of sync)", pg, qd)
    out.append(T("data", "PostgreSQL and Qdrant hold the same number of memories", consistency))
    return out


# ═════════════════════════════════ BUS ═════════════════════════════════
def bus_tests(ctx):
    out = []

    async def notify_bus(ctx):
        ctx.cleanups.append(lambda: ctx.call("DELETE", U("notification", "/history/test")))
        src = f"{MARK}-{uuid.uuid4().hex[:6]}"
        await pub("aim.notification.send", {"title": MARK, "message": f"{MARK} bus", "confidence": 0.9, "source": src})

        async def seen():
            h = await ctx.call("GET", U("notification", "/history?limit=50"))
            return [x for x in h.json() if x.get("source") == src]
        got = await until(seen, 5)
        check(got, "notification published on the bus never reached the history", "1 entry", "none after 5 s")
        await pub("aim.notification.send", {"title": MARK, "message": f"{MARK} again", "confidence": 0.9, "source": src})
        await asyncio.sleep(1.5)
        check(len(await seen()) == 1, "cooldown did not suppress the second notification from the same source", 1, len(await seen()))
        low = f"{MARK}-low-{uuid.uuid4().hex[:4]}"
        await pub("aim.notification.send", {"title": MARK, "message": MARK, "confidence": 0.1, "source": low})
        await asyncio.sleep(1.5)
        h = await ctx.call("GET", U("notification", "/history?limit=50"))
        check(not [x for x in h.json() if x.get("source") == low], "low-confidence notification was NOT filtered", "dropped", "delivered")
    out.append(T("bus", "notification: delivered, cooldown suppresses repeats, low confidence filtered", notify_bus))

    async def timeline_bus(ctx):
        ctx.cleanups.append(lambda: ctx.call("DELETE", U("timeline", "/events/test")))
        await pub("aim.timeline.event", {"event_type": "aimtest.bus", "description": f"{MARK} via redis"})

        async def seen():
            r = await ctx.call("GET", U("timeline", "/events?event_type=aimtest.bus&limit=5"))
            return r.status == 200 and r.json()
        check(await until(seen, 5), "timeline never recorded an event published on aim.timeline.event", "event in PostgreSQL", "none after 5 s")
    out.append(T("bus", "timeline: event published on the bus is stored in PostgreSQL", timeline_bus))

    async def mem_event(ctx):
        r = rclient(); ps = r.pubsub()
        try:
            await ps.subscribe("aim.memory.stored"); await ps.get_message(timeout=1)
            c = await ctx.call("POST", U("memory", "/memories"), json={"content": f"{MARK} bus memory", "source": MARK, "tier": "short"}, timeout=60)
            track_memory(ctx, c.json()["id"])
            m = await until(lambda: ps.get_message(ignore_subscribe_messages=True, timeout=1), 5)
            check(m, "memory service did not announce aim.memory.stored", "an event", "silence")
        finally:
            await ps.aclose(); await r.aclose()
    out.append(T("bus", "memory: storing a memory announces aim.memory.stored", mem_event))

    async def brain_bus(ctx):
        ctx.cleanups.append(lambda: purge_test_rows(ctx))
        await pub("aim.browser.tab", {"url": "http://aimtest.example/page", "title": f"{MARK} page", "app": "browser", "summary": f"{MARK} browsing event"})

        async def seen():
            r = await ctx.call("GET", U("timeline", "/events?event_type=browser.tab&limit=10"))
            return any(MARK in json.dumps(e) for e in r.json()) if r.status == 200 else False
        check(await until(seen, 60, 2), "brain never turned a browser-tab event into a timeline event within 60 s", "timeline event", "none")
    out.append(T("bus", "brain: a browsing event is classified by the LLM and logged to the timeline", brain_bus, ai=True, timeout=90))

    async def research_bus(ctx):
        if not await cloud_ai(ctx):
            raise TestWarn(NO_CLOUD)
        ctx.cleanups.append(lambda: purge_test_rows(ctx))
        r = rclient(); ps = r.pubsub()
        try:
            await ps.subscribe("aim.research.result"); await ps.get_message(timeout=1)
            await pub("aim.research.request", {"query": f"{MARK} what does Redis PUBSUB NUMSUB do"})
            m = await until(lambda: ps.get_message(ignore_subscribe_messages=True, timeout=1), 90, 1)
            check(m, "research agent never answered aim.research.request within 90 s", "aim.research.result", "silence")
            d = json.loads(m["data"])
            if d.get("memory_id"):
                track_memory(ctx, d["memory_id"])
            check(d.get("summary"), "research result has an empty summary")
        finally:
            await ps.aclose(); await r.aclose()
    out.append(T("bus", "research: a request on the bus produces a stored result", research_bus, ai=True, timeout=120))
    return out


# ═════════════════════════════════ FLOWS ═════════════════════════════════
def flow_tests(ctx):
    out = []

    async def voice_text(ctx):
        r = await ctx.call("POST", U("voice-agent", "/converse/text"), json={"text": f"{MARK} say hello in one short sentence"}, timeout=120)
        check(r.status == 200, f"converse/text returned {r.status}", 200, r.status)
        d = r.json()
        check(d.get("reply"), "empty reply")
        check("trouble reaching my thinking models" not in d["reply"], "voice agent fell back to its error message (router/LLM unreachable)", "a real answer", d["reply"][:80])
        dur, sr, size = wav_info(d["audio_b64"])
        check(0.3 < dur < 90, f"audio length {dur:.1f}s is implausible", "0.3-90 s", round(dur, 1))
        ctx.note(f"reply {len(d['reply'])} chars, audio {dur:.1f}s @ {sr} Hz, total {r.ms} ms")
        if r.ms > 15000:
            raise TestWarn(f"slow answer ({r.ms} ms)")
    out.append(T("flows", "voice: text in -> memory recall -> LLM -> Baymax speech (valid WAV)", voice_text, ai=True, timeout=150))

    async def tts_stt(ctx):
        import httpx
        async with httpx.AsyncClient(timeout=120) as c:
            sr = await c.post(U("voice-agent", "/speak"), json={"text": "hello world this is a test", "fx": False})
            tr = await c.post(U("voice-agent", "/transcribe"), files={"file": ("t.wav", sr.content, "audio/wav")})
        check(tr.status_code == 200, f"transcribe returned {tr.status_code}", 200, tr.status_code)
        txt = tr.json().get("text", "").lower()
        check("hello" in txt, f"speech round trip lost the words: {txt!r}", "contains 'hello'", txt)
        ctx.note(f"heard: {txt.strip()!r}")
    out.append(T("flows", "voice: speak -> transcribe round trip (TTS and speech recognition agree)", tts_stt, timeout=150))

    async def browse(ctx):
        r = await ctx.call("POST", U("browser-agent", "/browse"), json={"url": "http://dashboard:8888/index.html", "summarize": False, "store": False}, timeout=60)
        check(r.status == 200, f"browse returned {r.status}: {r.text[:100]}", 200, r.status)
        d = r.json()
        check("AIM" in d["title"], f"unexpected page title {d['title']!r}", "contains AIM", d["title"])
        check(len(d["text"]) > 100, "page text too short")
    out.append(T("flows", "browser agent: loads the dashboard in headless Chromium", browse))

    async def browse_sum(ctx):
        r = await ctx.call("POST", U("browser-agent", "/browse"), json={"url": "http://dashboard:8888/research.html", "summarize": True, "store": True}, timeout=180)
        check(r.status == 200, f"browse returned {r.status}")
        d = r.json()
        if d.get("memory_id"):
            track_memory(ctx, d["memory_id"])
        check(d.get("summary"), "no summary produced", "summary", d.get("summary"))
        check(d.get("memory_id"), "page was not stored in memory", "memory id", d.get("memory_id"))
    out.append(T("flows", "browser agent: browse -> LLM summary -> stored in memory", browse_sum, ai=True, timeout=200))

    async def browse_announces(ctx):
        ctx.cleanups.append(lambda: purge_test_rows(ctx))
        r = rclient(); ps = r.pubsub()
        try:
            await ps.subscribe("aim.browser.tab"); await ps.get_message(timeout=1)
            b = await ctx.call("POST", U("browser-agent", "/browse"), json={"url": "http://dashboard:8888/timer.html", "summarize": False, "store": True}, timeout=60)
            check(b.status == 200, f"browse returned {b.status}")
            if b.json().get("memory_id"):
                track_memory(ctx, b.json()["memory_id"])
            m = await until(lambda: ps.get_message(ignore_subscribe_messages=True, timeout=1), 8)
            check(m, "browser agent did not publish aim.browser.tab after browsing", "an event", "silence")
            check("timer.html" in json.loads(m["data"])["url"], "event carries the wrong URL")
        finally:
            await ps.aclose(); await r.aclose()
    out.append(T("flows", "browser agent: every stored browse is announced on aim.browser.tab", browse_announces, timeout=90))

    async def router_fast(ctx):
        r = await ctx.call("POST", U("llm-router", "/route"), json={"task": "fast", "prompt": "Reply with exactly the single word: PONG"}, timeout=90)
        check(r.status == 200, f"router returned {r.status}: {r.text[:120]}")
        d = r.json()
        check("PONG" in d["result"].upper(), f"model ignored the instruction: {d['result'][:60]!r}", "PONG", d["result"][:60])
        ctx.note(f"{d['provider']}/{d['model']} in {r.ms} ms")
    out.append(T("flows", "llm-router: fast task answers through a cloud model", router_fast, ai=True))

    async def router_offline(ctx):
        r = await ctx.call("POST", U("llm-router", "/route"), json={"task": "fast", "prompt": "Reply with exactly the single word: PONG", "offline": True}, timeout=170)
        check(r.status == 200, f"offline route returned {r.status}: {r.text[:120]}")
        d = r.json()
        check(d["provider"] == "ollama", f"offline request used {d['provider']}", "ollama", d["provider"])
        check("PONG" in d["result"].upper(), f"local model answer: {d['result'][:60]!r}", "PONG", d["result"][:60])
        ctx.note(f"{d['model']} in {r.ms} ms")
        if r.ms > 60000:
            raise TestWarn(f"local fallback took {r.ms // 1000}s")
    out.append(T("flows", "llm-router: offline mode falls back to the local model", router_offline, timeout=180))

    async def project_life(ctx):
        base = "research-agent"
        r = await ctx.call("POST", U(base, "/projects/new"), json={"name": f"{MARK} life", "type": "cli", "idea": ""})
        check(r.status == 200, f"create returned {r.status}")
        pid = r.json()["id"]
        ctx.cleanups.append(lambda: ctx.call("DELETE", U(base, f"/projects/{pid}")))
        a = (await ctx.call("GET", U(base, f"/projects/{pid}/live"))).json()
        check(a["completion"]["pct"] == 0, f"fresh plan should be 0% but is {a['completion']['pct']}%", 0, a["completion"]["pct"])
        check(all(e["state"] == "planned" for e in a["edges"].values()), "planned links should all be 'planned'")
        await ctx.call("POST", U(base, f"/projects/{pid}/override"), json={"key": "cli:built", "value": True})
        await asyncio.sleep(2.2)
        b = (await ctx.call("GET", U(base, f"/projects/{pid}/live"))).json()
        check(b["completion"]["pct"] > 0, "ticking a box did not raise the completion %", "> 0", b["completion"]["pct"])
        await ctx.call("POST", U(base, f"/projects/{pid}/override"), json={"key": "task:t1", "value": True})
        await asyncio.sleep(2.2)
        c = (await ctx.call("GET", U(base, f"/projects/{pid}/live"))).json()
        check(c["completion"]["pct"] > b["completion"]["pct"], "ticking a task did not raise the completion %", b["completion"]["pct"], c["completion"]["pct"])
        d = await ctx.call("DELETE", U(base, f"/projects/{pid}"))
        check(d.status == 200 and (await ctx.call("GET", U(base, f"/projects/{pid}"))).status == 404, "project not deleted")
        ctx.note(f"0% -> {b['completion']['pct']}% -> {c['completion']['pct']}%")
    out.append(T("flows", "research center: new plan -> 0% -> tick items -> % rises -> delete", project_life, timeout=60))

    async def scanner_aim(ctx):
        r = await ctx.call("POST", U("research-agent", "/projects/open"), json={"path": ctx.root, "name": "AIM OS"}, timeout=120)
        check(r.status == 200, f"scan returned {r.status}: {r.text[:100]}")
        g = r.json()["graph"]
        check(len(g["nodes"]) >= 35 and len(g["edges"]) >= 60, f"scan found only {len(g['nodes'])} nodes / {len(g['edges'])} edges", ">=35 / >=60", f"{len(g['nodes'])} / {len(g['edges'])}")
        ids = [n["id"] for n in g["nodes"]]
        check(len(ids) == len(set(ids)), "duplicate node ids")
        labels = {(e["from"], e["to"], e["label"]) for e in g["edges"]}
        check(("agents/voice/main.py", "core/memory/main.py", "POST /memories/search") in labels, "expected link voice -> memory (POST /memories/search) not detected")
        check(any(e["kind"] == "redis" and e.get("channel") == "aim.notification.send" for e in g["edges"]), "expected Redis channel aim.notification.send not detected")
    out.append(T("flows", "scanner: AIM OS scan finds the known services, routes and Redis channels", scanner_aim, timeout=130))

    async def notify_direct(ctx):
        ctx.cleanups.append(lambda: ctx.call("DELETE", U("notification", "/history/test")))
        src = f"{MARK}-{uuid.uuid4().hex[:6]}"
        r = await ctx.call("POST", U("notification", "/notify"), json={"title": MARK, "message": "direct", "confidence": 0.95, "source": src})
        check(r.status == 200, f"notify returned {r.status}")
        h = (await ctx.call("GET", U("notification", "/history?limit=50"))).json()
        check(any(x.get("source") == src for x in h), "notification missing from history")
    out.append(T("flows", "notification: POST /notify lands in history", notify_direct))

    async def timeline_shapes(ctx):
        for path in ("/events/today", "/events/week"):
            r = await ctx.call("GET", U("timeline", path))
            check(r.status == 200 and isinstance(r.json(), list), f"{path} should return a list", "list", r.text[:60])
        s = await ctx.call("GET", U("timeline", "/stats"))
        check(s.status == 200 and isinstance(s.json(), dict), "/stats should return an object", "object", s.text[:60])
    out.append(T("flows", "timeline: today / week / stats return the right shapes", timeline_shapes))

    async def semantic(ctx):
        ids = []
        for txt in (f"{MARK} my cat sleeps on the sofa all day and purrs", f"{MARK} rockets burn fuel to reach orbit around the earth", f"{MARK} boil pasta in salted water for ten minutes"):
            r = await ctx.call("POST", U("memory", "/memories"), json={"content": txt, "source": MARK, "tier": "short", "importance": 0.1}, timeout=60)
            ids.append(r.json()["id"]); track_memory(ctx, ids[-1])
        s = await ctx.call("POST", U("memory", "/memories/search"), json={"query": "domestic feline pets", "limit": 3}, timeout=60)
        top = [x for x in s.json()["results"] if x["id"] in ids]
        check(top and top[0]["id"] == ids[0], "semantic search ranked the wrong memory first (embeddings not understanding meaning)", "cat memory first", top[0]["content"][:40] if top else "none")
        s2 = await ctx.call("POST", U("memory", "/memories/search"), json={"query": "space travel launch", "limit": 3}, timeout=60)
        top2 = [x for x in s2.json()["results"] if x["id"] in ids]
        check(top2 and top2[0]["id"] == ids[1], "space query did not rank the rocket memory first")
    out.append(T("flows", "memory: semantic search understands meaning (cats vs rockets vs pasta)", semantic, timeout=150))

    async def web_research(ctx):
        if not await cloud_ai(ctx):
            raise TestWarn(NO_CLOUD)
        r = await ctx.call("POST", U("research-agent", "/research"), json={"query": f"{MARK} what is a Redis pub/sub channel"}, timeout=120)
        check(r.status == 200, f"research returned {r.status}")
        d = r.json()
        if d.get("stored_memory_id"):
            track_memory(ctx, d["stored_memory_id"])
        if d["summary"].startswith("Research on:") and not d["key_points"]:
            raise TestWarn("AI unavailable right now (rate limit or quota): research returned only a placeholder")
        check(len(d["summary"]) > 40 and d["key_points"], "research result is empty", "summary + key points", d["summary"][:60])
    out.append(T("flows", "research: web question -> structured answer stored in memory", web_research, ai=True, timeout=150))

    async def web_search_flow(ctx):
        st = (await ctx.call("GET", U("research-agent", "/search/status"))).json()
        if not st["tavily"]:
            raise TestWarn("TAVILY_API_KEY is not set in .env - add it and run: python aim.py start", "Tavily search configured", "not configured")
        r = await ctx.call("POST", U("research-agent", "/search"), json={"query": "redis pubsub numsub command", "n": 3}, timeout=40)
        if r.status == 503:
            raise TestWarn(f"Tavily unavailable: {r.text[:120]}")
        check(r.status == 200 and r.json()["results"], f"search returned {r.status}: {r.text[:100]}", ">= 1 result", r.text[:80])
        res = r.json()["results"][0]
        check(res["url"].startswith("http") and res["content"], "result has no url or content")
        ctx.note(f"{len(r.json()['results'])} results, first: {res['url'][:50]}")
    out.append(T("flows", "live web search: Tavily returns real results (uses 1 free credit)", web_search_flow, ai=True, timeout=60))

    async def think_compare(ctx):
        r = await ctx.call("POST", U("research-agent", "/ai/think"), json={"name": MARK, "type": "app", "messages": [{"role": "user", "content": f"{MARK} I want a tiny todo app because I forget things"}]}, timeout=90)
        d = r.json()
        if d.get("rate_limited"): raise TestWarn("rate limited")
        check(r.status == 200 and d.get("reply"), "think-with-me returned no reply", "a reply", r.text[:100])
        if not await cloud_ai(ctx):
            raise TestWarn("think-with-me works; comparison not run: " + NO_CLOUD)
        c = await ctx.call("POST", U("research-agent", "/ai/compare"), json={"name": MARK, "type": "app", "idea": f"{MARK} tiny todo app"}, timeout=150)
        d = c.json()
        if d.get("rate_limited"): raise TestWarn("rate limited")
        check(len(d.get("approaches", [])) == 3, "comparison should have exactly 3 approaches", 3, len(d.get("approaches", [])))
        check(all(a.get("scores") and len(a["scores"]) == 7 for a in d["approaches"]), "an approach is missing its 7 scores")
        check(d["recommendation"]["id"] in "abc", "recommendation does not point at an approach")
    out.append(T("flows", "new-project coach: think-with-me reply + 3-way comparison with scores", think_compare, ai=True, timeout=260))

    async def novelty(ctx):
        if not await cloud_ai(ctx):
            raise TestWarn(NO_CLOUD)
        r = await ctx.call("POST", U("research-agent", "/ai/novelty"), json={"name": MARK, "type": "app", "idea": f"{MARK} tiny todo app"}, timeout=150)
        d = r.json()
        if d.get("rate_limited"): raise TestWarn("rate limited")
        check(d.get("verdict") in ("crowded", "niche", "open"), "bad novelty verdict", "crowded|niche|open", d.get("verdict"))
        check(len(d.get("differentiators", [])) >= 2 and all(1 <= x["novelty"] <= 10 for x in d["differentiators"]), "differentiators missing or out of range")
    out.append(T("flows", "novelty check: verdict, similar products, differentiators in range", novelty, ai=True, timeout=180))

    async def clone(ctx):
        import shutil
        dest = "/projects/workspace/octocat__Hello-World"
        existed = os.path.isdir(dest)
        r = await ctx.call("POST", U("research-agent", "/projects/clone"), json={"url": "https://github.com/octocat/Hello-World"}, timeout=200)
        if r.status in (400, 504) and "clone" in r.text.lower():
            raise TestWarn(f"could not clone (network?): {r.text[:100]}")
        check(r.status == 200, f"clone returned {r.status}: {r.text[:100]}")
        pid = r.json()["id"]
        ctx.cleanups.append(lambda: ctx.call("DELETE", U("research-agent", f"/projects/{pid}")))
        if not existed:
            async def rm():
                shutil.rmtree(dest, ignore_errors=True)
            ctx.cleanups.append(rm)
        check(r.json()["graph"].get("source", {}).get("git"), "clone did not record its source")
    out.append(T("flows", "git clone: public repo is cloned, scanned and recorded (then removed)", clone, ai=True, timeout=220))
    return out


# ═════════════════════════════════ SECURITY ═════════════════════════════════
def security_tests(ctx):
    out = []

    async def bridge_gone(ctx):
        for path in ("/screenshot", "/context", "/active-window", "/processes"):
            r = await ctx.call("GET", BRIDGE + path)
            check(r.status == 404, f"host bridge still answers {path} ({r.status}): screen capture must not exist", 404, r.status)
        r = await ctx.call("GET", BRIDGE + "/system-stats", headers={"Origin": "https://evil.example"})
        check(r.headers.get("access-control-allow-origin") != "*", "host bridge allows every website to read it (CORS *)", "http://localhost:8888", r.headers.get("access-control-allow-origin"))
    out.append(T("security", "host bridge: no screenshots, window titles or process lists exist; CORS is dashboard-only", bridge_gone))

    async def leak(ctx):
        vals = {v: os.getenv(v) for v in SECRET_VARS if len(os.getenv(v) or "") >= 8}
        hits = []
        for svc in HOSTS:
            for path in ("/health", "/openapi.json"):
                try:
                    r = await ctx.call("GET", U(svc, path), timeout=8)
                except TestFail:
                    continue
                for n, v in vals.items():
                    if v in r.text:
                        hits.append(f"{n} in {svc}{path}")
        check(not hits, f"secrets leaked in responses (names only): {hits}", "none", hits)
        ctx.note(f"{len(vals)} secrets x {len(HOSTS) * 2} responses")
    out.append(T("security", "no secret value appears in any /health or /openapi response", leak))

    async def cors(ctx):
        open_ = []
        for svc in HOSTS:
            if svc == "dashboard": continue
            r = await ctx.call("OPTIONS", U(svc, "/health"), headers={"Origin": "https://evil.example", "Access-Control-Request-Method": "GET"})
            if r.headers.get("access-control-allow-origin") in ("*", "https://evil.example"):
                open_.append(svc)
        if open_:
            raise TestWarn(f"any website you visit can call these APIs from your browser (CORS allows every origin): {open_}", "allow only http://localhost:8888", "*")
    out.append(T("security", "CORS only allows the dashboard origin", cors))

    async def exposure(ctx):
        d = yaml.safe_load(rd(os.path.join(ctx.root, "docker-compose.yml")))
        db, api = [], []
        for n, s in d["services"].items():
            for p in s.get("ports", []) or []:
                if not str(p).startswith("127.0.0.1"):
                    (db if any(k in str(s.get("image", "")) for k in ("postgres", "redis", "qdrant", "ollama")) else api).append(f"{n}:{p}")
        check(not db, f"databases/models published to every network interface: {db}", "127.0.0.1 only", db)
        if api:
            raise TestWarn(f"APIs listen on all interfaces with no authentication (anyone on your Wi-Fi/LAN can call them): {api}", "127.0.0.1:port", api)
    out.append(T("security", "published ports: databases are local-only, APIs bound to localhost", exposure))

    async def csrf(ctx):
        bad = []
        for svc, meth, path, body in (("memory", "DELETE", f"/memories/{uuid.uuid4()}", None), ("memory", "POST", "/memories/search", {"query": "x"}),
                                      ("timeline", "POST", "/events", {"event_type": "aimtest", "description": "x"}), ("notification", "DELETE", "/history/test", None),
                                      ("research-agent", "POST", "/projects/clone", {"url": "https://github.com/a/b"}), ("research-agent", "POST", "/projects/new", {"name": "aimtest"}),
                                      ("browser-agent", "POST", "/browse", {"url": "http://dashboard:8888/", "summarize": False, "store": False}), ("voice-agent", "POST", "/speak", {"text": "x"})):
            kw = {"json": body} if body is not None else {}
            r = await ctx.call(meth, U(svc, path), headers={"Origin": "https://evil.example"}, **kw)
            if r.status != 403:
                bad.append(f"{meth} {svc}{path} -> {r.status}")
        check(not bad, f"requests from another website were NOT refused: {bad}", "403", bad)
        ok = await ctx.call("POST", U("memory", "/memories/search"), headers={"Origin": "http://localhost:8888"}, json={"query": "aimtest"}, timeout=40)
        check(ok.status != 403, "the dashboard's own origin must still work", "not 403", ok.status)
        ctx.note("there is no login (local-only app): protection = ports bound to localhost + cross-site writes refused")
    out.append(T("security", "cross-site (CSRF) write requests are refused; the dashboard origin still works", csrf))

    async def dash_cors(ctx):
        r = await ctx.call("GET", U("dashboard", "/index.html"), headers={"Origin": "https://evil.example"})
        check(r.headers.get("access-control-allow-origin") is None, "dashboard server lets any website read its pages (CORS)", "no CORS header", r.headers.get("access-control-allow-origin"))
    out.append(T("security", "dashboard server sends no permissive CORS header", dash_cors))

    async def sqli(ctx):
        bad = []
        probes = [("timeline", "/events", {"project_id": "' OR 1=1 --"}), ("timeline", "/events", {"event_type": "x' OR '1'='1"}),
                  ("timeline", "/events", {"limit": "1; DROP TABLE timeline_events"}), ("memory", "/memories", {"tier": "short' OR 1=1 --"}),
                  ("memory", "/memories", {"limit": "5; DROP TABLE memories"})]
        for svc, path, q in probes:
            r = await ctx.call("GET", U(svc, path), params=q)
            if r.status >= 500:
                bad.append(f"{svc}{path}?{list(q)[0]} -> {r.status}")
        check(not bad, f"malformed/hostile input causes server errors (should be 400/422): {bad}", "4xx", bad)
        t = await ctx.call("GET", U("timeline", "/stats"))
        check(t.status == 200, "timeline broken after injection probes")
    out.append(T("security", "SQL injection probes are rejected cleanly and break nothing", sqli))

    async def special(ctx):
        txt = f"{MARK} <script>alert(1)</script> 'quote' \"dq\" \\back 日本語 😀 " + "x" * 2000
        r = await ctx.call("POST", U("memory", "/memories"), json={"content": txt, "source": MARK, "tier": "short"}, timeout=60)
        check(r.status == 200, f"special characters rejected ({r.status}): {r.text[:100]}")
        mid = r.json()["id"]; track_memory(ctx, mid)
        g = await ctx.call("GET", U("memory", f"/memories/{mid}"))
        check(g.json()["content"] == txt, "special characters were altered on the way through PostgreSQL", "identical", "changed")
    out.append(T("security", "HTML, quotes and unicode survive a store/read round trip unchanged", special))

    async def oversize(ctx):
        r = await ctx.call("POST", U("memory", "/memories"), json={"content": MARK + " " + "lorem ipsum " * 6000, "source": MARK, "tier": "short"}, timeout=90)
        if r.status == 200:
            track_memory(ctx, r.json()["id"])
        check(r.status < 500, f"a 70 KB memory crashes the service ({r.status}): {r.text[:120]}", "accepted or 4xx", r.status)
        ctx.note(f"{r.status} in {r.ms} ms")
    out.append(T("security", "a very large payload does not crash the memory service", oversize, timeout=120))

    async def traversal(ctx):
        for pth in ("/projects/aim-os/../../etc", "/etc", "/projects/../etc/passwd"):
            r = await ctx.call("POST", U("research-agent", "/projects/open"), json={"path": pth})
            check(r.status == 400, f"path {pth!r} was not refused ({r.status})", 400, r.status)
    out.append(T("security", "project open refuses paths outside the allowed folders", traversal))

    async def clone_rules(ctx):
        for u in ("file:///etc/passwd", "http://github.com/a/b", "https://evil.example.com/a/b", "https://github.com/a/b; rm -rf /", "https://github.com/a/b/../../c", "ext::sh -c id"):
            r = await ctx.call("POST", U("research-agent", "/projects/clone"), json={"url": u})
            check(r.status == 400, f"clone accepted {u!r} ({r.status})", 400, r.status)
    out.append(T("security", "git clone only accepts public https links on known hosts", clone_rules))

    async def ssrf(ctx):
        base = {"summarize": False, "store": False}
        r = await ctx.call("POST", U("browser-agent", "/browse"), json={"url": "file:///etc/passwd", **base})
        check(r.status == 400, f"browser agent accepted a file:// URL ({r.status})", 400, r.status)
        for target in ("http://memory:8003/health", "http://redis:6379/", "http://169.254.169.254/latest/meta-data/", "http://host.docker.internal:9999/health", "http://127.0.0.1:8005/health"):
            r = await ctx.call("POST", U("browser-agent", "/browse"), json={"url": target, **base}, timeout=40)
            check(r.status == 403, f"browser agent fetched an internal address: {target} ({r.status})", 403, r.status)
        ok = await ctx.call("POST", U("browser-agent", "/browse"), json={"url": "http://dashboard:8888/index.html", **base}, timeout=40)
        check(ok.status == 200, "the allow-listed dashboard must still be browsable", 200, ok.status)
    out.append(T("security", "browser agent blocks private/internal addresses (SSRF) but allows the dashboard", ssrf, timeout=200))

    async def hammer(ctx):
        rs = await asyncio.gather(*[ctx.call("GET", U("timeline", "/health")) for _ in range(60)])
        bad = [r.status for r in rs if r.status >= 500]
        check(not bad, f"{len(bad)} of 60 rapid requests failed with 5xx", 0, len(bad))
    out.append(T("security", "60 rapid requests produce no server errors", hammer))
    return out


# ═════════════════════════════════ PERFORMANCE ═════════════════════════════════
def perf_tests(ctx):
    out = []
    for svc in ("llm-router", "timeline", "memory", "brain", "research-agent", "notification", "browser-agent", "voice-agent"):
        async def fn(ctx, svc=svc):
            ms = []
            for _ in range(20):
                ms.append((await ctx.call("GET", U(svc, "/health"))).ms)
            p50, p95 = int(statistics.median(ms)), pctl(ms, 0.95)
            ctx.note(f"p50 {p50} ms, p95 {p95} ms (20 requests)")
            check(p95 < 1500, f"p95 latency {p95} ms", "< 1500 ms", p95)
            if p95 > 400:
                raise TestWarn(f"p95 {p95} ms is slower than expected for /health")
        out.append(T("perf", f"{svc} /health latency (20 sequential)", fn))

    async def conc(ctx):
        rs = await asyncio.gather(*[ctx.call("GET", U("memory", "/memories?limit=5"), timeout=30) for _ in range(30)])
        check(all(r.status == 200 for r in rs), f"{sum(r.status != 200 for r in rs)} of 30 concurrent reads failed", 30, sum(r.status == 200 for r in rs))
        ms = [r.ms for r in rs]
        ctx.note(f"30 concurrent: p50 {int(statistics.median(ms))} ms, p95 {pctl(ms, 0.95)} ms")
        check(pctl(ms, 0.95) < 3000, f"p95 {pctl(ms, 0.95)} ms under 30 concurrent reads", "< 3000 ms", pctl(ms, 0.95))
    out.append(T("perf", "memory: 30 concurrent reads", conc))

    async def embed(ctx):
        r = await ctx.call("POST", U("memory", "/memories"), json={"content": f"{MARK} latency probe", "source": MARK, "tier": "short"}, timeout=60)
        track_memory(ctx, r.json()["id"])
        ctx.note(f"{r.ms} ms")
        check(r.ms < 10000, f"storing a memory took {r.ms} ms", "< 10 s", r.ms)
        if r.ms > 3000:
            raise TestWarn(f"storing a memory took {r.ms} ms (embedding model cold or GPU busy)")
    out.append(T("perf", "memory: store + embed latency", embed))

    async def tts(ctx):
        r = await ctx.call("POST", U("voice-agent", "/speak"), json={"text": "Hello there, how are you today?"}, timeout=30)
        ctx.note(f"{r.ms} ms")
        check(r.status == 200 and r.ms < 6000, f"speech synthesis took {r.ms} ms", "< 6 s", r.ms)
    out.append(T("perf", "voice: text-to-speech latency", tts))

    async def stats(ctx):
        r = await ctx.call("GET", U("timeline", "/stats"))
        ctx.note(f"{r.ms} ms")
        check(r.ms < 1500, f"/stats took {r.ms} ms", "< 1.5 s", r.ms)
    out.append(T("perf", "timeline: stats query latency", stats))

    async def router(ctx):
        r = await ctx.call("POST", U("llm-router", "/route"), json={"task": "fast", "prompt": "Say OK"}, timeout=60)
        ctx.note(f"{r.ms} ms via {r.json().get('provider')}")
        check(r.status == 200, f"router {r.status}")
        if r.ms > 8000:
            raise TestWarn(f"fast route took {r.ms} ms")
    out.append(T("perf", "llm-router: fast route latency", router, ai=True))
    return out
