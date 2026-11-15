"""
AIM test engine: a layered runner that executes real checks against a live project and records what it saw.

Statuses: pass | fail | warn | skip   (warn = works but worth attention, e.g. flaky, slow, known gap)
A failed test is re-run once: if it passes the second time it is reported as `warn` (flaky) so it can never hide.
Nothing destructive runs by default; every test cleans up what it creates (marker: aimtest).
"""
import asyncio
import json
import re
import time
import uuid

import httpx

PASS, FAIL, WARN, SKIP = "pass", "fail", "warn", "skip"

LAYERS = [
    ("static", "Static checks", "Every source file compiles, config parses, no secrets in code or URLs"),
    ("smoke", "Smoke", "Every service answers /health fast; models, databases and dashboard pages are present"),
    ("contract", "API contract", "Every route in each service's OpenAPI: a valid call works, bad input is rejected cleanly, never a 5xx"),
    ("data", "Data layer", "PostgreSQL, Qdrant and Redis agree; full create, search and delete round trips"),
    ("bus", "Event bus", "Publishing on Redis channels triggers the right reaction in the right service"),
    ("flows", "End-to-end flows", "Real journeys across several services: voice, memory, browsing, research, projects"),
    ("security", "Security", "Tokens enforced, no secrets leaked, hostile and oversized input handled"),
    ("perf", "Performance", "Latency percentiles and concurrency on the main read and write paths"),
    ("resilience", "Resilience", "A service is stopped on purpose: the map must turn it red within seconds, everything else must keep working, and it must recover by itself"),
    ("ui", "UI", "Every button, link and input on the dashboard pages exercised in a real browser"),
    ("links", "Link proof", "Every connection on the map carried real traffic during this run and none is failing"),
]


class TestFail(Exception):
    def __init__(self, msg, expect=None, actual=None):
        super().__init__(msg)
        self.expect, self.actual = expect, actual


class TestWarn(TestFail):
    pass


class TestSkip(Exception):
    pass


def check(cond, msg, expect=None, actual=None):
    if not cond:
        raise TestFail(msg, expect, actual)


class Resp:
    def __init__(self, status, text, ms, headers):
        self.status, self.text, self.ms, self.headers = status, text, ms, headers

    def json(self):
        return json.loads(self.text)


class Ctx:
    def __init__(self, root, ai=True, destructive=False):
        self.root, self.ai, self.destructive = root, ai, destructive
        self.shared = {}
        self.cleanups = []          # async callables run at the end
        self.t0 = time.time()
        self.cur = None

    def note(self, msg):
        """Attach a one-line observation to the running test (shown even when it passes)."""
        if self.cur is not None:
            self.cur["detail"] = str(msg)

    async def call(self, method, url, timeout=30, **kw):
        t = time.time()
        try:
            async with httpx.AsyncClient(timeout=timeout) as c:
                r = await c.request(method, url, **kw)
            return Resp(r.status_code, r.text, int((time.time() - t) * 1000), r.headers)
        except Exception as e:
            raise TestFail(f"{method} {url.split('?')[0]} could not be reached: {type(e).__name__}", "a response", type(e).__name__)


def T(layer, name, fn, timeout=60, ai=False, unsafe=False, id=None):
    return {"id": id or f"{layer}:{name}", "layer": layer, "name": name, "fn": fn, "timeout": timeout, "ai": ai, "unsafe": unsafe,
            "status": "pending", "ms": None, "detail": "", "expect": None, "actual": None, "flaky": False}


async def _exec(t, ctx):
    t0 = time.time()
    ctx.cur = t
    t["detail"] = ""
    try:
        await asyncio.wait_for(t["fn"](ctx), timeout=t["timeout"])
        t["status"] = PASS
    except TestSkip as e:
        t["status"], t["detail"] = SKIP, str(e)
    except TestWarn as e:
        t["status"], t["detail"], t["expect"], t["actual"] = WARN, str(e), e.expect, e.actual
    except TestFail as e:
        t["status"], t["detail"], t["expect"], t["actual"] = FAIL, str(e), e.expect, e.actual
    except asyncio.TimeoutError:
        t["status"], t["detail"] = FAIL, f"timed out after {t['timeout']}s"
    except Exception as e:
        t["status"], t["detail"] = FAIL, f"{type(e).__name__}: {str(e)[:200]}"
    t["ms"] = int((time.time() - t0) * 1000)


async def run_tests(tests, ctx, progress=None, retry=True):
    for t in tests:
        if progress:
            progress(t, "start")
        if t["unsafe"] and not ctx.destructive:
            t["status"], t["detail"], t["ms"] = SKIP, "destructive - only runs with 'destructive' enabled", 0
        elif t["ai"] and not ctx.ai:
            t["status"], t["detail"], t["ms"] = SKIP, "uses AI quota - only runs with 'include AI calls' enabled", 0
        else:
            await _exec(t, ctx)
            if t["status"] == FAIL and retry:
                first = t["detail"]
                await asyncio.sleep(1.0)
                await _exec(t, ctx)
                if t["status"] == PASS:
                    t["status"], t["flaky"], t["detail"] = WARN, True, f"flaky: failed once ({first}) then passed on retry"
        if progress:
            progress(t, "end")
    for fn in reversed(ctx.cleanups):
        try:
            await fn()
        except Exception:
            pass


def summarize(layers):
    c = {PASS: 0, FAIL: 0, WARN: 0, SKIP: 0}
    ms = 0
    for l in layers:
        for t in l["tests"]:
            if t["status"] in c:
                c[t["status"]] += 1
            ms += t.get("ms") or 0
    total = sum(c.values())
    ran = c[PASS] + c[FAIL] + c[WARN]
    verdict = "FAIL" if c[FAIL] else "PASS WITH WARNINGS" if c[WARN] else "PASS"
    return {**c, "total": total, "ran": ran, "pct": round(100 * (c[PASS] + c[WARN]) / ran) if ran else 0, "verdict": verdict, "ms": ms}


# ═════════════════ OpenAPI -> contract tests (generic: works for any FastAPI service) ═════════════════
def _resolve(spec, node):
    seen = 0
    while isinstance(node, dict) and "$ref" in node and seen < 10:
        cur = spec
        for part in node["$ref"].lstrip("#/").split("/"):
            cur = cur.get(part, {})
        node, seen = cur, seen + 1
    return node


def sample(spec, schema, depth=0):
    schema = _resolve(spec, schema or {})
    if depth > 5 or not isinstance(schema, dict):
        return None
    for k in ("example", "default"):
        if schema.get(k) is not None:
            return schema[k]
    if schema.get("enum"):
        return schema["enum"][0]
    for k in ("anyOf", "oneOf", "allOf"):
        if schema.get(k):
            opts = [o for o in schema[k] if _resolve(spec, o).get("type") != "null"]
            return sample(spec, opts[0], depth + 1) if opts else None
    t = schema.get("type")
    if t == "string":
        f = schema.get("format")
        return str(uuid.uuid4()) if f == "uuid" else "2026-01-01T00:00:00" if f == "date-time" else "aimtest"
    if t in ("integer", "number"):
        return max(1, schema.get("minimum", 1))
    if t == "boolean":
        return False
    if t == "array":
        return [sample(spec, schema.get("items"), depth + 1)]
    if t == "object" or "properties" in schema:
        req = schema.get("required", [])
        return {k: sample(spec, v, depth + 1) for k, v in schema.get("properties", {}).items() if k in req}
    return None


def operations(spec):
    for path, item in (spec.get("paths") or {}).items():
        for method, op in item.items():
            if method.lower() in ("get", "post", "put", "patch", "delete"):
                yield path, method.upper(), op


def build_request(spec, path, op):
    """-> (url_path, query, json_body | None, multipart: bool, has_path_params)"""
    params = op.get("parameters", [])
    query, has_pp, p = {}, False, path
    for prm in params:
        prm = _resolve(spec, prm)
        v = sample(spec, prm.get("schema"))
        if prm.get("in") == "path":
            has_pp = True
            p = p.replace("{" + prm["name"] + "}", str(uuid.uuid4()) if (prm.get("schema") or {}).get("format") == "uuid" else str(uuid.uuid4()))
        elif prm.get("in") == "query" and prm.get("required"):
            query[prm["name"]] = v
    body, multipart = None, False
    rb = _resolve(spec, op.get("requestBody") or {})
    content = rb.get("content", {}) if rb else {}
    if "application/json" in content:
        body = sample(spec, content["application/json"].get("schema"))
    elif content:
        multipart = True
    return p, query, body, multipart, has_pp


def has_required_body(spec, op):
    rb = _resolve(spec, op.get("requestBody") or {})
    sch = _resolve(spec, ((rb.get("content") or {}).get("application/json") or {}).get("schema") or {})
    return bool(sch.get("required"))
