"""
Generated tests for ANY scanned project (uploaded folder or cloned repo). Principles:
  - nothing in the project is ever executed; static checks only read files
  - live checks only call services that are ALREADY running (published ports), and by default only read-only GET calls
  - calls that could change the project's data (POST/PUT/PATCH/DELETE, invalid-input probes) exist only when the owner opts in
Every test carries a `kind` so the plan shown for approval says exactly what will be done.
"""
import json
import os
import re
import subprocess

import yaml

from tester import (T, check, TestFail, TestSkip, TestWarn, build_request, has_required_body, operations, _resolve)
from suites_aim import KEY_PATTERNS, SKIP_DIRS, files, rd

K_STATIC, K_GET, K_MUTATE = "reads files only", "read-only GET call to your running service", "changes data: POST/PUT/PATCH/DELETE or invalid-input probe"


def tag(t, kind):
    t["kind"] = kind
    return t


def static_tests(ctx, graph):
    root, out = ctx.root, []
    for p in files(root, {".py"}):
        rel = os.path.relpath(p, root).replace("\\", "/")

        async def fn(ctx, p=p, rel=rel):
            try:
                compile(rd(p), p, "exec")
            except SyntaxError as e:
                raise TestFail(f"syntax error in {rel} line {e.lineno}: {e.msg}", "valid Python", f"line {e.lineno}")
        out.append(tag(T("static", f"compiles: {rel}", fn), K_STATIC))
    for p in files(root, {".json"}):
        rel = os.path.relpath(p, root).replace("\\", "/")
        if "package-lock" in rel or rel.endswith(".min.json"):
            continue

        async def fn(ctx, p=p, rel=rel):
            try:
                json.loads(rd(p) or "null")
            except ValueError as e:
                raise TestFail(f"invalid JSON in {rel}: {str(e)[:80]}")
        out.append(tag(T("static", f"valid JSON: {rel}", fn), K_STATIC))
    for p in files(root, {".yml", ".yaml"}):
        rel = os.path.relpath(p, root).replace("\\", "/")

        async def fn(ctx, p=p, rel=rel):
            try:
                list(yaml.safe_load_all(rd(p)))
            except yaml.YAMLError as e:
                raise TestFail(f"invalid YAML in {rel}: {str(e)[:80]}")
        out.append(tag(T("static", f"valid YAML: {rel}", fn), K_STATIC))
    for p in files(root, {".html"}):
        rel = os.path.relpath(p, root).replace("\\", "/")

        async def fn(ctx, p=p, rel=rel):
            t = rd(p)
            check(len(re.findall(r"<script\b", t)) == len(re.findall(r"</script>", t)), "unbalanced <script> tags")
        out.append(tag(T("static", f"HTML structure: {rel}", fn), K_STATIC))

    async def keyshape(ctx):
        hits = []
        for p in files(root, {".py", ".js", ".ts", ".html", ".yml", ".yaml", ".json", ".md", ".sh", ".txt", ".toml", ".cfg", ".ini", ".properties"}):
            t = rd(p)
            for rx in KEY_PATTERNS:
                if re.search(rx, t):
                    hits.append(os.path.relpath(p, root).replace("\\", "/"))
        check(not hits, f"key-shaped secrets found in: {sorted(set(hits))}", "none", hits)
    out.append(tag(T("static", "no API-key-shaped secrets in source files", keyshape), K_STATIC))

    async def envfiles(ctx):
        found = []
        for d, dirs, fs in os.walk(root):
            dirs[:] = [x for x in dirs if x not in SKIP_DIRS and not x.startswith(".") or x == ".github"]
            for f in fs:
                if re.match(r"^\.env(\..*)?$", f) and f.lower() not in (".env.example", ".env.sample", ".env.template"):
                    found.append(os.path.relpath(os.path.join(d, f), root).replace("\\", "/"))
        if not found:
            return
        tracked = []
        if os.path.isdir(os.path.join(root, ".git")):
            try:
                ls = subprocess.run(["git", "-C", root, "ls-files"], capture_output=True, text=True, timeout=20).stdout.split()
                tracked = [f for f in found if f in ls]
            except Exception:
                pass
        fixture = re.compile(r"(^|/)(tests?|fixtures?|examples?|samples?|docs?)/")
        real = [f for f in tracked if not fixture.search(f)]
        check(not real, f"env files are committed to git: {real}", "never committed", real)
        if tracked:
            raise TestWarn(f"env files committed inside test/example folders (probably fixtures, check they hold no real secrets): {tracked}")
        raise TestWarn(f"env file present: {found} - keep it out of version control and out of zips you share")
    out.append(tag(T("static", "no .env file is committed to git", envfiles), K_STATIC))

    async def readme(ctx):
        names = [f.lower() for f in os.listdir(root)]
        if not any(n.startswith("readme") for n in names):
            raise TestWarn("no README")
    out.append(tag(T("static", "has a README", readme), K_STATIC))

    async def has_tests(ctx):
        if not (graph.get("tests") or any(re.search(r"(^|/)(tests?|__tests__|spec)(/|$)|\.test\.|_test\.|test_", os.path.relpath(p, root).replace("\\", "/").lower()) for p in files(root, {".py", ".js", ".ts"}))):
            raise TestWarn("the project has no tests of its own")
    out.append(tag(T("static", "the project ships its own tests", has_tests), K_STATIC))

    async def pins(ctx):
        miss = []
        for rq in files(root, {".txt"}):
            if os.path.basename(rq) == "requirements.txt":
                for line in rd(rq).splitlines():
                    line = line.strip()
                    if line and not line.startswith(("#", "-")) and "==" not in line:
                        miss.append(f"{os.path.relpath(rq, root)}: {line}")
        pj = os.path.join(root, "package.json")
        if os.path.exists(pj):
            try:
                d = json.loads(rd(pj))
                for sec in ("dependencies", "devDependencies"):
                    for k, v in (d.get(sec) or {}).items():
                        if str(v).startswith(("^", "~", "*", "latest")) or v == "":
                            miss.append(f"package.json {k}: {v}")
            except ValueError:
                pass
        if miss:
            raise TestWarn(f"{len(miss)} unpinned dependencies (builds not reproducible), e.g. {miss[:3]}")
    out.append(tag(T("static", "dependencies are pinned", pins), K_STATIC))
    return out


def services(graph):
    """Running-service candidates from the scan: published ports of compose services."""
    out = []
    for n in graph.get("nodes", []):
        pr = n.get("probe") or {}
        if n["id"].startswith("svc:") and n["kind"] == "service" and pr.get("hostPort"):
            out.append({"name": n["name"], "base": f"http://host.docker.internal:{pr['hostPort']}", "routes": n.get("routes", [])})
    return out


async def live_tests(ctx, graph, mutating):
    smoke, contract = [], []
    ctx.shared["routes"], ctx.shared["routes_tested"] = set(), set()
    svcs = services(graph)
    if not svcs:
        async def none(ctx):
            raise TestSkip("no service with a published port was found in the scan - live checks need a running docker-compose project")
        smoke.append(tag(T("smoke", "running services", none), K_GET))
        return smoke, contract
    for sv in svcs:
        async def up(ctx, sv=sv):
            last = None
            for path in ("/health", "/healthz", "/"):
                try:
                    r = await ctx.call("GET", sv["base"] + path, timeout=8)
                except TestFail as e:
                    last = str(e); continue
                if r.status < 500:
                    ctx.note(f"{path} -> {r.status} in {r.ms} ms")
                    return
                last = f"{path} -> {r.status}"
            raise TestSkip(f"{sv['name']} is not running or not reachable ({last}) - start the project first") if "reached" in (last or "") else TestFail(f"{sv['name']} answers with server errors: {last}")
        smoke.append(tag(T("smoke", f"{sv['name']} is running and answers", up), K_GET))

        try:
            from tester import Ctx  # noqa
            r = await ctx.call("GET", sv["base"] + "/openapi.json", timeout=6)
            spec = r.json() if r.status == 200 else None
        except Exception:
            spec = None
        if spec:
            for path, method, op in operations(spec):
                if path.startswith(("/docs", "/redoc", "/openapi")) or "/stream" in path:
                    continue
                ctx.shared["routes"].add((sv["name"], method, path))
                safe = method == "GET"

                async def pos(ctx, sv=sv, method=method, path=path, op=op, spec=spec):
                    p, q, body, multipart, has_pp = build_request(spec, path, op)
                    if multipart:
                        raise TestSkip("multipart upload route")
                    kw = {}
                    if q: kw["params"] = q
                    if body is not None: kw["json"] = body
                    r = await ctx.call(method, sv["base"] + p, timeout=30, **kw)
                    ctx.shared["routes_tested"].add((sv["name"], method, path))
                    check(r.status < 500, f"server error {r.status}", "status < 500", f"{r.status}: {r.text[:120]}")
                    ctx.note(f"{r.status} in {r.ms} ms")
                if safe or mutating:
                    contract.append(tag(T("contract", f"{method} {sv['name']}{path}", pos), K_GET if safe else K_MUTATE))
                if mutating and method in ("POST", "PUT", "PATCH") and has_required_body(spec, op):
                    async def neg(ctx, sv=sv, method=method, path=path, op=op, spec=spec):
                        p, _, _, _, _ = build_request(spec, path, op)
                        r = await ctx.call(method, sv["base"] + p, json={})
                        check(r.status < 500, f"empty body caused a server error ({r.status})", "4xx", r.status)
                    contract.append(tag(T("contract", f"{method} {sv['name']}{path} survives bad input", neg), K_MUTATE))
        else:
            for n in graph.get("nodes", []):
                if n["kind"] != "file" or n.get("parent") != f"svc:{sv['name']}":
                    continue
                for r_ in n.get("routes", []):
                    m, _, path = r_.partition(" ")
                    if m == "GET" and not re.search(r"[{:<*]", path):
                        ctx.shared["routes"].add((sv["name"], m, path))

                        async def pos2(ctx, sv=sv, path=path):
                            r = await ctx.call("GET", sv["base"] + path, timeout=20)
                            ctx.shared["routes_tested"].add((sv["name"], "GET", path))
                            check(r.status < 500, f"server error {r.status}", "status < 500", r.status)
                            ctx.note(f"{r.status} in {r.ms} ms")
                        contract.append(tag(T("contract", f"GET {sv['name']}{path} (from source scan)", pos2), K_GET))
    return smoke, contract


async def build(ctx, graph, mutating=False):
    smoke, contract = await live_tests(ctx, graph, mutating)
    return {"static": static_tests(ctx, graph), "smoke": smoke, "contract": contract}
