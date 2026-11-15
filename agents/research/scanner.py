"""
AIM project scanner: turns a project folder into a graph of real structure.

Detects the stack (docker-compose, package.json, requirements.txt, Unity/Godot, ...), then extracts
files, services, routes, imports, HTTP calls, Redis channels, DB usage and persisted folders.
Output: {"project","type","stack","nodes","edges","tests","scanned_at"}.

Node  = {id,name,kind,layer,parent,path,size,mtime,lines,purpose,imports,exports,routes,probe,status}
Edge  = {id,from,to,kind,label,method,path,channel,fromSvc,toSvc}
Layers (left to right): ui, api, core, data, infra, external
"""
import json, os, re, time

try:
    import yaml
except ImportError:  # scanner still works without compose parsing
    yaml = None

SKIP_DIRS = {"node_modules", ".git", "__pycache__", "venv", ".venv", "dist", "build", "data", "runtime",
             "target", ".idea", ".vscode", "site-packages", "bin", "obj", "Library", "Temp", "logs", "coverage"}
EXT = {".py": "python", ".js": "javascript", ".mjs": "javascript", ".jsx": "javascript", ".ts": "typescript",
       ".tsx": "typescript", ".html": "html", ".go": "go", ".rs": "rust", ".java": "java", ".cs": "csharp",
       ".gd": "gdscript", ".php": "php", ".rb": "ruby", ".sh": "shell"}
MAX_FILE = 400_000
DB_IMAGES = {"postgres": "PostgreSQL", "mysql": "MySQL", "mariadb": "MariaDB", "mongo": "MongoDB",
             "qdrant": "Qdrant", "redis": "Redis", "rabbitmq": "RabbitMQ", "kafka": "Kafka", "elasticsearch": "Elasticsearch",
             "ollama": "Ollama", "nginx": "Nginx", "minio": "MinIO"}
DATA_KINDS = {"postgres", "mysql", "mariadb", "mongo", "qdrant", "elasticsearch", "minio"}
INFRA_PROBE = {"qdrant": "/readyz", "ollama": "/api/tags"}

PY_IMP = re.compile(r"^\s*(?:from\s+([\w.]+)\s+import|import\s+([\w.]+))", re.M)
JS_IMP = re.compile(r"""(?:import\s[^'"]*?from\s*|import\s*|require\(\s*)['"](\.{1,2}/[^'"]+)['"]""")
ROUTE = re.compile(r"""@\w+\.(get|post|put|delete|patch)\(\s*['"]([^'"]+)['"]""")
EXPRESS = re.compile(r"""\b(?:app|router)\.(get|post|put|delete|patch)\(\s*['"]([^'"]+)['"]""")
PY_CALL = re.compile(r"""\.(get|post|put|delete|patch|request)\(\s*(f?)(["'])(.+?)\3""")
JS_FETCH = re.compile(r"""fetch\(\s*(`[^`]*`|'[^']*'|"[^"]*"|[A-Za-z_][\w.]*\s*\+\s*['"][^'"]*['"])""")
ASSIGN_URL = re.compile(r"""(\w+)\s*(?:=|:)\s*(?:os\.getenv\(\s*['"]\w+['"]\s*,\s*)?(?:f?['"])(https?://[^'"]+)['"]""")
URL_HOST = re.compile(r"https?://([\w.\-]+)(?::(\d+))?(/[^\s'\"`)]*)?")
PUBLISH = re.compile(r"""\.publish\(\s*f?['"]([\w.:\-]+)['"]""")
SUBSCRIBE = re.compile(r"""\.p?subscribe\(([^)]*)\)""", re.S)
STR = re.compile(r"""['"]([\w.:\-*]+\.[\w.:\-*]+)['"]""")
SQL_W = re.compile(r"\b(INSERT\s+INTO|UPDATE\s+\w+\s+SET|DELETE\s+FROM|CREATE\s+TABLE|\.upsert\(|\.upload_points)", re.I)
SQL_R = re.compile(r"\b(SELECT\s|\.fetch\(|\.fetchrow\(|\.search\(|\.scroll\()", re.I)
_I = r"^\s*(?:import|from)\s+"
_R = r"(?:require\(\s*|from\s+)['\"]"
DB_HINT = {"postgres": re.compile(_I + r"(?:asyncpg|psycopg2?|sqlalchemy)|" + _R + r"(?:pg|postgres)['\"]", re.M),
           "qdrant": re.compile(_I + r"qdrant_client|" + _R + r"@qdrant", re.M),
           "redis": re.compile(_I + r"(?:redis|aioredis)|" + _R + r"(?:redis|ioredis)['\"]", re.M),
           "mongo": re.compile(_I + r"(?:pymongo|motor)|" + _R + r"(?:mongodb|mongoose)['\"]", re.M),
           "mysql": re.compile(_I + r"(?:pymysql|mysql)|" + _R + r"mysql2?['\"]", re.M),
           "sqlite": re.compile(_I + r"(?:sqlite3|aiosqlite)|" + _R + r"(?:sqlite3|better-sqlite3)['\"]", re.M)}


def _read(path):
    try:
        if os.path.getsize(path) > MAX_FILE:
            return ""
        with open(path, encoding="utf-8", errors="ignore") as f:
            return f.read()
    except OSError:
        return ""


def _ignore_rules(root):
    rules = []
    p = os.path.join(root, ".aimignore")
    if os.path.exists(p):
        for line in _read(p).splitlines():
            line = line.strip().replace("\\", "/")
            if line and not line.startswith("#"):
                rules.append(line.rstrip("/"))
    return rules


def _ignored(rel, rules):
    rel = rel.replace("\\", "/")
    return any(rel == r or rel.startswith(r + "/") for r in rules)


def detect_stack(root):
    names = set(os.listdir(root))
    stack = []
    if names & {"docker-compose.yml", "docker-compose.yaml", "compose.yaml"}: stack.append("docker-compose")
    elif "Dockerfile" in names: stack.append("docker")
    if "package.json" in names: stack.append("node")
    if names & {"requirements.txt", "pyproject.toml", "setup.py", "Pipfile"}: stack.append("python")
    if "go.mod" in names: stack.append("go")
    if "Cargo.toml" in names: stack.append("rust")
    if "pom.xml" in names or "build.gradle" in names: stack.append("java")
    if "project.godot" in names: stack.append("godot")
    if "ProjectSettings" in names or "Assets" in names: stack.append("unity")
    if "index.html" in names and not stack: stack.append("static-web")
    if any(n.endswith(".ino") for n in names) or "platformio.ini" in names: stack.append("arduino")
    if "package.xml" in names: stack.append("ros")
    return stack


def _project_type(stack):
    if "docker-compose" in stack: return "microservices"
    for s in ("unity", "godot"):
        if s in stack: return "game"
    if "node" in stack: return "web/node"
    if "python" in stack: return "python"
    if "static-web" in stack: return "website"
    return stack[0] if stack else "unknown"


def _layer_for(rel, ext, routes):
    r = rel.replace("\\", "/").lower()
    if ext == ".html" or any(p in r.split("/") for p in ("ui", "frontend", "client", "public", "static", "components", "pages", "aim-ui")) \
            or r.startswith("apps/"):
        return "ui"
    if r.startswith("core/") or "/core/" in r or "/lib/" in r or "/utils/" in r or "/models/" in r:
        return "core"
    if routes or r.startswith(("services/", "agents/", "api/", "server/", "backend/", "routes/")):
        return "api"
    return "core"


def _compose(root):
    for n in ("docker-compose.yml", "docker-compose.yaml", "compose.yaml"):
        p = os.path.join(root, n)
        if os.path.exists(p) and yaml:
            try:
                return yaml.safe_load(_read(p)) or {}
            except Exception:
                return {}
    return {}


def _ports(spec):
    out = []
    for p in spec.get("ports", []) or []:
        s = str(p).split("/")[0].replace('"', "")
        parts = s.split(":")
        try:
            out.append((int(parts[-2]) if len(parts) >= 2 else int(parts[-1]), int(parts[-1])))
        except ValueError:
            pass
    return out  # [(host, container)]


def _env_urls(spec):
    env = spec.get("environment") or {}
    if isinstance(env, list):
        env = dict(e.split("=", 1) for e in env if "=" in e)
    return {k: str(v) for k, v in env.items() if isinstance(v, (str, int)) and "http" in str(v)}


def _template_to_path(s):
    s = re.sub(r"\{[^}]*\}", "{x}", s)
    return s.split("?")[0]


def scan(root, project_name=None):
    root = os.path.abspath(root)
    rules = _ignore_rules(root)
    stack = detect_stack(root)
    comp = _compose(root)
    services = comp.get("services", {}) or {}

    svc_ctx = {}   # service -> relative build context
    svc_info = {}
    port_to_svc = {}
    for name, spec in services.items():
        spec = spec or {}
        build = spec.get("build")
        ctx = build.get("context") if isinstance(build, dict) else build
        ctx = os.path.normpath(ctx).replace("\\", "/") if isinstance(ctx, str) else None
        if ctx == ".": ctx = ""
        image = str(spec.get("image", ""))
        ports = _ports(spec)
        svc_info[name] = {"ctx": ctx, "image": image, "ports": ports, "depends": list((spec.get("depends_on") or {}).keys()
                          if isinstance(spec.get("depends_on"), dict) else spec.get("depends_on") or []),
                          "volumes": spec.get("volumes") or [], "env_urls": _env_urls(spec), "container": spec.get("container_name", name)}
        if ctx is not None: svc_ctx[name] = ctx
        for hp, cp in ports:
            port_to_svc[hp] = name
        # internal port reachable only through service name; remember it too for name:port lookups
    nodes, edges, seen_edges = {}, [], set()

    def add_node(n):
        nodes.setdefault(n["id"], n)
        return nodes[n["id"]]

    def add_edge(frm, to, kind, label="", **kw):
        if frm == to: return
        eid = f"{frm}>{to}|{kind}|{label}"
        if eid in seen_edges: return
        seen_edges.add(eid)
        edges.append({"id": eid, "from": frm, "to": to, "kind": kind, "label": label, **kw})

    # ── files ──
    files = []
    for d, dirs, fs in os.walk(root):
        reld = os.path.relpath(d, root).replace("\\", "/")
        dirs[:] = [x for x in dirs if x not in SKIP_DIRS and not x.startswith(".")
                   and not _ignored(("" if reld == "." else reld + "/") + x, rules)]
        for f in fs:
            ext = os.path.splitext(f)[1].lower()
            if ext in EXT:
                rel = (f if reld == "." else reld + "/" + f)
                if not _ignored(rel, rules):
                    files.append((rel, ext))

    def service_of(rel):
        best, blen = None, -1
        for name, ctx in svc_ctx.items():
            if ctx == "" and (rel.count("/") == 0):
                if blen < 0: best, blen = name, 0
            elif ctx and (rel == ctx or rel.startswith(ctx + "/")) and len(ctx) > blen:
                best, blen = name, len(ctx)
        return best

    texts, file_routes, file_urls = {}, {}, {}
    for rel, ext in files:
        full = os.path.join(root, rel)
        text = _read(full)
        texts[rel] = text
        routes = [(m, p) for m, p in ROUTE.findall(text)] + ([(m, p) for m, p in EXPRESS.findall(text)] if ext in (".js", ".ts", ".mjs") else [])
        file_routes[rel] = [(m.upper(), p) for m, p in routes]
        st = os.stat(full)
        svc = service_of(rel)
        exports = re.findall(r"^(?:async\s+)?(?:def|class)\s+([A-Za-z]\w+)", text, re.M)[:8] if ext == ".py" else \
            re.findall(r"(?:export\s+(?:default\s+)?(?:async\s+)?(?:function|class|const)\s+|^function\s+)([A-Za-z]\w+)", text, re.M)[:8]
        first = ""
        m = re.match(r'\s*(?:#!.*\n)?\s*(?:"""|\'\'\')(.+?)(?:"""|\'\'\')', text, re.S)
        if m: first = m.group(1).strip().split("\n")[0]
        elif text.lstrip().startswith(("//", "#")): first = text.lstrip().split("\n")[0].lstrip("/# ").strip()
        title = re.search(r"<title>(.*?)</title>", text, re.S) if ext == ".html" else None
        if title: first = title.group(1).strip()
        n = add_node({"id": rel, "name": os.path.basename(rel), "kind": "file", "layer": _layer_for(rel, ext, routes),
                      "parent": f"svc:{svc}" if svc else None, "path": rel, "size": st.st_size,
                      "mtime": int(st.st_mtime), "lines": text.count("\n") + 1, "purpose": first[:160],
                      "imports": [], "exports": exports, "routes": [f"{m} {p}" for m, p in file_routes[rel]],
                      "lang": EXT[ext], "status": "built"})

    # ── infra / service nodes from compose ──
    code_services = set(svc_ctx.keys())
    for name, info in svc_info.items():
        img = info["image"].split("/")[-1].split(":")[0].lower()
        kind_key = next((k for k in DB_IMAGES if k in img), None)
        host_port = info["ports"][0][0] if info["ports"] else None
        cport = info["ports"][0][1] if info["ports"] else None
        if info["ctx"] is not None and name in code_services:
            member = [r for r, _ in files if service_of(r) == name]
            routes_all = [p for r in member for _, p in file_routes[r]]
            health = next((p for p in ("/health", "/healthz", "/readyz") if p in routes_all), None)
            layers = [nodes[r]["layer"] for r in member]
            layer = ("ui" if layers and all(l == "ui" for l in layers) else
                     "core" if info["ctx"].startswith("core") else "api")
            if any(r.startswith("agents") for r in [info["ctx"]]) or info["ctx"].startswith(("services", "agents")): layer = "api"
            add_node({"id": f"svc:{name}", "name": name, "kind": "service", "layer": layer, "parent": None, "path": info["ctx"] or ".",
                      "files": member, "size": sum(nodes[r]["size"] for r in member), "mtime": max([nodes[r]["mtime"] for r in member] or [0]),
                      "lines": sum(nodes[r]["lines"] for r in member), "purpose": f"{name} service" + (f" on :{host_port}" if host_port else ""),
                      "imports": [], "exports": [], "routes": routes_all[:40], "status": "built",
                      "probe": {"service": name, "port": cport, "hostPort": host_port, "path": health or ("/" if cport else None),
                                "mode": "http" if cport else "none"}})
        else:
            label = DB_IMAGES.get(kind_key, name)
            layer = "data" if kind_key in DATA_KINDS else "infra"
            tcp = kind_key in ("redis", "postgres", "mysql", "mariadb", "mongo", "rabbitmq", "kafka")
            add_node({"id": f"svc:{name}", "name": label if label.lower() == name.lower() or not kind_key else f"{label} ({name})",
                      "job": str(services[name].get("restart", "")).strip("\"'") == "no", "kind": "db" if layer == "data" else "infra",
                      "layer": layer, "parent": None, "path": info["image"] or name, "size": 0, "mtime": 0, "lines": 0,
                      "purpose": f"{info['image']} container" + (f" (:{host_port or cport})" if cport else ""), "imports": [], "exports": [],
                      "routes": [], "status": "built",
                      "probe": {"service": name, "port": cport or (6379 if kind_key == "redis" else 5432 if kind_key == "postgres" else None),
                                "hostPort": host_port, "path": INFRA_PROBE.get(kind_key), "mode": "tcp" if tcp or not INFRA_PROBE.get(kind_key) else "http"},
                      "dbkind": kind_key})

    # ── name resolution for URLs ──
    def resolve_host(host, port):
        """URL host:port -> compose service name, or None."""
        if host in services: return host
        if host in ("localhost", "127.0.0.1", "host.docker.internal") and port:
            if int(port) in port_to_svc:
                return port_to_svc[int(port)]
            owners = [n for n, i in svc_info.items() if any(cp == int(port) for _, cp in i["ports"])]
            if len(owners) == 1:
                return owners[0]
        return None

    def target_node(svc, method, path, host, port):
        """Find file implementing the route in the service, else the service node."""
        if svc and f"svc:{svc}" in nodes:
            members = nodes[f"svc:{svc}"].get("files", [])
            for r in members:
                for m, p in file_routes.get(r, []):
                    pat = "^" + re.sub(r"\\\{[^}]*\\\}", "[^/]+", re.escape(p)) + "$"
                    if re.match(pat, path or "") and (m == method or not method):
                        return r, svc
            return f"svc:{svc}", svc
        # unknown local/external host
        ext = host not in ("localhost", "127.0.0.1", "host.docker.internal") and "." in host
        tid = f"ext:{host}" if ext else f"local:{host}:{port or ''}"
        add_node({"id": tid, "name": host + (f":{port}" if port else ""), "kind": "external" if ext else "service",
                  "layer": "external" if ext else "infra", "parent": None, "path": host, "size": 0, "mtime": 0, "lines": 0,
                  "purpose": "external API" if ext else "local endpoint outside the project", "imports": [], "exports": [], "routes": [],
                  "status": "built", "probe": {"service": None, "host": host, "port": int(port) if port else None,
                                               "path": "/health" if not ext else None, "mode": "http" if (port and not ext) else "none"}})
        return tid, None

    # ── per-file edges ──
    by_stem = {}
    for rel, _ in files:
        by_stem.setdefault(os.path.splitext(os.path.basename(rel))[0], []).append(rel)
    channels_pub, channels_sub = {}, {}
    for rel, ext in files:
        text = texts[rel]
        me_svc = nodes[rel]["parent"].replace("svc:", "") if nodes[rel]["parent"] else None
        # imports
        imps = set()
        if ext == ".py":
            for a, b in PY_IMP.findall(text):
                mod = (a or b).split(".")[-1]
                if mod in by_stem and mod not in ("main",):
                    imps.update(t for t in by_stem[mod] if t != rel and (nodes[t]["parent"] == nodes[rel]["parent"]))
        elif ext in (".js", ".ts", ".mjs", ".jsx", ".tsx"):
            for p in JS_IMP.findall(text):
                stem = os.path.splitext(os.path.basename(p))[0]
                imps.update(t for t in by_stem.get(stem, []) if t != rel)
        for t in sorted(imps):
            nodes[rel]["imports"].append(t)
            add_edge(rel, t, "import", "import")
        # url constants
        consts = {}
        for name, url in ASSIGN_URL.findall(text):
            consts[name] = url
        # http calls (python)
        if ext == ".py":
            for method, isf, q, body in PY_CALL.findall(text):
                m = re.match(r"\{(\w+)\}(/[^\s'\"]*)?", body) if isf else None
                url = None
                if m and m.group(1) in consts: url = consts[m.group(1)].rstrip("/") + (m.group(2) or "")
                elif body.startswith("http"): url = body
                if not url: continue
                uh = URL_HOST.match(re.sub(r"\{[^}]*\}", "x", url) if "://" in url else url)
                if not uh: continue
                host, port, path = uh.group(1), uh.group(2), _template_to_path(uh.group(3) or "/")
                _add_http(rel, me_svc, method.upper() if method != "request" else "GET", host, port, path, resolve_host, target_node, add_edge, nodes, svc_info)
        else:  # js / html fetches
            for arg in JS_FETCH.findall(text):
                url = None
                m = re.match(r"[`'\"]?\$\{(\w+)\.(\w+)\}(/[^`'\"]*)?", arg)
                c = re.match(r"(\w+)\s*\+\s*['\"]([^'\"]*)['\"]", arg)
                lit = re.match(r"[`'\"](https?://[^`'\"]+)[`'\"]", arg)
                if m:
                    base = re.search(rf"\b{m.group(2)}\s*:\s*['\"](https?://[^'\"]+)['\"]", text)
                    if base: url = base.group(1).rstrip("/") + (m.group(3) or "")
                elif c and c.group(1) in consts: url = consts[c.group(1)].rstrip("/") + c.group(2)
                elif lit: url = lit.group(1)
                if not url: continue
                idx = text.find(arg)
                mm = re.search(r"method\s*:\s*['\"](\w+)['\"]", text[idx: idx + 260])
                uh = URL_HOST.match(re.sub(r"\$\{[^}]*\}", "x", url))
                if not uh: continue
                _add_http(rel, me_svc, (mm.group(1).upper() if mm else "GET"), uh.group(1), uh.group(2), _template_to_path(uh.group(3) or "/"),
                          resolve_host, target_node, add_edge, nodes, svc_info)
        # redis channels
        for ch in PUBLISH.findall(text): channels_pub.setdefault(ch, set()).add(rel)
        for blob in SUBSCRIBE.findall(text):
            for ch in STR.findall(blob): channels_sub.setdefault(ch, set()).add(rel)
        # db usage
        for key, rx in DB_HINT.items():
            if ext == ".html" or not rx.search(text): continue
            target = next((f"svc:{n}" for n in svc_info if key in svc_info[n]["image"].lower()), None)
            if not target:
                if key == "redis": continue
                target = f"db:{key}"
                add_node({"id": target, "name": key.capitalize(), "kind": "db", "layer": "data", "parent": None, "path": key, "size": 0, "mtime": 0,
                          "lines": 0, "purpose": f"{key} usage detected in code", "imports": [], "exports": [], "routes": [], "status": "built",
                          "probe": {"mode": "none"}, "dbkind": key})
            write, read = bool(SQL_W.search(text)), bool(SQL_R.search(text))
            if key == "redis":
                if not (channels_pub or channels_sub) and "get(" not in text: continue
                add_edge(rel, target, "db", "kv / cache", fromSvc=me_svc, toSvc=target.replace("svc:", ""))
                continue
            label = "read + write" if write and read else "write" if write else "read" if read else "connects"
            add_edge(rel, target, "db", label, fromSvc=me_svc, toSvc=target.replace("svc:", ""))
    for ch, pubs in channels_pub.items():
        for p in pubs:
            for s in channels_sub.get(ch, ()):
                if p != s:
                    add_edge(p, s, "redis", ch, channel=ch, fromSvc=nodes[p]["parent"] and nodes[p]["parent"][4:],
                             toSvc=nodes[s]["parent"] and nodes[s]["parent"][4:])
    # channels published but nobody subscribes in this project / subscribed but never published: keep as dangling edge to redis
    redis_id = next((i for i, n in nodes.items() if n.get("dbkind") == "redis"), None)
    for ch, pubs in channels_pub.items():
        if ch not in channels_sub and redis_id:
            for p in pubs:
                add_edge(p, redis_id, "redis", ch, channel=ch, fromSvc=nodes[p]["parent"] and nodes[p]["parent"][4:], toSvc=redis_id[4:], dangling=True)
    for ch, subs in channels_sub.items():
        if ch not in channels_pub and redis_id:
            for s in subs:
                add_edge(redis_id, s, "redis", ch, channel=ch, fromSvc=redis_id[4:], toSvc=nodes[s]["parent"] and nodes[s]["parent"][4:], dangling=True)

    # ── compose structure: depends_on, env urls, volumes ──
    for name, info in svc_info.items():
        me = f"svc:{name}"
        for dep in info["depends"]:
            if f"svc:{dep}" in nodes: add_edge(me, f"svc:{dep}", "import", "depends_on", fromSvc=name, toSvc=dep)
        for vol in info["volumes"]:
            v = str(vol if not isinstance(vol, dict) else vol.get("source", "") + ":" + vol.get("target", "")).replace("\\", "/")
            parts = v.split(":")
            if parts and parts[-1] in ("ro", "rw", "z", "Z"): parts.pop()
            if len(parts) < 2: continue
            target = parts.pop()
            src = ":".join(parts)
            src = re.sub(r"\$\{[^}:]*:-([^}]*)\}", r"\1", src)
            src = re.sub(r"\$\{[^}]*\}", "", src)
            if not src or src in (".", "./") or "docker.sock" in src or target.startswith("/docker-entrypoint-initdb.d") or src.endswith((".env", ".sql")): continue
            vid = "vol:" + os.path.normpath(src).replace("\\", "/").lstrip("./")
            add_node({"id": vid, "name": vid[4:], "kind": "storage", "layer": "data", "parent": None, "path": vid[4:], "size": 0, "mtime": 0,
                      "lines": 0, "purpose": "persisted data folder / volume", "imports": [], "exports": [], "routes": [], "status": "built",
                      "probe": {"mode": "none"}})
            add_edge(me, vid, "file", "persists data", fromSvc=name, toSvc=None)

    # ── tests: which services are referenced by test files / collections ──
    tests = {}
    test_paths = []
    for d, dirs, fs in os.walk(root):
        dirs[:] = [x for x in dirs if x not in SKIP_DIRS and not x.startswith(".")]
        for f in fs:
            rel = os.path.relpath(os.path.join(d, f), root).replace("\\", "/")
            if re.search(r"(^|/)(tests?|__tests__|spec)(/|$)|\.test\.|_test\.|test_|\.spec\.|postman", rel.lower()) and f.endswith((".py", ".js", ".ts", ".json")):
                test_paths.append(rel)
    for rel in test_paths:
        text = _read(os.path.join(root, rel))
        for name, info in svc_info.items():
            hit = any(f":{hp}" in text for hp, _ in info["ports"]) or re.search(rf"\b{re.escape(name)}\b", text)
            if hit: tests.setdefault(name, []).append(rel)

    # finalize: file-level endpoints for edges that were service-targeted get fromSvc/toSvc
    for e in edges:
        if "fromSvc" not in e:
            e["fromSvc"] = nodes[e["from"]]["parent"][4:] if nodes.get(e["from"], {}).get("parent") else (e["from"][4:] if e["from"].startswith("svc:") else None)
        if "toSvc" not in e:
            e["toSvc"] = nodes[e["to"]]["parent"][4:] if nodes.get(e["to"], {}).get("parent") else (e["to"][4:] if e["to"].startswith("svc:") else None)
    # drop services nobody references and without files? keep all compose services.
    return {"project": project_name or os.path.basename(root), "type": _project_type(stack), "stack": stack,
            "nodes": list(nodes.values()), "edges": edges, "tests": tests, "files_scanned": len(files),
            "has_readme": any(f.lower().startswith("readme") for f in os.listdir(root)), "scanned_at": int(time.time())}


def _add_http(rel, me_svc, method, host, port, path, resolve_host, target_node, add_edge, nodes, svc_info):
    if host in ("x", "") or "{" in host:
        return          # f-string placeholder, not a real endpoint
    svc = resolve_host(host, port)
    tid, tsvc = target_node(svc, method, path, host, port)
    if me_svc and tsvc == me_svc: return
    add_edge(rel, tid, "http", f"{method} {path}", method=method, path=path, fromSvc=me_svc, toSvc=tsvc)


if __name__ == "__main__":
    import argparse
    ap = argparse.ArgumentParser(description="AIM project scanner")
    ap.add_argument("--path", default=".")
    ap.add_argument("--out", default="")
    a = ap.parse_args()
    if not os.path.isdir(a.path):
        raise SystemExit(f"Not a folder: {a.path}")
    res = scan(a.path)
    if a.out:
        with open(a.out, "w", encoding="utf-8") as f:
            json.dump(res, f, indent=2)
        print(f"{res['project']}: {res['files_scanned']} files, {len(res['nodes'])} nodes, {len(res['edges'])} edges -> {a.out}")
    else:
        print(json.dumps(res, indent=2))
