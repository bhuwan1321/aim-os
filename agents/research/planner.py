"""
Starter-graph generator for "New project": name + type + short idea -> planned nodes/edges (all "not built") + checklist.
Deterministic (no LLM, no quota): templates per project type plus add-ons picked from keywords in the idea.
"""
import re


def N(id, name, layer, kind, purpose):
    return {"id": id, "name": name, "kind": kind, "layer": layer, "parent": None, "path": None, "size": 0, "mtime": 0, "lines": 0,
            "purpose": purpose, "imports": [], "exports": [], "routes": [], "status": "planned", "probe": {"mode": "none"}}


def E(a, b, kind, label, **kw):
    return {"id": f"{a}>{b}|{kind}|{label}", "from": a, "to": b, "kind": kind, "label": label, "fromSvc": a, "toSvc": b, **kw}


TEMPLATES = {
    "website": {
        "nodes": [N("pages", "Pages / Layout", "ui", "ui", "HTML pages and layout"), N("styles", "Styles & Assets", "ui", "ui", "CSS, images, fonts"),
                  N("scripts", "Client Scripts", "core", "service", "Interactivity and form handling"),
                  N("forms", "Form Handler", "api", "api", "Receives contact / signup forms"), N("host", "Hosting / CDN", "infra", "infra", "Where the site is served from")],
        "edges": [("pages", "styles", "import", "loads"), ("pages", "scripts", "import", "loads"), ("scripts", "forms", "http", "POST /forms"),
                  ("host", "pages", "file", "serves")],
    },
    "webapp": {
        "nodes": [N("ui", "Frontend UI", "ui", "ui", "Screens and components"), N("api", "Backend API", "api", "api", "REST endpoints"),
                  N("core", "Business Logic", "core", "service", "Domain rules and services"), N("db", "Database", "data", "db", "Persistent data"),
                  N("host", "Hosting / Deploy", "infra", "infra", "Where the app runs")],
        "edges": [("ui", "api", "http", "REST calls"), ("api", "core", "import", "calls"), ("core", "db", "db", "read / write"), ("host", "api", "file", "runs")],
    },
    "microservices": {
        "nodes": [N("ui", "Dashboard UI", "ui", "ui", "Operator interface"), N("gateway", "API Gateway", "api", "api", "Single entry point and routing"),
                  N("svc_a", "Core Service", "core", "service", "Main domain service"), N("worker", "Worker", "core", "service", "Background jobs"),
                  N("bus", "Message Bus", "infra", "infra", "Event backbone (Redis / queue)"), N("db", "Database", "data", "db", "Persistent data")],
        "edges": [("ui", "gateway", "http", "REST calls"), ("gateway", "svc_a", "http", "route requests"), ("svc_a", "bus", "redis", "publish events"),
                  ("bus", "worker", "redis", "consume jobs"), ("svc_a", "db", "db", "read / write"), ("worker", "db", "db", "write results")],
    },
    "app": {
        "nodes": [N("screens", "Screens / UI", "ui", "ui", "User-facing screens"), N("state", "App State", "core", "service", "State management and navigation"),
                  N("client", "API Client", "core", "service", "Talks to the backend"), N("local", "Local Storage", "data", "db", "Offline cache and settings"),
                  N("backend", "Backend API", "api", "api", "Server the app talks to")],
        "edges": [("screens", "state", "import", "binds"), ("state", "client", "import", "calls"), ("client", "backend", "http", "REST calls"), ("state", "local", "db", "cache")],
    },
    "game": {
        "nodes": [N("input", "Input", "ui", "ui", "Keyboard / controller / touch"), N("hud", "HUD & Menus", "ui", "ui", "Interface overlay"),
                  N("loop", "Game Loop", "core", "service", "Update / render cycle"), N("physics", "Physics & Collision", "core", "service", "Movement and hits"),
                  N("render", "Renderer", "core", "service", "Draws the world"), N("audio", "Audio", "core", "service", "Music and effects"),
                  N("assets", "Assets", "data", "db", "Sprites, models, sounds, levels"), N("save", "Save System", "data", "db", "Progress and settings")],
        "edges": [("input", "loop", "import", "events"), ("loop", "physics", "import", "tick"), ("loop", "render", "import", "frame"), ("loop", "audio", "import", "triggers"),
                  ("hud", "loop", "import", "state"), ("render", "assets", "file", "loads"), ("audio", "assets", "file", "loads"), ("loop", "save", "file", "read / write")],
    },
    "cli": {
        "nodes": [N("cli", "CLI Entry", "ui", "ui", "Argument parsing and help"), N("cmds", "Commands", "core", "service", "Command implementations"),
                  N("lib", "Core Library", "core", "service", "Reusable logic"), N("config", "Config / State", "data", "db", "Config files and cache")],
        "edges": [("cli", "cmds", "import", "dispatch"), ("cmds", "lib", "import", "calls"), ("cmds", "config", "file", "read / write")],
    },
    "pipeline": {
        "nodes": [N("ingest", "Ingest", "api", "service", "Pulls data from sources"), N("transform", "Transform", "core", "service", "Clean, join, enrich"),
                  N("store", "Data Store", "data", "db", "Warehouse / files"), N("sched", "Scheduler", "infra", "infra", "Runs the pipeline on time"),
                  N("report", "Reports / Dashboard", "ui", "ui", "Outputs for humans")],
        "edges": [("sched", "ingest", "import", "triggers"), ("ingest", "transform", "import", "raw data"), ("transform", "store", "db", "write"), ("report", "store", "db", "read")],
    },
}

ADDONS = [  # (regex, nodes, edges)
    (r"login|user|account|sign ?(in|up)|auth", [N("auth", "Authentication", "core", "service", "Login, sessions, permissions")],
     [("auth", "db", "db", "users / sessions")], ["api", "gateway", "backend", "cmds"]),
    (r"pay|stripe|checkout|subscription|billing", [N("payments", "Payments", "api", "api", "Checkout and billing provider"),
                                                    N("pay_ext", "Payment Provider", "external", "external", "Stripe / PayPal")],
     [("payments", "pay_ext", "http", "charge / webhook")], ["api", "gateway", "backend"]),
    (r"chat|realtime|real-time|live|websocket|multiplayer", [N("realtime", "Realtime Channel", "api", "api", "WebSocket / push updates")],
     [], ["ui", "screens"]),
    (r"\bai\b|llm|gpt|ml|model|recommend|assistant|chatbot", [N("ai", "AI / Model Service", "core", "service", "Model calls and prompts"),
                                                              N("ai_ext", "Model Provider", "external", "external", "LLM / ML API")],
     [("ai", "ai_ext", "http", "inference calls")], ["api", "gateway", "core", "backend", "cmds", "transform"]),
    (r"e-?mail|notif|newsletter|alert", [N("notify", "Notifications", "core", "service", "Email / push messages")], [], ["api", "gateway", "core", "backend"]),
    (r"search|filter|recommend", [N("search", "Search Index", "data", "db", "Full-text / vector index")], [], ["api", "core", "backend", "gateway"]),
    (r"upload|file|image|video|photo|storage", [N("files", "File Storage", "data", "db", "Uploads and media")], [], ["api", "core", "backend", "gateway"]),
    (r"map|location|gps", [N("maps", "Maps / Location API", "external", "external", "Geocoding and tiles")], [], ["api", "core", "backend", "ui"]),
]

TASKS = ["Write down the goal and requirements", "Create the repository and project structure", "Set up automated tests and CI",
         "Write a README (run, build, deploy)", "Deploy a first working version", "Add monitoring / health checks"]


def plan(name: str, ptype: str, idea: str, approach: dict = None, comparison: dict = None, motive: str = None, novelty: dict = None) -> dict:
    t = TEMPLATES.get(ptype) or TEMPLATES["webapp"]
    nodes = [dict(n, imports=[], exports=[], routes=[]) for n in t["nodes"]]
    ids = {n["id"] for n in nodes}
    edges = [E(*e[:4]) for e in t["edges"]]
    text = f"{name} {idea}".lower()
    for rx, add_nodes, add_edges, attach in ADDONS:
        if not re.search(rx, text):
            continue
        for n in add_nodes:
            if n["id"] not in ids:
                nodes.append(n); ids.add(n["id"])
        for a, b, k, l in add_edges:
            if a in ids and b in ids:
                edges.append(E(a, b, k, l))
        anchor = next((x for x in attach if x in ids), None)
        first = add_nodes[0]["id"]
        if anchor and anchor != first:
            edges.append(E(anchor, first, "http" if first in ("payments", "realtime") else "import", "uses"))
    tasks = [{"id": f"t{i + 1}", "label": lbl} for i, lbl in enumerate(TASKS)]
    for i, q in enumerate(((comparison or {}).get("questions") or [])[:3]):
        tasks.append({"id": f"q{i + 1}", "label": f"Decide: {str(q)[:110]}"})
    return {"project": name, "type": ptype, "goal": idea, "nodes": nodes, "edges": edges, "tests": {}, "tasks": tasks,
            "approach": approach or None, "comparison": comparison or None, "motive": motive or None, "novelty": novelty or None}
