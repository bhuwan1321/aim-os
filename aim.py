#!/usr/bin/env python3
"""
AIM OS launcher - one command on Windows, macOS and Linux (Python 3.8+, stdlib only).

    python aim.py start      start everything and open the dashboard
    python aim.py stop       stop everything
    python aim.py status     health of every service
    python aim.py logs [svc] follow logs
    python aim.py test       run the Postman API smoke tests (needs Postman CLI)
    python aim.py clean      stop and remove containers + images (keeps ./data)

Everything AIM OS needs (databases, AI models, voices, the host-bridge virtualenv)
is stored inside this folder: ./data and ./runtime. Delete the folder and it is gone.
"""
import argparse
import json
import os
import platform
import secrets
import shutil
import subprocess
import sys
import time
import urllib.request
import webbrowser

ROOT = os.path.dirname(os.path.abspath(__file__))
os.chdir(ROOT)
IS_WIN = platform.system() == "Windows"
RUNTIME = os.path.join(ROOT, "runtime")
VENV = os.path.join(RUNTIME, "venv")
VPY = os.path.join(VENV, "Scripts" if IS_WIN else "bin", "python.exe" if IS_WIN else "python")
PIDFILE = os.path.join(RUNTIME, "bridge.pid")
UI_URL = "http://localhost:8888/index.html"

SERVICES = [
    ("Dashboard (JARVIS UI)", "http://localhost:8888/index.html"),
    ("LLM Router", "http://localhost:8001/health"),
    ("Timeline", "http://localhost:8002/health"),
    ("Memory Engine", "http://localhost:8003/health"),
    ("Core Brain", "http://localhost:8004/health"),
    ("Research Agent", "http://localhost:8005/health"),
    ("Notification", "http://localhost:8006/health"),
    ("Browser Agent", "http://localhost:8007/health"),
    ("Voice Agent (Baymax)", "http://localhost:8008/health"),
    ("Ollama (local models)", "http://localhost:11435/api/tags"),
    ("Qdrant", "http://localhost:6333/readyz"),
    ("Host Bridge", "http://localhost:9999/health"),
]


def say(msg=""):
    print(msg, flush=True)


def run(cmd, check=True, capture=False, **kw):
    return subprocess.run(cmd, check=check, text=True, capture_output=capture, **kw)


def http_ok(url, timeout=3):
    try:
        with urllib.request.urlopen(url, timeout=timeout) as r:
            return 200 <= r.status < 400
    except Exception:
        return False


# ── .env ──────────────────────────────────────────────────────────────
def read_env(path):
    out = {}
    if os.path.exists(path):
        for line in open(path, encoding="utf-8-sig"):
            line = line.strip()
            if line and not line.startswith("#") and "=" in line:
                k, v = line.split("=", 1)
                out[k.strip()] = v.strip()
    return out


def ensure_env():
    """Create .env from .env.example (random passwords) or top up an existing one."""
    example, target = os.path.join(ROOT, ".env.example"), os.path.join(ROOT, ".env")
    have = read_env(target)
    lines = open(example, encoding="utf-8").read().splitlines()
    new, added = [], []
    for line in lines:
        if line.strip() and not line.startswith("#") and "=" in line:
            k, v = line.split("=", 1)
            if k in have:
                continue
            if v == "__GENERATE__":
                v = secrets.token_urlsafe(24)
            added.append(f"{k}={v}")
    if not os.path.exists(target):
        text = "\n".join(
            (f"{l.split('=')[0]}={secrets.token_urlsafe(24)}" if l.endswith("=__GENERATE__") else l) for l in lines
        ) + "\n"
        open(target, "w", encoding="utf-8").write(text)
        say("  Created .env with fresh random passwords. Add GROQ_API_KEY / GOOGLE_API_KEY there (optional).")
    elif added:
        with open(target, "a", encoding="utf-8") as f:
            f.write("\n# added by aim.py\n" + "\n".join(added) + "\n")
        say(f"  Added {len(added)} new settings to .env")
    # Older installs pointed at an Ollama on the host; models now run in Docker.
    env = read_env(target)
    if "host.docker.internal:11434" in env.get("OLLAMA_HOST", ""):
        txt = open(target, encoding="utf-8-sig").read().replace(env["OLLAMA_HOST"], "http://ollama:11434")
        open(target, "w", encoding="utf-8").write(txt)
        say("  Switched OLLAMA_HOST to the bundled Ollama container")
    return read_env(target)


# ── Docker ────────────────────────────────────────────────────────────
def docker_up():
    if shutil.which("docker") is None:
        sys.exit("Docker is not installed. Get Docker Desktop (Windows/macOS) or Docker Engine (Linux): https://docs.docker.com/get-docker/")
    if run(["docker", "info"], check=False, capture=True).returncode == 0:
        return
    say("  Docker is not running - trying to start it...")
    try:
        if platform.system() == "Darwin":
            run(["open", "-a", "Docker"], check=False)
        elif IS_WIN:
            for base in (os.environ.get("ProgramFiles", ""), os.environ.get("ProgramW6432", "")):
                exe = os.path.join(base, "Docker", "Docker", "Docker Desktop.exe")
                if os.path.exists(exe):
                    subprocess.Popen([exe]); break
        else:
            say("  Please start the Docker service (e.g. `sudo systemctl start docker`).")
    except Exception:
        pass
    for _ in range(60):
        time.sleep(3)
        if run(["docker", "info"], check=False, capture=True).returncode == 0:
            return
    sys.exit("Docker did not start in time. Start Docker and run again.")


def compose_cmd():
    cmd = ["docker", "compose", "-f", "docker-compose.yml"]
    if shutil.which("nvidia-smi") and os.path.exists("docker-compose.gpu.yml"):
        if run(["nvidia-smi"], check=False, capture=True).returncode == 0 and platform.system() != "Darwin":
            cmd += ["-f", "docker-compose.gpu.yml"]
    return cmd


def compose(*args, **kw):
    return run(compose_cmd() + list(args), **kw)


# ── Host bridge (runs on the real OS, not in Docker) ──────────────────
def ensure_venv():
    marker = os.path.join(RUNTIME, ".bridge-deps-installed")
    if os.path.exists(VPY) and os.path.exists(marker):
        return
    say("  Setting up host-bridge environment in ./runtime (one time)...")
    os.makedirs(RUNTIME, exist_ok=True)
    run([sys.executable, "-m", "venv", VENV])
    env = dict(os.environ, PIP_CACHE_DIR=os.path.join(RUNTIME, "pip-cache"))
    run([VPY, "-m", "pip", "install", "-q", "-r", os.path.join("tools", "bridge-requirements.txt")], env=env)
    open(marker, "w").write("ok")


def bridge_start(env):
    if http_ok("http://localhost:9999/health"):
        return
    ensure_venv()
    log = open(os.path.join(RUNTIME, "bridge.log"), "ab")
    kw = {}
    if IS_WIN:
        kw["creationflags"] = 0x00000008 | 0x00000200  # DETACHED_PROCESS | NEW_PROCESS_GROUP
    else:
        kw["start_new_session"] = True
    p = subprocess.Popen([VPY, os.path.join("agents", "screen", "host_bridge.py")], stdout=log, stderr=log,
                         env=dict(os.environ, AIM_BRIDGE_TOKEN=env.get("AIM_BRIDGE_TOKEN", "")), **kw)
    open(PIDFILE, "w").write(str(p.pid))


def bridge_stop():
    if not os.path.exists(PIDFILE):
        return
    try:
        pid = int(open(PIDFILE).read())
        if IS_WIN:
            run(["taskkill", "/PID", str(pid), "/F", "/T"], check=False, capture=True)
        else:
            os.kill(pid, 15)
    except Exception:
        pass
    os.remove(PIDFILE)


# ── Commands ──────────────────────────────────────────────────────────
def cmd_start(a):
    say("\n  AIM OS - starting\n")
    env = ensure_env()
    os.makedirs(os.path.join(ROOT, env.get("AIM_DATA", "./data").lstrip("./") or "data"), exist_ok=True)
    docker_up()
    say("  [1/3] Starting containers (first run builds images + downloads models; this can take a while)...")
    compose("up", "-d", "--build", "--remove-orphans")
    say("  [2/3] Starting host bridge...")
    try:
        bridge_start(env)
    except Exception as e:
        say(f"  ! Host bridge could not start ({e}). AIM still works; screen awareness is off.")
    say("  [3/3] Waiting for services...")
    deadline = time.time() + 180
    while time.time() < deadline and not http_ok(UI_URL):
        time.sleep(2)
    cmd_status(a)
    say("\n  Local AI models download in the background the first time (about 6 GB).")
    say("  Watch progress:  python aim.py logs ollama-init")
    if not a.no_browser:
        webbrowser.open(UI_URL)
    say(f"\n  JARVIS dashboard: {UI_URL}\n  Stop with:        python aim.py stop\n")


def cmd_stop(_a):
    say("  Stopping AIM OS...")
    bridge_stop()
    if shutil.which("docker"):
        compose("down", "--remove-orphans", check=False)
    say("  Stopped. Your data is kept in ./data")


def cmd_status(_a):
    say("")
    for name, url in SERVICES:
        say(f"  {'OK ' if http_ok(url) else '-- '} {name:<24} {url}")


def cmd_logs(a):
    compose("logs", "-f", "--tail", "100", *([a.service] if a.service else []), check=False)


def cmd_test(a):
    """Full test cycle (static, smoke, API contract, data, bus, flows, security, performance, link proof; --chaos adds resilience)."""
    args = [sys.executable, os.path.join("tests", "run_all.py")]
    if a.no_ai: args.append("--no-ai")
    if a.chaos: args.append("--chaos")
    if a.verbose: args.append("-v")
    rc = run(args, check=False)
    rc = getattr(rc, "returncode", 0)
    say("\n  UI layer: open http://localhost:8888/research.html, select AIM OS, press Test -> Run UI tests.")
    sys.exit(rc)


def cmd_clean(_a):
    bridge_stop()
    compose("down", "--rmi", "all", "--remove-orphans", check=False)
    say("  Containers and images removed. Models and databases are still in ./data (delete that folder to remove them).")


def main():
    p = argparse.ArgumentParser(description="AIM OS launcher")
    sub = p.add_subparsers(dest="cmd")
    s = sub.add_parser("start"); s.add_argument("--no-browser", action="store_true"); s.set_defaults(f=cmd_start)
    sub.add_parser("stop").set_defaults(f=cmd_stop)
    sub.add_parser("status").set_defaults(f=cmd_status)
    l = sub.add_parser("logs"); l.add_argument("service", nargs="?"); l.set_defaults(f=cmd_logs)
    t = sub.add_parser("test", help="run the full test cycle")
    t.add_argument("--no-ai", action="store_true", help="skip tests that use LLMs / web / git (no quota used)")
    t.add_argument("--chaos", action="store_true", help="also stop services on purpose and check recovery (needs Docker CLI)")
    t.add_argument("-v", "--verbose", action="store_true", help="list passing tests too")
    t.set_defaults(f=cmd_test)
    sub.add_parser("clean").set_defaults(f=cmd_clean)
    a = p.parse_args()
    if not a.cmd:
        a = p.parse_args(["start"])
    a.f(a)


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        pass
