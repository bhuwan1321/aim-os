"""
AIM Host Bridge - runs directly on your computer (Windows, macOS or Linux),
NOT in Docker. Gives the containers access to the active window, screenshots
and system stats on http://localhost:9999.

Started automatically by aim.py. Manual run:  python host_bridge.py

/screenshot and /context need the header  X-AIM-Token: <AIM_BRIDGE_TOKEN>
(from .env) so other machines on your network cannot read your screen.
"""
import base64
import json
import os
import platform
import shutil
import subprocess
import sys
import time
from datetime import datetime
from http.server import BaseHTTPRequestHandler, HTTPServer
from io import BytesIO
from socketserver import ThreadingMixIn

try:
    import psutil
    from PIL import Image
    import mss
except ImportError as e:  # aim.py installs these into runtime/venv
    sys.exit(f"Missing dependency: {e.name}. Run:  python aim.py start")

OS = platform.system()          # 'Windows' | 'Darwin' | 'Linux'
PORT = int(os.getenv("AIM_BRIDGE_PORT", 9999))
QUALITY, SCALE, SCREENSHOT_CACHE = 60, 0.5, 5
TOKEN = os.getenv("AIM_BRIDGE_TOKEN", "")

_last = {"img": None, "t": 0.0}

APP_PATTERNS = {
    "jarvis-ui": ["aim os", "jarvis"],
    "chatgpt": ["chatgpt", "chat.openai"],
    "github": ["github.com", "github -"],
    "stackoverflow": ["stack overflow"],
    "localhost": ["localhost:", "127.0.0.1"],
    "youtube": ["youtube"],
    "vscode": ["visual studio code", "code -"],
    "browser": ["chrome", "brave", "firefox", "edge", "safari", "arc"],
    "terminal": ["terminal", "powershell", "cmd", "iterm", "konsole", "gnome-terminal", "warp"],
    "discord": ["discord"], "notion": ["notion"], "docker": ["docker desktop"],
    "figma": ["figma"], "postman": ["postman"], "obsidian": ["obsidian"],
    "spotify": ["spotify"], "explorer": ["file explorer", "windows explorer", "finder", "nautilus", "dolphin"],
    "word": ["microsoft word"], "excel": ["microsoft excel"], "pycharm": ["pycharm"],
    "jupyter": ["jupyter"], "ollama": ["ollama"], "slack": ["slack"], "zoom": ["zoom"],
    "teams": ["microsoft teams"], "notepad": ["notepad", "textedit"], "vlc": ["vlc"],
    "obs": ["obs studio"], "steam": ["steam"], "task-manager": ["task manager", "activity monitor"],
}


def detect_app(title: str, owner: str = "") -> dict:
    text = f"{owner} {title}".lower().strip()
    if not text:
        return {"app": "unknown", "known": True}
    for app, pats in APP_PATTERNS.items():
        if any(p in text for p in pats):
            return {"app": app, "known": True}
    guess = (owner or (title.split(" - ")[-1] if " - " in title else title))[:30].strip()
    return {"app": guess or "unknown", "known": False}


# ── Active window, per OS ─────────────────────────────────────────────
def _win_title() -> tuple:
    import ctypes
    u = ctypes.windll.user32
    hwnd = u.GetForegroundWindow()
    n = u.GetWindowTextLengthW(hwnd)
    buf = ctypes.create_unicode_buffer(n + 1)
    u.GetWindowTextW(hwnd, buf, n + 1)
    return buf.value, ""


def _mac_title() -> tuple:
    script = ('tell application "System Events"\n'
              'set p to first application process whose frontmost is true\n'
              'set n to name of p\n'
              'set t to ""\n'
              'try\nset t to name of front window of p\nend try\n'
              'return n & "|||" & t\nend tell')
    out = subprocess.run(["osascript", "-e", script], capture_output=True, text=True, timeout=3).stdout.strip()
    owner, _, title = out.partition("|||")
    return title, owner


def _linux_title() -> tuple:
    if shutil.which("xdotool"):
        out = subprocess.run(["xdotool", "getactivewindow", "getwindowname"],
                             capture_output=True, text=True, timeout=3).stdout.strip()
        return out, ""
    return "", ""


def get_active_window() -> dict:
    now = datetime.now().isoformat()
    try:
        title, owner = {"Windows": _win_title, "Darwin": _mac_title}.get(OS, _linux_title)()
        info = detect_app(title, owner)
        return {"app": info["app"], "known": info["known"], "title": title or owner, "timestamp": now}
    except Exception as e:
        return {"app": "unknown", "title": "", "known": True, "error": str(e), "timestamp": now}


# ── Screenshot (mss works on all three OSes) ──────────────────────────
def take_screenshot() -> str:
    now = time.time()
    if _last["img"] and now - _last["t"] < SCREENSHOT_CACHE:
        return _last["img"]
    try:
        with mss.mss() as sct:
            shot = sct.grab(sct.monitors[1])
            img = Image.frombytes("RGB", shot.size, shot.bgra, "raw", "BGRX")
        img = img.resize((int(img.width * SCALE), int(img.height * SCALE)), Image.LANCZOS)
        buf = BytesIO()
        img.save(buf, format="JPEG", quality=QUALITY, optimize=True)
        _last["img"], _last["t"] = base64.b64encode(buf.getvalue()).decode(), now
        return _last["img"]
    except Exception as e:
        print(f"  Screenshot error: {e}")
        return ""


# ── Stats / processes ─────────────────────────────────────────────────
def get_system_stats() -> dict:
    try:
        mem = psutil.virtual_memory()
        disk = psutil.disk_usage(os.path.abspath(os.sep))
        return {
            "cpu_percent": psutil.cpu_percent(interval=0.1),
            "ram_used_gb": round(mem.used / 1e9, 1), "ram_total_gb": round(mem.total / 1e9, 1),
            "ram_percent": mem.percent,
            "disk_used_gb": round(disk.used / 1e9, 1), "disk_total_gb": round(disk.total / 1e9, 1),
            "disk_percent": disk.percent, "os": OS,
        }
    except Exception as e:
        return {"error": str(e)}


def get_processes() -> list:
    procs = []
    for p in psutil.process_iter(["name", "cpu_percent", "memory_percent"]):
        try:
            if p.info["cpu_percent"] and p.info["cpu_percent"] > 0.1:
                procs.append({"name": p.info["name"], "cpu": round(p.info["cpu_percent"], 1),
                              "memory": round(p.info["memory_percent"], 1)})
        except Exception:
            pass
    return sorted(procs, key=lambda x: x["cpu"], reverse=True)[:10]


# ── HTTP ──────────────────────────────────────────────────────────────
PROTECTED = {"/screenshot", "/context"}


class Handler(BaseHTTPRequestHandler):
    def log_message(self, *a):
        pass

    def _cors(self):
        self.send_header("Access-Control-Allow-Origin", "http://localhost:8888")
        self.send_header("Access-Control-Allow-Methods", "GET, OPTIONS")
        self.send_header("Access-Control-Allow-Headers", "Content-Type, X-AIM-Token")

    def do_OPTIONS(self):
        self.send_response(200); self._cors(); self.end_headers()

    def do_GET(self):
        path = self.path.split("?")[0]
        if path in PROTECTED and TOKEN and self.headers.get("X-AIM-Token") != TOKEN:
            return self.send_json({"error": "unauthorized"}, 401)
        now = datetime.now().isoformat()
        if path == "/health":
            return self.send_json({"status": "ok", "service": "host-bridge", "os": OS, "port": PORT, "timestamp": now})
        if path == "/active-window":
            return self.send_json(get_active_window())
        if path == "/system-stats":
            return self.send_json(get_system_stats())
        if path == "/processes":
            return self.send_json({"processes": get_processes()})
        if path == "/screenshot":
            img = take_screenshot()
            return self.send_json({"image_b64": img, "timestamp": now} if img else {"error": "Screenshot failed"},
                                  200 if img else 500)
        if path == "/context":
            return self.send_json({"window": get_active_window(), "stats": get_system_stats(),
                                   "image_b64": take_screenshot(), "timestamp": now})
        self.send_json({"error": "not found"}, 404)

    def send_json(self, data, code=200):
        try:
            body = json.dumps(data).encode()
            self.send_response(code)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(body)))
            self._cors(); self.end_headers(); self.wfile.write(body)
        except (BrokenPipeError, ConnectionResetError, ConnectionAbortedError):
            pass


class Server(ThreadingMixIn, HTTPServer):
    daemon_threads = True


if __name__ == "__main__":
    print(f"AIM Host Bridge [{OS}] on port {PORT}  (Ctrl+C to stop)")
    try:
        Server(("0.0.0.0", PORT), Handler).serve_forever()
    except KeyboardInterrupt:
        print("Host Bridge stopped.")
