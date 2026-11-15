"""
AIM Host Bridge - runs directly on your computer (Windows, macOS or Linux), NOT in Docker.
It only reports system stats (CPU, RAM, disk) to the dashboard on http://localhost:9999.
No screenshots, no window titles, no process lists: the screen agent is gone and so is everything it needed.

Started automatically by aim.py. Manual run:  python tools/host_bridge.py
"""
import json
import os
import platform
import sys
from datetime import datetime
from http.server import BaseHTTPRequestHandler, HTTPServer
from socketserver import ThreadingMixIn

try:
    import psutil
except ImportError as e:  # aim.py installs it into runtime/venv
    sys.exit(f"Missing dependency: {e.name}. Run:  python aim.py start")

OS = platform.system()
PORT = int(os.getenv("AIM_BRIDGE_PORT", 9999))
ORIGIN = "http://localhost:8888"


def get_system_stats() -> dict:
    try:
        mem = psutil.virtual_memory()
        disk = psutil.disk_usage(os.path.abspath(os.sep))
        return {"cpu_percent": psutil.cpu_percent(interval=0.1), "ram_used_gb": round(mem.used / 1e9, 1), "ram_total_gb": round(mem.total / 1e9, 1),
                "ram_percent": mem.percent, "disk_used_gb": round(disk.used / 1e9, 1), "disk_total_gb": round(disk.total / 1e9, 1),
                "disk_percent": disk.percent, "os": OS}
    except Exception as e:
        return {"error": str(e)}


class Handler(BaseHTTPRequestHandler):
    def log_message(self, *a):
        pass

    def _cors(self):
        self.send_header("Access-Control-Allow-Origin", ORIGIN)
        self.send_header("Access-Control-Allow-Methods", "GET, OPTIONS")
        self.send_header("Access-Control-Allow-Headers", "Content-Type")

    def do_OPTIONS(self):
        self.send_response(200); self._cors(); self.end_headers()

    def do_GET(self):
        path = self.path.split("?")[0]
        if path == "/health":
            return self.send_json({"status": "ok", "service": "host-bridge", "os": OS, "port": PORT, "timestamp": datetime.now().isoformat()})
        if path == "/system-stats":
            return self.send_json(get_system_stats())
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
        Server(("127.0.0.1", PORT), Handler).serve_forever()     # localhost only
    except KeyboardInterrupt:
        print("Host Bridge stopped.")
