"""
AIM UI Server — serves JARVIS on http://localhost:8888
Run: python server.py
"""
import http.server
import socketserver
import os

PORT = 8888
os.chdir(os.path.dirname(os.path.abspath(__file__)))

class Handler(http.server.SimpleHTTPRequestHandler):
    def log_message(self, fmt, *args):
        pass  # silent
    def end_headers(self):
        self.send_header("Access-Control-Allow-Origin", "*")
        self.send_header("Access-Control-Allow-Methods", "GET, POST, OPTIONS")
        self.send_header("Access-Control-Allow-Headers", "Content-Type")
        self.send_header("Cache-Control", "no-cache")
        super().end_headers()

with socketserver.TCPServer(("", PORT), Handler) as httpd:
    print("=" * 40)
    print("  AIM JARVIS UI")
    print(f"  http://localhost:{PORT}/index.html")
    print("  Press Ctrl+C to stop")
    print("=" * 40)
    httpd.serve_forever()