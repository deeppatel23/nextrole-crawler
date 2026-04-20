import os
import subprocess
import sys
from http.server import BaseHTTPRequestHandler
from urllib.parse import parse_qs, urlparse


def _truncate(text: str, limit: int = 8000) -> str:
    if len(text) <= limit:
        return text
    return text[: limit - 200] + "\n...\n" + text[-200:]


def _unauthorized(handler: BaseHTTPRequestHandler) -> None:
    handler.send_response(401)
    handler.send_header("Content-Type", "text/plain; charset=utf-8")
    handler.end_headers()
    handler.wfile.write(b"Unauthorized\n")


def _run_crawler() -> tuple[int, str]:
    # Run as a script so that apps/careers_crawler is on sys.path (it relies on that).
    completed = subprocess.run(
        [sys.executable, "apps/careers_crawler/main.py"],
        check=False,
        capture_output=True,
        text=True,
        env=os.environ.copy(),
    )
    combined = ""
    if completed.stdout:
        combined += completed.stdout
    if completed.stderr:
        combined += ("\n" if combined else "") + completed.stderr
    return completed.returncode, _truncate(combined)


class handler(BaseHTTPRequestHandler):
    def do_GET(self):
        cron_secret = os.getenv("CRON_SECRET", "").strip()
        parsed = urlparse(self.path)
        qs = parse_qs(parsed.query or "")
        secret_from_qs = (qs.get("secret") or [""])[0]

        auth_header = (self.headers.get("authorization") or "").strip()
        secret_from_auth = ""
        if auth_header.lower().startswith("bearer "):
            secret_from_auth = auth_header[7:].strip()

        if cron_secret and secret_from_qs != cron_secret and secret_from_auth != cron_secret:
            return _unauthorized(self)

        code, output = _run_crawler()
        status = 200 if code == 0 else 500

        self.send_response(status)
        self.send_header("Content-Type", "text/plain; charset=utf-8")
        self.end_headers()
        self.wfile.write(output.encode("utf-8", errors="replace"))

    def do_POST(self):
        self.send_response(405)
        self.send_header("Content-Type", "text/plain; charset=utf-8")
        self.end_headers()
        self.wfile.write(b"Method Not Allowed\n")

