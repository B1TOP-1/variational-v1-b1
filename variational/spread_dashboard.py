from __future__ import annotations

import json
import errno
import threading
import time
from http import HTTPStatus
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from urllib.parse import parse_qs, urlparse

from variational.spread_store import SpreadStore


class SpreadDashboardServer:
    def __init__(self, store: SpreadStore, host: str, port: int, html_file: Path) -> None:
        self.store = store
        self.host = host
        self.port = port
        self.html_file = html_file
        self._server: ThreadingHTTPServer | None = None
        self._thread: threading.Thread | None = None

    def start(self) -> bool:
        store = self.store
        html_file = self.html_file

        class Handler(BaseHTTPRequestHandler):
            def do_GET(self) -> None:
                parsed = urlparse(self.path)
                if parsed.path in ("/", "/index.html"):
                    self._send_bytes(HTTPStatus.OK, "text/html; charset=utf-8", html_file.read_bytes())
                    return
                if parsed.path == "/api/assets":
                    self._send_json({"assets": store.assets()})
                    return
                if parsed.path == "/api/history":
                    query = parse_qs(parsed.query)
                    asset = str(query.get("asset", [""])[0]).strip().upper()
                    if not asset:
                        self._send_json({"error": "asset is required"}, HTTPStatus.BAD_REQUEST)
                        return
                    window_seconds = self._bounded_number(query, "range", 86400, 60, 31 * 86400)
                    max_points = int(self._bounded_number(query, "points", 1200, 100, 3000))
                    now_ms = int(time.time() * 1000)
                    end_ms = int(self._bounded_number(query, "end", now_ms, 0, now_ms))
                    start_ms = end_ms - int(window_seconds * 1000)
                    stats = {}
                    for seconds, label in ((300, "5m"), (1800, "30m"), (3600, "1h")):
                        stats[label] = {
                            side: dict(zip(("median", "p90", "p10"), store.window_stats(asset, seconds, side)))
                            for side in ("long", "short")
                        }
                    self._send_json({
                        "asset": asset,
                        "rangeSeconds": window_seconds,
                        "windowStartMs": start_ms,
                        "windowEndMs": end_ms,
                        "latest": store.latest(asset),
                        "sampleCount": store.sample_count(asset, window_seconds, end_ms=end_ms),
                        "points": store.history(asset, window_seconds, max_points, end_ms=end_ms),
                        "stats": stats,
                    })
                    return
                self._send_json({"error": "not found"}, HTTPStatus.NOT_FOUND)

            def log_message(self, _format: str, *_args: object) -> None:
                return

            @staticmethod
            def _bounded_number(query: dict[str, list[str]], key: str, default: float, minimum: float, maximum: float) -> float:
                try:
                    value = float(query.get(key, [str(default)])[0])
                except (TypeError, ValueError):
                    value = default
                return min(maximum, max(minimum, value))

            def _send_json(self, payload: object, status: HTTPStatus = HTTPStatus.OK) -> None:
                self._send_bytes(status, "application/json; charset=utf-8", json.dumps(payload, ensure_ascii=False).encode("utf-8"))

            def _send_bytes(self, status: HTTPStatus, content_type: str, body: bytes) -> None:
                self.send_response(status)
                self.send_header("Content-Type", content_type)
                self.send_header("Content-Length", str(len(body)))
                self.send_header("Cache-Control", "no-store")
                self.send_header("X-Content-Type-Options", "nosniff")
                self.end_headers()
                self.wfile.write(body)

        try:
            self._server = ThreadingHTTPServer((self.host, self.port), Handler)
        except OSError as error:
            if error.errno not in (errno.EADDRINUSE, 48, 98):
                raise
            self._server = None
            return False
        self._thread = threading.Thread(target=self._server.serve_forever, name="spread-dashboard", daemon=True)
        self._thread.start()
        return True

    def stop(self) -> None:
        if self._server is not None:
            self._server.shutdown()
            self._server.server_close()
            self._server = None
        if self._thread is not None:
            self._thread.join(timeout=2)
            self._thread = None
