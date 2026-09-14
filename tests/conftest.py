import gzip
import json
import socket
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

import pytest


@pytest.fixture(autouse=True)
def isolated_spool(tmp_path, monkeypatch):
    monkeypatch.setenv("FEED_SPOOL_DIR", str(tmp_path / "spool"))


class _Handler(BaseHTTPRequestHandler):
    def log_message(self, *args):  # silence
        pass

    def _respond(self, code, body: bytes):
        self.send_response(code)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def do_GET(self):
        if self.path.endswith("/blacklist"):
            if not self.server.available:
                self._respond(503, b"{}")
                return
            with self.server.lock:
                self.server.auth_headers.append(self.headers.get("Authorization"))
            body = json.dumps({"rules": self.server.blacklist_rules}).encode()
            self._respond(200, body)
        else:
            self._respond(404, b"{}")

    def do_POST(self):
        if self.path.endswith("/telemetry"):
            length = int(self.headers.get("Content-Length", 0))
            raw = self.rfile.read(length)
            if self.headers.get("Content-Encoding") == "gzip":
                raw = gzip.decompress(raw)
            batch = json.loads(raw)
            with self.server.lock:
                self.server.attempts.append(batch)
                self.server.request_paths.append(self.path)
                response_status = (
                    self.server.response_statuses.pop(0)
                    if self.server.response_statuses
                    else 200
                )
            if not self.server.available:
                self._respond(503, b"{}")
                return
            if self.server.responder is not None:
                response_status = self.server.responder(self.path, batch)
            if response_status != 200:
                self._respond(response_status, b"{}")
                return
            max_events = self.server.max_events_per_request
            if max_events is not None and len(batch.get("events", [])) > max_events:
                self._respond(413, b"{}")
                return
            with self.server.lock:
                self.server.received.append(batch)
                self.server.auth_headers.append(self.headers.get("Authorization"))
                disconnect = self.server.disconnect_after_accept
                self.server.disconnect_after_accept = False
            if disconnect:
                self.close_connection = True
                self.connection.shutdown(socket.SHUT_RDWR)
                self.connection.close()
                return
            n = len(batch.get("events", []))
            self._respond(200, json.dumps({"ingested": n, "dropped": 0}).encode())
        else:
            self._respond(404, b"{}")


@pytest.fixture
def mock_server():
    server = ThreadingHTTPServer(("127.0.0.1", 0), _Handler)
    server.received = []
    server.attempts = []
    server.response_statuses = []
    server.blacklist_rules = []
    server.auth_headers = []
    server.max_events_per_request = None
    server.available = True
    server.responder = None
    server.request_paths = []
    server.disconnect_after_accept = False
    server.lock = threading.Lock()
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    host, port = server.server_address
    yield f"http://{host}:{port}", server
    server.shutdown()
    server.server_close()
