"""Local log-search backend.

This module is launched as a *subprocess* by the FastAPI web app.  It is kept
stdlib-only so that starting it is cheap and it has no import-time dependency
on the web framework.

Readiness protocol: the child binds an ephemeral loopback port, prints one JSON
line ``{"event": "listening", "port": ...}`` on stdout, then answers
``/health`` with 503 until the configured startup delay has elapsed.  The
parent only considers the backend ready once ``/health`` returns 200.

Search endpoint: ``GET /search?q=<text>&limit=<n>``
Runtime config: ``response_delay_ms`` is re-read from the runtime config file on
every request, so it can be changed without restarting the process.
"""

from __future__ import annotations

import argparse
import json
import os
import signal
import sys
import threading
import time
from http import HTTPStatus
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Any
from urllib.parse import parse_qs, urlparse

_READY = threading.Event()
_RUNTIME_CONFIG: dict[str, Any] = {"path": None, "response_delay_ms": 0}


def _load_lines(path: str) -> list[str]:
    try:
        with open(path, "r", encoding="utf-8") as handle:
            return [line.rstrip("\n") for line in handle if line.strip()]
    except OSError:
        return []


def _read_response_delay(default_ms: int) -> int:
    path = _RUNTIME_CONFIG.get("path")
    if not path:
        return default_ms
    try:
        with open(path, "r", encoding="utf-8") as handle:
            payload = json.load(handle)
        value = int(payload.get("response_delay_ms", default_ms))
    except (OSError, ValueError, TypeError, json.JSONDecodeError):
        return default_ms
    return max(0, min(15000, value))


class Handler(BaseHTTPRequestHandler):
    server_version = "SampleLogBackend/1.0"
    protocol_version = "HTTP/1.1"

    def log_message(self, fmt: str, *args: Any) -> None:  # keep child stdout clean
        pass

    def _send_json(self, status: int, payload: dict[str, Any]) -> None:
        body = json.dumps(payload, ensure_ascii=False).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(body)))
        self.send_header("Connection", "keep-alive")
        self.end_headers()
        self.wfile.write(body)

    def do_GET(self) -> None:  # noqa: N802 (stdlib naming)
        parsed = urlparse(self.path)
        if parsed.path == "/health":
            if _READY.is_set():
                self._send_json(HTTPStatus.OK, {"ready": True, "pid": os.getpid()})
            else:
                self._send_json(HTTPStatus.SERVICE_UNAVAILABLE, {"ready": False})
            return
        if parsed.path == "/search":
            self._handle_search(parsed.query)
            return
        self._send_json(HTTPStatus.NOT_FOUND, {"error": "not found"})

    def _handle_search(self, query: str) -> None:
        params = parse_qs(query)
        term = (params.get("q") or [""])[0].strip()
        try:
            limit = int((params.get("limit") or ["50"])[0])
        except ValueError:
            limit = 50
        limit = max(1, min(200, limit))

        delay_ms = _read_response_delay(int(self.server.default_response_delay_ms))
        if delay_ms:
            time.sleep(delay_ms / 1000.0)

        if not _READY.is_set():
            self._send_json(HTTPStatus.SERVICE_UNAVAILABLE, {"error": "not ready"})
            return

        started = time.monotonic()
        lines = _load_lines(self.server.log_file)
        needle = term.lower()
        matches: list[dict[str, Any]] = []
        for index, line in enumerate(lines):
            if not needle or needle in line.lower():
                match: dict[str, Any] = {"line": index + 1, "raw": line}
                try:
                    match["log"] = json.loads(line)
                except json.JSONDecodeError:
                    pass
                matches.append(match)
                if len(matches) >= limit:
                    break

        self._send_json(
            HTTPStatus.OK,
            {
                "query": term,
                "total_scanned": len(lines),
                "count": len(matches),
                "truncated": len(matches) >= limit,
                "results": matches,
                "backend_pid": os.getpid(),
                "backend_elapsed_ms": round((time.monotonic() - started) * 1000, 2),
            },
        )


class BackendServer(ThreadingHTTPServer):
    daemon_threads = True
    allow_reuse_address = True

    def __init__(self, address: tuple[str, int], log_file: str, response_delay_ms: int):
        super().__init__(address, Handler)
        self.log_file = log_file
        self.default_response_delay_ms = response_delay_ms


def _set_ready_later(delay_ms: int) -> None:
    timer = threading.Timer(delay_ms / 1000.0, _READY.set)
    timer.daemon = True
    timer.start()


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--log-file", required=True)
    parser.add_argument("--runtime-config", default="")
    parser.add_argument("--startup-delay-ms", type=int, default=0)
    parser.add_argument("--response-delay-ms", type=int, default=0)
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=0)
    args = parser.parse_args(argv)

    if os.getenv("BACKEND_FAIL_START") == "1":
        print("simulated startup failure", file=sys.stderr, flush=True)
        return 3

    _RUNTIME_CONFIG["path"] = args.runtime_config or None
    _RUNTIME_CONFIG["response_delay_ms"] = args.response_delay_ms

    try:
        server = BackendServer(
            (args.host, args.port), args.log_file, args.response_delay_ms
        )
    except OSError as exc:
        print(f"failed to bind: {exc}", file=sys.stderr, flush=True)
        return 4

    port = server.server_address[1]
    print(
        json.dumps({"event": "listening", "port": port, "pid": os.getpid()}),
        flush=True,
    )
    _set_ready_later(max(0, args.startup_delay_ms))

    def _shutdown(_signum: int, _frame: Any) -> None:
        threading.Thread(target=server.shutdown, daemon=True).start()

    signal.signal(signal.SIGTERM, _shutdown)
    signal.signal(signal.SIGINT, _shutdown)

    try:
        server.serve_forever(poll_interval=0.1)
    finally:
        server.server_close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
