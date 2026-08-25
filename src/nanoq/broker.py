"""Threaded TCP broker for NanoQ."""

from __future__ import annotations

import socketserver
import threading
import time
from pathlib import Path
from typing import Any, Dict, Optional, Tuple

from .errors import NanoQError
from .protocol import MAX_LINE_BYTES, ProtocolError, decode_request, encode_response
from .store import SQLiteStore


class QueueService:
    def __init__(self, store: SQLiteStore, visibility_timeout: float = 30.0) -> None:
        self.store = store
        self.visibility_timeout = visibility_timeout
        self._available = threading.Condition()

    def dispatch(self, request: Dict[str, Any]) -> Dict[str, Any]:
        operation = request.get("op")
        try:
            if operation == "put":
                if "queue" not in request or "data" not in request:
                    raise ProtocolError("missing_field")
                message_id = self.store.put(request["queue"], request["data"])
                with self._available:
                    self._available.notify_all()
                return {"ok": True, "id": message_id}
            if operation == "get":
                if "queue" not in request:
                    raise ProtocolError("missing_field")
                timeout = request.get("timeout")
                if timeout is not None and (
                    isinstance(timeout, bool) or not isinstance(timeout, (int, float)) or timeout < 0
                ):
                    raise ProtocolError("invalid_timeout")
                message = self._blocking_get(request["queue"], timeout)
                if message is None:
                    return {"ok": True, "message": None}
                return {
                    "ok": True,
                    "message": {
                        "id": message.id,
                        "queue": message.queue,
                        "data": message.data,
                        "attempts": message.attempts,
                    },
                }
            if operation in ("ack", "nack"):
                message_id = request.get("id")
                if not isinstance(message_id, str) or not message_id:
                    raise ProtocolError("missing_field")
                changed = getattr(self.store, operation)(message_id)
                if operation == "nack" and changed:
                    with self._available:
                        self._available.notify_all()
                return {"ok": True, "changed": changed}
            raise ProtocolError("unknown_operation")
        except ProtocolError:
            raise
        except (NanoQError, ValueError) as error:
            raise ProtocolError("invalid_request") from error

    def _blocking_get(self, queue: str, timeout: Optional[float]) -> Any:
        deadline = None if timeout is None else time.monotonic() + timeout
        while True:
            message = self.store.reserve(queue, self.visibility_timeout)
            if message is not None:
                return message
            if deadline is not None:
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    return None
                wait_for = min(remaining, 0.25)
            else:
                wait_for = 0.25
            with self._available:
                self._available.wait(wait_for)


class NanoQRequestHandler(socketserver.StreamRequestHandler):
    def handle(self) -> None:
        while True:
            line = self.rfile.readline(MAX_LINE_BYTES + 1)
            if not line:
                return
            if len(line) > MAX_LINE_BYTES or not line.endswith(b"\n"):
                self.wfile.write(encode_response({"ok": False, "error": "request_too_large"}))
                return
            try:
                response = self.server.service.dispatch(decode_request(line))  # type: ignore[attr-defined]
            except ProtocolError as error:
                response = {"ok": False, "error": error.code}
            except Exception:
                response = {"ok": False, "error": "internal_error"}
            self.wfile.write(encode_response(response))


class NanoQServer(socketserver.ThreadingTCPServer):
    allow_reuse_address = True
    daemon_threads = True

    def __init__(self, address: Tuple[str, int], service: QueueService) -> None:
        self.service = service
        super().__init__(address, NanoQRequestHandler)


class Broker:
    """Owns the store and TCP server lifecycle."""

    def __init__(
        self,
        host: str = "127.0.0.1",
        port: int = 8765,
        database: str | Path = "db/nanoq.db",
        visibility_timeout: float = 30.0,
    ) -> None:
        database_path = Path(database)
        database_path.parent.mkdir(parents=True, exist_ok=True)
        self.store = SQLiteStore(database_path)
        self.server = NanoQServer((host, port), QueueService(self.store, visibility_timeout))
        self._serving = False

    @property
    def address(self) -> Tuple[str, int]:
        host, port = self.server.server_address
        return str(host), int(port)

    def serve_forever(self) -> None:
        self._serving = True
        try:
            self.server.serve_forever()
        finally:
            self._serving = False

    def close(self) -> None:
        if self._serving:
            self.server.shutdown()
        self.server.server_close()
        self.store.close()

    def __enter__(self) -> "Broker":
        return self

    def __exit__(self, *_: object) -> None:
        self.close()
