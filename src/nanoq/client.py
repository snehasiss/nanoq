"""Small synchronous Python client for NanoQ."""

from __future__ import annotations

import json
import socket
from typing import Any, Dict, Optional

from .message import Message


class NanoQ:
    def __init__(self, host: str = "127.0.0.1", port: int = 8765) -> None:
        self.host = host
        self.port = port

    def _request(self, request: Dict[str, Any], socket_timeout: Optional[float] = 10.0) -> Dict[str, Any]:
        payload = (json.dumps(request, allow_nan=False, separators=(",", ":")) + "\n").encode("utf-8")
        with socket.create_connection((self.host, self.port), timeout=socket_timeout) as connection:
            connection.settimeout(socket_timeout)
            connection.sendall(payload)
            response_file = connection.makefile("rb")
            line = response_file.readline()
        if not line:
            raise ConnectionError("broker closed the connection without a response")
        response = json.loads(line.decode("utf-8"))
        if not response.get("ok"):
            raise RuntimeError("NanoQ protocol error: " + str(response.get("error", "unknown")))
        return response

    def put(self, queue: str, data: Any) -> str:
        return str(self._request({"op": "put", "queue": queue, "data": data})["id"])

    def get(self, queue: str, timeout: Optional[float] = None) -> Optional[Message]:
        network_timeout = None if timeout is None else timeout + 5.0
        response = self._request({"op": "get", "queue": queue, "timeout": timeout}, network_timeout)
        value = response["message"]
        if value is None:
            return None
        return Message(
            id=value["id"],
            queue=value["queue"],
            data=value["data"],
            attempts=value["attempts"],
            _acknowledge=self.ack,
            _negative_acknowledge=self.nack,
        )

    def ack(self, message_id: str) -> bool:
        return bool(self._request({"op": "ack", "id": message_id})["changed"])

    def nack(self, message_id: str) -> bool:
        return bool(self._request({"op": "nack", "id": message_id})["changed"])

    def close(self) -> None:
        """Present for API symmetry; this client does not retain connections."""

    def __enter__(self) -> "NanoQ":
        return self

    def __exit__(self, *_: object) -> None:
        self.close()
