"""NDJSON protocol helpers and request validation."""

from __future__ import annotations

import json
from typing import Any, Dict

MAX_LINE_BYTES = 1024 * 1024


class ProtocolError(ValueError):
    def __init__(self, code: str) -> None:
        super().__init__(code)
        self.code = code


def decode_request(line: bytes) -> Dict[str, Any]:
    try:
        value = json.loads(line.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError) as error:
        raise ProtocolError("invalid_json") from error
    if not isinstance(value, dict):
        raise ProtocolError("invalid_request")
    return value


def encode_response(response: Dict[str, Any]) -> bytes:
    return (json.dumps(response, allow_nan=False, separators=(",", ":")) + "\n").encode("utf-8")
