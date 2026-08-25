"""Validation and serialization at NanoQ's data boundary."""

import json
from typing import Any

from .errors import InvalidPayload, InvalidQueueName

MAX_QUEUE_NAME_LENGTH = 255


def validate_queue_name(queue: object) -> str:
    if not isinstance(queue, str):
        raise InvalidQueueName("queue name must be a string")
    if not queue or queue != queue.strip():
        raise InvalidQueueName("queue name must be non-empty without surrounding whitespace")
    if len(queue) > MAX_QUEUE_NAME_LENGTH:
        raise InvalidQueueName(f"queue name must not exceed {MAX_QUEUE_NAME_LENGTH} characters")
    if any(ord(character) < 32 or ord(character) == 127 for character in queue):
        raise InvalidQueueName("queue name must not contain control characters")
    return queue


def encode_payload(data: Any) -> str:
    try:
        return json.dumps(
            data,
            allow_nan=False,
            ensure_ascii=False,
            separators=(",", ":"),
        )
    except (TypeError, ValueError, OverflowError, RecursionError) as error:
        raise InvalidPayload("message body must be a finite JSON-compatible value") from error


def decode_payload(body: str) -> Any:
    return json.loads(body)

