"""Message value objects used by the storage layer."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any


@dataclass(frozen=True)
class StoredMessage:
    """A message reserved for a consumer."""

    id: str
    queue: str
    data: Any
    attempts: int
