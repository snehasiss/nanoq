"""Message value objects used by the storage layer."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Callable


@dataclass
class StoredMessage:
    """A message reserved for a consumer."""

    id: str
    queue: str
    data: Any
    attempts: int


@dataclass(frozen=True)
class Message:
    """A message returned by the network client."""

    id: str
    queue: str
    data: Any
    attempts: int
    _acknowledge: Callable[[str], bool]
    _negative_acknowledge: Callable[[str], bool]

    def ack(self) -> bool:
        return self._acknowledge(self.id)

    def nack(self) -> bool:
        return self._negative_acknowledge(self.id)
