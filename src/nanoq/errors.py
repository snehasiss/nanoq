"""Exceptions raised by NanoQ."""


class NanoQError(Exception):
    """Base class for expected NanoQ errors."""


class InvalidQueueName(NanoQError, ValueError):
    """Raised when a queue name does not satisfy NanoQ's contract."""


class InvalidPayload(NanoQError, ValueError):
    """Raised when a body cannot be represented as strict JSON."""

