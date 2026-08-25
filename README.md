# NanoQ

NanoQ is a lightweight persistent FIFO message queue implemented using only
the Python standard library.

The project is intentionally small. NanoQ is a queue, not a messaging
platform: it provides named queues, JSON-compatible payloads, acknowledgements,
negative acknowledgements, visibility timeouts, and at-least-once delivery.

## Status

The SQLite storage engine and its public contracts are implemented. The TCP
broker and Python network client are the next milestones.

## Core semantics

- Queue names are non-empty strings of at most 255 characters. Leading or
  trailing whitespace and control characters are rejected.
- Message bodies must be JSON-compatible. NaN and infinity are rejected.
- FIFO order is determined by the database sequence number, not timestamps.
- Reserving a message hides it for the requested visibility timeout and
  increments its delivery-attempt count.
- `ack()` permanently deletes a message. `nack()` makes it immediately
  available again. Both return `False` when the ID does not exist.
- An expired reservation becomes eligible for delivery again automatically.
- Delivery is at least once; consumers must tolerate duplicate processing.

## Development

The runtime has no third-party dependencies. Run the tests with the standard
library:

```bash
PYTHONPATH=src python -m unittest discover -s tests -v
```
