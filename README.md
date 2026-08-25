# NanoQ

NanoQ is a lightweight persistent FIFO message queue implemented using only
the Python standard library.

The project is intentionally small. NanoQ is a queue, not a messaging
platform: it provides named queues, JSON-compatible payloads, acknowledgements,
negative acknowledgements, visibility timeouts, and at-least-once delivery.

## Start the broker

From the repository root, install the package in editable mode and start it:

```bash
python -m pip install -e .
python -m nanoq
```

By default NanoQ listens on `127.0.0.1:8765` and stores messages in
`db/nanoq.db`. The directory and database are created automatically. Override
the defaults when needed:

```bash
python -m nanoq --host 127.0.0.1 --port 8765 --db db/nanoq.db
```

Stop the broker with Ctrl-C. NanoQ defaults to localhost and is not intended
for direct public-internet exposure.

## Test producer and consumer behavior

NanoQ is a work queue, not broadcast pub/sub: one `put` publishes work to a
named queue and one consumer reserves each delivery. Start the broker, then in
a second terminal publish a message:

```bash
python - <<'PY'
from nanoq import NanoQ

q = NanoQ()
message_id = q.put("demo", {"text": "hello from NanoQ"})
print("published", message_id)
PY
```

In a third terminal consume and acknowledge it:

```bash
python - <<'PY'
from nanoq import NanoQ

q = NanoQ()
message = q.get("demo", timeout=10)
if message is None:
    print("no message received")
else:
    print("received", message.id, message.data, "attempt", message.attempts)
    message.ack()
PY
```

Running the consumer again should return no message because ACK permanently
removed it. Replace `message.ack()` with `message.nack()` to verify immediate
redelivery; the next delivery will have an incremented `attempts` value.

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
