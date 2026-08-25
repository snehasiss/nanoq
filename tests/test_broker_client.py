import tempfile
import threading
import time
import unittest
from pathlib import Path

from nanoq import NanoQ
from nanoq.broker import Broker


class BrokerClientTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temporary_directory = tempfile.TemporaryDirectory()
        self.broker = Broker(port=0, database=Path(self.temporary_directory.name) / "nanoq.db")
        self.thread = threading.Thread(target=self.broker.serve_forever, daemon=True)
        self.thread.start()
        host, port = self.broker.address
        self.client = NanoQ(host, port)

    def tearDown(self) -> None:
        self.broker.close()
        self.thread.join(timeout=2)
        self.temporary_directory.cleanup()

    def test_put_get_and_ack(self) -> None:
        message_id = self.client.put("events", {"kind": "ready"})
        message = self.client.get("events", timeout=1)
        self.assertIsNotNone(message)
        assert message is not None
        self.assertEqual(message.id, message_id)
        self.assertEqual(message.data, {"kind": "ready"})
        self.assertTrue(message.ack())
        self.assertIsNone(self.client.get("events", timeout=0.01))

    def test_nack_redelivers(self) -> None:
        self.client.put("jobs", "retry")
        first = self.client.get("jobs", timeout=1)
        assert first is not None
        self.assertTrue(first.nack())
        second = self.client.get("jobs", timeout=1)
        assert second is not None
        self.assertEqual(second.id, first.id)
        self.assertEqual(second.attempts, 2)

    def test_blocking_get_wakes_for_put(self) -> None:
        result = []

        def consume() -> None:
            result.append(self.client.get("later", timeout=2))

        consumer = threading.Thread(target=consume)
        consumer.start()
        time.sleep(0.05)
        self.client.put("later", 42)
        consumer.join(timeout=2)
        self.assertEqual(result[0].data, 42)


if __name__ == "__main__":
    unittest.main()
