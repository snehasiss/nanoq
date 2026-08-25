from __future__ import annotations

import concurrent.futures
import math
import tempfile
import threading
import unittest
from pathlib import Path

from nanoq.store import SQLiteStore


class FakeClock:
    def __init__(self, now: float = 1_000.0) -> None:
        self.now = now

    def __call__(self) -> float:
        return self.now


class StoreTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temporary_directory = tempfile.TemporaryDirectory()
        self.database = Path(self.temporary_directory.name) / "nanoq.db"
        self.clock = FakeClock()
        self.store = SQLiteStore(self.database, clock=self.clock)

    def tearDown(self) -> None:
        self.store.close()
        self.temporary_directory.cleanup()

    def test_put_and_reserve_preserve_fifo(self) -> None:
        first_id = self.store.put("orders", {"number": 1})
        second_id = self.store.put("orders", {"number": 2})

        first = self.store.reserve("orders", 30)
        self.assertIsNotNone(first)
        assert first is not None
        self.assertEqual((first.id, first.data, first.attempts), (first_id, {"number": 1}, 1))
        self.store.ack(first.id)
        second = self.store.reserve("orders", 30)
        self.assertIsNotNone(second)
        assert second is not None
        self.assertEqual(second.id, second_id)

    def test_queues_are_isolated(self) -> None:
        self.store.put("alpha", 1)
        beta_id = self.store.put("beta", 2)
        message = self.store.reserve("beta", 30)
        self.assertIsNotNone(message)
        assert message is not None
        self.assertEqual(message.id, beta_id)

    def test_ack_deletes_and_unknown_ack_is_false(self) -> None:
        message_id = self.store.put("jobs", "work")
        self.assertTrue(self.store.ack(message_id))
        self.assertFalse(self.store.ack(message_id))
        self.assertIsNone(self.store.reserve("jobs", 30))

    def test_nack_requeues_immediately(self) -> None:
        self.store.put("jobs", "work")
        first = self.store.reserve("jobs", 30)
        assert first is not None
        self.assertTrue(self.store.nack(first.id))
        second = self.store.reserve("jobs", 30)
        assert second is not None
        self.assertEqual(second.id, first.id)
        self.assertEqual(second.attempts, 2)

    def test_visibility_timeout_recovers_unacknowledged_message(self) -> None:
        self.store.put("jobs", "work")
        first = self.store.reserve("jobs", 30)
        self.assertIsNotNone(first)
        self.assertIsNone(self.store.reserve("jobs", 30))
        self.clock.now += 30
        recovered = self.store.reserve("jobs", 30)
        assert first is not None and recovered is not None
        self.assertEqual(recovered.id, first.id)
        self.assertEqual(recovered.attempts, 2)

    def test_visibility_timeout_must_be_finite_and_non_negative(self) -> None:
        for value in (-1, math.nan, math.inf, "30", True):
            with self.subTest(value=value), self.assertRaises(ValueError):
                self.store.reserve("jobs", value)

    def test_messages_survive_reopen(self) -> None:
        message_id = self.store.put("durable", {"saved": True})
        self.store.close()
        self.store = SQLiteStore(self.database, clock=self.clock)
        message = self.store.reserve("durable", 30)
        assert message is not None
        self.assertEqual(message.id, message_id)

    def test_concurrent_threads_reserve_a_message_only_once(self) -> None:
        message_id = self.store.put("jobs", "only once")
        barrier = threading.Barrier(8)

        def reserve() -> str | None:
            barrier.wait()
            message = self.store.reserve("jobs", 30)
            return message.id if message else None

        with concurrent.futures.ThreadPoolExecutor(max_workers=8) as executor:
            results = list(executor.map(lambda _: reserve(), range(8)))
        self.assertEqual([result for result in results if result is not None], [message_id])


if __name__ == "__main__":
    unittest.main()
