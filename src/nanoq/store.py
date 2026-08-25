"""Persistent SQLite storage for NanoQ.

The store intentionally has no blocking-wait behavior. The future broker owns
consumer waiting and notifications; this module owns durable queue semantics.
"""

from __future__ import annotations

import math
import sqlite3
import threading
import time
import uuid
from pathlib import Path
from typing import Any, Callable

from .message import StoredMessage
from .validation import decode_payload, encode_payload, validate_queue_name

Clock = Callable[[], float]


class SQLiteStore:
    """A thread-safe, persistent store for named FIFO queues."""

    def __init__(self, database: str | Path, *, clock: Clock = time.time) -> None:
        self._clock = clock
        self._lock = threading.RLock()
        self._closed = False
        self._connection = sqlite3.connect(
            str(database),
            isolation_level=None,
            check_same_thread=False,
            timeout=30.0,
        )
        self._connection.row_factory = sqlite3.Row
        self._connection.execute("PRAGMA journal_mode = WAL")
        self._connection.execute("PRAGMA synchronous = FULL")
        self._initialize()

    def _initialize(self) -> None:
        with self._lock:
            self._connection.executescript(
                """
                CREATE TABLE IF NOT EXISTS messages (
                    seq         INTEGER PRIMARY KEY AUTOINCREMENT,
                    id          TEXT UNIQUE NOT NULL,
                    queue       TEXT NOT NULL,
                    body        TEXT NOT NULL,
                    created_at  REAL NOT NULL,
                    visible_at  REAL NOT NULL,
                    attempts    INTEGER NOT NULL DEFAULT 0
                );
                CREATE INDEX IF NOT EXISTS idx_messages_queue_visible
                ON messages(queue, visible_at, seq);
                """
            )

    def put(self, queue: str, data: Any) -> str:
        queue = validate_queue_name(queue)
        body = encode_payload(data)
        message_id = uuid.uuid4().hex
        now = self._clock()
        with self._lock:
            self._ensure_open()
            self._connection.execute(
                """INSERT INTO messages(id, queue, body, created_at, visible_at)
                   VALUES (?, ?, ?, ?, ?)""",
                (message_id, queue, body, now, now),
            )
        return message_id

    def reserve(self, queue: str, visibility_timeout: float) -> StoredMessage | None:
        queue = validate_queue_name(queue)
        if isinstance(visibility_timeout, bool) or not isinstance(visibility_timeout, (int, float)):
            raise ValueError("visibility timeout must be a non-negative number")
        if visibility_timeout < 0 or not math.isfinite(visibility_timeout):
            raise ValueError("visibility timeout must be a finite non-negative number")

        now = self._clock()
        with self._lock:
            self._ensure_open()
            self._connection.execute("BEGIN IMMEDIATE")
            try:
                row = self._connection.execute(
                    """SELECT id, queue, body, attempts
                       FROM messages
                       WHERE queue = ? AND visible_at <= ?
                       ORDER BY seq
                       LIMIT 1""",
                    (queue, now),
                ).fetchone()
                if row is None:
                    self._connection.execute("COMMIT")
                    return None
                self._connection.execute(
                    """UPDATE messages
                       SET visible_at = ?, attempts = attempts + 1
                       WHERE id = ?""",
                    (now + visibility_timeout, row["id"]),
                )
                self._connection.execute("COMMIT")
            except BaseException:
                self._connection.execute("ROLLBACK")
                raise

        return StoredMessage(
            id=row["id"],
            queue=row["queue"],
            data=decode_payload(row["body"]),
            attempts=row["attempts"] + 1,
        )

    def ack(self, message_id: str) -> bool:
        with self._lock:
            self._ensure_open()
            cursor = self._connection.execute("DELETE FROM messages WHERE id = ?", (message_id,))
            return cursor.rowcount == 1

    def nack(self, message_id: str) -> bool:
        with self._lock:
            self._ensure_open()
            cursor = self._connection.execute(
                "UPDATE messages SET visible_at = ? WHERE id = ?",
                (self._clock(), message_id),
            )
            return cursor.rowcount == 1

    def close(self) -> None:
        with self._lock:
            if not self._closed:
                self._connection.close()
                self._closed = True

    def _ensure_open(self) -> None:
        if self._closed:
            raise RuntimeError("store is closed")

    def __enter__(self) -> "SQLiteStore":
        self._ensure_open()
        return self

    def __exit__(self, *_: object) -> None:
        self.close()
