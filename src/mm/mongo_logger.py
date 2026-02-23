"""Async MongoDB logger for fills, snapshots, and Python logs.

All writes go through an asyncio.Queue and are flushed in batches
by a background task every 2 seconds.  If MongoDB is unavailable the
records are silently dropped (fire-and-forget) so trading latency is
never affected.
"""
from __future__ import annotations

import asyncio
import logging
import time
from typing import Optional

from .types import Fill

log = logging.getLogger("mm.mongo")


class MongoLogger:
    """Buffered async writer to MongoDB via motor."""

    def __init__(self, uri: str, db_name: str = "pm_bot"):
        self._uri = uri
        self._db_name = db_name
        self._queue: asyncio.Queue = asyncio.Queue(maxsize=10_000)
        self._client = None  # motor.AsyncIOMotorClient
        self._db = None
        self._running = False
        self._task: Optional[asyncio.Task] = None

    async def start(self) -> None:
        """Connect to MongoDB and launch background flush task."""
        import motor.motor_asyncio

        self._client = motor.motor_asyncio.AsyncIOMotorClient(self._uri)
        self._db = self._client[self._db_name]
        self._running = True
        self._task = asyncio.create_task(self._flush_loop())

        # Create indexes (best-effort, idempotent)
        try:
            await self._db.fills.create_index("ts")
            await self._db.snapshots.create_index("ts")
            await self._db.logs.create_index("ts")
            await self._db.logs.create_index(
                "ts", expireAfterSeconds=30 * 86400, name="ttl_30d"
            )
        except Exception:
            pass  # indexes may already exist or mongo unreachable

        log.info("MongoLogger started → %s/%s", self._uri.split("@")[-1], self._db_name)

    async def stop(self) -> None:
        """Flush remaining records and close the connection."""
        self._running = False
        if self._task:
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass
        await self._flush_batch()
        if self._client:
            self._client.close()
        log.info("MongoLogger stopped")

    # ── background loop ────────────────────────────────────────

    async def _flush_loop(self) -> None:
        while self._running:
            await asyncio.sleep(2.0)
            await self._flush_batch()

    async def _flush_batch(self) -> None:
        batches: dict[str, list] = {"fills": [], "snapshots": [], "logs": []}
        while not self._queue.empty():
            try:
                item = self._queue.get_nowait()
                collection = item.pop("_collection", "logs")
                batches.get(collection, batches["logs"]).append(item)
            except asyncio.QueueEmpty:
                break
        for collection, docs in batches.items():
            if docs:
                try:
                    await self._db[collection].insert_many(docs, ordered=False)
                except Exception:
                    pass  # fire-and-forget

    # ── public helpers ─────────────────────────────────────────

    def log_fill(self, fill: Fill, token_type: str, context: dict) -> None:
        """Enqueue a fill record with market context."""
        doc = {
            "_collection": "fills",
            "ts": fill.ts,
            "side": fill.side,
            "token_type": token_type,
            "token_id": fill.token_id,
            "price": fill.price,
            "size": fill.size,
            "fee": fill.fee,
            "order_id": fill.order_id,
            "is_maker": fill.is_maker,
            "market": context.get("market"),
            "inventory": context.get("inventory"),
            "fair_value": context.get("fair_value"),
            "pnl": context.get("pnl"),
            "paper_mode": context.get("paper_mode"),
        }
        try:
            self._queue.put_nowait(doc)
        except asyncio.QueueFull:
            pass

    def log_snapshot(self, state: dict) -> None:
        """Enqueue a periodic bot-state snapshot."""
        doc = {
            "_collection": "snapshots",
            "ts": time.time(),
            **state,
        }
        try:
            self._queue.put_nowait(doc)
        except asyncio.QueueFull:
            pass


class MongoLogHandler(logging.Handler):
    """Python logging.Handler that forwards records to MongoLogger."""

    def __init__(self, mongo_logger: MongoLogger):
        super().__init__()
        self._dedup: dict[tuple, tuple[float, int]] = {}  # (level, name, msg_prefix) -> (last_ts, suppressed_count)
        self._dedup_maxsize: int = 500
        self._logger = mongo_logger

    def emit(self, record: logging.LogRecord) -> None:
        now = time.time()
        # Dedup key: (level, logger name, first 80 chars of message)
        msg_text = self.format(record)
        key = (record.levelname, record.name, msg_text[:80])

        if key in self._dedup:
            last_ts, suppressed = self._dedup[key]
            if now - last_ts < 5.0:
                self._dedup[key] = (last_ts, suppressed + 1)
                return
            # Cooldown expired — emit with suppression count
            if suppressed > 0:
                msg_text = f"{msg_text} (suppressed {suppressed} similar)"
            self._dedup[key] = (now, 0)
        else:
            # Evict oldest if too many keys
            if len(self._dedup) >= self._dedup_maxsize:
                oldest_key = min(self._dedup, key=lambda k: self._dedup[k][0])
                del self._dedup[oldest_key]
            self._dedup[key] = (now, 0)

        doc = {
            "_collection": "logs",
            "ts": record.created,
            "level": record.levelname,
            "name": record.name,
            "msg": msg_text,
        }
        try:
            self._logger._queue.put_nowait(doc)
        except asyncio.QueueFull:
            pass
