"""Heartbeat Manager — keeps orders alive on Polymarket CLOB.

Polymarket has a heartbeat mechanism: if you don't send heartbeats,
all your orders get auto-cancelled (safety feature for disconnects).

This runs a background loop sending heartbeats at regular intervals.
"""
from __future__ import annotations
import asyncio
import logging
import time
import uuid
from typing import Any

log = logging.getLogger("mm.heartbeat")


class HeartbeatManager:
    """Background heartbeat sender for CLOB orders."""

    def __init__(self, clob_client: Any, interval_sec: int = 55):
        """
        Args:
            clob_client: py_clob_client.ClobClient instance
            interval_sec: Seconds between heartbeats (CLOB timeout is ~60s)
        """
        self.client = clob_client
        self.interval = interval_sec
        self._task: asyncio.Task | None = None
        self._running = False
        self._last_heartbeat: float = 0.0
        self._heartbeat_count: int = 0
        self._error_count: int = 0
        self._heartbeat_id: str = str(uuid.uuid4())  # Stable ID for session

    @property
    def is_running(self) -> bool:
        return self._running

    @property
    def last_heartbeat(self) -> float:
        return self._last_heartbeat

    @property
    def stats(self) -> dict:
        return {
            "running": self._running,
            "last_heartbeat": self._last_heartbeat,
            "heartbeat_count": self._heartbeat_count,
            "error_count": self._error_count,
            "interval_sec": self.interval,
        }

    async def _send_heartbeat(self) -> bool:
        """Send a single heartbeat.

        Real ClobClient requires heartbeat_id parameter.
        MockClobClient accepts no args (handled gracefully).
        """
        try:
            is_mock = hasattr(self.client, '_orders')
            if is_mock:
                await asyncio.to_thread(self.client.post_heartbeat)
            else:
                await asyncio.to_thread(
                    self.client.post_heartbeat, self._heartbeat_id
                )
            self._last_heartbeat = time.time()
            self._heartbeat_count += 1
            return True
        except Exception as e:
            self._error_count += 1
            log.warning(f"Heartbeat failed: {e}")
            return False

    async def _loop(self):
        """Background heartbeat loop."""
        log.info(f"Heartbeat loop started (interval={self.interval}s)")
        while self._running:
            await self._send_heartbeat()
            try:
                await asyncio.sleep(self.interval)
            except asyncio.CancelledError:
                break
        log.info("Heartbeat loop stopped")

    def start(self) -> None:
        """Start the heartbeat background loop."""
        if self._running:
            return
        self._running = True
        self._task = asyncio.ensure_future(self._loop())

    async def stop(self) -> None:
        """Stop the heartbeat loop. Orders will auto-cancel after timeout."""
        self._running = False
        if self._task:
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass
            self._task = None
        log.info("Heartbeat stopped — orders will auto-cancel soon")

    def reset(self) -> None:
        """Reset counters."""
        self._heartbeat_count = 0
        self._error_count = 0
        self._last_heartbeat = 0.0
