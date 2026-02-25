"""Heartbeat Manager — keeps orders alive on Polymarket CLOB.

Polymarket has a heartbeat mechanism: if you don't send heartbeats,
all your orders get auto-cancelled (safety feature for disconnects).

This runs a background loop sending heartbeats at regular intervals.
"""
from __future__ import annotations
import asyncio
import logging
import re
import time
import uuid
from typing import Any, Callable

log = logging.getLogger("mm.heartbeat")


class HeartbeatManager:
    """Background heartbeat sender for CLOB orders."""

    def __init__(
        self,
        clob_client: Any,
        interval_sec: int = 55,
        on_failure: Callable[[], None] | None = None,
    ):
        """
        Args:
            clob_client: py_clob_client.ClobClient instance
            interval_sec: Seconds between heartbeats (CLOB timeout is ~10s, send every 5s)
            on_failure: Optional callback fired after 3 consecutive heartbeat failures
        """
        self.client = clob_client
        self.interval = interval_sec
        self._on_failure = on_failure
        self._task: asyncio.Task | None = None
        self._running = False
        self._last_heartbeat: float = 0.0
        self._heartbeat_count: int = 0
        self._error_count: int = 0
        self._consecutive_failures: int = 0
        self._id_refresh_count: int = 0  # ID adopted from server (not errors)
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
            "id_refresh_count": self._id_refresh_count,
            "interval_sec": self.interval,
            "heartbeat_id": self._heartbeat_id[:8] + "...",
        }

    def regenerate_id(self) -> str:
        """Force-regenerate heartbeat ID (e.g. after reconnect)."""
        self._heartbeat_id = str(uuid.uuid4())
        self._consecutive_failures = 0
        log.info("Heartbeat ID force-regenerated: %s…", self._heartbeat_id[:8])
        return self._heartbeat_id

    def update_id(self, new_id: str) -> None:
        """Update heartbeat ID from external source (e.g. order response).

        Call this after order placement/cancellation if PM response
        contains a new heartbeat_id. Prevents stale-ID errors on next cycle.
        """
        if new_id and new_id != self._heartbeat_id:
            old = self._heartbeat_id[:8]
            self._heartbeat_id = new_id
            self._id_refresh_count += 1
            log.debug("Heartbeat ID updated externally: %s… → %s…", old, new_id[:8])

    @staticmethod
    def _extract_server_heartbeat_id(error_str: str) -> str | None:
        """Extract heartbeat_id UUID from PM error response string."""
        # Match UUID pattern in 'heartbeat_id': '...' or "heartbeat_id": "..."
        m = re.search(
            r"['\"]?heartbeat_id['\"]?\s*[:=]\s*['\"]?"
            r"([0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12})",
            error_str, re.IGNORECASE,
        )
        return m.group(1) if m else None

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
            self._consecutive_failures = 0
            return True
        except Exception as e:
            self._consecutive_failures += 1
            err_str = str(e)
            err_lower = err_str.lower()
            # PM says our ID is invalid — try to extract the server's active ID
            if "invalid" in err_lower or "not found" in err_lower:
                old_id = self._heartbeat_id[:8]
                server_id = self._extract_server_heartbeat_id(err_str)
                if server_id and server_id != self._heartbeat_id:
                    self._heartbeat_id = server_id
                    log.debug("Heartbeat ID adopted from server: %s… → %s…",
                              old_id, self._heartbeat_id[:8])
                else:
                    self._heartbeat_id = str(uuid.uuid4())
                    log.debug("Heartbeat ID regenerated: %s… → %s…",
                              old_id, self._heartbeat_id[:8])
                # Immediately retry with the new ID
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
                    self._consecutive_failures = 0
                    self._id_refresh_count += 1
                    return True
                except Exception as retry_err:
                    log.warning("Heartbeat retry with new ID also failed: %s", retry_err)
                    self._error_count += 1
                    self._consecutive_failures += 1
                    if self._consecutive_failures >= 3:
                        log.critical(
                            "Heartbeat failed %s times in a row; orders may have been cancelled",
                            self._consecutive_failures,
                        )
                        if self._on_failure:
                            try:
                                self._on_failure()
                            except Exception as cb_err:
                                log.error(f"Heartbeat failure callback error: {cb_err}", exc_info=True)
                    return False
            # Non-ID error — real failure
            self._error_count += 1
            log.warning("Heartbeat failed: %s", e)
            if self._consecutive_failures >= 3:
                log.critical(
                    "Heartbeat failed %s times in a row; orders may have been cancelled",
                    self._consecutive_failures,
                )
                if self._on_failure:
                    try:
                        self._on_failure()
                    except Exception as cb_err:
                        log.error(f"Heartbeat failure callback error: {cb_err}", exc_info=True)
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
        self._id_refresh_count = 0
        self._last_heartbeat = 0.0
