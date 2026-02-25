"""Markout TCA - Transaction Cost Analysis via post-fill mid tracking.

After each fill, record the mid-price at 1s, 5s, and 30s intervals.
If the mid moved against us after a BUY, we experienced adverse selection.

Example: BUY at 0.50, mid at +5s = 0.48 -> markout = -0.02 (adverse)
         BUY at 0.50, mid at +5s = 0.52 -> markout = +0.02 (favorable)
         SELL at 0.50, mid at +5s = 0.52 -> markout = -0.02 (adverse)
"""

from __future__ import annotations

import asyncio
import logging
import time
from dataclasses import dataclass, field
from typing import Callable

log = logging.getLogger("mm.tca")

MARKOUT_INTERVALS_SEC = [1.0, 5.0, 30.0]


@dataclass
class MarkoutRecord:
    """Record of a single fill and its markout measurements."""

    fill_ts: float
    side: str  # "BUY" or "SELL"
    token_id: str
    fill_price: float
    fill_size: float
    is_maker: bool
    # Markout mids: key = interval_sec, value = mid at that time (None = not yet measured)
    markout_mids: dict[float, float | None] = field(default_factory=dict)

    @property
    def markouts(self) -> dict[float, float | None]:
        """Compute markout P&L for each interval.

        For BUY: markout = mid_later - fill_price (positive = favorable)
        For SELL: markout = fill_price - mid_later (positive = favorable)
        """
        result: dict[float, float | None] = {}
        for interval, mid in self.markout_mids.items():
            if mid is None:
                result[interval] = None
                continue
            if self.side == "BUY":
                result[interval] = mid - self.fill_price
            else:
                result[interval] = self.fill_price - mid
        return result

    @property
    def is_complete(self) -> bool:
        return all(v is not None for v in self.markout_mids.values())

    def to_dict(self) -> dict:
        return {
            "fill_ts": self.fill_ts,
            "side": self.side,
            "token_id": self.token_id[:12],
            "fill_price": self.fill_price,
            "fill_size": self.fill_size,
            "is_maker": self.is_maker,
            "markouts": {
                f"{k}s": round(v, 6) if v is not None else None for k, v in self.markouts.items()
            },
        }


class MarkoutTracker:
    """Track post-fill mid-prices for adverse selection analysis."""

    def __init__(
        self,
        get_mid_func: Callable[[str], float | None] | Callable[[], float | None] | None = None,
        max_records: int = 200,
    ):
        """
        Args:
            get_mid_func: Callable that returns current mid-price for the token.
                         Called periodically to measure markout.
            max_records: Maximum completed records to keep.
        """
        self._get_mid = get_mid_func
        self._max_records = max_records
        self._pending: list[MarkoutRecord] = []  # Fills waiting for markout measurement
        self._completed: list[MarkoutRecord] = []  # Fills with all markouts measured
        self._task: asyncio.Task | None = None
        self._running = False

    def _current_mid(self, token_id: str) -> float | None:
        """Get current mid for a token (supports callback with or without token_id arg)."""
        if not self._get_mid:
            return None
        try:
            return self._get_mid(token_id)  # type: ignore[misc]
        except TypeError:
            return self._get_mid()  # type: ignore[misc]

    def record_fill(
        self, side: str, token_id: str, fill_price: float, fill_size: float, is_maker: bool = True
    ) -> None:
        """Record a new fill for markout tracking."""
        record = MarkoutRecord(
            fill_ts=time.time(),
            side=side,
            token_id=token_id,
            fill_price=fill_price,
            fill_size=fill_size,
            is_maker=is_maker,
            markout_mids={interval: None for interval in MARKOUT_INTERVALS_SEC},
        )
        self._pending.append(record)

    async def check_markouts(self) -> None:
        """Check pending records and fill in markout measurements."""
        if not self._get_mid or not self._pending:
            return

        now = time.time()

        mids_cache: dict[str, float | None] = {}
        newly_complete: list[MarkoutRecord] = []
        for record in self._pending:
            elapsed = now - record.fill_ts
            mid = mids_cache.get(record.token_id)
            if mid is None and record.token_id not in mids_cache:
                mid = self._current_mid(record.token_id)
                mids_cache[record.token_id] = mid
            if mid is None or mid <= 0:
                continue
            for interval in MARKOUT_INTERVALS_SEC:
                if record.markout_mids[interval] is None and elapsed >= interval:
                    record.markout_mids[interval] = mid

            if record.is_complete:
                newly_complete.append(record)

        for record in newly_complete:
            self._pending.remove(record)
            self._completed.append(record)
            log.debug(
                "Markout complete: %s %s@%.4f -> %s",
                record.side,
                record.token_id[:8],
                record.fill_price,
                {f"{k}s": f"{v:.4f}" for k, v in record.markouts.items() if v is not None},
            )

        # Trim completed records
        if len(self._completed) > self._max_records:
            self._completed = self._completed[-self._max_records :]

    def start(self) -> None:
        """Start periodic markout checking."""
        if self._running:
            return
        self._running = True
        self._task = asyncio.ensure_future(self._check_loop())

    async def _check_loop(self) -> None:
        """Periodically check markouts every 1 second."""
        while self._running:
            try:
                await self.check_markouts()
            except Exception as e:
                log.debug("Markout check error: %s", e)
            await asyncio.sleep(1.0)

    def stop(self) -> None:
        """Stop periodic checking."""
        self._running = False
        if self._task:
            self._task.cancel()
            self._task = None

    @property
    def stats(self) -> dict:
        """Summary statistics of markout measurements."""
        if not self._completed:
            return {
                "total_fills": 0,
                "pending_fills": len(self._pending),
                "avg_markout_1s": 0.0,
                "avg_markout_5s": 0.0,
                "avg_markout_30s": 0.0,
                "adverse_pct_5s": 0.0,
            }

        markouts_1s = [r.markouts[1.0] for r in self._completed if r.markouts.get(1.0) is not None]
        markouts_5s = [r.markouts[5.0] for r in self._completed if r.markouts.get(5.0) is not None]
        markouts_30s = [r.markouts[30.0] for r in self._completed if r.markouts.get(30.0) is not None]

        adverse_5s = sum(1 for m in markouts_5s if m < 0) if markouts_5s else 0

        return {
            "total_fills": len(self._completed),
            "pending_fills": len(self._pending),
            "avg_markout_1s": round(sum(markouts_1s) / len(markouts_1s), 6) if markouts_1s else 0.0,
            "avg_markout_5s": round(sum(markouts_5s) / len(markouts_5s), 6) if markouts_5s else 0.0,
            "avg_markout_30s": round(sum(markouts_30s) / len(markouts_30s), 6) if markouts_30s else 0.0,
            "adverse_pct_5s": round(adverse_5s / len(markouts_5s) * 100, 1) if markouts_5s else 0.0,
        }

    @property
    def recent_records(self) -> list[dict]:
        """Last 20 completed markout records."""
        return [r.to_dict() for r in self._completed[-20:]]
