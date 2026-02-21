"""Rebate Tracker — monitors order scoring for Polymarket Maker Rebates Program.

Polymarket's Maker Rebates Program pays daily USDC rebates to qualifying
market makers. Orders must meet criteria:
- Minimum size
- Maximum spread from mid
- Minimum time in book

We use the /order-scoring endpoint to check eligibility.
"""
from __future__ import annotations
import asyncio
import logging
import time
from typing import Any, Optional

log = logging.getLogger("mm.rebate")


class RebateTracker:
    """Track order scoring and rebate eligibility."""

    def __init__(self, clob_client: Any):
        self.client = clob_client
        self._scoring_cache: dict[str, dict] = {}  # order_id -> scoring result
        self._eligible_count: int = 0
        self._total_checked: int = 0
        self._estimated_daily_rebate: float = 0.0
        self._last_check: float = 0.0

    async def check_order_scoring(self, token_id: str, price: float,
                                   size: float, side: str) -> Optional[dict]:
        """Check if an order would be eligible for rebates.

        Args:
            token_id: Polymarket token ID
            price: Order price
            size: Order size in shares
            side: "BUY" or "SELL"

        Returns:
            Scoring result dict or None on error.
        """
        try:
            params = {
                "token_id": token_id,
                "price": price,
                "size": size,
                "side": side,
            }
            result = await asyncio.to_thread(
                self.client.is_order_scoring, params
            )
            self._total_checked += 1
            if result and result.get("scoring", False):
                self._eligible_count += 1
            self._last_check = time.time()
            return result
        except Exception as e:
            log.debug(f"Order scoring check failed: {e}")
            return None

    async def check_batch(self, orders: list[dict]) -> list[Optional[dict]]:
        """Check multiple orders for scoring eligibility.

        Args:
            orders: List of dicts with token_id, price, size, side

        Returns:
            List of scoring results (None for failures).
        """
        results = []
        for o in orders:
            r = await self.check_order_scoring(
                o["token_id"], o["price"], o["size"], o["side"]
            )
            results.append(r)
        return results

    def estimate_daily_rebate(self, daily_volume_usd: float,
                              avg_time_in_book_sec: float,
                              eligible_ratio: float = None) -> float:
        """Rough estimate of daily rebate based on activity.

        This is a simplified estimate — actual rebates depend on
        the total pool and all participants' activity.

        Args:
            daily_volume_usd: Our expected daily maker volume
            avg_time_in_book_sec: Average time orders stay in book
            eligible_ratio: Fraction of orders that score (auto from history if None)

        Returns:
            Estimated daily rebate in USD.
        """
        if eligible_ratio is None:
            eligible_ratio = (self._eligible_count / self._total_checked
                              if self._total_checked > 0 else 0.5)

        # Simplified model: rebate proportional to volume × time × eligibility
        # Actual formula depends on pool size and competition
        # Conservative estimate: ~0.1-0.5% of eligible volume
        base_rate = 0.002  # 0.2% of volume as rebate (conservative)
        estimate = daily_volume_usd * base_rate * eligible_ratio

        self._estimated_daily_rebate = round(estimate, 4)
        return self._estimated_daily_rebate

    @property
    def stats(self) -> dict:
        return {
            "eligible_count": self._eligible_count,
            "total_checked": self._total_checked,
            "eligible_ratio": (self._eligible_count / self._total_checked
                               if self._total_checked > 0 else 0.0),
            "estimated_daily_rebate": self._estimated_daily_rebate,
            "last_check": self._last_check,
        }

    def reset(self) -> None:
        """Reset counters."""
        self._scoring_cache.clear()
        self._eligible_count = 0
        self._total_checked = 0
        self._estimated_daily_rebate = 0.0
        self._last_check = 0.0
