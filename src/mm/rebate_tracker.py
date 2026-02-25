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
        self._fee_weights: list[tuple[float, float, str]] = []
        self._zone_eligibility: dict[str, dict[str, int]] = {
            "extreme": {"eligible": 0, "ineligible": 0},
            "moderate": {"eligible": 0, "ineligible": 0},
            "balanced": {"eligible": 0, "ineligible": 0},
        }

    @staticmethod
    def _price_zone(price: float) -> str:
        """Classify price into rebate scoring zones."""
        if price < 0.15 or price > 0.85:
            return "extreme"
        if (0.15 <= price < 0.35) or (0.65 < price <= 0.85):
            return "moderate"
        return "balanced"

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
            zone = self._price_zone(price)
            weight = (price * (1.0 - price)) ** 2
            params = {
                "token_id": token_id,
                "price": price,
                "size": size,
                "side": side,
            }
            result = await asyncio.to_thread(
                self.client.is_order_scoring, params
            )
            self._fee_weights.append((time.time(), weight, token_id))
            if len(self._fee_weights) > 500:
                self._fee_weights = self._fee_weights[-500:]

            self._total_checked += 1
            is_eligible = bool(result and result.get("scoring", False))
            if is_eligible:
                self._eligible_count += 1
            self._zone_eligibility[zone]["eligible" if is_eligible else "ineligible"] += 1
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
                              eligible_ratio: Optional[float] = None,
                              pool_size_usd: float = 5000.0,
                              our_market_share: float = 0.01) -> float:
        """Rough estimate of daily rebate based on activity.

        This is a simplified estimate — actual rebates depend on
        the total pool and all participants' activity.

        Args:
            daily_volume_usd: Our expected daily maker volume
            avg_time_in_book_sec: Average time orders stay in book
            eligible_ratio: Fraction of orders that score (auto from history if None)
            pool_size_usd: Estimated daily maker rebate pool in USD
            our_market_share: Our expected share of total maker volume

        Returns:
            Estimated daily rebate in USD.
        """
        if eligible_ratio is None:
            eligible_ratio = (self._eligible_count / self._total_checked
                              if self._total_checked > 0 else 0.5)

        # Keep daily_volume_usd and avg_time_in_book_sec in the signature for
        # backward compatibility, but estimate from pool, share, and eligibility.
        estimate = pool_size_usd * our_market_share * eligible_ratio

        self._estimated_daily_rebate = round(estimate, 4)
        log.info(
            "Estimated daily rebate=%.4f (pool_size_usd=%.2f, market_share=%.4f, "
            "eligible_ratio=%.4f, daily_volume_usd=%.2f, avg_time_in_book_sec=%.1f)",
            self._estimated_daily_rebate,
            pool_size_usd,
            our_market_share,
            eligible_ratio,
            daily_volume_usd,
            avg_time_in_book_sec,
        )
        return self._estimated_daily_rebate

    def scoring_eligibility_summary(self) -> dict:
        """Return eligible vs ineligible scoring counts by price zone."""
        summary: dict[str, dict[str, float | int]] = {}
        for zone, counts in self._zone_eligibility.items():
            eligible = counts["eligible"]
            ineligible = counts["ineligible"]
            total = eligible + ineligible
            summary[zone] = {
                "eligible": eligible,
                "ineligible": ineligible,
                "total": total,
                "eligible_ratio": (eligible / total if total > 0 else 0.0),
            }
        return summary

    def recommend_quote_zone(self) -> str:
        """Recommend a quoting focus zone from scoring history."""
        if self._total_checked < 10:
            return "insufficient_data"

        summary = self.scoring_eligibility_summary()
        zone_order = ("extreme", "moderate", "balanced")
        best_zone = "insufficient_data"
        best_ratio = -1.0
        best_total = -1

        for zone in zone_order:
            ratio = float(summary[zone]["eligible_ratio"])
            total = int(summary[zone]["total"])
            if ratio > best_ratio or (ratio == best_ratio and total > best_total):
                best_zone = zone
                best_ratio = ratio
                best_total = total

        return best_zone

    @property
    def fee_curve_weight_log(self) -> list[tuple[float, float, str]]:
        """Recent fee curve weights as (timestamp, weight, token_id)."""
        return list(self._fee_weights)

    @property
    def stats(self) -> dict:
        summary = self.scoring_eligibility_summary()
        avg_fee_weight = (sum(w for _, w, _ in self._fee_weights) / len(self._fee_weights)
                          if self._fee_weights else 0.0)
        return {
            "eligible_count": self._eligible_count,
            "total_checked": self._total_checked,
            "eligible_ratio": (self._eligible_count / self._total_checked
                               if self._total_checked > 0 else 0.0),
            "estimated_daily_rebate": self._estimated_daily_rebate,
            "last_check": self._last_check,
            "avg_fee_weight": avg_fee_weight,
            "price_zone_breakdown": {
                zone: int(data["total"]) for zone, data in summary.items()
            },
            "recommendation": self.recommend_quote_zone(),
        }

    def reset(self) -> None:
        """Reset counters."""
        self._scoring_cache.clear()
        self._eligible_count = 0
        self._total_checked = 0
        self._estimated_daily_rebate = 0.0
        self._last_check = 0.0
        self._fee_weights.clear()
        self._zone_eligibility = {
            "extreme": {"eligible": 0, "ineligible": 0},
            "moderate": {"eligible": 0, "ineligible": 0},
            "balanced": {"eligible": 0, "ineligible": 0},
        }
