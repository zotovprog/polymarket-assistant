"""PnL Decomposition — break down MM profit/loss into components.

Components:
1. Spread capture: profit from bid-ask spread (buy low, sell high on same token)
2. Markout cost: adverse selection cost (price moves against us after fill)
3. Fee leakage: taker fees paid (should be ~0 with maker-only)
4. Merge profit: profit from merging UP+DN pairs via CTF ($1 - cost)
5. Resolution profit: profit from tokens resolving to $1 at expiry
6. Inventory cost: unrealized P&L from holding unhedged positions
"""

from __future__ import annotations

import logging
import time
from dataclasses import dataclass

log = logging.getLogger("mm.pnl")


@dataclass
class PnLComponent:
    """A single PnL component with running totals."""

    name: str
    total_usd: float = 0.0
    count: int = 0

    def add(self, amount: float) -> None:
        self.total_usd += amount
        self.count += 1

    def to_dict(self) -> dict:
        return {
            "name": self.name,
            "total_usd": round(self.total_usd, 4),
            "count": self.count,
            "avg_per_event": round(self.total_usd / self.count, 6)
            if self.count > 0
            else 0.0,
        }


class PnLDecomposition:
    """Track and decompose PnL into components."""

    def __init__(self):
        self.spread_capture = PnLComponent("spread_capture")
        self.fee_leakage = PnLComponent("fee_leakage")
        self.merge_profit = PnLComponent("merge_profit")
        self.resolution_profit = PnLComponent("resolution_profit")
        self.inventory_cost = PnLComponent("inventory_cost")

        # Track per-token cost basis for spread calculation
        self._token_buys: dict[str, list[tuple[float, float]]] = {}
        self._token_sells: dict[str, list[tuple[float, float]]] = {}
        self._start_time: float = time.time()

    def record_fill(
        self, side: str, token_id: str, price: float, size: float, fee: float, is_maker: bool
    ) -> None:
        """Record a fill and update PnL components."""
        # Fee leakage
        if fee > 0 and not is_maker:
            self.fee_leakage.add(-fee)

        # Track buys and sells for spread capture
        if side == "BUY":
            if token_id not in self._token_buys:
                self._token_buys[token_id] = []
            self._token_buys[token_id].append((price, size))
        else:
            if token_id not in self._token_sells:
                self._token_sells[token_id] = []
            self._token_sells[token_id].append((price, size))

            # Calculate spread capture for matched buys/sells
            buys = self._token_buys.get(token_id, [])
            if buys:
                # Match with oldest buy (FIFO)
                buy_price, buy_size = buys[0]
                matched_size = min(size, buy_size)
                spread_pnl = (price - buy_price) * matched_size
                self.spread_capture.add(spread_pnl)

                # Update remaining buys
                remaining = buy_size - matched_size
                if remaining <= 0.01:
                    buys.pop(0)
                else:
                    buys[0] = (buy_price, remaining)

    def record_merge(self, pairs: float, total_cost: float) -> None:
        """Record a merge operation. profit = pairs * $1.00 - total_cost."""
        profit = pairs - total_cost
        self.merge_profit.add(profit)

    def record_resolution(
        self, token_type: str, shares: float, avg_cost: float, resolved_to: float
    ) -> None:
        """Record token resolution at expiry.

        Args:
            token_type: "up" or "dn"
            shares: Number of shares held
            avg_cost: Average entry price per share
            resolved_to: Resolution value ($1.00 if correct, $0.00 if wrong)
        """
        pnl = (resolved_to - avg_cost) * shares
        self.resolution_profit.add(pnl)

    def update_inventory_cost(
        self,
        up_shares: float,
        up_avg_price: float,
        up_current_fv: float,
        dn_shares: float,
        dn_avg_price: float,
        dn_current_fv: float,
    ) -> float:
        """Calculate current unrealized inventory cost.

        Returns unrealized P&L from holding.
        """
        up_unrealized = (up_current_fv - up_avg_price) * up_shares if up_shares > 0 else 0.0
        dn_unrealized = (dn_current_fv - dn_avg_price) * dn_shares if dn_shares > 0 else 0.0
        total_unrealized = up_unrealized + dn_unrealized
        return total_unrealized

    @property
    def total_realized(self) -> float:
        """Total realized PnL across all components."""
        return (
            self.spread_capture.total_usd
            + self.fee_leakage.total_usd
            + self.merge_profit.total_usd
            + self.resolution_profit.total_usd
        )

    @property
    def stats(self) -> dict:
        """Full PnL decomposition summary."""
        elapsed = time.time() - self._start_time
        return {
            "components": {
                "spread_capture": self.spread_capture.to_dict(),
                "fee_leakage": self.fee_leakage.to_dict(),
                "merge_profit": self.merge_profit.to_dict(),
                "resolution_profit": self.resolution_profit.to_dict(),
                "inventory_cost": self.inventory_cost.to_dict(),
            },
            "total_realized_usd": round(self.total_realized, 4),
            "elapsed_sec": round(elapsed, 1),
            "pnl_per_minute": round(self.total_realized / max(elapsed / 60, 0.1), 4),
        }

    def reset(self) -> None:
        """Reset all components."""
        self.spread_capture = PnLComponent("spread_capture")
        self.fee_leakage = PnLComponent("fee_leakage")
        self.merge_profit = PnLComponent("merge_profit")
        self.resolution_profit = PnLComponent("resolution_profit")
        self.inventory_cost = PnLComponent("inventory_cost")
        self._token_buys.clear()
        self._token_sells.clear()
        self._start_time = time.time()
