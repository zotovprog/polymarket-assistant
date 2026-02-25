"""Risk Manager — inventory limits, PnL tracking, pause conditions."""
from __future__ import annotations
import time
from dataclasses import dataclass
from .types import Inventory, Fill, MMState
from .mm_config import MMConfig


@dataclass
class LiquidationLock:
    """Snapshot of prices at the moment liquidation is triggered."""
    triggered_at: float = 0.0
    trigger_pnl: float = 0.0
    up_avg_entry: float = 0.5
    dn_avg_entry: float = 0.5
    min_sell_price_up: float = 0.01
    min_sell_price_dn: float = 0.01


class RiskManager:
    """Monitor risk limits and decide when to pause quoting."""

    def __init__(self, config: MMConfig):
        self.config = config
        self._fills: list[Fill] = []
        self._session_start: float = time.time()
        self._vol_history: list[float] = []  # recent vol readings
        self._peak_pnl: float = 0.0
        self._liquidation_lock: LiquidationLock | None = None

    def record_fill(self, fill: Fill) -> None:
        """Record a fill for PnL tracking."""
        self._fills.append(fill)

    def record_vol(self, vol: float) -> None:
        """Record a volatility reading."""
        self._vol_history.append(vol)
        # Keep last 100 readings
        if len(self._vol_history) > 100:
            self._vol_history = self._vol_history[-100:]

    @property
    def avg_volatility(self) -> float:
        if not self._vol_history:
            return 0.0
        return sum(self._vol_history) / len(self._vol_history)

    def check_inventory_limit(self, inventory: Inventory) -> bool:
        """Return True if inventory is within limits."""
        if abs(inventory.up_shares) > self.config.max_inventory_shares:
            return False
        if abs(inventory.dn_shares) > self.config.max_inventory_shares:
            return False
        if abs(inventory.net_delta) > self.config.max_net_delta_shares:
            return False
        return True

    def compute_pnl(self, inventory: Inventory,
                    fv_up: float = 0.5, fv_dn: float = 0.5) -> dict:
        """Compute realized and unrealized PnL.

        Returns dict with:
            realized_pnl: Sum of (sell_notional - buy_notional) for closed trades
            unrealized_pnl: Mark-to-market of current inventory
            total_fees: Sum of all fees paid
            total_volume: Sum of all fill notionals
            fill_count: Number of fills
        """
        total_fees = sum(f.fee for f in self._fills)
        total_volume = sum(f.notional for f in self._fills)

        # Realized PnL from fills
        realized = 0.0
        for f in self._fills:
            if f.side == "SELL":
                realized += f.notional - f.fee
            else:
                realized -= f.notional + f.fee

        # Unrealized PnL: current inventory at fair value
        unrealized = (inventory.up_shares * fv_up +
                      inventory.dn_shares * fv_dn)

        return {
            "realized_pnl": round(realized, 4),
            "unrealized_pnl": round(unrealized, 4),
            "total_pnl": round(realized + unrealized, 4),
            "total_fees": round(total_fees, 4),
            "total_volume": round(total_volume, 4),
            "fill_count": len(self._fills),
        }

    def should_pause(self, inventory: Inventory,
                     current_vol: float = 0.0,
                     fv_up: float = 0.5,
                     fv_dn: float = 0.5,
                     session_pnl: float | None = None) -> tuple[bool, str]:
        """Check if MM should pause quoting.

        Args:
            session_pnl: PM-balance-based session PnL (more reliable than internal).
                         If provided, used for drawdown/TP/trailing stop checks.

        Returns (should_pause, reason).
        """
        # Check PnL-based exits FIRST (take profit, drawdown, trailing stop)
        # These must run before inventory limit — inventory pause must not block profit-taking
        pnl = self.compute_pnl(inventory, fv_up, fv_dn)
        # Prefer session_pnl (PM-balance-based) — immune to reconciliation oscillation
        check_pnl = session_pnl if session_pnl is not None else pnl["total_pnl"]

        # Check take-profit
        if self.config.take_profit_usd > 0 and check_pnl >= self.config.take_profit_usd:
            return True, f"Take profit hit: PnL=${check_pnl:.2f} >= ${self.config.take_profit_usd:.2f}"

        # Check drawdown
        if check_pnl < -self.config.max_drawdown_usd:
            return True, f"Max drawdown exceeded: PnL=${check_pnl:.2f}"

        # Trailing stop — only activates after peak PnL reaches a meaningful level
        if check_pnl > self._peak_pnl:
            self._peak_pnl = check_pnl
        if self.config.trailing_stop_pct > 0 and self._peak_pnl > 0:
            # Min peak: 25% of take_profit or $2, whichever is larger
            min_peak = max(2.0, self.config.take_profit_usd * 0.25) if self.config.take_profit_usd > 0 else 2.0
            if self._peak_pnl >= min_peak:
                trail_threshold = self._peak_pnl * (1 - self.config.trailing_stop_pct)
                if check_pnl < trail_threshold:
                    return True, f"Trailing stop: PnL=${check_pnl:.2f} dropped from peak ${self._peak_pnl:.2f}"

        # Check inventory limit (after PnL exits)
        if not self.check_inventory_limit(inventory):
            if abs(inventory.net_delta) > self.config.max_net_delta_shares:
                return True, (
                    f"Inventory limit (net delta): |{inventory.net_delta:.1f}| > "
                    f"{self.config.max_net_delta_shares:.0f} "
                    f"(UP={inventory.up_shares:.1f}, DN={inventory.dn_shares:.1f})"
                )
            return True, f"Inventory limit exceeded: UP={inventory.up_shares:.1f}, DN={inventory.dn_shares:.1f}"

        # Check volatility spike
        avg_vol = self.avg_volatility
        if avg_vol > 0 and current_vol > avg_vol * self.config.volatility_pause_mult:
            return True, f"Volatility spike: {current_vol:.4f} > {avg_vol * self.config.volatility_pause_mult:.4f}"

        # Check if config disabled
        if not self.config.enabled:
            return True, "MM disabled via config"

        return False, ""

    def get_stats(self, inventory: Inventory,
                  fv_up: float = 0.5, fv_dn: float = 0.5) -> dict:
        """Get comprehensive risk/stats summary."""
        pnl = self.compute_pnl(inventory, fv_up, fv_dn)
        uptime = time.time() - self._session_start

        return {
            **pnl,
            "peak_pnl": round(self._peak_pnl, 4),
            "uptime_sec": round(uptime, 1),
            "avg_volatility": round(self.avg_volatility, 6),
            "inventory_up": round(inventory.up_shares, 2),
            "inventory_dn": round(inventory.dn_shares, 2),
            "net_delta": round(inventory.net_delta, 2),
            "max_net_delta_shares": self.config.max_net_delta_shares,
            "usdc_balance": round(inventory.usdc, 2),
        }

    @property
    def fills(self) -> list[Fill]:
        return self._fills

    def lock_pnl(self, inventory: Inventory, fv_up: float, fv_dn: float,
                 margin: float = 0.01,
                 best_bid_up: float | None = None,
                 best_bid_dn: float | None = None) -> LiquidationLock:
        """Snapshot prices at liquidation trigger time."""
        def _min_sell_floor(avg_entry: float, best_bid: float | None) -> float:
            cost_floor = max(0.01, avg_entry + margin)
            if best_bid is None or best_bid <= 0.02:
                return cost_floor
            market_floor = max(0.01, best_bid - 0.02)
            return min(cost_floor, market_floor)

        pnl = self.compute_pnl(inventory, fv_up, fv_dn)
        lock = LiquidationLock(
            triggered_at=time.time(),
            trigger_pnl=pnl["total_pnl"],
            up_avg_entry=inventory.up_cost.avg_entry_price,
            dn_avg_entry=inventory.dn_cost.avg_entry_price,
            min_sell_price_up=_min_sell_floor(inventory.up_cost.avg_entry_price, best_bid_up),
            min_sell_price_dn=_min_sell_floor(inventory.dn_cost.avg_entry_price, best_bid_dn),
        )
        self._liquidation_lock = lock
        return lock

    @property
    def liquidation_lock(self) -> LiquidationLock | None:
        return self._liquidation_lock

    def reset(self) -> None:
        """Reset for new session."""
        self._fills.clear()
        self._vol_history.clear()
        self._peak_pnl = 0.0
        self._liquidation_lock = None
        self._session_start = time.time()
