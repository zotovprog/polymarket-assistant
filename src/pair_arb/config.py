"""Configuration for the Pair Arbitrage engine."""
from __future__ import annotations

import math
from dataclasses import asdict, dataclass
from typing import Any, ClassVar


@dataclass
class PairArbConfig:
    """Runtime configuration for pair arb scanning and execution."""

    VALIDATION_BOUNDS: ClassVar[dict[str, tuple[float, float]]] = {
        "min_profit_bps": (1.0, 500.0),
        "gas_cost_usd": (0.001, 1.0),
        "maker_order_ttl_sec": (1.0, 60.0),
        "max_clip_shares": (5.0, 500.0),
        "min_clip_shares": (1.0, 100.0),
        "max_unmerged_exposure_usd": (5.0, 1000.0),
        "max_leg_risk_usd": (1.0, 100.0),
        "leg_risk_retry_count": (1.0, 10.0),
        "leg_risk_max_hold_sec": (5.0, 120.0),
        "hard_drawdown_usd": (1.0, 100.0),
        "scan_interval_sec": (0.5, 30.0),
        "merge_check_interval_sec": (1.0, 60.0),
        "cooldown_after_arb_sec": (0.5, 30.0),
        "session_budget_usd": (5.0, 10000.0),
    }

    # Markets to scan (comma-separated scopes like "BTC_5m,ETH_15m")
    market_scopes: str = "BTC_5m,BTC_15m,ETH_5m,ETH_15m,SOL_5m,SOL_15m"

    # Profitability thresholds
    min_profit_bps: float = 10.0       # Min profit in bps of $1.00 payout
    gas_cost_usd: float = 0.01         # Polygon merge gas estimate

    # Order mode
    use_maker_orders: bool = True      # True = post-only (0 fee), False = taker
    maker_order_ttl_sec: float = 10.0  # Cancel maker orders after this TTL

    # Position sizing
    max_clip_shares: float = 20.0      # Max shares per side per arb
    min_clip_shares: float = 5.0       # PM minimum order size

    # Risk limits
    max_unmerged_exposure_usd: float = 50.0
    max_leg_risk_usd: float = 5.0
    leg_risk_retry_count: int = 3
    leg_risk_max_hold_sec: float = 30.0
    hard_drawdown_usd: float = 3.0

    # Timing
    scan_interval_sec: float = 1.5
    merge_check_interval_sec: float = 5.0
    cooldown_after_arb_sec: float = 2.0

    # Session
    session_budget_usd: float = 50.0

    def validate(self) -> None:
        for field_name, (lo, hi) in self.VALIDATION_BOUNDS.items():
            raw = getattr(self, field_name)
            if isinstance(raw, bool):
                continue
            value = float(raw)
            if not math.isfinite(value):
                value = lo
            value = min(max(value, lo), hi)
            default_value = getattr(type(self), field_name)
            if isinstance(default_value, int) and not isinstance(default_value, bool):
                setattr(self, field_name, int(round(value)))
            else:
                setattr(self, field_name, float(value))

    def update(self, **kwargs: Any) -> None:
        for key, value in kwargs.items():
            if not hasattr(self, key):
                continue
            current = getattr(self, key)
            try:
                if isinstance(current, bool):
                    casted = bool(value)
                else:
                    casted = type(current)(value)
            except Exception:
                continue
            setattr(self, key, casted)
        self.validate()

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)

    @classmethod
    def from_dict(cls, payload: dict[str, Any]) -> PairArbConfig:
        valid = {f.name for f in cls.__dataclass_fields__.values()}
        cfg = cls(**{k: v for k, v in payload.items() if k in valid})
        cfg.validate()
        return cfg

    def parsed_scopes(self) -> list[tuple[str, str]]:
        """Return list of (coin, timeframe) tuples from market_scopes."""
        result = []
        for scope in self.market_scopes.split(","):
            scope = scope.strip()
            if not scope:
                continue
            parts = scope.split("_", 1)
            if len(parts) == 2:
                result.append((parts[0].upper(), parts[1].lower()))
        return result
