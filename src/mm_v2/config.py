from __future__ import annotations

from dataclasses import asdict, dataclass
import math
from typing import Any, ClassVar

from mm.mm_config import MMConfig

ENTER_CONFIRM_TICKS = 2
EXIT_CONFIRM_TICKS = 5
QUALITY_DEFENSIVE_TICKS = 3
HELPFUL_SIZE_MULT_MAX = 1.80
HARMFUL_SIZE_MULT_MIN = 0.25
HELPFUL_PRICE_TICKS_MAX = 3
HARMFUL_PRICE_TICKS_MAX = 5
PAIR_SHARE_CLIP_PRICE_FLOOR = 0.50
HELPFUL_MIN_PROMOTION_MULT = 1.20
UNWIND_STUCK_WINDOW_SEC = 30.0
UNWIND_MIN_PROGRESS_RATIO = 0.10
FOUR_QUOTE_MIN_RATIO_FOR_MM = 0.35
NO_HELPFUL_TICKS_FOR_UNWIND = 3


@dataclass
class MMConfigV2:
    UPDATE_ALIASES: ClassVar[dict[str, str]] = {
        "session_limit": "session_budget_usd",
        "max_drawdown_usd": "hard_drawdown_usd",
        "order_size_usd": "base_clip_usd",
    }
    VALIDATION_BOUNDS: ClassVar[dict[str, tuple[float, float]]] = {
        "session_budget_usd": (5.0, 10000.0),
        "base_clip_usd": (1.0, 100.0),
        "target_pair_value_ratio": (0.10, 0.95),
        "soft_excess_value_ratio": (0.05, 0.60),
        "defensive_excess_value_ratio": (0.10, 0.80),
        "hard_excess_value_ratio": (0.10, 0.90),
        "base_half_spread_bps": (5.0, 5000.0),
        "max_half_spread_bps": (25.0, 10000.0),
        "inventory_skew_strength": (0.1, 10.0),
        "defensive_spread_mult": (1.0, 10.0),
        "defensive_size_mult": (0.1, 1.0),
        "unwind_window_sec": (5.0, 600.0),
        "emergency_unwind_timeout_sec": (1.0, 120.0),
        "emergency_taker_start_sec": (5.0, 120.0),
        "hard_drawdown_usd": (1.0, 1000.0),
        "max_transport_failures": (1.0, 20.0),
        "tick_interval_sec": (0.25, 10.0),
        "min_market_quality_score": (0.0, 1.0),
        "min_entry_depth_usd": (10.0, 5000.0),
        "max_entry_spread_bps": (50.0, 5000.0),
        "reconcile_drift_threshold_shares": (0.25, 50.0),
        "fill_settlement_grace_sec": (1.0, 15.0),
        "requote_threshold_bps": (1.0, 500.0),
        "fallback_poll_cap": (1.0, 30.0),
    }

    session_budget_usd: float = 15.0
    base_clip_usd: float = 3.0
    target_pair_value_ratio: float = 0.70
    soft_excess_value_ratio: float = 0.10
    defensive_excess_value_ratio: float = 0.18
    hard_excess_value_ratio: float = 0.25
    base_half_spread_bps: float = 150.0
    max_half_spread_bps: float = 600.0
    inventory_skew_strength: float = 1.0
    defensive_spread_mult: float = 1.8
    defensive_size_mult: float = 0.5
    unwind_window_sec: float = 90.0
    emergency_unwind_timeout_sec: float = 10.0
    emergency_taker_start_sec: float = 20.0
    hard_drawdown_usd: float = 4.0
    max_transport_failures: int = 5
    market_scope: str = "BTC_15m"

    # Internal/runtime parameters kept explicit for reproducibility.
    tick_interval_sec: float = 2.0
    min_market_quality_score: float = 0.35
    min_entry_depth_usd: float = 50.0
    max_entry_spread_bps: float = 800.0
    reconcile_drift_threshold_shares: float = 1.5
    fill_settlement_grace_sec: float = 6.0
    requote_threshold_bps: float = 15.0
    fallback_poll_cap: int = 12

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
        self.soft_excess_value_ratio = min(self.soft_excess_value_ratio, self.defensive_excess_value_ratio)
        self.defensive_excess_value_ratio = min(
            max(self.defensive_excess_value_ratio, self.soft_excess_value_ratio),
            self.hard_excess_value_ratio,
        )
        if self.base_half_spread_bps > self.max_half_spread_bps:
            self.base_half_spread_bps = self.max_half_spread_bps

    def update(self, **kwargs: Any) -> None:
        normalized: dict[str, Any] = {}
        for key, value in kwargs.items():
            normalized[self.UPDATE_ALIASES.get(key, key)] = value
        for key, value in normalized.items():
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
    def from_dict(cls, payload: dict[str, Any]) -> "MMConfigV2":
        valid = {f.name for f in cls.__dataclass_fields__.values()}
        cfg = cls(**{k: v for k, v in payload.items() if k in valid})
        cfg.validate()
        return cfg

    def to_mm_config(self) -> MMConfig:
        cfg = MMConfig()
        cfg.order_size_usd = float(self.base_clip_usd)
        cfg.session_limit = float(self.session_budget_usd)
        cfg.half_spread_bps = float(self.base_half_spread_bps)
        cfg.max_spread_bps = float(self.max_half_spread_bps)
        cfg.max_drawdown_usd = float(self.hard_drawdown_usd)
        cfg.use_post_only = True
        # V2 quotes both sides of the pair. SELL intents may therefore use
        # PM's short-collateral path even when no inventory is pre-owned.
        cfg.allow_short_sells = True
        cfg.min_market_quality_score = float(self.min_market_quality_score)
        cfg.min_entry_depth_usd = float(self.min_entry_depth_usd)
        cfg.max_entry_spread_bps = float(self.max_entry_spread_bps)
        cfg.requote_interval_sec = float(self.tick_interval_sec)
        cfg.requote_threshold_bps = float(self.requote_threshold_bps)
        cfg.fallback_poll_cap = int(self.fallback_poll_cap)
        cfg.fill_settlement_grace_sec = float(self.fill_settlement_grace_sec)
        cfg.critical_reconcile_drift_shares = float(self.reconcile_drift_threshold_shares)
        cfg.heartbeat_interval_sec = 5
        cfg.heartbeat_failures_before_shutdown = 3
        return cfg
