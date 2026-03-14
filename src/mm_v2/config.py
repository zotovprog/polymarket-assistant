from __future__ import annotations

from dataclasses import asdict, dataclass
import math
from typing import Any, ClassVar

from mm_shared.mm_config import MMConfig

ENTER_CONFIRM_TICKS = 2
EXIT_CONFIRM_TICKS = 5
EMERGENCY_EXIT_CONFIRM_TICKS = 2
EMERGENCY_EXIT_MIN_HOLD_SEC = 6.0
UNWIND_EXIT_CONFIRM_TICKS = 3
UNWIND_MIN_HOLD_SEC = 6.0
UNWIND_REENTRY_COOLDOWN_SEC = 8.0
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
LOW_BUDGET_PROFILE_THRESHOLD_USD = 15.0
LOW_BUDGET_CLIP_RATIO = 0.30
LOW_BUDGET_CLIP_MIN_USD = 3.0
LOW_BUDGET_HARD_EXCESS_MIN_RATIO = 0.35
DRAWDOWN_CONFIRM_TICKS = 3
DRAWDOWN_CONFIRM_MIN_AGE_SEC = 8.0
DRAWDOWN_RESET_HYSTERESIS_USD = 0.25
TARGET_RATIO_ACTIVATION_MIN_USD = 4.0
TARGET_RATIO_ACTIVATION_BUDGET_RATIO = 0.12
MM_REGIME_WINDOW_SEC = 60.0
MM_REGIME_DEGRADED_CONFIRM_SEC = 120.0
FORCED_UNWIND_EXCESS_MULT = 1.35
FORCED_UNWIND_CONFIRM_TICKS = 3


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
        "harmful_buy_suppress_ratio": (0.10, 0.90),
        "base_half_spread_bps": (5.0, 5000.0),
        "max_half_spread_bps": (25.0, 10000.0),
        "vol_spread_multiplier": (0.5, 10.0),
        "spread_amplifier_knee_min": (0.5, 30.0),
        "maker_fee_bps": (0.0, 1000.0),
        "taker_fee_bps": (0.0, 1000.0),
        "min_edge_bps": (0.0, 1000.0),
        "inventory_skew_strength": (0.1, 10.0),
        "defensive_spread_mult": (1.0, 10.0),
        "defensive_size_mult": (0.1, 1.0),
        "unwind_window_sec": (5.0, 600.0),
        "terminal_liquidation_start_sec": (5.0, 600.0),
        "emergency_unwind_timeout_sec": (1.0, 120.0),
        "emergency_taker_start_sec": (5.0, 120.0),
        "hard_drawdown_usd": (1.0, 1000.0),
        "hard_drawdown_budget_ratio": (0.05, 0.80),
        "fast_move_soft_bps_1s": (1.0, 1000.0),
        "fast_move_hard_bps_1s": (1.0, 2000.0),
        "fast_move_soft_bps_5s": (1.0, 2000.0),
        "fast_move_hard_bps_5s": (1.0, 4000.0),
        "fast_move_pause_sec": (0.5, 30.0),
        "max_transport_failures": (1.0, 20.0),
        "tick_interval_sec": (0.25, 10.0),
        "min_market_quality_score": (0.0, 1.0),
        "min_entry_depth_usd": (10.0, 5000.0),
        "max_entry_spread_bps": (50.0, 5000.0),
        "reconcile_drift_threshold_shares": (0.25, 50.0),
        "fill_settlement_grace_sec": (1.0, 15.0),
        "sell_release_grace_sec": (0.5, 10.0),
        "requote_threshold_bps": (1.0, 500.0),
        "fallback_poll_cap": (1.0, 30.0),
        "post_drift_recovery_cooldown_sec": (5.0, 120.0),
    }

    session_budget_usd: float = 30.0
    base_clip_usd: float = 4.0
    target_pair_value_ratio: float = 0.50
    soft_excess_value_ratio: float = 0.20
    defensive_excess_value_ratio: float = 0.35
    hard_excess_value_ratio: float = 0.45
    harmful_buy_suppress_ratio: float = 0.30
    base_half_spread_bps: float = 100.0
    max_half_spread_bps: float = 600.0
    vol_spread_multiplier: float = 2.0
    spread_amplifier_knee_min: float = 5.0
    maker_fee_bps: float = 0.0
    taker_fee_bps: float = 78.0
    min_edge_bps: float = 20.0
    inventory_skew_strength: float = 2.0
    defensive_spread_mult: float = 1.5
    defensive_size_mult: float = 0.4
    unwind_window_sec: float = 240.0
    terminal_liquidation_start_sec: float = 20.0
    emergency_unwind_timeout_sec: float = 10.0
    emergency_taker_start_sec: float = 20.0
    hard_drawdown_usd: float = 4.0
    hard_drawdown_budget_ratio: float = 0.30
    fast_move_soft_bps_1s: float = 30.0
    fast_move_hard_bps_1s: float = 60.0
    fast_move_soft_bps_5s: float = 50.0
    fast_move_hard_bps_5s: float = 100.0
    fast_move_pause_sec: float = 5.0
    max_transport_failures: int = 5
    market_scope: str = "BTC_15m"

    # Internal/runtime parameters kept explicit for reproducibility.
    tick_interval_sec: float = 1.0
    min_market_quality_score: float = 0.35
    min_entry_depth_usd: float = 50.0
    max_entry_spread_bps: float = 800.0
    reconcile_drift_threshold_shares: float = 1.5
    fill_settlement_grace_sec: float = 12.0
    sell_release_grace_sec: float = 3.0
    requote_threshold_bps: float = 8.0
    fallback_poll_cap: int = 12
    post_drift_recovery_cooldown_sec: float = 30.0

    def effective_base_clip_usd(self) -> float:
        base = float(self.base_clip_usd)
        budget = max(0.01, float(self.session_budget_usd))
        if budget <= LOW_BUDGET_PROFILE_THRESHOLD_USD:
            low_budget_cap = max(LOW_BUDGET_CLIP_MIN_USD, budget * LOW_BUDGET_CLIP_RATIO)
            return min(base, low_budget_cap)
        return base

    def effective_hard_excess_value_ratio(self) -> float:
        ratio = float(self.hard_excess_value_ratio)
        budget = max(0.01, float(self.session_budget_usd))
        if budget <= LOW_BUDGET_PROFILE_THRESHOLD_USD:
            return max(ratio, LOW_BUDGET_HARD_EXCESS_MIN_RATIO)
        return ratio

    def effective_hard_drawdown_usd(self) -> float:
        dynamic = float(self.hard_drawdown_budget_ratio) * max(0.0, float(self.session_budget_usd))
        return max(float(self.hard_drawdown_usd), dynamic)

    def effective_target_ratio_activation_usd(self) -> float:
        return max(
            float(TARGET_RATIO_ACTIVATION_MIN_USD),
            float(TARGET_RATIO_ACTIVATION_BUDGET_RATIO) * max(0.0, float(self.session_budget_usd)),
        )

    def effective_harmful_buy_suppress_usd(self) -> float:
        budget = max(0.0, float(self.session_budget_usd))
        return max(2.0, float(self.harmful_buy_suppress_ratio) * budget)

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
        cfg.sell_release_grace_sec = float(self.sell_release_grace_sec)
        cfg.critical_reconcile_drift_shares = float(self.reconcile_drift_threshold_shares)
        cfg.heartbeat_interval_sec = 5
        cfg.heartbeat_failures_before_shutdown = 3
        return cfg
