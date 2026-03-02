"""Market Making configuration with runtime-adjustable parameters."""
from __future__ import annotations
import logging
import math
from dataclasses import dataclass, asdict
from typing import Any, ClassVar


log = logging.getLogger("mm.config")


@dataclass
class MMConfig:
    """All MM parameters — can be updated at runtime via API."""

    # Backward-compatible aliases accepted by update().
    UPDATE_ALIASES: ClassVar[dict[str, str]] = {
        "refresh_interval_s": "requote_interval_sec",
        "liq_chunk_interval_s": "liq_chunk_interval_sec",
    }

    # Safety bounds for critical runtime-adjustable parameters.
    VALIDATION_BOUNDS: ClassVar[dict[str, tuple[float, float]]] = {
        "half_spread_bps": (5.0, 5000.0),
        "min_spread_bps": (1.0, 2000.0),
        "max_spread_bps": (10.0, 10000.0),
        "vol_spread_mult": (1.0, 10.0),
        "dynamic_spread_gamma": (0.01, 5.0),
        "dynamic_spread_k": (0.05, 20.0),
        "dynamic_spread_min_bps": (1.0, 2000.0),
        "dynamic_spread_max_bps": (10.0, 10000.0),
        "fv_vol_floor": (0.0001, 0.01),
        "fv_signal_weight": (0.0, 0.10),
        "order_size_usd": (1.0, 500.0),
        "min_order_size_usd": (0.5, 200.0),
        "max_order_size_usd": (1.0, 1000.0),
        "max_position_usd": (5.0, 5000.0),
        "max_inventory_shares": (1.0, 500.0),
        "max_net_delta_shares": (1.0, 200.0),
        "min_quote_size_shares": (0.1, 200.0),
        "layers": (1.0, 10.0),
        "layer_spacing_bps": (1.0, 1000.0),
        "skew_bps_per_unit": (0.0, 500.0),
        "refresh_interval_s": (0.5, 60.0),
        "requote_threshold_bps": (1.0, 500.0),
        "requote_interval_jitter_sec": (0.0, 10.0),
        "price_jitter_ticks": (0.0, 10.0),
        "size_jitter_pct": (0.0, 1.0),
        "event_pm_mid_threshold_bps": (1.0, 500.0),
        "event_binance_threshold_bps": (1.0, 1000.0),
        "event_poll_interval_sec": (0.05, 5.0),
        "event_fallback_interval_sec": (0.5, 60.0),
        "heartbeat_interval_sec": (1.0, 30.0),
        "heartbeat_failures_before_shutdown": (1.0, 10.0),
        "session_limit": (0.0, 100000.0),
        "max_drawdown_usd": (1.0, 1000.0),
        "volatility_pause_mult": (1.0, 20.0),
        "max_loss_per_fill_usd": (0.5, 100.0),
        "take_profit_usd": (0.0, 10000.0),
        "trailing_stop_pct": (0.0, 1.0),
        "gamma_bps": (0.0, 500.0),
        "gamma_decay": (0.0, 1.0),
        "liq_chunk_pct": (0.01, 1.0),
        "liq_chunk_interval_s": (1.0, 300.0),
        "liq_gradual_chunks": (1.0, 50.0),
        "liq_max_discount_from_fv": (0.0, 0.5),
        "merge_sell_epsilon": (0.0, 0.2),
        "merge_sell_min_depth_pairs": (0.5, 10000.0),
        "gtd_duration_sec": (30.0, 3600.0),
        "entry_settle_sec": (0.0, 300.0),
        "close_window_sec": (5.0, 300.0),
        "resolution_wait_sec": (10.0, 600.0),
        "rebate_check_interval_ticks": (1.0, 200.0),
        "rebate_non_scoring_size_mult": (0.1, 1.0),
        "rebate_score_timeout_sec": (0.5, 15.0),
        "market_selector_min_score": (0.0, 1.0),
        "redeem_retry_interval_sec": (2.0, 120.0),
        "toxic_divergence_threshold": (0.02, 0.40),
        "toxic_divergence_ticks": (1.0, 120.0),
        "critical_reconcile_drift_shares": (0.5, 50.0),
        "fill_settlement_grace_sec": (1.0, 15.0),
        "pre_entry_stable_checks": (1.0, 20.0),
        "pre_entry_min_quality_score": (0.0, 1.0),
        "pre_entry_max_spread_bps": (25.0, 5000.0),
        "pre_entry_max_divergence": (0.01, 0.50),
        "post_fill_entry_guard_sec": (5.0, 120.0),
        "post_fill_entry_score_drop": (0.05, 1.0),
        "post_fill_entry_spread_widen_bps": (50.0, 5000.0),
        "one_sided_protect_ticks": (1.0, 600.0),
        "flat_start_max_shares": (0.0, 1000.0),
    }

    # ── Spread ───────────────────────────────────────────────────
    half_spread_bps: float = 200.0       # 2% half-spread to improve queue competitiveness
    min_spread_bps: float = 50.0         # Absolute minimum half-spread
    max_spread_bps: float = 500.0        # Cap emergency widening at 5%
    vol_spread_mult: float = 1.5         # Widen spread by this factor in high-vol
    dynamic_spread_enabled: bool = True
    dynamic_spread_gamma: float = 0.10
    dynamic_spread_k: float = 1.5
    dynamic_spread_min_bps: float = 50.0
    dynamic_spread_max_bps: float = 800.0

    # ── Fair Value ───────────────────────────────────────────────
    fv_vol_floor: float = 0.0003         # Min per-kline vol for FV model
    fv_signal_weight: float = 0.0        # TA signal weight (0 = disabled)

    # ── Sizing ───────────────────────────────────────────────────
    order_size_usd: float = 3.5          # $3.50 per clip for better risk granularity
    min_order_size_usd: float = 2.0      # Below this, don't quote
    max_order_size_usd: float = 100.0

    # ── Inventory ────────────────────────────────────────────────
    max_inventory_shares: float = 20.0   # Tighter inventory cap for $25 budget
    max_net_delta_shares: float = 12.0   # Lower directional cap to reduce exposure
    skew_bps_per_unit: float = 6.0       # Softer skew to avoid overreacting to small imbalances

    # ── Requoting ────────────────────────────────────────────────
    requote_interval_sec: float = 4.0    # 4s quote refresh
    requote_threshold_bps: float = 40.0  # Requote only on 40bps+ fair-value moves
    requote_interval_jitter_enabled: bool = True
    requote_interval_jitter_sec: float = 1.5
    event_requote_enabled: bool = True
    event_pm_mid_threshold_bps: float = 20.0
    event_binance_threshold_bps: float = 35.0
    event_poll_interval_sec: float = 0.25
    event_fallback_interval_sec: float = 8.0

    # ── Order Types ──────────────────────────────────────────────
    gtd_duration_sec: int = 120          # Shorter GTD lifetime keeps stale orders off-book
    heartbeat_interval_sec: int = 5      # heartbeat interval (PM timeout ~10s)
    heartbeat_failures_before_shutdown: int = 3  # Consecutive heartbeat failures before emergency shutdown
    use_post_only: bool = True           # force post-only (maker) orders
    use_gtd: bool = True                 # use GTD order type
    price_jitter_enabled: bool = True
    price_jitter_ticks: int = 1
    size_jitter_enabled: bool = True
    size_jitter_pct: float = 0.20
    min_quote_size_shares: float = 1.0

    # ── Risk ─────────────────────────────────────────────────────
    max_drawdown_usd: float = 4.0        # Tighter session stop-loss for faster shutdown on losses
    volatility_pause_mult: float = 3.0   # pause if vol > N × avg
    max_loss_per_fill_usd: float = 5.0   # Max acceptable loss on single fill
    take_profit_usd: float = 0.0       # Exit if total_pnl >= this (0 = disabled)
    trailing_stop_pct: float = 0.0     # Exit if PnL drops this fraction from peak (0 = disabled)
    allow_short_sells: bool = False    # Safety: close-only SELLs by default (no naked shorting)
    toxic_divergence_threshold: float = 0.10  # No-trade threshold for persistent FV/PM divergence
    toxic_divergence_ticks: int = 8            # Consecutive toxic ticks before quote freeze
    critical_reconcile_drift_shares: float = 1.5  # Immediate pause if |internal-real| exceeds this many shares
    fill_settlement_grace_sec: float = 6.0    # Grace window for PM balance lag right after live fills
    pre_entry_stable_checks: int = 3          # Require this many consecutive strong quality checks before first BUY
    pre_entry_min_quality_score: float = 0.75  # Stricter score floor before opening a fresh position
    pre_entry_max_spread_bps: float = 500.0   # Stricter spread ceiling before first BUY
    pre_entry_max_divergence: float = 0.08    # Max FV/PM divergence before first BUY
    post_fill_entry_guard_sec: float = 45.0   # After a BUY fill, watch quality closely for this long
    post_fill_entry_score_drop: float = 0.20  # Block new BUYs if overall quality drops this much vs fill-time anchor
    post_fill_entry_spread_widen_bps: float = 250.0  # Block new BUYs if spread widens this much vs fill-time anchor
    one_sided_protect_ticks: int = 30          # Start anti-expansion protection before hard-close trigger
    require_flat_start: bool = True            # Block start when wallet already carries non-dust token inventory
    flat_start_max_shares: float = 0.25        # Per-side dust threshold allowed at startup
    taker_fee_rate: float = 0.02  # Fallback only; primary PM taker fee logic lives in pm_fees.py

    # ── Liquidation ─────────────────────────────────────────
    liq_price_floor_enabled: bool = True       # Don't sell below avg entry
    liq_price_floor_margin: float = 0.01       # Min margin above cost basis (1 cent)
    liq_gradual_chunks: int = 3                # Split liquidation into N chunks
    liq_chunk_interval_sec: float = 5.0        # Interval between chunks
    liq_taker_threshold_sec: float = 10.0      # Use taker only in final 10s of the window
    liq_max_discount_from_fv: float = 0.03     # Max discount from FV for limit orders
    liq_abandon_below_floor: bool = True       # Don't sell below floor, let expire
    merge_sell_epsilon: float = 0.01           # Require this premium over $1 before preferring sell-over-merge
    merge_sell_min_depth_pairs: float = 5.0    # Minimum top-book pair depth to choose sell-over-merge

    # ── One-Sided Exposure ─────────────────────────────────
    max_one_sided_ticks: int = 180  # Close if one-sided exposure for this many consecutive ticks (~3min at 1s ticks)
    min_fv_to_quote: float = 0.20  # Don't quote if FV <20% or >80%

    # ── Window Management ────────────────────────────────────────
    entry_settle_sec: float = 15.0     # Wait shortly after window open before entering quotes
    close_window_sec: float = 120.0    # Start closing 2 min before expiry
    auto_next_window: bool = True    # Auto-restart for next window after resolution
    resolution_wait_sec: float = 90.0 # Seconds to wait after expiry before restarting
    redeem_after_resolution_enabled: bool = True
    redeem_retry_interval_sec: float = 15.0

    # ── Rebate / Market Selection ────────────────────────────────
    rebate_scoring_enabled: bool = True
    rebate_check_interval_ticks: int = 20
    rebate_require_scoring: bool = False
    rebate_non_scoring_size_mult: float = 0.70
    rebate_score_timeout_sec: float = 3.0
    market_selector_min_score: float = 0.40
    paired_fill_ioc_enabled: bool = False

    # ── Market Quality ─────────────────────────────────────────
    min_market_quality_score: float = 0.45   # Require stronger liquidity/quality before entry
    min_entry_depth_usd: float = 100.0       # Min book depth to enter
    max_entry_spread_bps: float = 600.0      # Max spread to enter
    session_limit: float = 25.0              # Max USDC per session
    exit_liquidity_threshold: float = 0.15  # Exit if liquidity_score drops below
    quality_check_interval: int = 5         # Check every N ticks

    # ── Enabled ──────────────────────────────────────────────────
    enabled: bool = True  # Master switch

    def to_dict(self) -> dict:
        return asdict(self)

    @classmethod
    def from_dict(cls, d: dict) -> "MMConfig":
        valid = {f.name for f in cls.__dataclass_fields__.values()}
        cfg = cls(**{k: v for k, v in d.items() if k in valid})
        cfg.validate()
        return cfg

    def update(self, **kwargs) -> None:
        """Update parameters at runtime."""
        normalized: dict[str, Any] = {}
        for k, v in kwargs.items():
            target_key = self.UPDATE_ALIASES.get(k, k)
            # Canonical keys win over aliases when both are provided.
            if k in self.UPDATE_ALIASES and target_key in normalized:
                continue
            normalized[target_key] = v

        for k, v in normalized.items():
            if not hasattr(self, k):
                continue
            current = getattr(self, k)
            try:
                if isinstance(current, bool):
                    if isinstance(v, str):
                        parsed = v.strip().lower()
                        if parsed in {"1", "true", "yes", "on"}:
                            casted = True
                        elif parsed in {"0", "false", "no", "off"}:
                            casted = False
                        else:
                            raise ValueError(f"invalid bool string: {v!r}")
                    else:
                        casted = bool(v)
                else:
                    casted = type(current)(v)
            except (TypeError, ValueError):
                log.warning("Ignoring invalid MMConfig value for %s=%r", k, v)
                continue
            setattr(self, k, casted)

        self.validate()

    def _clamp_numeric(self, field_name: str, min_value: float, max_value: float) -> None:
        if not hasattr(self, field_name):
            return

        raw = getattr(self, field_name)
        if isinstance(raw, bool):
            return

        default_value = getattr(type(self), field_name, raw)
        is_int_field = isinstance(default_value, int) and not isinstance(default_value, bool)

        try:
            numeric_value = float(raw)
            if not math.isfinite(numeric_value):
                raise ValueError("non-finite value")
        except (TypeError, ValueError):
            numeric_value = float(min_value)
            log.warning(
                "MMConfig %s=%r is invalid; clamped to %.6g",
                field_name, raw, numeric_value,
            )

        clamped = min(max(numeric_value, min_value), max_value)
        if clamped != numeric_value:
            log.warning(
                "MMConfig %s=%.6g out of bounds [%.6g, %.6g]; clamped to %.6g",
                field_name, numeric_value, min_value, max_value, clamped,
            )

        setattr(self, field_name, int(round(clamped)) if is_int_field else float(clamped))

    def validate(self) -> None:
        """Clamp critical parameters to safe ranges."""
        seen: set[str] = set()
        for raw_field, (min_value, max_value) in self.VALIDATION_BOUNDS.items():
            field_name = self.UPDATE_ALIASES.get(raw_field, raw_field)
            if field_name in seen:
                continue
            seen.add(field_name)
            self._clamp_numeric(field_name, min_value, max_value)
