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
        "order_size_usd": (1.0, 500.0),
        "max_position_usd": (5.0, 5000.0),
        "layers": (1.0, 10.0),
        "layer_spacing_bps": (1.0, 1000.0),
        "skew_bps_per_unit": (0.0, 500.0),
        "refresh_interval_s": (0.5, 60.0),
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
    }

    # ── Spread ───────────────────────────────────────────────────
    half_spread_bps: float = 300.0       # 3% half-spread default (safe for 15m binaries)
    min_spread_bps: float = 50.0         # Absolute minimum half-spread
    max_spread_bps: float = 1000.0       # Absolute maximum half-spread (raised for gamma widening)
    vol_spread_mult: float = 1.5         # Widen spread by this factor in high-vol

    # ── Sizing ───────────────────────────────────────────────────
    order_size_usd: float = 10.0         # USD per side
    min_order_size_usd: float = 2.0      # Below this, don't quote
    max_order_size_usd: float = 100.0

    # ── Inventory ────────────────────────────────────────────────
    max_inventory_shares: float = 25.0   # max shares one-sided
    skew_bps_per_unit: float = 15.0      # skew per share of net delta

    # ── Requoting ────────────────────────────────────────────────
    requote_interval_sec: float = 1.0    # seconds between requote checks (was 2.0)
    requote_threshold_bps: float = 10.0   # min price move to requote (raised for 1s tick interval)

    # ── Order Types ──────────────────────────────────────────────
    gtd_duration_sec: int = 300          # GTD order lifetime (5 min)
    heartbeat_interval_sec: int = 5      # heartbeat interval (PM timeout ~10s)
    use_post_only: bool = True           # force post-only (maker) orders
    use_gtd: bool = True                 # use GTD order type

    # ── Risk ─────────────────────────────────────────────────────
    max_drawdown_usd: float = 15.0       # max session drawdown (conservative default)
    volatility_pause_mult: float = 3.0   # pause if vol > N × avg
    max_loss_per_fill_usd: float = 5.0   # Max acceptable loss on single fill
    take_profit_usd: float = 0.0       # Exit if total_pnl >= this (0 = disabled)
    trailing_stop_pct: float = 0.0     # Exit if PnL drops this fraction from peak (0 = disabled)
    taker_fee_rate: float = 0.02  # Taker fee rate (2% safe default for PM crypto markets)

    # ── Liquidation ─────────────────────────────────────────
    liq_price_floor_enabled: bool = True       # Don't sell below avg entry
    liq_price_floor_margin: float = 0.01       # Min margin above cost basis (1 cent)
    liq_gradual_chunks: int = 3                # Split liquidation into N chunks
    liq_chunk_interval_sec: float = 5.0        # Interval between chunks
    liq_taker_threshold_sec: float = 20.0      # Switch to taker when < N seconds left
    liq_max_discount_from_fv: float = 0.03     # Max discount from FV for limit orders
    liq_abandon_below_floor: bool = True       # Don't sell below floor, let expire

    # ── One-Sided Exposure ─────────────────────────────────
    max_one_sided_ticks: int = 180  # Close if one-sided exposure for this many consecutive ticks (~3min at 1s ticks)
    min_fv_to_quote: float = 0.15  # Don't quote a side if its FV < this (market already decided)

    # ── Window Management ────────────────────────────────────────
    close_window_sec: float = 30.0    # Seconds before expiry: enter closing mode
    auto_next_window: bool = True    # Auto-restart for next window after resolution
    resolution_wait_sec: float = 90.0 # Seconds to wait after expiry before restarting

    # ── Market Quality ─────────────────────────────────────────
    min_market_quality_score: float = 0.3   # Min overall score to enter window
    min_entry_depth_usd: float = 50.0       # Min book depth to enter
    max_entry_spread_bps: float = 800.0     # Max spread to enter
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
