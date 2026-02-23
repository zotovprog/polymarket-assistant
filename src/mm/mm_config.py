"""Market Making configuration with runtime-adjustable parameters."""
from __future__ import annotations
from dataclasses import dataclass, asdict
import config as app_config


@dataclass
class MMConfig:
    """All MM parameters — can be updated at runtime via API."""

    # ── Spread ───────────────────────────────────────────────────
    half_spread_bps: float = app_config.MM_HALF_SPREAD_BPS
    min_spread_bps: float = 50.0     # Absolute minimum half-spread
    max_spread_bps: float = 500.0    # Absolute maximum half-spread
    vol_spread_mult: float = 1.5     # Widen spread by this factor in high-vol

    # ── Sizing ───────────────────────────────────────────────────
    order_size_usd: float = app_config.MM_ORDER_SIZE_USD
    min_order_size_usd: float = 2.0  # Below this, don't quote
    max_order_size_usd: float = 100.0

    # ── Inventory ────────────────────────────────────────────────
    max_inventory_shares: float = app_config.MM_MAX_INVENTORY
    skew_bps_per_unit: float = app_config.MM_SKEW_BPS_PER_UNIT

    # ── Requoting ────────────────────────────────────────────────
    requote_interval_sec: float = app_config.MM_REQUOTE_SEC
    requote_threshold_bps: float = app_config.MM_REQUOTE_THRESH_BPS

    # ── Order Types ──────────────────────────────────────────────
    gtd_duration_sec: int = app_config.MM_GTD_DURATION_SEC
    heartbeat_interval_sec: int = app_config.MM_HEARTBEAT_SEC
    use_post_only: bool = app_config.MM_USE_POST_ONLY
    use_gtd: bool = app_config.MM_USE_GTD

    # ── Risk ─────────────────────────────────────────────────────
    max_drawdown_usd: float = app_config.MM_MAX_DRAWDOWN_USD
    volatility_pause_mult: float = app_config.MM_VOL_PAUSE_MULT
    max_loss_per_fill_usd: float = 5.0  # Max acceptable loss on single fill
    take_profit_usd: float = 0.0       # Exit if total_pnl >= this (0 = disabled)
    trailing_stop_pct: float = 0.0     # Exit if PnL drops this fraction from peak (0 = disabled)

    # ── Liquidation ─────────────────────────────────────────
    liq_price_floor_enabled: bool = True       # Don't sell below avg entry
    liq_price_floor_margin: float = 0.01       # Min margin above cost basis (1 cent)
    liq_gradual_chunks: int = 3                # Split liquidation into N chunks
    liq_chunk_interval_sec: float = 5.0        # Interval between chunks
    liq_taker_threshold_sec: float = 20.0      # Switch to taker when < N seconds left
    liq_max_discount_from_fv: float = 0.03     # Max discount from FV for limit orders
    liq_abandon_below_floor: bool = True       # Don't sell below floor, let expire

    # ── One-Sided Exposure ─────────────────────────────────
    max_one_sided_ticks: int = 30  # Close if one-sided exposure for this many consecutive ticks

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
        return cls(**{k: v for k, v in d.items() if k in valid})

    def update(self, **kwargs) -> None:
        """Update parameters at runtime."""
        for k, v in kwargs.items():
            if hasattr(self, k):
                setattr(self, k, type(getattr(self, k))(v))
