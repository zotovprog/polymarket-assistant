from __future__ import annotations

from dataclasses import asdict, dataclass, field
from typing import Any, Literal


@dataclass
class PairMarketSnapshot:
    ts: float
    market_id: str
    up_token_id: str
    dn_token_id: str
    time_left_sec: float
    fv_up: float
    fv_dn: float
    fv_confidence: float
    pm_mid_up: float | None
    pm_mid_dn: float | None
    up_best_bid: float | None
    up_best_ask: float | None
    dn_best_bid: float | None
    dn_best_ask: float | None
    up_bid_depth_usd: float
    up_ask_depth_usd: float
    dn_bid_depth_usd: float
    dn_ask_depth_usd: float
    market_quality_score: float
    market_tradeable: bool
    realized_vol_per_min: float = 0.0005
    midpoint_anchor_up: float | None = None
    midpoint_anchor_dn: float | None = None
    model_anchor_up: float | None = None
    model_anchor_dn: float | None = None
    buy_edge_gap_up: float = 0.0
    buy_edge_gap_dn: float = 0.0
    anchor_divergence_up: float = 0.0
    anchor_divergence_dn: float = 0.0
    quote_anchor_mode: str = "midpoint_first"
    divergence_up: float = 0.0
    divergence_dn: float = 0.0
    valuation_source: str = "midpoint_first"
    valuation_regime: str = "normal"
    pm_age_sec: float = 999.0
    underlying_mid_price: float = 0.0
    price_move_bps_1s: float = 0.0
    price_move_bps_5s: float = 0.0
    fast_move_soft_active: bool = False
    fast_move_hard_active: bool = False
    fast_move_pause_active: bool = False


@dataclass
class PairInventoryState:
    up_shares: float
    dn_shares: float
    free_usdc: float
    reserved_usdc: float
    pending_buy_up: float
    pending_buy_dn: float
    pending_sell_up: float
    pending_sell_dn: float
    paired_qty: float
    excess_up_qty: float
    excess_dn_qty: float
    paired_value_usd: float
    excess_up_value_usd: float
    excess_dn_value_usd: float
    total_inventory_value_usd: float
    wallet_total_usdc: float = 0.0
    wallet_reserved_usdc: float = 0.0
    pending_buy_reserved_usdc: float = 0.0
    excess_value_usd: float = 0.0
    signed_excess_value_usd: float = 0.0
    target_pair_value_usd: float = 0.0
    pair_value_ratio: float = 0.0
    pair_value_over_target_usd: float = 0.0
    pair_entry_cost: float = 0.0
    pair_entry_pnl_per_share: float = 0.0
    inventory_pressure_abs: float = 0.0
    inventory_pressure_signed: float = 0.0
    sellable_up_shares: float = 0.0
    sellable_dn_shares: float = 0.0


@dataclass
class QuoteIntent:
    token: str
    side: Literal["BUY", "SELL"]
    price: float
    size: float
    quote_role: Literal[
        "base_bid",
        "base_ask",
        "inventory_rebalance",
        "unwind",
        "emergency_unwind",
    ]
    post_only: bool
    inventory_effect: Literal["helpful", "neutral", "harmful"] = "neutral"
    size_mult: float = 1.0
    price_adjust_ticks: int = 0
    suppressed_reason: str | None = None
    min_rest_sec: float = 0.0
    hold_mode_active: bool = False
    hold_mode_reason: str = ""
    hold_reprice_threshold_ticks: int = 0
    hold_max_age_sec: float = 0.0
    hold_tick_size: float = 0.0


@dataclass
class QuotePlan:
    up_bid: QuoteIntent | None
    up_ask: QuoteIntent | None
    dn_bid: QuoteIntent | None
    dn_ask: QuoteIntent | None
    regime: str
    reason: str
    quote_balance_state: Literal[
        "balanced",
        "helpful_only",
        "harmful_only_blocked",
        "reduced",
        "none",
    ] = "none"
    quote_viability_reason: str = ""
    suppressed_reasons: dict[str, str] = field(default_factory=dict)
    harmful_buy_brake_active: bool = False
    harmful_buy_brake_hits: int = 0
    gross_inventory_brake_active: bool = False
    gross_inventory_brake_hits: int = 0
    pair_over_target_buy_blocks: int = 0
    dual_bid_guard_inventory_budget_hits: int = 0
    midpoint_first_brake_hits: int = 0
    simultaneous_bid_block_prevented: int = 0
    divergence_soft_brake_up_active: bool = False
    divergence_soft_brake_dn_active: bool = False
    divergence_hard_suppress_up_active: bool = False
    divergence_hard_suppress_dn_active: bool = False
    divergence_soft_brake_hits: int = 0
    divergence_hard_suppress_hits: int = 0
    dual_bid_exception_active: bool = False
    dual_bid_exception_reason: str = ""
    quote_anchor_mode: str = "midpoint_first"
    sell_churn_hold_up_active: bool = False
    sell_churn_hold_dn_active: bool = False
    sell_churn_hold_side: str = ""


@dataclass
class RiskRegime:
    soft_mode: Literal["normal", "inventory_skewed", "defensive", "unwind"]
    hard_mode: Literal["none", "emergency_unwind", "halted"]
    reason: str
    inventory_pressure: float
    edge_score: float
    drawdown_pct_budget: float
    target_soft_mode: Literal["normal", "inventory_skewed", "defensive", "unwind"] = "normal"
    inventory_side: Literal["flat", "up", "dn"] = "flat"
    inventory_pressure_abs: float = 0.0
    inventory_pressure_signed: float = 0.0
    quality_pressure: float = 0.0
    target_ratio_pressure: float = 0.0
    early_drawdown_pressure: float = 0.0
    emergency_taker_forced: bool = False
    post_fill_markout_5s_up: float = 0.0
    post_fill_markout_5s_dn: float = 0.0
    negative_spread_capture_streak_up: int = 0
    negative_spread_capture_streak_dn: int = 0
    toxic_fill_streak_up: int = 0
    toxic_fill_streak_dn: int = 0
    side_soft_brake_up_active: bool = False
    side_soft_brake_dn_active: bool = False
    side_reentry_cooldown_up_sec: float = 0.0
    side_reentry_cooldown_dn_sec: float = 0.0
    side_hard_block_up_sec: float = 0.0
    side_hard_block_dn_sec: float = 0.0
    marketability_guard_active: bool = False
    marketability_guard_reason: str = ""
    marketability_guard_up_active: bool = False
    marketability_guard_dn_active: bool = False
    marketability_churn_confirmed: bool = False
    marketability_problem_side: Literal["", "up", "dn"] = ""
    marketability_side_locked: Literal["", "up", "dn"] = ""
    marketability_side_lock_age_sec: float = 0.0
    pair_entry_cost: float = 0.0
    pair_entry_pnl_per_share: float = 0.0
    rolling_markout_up_5s: float = 0.0
    rolling_markout_dn_5s: float = 0.0
    rolling_spread_capture_up: float = 0.0
    rolling_spread_capture_dn: float = 0.0
    fast_move_soft_active: bool = False
    fast_move_hard_active: bool = False
    fast_move_pause_active: bool = False


@dataclass
class ExecutionState:
    open_orders: int = 0
    pending_buy_up: float = 0.0
    pending_buy_dn: float = 0.0
    pending_sell_up: float = 0.0
    pending_sell_dn: float = 0.0
    transport_failures: int = 0
    last_api_error: str = ""
    last_fallback_poll_count: int = 0
    current_order_ids: dict[str, str] = field(default_factory=dict)
    recent_cancelled_sell_reserve_up: float = 0.0
    recent_cancelled_sell_reserve_dn: float = 0.0
    sell_release_lag_up_sec: float = 0.0
    sell_release_lag_dn_sec: float = 0.0
    up_cooldown_sec: float = 0.0
    dn_cooldown_sec: float = 0.0
    active_sell_release_reason: str = ""
    last_sellability_lag_reason: str = ""


@dataclass
class AnalyticsState:
    fill_count: int = 0
    session_pnl: float = 0.0
    session_pnl_equity_usd: float = 0.0
    session_pnl_drawdown_usd: float = 0.0
    session_pnl_operator_usd: float = 0.0
    session_pnl_operator_ema_usd: float = 0.0
    position_mark_value_usd: float = 0.0
    position_mark_value_bid_usd: float = 0.0
    position_mark_value_mid_usd: float = 0.0
    portfolio_mark_value_usd: float = 0.0
    portfolio_mark_value_mid_usd: float = 0.0
    tradeable_portfolio_value_usd: float = 0.0
    pair_entry_cost: float = 0.0
    pair_entry_pnl_per_share: float = 0.0
    anchor_divergence_up: float = 0.0
    anchor_divergence_dn: float = 0.0
    buy_edge_gap_up: float = 0.0
    buy_edge_gap_dn: float = 0.0
    quote_shift_from_mid_up: float = 0.0
    quote_shift_from_mid_dn: float = 0.0
    post_fill_markout_5s_up: float = 0.0
    post_fill_markout_5s_dn: float = 0.0
    rolling_markout_up_5s: float = 0.0
    rolling_markout_dn_5s: float = 0.0
    rolling_spread_capture_up: float = 0.0
    rolling_spread_capture_dn: float = 0.0
    toxic_fill_streak_up: int = 0
    toxic_fill_streak_dn: int = 0
    side_soft_brake_up_active: bool = False
    side_soft_brake_dn_active: bool = False
    negative_spread_capture_streak_up: int = 0
    negative_spread_capture_streak_dn: int = 0
    side_reentry_cooldown_up_sec: float = 0.0
    side_reentry_cooldown_dn_sec: float = 0.0
    side_hard_block_up_sec: float = 0.0
    side_hard_block_dn_sec: float = 0.0
    quote_anchor_mode: str = "midpoint_first"
    midpoint_reference_mode: str = "midpoint_first"
    pnl_calc_mode: str = "wallet_total_plus_mark"
    pnl_mark_basis: str = "conservative_bid"
    pnl_updated_ts: float = 0.0
    markout_1s: float = 0.0
    markout_5s: float = 0.0
    spread_capture_usd: float = 0.0
    fill_rate: float = 0.0
    quote_presence_ratio: float = 0.0
    excess_value_usd: float = 0.0
    target_pair_value_usd: float = 0.0
    pair_value_ratio: float = 0.0
    pair_value_over_target_usd: float = 0.0
    target_ratio_activation_usd_effective: float = 0.0
    target_ratio_cap_active: bool = False
    target_ratio_cap_hits_60s: int = 0
    target_ratio_pressure: float = 0.0
    gross_inventory_brake_active: bool = False
    gross_inventory_brake_hits_60s: int = 0
    pair_over_target_buy_blocks_60s: int = 0
    dual_bid_guard_inventory_budget_hits_60s: int = 0
    inventory_pressure_abs: float = 0.0
    inventory_pressure_signed: float = 0.0
    inventory_half_life_sec: float = 0.0
    four_quote_presence_ratio: float = 0.0
    helpful_quote_count: int = 0
    harmful_quote_count: int = 0
    quote_balance_state: str = "none"
    min_viable_clip_usd: float = 0.0
    quote_viability_reason: str = ""
    quoting_ratio_60s: float = 0.0
    inventory_skewed_ratio_60s: float = 0.0
    defensive_ratio_60s: float = 0.0
    unwind_ratio_60s: float = 0.0
    emergency_unwind_ratio_60s: float = 0.0
    four_quote_ratio_60s: float = 0.0
    mm_effective_ratio_60s: float = 0.0
    dual_bid_ratio_60s: float = 0.0
    one_sided_bid_streak_outside: int = 0
    harmful_suppressed_count_60s: int = 0
    target_ratio_breaches_60s: int = 0
    defensive_to_unwind_count_window: int = 0
    quote_cancel_to_fill_ratio_60s: float = 0.0
    maker_cross_guard_hits_60s: int = 0
    dual_bid_guard_hits_60s: int = 0
    dual_bid_guard_fail_hits_60s: int = 0
    midpoint_first_brake_hits_60s: int = 0
    simultaneous_bid_block_prevented_hits_60s: int = 0
    divergence_soft_brake_up_active: bool = False
    divergence_soft_brake_dn_active: bool = False
    divergence_hard_suppress_up_active: bool = False
    divergence_hard_suppress_dn_active: bool = False
    divergence_soft_brake_hits_60s: int = 0
    divergence_hard_suppress_hits_60s: int = 0
    max_buy_edge_gap_60s: float = 0.0
    price_move_bps_1s: float = 0.0
    price_move_bps_5s: float = 0.0
    fast_move_soft_active: bool = False
    fast_move_hard_active: bool = False
    fast_move_pause_active: bool = False
    dual_bid_exception_active: bool = False
    dual_bid_exception_reason: str = ""
    harmful_buy_brake_active: bool = False
    harmful_buy_brake_hits_60s: int = 0
    emergency_taker_forced: bool = False
    emergency_taker_forced_hits_60s: int = 0
    emergency_no_progress_sec: float = 0.0
    unwind_deferred_hits_60s: int = 0
    forced_unwind_extreme_excess_hits_60s: int = 0
    mm_regime_degraded_reason: str = ""
    marketability_guard_active: bool = False
    marketability_guard_reason: str = ""
    marketability_churn_confirmed: bool = False
    marketability_problem_side: str = ""
    marketability_side_locked: str = ""
    marketability_side_lock_age_sec: float = 0.0
    sell_churn_hold_up_active: bool = False
    sell_churn_hold_dn_active: bool = False
    sell_churn_hold_side: str = ""
    sell_churn_hold_order_age_up_sec: float = 0.0
    sell_churn_hold_order_age_dn_sec: float = 0.0
    sell_churn_hold_reprice_due_up: bool = False
    sell_churn_hold_reprice_due_dn: bool = False
    sell_churn_hold_reprice_suppressed_hits_60s: int = 0
    sell_churn_hold_cancel_avoided_hits_60s: int = 0
    collateral_warning_hits_60s: int = 0
    sell_skip_cooldown_hits_60s: int = 0
    up_collateral_warning_streak: int = 0
    dn_collateral_warning_streak: int = 0
    up_sell_skip_cooldown_streak: int = 0
    dn_sell_skip_cooldown_streak: int = 0
    collateral_warning_streak_current: int = 0
    sell_skip_cooldown_streak_current: int = 0
    execution_churn_ratio_60s: float = 0.0
    untradeable_tolerated_samples_60s: int = 0
    failure_bucket_current: str = ""
    execution_replay_blocker_hint: str = ""
    diagnostic_no_guards_active: bool = False
    post_terminal_cleanup_grace_active: bool = False
    unwind_target_mismatch_ticks: int = 0
    unwind_target_mismatch_sec: float = 0.0
    unwind_exit_armed: bool = False
    emergency_exit_armed: bool = False
    recent_fills: list[dict[str, Any]] = field(default_factory=list)


@dataclass
class QuoteViabilitySummary:
    any_quote: bool = False
    four_quotes: bool = False
    helpful_count: int = 0
    harmful_count: int = 0
    helpful_only: bool = False
    harmful_only: bool = False
    four_quote_presence_ratio: float = 0.0
    quote_balance_state: str = "none"


@dataclass
class SoftTransitionResult:
    lifecycle: Literal[
        "bootstrapping",
        "quoting",
        "inventory_skewed",
        "defensive",
        "unwind",
        "emergency_unwind",
        "expired",
        "halted",
    ]
    effective_soft_mode: Literal["normal", "inventory_skewed", "defensive", "unwind"]
    target_soft_mode: Literal["normal", "inventory_skewed", "defensive", "unwind"]
    progress_ratio: float = 0.0
    no_progress: bool = False
    reason: str = ""
    unwind_exit_armed: bool = False
    emergency_exit_armed: bool = False
    unwind_deferred: bool = False
    forced_unwind_extreme_excess: bool = False


@dataclass
class HealthState:
    reconcile_status: str = "unknown"
    heartbeat_ok: bool = True
    transport_ok: bool = True
    last_api_error: str = ""
    last_api_error_op: str = ""
    last_api_error_status_code: int = 0
    last_api_error_raw: str = ""
    last_fallback_poll_count: int = 0
    true_drift: bool = False
    residual_inventory_failure: bool = False
    sellability_lag_active: bool = False
    wallet_snapshot_stale: bool = False
    true_drift_age_sec: float = 0.0
    true_drift_no_progress_sec: float = 0.0
    drawdown_breach_ticks: int = 0
    drawdown_breach_age_sec: float = 0.0
    drawdown_breach_active: bool = False
    drawdown_threshold_usd_effective: float = 0.0
    post_terminal_cleanup_grace_active: bool = False
    drift_evidence: dict[str, Any] = field(default_factory=dict)


@dataclass
class EngineState:
    lifecycle: Literal[
        "bootstrapping",
        "quoting",
        "inventory_skewed",
        "defensive",
        "unwind",
        "emergency_unwind",
        "expired",
        "halted",
    ]
    market: PairMarketSnapshot | None
    inventory: PairInventoryState
    risk: RiskRegime
    current_quotes: QuotePlan
    execution: ExecutionState
    analytics: AnalyticsState
    health: HealthState
    alerts: list[dict[str, Any]] = field(default_factory=list)

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)
