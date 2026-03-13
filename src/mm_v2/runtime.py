from __future__ import annotations

import asyncio
from dataclasses import dataclass, replace
import time
from typing import Any

from mm_shared.heartbeat import HeartbeatManager
from mm_shared.runtime_metrics import runtime_metrics
from mm_shared.types import Fill, MarketInfo

from .config import (
    DRAWDOWN_CONFIRM_MIN_AGE_SEC,
    DRAWDOWN_CONFIRM_TICKS,
    DRAWDOWN_RESET_HYSTERESIS_USD,
    MM_REGIME_DEGRADED_CONFIRM_SEC,
    MM_REGIME_WINDOW_SEC,
    MMConfigV2,
)
from .execution_policy import ExecutionPolicyV2
from .order_tracker import OrderTrackerV2
from .pair_valuation import PairValuationEngine
from .pm_gateway import PMGateway
from .quote_policy import QuoteContext, QuotePolicyV2
from .reconcile import ReconcileV2
from .risk_kernel import HardSafetyKernel
from .state_api import serialize_engine_state
from .state_machine import StateMachineV2
from .types import (
    AnalyticsState,
    EngineState,
    ExecutionState,
    HealthState,
    PairInventoryState,
    PairMarketSnapshot,
    QuotePlan,
    QuoteViabilitySummary,
    RiskRegime,
    SoftTransitionResult,
)


@dataclass
class _PendingMarkoutEval:
    ts: float
    token_side: str
    side: str
    price: float


class MarketMakerV2:
    OPERATOR_PNL_EMA_ALPHA = 0.20
    ROLLING_MARKOUT_EMA_ALPHA = 0.20
    POST_FILL_MARKOUT_EVAL_SEC = 5.0
    TOXIC_FILL_MARKOUT_TICKS = 3
    NEGATIVE_MARKOUT_TICKS = 1
    HARD_BLOCK_NEGATIVE_STREAK = 3
    HARD_BLOCK_TOXIC_STREAK = 2
    SIDE_REENTRY_COOLDOWN_SEC = 12.0
    MARKETABILITY_CHURN_HOLD_SEC = 12.0
    MARKETABILITY_SIDE_LOCK_SEC = 20.0
    MARKETABILITY_SIDE_SWITCH_SCORE_MARGIN = 2

    def __init__(
        self,
        feed_state: Any,
        clob_client: Any,
        config: MMConfigV2,
        *,
        force_normal_soft_mode_paper: bool = False,
        force_normal_no_guards_paper: bool = False,
    ):
        self.feed_state = feed_state
        self.config = config
        self._force_normal_soft_mode_paper = bool(force_normal_soft_mode_paper)
        self._force_normal_no_guards_paper = bool(force_normal_no_guards_paper)
        self.gateway = PMGateway(clob_client, config)
        self.valuation = PairValuationEngine(config)
        self.reconcile = ReconcileV2(config)
        self.risk_kernel = HardSafetyKernel(config)
        self.state_machine = StateMachineV2(config)
        self.tracker = OrderTrackerV2()
        self.execution_policy = ExecutionPolicyV2(
            self.gateway,
            self.tracker,
            requote_threshold_bps=float(config.requote_threshold_bps),
        )
        self.market: MarketInfo | None = None
        self._running = False
        self._task: asyncio.Task | None = None
        self._started_at = 0.0
        self._heartbeat_failed = False
        self._alerts: dict[str, dict[str, Any]] = {}
        self._snapshot_callbacks: list[Any] = []
        self._fill_callbacks: list[Any] = []
        self._fills: list[Fill] = []
        self._pending_markout_evals: list[_PendingMarkoutEval] = []
        self._excess_history: list[tuple[float, float]] = []
        self._quote_presence_history: list[tuple[float, tuple[bool, bool]]] = []
        self._mode_history: list[tuple[float, str]] = []
        self._lifecycle_history: list[tuple[float, str]] = []
        self._harmful_suppressed_history: list[tuple[float, int]] = []
        self._harmful_buy_brake_history: list[tuple[float, int]] = []
        self._gross_inventory_brake_history: list[tuple[float, int]] = []
        self._pair_over_target_buy_block_history: list[tuple[float, int]] = []
        self._target_ratio_breach_history: list[tuple[float, int]] = []
        self._order_removal_history: list[tuple[float, int]] = []
        self._maker_cross_guard_history: list[tuple[float, int]] = []
        self._dual_bid_outside_sample_history: list[tuple[float, int]] = []
        self._dual_bid_outside_success_history: list[tuple[float, int]] = []
        self._dual_bid_guard_hits_history: list[tuple[float, int]] = []
        self._dual_bid_guard_fail_history: list[tuple[float, int]] = []
        self._dual_bid_guard_inventory_budget_history: list[tuple[float, int]] = []
        self._midpoint_first_brake_history: list[tuple[float, int]] = []
        self._simultaneous_bid_block_prevented_history: list[tuple[float, int]] = []
        self._divergence_soft_brake_history: list[tuple[float, int]] = []
        self._divergence_hard_suppress_history: list[tuple[float, int]] = []
        self._buy_edge_gap_history: list[tuple[float, float]] = []
        self._sell_churn_hold_reprice_suppressed_history: list[tuple[float, int]] = []
        self._sell_churn_hold_cancel_avoided_history: list[tuple[float, int]] = []
        self._untradeable_tolerated_history: list[tuple[float, int]] = []
        self._emergency_taker_forced_history: list[tuple[float, int]] = []
        self._one_sided_bid_streak_outside: int = 0
        self._unwind_deferred_history: list[tuple[float, int]] = []
        self._forced_unwind_extreme_excess_history: list[tuple[float, int]] = []
        self._prev_active_order_ids: set[str] = set()
        self._starting_portfolio = 0.0
        self._starting_usdc = 0.0
        self._session_pnl = 0.0
        self._session_pnl_equity_usd = 0.0
        self._session_pnl_drawdown_usd = 0.0
        self._session_pnl_operator_ema_usd = 0.0
        self._session_pnl_operator_usd = 0.0
        self._portfolio_mark_value_mid_usd = 0.0
        self._operator_pnl_initialized = False
        self._avg_entry_price_up = 0.0
        self._avg_entry_price_dn = 0.0
        self._entry_position_up = 0.0
        self._entry_position_dn = 0.0
        self._total_bought_up = 0.0
        self._total_bought_dn = 0.0
        self._total_sold_up = 0.0
        self._total_sold_dn = 0.0
        self._rolling_markout_up_5s = 0.0
        self._rolling_markout_dn_5s = 0.0
        self._rolling_spread_capture_up = 0.0
        self._rolling_spread_capture_dn = 0.0
        self._pending_markout_evals.clear()
        self._post_fill_markout_5s_up = 0.0
        self._post_fill_markout_5s_dn = 0.0
        self._negative_spread_capture_streak_up = 0
        self._negative_spread_capture_streak_dn = 0
        self._toxic_fill_streak_up = 0
        self._toxic_fill_streak_dn = 0
        self._side_reentry_blocked_until_up = 0.0
        self._side_reentry_blocked_until_dn = 0.0
        self._drawdown_breach_ticks = 0
        self._drawdown_breach_started_ts = 0.0
        self._drawdown_breach_active = False
        self._emergency_progress_baseline_excess = 0.0
        self._emergency_no_progress_started_ts = 0.0
        self._emergency_no_progress_ticks = 0
        self._emergency_taker_forced = False
        self._mm_regime_degraded_started_ts = 0.0
        self._mm_regime_degraded_reason = ""
        self._unwind_target_mismatch_ticks = 0
        self._unwind_target_mismatch_started_ts = 0.0
        self._marketability_churn_hold_until = 0.0
        self._marketability_side_locked = ""
        self._marketability_side_locked_until = 0.0
        self._marketability_side_lock_started_ts = 0.0
        self._underlying_mid_history: list[tuple[float, float]] = []
        self._fast_move_pause_until: float = 0.0
        self._last_merge_attempt_ts: float = 0.0
        self._orders_cancelled_for_merge: bool = False
        self._merge_consecutive_failures: int = 0
        self._consecutive_balance_failures: int = 0
        self._balance_api_degraded: bool = False
        self._private_key: str = ""
        self._last_snapshot: PairMarketSnapshot | None = None
        self._post_fill_markout_5s_up = 0.0
        self._post_fill_markout_5s_dn = 0.0
        self._negative_spread_capture_streak_up = 0
        self._negative_spread_capture_streak_dn = 0
        self._toxic_fill_streak_up = 0
        self._toxic_fill_streak_dn = 0
        self._side_reentry_blocked_until_up = 0.0
        self._side_reentry_blocked_until_dn = 0.0
        self._last_inventory = PairInventoryState(
            up_shares=0.0,
            dn_shares=0.0,
            free_usdc=0.0,
            reserved_usdc=0.0,
            pending_buy_up=0.0,
            pending_buy_dn=0.0,
            pending_sell_up=0.0,
            pending_sell_dn=0.0,
            paired_qty=0.0,
            excess_up_qty=0.0,
            excess_dn_qty=0.0,
            paired_value_usd=0.0,
            excess_up_value_usd=0.0,
            excess_dn_value_usd=0.0,
            total_inventory_value_usd=0.0,
            excess_value_usd=0.0,
            signed_excess_value_usd=0.0,
            inventory_pressure_abs=0.0,
            inventory_pressure_signed=0.0,
        )
        self._last_risk = RiskRegime(
            soft_mode="normal",
            hard_mode="none",
            target_soft_mode="normal",
            reason="boot",
            inventory_pressure=0.0,
            edge_score=0.0,
            drawdown_pct_budget=1.0,
            inventory_side="flat",
            inventory_pressure_abs=0.0,
            inventory_pressure_signed=0.0,
            quality_pressure=0.0,
        )
        self._last_plan = QuotePlan(None, None, None, None, "boot", "boot")
        self._last_execution = ExecutionState()
        self._last_analytics = AnalyticsState()
        self._last_health = HealthState()
        self._last_terminal_reason: str = ""
        self._last_terminal_ts: float = 0.0
        self._last_terminal_wallet_total_usdc: float = 0.0
        self._last_terminal_up_shares: float = 0.0
        self._last_terminal_dn_shares: float = 0.0
        self._last_terminal_pnl_equity_usd: float = 0.0
        self._terminal_liquidation_active: bool = False
        self._terminal_liquidation_started_ts: float = 0.0
        self._terminal_liquidation_round_idx: int = 0
        self._terminal_liquidation_attempted_orders: int = 0
        self._terminal_liquidation_placed_orders: int = 0
        self._terminal_liquidation_remaining_up: float = 0.0
        self._terminal_liquidation_remaining_dn: float = 0.0
        self._terminal_liquidation_done: bool = False
        self._terminal_liquidation_reason: str = ""
        self._post_terminal_cleanup_grace_started_ts: float = 0.0
        self._true_drift_started_ts: float = 0.0
        self._true_drift_last_progress_ts: float = 0.0
        self._true_drift_best_exposure: float = 0.0
        self.heartbeat = HeartbeatManager(
            clob_client,
            5,
            failure_threshold=3,
            on_failure=self._on_heartbeat_failure,
            should_send=lambda: bool(self.gateway.active_order_ids()),
        )

    def _provisional_quote_viability(self, plan: QuotePlan) -> QuoteViabilitySummary:
        helpful = 0
        harmful = 0
        active = 0
        for intent in (plan.up_bid, plan.up_ask, plan.dn_bid, plan.dn_ask):
            if not intent:
                continue
            active += 1
            if intent.inventory_effect == "helpful":
                helpful += 1
            elif intent.inventory_effect == "harmful":
                harmful += 1
        _, rolling_four_quote_presence = self._quote_presence_ratio(
            extra_present=(
                any([plan.up_bid, plan.up_ask, plan.dn_bid, plan.dn_ask]),
                all([plan.up_bid, plan.up_ask, plan.dn_bid, plan.dn_ask]),
            )
        )
        return QuoteViabilitySummary(
            any_quote=active > 0,
            four_quotes=active == 4,
            helpful_count=helpful,
            harmful_count=harmful,
            helpful_only=helpful > 0 and harmful == 0,
            harmful_only=harmful > 0 and helpful == 0,
            four_quote_presence_ratio=rolling_four_quote_presence,
            quote_balance_state=str(getattr(plan, "quote_balance_state", "none") or "none"),
        )

    def set_market(self, market: MarketInfo) -> None:
        self.market = market
        self.gateway.set_market(market)

    def set_alert(self, source: str, message: str, level: str = "warning") -> None:
        self._alerts[source] = {
            "source": source,
            "level": level,
            "message": message,
            "ts": time.time(),
        }

    def clear_alert(self, source: str) -> None:
        self._alerts.pop(source, None)

    def on_snapshot(self, callback) -> None:
        self._snapshot_callbacks.append(callback)

    def on_fill(self, callback) -> None:
        self._fill_callbacks.append(callback)

    def _fill_token_type(self, fill: Fill) -> str:
        if not self.market:
            return ""
        if fill.token_id == self.market.up_token_id:
            return "up"
        if fill.token_id == self.market.dn_token_id:
            return "dn"
        return ""

    def _emit_fill_callbacks(self, fill: Fill) -> None:
        token_type = self._fill_token_type(fill)
        for callback in self._fill_callbacks:
            try:
                callback(fill, token_type)
            except Exception:
                pass

    def _record_fill_entry_metrics(self, fill: Fill, token_side: str) -> None:
        if token_side not in {"up", "dn"}:
            return
        size = max(0.0, float(fill.size))
        price = max(0.0, float(fill.price))
        fee = max(0.0, float(getattr(fill, "fee", 0.0) or 0.0))
        if token_side == "up":
            avg_attr = "_avg_entry_price_up"
            pos_attr = "_entry_position_up"
            bought_attr = "_total_bought_up"
            sold_attr = "_total_sold_up"
        else:
            avg_attr = "_avg_entry_price_dn"
            pos_attr = "_entry_position_dn"
            bought_attr = "_total_bought_dn"
            sold_attr = "_total_sold_dn"

        current_position = max(0.0, float(getattr(self, pos_attr)))
        current_avg = max(0.0, float(getattr(self, avg_attr)))
        if str(fill.side).upper() == "BUY":
            weighted_cost = current_avg * current_position
            new_position = current_position + size
            if new_position > 0.0:
                setattr(self, avg_attr, (weighted_cost + (price * size) + fee) / new_position)
            else:
                setattr(self, avg_attr, 0.0)
            setattr(self, pos_attr, new_position)
            setattr(self, bought_attr, float(getattr(self, bought_attr)) + size)
            return

        matched_size = min(size, current_position)
        new_position = max(0.0, current_position - matched_size)
        setattr(self, pos_attr, new_position)
        setattr(self, sold_attr, float(getattr(self, sold_attr)) + size)
        if new_position <= 1e-9:
            setattr(self, avg_attr, 0.0)

    @staticmethod
    def _ema(prev: float, new_value: float, alpha: float) -> float:
        if abs(float(prev)) <= 1e-12:
            return float(new_value)
        return (float(alpha) * float(new_value)) + ((1.0 - float(alpha)) * float(prev))

    async def _on_heartbeat_failure(self) -> None:
        self._heartbeat_failed = True
        self.set_alert("heartbeat_failure_v2", "Heartbeat failure", level="error")

    def _coalesce_wallet_snapshot(
        self,
        *,
        up: float | None,
        dn: float | None,
        total_usdc: float | None,
        available_usdc: float | None,
    ) -> tuple[float, float, float, float, bool]:
        """Use trusted local state when PM balance endpoints return partial data.

        PM balance reads may transiently fail (timeouts/transport). Treating such
        reads as literal zero creates false drift and false drawdown transitions.
        """
        stale = False
        expected_up, expected_dn = self.reconcile.expected_balances()

        up_v = up
        if up_v is None:
            stale = True
            up_v = expected_up if expected_up is not None else float(self._last_inventory.up_shares)

        dn_v = dn
        if dn_v is None:
            stale = True
            dn_v = expected_dn if expected_dn is not None else float(self._last_inventory.dn_shares)

        total_v = total_usdc
        if total_v is None:
            stale = True
            total_v = float(self._last_inventory.free_usdc + self._last_inventory.reserved_usdc)

        available_v = available_usdc
        if available_v is None:
            stale = True
            available_v = float(self._last_inventory.free_usdc)

        up_f = max(0.0, float(up_v or 0.0))
        dn_f = max(0.0, float(dn_v or 0.0))
        total_f = max(0.0, float(total_v or 0.0))
        available_f = max(0.0, float(available_v or 0.0))
        if available_f > total_f:
            available_f = total_f
        return up_f, dn_f, total_f, available_f, stale

    async def start(self) -> None:
        if self._running:
            return
        if not self.market:
            raise RuntimeError("market is not set")
        self._running = True
        self._started_at = time.time()
        self._session_pnl = 0.0
        self._session_pnl_equity_usd = 0.0
        self._session_pnl_drawdown_usd = 0.0
        self._session_pnl_operator_ema_usd = 0.0
        self._session_pnl_operator_usd = 0.0
        self._portfolio_mark_value_mid_usd = 0.0
        self._operator_pnl_initialized = False
        self._drawdown_breach_ticks = 0
        self._drawdown_breach_started_ts = 0.0
        self._drawdown_breach_active = False
        self._avg_entry_price_up = 0.0
        self._avg_entry_price_dn = 0.0
        self._entry_position_up = 0.0
        self._entry_position_dn = 0.0
        self._total_bought_up = 0.0
        self._total_bought_dn = 0.0
        self._total_sold_up = 0.0
        self._total_sold_dn = 0.0
        self._rolling_markout_up_5s = 0.0
        self._rolling_markout_dn_5s = 0.0
        self._rolling_spread_capture_up = 0.0
        self._rolling_spread_capture_dn = 0.0
        self._mm_regime_degraded_started_ts = 0.0
        self._mm_regime_degraded_reason = ""
        self._unwind_target_mismatch_ticks = 0
        self._unwind_target_mismatch_started_ts = 0.0
        self._lifecycle_history.clear()
        self._harmful_suppressed_history.clear()
        self._harmful_buy_brake_history.clear()
        self._gross_inventory_brake_history.clear()
        self._pair_over_target_buy_block_history.clear()
        self._target_ratio_breach_history.clear()
        self._order_removal_history.clear()
        self._maker_cross_guard_history.clear()
        self._dual_bid_outside_sample_history.clear()
        self._dual_bid_outside_success_history.clear()
        self._dual_bid_guard_hits_history.clear()
        self._dual_bid_guard_fail_history.clear()
        self._dual_bid_guard_inventory_budget_history.clear()
        self._midpoint_first_brake_history.clear()
        self._simultaneous_bid_block_prevented_history.clear()
        self._divergence_soft_brake_history.clear()
        self._divergence_hard_suppress_history.clear()
        self._buy_edge_gap_history.clear()
        self._untradeable_tolerated_history.clear()
        self._emergency_taker_forced_history.clear()
        self._one_sided_bid_streak_outside = 0
        self._unwind_deferred_history.clear()
        self._forced_unwind_extreme_excess_history.clear()
        self._emergency_progress_baseline_excess = 0.0
        self._emergency_no_progress_started_ts = 0.0
        self._emergency_no_progress_ticks = 0
        self._emergency_taker_forced = False
        self._last_terminal_reason = ""
        self._last_terminal_ts = 0.0
        self._last_terminal_wallet_total_usdc = 0.0
        self._last_terminal_up_shares = 0.0
        self._last_terminal_dn_shares = 0.0
        self._last_terminal_pnl_equity_usd = 0.0
        self._terminal_liquidation_active = False
        self._terminal_liquidation_started_ts = 0.0
        self._terminal_liquidation_round_idx = 0
        self._terminal_liquidation_attempted_orders = 0
        self._terminal_liquidation_placed_orders = 0
        self._terminal_liquidation_remaining_up = 0.0
        self._terminal_liquidation_remaining_dn = 0.0
        self._terminal_liquidation_done = False
        self._terminal_liquidation_reason = ""
        self._post_terminal_cleanup_grace_started_ts = 0.0
        self._underlying_mid_history.clear()
        self._fast_move_pause_until = 0.0
        self._last_merge_attempt_ts = 0.0
        self._orders_cancelled_for_merge = False
        self._merge_consecutive_failures = 0
        self._consecutive_balance_failures = 0
        self._balance_api_degraded = False
        up_raw, dn_raw, total_usdc_raw, available_usdc_raw = await self.gateway.get_wallet_balances()
        up, dn, total_usdc, available_usdc, stale_wallet = self._coalesce_wallet_snapshot(
            up=up_raw,
            dn=dn_raw,
            total_usdc=total_usdc_raw,
            available_usdc=available_usdc_raw,
        )
        if stale_wallet and self._last_inventory.free_usdc <= 0 and self._last_inventory.up_shares <= 0 and self._last_inventory.dn_shares <= 0:
            self._running = False
            raise RuntimeError("Unable to fetch initial PM wallet snapshot (token/USDC balances unavailable)")
        if stale_wallet:
            self.set_alert(
                "wallet_snapshot_stale_v2",
                "PM wallet snapshot partial/unavailable; using local fallback balances",
                level="warning",
            )
        else:
            self.clear_alert("wallet_snapshot_stale_v2")
        self.reconcile.start_session(up, dn)
        self._starting_usdc = float(total_usdc)
        fv_up = float(getattr(self.feed_state, "pm_up", 0.5) or 0.5)
        fv_dn = float(getattr(self.feed_state, "pm_dn", 0.5) or max(0.01, 1.0 - fv_up))
        self._starting_portfolio = float(total_usdc) + up * fv_up + dn * fv_dn
        self.heartbeat.start()
        self._prev_active_order_ids = set(self.gateway.active_order_ids())
        self._task = asyncio.create_task(self._run_loop())

    async def stop(self, *, liquidate: bool = True) -> dict[str, Any]:
        liquidation_result: dict[str, Any] = {
            "enabled": bool(liquidate),
            "attempted_orders": 0,
            "placed_orders": 0,
            "remaining_up": 0.0,
            "remaining_dn": 0.0,
            "done": not bool(liquidate),
            "reason": "skipped" if not liquidate else "",
        }
        self._running = False
        if self._task and not self._task.done():
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass
        self._task = None
        await self.gateway.cancel_all()
        if liquidate:
            try:
                liq = await self.gateway.emergency_flatten_on_stop()
                liquidation_result = dict(liq or {})
                liquidation_result["enabled"] = True
                if not bool(liq.get("done", True)):
                    self.set_alert(
                        "stop_liquidation_v2",
                        (
                            "Stop liquidation incomplete: "
                            f"remaining_up={liq.get('remaining_up')} "
                            f"remaining_dn={liq.get('remaining_dn')}"
                        ),
                        level="warning",
                    )
                else:
                    self.clear_alert("stop_liquidation_v2")
            except Exception as exc:
                self.set_alert("stop_liquidation_v2", f"Stop liquidation failed: {exc}", level="error")
                liquidation_result = {
                    "enabled": True,
                    "attempted_orders": 0,
                    "placed_orders": 0,
                    "remaining_up": 0.0,
                    "remaining_dn": 0.0,
                    "done": False,
                    "reason": f"exception: {exc}",
                }
        await self.gateway.cancel_all()
        await self.heartbeat.stop()
        if not self._last_terminal_reason:
            self._set_terminal_reason("manual_stop")
        if isinstance(liquidation_result, dict):
            self._capture_terminal_state(
                up_shares=float(liquidation_result.get("remaining_up", self._last_inventory.up_shares) or 0.0),
                dn_shares=float(liquidation_result.get("remaining_dn", self._last_inventory.dn_shares) or 0.0),
            )
        return liquidation_result

    def fills_page(self, *, limit: int = 50, offset: int = 0) -> dict[str, Any]:
        total = len(self._fills)
        if limit <= 0:
            page: list[Fill] = []
        elif offset > 0:
            page = self._fills[-(offset + limit):-offset or None]
        else:
            page = self._fills[-limit:]
        return {
            "fills": [
                {
                    "ts": f.ts,
                    "side": f.side,
                    "token_id": f.token_id,
                    "token_type": (
                        "up"
                        if self.market and f.token_id == self.market.up_token_id
                        else "dn"
                    ),
                    "price": f.price,
                    "size": f.size,
                    "fee": f.fee,
                    "is_maker": f.is_maker,
                }
                for f in reversed(page)
            ],
            "total": total,
        }

    async def _run_loop(self) -> None:
        while self._running:
            runtime_metrics.incr("mmv2.loop")
            try:
                await self._tick()
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                self.set_alert("runtime_v2", f"V2 tick error: {exc}", level="error")
            await asyncio.sleep(float(self.config.tick_interval_sec))

    def _position_mark_values(
        self,
        *,
        snapshot: PairMarketSnapshot,
        inventory: PairInventoryState,
    ) -> tuple[float, float]:
        up_mid = float(snapshot.pm_mid_up) if snapshot.pm_mid_up is not None else float(snapshot.fv_up)
        dn_mid = float(snapshot.pm_mid_dn) if snapshot.pm_mid_dn is not None else float(snapshot.fv_dn)
        up_mid = max(0.0, up_mid)
        dn_mid = max(0.0, dn_mid)

        up_bid = float(snapshot.up_best_bid) if snapshot.up_best_bid is not None else up_mid
        dn_bid = float(snapshot.dn_best_bid) if snapshot.dn_best_bid is not None else dn_mid
        up_bid = max(0.0, up_bid)
        dn_bid = max(0.0, dn_bid)

        up_shares = max(0.0, float(inventory.up_shares))
        dn_shares = max(0.0, float(inventory.dn_shares))
        position_mark_bid = up_shares * up_bid + dn_shares * dn_bid
        position_mark_mid = up_shares * up_mid + dn_shares * dn_mid
        return position_mark_bid, position_mark_mid

    def _portfolio_components(
        self,
        *,
        inventory: PairInventoryState,
        total_usdc: float,
        snapshot: PairMarketSnapshot,
    ) -> tuple[float, float, float, float, float]:
        position_mark_bid, position_mark_mid = self._position_mark_values(
            snapshot=snapshot,
            inventory=inventory,
        )
        portfolio_mark_value = max(0.0, float(total_usdc)) + position_mark_bid
        portfolio_mark_value_mid = max(0.0, float(total_usdc)) + position_mark_mid
        tradeable_portfolio_value = max(0.0, float(inventory.free_usdc)) + position_mark_bid
        return (
            position_mark_bid,
            position_mark_mid,
            portfolio_mark_value,
            portfolio_mark_value_mid,
            tradeable_portfolio_value,
        )

    def _update_session_pnl(
        self,
        inventory: PairInventoryState,
        *,
        total_usdc: float,
        snapshot: PairMarketSnapshot,
    ) -> tuple[float, float, float, float, float]:
        # PnL must use wallet-total USDC + marked inventory value.
        # Pending/reserved order bookkeeping must not move session PnL.
        (
            position_mark_bid,
            position_mark_mid,
            current_portfolio,
            current_portfolio_mid,
            tradeable_portfolio,
        ) = self._portfolio_components(inventory=inventory, total_usdc=total_usdc, snapshot=snapshot)
        equity_pnl = current_portfolio - self._starting_portfolio
        operator_pnl = current_portfolio_mid - self._starting_portfolio
        drawdown_portfolio = max(0.0, float(total_usdc)) + ((float(position_mark_bid) + float(position_mark_mid)) * 0.5)
        drawdown_pnl = drawdown_portfolio - self._starting_portfolio
        self._session_pnl_equity_usd = float(equity_pnl)
        self._session_pnl_drawdown_usd = float(drawdown_pnl)
        # Backward-compatible alias.
        self._session_pnl = self._session_pnl_equity_usd
        self._portfolio_mark_value_mid_usd = float(current_portfolio_mid)
        if not self._operator_pnl_initialized:
            self._session_pnl_operator_ema_usd = float(operator_pnl)
            self._operator_pnl_initialized = True
        else:
            alpha = float(self.OPERATOR_PNL_EMA_ALPHA)
            self._session_pnl_operator_ema_usd = (
                alpha * float(operator_pnl)
                + (1.0 - alpha) * self._session_pnl_operator_ema_usd
            )
        self._session_pnl_operator_usd = self._session_pnl_operator_ema_usd
        return (
            position_mark_bid,
            position_mark_mid,
            current_portfolio,
            current_portfolio_mid,
            tradeable_portfolio,
        )

    @staticmethod
    def _token_side_for_fill(fill: Fill, market: MarketInfo | None) -> str:
        if not market:
            return ""
        if fill.token_id == market.up_token_id:
            return "up"
        if fill.token_id == market.dn_token_id:
            return "dn"
        return ""

    @staticmethod
    def _snapshot_anchor_price(snapshot: PairMarketSnapshot, token_side: str) -> float:
        if token_side == "up":
            return max(
                0.01,
                float(
                    snapshot.midpoint_anchor_up
                    if snapshot.midpoint_anchor_up is not None
                    else snapshot.pm_mid_up
                    if snapshot.pm_mid_up is not None
                    else snapshot.fv_up
                ),
            )
        return max(
            0.01,
            float(
                snapshot.midpoint_anchor_dn
                if snapshot.midpoint_anchor_dn is not None
                else snapshot.pm_mid_dn
                if snapshot.pm_mid_dn is not None
                else snapshot.fv_dn
            ),
        )

    def _side_reentry_cooldown_sec(self, token_side: str, *, now: float) -> float:
        if token_side == "up":
            return max(0.0, float(self._side_reentry_blocked_until_up - now))
        return max(0.0, float(self._side_reentry_blocked_until_dn - now))

    def _side_soft_brake_active(self, token_side: str) -> bool:
        if token_side == "up":
            return bool(
                int(self._negative_spread_capture_streak_up) > 0
                or int(self._toxic_fill_streak_up) > 0
            )
        return bool(
            int(self._negative_spread_capture_streak_dn) > 0
            or int(self._toxic_fill_streak_dn) > 0
        )

    def _has_material_inventory(self, inventory: PairInventoryState) -> bool:
        if not self.market:
            return False
        min_size = max(0.0, float(self.market.min_order_size or 0.0))
        return bool(
            float(inventory.up_shares) >= min_size
            or float(inventory.dn_shares) >= min_size
        )

    def _terminal_liquidation_timeout_sec(self) -> float:
        return max(1.0, float(self.config.emergency_unwind_timeout_sec))

    def _terminal_liquidation_start_sec(self) -> float:
        return max(
            float(self.config.emergency_taker_start_sec),
            float(self.config.terminal_liquidation_start_sec),
        )

    def _update_fast_move_state(self, *, snapshot: PairMarketSnapshot, now: float) -> None:
        underlying_mid = max(0.0, float(getattr(snapshot, "underlying_mid_price", 0.0) or 0.0))
        if underlying_mid <= 0.0:
            snapshot.price_move_bps_1s = 0.0
            snapshot.price_move_bps_5s = 0.0
            snapshot.fast_move_soft_active = False
            snapshot.fast_move_hard_active = False
            snapshot.fast_move_pause_active = now < float(self._fast_move_pause_until)
            return
        self._underlying_mid_history.append((float(now), underlying_mid))
        cutoff = float(now) - 6.0
        self._underlying_mid_history = [
            (ts, px)
            for ts, px in self._underlying_mid_history
            if ts >= cutoff and px > 0.0
        ]

        def _move_bps(lookback_sec: float) -> float:
            target_ts = float(now) - float(lookback_sec)
            reference = underlying_mid
            found_reference = False
            for ts, px in self._underlying_mid_history:
                if ts <= target_ts:
                    reference = float(px)
                    found_reference = True
                else:
                    break
            if not found_reference and self._underlying_mid_history:
                reference = float(self._underlying_mid_history[0][1])
            return abs(underlying_mid - reference) / max(1e-9, reference) * 10000.0

        move_bps_1s = _move_bps(1.0)
        move_bps_5s = _move_bps(5.0)
        soft_active = bool(
            move_bps_1s >= float(self.config.fast_move_soft_bps_1s)
            or move_bps_5s >= float(self.config.fast_move_soft_bps_5s)
        )
        hard_active = bool(
            move_bps_1s >= float(self.config.fast_move_hard_bps_1s)
            or move_bps_5s >= float(self.config.fast_move_hard_bps_5s)
        )
        if hard_active:
            self._fast_move_pause_until = max(
                float(self._fast_move_pause_until),
                float(now) + float(self.config.fast_move_pause_sec),
            )
        snapshot.price_move_bps_1s = float(move_bps_1s)
        snapshot.price_move_bps_5s = float(move_bps_5s)
        snapshot.fast_move_soft_active = bool(soft_active)
        snapshot.fast_move_hard_active = bool(hard_active)
        snapshot.fast_move_pause_active = bool(now < float(self._fast_move_pause_until))

    async def _maybe_merge_pairs(
        self,
        *,
        snapshot: PairMarketSnapshot,
        inventory: PairInventoryState,
        risk: RiskRegime,
        now: float,
    ) -> None:
        if not self.market or not self.market.condition_id:
            return
        if self._force_normal_no_guards_active(snapshot=snapshot, risk=risk):
            return
        if getattr(self, "_merge_consecutive_failures", 0) >= 5:
            return
        time_left_sec = float(snapshot.time_left_sec)
        terminal_window_active = time_left_sec <= float(self._terminal_liquidation_start_sec())
        has_paired_shares = float(inventory.paired_qty) > 1e-9
        pair_entry_cost = max(0.0, float(getattr(inventory, "pair_entry_cost", 0.0) or 0.0))
        merge_for_underwater_pairs = bool(pair_entry_cost > 1.0 and has_paired_shares)
        merge_for_terminal_pairs = bool(terminal_window_active and has_paired_shares)
        merge_override_active = bool(merge_for_underwater_pairs or merge_for_terminal_pairs)
        unwind_merge_window_active = bool(
            time_left_sec <= float(self.config.unwind_window_sec)
            and not terminal_window_active
        )
        if not (unwind_merge_window_active or merge_override_active):
            return
        hard_mode = str(risk.hard_mode or "")
        if hard_mode != "none" and not (
            hard_mode == "emergency_unwind" and merge_override_active
        ):
            return
        if not has_paired_shares:
            return
        if (
            float(inventory.paired_qty) < float(self.market.min_order_size)
            and not merge_override_active
        ):
            return
        if (float(now) - float(self._last_merge_attempt_ts)) < 10.0:
            return
        if not hasattr(self.gateway.order_mgr.client, "_orders") and not str(self._private_key or "").strip():
            return
        self._last_merge_attempt_ts = float(now)
        cancelled_orders = False
        try:
            await self.gateway.cancel_all()
            self.tracker.refresh_from_active(self.gateway.active_orders())
            cancelled_orders = True
            result = await self.gateway.merge_pairs(
                condition_id=str(self.market.condition_id),
                amount_shares=float(inventory.paired_qty),
                private_key=str(self._private_key or ""),
            )
        except Exception as exc:
            self.set_alert("merge_pairs_v2", f"Pair merge failed: {exc}", level="warning")
            self._merge_consecutive_failures = getattr(self, "_merge_consecutive_failures", 0) + 1
            if cancelled_orders:
                self._orders_cancelled_for_merge = True
            return
        if bool((result or {}).get("success")):
            self.clear_alert("merge_pairs_v2")
            self.gateway.invalidate_balance_caches()
            self._merge_consecutive_failures = 0
        elif (result or {}).get("error"):
            self.set_alert("merge_pairs_v2", f"Pair merge skipped: {result.get('error')}", level="warning")
            self._merge_consecutive_failures = getattr(self, "_merge_consecutive_failures", 0) + 1
        if cancelled_orders:
            self._orders_cancelled_for_merge = True

    def _terminal_liquidation_elapsed(self, *, now: float) -> float:
        if self._terminal_liquidation_started_ts <= 0.0:
            return 0.0
        return max(0.0, float(now - self._terminal_liquidation_started_ts))

    def _force_normal_soft_mode_active(
        self,
        *,
        snapshot: PairMarketSnapshot,
        risk: RiskRegime,
    ) -> bool:
        return bool(
            (self._force_normal_soft_mode_paper or self._force_normal_no_guards_paper)
            and str(risk.hard_mode or "") == "none"
            and float(snapshot.time_left_sec) > float(self._terminal_liquidation_start_sec())
        )

    def _force_normal_no_guards_active(
        self,
        *,
        snapshot: PairMarketSnapshot,
        risk: RiskRegime,
    ) -> bool:
        return bool(
            self._force_normal_no_guards_paper
            and str(risk.hard_mode or "") == "none"
            and float(snapshot.time_left_sec) > float(self._terminal_liquidation_start_sec())
        )

    def _coerce_terminal_drawdown_risk(
        self,
        *,
        snapshot: PairMarketSnapshot,
        inventory: PairInventoryState,
        risk: RiskRegime,
        health: HealthState,
    ) -> RiskRegime:
        if risk.hard_mode != "halted":
            return risk
        if not bool(getattr(health, "drawdown_breach_active", False)):
            return risk
        if not str(risk.reason or "").startswith("hard drawdown"):
            return risk
        terminal_window_active = float(snapshot.time_left_sec) <= float(self._terminal_liquidation_start_sec())
        if not terminal_window_active:
            return risk
        residual_inventory = max(
            float(inventory.up_shares),
            float(inventory.dn_shares),
            float(self._terminal_liquidation_remaining_up),
            float(self._terminal_liquidation_remaining_dn),
        )
        if residual_inventory <= 1e-9 and not self._terminal_liquidation_active:
            return risk
        return replace(risk, hard_mode="emergency_unwind")

    def _apply_side_markout_result(
        self,
        *,
        token_side: str,
        markout: float,
        tick_size: float,
        now: float,
    ) -> None:
        negative_threshold = -max(float(tick_size) * float(self.NEGATIVE_MARKOUT_TICKS), 0.01)
        toxic_threshold = -max(float(tick_size), 0.01) * float(self.TOXIC_FILL_MARKOUT_TICKS)
        is_negative = markout <= negative_threshold
        is_toxic = markout <= toxic_threshold

        if token_side == "up":
            self._post_fill_markout_5s_up = float(markout)
            self._rolling_markout_up_5s = self._ema(
                self._rolling_markout_up_5s,
                float(markout),
                float(self.ROLLING_MARKOUT_EMA_ALPHA),
            )
            self._rolling_spread_capture_up = self._ema(
                self._rolling_spread_capture_up,
                float(markout),
                float(self.ROLLING_MARKOUT_EMA_ALPHA),
            )
            self._negative_spread_capture_streak_up = (
                self._negative_spread_capture_streak_up + 1
                if is_negative
                else max(0, self._negative_spread_capture_streak_up - 1)
            )
            self._toxic_fill_streak_up = (
                self._toxic_fill_streak_up + 1
                if is_toxic
                else max(0, self._toxic_fill_streak_up - 1)
            )
            if (
                self._toxic_fill_streak_up >= int(self.HARD_BLOCK_TOXIC_STREAK)
                or self._negative_spread_capture_streak_up >= int(self.HARD_BLOCK_NEGATIVE_STREAK)
            ):
                self._side_reentry_blocked_until_up = max(
                    float(self._side_reentry_blocked_until_up),
                    now + float(self.SIDE_REENTRY_COOLDOWN_SEC),
                )
            return

        self._post_fill_markout_5s_dn = float(markout)
        self._rolling_markout_dn_5s = self._ema(
            self._rolling_markout_dn_5s,
            float(markout),
            float(self.ROLLING_MARKOUT_EMA_ALPHA),
        )
        self._rolling_spread_capture_dn = self._ema(
            self._rolling_spread_capture_dn,
            float(markout),
            float(self.ROLLING_MARKOUT_EMA_ALPHA),
        )
        self._negative_spread_capture_streak_dn = (
            self._negative_spread_capture_streak_dn + 1
            if is_negative
            else max(0, self._negative_spread_capture_streak_dn - 1)
        )
        self._toxic_fill_streak_dn = (
            self._toxic_fill_streak_dn + 1
            if is_toxic
            else max(0, self._toxic_fill_streak_dn - 1)
        )
        if (
            self._toxic_fill_streak_dn >= int(self.HARD_BLOCK_TOXIC_STREAK)
            or self._negative_spread_capture_streak_dn >= int(self.HARD_BLOCK_NEGATIVE_STREAK)
        ):
            self._side_reentry_blocked_until_dn = max(
                float(self._side_reentry_blocked_until_dn),
                now + float(self.SIDE_REENTRY_COOLDOWN_SEC),
            )

    def _update_pending_markouts(
        self,
        *,
        snapshot: PairMarketSnapshot,
        now: float,
    ) -> None:
        if not self._pending_markout_evals:
            return
        keep: list[_PendingMarkoutEval] = []
        tick_size = float(self.market.tick_size if self.market else 0.01)
        for item in self._pending_markout_evals:
            if now - float(item.ts) < float(self.POST_FILL_MARKOUT_EVAL_SEC):
                keep.append(item)
                continue
            anchor_price = self._snapshot_anchor_price(snapshot, str(item.token_side))
            if str(item.side).upper() == "BUY":
                markout = float(anchor_price) - float(item.price)
            else:
                markout = float(item.price) - float(anchor_price)
            self._apply_side_markout_result(
                token_side=str(item.token_side),
                markout=float(markout),
                tick_size=tick_size,
                now=now,
            )
        self._pending_markout_evals = keep

    def _quote_shift_from_mid(
        self,
        *,
        plan: QuotePlan,
        snapshot: PairMarketSnapshot,
        token_side: str,
    ) -> float:
        anchor_price = self._snapshot_anchor_price(snapshot, token_side)
        quotes = (
            (plan.up_bid, plan.up_ask)
            if token_side == "up"
            else (plan.dn_bid, plan.dn_ask)
        )
        shifts = [abs(float(intent.price) - anchor_price) for intent in quotes if intent is not None]
        return max(shifts) if shifts else 0.0

    def _build_health(
        self,
        *,
        api_stats: dict[str, Any] | None = None,
        sellability_lag_active: bool = False,
        wallet_snapshot_stale: bool = False,
        post_terminal_cleanup_grace_active: bool = False,
        true_drift_age_sec: float = 0.0,
        true_drift_no_progress_sec: float = 0.0,
        drawdown_breach_ticks: int = 0,
        drawdown_breach_age_sec: float = 0.0,
        drawdown_breach_active: bool = False,
        drawdown_threshold_usd_effective: float = 0.0,
    ) -> HealthState:
        api_stats = dict(api_stats or self.gateway.api_error_stats() or {})
        recent = api_stats.get("recent") or []
        last_message = ""
        last_api_error_op = ""
        last_api_error_status_code = 0
        last_api_error_raw = ""
        if recent:
            last_event = recent[-1] if isinstance(recent[-1], dict) else {}
            last_message = str(last_event.get("message") or "")
            last_api_error_op = str(last_event.get("op") or "")
            status_raw = last_event.get("status_code")
            try:
                last_api_error_status_code = int(status_raw) if status_raw is not None else 0
            except (TypeError, ValueError):
                last_api_error_status_code = 0
            details = last_event.get("details") if isinstance(last_event, dict) else {}
            if isinstance(details, dict):
                last_api_error_raw = str(details.get("raw_error") or "")
        transport_totals = api_stats.get("transport_total_by_op")
        if not isinstance(transport_totals, dict):
            transport_totals = {}
        has_recent_transport_window = "transport_recent_60s_total" in api_stats
        recent_transport_failures = int(api_stats.get("transport_recent_60s_total") or 0)
        if not has_recent_transport_window:
            recent_transport_failures = int(sum(int(v or 0) for v in transport_totals.values()))
        transport_ok = recent_transport_failures < int(self.config.max_transport_failures)
        last_fallback = int(getattr(self.gateway.order_mgr, "_last_fallback_poll_count", 0))
        return HealthState(
            reconcile_status=self.reconcile.status,
            heartbeat_ok=not self._heartbeat_failed,
            transport_ok=transport_ok,
            last_api_error=last_message,
            last_api_error_op=last_api_error_op,
            last_api_error_status_code=last_api_error_status_code,
            last_api_error_raw=last_api_error_raw,
            last_fallback_poll_count=last_fallback,
            true_drift=self.reconcile.true_drift,
            residual_inventory_failure=bool(self._alerts.get("residual_inventory_v2")),
            sellability_lag_active=bool(sellability_lag_active),
            wallet_snapshot_stale=bool(wallet_snapshot_stale),
            post_terminal_cleanup_grace_active=bool(post_terminal_cleanup_grace_active),
            true_drift_age_sec=max(0.0, float(true_drift_age_sec)),
            true_drift_no_progress_sec=max(0.0, float(true_drift_no_progress_sec)),
            drawdown_breach_ticks=max(0, int(drawdown_breach_ticks)),
            drawdown_breach_age_sec=max(0.0, float(drawdown_breach_age_sec)),
            drawdown_breach_active=bool(drawdown_breach_active),
            drawdown_threshold_usd_effective=max(0.0, float(drawdown_threshold_usd_effective)),
            drift_evidence=self.reconcile.drift_evidence.to_dict(),
        )

    def _set_terminal_reason(self, reason: str) -> None:
        text = str(reason or "").strip()
        if not text:
            return
        self._last_terminal_reason = text
        self._last_terminal_ts = time.time()
        self._capture_terminal_state()

    def _capture_terminal_state(
        self,
        *,
        wallet_total_usdc: float | None = None,
        up_shares: float | None = None,
        dn_shares: float | None = None,
        pnl_equity_usd: float | None = None,
    ) -> None:
        self._last_terminal_wallet_total_usdc = float(
            self._last_inventory.wallet_total_usdc if wallet_total_usdc is None else wallet_total_usdc
        )
        self._last_terminal_up_shares = float(
            self._last_inventory.up_shares if up_shares is None else up_shares
        )
        self._last_terminal_dn_shares = float(
            self._last_inventory.dn_shares if dn_shares is None else dn_shares
        )
        self._last_terminal_pnl_equity_usd = float(
            self._session_pnl_equity_usd if pnl_equity_usd is None else pnl_equity_usd
        )

    def _terminal_cleanup_dust_threshold(self) -> float:
        if self.market is None:
            return 5.0
        return float(self.market.min_order_size)

    def _post_terminal_cleanup_grace_state(self, *, now: float | None = None) -> tuple[bool, float]:
        ref_now = time.time() if now is None else float(now)
        active = bool(
            self._terminal_liquidation_active
            and self._terminal_liquidation_done
            and max(
                float(self._terminal_liquidation_remaining_up),
                float(self._terminal_liquidation_remaining_dn),
            ) < self._terminal_cleanup_dust_threshold()
        )
        if active:
            if self._post_terminal_cleanup_grace_started_ts <= 0.0:
                self._post_terminal_cleanup_grace_started_ts = ref_now
            return True, max(0.0, ref_now - self._post_terminal_cleanup_grace_started_ts)
        self._post_terminal_cleanup_grace_started_ts = 0.0
        return False, 0.0

    def _update_true_drift_progress(
        self,
        inventory: PairInventoryState,
        *,
        post_terminal_cleanup_grace_active: bool = False,
    ) -> tuple[float, float]:
        if post_terminal_cleanup_grace_active or not self.reconcile.true_drift:
            self._true_drift_started_ts = 0.0
            self._true_drift_last_progress_ts = 0.0
            self._true_drift_best_exposure = 0.0
            return 0.0, 0.0

        now = time.time()
        exposure = max(0.0, float(inventory.up_shares) + float(inventory.dn_shares))
        if self._true_drift_started_ts <= 0.0:
            self._true_drift_started_ts = now
            self._true_drift_last_progress_ts = now
            self._true_drift_best_exposure = exposure
        elif exposure < (self._true_drift_best_exposure - 0.25):
            self._true_drift_best_exposure = exposure
            self._true_drift_last_progress_ts = now

        age = max(0.0, now - self._true_drift_started_ts)
        no_progress = max(0.0, now - self._true_drift_last_progress_ts)
        return age, no_progress

    def _update_drawdown_breach(self, equity_pnl: float) -> tuple[int, float, bool]:
        effective_drawdown_usd = float(self.config.effective_hard_drawdown_usd())
        if effective_drawdown_usd <= 0.0:
            self._drawdown_breach_ticks = 0
            self._drawdown_breach_started_ts = 0.0
            self._drawdown_breach_active = False
            return 0, 0.0, False

        now = time.time()
        threshold = -effective_drawdown_usd
        reset_level = threshold + float(DRAWDOWN_RESET_HYSTERESIS_USD)

        if equity_pnl <= threshold:
            if self._drawdown_breach_started_ts <= 0.0:
                self._drawdown_breach_started_ts = now
                self._drawdown_breach_ticks = 1
            else:
                self._drawdown_breach_ticks += 1
        elif equity_pnl >= reset_level:
            self._drawdown_breach_ticks = 0
            self._drawdown_breach_started_ts = 0.0
            self._drawdown_breach_active = False
            return 0, 0.0, False

        age = (
            max(0.0, now - self._drawdown_breach_started_ts)
            if self._drawdown_breach_started_ts > 0.0
            else 0.0
        )
        self._drawdown_breach_active = (
            self._drawdown_breach_ticks >= int(DRAWDOWN_CONFIRM_TICKS)
            and age >= float(DRAWDOWN_CONFIRM_MIN_AGE_SEC)
        )
        return int(self._drawdown_breach_ticks), float(age), bool(self._drawdown_breach_active)

    def _update_emergency_taker_force(
        self,
        *,
        hard_mode: str,
        excess_value_usd: float,
    ) -> tuple[bool, float]:
        """Enable taker failover in emergency unwind only after confirmed no-progress."""
        if hard_mode != "emergency_unwind":
            self._emergency_progress_baseline_excess = max(0.0, float(excess_value_usd))
            self._emergency_no_progress_started_ts = 0.0
            self._emergency_no_progress_ticks = 0
            self._emergency_taker_forced = False
            return False, 0.0

        now = time.time()
        current_excess = max(0.0, float(excess_value_usd))
        if self._emergency_progress_baseline_excess <= 0.0:
            self._emergency_progress_baseline_excess = current_excess

        baseline = max(1e-9, float(self._emergency_progress_baseline_excess))
        progress = current_excess <= (baseline * 0.95)
        if progress:
            self._emergency_progress_baseline_excess = current_excess
            self._emergency_no_progress_started_ts = 0.0
            self._emergency_no_progress_ticks = 0
            self._emergency_taker_forced = False
            return False, 0.0

        if self._emergency_no_progress_started_ts <= 0.0:
            self._emergency_no_progress_started_ts = now
            self._emergency_no_progress_ticks = 1
        else:
            self._emergency_no_progress_ticks += 1
        no_progress_sec = max(0.0, now - self._emergency_no_progress_started_ts)
        self._emergency_taker_forced = bool(
            self._emergency_no_progress_ticks >= 3
            and no_progress_sec >= 8.0
        )
        return bool(self._emergency_taker_forced), float(no_progress_sec)

    def _prune_history(
        self,
        entries: list[tuple[float, Any]],
        *,
        max_age_sec: float = 120.0,
        now: float | None = None,
    ) -> None:
        ref_now = float(now if now is not None else time.time())
        cutoff = ref_now - max_age_sec
        while entries and entries[0][0] < cutoff:
            entries.pop(0)

    def _estimate_inventory_half_life(self) -> float:
        if len(self._excess_history) < 2:
            return 0.0
        start_ts, start_excess = self._excess_history[0]
        if start_excess <= 0:
            return 0.0
        target = start_excess * 0.5
        for ts, excess in self._excess_history[1:]:
            if excess <= target:
                return max(0.0, ts - start_ts)
        return 0.0

    def _quote_presence_ratio(self, extra_present: tuple[bool, bool] | None = None) -> tuple[float, float]:
        history = list(self._quote_presence_history)
        if extra_present is not None:
            history.append((time.time(), extra_present))
        if not history:
            return 0.0, 0.0
        total = len(history)
        any_quote = sum(1 for _, present in history if present[0])
        four_quote = sum(1 for _, present in history if present[1])
        return any_quote / total, four_quote / total

    def _quote_presence_ratio_window(
        self,
        *,
        window_sec: float,
        now: float | None = None,
    ) -> tuple[float, float]:
        ref_now = float(now if now is not None else time.time())
        cutoff = ref_now - max(1.0, float(window_sec))
        history = [entry for entry in self._quote_presence_history if entry[0] >= cutoff]
        if not history:
            return 0.0, 0.0
        total = len(history)
        any_quote = sum(1 for _, present in history if present[0])
        four_quote = sum(1 for _, present in history if present[1])
        return any_quote / total, four_quote / total

    def _lifecycle_ratios(
        self,
        *,
        window_sec: float,
        now: float | None = None,
    ) -> dict[str, float]:
        ref_now = float(now if now is not None else time.time())
        cutoff = ref_now - max(1.0, float(window_sec))
        history = [entry for entry in self._lifecycle_history if entry[0] >= cutoff]
        if not history:
            return {
                "quoting_ratio_60s": 0.0,
                "inventory_skewed_ratio_60s": 0.0,
                "defensive_ratio_60s": 0.0,
                "unwind_ratio_60s": 0.0,
                "emergency_unwind_ratio_60s": 0.0,
            }
        total = float(len(history))
        counts = {
            "quoting": 0,
            "inventory_skewed": 0,
            "defensive": 0,
            "unwind": 0,
            "emergency_unwind": 0,
        }
        for _, lifecycle in history:
            if lifecycle in counts:
                counts[lifecycle] += 1
        return {
            "quoting_ratio_60s": counts["quoting"] / total,
            "inventory_skewed_ratio_60s": counts["inventory_skewed"] / total,
            "defensive_ratio_60s": counts["defensive"] / total,
            "unwind_ratio_60s": counts["unwind"] / total,
            "emergency_unwind_ratio_60s": counts["emergency_unwind"] / total,
        }

    @staticmethod
    def _window_sum(
        history: list[tuple[float, int]],
        *,
        window_sec: float,
        now: float | None = None,
    ) -> int:
        ref_now = float(now if now is not None else time.time())
        cutoff = ref_now - max(1.0, float(window_sec))
        return int(sum(int(value or 0) for ts, value in history if ts >= cutoff))

    @staticmethod
    def _window_max(
        history: list[tuple[float, float]],
        *,
        window_sec: float,
        now: float | None = None,
    ) -> float:
        ref_now = float(now if now is not None else time.time())
        cutoff = ref_now - max(1.0, float(window_sec))
        values = [float(value or 0.0) for ts, value in history if ts >= cutoff]
        if not values:
            return 0.0
        return float(max(values))

    def _fills_count_window(self, *, window_sec: float, now: float | None = None) -> int:
        ref_now = float(now if now is not None else time.time())
        cutoff = ref_now - max(1.0, float(window_sec))
        return int(sum(1 for fill in self._fills if float(getattr(fill, "ts", 0.0) or 0.0) >= cutoff))

    def _quote_cancel_to_fill_ratio(self, *, window_sec: float, now: float | None = None) -> float:
        removals = self._window_sum(self._order_removal_history, window_sec=window_sec, now=now)
        fills = self._fills_count_window(window_sec=window_sec, now=now)
        return float(removals / max(1, fills))

    def _defensive_to_unwind_count(self, *, window_sec: float, now: float | None = None) -> int:
        ref_now = float(now if now is not None else time.time())
        cutoff = ref_now - max(1.0, float(window_sec))
        history = [entry for entry in self._lifecycle_history if entry[0] >= cutoff]
        if len(history) < 2:
            return 0
        transitions = 0
        prev = history[0][1]
        for _, lifecycle in history[1:]:
            if prev == "defensive" and lifecycle == "unwind":
                transitions += 1
            prev = lifecycle
        return transitions

    @staticmethod
    def _marketability_side_score(marketability_state: dict[str, Any], side: str) -> int:
        prefix = f"{side}_"
        return int(
            max(
                int(marketability_state.get(f"{prefix}collateral_warning_streak") or 0),
                int(marketability_state.get(f"{prefix}sell_skip_cooldown_streak") or 0),
                int(marketability_state.get(f"{prefix}collateral_warning_hits_60s") or 0),
                int(marketability_state.get(f"{prefix}sell_skip_cooldown_hits_60s") or 0),
                int(round(float(marketability_state.get(f"{prefix}execution_churn_ratio_60s") or 0.0) * 10.0)),
            )
        )

    def _marketability_locked_side(self, *, now: float | None = None) -> str:
        ref_now = float(time.time() if now is None else now)
        if (
            self._marketability_side_locked in {"up", "dn"}
            and ref_now < float(self._marketability_side_locked_until)
        ):
            return str(self._marketability_side_locked)
        return ""

    def _marketability_side_lock_age_sec(self, *, now: float | None = None) -> float:
        locked_side = self._marketability_locked_side(now=now)
        if locked_side not in {"up", "dn"} or self._marketability_side_lock_started_ts <= 0.0:
            return 0.0
        ref_now = float(time.time() if now is None else now)
        return max(0.0, ref_now - float(self._marketability_side_lock_started_ts))

    def _set_marketability_side_lock(self, side: str, *, now: float | None = None) -> None:
        ref_now = float(time.time() if now is None else now)
        if side not in {"up", "dn"}:
            self._marketability_side_locked = ""
            self._marketability_side_locked_until = 0.0
            self._marketability_side_lock_started_ts = 0.0
            return
        if self._marketability_side_locked != side:
            self._marketability_side_lock_started_ts = ref_now
        elif self._marketability_side_lock_started_ts <= 0.0:
            self._marketability_side_lock_started_ts = ref_now
        self._marketability_side_locked = str(side)
        self._marketability_side_locked_until = ref_now + float(self.MARKETABILITY_SIDE_LOCK_SEC)

    def _clear_marketability_side_lock(self) -> None:
        self._marketability_side_locked = ""
        self._marketability_side_locked_until = 0.0
        self._marketability_side_lock_started_ts = 0.0

    def _has_active_sell_churn_hold_order(self, side: str | None = None) -> bool:
        slot_keys = []
        if side in {"up", "dn"}:
            slot_keys.append(f"{side}_sell")
        else:
            slot_keys.extend(["up_sell", "dn_sell"])
        for slot_key in slot_keys:
            intent = self.tracker.get_intent(slot_key)
            if intent is None:
                continue
            if (
                bool(getattr(intent, "hold_mode_active", False))
                and str(getattr(intent, "hold_mode_reason", "") or "") == "sell_churn_hold_mode"
            ):
                return True
        return False

    def _execution_replay_blocker_hint(
        self,
        *,
        failure_bucket_current: str,
        plan: QuotePlan,
        risk: RiskRegime,
        health: HealthState,
    ) -> str:
        if failure_bucket_current == "marketability_churn":
            if str(getattr(plan, "sell_churn_hold_side", "") or "") in {"up", "dn"}:
                return "sell_churn_hold_mode"
            if str(getattr(risk, "marketability_guard_reason", "") or "") == "sell_skip_cooldown":
                return "cancel/repost_churn"
            if str(getattr(risk, "marketability_guard_reason", "") or "") == "collateral_warning":
                return "collateral_warning"
            return "marketability_churn"
        if failure_bucket_current == "edge_divergence":
            if bool(getattr(plan, "divergence_hard_suppress_up_active", False)) or bool(
                getattr(plan, "divergence_hard_suppress_dn_active", False)
            ):
                return "divergence hard suppress"
            return "edge_divergence"
        if failure_bucket_current == "terminal_execution":
            return "terminal_execution"
        if failure_bucket_current == "drift_transport":
            if bool(getattr(health, "post_terminal_cleanup_grace_active", False)):
                return "post-terminal false drift"
            return "drift_transport"
        if failure_bucket_current == "inventory_regime":
            return "inventory_regime"
        return ""

    def _classify_marketability_churn(
        self,
        marketability_state: dict[str, Any],
        *,
        now: float | None = None,
    ) -> tuple[bool, str]:
        ref_now = float(time.time() if now is None else now)
        up_collateral_streak = int(marketability_state.get("up_collateral_warning_streak") or 0)
        dn_collateral_streak = int(marketability_state.get("dn_collateral_warning_streak") or 0)
        up_sell_skip_streak = int(marketability_state.get("up_sell_skip_cooldown_streak") or 0)
        dn_sell_skip_streak = int(marketability_state.get("dn_sell_skip_cooldown_streak") or 0)
        up_collateral_hits = int(marketability_state.get("up_collateral_warning_hits_60s") or 0)
        dn_collateral_hits = int(marketability_state.get("dn_collateral_warning_hits_60s") or 0)
        up_sell_skip_hits = int(marketability_state.get("up_sell_skip_cooldown_hits_60s") or 0)
        dn_sell_skip_hits = int(marketability_state.get("dn_sell_skip_cooldown_hits_60s") or 0)
        execution_churn_ratio = float(marketability_state.get("execution_churn_ratio_60s") or 0.0)
        confirmed = bool(
            max(
                up_collateral_streak,
                dn_collateral_streak,
                up_sell_skip_streak,
                dn_sell_skip_streak,
                up_collateral_hits,
                dn_collateral_hits,
                up_sell_skip_hits,
                dn_sell_skip_hits,
            )
            > 3
            or execution_churn_ratio >= 0.50
        )
        locked_side = self._marketability_locked_side(now=ref_now)
        if not confirmed:
            if ref_now < float(self._marketability_churn_hold_until):
                return True, str(locked_side or "")
            self._marketability_churn_hold_until = 0.0
            if not self._has_active_sell_churn_hold_order():
                self._clear_marketability_side_lock()
            return False, ""
        up_score = self._marketability_side_score(marketability_state, "up")
        dn_score = self._marketability_side_score(marketability_state, "dn")
        if up_score <= 0 and dn_score <= 0:
            self._marketability_churn_hold_until = ref_now + float(self.MARKETABILITY_CHURN_HOLD_SEC)
            return True, str(locked_side or "")
        side = "up" if up_score >= dn_score else "dn"
        if locked_side in {"up", "dn"}:
            other_side = "dn" if locked_side == "up" else "up"
            locked_score = self._marketability_side_score(marketability_state, locked_side)
            other_score = self._marketability_side_score(marketability_state, other_side)
            other_streak = max(
                int(marketability_state.get(f"{other_side}_collateral_warning_streak") or 0),
                int(marketability_state.get(f"{other_side}_sell_skip_cooldown_streak") or 0),
                int(marketability_state.get(f"{other_side}_collateral_warning_hits_60s") or 0),
                int(marketability_state.get(f"{other_side}_sell_skip_cooldown_hits_60s") or 0),
            )
            if locked_score > 0 and other_score < (locked_score + int(self.MARKETABILITY_SIDE_SWITCH_SCORE_MARGIN)):
                side = locked_side
            elif (
                other_score >= (locked_score + int(self.MARKETABILITY_SIDE_SWITCH_SCORE_MARGIN))
                and other_streak > 3
            ):
                side = other_side
            else:
                side = locked_side
        self._marketability_churn_hold_until = ref_now + float(self.MARKETABILITY_CHURN_HOLD_SEC)
        self._set_marketability_side_lock(side, now=ref_now)
        return True, side

    def _normalize_marketability_churn_state(
        self,
        *,
        confirmed: bool,
        side: str,
        marketability_state: dict[str, Any],
        inventory: PairInventoryState,
    ) -> tuple[bool, str]:
        locked_side = self._marketability_locked_side()
        active_hold_order = self._has_active_sell_churn_hold_order(side or locked_side or None)
        if not confirmed:
            if not active_hold_order:
                self._clear_marketability_side_lock()
            return False, ""
        flat_inventory = (
            abs(float(inventory.up_shares)) <= 1e-9
            and abs(float(inventory.dn_shares)) <= 1e-9
        )
        if flat_inventory and not active_hold_order:
            self._marketability_churn_hold_until = 0.0
            self._clear_marketability_side_lock()
            return False, ""
        if not flat_inventory:
            resolved_side = str(side or locked_side or "")
            if resolved_side in {"up", "dn"}:
                self._set_marketability_side_lock(resolved_side)
            return True, resolved_side
        if bool(marketability_state.get("active")):
            return True, str(side or locked_side or "")
        if bool(marketability_state.get("up_active")) or bool(marketability_state.get("dn_active")):
            return True, str(side or locked_side or "")
        if active_hold_order:
            return True, str(side or locked_side or "")
        self._marketability_churn_hold_until = 0.0
        self._clear_marketability_side_lock()
        return False, ""

    def _classify_failure_bucket(
        self,
        *,
        snapshot: PairMarketSnapshot,
        inventory: PairInventoryState,
        risk: RiskRegime,
        health: HealthState,
        plan: QuotePlan,
        lifecycle: str,
        marketability_guard_active: bool,
    ) -> str:
        if (not bool(getattr(health, "post_terminal_cleanup_grace_active", False))) and (
            bool(health.true_drift)
            or (
            bool(health.wallet_snapshot_stale) and bool(health.last_api_error_op)
            )
        ):
            return "drift_transport"
        if self._terminal_liquidation_active and (
            (not bool(self._terminal_liquidation_done))
            or max(
                float(self._terminal_liquidation_remaining_up),
                float(self._terminal_liquidation_remaining_dn),
            ) >= float(self.market.min_order_size if self.market else 5.0)
        ):
            return "terminal_execution"
        if marketability_guard_active or bool(getattr(risk, "marketability_churn_confirmed", False)):
            return "marketability_churn"
        if (
            str(getattr(snapshot, "valuation_regime", "") or "") == "toxic_divergence"
            or bool(getattr(plan, "divergence_hard_suppress_up_active", False))
            or bool(getattr(plan, "divergence_hard_suppress_dn_active", False))
        ):
            return "edge_divergence"
        if (
            str(risk.hard_mode or "") in {"emergency_unwind", "halted"}
            or str(risk.soft_mode or "") == "unwind"
            or str(lifecycle or "") in {"unwind", "emergency_unwind", "halted"}
        ):
            return "inventory_regime"
        return ""

    def _update_mm_regime_alert(
        self,
        *,
        quoting_ratio_60s: float,
        inventory_skewed_ratio_60s: float,
        defensive_ratio_60s: float,
        unwind_ratio_60s: float,
        emergency_unwind_ratio_60s: float,
        dual_bid_ratio_60s: float = 1.0,
        one_sided_bid_streak_outside: int = 0,
        outside_near_expiry: bool = True,
        quote_balance_state: str = "",
        dual_bid_exception_active: bool = False,
        dual_bid_exception_reason: str = "",
        marketability_guard_active: bool = False,
        marketability_guard_reason: str = "",
    ) -> None:
        now = time.time()
        mm_active_ratio_60s = float(
            quoting_ratio_60s + inventory_skewed_ratio_60s + defensive_ratio_60s
        )
        reason = ""
        if unwind_ratio_60s > 0.50:
            reason = "high_unwind_ratio"
        elif emergency_unwind_ratio_60s > 0.20:
            reason = "high_emergency_ratio"
        elif outside_near_expiry and dual_bid_exception_active and dual_bid_exception_reason:
            reason = str(dual_bid_exception_reason)
        elif marketability_guard_active:
            reason = "marketability_churn"
        elif mm_active_ratio_60s < 0.30:
            reason = "low_mm_effective"
        elif outside_near_expiry and dual_bid_ratio_60s < 0.70:
            reason = "low_dual_bid_ratio"
        elif str(quote_balance_state or "").lower() == "none":
            reason = "quote_none_sustained"
        degraded = bool(reason)
        self._mm_regime_degraded_reason = reason
        if degraded:
            if self._mm_regime_degraded_started_ts <= 0.0:
                self._mm_regime_degraded_started_ts = now
            elif now - self._mm_regime_degraded_started_ts >= float(MM_REGIME_DEGRADED_CONFIRM_SEC):
                self.set_alert(
                    "mm_regime_degraded",
                    (
                        f"MM regime degraded: reason={reason}, "
                        f"marketability_guard_reason={marketability_guard_reason or 'none'}, "
                        f"mm_effective_ratio_60s={mm_active_ratio_60s:.2f}, "
                        f"quoting_ratio_60s={quoting_ratio_60s:.2f}, "
                        f"dual_bid_ratio_60s={dual_bid_ratio_60s:.2f}, "
                        f"one_sided_bid_streak_outside={int(one_sided_bid_streak_outside)}, "
                        f"unwind_ratio_60s={unwind_ratio_60s:.2f}, "
                        f"emergency_unwind_ratio_60s={emergency_unwind_ratio_60s:.2f}"
                    ),
                    level="warning",
                )
            return
        self._mm_regime_degraded_started_ts = 0.0
        self._mm_regime_degraded_reason = ""
        self.clear_alert("mm_regime_degraded")

    def _update_unwind_target_mismatch(
        self,
        *,
        effective_soft_mode: str,
        target_soft_mode: str,
    ) -> float:
        now = time.time()
        if effective_soft_mode == "unwind" and target_soft_mode != "unwind":
            if self._unwind_target_mismatch_started_ts <= 0.0:
                self._unwind_target_mismatch_started_ts = now
                self._unwind_target_mismatch_ticks = 1
            else:
                self._unwind_target_mismatch_ticks += 1
            return max(0.0, now - self._unwind_target_mismatch_started_ts)
        self._unwind_target_mismatch_ticks = 0
        self._unwind_target_mismatch_started_ts = 0.0
        return 0.0

    async def _tick(self) -> None:
        if not self.market:
            return
        fills = await self.gateway.check_fills()
        for fill in fills:
            self._fills.append(fill)
            self.reconcile.record_fill(fill, self.market)
            token_side = self._token_side_for_fill(fill, self.market)
            if token_side:
                self._record_fill_entry_metrics(fill, token_side)
            if token_side:
                self._pending_markout_evals.append(
                    _PendingMarkoutEval(
                        ts=float(getattr(fill, "ts", time.time()) or time.time()),
                        token_side=str(token_side),
                        side=str(fill.side),
                        price=float(fill.price),
                    )
                )
            self._emit_fill_callbacks(fill)
        up_book, dn_book = await self.gateway.get_books()
        valuation, snapshot = self.valuation.compute(
            market=self.market,
            feed_state=self.feed_state,
            up_book=up_book,
            dn_book=dn_book,
        )
        now = time.time()
        self._update_fast_move_state(snapshot=snapshot, now=now)
        self._update_pending_markouts(snapshot=snapshot, now=now)
        self.gateway.sync_paper_prices(
            fv_up=valuation.fv_up,
            fv_dn=valuation.fv_dn,
            pm_prices={"up": snapshot.pm_mid_up, "dn": snapshot.pm_mid_dn},
        )
        expected_up, expected_dn = self.reconcile.expected_balances()
        if expected_up is not None and expected_dn is not None:
            reference_balances = (
                float(expected_up),
                float(expected_dn),
            )
        else:
            reference_balances = (
                float(self._last_inventory.up_shares),
                float(self._last_inventory.dn_shares),
            )
        up_raw, dn_raw, total_usdc_raw, available_usdc_raw = await self.gateway.get_wallet_balances(
            reference_balances=reference_balances,
        )
        up, dn, total_usdc, available_usdc, stale_wallet = self._coalesce_wallet_snapshot(
            up=up_raw,
            dn=dn_raw,
            total_usdc=total_usdc_raw,
            available_usdc=available_usdc_raw,
        )
        api_stats = self.gateway.api_error_stats()
        balance_fetch_health = self.gateway.balance_fetch_health_state()
        reconcile_balance_error_active = bool(
            (balance_fetch_health or {}).get("reconcile_balance_error_active")
        )
        transport_recent_failures = int((api_stats or {}).get("transport_recent_60s_total") or 0)
        transport_unhealthy_recent = transport_recent_failures >= int(self.config.max_transport_failures)
        effective_wallet_stale = bool(stale_wallet or reconcile_balance_error_active or transport_unhealthy_recent)
        sellable_up, sellable_dn = await self.gateway.get_sellable_balances(
            reference_balances=(float(up), float(dn)),
        )
        sell_release_lag = self.gateway.sell_release_lag_state()
        marketability_state = self.gateway.marketability_state()
        marketability_guard_active = bool((marketability_state or {}).get("active"))
        marketability_guard_reason = str((marketability_state or {}).get("reason") or "")
        marketability_churn_confirmed, marketability_problem_side = self._classify_marketability_churn(
            dict(marketability_state or {})
        )
        post_terminal_cleanup_grace_active, post_terminal_cleanup_grace_sec = (
            self._post_terminal_cleanup_grace_state(now=now)
        )
        if effective_wallet_stale:
            stale_reason = "PM wallet snapshot partial/unavailable"
            if not stale_wallet and reconcile_balance_error_active:
                stale_reason = "PM reconcile balance fetch errors active; drift guard armed"
            elif transport_unhealthy_recent:
                stale_reason = (
                    f"PM transport errors active ({transport_recent_failures}/60s); drift guard armed"
                )
            self.set_alert(
                "wallet_snapshot_stale_v2",
                stale_reason,
                level="warning",
            )
        else:
            self.clear_alert("wallet_snapshot_stale_v2")
        sellable_up = max(0.0, float(up if sellable_up is None else sellable_up))
        sellable_dn = max(0.0, float(dn if sellable_dn is None else sellable_dn))
        inventory = self.reconcile.reconcile(
            market=self.market,
            real_up=up,
            real_dn=dn,
            total_usdc=total_usdc,
            available_usdc=available_usdc,
            active_orders=self.gateway.active_orders(),
            fv_up=valuation.fv_up,
            fv_dn=valuation.fv_dn,
            sellability_lag_active=bool(sell_release_lag.get("active")),
            wallet_snapshot_stale=effective_wallet_stale,
            terminal_cleanup_grace=bool(post_terminal_cleanup_grace_active),
        )
        inventory.sellable_up_shares = sellable_up
        inventory.sellable_dn_shares = sellable_dn
        sellable_balance_unknown = bool(
            float(inventory.up_shares) > 1e-9
            and float(inventory.sellable_up_shares) <= 1e-9
            and float(inventory.pending_sell_up) <= 1e-9
            and float(sell_release_lag.get("up_reserve", 0.0) or 0.0) <= 1e-9
        ) or bool(
            float(inventory.dn_shares) > 1e-9
            and float(inventory.sellable_dn_shares) <= 1e-9
            and float(inventory.pending_sell_dn) <= 1e-9
            and float(sell_release_lag.get("dn_reserve", 0.0) or 0.0) <= 1e-9
        )
        if bool(effective_wallet_stale) or sellable_balance_unknown:
            self._consecutive_balance_failures = getattr(self, "_consecutive_balance_failures", 0) + 1
        else:
            self._consecutive_balance_failures = 0
        self._balance_api_degraded = bool(self._consecutive_balance_failures >= 3)
        pair_entry_cost = 0.0
        if float(inventory.paired_qty) > 0.0:
            pair_entry_cost = max(0.0, float(self._avg_entry_price_up) + float(self._avg_entry_price_dn))
        inventory.pair_entry_cost = float(pair_entry_cost)
        inventory.pair_entry_pnl_per_share = 1.0 - float(pair_entry_cost)
        marketability_churn_confirmed, marketability_problem_side = self._normalize_marketability_churn_state(
            confirmed=bool(marketability_churn_confirmed),
            side=str(marketability_problem_side or ""),
            marketability_state=dict(marketability_state or {}),
            inventory=inventory,
        )
        marketability_side_locked = self._marketability_locked_side(now=now)
        marketability_side_lock_age_sec = self._marketability_side_lock_age_sec(now=now)
        (
            position_mark_value_bid,
            position_mark_value_mid,
            portfolio_mark_value,
            portfolio_mark_value_mid,
            tradeable_portfolio_value,
        ) = self._update_session_pnl(
            inventory,
            total_usdc=total_usdc,
            snapshot=snapshot,
        )
        drawdown_breach_ticks, drawdown_breach_age_sec, drawdown_breach_active = self._update_drawdown_breach(
            self._session_pnl_drawdown_usd,
        )
        drawdown_threshold_usd_effective = float(self.config.effective_hard_drawdown_usd())
        if inventory.free_usdc > (inventory.wallet_total_usdc + 1e-6):
            self.set_alert(
                "wallet_invariant_v2",
                (
                    "wallet invariant violated: free_usdc exceeds wallet_total_usdc "
                    f"({inventory.free_usdc:.4f}>{inventory.wallet_total_usdc:.4f})"
                ),
                level="warning",
            )
        elif abs((inventory.free_usdc + inventory.wallet_reserved_usdc) - inventory.wallet_total_usdc) > 1e-6:
            self.set_alert(
                "wallet_invariant_v2",
                (
                    "wallet invariant violated: free+reserved != total "
                    f"({inventory.free_usdc + inventory.wallet_reserved_usdc:.4f}!={inventory.wallet_total_usdc:.4f})"
                ),
                level="warning",
            )
        else:
            self.clear_alert("wallet_invariant_v2")
        pre_analytics = AnalyticsState(
            fill_count=len(self._fills),
            session_pnl=self._session_pnl_equity_usd,
            session_pnl_equity_usd=self._session_pnl_equity_usd,
            session_pnl_drawdown_usd=self._session_pnl_drawdown_usd,
            session_pnl_operator_usd=self._session_pnl_operator_usd,
            session_pnl_operator_ema_usd=self._session_pnl_operator_ema_usd,
            portfolio_mark_value_mid_usd=float(portfolio_mark_value_mid),
            pair_entry_cost=float(inventory.pair_entry_cost),
            pair_entry_pnl_per_share=float(inventory.pair_entry_pnl_per_share),
            rolling_markout_up_5s=float(self._rolling_markout_up_5s),
            rolling_markout_dn_5s=float(self._rolling_markout_dn_5s),
            rolling_spread_capture_up=float(self._rolling_spread_capture_up),
            rolling_spread_capture_dn=float(self._rolling_spread_capture_dn),
            price_move_bps_1s=float(snapshot.price_move_bps_1s),
            price_move_bps_5s=float(snapshot.price_move_bps_5s),
            fast_move_soft_active=bool(snapshot.fast_move_soft_active),
            fast_move_hard_active=bool(snapshot.fast_move_hard_active),
            fast_move_pause_active=bool(snapshot.fast_move_pause_active),
            marketability_guard_active=bool(marketability_guard_active),
            marketability_guard_reason=str(marketability_guard_reason),
            marketability_churn_confirmed=bool(marketability_churn_confirmed),
            marketability_problem_side=str(marketability_problem_side or ""),
            marketability_side_locked=str(marketability_side_locked or ""),
            marketability_side_lock_age_sec=float(marketability_side_lock_age_sec),
            collateral_warning_hits_60s=int(marketability_state.get("collateral_warning_hits_60s") or 0),
            sell_skip_cooldown_hits_60s=int(marketability_state.get("sell_skip_cooldown_hits_60s") or 0),
            up_collateral_warning_streak=int(marketability_state.get("up_collateral_warning_streak") or 0),
            dn_collateral_warning_streak=int(marketability_state.get("dn_collateral_warning_streak") or 0),
            up_sell_skip_cooldown_streak=int(marketability_state.get("up_sell_skip_cooldown_streak") or 0),
            dn_sell_skip_cooldown_streak=int(marketability_state.get("dn_sell_skip_cooldown_streak") or 0),
            collateral_warning_streak_current=max(
                int(marketability_state.get("up_collateral_warning_streak") or 0),
                int(marketability_state.get("dn_collateral_warning_streak") or 0),
            ),
            sell_skip_cooldown_streak_current=max(
                int(marketability_state.get("up_sell_skip_cooldown_streak") or 0),
                int(marketability_state.get("dn_sell_skip_cooldown_streak") or 0),
            ),
            execution_churn_ratio_60s=float(marketability_state.get("execution_churn_ratio_60s") or 0.0),
        )
        true_drift_age_sec, true_drift_no_progress_sec = self._update_true_drift_progress(
            inventory,
            post_terminal_cleanup_grace_active=bool(post_terminal_cleanup_grace_active),
        )
        health = self._build_health(
            api_stats=api_stats,
            sellability_lag_active=bool(sell_release_lag.get("active")),
            wallet_snapshot_stale=effective_wallet_stale,
            post_terminal_cleanup_grace_active=bool(post_terminal_cleanup_grace_active),
            true_drift_age_sec=true_drift_age_sec,
            true_drift_no_progress_sec=true_drift_no_progress_sec,
            drawdown_breach_ticks=drawdown_breach_ticks,
            drawdown_breach_age_sec=drawdown_breach_age_sec,
            drawdown_breach_active=drawdown_breach_active,
            drawdown_threshold_usd_effective=drawdown_threshold_usd_effective,
        )
        risk = self.risk_kernel.evaluate(
            snapshot=snapshot,
            inventory=inventory,
            analytics=pre_analytics,
            health=health,
        )
        risk = replace(
            risk,
            post_fill_markout_5s_up=float(self._post_fill_markout_5s_up),
            post_fill_markout_5s_dn=float(self._post_fill_markout_5s_dn),
            negative_spread_capture_streak_up=int(self._negative_spread_capture_streak_up),
            negative_spread_capture_streak_dn=int(self._negative_spread_capture_streak_dn),
            toxic_fill_streak_up=int(self._toxic_fill_streak_up),
            toxic_fill_streak_dn=int(self._toxic_fill_streak_dn),
            side_soft_brake_up_active=bool(self._side_soft_brake_active("up")),
            side_soft_brake_dn_active=bool(self._side_soft_brake_active("dn")),
            side_reentry_cooldown_up_sec=float(self._side_reentry_cooldown_sec("up", now=now)),
            side_reentry_cooldown_dn_sec=float(self._side_reentry_cooldown_sec("dn", now=now)),
            side_hard_block_up_sec=float(self._side_reentry_cooldown_sec("up", now=now)),
            side_hard_block_dn_sec=float(self._side_reentry_cooldown_sec("dn", now=now)),
            marketability_guard_active=bool(marketability_guard_active),
            marketability_guard_reason=str(marketability_guard_reason),
            marketability_guard_up_active=bool(marketability_state.get("up_active") or False),
            marketability_guard_dn_active=bool(marketability_state.get("dn_active") or False),
            marketability_churn_confirmed=bool(marketability_churn_confirmed),
            marketability_problem_side=str(marketability_problem_side or ""),
            marketability_side_locked=str(marketability_side_locked or ""),
            marketability_side_lock_age_sec=float(marketability_side_lock_age_sec),
            pair_entry_cost=float(inventory.pair_entry_cost),
            pair_entry_pnl_per_share=float(inventory.pair_entry_pnl_per_share),
            rolling_markout_up_5s=float(self._rolling_markout_up_5s),
            rolling_markout_dn_5s=float(self._rolling_markout_dn_5s),
            rolling_spread_capture_up=float(self._rolling_spread_capture_up),
            rolling_spread_capture_dn=float(self._rolling_spread_capture_dn),
            fast_move_soft_active=bool(snapshot.fast_move_soft_active),
            fast_move_hard_active=bool(snapshot.fast_move_hard_active),
            fast_move_pause_active=bool(snapshot.fast_move_pause_active),
            balance_api_degraded=bool(self._balance_api_degraded),
        )
        risk = self._coerce_terminal_drawdown_risk(
            snapshot=snapshot,
            inventory=inventory,
            risk=risk,
            health=health,
        )
        emergency_taker_forced, emergency_no_progress_sec = self._update_emergency_taker_force(
            hard_mode=str(risk.hard_mode),
            excess_value_usd=float(inventory.excess_value_usd),
        )
        if bool(getattr(risk, "emergency_taker_forced", False)) != emergency_taker_forced:
            risk = replace(
                risk,
                emergency_taker_forced=bool(emergency_taker_forced),
            )
        inventory.inventory_pressure_abs = risk.inventory_pressure_abs
        inventory.inventory_pressure_signed = risk.inventory_pressure_signed
        force_normal_no_guards_active = self._force_normal_no_guards_active(
            snapshot=snapshot,
            risk=risk,
        )
        ctx = QuoteContext(
            tick_size=float(self.market.tick_size),
            min_order_size=float(self.market.min_order_size),
            allow_naked_sells=self.gateway.supports_naked_sells(),
            diagnostic_no_guards=bool(force_normal_no_guards_active),
        )
        force_normal_soft_mode_active = self._force_normal_soft_mode_active(
            snapshot=snapshot,
            risk=risk,
        )
        provisional_risk = (
            replace(
                risk,
                soft_mode="normal",
                target_soft_mode="normal",
                reason="paper override: force normal soft mode",
            )
            if force_normal_soft_mode_active
            else risk
        )
        provisional_plan = QuotePolicyV2(self.config).generate(
            snapshot=snapshot,
            inventory=inventory,
            risk=provisional_risk,
            ctx=ctx,
        )
        terminal_window_active = float(snapshot.time_left_sec) <= float(self._terminal_liquidation_start_sec())
        has_any_inventory = bool(
            float(inventory.up_shares) > 1e-9
            or float(inventory.dn_shares) > 1e-9
        )
        terminal_liquidation_should_arm = bool(
            risk.hard_mode != "halted"
            and (self._has_material_inventory(inventory) or has_any_inventory)
            and terminal_window_active
        )
        if terminal_liquidation_should_arm or (
            self._terminal_liquidation_active and terminal_window_active
        ):
            just_armed_terminal = False
            if not self._terminal_liquidation_active:
                self._terminal_liquidation_active = True
                self._terminal_liquidation_started_ts = now
                self._terminal_liquidation_round_idx = 0
                self._terminal_liquidation_attempted_orders = 0
                self._terminal_liquidation_placed_orders = 0
                self._terminal_liquidation_remaining_up = float(inventory.up_shares)
                self._terminal_liquidation_remaining_dn = float(inventory.dn_shares)
                self._terminal_liquidation_done = False
                self._terminal_liquidation_reason = "terminal_liquidation_active"
                just_armed_terminal = True
                await self.gateway.cancel_all()
                self.tracker.refresh_from_active(self.gateway.active_orders())
            terminal_timeout = (
                self._terminal_liquidation_elapsed(now=now) >= self._terminal_liquidation_timeout_sec()
            )
            if not self._terminal_liquidation_done:
                step = await self.gateway.run_terminal_liquidation_step(
                    round_idx=int(self._terminal_liquidation_round_idx),
                    cancel_existing=not just_armed_terminal,
                )
                self._terminal_liquidation_round_idx += 1
                self._terminal_liquidation_attempted_orders += int(step.get("attempted_orders") or 0)
                self._terminal_liquidation_placed_orders += int(step.get("placed_orders") or 0)
                self._terminal_liquidation_remaining_up = float(step.get("remaining_up") or 0.0)
                self._terminal_liquidation_remaining_dn = float(step.get("remaining_dn") or 0.0)
                self._terminal_liquidation_done = bool(step.get("done", False))
                remaining_inventory = replace(
                    inventory,
                    up_shares=float(self._terminal_liquidation_remaining_up),
                    dn_shares=float(self._terminal_liquidation_remaining_dn),
                )
                remaining_has_any_inventory = bool(
                    float(remaining_inventory.up_shares) > 1e-9
                    or float(remaining_inventory.dn_shares) > 1e-9
                )
                terminal_done_reason = ""
                if (
                    not self._terminal_liquidation_done
                    and not self._has_material_inventory(remaining_inventory)
                    and remaining_has_any_inventory
                    and self._terminal_liquidation_round_idx >= 2
                ):
                    self._terminal_liquidation_done = True
                    terminal_done_reason = "sub_minimum_inventory_done"
                step_reason = str(step.get("reason") or "")
                if self._terminal_liquidation_done:
                    self._terminal_liquidation_reason = (
                        terminal_done_reason
                        or (
                            step_reason
                            if step_reason not in {"", "ok"}
                            else "terminal_liquidation_done"
                        )
                    )
                else:
                    self._terminal_liquidation_reason = (
                        step_reason or "terminal_liquidation_active"
                    )
                terminal_wallet_total_usdc = float(step.get("wallet_total_usdc") or 0.0)
                inventory = replace(
                    remaining_inventory,
                    free_usdc=float(terminal_wallet_total_usdc),
                    reserved_usdc=0.0,
                    wallet_total_usdc=float(terminal_wallet_total_usdc),
                    wallet_reserved_usdc=0.0,
                )
                (
                    position_mark_value_bid,
                    position_mark_value_mid,
                    portfolio_mark_value,
                    portfolio_mark_value_mid,
                    tradeable_portfolio_value,
                ) = self._update_session_pnl(
                    inventory,
                    total_usdc=float(terminal_wallet_total_usdc),
                    snapshot=snapshot,
                )
            elif terminal_timeout and not self._terminal_liquidation_done:
                self._terminal_liquidation_reason = (
                    "terminal_liquidation_timeout"
                    if terminal_timeout
                    else "terminal_liquidation_done"
                )
            if float(snapshot.time_left_sec) <= 0.0 and (
                bool(self._terminal_liquidation_done) or bool(terminal_timeout)
            ):
                await self.gateway.cancel_all()
                self.tracker.refresh_from_active(self.gateway.active_orders())
                lifecycle = "expired"
                self.state_machine._set_lifecycle("expired")
                self._set_terminal_reason(str(self._terminal_liquidation_reason or "expired"))
                self._running = False
                if (
                    max(
                        float(self._terminal_liquidation_remaining_up),
                        float(self._terminal_liquidation_remaining_dn),
                    )
                    >= float(self.market.min_order_size)
                ):
                    self.set_alert(
                        "terminal_liquidation_v2",
                        (
                            "Terminal liquidation incomplete: "
                            f"rem_up={self._terminal_liquidation_remaining_up:.4f} "
                            f"rem_dn={self._terminal_liquidation_remaining_dn:.4f}"
                        ),
                        level="warning",
                    )
                else:
                    self.clear_alert("terminal_liquidation_v2")
            else:
                lifecycle = "unwind"
                self.state_machine._set_lifecycle("unwind")
                if self._terminal_liquidation_done:
                    self.clear_alert("terminal_liquidation_v2")
                else:
                    self.set_alert(
                        "terminal_liquidation_v2",
                        (
                            "Terminal liquidation active: "
                            f"rem_up={self._terminal_liquidation_remaining_up:.4f} "
                            f"rem_dn={self._terminal_liquidation_remaining_dn:.4f}"
                        ),
                        level="warning",
                    )
            transition = SoftTransitionResult(
                lifecycle=lifecycle,  # type: ignore[arg-type]
                effective_soft_mode="unwind" if lifecycle != "expired" else "normal",
                target_soft_mode="unwind",
                reason=str(self._terminal_liquidation_reason or "terminal_liquidation_active"),
            )
            effective_risk = replace(
                risk,
                soft_mode="unwind",
                target_soft_mode="unwind",
                reason=str(self._terminal_liquidation_reason or risk.reason),
                emergency_taker_forced=bool(emergency_taker_forced),
            )
            plan = QuotePlan(
                None,
                None,
                None,
                None,
                "unwind",
                str(self._terminal_liquidation_reason or "terminal_liquidation_active"),
                quote_balance_state="none",
                quote_viability_reason="terminal_liquidation",
            )
        else:
            if self._terminal_liquidation_active:
                self._terminal_liquidation_active = False
                self._terminal_liquidation_reason = ""
                self._terminal_liquidation_done = False
                self._terminal_liquidation_started_ts = 0.0
                self._terminal_liquidation_round_idx = 0
                self.clear_alert("terminal_liquidation_v2")
            if force_normal_soft_mode_active:
                self.state_machine._set_lifecycle("quoting")
                transition = SoftTransitionResult(
                    lifecycle="quoting",
                    effective_soft_mode="normal",
                    target_soft_mode="normal",
                    reason="paper override: force normal soft mode",
                )
                effective_risk = replace(
                    risk,
                    soft_mode="normal",
                    target_soft_mode="normal",
                    reason="paper override: force normal soft mode",
                    emergency_taker_forced=bool(emergency_taker_forced),
                )
                lifecycle = transition.lifecycle
                plan = QuotePolicyV2(self.config).generate(
                    snapshot=snapshot,
                    inventory=inventory,
                    risk=effective_risk,
                    ctx=ctx,
                )
                await self.execution_policy.sync(plan)
                self._orders_cancelled_for_merge = False
            else:
                transition = self.state_machine.transition(
                    snapshot=snapshot,
                    inventory=inventory,
                    risk=risk,
                    viability=self._provisional_quote_viability(provisional_plan),
                )
                effective_risk = replace(
                    risk,
                    soft_mode=transition.effective_soft_mode,
                    target_soft_mode=transition.target_soft_mode,
                    reason=transition.reason or risk.reason,
                    emergency_taker_forced=bool(emergency_taker_forced),
                )
                lifecycle = transition.lifecycle
                if lifecycle in {"halted", "expired"}:
                    await self.gateway.cancel_all()
                    self.tracker.refresh_from_active(self.gateway.active_orders())
                    plan = QuotePlan(None, None, None, None, lifecycle, risk.reason)
                    terminal_reason = risk.reason if lifecycle == "halted" else "expired"
                    self._set_terminal_reason(terminal_reason or lifecycle)
                    self._running = False
                else:
                    plan = QuotePolicyV2(self.config).generate(
                        snapshot=snapshot,
                        inventory=inventory,
                        risk=effective_risk,
                        ctx=ctx,
                    )
                    await self.execution_policy.sync(plan)
                    self._orders_cancelled_for_merge = False
        sync_metrics = self.execution_policy.consume_sync_metrics()
        lifecycle = transition.lifecycle
        unwind_target_mismatch_sec = self._update_unwind_target_mismatch(
            effective_soft_mode=effective_risk.soft_mode,
            target_soft_mode=effective_risk.target_soft_mode,
        )
        outside_near_expiry = float(snapshot.time_left_sec) > float(self.config.unwind_window_sec)
        helpful_quote_count = sum(
            1
            for intent in (plan.up_bid, plan.up_ask, plan.dn_bid, plan.dn_ask)
            if intent and intent.inventory_effect == "helpful"
        )
        harmful_quote_count = sum(
            1
            for intent in (plan.up_bid, plan.up_ask, plan.dn_bid, plan.dn_ask)
            if intent and intent.inventory_effect == "harmful"
        )
        now = time.time()
        up_bid_active = plan.up_bid is not None
        dn_bid_active = plan.dn_bid is not None
        dual_bid_active = bool(up_bid_active and dn_bid_active)
        one_sided_bid_active = bool(up_bid_active) ^ bool(dn_bid_active)
        dual_bid_mode_eligible = bool(
            outside_near_expiry
            and effective_risk.hard_mode == "none"
            and effective_risk.soft_mode in {"normal", "inventory_skewed"}
            and (up_bid_active or dn_bid_active)
        )
        dual_bid_exception_active = bool(getattr(plan, "dual_bid_exception_active", False))
        dual_bid_exception_reason = str(getattr(plan, "dual_bid_exception_reason", "") or "")
        if dual_bid_mode_eligible:
            self._dual_bid_outside_sample_history.append((now, 1))
            dual_bid_success = bool(dual_bid_active or (one_sided_bid_active and dual_bid_exception_active))
            self._dual_bid_outside_success_history.append((now, 1 if dual_bid_success else 0))
            if one_sided_bid_active and not dual_bid_exception_active:
                self._one_sided_bid_streak_outside += 1
            else:
                self._one_sided_bid_streak_outside = 0
        else:
            self._one_sided_bid_streak_outside = 0
        harmful_suppressed_count_tick = sum(
            1
            for reason in plan.suppressed_reasons.values()
            if str(reason).startswith("harmful_suppressed_")
        )
        maker_cross_guard_hits_tick = sum(
            1 for reason in plan.suppressed_reasons.values() if str(reason) == "maker_cross_guard"
        )
        harmful_buy_brake_hits_tick = int(getattr(plan, "harmful_buy_brake_hits", 0) or 0)
        gross_inventory_brake_hits_tick = int(getattr(plan, "gross_inventory_brake_hits", 0) or 0)
        pair_over_target_buy_blocks_tick = int(getattr(plan, "pair_over_target_buy_blocks", 0) or 0)
        dual_bid_guard_hits_tick = sum(
            1 for reason in plan.suppressed_reasons.values() if str(reason) == "dual_bid_guard_applied"
        )
        dual_bid_guard_inventory_budget_hits_tick = int(
            getattr(plan, "dual_bid_guard_inventory_budget_hits", 0) or 0
        )
        midpoint_first_brake_hits_tick = int(getattr(plan, "midpoint_first_brake_hits", 0) or 0)
        simultaneous_bid_block_prevented_tick = int(
            getattr(plan, "simultaneous_bid_block_prevented", 0) or 0
        )
        divergence_soft_brake_hits_tick = int(getattr(plan, "divergence_soft_brake_hits", 0) or 0)
        divergence_hard_suppress_hits_tick = int(getattr(plan, "divergence_hard_suppress_hits", 0) or 0)
        sell_churn_hold_reprice_suppressed_tick = int(
            sync_metrics.get("sell_churn_hold_reprice_suppressed_hits") or 0
        )
        sell_churn_hold_cancel_avoided_tick = int(
            sync_metrics.get("sell_churn_hold_cancel_avoided_hits") or 0
        )
        buy_edge_gap_tick = max(
            0.0,
            float(getattr(snapshot, "buy_edge_gap_up", 0.0) or 0.0),
            float(getattr(snapshot, "buy_edge_gap_dn", 0.0) or 0.0),
        )
        dual_bid_guard_fail_hits_tick = sum(
            1
            for reason in plan.suppressed_reasons.values()
            if str(reason)
            in {
                "dual_bid_guard_headroom",
                "dual_bid_guard_viability",
                "dual_bid_guard_market",
                "dual_bid_guard_inventory_budget",
                "dual_bid_guard_drawdown_block",
            }
        )
        target_ratio_cap_hits_tick = sum(
            1 for reason in plan.suppressed_reasons.values() if str(reason) == "target_pair_ratio_cap"
        )
        unwind_deferred_hits_tick = 1 if transition.unwind_deferred else 0
        forced_unwind_extreme_excess_hits_tick = 1 if transition.forced_unwind_extreme_excess else 0
        untradeable_tolerated_tick = int(
            (
                not bool(snapshot.market_tradeable)
                and not bool(self._terminal_liquidation_active)
                and str(effective_risk.soft_mode or "") == "normal"
                and str(effective_risk.reason or "").startswith("normal quoting (untradeable tolerated)")
            )
        )
        current_active_order_ids = set(self.gateway.active_order_ids())
        removed_orders_tick = len(self._prev_active_order_ids - current_active_order_ids)
        self._prev_active_order_ids = current_active_order_ids
        self._excess_history.append((now, float(inventory.excess_value_usd)))
        self._quote_presence_history.append(
            (
                now,
                (
                    any([plan.up_bid, plan.up_ask, plan.dn_bid, plan.dn_ask]),
                    all([plan.up_bid, plan.up_ask, plan.dn_bid, plan.dn_ask]),
                ),
            )
        )
        self._mode_history.append((now, effective_risk.soft_mode))
        self._lifecycle_history.append((now, str(lifecycle)))
        self._harmful_suppressed_history.append((now, int(harmful_suppressed_count_tick)))
        self._harmful_buy_brake_history.append((now, int(harmful_buy_brake_hits_tick)))
        self._gross_inventory_brake_history.append((now, int(gross_inventory_brake_hits_tick)))
        self._pair_over_target_buy_block_history.append((now, int(pair_over_target_buy_blocks_tick)))
        self._target_ratio_breach_history.append((now, int(target_ratio_cap_hits_tick)))
        self._order_removal_history.append((now, int(removed_orders_tick)))
        self._maker_cross_guard_history.append((now, int(maker_cross_guard_hits_tick)))
        self._dual_bid_guard_hits_history.append((now, int(dual_bid_guard_hits_tick)))
        self._dual_bid_guard_fail_history.append((now, int(dual_bid_guard_fail_hits_tick)))
        self._dual_bid_guard_inventory_budget_history.append((now, int(dual_bid_guard_inventory_budget_hits_tick)))
        self._midpoint_first_brake_history.append((now, int(midpoint_first_brake_hits_tick)))
        self._simultaneous_bid_block_prevented_history.append((now, int(simultaneous_bid_block_prevented_tick)))
        self._divergence_soft_brake_history.append((now, int(divergence_soft_brake_hits_tick)))
        self._divergence_hard_suppress_history.append((now, int(divergence_hard_suppress_hits_tick)))
        self._buy_edge_gap_history.append((now, float(buy_edge_gap_tick)))
        self._sell_churn_hold_reprice_suppressed_history.append((now, int(sell_churn_hold_reprice_suppressed_tick)))
        self._sell_churn_hold_cancel_avoided_history.append((now, int(sell_churn_hold_cancel_avoided_tick)))
        self._untradeable_tolerated_history.append((now, int(untradeable_tolerated_tick)))
        self._emergency_taker_forced_history.append((now, 1 if emergency_taker_forced else 0))
        self._unwind_deferred_history.append((now, int(unwind_deferred_hits_tick)))
        self._forced_unwind_extreme_excess_history.append((now, int(forced_unwind_extreme_excess_hits_tick)))
        window_now = float(now)
        self._prune_history(self._excess_history, now=window_now)
        self._prune_history(self._quote_presence_history, now=window_now)
        self._prune_history(self._mode_history, now=window_now)
        self._prune_history(self._lifecycle_history, now=window_now)
        self._prune_history(self._harmful_suppressed_history, now=window_now)
        self._prune_history(self._harmful_buy_brake_history, now=window_now)
        self._prune_history(self._gross_inventory_brake_history, now=window_now)
        self._prune_history(self._pair_over_target_buy_block_history, now=window_now)
        self._prune_history(self._target_ratio_breach_history, now=window_now)
        self._prune_history(self._order_removal_history, now=window_now)
        self._prune_history(self._maker_cross_guard_history, now=window_now)
        self._prune_history(self._dual_bid_outside_sample_history, now=window_now)
        self._prune_history(self._dual_bid_outside_success_history, now=window_now)
        self._prune_history(self._dual_bid_guard_hits_history, now=window_now)
        self._prune_history(self._dual_bid_guard_fail_history, now=window_now)
        self._prune_history(self._dual_bid_guard_inventory_budget_history, now=window_now)
        self._prune_history(self._midpoint_first_brake_history, now=window_now)
        self._prune_history(self._simultaneous_bid_block_prevented_history, now=window_now)
        self._prune_history(self._divergence_soft_brake_history, now=window_now)
        self._prune_history(self._divergence_hard_suppress_history, now=window_now)
        self._prune_history(self._buy_edge_gap_history, now=window_now)
        self._prune_history(self._sell_churn_hold_reprice_suppressed_history, now=window_now)
        self._prune_history(self._sell_churn_hold_cancel_avoided_history, now=window_now)
        self._prune_history(self._untradeable_tolerated_history, now=window_now)
        self._prune_history(self._emergency_taker_forced_history, now=window_now)
        self._prune_history(self._unwind_deferred_history, now=window_now)
        self._prune_history(self._forced_unwind_extreme_excess_history, now=window_now)
        quote_presence_ratio, four_quote_presence_ratio = self._quote_presence_ratio()
        _, four_quote_ratio_60s = self._quote_presence_ratio_window(
            window_sec=float(MM_REGIME_WINDOW_SEC),
            now=window_now,
        )
        regime_ratios = self._lifecycle_ratios(window_sec=float(MM_REGIME_WINDOW_SEC), now=window_now)
        mm_effective_ratio_60s = float(
            regime_ratios["quoting_ratio_60s"]
            + regime_ratios["inventory_skewed_ratio_60s"]
            + regime_ratios["defensive_ratio_60s"]
        )
        harmful_suppressed_count_60s = self._window_sum(
            self._harmful_suppressed_history,
            window_sec=float(MM_REGIME_WINDOW_SEC),
            now=window_now,
        )
        target_ratio_breaches_60s = self._window_sum(
            self._target_ratio_breach_history,
            window_sec=float(MM_REGIME_WINDOW_SEC),
            now=window_now,
        )
        harmful_buy_brake_hits_60s = self._window_sum(
            self._harmful_buy_brake_history,
            window_sec=float(MM_REGIME_WINDOW_SEC),
            now=window_now,
        )
        gross_inventory_brake_hits_60s = self._window_sum(
            self._gross_inventory_brake_history,
            window_sec=float(MM_REGIME_WINDOW_SEC),
            now=window_now,
        )
        pair_over_target_buy_blocks_60s = self._window_sum(
            self._pair_over_target_buy_block_history,
            window_sec=float(MM_REGIME_WINDOW_SEC),
            now=window_now,
        )
        maker_cross_guard_hits_60s = self._window_sum(
            self._maker_cross_guard_history,
            window_sec=float(MM_REGIME_WINDOW_SEC),
            now=window_now,
        )
        dual_bid_guard_hits_60s = self._window_sum(
            self._dual_bid_guard_hits_history,
            window_sec=float(MM_REGIME_WINDOW_SEC),
            now=window_now,
        )
        dual_bid_guard_fail_hits_60s = self._window_sum(
            self._dual_bid_guard_fail_history,
            window_sec=float(MM_REGIME_WINDOW_SEC),
            now=window_now,
        )
        dual_bid_guard_inventory_budget_hits_60s = self._window_sum(
            self._dual_bid_guard_inventory_budget_history,
            window_sec=float(MM_REGIME_WINDOW_SEC),
            now=window_now,
        )
        midpoint_first_brake_hits_60s = self._window_sum(
            self._midpoint_first_brake_history,
            window_sec=float(MM_REGIME_WINDOW_SEC),
            now=window_now,
        )
        simultaneous_bid_block_prevented_hits_60s = self._window_sum(
            self._simultaneous_bid_block_prevented_history,
            window_sec=float(MM_REGIME_WINDOW_SEC),
            now=window_now,
        )
        divergence_soft_brake_hits_60s = self._window_sum(
            self._divergence_soft_brake_history,
            window_sec=float(MM_REGIME_WINDOW_SEC),
            now=window_now,
        )
        divergence_hard_suppress_hits_60s = self._window_sum(
            self._divergence_hard_suppress_history,
            window_sec=float(MM_REGIME_WINDOW_SEC),
            now=window_now,
        )
        max_buy_edge_gap_60s = self._window_max(
            self._buy_edge_gap_history,
            window_sec=float(MM_REGIME_WINDOW_SEC),
            now=window_now,
        )
        sell_churn_hold_reprice_suppressed_hits_60s = self._window_sum(
            self._sell_churn_hold_reprice_suppressed_history,
            window_sec=float(MM_REGIME_WINDOW_SEC),
            now=window_now,
        )
        sell_churn_hold_cancel_avoided_hits_60s = self._window_sum(
            self._sell_churn_hold_cancel_avoided_history,
            window_sec=float(MM_REGIME_WINDOW_SEC),
            now=window_now,
        )
        untradeable_tolerated_samples_60s = self._window_sum(
            self._untradeable_tolerated_history,
            window_sec=float(MM_REGIME_WINDOW_SEC),
            now=window_now,
        )
        dual_bid_outside_samples_60s = self._window_sum(
            self._dual_bid_outside_sample_history,
            window_sec=float(MM_REGIME_WINDOW_SEC),
            now=window_now,
        )
        dual_bid_outside_success_60s = self._window_sum(
            self._dual_bid_outside_success_history,
            window_sec=float(MM_REGIME_WINDOW_SEC),
            now=window_now,
        )
        dual_bid_ratio_60s = (
            float(dual_bid_outside_success_60s / max(1, dual_bid_outside_samples_60s))
            if dual_bid_outside_samples_60s > 0
            else 1.0
        )
        unwind_deferred_hits_60s = self._window_sum(
            self._unwind_deferred_history,
            window_sec=float(MM_REGIME_WINDOW_SEC),
            now=window_now,
        )
        forced_unwind_extreme_excess_hits_60s = self._window_sum(
            self._forced_unwind_extreme_excess_history,
            window_sec=float(MM_REGIME_WINDOW_SEC),
            now=window_now,
        )
        emergency_taker_forced_hits_60s = self._window_sum(
            self._emergency_taker_forced_history,
            window_sec=float(MM_REGIME_WINDOW_SEC),
            now=window_now,
        )
        defensive_to_unwind_count_window = self._defensive_to_unwind_count(
            window_sec=float(MM_REGIME_WINDOW_SEC),
            now=window_now,
        )
        quote_cancel_to_fill_ratio_60s = self._quote_cancel_to_fill_ratio(
            window_sec=float(MM_REGIME_WINDOW_SEC),
            now=window_now,
        )
        failure_bucket_current = self._classify_failure_bucket(
            snapshot=snapshot,
            inventory=inventory,
            risk=effective_risk,
            health=health,
            plan=plan,
            lifecycle=str(lifecycle),
            marketability_guard_active=bool(effective_risk.marketability_guard_active),
        )
        self._update_mm_regime_alert(
            quoting_ratio_60s=float(regime_ratios["quoting_ratio_60s"]),
            inventory_skewed_ratio_60s=float(regime_ratios["inventory_skewed_ratio_60s"]),
            defensive_ratio_60s=float(regime_ratios["defensive_ratio_60s"]),
            unwind_ratio_60s=float(regime_ratios["unwind_ratio_60s"]),
            emergency_unwind_ratio_60s=float(regime_ratios["emergency_unwind_ratio_60s"]),
            dual_bid_ratio_60s=float(dual_bid_ratio_60s),
            one_sided_bid_streak_outside=int(self._one_sided_bid_streak_outside),
            outside_near_expiry=bool(outside_near_expiry),
            quote_balance_state=str(plan.quote_balance_state),
            dual_bid_exception_active=bool(dual_bid_exception_active),
            dual_bid_exception_reason=str(dual_bid_exception_reason),
            marketability_guard_active=bool(
                effective_risk.marketability_guard_active or effective_risk.marketability_churn_confirmed
            ),
            marketability_guard_reason=str(
                effective_risk.marketability_guard_reason
                or ("confirmed_churn" if effective_risk.marketability_churn_confirmed else "")
            ),
        )
        target_ratio_activation_usd_effective = float(self.config.effective_target_ratio_activation_usd())
        target_ratio_cap_active = bool(target_ratio_cap_hits_tick > 0)
        quote_shift_from_mid_up = self._quote_shift_from_mid(
            plan=plan,
            snapshot=snapshot,
            token_side="up",
        )
        quote_shift_from_mid_dn = self._quote_shift_from_mid(
            plan=plan,
            snapshot=snapshot,
            token_side="dn",
        )
        up_sell_hold_state = self.execution_policy.hold_order_state(
            "up_sell",
            desired=plan.up_ask,
            now=window_now,
        )
        dn_sell_hold_state = self.execution_policy.hold_order_state(
            "dn_sell",
            desired=plan.dn_ask,
            now=window_now,
        )
        execution_replay_blocker_hint = self._execution_replay_blocker_hint(
            failure_bucket_current=str(failure_bucket_current or ""),
            plan=plan,
            risk=effective_risk,
            health=health,
        )
        await self._maybe_merge_pairs(
            snapshot=snapshot,
            inventory=inventory,
            risk=effective_risk,
            now=window_now,
        )
        analytics = AnalyticsState(
            fill_count=len(self._fills),
            session_pnl=self._session_pnl_equity_usd,
            session_pnl_equity_usd=self._session_pnl_equity_usd,
            session_pnl_drawdown_usd=self._session_pnl_drawdown_usd,
            session_pnl_operator_usd=self._session_pnl_operator_usd,
            session_pnl_operator_ema_usd=self._session_pnl_operator_ema_usd,
            position_mark_value_usd=float(position_mark_value_bid),
            position_mark_value_bid_usd=float(position_mark_value_bid),
            position_mark_value_mid_usd=float(position_mark_value_mid),
            portfolio_mark_value_usd=float(portfolio_mark_value),
            portfolio_mark_value_mid_usd=float(portfolio_mark_value_mid),
            tradeable_portfolio_value_usd=float(tradeable_portfolio_value),
            pair_entry_cost=float(inventory.pair_entry_cost),
            pair_entry_pnl_per_share=float(inventory.pair_entry_pnl_per_share),
            anchor_divergence_up=float(snapshot.anchor_divergence_up),
            anchor_divergence_dn=float(snapshot.anchor_divergence_dn),
            buy_edge_gap_up=float(getattr(snapshot, "buy_edge_gap_up", 0.0) or 0.0),
            buy_edge_gap_dn=float(getattr(snapshot, "buy_edge_gap_dn", 0.0) or 0.0),
            quote_shift_from_mid_up=float(quote_shift_from_mid_up),
            quote_shift_from_mid_dn=float(quote_shift_from_mid_dn),
            post_fill_markout_5s_up=float(self._post_fill_markout_5s_up),
            post_fill_markout_5s_dn=float(self._post_fill_markout_5s_dn),
            rolling_markout_up_5s=float(self._rolling_markout_up_5s),
            rolling_markout_dn_5s=float(self._rolling_markout_dn_5s),
            rolling_spread_capture_up=float(self._rolling_spread_capture_up),
            rolling_spread_capture_dn=float(self._rolling_spread_capture_dn),
            toxic_fill_streak_up=int(self._toxic_fill_streak_up),
            toxic_fill_streak_dn=int(self._toxic_fill_streak_dn),
            side_soft_brake_up_active=bool(self._side_soft_brake_active("up")),
            side_soft_brake_dn_active=bool(self._side_soft_brake_active("dn")),
            negative_spread_capture_streak_up=int(self._negative_spread_capture_streak_up),
            negative_spread_capture_streak_dn=int(self._negative_spread_capture_streak_dn),
            side_reentry_cooldown_up_sec=float(self._side_reentry_cooldown_sec("up", now=window_now)),
            side_reentry_cooldown_dn_sec=float(self._side_reentry_cooldown_sec("dn", now=window_now)),
            side_hard_block_up_sec=float(self._side_reentry_cooldown_sec("up", now=window_now)),
            side_hard_block_dn_sec=float(self._side_reentry_cooldown_sec("dn", now=window_now)),
            quote_anchor_mode=str(getattr(snapshot, "quote_anchor_mode", "midpoint_first") or "midpoint_first"),
            midpoint_reference_mode=str(getattr(snapshot, "valuation_source", "midpoint_first") or "midpoint_first"),
            pnl_calc_mode="wallet_total_plus_mark",
            pnl_mark_basis="conservative_bid",
            pnl_updated_ts=float(now),
            markout_1s=0.0,
            markout_5s=float(max(abs(self._rolling_markout_up_5s), abs(self._rolling_markout_dn_5s))),
            spread_capture_usd=float(self._rolling_spread_capture_up + self._rolling_spread_capture_dn),
            fill_rate=0.0,
            quote_presence_ratio=quote_presence_ratio,
            excess_value_usd=float(inventory.excess_value_usd),
            target_pair_value_usd=float(inventory.target_pair_value_usd),
            pair_value_ratio=float(inventory.pair_value_ratio),
            pair_value_over_target_usd=float(inventory.pair_value_over_target_usd),
            target_ratio_activation_usd_effective=float(target_ratio_activation_usd_effective),
            target_ratio_cap_active=bool(target_ratio_cap_active),
            target_ratio_cap_hits_60s=int(target_ratio_breaches_60s),
            target_ratio_pressure=float(effective_risk.target_ratio_pressure),
            gross_inventory_brake_active=bool(getattr(plan, "gross_inventory_brake_active", False)),
            gross_inventory_brake_hits_60s=int(gross_inventory_brake_hits_60s),
            pair_over_target_buy_blocks_60s=int(pair_over_target_buy_blocks_60s),
            dual_bid_guard_inventory_budget_hits_60s=int(dual_bid_guard_inventory_budget_hits_60s),
            inventory_pressure_abs=float(risk.inventory_pressure_abs),
            inventory_pressure_signed=float(risk.inventory_pressure_signed),
            inventory_half_life_sec=self._estimate_inventory_half_life(),
            four_quote_presence_ratio=four_quote_presence_ratio,
            helpful_quote_count=helpful_quote_count,
            harmful_quote_count=harmful_quote_count,
            quote_balance_state=plan.quote_balance_state,
            min_viable_clip_usd=float(QuotePolicyV2(self.config)._min_viable_clip_usd(snapshot, ctx)),
            quote_viability_reason=plan.quote_viability_reason,
            quoting_ratio_60s=float(regime_ratios["quoting_ratio_60s"]),
            inventory_skewed_ratio_60s=float(regime_ratios["inventory_skewed_ratio_60s"]),
            defensive_ratio_60s=float(regime_ratios["defensive_ratio_60s"]),
            unwind_ratio_60s=float(regime_ratios["unwind_ratio_60s"]),
            emergency_unwind_ratio_60s=float(regime_ratios["emergency_unwind_ratio_60s"]),
            four_quote_ratio_60s=float(four_quote_ratio_60s),
            mm_effective_ratio_60s=float(mm_effective_ratio_60s),
            dual_bid_ratio_60s=float(dual_bid_ratio_60s),
            one_sided_bid_streak_outside=int(self._one_sided_bid_streak_outside),
            harmful_suppressed_count_60s=int(harmful_suppressed_count_60s),
            target_ratio_breaches_60s=int(target_ratio_breaches_60s),
            harmful_buy_brake_active=bool(getattr(plan, "harmful_buy_brake_active", False)),
            harmful_buy_brake_hits_60s=int(harmful_buy_brake_hits_60s),
            emergency_taker_forced=bool(emergency_taker_forced),
            emergency_taker_forced_hits_60s=int(emergency_taker_forced_hits_60s),
            emergency_no_progress_sec=float(emergency_no_progress_sec),
            defensive_to_unwind_count_window=int(defensive_to_unwind_count_window),
            quote_cancel_to_fill_ratio_60s=float(quote_cancel_to_fill_ratio_60s),
            maker_cross_guard_hits_60s=int(maker_cross_guard_hits_60s),
            dual_bid_guard_hits_60s=int(dual_bid_guard_hits_60s),
            dual_bid_guard_fail_hits_60s=int(dual_bid_guard_fail_hits_60s),
            midpoint_first_brake_hits_60s=int(midpoint_first_brake_hits_60s),
            simultaneous_bid_block_prevented_hits_60s=int(simultaneous_bid_block_prevented_hits_60s),
            divergence_soft_brake_up_active=bool(getattr(plan, "divergence_soft_brake_up_active", False)),
            divergence_soft_brake_dn_active=bool(getattr(plan, "divergence_soft_brake_dn_active", False)),
            divergence_hard_suppress_up_active=bool(getattr(plan, "divergence_hard_suppress_up_active", False)),
            divergence_hard_suppress_dn_active=bool(getattr(plan, "divergence_hard_suppress_dn_active", False)),
            divergence_soft_brake_hits_60s=int(divergence_soft_brake_hits_60s),
            divergence_hard_suppress_hits_60s=int(divergence_hard_suppress_hits_60s),
            max_buy_edge_gap_60s=float(max_buy_edge_gap_60s),
            price_move_bps_1s=float(snapshot.price_move_bps_1s),
            price_move_bps_5s=float(snapshot.price_move_bps_5s),
            fast_move_soft_active=bool(snapshot.fast_move_soft_active),
            fast_move_hard_active=bool(snapshot.fast_move_hard_active),
            fast_move_pause_active=bool(snapshot.fast_move_pause_active),
            dual_bid_exception_active=bool(dual_bid_exception_active),
            dual_bid_exception_reason=str(dual_bid_exception_reason),
            marketability_guard_active=bool(effective_risk.marketability_guard_active),
            marketability_guard_reason=str(effective_risk.marketability_guard_reason or ""),
            marketability_churn_confirmed=bool(effective_risk.marketability_churn_confirmed),
            marketability_problem_side=str(effective_risk.marketability_problem_side or ""),
            marketability_side_locked=str(marketability_side_locked or ""),
            marketability_side_lock_age_sec=float(marketability_side_lock_age_sec),
            sell_churn_hold_up_active=bool(getattr(plan, "sell_churn_hold_up_active", False)),
            sell_churn_hold_dn_active=bool(getattr(plan, "sell_churn_hold_dn_active", False)),
            sell_churn_hold_side=str(getattr(plan, "sell_churn_hold_side", "") or ""),
            sell_churn_hold_order_age_up_sec=float(up_sell_hold_state.get("age_sec") or 0.0),
            sell_churn_hold_order_age_dn_sec=float(dn_sell_hold_state.get("age_sec") or 0.0),
            sell_churn_hold_reprice_due_up=bool(up_sell_hold_state.get("reprice_due", False)),
            sell_churn_hold_reprice_due_dn=bool(dn_sell_hold_state.get("reprice_due", False)),
            sell_churn_hold_reprice_suppressed_hits_60s=int(sell_churn_hold_reprice_suppressed_hits_60s),
            sell_churn_hold_cancel_avoided_hits_60s=int(sell_churn_hold_cancel_avoided_hits_60s),
            collateral_warning_hits_60s=int(marketability_state.get("collateral_warning_hits_60s") or 0),
            sell_skip_cooldown_hits_60s=int(marketability_state.get("sell_skip_cooldown_hits_60s") or 0),
            up_collateral_warning_streak=int(marketability_state.get("up_collateral_warning_streak") or 0),
            dn_collateral_warning_streak=int(marketability_state.get("dn_collateral_warning_streak") or 0),
            up_sell_skip_cooldown_streak=int(marketability_state.get("up_sell_skip_cooldown_streak") or 0),
            dn_sell_skip_cooldown_streak=int(marketability_state.get("dn_sell_skip_cooldown_streak") or 0),
            collateral_warning_streak_current=max(
                int(marketability_state.get("up_collateral_warning_streak") or 0),
                int(marketability_state.get("dn_collateral_warning_streak") or 0),
            ),
            sell_skip_cooldown_streak_current=max(
                int(marketability_state.get("up_sell_skip_cooldown_streak") or 0),
                int(marketability_state.get("dn_sell_skip_cooldown_streak") or 0),
            ),
            execution_churn_ratio_60s=float(marketability_state.get("execution_churn_ratio_60s") or 0.0),
            untradeable_tolerated_samples_60s=int(untradeable_tolerated_samples_60s),
            post_terminal_cleanup_grace_active=bool(post_terminal_cleanup_grace_active),
            failure_bucket_current=str(failure_bucket_current or ""),
            execution_replay_blocker_hint=str(execution_replay_blocker_hint or ""),
            diagnostic_no_guards_active=bool(force_normal_no_guards_active),
            unwind_deferred_hits_60s=int(unwind_deferred_hits_60s),
            forced_unwind_extreme_excess_hits_60s=int(forced_unwind_extreme_excess_hits_60s),
            mm_regime_degraded_reason=str(self._mm_regime_degraded_reason or ""),
            unwind_target_mismatch_ticks=int(self._unwind_target_mismatch_ticks),
            unwind_target_mismatch_sec=float(unwind_target_mismatch_sec),
            unwind_exit_armed=bool(transition.unwind_exit_armed),
            emergency_exit_armed=bool(transition.emergency_exit_armed),
            recent_fills=[
                {
                    "ts": f.ts,
                    "side": f.side,
                    "token_id": f.token_id,
                    "price": f.price,
                    "size": f.size,
                }
                for f in self._fills[-20:]
            ],
        )
        if lifecycle in {"emergency_unwind", "unwind"} and snapshot.time_left_sec <= float(self.config.emergency_taker_start_sec):
            if inventory.up_shares > 0.5 or inventory.dn_shares > 0.5:
                self.set_alert("emergency_unwind_v2", f"{lifecycle}: {risk.reason}", level="warning")
        if lifecycle == "halted":
            self.set_alert("halted_v2", risk.reason or "halted", level="error")
            self._running = False
        if inventory.up_shares < float(self.market.min_order_size) and inventory.dn_shares < float(self.market.min_order_size) and (inventory.up_shares > 0.1 or inventory.dn_shares > 0.1):
            self.set_alert("residual_inventory_v2", "Residual inventory below PM minimum", level="warning")
        else:
            self.clear_alert("residual_inventory_v2")

        self._last_snapshot = snapshot
        self._last_inventory = inventory
        self._last_risk = effective_risk
        self._last_plan = plan
        self._last_analytics = analytics
        self._last_health = health
        if lifecycle in {"expired", "halted"}:
            self._capture_terminal_state(
                wallet_total_usdc=float(inventory.wallet_total_usdc),
                up_shares=float(inventory.up_shares),
                dn_shares=float(inventory.dn_shares),
                pnl_equity_usd=float(self._session_pnl_equity_usd),
            )
        transport_totals = api_stats.get("transport_total_by_op")
        if not isinstance(transport_totals, dict):
            transport_totals = {}
        has_recent_transport_window = "transport_recent_60s_total" in api_stats
        transport_failures = int(api_stats.get("transport_recent_60s_total") or 0)
        if not has_recent_transport_window:
            transport_failures = int(sum(int(v or 0) for v in transport_totals.values()))
        execution = self.tracker.execution_state(
            active_orders=self.gateway.active_orders(),
            transport_failures=transport_failures,
            last_api_error=health.last_api_error,
            last_fallback_poll_count=health.last_fallback_poll_count,
            up_token_id=self.market.up_token_id,
            dn_token_id=self.market.dn_token_id,
        )
        execution = replace(
            execution,
            recent_cancelled_sell_reserve_up=float(sell_release_lag.get("up_reserve", 0.0) or 0.0),
            recent_cancelled_sell_reserve_dn=float(sell_release_lag.get("dn_reserve", 0.0) or 0.0),
            sell_release_lag_up_sec=float(sell_release_lag.get("up_seconds_left", 0.0) or 0.0),
            sell_release_lag_dn_sec=float(sell_release_lag.get("dn_seconds_left", 0.0) or 0.0),
            up_cooldown_sec=float(sell_release_lag.get("up_cooldown_sec", 0.0) or 0.0),
            dn_cooldown_sec=float(sell_release_lag.get("dn_cooldown_sec", 0.0) or 0.0),
            active_sell_release_reason=str(sell_release_lag.get("active_reason") or ""),
            last_sellability_lag_reason=str(sell_release_lag.get("reason") or ""),
        )
        self._last_execution = execution
        state = EngineState(
            lifecycle=lifecycle,  # type: ignore[arg-type]
            market=snapshot,
            inventory=inventory,
            risk=effective_risk,
            current_quotes=plan,
            execution=execution,
            analytics=analytics,
            health=health,
            alerts=sorted(self._alerts.values(), key=lambda x: x.get("ts", 0.0), reverse=True),
        )
        for callback in self._snapshot_callbacks:
            try:
                callback(self.snapshot())
            except Exception:
                pass

    def snapshot(self, *, app_version: str = "", app_git_hash: str = "") -> dict[str, Any]:
        state = EngineState(
            lifecycle=self.state_machine.lifecycle,  # type: ignore[arg-type]
            market=self._last_snapshot,
            inventory=self._last_inventory,
            risk=self._last_risk,
            current_quotes=self._last_plan,
            execution=self._last_execution,
            analytics=self._last_analytics,
            health=self._last_health,
            alerts=sorted(self._alerts.values(), key=lambda x: x.get("ts", 0.0), reverse=True),
        )
        snap = serialize_engine_state(
            state,
            config=self.config,
            app_version=app_version,
            app_git_hash=app_git_hash,
        )
        grace_active, grace_sec = self._post_terminal_cleanup_grace_state(now=time.time())
        snap["runtime"] = {
            "last_terminal_reason": self._last_terminal_reason,
            "last_terminal_ts": self._last_terminal_ts,
            "last_terminal_wallet_total_usdc": float(self._last_terminal_wallet_total_usdc),
            "last_terminal_up_shares": float(self._last_terminal_up_shares),
            "last_terminal_dn_shares": float(self._last_terminal_dn_shares),
            "last_terminal_pnl_equity_usd": float(self._last_terminal_pnl_equity_usd),
            "terminal_liquidation_active": bool(self._terminal_liquidation_active),
            "terminal_liquidation_attempted_orders": int(self._terminal_liquidation_attempted_orders),
            "terminal_liquidation_placed_orders": int(self._terminal_liquidation_placed_orders),
            "terminal_liquidation_remaining_up": float(self._terminal_liquidation_remaining_up),
            "terminal_liquidation_remaining_dn": float(self._terminal_liquidation_remaining_dn),
            "terminal_liquidation_done": bool(self._terminal_liquidation_done),
            "terminal_liquidation_reason": str(self._terminal_liquidation_reason or ""),
            "post_terminal_cleanup_grace_active": bool(grace_active),
            "post_terminal_cleanup_grace_sec": float(grace_sec),
            "drawdown_breach_ticks": int(self._drawdown_breach_ticks),
            "force_normal_soft_mode_paper": bool(self._force_normal_soft_mode_paper),
            "force_normal_no_guards_paper": bool(self._force_normal_no_guards_paper),
            "drawdown_breach_age_sec": (
                max(0.0, time.time() - self._drawdown_breach_started_ts)
                if self._drawdown_breach_started_ts > 0.0
                else 0.0
            ),
        }
        snap["is_running"] = bool(self._running)
        snap["started_at"] = self._started_at
        return snap


# Backward-compatible alias for legacy imports.
MMRuntimeV2 = MarketMakerV2
