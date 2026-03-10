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
)


@dataclass
class _PendingMarkoutEval:
    ts: float
    token_side: str
    side: str
    price: float


class MarketMakerV2:
    OPERATOR_PNL_EMA_ALPHA = 0.20
    POST_FILL_MARKOUT_EVAL_SEC = 5.0
    TOXIC_FILL_MARKOUT_TICKS = 3
    NEGATIVE_MARKOUT_TICKS = 1
    HARD_BLOCK_NEGATIVE_STREAK = 3
    HARD_BLOCK_TOXIC_STREAK = 2
    SIDE_REENTRY_COOLDOWN_SEC = 12.0

    def __init__(self, feed_state: Any, clob_client: Any, config: MMConfigV2):
        self.feed_state = feed_state
        self.config = config
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
        self._emergency_taker_forced_history: list[tuple[float, int]] = []
        self._one_sided_bid_streak_outside: int = 0
        self._unwind_deferred_history: list[tuple[float, int]] = []
        self._forced_unwind_extreme_excess_history: list[tuple[float, int]] = []
        self._prev_active_order_ids: set[str] = set()
        self._starting_portfolio = 0.0
        self._starting_usdc = 0.0
        self._session_pnl = 0.0
        self._session_pnl_equity_usd = 0.0
        self._session_pnl_operator_ema_usd = 0.0
        self._session_pnl_operator_usd = 0.0
        self._operator_pnl_initialized = False
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
        self._session_pnl_operator_ema_usd = 0.0
        self._session_pnl_operator_usd = 0.0
        self._operator_pnl_initialized = False
        self._drawdown_breach_ticks = 0
        self._drawdown_breach_started_ts = 0.0
        self._drawdown_breach_active = False
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
    ) -> tuple[float, float, float, float]:
        position_mark_bid, position_mark_mid = self._position_mark_values(
            snapshot=snapshot,
            inventory=inventory,
        )
        portfolio_mark_value = max(0.0, float(total_usdc)) + position_mark_bid
        tradeable_portfolio_value = max(0.0, float(inventory.free_usdc)) + position_mark_bid
        return position_mark_bid, position_mark_mid, portfolio_mark_value, tradeable_portfolio_value

    def _update_session_pnl(
        self,
        inventory: PairInventoryState,
        *,
        total_usdc: float,
        snapshot: PairMarketSnapshot,
    ) -> tuple[float, float, float, float]:
        # PnL must use wallet-total USDC + marked inventory value.
        # Pending/reserved order bookkeeping must not move session PnL.
        position_mark_bid, position_mark_mid, current_portfolio, tradeable_portfolio = self._portfolio_components(
            inventory=inventory,
            total_usdc=total_usdc,
            snapshot=snapshot,
        )
        equity_pnl = current_portfolio - self._starting_portfolio
        self._session_pnl_equity_usd = float(equity_pnl)
        # Backward-compatible alias.
        self._session_pnl = self._session_pnl_equity_usd
        if not self._operator_pnl_initialized:
            self._session_pnl_operator_ema_usd = self._session_pnl_equity_usd
            self._operator_pnl_initialized = True
        else:
            alpha = float(self.OPERATOR_PNL_EMA_ALPHA)
            self._session_pnl_operator_ema_usd = (
                alpha * self._session_pnl_equity_usd
                + (1.0 - alpha) * self._session_pnl_operator_ema_usd
            )
        self._session_pnl_operator_usd = self._session_pnl_operator_ema_usd
        return position_mark_bid, position_mark_mid, current_portfolio, tradeable_portfolio

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

    def _update_true_drift_progress(self, inventory: PairInventoryState) -> tuple[float, float]:
        if not self.reconcile.true_drift:
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
        )
        inventory.sellable_up_shares = sellable_up
        inventory.sellable_dn_shares = sellable_dn
        (
            position_mark_value_bid,
            position_mark_value_mid,
            portfolio_mark_value,
            tradeable_portfolio_value,
        ) = self._update_session_pnl(
            inventory,
            total_usdc=total_usdc,
            snapshot=snapshot,
        )
        drawdown_breach_ticks, drawdown_breach_age_sec, drawdown_breach_active = self._update_drawdown_breach(
            self._session_pnl_equity_usd,
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
            session_pnl_operator_usd=self._session_pnl_operator_usd,
            session_pnl_operator_ema_usd=self._session_pnl_operator_ema_usd,
        )
        true_drift_age_sec, true_drift_no_progress_sec = self._update_true_drift_progress(inventory)
        health = self._build_health(
            api_stats=api_stats,
            sellability_lag_active=bool(sell_release_lag.get("active")),
            wallet_snapshot_stale=effective_wallet_stale,
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
        ctx = QuoteContext(
            tick_size=float(self.market.tick_size),
            min_order_size=float(self.market.min_order_size),
            allow_naked_sells=self.gateway.supports_naked_sells(),
        )
        provisional_plan = QuotePolicyV2(self.config).generate(
            snapshot=snapshot,
            inventory=inventory,
            risk=risk,
            ctx=ctx,
        )
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
        unwind_target_mismatch_sec = self._update_unwind_target_mismatch(
            effective_soft_mode=effective_risk.soft_mode,
            target_soft_mode=effective_risk.target_soft_mode,
        )
        outside_near_expiry = float(snapshot.time_left_sec) > float(self.config.unwind_window_sec)
        if lifecycle in {"halted", "expired"}:
            await self.gateway.cancel_all()
            plan = QuotePlan(None, None, None, None, lifecycle, risk.reason)
            terminal_reason = risk.reason if lifecycle == "halted" else "expired"
            self._set_terminal_reason(terminal_reason or lifecycle)
            # Expired/halted must terminate the loop so the outer runtime can
            # start the next window without manual recovery.
            self._running = False
        else:
            plan = QuotePolicyV2(self.config).generate(
                snapshot=snapshot,
                inventory=inventory,
                risk=effective_risk,
                ctx=ctx,
            )
            await self.execution_policy.sync(plan)
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
        if dual_bid_mode_eligible:
            self._dual_bid_outside_sample_history.append((now, 1))
            self._dual_bid_outside_success_history.append((now, 1 if dual_bid_active else 0))
            if one_sided_bid_active:
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
        analytics = AnalyticsState(
            fill_count=len(self._fills),
            session_pnl=self._session_pnl_equity_usd,
            session_pnl_equity_usd=self._session_pnl_equity_usd,
            session_pnl_operator_usd=self._session_pnl_operator_usd,
            session_pnl_operator_ema_usd=self._session_pnl_operator_ema_usd,
            position_mark_value_usd=float(position_mark_value_bid),
            position_mark_value_bid_usd=float(position_mark_value_bid),
            position_mark_value_mid_usd=float(position_mark_value_mid),
            portfolio_mark_value_usd=float(portfolio_mark_value),
            tradeable_portfolio_value_usd=float(tradeable_portfolio_value),
            anchor_divergence_up=float(snapshot.anchor_divergence_up),
            anchor_divergence_dn=float(snapshot.anchor_divergence_dn),
            quote_shift_from_mid_up=float(quote_shift_from_mid_up),
            quote_shift_from_mid_dn=float(quote_shift_from_mid_dn),
            post_fill_markout_5s_up=float(self._post_fill_markout_5s_up),
            post_fill_markout_5s_dn=float(self._post_fill_markout_5s_dn),
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
            markout_5s=0.0,
            spread_capture_usd=0.0,
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
        snap["runtime"] = {
            "last_terminal_reason": self._last_terminal_reason,
            "last_terminal_ts": self._last_terminal_ts,
            "last_terminal_wallet_total_usdc": float(self._last_terminal_wallet_total_usdc),
            "last_terminal_up_shares": float(self._last_terminal_up_shares),
            "last_terminal_dn_shares": float(self._last_terminal_dn_shares),
            "last_terminal_pnl_equity_usd": float(self._last_terminal_pnl_equity_usd),
            "drawdown_breach_ticks": int(self._drawdown_breach_ticks),
            "drawdown_breach_age_sec": (
                max(0.0, time.time() - self._drawdown_breach_started_ts)
                if self._drawdown_breach_started_ts > 0.0
                else 0.0
            ),
        }
        snap["is_running"] = bool(self._running)
        snap["started_at"] = self._started_at
        return snap
