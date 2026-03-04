from __future__ import annotations

import asyncio
from dataclasses import replace
import time
from typing import Any

from mm.heartbeat import HeartbeatManager
from mm.runtime_metrics import runtime_metrics
from mm.types import Fill, MarketInfo

from .config import MMConfigV2
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


class MarketMakerV2:
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
        self._fills: list[Fill] = []
        self._excess_history: list[tuple[float, float]] = []
        self._quote_presence_history: list[tuple[float, tuple[bool, bool]]] = []
        self._mode_history: list[tuple[float, str]] = []
        self._starting_portfolio = 0.0
        self._starting_usdc = 0.0
        self._session_pnl = 0.0
        self._last_snapshot: PairMarketSnapshot | None = None
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
        self._last_analytics = AnalyticsState()
        self._last_health = HealthState()
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

    async def _on_heartbeat_failure(self) -> None:
        self._heartbeat_failed = True
        self.set_alert("heartbeat_failure_v2", "Heartbeat failure", level="error")

    async def start(self) -> None:
        if self._running:
            return
        if not self.market:
            raise RuntimeError("market is not set")
        self._running = True
        self._started_at = time.time()
        up, dn, total_usdc, available_usdc = await self.gateway.get_balances()
        if up is None or dn is None:
            up = 0.0
            dn = 0.0
        self.reconcile.align(up, dn)
        self._starting_usdc = float(total_usdc or 0.0)
        fv_up = float(getattr(self.feed_state, "pm_up", 0.5) or 0.5)
        fv_dn = float(getattr(self.feed_state, "pm_dn", 0.5) or max(0.01, 1.0 - fv_up))
        self._starting_portfolio = float(total_usdc or 0.0) + up * fv_up + dn * fv_dn
        self.heartbeat.start()
        self._task = asyncio.create_task(self._run_loop())

    async def stop(self, *, liquidate: bool = True) -> None:
        self._running = False
        if self._task and not self._task.done():
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass
        self._task = None
        await self.gateway.cancel_all()
        await self.heartbeat.stop()

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

    def _update_session_pnl(self, inventory: PairInventoryState) -> None:
        current_portfolio = inventory.free_usdc + inventory.reserved_usdc + inventory.total_inventory_value_usd
        self._session_pnl = current_portfolio - self._starting_portfolio

    def _build_health(self) -> HealthState:
        api_stats = self.gateway.api_error_stats()
        recent = api_stats.get("recent") or []
        last_message = ""
        if recent:
            last_message = str(recent[-1].get("message") or "")
        total_failures = int(sum(int(v or 0) for v in (api_stats.get("total_by_op") or {}).values()))
        transport_ok = total_failures < int(self.config.max_transport_failures)
        last_fallback = int(getattr(self.gateway.order_mgr, "_last_fallback_poll_count", 0))
        return HealthState(
            reconcile_status=self.reconcile.status,
            heartbeat_ok=not self._heartbeat_failed,
            transport_ok=transport_ok,
            last_api_error=last_message,
            last_fallback_poll_count=last_fallback,
            true_drift=self.reconcile.true_drift,
            residual_inventory_failure=bool(self._alerts.get("residual_inventory_v2")),
        )

    def _prune_history(self, entries: list[tuple[float, Any]], *, max_age_sec: float = 120.0) -> None:
        cutoff = time.time() - max_age_sec
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

    async def _tick(self) -> None:
        if not self.market:
            return
        fills = await self.gateway.check_fills()
        for fill in fills:
            self._fills.append(fill)
            self.reconcile.record_fill(fill, self.market)
        up_book, dn_book = await self.gateway.get_books()
        valuation, snapshot = self.valuation.compute(
            market=self.market,
            feed_state=self.feed_state,
            up_book=up_book,
            dn_book=dn_book,
        )
        self.gateway.sync_paper_prices(
            fv_up=valuation.fv_up,
            fv_dn=valuation.fv_dn,
            pm_prices={"up": snapshot.pm_mid_up, "dn": snapshot.pm_mid_dn},
        )
        up, dn, total_usdc, available_usdc = await self.gateway.get_balances()
        up = float(up or 0.0)
        dn = float(dn or 0.0)
        total_usdc = float(total_usdc or 0.0)
        inventory = self.reconcile.reconcile(
            market=self.market,
            real_up=up,
            real_dn=dn,
            total_usdc=total_usdc,
            available_usdc=available_usdc,
            active_orders=self.gateway.active_orders(),
            fv_up=valuation.fv_up,
            fv_dn=valuation.fv_dn,
        )
        self._update_session_pnl(inventory)
        pre_analytics = AnalyticsState(
            fill_count=len(self._fills),
            session_pnl=self._session_pnl,
        )
        health = self._build_health()
        risk = self.risk_kernel.evaluate(
            snapshot=snapshot,
            inventory=inventory,
            analytics=pre_analytics,
            health=health,
        )
        inventory.inventory_pressure_abs = risk.inventory_pressure_abs
        inventory.inventory_pressure_signed = risk.inventory_pressure_signed
        ctx = QuoteContext(
            tick_size=float(self.market.tick_size),
            min_order_size=float(self.market.min_order_size),
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
        )
        lifecycle = transition.lifecycle
        if lifecycle in {"halted", "expired"}:
            await self.gateway.cancel_all()
            plan = QuotePlan(None, None, None, None, lifecycle, risk.reason)
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
        self._prune_history(self._excess_history)
        self._prune_history(self._quote_presence_history)
        self._prune_history(self._mode_history)
        quote_presence_ratio, four_quote_presence_ratio = self._quote_presence_ratio()
        analytics = AnalyticsState(
            fill_count=len(self._fills),
            session_pnl=self._session_pnl,
            markout_1s=0.0,
            markout_5s=0.0,
            spread_capture_usd=0.0,
            fill_rate=0.0,
            quote_presence_ratio=quote_presence_ratio,
            excess_value_usd=float(inventory.excess_value_usd),
            inventory_pressure_abs=float(risk.inventory_pressure_abs),
            inventory_pressure_signed=float(risk.inventory_pressure_signed),
            inventory_half_life_sec=self._estimate_inventory_half_life(),
            four_quote_presence_ratio=four_quote_presence_ratio,
            helpful_quote_count=helpful_quote_count,
            harmful_quote_count=harmful_quote_count,
            quote_balance_state=plan.quote_balance_state,
            min_viable_clip_usd=float(QuotePolicyV2(self.config)._min_viable_clip_usd(snapshot, ctx)),
            quote_viability_reason=plan.quote_viability_reason,
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
        state = EngineState(
            lifecycle=lifecycle,  # type: ignore[arg-type]
            market=snapshot,
            inventory=inventory,
            risk=risk,
            current_quotes=plan,
            execution=self.tracker.execution_state(
                active_orders=self.gateway.active_orders(),
                transport_failures=int(sum(int(v or 0) for v in (self.gateway.api_error_stats().get("total_by_op") or {}).values())),
                last_api_error=health.last_api_error,
                last_fallback_poll_count=health.last_fallback_poll_count,
                up_token_id=self.market.up_token_id,
                dn_token_id=self.market.dn_token_id,
            ),
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
            execution=self.tracker.execution_state(
                active_orders=self.gateway.active_orders(),
                transport_failures=int(sum(int(v or 0) for v in (self.gateway.api_error_stats().get("total_by_op") or {}).values())),
                last_api_error=self._last_health.last_api_error,
                last_fallback_poll_count=self._last_health.last_fallback_poll_count,
                up_token_id=self.market.up_token_id if self.market else "",
                dn_token_id=self.market.dn_token_id if self.market else "",
            ),
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
        snap["is_running"] = bool(self._running)
        snap["started_at"] = self._started_at
        return snap
