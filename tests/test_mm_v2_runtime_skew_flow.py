from __future__ import annotations

import os
import sys
import time
from types import SimpleNamespace

import pytest


BASE = os.path.dirname(os.path.dirname(__file__))
SRC = os.path.join(BASE, "src")
if SRC not in sys.path:
    sys.path.insert(0, SRC)
if BASE not in sys.path:
    sys.path.insert(0, BASE)

from mm_v2.config import (
    EMERGENCY_EXIT_CONFIRM_TICKS,
    MMConfigV2,
    NO_HELPFUL_TICKS_FOR_UNWIND,
    UNWIND_EXIT_CONFIRM_TICKS,
    UNWIND_STUCK_WINDOW_SEC,
)
from mm_v2.execution_policy import ExecutionPolicyV2
from mm_v2.order_tracker import OrderTrackerV2
from mm_v2.pair_valuation import PairValuationResult
from mm_v2.quote_policy import QuoteContext, QuotePolicyV2
from mm_v2.reconcile import ReconcileV2
from mm_v2.risk_kernel import HardSafetyKernel
from mm_v2.runtime import MarketMakerV2
from mm_v2.state_machine import StateMachineV2
from mm_v2.types import AnalyticsState, HealthState, PairInventoryState, PairMarketSnapshot, QuoteIntent, QuotePlan, QuoteViabilitySummary, RiskRegime
from mm_shared.types import MarketInfo, Quote


def _snapshot(**overrides) -> PairMarketSnapshot:
    payload = dict(
        ts=time.time(),
        market_id="btc-15m",
        up_token_id="up-token",
        dn_token_id="dn-token",
        time_left_sec=900.0,
        fv_up=0.54,
        fv_dn=0.46,
        fv_confidence=0.9,
        pm_mid_up=0.53,
        pm_mid_dn=0.47,
        up_best_bid=0.52,
        up_best_ask=0.54,
        dn_best_bid=0.46,
        dn_best_ask=0.48,
        up_bid_depth_usd=200.0,
        up_ask_depth_usd=200.0,
        dn_bid_depth_usd=200.0,
        dn_ask_depth_usd=200.0,
        market_quality_score=0.9,
        market_tradeable=True,
        divergence_up=0.01,
        divergence_dn=0.01,
    )
    payload.update(overrides)
    return PairMarketSnapshot(**payload)


def _inventory(**overrides) -> PairInventoryState:
    payload = dict(
        up_shares=0.0,
        dn_shares=0.0,
        free_usdc=50.0,
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
    payload.update(overrides)
    return PairInventoryState(**payload)


def _market() -> MarketInfo:
    now = time.time()
    return MarketInfo(
        coin="BTC",
        timeframe="15m",
        question="BTC 15m",
        condition_id="cond",
        up_token_id="up-token",
        dn_token_id="dn-token",
        strike=100000.0,
        tick_size=0.01,
        min_order_size=5.0,
        window_start=now,
        window_end=now + 900.0,
    )


def _risk_and_plan(cfg: MMConfigV2, snapshot: PairMarketSnapshot, inventory: PairInventoryState):
    risk = HardSafetyKernel(cfg).evaluate(
        snapshot=snapshot,
        inventory=inventory,
        analytics=AnalyticsState(),
        health=HealthState(),
    )
    plan = QuotePolicyV2(cfg).generate(
        snapshot=snapshot,
        inventory=inventory,
        risk=risk,
        ctx=QuoteContext(tick_size=0.01, min_order_size=5.0),
    )
    viability = QuoteViabilitySummary(
        any_quote=any([plan.up_bid, plan.up_ask, plan.dn_bid, plan.dn_ask]),
        four_quotes=all([plan.up_bid, plan.up_ask, plan.dn_bid, plan.dn_ask]),
        helpful_count=sum(1 for q in (plan.up_bid, plan.up_ask, plan.dn_bid, plan.dn_ask) if q and q.inventory_effect == "helpful"),
        harmful_count=sum(1 for q in (plan.up_bid, plan.up_ask, plan.dn_bid, plan.dn_ask) if q and q.inventory_effect == "harmful"),
        helpful_only=False,
        harmful_only=False,
        four_quote_presence_ratio=1.0 if all([plan.up_bid, plan.up_ask, plan.dn_bid, plan.dn_ask]) else 0.25,
    )
    viability.helpful_only = viability.helpful_count > 0 and viability.harmful_count == 0
    viability.harmful_only = viability.harmful_count > 0 and viability.helpful_count == 0
    return risk, plan, viability


def _risk(**overrides) -> RiskRegime:
    payload = dict(
        soft_mode="defensive",
        hard_mode="none",
        reason="x",
        inventory_pressure=0.0,
        edge_score=0.0,
        drawdown_pct_budget=1.0,
        inventory_side="flat",
        inventory_pressure_abs=0.0,
        inventory_pressure_signed=0.0,
        quality_pressure=0.0,
        target_soft_mode="defensive",
    )
    payload.update(overrides)
    return RiskRegime(**payload)


def test_buy_requote_threshold_is_more_sensitive_than_sell():
    class _Gateway:
        def active_orders(self):
            return {}

    tracker = OrderTrackerV2()
    policy = ExecutionPolicyV2(_Gateway(), tracker, requote_threshold_bps=10.0)
    current_buy = QuoteIntent("up-token", "BUY", 0.50, 5.0, "base_bid", True)
    new_buy = QuoteIntent("up-token", "BUY", 0.5004, 5.0, "base_bid", True)
    current_sell = QuoteIntent("up-token", "SELL", 0.50, 5.0, "base_ask", True)
    new_sell = QuoteIntent("up-token", "SELL", 0.5004, 5.0, "base_ask", True)

    assert policy._materially_different(current_buy, new_buy) is True
    assert policy._materially_different(current_sell, new_sell) is False


def test_force_normal_soft_mode_active_only_outside_terminal_and_without_hard_risk():
    class _MockClient:
        pass

    mm = MarketMakerV2(
        SimpleNamespace(),
        _MockClient(),
        MMConfigV2(unwind_window_sec=90.0, emergency_taker_start_sec=20.0),
        force_normal_soft_mode_paper=True,
    )
    assert mm._force_normal_soft_mode_active(
        snapshot=_snapshot(time_left_sec=300.0),
        risk=_risk(),
    ) is True
    assert mm._force_normal_soft_mode_active(
        snapshot=_snapshot(time_left_sec=60.0),
        risk=_risk(),
    ) is False
    assert mm._force_normal_soft_mode_active(
        snapshot=_snapshot(time_left_sec=300.0),
        risk=_risk(hard_mode="emergency_unwind"),
    ) is False


def test_force_normal_no_guards_active_only_outside_terminal_and_without_hard_risk():
    class _MockClient:
        pass

    mm = MarketMakerV2(
        SimpleNamespace(),
        _MockClient(),
        MMConfigV2(unwind_window_sec=90.0, emergency_taker_start_sec=20.0),
        force_normal_no_guards_paper=True,
    )
    assert mm._force_normal_no_guards_active(
        snapshot=_snapshot(time_left_sec=300.0),
        risk=_risk(),
    ) is True
    assert mm._force_normal_no_guards_active(
        snapshot=_snapshot(time_left_sec=60.0),
        risk=_risk(),
    ) is False
    assert mm._force_normal_no_guards_active(
        snapshot=_snapshot(time_left_sec=300.0),
        risk=_risk(hard_mode="emergency_unwind"),
    ) is False


def test_dn_excess_near_endpoint_below_hard_cap_keeps_safe_plan():
    cfg = MMConfigV2(session_budget_usd=50.0, base_clip_usd=6.0)
    snapshot = _snapshot(
        fv_up=0.99,
        fv_dn=0.01,
        pm_mid_up=0.99,
        pm_mid_dn=0.01,
        up_best_bid=0.98,
        up_best_ask=0.99,
        dn_best_bid=0.01,
        dn_best_ask=0.02,
    )
    inventory = _inventory(
        dn_shares=40.0,
        excess_dn_qty=40.0,
        excess_dn_value_usd=12.0,
        excess_value_usd=12.0,
        signed_excess_value_usd=-12.0,
        total_inventory_value_usd=12.0,
    )
    risk, plan, _ = _risk_and_plan(cfg, snapshot, inventory)
    assert risk.hard_mode == "none"
    assert risk.soft_mode in {"inventory_skewed", "defensive"}
    assert plan.quote_balance_state in {"balanced", "helpful_only", "reduced", "harmful_only_blocked"}
    assert not (plan.quote_balance_state == "harmful_only_blocked" and plan.up_bid is None and plan.dn_ask is None)


def test_untradeable_material_inventory_enters_defensive_but_keeps_helpful_bid():
    cfg = MMConfigV2(session_budget_usd=30.0, base_clip_usd=4.0)
    snapshot = _snapshot(
        market_tradeable=False,
        market_quality_score=0.62,
        divergence_up=0.08,
        divergence_dn=0.07,
        up_best_bid=0.01,
        up_best_ask=0.03,
        dn_best_bid=0.97,
        dn_best_ask=0.99,
    )
    inventory = _inventory(
        dn_shares=8.0,
        excess_dn_qty=8.0,
        excess_dn_value_usd=6.5,
        excess_value_usd=6.5,
        signed_excess_value_usd=-6.5,
        total_inventory_value_usd=6.5,
        free_usdc=30.0,
    )
    risk, plan, _ = _risk_and_plan(cfg, snapshot, inventory)
    assert risk.soft_mode == "defensive"
    assert risk.hard_mode == "none"
    assert plan.up_bid is not None
    assert plan.dn_bid is None
    assert plan.quote_balance_state == "helpful_only"


@pytest.mark.asyncio
async def test_execution_policy_holds_helpful_sell_during_marketability_rest_window(monkeypatch):
    class DummyGateway:
        def __init__(self) -> None:
            self.cancelled: list[str] = []
            self.placed: list[QuoteIntent] = []
            self._active_orders = {
                "oid-1": Quote(side="SELL", token_id="up-token", price=0.55, size=5.0),
            }

        def active_orders(self):
            return dict(self._active_orders)

        async def cancel(self, order_id: str) -> bool:
            self.cancelled.append(order_id)
            self._active_orders.pop(order_id, None)
            return True

        async def place_intent(self, intent: QuoteIntent) -> str | None:
            self.placed.append(intent)
            order_id = f"oid-{len(self.placed) + 1}"
            self._active_orders[order_id] = Quote(
                side=intent.side,
                token_id=intent.token,
                price=float(intent.price),
                size=float(intent.size),
            )
            return order_id

    gateway = DummyGateway()
    tracker = OrderTrackerV2()
    tracker.set(
        "up_sell",
        "oid-1",
        QuoteIntent(
            token="up-token",
            side="SELL",
            price=0.55,
            size=5.0,
            quote_role="base_ask",
            post_only=True,
            inventory_effect="helpful",
            min_rest_sec=6.0,
        ),
    )
    tracker.slots["up_sell"].created_at = 100.0
    policy = ExecutionPolicyV2(gateway, tracker, requote_threshold_bps=15.0)
    desired = QuoteIntent(
        token="up-token",
        side="SELL",
        price=0.58,
        size=5.0,
        quote_role="base_ask",
        post_only=True,
        inventory_effect="helpful",
        min_rest_sec=6.0,
    )
    monkeypatch.setattr("mm_v2.execution_policy.time.time", lambda: 103.0)
    await policy.sync(QuotePlan(None, desired, None, None, "defensive", "marketability"))
    assert gateway.cancelled == []
    assert gateway.placed == []

    monkeypatch.setattr("mm_v2.execution_policy.time.time", lambda: 107.5)
    await policy.sync(QuotePlan(None, desired, None, None, "defensive", "marketability"))
    assert gateway.cancelled == ["oid-1"]
    assert len(gateway.placed) == 1


@pytest.mark.asyncio
async def test_execution_policy_sell_churn_hold_avoids_cancel_for_small_reprice(monkeypatch):
    class DummyGateway:
        def __init__(self) -> None:
            self.cancelled: list[str] = []
            self.placed: list[QuoteIntent] = []
            self._active_orders = {
                "oid-1": Quote(side="SELL", token_id="up-token", price=0.55, size=5.0),
            }

        def active_orders(self):
            return dict(self._active_orders)

        async def cancel(self, order_id: str) -> bool:
            self.cancelled.append(order_id)
            self._active_orders.pop(order_id, None)
            return True

        async def place_intent(self, intent: QuoteIntent) -> str | None:
            self.placed.append(intent)
            order_id = f"oid-{len(self.placed) + 1}"
            self._active_orders[order_id] = Quote(
                side=intent.side,
                token_id=intent.token,
                price=float(intent.price),
                size=float(intent.size),
            )
            return order_id

    gateway = DummyGateway()
    tracker = OrderTrackerV2()
    tracker.set(
        "up_sell",
        "oid-1",
        QuoteIntent(
            token="up-token",
            side="SELL",
            price=0.55,
            size=5.0,
            quote_role="base_ask",
            post_only=True,
            inventory_effect="helpful",
            hold_mode_active=True,
            hold_mode_reason="sell_churn_hold_mode",
            hold_reprice_threshold_ticks=6,
            hold_max_age_sec=20.0,
            hold_tick_size=0.01,
        ),
    )
    tracker.slots["up_sell"].created_at = 100.0
    policy = ExecutionPolicyV2(gateway, tracker, requote_threshold_bps=15.0)
    desired = QuoteIntent(
        token="up-token",
        side="SELL",
        price=0.53,
        size=5.0,
        quote_role="base_ask",
        post_only=True,
        inventory_effect="helpful",
        hold_mode_active=True,
        hold_mode_reason="sell_churn_hold_mode",
        hold_reprice_threshold_ticks=6,
        hold_max_age_sec=20.0,
        hold_tick_size=0.01,
    )
    monkeypatch.setattr("mm_v2.execution_policy.time.time", lambda: 110.0)
    await policy.sync(QuotePlan(None, desired, None, None, "defensive", "marketability"))
    metrics = policy.consume_sync_metrics()
    assert gateway.cancelled == []
    assert gateway.placed == []
    assert metrics["sell_churn_hold_cancel_avoided_hits"] == 1
    assert metrics["sell_churn_hold_reprice_suppressed_hits"] == 1


@pytest.mark.asyncio
async def test_execution_policy_sell_churn_hold_avoids_cancel_for_small_upward_reprice(monkeypatch):
    class DummyGateway:
        def __init__(self) -> None:
            self.cancelled: list[str] = []
            self.placed: list[QuoteIntent] = []
            self._active_orders = {
                "oid-1": Quote(side="SELL", token_id="up-token", price=0.71, size=5.0),
            }

        def active_orders(self):
            return dict(self._active_orders)

        async def cancel(self, order_id: str) -> bool:
            self.cancelled.append(order_id)
            self._active_orders.pop(order_id, None)
            return True

        async def place_intent(self, intent: QuoteIntent) -> str | None:
            self.placed.append(intent)
            order_id = f"oid-{len(self.placed) + 1}"
            self._active_orders[order_id] = Quote(
                side=intent.side,
                token_id=intent.token,
                price=float(intent.price),
                size=float(intent.size),
            )
            return order_id

    gateway = DummyGateway()
    tracker = OrderTrackerV2()
    tracker.set(
        "up_sell",
        "oid-1",
        QuoteIntent(
            token="up-token",
            side="SELL",
            price=0.71,
            size=5.0,
            quote_role="base_ask",
            post_only=True,
            inventory_effect="helpful",
            hold_mode_active=True,
            hold_mode_reason="sell_churn_hold_mode",
            hold_reprice_threshold_ticks=6,
            hold_max_age_sec=20.0,
            hold_tick_size=0.01,
        ),
    )
    tracker.slots["up_sell"].created_at = 100.0
    policy = ExecutionPolicyV2(gateway, tracker, requote_threshold_bps=15.0)
    desired = QuoteIntent(
        token="up-token",
        side="SELL",
        price=0.73,
        size=5.0,
        quote_role="base_ask",
        post_only=True,
        inventory_effect="helpful",
        hold_mode_active=True,
        hold_mode_reason="sell_churn_hold_mode",
        hold_reprice_threshold_ticks=6,
        hold_max_age_sec=20.0,
        hold_tick_size=0.01,
    )
    monkeypatch.setattr("mm_v2.execution_policy.time.time", lambda: 110.0)
    await policy.sync(QuotePlan(None, desired, None, None, "defensive", "marketability"))
    metrics = policy.consume_sync_metrics()
    assert gateway.cancelled == []
    assert gateway.placed == []
    assert metrics["sell_churn_hold_cancel_avoided_hits"] == 1
    assert metrics["sell_churn_hold_reprice_suppressed_hits"] == 1


@pytest.mark.asyncio
async def test_execution_policy_sell_reprice_hold_avoids_cancel_for_small_reprice(monkeypatch):
    class DummyGateway:
        def __init__(self) -> None:
            self.cancelled: list[str] = []
            self.placed: list[QuoteIntent] = []
            self._active_orders = {
                "oid-1": Quote(side="SELL", token_id="up-token", price=0.54, size=7.8),
            }

        def active_orders(self):
            return dict(self._active_orders)

        async def cancel(self, order_id: str) -> bool:
            self.cancelled.append(order_id)
            self._active_orders.pop(order_id, None)
            return True

        async def place_intent(self, intent: QuoteIntent) -> str | None:
            self.placed.append(intent)
            order_id = f"oid-{len(self.placed) + 1}"
            self._active_orders[order_id] = Quote(
                side=intent.side,
                token_id=intent.token,
                price=float(intent.price),
                size=float(intent.size),
            )
            return order_id

    gateway = DummyGateway()
    tracker = OrderTrackerV2()
    tracker.set(
        "up_sell",
        "oid-1",
        QuoteIntent(
            token="up-token",
            side="SELL",
            price=0.54,
            size=7.8,
            quote_role="base_ask",
            post_only=True,
            inventory_effect="neutral",
            hold_mode_active=True,
            hold_mode_reason="sell_reprice_hold_mode",
            hold_reprice_threshold_ticks=2,
            hold_max_age_sec=6.0,
            hold_tick_size=0.01,
        ),
    )
    tracker.slots["up_sell"].created_at = 100.0
    policy = ExecutionPolicyV2(gateway, tracker, requote_threshold_bps=15.0)
    desired = QuoteIntent(
        token="up-token",
        side="SELL",
        price=0.55,
        size=5.4,
        quote_role="base_ask",
        post_only=True,
        inventory_effect="neutral",
        hold_mode_active=True,
        hold_mode_reason="sell_reprice_hold_mode",
        hold_reprice_threshold_ticks=2,
        hold_max_age_sec=6.0,
        hold_tick_size=0.01,
    )
    monkeypatch.setattr("mm_v2.execution_policy.time.time", lambda: 103.0)
    await policy.sync(QuotePlan(None, desired, None, None, "normal", "sell_reprice"))

    assert gateway.cancelled == []
    assert gateway.placed == []


@pytest.mark.asyncio
async def test_execution_policy_sell_churn_hold_allows_inventory_backed_non_helpful_sell(monkeypatch):
    class DummyGateway:
        def __init__(self) -> None:
            self.cancelled: list[str] = []
            self.placed: list[QuoteIntent] = []
            self._active_orders = {
                "oid-1": Quote(side="SELL", token_id="dn-token", price=0.97, size=5.0),
            }

        def active_orders(self):
            return dict(self._active_orders)

        async def cancel(self, order_id: str) -> bool:
            self.cancelled.append(order_id)
            self._active_orders.pop(order_id, None)
            return True

        async def place_intent(self, intent: QuoteIntent) -> str | None:
            self.placed.append(intent)
            order_id = f"oid-{len(self.placed) + 1}"
            self._active_orders[order_id] = Quote(
                side=intent.side,
                token_id=intent.token,
                price=float(intent.price),
                size=float(intent.size),
            )
            return order_id

    gateway = DummyGateway()
    tracker = OrderTrackerV2()
    tracker.set(
        "dn_sell",
        "oid-1",
        QuoteIntent(
            token="dn-token",
            side="SELL",
            price=0.97,
            size=5.0,
            quote_role="base_ask",
            post_only=True,
            inventory_effect="harmful",
            hold_mode_active=True,
            hold_mode_reason="sell_churn_hold_mode",
            hold_reprice_threshold_ticks=6,
            hold_max_age_sec=20.0,
            hold_tick_size=0.01,
        ),
    )
    tracker.slots["dn_sell"].created_at = 100.0
    policy = ExecutionPolicyV2(gateway, tracker, requote_threshold_bps=15.0)
    desired = QuoteIntent(
        token="dn-token",
        side="SELL",
        price=0.95,
        size=5.0,
        quote_role="base_ask",
        post_only=True,
        inventory_effect="harmful",
        hold_mode_active=True,
        hold_mode_reason="sell_churn_hold_mode",
        hold_reprice_threshold_ticks=6,
        hold_max_age_sec=20.0,
        hold_tick_size=0.01,
    )
    monkeypatch.setattr("mm_v2.execution_policy.time.time", lambda: 110.0)
    await policy.sync(QuotePlan(None, None, None, desired, "defensive", "marketability"))
    metrics = policy.consume_sync_metrics()
    assert gateway.cancelled == []
    assert gateway.placed == []
    assert metrics["sell_churn_hold_cancel_avoided_hits"] == 1
    assert metrics["sell_churn_hold_reprice_suppressed_hits"] == 1


@pytest.mark.asyncio
async def test_execution_policy_sell_churn_hold_reprices_on_large_move_or_age(monkeypatch):
    class DummyGateway:
        def __init__(self) -> None:
            self.cancelled: list[str] = []
            self.placed: list[QuoteIntent] = []
            self._active_orders = {
                "oid-1": Quote(side="SELL", token_id="up-token", price=0.55, size=5.0),
            }

        def active_orders(self):
            return dict(self._active_orders)

        async def cancel(self, order_id: str) -> bool:
            self.cancelled.append(order_id)
            self._active_orders.pop(order_id, None)
            return True

        async def place_intent(self, intent: QuoteIntent) -> str | None:
            self.placed.append(intent)
            order_id = f"oid-{len(self.placed) + 1}"
            self._active_orders[order_id] = Quote(
                side=intent.side,
                token_id=intent.token,
                price=float(intent.price),
                size=float(intent.size),
            )
            return order_id

    gateway = DummyGateway()
    tracker = OrderTrackerV2()
    tracker.set(
        "up_sell",
        "oid-1",
        QuoteIntent(
            token="up-token",
            side="SELL",
            price=0.55,
            size=5.0,
            quote_role="base_ask",
            post_only=True,
            inventory_effect="helpful",
            hold_mode_active=True,
            hold_mode_reason="sell_churn_hold_mode",
            hold_reprice_threshold_ticks=6,
            hold_max_age_sec=20.0,
            hold_tick_size=0.01,
        ),
    )
    tracker.slots["up_sell"].created_at = 100.0
    policy = ExecutionPolicyV2(gateway, tracker, requote_threshold_bps=15.0)
    large_move = QuoteIntent(
        token="up-token",
        side="SELL",
        price=0.47,
        size=5.0,
        quote_role="base_ask",
        post_only=True,
        inventory_effect="helpful",
        hold_mode_active=True,
        hold_mode_reason="sell_churn_hold_mode",
        hold_reprice_threshold_ticks=6,
        hold_max_age_sec=20.0,
        hold_tick_size=0.01,
    )
    monkeypatch.setattr("mm_v2.execution_policy.time.time", lambda: 110.0)
    await policy.sync(QuotePlan(None, large_move, None, None, "defensive", "marketability"))
    assert gateway.cancelled == ["oid-1"]
    assert len(gateway.placed) == 1

    gateway.cancelled.clear()
    gateway.placed.clear()
    tracker.set(
        "up_sell",
        "oid-2",
        QuoteIntent(
            token="up-token",
            side="SELL",
            price=0.55,
            size=5.0,
            quote_role="base_ask",
            post_only=True,
            inventory_effect="helpful",
            hold_mode_active=True,
            hold_mode_reason="sell_churn_hold_mode",
            hold_reprice_threshold_ticks=6,
            hold_max_age_sec=20.0,
            hold_tick_size=0.01,
        ),
    )
    gateway._active_orders = {"oid-2": Quote(side="SELL", token_id="up-token", price=0.55, size=5.0)}
    tracker.slots["up_sell"].created_at = 100.0
    stale_desired = QuoteIntent(
        token="up-token",
        side="SELL",
        price=0.53,
        size=5.0,
        quote_role="base_ask",
        post_only=True,
        inventory_effect="helpful",
        hold_mode_active=True,
        hold_mode_reason="sell_churn_hold_mode",
        hold_reprice_threshold_ticks=6,
        hold_max_age_sec=20.0,
        hold_tick_size=0.01,
    )
    monkeypatch.setattr("mm_v2.execution_policy.time.time", lambda: 121.0)
    await policy.sync(QuotePlan(None, stale_desired, None, None, "defensive", "marketability"))
    assert gateway.cancelled == ["oid-2"]
    assert len(gateway.placed) == 1


@pytest.mark.asyncio
async def test_execution_policy_sell_churn_hold_keeps_existing_when_plan_drops_sub_min(monkeypatch):
    class DummyGateway:
        def __init__(self) -> None:
            self.cancelled: list[str] = []
            self.placed: list[QuoteIntent] = []
            self._active_orders = {
                "oid-1": Quote(side="SELL", token_id="up-token", price=0.54, size=7.8),
            }

        def active_orders(self):
            return dict(self._active_orders)

        async def cancel(self, order_id: str) -> bool:
            self.cancelled.append(order_id)
            self._active_orders.pop(order_id, None)
            return True

        async def place_intent(self, intent: QuoteIntent) -> str | None:
            self.placed.append(intent)
            return "oid-new"

    gateway = DummyGateway()
    tracker = OrderTrackerV2()
    tracker.set(
        "up_sell",
        "oid-1",
        QuoteIntent(
            token="up-token",
            side="SELL",
            price=0.54,
            size=7.8,
            quote_role="base_ask",
            post_only=True,
            inventory_effect="helpful",
            hold_mode_active=True,
            hold_mode_reason="sell_churn_hold_mode",
            hold_reprice_threshold_ticks=6,
            hold_max_age_sec=20.0,
            hold_tick_size=0.01,
        ),
    )
    tracker.slots["up_sell"].created_at = 100.0
    policy = ExecutionPolicyV2(gateway, tracker, requote_threshold_bps=15.0)
    monkeypatch.setattr("mm_v2.execution_policy.time.time", lambda: 110.0)

    await policy.sync(QuotePlan(None, None, None, None, "defensive", "marketability"))

    assert gateway.cancelled == []
    assert gateway.placed == []
    assert tracker.get("up_sell") is not None
    metrics = policy.consume_sync_metrics()
    assert metrics["sell_churn_hold_cancel_avoided_hits"] == 1


def test_helpful_quotes_can_be_restored_by_promotion():
    cfg = MMConfigV2(session_budget_usd=50.0, base_clip_usd=6.0)
    snapshot = _snapshot(
        fv_up=0.98,
        fv_dn=0.02,
        pm_mid_up=0.98,
        pm_mid_dn=0.02,
        up_best_bid=0.97,
        up_best_ask=0.98,
        dn_best_bid=0.02,
        dn_best_ask=0.03,
    )
    inventory = _inventory(
        dn_shares=6.0,
        excess_dn_qty=6.0,
        excess_dn_value_usd=6.0,
        excess_value_usd=6.0,
        signed_excess_value_usd=-6.0,
        total_inventory_value_usd=6.0,
    )
    _, plan, viability = _risk_and_plan(cfg, snapshot, inventory)
    assert viability.helpful_count >= 1
    assert not viability.harmful_only


def test_no_progress_for_30s_with_helpful_quotes_stays_defensive():
    cfg = MMConfigV2(session_budget_usd=15.0, base_clip_usd=6.0)
    snapshot = _snapshot()
    inventory = _inventory(
        dn_shares=6.0,
        excess_dn_qty=6.0,
        excess_dn_value_usd=5.6,
        excess_value_usd=5.6,
        signed_excess_value_usd=-5.6,
        total_inventory_value_usd=5.6,
    )
    risk, plan, viability = _risk_and_plan(cfg, snapshot, inventory)
    sm = StateMachineV2(cfg)
    sm.transition(snapshot=snapshot, inventory=inventory, risk=risk, viability=viability)
    sm.transition(snapshot=snapshot, inventory=inventory, risk=risk, viability=viability)
    sm.transition(snapshot=snapshot, inventory=inventory, risk=risk, viability=viability)
    sm._excess_baseline_ts = time.time() - 31.0
    sm._excess_baseline_value_usd = 5.6
    result = sm.transition(snapshot=snapshot, inventory=inventory, risk=risk, viability=viability)
    assert result.lifecycle == "defensive"
    assert result.no_progress is True


def test_no_progress_for_30s_and_no_helpful_quotes_enters_unwind():
    cfg = MMConfigV2(session_budget_usd=15.0, base_clip_usd=6.0)
    snapshot = _snapshot()
    inventory = _inventory(
        dn_shares=6.0,
        excess_dn_qty=6.0,
        excess_dn_value_usd=5.6,
        excess_value_usd=5.6,
        signed_excess_value_usd=-5.6,
        total_inventory_value_usd=5.6,
    )
    risk, _, _ = _risk_and_plan(cfg, snapshot, inventory)
    sm = StateMachineV2(cfg)
    bad_viability = QuoteViabilitySummary(
        any_quote=True,
        four_quotes=False,
        helpful_count=0,
        harmful_count=2,
        helpful_only=False,
        harmful_only=True,
        four_quote_presence_ratio=0.10,
    )
    sm.transition(snapshot=snapshot, inventory=inventory, risk=risk, viability=bad_viability)
    sm.transition(snapshot=snapshot, inventory=inventory, risk=risk, viability=bad_viability)
    sm.transition(snapshot=snapshot, inventory=inventory, risk=risk, viability=bad_viability)
    sm._excess_baseline_ts = time.time() - 31.0
    sm._excess_baseline_value_usd = 5.6
    for _ in range(3):
        result = sm.transition(snapshot=snapshot, inventory=inventory, risk=risk, viability=bad_viability)
    assert result.lifecycle == "unwind"


def test_hard_cap_exceeded_disables_pair_expanding_intents_without_halt():
    cfg = MMConfigV2(session_budget_usd=15.0, base_clip_usd=6.0)
    snapshot = _snapshot()
    inventory = _inventory(excess_dn_value_usd=7.2, excess_value_usd=7.2, signed_excess_value_usd=-7.2)
    risk, plan, _ = _risk_and_plan(cfg, snapshot, inventory)
    assert risk.target_soft_mode == "unwind"
    assert risk.hard_mode == "none"
    assert plan.dn_bid is None
    assert plan.up_ask is None


def test_first_skew_fill_below_hard_cap_keeps_helpful_quotes_alive():
    cfg = MMConfigV2(session_budget_usd=50.0, base_clip_usd=6.0)
    snapshot = _snapshot(
        fv_up=0.98,
        fv_dn=0.02,
        pm_mid_up=0.98,
        pm_mid_dn=0.02,
        up_best_bid=0.97,
        up_best_ask=0.98,
        dn_best_bid=0.02,
        dn_best_ask=0.03,
    )
    inventory = _inventory(
        dn_shares=6.0,
        excess_dn_qty=6.0,
        excess_dn_value_usd=12.0,
        excess_value_usd=12.0,
        signed_excess_value_usd=-12.0,
        total_inventory_value_usd=12.0,
        free_usdc=50.0,
    )
    risk, plan, viability = _risk_and_plan(cfg, snapshot, inventory)
    assert risk.hard_mode == "none"
    assert viability.helpful_count >= 1
    assert plan.quote_balance_state != "none"


def test_below_hard_cap_no_early_unwind_when_helpful_quotes_alive_after_first_fill():
    cfg = MMConfigV2(session_budget_usd=50.0, base_clip_usd=6.0)
    snapshot = _snapshot(
        fv_up=0.98,
        fv_dn=0.02,
        pm_mid_up=0.98,
        pm_mid_dn=0.02,
        up_best_bid=0.97,
        up_best_ask=0.98,
        dn_best_bid=0.02,
        dn_best_ask=0.03,
    )
    inventory = _inventory(
        dn_shares=6.0,
        excess_dn_qty=6.0,
        excess_dn_value_usd=12.0,
        excess_value_usd=12.0,
        signed_excess_value_usd=-12.0,
        total_inventory_value_usd=12.0,
        free_usdc=50.0,
    )
    risk, _, viability = _risk_and_plan(cfg, snapshot, inventory)
    sm = StateMachineV2(cfg)
    sm.transition(snapshot=snapshot, inventory=inventory, risk=risk, viability=viability)
    sm.transition(snapshot=snapshot, inventory=inventory, risk=risk, viability=viability)
    sm.transition(snapshot=snapshot, inventory=inventory, risk=risk, viability=viability)
    sm._excess_baseline_ts = time.time() - 31.0
    sm._excess_baseline_value_usd = inventory.excess_value_usd
    result = sm.transition(snapshot=snapshot, inventory=inventory, risk=risk, viability=viability)
    assert viability.helpful_count >= 1
    assert result.lifecycle in {"quoting", "inventory_skewed", "defensive"}
    assert result.lifecycle != "unwind"


def test_live_like_paired_inventory_with_low_free_usdc_keeps_helpful_quotes_alive():
    cfg = MMConfigV2(session_budget_usd=15.0, base_clip_usd=6.0)
    snapshot = _snapshot(
        fv_up=0.59,
        fv_dn=0.41,
        pm_mid_up=0.58,
        pm_mid_dn=0.42,
        up_best_bid=0.57,
        up_best_ask=0.59,
        dn_best_bid=0.40,
        dn_best_ask=0.42,
        market_quality_score=0.40,
        market_tradeable=True,
        divergence_up=0.02,
        divergence_dn=0.02,
    )
    inventory = _inventory(
        up_shares=5.84,
        dn_shares=6.16,
        paired_qty=5.84,
        excess_dn_qty=0.32,
        paired_value_usd=5.84,
        excess_dn_value_usd=0.19,
        excess_value_usd=0.19,
        signed_excess_value_usd=-0.19,
        total_inventory_value_usd=6.03,
        free_usdc=9.94,
    )
    risk, plan, viability = _risk_and_plan(cfg, snapshot, inventory)
    assert risk.hard_mode == "none"
    assert risk.inventory_side == "flat"
    assert viability.four_quotes is True
    assert plan.quote_balance_state != "none"
    assert plan.quote_viability_reason != "all_quotes_below_min_size"


def test_live_like_paired_inventory_below_hard_cap_does_not_early_unwind():
    cfg = MMConfigV2(session_budget_usd=15.0, base_clip_usd=6.0)
    sm = StateMachineV2(cfg)
    snapshot = _snapshot(
        fv_up=0.59,
        fv_dn=0.41,
        pm_mid_up=0.58,
        pm_mid_dn=0.42,
        up_best_bid=0.57,
        up_best_ask=0.59,
        dn_best_bid=0.40,
        dn_best_ask=0.42,
        market_quality_score=0.40,
        market_tradeable=True,
        divergence_up=0.02,
        divergence_dn=0.02,
    )
    inventory = _inventory(
        up_shares=5.84,
        dn_shares=6.16,
        paired_qty=5.84,
        excess_dn_qty=0.32,
        paired_value_usd=5.84,
        excess_dn_value_usd=0.19,
        excess_value_usd=0.19,
        signed_excess_value_usd=-0.19,
        total_inventory_value_usd=6.03,
        free_usdc=9.94,
    )
    risk, _, viability = _risk_and_plan(cfg, snapshot, inventory)
    sm.transition(snapshot=snapshot, inventory=inventory, risk=risk, viability=viability)
    sm.transition(snapshot=snapshot, inventory=inventory, risk=risk, viability=viability)
    sm._excess_baseline_ts = time.time() - 31.0
    sm._excess_baseline_value_usd = inventory.excess_value_usd
    result = sm.transition(snapshot=snapshot, inventory=inventory, risk=risk, viability=viability)
    assert risk.inventory_side == "flat"
    assert viability.four_quotes is True
    assert result.lifecycle != "unwind"


def test_wallet_stale_does_not_trigger_true_drift():
    cfg = MMConfigV2(reconcile_drift_threshold_shares=1.5)
    reconcile = ReconcileV2(cfg)
    market = _market()
    reconcile.align(0.0, 0.0)
    reconcile.reconcile(
        market=market,
        real_up=8.0,
        real_dn=0.0,
        total_usdc=15.0,
        available_usdc=15.0,
        active_orders={},
        fv_up=0.54,
        fv_dn=0.46,
        wallet_snapshot_stale=True,
    )
    assert reconcile.status == "wallet_stale"
    assert reconcile.true_drift is False
    assert reconcile.drift_evidence.classification == "wallet_stale"


def test_reconcile_balance_fetch_error_sets_wallet_stale_and_blocks_true_drift():
    cfg = MMConfigV2(reconcile_drift_threshold_shares=1.5)
    reconcile = ReconcileV2(cfg)
    market = _market()
    reconcile.align(0.0, 0.0)

    reconcile.reconcile(
        market=market,
        real_up=8.0,
        real_dn=0.0,
        total_usdc=15.0,
        available_usdc=15.0,
        active_orders={},
        fv_up=0.54,
        fv_dn=0.46,
        wallet_snapshot_stale=True,
    )

    assert reconcile.status == "wallet_stale"
    assert reconcile.true_drift is False
    assert reconcile.drift_evidence.classification == "wallet_stale"


def test_wallet_stale_rearm_requires_two_clean_ticks_before_drift_candidate():
    cfg = MMConfigV2(reconcile_drift_threshold_shares=1.5)
    reconcile = ReconcileV2(cfg)
    market = _market()
    reconcile.align(0.0, 0.0)

    # Tick 1: stale snapshot arms rearm.
    reconcile.reconcile(
        market=market,
        real_up=8.0,
        real_dn=0.0,
        total_usdc=15.0,
        available_usdc=15.0,
        active_orders={},
        fv_up=0.54,
        fv_dn=0.46,
        wallet_snapshot_stale=True,
    )
    assert reconcile.status == "wallet_stale"
    assert reconcile.true_drift is False

    # Tick 2: first clean tick with mismatch -> still recovering.
    reconcile.reconcile(
        market=market,
        real_up=0.0,
        real_dn=0.0,
        total_usdc=15.0,
        available_usdc=15.0,
        active_orders={},
        fv_up=0.54,
        fv_dn=0.46,
        wallet_snapshot_stale=False,
    )
    assert reconcile.status == "wallet_recovering"
    assert reconcile.true_drift is False
    assert reconcile.drift_evidence.candidate_count == 0

    # Tick 3: second clean tick with mismatch -> still recovering.
    reconcile.reconcile(
        market=market,
        real_up=8.0,
        real_dn=0.0,
        total_usdc=15.0,
        available_usdc=15.0,
        active_orders={},
        fv_up=0.54,
        fv_dn=0.46,
        wallet_snapshot_stale=False,
    )
    assert reconcile.status == "wallet_recovering"
    assert reconcile.true_drift is False
    assert reconcile.drift_evidence.candidate_count == 0

    # Tick 4: rearm is complete, mismatch can become drift candidate.
    reconcile.reconcile(
        market=market,
        real_up=0.0,
        real_dn=0.0,
        total_usdc=15.0,
        available_usdc=15.0,
        active_orders={},
        fv_up=0.54,
        fv_dn=0.46,
        wallet_snapshot_stale=False,
    )
    assert reconcile.status == "drift_pending"
    assert reconcile.true_drift is False
    assert reconcile.drift_evidence.candidate_count == 1


def test_sellability_lag_does_not_trigger_true_drift():
    cfg = MMConfigV2(reconcile_drift_threshold_shares=1.5)
    reconcile = ReconcileV2(cfg)
    market = _market()
    reconcile.align(0.0, 6.0)
    reconcile.reconcile(
        market=market,
        real_up=0.0,
        real_dn=0.0,
        total_usdc=15.0,
        available_usdc=15.0,
        active_orders={},
        fv_up=0.54,
        fv_dn=0.46,
        sellability_lag_active=True,
    )
    assert reconcile.status == "sellability_lag"
    assert reconcile.true_drift is False
    assert reconcile.drift_evidence.classification == "sellability_lag"


def test_true_drift_with_position_enters_emergency_unwind_before_halt():
    cfg = MMConfigV2(session_budget_usd=15.0, base_clip_usd=6.0)
    snapshot = _snapshot()
    inventory = _inventory(
        up_shares=6.0,
        excess_up_qty=6.0,
        excess_up_value_usd=3.24,
        excess_value_usd=3.24,
        signed_excess_value_usd=3.24,
        total_inventory_value_usd=3.24,
    )
    analytics = AnalyticsState()

    risk_emergency = HardSafetyKernel(cfg).evaluate(
        snapshot=snapshot,
        inventory=inventory,
        analytics=analytics,
        health=HealthState(true_drift=True, true_drift_no_progress_sec=5.0),
    )
    assert risk_emergency.hard_mode == "emergency_unwind"

    risk_halted = HardSafetyKernel(cfg).evaluate(
        snapshot=snapshot,
        inventory=inventory,
        analytics=analytics,
        health=HealthState(true_drift=True, true_drift_no_progress_sec=25.0),
    )
    assert risk_halted.hard_mode == "halted"


def test_cancel_repost_sell_lag_does_not_halt_runtime():
    cfg = MMConfigV2(reconcile_drift_threshold_shares=1.5, session_budget_usd=15.0, base_clip_usd=6.0)
    reconcile = ReconcileV2(cfg)
    market = _market()
    snapshot = _snapshot()

    # Start with inventory on DN leg, then emulate PM post-cancel release lag:
    # free DN appears as zero for a few ticks even though wallet truth is not broken.
    reconcile.align(0.0, 6.0)
    inventory = reconcile.reconcile(
        market=market,
        real_up=0.0,
        real_dn=0.0,
        total_usdc=15.0,
        available_usdc=15.0,
        active_orders={},
        fv_up=0.54,
        fv_dn=0.46,
        sellability_lag_active=True,
    )

    assert reconcile.status == "sellability_lag"
    assert reconcile.true_drift is False
    assert reconcile.drift_evidence.classification == "sellability_lag"

    risk = HardSafetyKernel(cfg).evaluate(
        snapshot=snapshot,
        inventory=inventory,
        analytics=AnalyticsState(),
        health=HealthState(
            reconcile_status=reconcile.status,
            true_drift=reconcile.true_drift,
            sellability_lag_active=True,
        ),
    )
    assert risk.hard_mode == "none"


def test_drawdown_requires_persistence_before_hard_mode():
    cfg = MMConfigV2(session_budget_usd=50.0, base_clip_usd=6.0, hard_drawdown_usd=4.0)
    snapshot = _snapshot()
    inventory = _inventory(
        up_shares=6.0,
        excess_up_qty=6.0,
        excess_up_value_usd=3.24,
        excess_value_usd=3.24,
        signed_excess_value_usd=3.24,
        total_inventory_value_usd=3.24,
    )
    analytics = AnalyticsState(session_pnl_equity_usd=-4.8, session_pnl=-4.8)

    risk_pre = HardSafetyKernel(cfg).evaluate(
        snapshot=snapshot,
        inventory=inventory,
        analytics=analytics,
        health=HealthState(drawdown_breach_active=False, drawdown_breach_ticks=1, drawdown_breach_age_sec=1.0),
    )
    assert risk_pre.hard_mode == "none"

    risk_post = HardSafetyKernel(cfg).evaluate(
        snapshot=snapshot,
        inventory=inventory,
        analytics=analytics,
        health=HealthState(drawdown_breach_active=True, drawdown_breach_ticks=3, drawdown_breach_age_sec=9.0),
    )
    assert risk_post.hard_mode == "emergency_unwind"


def test_dynamic_drawdown_threshold_delays_emergency_for_same_pnl():
    class _MockClient:
        _orders = {}

    low_cfg = MMConfigV2(session_budget_usd=15.0, hard_drawdown_usd=4.0, hard_drawdown_budget_ratio=0.30)
    high_cfg = MMConfigV2(session_budget_usd=50.0, hard_drawdown_usd=4.0, hard_drawdown_budget_ratio=0.30)
    low_mm = MarketMakerV2(SimpleNamespace(), _MockClient(), low_cfg)
    high_mm = MarketMakerV2(SimpleNamespace(), _MockClient(), high_cfg)

    # Same PnL breach: low-budget run should start breach ticks, high-budget run should not.
    low_ticks, _, _ = low_mm._update_drawdown_breach(-8.0)
    high_ticks, _, _ = high_mm._update_drawdown_breach(-8.0)
    assert low_ticks >= 1
    assert high_ticks == 0
    assert low_cfg.effective_hard_drawdown_usd() < high_cfg.effective_hard_drawdown_usd()


def test_marketability_churn_stays_confirmed_from_hits_without_live_streak():
    class _MockClient:
        _orders = {}

    mm = MarketMakerV2(SimpleNamespace(), _MockClient(), MMConfigV2())
    confirmed, side = mm._classify_marketability_churn(
        {
            "up_collateral_warning_streak": 0,
            "dn_collateral_warning_streak": 0,
            "up_sell_skip_cooldown_streak": 0,
            "dn_sell_skip_cooldown_streak": 0,
            "up_collateral_warning_hits_60s": 0,
            "dn_collateral_warning_hits_60s": 0,
            "up_sell_skip_cooldown_hits_60s": 5,
            "dn_sell_skip_cooldown_hits_60s": 1,
            "up_execution_churn_ratio_60s": 0.1,
            "dn_execution_churn_ratio_60s": 0.0,
            "execution_churn_ratio_60s": 0.2,
        }
    )
    assert confirmed is True
    assert side == "up"


def test_marketability_churn_confirmation_has_short_hysteresis():
    class _MockClient:
        _orders = {}

    mm = MarketMakerV2(SimpleNamespace(), _MockClient(), MMConfigV2())
    confirmed, side = mm._classify_marketability_churn(
        {
            "up_collateral_warning_streak": 0,
            "dn_collateral_warning_streak": 0,
            "up_sell_skip_cooldown_streak": 4,
            "dn_sell_skip_cooldown_streak": 0,
            "up_collateral_warning_hits_60s": 0,
            "dn_collateral_warning_hits_60s": 0,
            "up_sell_skip_cooldown_hits_60s": 4,
            "dn_sell_skip_cooldown_hits_60s": 0,
            "up_execution_churn_ratio_60s": 0.5,
            "dn_execution_churn_ratio_60s": 0.0,
            "execution_churn_ratio_60s": 0.5,
        },
        now=100.0,
    )
    assert confirmed is True
    assert side == "up"

    confirmed, side = mm._classify_marketability_churn(
        {
            "up_collateral_warning_streak": 0,
            "dn_collateral_warning_streak": 0,
            "up_sell_skip_cooldown_streak": 0,
            "dn_sell_skip_cooldown_streak": 0,
            "up_collateral_warning_hits_60s": 1,
            "dn_collateral_warning_hits_60s": 0,
            "up_sell_skip_cooldown_hits_60s": 1,
            "dn_sell_skip_cooldown_hits_60s": 0,
            "up_execution_churn_ratio_60s": 0.1,
            "dn_execution_churn_ratio_60s": 0.0,
            "execution_churn_ratio_60s": 0.1,
        },
        now=105.0,
    )
    assert confirmed is True
    assert side == "up"

    confirmed, side = mm._classify_marketability_churn(
        {
            "up_collateral_warning_streak": 0,
            "dn_collateral_warning_streak": 0,
            "up_sell_skip_cooldown_streak": 0,
            "dn_sell_skip_cooldown_streak": 0,
            "up_collateral_warning_hits_60s": 0,
            "dn_collateral_warning_hits_60s": 0,
            "up_sell_skip_cooldown_hits_60s": 0,
            "dn_sell_skip_cooldown_hits_60s": 0,
            "up_execution_churn_ratio_60s": 0.0,
            "dn_execution_churn_ratio_60s": 0.0,
            "execution_churn_ratio_60s": 0.0,
        },
        now=113.5,
    )
    assert confirmed is False
    assert side == ""


def test_marketability_churn_hold_clears_after_flat_inventory_without_active_guard():
    class _MockClient:
        _orders = {}

    mm = MarketMakerV2(SimpleNamespace(), _MockClient(), MMConfigV2())
    mm._marketability_churn_hold_until = 200.0
    mm._set_marketability_side_lock("up", now=100.0)
    confirmed, side = mm._normalize_marketability_churn_state(
        confirmed=True,
        side="up",
        marketability_state={},
        inventory=_inventory(up_shares=0.0, dn_shares=0.0),
    )
    assert confirmed is False
    assert side == ""
    assert mm._marketability_churn_hold_until == 0.0
    assert mm._marketability_locked_side(now=200.0) == ""


def test_marketability_side_lock_does_not_flip_without_dominant_other_side():
    class _MockClient:
        _orders = {}

    mm = MarketMakerV2(SimpleNamespace(), _MockClient(), MMConfigV2())
    confirmed, side = mm._classify_marketability_churn(
        {
            "up_collateral_warning_streak": 0,
            "dn_collateral_warning_streak": 0,
            "up_sell_skip_cooldown_streak": 5,
            "dn_sell_skip_cooldown_streak": 0,
            "up_collateral_warning_hits_60s": 0,
            "dn_collateral_warning_hits_60s": 0,
            "up_sell_skip_cooldown_hits_60s": 5,
            "dn_sell_skip_cooldown_hits_60s": 0,
            "up_execution_churn_ratio_60s": 0.6,
            "dn_execution_churn_ratio_60s": 0.0,
            "execution_churn_ratio_60s": 0.6,
        },
        now=100.0,
    )
    assert confirmed is True
    assert side == "up"
    assert mm._marketability_locked_side(now=100.0) == "up"

    confirmed, side = mm._classify_marketability_churn(
        {
            "up_collateral_warning_streak": 0,
            "dn_collateral_warning_streak": 0,
            "up_sell_skip_cooldown_streak": 2,
            "dn_sell_skip_cooldown_streak": 3,
            "up_collateral_warning_hits_60s": 2,
            "dn_collateral_warning_hits_60s": 2,
            "up_sell_skip_cooldown_hits_60s": 3,
            "dn_sell_skip_cooldown_hits_60s": 4,
            "up_execution_churn_ratio_60s": 0.3,
            "dn_execution_churn_ratio_60s": 0.4,
            "execution_churn_ratio_60s": 0.5,
        },
        now=108.0,
    )
    assert confirmed is True
    assert side == "up"
    assert mm._marketability_locked_side(now=108.0) == "up"


def test_marketability_side_lock_and_churn_clear_after_flat_without_hold_order():
    class _MockClient:
        _orders = {}

    mm = MarketMakerV2(SimpleNamespace(), _MockClient(), MMConfigV2())
    mm._marketability_churn_hold_until = 200.0
    mm._set_marketability_side_lock("dn", now=100.0)
    confirmed, side = mm._normalize_marketability_churn_state(
        confirmed=True,
        side="dn",
        marketability_state={"active": False, "up_active": False, "dn_active": False},
        inventory=_inventory(up_shares=0.0, dn_shares=0.0),
    )
    assert confirmed is False
    assert side == ""
    assert mm._marketability_locked_side(now=150.0) == ""
    assert mm._marketability_churn_hold_until == 0.0


def test_emergency_taker_force_enables_only_after_confirmed_no_progress(monkeypatch):
    class _MockClient:
        _orders = {}

    mm = MarketMakerV2(SimpleNamespace(), _MockClient(), MMConfigV2())
    now = [1000.0]
    monkeypatch.setattr("mm_v2.runtime.time.time", lambda: now[0])

    forced, age = mm._update_emergency_taker_force(hard_mode="emergency_unwind", excess_value_usd=10.0)
    assert forced is False
    assert age == pytest.approx(0.0)

    now[0] += 3.0
    forced, age = mm._update_emergency_taker_force(hard_mode="emergency_unwind", excess_value_usd=9.9)
    assert forced is False
    assert age >= 3.0

    now[0] += 6.0
    forced, age = mm._update_emergency_taker_force(hard_mode="emergency_unwind", excess_value_usd=9.8)
    assert forced is True
    assert age >= 8.0


def test_emergency_taker_force_does_not_enable_when_progress_exists(monkeypatch):
    class _MockClient:
        _orders = {}

    mm = MarketMakerV2(SimpleNamespace(), _MockClient(), MMConfigV2())
    now = [2000.0]
    monkeypatch.setattr("mm_v2.runtime.time.time", lambda: now[0])

    forced, _ = mm._update_emergency_taker_force(hard_mode="emergency_unwind", excess_value_usd=10.0)
    assert forced is False
    now[0] += 4.0
    forced, _ = mm._update_emergency_taker_force(hard_mode="emergency_unwind", excess_value_usd=9.4)
    assert forced is False
    assert mm._emergency_no_progress_ticks == 0
    assert mm._emergency_no_progress_started_ts == pytest.approx(0.0)


def test_emergency_taker_force_is_disabled_outside_hard_mode(monkeypatch):
    class _MockClient:
        _orders = {}

    mm = MarketMakerV2(SimpleNamespace(), _MockClient(), MMConfigV2())
    now = [3000.0]
    monkeypatch.setattr("mm_v2.runtime.time.time", lambda: now[0])

    mm._emergency_taker_forced = True
    forced, age = mm._update_emergency_taker_force(hard_mode="none", excess_value_usd=6.0)
    assert forced is False
    assert age == pytest.approx(0.0)
    assert mm._emergency_taker_forced is False


def test_live_like_window_reports_mm_regime_ratios():
    class _MockClient:
        _orders = {}

    mm = MarketMakerV2(SimpleNamespace(), _MockClient(), MMConfigV2())
    now = time.time()
    mm._lifecycle_history = [
        (now - 50.0, "quoting"),
        (now - 40.0, "quoting"),
        (now - 30.0, "inventory_skewed"),
        (now - 20.0, "defensive"),
        (now - 10.0, "unwind"),
        (now - 5.0, "unwind"),
    ]
    ratios = mm._lifecycle_ratios(window_sec=60.0)
    assert ratios["quoting_ratio_60s"] > 0.30
    assert ratios["unwind_ratio_60s"] > 0.30
    assert ratios["inventory_skewed_ratio_60s"] > 0.0
    assert ratios["defensive_ratio_60s"] > 0.0


def test_mm_regime_degraded_alert_fires_on_unwind_dominance():
    class _MockClient:
        _orders = {}

    mm = MarketMakerV2(SimpleNamespace(), _MockClient(), MMConfigV2())
    # Emulate prolonged degraded regime.
    mm._mm_regime_degraded_started_ts = time.time() - 130.0
    mm._update_mm_regime_alert(
        quoting_ratio_60s=0.10,
        inventory_skewed_ratio_60s=0.05,
        defensive_ratio_60s=0.05,
        unwind_ratio_60s=0.80,
        emergency_unwind_ratio_60s=0.05,
        quote_balance_state="helpful_only",
    )
    assert "mm_regime_degraded" in mm._alerts


def test_mm_regime_degraded_alert_does_not_fire_for_defensive_mm_activity():
    class _MockClient:
        _orders = {}

    mm = MarketMakerV2(SimpleNamespace(), _MockClient(), MMConfigV2())
    mm._mm_regime_degraded_started_ts = time.time() - 130.0
    mm._update_mm_regime_alert(
        quoting_ratio_60s=0.00,
        inventory_skewed_ratio_60s=0.30,
        defensive_ratio_60s=0.40,
        unwind_ratio_60s=0.27,
        emergency_unwind_ratio_60s=0.05,
        quote_balance_state="helpful_only",
    )
    assert "mm_regime_degraded" not in mm._alerts


def test_mm_regime_degraded_reason_reports_high_emergency_ratio():
    class _MockClient:
        _orders = {}

    mm = MarketMakerV2(SimpleNamespace(), _MockClient(), MMConfigV2())
    mm._mm_regime_degraded_started_ts = time.time() - 130.0
    mm._update_mm_regime_alert(
        quoting_ratio_60s=0.40,
        inventory_skewed_ratio_60s=0.20,
        defensive_ratio_60s=0.10,
        unwind_ratio_60s=0.10,
        emergency_unwind_ratio_60s=0.30,
        quote_balance_state="reduced",
    )
    assert mm._mm_regime_degraded_reason == "high_emergency_ratio"
    alert = mm._alerts.get("mm_regime_degraded")
    assert alert is not None
    assert "high_emergency_ratio" in alert["message"]


def test_mm_regime_degraded_reason_low_dual_bid_ratio():
    class _MockClient:
        _orders = {}

    mm = MarketMakerV2(SimpleNamespace(), _MockClient(), MMConfigV2())
    mm._mm_regime_degraded_started_ts = time.time() - 130.0
    mm._update_mm_regime_alert(
        quoting_ratio_60s=0.45,
        inventory_skewed_ratio_60s=0.20,
        defensive_ratio_60s=0.10,
        unwind_ratio_60s=0.05,
        emergency_unwind_ratio_60s=0.0,
        dual_bid_ratio_60s=0.40,
        one_sided_bid_streak_outside=7,
        outside_near_expiry=True,
        quote_balance_state="reduced",
    )
    assert mm._mm_regime_degraded_reason == "low_dual_bid_ratio"
    alert = mm._alerts.get("mm_regime_degraded")
    assert alert is not None
    assert "low_dual_bid_ratio" in alert["message"]


def test_mm_regime_degraded_reason_marketability_churn():
    class _MockClient:
        _orders = {}

    mm = MarketMakerV2(SimpleNamespace(), _MockClient(), MMConfigV2())
    mm._mm_regime_degraded_started_ts = time.time() - 130.0
    mm._update_mm_regime_alert(
        quoting_ratio_60s=0.45,
        inventory_skewed_ratio_60s=0.20,
        defensive_ratio_60s=0.10,
        unwind_ratio_60s=0.0,
        emergency_unwind_ratio_60s=0.0,
        dual_bid_ratio_60s=0.90,
        one_sided_bid_streak_outside=0,
        outside_near_expiry=True,
        quote_balance_state="reduced",
        marketability_guard_active=True,
        marketability_guard_reason="collateral_warning",
    )
    assert mm._mm_regime_degraded_reason == "marketability_churn"
    alert = mm._alerts.get("mm_regime_degraded")
    assert alert is not None
    assert "marketability_guard_reason=collateral_warning" in alert["message"]


def test_one_sided_bid_streak_tracks_only_outside_near_expiry():
    class _MockClient:
        _orders = {}

    mm = MarketMakerV2(SimpleNamespace(), _MockClient(), MMConfigV2())
    mm._mm_regime_degraded_started_ts = time.time() - 130.0
    mm._update_mm_regime_alert(
        quoting_ratio_60s=0.45,
        inventory_skewed_ratio_60s=0.20,
        defensive_ratio_60s=0.10,
        unwind_ratio_60s=0.05,
        emergency_unwind_ratio_60s=0.0,
        dual_bid_ratio_60s=0.20,
        one_sided_bid_streak_outside=8,
        outside_near_expiry=False,
        quote_balance_state="reduced",
    )
    assert mm._mm_regime_degraded_reason != "low_dual_bid_ratio"


def test_dual_bid_ratio_ignores_no_bid_ticks():
    class _MockClient:
        _orders = {}

    mm = MarketMakerV2(SimpleNamespace(), _MockClient(), MMConfigV2())
    mm._mm_regime_degraded_started_ts = time.time() - 130.0
    # No bids on both sides should not be considered one-sided degradation.
    mm._update_mm_regime_alert(
        quoting_ratio_60s=0.40,
        inventory_skewed_ratio_60s=0.30,
        defensive_ratio_60s=0.10,
        unwind_ratio_60s=0.0,
        emergency_unwind_ratio_60s=0.0,
        dual_bid_ratio_60s=1.0,
        one_sided_bid_streak_outside=0,
        outside_near_expiry=True,
        quote_balance_state="reduced",
    )
    assert mm._mm_regime_degraded_reason != "low_dual_bid_ratio"


def test_divergence_dual_bid_exception_overrides_low_dual_bid_reason():
    class _MockClient:
        _orders = {}

    mm = MarketMakerV2(SimpleNamespace(), _MockClient(), MMConfigV2())
    mm._update_mm_regime_alert(
        quoting_ratio_60s=0.45,
        inventory_skewed_ratio_60s=0.30,
        defensive_ratio_60s=0.10,
        unwind_ratio_60s=0.0,
        emergency_unwind_ratio_60s=0.0,
        dual_bid_ratio_60s=0.10,
        one_sided_bid_streak_outside=4,
        outside_near_expiry=True,
        quote_balance_state="helpful_only",
        dual_bid_exception_active=True,
        dual_bid_exception_reason="divergence_buy_hard_suppress",
    )
    assert mm._mm_regime_degraded_reason == "divergence_buy_hard_suppress"


def test_failure_bucket_prefers_marketability_churn():
    class _MockClient:
        _orders = {}

    mm = MarketMakerV2(SimpleNamespace(), _MockClient(), MMConfigV2())
    mm.market = _market()
    bucket = mm._classify_failure_bucket(
        snapshot=_snapshot(market_tradeable=False),
        inventory=_inventory(total_inventory_value_usd=7.0, excess_value_usd=4.0, signed_excess_value_usd=4.0),
        risk=RiskRegime(
            soft_mode="defensive",
            hard_mode="none",
            target_soft_mode="defensive",
            reason="defensive marketability guard (collateral_warning)",
            inventory_pressure=0.3,
            edge_score=0.0,
            drawdown_pct_budget=1.0,
            marketability_guard_active=True,
            marketability_guard_reason="collateral_warning",
            marketability_guard_up_active=True,
        ),
        health=HealthState(),
        plan=QuotePlan(None, None, None, None, "defensive", "marketability"),
        lifecycle="defensive",
        marketability_guard_active=True,
    )
    assert bucket == "marketability_churn"


def test_two_weak_negative_markouts_only_arm_soft_brake_not_hard_block():
    class _MockClient:
        _orders = {}

    mm = MarketMakerV2(SimpleNamespace(), _MockClient(), MMConfigV2())
    now = time.time()
    mm._apply_side_markout_result(token_side="up", markout=-0.01, tick_size=0.01, now=now)
    mm._apply_side_markout_result(token_side="up", markout=-0.01, tick_size=0.01, now=now + 1.0)
    assert mm._negative_spread_capture_streak_up == 2
    assert mm._toxic_fill_streak_up == 0
    assert mm._side_soft_brake_active("up") is True
    assert mm._side_reentry_cooldown_sec("up", now=now + 1.0) == pytest.approx(0.0)


def test_hard_block_requires_stronger_toxic_confirmation():
    class _MockClient:
        _orders = {}

    mm = MarketMakerV2(SimpleNamespace(), _MockClient(), MMConfigV2())
    now = time.time()
    mm._apply_side_markout_result(token_side="dn", markout=-0.04, tick_size=0.01, now=now)
    assert mm._side_reentry_cooldown_sec("dn", now=now) == pytest.approx(0.0)
    mm._apply_side_markout_result(token_side="dn", markout=-0.04, tick_size=0.01, now=now + 1.0)
    assert mm._toxic_fill_streak_dn >= 2
    assert mm._side_soft_brake_active("dn") is True
    assert mm._side_reentry_cooldown_sec("dn", now=now + 1.0) > 0.0


def test_hard_cap_entry_then_recovery_exits_unwind_before_expiry():
    cfg = MMConfigV2(session_budget_usd=15.0, base_clip_usd=6.0)
    sm = StateMachineV2(cfg)
    snapshot = _snapshot(time_left_sec=600.0)
    high_excess_inventory = _inventory(
        dn_shares=11.0,
        excess_dn_qty=11.0,
        excess_dn_value_usd=7.2,
        excess_value_usd=7.2,
        signed_excess_value_usd=-7.2,
        total_inventory_value_usd=7.2,
    )
    high_risk, _, _ = _risk_and_plan(cfg, snapshot, high_excess_inventory)
    assert high_risk.target_soft_mode == "unwind"
    viability = QuoteViabilitySummary(any_quote=True, four_quotes=False, helpful_count=1, four_quote_presence_ratio=0.30)
    for _ in range(8):
        sm.transition(snapshot=snapshot, inventory=high_excess_inventory, risk=high_risk, viability=viability)
    assert sm.lifecycle == "defensive"
    # Confirmed no-progress + degraded viability should still allow unwind.
    sm._excess_baseline_value_usd = float(high_excess_inventory.excess_value_usd)
    sm._excess_baseline_ts = time.time() - (float(UNWIND_STUCK_WINDOW_SEC) + 1.0)
    degraded = QuoteViabilitySummary(
        any_quote=True,
        four_quotes=False,
        helpful_count=0,
        quote_balance_state="reduced",
        four_quote_presence_ratio=0.20,
    )
    for _ in range(int(NO_HELPFUL_TICKS_FOR_UNWIND)):
        sm.transition(snapshot=snapshot, inventory=high_excess_inventory, risk=high_risk, viability=degraded)
    assert sm.lifecycle == "unwind"
    sm._unwind_started_at = time.time() - 10.0

    recovered_inventory = _inventory(
        dn_shares=6.0,
        excess_dn_qty=6.0,
        excess_dn_value_usd=5.6,
        excess_value_usd=5.6,
        signed_excess_value_usd=-5.6,
        total_inventory_value_usd=5.6,
    )
    recovered_risk, _, _ = _risk_and_plan(cfg, snapshot, recovered_inventory)
    assert recovered_risk.target_soft_mode == "defensive"
    result = None
    for _ in range(UNWIND_EXIT_CONFIRM_TICKS):
        result = sm.transition(snapshot=snapshot, inventory=recovered_inventory, risk=recovered_risk, viability=viability)
    assert result is not None
    assert result.lifecycle == "defensive"
    assert result.effective_soft_mode == "defensive"


def test_unwind_target_mismatch_metrics_are_exposed_and_decay_after_exit():
    class _MockClient:
        _orders = {}

    mm = MarketMakerV2(SimpleNamespace(), _MockClient(), MMConfigV2())
    sec_1 = mm._update_unwind_target_mismatch(effective_soft_mode="unwind", target_soft_mode="defensive")
    assert mm._unwind_target_mismatch_ticks == 1
    sec_2 = mm._update_unwind_target_mismatch(effective_soft_mode="unwind", target_soft_mode="inventory_skewed")
    assert mm._unwind_target_mismatch_ticks == 2
    assert sec_2 >= sec_1

    mm._last_analytics = AnalyticsState(
        unwind_target_mismatch_ticks=mm._unwind_target_mismatch_ticks,
        unwind_target_mismatch_sec=sec_2,
        unwind_exit_armed=True,
    )
    snap = mm.snapshot()
    assert snap["analytics"]["unwind_target_mismatch_ticks"] == 2
    assert snap["analytics"]["unwind_target_mismatch_sec"] >= 0.0
    assert snap["analytics"]["unwind_exit_armed"] is True

    sec_3 = mm._update_unwind_target_mismatch(effective_soft_mode="defensive", target_soft_mode="defensive")
    assert sec_3 == 0.0
    assert mm._unwind_target_mismatch_ticks == 0


def test_sellability_lag_with_lower_target_does_not_stick_unwind_indefinitely():
    cfg = MMConfigV2(session_budget_usd=15.0, base_clip_usd=6.0)
    sm = StateMachineV2(cfg)
    sm._set_lifecycle("unwind")
    sm._unwind_started_at = time.time() - 10.0
    snapshot = _snapshot(time_left_sec=600.0)
    inventory = _inventory(
        up_shares=6.0,
        excess_up_qty=6.0,
        excess_up_value_usd=5.6,
        excess_value_usd=5.6,
        signed_excess_value_usd=5.6,
        total_inventory_value_usd=5.6,
    )
    risk = HardSafetyKernel(cfg).evaluate(
        snapshot=snapshot,
        inventory=inventory,
        analytics=AnalyticsState(),
        health=HealthState(sellability_lag_active=True),
    )
    assert risk.hard_mode == "none"
    assert risk.target_soft_mode == "defensive"
    viability = QuoteViabilitySummary(any_quote=True, four_quotes=False, helpful_count=1, four_quote_presence_ratio=0.30)
    result = None
    for _ in range(UNWIND_EXIT_CONFIRM_TICKS):
        result = sm.transition(snapshot=snapshot, inventory=inventory, risk=risk, viability=viability)
    assert result is not None
    assert result.lifecycle == "defensive"


def test_hard_mode_clear_allows_emergency_unwind_exit_path():
    cfg = MMConfigV2(session_budget_usd=50.0, base_clip_usd=6.0)
    sm = StateMachineV2(cfg)
    snapshot = _snapshot(time_left_sec=600.0)
    inventory = _inventory(
        up_shares=6.0,
        excess_up_qty=6.0,
        excess_up_value_usd=6.0,
        excess_value_usd=6.0,
        signed_excess_value_usd=6.0,
        total_inventory_value_usd=6.0,
    )

    emergency_risk = HardSafetyKernel(cfg).evaluate(
        snapshot=snapshot,
        inventory=inventory,
        analytics=AnalyticsState(),
        health=HealthState(true_drift=True, true_drift_no_progress_sec=5.0),
    )
    assert emergency_risk.hard_mode == "emergency_unwind"
    first = sm.transition(
        snapshot=snapshot,
        inventory=inventory,
        risk=emergency_risk,
        viability=QuoteViabilitySummary(any_quote=True, four_quotes=False, helpful_count=1, four_quote_presence_ratio=0.25),
    )
    assert first.lifecycle == "emergency_unwind"

    sm._emergency_started_at = time.time() - 10.0
    cleared_risk = HardSafetyKernel(cfg).evaluate(
        snapshot=snapshot,
        inventory=inventory,
        analytics=AnalyticsState(),
        health=HealthState(),
    )
    cleared_risk.hard_mode = "none"
    cleared_risk.target_soft_mode = "defensive"
    cleared_risk.soft_mode = "defensive"
    viability = QuoteViabilitySummary(any_quote=True, four_quotes=False, helpful_count=1, four_quote_presence_ratio=0.25)
    result = None
    for _ in range(EMERGENCY_EXIT_CONFIRM_TICKS):
        result = sm.transition(snapshot=snapshot, inventory=inventory, risk=cleared_risk, viability=viability)
    assert result is not None
    assert result.lifecycle == "defensive"


@pytest.mark.asyncio
async def test_near_expiry_material_inventory_uses_terminal_liquidation_step_not_normal_requote(monkeypatch):
    class _MockClient:
        _orders = {}

    mm = MarketMakerV2(SimpleNamespace(), _MockClient(), MMConfigV2(emergency_taker_start_sec=20.0))
    mm.set_market(_market())
    snapshot = _snapshot(time_left_sec=5.0)
    valuation = PairValuationResult(
        fv_up=snapshot.fv_up,
        fv_dn=snapshot.fv_dn,
        pair_mid=0.5,
        source="midpoint_bounded_model",
        divergence_up=0.0,
        divergence_dn=0.0,
        confidence=snapshot.fv_confidence,
        regime="normal",
        pm_age_sec=0.0,
    )
    inventory = _inventory(
        up_shares=6.0,
        total_inventory_value_usd=3.0,
        free_usdc=15.0,
        wallet_total_usdc=15.0,
    )
    calls = {"sync": 0, "step": 0, "cancel_all": 0}

    async def _check_fills():
        return []

    async def _get_books():
        return {}, {}

    def _compute(*, market, feed_state, up_book, dn_book):
        del market, feed_state, up_book, dn_book
        return valuation, snapshot

    def _sync_paper_prices(**kwargs):
        del kwargs

    async def _wallet_balances(*, reference_balances=None):
        del reference_balances
        return 6.0, 0.0, 15.0, 15.0

    async def _get_sellable_balances(*, reference_balances=None):
        del reference_balances
        return 6.0, 0.0

    def _reconcile(**kwargs):
        del kwargs
        return inventory

    async def _step(*, round_idx=0, cancel_existing=True):
        calls["step"] += 1
        assert round_idx == 0
        assert cancel_existing is False
        return {
            "attempted_orders": 1,
            "placed_orders": 1,
            "remaining_up": 6.0,
            "remaining_dn": 0.0,
            "wallet_total_usdc": 15.0,
            "done": False,
            "reason": "no_book_liquidity",
            "placed_order_ids": ["oid-1"],
            "cancelled_orders": 0,
        }

    async def _cancel_all():
        calls["cancel_all"] += 1
        return 0

    async def _sync(_plan):
        calls["sync"] += 1

    monkeypatch.setattr(mm.gateway, "check_fills", _check_fills)
    monkeypatch.setattr(mm.gateway, "get_books", _get_books)
    monkeypatch.setattr(mm.valuation, "compute", _compute)
    monkeypatch.setattr(mm.gateway, "sync_paper_prices", _sync_paper_prices)
    monkeypatch.setattr(mm.gateway, "get_wallet_balances", _wallet_balances)
    monkeypatch.setattr(mm.gateway, "api_error_stats", lambda: {})
    monkeypatch.setattr(mm.gateway, "balance_fetch_health_state", lambda: {})
    monkeypatch.setattr(mm.gateway, "get_sellable_balances", _get_sellable_balances)
    monkeypatch.setattr(mm.gateway, "sell_release_lag_state", lambda: {"active": False})
    monkeypatch.setattr(mm.reconcile, "reconcile", _reconcile)
    monkeypatch.setattr(mm.gateway, "run_terminal_liquidation_step", _step)
    monkeypatch.setattr(mm.gateway, "cancel_all", _cancel_all)
    monkeypatch.setattr(mm.execution_policy, "sync", _sync)

    await mm._tick()

    state = mm.snapshot()
    assert calls["cancel_all"] == 1
    assert calls["step"] == 1
    assert calls["sync"] == 0
    assert state["lifecycle"] == "unwind"
    assert state["quote_balance_state"] == "none"
    assert state["runtime"]["terminal_liquidation_active"] is True
    assert state["runtime"]["terminal_liquidation_attempted_orders"] == 1
    assert state["runtime"]["terminal_liquidation_placed_orders"] == 1
    assert state["runtime"]["terminal_liquidation_reason"] == "no_book_liquidity"


@pytest.mark.asyncio
async def test_close_window_material_inventory_arms_terminal_liquidation_before_emergency_threshold(monkeypatch):
    class _MockClient:
        _orders = {}

    mm = MarketMakerV2(SimpleNamespace(), _MockClient(), MMConfigV2(unwind_window_sec=90.0, emergency_taker_start_sec=20.0))
    mm.set_market(_market())
    snapshot = _snapshot(time_left_sec=60.0)
    valuation = PairValuationResult(
        fv_up=snapshot.fv_up,
        fv_dn=snapshot.fv_dn,
        pair_mid=0.5,
        source="midpoint_bounded_model",
        divergence_up=0.0,
        divergence_dn=0.0,
        confidence=snapshot.fv_confidence,
        regime="normal",
        pm_age_sec=0.0,
    )
    inventory = _inventory(
        up_shares=6.0,
        total_inventory_value_usd=3.0,
        free_usdc=15.0,
        wallet_total_usdc=15.0,
    )
    calls = {"step": 0, "sync": 0}

    async def _check_fills():
        return []

    async def _get_books():
        return {}, {}

    def _compute(*, market, feed_state, up_book, dn_book):
        del market, feed_state, up_book, dn_book
        return valuation, snapshot

    def _sync_paper_prices(**kwargs):
        del kwargs

    async def _wallet_balances(*, reference_balances=None):
        del reference_balances
        return 6.0, 0.0, 15.0, 15.0

    async def _get_sellable_balances(*, reference_balances=None):
        del reference_balances
        return 6.0, 0.0

    def _reconcile(**kwargs):
        del kwargs
        return inventory

    async def _step(*, round_idx=0, cancel_existing=True):
        calls["step"] += 1
        assert round_idx == 0
        assert cancel_existing is False
        return {
            "attempted_orders": 1,
            "placed_orders": 1,
            "remaining_up": 6.0,
            "remaining_dn": 0.0,
            "wallet_total_usdc": 15.0,
            "done": False,
            "reason": "no_book_liquidity",
            "placed_order_ids": ["oid-1"],
            "cancelled_orders": 0,
        }

    async def _cancel_all():
        return 0

    async def _sync(_plan):
        calls["sync"] += 1

    monkeypatch.setattr(mm.gateway, "check_fills", _check_fills)
    monkeypatch.setattr(mm.gateway, "get_books", _get_books)
    monkeypatch.setattr(mm.valuation, "compute", _compute)
    monkeypatch.setattr(mm.gateway, "sync_paper_prices", _sync_paper_prices)
    monkeypatch.setattr(mm.gateway, "get_wallet_balances", _wallet_balances)
    monkeypatch.setattr(mm.gateway, "api_error_stats", lambda: {})
    monkeypatch.setattr(mm.gateway, "balance_fetch_health_state", lambda: {})
    monkeypatch.setattr(mm.gateway, "get_sellable_balances", _get_sellable_balances)
    monkeypatch.setattr(mm.gateway, "sell_release_lag_state", lambda: {"active": False})
    monkeypatch.setattr(mm.reconcile, "reconcile", _reconcile)
    monkeypatch.setattr(mm.gateway, "run_terminal_liquidation_step", _step)
    monkeypatch.setattr(mm.gateway, "cancel_all", _cancel_all)
    monkeypatch.setattr(mm.execution_policy, "sync", _sync)

    await mm._tick()

    state = mm.snapshot()
    assert calls["step"] == 1
    assert calls["sync"] == 0
    assert state["runtime"]["terminal_liquidation_active"] is True
    assert state["market"]["time_left_sec"] == pytest.approx(60.0)


@pytest.mark.asyncio
async def test_terminal_liquidation_timeout_does_not_stop_further_steps_before_expiry(monkeypatch):
    class _MockClient:
        _orders = {}

    mm = MarketMakerV2(SimpleNamespace(), _MockClient(), MMConfigV2(unwind_window_sec=90.0, emergency_taker_start_sec=20.0, emergency_unwind_timeout_sec=10.0))
    mm.set_market(_market())
    snapshot = _snapshot(time_left_sec=40.0)
    valuation = PairValuationResult(
        fv_up=snapshot.fv_up,
        fv_dn=snapshot.fv_dn,
        pair_mid=0.5,
        source="midpoint_bounded_model",
        divergence_up=0.0,
        divergence_dn=0.0,
        confidence=snapshot.fv_confidence,
        regime="normal",
        pm_age_sec=0.0,
    )
    inventory = _inventory(
        up_shares=6.0,
        total_inventory_value_usd=3.0,
        free_usdc=15.0,
        wallet_total_usdc=15.0,
    )
    now = [1000.0]
    calls = {"step": 0}

    async def _check_fills():
        return []

    async def _get_books():
        return {}, {}

    def _compute(*, market, feed_state, up_book, dn_book):
        del market, feed_state, up_book, dn_book
        return valuation, snapshot

    def _sync_paper_prices(**kwargs):
        del kwargs

    async def _wallet_balances(*, reference_balances=None):
        del reference_balances
        return 6.0, 0.0, 15.0, 15.0

    async def _get_sellable_balances(*, reference_balances=None):
        del reference_balances
        return 6.0, 0.0

    def _reconcile(**kwargs):
        del kwargs
        return inventory

    async def _step(*, round_idx=0, cancel_existing=True):
        calls["step"] += 1
        return {
            "attempted_orders": 1,
            "placed_orders": 1,
            "remaining_up": 6.0,
            "remaining_dn": 0.0,
            "wallet_total_usdc": 15.0,
            "done": False,
            "reason": "no_book_liquidity",
            "placed_order_ids": [],
            "cancelled_orders": 0,
        }

    async def _cancel_all():
        return 0

    async def _sync(_plan):
        raise AssertionError("normal execution sync should not run during terminal liquidation")

    monkeypatch.setattr("mm_v2.runtime.time.time", lambda: now[0])
    monkeypatch.setattr(mm.gateway, "check_fills", _check_fills)
    monkeypatch.setattr(mm.gateway, "get_books", _get_books)
    monkeypatch.setattr(mm.valuation, "compute", _compute)
    monkeypatch.setattr(mm.gateway, "sync_paper_prices", _sync_paper_prices)
    monkeypatch.setattr(mm.gateway, "get_wallet_balances", _wallet_balances)
    monkeypatch.setattr(mm.gateway, "api_error_stats", lambda: {})
    monkeypatch.setattr(mm.gateway, "balance_fetch_health_state", lambda: {})
    monkeypatch.setattr(mm.gateway, "get_sellable_balances", _get_sellable_balances)
    monkeypatch.setattr(mm.gateway, "sell_release_lag_state", lambda: {"active": False})
    monkeypatch.setattr(mm.reconcile, "reconcile", _reconcile)
    monkeypatch.setattr(mm.gateway, "run_terminal_liquidation_step", _step)
    monkeypatch.setattr(mm.gateway, "cancel_all", _cancel_all)
    monkeypatch.setattr(mm.execution_policy, "sync", _sync)

    mm._terminal_liquidation_active = True
    mm._terminal_liquidation_started_ts = now[0] - 15.0
    mm._terminal_liquidation_remaining_up = 6.0
    mm._terminal_liquidation_remaining_dn = 0.0
    mm._terminal_liquidation_done = False

    await mm._tick()

    assert calls["step"] == 1
    state = mm.snapshot()
    assert state["runtime"]["terminal_liquidation_active"] is True
    assert state["runtime"]["terminal_liquidation_reason"] == "no_book_liquidity"


@pytest.mark.asyncio
async def test_terminal_liquidation_done_at_expiry_sets_explicit_terminal_reason(monkeypatch):
    class _MockClient:
        _orders = {}

    mm = MarketMakerV2(SimpleNamespace(), _MockClient(), MMConfigV2(emergency_taker_start_sec=20.0))
    mm.set_market(_market())
    mm._running = True
    snapshot = _snapshot(time_left_sec=0.0)
    valuation = PairValuationResult(
        fv_up=snapshot.fv_up,
        fv_dn=snapshot.fv_dn,
        pair_mid=0.5,
        source="midpoint_bounded_model",
        divergence_up=0.0,
        divergence_dn=0.0,
        confidence=snapshot.fv_confidence,
        regime="normal",
        pm_age_sec=0.0,
    )
    inventory = _inventory(
        up_shares=6.0,
        total_inventory_value_usd=3.0,
        free_usdc=15.0,
        wallet_total_usdc=15.0,
    )
    calls = {"cancel_all": 0}

    async def _check_fills():
        return []

    async def _get_books():
        return {}, {}

    def _compute(*, market, feed_state, up_book, dn_book):
        del market, feed_state, up_book, dn_book
        return valuation, snapshot

    def _sync_paper_prices(**kwargs):
        del kwargs

    async def _wallet_balances(*, reference_balances=None):
        del reference_balances
        return 0.0, 0.0, 21.0, 21.0

    async def _get_sellable_balances(*, reference_balances=None):
        del reference_balances
        return 6.0, 0.0

    def _reconcile(**kwargs):
        del kwargs
        return inventory

    async def _step(*, round_idx=0, cancel_existing=True):
        del round_idx, cancel_existing
        return {
            "attempted_orders": 1,
            "placed_orders": 1,
            "remaining_up": 0.0,
            "remaining_dn": 0.0,
            "wallet_total_usdc": 21.0,
            "done": True,
            "reason": "ok",
            "placed_order_ids": ["oid-1"],
            "cancelled_orders": 0,
        }

    async def _cancel_all():
        calls["cancel_all"] += 1
        return 0

    async def _sync(_plan):
        raise AssertionError("normal execution sync should not run during terminal liquidation")

    monkeypatch.setattr(mm.gateway, "check_fills", _check_fills)
    monkeypatch.setattr(mm.gateway, "get_books", _get_books)
    monkeypatch.setattr(mm.valuation, "compute", _compute)
    monkeypatch.setattr(mm.gateway, "sync_paper_prices", _sync_paper_prices)
    monkeypatch.setattr(mm.gateway, "get_wallet_balances", _wallet_balances)
    monkeypatch.setattr(mm.gateway, "api_error_stats", lambda: {})
    monkeypatch.setattr(mm.gateway, "balance_fetch_health_state", lambda: {})
    monkeypatch.setattr(mm.gateway, "get_sellable_balances", _get_sellable_balances)
    monkeypatch.setattr(mm.gateway, "sell_release_lag_state", lambda: {"active": False})
    monkeypatch.setattr(mm.reconcile, "reconcile", _reconcile)
    monkeypatch.setattr(mm.gateway, "run_terminal_liquidation_step", _step)
    monkeypatch.setattr(mm.gateway, "cancel_all", _cancel_all)
    monkeypatch.setattr(mm.execution_policy, "sync", _sync)

    await mm._tick()

    state = mm.snapshot()
    assert calls["cancel_all"] == 2
    assert mm._running is False
    assert state["runtime"]["terminal_liquidation_done"] is True
    assert state["runtime"]["terminal_liquidation_reason"] == "terminal_liquidation_done"
    assert state["runtime"]["last_terminal_reason"] == "terminal_liquidation_done"
    assert state["runtime"]["last_terminal_up_shares"] == pytest.approx(0.0)
    assert state["runtime"]["last_terminal_dn_shares"] == pytest.approx(0.0)


@pytest.mark.asyncio
async def test_terminal_liquidation_done_before_expiry_does_not_resume_normal_quoting(monkeypatch):
    class _MockClient:
        _orders = {}

    mm = MarketMakerV2(SimpleNamespace(), _MockClient(), MMConfigV2(unwind_window_sec=90.0, emergency_taker_start_sec=20.0))
    mm.set_market(_market())
    snapshot = _snapshot(time_left_sec=30.0)
    valuation = PairValuationResult(
        fv_up=snapshot.fv_up,
        fv_dn=snapshot.fv_dn,
        pair_mid=0.5,
        source="midpoint_bounded_model",
        divergence_up=0.0,
        divergence_dn=0.0,
        confidence=snapshot.fv_confidence,
        regime="normal",
        pm_age_sec=0.0,
    )
    inventory = _inventory(
        up_shares=0.0,
        dn_shares=0.0,
        total_inventory_value_usd=0.0,
        free_usdc=21.0,
        wallet_total_usdc=21.0,
    )
    calls = {"step": 0, "sync": 0}

    async def _check_fills():
        return []

    async def _get_books():
        return {}, {}

    def _compute(*, market, feed_state, up_book, dn_book):
        del market, feed_state, up_book, dn_book
        return valuation, snapshot

    def _sync_paper_prices(**kwargs):
        del kwargs

    async def _wallet_balances(*, reference_balances=None):
        del reference_balances
        return 0.0, 0.0, 21.0, 21.0

    async def _get_sellable_balances(*, reference_balances=None):
        del reference_balances
        return 0.0, 0.0

    def _reconcile(**kwargs):
        del kwargs
        return inventory

    async def _step(*, round_idx=0, cancel_existing=True):
        calls["step"] += 1
        raise AssertionError("terminal step should not rerun once done and flat")

    async def _cancel_all():
        return 0

    async def _sync(_plan):
        calls["sync"] += 1

    monkeypatch.setattr(mm.gateway, "check_fills", _check_fills)
    monkeypatch.setattr(mm.gateway, "get_books", _get_books)
    monkeypatch.setattr(mm.valuation, "compute", _compute)
    monkeypatch.setattr(mm.gateway, "sync_paper_prices", _sync_paper_prices)
    monkeypatch.setattr(mm.gateway, "get_wallet_balances", _wallet_balances)
    monkeypatch.setattr(mm.gateway, "api_error_stats", lambda: {})
    monkeypatch.setattr(mm.gateway, "balance_fetch_health_state", lambda: {})
    monkeypatch.setattr(mm.gateway, "get_sellable_balances", _get_sellable_balances)
    monkeypatch.setattr(mm.gateway, "sell_release_lag_state", lambda: {"active": False})
    monkeypatch.setattr(mm.reconcile, "reconcile", _reconcile)
    monkeypatch.setattr(mm.gateway, "run_terminal_liquidation_step", _step)
    monkeypatch.setattr(mm.gateway, "cancel_all", _cancel_all)
    monkeypatch.setattr(mm.execution_policy, "sync", _sync)

    mm._terminal_liquidation_active = True
    mm._terminal_liquidation_started_ts = time.time() - 5.0
    mm._terminal_liquidation_done = True
    mm._terminal_liquidation_reason = "terminal_liquidation_done"

    await mm._tick()

    state = mm.snapshot()
    assert calls["step"] == 0
    assert calls["sync"] == 0
    assert state["runtime"]["terminal_liquidation_active"] is True
    assert state["runtime"]["terminal_liquidation_done"] is True
    assert state["lifecycle"] == "unwind"


@pytest.mark.asyncio
async def test_terminal_drawdown_cleanup_keeps_emergency_mode_not_halted(monkeypatch):
    class _MockClient:
        _orders = {}

    mm = MarketMakerV2(SimpleNamespace(), _MockClient(), MMConfigV2(unwind_window_sec=90.0, emergency_taker_start_sec=20.0))
    mm.set_market(_market())
    snapshot = _snapshot(time_left_sec=15.0)
    valuation = PairValuationResult(
        fv_up=snapshot.fv_up,
        fv_dn=snapshot.fv_dn,
        pair_mid=0.5,
        source="midpoint_bounded_model",
        divergence_up=0.0,
        divergence_dn=0.0,
        confidence=snapshot.fv_confidence,
        regime="normal",
        pm_age_sec=0.0,
    )
    inventory = _inventory(
        up_shares=0.4,
        total_inventory_value_usd=0.2,
        free_usdc=15.0,
        wallet_total_usdc=15.0,
    )
    calls = {"step": 0, "sync": 0}

    async def _check_fills():
        return []

    async def _get_books():
        return {}, {}

    def _compute(*, market, feed_state, up_book, dn_book):
        del market, feed_state, up_book, dn_book
        return valuation, snapshot

    def _sync_paper_prices(**kwargs):
        del kwargs

    async def _wallet_balances(*, reference_balances=None):
        del reference_balances
        return 0.4, 0.0, 15.0, 15.0

    async def _get_sellable_balances(*, reference_balances=None):
        del reference_balances
        return 0.4, 0.0

    def _reconcile(**kwargs):
        del kwargs
        return inventory

    async def _step(*, round_idx=0, cancel_existing=True):
        calls["step"] += 1
        return {
            "attempted_orders": 1,
            "placed_orders": 1,
            "remaining_up": 0.4,
            "remaining_dn": 0.0,
            "wallet_total_usdc": 15.0,
            "done": False,
            "reason": "terminal_liquidation_active",
            "placed_order_ids": ["oid-1"],
            "cancelled_orders": 0,
        }

    async def _cancel_all():
        return 0

    async def _sync(_plan):
        calls["sync"] += 1

    def _risk_evaluate(*, snapshot, inventory, analytics, health):
        del snapshot, inventory, analytics
        assert health.drawdown_breach_active is True
        return RiskRegime(
            soft_mode="normal",
            hard_mode="halted",
            target_soft_mode="normal",
            reason="hard drawdown $-10.00 (thr $9.00)",
            inventory_pressure=0.0,
            edge_score=0.8,
            drawdown_pct_budget=0.0,
        )

    monkeypatch.setattr(mm.gateway, "check_fills", _check_fills)
    monkeypatch.setattr(mm.gateway, "get_books", _get_books)
    monkeypatch.setattr(mm.valuation, "compute", _compute)
    monkeypatch.setattr(mm.gateway, "sync_paper_prices", _sync_paper_prices)
    monkeypatch.setattr(mm.gateway, "get_wallet_balances", _wallet_balances)
    monkeypatch.setattr(mm.gateway, "api_error_stats", lambda: {})
    monkeypatch.setattr(mm.gateway, "balance_fetch_health_state", lambda: {})
    monkeypatch.setattr(mm.gateway, "get_sellable_balances", _get_sellable_balances)
    monkeypatch.setattr(mm.gateway, "sell_release_lag_state", lambda: {"active": False})
    monkeypatch.setattr(mm.reconcile, "reconcile", _reconcile)
    monkeypatch.setattr(mm.gateway, "run_terminal_liquidation_step", _step)
    monkeypatch.setattr(mm.gateway, "cancel_all", _cancel_all)
    monkeypatch.setattr(mm.execution_policy, "sync", _sync)
    monkeypatch.setattr(mm, "_update_drawdown_breach", lambda pnl: (3, 8.0, True))
    monkeypatch.setattr(mm.risk_kernel, "evaluate", _risk_evaluate)

    mm._terminal_liquidation_active = True
    mm._terminal_liquidation_started_ts = time.time() - 5.0
    mm._terminal_liquidation_remaining_up = 0.4
    mm._terminal_liquidation_remaining_dn = 0.0

    await mm._tick()

    state = mm.snapshot()
    assert calls["step"] == 1
    assert calls["sync"] == 0
    assert state["lifecycle"] == "unwind"
    assert state["risk"]["hard_mode"] == "emergency_unwind"
    assert state["runtime"]["terminal_liquidation_active"] is True


@pytest.mark.asyncio
async def test_terminal_liquidation_done_reason_is_not_overwritten_by_timeout(monkeypatch):
    class _MockClient:
        _orders = {}

    mm = MarketMakerV2(SimpleNamespace(), _MockClient(), MMConfigV2(unwind_window_sec=90.0, emergency_taker_start_sec=20.0, emergency_unwind_timeout_sec=10.0))
    mm.set_market(_market())
    snapshot = _snapshot(time_left_sec=15.0)
    valuation = PairValuationResult(
        fv_up=snapshot.fv_up,
        fv_dn=snapshot.fv_dn,
        pair_mid=0.5,
        source="midpoint_bounded_model",
        divergence_up=0.0,
        divergence_dn=0.0,
        confidence=snapshot.fv_confidence,
        regime="normal",
        pm_age_sec=0.0,
    )
    inventory = _inventory(
        up_shares=0.03,
        dn_shares=4.88,
        total_inventory_value_usd=2.45,
        free_usdc=20.5,
        wallet_total_usdc=20.5,
    )
    now = [1000.0]

    async def _check_fills():
        return []

    async def _get_books():
        return {}, {}

    def _compute(*, market, feed_state, up_book, dn_book):
        del market, feed_state, up_book, dn_book
        return valuation, snapshot

    def _sync_paper_prices(**kwargs):
        del kwargs

    async def _wallet_balances(*, reference_balances=None):
        del reference_balances
        return 0.03, 4.88, 20.5, 20.5

    async def _get_sellable_balances(*, reference_balances=None):
        del reference_balances
        return 0.03, 4.88

    def _reconcile(**kwargs):
        del kwargs
        return inventory

    async def _step(*, round_idx=0, cancel_existing=True):
        raise AssertionError("terminal step should not rerun when terminal_liquidation_done is already true")

    async def _cancel_all():
        return 0

    async def _sync(_plan):
        raise AssertionError("normal execution sync should not run during terminal liquidation")

    monkeypatch.setattr("mm_v2.runtime.time.time", lambda: now[0])
    monkeypatch.setattr(mm.gateway, "check_fills", _check_fills)
    monkeypatch.setattr(mm.gateway, "get_books", _get_books)
    monkeypatch.setattr(mm.valuation, "compute", _compute)
    monkeypatch.setattr(mm.gateway, "sync_paper_prices", _sync_paper_prices)
    monkeypatch.setattr(mm.gateway, "get_wallet_balances", _wallet_balances)
    monkeypatch.setattr(mm.gateway, "api_error_stats", lambda: {})
    monkeypatch.setattr(mm.gateway, "balance_fetch_health_state", lambda: {})
    monkeypatch.setattr(mm.gateway, "get_sellable_balances", _get_sellable_balances)
    monkeypatch.setattr(mm.gateway, "sell_release_lag_state", lambda: {"active": False})
    monkeypatch.setattr(mm.reconcile, "reconcile", _reconcile)
    monkeypatch.setattr(mm.gateway, "run_terminal_liquidation_step", _step)
    monkeypatch.setattr(mm.gateway, "cancel_all", _cancel_all)
    monkeypatch.setattr(mm.execution_policy, "sync", _sync)

    mm._terminal_liquidation_active = True
    mm._terminal_liquidation_started_ts = now[0] - 15.0
    mm._terminal_liquidation_done = True
    mm._terminal_liquidation_reason = "terminal_liquidation_done"
    mm._terminal_liquidation_remaining_up = 0.03
    mm._terminal_liquidation_remaining_dn = 4.88

    await mm._tick()

    state = mm.snapshot()
    assert state["runtime"]["terminal_liquidation_done"] is True
    assert state["runtime"]["terminal_liquidation_reason"] == "terminal_liquidation_done"


def test_terminal_cleanup_grace_blocks_halted_after_done():
    cfg = MMConfigV2(unwind_window_sec=90.0, emergency_taker_start_sec=20.0)
    snapshot = _snapshot(time_left_sec=15.0)
    inventory = _inventory(
        up_shares=0.4,
        total_inventory_value_usd=0.2,
        free_usdc=15.0,
        wallet_total_usdc=15.0,
    )
    risk = HardSafetyKernel(cfg).evaluate(
        snapshot=snapshot,
        inventory=inventory,
        analytics=AnalyticsState(),
        health=HealthState(
            drawdown_breach_active=True,
            post_terminal_cleanup_grace_active=True,
        ),
    )
    assert risk.hard_mode == "none"
    assert risk.reason.startswith("terminal cleanup grace:")


@pytest.mark.asyncio
async def test_terminal_liquidation_done_with_dust_exposes_cleanup_grace(monkeypatch):
    class _MockClient:
        _orders = {}

    mm = MarketMakerV2(SimpleNamespace(), _MockClient(), MMConfigV2(unwind_window_sec=90.0, emergency_taker_start_sec=20.0))
    mm.set_market(_market())
    snapshot = _snapshot(time_left_sec=12.0)
    valuation = PairValuationResult(
        fv_up=snapshot.fv_up,
        fv_dn=snapshot.fv_dn,
        pair_mid=0.5,
        source="midpoint_bounded_model",
        divergence_up=0.0,
        divergence_dn=0.0,
        confidence=snapshot.fv_confidence,
        regime="normal",
        pm_age_sec=0.0,
    )
    inventory = _inventory(
        up_shares=0.3,
        dn_shares=0.2,
        total_inventory_value_usd=0.25,
        free_usdc=15.0,
        wallet_total_usdc=15.0,
    )
    seen = {"terminal_cleanup_grace": None}

    async def _check_fills():
        return []

    async def _get_books():
        return {}, {}

    def _compute(*, market, feed_state, up_book, dn_book):
        del market, feed_state, up_book, dn_book
        return valuation, snapshot

    def _sync_paper_prices(**kwargs):
        del kwargs

    async def _wallet_balances(*, reference_balances=None):
        del reference_balances
        return 0.3, 0.2, 15.0, 15.0

    async def _get_sellable_balances(*, reference_balances=None):
        del reference_balances
        return 0.3, 0.2

    def _reconcile(**kwargs):
        seen["terminal_cleanup_grace"] = kwargs.get("terminal_cleanup_grace")
        return inventory

    async def _cancel_all():
        return 0

    async def _sync(_plan):
        raise AssertionError("normal execution sync should not run during terminal cleanup grace")

    monkeypatch.setattr(mm.gateway, "check_fills", _check_fills)
    monkeypatch.setattr(mm.gateway, "get_books", _get_books)
    monkeypatch.setattr(mm.valuation, "compute", _compute)
    monkeypatch.setattr(mm.gateway, "sync_paper_prices", _sync_paper_prices)
    monkeypatch.setattr(mm.gateway, "get_wallet_balances", _wallet_balances)
    monkeypatch.setattr(mm.gateway, "api_error_stats", lambda: {})
    monkeypatch.setattr(mm.gateway, "balance_fetch_health_state", lambda: {})
    monkeypatch.setattr(mm.gateway, "get_sellable_balances", _get_sellable_balances)
    monkeypatch.setattr(mm.gateway, "sell_release_lag_state", lambda: {"active": False})
    monkeypatch.setattr(mm.reconcile, "reconcile", _reconcile)
    monkeypatch.setattr(mm.gateway, "cancel_all", _cancel_all)
    monkeypatch.setattr(mm.execution_policy, "sync", _sync)

    mm._terminal_liquidation_active = True
    mm._terminal_liquidation_done = True
    mm._terminal_liquidation_reason = "terminal_liquidation_done"
    mm._terminal_liquidation_remaining_up = 0.3
    mm._terminal_liquidation_remaining_dn = 0.2

    await mm._tick()

    state = mm.snapshot()
    assert seen["terminal_cleanup_grace"] is True
    assert state["health"]["post_terminal_cleanup_grace_active"] is True
    assert state["runtime"]["post_terminal_cleanup_grace_active"] is True
    assert state["runtime"]["post_terminal_cleanup_grace_sec"] >= 0.0


def test_balanced_profile_avoids_early_unwind_after_first_fill():
    snapshot = _snapshot()
    first_fill_inventory = _inventory(
        dn_shares=6.0,
        excess_dn_qty=6.0,
        excess_dn_value_usd=6.5,
        excess_value_usd=6.5,
        signed_excess_value_usd=-6.5,
        total_inventory_value_usd=6.5,
    )
    legacy_cfg = MMConfigV2(
        session_budget_usd=15.0,
        base_clip_usd=6.0,
        soft_excess_value_ratio=0.10,
        defensive_excess_value_ratio=0.18,
        hard_excess_value_ratio=0.25,
        defensive_spread_mult=1.8,
        defensive_size_mult=0.5,
    )
    balanced_cfg = MMConfigV2(session_budget_usd=30.0, base_clip_usd=6.0, defensive_spread_mult=1.5, defensive_size_mult=0.4)
    legacy_risk, _, _ = _risk_and_plan(legacy_cfg, snapshot, first_fill_inventory)
    balanced_risk, _, balanced_viability = _risk_and_plan(balanced_cfg, snapshot, first_fill_inventory)
    assert legacy_risk.target_soft_mode in {"defensive", "unwind"}
    assert balanced_risk.target_soft_mode in {"normal", "inventory_skewed"}

    sm = StateMachineV2(balanced_cfg)
    sm.transition(snapshot=snapshot, inventory=first_fill_inventory, risk=balanced_risk, viability=balanced_viability)
    sm.transition(snapshot=snapshot, inventory=first_fill_inventory, risk=balanced_risk, viability=balanced_viability)
    result = sm.transition(snapshot=snapshot, inventory=first_fill_inventory, risk=balanced_risk, viability=balanced_viability)
    assert result.lifecycle in {"inventory_skewed", "defensive"}
    assert result.lifecycle != "unwind"


def test_mode_ratios_improve_vs_report_baseline():
    class _MockClient:
        _orders = {}

    mm = MarketMakerV2(SimpleNamespace(), _MockClient(), MMConfigV2())
    now = time.time()
    mm._lifecycle_history = [
        (now - 55.0, "quoting"),
        (now - 50.0, "quoting"),
        (now - 45.0, "inventory_skewed"),
        (now - 40.0, "defensive"),
        (now - 35.0, "quoting"),
        (now - 30.0, "inventory_skewed"),
        (now - 25.0, "defensive"),
        (now - 20.0, "quoting"),
        (now - 15.0, "unwind"),
        (now - 10.0, "inventory_skewed"),
        (now - 5.0, "defensive"),
    ]
    ratios = mm._lifecycle_ratios(window_sec=60.0)
    mm_effective_ratio = (
        ratios["quoting_ratio_60s"]
        + ratios["inventory_skewed_ratio_60s"]
        + ratios["defensive_ratio_60s"]
    )
    assert mm_effective_ratio >= 0.60
    assert ratios["unwind_ratio_60s"] <= 0.35
