from __future__ import annotations

import importlib
import inspect
import os
import sys
import time
from types import SimpleNamespace

import pytest
from fastapi import Response


BASE = os.path.dirname(os.path.dirname(__file__))
SRC = os.path.join(BASE, "src")
if SRC not in sys.path:
    sys.path.insert(0, SRC)
if BASE not in sys.path:
    sys.path.insert(0, BASE)

from mm_shared.types import Fill, MarketInfo, Quote
from mm_shared.mm_config import MMConfig
from mm_shared.order_manager import OrderManager
from mm_v2.config import MMConfigV2
from mm_v2.pair_inventory import build_pair_inventory
from mm_v2.pm_gateway import PMGateway
from mm_v2.quote_policy import QuoteContext, QuotePolicyV2
from mm_v2.reconcile import ReconcileV2
from mm_v2.risk_kernel import HardSafetyKernel
from mm_v2.runtime import MarketMakerV2
from mm_v2.state_machine import StateMachineV2
from mm_v2.types import AnalyticsState, HealthState, PairInventoryState, PairMarketSnapshot, QuoteIntent, QuoteViabilitySummary


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


def _snapshot(**overrides) -> PairMarketSnapshot:
    now = time.time()
    payload = dict(
        ts=now,
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
        free_usdc=15.0,
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


def test_pmgateway_disables_naked_sells_for_live_client():
    class _LiveClient:
        pass

    gateway = PMGateway(_LiveClient(), MMConfigV2())
    assert gateway.supports_naked_sells() is False
    assert gateway.transport_config.allow_short_sells is False


def test_pmgateway_keeps_naked_sells_for_mock_client():
    class _MockClient:
        _orders = {}

    gateway = PMGateway(_MockClient(), MMConfigV2())
    assert gateway.supports_naked_sells() is True
    assert gateway.transport_config.allow_short_sells is True


@pytest.mark.asyncio
async def test_pmgateway_reprices_post_only_sell_after_crosses_book(monkeypatch):
    class _LiveClient:
        pass

    gateway = PMGateway(_LiveClient(), MMConfigV2())
    gateway.set_market(_market())
    calls: list[Quote] = []

    async def _place_order(quote: Quote, *, post_only=None, fallback_taker=False):
        calls.append(quote)
        if len(calls) == 1:
            return None
        return "oid-2"

    async def _get_full_book(token_id: str):
        assert token_id == "up-token"
        return {"best_bid": 0.58, "best_ask": 0.60, "bids": [], "asks": []}

    monkeypatch.setattr(gateway.order_mgr, "place_order", _place_order)
    monkeypatch.setattr(
        gateway.order_mgr,
        "get_api_error_stats",
        lambda: {
            "total_by_op": {"place_order": 1},
            "transport_total_by_op": {},
            "recent": [
                {
                    "op": "place_order",
                    "token_id": "up-token",
                    "message": "PolyApiException[status_code=400, error_message={'error': 'invalid post-only order: order crosses book'}]",
                    "details": {"side": "SELL"},
                }
            ],
        },
    )
    monkeypatch.setattr(gateway.order_mgr, "get_full_book", _get_full_book)

    order_id = await gateway.place_intent(
        QuoteIntent(
            token="up-token",
            side="SELL",
            price=0.58,
            size=5.0,
            quote_role="base_ask",
            post_only=True,
            inventory_effect="helpful",
        )
    )

    assert order_id == "oid-2"
    assert len(calls) == 2
    assert calls[1].price > calls[0].price
    assert calls[1].price > 0.58


@pytest.mark.asyncio
async def test_pmgateway_stop_liquidation_uses_owned_fallback_and_force_sell(monkeypatch):
    class _MockClient:
        _orders = {}

    gateway = PMGateway(_MockClient(), MMConfigV2())
    gateway.set_market(_market())
    calls: list[tuple[Quote, dict]] = []
    sellable_refs: list[tuple[float, float] | None] = []

    async def _sellable_balances(*, reference_balances=None):
        sellable_refs.append(reference_balances)
        return 0.0, 0.0

    async def _wallet_balances(*, reference_balances=None):
        del reference_balances
        return 6.2, 7.4, 15.0, 15.0

    async def _get_full_book(_token_id: str):
        return {"best_bid": 0.51}

    async def _place_order(quote: Quote, **kwargs):
        calls.append((quote, kwargs))
        return f"oid-{len(calls)}"

    async def _check_fills():
        return []

    monkeypatch.setattr(gateway, "get_sellable_balances", _sellable_balances)
    monkeypatch.setattr(gateway, "get_wallet_balances", _wallet_balances)
    monkeypatch.setattr(gateway.order_mgr, "get_full_book", _get_full_book)
    monkeypatch.setattr(gateway.order_mgr, "place_order", _place_order)
    monkeypatch.setattr(gateway.order_mgr, "check_fills", _check_fills)

    result = await gateway.emergency_flatten_on_stop(rounds=1, round_delay_sec=0.01)

    assert result["attempted_orders"] == 2
    assert result["placed_orders"] == 2
    assert len(calls) == 2
    assert sellable_refs
    assert sellable_refs[0] == pytest.approx((6.2, 7.4))
    for _quote, kwargs in calls:
        assert kwargs.get("post_only") is False
        assert kwargs.get("ignore_sell_cooldowns") is True
        assert kwargs.get("ignore_recent_cancelled_reserve") is True


def test_order_manager_crosses_book_not_counted_as_transport_failure():
    class _MockClient:
        _orders = {}

    mgr = OrderManager(_MockClient(), MMConfig())
    mgr._record_api_error(
        op="place_order",
        token_id="up-token",
        status_code=400,
        message="invalid post-only order: order crosses book",
        details={"side": "SELL", "post_only": True},
    )
    stats = mgr.get_api_error_stats()
    assert stats["total_by_op"]["place_order"] == 1
    assert stats["transport_total_by_op"] == {}


def test_mmv2_health_ignores_crosses_book_when_transport_totals_empty(monkeypatch):
    class _MockClient:
        _orders = {}

    mm = MarketMakerV2(SimpleNamespace(), _MockClient(), MMConfigV2())
    monkeypatch.setattr(
        mm.gateway,
        "api_error_stats",
        lambda: {
            "total_by_op": {"place_order": 99},
            "transport_total_by_op": {},
            "recent": [
                {
                    "op": "place_order",
                    "message": "invalid post-only order: order crosses book",
                }
            ],
        },
    )
    health = mm._build_health()
    assert health.transport_ok is True
    assert "crosses book" in health.last_api_error


def test_runtime_wallet_snapshot_coalesce_uses_expected_balances_for_missing_values():
    class _MockClient:
        _orders = {}

    mm = MarketMakerV2(SimpleNamespace(), _MockClient(), MMConfigV2())
    mm.reconcile.start_session(7.0, 3.0)
    mm._last_inventory = _inventory(up_shares=1.0, dn_shares=2.0, free_usdc=11.0, reserved_usdc=2.0)

    up, dn, total, available, stale = mm._coalesce_wallet_snapshot(
        up=None,
        dn=None,
        total_usdc=None,
        available_usdc=None,
    )

    assert stale is True
    assert up == pytest.approx(7.0)
    assert dn == pytest.approx(3.0)
    assert total == pytest.approx(13.0)
    assert available == pytest.approx(11.0)


def test_runtime_on_fill_emits_callbacks_with_token_type():
    class _MockClient:
        _orders = {}

    mm = MarketMakerV2(SimpleNamespace(), _MockClient(), MMConfigV2())
    mm.set_market(_market())
    seen: list[tuple[str, str]] = []
    mm.on_fill(lambda fill, token_type: seen.append((fill.token_id, token_type)))

    mm._emit_fill_callbacks(
        Fill(
            ts=time.time(),
            side="BUY",
            token_id=mm.market.up_token_id,
            price=0.51,
            size=5.0,
            fee=0.0,
            is_maker=True,
        )
    )
    mm._emit_fill_callbacks(
        Fill(
            ts=time.time(),
            side="SELL",
            token_id=mm.market.dn_token_id,
            price=0.49,
            size=5.0,
            fee=0.0,
            is_maker=True,
        )
    )

    assert seen[0] == (mm.market.up_token_id, "up")
    assert seen[1] == (mm.market.dn_token_id, "dn")


def test_runtime_session_pnl_uses_wallet_total_not_reserved_bookkeeping():
    class _MockClient:
        _orders = {}

    mm = MarketMakerV2(SimpleNamespace(), _MockClient(), MMConfigV2())
    mm._starting_portfolio = 15.0
    inventory = _inventory(
        up_shares=2.0,
        free_usdc=9.0,
        reserved_usdc=8.0,  # stale/local reservation can exceed true wallet lock
    )
    mm._update_session_pnl(
        inventory,
        total_usdc=10.0,
        snapshot=_snapshot(pm_mid_up=0.5, pm_mid_dn=0.5, up_best_bid=None, dn_best_bid=None),
    )
    assert mm._session_pnl == pytest.approx(-4.0)


def test_runtime_session_pnl_marks_inventory_with_conservative_bid_when_available():
    class _MockClient:
        _orders = {}

    mm = MarketMakerV2(SimpleNamespace(), _MockClient(), MMConfigV2())
    mm._starting_portfolio = 10.0
    inventory = _inventory(
        up_shares=1.0,
        dn_shares=0.0,
        free_usdc=8.0,
        reserved_usdc=1.0,
    )
    # Conservative mark must use best bid first for risk PnL.
    snap = _snapshot(pm_mid_up=0.9, pm_mid_dn=0.1, up_best_bid=0.6, dn_best_bid=0.2, fv_up=0.2, fv_dn=0.8)
    mm._update_session_pnl(inventory, total_usdc=8.0, snapshot=snap)
    assert mm._session_pnl == pytest.approx(-1.4)
    assert mm._session_pnl_equity_usd == pytest.approx(-1.4)


def test_runtime_v2_fill_context_includes_coin_timeframe():
    web_server = importlib.import_module("web_server")
    runtime = web_server.MMRuntimeV2()
    runtime._coin = "BTC"
    runtime._timeframe = "15m"
    runtime._paper_mode = False
    runtime.mm_v2 = SimpleNamespace(
        snapshot=lambda app_version="", app_git_hash="": {
            "market": {"market_id": "btc-15m"},
            "inventory": {"up_shares": 1.0},
            "valuation": {"fv_up": 0.52},
            "risk": {"soft_mode": "normal"},
            "analytics": {"session_pnl_equity_usd": 1.2, "session_pnl_operator_usd": 0.8},
        }
    )

    ctx = runtime._fill_context_v2()
    assert ctx["market"]["coin"] == "BTC"
    assert ctx["market"]["timeframe"] == "15m"
    assert ctx["paper_mode"] is False
    assert ctx["engine"] == "v2"
    assert ctx["pnl"]["session_pnl_equity_usd"] == pytest.approx(1.2)


@pytest.mark.asyncio
async def test_runtime_start_requires_initial_wallet_snapshot(monkeypatch):
    class _MockClient:
        _orders = {}

    mm = MarketMakerV2(SimpleNamespace(), _MockClient(), MMConfigV2())
    mm.set_market(_market())

    async def _missing_wallet_balances(*, reference_balances=None):
        del reference_balances
        return None, None, None, None

    monkeypatch.setattr(mm.gateway, "get_wallet_balances", _missing_wallet_balances)

    with pytest.raises(RuntimeError, match="Unable to fetch initial PM wallet snapshot"):
        await mm.start()
    assert mm._running is False


@pytest.mark.asyncio
async def test_mmv2_stop_runs_emergency_flatten_when_liquidate_true(monkeypatch):
    class _MockClient:
        _orders = {}

    mm = MarketMakerV2(SimpleNamespace(), _MockClient(), MMConfigV2())
    calls: list[str] = []

    async def _cancel_all():
        calls.append("cancel_all")
        return 0

    async def _flatten():
        calls.append("flatten")
        return {"done": True, "remaining_up": 0.0, "remaining_dn": 0.0}

    async def _hb_stop():
        calls.append("heartbeat_stop")

    monkeypatch.setattr(mm.gateway, "cancel_all", _cancel_all)
    monkeypatch.setattr(mm.gateway, "emergency_flatten_on_stop", _flatten)
    monkeypatch.setattr(mm.heartbeat, "stop", _hb_stop)

    result = await mm.stop(liquidate=True)
    assert calls == ["cancel_all", "flatten", "cancel_all", "heartbeat_stop"]
    assert result["enabled"] is True
    assert result["done"] is True


@pytest.mark.asyncio
async def test_mmv2_stop_skips_emergency_flatten_when_liquidate_false(monkeypatch):
    class _MockClient:
        _orders = {}

    mm = MarketMakerV2(SimpleNamespace(), _MockClient(), MMConfigV2())
    calls: list[str] = []

    async def _cancel_all():
        calls.append("cancel_all")
        return 0

    async def _flatten():
        calls.append("flatten")
        return {"done": True}

    async def _hb_stop():
        calls.append("heartbeat_stop")

    monkeypatch.setattr(mm.gateway, "cancel_all", _cancel_all)
    monkeypatch.setattr(mm.gateway, "emergency_flatten_on_stop", _flatten)
    monkeypatch.setattr(mm.heartbeat, "stop", _hb_stop)

    result = await mm.stop(liquidate=False)
    assert calls == ["cancel_all", "cancel_all", "heartbeat_stop"]
    assert result["enabled"] is False
    assert result["done"] is True


def test_pair_inventory_decomposition_tracks_pair_and_pending_orders():
    active_orders = {
        "b-up": Quote(side="BUY", token_id="up-token", price=0.45, size=5.0),
        "s-dn": Quote(side="SELL", token_id="dn-token", price=0.55, size=6.0),
    }
    state = build_pair_inventory(
        up_shares=12.0,
        dn_shares=7.0,
        total_usdc=20.0,
        available_usdc=17.75,
        active_orders=active_orders,
        fv_up=0.6,
        fv_dn=0.4,
        up_token_id="up-token",
        dn_token_id="dn-token",
    )
    assert state.paired_qty == 7.0
    assert state.excess_up_qty == 5.0
    assert state.excess_dn_qty == 0.0
    assert state.pending_buy_up == 5.0
    assert state.pending_sell_dn == 6.0
    assert state.reserved_usdc == pytest.approx(2.25)
    assert state.paired_value_usd == pytest.approx(7.0)
    assert state.excess_up_value_usd == pytest.approx(3.0)


def test_mmv2_balanced_default_profile_and_caps():
    cfg = MMConfigV2()
    assert cfg.session_budget_usd == pytest.approx(30.0)
    assert cfg.base_half_spread_bps == pytest.approx(100.0)
    assert cfg.defensive_spread_mult == pytest.approx(1.5)
    assert cfg.defensive_size_mult == pytest.approx(0.4)
    soft_cap = cfg.session_budget_usd * cfg.soft_excess_value_ratio
    def_cap = cfg.session_budget_usd * cfg.defensive_excess_value_ratio
    hard_cap = cfg.session_budget_usd * cfg.hard_excess_value_ratio
    assert soft_cap == pytest.approx(3.0)
    assert def_cap == pytest.approx(5.4)
    assert hard_cap == pytest.approx(7.5)


def test_quote_policy_preserves_pair_shape_and_two_sided_bids():
    cfg = MMConfigV2(base_clip_usd=6.0)
    snap = _snapshot()
    risk = HardSafetyKernel(cfg).evaluate(
        snapshot=snap,
        inventory=_inventory(),
        analytics=AnalyticsState(),
        health=HealthState(),
    )
    plan = QuotePolicyV2(cfg).generate(
        snapshot=snap,
        inventory=_inventory(),
        risk=risk,
        ctx=QuoteContext(tick_size=0.01, min_order_size=1.0),
    )
    assert plan.up_bid is not None
    assert plan.up_ask is not None
    assert plan.dn_bid is not None
    assert plan.dn_ask is not None
    assert plan.up_bid.price < 0.54
    assert plan.dn_bid.price < 0.47
    assert plan.up_bid.price + plan.dn_ask.price <= 0.99
    assert plan.dn_bid.price + plan.up_ask.price <= 0.99
    assert plan.up_bid.price < snap.up_best_ask
    assert plan.dn_bid.price < snap.dn_best_ask


def test_quote_policy_skews_against_excess_up_inventory():
    cfg = MMConfigV2(base_clip_usd=6.0)
    policy = QuotePolicyV2(cfg)
    snap = _snapshot()
    flat_plan = policy.generate(
        snapshot=snap,
        inventory=_inventory(),
        risk=HardSafetyKernel(cfg).evaluate(
            snapshot=snap,
            inventory=_inventory(),
            analytics=AnalyticsState(),
            health=HealthState(),
        ),
        ctx=QuoteContext(tick_size=0.01, min_order_size=1.0),
    )
    excess_inventory = _inventory(
        up_shares=8.0,
        dn_shares=2.0,
        paired_qty=2.0,
        excess_up_qty=6.0,
        paired_value_usd=2.0,
        excess_up_value_usd=3.24,
        excess_value_usd=3.24,
        signed_excess_value_usd=3.24,
        total_inventory_value_usd=4.16,
    )
    risk = HardSafetyKernel(cfg).evaluate(
        snapshot=snap,
        inventory=excess_inventory,
        analytics=AnalyticsState(),
        health=HealthState(),
    )
    skewed_plan = policy.generate(
        snapshot=snap,
        inventory=excess_inventory,
        risk=risk,
        ctx=QuoteContext(tick_size=0.01, min_order_size=1.0),
    )
    assert skewed_plan.up_bid is not None and flat_plan.up_bid is not None
    assert skewed_plan.dn_bid is not None and flat_plan.dn_bid is not None
    assert skewed_plan.up_bid.price <= flat_plan.up_bid.price
    assert skewed_plan.dn_bid.price >= flat_plan.dn_bid.price
    assert skewed_plan.up_bid.inventory_effect == "harmful"
    assert skewed_plan.dn_bid.inventory_effect == "helpful"
    assert skewed_plan.regime in {"inventory_skewed", "unwind", "defensive", "normal"}


def test_reconcile_settlement_lag_does_not_trigger_true_drift():
    cfg = MMConfigV2(fill_settlement_grace_sec=6.0, reconcile_drift_threshold_shares=1.5)
    reconcile = ReconcileV2(cfg)
    market = _market()
    reconcile.align(0.0, 0.0)
    reconcile.record_fill(
        Fill(ts=time.time(), side="BUY", token_id=market.up_token_id, price=0.54, size=5.0, fee=0.0, is_maker=True),
        market,
    )
    state = reconcile.reconcile(
        market=market,
        real_up=0.0,
        real_dn=0.0,
        total_usdc=15.0,
        available_usdc=15.0,
        active_orders={},
        fv_up=0.54,
        fv_dn=0.46,
    )
    assert reconcile.status == "settlement_lag"
    assert reconcile.true_drift is False
    assert state.up_shares == 0.0


def test_reconcile_sellability_lag_does_not_trigger_true_drift():
    cfg = MMConfigV2(reconcile_drift_threshold_shares=1.5)
    reconcile = ReconcileV2(cfg)
    market = _market()
    reconcile.align(0.0, 6.0)
    state = reconcile.reconcile(
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
    assert state.dn_shares == 0.0


def test_reconcile_startup_balance_jump_without_fills_realigns_not_broken():
    cfg = MMConfigV2(reconcile_drift_threshold_shares=1.5)
    reconcile = ReconcileV2(cfg)
    market = _market()
    reconcile.start_session(0.0, 0.0)

    state = reconcile.reconcile(
        market=market,
        real_up=5.0,
        real_dn=0.0,
        total_usdc=15.0,
        available_usdc=15.0,
        active_orders={},
        fv_up=0.54,
        fv_dn=0.46,
    )
    assert reconcile.status == "startup_realign"
    assert reconcile.true_drift is False
    assert state.up_shares == pytest.approx(5.0)

    reconcile.reconcile(
        market=market,
        real_up=5.0,
        real_dn=0.0,
        total_usdc=15.0,
        available_usdc=15.0,
        active_orders={},
        fv_up=0.54,
        fv_dn=0.46,
    )
    assert reconcile.status == "ok"
    assert reconcile.true_drift is False


def test_reconcile_startup_realign_limit_still_allows_true_drift():
    cfg = MMConfigV2(reconcile_drift_threshold_shares=1.5)
    reconcile = ReconcileV2(cfg)
    market = _market()
    reconcile.start_session(0.0, 0.0)
    sequence = [(5.0, 0.0), (0.0, 5.0), (5.0, 0.0), (0.0, 5.0), (0.0, 5.0)]
    statuses: list[str] = []
    for up, dn in sequence:
        reconcile.reconcile(
            market=market,
            real_up=up,
            real_dn=dn,
            total_usdc=15.0,
            available_usdc=15.0,
            active_orders={},
            fv_up=0.54,
            fv_dn=0.46,
        )
        statuses.append(reconcile.status)

    # Drift confirmation now requires minimum candidate age.
    reconcile._drift_candidate_started_ts = time.time() - 9.0
    reconcile.reconcile(
        market=market,
        real_up=0.0,
        real_dn=5.0,
        total_usdc=15.0,
        available_usdc=15.0,
        active_orders={},
        fv_up=0.54,
        fv_dn=0.46,
    )
    statuses.append(reconcile.status)

    assert statuses[:3] == ["startup_realign", "startup_realign", "startup_realign"]
    assert statuses[3] == "drift_pending"
    assert statuses[4] == "drift_pending"
    assert statuses[5] == "broken"
    assert reconcile.true_drift is True


def test_reconcile_requires_persistent_unexplained_mismatch_before_true_drift():
    cfg = MMConfigV2(reconcile_drift_threshold_shares=1.5)
    reconcile = ReconcileV2(cfg)
    market = _market()
    reconcile.align(0.0, 0.0)
    statuses: list[str] = []
    flags: list[bool] = []
    for _ in range(3):
        reconcile.reconcile(
            market=market,
            real_up=8.0,
            real_dn=0.0,
            total_usdc=15.0,
            available_usdc=15.0,
            active_orders={},
            fv_up=0.54,
            fv_dn=0.46,
            sellability_lag_active=False,
        )
        statuses.append(reconcile.status)
        flags.append(reconcile.true_drift)
        if len(statuses) == 2:
            reconcile._drift_candidate_started_ts = time.time() - 9.0
    assert statuses[0] == "drift_pending"
    assert statuses[1] == "drift_pending"
    assert statuses[2] == "broken"
    assert flags == [False, False, True]


def test_hard_excess_transitions_state_machine_to_unwind():
    cfg = MMConfigV2(session_budget_usd=15.0, hard_excess_value_ratio=0.25)
    inventory = _inventory(
        up_shares=12.0,
        dn_shares=1.0,
        paired_qty=1.0,
        excess_up_qty=11.0,
        paired_value_usd=1.0,
        excess_up_value_usd=6.16,
        excess_value_usd=6.16,
        signed_excess_value_usd=6.16,
        total_inventory_value_usd=6.62,
    )
    sm = StateMachineV2(cfg)
    risk = HardSafetyKernel(cfg).evaluate(
        snapshot=_snapshot(),
        inventory=inventory,
        analytics=AnalyticsState(),
        health=HealthState(),
    )
    transition = sm.transition(
        snapshot=_snapshot(),
        inventory=inventory,
        risk=risk,
        viability=QuoteViabilitySummary(any_quote=True, four_quotes=False, helpful_count=1),
    )
    assert transition.lifecycle == "quoting"
    transition = sm.transition(
        snapshot=_snapshot(),
        inventory=inventory,
        risk=risk,
        viability=QuoteViabilitySummary(any_quote=True, four_quotes=False, helpful_count=1),
    )
    assert transition.lifecycle == "inventory_skewed"
    transition = sm.transition(
        snapshot=_snapshot(),
        inventory=inventory,
        risk=risk,
        viability=QuoteViabilitySummary(any_quote=True, four_quotes=False, helpful_count=1),
    )
    assert transition.lifecycle == "defensive"
    transition = sm.transition(
        snapshot=_snapshot(),
        inventory=inventory,
        risk=risk,
        viability=QuoteViabilitySummary(any_quote=True, four_quotes=False, helpful_count=0, harmful_count=2),
    )
    assert risk.target_soft_mode == "unwind"
    assert transition.lifecycle == "unwind"
    assert transition.effective_soft_mode == "unwind"


def test_low_budget_profile_reduces_effective_clip_usd():
    cfg = MMConfigV2(session_budget_usd=15.0, base_clip_usd=6.0)
    assert cfg.effective_base_clip_usd() == pytest.approx(4.5)


def test_low_budget_profile_raises_effective_hard_excess_ratio():
    cfg = MMConfigV2(session_budget_usd=15.0, hard_excess_value_ratio=0.25)
    assert cfg.effective_hard_excess_value_ratio() == pytest.approx(0.35)


@pytest.mark.asyncio
async def test_dashboard_state_surfaces_inventory_pressure_fields(monkeypatch):
    web_server = importlib.import_module("web_server")
    monkeypatch.setattr(web_server, "_require_auth", lambda _request: None)
    monkeypatch.setattr(
        web_server._runtime_v2,
        "snapshot",
        lambda: {
            "lifecycle": "inventory_skewed",
            "risk": {
                "soft_mode": "inventory_skewed",
                "target_soft_mode": "defensive",
                "inventory_side": "dn",
                "inventory_pressure_abs": 0.42,
                "inventory_pressure_signed": -0.42,
                "quality_pressure": 0.1,
            },
            "analytics": {
                "excess_value_usd": 6.3,
                "inventory_half_life_sec": 28.0,
                "four_quote_presence_ratio": 0.75,
                "helpful_quote_count": 2,
                "harmful_quote_count": 2,
                "quote_balance_state": "balanced",
            },
        },
    )
    resp = await web_server.mmv2_state(request=object())
    assert resp["risk"]["inventory_side"] == "dn"
    assert resp["risk"]["inventory_pressure_abs"] == pytest.approx(0.42)
    assert resp["analytics"]["excess_value_usd"] == pytest.approx(6.3)
    assert resp["analytics"]["inventory_half_life_sec"] == pytest.approx(28.0)
    assert resp["risk"]["target_soft_mode"] == "defensive"


@pytest.mark.asyncio
async def test_mmv2_state_exposes_quote_inventory_effect(monkeypatch):
    web_server = importlib.import_module("web_server")
    monkeypatch.setattr(web_server, "_require_auth", lambda _request: None)
    monkeypatch.setattr(
        web_server._runtime_v2,
        "snapshot",
        lambda: {
            "lifecycle": "quoting",
            "quotes": {
                "up_bid": {
                    "token": "up-token",
                    "side": "BUY",
                    "price": 0.52,
                    "size": 5.0,
                    "active": True,
                    "inventory_effect": "helpful",
                    "size_mult": 1.4,
                    "price_adjust_ticks": 2,
                    "suppressed_reason": None,
                }
            },
            "risk": {"soft_mode": "inventory_skewed"},
        },
    )
    resp = await web_server.mmv2_state(request=object())
    assert resp["quotes"]["up_bid"]["inventory_effect"] == "helpful"
    assert resp["quotes"]["up_bid"]["size_mult"] == pytest.approx(1.4)
    assert resp["quotes"]["up_bid"]["price_adjust_ticks"] == 2


@pytest.mark.asyncio
async def test_mmv2_state_exposes_target_and_effective_soft_modes(monkeypatch):
    web_server = importlib.import_module("web_server")
    monkeypatch.setattr(web_server, "_require_auth", lambda _request: None)
    monkeypatch.setattr(
        web_server._runtime_v2,
        "snapshot",
        lambda: {
            "lifecycle": "defensive",
            "risk": {
                "soft_mode": "defensive",
                "target_soft_mode": "inventory_skewed",
            },
        },
    )
    resp = await web_server.mmv2_state(request=object())
    assert resp["risk"]["soft_mode"] == "defensive"
    assert resp["risk"]["target_soft_mode"] == "inventory_skewed"


@pytest.mark.asyncio
async def test_mmv2_state_exposes_quote_balance_state(monkeypatch):
    web_server = importlib.import_module("web_server")
    monkeypatch.setattr(web_server, "_require_auth", lambda _request: None)
    monkeypatch.setattr(
        web_server._runtime_v2,
        "snapshot",
        lambda: {
            "lifecycle": "defensive",
            "analytics": {"quote_balance_state": "harmful_only_blocked"},
            "quote_balance_state": "harmful_only_blocked",
        },
    )
    resp = await web_server.mmv2_state(request=object())
    assert resp["analytics"]["quote_balance_state"] == "harmful_only_blocked"
    assert resp["quote_balance_state"] == "harmful_only_blocked"


@pytest.mark.asyncio
async def test_mmv2_state_exposes_quote_suppressed_reason(monkeypatch):
    web_server = importlib.import_module("web_server")
    monkeypatch.setattr(web_server, "_require_auth", lambda _request: None)
    monkeypatch.setattr(
        web_server._runtime_v2,
        "snapshot",
        lambda: {
            "lifecycle": "inventory_skewed",
            "quotes": {
                "dn_bid": {
                    "active": False,
                    "suppressed_reason": "harmful blocked without helpful viability",
                }
            },
        },
    )
    resp = await web_server.mmv2_state(request=object())
    assert resp["quotes"]["dn_bid"]["suppressed_reason"] == "harmful blocked without helpful viability"


@pytest.mark.asyncio
async def test_state_exposes_min_viable_clip_usd(monkeypatch):
    web_server = importlib.import_module("web_server")
    monkeypatch.setattr(web_server, "_require_auth", lambda _request: None)
    monkeypatch.setattr(
        web_server._runtime_v2,
        "snapshot",
        lambda: {
            "lifecycle": "defensive",
            "analytics": {
                "min_viable_clip_usd": 4.9,
                "quote_viability_reason": "helpful_floor_applied",
            },
        },
    )
    resp = await web_server.mmv2_state(request=object())
    assert resp["analytics"]["min_viable_clip_usd"] == pytest.approx(4.9)


@pytest.mark.asyncio
async def test_state_exposes_quote_viability_reason(monkeypatch):
    web_server = importlib.import_module("web_server")
    monkeypatch.setattr(web_server, "_require_auth", lambda _request: None)
    monkeypatch.setattr(
        web_server._runtime_v2,
        "snapshot",
        lambda: {
            "lifecycle": "defensive",
            "analytics": {
                "quote_viability_reason": "all_quotes_below_min_size",
            },
        },
    )
    resp = await web_server.mmv2_state(request=object())
    assert resp["analytics"]["quote_viability_reason"] == "all_quotes_below_min_size"


@pytest.mark.asyncio
async def test_state_exposes_wallet_inventory_usdc_fields(monkeypatch):
    web_server = importlib.import_module("web_server")
    monkeypatch.setattr(web_server, "_require_auth", lambda _request: None)
    monkeypatch.setattr(
        web_server._runtime_v2,
        "snapshot",
        lambda: {
            "lifecycle": "quoting",
            "inventory": {
                "wallet_total_usdc": 27.5,
                "wallet_reserved_usdc": 4.5,
                "pending_buy_reserved_usdc": 1.2,
            },
        },
    )
    resp = await web_server.mmv2_state(request=object())
    assert resp["inventory"]["wallet_total_usdc"] == pytest.approx(27.5)
    assert resp["inventory"]["wallet_reserved_usdc"] == pytest.approx(4.5)
    assert resp["inventory"]["pending_buy_reserved_usdc"] == pytest.approx(1.2)


@pytest.mark.asyncio
async def test_state_exposes_pnl_component_fields(monkeypatch):
    web_server = importlib.import_module("web_server")
    monkeypatch.setattr(web_server, "_require_auth", lambda _request: None)
    monkeypatch.setattr(
        web_server._runtime_v2,
        "snapshot",
        lambda: {
            "lifecycle": "quoting",
            "analytics": {
                "session_pnl": 0.42,
                "session_pnl_equity_usd": 0.42,
                "session_pnl_operator_usd": 0.31,
                "session_pnl_operator_ema_usd": 0.31,
                "position_mark_value_usd": 8.15,
                "position_mark_value_bid_usd": 7.95,
                "position_mark_value_mid_usd": 8.15,
                "portfolio_mark_value_usd": 23.15,
                "tradeable_portfolio_value_usd": 21.0,
                "pnl_calc_mode": "wallet_total_plus_mark",
                "pnl_mark_basis": "conservative_bid",
                "pnl_updated_ts": 123456.0,
            },
        },
    )
    resp = await web_server.mmv2_state(request=object())
    assert resp["analytics"]["session_pnl_equity_usd"] == pytest.approx(0.42)
    assert resp["analytics"]["session_pnl_operator_usd"] == pytest.approx(0.31)
    assert resp["analytics"]["session_pnl_operator_ema_usd"] == pytest.approx(0.31)
    assert resp["analytics"]["position_mark_value_usd"] == pytest.approx(8.15)
    assert resp["analytics"]["position_mark_value_bid_usd"] == pytest.approx(7.95)
    assert resp["analytics"]["position_mark_value_mid_usd"] == pytest.approx(8.15)
    assert resp["analytics"]["portfolio_mark_value_usd"] == pytest.approx(23.15)
    assert resp["analytics"]["tradeable_portfolio_value_usd"] == pytest.approx(21.0)
    assert resp["analytics"]["pnl_calc_mode"] == "wallet_total_plus_mark"
    assert resp["analytics"]["pnl_mark_basis"] == "conservative_bid"
    assert resp["analytics"]["pnl_updated_ts"] == pytest.approx(123456.0)


@pytest.mark.asyncio
async def test_state_exposes_sellable_inventory_fields(monkeypatch):
    web_server = importlib.import_module("web_server")
    monkeypatch.setattr(web_server, "_require_auth", lambda _request: None)
    monkeypatch.setattr(
        web_server._runtime_v2,
        "snapshot",
        lambda: {
            "lifecycle": "inventory_skewed",
            "pair_inventory": {
                "sellable_up_shares": 4.25,
                "sellable_dn_shares": 1.75,
            },
        },
    )
    resp = await web_server.mmv2_state(request=object())
    assert resp["pair_inventory"]["sellable_up_shares"] == pytest.approx(4.25)
    assert resp["pair_inventory"]["sellable_dn_shares"] == pytest.approx(1.75)


@pytest.mark.asyncio
async def test_state_exposes_sell_release_lag_fields(monkeypatch):
    web_server = importlib.import_module("web_server")
    monkeypatch.setattr(web_server, "_require_auth", lambda _request: None)
    monkeypatch.setattr(
        web_server._runtime_v2,
        "snapshot",
        lambda: {
            "lifecycle": "defensive",
            "execution": {
                "recent_cancelled_sell_reserve_up": 3.0,
                "recent_cancelled_sell_reserve_dn": 1.5,
                "sell_release_lag_up_sec": 2.2,
                "sell_release_lag_dn_sec": 0.8,
                "up_cooldown_sec": 1.8,
                "dn_cooldown_sec": 0.5,
                "active_sell_release_reason": "both",
                "last_sellability_lag_reason": "batch_place: sellability_lag",
            },
        },
    )
    resp = await web_server.mmv2_state(request=object())
    execution = resp["execution"]
    assert execution["recent_cancelled_sell_reserve_up"] == pytest.approx(3.0)
    assert execution["recent_cancelled_sell_reserve_dn"] == pytest.approx(1.5)
    assert execution["sell_release_lag_up_sec"] == pytest.approx(2.2)
    assert execution["sell_release_lag_dn_sec"] == pytest.approx(0.8)
    assert execution["up_cooldown_sec"] == pytest.approx(1.8)
    assert execution["dn_cooldown_sec"] == pytest.approx(0.5)
    assert execution["active_sell_release_reason"] == "both"
    assert "sellability_lag" in execution["last_sellability_lag_reason"]


@pytest.mark.asyncio
async def test_health_exposes_sellability_lag_active(monkeypatch):
    web_server = importlib.import_module("web_server")
    monkeypatch.setattr(web_server, "_require_auth", lambda _request: None)
    monkeypatch.setattr(
        web_server._runtime_v2,
        "snapshot",
        lambda: {
            "lifecycle": "inventory_skewed",
            "health": {"sellability_lag_active": True},
        },
    )
    resp = await web_server.mmv2_state(request=object())
    assert resp["health"]["sellability_lag_active"] is True


@pytest.mark.asyncio
async def test_state_exposes_drift_evidence(monkeypatch):
    web_server = importlib.import_module("web_server")
    monkeypatch.setattr(web_server, "_require_auth", lambda _request: None)
    monkeypatch.setattr(
        web_server._runtime_v2,
        "snapshot",
        lambda: {
            "lifecycle": "inventory_skewed",
            "health": {
                "true_drift": False,
                "drift_evidence": {
                    "classification": "drift_pending",
                    "candidate_count": 2,
                    "candidate_age_sec": 6.4,
                    "reason": "unexplained drift candidate",
                },
            },
        },
    )
    resp = await web_server.mmv2_state(request=object())
    assert resp["health"]["drift_evidence"]["classification"] == "drift_pending"
    assert resp["health"]["drift_evidence"]["candidate_count"] == 2


@pytest.mark.asyncio
async def test_state_exposes_runtime_terminal_reason(monkeypatch):
    web_server = importlib.import_module("web_server")
    monkeypatch.setattr(web_server, "_require_auth", lambda _request: None)
    monkeypatch.setattr(
        web_server._runtime_v2,
        "snapshot",
        lambda: {
            "lifecycle": "bootstrapping",
            "runtime": {
                "last_terminal_reason": "true inventory drift: no unwind progress",
                "last_terminal_ts": 123456.0,
                "live_budget_gate_passed": True,
                "drawdown_breach_ticks": 4,
                "drawdown_breach_age_sec": 9.5,
            },
        },
    )
    resp = await web_server.mmv2_state(request=object())
    assert resp["runtime"]["last_terminal_reason"] == "true inventory drift: no unwind progress"
    assert resp["runtime"]["last_terminal_ts"] == pytest.approx(123456.0)
    assert resp["runtime"]["live_budget_gate_passed"] is True
    assert resp["runtime"]["drawdown_breach_ticks"] == 4
    assert resp["runtime"]["drawdown_breach_age_sec"] == pytest.approx(9.5)


@pytest.mark.asyncio
async def test_mmv2_state_endpoint_returns_runtime_snapshot(monkeypatch):
    web_server = importlib.import_module("web_server")
    monkeypatch.setattr(web_server, "_require_auth", lambda _request: None)
    monkeypatch.setattr(
        web_server._runtime_v2,
        "snapshot",
        lambda: {"lifecycle": "quoting", "risk": {"soft_mode": "normal"}},
    )
    resp = await web_server.mmv2_state(request=object())
    assert resp["lifecycle"] == "quoting"
    assert resp["risk"]["soft_mode"] == "normal"


@pytest.mark.asyncio
async def test_mmv2_start_ignores_legacy_runtime_flag(monkeypatch):
    web_server = importlib.import_module("web_server")
    monkeypatch.setattr(web_server, "_require_auth", lambda _request: None)
    monkeypatch.setattr(web_server._runtime, "_running", True)
    captured = {}

    async def _fake_start(coin, timeframe, paper_mode, initial_usdc, dev=False, session_budget_usd=None):
        captured.update(
            {
                "coin": coin,
                "timeframe": timeframe,
                "paper_mode": paper_mode,
                "initial_usdc": initial_usdc,
                "dev": dev,
                "session_budget_usd": session_budget_usd,
            }
        )
        return {"ok": True}

    monkeypatch.setattr(web_server._runtime_v2, "start", _fake_start)
    req = web_server.StartRequest(coin="BTC", timeframe="15m", paper_mode=True, initial_usdc=15.0, dev=True)
    resp = await web_server.mmv2_start(req=req, request=object())
    assert resp["ok"] is True
    assert captured["coin"] == "BTC"
    assert captured["timeframe"] == "15m"
    assert captured["session_budget_usd"] == pytest.approx(15.0)


@pytest.mark.asyncio
async def test_mmv2_start_live_uses_config_budget_when_initial_usdc_omitted(monkeypatch):
    web_server = importlib.import_module("web_server")
    monkeypatch.setattr(web_server, "_require_auth", lambda _request: None)
    monkeypatch.setattr(web_server._runtime, "_running", False)
    web_server._runtime_v2.mm_config_v2.session_budget_usd = 75.0
    captured = {}

    async def _fake_start(coin, timeframe, paper_mode, initial_usdc, dev=False, session_budget_usd=None):
        captured.update(
            {
                "coin": coin,
                "timeframe": timeframe,
                "paper_mode": paper_mode,
                "initial_usdc": initial_usdc,
                "dev": dev,
                "session_budget_usd": session_budget_usd,
            }
        )
        return {"ok": True}

    monkeypatch.setattr(web_server._runtime_v2, "start", _fake_start)
    req = web_server.StartRequest(coin="BTC", timeframe="15m", paper_mode=False, dev=True)
    resp = await web_server.mmv2_start(req=req, request=object())
    assert resp["ok"] is True
    assert captured["paper_mode"] is False
    assert captured["initial_usdc"] == pytest.approx(1000.0)
    assert captured["session_budget_usd"] == pytest.approx(75.0)


@pytest.mark.asyncio
async def test_live_start_rejects_budget_below_50(monkeypatch):
    web_server = importlib.import_module("web_server")
    monkeypatch.setattr(web_server, "_require_auth", lambda _request: None)
    monkeypatch.setattr(web_server._runtime, "_running", False)
    web_server._runtime_v2.mm_config_v2.session_budget_usd = 15.0

    req = web_server.StartRequest(coin="BTC", timeframe="15m", paper_mode=False, dev=True)
    with pytest.raises(web_server.HTTPException) as exc:
        await web_server.mmv2_start(req=req, request=object())
    assert exc.value.status_code == 400
    assert "live_min_budget_50_required" in str(exc.value.detail)


def test_runtime_v2_start_accepts_session_budget_kwarg():
    web_server = importlib.import_module("web_server")
    params = inspect.signature(web_server._runtime_v2.start).parameters
    assert "session_budget_usd" in params


@pytest.mark.asyncio
async def test_mmv2_verification_route_delegates_to_runtime(monkeypatch):
    web_server = importlib.import_module("web_server")
    monkeypatch.setattr(web_server, "_require_auth", lambda _request: None)

    async def _fake_start_verification(kind: str):
        return {"running": True, "kind": kind}

    monkeypatch.setattr(web_server._runtime_v2, "start_verification", _fake_start_verification)
    req = web_server.VerificationRunRequest(kind="pytest_v2")
    resp = await web_server.mmv2_verification_run(req=req, request=object())
    assert resp["ok"] is True
    assert resp["verification"]["kind"] == "pytest_v2"


@pytest.mark.asyncio
async def test_dashboard_state_adapts_running_v2_snapshot(monkeypatch):
    web_server = importlib.import_module("web_server")
    monkeypatch.setattr(web_server, "_require_auth", lambda _request: None)
    monkeypatch.setattr(web_server._runtime, "_running", False)
    monkeypatch.setattr(web_server._runtime, "_watching", False)
    monkeypatch.setattr(web_server._runtime, "mm", None)
    monkeypatch.setattr(web_server._runtime_v2, "_running", True)
    monkeypatch.setattr(web_server._runtime_v2, "_coin", "BTC")
    monkeypatch.setattr(web_server._runtime_v2, "_timeframe", "15m")
    monkeypatch.setattr(web_server._runtime_v2, "_paper_mode", True)
    monkeypatch.setattr(web_server._runtime_v2, "_initial_usdc", 50.0)
    monkeypatch.setattr(
        web_server._runtime_v2,
        "fills_page",
        lambda limit=20, offset=0: {
            "fills": [
                {
                    "ts": time.time(),
                    "side": "BUY",
                    "token_type": "up",
                    "price": 0.54,
                    "size": 5.0,
                    "fee": 0.0,
                    "is_maker": True,
                }
            ],
            "total": 1,
        },
    )
    fake_market = SimpleNamespace(
        strike=100000.0,
        up_token_id="up-token",
        dn_token_id="dn-token",
    )
    fake_mm = SimpleNamespace(
        _started_at=time.time() - 10.0,
        market=fake_market,
        heartbeat=SimpleNamespace(stats={"running": True, "heartbeat_count": 3}),
        gateway=SimpleNamespace(
            order_mgr=SimpleNamespace(
                get_active_orders_detail=lambda **_kwargs: [
                    {
                        "order_id": "abc",
                        "side": "BUY",
                        "price": 0.52,
                        "size": 5.0,
                        "notional": 2.6,
                        "token": "UP",
                        "age_sec": 1.0,
                        "type": "quote",
                    }
                ]
            )
        ),
    )
    monkeypatch.setattr(web_server._runtime_v2, "mm_v2", fake_mm)
    monkeypatch.setattr(
        web_server._runtime_v2,
        "snapshot",
        lambda: {
            "app_version": "test",
            "app_git_hash": "deadbeef",
            "is_running": True,
            "lifecycle": "quoting",
            "market": {
                "pm_mid_up": 0.53,
                "pm_mid_dn": 0.47,
                "up_best_bid": 0.52,
                "up_best_ask": 0.54,
                "dn_best_bid": 0.46,
                "dn_best_ask": 0.48,
                "up_bid_depth_usd": 100.0,
                "up_ask_depth_usd": 100.0,
                "dn_bid_depth_usd": 110.0,
                "dn_ask_depth_usd": 90.0,
                "market_quality_score": 0.82,
                "market_tradeable": True,
                "time_left_sec": 321.0,
            },
            "valuation": {"fv_up": 0.54, "fv_dn": 0.46},
            "inventory": {
                "up_shares": 5.0,
                "dn_shares": 1.0,
                "free_usdc": 40.0,
                "reserved_usdc": 5.0,
                "wallet_total_usdc": 45.0,
                "wallet_reserved_usdc": 5.0,
                "pending_buy_reserved_usdc": 2.6,
                "total_inventory_value_usd": 999.0,
            },
            "quotes": {
                "up_bid": {"price": 0.52, "size": 5.0},
                "up_ask": {"price": 0.56, "size": 5.0},
                "dn_bid": {"price": 0.44, "size": 5.0},
                "dn_ask": {"price": 0.48, "size": 5.0},
            },
            "risk": {"hard_mode": "none", "reason": ""},
            "health": {"last_fallback_poll_count": 4},
            "analytics": {
                "session_pnl": 1.23,
                "session_pnl_equity_usd": 1.23,
                "session_pnl_operator_usd": 0.78,
                "fill_count": 1,
                "spread_capture_usd": 0.0,
                "position_mark_value_usd": 3.12,
                "tradeable_portfolio_value_usd": 43.12,
                "portfolio_mark_value_usd": 48.12,
            },
            "alerts": [],
            "config": {"session_budget_usd": 50.0},
        },
    )
    web_server._runtime_v2.feed_state = SimpleNamespace(mid=99999.0)
    resp = web_server._dashboard_snapshot("v2")
    assert resp["dashboard_engine"] == "v2"
    assert resp["market"]["coin"] == "BTC"
    assert resp["market"]["timeframe"] == "15m"
    assert resp["session_limit"] == pytest.approx(50.0)
    assert resp["inventory"]["net_delta"] == pytest.approx(4.0)
    assert resp["active_orders_detail"][0]["token"] == "UP"
    assert resp["usdc_reserved_pm"] == pytest.approx(5.0)
    assert resp["position_value_pm"] == pytest.approx(3.12)
    assert resp["portfolio_value"] == pytest.approx(43.12)
    assert resp["wallet_portfolio_value"] == pytest.approx(48.12)
    assert resp["session_pnl"] == pytest.approx(0.78)
    assert resp["session_pnl_risk_equity"] == pytest.approx(1.23)
    assert resp["fill_count"] == 1


@pytest.mark.asyncio
async def test_legacy_alias_routes_removed(monkeypatch):
    web_server = importlib.import_module("web_server")
    monkeypatch.setattr(web_server, "_require_auth", lambda _request: None)
    with pytest.raises(web_server.HTTPException) as exc:
        await web_server.mm_state(request=SimpleNamespace(query_params={}), response=Response())
    assert exc.value.status_code == 410
    detail = exc.value.detail or {}
    assert detail.get("error") == "legacy_v1_removed_use_mmv2"


@pytest.mark.asyncio
async def test_mmv2_stop_exposes_stop_liquidation_summary(monkeypatch):
    web_server = importlib.import_module("web_server")
    monkeypatch.setattr(web_server, "_require_auth", lambda _request: None)

    class _DummyMMV2:
        async def stop(self, *, liquidate: bool = True):
            assert liquidate is True
            return {
                "enabled": True,
                "attempted_orders": 2,
                "placed_orders": 1,
                "remaining_up": 1.0,
                "remaining_dn": 0.0,
                "done": False,
                "reason": "ok",
            }

    async def _noop():
        return None

    monkeypatch.setattr(web_server._runtime_v2, "_running", True)
    monkeypatch.setattr(web_server._runtime_v2, "mm_v2", _DummyMMV2())
    monkeypatch.setattr(web_server._runtime_v2, "_cancel_monitor_task", _noop)
    monkeypatch.setattr(web_server._runtime_v2, "_cancel_strike_retry_task", _noop)
    monkeypatch.setattr(web_server._runtime_v2, "_stop_feed_tasks", _noop)
    monkeypatch.setattr(web_server._runtime_v2, "snapshot", lambda: {"alerts": []})

    resp = await web_server.mmv2_stop(request=object())
    assert resp["ok"] is True
    assert resp["state"]["stop_liquidation"]["enabled"] is True
    assert resp["state"]["stop_liquidation"]["done"] is False
