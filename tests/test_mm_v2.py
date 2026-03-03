from __future__ import annotations

import importlib
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

from mm.types import Fill, MarketInfo, Quote
from mm_v2.config import MMConfigV2
from mm_v2.pair_inventory import build_pair_inventory
from mm_v2.quote_policy import QuoteContext, QuotePolicyV2
from mm_v2.reconcile import ReconcileV2
from mm_v2.risk_kernel import HardSafetyKernel
from mm_v2.state_machine import StateMachineV2
from mm_v2.types import AnalyticsState, HealthState, PairInventoryState, PairMarketSnapshot


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
    )
    payload.update(overrides)
    return PairInventoryState(**payload)


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


def test_hard_excess_transitions_state_machine_to_unwind():
    cfg = MMConfigV2(session_budget_usd=15.0, hard_excess_value_ratio=0.25)
    inventory = _inventory(
        up_shares=12.0,
        dn_shares=1.0,
        paired_qty=1.0,
        excess_up_qty=11.0,
        paired_value_usd=1.0,
        excess_up_value_usd=6.16,
        total_inventory_value_usd=6.62,
    )
    risk = HardSafetyKernel(cfg).evaluate(
        snapshot=_snapshot(),
        inventory=inventory,
        analytics=AnalyticsState(),
        health=HealthState(),
    )
    lifecycle = StateMachineV2(cfg).transition(
        snapshot=_snapshot(),
        inventory=inventory,
        risk=risk,
    )
    assert risk.soft_mode == "unwind"
    assert lifecycle == "unwind"


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
async def test_mmv2_start_rejects_when_legacy_runtime_is_running(monkeypatch):
    web_server = importlib.import_module("web_server")
    monkeypatch.setattr(web_server, "_require_auth", lambda _request: None)
    monkeypatch.setattr(web_server._runtime, "_running", True)
    req = web_server.StartRequest(coin="BTC", timeframe="15m", paper_mode=True, initial_usdc=15.0, dev=True)
    with pytest.raises(web_server.HTTPException) as exc:
        await web_server.mmv2_start(req=req, request=object())
    assert exc.value.status_code == 409
    assert "Legacy MM" in str(exc.value.detail)


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
                "total_inventory_value_usd": 3.12,
            },
            "quotes": {
                "up_bid": {"price": 0.52, "size": 5.0},
                "up_ask": {"price": 0.56, "size": 5.0},
                "dn_bid": {"price": 0.44, "size": 5.0},
                "dn_ask": {"price": 0.48, "size": 5.0},
            },
            "risk": {"hard_mode": "none", "reason": ""},
            "health": {"last_fallback_poll_count": 4},
            "analytics": {"session_pnl": 1.23, "fill_count": 1, "spread_capture_usd": 0.0},
            "alerts": [],
            "config": {"session_budget_usd": 50.0},
        },
    )
    web_server._runtime_v2.feed_state = SimpleNamespace(mid=99999.0)
    req = SimpleNamespace(query_params={})
    resp = await web_server.mm_state(request=req)
    assert resp["dashboard_engine"] == "v2"
    assert resp["market"]["coin"] == "BTC"
    assert resp["market"]["timeframe"] == "15m"
    assert resp["session_limit"] == pytest.approx(50.0)
    assert resp["inventory"]["net_delta"] == pytest.approx(4.0)
    assert resp["active_orders_detail"][0]["token"] == "UP"
    assert resp["fill_count"] == 1
