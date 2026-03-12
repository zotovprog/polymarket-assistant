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

from mm_v2.config import MMConfigV2
from mm_v2.runtime import MarketMakerV2
from mm_v2.types import PairInventoryState, PairMarketSnapshot
from mm_shared.types import Fill


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
        up_shares=2.0,
        dn_shares=0.0,
        free_usdc=10.0,
        reserved_usdc=5.0,
        pending_buy_up=0.0,
        pending_buy_dn=0.0,
        pending_sell_up=0.0,
        pending_sell_dn=0.0,
        paired_qty=0.0,
        excess_up_qty=2.0,
        excess_dn_qty=0.0,
        paired_value_usd=0.0,
        excess_up_value_usd=1.0,
        excess_dn_value_usd=0.0,
        total_inventory_value_usd=1.0,
        wallet_total_usdc=15.0,
        wallet_reserved_usdc=5.0,
        pending_buy_reserved_usdc=4.5,
    )
    payload.update(overrides)
    return PairInventoryState(**payload)


def test_session_pnl_ignores_pending_orders_without_fills():
    class _MockClient:
        _orders = {}

    mm = MarketMakerV2(SimpleNamespace(), _MockClient(), MMConfigV2())
    mm._starting_portfolio = 16.0
    snap = _snapshot(pm_mid_up=0.5, pm_mid_dn=0.5)

    inv_a = _inventory(
        free_usdc=10.0,
        reserved_usdc=5.0,
        wallet_total_usdc=15.0,
        wallet_reserved_usdc=5.0,
        pending_buy_reserved_usdc=4.5,
    )
    mm._update_session_pnl(inv_a, total_usdc=15.0, snapshot=snap)
    pnl_a = mm._session_pnl

    inv_b = _inventory(
        free_usdc=14.5,
        reserved_usdc=0.5,
        wallet_total_usdc=15.0,
        wallet_reserved_usdc=0.5,
        pending_buy_reserved_usdc=0.4,
    )
    mm._update_session_pnl(inv_b, total_usdc=15.0, snapshot=snap)
    pnl_b = mm._session_pnl

    assert pnl_a == pytest.approx(pnl_b)


def test_tradeable_portfolio_excludes_reserved_usdc(monkeypatch):
    monkeypatch.setenv("PM_WEB_ACCESS_KEY", "test-key")
    web_server = importlib.import_module("web_server")

    monkeypatch.setattr(web_server._runtime_v2, "mm_v2", None)
    monkeypatch.setattr(web_server._runtime_v2, "fills_page", lambda limit=20, offset=0: {"fills": [], "total": 0})
    monkeypatch.setattr(web_server._runtime_v2, "feed_state", SimpleNamespace(mid=100000.0))

    snap = web_server._dashboard_snapshot_from_v2(
        {
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
                "dn_bid_depth_usd": 100.0,
                "dn_ask_depth_usd": 100.0,
                "market_quality_score": 0.9,
                "market_tradeable": True,
                "time_left_sec": 300.0,
            },
            "valuation": {"fv_up": 0.54, "fv_dn": 0.46},
            "inventory": {
                "up_shares": 5.0,
                "dn_shares": 1.0,
                "free_usdc": 40.0,
                "reserved_usdc": 5.0,
                "wallet_total_usdc": 45.0,
                "wallet_reserved_usdc": 5.0,
            },
            "risk": {"hard_mode": "none", "reason": ""},
            "health": {},
            "analytics": {
                "session_pnl": 1.23,
                "position_mark_value_usd": 3.12,
                "tradeable_portfolio_value_usd": 43.12,
                "portfolio_mark_value_usd": 48.12,
            },
            "quotes": {},
            "config": {"session_budget_usd": 50.0},
            "alerts": [],
        }
    )

    assert snap["portfolio_value"] == pytest.approx(43.12)
    assert snap["usdc_reserved_pm"] == pytest.approx(5.0)


def test_wallet_portfolio_includes_reserved_usdc(monkeypatch):
    monkeypatch.setenv("PM_WEB_ACCESS_KEY", "test-key")
    web_server = importlib.import_module("web_server")

    monkeypatch.setattr(web_server._runtime_v2, "mm_v2", None)
    monkeypatch.setattr(web_server._runtime_v2, "fills_page", lambda limit=20, offset=0: {"fills": [], "total": 0})
    monkeypatch.setattr(web_server._runtime_v2, "feed_state", SimpleNamespace(mid=100000.0))

    snap = web_server._dashboard_snapshot_from_v2(
        {
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
                "dn_bid_depth_usd": 100.0,
                "dn_ask_depth_usd": 100.0,
                "market_quality_score": 0.9,
                "market_tradeable": True,
                "time_left_sec": 300.0,
            },
            "valuation": {"fv_up": 0.54, "fv_dn": 0.46},
            "inventory": {
                "up_shares": 5.0,
                "dn_shares": 1.0,
                "free_usdc": 40.0,
                "reserved_usdc": 5.0,
                "wallet_total_usdc": 45.0,
                "wallet_reserved_usdc": 5.0,
            },
            "risk": {"hard_mode": "none", "reason": ""},
            "health": {},
            "analytics": {
                "session_pnl": 1.23,
                "position_mark_value_usd": 3.12,
                "tradeable_portfolio_value_usd": 43.12,
                "portfolio_mark_value_usd": 48.12,
            },
            "quotes": {},
            "config": {"session_budget_usd": 50.0},
            "alerts": [],
        }
    )

    assert snap["wallet_portfolio_value"] == pytest.approx(48.12)
    assert snap["usdc_balance_pm"] == pytest.approx(45.0)


def test_state_exposes_pnl_component_fields():
    analytics = MarketMakerV2(SimpleNamespace(), type("_MockClient", (), {"_orders": {}})(), MMConfigV2())._last_analytics
    assert hasattr(analytics, "position_mark_value_usd")


def test_runtime_tracks_pair_entry_cost_from_buys():
    mm = MarketMakerV2(SimpleNamespace(), type("_MockClient", (), {"_orders": {}})(), MMConfigV2())
    mm._record_fill_entry_metrics(Fill(ts=time.time(), side="BUY", token_id="up-token", price=0.42, size=5.0), "up")
    mm._record_fill_entry_metrics(Fill(ts=time.time(), side="BUY", token_id="dn-token", price=0.67, size=5.0), "dn")

    inventory = _inventory(
        up_shares=5.0,
        dn_shares=5.0,
        paired_qty=5.0,
        total_inventory_value_usd=5.45,
    )
    inventory.pair_entry_cost = mm._avg_entry_price_up + mm._avg_entry_price_dn
    inventory.pair_entry_pnl_per_share = 1.0 - inventory.pair_entry_cost

    analytics = mm._last_analytics
    assert inventory.pair_entry_cost == pytest.approx(1.09)
    assert inventory.pair_entry_pnl_per_share == pytest.approx(-0.09)
    assert hasattr(analytics, "position_mark_value_bid_usd")
    assert hasattr(analytics, "position_mark_value_mid_usd")
    assert hasattr(analytics, "portfolio_mark_value_usd")
    assert hasattr(analytics, "tradeable_portfolio_value_usd")
    assert hasattr(analytics, "session_pnl_equity_usd")
    assert hasattr(analytics, "session_pnl_operator_usd")
    assert hasattr(analytics, "session_pnl_operator_ema_usd")
    assert analytics.pnl_calc_mode == "wallet_total_plus_mark"
    assert analytics.pnl_mark_basis == "conservative_bid"


def test_session_pnl_equity_uses_conservative_bid_mark():
    class _MockClient:
        _orders = {}

    mm = MarketMakerV2(SimpleNamespace(), _MockClient(), MMConfigV2())
    mm._starting_portfolio = 10.0
    inv = _inventory(
        up_shares=2.0,
        dn_shares=0.0,
        free_usdc=8.0,
        reserved_usdc=0.0,
        wallet_total_usdc=8.0,
    )
    snap = _snapshot(
        pm_mid_up=0.9,
        pm_mid_dn=0.1,
        up_best_bid=0.6,
        up_best_ask=0.92,
    )
    mm._update_session_pnl(inv, total_usdc=8.0, snapshot=snap)
    # Equity MTM must use bid: 8 + 2*0.6 = 9.2 => pnl -0.8
    assert mm._session_pnl_equity_usd == pytest.approx(-0.8)


def test_operator_pnl_is_ema_smoothed():
    class _MockClient:
        _orders = {}

    mm = MarketMakerV2(SimpleNamespace(), _MockClient(), MMConfigV2())
    mm._starting_portfolio = 10.0
    inv = _inventory(
        up_shares=1.0,
        dn_shares=0.0,
        free_usdc=9.0,
        reserved_usdc=0.0,
        wallet_total_usdc=9.0,
    )
    snap_a = _snapshot(pm_mid_up=0.9, up_best_bid=0.8)
    snap_b = _snapshot(pm_mid_up=0.2, up_best_bid=0.1)

    mm._update_session_pnl(inv, total_usdc=9.0, snapshot=snap_a)
    first_equity = mm._session_pnl_equity_usd
    first_operator = mm._session_pnl_operator_usd
    assert first_equity == pytest.approx(-0.2)
    assert first_operator == pytest.approx(-0.1)

    mm._update_session_pnl(inv, total_usdc=9.0, snapshot=snap_b)
    second_equity = mm._session_pnl_equity_usd
    second_operator = mm._session_pnl_operator_usd

    assert second_equity != pytest.approx(second_operator)
    assert min(first_operator, second_equity) <= second_operator <= max(first_operator, second_equity)


def test_drawdown_breach_uses_effective_threshold_not_raw_static_value():
    class _MockClient:
        _orders = {}

    cfg = MMConfigV2(session_budget_usd=50.0, hard_drawdown_usd=4.0, hard_drawdown_budget_ratio=0.30)
    mm = MarketMakerV2(SimpleNamespace(), _MockClient(), cfg)

    # Raw threshold would breach at -6, but effective threshold is -15.
    ticks_1, age_1, active_1 = mm._update_drawdown_breach(-6.0)
    assert ticks_1 == 0
    assert age_1 == pytest.approx(0.0)
    assert active_1 is False

    ticks_2, _, _ = mm._update_drawdown_breach(-16.0)
    assert ticks_2 >= 1
