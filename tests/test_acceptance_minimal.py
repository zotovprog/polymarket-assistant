"""Minimal acceptance tests (live-like) for MM safety-critical scenarios.

These tests intentionally run through real runtime components:
- web_server.MockClobClient (paper/live-like behavior),
- MarketMaker + OrderManager paths,
- heartbeat emergency flow.
"""

from __future__ import annotations

import importlib
import os
import sys
import time
import types
from dataclasses import dataclass, field
from unittest.mock import AsyncMock

import pytest


BASE = os.path.dirname(os.path.dirname(__file__))
SRC = os.path.join(BASE, "src")
if SRC not in sys.path:
    sys.path.insert(0, SRC)
if BASE not in sys.path:
    sys.path.insert(0, BASE)

import mm.market_maker as market_maker_mod
import mm.order_manager as order_manager_mod
from mm.market_maker import MarketMaker
from mm.mm_config import MMConfig
from mm.order_manager import OrderManager
from mm.types import Inventory, MarketInfo, Quote


def _web_server_module():
    if "aiohttp" not in sys.modules:
        sys.modules["aiohttp"] = types.ModuleType("aiohttp")
    return importlib.import_module("web_server")


class _Level:
    def __init__(self, price: float):
        self.price = str(price)


class _Book:
    def __init__(self, *, bid: float, ask: float):
        self.bids = [_Level(bid)]
        self.asks = [_Level(ask)]


class _LiveNoopClient:
    """Live-like client (no _orders attribute) for reconcile/error paths."""

    def create_order(self, order_args):
        return {
            "token_id": order_args.token_id,
            "price": order_args.price,
            "size": order_args.size,
            "side": order_args.side,
        }

    def post_order(self, *_args, **_kwargs):
        return {"orderID": "live-001"}

    def cancel_all(self):
        return {"success": True}

    def get_order(self, _oid):
        return {"status": "LIVE", "size_matched": "0"}

    def get_order_book(self, _token_id):
        return _Book(bid=0.49, ask=0.51)

    def post_heartbeat(self, _heartbeat_id):
        return {"success": True}


class _DummyOrderType:
    GTC = "GTC"
    GTD = "GTD"


class _DummyOrderArgs:
    def __init__(self, token_id: str, price: float, size: float, side: str):
        self.token_id = token_id
        self.price = price
        self.size = size
        self.side = side
        self.expiration = None


class _LiveCrossBookRejectClient:
    """Live-like client: batch post-only rejects, single taker succeeds."""

    def __init__(self):
        self.single_post_calls = 0

    def create_order(self, order_args):
        return {
            "token_id": order_args.token_id,
            "price": order_args.price,
            "size": order_args.size,
            "side": order_args.side,
        }

    def post_orders(self, signed_orders):
        return {
            "orders": [
                {
                    "errorMsg": "invalid post-only order: order crosses book",
                    "status": "",
                    "orderID": "",
                }
                for _ in signed_orders
            ]
        }

    def post_order(self, *_args, **_kwargs):
        self.single_post_calls += 1
        return {"orderID": f"taker-{self.single_post_calls}"}

    def get_order_book(self, _token_id):
        return _Book(bid=0.50, ask=0.51)


@dataclass
class _FeedState:
    mid: float = 100000.0
    bids: list = field(default_factory=list)
    asks: list = field(default_factory=list)
    trades: list = field(default_factory=list)
    klines: list = field(default_factory=list)
    pm_up: float = 0.5
    pm_dn: float = 0.5
    pm_up_bid: float = 0.5
    pm_dn_bid: float = 0.5
    pm_last_update_ts: float = field(default_factory=time.time)
    binance_ob_last_ok_ts: float = field(default_factory=time.time)
    binance_ws_last_ok_ts: float = field(default_factory=time.time)
    pm_connected: bool = False


def _market(*, window_end_offset_sec: float = 900.0) -> MarketInfo:
    now = time.time()
    return MarketInfo(
        coin="BTC",
        timeframe="15m",
        up_token_id="up_token_123",
        dn_token_id="dn_token_456",
        strike=100000.0,
        window_start=now - 60.0,
        window_end=now + window_end_offset_sec,
        condition_id="cond_789",
        min_order_size=5.0,
        tick_size=0.01,
    )


def _make_mock_client(*, fill_prob: float, usdc_balance: float):
    web_server = _web_server_module()
    return web_server.MockClobClient(fill_prob=fill_prob, usdc_balance=usdc_balance)


def _make_mm(client, *, window_end_offset_sec: float = 900.0) -> MarketMaker:
    mm = MarketMaker(_FeedState(), client, MMConfig())
    mm.set_market(_market(window_end_offset_sec=window_end_offset_sec))
    return mm


@pytest.mark.anyio
async def test_acceptance_smoke_single_trade_one_usdc_roundtrip_zero_pnl(monkeypatch):
    web_server = _web_server_module()
    # Deterministic full fills for this acceptance case.
    monkeypatch.setattr(web_server.random, "random", lambda: 0.0)
    monkeypatch.setattr(web_server.random, "uniform", lambda _a, _b: 1.0)

    client = _make_mock_client(fill_prob=1.0, usdc_balance=20.0)
    market = _market()
    client.set_fair_values(0.5, 0.5, market, pm_prices={"up": 0.5, "dn": 0.5})

    om = OrderManager(client, MMConfig())
    inv = Inventory(usdc=20.0, initial_usdc=20.0)
    up_token = market.up_token_id
    dn_token = market.dn_token_id

    # BUY $1 UP + $1 DN at 0.50.
    up_buy = Quote(side="BUY", token_id=up_token, price=0.50, size=2.0)
    dn_buy = Quote(side="BUY", token_id=dn_token, price=0.50, size=2.0)
    up_buy_oid = await om.place_order(up_buy)
    dn_buy_oid = await om.place_order(dn_buy)
    assert up_buy_oid and dn_buy_oid

    om._last_fill_check_ts = 0.0
    om._active_orders[up_buy_oid].placed_at = time.time() - 31.0
    om._active_orders[dn_buy_oid].placed_at = time.time() - 31.0
    buy_fills = await om.check_fills()
    assert len(buy_fills) == 2
    for fill in buy_fills:
        token_type = "up" if fill.token_id == up_token else "dn"
        inv.update_from_fill(fill, token_type)

    # Close both legs symmetrically.
    up_sell = Quote(side="SELL", token_id=up_token, price=0.50, size=2.0)
    dn_sell = Quote(side="SELL", token_id=dn_token, price=0.50, size=2.0)
    up_sell_oid = await om.place_order(up_sell)
    dn_sell_oid = await om.place_order(dn_sell)
    assert up_sell_oid and dn_sell_oid

    om._last_fill_check_ts = 0.0
    om._active_orders[up_sell_oid].placed_at = time.time() - 31.0
    om._active_orders[dn_sell_oid].placed_at = time.time() - 31.0
    sell_fills = await om.check_fills()
    assert len(sell_fills) == 2
    for fill in sell_fills:
        token_type = "up" if fill.token_id == up_token else "dn"
        inv.update_from_fill(fill, token_type)

    assert inv.up_shares == pytest.approx(0.0)
    assert inv.dn_shares == pytest.approx(0.0)
    assert inv.usdc == pytest.approx(inv.initial_usdc, abs=1e-6)


@pytest.mark.anyio
async def test_acceptance_post_only_reject_retries_as_taker(monkeypatch):
    monkeypatch.setattr(order_manager_mod, "_HAS_CLOB_TYPES", True)
    monkeypatch.setattr(order_manager_mod, "OrderType", _DummyOrderType, raising=False)
    monkeypatch.setattr(order_manager_mod, "OrderArgs", _DummyOrderArgs, raising=False)

    om = OrderManager(_LiveCrossBookRejectClient(), MMConfig())
    q = Quote(side="BUY", token_id="up_token_123", price=0.51, size=5.0)
    result = await om.place_orders_batch([q], post_only=True)

    assert result[0] is not None
    assert result[0].startswith("taker-")


@pytest.mark.anyio
async def test_acceptance_reconcile_restores_internal_state_to_pm_balances():
    mm = _make_mm(_LiveNoopClient())
    mm._running = True
    mm.feed_state.mid = 0.0  # stop after reconcile section
    mm.inventory.up_shares = 0.0
    mm.inventory.dn_shares = 0.0
    mm.inventory.usdc = 0.0

    mm.order_mgr._reconcile_requested = True
    mm.order_mgr.check_fills = AsyncMock(return_value=[])
    mm.order_mgr.get_all_token_balances = AsyncMock(return_value=(7.0, 3.0))
    mm.order_mgr.get_usdc_balances = AsyncMock(return_value=(19.0, 19.0))

    await mm._tick()

    assert mm.inventory.up_shares == pytest.approx(7.0)
    assert mm.inventory.dn_shares == pytest.approx(3.0)
    assert mm.inventory.usdc == pytest.approx(19.0)
    assert mm.order_mgr.reconcile_requested is False


@pytest.mark.anyio
async def test_acceptance_expired_window_cancels_orders_and_merges_pairs(monkeypatch):
    async def _noop_sleep(_seconds: float):
        return None

    monkeypatch.setattr(market_maker_mod.asyncio, "sleep", _noop_sleep)

    client = _make_mock_client(fill_prob=0.2, usdc_balance=20.0)
    mm = _make_mm(client, window_end_offset_sec=-1.0)
    mm._running = True
    mm.inventory.up_shares = 3.0
    mm.inventory.dn_shares = 3.0
    mm.inventory.usdc = 20.0
    mm.inventory.up_cost.total_shares = 3.0
    mm.inventory.up_cost.total_cost = 1.5
    mm.inventory.dn_cost.total_shares = 3.0
    mm.inventory.dn_cost.total_cost = 1.5
    mm.order_mgr._mock_token_balances[mm.market.up_token_id] = 3.0
    mm.order_mgr._mock_token_balances[mm.market.dn_token_id] = 3.0

    await mm._tick()

    assert mm._running is False
    assert mm.order_mgr.active_order_ids == []
    assert mm.inventory.up_shares == pytest.approx(0.0)
    assert mm.inventory.dn_shares == pytest.approx(0.0)
    assert mm.order_mgr._mock_token_balances[mm.market.up_token_id] == pytest.approx(0.0)
    assert mm.order_mgr._mock_token_balances[mm.market.dn_token_id] == pytest.approx(0.0)
    assert mm.inventory.usdc == pytest.approx(23.0)
    assert mm._current_quotes == {"up": (None, None), "dn": (None, None)}


@pytest.mark.anyio
async def test_acceptance_three_heartbeat_failures_trigger_emergency_shutdown():
    client = _make_mock_client(fill_prob=0.2, usdc_balance=25.0)

    def _fail_heartbeat():
        raise RuntimeError("heartbeat network down")

    client.post_heartbeat = _fail_heartbeat  # type: ignore[assignment]

    mm = _make_mm(client)
    mm._running = True
    live_bid = Quote(
        side="BUY",
        token_id=mm.market.up_token_id,
        price=0.50,
        size=5.0,
        order_id="oid-1",
    )
    mm.order_mgr._active_orders = {"oid-1": live_bid}
    mm._current_quotes = {"up": (live_bid, None), "dn": (None, None)}
    mm.order_mgr.cancel_all = AsyncMock(return_value=1)
    mm.heartbeat.stop = AsyncMock(return_value=None)

    for _ in range(3):
        ok = await mm.heartbeat._send_heartbeat()
        assert ok is False

    assert mm._heartbeat_failure_task is not None
    await mm._heartbeat_failure_task

    mm.order_mgr.cancel_all.assert_awaited_once()
    mm.heartbeat.stop.assert_awaited_once()
    assert mm._running is False
    assert mm._paused is True
    assert "Heartbeat failed 3 consecutive times" in mm._pause_reason
    assert mm.order_mgr.active_order_ids == []
    assert mm._current_quotes == {"up": (None, None), "dn": (None, None)}


@pytest.mark.anyio
async def test_acceptance_low_balance_session_cap_limits_buys():
    client = _make_mock_client(fill_prob=0.2, usdc_balance=15.0)
    mm = _make_mm(client)
    mm.inventory.initial_usdc = 15.0  # requested session cap

    async def _noop_run_loop():
        return None

    mm._run_loop = _noop_run_loop
    await mm.start()
    if mm._task:
        await mm._task
    await mm.heartbeat.stop()
    mm._running = False

    assert mm.order_mgr._session_budget == pytest.approx(15.0)
    assert mm._starting_usdc_pm == pytest.approx(15.0)

    q1 = Quote(side="BUY", token_id=mm.market.up_token_id, price=0.50, size=20.0)  # $10
    q2 = Quote(side="BUY", token_id=mm.market.dn_token_id, price=0.50, size=20.0)  # $10 -> clamp to $5
    q3 = Quote(side="BUY", token_id=mm.market.up_token_id, price=0.50, size=20.0)  # reject

    oid1 = await mm.order_mgr.place_order(q1)
    oid2 = await mm.order_mgr.place_order(q2)
    oid3 = await mm.order_mgr.place_order(q3)

    assert oid1 is not None
    assert oid2 is not None
    assert oid3 is None

    active_buy_notional = sum(
        mm.order_mgr.required_collateral(q)
        for q in mm.order_mgr.active_orders.values()
        if q.side == "BUY"
    )
    assert active_buy_notional <= 15.01


@pytest.mark.anyio
async def test_acceptance_api_balance_error_safe_cancels_open_orders():
    mm = _make_mm(_LiveNoopClient())
    mm._running = True
    mm._tick_count = 14  # next tick triggers periodic reconcile (%15 == 0)

    live_bid = Quote(
        side="BUY",
        token_id=mm.market.up_token_id,
        price=0.49,
        size=5.0,
        order_id="oid-1",
    )
    mm.order_mgr._active_orders = {"oid-1": live_bid}
    mm._current_quotes = {"up": (live_bid, None), "dn": (None, None)}
    mm.order_mgr.check_fills = AsyncMock(return_value=[])
    mm.order_mgr.get_all_token_balances = AsyncMock(side_effect=RuntimeError("balance api down"))
    mm.order_mgr.get_usdc_balances = AsyncMock(return_value=(100.0, 100.0))

    async def _cancel_and_clear(*_args, **_kwargs):
        mm.order_mgr.clear_local_order_tracking()
        return 1

    mm.order_mgr.cancel_all = AsyncMock(side_effect=_cancel_and_clear)

    await mm._tick()

    mm.order_mgr.cancel_all.assert_awaited_once()
    assert mm.order_mgr.active_order_ids == []
    assert mm._current_quotes == {"up": (None, None), "dn": (None, None)}


@pytest.mark.anyio
async def test_acceptance_api_orderbook_error_safe_cancels_open_orders():
    mm = _make_mm(_LiveNoopClient())
    mm._running = True

    live_bid = Quote(
        side="BUY",
        token_id=mm.market.up_token_id,
        price=0.49,
        size=5.0,
        order_id="oid-1",
    )
    mm.order_mgr._active_orders = {"oid-1": live_bid}
    mm._current_quotes = {"up": (live_bid, None), "dn": (None, None)}
    mm.order_mgr.check_fills = AsyncMock(return_value=[])
    mm.order_mgr.get_book_summary = AsyncMock(side_effect=RuntimeError("book api down"))

    async def _cancel_and_clear(*_args, **_kwargs):
        mm.order_mgr.clear_local_order_tracking()
        return 1

    mm.order_mgr.cancel_all = AsyncMock(side_effect=_cancel_and_clear)

    await mm._tick()

    mm.order_mgr.cancel_all.assert_awaited_once()
    assert mm.order_mgr.active_order_ids == []
    assert mm._current_quotes == {"up": (None, None), "dn": (None, None)}
