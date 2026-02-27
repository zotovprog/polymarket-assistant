"""Targeted regressions for batch reject diagnostics and runtime sync guard."""

from __future__ import annotations

import asyncio
import importlib
import logging
import os
import sys
import time
from unittest.mock import AsyncMock

import pytest


BASE = os.path.dirname(os.path.dirname(__file__))
SRC = os.path.join(BASE, "src")
if SRC not in sys.path:
    sys.path.insert(0, SRC)
if BASE not in sys.path:
    sys.path.insert(0, BASE)

from mm.mm_config import MMConfig
from mm.types import Quote
import mm.order_manager as order_manager_mod


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


class _LiveRejectClient:
    """Minimal live-like client with batch rejects."""

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
                    "status": "rejected",
                    "errorMsg": "price outside bounds",
                    "code": "REJECTED",
                }
                for _ in signed_orders
            ]
        }


class _Level:
    def __init__(self, price: float):
        self.price = str(price)


class _Book:
    def __init__(self, *, bid: float, ask: float):
        self.bids = [_Level(bid)]
        self.asks = [_Level(ask)]


class _LiveCrossBookRejectClient:
    """Live-like client: batch post-only rejects, single taker succeeds."""

    def __init__(self, *, bid: float = 0.50, ask: float = 0.51):
        self.bid = bid
        self.ask = ask
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
        return _Book(bid=self.bid, ask=self.ask)


class _LiveBatchCaptureClient:
    """Live-like client that tracks whether create_order was reached."""

    def __init__(self):
        self.create_calls = 0

    def create_order(self, order_args):
        self.create_calls += 1
        return {
            "token_id": order_args.token_id,
            "price": order_args.price,
            "size": order_args.size,
            "side": order_args.side,
        }

    def post_orders(self, signed_orders):
        return {"orders": [{"orderID": "oid-1"} for _ in signed_orders]}


class _LiveBalanceRejectClient:
    """Live-like client returning balance/allowance rejects."""

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
                    "status": "rejected",
                    "errorMsg": "not enough balance / allowance",
                }
                for _ in signed_orders
            ]
        }


class _LiveSingleSuccessClient:
    """Live-like client for successful single-order placement."""

    def __init__(self):
        self.post_count = 0

    def create_order(self, order_args):
        return {
            "token_id": order_args.token_id,
            "price": order_args.price,
            "size": order_args.size,
            "side": order_args.side,
        }

    def post_order(self, *_args, **_kwargs):
        self.post_count += 1
        return {"orderID": f"oid-{self.post_count}"}


class _DummyAssetType:
    CONDITIONAL = "CONDITIONAL"


class _DummyBalanceAllowanceParams:
    def __init__(self, asset_type=None, token_id=None):
        self.asset_type = asset_type
        self.token_id = token_id


class _AllowanceClient:
    def __init__(self):
        self.get_calls = 0
        self.update_calls = 0

    def get_balance_allowance(self, _params):
        self.get_calls += 1
        return {"allowance": 0, "balance": 5_290_000}

    def update_balance_allowance(self, _params):
        self.update_calls += 1
        return {"success": True}


class _LateFillClient:
    """Live-like client for late fill reconciliation on recently cancelled orders."""

    def __init__(self):
        self.get_order_calls = 0

    def cancel_all(self):
        return {"success": True}

    def get_order(self, _oid):
        self.get_order_calls += 1
        return {"status": "MATCHED", "size_matched": "10", "price": "0.50"}


def test_place_orders_batch_logs_raw_reject(monkeypatch, caplog):
    monkeypatch.setattr(order_manager_mod, "_HAS_CLOB_TYPES", True)
    monkeypatch.setattr(order_manager_mod, "OrderType", _DummyOrderType, raising=False)
    monkeypatch.setattr(order_manager_mod, "OrderArgs", _DummyOrderArgs, raising=False)

    om = order_manager_mod.OrderManager(_LiveRejectClient(), MMConfig())
    quote = Quote(side="BUY", token_id="up_tok_123", price=0.51, size=10.0)

    caplog.set_level(logging.ERROR, logger="mm.orders")
    result = asyncio.run(om.place_orders_batch([quote], post_only=True))

    assert result == [None]
    messages = "\n".join(rec.getMessage() for rec in caplog.records)
    assert "Batch reject BUY" in messages
    assert "price outside bounds" in messages
    assert "raw=" in messages


def test_place_orders_batch_crosses_book_stays_maker_only(monkeypatch):
    monkeypatch.setattr(order_manager_mod, "_HAS_CLOB_TYPES", True)
    monkeypatch.setattr(order_manager_mod, "OrderType", _DummyOrderType, raising=False)
    monkeypatch.setattr(order_manager_mod, "OrderArgs", _DummyOrderArgs, raising=False)

    client = _LiveCrossBookRejectClient(bid=0.50, ask=0.51)
    om = order_manager_mod.OrderManager(client, MMConfig())
    quote = Quote(side="BUY", token_id="up_tok_123", price=0.51, size=5.0)

    result = asyncio.run(om.place_orders_batch([quote], post_only=True))

    assert result == [None]
    assert client.single_post_calls == 0


def test_place_orders_batch_crosses_book_retry_blocked_by_price_guard(monkeypatch):
    monkeypatch.setattr(order_manager_mod, "_HAS_CLOB_TYPES", True)
    monkeypatch.setattr(order_manager_mod, "OrderType", _DummyOrderType, raising=False)
    monkeypatch.setattr(order_manager_mod, "OrderArgs", _DummyOrderArgs, raising=False)

    client = _LiveCrossBookRejectClient(bid=0.49, ask=0.50)
    cfg = MMConfig()
    cfg.requote_threshold_bps = 20.0  # strict guard for this test
    om = order_manager_mod.OrderManager(client, cfg)
    quote = Quote(side="BUY", token_id="up_tok_123", price=0.70, size=5.0)

    result = asyncio.run(om.place_orders_batch([quote], post_only=True))

    assert result == [None]
    assert client.single_post_calls == 0


@pytest.mark.anyio
async def test_ensure_sell_allowance_uses_cached_cap_for_equal_required_size(monkeypatch):
    monkeypatch.setattr(order_manager_mod, "_HAS_CLOB_TYPES", True)
    monkeypatch.setattr(order_manager_mod, "AssetType", _DummyAssetType, raising=False)
    monkeypatch.setattr(
        order_manager_mod,
        "BalanceAllowanceParams",
        _DummyBalanceAllowanceParams,
        raising=False,
    )

    client = _AllowanceClient()
    om = order_manager_mod.OrderManager(client, MMConfig())

    ok_first = await om.ensure_sell_allowance("tok_123", required_shares=5.29)
    ok_second = await om.ensure_sell_allowance("tok_123", required_shares=5.29)

    assert ok_first is True
    assert ok_second is True
    assert client.update_calls == 1
    assert client.get_calls == 1


@pytest.mark.anyio
async def test_place_order_blocks_naked_sell_in_close_only_mode():
    om = order_manager_mod.OrderManager(object(), MMConfig())
    om.ensure_sell_allowance = AsyncMock(return_value=True)
    om.get_token_balance = AsyncMock(return_value=0.0)
    om._place_order_inner = AsyncMock(return_value="should-not-place")

    quote = Quote(side="SELL", token_id="dn_tok_123", price=0.80, size=8.0)
    oid = await om.place_order(quote)

    assert oid is None
    om._place_order_inner.assert_not_awaited()


@pytest.mark.anyio
async def test_place_order_trims_close_only_sell_to_available_inventory():
    om = order_manager_mod.OrderManager(object(), MMConfig())
    om.ensure_sell_allowance = AsyncMock(return_value=True)
    om.get_token_balance = AsyncMock(return_value=6.567)
    om._place_order_inner = AsyncMock(return_value="oid-trim")

    quote = Quote(side="SELL", token_id="dn_tok_123", price=0.80, size=8.0)
    oid = await om.place_order(quote)

    assert oid == "oid-trim"
    assert quote.size == pytest.approx(6.56)
    om._place_order_inner.assert_awaited_once()


@pytest.mark.anyio
async def test_place_order_buy_balance_retry_skips_when_below_pm_min():
    om = order_manager_mod.OrderManager(object(), MMConfig())
    om.get_usdc_available_balance = AsyncMock(side_effect=[100.0, 2.4])
    om._place_order_inner = AsyncMock(
        side_effect=[RuntimeError("not enough balance"), "should-not-run"],
    )

    quote = Quote(side="BUY", token_id="up_tok_123", price=0.50, size=10.0)
    oid = await om.place_order(quote)

    assert oid is None
    assert om._place_order_inner.await_count == 1
    assert om.reconcile_requested is True
    assert quote.size == pytest.approx(10.0)


@pytest.mark.anyio
async def test_place_order_buy_balance_retry_recomputes_size_and_retries():
    om = order_manager_mod.OrderManager(object(), MMConfig())
    om.get_usdc_available_balance = AsyncMock(side_effect=[100.0, 3.2])
    om._place_order_inner = AsyncMock(
        side_effect=[RuntimeError("not enough balance"), "oid-buy-retry"],
    )

    quote = Quote(side="BUY", token_id="up_tok_123", price=0.50, size=10.0)
    oid = await om.place_order(quote)

    assert oid == "oid-buy-retry"
    assert om._place_order_inner.await_count == 2
    assert quote.size == pytest.approx(6.4)
    assert quote.size >= 5.0
    assert om.reconcile_requested is True


def test_place_orders_batch_blocks_naked_sell_in_close_only_mode(monkeypatch):
    monkeypatch.setattr(order_manager_mod, "_HAS_CLOB_TYPES", True)
    monkeypatch.setattr(order_manager_mod, "OrderType", _DummyOrderType, raising=False)
    monkeypatch.setattr(order_manager_mod, "OrderArgs", _DummyOrderArgs, raising=False)

    client = _LiveBatchCaptureClient()
    om = order_manager_mod.OrderManager(client, MMConfig())
    om.ensure_sell_allowance = AsyncMock(return_value=True)
    om.get_token_balance = AsyncMock(return_value=0.0)

    quote = Quote(side="SELL", token_id="dn_tok_123", price=0.80, size=8.0)
    result = asyncio.run(om.place_orders_batch([quote], post_only=True))

    assert result == [None]
    assert client.create_calls == 0


def test_balance_allowance_batch_reject_requests_reconcile_and_sell_cooldown(monkeypatch):
    monkeypatch.setattr(order_manager_mod, "_HAS_CLOB_TYPES", True)
    monkeypatch.setattr(order_manager_mod, "OrderType", _DummyOrderType, raising=False)
    monkeypatch.setattr(order_manager_mod, "OrderArgs", _DummyOrderArgs, raising=False)

    token_id = "dn_tok_balance_reject"
    om = order_manager_mod.OrderManager(_LiveBalanceRejectClient(), MMConfig())
    om.ensure_sell_allowance = AsyncMock(return_value=True)
    om.get_token_balance = AsyncMock(return_value=10.0)
    om._allowance_set.add(token_id)
    om._allowance_cap_shares[token_id] = 25.0

    q1 = Quote(side="SELL", token_id=token_id, price=0.80, size=6.0)
    result = asyncio.run(om.place_orders_batch([q1], post_only=True))

    assert result == [None]
    assert om.reconcile_requested is True
    assert token_id in om._sell_reject_cooldown_until
    assert token_id not in om._allowance_set
    assert token_id not in om._allowance_cap_shares

    om._place_order_inner = AsyncMock(return_value="should-not-run")
    q2 = Quote(side="SELL", token_id=token_id, price=0.81, size=6.0)
    oid = asyncio.run(om.place_order(q2))
    assert oid is None
    om._place_order_inner.assert_not_awaited()


@pytest.mark.anyio
async def test_sell_cooldown_expires_and_single_place_resumes(monkeypatch):
    monkeypatch.setattr(order_manager_mod, "_HAS_CLOB_TYPES", True)
    monkeypatch.setattr(order_manager_mod, "OrderType", _DummyOrderType, raising=False)
    monkeypatch.setattr(order_manager_mod, "OrderArgs", _DummyOrderArgs, raising=False)

    token_id = "dn_tok_resume"
    client = _LiveSingleSuccessClient()
    om = order_manager_mod.OrderManager(client, MMConfig())
    om.ensure_sell_allowance = AsyncMock(return_value=True)
    om.get_token_balance = AsyncMock(return_value=10.0)
    om._sell_reject_cooldown_until[token_id] = time.time() - 1.0

    q = Quote(side="SELL", token_id=token_id, price=0.80, size=6.0)
    oid = await om.place_order(q)

    assert oid is not None
    assert oid.startswith("oid-")
    assert token_id not in om._sell_reject_cooldown_until


@pytest.mark.anyio
async def test_check_fills_reconciles_late_fill_after_cancel_all():
    om = order_manager_mod.OrderManager(_LateFillClient(), MMConfig())
    oid = "oid-late-fill"
    quote = Quote(side="BUY", token_id="up_tok_123", price=0.50, size=10.0, order_id=oid)
    quote.placed_at = 1.0
    om._active_orders[oid] = quote
    om._order_post_only[oid] = True

    cancelled = await om.cancel_all()
    assert cancelled == 1
    assert oid in om._recent_orders
    assert oid not in om._active_orders

    fills = await om.check_fills()
    assert len(fills) == 1
    assert fills[0].order_id == oid
    assert fills[0].side == "BUY"
    assert fills[0].size == pytest.approx(10.0)
    assert oid not in om._recent_orders


@pytest.mark.anyio
async def test_monitor_syncs_runtime_when_mm_stops_unexpectedly(monkeypatch):
    if "aiohttp" not in sys.modules:
        import types

        sys.modules["aiohttp"] = types.ModuleType("aiohttp")

    web_server = importlib.import_module("web_server")
    runtime = web_server.MMRuntime()

    class _Heartbeat:
        is_running = False

        async def stop(self):
            self.is_running = False

    class _Market:
        time_remaining = 300.0

    class _StoppedMM:
        _running = False
        _is_closing = False
        _emergency_stopped = False
        market = _Market()
        heartbeat = _Heartbeat()

    runtime.mm = _StoppedMM()
    runtime._running = True
    runtime._cancel_strike_retry_task = AsyncMock()
    runtime._stop_feed_tasks = AsyncMock()

    async def _instant_sleep(_seconds: float):
        return None

    monkeypatch.setattr(web_server.asyncio, "sleep", _instant_sleep)

    await runtime._monitor_window_expiry()

    assert runtime._running is False
    runtime._cancel_strike_retry_task.assert_awaited_once()
    runtime._stop_feed_tasks.assert_awaited_once()


@pytest.mark.anyio
async def test_mm_emergency_uses_runtime_stop(monkeypatch):
    if "aiohttp" not in sys.modules:
        import types

        sys.modules["aiohttp"] = types.ModuleType("aiohttp")

    web_server = importlib.import_module("web_server")
    called: dict[str, bool] = {}

    async def _fake_stop(*, liquidate: bool = True, emergency: bool = False):
        called["liquidate"] = liquidate
        called["emergency"] = emergency
        return {"is_running": False}

    monkeypatch.setattr(web_server, "_require_auth", lambda _request: None)
    monkeypatch.setattr(web_server._runtime, "stop", _fake_stop)
    web_server._runtime.mm = None

    resp = await web_server.mm_emergency(request=object())

    assert resp["ok"] is True
    assert resp["cancelled"] == 0
    assert called == {"liquidate": False, "emergency": True}
