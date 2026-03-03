"""Targeted regressions for batch reject diagnostics and runtime sync guard."""

from __future__ import annotations

import asyncio
import contextlib
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


class _OrderArgsWithFee:
    def __init__(
        self,
        token_id: str,
        price: float,
        size: float,
        side: str,
        fee_rate_bps: int = 0,
    ):
        self.token_id = token_id
        self.price = price
        self.size = size
        self.side = side
        self.fee_rate_bps = fee_rate_bps
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


class _LiveFeeCaptureClient:
    """Live-like client capturing OrderArgs used for signing."""

    def __init__(self):
        self.last_order_args = None
        self.post_count = 0

    def create_order(self, order_args):
        self.last_order_args = order_args
        return {
            "token_id": order_args.token_id,
            "price": order_args.price,
            "size": order_args.size,
            "side": order_args.side,
        }

    def post_order(self, *_args, **_kwargs):
        self.post_count += 1
        return {"orderID": f"fee-oid-{self.post_count}"}

    def get_balance_allowance(self, _params):
        return {"balance": 100_000_000, "allowance": 100_000_000}


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


class _CancelAllFailClient:
    def cancel_all(self):
        raise RuntimeError("cancel_all boom")


class _FallbackPollErrorClient:
    def get_order(self, _oid):
        raise RuntimeError("fallback poll boom")


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
    stats = om.get_api_error_stats()
    assert stats["total_by_op"]["place_batch_item"] == 1
    assert stats["recent"][-1]["token_id"] == "up_tok_123"
    assert stats["recent"][-1]["message"] == "price outside bounds"


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
async def test_cancel_all_failure_recorded_in_api_error_stats():
    om = order_manager_mod.OrderManager(_CancelAllFailClient(), MMConfig())

    with pytest.raises(RuntimeError, match="cancel_all boom"):
        await om.cancel_all(force_exchange=True)

    stats = om.get_api_error_stats()
    assert stats["total_by_op"]["cancel_all"] == 1
    assert stats["recent"][-1]["message"] == "cancel_all boom"


@pytest.mark.anyio
async def test_fill_poll_failure_is_visible_in_state_api_errors():
    om = order_manager_mod.OrderManager(_FallbackPollErrorClient(), MMConfig())
    oid = "oid-fallback-error"
    quote = Quote(side="BUY", token_id="up_tok_123", price=0.50, size=10.0, order_id=oid)
    quote.placed_at = time.time() - 31.0
    om._active_orders[oid] = quote
    om._order_post_only[oid] = True

    fills = await om.check_fills()

    assert fills == []
    stats = om.get_api_error_stats()
    assert stats["total_by_op"]["fallback_poll"] == 1
    assert stats["recent"][-1]["order_id"] == oid[:12]


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
    assert quote.size == pytest.approx(6.54)
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


@pytest.mark.anyio
async def test_place_order_includes_fee_rate_bps_when_supported(monkeypatch):
    monkeypatch.setattr(order_manager_mod, "_HAS_CLOB_TYPES", True)
    monkeypatch.setattr(order_manager_mod, "OrderType", _DummyOrderType, raising=False)
    monkeypatch.setattr(order_manager_mod, "OrderArgs", _OrderArgsWithFee, raising=False)
    monkeypatch.setattr(
        order_manager_mod,
        "fetch_fee_rate",
        AsyncMock(return_value={"feeRate": 0.10}),
    )

    client = _LiveFeeCaptureClient()
    om = order_manager_mod.OrderManager(client, MMConfig())
    quote = Quote(side="BUY", token_id="up_tok_123", price=0.50, size=10.0)

    oid = await om._place_order_inner(quote, post_only=True)

    assert oid is not None
    assert client.last_order_args is not None
    assert getattr(client.last_order_args, "fee_rate_bps", None) == 1000


@pytest.mark.anyio
async def test_place_order_blocks_when_fee_rate_unavailable_on_live(monkeypatch):
    monkeypatch.setattr(order_manager_mod, "_HAS_CLOB_TYPES", True)
    monkeypatch.setattr(order_manager_mod, "OrderType", _DummyOrderType, raising=False)
    monkeypatch.setattr(order_manager_mod, "OrderArgs", _OrderArgsWithFee, raising=False)
    monkeypatch.setattr(order_manager_mod, "fetch_fee_rate", AsyncMock(return_value=None))

    client = _LiveFeeCaptureClient()
    om = order_manager_mod.OrderManager(client, MMConfig())
    quote = Quote(side="BUY", token_id="up_tok_123", price=0.50, size=10.0)

    with pytest.raises(RuntimeError, match="fee rate unavailable"):
        await om._place_order_inner(quote, post_only=True)


@pytest.mark.anyio
async def test_buy_balance_retry_uses_market_min_order_size():
    om = order_manager_mod.OrderManager(object(), MMConfig())
    om.set_market_context(min_order_size=10.0)
    om.get_usdc_available_balance = AsyncMock(side_effect=[100.0, 4.0])  # max_size=8 < min 10
    om._place_order_inner = AsyncMock(
        side_effect=[RuntimeError("not enough balance"), "should-not-run"],
    )

    quote = Quote(side="BUY", token_id="up_tok_123", price=0.50, size=20.0)
    oid = await om.place_order(quote)

    assert oid is None
    assert om._place_order_inner.await_count == 1


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
async def test_stop_feed_tasks_sweeps_untracked_leaked_feed_task():
    if "aiohttp" not in sys.modules:
        import types

        sys.modules["aiohttp"] = types.ModuleType("aiohttp")

    web_server = importlib.import_module("web_server")
    runtime = web_server.MMRuntime()

    async def ob_poller():
        while True:
            await asyncio.sleep(60)

    leaked = asyncio.create_task(ob_poller())
    try:
        assert leaked.done() is False
        assert runtime._feed_tasks == []

        await runtime._stop_feed_tasks()

        assert leaked.done() is True
    finally:
        leaked.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await leaked


@pytest.mark.anyio
async def test_cancel_monitor_task_handles_self_task():
    if "aiohttp" not in sys.modules:
        import types

        sys.modules["aiohttp"] = types.ModuleType("aiohttp")

    web_server = importlib.import_module("web_server")
    runtime = web_server.MMRuntime()
    runtime._monitor_task = asyncio.current_task()

    await runtime._cancel_monitor_task()

    assert runtime._monitor_task is None


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


def test_runtime_enforce_maker_only_sets_alert():
    if "aiohttp" not in sys.modules:
        import types

        sys.modules["aiohttp"] = types.ModuleType("aiohttp")

    web_server = importlib.import_module("web_server")
    runtime = web_server.MMRuntime()
    runtime.mm_config.use_post_only = False

    runtime._enforce_maker_only("test")

    assert runtime.mm_config.use_post_only is True
    alerts = runtime.list_alerts()
    assert any(a.get("source") == "maker_only" for a in alerts)


@pytest.mark.anyio
async def test_mm_config_update_rejects_disabling_post_only(monkeypatch):
    if "aiohttp" not in sys.modules:
        import types

        sys.modules["aiohttp"] = types.ModuleType("aiohttp")

    web_server = importlib.import_module("web_server")
    monkeypatch.setattr(web_server, "_require_auth", lambda _request: None)

    req = web_server.ConfigUpdateRequest(use_post_only=False)
    resp = await web_server.mm_config_update(req=req, request=object())

    assert getattr(resp, "status_code", None) == 400
    body = getattr(resp, "body", b"")
    assert b"use_post_only=false" in body


def test_fee_or_signature_reject_invalidates_fee_cache(monkeypatch):
    om = order_manager_mod.OrderManager(object(), MMConfig())
    calls: list[str] = []

    def _capture(token_id=None):
        calls.append(token_id)

    monkeypatch.setattr(order_manager_mod, "invalidate_fee_rate_cache", _capture)

    quote = Quote(side="BUY", token_id="tok_fee_123", price=0.5, size=10.0)
    om._handle_fee_or_signature_reject(
        quote,
        reason="invalid feeRateBps signature",
        source="unit",
    )

    assert calls == ["tok_fee_123"]


@pytest.mark.anyio
async def test_live_start_blocks_on_startup_cancel_all_failure():
    import mm.market_maker as market_maker_mod
    from mm.types import MarketInfo

    class _FeedState:
        pass

    class _LiveClient:
        pass

    mm = market_maker_mod.MarketMaker(_FeedState(), _LiveClient(), MMConfig())
    mm.set_market(
        MarketInfo(
            coin="BTC",
            timeframe="5m",
            up_token_id="up_token",
            dn_token_id="dn_token",
            strike=100000.0,
            window_start=time.time(),
            window_end=time.time() + 300.0,
        )
    )
    mm._cancel_all_guarded = AsyncMock(side_effect=RuntimeError("cancel_all boom"))

    with pytest.raises(market_maker_mod.StartBlockedError, match="startup cancel_all failed"):
        await mm.start()


def test_pm_apply_accepts_099_prices():
    import feeds

    state = feeds.State()
    state.pm_up_id = "up_token"
    feeds._pm_apply(
        "up_token",
        asks=[{"price": "0.99"}],
        bids=[{"price": "0.99"}],
        state=state,
    )

    assert state.pm_up == pytest.approx(0.99)
    assert state.pm_up_bid == pytest.approx(0.99)
    assert state.pm_last_update_ts > 0


@pytest.mark.anyio
async def test_runtime_start_rejects_window_too_close_to_close(monkeypatch):
    if "aiohttp" not in sys.modules:
        import types

        sys.modules["aiohttp"] = types.ModuleType("aiohttp")

    web_server = importlib.import_module("web_server")
    runtime = web_server.MMRuntime()

    class _State:
        def __init__(self):
            self.mid = 100000.0
            self.pm_up_id = None
            self.pm_dn_id = None

    async def _noop_feed(*_args, **_kwargs):
        return None

    now = time.time()
    near_close_market = web_server.MarketInfo(
        coin="BTC",
        timeframe="15m",
        up_token_id="up_token",
        dn_token_id="dn_token",
        condition_id="cond_token",
        strike=100000.0,
        window_start=now - 870.0,
        window_end=now + 30.0,
        min_order_size=5.0,
        tick_size=0.01,
    )

    monkeypatch.setattr(runtime, "_auto_select_market", AsyncMock(return_value=("BTC", "15m")))
    monkeypatch.setattr(web_server.feeds, "State", _State)
    monkeypatch.setattr(web_server.feeds, "ob_poller", _noop_feed)
    monkeypatch.setattr(web_server.feeds, "binance_feed", _noop_feed)
    monkeypatch.setattr(web_server.feeds, "pm_feed", _noop_feed)
    monkeypatch.setattr(web_server.feeds, "fetch_pm_tokens", lambda *_args, **_kwargs: ("up_token", "dn_token", "cond_token"))
    monkeypatch.setattr(runtime, "_build_market_info_from_tokens", lambda *_args, **_kwargs: near_close_market)
    monkeypatch.setattr(runtime, "_enrich_market_info", AsyncMock(return_value=None))
    monkeypatch.setattr(web_server, "_create_clob_client", lambda **_kwargs: object())
    monkeypatch.setattr(web_server._telegram, "switch_credentials", lambda **_kwargs: None)

    with pytest.raises(web_server.HTTPException) as exc:
        await runtime.start("BTC", "15m", paper_mode=True, initial_usdc=5.0, dev=False)

    assert exc.value.status_code == 409
    assert "too close to close" in str(exc.value.detail)
    assert runtime.mm is None
    assert runtime._running is False


def test_runtime_snapshot_exposes_runtime_watchdog():
    if "aiohttp" not in sys.modules:
        import types

        sys.modules["aiohttp"] = types.ModuleType("aiohttp")

    web_server = importlib.import_module("web_server")
    runtime = web_server.MMRuntime()
    runtime._runtime_watchdog["active"] = True
    runtime._runtime_watchdog["last_cpu_pct"] = 97.5
    runtime._runtime_watchdog["last_main_stack"] = ["market_maker.py:1500 in _tick"]

    snap = runtime.snapshot()

    assert "runtime_watchdog" in snap
    assert snap["runtime_watchdog"]["active"] is True
    assert snap["runtime_watchdog"]["last_cpu_pct"] == pytest.approx(97.5)
    assert "verification" in snap
    assert snap["verification"]["running"] is False


@pytest.mark.anyio
async def test_runtime_server_self_check_passes_when_idle():
    if "aiohttp" not in sys.modules:
        import types

        sys.modules["aiohttp"] = types.ModuleType("aiohttp")

    web_server = importlib.import_module("web_server")
    runtime = web_server.MMRuntime()

    result = await runtime._run_server_self_check()

    assert result["ok"] is True
    names = {check["name"] for check in result["checks"]}
    assert "state_contract:order_tracking" in names
    assert "state_contract:cycle_guard" in names
    assert "state_contract:negative_edge_guard" in names
    assert "state_contract:liquidation" in names
    assert "state_contract:api_errors" in names


@pytest.mark.anyio
async def test_runtime_verification_rejects_unknown_kind():
    if "aiohttp" not in sys.modules:
        import types

        sys.modules["aiohttp"] = types.ModuleType("aiohttp")

    web_server = importlib.import_module("web_server")
    runtime = web_server.MMRuntime()

    with pytest.raises(web_server.HTTPException) as exc:
        await runtime.start_verification("unsupported-kind")

    assert exc.value.status_code == 400


def test_fallback_poll_hot_alert_after_repeated_cap_hits():
    if "aiohttp" not in sys.modules:
        import types

        sys.modules["aiohttp"] = types.ModuleType("aiohttp")

    web_server = importlib.import_module("web_server")
    runtime = web_server.MMRuntime()

    class _MM:
        def snapshot(self):
            return {
                "config": {"fallback_poll_cap": 12},
                "order_tracking": {"last_fallback_poll_count": 12},
                "cycle_guard": {"mode": "off", "reason": "", "active": False},
                "negative_edge_guard": {"active": False, "reason": ""},
                "liquidation": {"mode": "inactive", "reason": "", "residual_inventory_failure": False},
                "pause_reason": "",
            }

    runtime.mm = _MM()
    runtime._running = True

    runtime.snapshot()
    snap = runtime.snapshot()

    assert any(a["source"] == "fallback_poll_hot" for a in snap["alerts"])


def test_runtime_snapshot_emits_guard_alerts():
    if "aiohttp" not in sys.modules:
        import types

        sys.modules["aiohttp"] = types.ModuleType("aiohttp")

    web_server = importlib.import_module("web_server")
    runtime = web_server.MMRuntime()

    class _MM:
        def snapshot(self):
            return {
                "config": {"fallback_poll_cap": 12},
                "order_tracking": {"last_fallback_poll_count": 11},
                "cycle_guard": {
                    "mode": "close_only",
                    "reason": "negative edge confirmed",
                    "active": True,
                },
                "negative_edge_guard": {
                    "active": True,
                    "reason": "negative edge confirmed",
                },
                "liquidation": {
                    "mode": "aggressive",
                    "reason": "Max drawdown exceeded: PnL=$-4.50",
                    "residual_inventory_failure": True,
                },
                "pause_reason": "Critical inventory drift: waiting for full reconcile",
            }

    runtime.mm = _MM()
    runtime._running = True

    snap = runtime.snapshot()
    alert_sources = {a["source"] for a in snap["alerts"]}

    assert "negative_edge_guard_activated" in alert_sources
    assert "cycle_guard_close_only" in alert_sources
    assert "drawdown_exit" in alert_sources
    assert "residual_inventory_failure" in alert_sources
    assert "critical_drift_pause" in alert_sources
    assert "aggressive_liquidation_activated" in alert_sources


def test_telegram_polling_enabled_env(monkeypatch):
    if "aiohttp" not in sys.modules:
        import types

        sys.modules["aiohttp"] = types.ModuleType("aiohttp")

    web_server = importlib.import_module("web_server")

    monkeypatch.setenv("TELEGRAM_POLLING_ENABLED", "0")
    assert web_server._telegram_polling_enabled() is False

    monkeypatch.setenv("TELEGRAM_POLLING_ENABLED", "1")
    assert web_server._telegram_polling_enabled() is True


@pytest.mark.anyio
async def test_check_fills_guarded_uses_order_ops_lock():
    import mm.market_maker as market_maker_mod
    from mm.types import MarketInfo

    class _FeedState:
        pass

    class _PaperClient:
        _orders = {}

    mm = market_maker_mod.MarketMaker(_FeedState(), _PaperClient(), MMConfig())
    mm.set_market(
        MarketInfo(
            coin="BTC",
            timeframe="5m",
            up_token_id="up_token",
            dn_token_id="dn_token",
            strike=100000.0,
            window_start=time.time(),
            window_end=time.time() + 300.0,
        )
    )

    lock_observed = {"locked": False}

    async def _fake_check_fills():
        lock_observed["locked"] = mm._order_ops_lock.locked()
        return []

    mm.order_mgr.check_fills = _fake_check_fills
    fills = await mm._check_fills_guarded()

    assert fills == []
    assert lock_observed["locked"] is True
