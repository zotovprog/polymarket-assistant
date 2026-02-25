"""Targeted regressions for batch reject diagnostics and runtime sync guard."""

from __future__ import annotations

import asyncio
import importlib
import logging
import os
import sys
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
