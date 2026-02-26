"""Regression tests for session-cap safety guards."""

from __future__ import annotations

import os
import sys
import time
from dataclasses import dataclass, field
from unittest.mock import AsyncMock

import pytest


BASE = os.path.dirname(os.path.dirname(__file__))
SRC = os.path.join(BASE, "src")
if SRC not in sys.path:
    sys.path.insert(0, SRC)
if BASE not in sys.path:
    sys.path.insert(0, BASE)

from mm.market_maker import MarketMaker, StartBlockedError
from mm.mm_config import MMConfig
from mm.types import MarketInfo, Quote


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


class _PaperClient:
    def __init__(self):
        self._orders = {}

    def get_balance(self):
        return 0.0


def _make_mm() -> MarketMaker:
    mm = MarketMaker(_FeedState(), _PaperClient(), MMConfig())
    now = time.time()
    mm.set_market(
        MarketInfo(
            coin="BTC",
            timeframe="15m",
            strike=100000.0,
            up_token_id="up_token_123",
            dn_token_id="dn_token_456",
            condition_id="cond_789",
            window_start=now,
            window_end=now + 900.0,
        )
    )
    return mm


@pytest.mark.anyio
async def test_start_keeps_requested_session_budget_when_wallet_is_larger():
    mm = _make_mm()
    mm.inventory.initial_usdc = 15.0  # requested session budget from runtime

    mm.order_mgr.cancel_all = AsyncMock(return_value=0)
    mm.order_mgr.get_all_token_balances = AsyncMock(return_value=(0.0, 0.0))
    mm.order_mgr.get_usdc_balances = AsyncMock(return_value=(42.0, 40.0))
    mm._refresh_fee_rate_cache = AsyncMock(return_value=None)
    mm.heartbeat.start = lambda: None
    mm.order_mgr.set_fill_callback = lambda *_args, **_kwargs: None
    mm.order_mgr.set_ws_reconnect_callback = lambda *_args, **_kwargs: None
    mm.order_mgr.set_heartbeat_id_callback = lambda *_args, **_kwargs: None

    async def _noop_run_loop():
        return None

    mm._run_loop = _noop_run_loop

    await mm.start()
    if mm._task:
        await mm._task
    mm._running = False

    assert mm.inventory.initial_usdc == pytest.approx(15.0)
    assert mm.order_mgr._session_budget == pytest.approx(15.0)
    assert mm._starting_usdc_pm == pytest.approx(42.0)


@pytest.mark.anyio
async def test_start_blocks_when_preexisting_exposure_exceeds_session_cap():
    mm = _make_mm()
    mm.inventory.initial_usdc = 15.0

    mm.order_mgr.cancel_all = AsyncMock(return_value=0)
    mm.order_mgr.get_all_token_balances = AsyncMock(return_value=(20.0, 20.0))
    mm.order_mgr.get_usdc_balances = AsyncMock(return_value=(42.0, 42.0))
    mm._refresh_fee_rate_cache = AsyncMock(return_value=None)
    mm.heartbeat.start = lambda: None
    mm.order_mgr.set_fill_callback = lambda *_args, **_kwargs: None
    mm.order_mgr.set_ws_reconnect_callback = lambda *_args, **_kwargs: None
    mm.order_mgr.set_heartbeat_id_callback = lambda *_args, **_kwargs: None

    async def _noop_run_loop():
        return None

    mm._run_loop = _noop_run_loop

    with pytest.raises(StartBlockedError):
        await mm.start()

    assert mm._running is False


@pytest.mark.anyio
async def test_start_blocks_when_non_flat_inventory_and_flat_guard_enabled():
    mm = _make_mm()
    mm.inventory.initial_usdc = 42.0
    mm.config.require_flat_start = True
    mm.config.flat_start_max_shares = 0.25

    mm.order_mgr.cancel_all = AsyncMock(return_value=0)
    mm.order_mgr.get_all_token_balances = AsyncMock(return_value=(0.6, 0.0))
    mm.order_mgr.get_usdc_balances = AsyncMock(return_value=(42.0, 42.0))
    mm._refresh_fee_rate_cache = AsyncMock(return_value=None)
    mm.heartbeat.start = lambda: None
    mm.order_mgr.set_fill_callback = lambda *_args, **_kwargs: None
    mm.order_mgr.set_ws_reconnect_callback = lambda *_args, **_kwargs: None
    mm.order_mgr.set_heartbeat_id_callback = lambda *_args, **_kwargs: None

    async def _noop_run_loop():
        return None

    mm._run_loop = _noop_run_loop

    with pytest.raises(StartBlockedError, match="non-flat wallet inventory"):
        await mm.start()

    assert mm._running is False


@pytest.mark.anyio
async def test_start_allows_non_flat_inventory_when_flat_guard_disabled():
    mm = _make_mm()
    mm.inventory.initial_usdc = 42.0
    mm.config.require_flat_start = False
    mm.config.flat_start_max_shares = 0.25

    mm.order_mgr.cancel_all = AsyncMock(return_value=0)
    mm.order_mgr.get_all_token_balances = AsyncMock(return_value=(0.6, 0.0))
    mm.order_mgr.get_usdc_balances = AsyncMock(return_value=(42.0, 42.0))
    mm._refresh_fee_rate_cache = AsyncMock(return_value=None)
    mm.heartbeat.start = lambda: None
    mm.order_mgr.set_fill_callback = lambda *_args, **_kwargs: None
    mm.order_mgr.set_ws_reconnect_callback = lambda *_args, **_kwargs: None
    mm.order_mgr.set_heartbeat_id_callback = lambda *_args, **_kwargs: None

    async def _noop_run_loop():
        return None

    mm._run_loop = _noop_run_loop

    await mm.start()
    if mm._task:
        await mm._task
    mm._running = False

    assert mm.inventory.up_shares == pytest.approx(0.6)


def test_session_exposure_cap_suppresses_all_buys_when_cap_is_exhausted():
    mm = _make_mm()
    mm.inventory.initial_usdc = 15.0
    mm._cached_pm_up_shares = 10.0
    mm._cached_pm_dn_shares = 10.0
    mm._cached_usdc_balance = 42.0
    mm._cached_usdc_available_balance = 36.0  # reserved=6

    quotes = {
        "up": (
            Quote(side="BUY", token_id=mm.market.up_token_id, price=0.45, size=12.0),
            Quote(side="SELL", token_id=mm.market.up_token_id, price=0.55, size=12.0),
        ),
        "dn": (
            Quote(side="BUY", token_id=mm.market.dn_token_id, price=0.44, size=12.0),
            Quote(side="SELL", token_id=mm.market.dn_token_id, price=0.56, size=12.0),
        ),
    }

    mm._enforce_session_exposure_cap(quotes, pm_up_price=0.50, pm_dn_price=0.50)

    assert quotes["up"][0] is None
    assert quotes["dn"][0] is None


def test_session_exposure_cap_scales_buys_to_remaining_headroom():
    mm = _make_mm()
    mm.inventory.initial_usdc = 15.0
    mm._cached_pm_up_shares = 8.0    # $4.00 at 0.5
    mm._cached_pm_dn_shares = 4.0    # $2.00 at 0.5
    mm._cached_usdc_balance = 30.0
    mm._cached_usdc_available_balance = 28.0  # reserved $2.00
    # Used = 4 + 2 + 2 = 8 => headroom = 7

    quotes = {
        "up": (
            Quote(side="BUY", token_id=mm.market.up_token_id, price=0.50, size=10.0),  # $5.00
            Quote(side="SELL", token_id=mm.market.up_token_id, price=0.60, size=10.0),
        ),
        "dn": (
            Quote(side="BUY", token_id=mm.market.dn_token_id, price=0.40, size=10.0),  # $4.00
            Quote(side="SELL", token_id=mm.market.dn_token_id, price=0.52, size=10.0),
        ),
    }

    mm._enforce_session_exposure_cap(quotes, pm_up_price=0.50, pm_dn_price=0.50)

    up_bid = quotes["up"][0]
    dn_bid = quotes["dn"][0]
    assert up_bid is not None
    assert dn_bid is not None
    planned_buy_notional = up_bid.size * up_bid.price + dn_bid.size * dn_bid.price
    assert planned_buy_notional <= 7.01
    assert up_bid.size < 10.0
    assert dn_bid.size < 10.0


@pytest.mark.anyio
async def test_liquidation_catastrophic_uses_single_drawdown_threshold():
    mm = _make_mm()
    mm.config.max_drawdown_usd = 8.0
    mm._catastrophic_count = 2
    mm._starting_portfolio_pm = 50.0
    mm._cached_usdc_balance = 41.0
    mm._cached_usdc_available_balance = 41.0
    mm._cached_pm_up_shares = 0.0
    mm._cached_pm_dn_shares = 0.0

    mm.order_mgr.get_token_balance = AsyncMock(side_effect=[0.0, 0.0])
    mm.order_mgr.get_usdc_balances = AsyncMock(return_value=(41.0, 41.0))
    mm.order_mgr.get_all_token_balances = AsyncMock(return_value=(0.0, 0.0))
    mm._emergency_shutdown = AsyncMock()

    await mm._liquidate_inventory()

    mm._emergency_shutdown.assert_awaited_once()
    reason = mm._emergency_shutdown.await_args.args[0]
    assert "CATASTROPHIC LOSS confirmed" in reason


@pytest.mark.anyio
async def test_inventory_limit_triggers_hard_close_and_cancel():
    mm = _make_mm()
    mm.config.max_inventory_shares = 5.0
    mm.inventory.up_shares = 12.0
    mm.inventory.dn_shares = 0.0
    mm.inventory.usdc = 15.0
    mm._cached_pm_up_shares = 12.0
    mm._cached_pm_dn_shares = 0.0
    mm._cached_usdc_balance = 15.0
    mm._cached_usdc_available_balance = 15.0

    mm.order_mgr.cancel_all = AsyncMock(return_value=0)
    mm.order_mgr.check_fills = AsyncMock(return_value=[])
    mm.order_mgr.get_all_token_balances = AsyncMock(return_value=(12.0, 0.0))
    mm.order_mgr.get_usdc_balances = AsyncMock(return_value=(15.0, 15.0))
    mm._get_liq_lock_best_bids = AsyncMock(return_value=(0.50, 0.50))
    mm._maybe_backfill_trades = AsyncMock(return_value=None)
    mm.markout_tracker.check_markouts = AsyncMock(return_value=None)

    mm._compute_fv = lambda: (0.50, 0.50)
    mm.fair_value.realized_vol = lambda _klines: 0.0005

    await mm._tick()

    mm.order_mgr.cancel_all.assert_awaited()
    assert mm._is_closing is True
    assert mm._paused is True
    assert "Inventory limit" in mm._pause_reason
    assert mm._current_quotes == {"up": (None, None), "dn": (None, None)}
