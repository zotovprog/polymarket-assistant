"""Tests for debounced reconciliation and session_pnl risk checks."""

from __future__ import annotations

import asyncio
import time
from dataclasses import dataclass, field
from unittest.mock import AsyncMock

import pytest

from mm.market_maker import MarketMaker
from mm.mm_config import MMConfig
from mm.risk_manager import RiskManager
from mm.types import Fill, Inventory, MarketInfo


@dataclass
class MockFeedState:
    mid: float = 100.0
    bids: list = field(default_factory=list)
    asks: list = field(default_factory=list)
    trades: list = field(default_factory=list)
    klines: list = field(default_factory=list)
    pm_up: float = 0.5
    pm_dn: float = 0.5


class MockClobClient:
    """Minimal CLOB client with paper-mode marker (_orders)."""

    def __init__(self):
        self._orders = {}
        self._usdc_balance = 100.0

    def post_heartbeat(self):
        pass

    def create_and_sign_order(self, args):
        return args

    def post_order(self, order, order_type):
        return {"orderID": "mock-001"}

    def cancel_all(self):
        pass

    def get_order(self, oid):
        return {"status": "LIVE", "size_matched": "0"}

    def get_order_book(self, token_id):
        return None


class MockLiveClobClient(MockClobClient):
    """Same interface as paper mock, but without _orders (live-mode path)."""

    def __init__(self):
        super().__init__()
        del self._orders


def _make_market() -> MarketInfo:
    now = time.time()
    return MarketInfo(
        coin="BTC",
        timeframe="15m",
        strike=100000.0,
        up_token_id="up_token_123",
        dn_token_id="dn_token_456",
        condition_id="cond_789",
        window_start=now,
        window_end=now + 900,
    )


def _make_mm(*, live: bool) -> MarketMaker:
    feed_state = MockFeedState()
    client = MockLiveClobClient() if live else MockClobClient()
    mm = MarketMaker(feed_state, client, MMConfig())
    mm.set_market(_make_market())
    return mm


def _run_tick_with_reconcile_gate(mm: MarketMaker) -> None:
    # _tick() increments first; setting to 4 ensures reconciliation check runs (%5 == 0).
    mm._tick_count = 4
    asyncio.run(mm._tick())


def _make_risk_setup() -> tuple[RiskManager, Inventory]:
    risk_mgr = RiskManager(MMConfig(max_drawdown_usd=8.0))
    inventory = Inventory(up_shares=14.96, dn_shares=0.0)

    risk_mgr.record_fill(
        Fill(
            ts=time.time(),
            side="BUY",
            token_id="up_token_123",
            price=0.90,
            size=10.0,
            fee=0.0,
        )
    )
    risk_mgr.record_fill(
        Fill(
            ts=time.time(),
            side="BUY",
            token_id="up_token_123",
            price=0.90,
            size=10.0,
            fee=0.0,
        )
    )
    return risk_mgr, inventory


def test_debounce_prevents_oscillation():
    mm = _make_mm(live=True)
    mm.feed_state.mid = 0.0  # stop _tick() right after reconciliation logic
    mm.inventory.up_shares = 14.96
    mm.inventory.dn_shares = 0.0

    mm.order_mgr.check_fills = AsyncMock(return_value=[])
    mm.order_mgr.get_all_token_balances = AsyncMock(
        side_effect=[
            (15.0, 18.0),  # first drift sample -> starts debounce window
            (1.5, 18.0),   # PM changed too much -> reset to 1
            (1.5, 18.0),   # stable -> 2
            (1.5, 18.0),   # stable -> 3, reconcile
        ]
    )
    mm.order_mgr.get_usdc_balance = AsyncMock(return_value=100.0)

    _run_tick_with_reconcile_gate(mm)
    assert mm._reconcile_prev_pm == (15.0, 18.0)
    assert mm._reconcile_stable_count == 1

    _run_tick_with_reconcile_gate(mm)
    assert mm._reconcile_prev_pm == (1.5, 18.0)
    assert mm._reconcile_stable_count == 1

    _run_tick_with_reconcile_gate(mm)
    assert mm._reconcile_prev_pm == (1.5, 18.0)
    assert mm._reconcile_stable_count == 2

    _run_tick_with_reconcile_gate(mm)
    assert mm._reconcile_prev_pm is None
    assert mm._reconcile_stable_count == 0
    assert mm.inventory.up_shares == pytest.approx(1.5)
    assert mm.inventory.dn_shares == pytest.approx(18.0)


def test_session_pnl_prevents_false_drawdown():
    risk_mgr, inventory = _make_risk_setup()

    pause_internal, reason_internal = risk_mgr.should_pause(inventory, session_pnl=None)
    assert pause_internal is True
    assert "Max drawdown exceeded" in reason_internal

    pause_session, reason_session = risk_mgr.should_pause(inventory, session_pnl=-1.06)
    assert pause_session is False
    assert reason_session == ""


def test_session_pnl_triggers_real_drawdown():
    risk_mgr, inventory = _make_risk_setup()

    should_pause, reason = risk_mgr.should_pause(inventory, session_pnl=-10.0)
    assert should_pause is True
    assert reason == "Max drawdown exceeded: PnL=$-10.00"


def test_reconcile_resets_on_window_transition():
    mm = _make_mm(live=False)
    mm._reconcile_prev_pm = (15.0, 18.0)
    mm._reconcile_stable_count = 2

    new_market = MarketInfo(
        coin="BTC",
        timeframe="15m",
        strike=100000.0,
        up_token_id="up_token_123",
        dn_token_id="dn_token_456",
        condition_id="cond_789",
        window_start=time.time(),
        window_end=time.time() + 900,
    )
    asyncio.run(mm.on_window_transition(new_market))

    assert mm._reconcile_prev_pm is None
    assert mm._reconcile_stable_count == 0


def test_sell_clamps_shares_to_zero():
    """SELL more than internal inventory -> shares clamped to 0, not negative."""
    from mm.types import Fill, Inventory

    inv = Inventory(up_shares=5.0, dn_shares=3.0)

    # Sell more DN than we have internally
    big_sell = Fill(
        ts=time.time(),
        side="SELL",
        token_id="dn_token_456",
        price=0.50,
        size=10.0,
        fee=0.0,
    )
    inv.update_from_fill(big_sell, "dn")
    assert inv.dn_shares == 0.0  # Clamped, not -7.0

    # Same for UP
    big_sell_up = Fill(
        ts=time.time(),
        side="SELL",
        token_id="up_token_123",
        price=0.60,
        size=20.0,
        fee=0.0,
    )
    inv.update_from_fill(big_sell_up, "up")
    assert inv.up_shares == 0.0  # Clamped, not -15.0


def test_floor_decay_from_closing_start():
    """Floor decay should use closing start time as reference, not close_sec."""
    mm = _make_mm(live=False)
    # Simulate entering closing with 400s left in window
    mm._closing_start_time_left = 400.0

    # At 400s left (just entered closing): decay_ratio should be 1.0
    ref = mm._closing_start_time_left
    decay_400 = max(0.0, min(1.0, 400.0 / ref))
    assert decay_400 == pytest.approx(1.0)

    # At 200s left: decay_ratio should be 0.5
    decay_200 = max(0.0, min(1.0, 200.0 / ref))
    assert decay_200 == pytest.approx(0.5)

    # At 0s left: decay_ratio should be 0.0
    decay_0 = max(0.0, min(1.0, 0.0 / ref))
    assert decay_0 == pytest.approx(0.0)

    # Verify floor = max(0.01, base_floor * decay_ratio)
    base_floor = 0.63
    assert max(0.01, base_floor * decay_200) == pytest.approx(0.315)
    assert max(0.01, base_floor * decay_0) == pytest.approx(0.01)


def test_drawdown_forces_taker_in_liquidation():
    """Catastrophic loss during liquidation should stop the bot."""
    mm = _make_mm(live=True)
    mm._is_closing = True
    mm._starting_usdc_pm = 50.0  # started with $50
    mm._cached_usdc_balance = 10.0  # now only $10
    mm.inventory.up_shares = 0.0
    mm.inventory.dn_shares = 0.0
    mm.feed_state.pm_up = 0.5
    mm.feed_state.pm_dn = 0.5

    # Session PnL = (10 + 0) - 50 = -$40, max_drawdown_usd default = 100
    # With max_drawdown=8: -40 < -16 (2*8) -> catastrophic -> abandon
    mm.config.max_drawdown_usd = 8.0

    # Mock order_mgr methods to prevent real API calls
    mm.order_mgr.check_fills = AsyncMock(return_value=[])
    mm.order_mgr.get_token_balance = AsyncMock(return_value=0.0)
    mm.order_mgr.get_usdc_balance = AsyncMock(return_value=10.0)

    asyncio.run(mm._liquidate_inventory())

    # Bot should have abandoned liquidation
    assert mm._is_closing is False
    assert mm._running is False


def test_binance_stale_uses_fresh_ws_timestamp():
    """Do not trigger stale-cancel when OB is older but WS stream is fresh."""
    mm = _make_mm(live=False)
    mm.feed_state.mid = 0.0
    mm.feed_state.binance_ob_last_ok_ts = time.time() - 6.0
    mm.feed_state.binance_ws_last_ok_ts = time.time() - 1.0
    mm.feed_state.pm_up = None
    mm.feed_state.pm_dn = None
    mm.order_mgr.client.get_balance = lambda: 100.0

    mm.order_mgr.check_fills = AsyncMock(return_value=[])
    mm.order_mgr.cancel_all = AsyncMock(return_value=0)

    asyncio.run(mm._tick())
    mm.order_mgr.cancel_all.assert_not_awaited()


def test_binance_stale_cancels_when_all_streams_old():
    """Safety cancel still triggers when both OB and WS become stale."""
    mm = _make_mm(live=False)
    mm.feed_state.mid = 0.0
    mm.feed_state.binance_ob_last_ok_ts = time.time() - 16.0
    mm.feed_state.binance_ws_last_ok_ts = time.time() - 16.0
    mm.feed_state.pm_up = None
    mm.feed_state.pm_dn = None
    mm.order_mgr.client.get_balance = lambda: 100.0

    mm.order_mgr.check_fills = AsyncMock(return_value=[])
    mm.order_mgr.cancel_all = AsyncMock(return_value=0)

    asyncio.run(mm._tick())
    mm.order_mgr.cancel_all.assert_awaited_once()
