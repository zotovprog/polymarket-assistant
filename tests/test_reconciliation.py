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
from mm.types import Fill, Inventory, MarketInfo, Quote


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
    # _tick() increments first; setting to 14 ensures reconciliation check runs (%15 == 0).
    mm._tick_count = 14
    asyncio.run(mm._tick())


def _make_risk_setup() -> tuple[RiskManager, Inventory]:
    risk_mgr = RiskManager(
        MMConfig(
            max_drawdown_usd=8.0,
            max_inventory_shares=100.0,
            max_net_delta_shares=100.0,
        )
    )
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
    mm.config.critical_reconcile_drift_shares = 100.0
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
    mm.order_mgr.get_usdc_balances = AsyncMock(return_value=(100.0, 100.0))

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


def test_forced_reconcile_arms_guard():
    mm = _make_mm(live=True)
    mm._running = True
    mm.feed_state.mid = 0.0  # stop after reconcile path
    mm.inventory.up_shares = 0.0
    mm.inventory.dn_shares = 0.0
    mm.order_mgr._reconcile_requested = True
    mm.order_mgr.check_fills = AsyncMock(return_value=[])
    mm.order_mgr.get_all_token_balances = AsyncMock(return_value=(15.0, 18.0))
    mm.order_mgr.get_usdc_balances = AsyncMock(return_value=(100.0, 100.0))

    before = time.time()
    _run_tick_with_reconcile_gate(mm)

    assert mm._reconcile_guard_until > before
    assert mm.order_mgr.reconcile_requested is False


def test_reconcile_guard_pauses_liquidation_tick():
    mm = _make_mm(live=True)
    mm._running = True
    mm._is_closing = True
    mm._reconcile_guard_until = time.time() + 8.0
    mm.market.window_end = time.time() + 120.0
    mm.order_mgr.check_fills = AsyncMock(return_value=[])
    mm._liquidate_inventory = AsyncMock()

    asyncio.run(mm._tick())

    mm._liquidate_inventory.assert_not_awaited()


def test_reconcile_guard_freezes_quotes_and_cancels_active_orders():
    mm = _make_mm(live=True)
    mm._running = True
    mm._is_closing = False
    mm._reconcile_guard_until = time.time() + 8.0

    live_bid = Quote(side="BUY", token_id=mm.market.up_token_id, price=0.50, size=5.0, order_id="oid-1")
    mm.order_mgr._active_orders = {"oid-1": live_bid}
    mm._current_quotes = {
        "up": (live_bid, None),
        "dn": (None, None),
    }

    mm.order_mgr.check_fills = AsyncMock(return_value=[])
    mm.order_mgr.cancel_all = AsyncMock(return_value=1)

    asyncio.run(mm._tick())

    mm.order_mgr.cancel_all.assert_awaited_once()
    assert mm._current_quotes == {"up": (None, None), "dn": (None, None)}


def test_critical_inventory_drift_triggers_pause_and_cancel():
    mm = _make_mm(live=True)
    mm._running = True
    mm.feed_state.mid = 0.0  # stop after reconcile path
    mm.inventory.up_shares = 9.0
    mm.inventory.dn_shares = 0.0
    mm.config.critical_reconcile_drift_shares = 1.0

    mm.order_mgr.check_fills = AsyncMock(return_value=[])
    mm.order_mgr.get_all_token_balances = AsyncMock(return_value=(0.0, 0.0))
    mm.order_mgr.get_usdc_balances = AsyncMock(return_value=(100.0, 100.0))
    mm.order_mgr.cancel_all = AsyncMock(return_value=1)

    _run_tick_with_reconcile_gate(mm)

    mm.order_mgr.cancel_all.assert_awaited_once()
    assert mm._paused is True
    assert "Critical inventory drift" in mm._pause_reason
    assert mm.inventory.up_shares == pytest.approx(0.0)
    assert mm.inventory.dn_shares == pytest.approx(0.0)


def test_toxic_divergence_no_trade_cancels_quotes_when_flat():
    mm = _make_mm(live=True)
    mm._running = True
    mm.config.toxic_divergence_threshold = 0.02
    mm.config.toxic_divergence_ticks = 1
    mm._cached_usdc_balance = 100.0
    mm._cached_usdc_available_balance = 100.0
    mm.feed_state.mid = 100.0
    mm.feed_state.pm_up = 0.50
    mm.feed_state.pm_dn = 0.50
    mm.feed_state.pm_up_bid = 0.49
    mm.feed_state.pm_dn_bid = 0.49
    mm.feed_state.pm_last_update_ts = time.time()

    stale_bid = Quote(side="BUY", token_id=mm.market.up_token_id, price=0.40, size=5.0, order_id="oid-1")
    mm.order_mgr._active_orders = {"oid-1": stale_bid}
    mm._current_quotes = {"up": (stale_bid, None), "dn": (None, None)}

    mm.order_mgr.check_fills = AsyncMock(return_value=[])
    mm.order_mgr.cancel_all = AsyncMock(return_value=1)

    asyncio.run(mm._tick())

    mm.order_mgr.cancel_all.assert_awaited_once()
    assert mm._current_quotes == {"up": (None, None), "dn": (None, None)}
    assert mm._toxic_divergence_count >= 1


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


def test_snapshot_prefers_pm_balances_for_dashboard_inventory():
    mm = _make_mm(live=True)
    mm._cached_pm_up_shares = 0.0
    mm._cached_pm_dn_shares = 0.0
    mm._cached_usdc_balance = 42.0
    mm._cached_usdc_available_balance = 40.0

    # Simulate internal drift after missed fill reconciliation.
    mm.inventory.up_shares = 0.0
    mm.inventory.dn_shares = 8.1
    mm.inventory.usdc = 84.33
    mm.inventory.dn_cost.total_shares = 8.1
    mm.inventory.dn_cost.total_cost = 8.1 * 0.56

    snap = mm.snapshot()

    # Dashboard-facing inventory must reflect PM-cached balances.
    assert snap["inventory"]["up_shares"] == pytest.approx(0.0)
    assert snap["inventory"]["dn_shares"] == pytest.approx(0.0)
    assert snap["inventory"]["usdc"] == pytest.approx(42.0)
    assert snap["usdc_balance_pm"] == pytest.approx(42.0)
    assert snap["usdc_free_pm"] == pytest.approx(40.0)

    # Internal inventory remains available for diagnostics.
    assert snap["inventory_internal"]["dn_shares"] == pytest.approx(8.1)
    assert snap["inventory_internal"]["usdc"] == pytest.approx(84.33)


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
    mm.order_mgr.get_usdc_balances = AsyncMock(return_value=(10.0, 10.0))

    asyncio.run(mm._liquidate_inventory())

    # Bot should remain in closing mode until window transition/end.
    assert mm._is_closing is True
    assert mm._running is False


def test_event_requote_suppressed_while_closing():
    mm = _make_mm(live=False)
    mm._is_closing = True

    events = mm._event_requote_snapshot(had_fill=False)

    assert events == []


def test_run_loop_throttles_event_storm():
    mm = _make_mm(live=False)
    mm.config.event_poll_interval_sec = 0.25
    mm._running = True
    mm._is_closing = False
    mm._requote_event.clear()

    ticks = {"count": 0}

    async def _fake_tick():
        ticks["count"] += 1
        if ticks["count"] >= 2:
            mm._running = False

    class _Event:
        event_type = "book_change"
        timestamp = time.time()
        detail = {}

    mm._tick = _fake_tick  # type: ignore[assignment]
    mm._event_requote_snapshot = lambda had_fill=False: [_Event()]  # type: ignore[assignment]

    started = time.monotonic()
    asyncio.run(mm._run_loop())
    elapsed = time.monotonic() - started

    assert ticks["count"] == 2
    assert elapsed >= 0.20


def test_liquidation_attempt_throttle_prevents_hot_retry():
    mm = _make_mm(live=True)
    mm._is_closing = True
    mm.market.window_end = time.time() + 60.0
    mm.config.liq_chunk_interval_sec = 5.0
    mm.config.liq_taker_threshold_sec = 10.0

    mm.order_mgr.get_token_balance = AsyncMock(return_value=0.0)
    mm.order_mgr.get_all_token_balances = AsyncMock(return_value=(0.0, 0.0))
    mm.order_mgr.get_book_summary = AsyncMock(return_value={"best_bid": None, "best_ask": None})
    mm.order_mgr.ensure_sell_allowance = AsyncMock(return_value=True)

    asyncio.run(mm._liquidate_inventory())
    first_calls = mm.order_mgr.get_token_balance.await_count

    # Immediate second attempt should be skipped by liquidation throttle.
    asyncio.run(mm._liquidate_inventory())
    assert mm.order_mgr.get_token_balance.await_count == first_calls


def test_tick_skips_quote_when_low_usdc_and_no_inventory():
    mm = _make_mm(live=True)
    mm._started_at = time.time()
    mm.feed_state.mid = 100.0
    mm._cached_usdc_balance = 1.0
    mm._cached_usdc_available_balance = 1.0
    mm.config.min_order_size_usd = 2.0
    mm.inventory.up_shares = 0.0
    mm.inventory.dn_shares = 0.0

    mm.order_mgr.check_fills = AsyncMock(return_value=[])
    mm.order_mgr.place_orders_batch = AsyncMock(return_value=[])
    mm._cancel_all_guarded = AsyncMock(return_value=0)  # type: ignore[assignment]

    asyncio.run(mm._tick())

    mm.order_mgr.place_orders_batch.assert_not_awaited()
    mm._cancel_all_guarded.assert_not_awaited()


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
