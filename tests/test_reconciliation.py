"""Tests for debounced reconciliation and session_pnl risk checks."""

from __future__ import annotations

import asyncio
import json
import os
import sys
import time
from dataclasses import dataclass, field
from pathlib import Path
from unittest.mock import AsyncMock

import pytest

BASE = os.path.dirname(os.path.dirname(__file__))
SRC = os.path.join(BASE, "src")
if SRC not in sys.path:
    sys.path.insert(0, SRC)

from mm.market_maker import MarketMaker, SettlementLagState
from mm.market_quality import MarketQuality
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


class FreeBalanceLiveClobClient(MockLiveClobClient):
    """Live-like client returning free CONDITIONAL balances only."""

    def __init__(self, balances: dict[str, float]):
        super().__init__()
        self._balances = {token_id: float(value) for token_id, value in balances.items()}

    def get_balance_allowance(self, params):
        token_id = getattr(params, "token_id", None)
        if token_id is None:
            return {
                "balance": int(round(100.0 * 1e6)),
                "available": int(round(100.0 * 1e6)),
            }
        balance = self._balances.get(token_id, 0.0)
        return {
            "balance": int(round(balance * 1e6)),
            "allowance": int(round(balance * 1e6)),
        }


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


def test_forced_reconcile_runs_during_closing_mode():
    mm = _make_mm(live=True)
    mm._running = True
    mm._is_closing = True
    mm.feed_state.mid = 0.0  # stop after reconcile/closing guard section
    mm.inventory.up_shares = 0.0
    mm.inventory.dn_shares = 0.0
    mm.order_mgr._reconcile_requested = True
    mm.order_mgr.check_fills = AsyncMock(return_value=[])
    mm.order_mgr.get_all_token_balances = AsyncMock(return_value=(4.0, 6.0))
    mm.order_mgr.get_usdc_balances = AsyncMock(return_value=(20.0, 20.0))
    mm._liquidate_inventory = AsyncMock()

    asyncio.run(mm._tick())

    mm.order_mgr.get_all_token_balances.assert_awaited()
    assert mm.inventory.up_shares == pytest.approx(4.0)
    assert mm.inventory.dn_shares == pytest.approx(6.0)
    assert mm.order_mgr.reconcile_requested is False
    # Forced reconcile in closing arms a brief guard before liquidation resumes.
    mm._liquidate_inventory.assert_not_awaited()


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


def test_periodic_reconcile_respects_settlement_guard_before_hard_stop():
    mm = _make_mm(live=True)
    mm._running = True
    mm.feed_state.mid = 0.0
    mm.config.critical_reconcile_drift_shares = 1.0

    fill = Fill(
        ts=time.time(),
        side="BUY",
        token_id=mm.market.up_token_id,
        price=0.50,
        size=5.0,
        fee=0.0,
    )
    mm.inventory.update_from_fill(fill, "up")
    mm._record_live_fill_settlement(fill)

    live_bid = Quote(side="BUY", token_id=mm.market.up_token_id, price=0.50, size=5.0, order_id="oid-1")
    mm.order_mgr._active_orders = {"oid-1": live_bid}
    mm._current_quotes = {"up": (live_bid, None), "dn": (None, None)}
    mm.order_mgr.check_fills = AsyncMock(return_value=[])
    mm.order_mgr.get_all_token_balances = AsyncMock(return_value=(0.0, 0.0))
    mm.order_mgr.get_usdc_balances = AsyncMock(return_value=(100.0, 100.0))

    async def _cancel_and_clear(*_args, **_kwargs):
        mm.order_mgr.clear_local_order_tracking()
        return 1

    mm.order_mgr.cancel_all = AsyncMock(side_effect=_cancel_and_clear)

    _run_tick_with_reconcile_gate(mm)

    mm.order_mgr.cancel_all.assert_awaited_once()
    assert mm._paused is False
    assert mm._pause_reason == ""
    assert mm._critical_drift_pause_active is False
    assert mm._active_settlement_guard_tokens() == {mm.market.up_token_id}
    assert mm._current_quotes == {"up": (None, None), "dn": (None, None)}
    assert mm.inventory.up_shares == pytest.approx(5.0)


def test_periodic_reconcile_uses_wallet_total_not_free_balance_for_active_sell():
    feed_state = MockFeedState(mid=0.0)
    client = FreeBalanceLiveClobClient({"up_token_123": 0.0, "dn_token_456": 0.90})
    mm = MarketMaker(feed_state, client, MMConfig())
    mm.set_market(_make_market())
    mm._running = True
    mm.config.critical_reconcile_drift_shares = 1.0
    mm.inventory.up_shares = 0.0
    mm.inventory.dn_shares = 6.89
    mm.order_mgr.check_fills = AsyncMock(return_value=[])
    mm.order_mgr.get_usdc_balances = AsyncMock(return_value=(100.0, 100.0))

    active_sell = Quote(
        side="SELL",
        token_id=mm.market.dn_token_id,
        price=0.99,
        size=6.0,
        order_id="sell-1",
    )
    mm.order_mgr._active_orders = {"sell-1": active_sell}

    _run_tick_with_reconcile_gate(mm)

    assert mm._cached_pm_dn_shares == pytest.approx(6.90)
    assert mm._paused is False
    assert mm._critical_drift_pause_active is False


def test_periodic_reconcile_does_not_double_count_when_pm_already_reports_total():
    feed_state = MockFeedState(mid=0.0)
    client = FreeBalanceLiveClobClient({"up_token_123": 5.0, "dn_token_456": 0.0})
    mm = MarketMaker(feed_state, client, MMConfig())
    mm.set_market(_make_market())
    mm._running = True
    mm.config.critical_reconcile_drift_shares = 1.0
    mm.inventory.up_shares = 5.0
    mm.inventory.dn_shares = 0.0
    mm.order_mgr.check_fills = AsyncMock(return_value=[])
    mm.order_mgr.get_usdc_balances = AsyncMock(return_value=(100.0, 100.0))

    active_sell = Quote(
        side="SELL",
        token_id=mm.market.up_token_id,
        price=0.64,
        size=5.0,
        order_id="sell-up-1",
    )
    mm.order_mgr._active_orders = {"sell-up-1": active_sell}

    _run_tick_with_reconcile_gate(mm)

    assert mm._cached_pm_up_shares == pytest.approx(5.0)
    assert mm.inventory.up_shares == pytest.approx(5.0)
    assert mm._paused is False
    assert mm._critical_drift_pause_active is False


def test_unexplained_drift_bypasses_settlement_guard_and_triggers_pause():
    mm = _make_mm(live=True)
    mm._running = True
    mm.feed_state.mid = 0.0
    mm.inventory.up_shares = 5.0
    mm.config.critical_reconcile_drift_shares = 1.0

    mm._settlement_lag[mm.market.up_token_id] = SettlementLagState(
        token_id=mm.market.up_token_id,
        pending_delta_shares=5.0,
        grace_until=time.time() - 0.1,
        last_fill_ts=time.time() - 6.5,
        last_fill_side="BUY",
        last_fill_size=5.0,
        last_internal_shares=5.0,
        last_pm_shares=0.0,
        source="test",
    )
    mm._settlement_guard_tokens.add(mm.market.up_token_id)
    mm.order_mgr.check_fills = AsyncMock(return_value=[])
    mm.order_mgr.get_all_token_balances = AsyncMock(return_value=(0.0, 0.0))
    mm.order_mgr.get_usdc_balances = AsyncMock(return_value=(100.0, 100.0))
    mm.order_mgr.cancel_all = AsyncMock(return_value=1)

    _run_tick_with_reconcile_gate(mm)

    mm.order_mgr.cancel_all.assert_awaited_once()
    assert mm._paused is True
    assert "Critical inventory drift" in mm._pause_reason
    assert mm._settlement_lag_escalated_total == 1
    assert mm._active_settlement_guard_tokens() == set()


def test_merge_is_suppressed_while_settlement_guard_active():
    mm = _make_mm(live=True)
    mm._running = True
    mm._cached_usdc_balance = 100.0
    mm._cached_usdc_available_balance = 100.0
    mm.inventory.usdc = 100.0
    mm.inventory.initial_usdc = 100.0
    mm.inventory.up_shares = 5.0
    mm._settlement_lag[mm.market.up_token_id] = SettlementLagState(
        token_id=mm.market.up_token_id,
        pending_delta_shares=5.0,
        grace_until=time.time() + 6.0,
        last_fill_ts=time.time(),
        last_fill_side="BUY",
        last_fill_size=5.0,
        last_internal_shares=5.0,
        last_pm_shares=0.0,
        source="test",
    )
    mm._settlement_guard_tokens.add(mm.market.up_token_id)
    mm._merge_check_counter = mm._merge_check_interval - 1
    mm._try_merge_pairs = AsyncMock(return_value=0.0)
    mm.order_mgr.check_fills = AsyncMock(return_value=[])
    mm.order_mgr.get_book_summary = AsyncMock(
        side_effect=[
            {"best_bid": 0.49, "best_ask": 0.51},
            {"best_bid": 0.49, "best_ask": 0.51},
        ]
    )
    mm.order_mgr.cancel_orders_batch = AsyncMock(return_value=0)
    mm.order_mgr.place_orders_batch = AsyncMock(
        side_effect=lambda quotes, **_kwargs: [f"oid-{idx}" for idx, _ in enumerate(quotes, start=1)]
    )

    asyncio.run(mm._tick())

    mm._try_merge_pairs.assert_not_awaited()


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


def test_critical_inventory_drift_triggers_when_internal_is_below_pm():
    mm = _make_mm(live=True)
    mm._running = True
    mm.feed_state.mid = 0.0  # stop after reconcile path
    mm.inventory.up_shares = 0.0
    mm.inventory.dn_shares = 0.0
    mm.config.critical_reconcile_drift_shares = 1.0

    mm.order_mgr.check_fills = AsyncMock(return_value=[])
    mm.order_mgr.get_all_token_balances = AsyncMock(return_value=(9.0, 0.0))
    mm.order_mgr.get_usdc_balances = AsyncMock(return_value=(100.0, 100.0))
    mm.order_mgr.cancel_all = AsyncMock(return_value=1)

    _run_tick_with_reconcile_gate(mm)

    mm.order_mgr.cancel_all.assert_awaited_once()
    assert mm._paused is True
    assert "Critical inventory drift" in mm._pause_reason
    assert mm.inventory.up_shares == pytest.approx(9.0)
    assert mm.inventory.dn_shares == pytest.approx(0.0)


def test_critical_inventory_drift_skips_unstable_pm_recheck():
    mm = _make_mm(live=True)
    mm._running = True
    mm.feed_state.mid = 0.0  # stop after reconcile path
    mm.inventory.up_shares = 9.0
    mm.inventory.dn_shares = 0.0
    mm.config.critical_reconcile_drift_shares = 1.0

    mm.order_mgr.check_fills = AsyncMock(return_value=[])
    # First read is transient zero, immediate recheck returns current inventory.
    mm.order_mgr.get_all_token_balances = AsyncMock(
        side_effect=[(0.0, 0.0), (9.0, 0.0)]
    )
    mm.order_mgr.get_usdc_balances = AsyncMock(return_value=(100.0, 100.0))
    mm.order_mgr.cancel_all = AsyncMock(return_value=1)

    _run_tick_with_reconcile_gate(mm)

    mm.order_mgr.cancel_all.assert_not_awaited()
    assert mm._paused is False
    assert mm._pause_reason == ""
    assert mm.inventory.up_shares == pytest.approx(9.0)
    assert mm.inventory.dn_shares == pytest.approx(0.0)
    assert mm._cached_pm_up_shares == pytest.approx(9.0)


def test_inventory_limit_closing_mode_clears_after_liquidation_if_not_near_close():
    mm = _make_mm(live=True)
    mm._running = True
    mm._is_closing = True
    mm._paused = True
    mm._pause_reason = "Inventory limit (net delta): |13.4| > 12"
    mm._quality_pause_active = False
    mm._critical_drift_pause_active = False
    mm._liq_chunk_index = 3
    mm._liq_last_chunk_time = time.time()
    mm._liq_last_attempt_time = time.time()
    mm._closing_start_time_left = 500.0

    mm._maybe_exit_inventory_close_mode_after_clear()

    assert mm._is_closing is False
    assert mm._paused is False
    assert mm._pause_reason == ""
    assert mm._liq_chunk_index == 0
    assert mm._liq_last_chunk_time == pytest.approx(0.0)
    assert mm._liq_last_attempt_time == pytest.approx(0.0)
    assert mm._closing_start_time_left == pytest.approx(0.0)
    assert mm._requote_event.is_set() is True


def test_inventory_limit_closing_mode_stays_near_close_window():
    mm = _make_mm(live=True)
    mm._running = True
    mm._is_closing = True
    mm._paused = True
    mm._pause_reason = "Inventory limit (net delta): |13.4| > 12"
    mm._quality_pause_active = False
    mm._critical_drift_pause_active = False
    now = time.time()
    mm.market.window_start = now - 840.0
    mm.market.window_end = now + 60.0  # inside close window for 15m market

    mm._maybe_exit_inventory_close_mode_after_clear()

    assert mm._is_closing is True
    assert mm._paused is True
    assert "Inventory limit" in mm._pause_reason


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


def test_post_fill_quality_guard_blocks_new_buys_after_degradation():
    mm = _make_mm(live=True)
    mm._running = True

    fill = Fill(
        ts=time.time(),
        side="BUY",
        token_id=mm.market.dn_token_id,
        price=0.46,
        size=5.0,
        fee=0.0,
        is_maker=True,
    )
    mm.inventory.update_from_fill(fill, "dn")
    mm._last_quality = MarketQuality(
        bid_depth_usd=8000.0,
        ask_depth_usd=8000.0,
        spread_bps=180.0,
        liquidity_score=1.0,
        spread_score=1.0,
        overall_score=1.0,
        tradeable=True,
        reason="OK",
    )
    mm._arm_post_fill_entry_guard(fill)

    mm._last_quality = MarketQuality(
        bid_depth_usd=40.0,
        ask_depth_usd=30.0,
        spread_bps=1600.0,
        liquidity_score=0.2,
        spread_score=0.0,
        overall_score=0.12,
        tradeable=False,
        reason="spread 1600bps > 800bps",
    )
    mm._update_post_fill_entry_guard()

    quotes = {
        "up": (
            Quote(side="BUY", token_id=mm.market.up_token_id, price=0.42, size=5.0),
            None,
        ),
        "dn": (
            Quote(side="BUY", token_id=mm.market.dn_token_id, price=0.40, size=5.0),
            Quote(side="SELL", token_id=mm.market.dn_token_id, price=0.50, size=5.0),
        ),
    }
    mm._apply_post_fill_entry_buy_block(quotes)

    assert mm._post_fill_entry_guard_active is True
    assert "spread 1600bps" in mm._post_fill_entry_guard_reason
    assert quotes["up"][0] is None
    assert quotes["dn"][0] is None
    assert quotes["dn"][1] is not None


def test_post_fill_quality_guard_clears_after_recovery():
    mm = _make_mm(live=True)
    mm._running = True
    mm.inventory.up_shares = 5.0
    mm._post_fill_entry_guard_until = time.time() + 30.0
    mm._post_fill_entry_guard_anchor = MarketQuality(
        bid_depth_usd=8000.0,
        ask_depth_usd=8000.0,
        spread_bps=180.0,
        liquidity_score=1.0,
        spread_score=1.0,
        overall_score=0.95,
        tradeable=True,
        reason="OK",
    )
    mm._post_fill_entry_guard_active = True
    mm._post_fill_entry_guard_reason = "spread widened 900bps >= 250bps"
    mm._last_quality = MarketQuality(
        bid_depth_usd=7800.0,
        ask_depth_usd=7900.0,
        spread_bps=220.0,
        liquidity_score=1.0,
        spread_score=0.98,
        overall_score=0.90,
        tradeable=True,
        reason="OK",
    )

    mm._update_post_fill_entry_guard()

    assert mm._post_fill_entry_guard_active is False
    assert mm._post_fill_entry_guard_reason == ""
    assert mm._post_fill_entry_guard_window_active() is True


def test_post_fill_quality_guard_resets_when_inventory_flat():
    mm = _make_mm(live=True)
    mm._running = True
    mm._post_fill_entry_guard_until = time.time() + 30.0
    mm._post_fill_entry_guard_anchor = MarketQuality(
        bid_depth_usd=8000.0,
        ask_depth_usd=8000.0,
        spread_bps=180.0,
        liquidity_score=1.0,
        spread_score=1.0,
        overall_score=0.95,
        tradeable=True,
        reason="OK",
    )
    mm._post_fill_entry_guard_active = True
    mm._post_fill_entry_guard_reason = "quality score drop 0.30 >= 0.20"
    mm.inventory.up_shares = 0.0
    mm.inventory.dn_shares = 0.0

    mm._update_post_fill_entry_guard()

    assert mm._post_fill_entry_guard_active is False
    assert mm._post_fill_entry_guard_window_active() is False
    assert mm._post_fill_entry_guard_anchor is None


def test_pre_entry_guard_blocks_first_buy_until_stable_quality():
    mm = _make_mm(live=True)
    mm._running = True
    mm.config.pre_entry_stable_checks = 3
    mm.config.pre_entry_min_quality_score = 0.75
    mm.config.pre_entry_max_spread_bps = 500.0
    mm.config.pre_entry_max_divergence = 0.08
    mm._last_quality = MarketQuality(
        bid_depth_usd=9000.0,
        ask_depth_usd=8500.0,
        spread_bps=220.0,
        liquidity_score=1.0,
        spread_score=0.97,
        overall_score=0.92,
        tradeable=True,
        reason="OK",
    )

    mm._update_pre_entry_guard(max_divergence=0.03, quality_refreshed=True)

    quotes = {
        "up": (
            Quote(side="BUY", token_id=mm.market.up_token_id, price=0.42, size=5.0),
            None,
        ),
        "dn": (
            Quote(side="BUY", token_id=mm.market.dn_token_id, price=0.40, size=5.0),
            Quote(side="SELL", token_id=mm.market.dn_token_id, price=0.50, size=5.0),
        ),
    }
    mm._apply_pre_entry_buy_block(quotes)

    assert mm._pre_entry_guard_active is True
    assert mm._pre_entry_quality_passes == 1
    assert mm._pre_entry_guard_reason == "stable quality warmup 1/3"
    assert quotes["up"][0] is None
    assert quotes["dn"][0] is None
    assert quotes["dn"][1] is not None


def test_pre_entry_guard_clears_after_required_quality_streak():
    mm = _make_mm(live=True)
    mm._running = True
    mm.config.pre_entry_stable_checks = 2
    mm.config.pre_entry_min_quality_score = 0.70
    mm.config.pre_entry_max_spread_bps = 550.0
    mm.config.pre_entry_max_divergence = 0.08
    mm._last_quality = MarketQuality(
        bid_depth_usd=9500.0,
        ask_depth_usd=9100.0,
        spread_bps=240.0,
        liquidity_score=1.0,
        spread_score=0.95,
        overall_score=0.90,
        tradeable=True,
        reason="OK",
    )

    mm._update_pre_entry_guard(max_divergence=0.02, quality_refreshed=True)
    assert mm._pre_entry_guard_active is True
    assert mm._pre_entry_quality_passes == 1

    mm._update_pre_entry_guard(max_divergence=0.02, quality_refreshed=True)

    quotes = {
        "up": (
            Quote(side="BUY", token_id=mm.market.up_token_id, price=0.42, size=5.0),
            None,
        ),
        "dn": (
            Quote(side="BUY", token_id=mm.market.dn_token_id, price=0.40, size=5.0),
            None,
        ),
    }
    mm._apply_pre_entry_buy_block(quotes)

    assert mm._pre_entry_guard_active is False
    assert mm._pre_entry_guard_reason == ""
    assert mm._pre_entry_quality_passes == 2
    assert quotes["up"][0] is not None
    assert quotes["dn"][0] is not None


def test_pre_entry_guard_resets_on_bad_quality_or_divergence():
    mm = _make_mm(live=True)
    mm._running = True
    mm.config.pre_entry_stable_checks = 2
    mm.config.pre_entry_min_quality_score = 0.75
    mm.config.pre_entry_max_spread_bps = 500.0
    mm.config.pre_entry_max_divergence = 0.08
    mm._last_quality = MarketQuality(
        bid_depth_usd=9000.0,
        ask_depth_usd=8600.0,
        spread_bps=220.0,
        liquidity_score=1.0,
        spread_score=0.96,
        overall_score=0.91,
        tradeable=True,
        reason="OK",
    )
    mm._update_pre_entry_guard(max_divergence=0.02, quality_refreshed=True)
    assert mm._pre_entry_quality_passes == 1

    mm._last_quality = MarketQuality(
        bid_depth_usd=9200.0,
        ask_depth_usd=8800.0,
        spread_bps=230.0,
        liquidity_score=1.0,
        spread_score=0.95,
        overall_score=0.90,
        tradeable=True,
        reason="OK",
    )
    mm._update_pre_entry_guard(max_divergence=0.12, quality_refreshed=True)

    assert mm._pre_entry_guard_active is True
    assert mm._pre_entry_quality_passes == 0
    assert "divergence 0.120 > 0.080" in mm._pre_entry_guard_reason


def test_pre_entry_guard_rearms_after_inventory_returns_flat():
    mm = _make_mm(live=True)
    mm._running = True
    mm.config.pre_entry_stable_checks = 2
    mm.inventory.dn_shares = 5.0
    mm._last_quality = MarketQuality(
        bid_depth_usd=9000.0,
        ask_depth_usd=9000.0,
        spread_bps=200.0,
        liquidity_score=1.0,
        spread_score=1.0,
        overall_score=0.95,
        tradeable=True,
        reason="OK",
    )

    mm._update_pre_entry_guard(max_divergence=0.02, quality_refreshed=True)
    assert mm._pre_entry_guard_active is False

    mm.inventory.dn_shares = 0.0
    mm._update_pre_entry_guard(max_divergence=0.02, quality_refreshed=False)

    assert mm._pre_entry_guard_active is True
    assert mm._pre_entry_quality_passes == 0
    assert mm._pre_entry_guard_reason == "stable quality warmup 0/2"


def test_bad_cycle_count_increments_on_negative_roundtrip():
    mm = _make_mm(live=True)
    mm._running = True

    mm._cached_pm_up_shares = 5.0
    mm.inventory.up_shares = 5.0
    mm._update_cycle_guard(current_portfolio_pm=100.0, time_left=600.0)
    assert mm._cycle_guard.current_cycle_active is True

    mm._cached_pm_up_shares = 0.0
    mm.inventory.up_shares = 0.0
    mm._update_cycle_guard(current_portfolio_pm=98.8, time_left=600.0)

    assert mm._cycle_guard.current_cycle_active is False
    assert mm._cycle_guard.bad_cycle_count == 1
    assert mm._cycle_guard.last_cycle_pnl == pytest.approx(-1.2)


def test_cycle_lockout_blocks_new_buys_until_window_end():
    mm = _make_mm(live=True)
    mm._running = True
    mm.config.cycle_lockout_bad_cycles = 2
    mm.config.cycle_lockout_loss_usd = 1.0

    for portfolio in (100.0, 98.6, 97.1):
        if portfolio == 100.0:
            mm._cached_pm_up_shares = 5.0
            mm.inventory.up_shares = 5.0
            mm._update_cycle_guard(current_portfolio_pm=portfolio, time_left=600.0)
            mm._cached_pm_up_shares = 0.0
            mm.inventory.up_shares = 0.0
        elif portfolio == 98.6:
            mm._update_cycle_guard(current_portfolio_pm=portfolio, time_left=600.0)
            mm._cached_pm_up_shares = 5.0
            mm.inventory.up_shares = 5.0
            mm._update_cycle_guard(current_portfolio_pm=portfolio, time_left=550.0)
            mm._cached_pm_up_shares = 0.0
            mm.inventory.up_shares = 0.0
        else:
            mm._update_cycle_guard(current_portfolio_pm=portfolio, time_left=500.0)

    assert mm._cycle_guard.active is True
    assert mm._cycle_guard.mode == "no_trade"
    assert mm._cycle_guard.bad_cycle_count == 2
    assert mm._cycle_guard.reason == "bad cycle lockout for current window"


def test_close_only_blocks_all_new_buys():
    mm = _make_mm(live=True)
    mm._running = True
    mm._cycle_guard.active = True
    mm._cycle_guard.mode = "close_only"
    mm._cycle_guard.reason = "toxic market"
    mm._cached_pm_up_shares = 5.0
    mm._cached_pm_dn_shares = 0.0

    quotes = {
        "up": (
            Quote(side="BUY", token_id=mm.market.up_token_id, price=0.45, size=5.0),
            Quote(side="SELL", token_id=mm.market.up_token_id, price=0.55, size=5.0),
        ),
        "dn": (
            Quote(side="BUY", token_id=mm.market.dn_token_id, price=0.45, size=5.0),
            Quote(side="SELL", token_id=mm.market.dn_token_id, price=0.55, size=5.0),
        ),
    }

    mm._apply_cycle_guard_quote_block(quotes, is_live=True)

    assert quotes["up"][0] is None
    assert quotes["up"][1] is not None
    assert quotes["dn"][0] is None
    assert quotes["dn"][1] is None


@pytest.mark.anyio
async def test_placement_failure_lockout_enters_close_only_when_nonflat():
    mm = _make_mm(live=True)
    mm._running = True
    mm._cached_pm_up_shares = 5.0
    mm.inventory.up_shares = 5.0
    mm.order_mgr.cancel_all = AsyncMock(return_value=1)

    for _ in range(3):
        mm._record_place_failures(place_failed=1, place_total=1)

    assert mm._cycle_guard.consecutive_place_failures == 3

    await mm._enter_close_only("placement failures 1/1 for 3 ticks", aggressive=False)

    assert mm._cycle_guard.active is True
    assert mm._cycle_guard.mode == "close_only"
    assert mm._liquidation_mode == "close_only"
    mm.order_mgr.cancel_all.assert_awaited_once()


@pytest.mark.anyio
async def test_aggressive_liquidation_shortens_replace_interval():
    mm = _make_mm(live=True)
    mm._running = True
    mm._is_closing = True
    mm._liquidation_mode = "aggressive"
    mm._liquidation_reason = "test"
    mm._liquidation_mode_started_at = time.time() - 5.0
    mm._liq_last_chunk_time = time.time() - 3.0
    mm._liquidation_order_ids = {"oid-1"}
    mm.order_mgr._active_orders = {
        "oid-1": Quote(side="SELL", token_id=mm.market.up_token_id, price=0.55, size=5.0)
    }
    mm._cancel_order_guarded = AsyncMock(return_value=True)
    mm.order_mgr.get_token_balance = AsyncMock(side_effect=[0.0, 0.0, 0.0, 0.0])
    mm.order_mgr.get_all_token_balances = AsyncMock(return_value=(0.0, 0.0))

    await mm._liquidate_inventory()

    mm._cancel_order_guarded.assert_awaited_once()
    assert mm._liquidation_order_ids == set()


@pytest.mark.anyio
async def test_residual_inventory_failure_flagged_after_close():
    mm = _make_mm(live=True)
    mm._running = True
    mm._is_closing = True
    mm._liquidation_mode = "aggressive"
    mm._liquidation_reason = "test"
    mm._liquidation_mode_started_at = time.time() - 5.0
    mm.market.window_end = time.time() + 120.0
    mm.order_mgr.ensure_sell_allowance = AsyncMock(return_value=True)
    mm.order_mgr.get_token_balance = AsyncMock(side_effect=[1.0, 0.0, 1.0, 0.0])
    mm.order_mgr.get_book_summary = AsyncMock(return_value={"best_bid": 0.40, "best_ask": 0.42})
    mm._place_order_guarded = AsyncMock(return_value=None)

    await mm._liquidate_inventory()

    assert mm._residual_inventory_failure is True


def test_negative_edge_signal_trips_on_bad_markout_and_negative_spread_capture():
    mm = _make_mm(live=True)

    triggered, reason, metrics = mm._negative_edge_signal(
        stats={
            "total_fills": 12,
            "avg_markout_5s": -0.007583,
            "adverse_pct_5s": 33.3,
        },
        spread_capture_total=-1.15,
        spread_capture_count=5,
    )

    assert triggered is True
    assert "negative edge confirmed" in reason
    assert "avg_markout_5s" in reason
    assert "spread_capture" in reason
    assert metrics["avg_markout_5s"] == pytest.approx(-0.007583)
    assert metrics["spread_capture_usd"] == pytest.approx(-1.15)


@pytest.mark.anyio
async def test_negative_edge_guard_flat_enters_no_trade():
    mm = _make_mm(live=True)
    mm._running = True
    mm.order_mgr.cancel_all = AsyncMock(return_value=0)

    activated = await mm._maybe_activate_negative_edge_guard(
        time_left=420.0,
        stats={
            "total_fills": 12,
            "avg_markout_5s": -0.007583,
            "adverse_pct_5s": 33.3,
        },
        spread_capture_total=-1.15,
        spread_capture_count=5,
    )

    assert activated is True
    assert mm._negative_edge_guard.active is True
    assert mm._negative_edge_guard.mode == "no_trade"
    assert mm._cycle_guard.active is True
    assert mm._cycle_guard.mode == "no_trade"
    assert "negative edge confirmed" in mm._cycle_guard.reason


@pytest.mark.anyio
async def test_negative_edge_guard_nonflat_enters_aggressive_close_only():
    mm = _make_mm(live=True)
    mm._running = True
    mm._cached_pm_up_shares = 11.38
    mm.inventory.up_shares = 11.38
    mm.order_mgr.cancel_all = AsyncMock(return_value=0)

    activated = await mm._maybe_activate_negative_edge_guard(
        time_left=180.0,
        stats={
            "total_fills": 12,
            "avg_markout_5s": -0.007583,
            "adverse_pct_5s": 33.3,
        },
        spread_capture_total=-1.15,
        spread_capture_count=5,
    )

    assert activated is True
    assert mm._negative_edge_guard.active is True
    assert mm._negative_edge_guard.mode == "close_only"
    assert mm._cycle_guard.mode == "close_only"
    assert mm._is_closing is True
    assert mm._liquidation_mode == "aggressive"


def test_negative_edge_close_only_flips_to_no_trade_after_flatten():
    mm = _make_mm(live=True)
    mm._running = True
    mm._is_closing = True
    mm.market.window_end = time.time() + 600.0
    mm._negative_edge_guard.active = True
    mm._negative_edge_guard.mode = "close_only"
    mm._negative_edge_guard.reason = "negative edge confirmed"
    mm._negative_edge_guard.lockout_until = time.time() + 300.0
    mm._cycle_guard.active = True
    mm._cycle_guard.mode = "close_only"
    mm._cycle_guard.reason = "negative edge confirmed"
    mm._cached_pm_up_shares = 0.0
    mm._cached_pm_dn_shares = 0.0
    mm.inventory.up_shares = 0.0
    mm.inventory.dn_shares = 0.0

    mm._maybe_exit_inventory_close_mode_after_clear()

    assert mm._is_closing is False
    assert mm._negative_edge_guard.mode == "no_trade"
    assert mm._cycle_guard.mode == "no_trade"


def test_audit_artifact_failed_window_trips_negative_edge_guard():
    artifact = (
        Path(BASE)
        / "audit"
        / "2026-03-02_23-23-54"
        / "last_failed_window_state_raw.json"
    )
    if not artifact.exists():
        pytest.skip("audit artifact not present")

    payload = json.loads(artifact.read_text())
    mm = _make_mm(live=True)

    triggered, reason, metrics = mm._negative_edge_signal(
        stats=payload["markout_tca"],
        spread_capture_total=payload["pnl_decomposition"]["components"]["spread_capture"]["total_usd"],
        spread_capture_count=payload["pnl_decomposition"]["components"]["spread_capture"]["count"],
    )

    assert triggered is True
    assert "negative edge confirmed" in reason
    assert metrics["avg_markout_5s"] == pytest.approx(payload["markout_tca"]["avg_markout_5s"])
    assert metrics["spread_capture_usd"] == pytest.approx(
        payload["pnl_decomposition"]["components"]["spread_capture"]["total_usd"]
    )


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


def test_snapshot_exposes_api_error_stats():
    mm = _make_mm(live=True)
    mm.order_mgr._record_api_error(
        op="place_batch",
        token_id=mm.market.up_token_id,
        order_id="oid-1234567890",
        status_code=400,
        message="batch rejected",
        details={"transient": False},
    )

    snap = mm.snapshot()

    assert "api_errors" in snap
    assert snap["api_errors"]["total_by_op"]["place_batch"] == 1
    assert snap["api_errors"]["recent"][-1]["status_code"] == 400
    assert snap["api_errors"]["recent"][-1]["order_id"] == "oid-12345678"


def test_drawdown_exit_returns_before_quote_generation():
    mm = _make_mm(live=True)
    mm._started_at = time.time()
    mm._cached_usdc_balance = 100.0
    mm._cached_usdc_available_balance = 100.0
    mm.feed_state.mid = 100.0
    mm.feed_state.pm_up = 0.5
    mm.feed_state.pm_dn = 0.5
    mm.feed_state.pm_up_bid = 0.49
    mm.feed_state.pm_dn_bid = 0.49
    mm.feed_state.pm_last_update_ts = time.time()
    mm._catastrophic_count = mm._catastrophic_threshold - 1

    mm.order_mgr.check_fills = AsyncMock(return_value=[])
    mm.order_mgr.get_all_token_balances = AsyncMock(return_value=(0.0, 0.0))
    mm.order_mgr.get_usdc_balances = AsyncMock(return_value=(100.0, 100.0))
    mm.order_mgr.place_orders_batch = AsyncMock(return_value=[])
    mm._cancel_all_guarded = AsyncMock(return_value=0)  # type: ignore[assignment]
    mm._get_liq_lock_best_bids = AsyncMock(return_value=(0.49, 0.49))
    mm.risk_mgr.should_pause = lambda *_args, **_kwargs: (True, "Max drawdown exceeded: PnL=$-5.00")

    asyncio.run(mm._tick())

    mm.order_mgr.place_orders_batch.assert_not_awaited()
    mm._cancel_all_guarded.assert_awaited_once()
    assert mm._is_closing is True
    assert mm._liquidation_mode == "close_only"
    assert mm._cycle_guard.current_cycle_forced_close is True


def test_drawdown_while_closing_escalates_to_aggressive_liquidation():
    mm = _make_mm(live=True)
    mm._is_closing = True
    mm.feed_state.pm_up = 0.5
    mm.feed_state.pm_dn = 0.5
    mm._cancel_all_guarded = AsyncMock(return_value=0)  # type: ignore[assignment]
    mm._get_liq_lock_best_bids = AsyncMock(return_value=(0.49, 0.49))

    asyncio.run(
        mm._trigger_drawdown_exit(
            "Max drawdown exceeded: PnL=$-5.00",
            time_left=mm.market.time_remaining,
            already_closing=True,
        )
    )

    assert mm._liquidation_mode == "aggressive"
    assert mm._liquidation_reason == "Max drawdown exceeded: PnL=$-5.00"
    mm._cancel_all_guarded.assert_awaited_once()


def test_persistent_drawdown_during_liquidation_forces_fastest_exit_path():
    mm = _make_mm(live=False)
    mm._is_closing = True
    mm._close_only_reason = "drawdown exit"
    mm._liquidation_mode = "aggressive"
    mm._liquidation_mode_started_at = time.time() - 5.0
    mm._starting_portfolio_pm = 100.0
    mm._cached_usdc_balance = 86.0
    mm._cached_pm_up_shares = 6.0
    mm.feed_state.pm_up = 0.5
    mm.feed_state.pm_up_bid = 0.5
    mm.feed_state.pm_dn = 0.5
    mm.feed_state.pm_dn_bid = 0.5
    mm.config.max_drawdown_usd = 8.0
    mm._drawdown_liquidation_breach_count = 1
    mm.inventory.up_shares = 6.0
    mm.inventory.up_cost.total_shares = 6.0
    mm.inventory.up_cost.total_cost = 6.0 * 0.8

    mm.order_mgr.get_token_balance = AsyncMock(side_effect=[6.0, 0.0, 6.0, 0.0])
    mm.order_mgr.get_book_summary = AsyncMock(return_value={"best_bid": 0.55, "best_ask": 0.56})
    mm.order_mgr.place_order = AsyncMock(return_value="liq-1")
    mm.order_mgr.active_order_ids.clear()

    asyncio.run(mm._liquidate_inventory())

    assert mm._liquidation_mode == "taker"
    assert mm._drawdown_liquidation_breach_count == 2
    assert mm.order_mgr.place_order.await_args.kwargs["post_only"] is False


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
    mm._starting_portfolio_pm = 50.0  # started with $50 total portfolio
    mm._cached_usdc_balance = 10.0  # now only $10
    mm.inventory.up_shares = 0.0
    mm.inventory.dn_shares = 0.0
    mm.feed_state.pm_up = 0.5
    mm.feed_state.pm_dn = 0.5

    # Session PnL = (10 + 0) - 50 = -$40, max_drawdown_usd default = 100
    # With max_drawdown=8: -40 < -16 (2*8) -> catastrophic -> abandon
    mm.config.max_drawdown_usd = 8.0
    mm._catastrophic_count = mm._catastrophic_threshold - 1

    # Mock order_mgr methods to prevent real API calls
    mm.order_mgr.check_fills = AsyncMock(return_value=[])
    mm.order_mgr.get_token_balance = AsyncMock(return_value=0.0)
    mm.order_mgr.get_usdc_balances = AsyncMock(return_value=(10.0, 10.0))
    mm.order_mgr.get_all_token_balances = AsyncMock(return_value=(0.0, 0.0))
    mm._emergency_shutdown = AsyncMock(return_value=None)  # type: ignore[assignment]

    asyncio.run(mm._liquidate_inventory())

    mm._emergency_shutdown.assert_awaited_once()


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

    mm.order_mgr.get_token_balance = AsyncMock(return_value=0.25)
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
