import os, sys
from pathlib import Path
BASE = Path(__file__).resolve().parent.parent
SRC = BASE / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))
if str(BASE) not in sys.path:
    sys.path.insert(0, str(BASE))

import asyncio
import math
import time
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from pair_arb.config import PairArbConfig
from pair_arb.maker import MakerArbManager
from pair_arb.merger import MergeTrigger
from pair_arb.risk import ArbRiskManager
from pair_arb.types import ArbMarket, ArbOpportunity


def _market():
    return ArbMarket(
        coin="BTC",
        timeframe="5m",
        up_token_id="UP_TOKEN_123",
        dn_token_id="DN_TOKEN_456",
        condition_id="0x" + "ab" * 32,
    )


def _mock_order_mgr(usdc=30.0, place_results=None, up_book=None, dn_book=None):
    """Create mock OrderManager.

    place_results: list of return values for sequential place_order calls.
    """
    mgr = AsyncMock()

    default_book = {
        "best_bid": 0.40,
        "best_ask": 0.42,
        "bids": [{"price": 0.40, "size": 100}],
        "asks": [{"price": 0.42, "size": 100}],
    }

    def book_side_effect(token_id):
        if token_id == "UP_TOKEN_123":
            return up_book or default_book
        return dn_book or default_book

    mgr.get_full_book = AsyncMock(side_effect=book_side_effect)
    mgr.get_usdc_available_balance = AsyncMock(return_value=usdc)
    if place_results is not None:
        mgr.place_order = AsyncMock(side_effect=place_results)
    else:
        mgr.place_order = AsyncMock(return_value="order_123")
    mgr.cancel_order = AsyncMock()
    mgr.cancel_all = AsyncMock()
    mgr.get_all_token_balances = AsyncMock(return_value=(0, 0, usdc, 0))
    mgr.merge_positions = AsyncMock(
        return_value={"success": True, "tx_hash": "0xabc", "amount_usdc": 5.0}
    )
    mgr.invalidate_usdc_cache = MagicMock()
    return mgr


def _config(**overrides):
    cfg = PairArbConfig(**overrides)
    cfg.validate()
    return cfg


def _opp(cost=0.98, shares=5.0):
    m = _market()
    return ArbOpportunity(
        market=m,
        ask_up=0.50,
        ask_dn=0.48,
        size_up=100,
        size_dn=100,
        fee_up_per_share=0,
        fee_dn_per_share=0,
        net_shares_up=shares,
        net_shares_dn=shares,
        max_arb_shares=shares,
        gross_cost_per_pair=cost,
        total_cost_per_pair=cost,
        profit_per_pair=1.0 - cost,
        profit_usd=(1.0 - cost) * shares,
    )


@pytest.mark.asyncio
async def test_orphan_cleanup_dn_fails():
    mgr = _mock_order_mgr(
        place_results=["order_up_1", None],
        up_book={"best_bid": 0.40, "best_ask": 0.42},
        dn_book={"best_bid": 0.40, "best_ask": 0.42},
    )
    maker = MakerArbManager(_market(), mgr, _config(max_clip_shares=5))

    result = await maker.tick()

    mgr.cancel_order.assert_awaited_once_with("order_up_1")
    assert maker.up_order_id is None
    assert result["action"] == "orphan_cleanup"
    assert result["failed_leg"] == "dn"


@pytest.mark.asyncio
async def test_up_fails_dn_not_attempted():
    """If UP leg fails, DN leg should NOT be placed at all (no orphan possible)."""
    mgr = _mock_order_mgr(
        place_results=[None, "order_dn_1"],
        up_book={"best_bid": 0.40, "best_ask": 0.42},
        dn_book={"best_bid": 0.40, "best_ask": 0.42},
    )
    maker = MakerArbManager(_market(), mgr, _config(max_clip_shares=5))

    result = await maker.tick()

    # UP failed → DN never placed → no cancel needed
    assert maker.up_order_id is None
    assert maker.dn_order_id is None
    mgr.cancel_order.assert_not_awaited()
    # place_order called only once (UP attempt), DN skipped
    assert mgr.place_order.await_count == 1


@pytest.mark.asyncio
async def test_both_succeed_no_cleanup():
    mgr = _mock_order_mgr(place_results=["order_up_1", "order_dn_1"])
    maker = MakerArbManager(_market(), mgr, _config(max_clip_shares=5))

    result = await maker.tick()

    assert result["action"] == "posted"
    assert maker.up_order_id == "order_up_1"
    assert maker.dn_order_id == "order_dn_1"
    assert maker.to_dict()["has_both_orders"] is True
    mgr.cancel_order.assert_not_awaited()


@pytest.mark.asyncio
async def test_both_fail_no_crash():
    mgr = _mock_order_mgr(place_results=[None, None])
    maker = MakerArbManager(_market(), mgr, _config(max_clip_shares=5))

    result = await maker.tick()

    assert result is None
    assert maker.up_order_id is None
    assert maker.dn_order_id is None
    mgr.cancel_order.assert_not_awaited()


@pytest.mark.asyncio
async def test_clip_raised_low_price():
    mgr = _mock_order_mgr(
        place_results=["order_up", "order_dn"],
        up_book={"best_bid": 0.92, "best_ask": 0.93},
        dn_book={"best_bid": 0.06, "best_ask": 0.07},
    )
    maker = MakerArbManager(_market(), mgr, _config(max_clip_shares=5))

    result = await maker.tick()

    assert result["action"] == "posted"
    sizes = [call.args[0].size for call in mgr.place_order.call_args_list]
    # 40% balance cap: floor(30*0.40/0.98) = 12, but notional min ceil(1/0.06)=17
    # cap wins: clip = max(12, min_clip=5) = 12
    assert sizes == [12.0, 12.0]


@pytest.mark.asyncio
async def test_clip_normal_price():
    mgr = _mock_order_mgr(
        place_results=["order_up", "order_dn"],
        up_book={"best_bid": 0.50, "best_ask": 0.51},
        dn_book={"best_bid": 0.40, "best_ask": 0.41},
    )
    maker = MakerArbManager(_market(), mgr, _config(max_clip_shares=5))

    result = await maker.tick()

    assert result["action"] == "posted"
    sizes = [call.args[0].size for call in mgr.place_order.call_args_list]
    assert sizes == [5.0, 5.0]


@pytest.mark.asyncio
async def test_balance_skip():
    mgr = _mock_order_mgr(
        usdc=2.0,
        place_results=["order_up", "order_dn"],
        up_book={"best_bid": 0.50, "best_ask": 0.51},
        dn_book={"best_bid": 0.40, "best_ask": 0.41},
    )
    maker = MakerArbManager(_market(), mgr, _config(max_clip_shares=5, min_clip_shares=1.0))

    result = await maker.tick()

    assert result["action"] == "posted"
    sizes = [call.args[0].size for call in mgr.place_order.call_args_list]
    assert all(size < 5.0 for size in sizes)
    # 40% cap: floor(2.0*0.40/0.90)=0, clamped to min_clip=1.0
    assert sizes == [1.0, 1.0]


@pytest.mark.asyncio
async def test_balance_skip_too_low():
    mgr = _mock_order_mgr(
        usdc=0.10,
        place_results=["order_up", "order_dn"],
        up_book={"best_bid": 0.50, "best_ask": 0.51},
        dn_book={"best_bid": 0.40, "best_ask": 0.41},
    )
    maker = MakerArbManager(_market(), mgr, _config(min_clip_shares=1.0))

    result = await maker.tick()

    assert result is None
    mgr.place_order.assert_not_awaited()


@pytest.mark.asyncio
async def test_merge_fires():
    mgr = _mock_order_mgr()
    mgr.get_all_token_balances = AsyncMock(return_value=(5.0, 5.0, 30.0, 0.0))
    merger = MergeTrigger(mgr, _config(min_clip_shares=1.0), "priv_key_123")
    merger.last_merge_ts = time.time() - 60.0

    result = await merger.check_and_merge(_market())

    assert result["merged"] == 5.0
    mgr.merge_positions.assert_awaited_once_with(
        _market().condition_id,
        5.0,
        "priv_key_123",
    )


@pytest.mark.asyncio
async def test_merge_skip_below_threshold():
    mgr = _mock_order_mgr()
    mgr.get_all_token_balances = AsyncMock(return_value=(0.5, 0.5, 30.0, 0.0))
    merger = MergeTrigger(mgr, _config(min_clip_shares=1.0), "priv_key_123")
    merger.last_merge_ts = time.time() - 60.0

    result = await merger.check_and_merge(_market())

    assert result is None
    mgr.merge_positions.assert_not_awaited()


@pytest.mark.asyncio
async def test_asymmetric_detected():
    mgr = _mock_order_mgr()
    mgr.get_all_token_balances = AsyncMock(return_value=(10.0, 0.0, 30.0, 0.0))
    merger = MergeTrigger(mgr, _config(min_clip_shares=1.0), "priv_key_123")

    result = await merger.check_asymmetric_fills(_market(), threshold=1.0)

    assert result is not None
    assert result["heavy_side"] == "up"
    assert result["diff"] == 10.0


@pytest.mark.asyncio
async def test_risk_cooldown():
    await asyncio.sleep(0)
    risk = ArbRiskManager(_config())
    risk.last_arb_ts = time.time()

    allowed, reason = risk.can_execute(_opp())

    assert (allowed, reason) == (False, "cooldown")


@pytest.mark.asyncio
async def test_risk_budget_exceeded():
    risk = ArbRiskManager(_config())
    risk.session_spent = 24.0

    allowed, reason = risk.can_execute(_opp(cost=0.98, shares=5.0))

    assert (allowed, reason) == (False, "budget_exceeded")


@pytest.mark.asyncio
async def test_risk_drawdown():
    risk = ArbRiskManager(_config())
    risk.session_pnl = -6.0

    allowed, reason = risk.can_execute(_opp())

    assert (allowed, reason) == (False, "drawdown_limit")


@pytest.mark.asyncio
async def test_config_defaults():
    await asyncio.sleep(0)
    cfg = PairArbConfig()

    assert cfg.min_clip_shares == 5.0
    assert cfg.session_budget_usd == 25.0
    assert cfg.max_unmerged_exposure_usd == 15.0
    assert cfg.hard_drawdown_usd == 5.0
