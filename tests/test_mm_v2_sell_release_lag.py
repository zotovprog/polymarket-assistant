from __future__ import annotations

import os
import sys
import pytest


BASE = os.path.dirname(os.path.dirname(__file__))
SRC = os.path.join(BASE, "src")
if SRC not in sys.path:
    sys.path.insert(0, SRC)
if BASE not in sys.path:
    sys.path.insert(0, BASE)

from mm.mm_config import MMConfig
from mm.order_manager import OrderManager
from mm.types import Quote


class _CancelOnlyClient:
    def cancel(self, _order_id: str):
        return {"ok": True}


class _MockPlaceClient:
    def __init__(self):
        self._orders: dict[str, dict] = {}
        self._order_idx = 0

    def create_and_sign_order(self, order_args: dict):
        return dict(order_args)

    def post_order(self, signed_order: dict, _order_type: str):
        self._order_idx += 1
        order_id = f"oid-{self._order_idx}"
        self._orders[order_id] = dict(signed_order)
        return {"orderID": order_id}


@pytest.mark.asyncio
async def test_recent_cancelled_sell_reserve_added_on_cancel():
    cfg = MMConfig()
    cfg.sell_release_grace_sec = 3.0
    om = OrderManager(_CancelOnlyClient(), cfg)
    oid = "sell-1"
    om._active_orders[oid] = Quote(side="SELL", token_id="tok-up", price=0.55, size=7.0)

    ok = await om.cancel_order(oid)
    assert ok is True
    reserve = om._recent_cancelled_sell_reserves.get(oid)
    assert reserve is not None
    assert reserve.token_id == "tok-up"
    assert reserve.remaining_size == pytest.approx(7.0)
    assert reserve.grace_until > reserve.cancelled_ts


def test_reserved_sell_inventory_includes_recent_cancelled_reserve():
    cfg = MMConfig()
    cfg.sell_release_grace_sec = 3.0
    om = OrderManager(_CancelOnlyClient(), cfg)
    om._active_orders["active-sell"] = Quote(side="SELL", token_id="tok-up", price=0.6, size=3.0)
    om._add_recent_cancelled_sell_reserve(
        "cancelled-sell",
        Quote(side="SELL", token_id="tok-up", price=0.6, size=2.0),
    )

    reserved = om._reserved_sell_inventory("tok-up")
    assert reserved == pytest.approx(5.0)


@pytest.mark.asyncio
async def test_get_reconcile_token_balance_uses_recent_cancelled_reserve(monkeypatch):
    cfg = MMConfig()
    cfg.sell_release_grace_sec = 3.0
    om = OrderManager(_CancelOnlyClient(), cfg)
    om._add_recent_cancelled_sell_reserve(
        "cancelled-sell",
        Quote(side="SELL", token_id="tok-up", price=0.6, size=4.0),
    )

    async def _fake_token_balance(_token_id: str):
        return 1.0

    monkeypatch.setattr(om, "get_token_balance", _fake_token_balance)
    balance = await om.get_reconcile_token_balance("tok-up", reference_shares=5.0)
    assert balance == pytest.approx(5.0)


@pytest.mark.asyncio
async def test_close_only_sell_blocks_immediate_repost_during_release_grace(monkeypatch):
    cfg = MMConfig()
    cfg.sell_release_grace_sec = 3.0
    om = OrderManager(_CancelOnlyClient(), cfg)
    om._add_recent_cancelled_sell_reserve(
        "cancelled-sell",
        Quote(side="SELL", token_id="tok-up", price=0.55, size=5.0),
    )

    async def _fake_token_balance(_token_id: str):
        return 5.0

    monkeypatch.setattr(om, "get_token_balance", _fake_token_balance)
    quote = Quote(side="SELL", token_id="tok-up", price=0.55, size=5.0)
    allowed = await om._enforce_close_only_sell(quote)
    assert allowed is False


@pytest.mark.asyncio
async def test_successful_new_sell_clears_recent_cancelled_reserve_for_token():
    cfg = MMConfig()
    cfg.sell_release_grace_sec = 3.0
    om = OrderManager(_MockPlaceClient(), cfg)
    om._add_recent_cancelled_sell_reserve(
        "cancelled-sell",
        Quote(side="SELL", token_id="tok-up", price=0.55, size=5.0),
    )

    order_id = await om._place_order_inner(
        Quote(side="SELL", token_id="tok-up", price=0.55, size=5.0),
        post_only=True,
    )
    assert order_id
    assert om._recent_cancelled_sell_inventory("tok-up") == pytest.approx(0.0)
    assert om._last_sellability_lag_reason == ""


def test_fill_reduces_recent_cancelled_reserve():
    cfg = MMConfig()
    cfg.sell_release_grace_sec = 3.0
    om = OrderManager(_CancelOnlyClient(), cfg)
    om._add_recent_cancelled_sell_reserve(
        "cancelled-sell",
        Quote(side="SELL", token_id="tok-up", price=0.55, size=6.0),
    )

    om._reduce_recent_cancelled_sell_reserves("tok-up", 2.5)
    assert om._recent_cancelled_sell_inventory("tok-up") == pytest.approx(3.5)
    om._reduce_recent_cancelled_sell_reserves("tok-up", 4.0)
    assert om._recent_cancelled_sell_inventory("tok-up") == pytest.approx(0.0)


def test_sell_reject_during_recent_cancel_is_classified_as_sellability_lag():
    cfg = MMConfig()
    cfg.sell_release_grace_sec = 3.0
    om = OrderManager(_CancelOnlyClient(), cfg)
    om._add_recent_cancelled_sell_reserve(
        "cancelled-sell",
        Quote(side="SELL", token_id="tok-up", price=0.55, size=3.0),
    )

    quote = Quote(side="SELL", token_id="tok-up", price=0.55, size=3.0)
    om._mark_reconcile_on_balance_reject(
        quote,
        reason="not enough balance / allowance",
        source="unit_test",
    )

    assert om.reconcile_requested is True
    assert "sellability_lag" in om._last_sellability_lag_reason
    assert om._sell_reject_cooldown_left("tok-up") > 0.0


def test_sell_balance_reject_with_lag_not_counted_as_transport_failure():
    cfg = MMConfig()
    cfg.sell_release_grace_sec = 3.0
    om = OrderManager(_CancelOnlyClient(), cfg)
    om._add_recent_cancelled_sell_reserve(
        "cancelled-sell",
        Quote(side="SELL", token_id="tok-up", price=0.55, size=3.0),
    )
    quote = Quote(side="SELL", token_id="tok-up", price=0.55, size=3.0)
    om._mark_reconcile_on_balance_reject(
        quote,
        reason="not enough balance",
        source="unit_test",
    )
    om._record_api_error(
        op="place_order",
        token_id="tok-up",
        status_code=400,
        message="not enough balance",
        details={"side": "SELL"},
    )
    stats = om.get_api_error_stats()
    assert stats["transport_total_by_op"] == {}
