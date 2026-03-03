from __future__ import annotations

import asyncio
import importlib
import os
import sys
import time
from types import SimpleNamespace

import pytest


BASE = os.path.dirname(os.path.dirname(__file__))
SRC = os.path.join(BASE, "src")
if SRC not in sys.path:
    sys.path.insert(0, SRC)
if BASE not in sys.path:
    sys.path.insert(0, BASE)

from mm.mm_config import MMConfig
from mm.order_manager import OrderManager
from mm.types import MarketInfo, Quote
from mm_v2.config import MMConfigV2
from mm_v2.runtime import MarketMakerV2


def _feed_state() -> SimpleNamespace:
    now = time.time()
    return SimpleNamespace(
        mid=100000.0,
        bids=[(99999.0, 1.0)],
        asks=[(100001.0, 1.0)],
        klines=[
            {"t": now - 300, "o": 99900.0, "h": 100100.0, "l": 99850.0, "c": 100000.0, "v": 100.0},
            {"t": now - 240, "o": 100000.0, "h": 100120.0, "l": 99920.0, "c": 100010.0, "v": 80.0},
        ],
        trades=[],
        pm_up=0.53,
        pm_dn=0.47,
        pm_last_update_ts=now,
    )


def _market() -> MarketInfo:
    now = time.time()
    return MarketInfo(
        coin="BTC",
        timeframe="15m",
        up_token_id="up-token-paper",
        dn_token_id="dn-token-paper",
        strike=100000.0,
        window_start=now,
        window_end=now + 900.0,
        condition_id="cond-paper",
        tick_size=0.01,
        min_order_size=5.0,
    )


@pytest.mark.asyncio
async def test_mmv2_paper_quotes_both_sides_from_flat(monkeypatch):
    web_server = importlib.import_module("web_server")
    monkeypatch.setattr(web_server.random, "random", lambda: 1.0)
    monkeypatch.setattr(web_server.random, "uniform", lambda a, _b: a)

    client = web_server.MockClobClient(fill_prob=0.0, usdc_balance=15.0)
    cfg = MMConfigV2(base_clip_usd=6.0, tick_interval_sec=10.0)
    mm = MarketMakerV2(_feed_state(), client, cfg)
    mm.set_market(_market())

    await mm.start()
    try:
        await mm._tick()
        snap = mm.snapshot()
        assert snap["lifecycle"] in {"quoting", "inventory_skewed", "defensive"}
        assert snap["quotes"]["up_bid"] is not None
        assert snap["quotes"]["up_ask"] is not None
        assert snap["quotes"]["dn_bid"] is not None
        assert snap["quotes"]["dn_ask"] is not None
        assert snap["execution"]["open_orders"] == 4
        assert snap["health"]["true_drift"] is False
        assert snap["risk"]["hard_mode"] == "none"
    finally:
        await mm.stop(liquidate=False)


@pytest.mark.asyncio
async def test_mmv2_runtime_rejects_invalid_strike(monkeypatch):
    web_server = importlib.import_module("web_server")
    runtime = web_server.MMRuntimeV2()

    async def _idle_feed(*_args, **_kwargs):
        await asyncio.sleep(60.0)

    monkeypatch.setattr(web_server._telegram, "switch_credentials", lambda **_kwargs: None)
    monkeypatch.setattr(web_server.feeds, "fetch_pm_tokens", lambda *_args, **_kwargs: ("up-token-paper", "dn-token-paper", "cond-paper"))
    monkeypatch.setattr(web_server.feeds, "ob_poller", _idle_feed)
    monkeypatch.setattr(web_server.feeds, "binance_feed", _idle_feed)
    monkeypatch.setattr(web_server.feeds, "pm_feed", _idle_feed)
    bad_market = _market()
    bad_market.strike = 0.0
    monkeypatch.setattr(runtime, "_build_market_info_from_tokens", lambda *_args, **_kwargs: bad_market)

    async def _noop_enrich(*_args, **_kwargs):
        return None

    monkeypatch.setattr(runtime, "_enrich_market_info", _noop_enrich)

    with pytest.raises(web_server.HTTPException) as exc:
        await runtime.start("BTC", "15m", paper_mode=True, initial_usdc=50.0, dev=True)

    assert exc.value.status_code == 503
    assert "valid strike" in str(exc.value.detail)
    assert runtime._running is False
    assert runtime.mm_v2 is None


@pytest.mark.asyncio
async def test_mock_short_sell_uses_shared_balances_and_reserved_usdc(monkeypatch):
    web_server = importlib.import_module("web_server")
    monkeypatch.setattr(web_server.random, "random", lambda: 0.0)
    monkeypatch.setattr(web_server.random, "uniform", lambda a, _b: a)

    market = _market()
    client = web_server.MockClobClient(fill_prob=0.0, usdc_balance=10.0)
    client.set_fair_values(0.53, 0.47, market, pm_prices={"up": 0.53, "dn": 0.47})
    cfg = MMConfig()
    cfg.allow_short_sells = True
    mgr = OrderManager(client, cfg)
    mgr.set_market_context(min_order_size=5.0, token_ids={market.up_token_id, market.dn_token_id})

    order_id = await mgr.place_order(
        Quote(side="SELL", token_id=market.up_token_id, price=0.60, size=5.0),
        post_only=True,
    )
    assert order_id
    assert mgr._mock_token_balances is client._mock_token_balances

    total_usdc, available_usdc = await mgr.get_usdc_balances(force_refresh=True)
    assert total_usdc == pytest.approx(10.0, abs=1e-6)
    assert available_usdc == pytest.approx(8.0, abs=1e-6)

    fills = await mgr.check_fills()
    assert fills
    up, dn = await mgr.get_all_token_balances(market.up_token_id, market.dn_token_id)
    assert up == pytest.approx(0.0, abs=1e-6)
    assert dn == pytest.approx(2.5, abs=1e-6)


@pytest.mark.asyncio
async def test_mmv2_paper_partial_fills_do_not_trigger_true_drift(monkeypatch):
    web_server = importlib.import_module("web_server")
    monkeypatch.setattr(web_server.random, "random", lambda: 0.0)
    monkeypatch.setattr(web_server.random, "uniform", lambda a, _b: a)

    client = web_server.MockClobClient(fill_prob=1.0, usdc_balance=50.0)
    cfg = MMConfigV2(session_budget_usd=50.0, base_clip_usd=6.0, tick_interval_sec=10.0)
    mm = MarketMakerV2(_feed_state(), client, cfg)
    mm.set_market(_market())

    await mm.start()
    try:
        await mm._tick()
        await mm._tick()
        snap = mm.snapshot()
        assert snap["analytics"]["fill_count"] > 0
        assert snap["health"]["true_drift"] is False
        assert snap["health"]["reconcile_status"] in {"ok", "settlement_lag"}
        assert snap["risk"]["hard_mode"] == "none"
        assert snap["lifecycle"] != "halted"
    finally:
        await mm.stop(liquidate=False)
