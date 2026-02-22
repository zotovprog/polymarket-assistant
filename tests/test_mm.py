import os
import sys
import time

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'src'))
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

import pytest
from backtest.data_loader import DataLoader
from backtest.simulator import MMSimulator
from mm.fair_value import FairValueEngine
from mm.mm_config import MMConfig
from mm.quote_engine import QuoteEngine
from mm.risk_manager import RiskManager
from mm.types import Fill, Inventory, MarketInfo, Quote


def test_inventory_limit_enforced() -> None:
    loader = DataLoader()
    data = loader.generate_pm_synthetic(
        n_ticks=3000,
        seed=42,
        volatility=0.01,
        spread_bps=300,
    )
    config = MMConfig(
        half_spread_bps=100,
        order_size_usd=10.0,
        max_inventory_shares=30.0,
    )

    result = MMSimulator(config).run(data, strike=0, window_duration_sec=300)

    assert result.max_inventory <= 30.0


def test_quote_engine_bid_lt_ask() -> None:
    engine = QuoteEngine(MMConfig(half_spread_bps=100, order_size_usd=10.0))
    inventory = Inventory()

    for fair_value in [0.1, 0.3, 0.5, 0.7, 0.9]:
        bid, ask = engine.generate_quotes(
            fair_value=fair_value,
            token_id="up-token",
            inventory=inventory,
        )
        assert bid.price < ask.price, f"bid={bid.price}, ask={ask.price}, fv={fair_value}"


def test_quote_engine_price_bounds() -> None:
    engine = QuoteEngine(MMConfig(half_spread_bps=100, order_size_usd=10.0))
    inventory = Inventory()

    for fair_value in [0.1, 0.3, 0.5, 0.7, 0.9]:
        bid, ask = engine.generate_quotes(
            fair_value=fair_value,
            token_id="up-token",
            inventory=inventory,
        )
        assert 0.01 <= bid.price <= 0.99
        assert 0.01 <= ask.price <= 0.99


def test_fair_value_range() -> None:
    engine = FairValueEngine()
    klines = [{"c": 100000.0 + i * 5.0} for i in range(30)]

    combos = [
        (100000.0, 100000.0, 300.0),
        (98000.0, 100000.0, 180.0),
        (102000.0, 100000.0, 180.0),
        (100000.0, 103000.0, 600.0),
        (100000.0, 97000.0, 600.0),
    ]

    for mid, strike, time_remaining in combos:
        fv_up, fv_dn = engine.compute(
            mid=mid,
            strike=strike,
            time_remaining_sec=time_remaining,
            klines=klines,
        )
        assert 0.01 <= fv_up <= 0.99
        assert 0.01 <= fv_dn <= 0.99


def test_risk_manager_pause_on_drawdown() -> None:
    manager = RiskManager(MMConfig(max_drawdown_usd=5.0, max_inventory_shares=100.0))
    manager.record_fill(
        Fill(
            ts=time.time(),
            side="BUY",
            token_id="up-token",
            price=0.90,
            size=10.0,
            fee=0.0,
        )
    )

    should_pause, reason = manager.should_pause(inventory=Inventory())

    assert should_pause is True
    assert "drawdown" in reason.lower()


def test_risk_manager_pause_on_inventory() -> None:
    manager = RiskManager(MMConfig(max_inventory_shares=5.0))
    inventory = Inventory(up_shares=6.0, dn_shares=0.0)

    should_pause, reason = manager.should_pause(inventory=inventory)

    assert should_pause is True
    assert "inventory" in reason.lower()


def test_mock_clob_client_lifecycle() -> None:
    import importlib
    import types

    if "aiohttp" not in sys.modules:
        sys.modules["aiohttp"] = types.ModuleType("aiohttp")

    web_server = importlib.import_module("web_server")
    MockClobClient = web_server.MockClobClient

    # Use fill_prob=0 to test basic lifecycle without random fills
    client = MockClobClient(fill_prob=0.0)

    signed = client.create_and_sign_order(
        {
            "token_id": "token-1",
            "price": 0.55,
            "size": 10.0,
            "side": "BUY",
        }
    )
    post_resp = client.post_order(signed, order_type="GTC")
    order_id = post_resp["orderID"]

    live_order = client.get_order(order_id)
    assert live_order["status"] == "LIVE"

    cancel_resp = client.cancel(order_id)
    assert cancel_resp["success"] is True

    cancelled_order = client.get_order(order_id)
    assert cancelled_order["status"] == "CANCELLED"


def test_mock_clob_client_fills() -> None:
    """Test that MockClobClient simulates fills with fill_prob=1.0."""
    import importlib
    import types

    if "aiohttp" not in sys.modules:
        sys.modules["aiohttp"] = types.ModuleType("aiohttp")

    web_server = importlib.import_module("web_server")
    MockClobClient = web_server.MockClobClient

    # 100% fill probability for deterministic test
    client = MockClobClient(fill_prob=1.0)

    signed = client.create_and_sign_order(
        {
            "token_id": "token-1",
            "price": 0.50,
            "size": 15.0,
            "side": "BUY",
        }
    )
    resp = client.post_order(signed, order_type="GTC")
    order_id = resp["orderID"]

    order = client.get_order(order_id)
    assert order["status"] == "MATCHED", f"Expected MATCHED, got {order['status']}"
    assert order["size_matched"] == 15.0, f"Expected 15.0, got {order['size_matched']}"


def test_mm_config_update() -> None:
    config = MMConfig(half_spread_bps=100.0)

    config.update(half_spread_bps=200)

    assert config.half_spread_bps == 200.0


def test_inventory_net_delta() -> None:
    inventory = Inventory(up_shares=10.0, dn_shares=5.0)

    assert inventory.net_delta == 5.0


def test_fill_notional() -> None:
    fill = Fill(
        ts=time.time(),
        side="BUY",
        token_id="up-token",
        price=0.55,
        size=10.0,
    )

    assert fill.notional == pytest.approx(5.5)
