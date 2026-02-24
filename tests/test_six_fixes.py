"""Tests for the 6 critical MM bot fixes:
1. Default spread 300 bps
2. Requote interval 1.0s
3. max_spread_bps 1000
4. max_one_sided_ticks 180
5. Skew inversion for DN tokens
6. Gamma-aware spread near expiry
"""
import os
import sys
import time

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'src'))
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

import pytest
from mm.mm_config import MMConfig
from mm.quote_engine import QuoteEngine
from mm.types import Inventory, MarketInfo


# ── Config defaults ────────────────────────────────────────────

def test_default_spread_300bps():
    config = MMConfig()
    assert config.half_spread_bps == 300.0


def test_default_requote_interval():
    config = MMConfig()
    assert config.requote_interval_sec == 1.0


def test_max_spread_raised():
    config = MMConfig()
    assert config.max_spread_bps == 1000.0


def test_one_sided_ticks_for_1s():
    config = MMConfig()
    assert config.max_one_sided_ticks == 180


# ── Skew inversion ─────────────────────────────────────────────

def test_skew_inversion_dn():
    config = MMConfig(half_spread_bps=100, skew_bps_per_unit=50.0, order_size_usd=10.0)
    engine = QuoteEngine(config)
    inv = Inventory(up_shares=10.0, dn_shares=0.0)

    # UP: skew positive (long UP) → bid/ask shift DOWN
    up_bid, up_ask = engine.generate_quotes(0.50, "up", inv)
    # DN: invert_skew=True → bid/ask shift UP
    dn_bid, dn_ask = engine.generate_quotes(0.50, "dn", inv, invert_skew=True)

    assert dn_bid.price > up_bid.price, (
        f"DN bid {dn_bid.price} should be > UP bid {up_bid.price} (DN buys aggressively)")
    assert dn_ask.price > up_ask.price, (
        f"DN ask {dn_ask.price} should be > UP ask {up_ask.price}")


def test_skew_zero_empty_inventory():
    engine = QuoteEngine(MMConfig(half_spread_bps=100, order_size_usd=10.0))
    inv = Inventory()

    up_bid, up_ask = engine.generate_quotes(0.50, "up", inv)
    dn_bid, dn_ask = engine.generate_quotes(0.50, "dn", inv, invert_skew=True)

    # No inventory → no skew → symmetric quotes
    assert up_bid.price == dn_bid.price, (
        f"UP bid {up_bid.price} != DN bid {dn_bid.price} with empty inventory")
    assert up_ask.price == dn_ask.price, (
        f"UP ask {up_ask.price} != DN ask {dn_ask.price} with empty inventory")


def test_generate_all_quotes_dn_inverted():
    config = MMConfig(half_spread_bps=100, skew_bps_per_unit=50.0, order_size_usd=10.0)
    engine = QuoteEngine(config)
    inv = Inventory(up_shares=10.0, dn_shares=0.0, initial_usdc=100.0)

    quotes = engine.generate_all_quotes(0.50, 0.50, "up", "dn", inv)
    up_bid, up_ask = quotes["up"]
    dn_bid, dn_ask = quotes["dn"]

    assert dn_bid.price > up_bid.price, (
        f"DN bid {dn_bid.price} should be > UP bid {up_bid.price} (DN buys aggressively)")


# ── Gamma-aware spread ─────────────────────────────────────────

def test_gamma_spread_at_60s():
    """At T=60s remaining, gamma widening should roughly double the spread."""
    config = MMConfig(half_spread_bps=100, max_spread_bps=1000, order_size_usd=10.0)
    engine = QuoteEngine(config)
    inv = Inventory()

    # Without gamma (time_remaining=-1 means disabled)
    b1, a1 = engine.generate_quotes(0.50, "t", inv, time_remaining=-1.0)
    # With gamma at T=60s → should be ~2x multiplier
    b2, a2 = engine.generate_quotes(0.50, "t", inv, time_remaining=60.0)

    spread_normal = a1.price - b1.price
    spread_gamma = a2.price - b2.price

    # Expected: time_mult = 1 + (120-60)/120*2 = 2.0x
    assert spread_gamma >= spread_normal * 1.9, (
        f"Gamma spread {spread_gamma:.4f} should be >= 1.9x normal {spread_normal:.4f}")
    assert spread_gamma <= spread_normal * 2.2, (
        f"Gamma spread {spread_gamma:.4f} should be <= 2.2x normal {spread_normal:.4f}")


def test_gamma_spread_at_0s_capped():
    """At T=0s, gamma 3x should be capped by max_spread_bps."""
    config = MMConfig(half_spread_bps=400, max_spread_bps=1000, order_size_usd=10.0)
    engine = QuoteEngine(config)
    inv = Inventory()

    b, a = engine.generate_quotes(0.50, "t", inv, time_remaining=0.0)
    spread = a.price - b.price

    # 400 * 3 = 1200 bps → clamped to 1000 bps half-spread = 0.10
    # Full spread = 2 * 0.10 = 0.20
    expected_spread = 0.20
    assert spread >= expected_spread - 0.02, (
        f"Spread {spread:.4f} should be >= {expected_spread - 0.02} (gamma clamping active)")
    assert spread <= expected_spread + 0.02, (
        f"Spread {spread:.4f} should be <= {expected_spread + 0.02} (capped by max_spread)")


def test_gamma_inactive_above_120s():
    """Gamma widening should not activate when time_remaining > 120s."""
    config = MMConfig(half_spread_bps=100, order_size_usd=10.0)
    engine = QuoteEngine(config)
    inv = Inventory()

    b1, a1 = engine.generate_quotes(0.50, "t", inv, time_remaining=-1.0)
    b2, a2 = engine.generate_quotes(0.50, "t", inv, time_remaining=150.0)

    spread1 = a1.price - b1.price
    spread2 = a2.price - b2.price

    assert spread1 == spread2, (
        f"Spread at T=150s ({spread2:.4f}) should equal no-gamma ({spread1:.4f})")


# ── Timer / MarketInfo ─────────────────────────────────────────

def test_market_info_time_remaining():
    now = time.time()
    m = MarketInfo(
        coin="BTC",
        timeframe="15m",
        up_token_id="u",
        dn_token_id="d",
        strike=100000.0,
        window_start=now,
        window_end=now + 100,
    )
    assert m.time_remaining <= 100.0
    assert m.time_remaining >= 99.0  # Within 1 second of creation
