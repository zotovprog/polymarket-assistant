from __future__ import annotations

import os
import sys
import time


BASE = os.path.dirname(os.path.dirname(__file__))
SRC = os.path.join(BASE, "src")
if SRC not in sys.path:
    sys.path.insert(0, SRC)
if BASE not in sys.path:
    sys.path.insert(0, BASE)

from mm_v2.config import MMConfigV2
from mm_v2.quote_policy import QuoteContext, QuotePolicyV2
from mm_v2.risk_kernel import HardSafetyKernel
from mm_v2.types import AnalyticsState, HealthState, PairInventoryState, PairMarketSnapshot


def _snapshot(**overrides) -> PairMarketSnapshot:
    payload = dict(
        ts=time.time(),
        market_id="btc-15m",
        up_token_id="up-token",
        dn_token_id="dn-token",
        time_left_sec=900.0,
        fv_up=0.54,
        fv_dn=0.46,
        fv_confidence=0.9,
        pm_mid_up=0.53,
        pm_mid_dn=0.47,
        up_best_bid=0.52,
        up_best_ask=0.54,
        dn_best_bid=0.46,
        dn_best_ask=0.48,
        up_bid_depth_usd=200.0,
        up_ask_depth_usd=200.0,
        dn_bid_depth_usd=200.0,
        dn_ask_depth_usd=200.0,
        market_quality_score=0.9,
        market_tradeable=True,
        divergence_up=0.01,
        divergence_dn=0.01,
    )
    payload.update(overrides)
    return PairMarketSnapshot(**payload)


def _inventory(**overrides) -> PairInventoryState:
    payload = dict(
        up_shares=0.0,
        dn_shares=0.0,
        free_usdc=15.0,
        reserved_usdc=0.0,
        pending_buy_up=0.0,
        pending_buy_dn=0.0,
        pending_sell_up=0.0,
        pending_sell_dn=0.0,
        paired_qty=0.0,
        excess_up_qty=0.0,
        excess_dn_qty=0.0,
        paired_value_usd=0.0,
        excess_up_value_usd=0.0,
        excess_dn_value_usd=0.0,
        total_inventory_value_usd=0.0,
        excess_value_usd=0.0,
        signed_excess_value_usd=0.0,
        inventory_pressure_abs=0.0,
        inventory_pressure_signed=0.0,
    )
    payload.update(overrides)
    return PairInventoryState(**payload)


def _plan_for(inventory: PairInventoryState):
    cfg = MMConfigV2(base_clip_usd=6.0)
    snapshot = _snapshot()
    risk = HardSafetyKernel(cfg).evaluate(
        snapshot=snapshot,
        inventory=inventory,
        analytics=AnalyticsState(),
        health=HealthState(),
    )
    plan = QuotePolicyV2(cfg).generate(
        snapshot=snapshot,
        inventory=inventory,
        risk=risk,
        ctx=QuoteContext(tick_size=0.01, min_order_size=1.0),
    )
    return risk, plan


def test_excess_dn_marks_dn_buy_harmful_and_dn_sell_helpful():
    _, plan = _plan_for(
        _inventory(excess_dn_value_usd=2.5, excess_value_usd=2.5, signed_excess_value_usd=-2.5),
    )
    assert plan.dn_bid is not None and plan.dn_bid.inventory_effect == "harmful"
    assert plan.dn_ask is not None and plan.dn_ask.inventory_effect == "helpful"


def test_excess_up_marks_up_buy_harmful_and_up_sell_helpful():
    _, plan = _plan_for(
        _inventory(excess_up_value_usd=2.5, excess_value_usd=2.5, signed_excess_value_usd=2.5),
    )
    assert plan.up_bid is not None and plan.up_bid.inventory_effect == "harmful"
    assert plan.up_ask is not None and plan.up_ask.inventory_effect == "helpful"


def test_helpful_buy_is_priced_more_aggressively_than_harmful_buy():
    _, flat_plan = _plan_for(_inventory())
    _, skew_plan = _plan_for(
        _inventory(excess_dn_value_usd=2.5, excess_value_usd=2.5, signed_excess_value_usd=-2.5),
    )
    assert skew_plan.up_bid is not None and flat_plan.up_bid is not None
    assert skew_plan.dn_bid is not None and flat_plan.dn_bid is not None
    assert skew_plan.up_bid.price >= flat_plan.up_bid.price
    assert skew_plan.dn_bid.price <= flat_plan.dn_bid.price


def test_helpful_sell_is_priced_more_aggressively_than_harmful_sell():
    _, flat_plan = _plan_for(_inventory())
    _, skew_plan = _plan_for(
        _inventory(excess_dn_value_usd=2.5, excess_value_usd=2.5, signed_excess_value_usd=-2.5),
    )
    assert skew_plan.dn_ask is not None and flat_plan.dn_ask is not None
    assert skew_plan.up_ask is not None and flat_plan.up_ask is not None
    assert skew_plan.dn_ask.price <= flat_plan.dn_ask.price
    assert skew_plan.up_ask.price >= flat_plan.up_ask.price


def test_helpful_size_is_larger_than_harmful_size_under_skew():
    _, plan = _plan_for(
        _inventory(excess_dn_value_usd=2.5, excess_value_usd=2.5, signed_excess_value_usd=-2.5),
    )
    assert plan.up_bid is not None and plan.dn_bid is not None
    assert plan.up_bid.size_mult > plan.dn_bid.size_mult
    assert plan.dn_ask is not None and plan.up_ask is not None
    assert plan.dn_ask.size_mult > plan.up_ask.size_mult


def test_skewed_mode_keeps_four_quotes_below_hard_cap():
    risk, plan = _plan_for(
        _inventory(excess_dn_value_usd=2.0, excess_value_usd=2.0, signed_excess_value_usd=-2.0),
    )
    assert risk.soft_mode == "inventory_skewed"
    assert plan.up_bid is not None
    assert plan.up_ask is not None
    assert plan.dn_bid is not None
    assert plan.dn_ask is not None


def test_unwind_mode_disables_only_pair_expanding_intents():
    risk, plan = _plan_for(
        _inventory(excess_dn_value_usd=4.5, excess_value_usd=4.5, signed_excess_value_usd=-4.5),
    )
    assert risk.soft_mode == "unwind"
    assert plan.dn_bid is None
    assert plan.up_ask is None
    assert plan.dn_ask is not None
    assert plan.up_bid is not None


def test_pair_complementarity_survives_inventory_skew_adjustment():
    _, plan = _plan_for(
        _inventory(excess_up_value_usd=2.5, excess_value_usd=2.5, signed_excess_value_usd=2.5),
    )
    assert plan.up_bid is not None and plan.dn_ask is not None
    assert plan.dn_bid is not None and plan.up_ask is not None
    assert plan.up_bid.price + plan.dn_ask.price <= 0.99
    assert plan.dn_bid.price + plan.up_ask.price <= 0.99
