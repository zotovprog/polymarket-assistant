from __future__ import annotations

import os
import sys
import time

import pytest


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


def test_live_flat_start_suppresses_naked_sells_but_keeps_bids():
    cfg = MMConfigV2(base_clip_usd=6.0)
    snapshot = _snapshot()
    inventory = _inventory(free_usdc=15.0)
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
        ctx=QuoteContext(tick_size=0.01, min_order_size=5.0, allow_naked_sells=False),
    )
    assert plan.up_bid is not None
    assert plan.dn_bid is not None
    assert plan.up_ask is None
    assert plan.dn_ask is None
    assert plan.suppressed_reasons["up_ask"] == "live_requires_inventory_backed_sell"
    assert plan.suppressed_reasons["dn_ask"] == "live_requires_inventory_backed_sell"
    assert plan.quote_balance_state == "reduced"


def test_paper_flat_start_keeps_two_sided_quotes():
    cfg = MMConfigV2(base_clip_usd=6.0)
    snapshot = _snapshot()
    inventory = _inventory(free_usdc=15.0)
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
        ctx=QuoteContext(tick_size=0.01, min_order_size=5.0, allow_naked_sells=True),
    )
    assert plan.up_bid is not None
    assert plan.dn_bid is not None
    assert plan.up_ask is not None
    assert plan.dn_ask is not None


def test_live_asks_use_sellable_not_owned_inventory():
    cfg = MMConfigV2(base_clip_usd=6.0, session_budget_usd=50.0)
    snapshot = _snapshot()
    inventory = _inventory(
        up_shares=12.0,
        dn_shares=0.0,
        sellable_up_shares=0.0,
        sellable_dn_shares=0.0,
        excess_up_qty=12.0,
        excess_up_value_usd=6.48,
        excess_value_usd=6.48,
        signed_excess_value_usd=6.48,
        total_inventory_value_usd=6.48,
        free_usdc=50.0,
    )
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
        ctx=QuoteContext(tick_size=0.01, min_order_size=5.0, allow_naked_sells=False),
    )
    assert plan.up_ask is None
    assert plan.suppressed_reasons["up_ask"] == "live_requires_inventory_backed_sell"


def test_paper_asks_still_use_owned_inventory():
    cfg = MMConfigV2(base_clip_usd=6.0, session_budget_usd=50.0)
    snapshot = _snapshot()
    inventory = _inventory(
        up_shares=12.0,
        dn_shares=0.0,
        sellable_up_shares=0.0,
        sellable_dn_shares=0.0,
        excess_up_qty=12.0,
        excess_up_value_usd=6.48,
        excess_value_usd=6.48,
        signed_excess_value_usd=6.48,
        total_inventory_value_usd=6.48,
        free_usdc=50.0,
    )
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
        ctx=QuoteContext(tick_size=0.01, min_order_size=5.0, allow_naked_sells=True),
    )
    assert plan.up_ask is not None


def test_unwind_mode_disables_only_pair_expanding_intents():
    risk, plan = _plan_for(
        _inventory(excess_dn_value_usd=5.8, excess_value_usd=5.8, signed_excess_value_usd=-5.8),
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


def test_endpoint_prices_cap_harmful_share_size_below_explosive_levels():
    cfg = MMConfigV2(session_budget_usd=50.0, base_clip_usd=6.0)
    snapshot = _snapshot(
        fv_up=0.99,
        fv_dn=0.01,
        pm_mid_up=0.99,
        pm_mid_dn=0.01,
        up_best_bid=0.98,
        up_best_ask=0.99,
        dn_best_bid=0.01,
        dn_best_ask=0.02,
    )
    inventory = _inventory(excess_dn_value_usd=6.0, excess_value_usd=6.0, signed_excess_value_usd=-6.0, free_usdc=50.0)
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
        ctx=QuoteContext(tick_size=0.01, min_order_size=5.0),
    )
    if plan.up_ask is not None:
        assert plan.up_ask.size <= 12.0
    if plan.dn_bid is not None:
        assert plan.dn_bid.size <= 12.0


def test_helpful_intent_is_promoted_to_min_order_size_when_safe():
    cfg = MMConfigV2(session_budget_usd=50.0, base_clip_usd=4.0)
    snapshot = _snapshot(
        fv_up=0.98,
        fv_dn=0.02,
        pm_mid_up=0.98,
        pm_mid_dn=0.02,
        up_best_bid=0.97,
        up_best_ask=0.98,
        dn_best_bid=0.02,
        dn_best_ask=0.03,
    )
    inventory = _inventory(excess_dn_value_usd=4.5, excess_value_usd=4.5, signed_excess_value_usd=-4.5, free_usdc=50.0)
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
        ctx=QuoteContext(tick_size=0.01, min_order_size=5.0),
    )
    assert plan.up_bid is not None
    assert plan.up_bid.inventory_effect == "helpful"
    assert plan.up_bid.size >= 5.0


def test_harmful_intents_are_blocked_when_no_helpful_intents_survive(monkeypatch):
    cfg = MMConfigV2(session_budget_usd=15.0, base_clip_usd=6.0)
    policy = QuotePolicyV2(cfg)
    snapshot = _snapshot(
        fv_up=0.99,
        fv_dn=0.01,
        pm_mid_up=0.99,
        pm_mid_dn=0.01,
        up_best_bid=0.98,
        up_best_ask=0.99,
        dn_best_bid=0.01,
        dn_best_ask=0.02,
    )
    inventory = _inventory(excess_dn_value_usd=3.0, excess_value_usd=3.0, signed_excess_value_usd=-3.0, free_usdc=50.0)
    risk = HardSafetyKernel(cfg).evaluate(
        snapshot=snapshot,
        inventory=inventory,
        analytics=AnalyticsState(),
        health=HealthState(),
    )

    original_make_intent = policy._make_intent

    def _fake_make_intent(**kwargs):
        if kwargs["inventory_effect"] == "helpful":
            return None, "below_min_order_size"
        return original_make_intent(**kwargs)

    monkeypatch.setattr(policy, "_make_intent", _fake_make_intent)
    plan = policy.generate(
        snapshot=snapshot,
        inventory=inventory,
        risk=risk,
        ctx=QuoteContext(tick_size=0.01, min_order_size=1.0),
    )
    assert plan.quote_balance_state == "harmful_only_blocked"
    assert plan.up_ask is None or plan.dn_bid is None
    assert plan.suppressed_reasons["up_ask"] == "harmful blocked without helpful viability"


def test_effective_soft_mode_drives_quote_generation_not_target_mode():
    cfg = MMConfigV2(session_budget_usd=15.0, base_clip_usd=6.0)
    inventory = _inventory(excess_dn_value_usd=3.0, excess_value_usd=3.0, signed_excess_value_usd=-3.0)
    risk = HardSafetyKernel(cfg).evaluate(
        snapshot=_snapshot(),
        inventory=inventory,
        analytics=AnalyticsState(),
        health=HealthState(),
    )
    risk.soft_mode = "unwind"
    risk.target_soft_mode = "defensive"
    plan = QuotePolicyV2(cfg).generate(
        snapshot=_snapshot(),
        inventory=inventory,
        risk=risk,
        ctx=QuoteContext(tick_size=0.01, min_order_size=1.0),
    )
    assert plan.regime == "unwind"
    assert plan.dn_bid is None
    assert plan.up_ask is None


def test_quote_state_includes_suppressed_reason_for_blocked_harmful_intents(monkeypatch):
    cfg = MMConfigV2(session_budget_usd=15.0, base_clip_usd=6.0)
    policy = QuotePolicyV2(cfg)
    snapshot = _snapshot(
        fv_up=0.99,
        fv_dn=0.01,
        pm_mid_up=0.99,
        pm_mid_dn=0.01,
        up_best_bid=0.98,
        up_best_ask=0.99,
        dn_best_bid=0.01,
        dn_best_ask=0.02,
    )
    inventory = _inventory(excess_dn_value_usd=3.0, excess_value_usd=3.0, signed_excess_value_usd=-3.0, free_usdc=50.0)
    risk = HardSafetyKernel(cfg).evaluate(
        snapshot=snapshot,
        inventory=inventory,
        analytics=AnalyticsState(),
        health=HealthState(),
    )

    original_make_intent = policy._make_intent

    def _fake_make_intent(**kwargs):
        if kwargs["inventory_effect"] == "helpful":
            return None, "below_min_order_size"
        return original_make_intent(**kwargs)

    monkeypatch.setattr(policy, "_make_intent", _fake_make_intent)
    plan = policy.generate(
        snapshot=snapshot,
        inventory=inventory,
        risk=risk,
        ctx=QuoteContext(tick_size=0.01, min_order_size=1.0),
    )
    assert plan.suppressed_reasons["dn_bid"] == "harmful blocked without helpful viability"


def test_default_clip_6_produces_flat_start_quotes_at_pm_min_size():
    cfg = MMConfigV2()
    snapshot = _snapshot()
    inventory = _inventory(free_usdc=50.0)
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
        ctx=QuoteContext(tick_size=0.01, min_order_size=5.0),
    )
    assert cfg.base_clip_usd == pytest.approx(6.0)
    assert plan.quote_balance_state != "none"
    assert all([plan.up_bid, plan.up_ask, plan.dn_bid, plan.dn_ask])


def test_flat_defensive_applies_min_viable_floor_and_keeps_quotes():
    cfg = MMConfigV2(session_budget_usd=15.0, base_clip_usd=6.0)
    snapshot = _snapshot(
        market_quality_score=0.2,
        market_tradeable=False,
        divergence_up=0.13,
        divergence_dn=0.13,
    )
    inventory = _inventory(free_usdc=50.0)
    risk = HardSafetyKernel(cfg).evaluate(
        snapshot=snapshot,
        inventory=inventory,
        analytics=AnalyticsState(),
        health=HealthState(),
    )
    assert risk.soft_mode == "defensive"
    assert risk.inventory_side == "flat"
    plan = QuotePolicyV2(cfg).generate(
        snapshot=snapshot,
        inventory=inventory,
        risk=risk,
        ctx=QuoteContext(tick_size=0.01, min_order_size=5.0),
    )
    assert plan.quote_balance_state != "none"
    assert all([plan.up_bid, plan.up_ask, plan.dn_bid, plan.dn_ask])
    assert plan.quote_viability_reason in {"balanced", "min_viable_floor_applied"}


def test_live_like_flat_defensive_start_with_real_free_usdc_keeps_quotes():
    cfg = MMConfigV2(session_budget_usd=15.0, base_clip_usd=6.0)
    snapshot = _snapshot(
        fv_up=0.016881827209533264,
        fv_dn=0.9831181727904666,
        pm_mid_up=0.02,
        pm_mid_dn=0.99,
        up_best_bid=0.01,
        up_best_ask=0.02,
        dn_best_bid=0.98,
        dn_best_ask=0.99,
        up_bid_depth_usd=11.0186,
        up_ask_depth_usd=30985.2231,
        dn_bid_depth_usd=31387.8769,
        dn_ask_depth_usd=1090.8414,
        market_quality_score=0.6,
        market_tradeable=False,
    )
    inventory = _inventory(free_usdc=16.098201)
    risk = HardSafetyKernel(cfg).evaluate(
        snapshot=snapshot,
        inventory=inventory,
        analytics=AnalyticsState(),
        health=HealthState(),
    )
    assert risk.soft_mode == "defensive"
    assert risk.inventory_side == "flat"
    plan = QuotePolicyV2(cfg).generate(
        snapshot=snapshot,
        inventory=inventory,
        risk=risk,
        ctx=QuoteContext(tick_size=0.01, min_order_size=5.0),
    )
    assert plan.quote_balance_state != "none"
    assert plan.quote_viability_reason in {"balanced", "min_viable_floor_applied", "reduced"}
    assert any([plan.up_bid, plan.up_ask, plan.dn_bid, plan.dn_ask])


def test_helpful_floor_keeps_quotes_alive_in_defensive_below_hard_cap():
    cfg = MMConfigV2(session_budget_usd=50.0, base_clip_usd=6.0)
    snapshot = _snapshot(
        fv_up=0.98,
        fv_dn=0.02,
        pm_mid_up=0.98,
        pm_mid_dn=0.02,
        up_best_bid=0.97,
        up_best_ask=0.98,
        dn_best_bid=0.02,
        dn_best_ask=0.03,
    )
    inventory = _inventory(
        dn_shares=6.0,
        excess_dn_qty=6.0,
        excess_dn_value_usd=9.4,
        excess_value_usd=9.4,
        signed_excess_value_usd=-9.4,
        total_inventory_value_usd=9.4,
        free_usdc=50.0,
    )
    risk = HardSafetyKernel(cfg).evaluate(
        snapshot=snapshot,
        inventory=inventory,
        analytics=AnalyticsState(),
        health=HealthState(),
    )
    assert risk.soft_mode == "defensive"
    plan = QuotePolicyV2(cfg).generate(
        snapshot=snapshot,
        inventory=inventory,
        risk=risk,
        ctx=QuoteContext(tick_size=0.01, min_order_size=5.0),
    )
    helpful_count = sum(
        1
        for intent in (plan.up_bid, plan.up_ask, plan.dn_bid, plan.dn_ask)
        if intent and intent.inventory_effect == "helpful"
    )
    assert helpful_count >= 1
    assert plan.quote_balance_state != "none"
    assert plan.quote_viability_reason in {"balanced", "helpful_floor_applied", "reduced", "helpful_only"}


def test_harmful_quotes_do_not_receive_min_viable_floor():
    cfg = MMConfigV2(session_budget_usd=50.0, base_clip_usd=6.0)
    snapshot = _snapshot(
        fv_up=0.98,
        fv_dn=0.02,
        pm_mid_up=0.98,
        pm_mid_dn=0.02,
        up_best_bid=0.97,
        up_best_ask=0.98,
        dn_best_bid=0.02,
        dn_best_ask=0.03,
    )
    inventory = _inventory(
        dn_shares=6.0,
        excess_dn_qty=6.0,
        excess_dn_value_usd=9.4,
        excess_value_usd=9.4,
        signed_excess_value_usd=-9.4,
        total_inventory_value_usd=9.4,
        free_usdc=50.0,
    )
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
        ctx=QuoteContext(tick_size=0.01, min_order_size=5.0),
    )
    assert plan.up_bid is not None
    assert plan.up_bid.inventory_effect == "helpful"
    if plan.dn_bid is not None:
        assert plan.dn_bid.inventory_effect == "harmful"
        assert plan.up_bid.size >= plan.dn_bid.size
    else:
        assert plan.suppressed_reasons["dn_bid"] == "below_min_order_size"


def test_no_viable_quotes_reason_is_explicit_when_all_quotes_below_min_size():
    cfg = MMConfigV2(session_budget_usd=50.0, base_clip_usd=1.0)
    snapshot = _snapshot()
    inventory = _inventory(free_usdc=2.0)
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
        ctx=QuoteContext(tick_size=0.01, min_order_size=20.0),
    )
    assert plan.quote_balance_state == "none"
    assert plan.quote_viability_reason == "all_quotes_below_min_size"


def test_neutral_floor_reason_is_exposed_for_flat_defensive_state():
    cfg = MMConfigV2(session_budget_usd=15.0, base_clip_usd=6.0)
    snapshot = _snapshot(
        fv_up=0.98,
        fv_dn=0.02,
        pm_mid_up=0.98,
        pm_mid_dn=0.02,
        up_best_bid=0.97,
        up_best_ask=0.98,
        dn_best_bid=0.02,
        dn_best_ask=0.03,
        market_quality_score=0.2,
        market_tradeable=False,
        divergence_up=0.13,
        divergence_dn=0.13,
    )
    inventory = _inventory(free_usdc=50.0)
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
        ctx=QuoteContext(tick_size=0.01, min_order_size=5.0),
    )
    assert plan.quote_viability_reason == "min_viable_floor_applied"


def test_pair_share_cap_still_limits_endpoint_explosion_after_helpful_floor():
    cfg = MMConfigV2(session_budget_usd=50.0, base_clip_usd=6.0)
    snapshot = _snapshot(
        fv_up=0.99,
        fv_dn=0.01,
        pm_mid_up=0.99,
        pm_mid_dn=0.01,
        up_best_bid=0.98,
        up_best_ask=0.99,
        dn_best_bid=0.01,
        dn_best_ask=0.02,
    )
    inventory = _inventory(
        dn_shares=6.0,
        excess_dn_qty=6.0,
        excess_dn_value_usd=9.8,
        excess_value_usd=9.8,
        signed_excess_value_usd=-9.8,
        total_inventory_value_usd=9.8,
        free_usdc=50.0,
    )
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
        ctx=QuoteContext(tick_size=0.01, min_order_size=5.0),
    )
    helpful_intents = [
        intent
        for intent in (plan.up_bid, plan.up_ask, plan.dn_bid, plan.dn_ask)
        if intent and intent.inventory_effect == "helpful"
    ]
    assert helpful_intents
    assert max(intent.size for intent in helpful_intents) <= 6.0


def test_inventory_backed_sells_survive_low_free_usdc_after_live_like_fills():
    cfg = MMConfigV2(session_budget_usd=15.0, base_clip_usd=6.0)
    snapshot = _snapshot(
        fv_up=0.59,
        fv_dn=0.41,
        pm_mid_up=0.58,
        pm_mid_dn=0.42,
        up_best_bid=0.57,
        up_best_ask=0.59,
        dn_best_bid=0.40,
        dn_best_ask=0.42,
        market_quality_score=0.40,
        market_tradeable=True,
        divergence_up=0.02,
        divergence_dn=0.02,
    )
    inventory = _inventory(
        up_shares=5.84,
        dn_shares=6.16,
        paired_qty=5.84,
        excess_dn_qty=0.32,
        paired_value_usd=5.84,
        excess_dn_value_usd=0.19,
        excess_value_usd=0.19,
        signed_excess_value_usd=-0.19,
        total_inventory_value_usd=6.03,
        free_usdc=9.94,
    )
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
        ctx=QuoteContext(tick_size=0.01, min_order_size=5.0),
    )
    assert risk.hard_mode == "none"
    assert risk.inventory_side == "flat"
    assert plan.quote_balance_state != "none"
    assert plan.dn_ask is not None
    assert plan.dn_ask.inventory_effect == "neutral"
    assert plan.dn_ask.size >= 5.0
    assert plan.quote_viability_reason != "all_quotes_below_min_size"


def test_inventory_backed_asks_ignore_buy_headroom_cap_when_owned_shares_exist():
    cfg = MMConfigV2(session_budget_usd=15.0, base_clip_usd=6.0)
    snapshot = _snapshot(
        fv_up=0.59,
        fv_dn=0.41,
        pm_mid_up=0.58,
        pm_mid_dn=0.42,
        up_best_bid=0.57,
        up_best_ask=0.59,
        dn_best_bid=0.40,
        dn_best_ask=0.42,
    )
    inventory = _inventory(
        up_shares=5.84,
        dn_shares=6.16,
        paired_qty=5.84,
        excess_dn_qty=0.32,
        excess_dn_value_usd=0.19,
        excess_value_usd=0.19,
        signed_excess_value_usd=-0.19,
        total_inventory_value_usd=6.03,
        free_usdc=9.94,
    )
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
        ctx=QuoteContext(tick_size=0.01, min_order_size=5.0),
    )
    buy_headroom_usd = max(1.0, inventory.free_usdc * 0.20)
    assert buy_headroom_usd < 5.0
    assert plan.dn_ask is not None
    assert plan.dn_ask.size > buy_headroom_usd / max(0.01, plan.dn_ask.price)


def test_emergency_unwind_post_only_sell_is_priced_above_best_bid():
    cfg = MMConfigV2(session_budget_usd=15.0, base_clip_usd=6.0)
    snapshot = _snapshot(
        time_left_sec=300.0,
        fv_up=0.83,
        fv_dn=0.17,
        pm_mid_up=0.75,
        pm_mid_dn=0.26,
        up_best_bid=0.74,
        up_best_ask=0.75,
        dn_best_bid=0.25,
        dn_best_ask=0.26,
    )
    inventory = _inventory(
        up_shares=9.75,
        excess_up_qty=9.75,
        excess_up_value_usd=8.10,
        excess_value_usd=8.10,
        signed_excess_value_usd=8.10,
        total_inventory_value_usd=8.10,
        free_usdc=11.13,
    )
    risk = HardSafetyKernel(cfg).evaluate(
        snapshot=snapshot,
        inventory=inventory,
        analytics=AnalyticsState(),
        health=HealthState(transport_ok=False, last_api_error="crosses book"),
    )
    assert risk.hard_mode == "emergency_unwind"
    plan = QuotePolicyV2(cfg).generate(
        snapshot=snapshot,
        inventory=inventory,
        risk=risk,
        ctx=QuoteContext(tick_size=0.01, min_order_size=5.0),
    )
    assert plan.up_ask is not None
    assert plan.up_ask.post_only is True
    assert plan.up_ask.price > float(snapshot.up_best_bid)
