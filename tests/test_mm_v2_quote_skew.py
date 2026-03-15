from __future__ import annotations

from dataclasses import replace
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
    cfg = MMConfigV2(session_budget_usd=15.0, base_clip_usd=6.0)
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


def test_dynamic_spread_uses_realized_vol_and_fee_floor():
    cfg = MMConfigV2(
        base_half_spread_bps=20.0,
        max_half_spread_bps=600.0,
        vol_spread_multiplier=2.0,
        maker_fee_bps=25.0,
    )
    policy = QuotePolicyV2(cfg)
    risk = HardSafetyKernel(cfg).evaluate(
        snapshot=_snapshot(realized_vol_per_min=0.003, time_left_sec=900.0),
        inventory=_inventory(),
        analytics=AnalyticsState(),
        health=HealthState(),
    )

    spread = policy._spread(risk, _snapshot(realized_vol_per_min=0.003, time_left_sec=900.0))

    assert spread > policy._bps_to_price(20.0)
    assert spread >= policy._bps_to_price(50.0)


def test_negative_pair_entry_cost_blocks_both_bids_when_pair_is_lossmaking():
    cfg = MMConfigV2(session_budget_usd=300.0, base_clip_usd=14.0)
    snapshot = _snapshot(realized_vol_per_min=0.0005)
    inventory = _inventory(
        up_shares=10.0,
        dn_shares=10.0,
        paired_qty=10.0,
        paired_value_usd=10.0,
        total_inventory_value_usd=10.0,
        pair_entry_cost=1.08,
        pair_entry_pnl_per_share=-0.08,
        free_usdc=300.0,
    )
    risk = HardSafetyKernel(cfg).evaluate(
        snapshot=snapshot,
        inventory=inventory,
        analytics=AnalyticsState(pair_entry_cost=1.08, pair_entry_pnl_per_share=-0.08),
        health=HealthState(),
    )
    plan = QuotePolicyV2(cfg).generate(
        snapshot=snapshot,
        inventory=inventory,
        risk=risk,
        ctx=QuoteContext(tick_size=0.01, min_order_size=5.0),
    )

    assert plan.up_bid is None
    assert plan.dn_bid is None
    assert "pair_entry_cost_block_both" in plan.suppressed_reasons.values()


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
        _inventory(excess_dn_value_usd=3.2, excess_value_usd=3.2, signed_excess_value_usd=-3.2),
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
        _inventory(excess_dn_value_usd=7.2, excess_value_usd=7.2, signed_excess_value_usd=-7.2),
    )
    assert risk.soft_mode == "unwind"
    assert plan.dn_bid is None
    assert plan.up_ask is None
    assert plan.dn_ask is not None
    assert plan.up_bid is not None


def test_unwind_helpful_floor_applies_for_helpful_buy_intents():
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
    )
    inventory = _inventory(
        dn_shares=6.0,
        excess_dn_qty=6.0,
        excess_dn_value_usd=7.2,
        excess_value_usd=7.2,
        signed_excess_value_usd=-7.2,
        total_inventory_value_usd=7.2,
        free_usdc=50.0,
    )
    risk = HardSafetyKernel(cfg).evaluate(
        snapshot=snapshot,
        inventory=inventory,
        analytics=AnalyticsState(),
        health=HealthState(),
    )
    assert risk.soft_mode == "unwind"
    plan = QuotePolicyV2(cfg).generate(
        snapshot=snapshot,
        inventory=inventory,
        risk=risk,
        ctx=QuoteContext(tick_size=0.01, min_order_size=5.0),
    )
    assert plan.up_bid is not None
    assert plan.up_bid.inventory_effect == "helpful"
    assert plan.up_bid.size >= 5.0
    assert plan.quote_viability_reason in {"helpful_floor_applied", "balanced", "helpful_only", "reduced"}


def test_unwind_helpful_buy_uses_full_free_usdc_headroom_not_20pct_cap():
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
    )
    inventory = _inventory(
        dn_shares=6.0,
        excess_dn_qty=6.0,
        excess_dn_value_usd=7.2,
        excess_value_usd=7.2,
        signed_excess_value_usd=-7.2,
        total_inventory_value_usd=7.2,
        free_usdc=12.0,
    )
    risk = HardSafetyKernel(cfg).evaluate(
        snapshot=snapshot,
        inventory=inventory,
        analytics=AnalyticsState(),
        health=HealthState(),
    )
    assert risk.soft_mode == "unwind"
    min_viable_clip = 5.0 * max(snapshot.fv_up, snapshot.fv_dn, 0.5)
    assert min_viable_clip > (inventory.free_usdc * 0.20)
    plan = QuotePolicyV2(cfg).generate(
        snapshot=snapshot,
        inventory=inventory,
        risk=risk,
        ctx=QuoteContext(tick_size=0.01, min_order_size=5.0),
    )
    assert plan.up_bid is not None
    assert plan.up_bid.inventory_effect == "helpful"
    assert plan.up_bid.size >= 5.0


def test_unwind_does_not_enable_harmful_intents():
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
    )
    inventory = _inventory(
        dn_shares=6.0,
        excess_dn_qty=6.0,
        excess_dn_value_usd=7.2,
        excess_value_usd=7.2,
        signed_excess_value_usd=-7.2,
        total_inventory_value_usd=7.2,
        free_usdc=50.0,
    )
    risk = HardSafetyKernel(cfg).evaluate(
        snapshot=snapshot,
        inventory=inventory,
        analytics=AnalyticsState(),
        health=HealthState(),
    )
    assert risk.soft_mode == "unwind"
    plan = QuotePolicyV2(cfg).generate(
        snapshot=snapshot,
        inventory=inventory,
        risk=risk,
        ctx=QuoteContext(tick_size=0.01, min_order_size=5.0),
    )
    harmful_active = [
        intent
        for intent in (plan.up_bid, plan.up_ask, plan.dn_bid, plan.dn_ask)
        if intent and intent.inventory_effect == "harmful"
    ]
    assert not harmful_active
    assert plan.dn_bid is None
    assert plan.up_ask is None


def test_unwind_below_hard_cap_avoids_none_when_helpful_liquidity_exists():
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
        excess_dn_value_usd=12.0,
        excess_value_usd=12.0,
        signed_excess_value_usd=-12.0,
        total_inventory_value_usd=12.0,
        free_usdc=12.0,
    )
    risk = HardSafetyKernel(cfg).evaluate(
        snapshot=snapshot,
        inventory=inventory,
        analytics=AnalyticsState(),
        health=HealthState(),
    )
    assert risk.soft_mode in {"inventory_skewed", "defensive"}
    risk.soft_mode = "unwind"
    risk.target_soft_mode = "defensive"
    plan = QuotePolicyV2(cfg).generate(
        snapshot=snapshot,
        inventory=inventory,
        risk=risk,
        ctx=QuoteContext(tick_size=0.01, min_order_size=5.0),
    )
    assert plan.quote_balance_state != "none"
    helpful_count = sum(
        1
        for intent in (plan.up_bid, plan.up_ask, plan.dn_bid, plan.dn_ask)
        if intent and intent.inventory_effect == "helpful"
    )
    assert helpful_count >= 1


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
    cfg = MMConfigV2(session_budget_usd=50.0, base_clip_usd=6.0)
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
    inventory = _inventory(excess_dn_value_usd=6.0, excess_value_usd=6.0, signed_excess_value_usd=-6.0, free_usdc=50.0)
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
    cfg = MMConfigV2(session_budget_usd=50.0, base_clip_usd=6.0)
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
    inventory = _inventory(excess_dn_value_usd=6.0, excess_value_usd=6.0, signed_excess_value_usd=-6.0, free_usdc=50.0)
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


def test_default_clip_4_produces_flat_start_quotes_at_pm_min_size():
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
    assert cfg.base_clip_usd == pytest.approx(4.0)
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


def test_live_like_flat_untradeable_tolerated_start_keeps_quotes():
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
    assert risk.soft_mode == "normal"
    assert risk.reason == "normal quoting (untradeable tolerated)"
    assert risk.inventory_side == "flat"
    plan = QuotePolicyV2(cfg).generate(
        snapshot=snapshot,
        inventory=inventory,
        risk=risk,
        ctx=QuoteContext(tick_size=0.01, min_order_size=5.0),
    )
    assert plan.quote_balance_state != "none"
    assert plan.quote_viability_reason in {"balanced", "reduced", "min_viable_floor_applied"}
    assert any([plan.up_bid, plan.up_ask, plan.dn_bid, plan.dn_ask])


def test_marketability_guard_blocks_pair_expanding_quotes_on_problem_side():
    cfg = MMConfigV2(session_budget_usd=30.0, base_clip_usd=4.0)
    snapshot = _snapshot(market_tradeable=False, market_quality_score=0.62)
    inventory = _inventory(
        up_shares=8.0,
        total_inventory_value_usd=7.0,
        excess_up_qty=8.0,
        excess_up_value_usd=4.2,
        excess_value_usd=4.2,
        signed_excess_value_usd=4.2,
        free_usdc=20.0,
    )
    base_risk = HardSafetyKernel(cfg).evaluate(
        snapshot=snapshot,
        inventory=inventory,
        analytics=AnalyticsState(
            marketability_guard_active=True,
            marketability_guard_reason="collateral_warning",
        ),
        health=HealthState(),
    )
    risk = replace(
        base_risk,
        marketability_guard_active=True,
        marketability_guard_reason="collateral_warning",
        marketability_guard_up_active=True,
    )
    plan = QuotePolicyV2(cfg).generate(
        snapshot=snapshot,
        inventory=inventory,
        risk=risk,
        ctx=QuoteContext(tick_size=0.01, min_order_size=1.0),
    )
    assert plan.up_bid is None
    assert plan.suppressed_reasons["up_bid"] == "marketability_guard"
    assert plan.up_ask is not None
    assert plan.quote_viability_reason == "marketability_guard"


def test_marketability_churn_confirmed_keeps_only_inventory_reducing_intents():
    cfg = MMConfigV2(session_budget_usd=30.0, base_clip_usd=4.0)
    snapshot = _snapshot(market_tradeable=False, market_quality_score=0.62)
    inventory = _inventory(
        up_shares=8.0,
        total_inventory_value_usd=7.0,
        excess_up_qty=8.0,
        excess_up_value_usd=4.2,
        excess_value_usd=4.2,
        signed_excess_value_usd=4.2,
        free_usdc=20.0,
    )
    base_risk = HardSafetyKernel(cfg).evaluate(
        snapshot=snapshot,
        inventory=inventory,
        analytics=AnalyticsState(
            marketability_guard_active=True,
            marketability_guard_reason="collateral_warning",
            marketability_churn_confirmed=True,
            marketability_problem_side="up",
        ),
        health=HealthState(),
    )
    risk = replace(
        base_risk,
        marketability_guard_active=True,
        marketability_guard_reason="collateral_warning",
        marketability_churn_confirmed=True,
        marketability_problem_side="up",
        marketability_guard_up_active=True,
    )
    plan = QuotePolicyV2(cfg).generate(
        snapshot=snapshot,
        inventory=inventory,
        risk=risk,
        ctx=QuoteContext(tick_size=0.01, min_order_size=1.0),
    )
    assert plan.up_bid is None
    assert plan.dn_bid is None
    assert plan.suppressed_reasons["up_bid"] == "marketability_churn_confirmed"
    assert plan.suppressed_reasons["dn_bid"] == "marketability_churn_confirmed"
    assert plan.up_ask is not None
    assert plan.up_ask.min_rest_sec == pytest.approx(6.0)
    assert plan.up_ask.hold_mode_active is True
    assert plan.up_ask.hold_mode_reason == "sell_reprice_hold_mode"
    assert plan.up_ask.hold_reprice_threshold_ticks == 12
    assert plan.up_ask.hold_max_age_sec == pytest.approx(3.0)
    assert plan.sell_churn_hold_up_active is False
    assert plan.sell_churn_hold_dn_active is False
    assert plan.sell_churn_hold_side == ""
    assert plan.quote_viability_reason == "marketability_churn_confirmed"


def test_flat_start_sells_get_short_sell_reprice_hold_mode():
    cfg = MMConfigV2(session_budget_usd=300.0, base_clip_usd=4.0)
    snapshot = _snapshot(
        market_tradeable=True,
        market_quality_score=0.9,
        fv_up=0.54,
        fv_dn=0.46,
        pm_mid_up=0.53,
        pm_mid_dn=0.47,
        up_best_bid=0.52,
        up_best_ask=0.54,
        dn_best_bid=0.46,
        dn_best_ask=0.48,
    )
    inventory = _inventory(
        up_shares=0.0,
        dn_shares=0.0,
        sellable_up_shares=0.0,
        sellable_dn_shares=0.0,
        total_inventory_value_usd=0.0,
        free_usdc=300.0,
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
    assert plan.up_ask is not None
    assert plan.dn_ask is not None
    assert plan.up_ask.hold_mode_active is True
    assert plan.up_ask.hold_mode_reason == "sell_reprice_hold_mode"
    assert plan.up_ask.hold_reprice_threshold_ticks == 12
    assert plan.up_ask.hold_max_age_sec == pytest.approx(3.0)
    assert plan.dn_ask.hold_mode_active is True
    assert plan.dn_ask.hold_mode_reason == "sell_reprice_hold_mode"


def test_sell_churn_hold_mode_marks_explicit_dual_bid_exception():
    cfg = MMConfigV2(session_budget_usd=300.0, base_clip_usd=4.0)
    snapshot = _snapshot(
        market_tradeable=True,
        market_quality_score=0.9,
        up_best_bid=0.49,
        up_best_ask=0.51,
        dn_best_bid=0.49,
        dn_best_ask=0.51,
    )
    inventory = _inventory(
        dn_shares=10.0,
        total_inventory_value_usd=10.0,
        excess_dn_qty=10.0,
        excess_dn_value_usd=5.0,
        excess_value_usd=5.0,
        signed_excess_value_usd=-5.0,
        free_usdc=295.0,
    )
    base_risk = HardSafetyKernel(cfg).evaluate(
        snapshot=snapshot,
        inventory=inventory,
        analytics=AnalyticsState(
            marketability_guard_active=True,
            marketability_guard_reason="sell_skip_cooldown",
            marketability_churn_confirmed=True,
            marketability_problem_side="dn",
        ),
        health=HealthState(),
    )
    risk = replace(
        base_risk,
        marketability_guard_active=True,
        marketability_guard_reason="sell_skip_cooldown",
        marketability_churn_confirmed=True,
        marketability_problem_side="dn",
        marketability_guard_dn_active=True,
        soft_mode="inventory_skewed",
        target_soft_mode="defensive",
    )
    plan = QuotePolicyV2(cfg).generate(
        snapshot=snapshot,
        inventory=inventory,
        risk=risk,
        ctx=QuoteContext(tick_size=0.01, min_order_size=1.0),
    )
    assert plan.dn_ask is not None
    assert plan.dn_ask.hold_mode_active is True
    assert plan.sell_churn_hold_dn_active is True
    assert plan.sell_churn_hold_side == "dn"
    assert plan.dual_bid_exception_active is True
    assert plan.dual_bid_exception_reason == "sell_churn_hold_mode"


def test_sell_churn_hold_uses_inventory_side_when_raw_problem_side_has_no_inventory():
    cfg = MMConfigV2(session_budget_usd=300.0, base_clip_usd=4.0)
    snapshot = _snapshot(
        market_tradeable=True,
        market_quality_score=0.84,
        fv_up=0.37,
        fv_dn=0.63,
        pm_mid_up=0.24,
        pm_mid_dn=0.77,
        up_best_bid=0.37,
        up_best_ask=0.39,
        dn_best_bid=0.61,
        dn_best_ask=0.63,
    )
    inventory = _inventory(
        up_shares=12.96,
        sellable_up_shares=12.96,
        dn_shares=0.0,
        sellable_dn_shares=0.0,
        total_inventory_value_usd=4.8,
        excess_up_qty=12.96,
        excess_up_value_usd=4.8,
        excess_value_usd=4.8,
        signed_excess_value_usd=4.8,
        free_usdc=294.0,
    )
    base_risk = HardSafetyKernel(cfg).evaluate(
        snapshot=snapshot,
        inventory=inventory,
        analytics=AnalyticsState(
            marketability_guard_active=True,
            marketability_guard_reason="sell_skip_cooldown",
            marketability_churn_confirmed=True,
            marketability_problem_side="dn",
        ),
        health=HealthState(),
    )
    risk = replace(
        base_risk,
        marketability_guard_active=True,
        marketability_guard_reason="sell_skip_cooldown",
        marketability_churn_confirmed=True,
        marketability_problem_side="dn",
        marketability_guard_up_active=True,
        marketability_guard_dn_active=True,
        soft_mode="inventory_skewed",
        target_soft_mode="defensive",
        inventory_side="up",
    )
    plan = QuotePolicyV2(cfg).generate(
        snapshot=snapshot,
        inventory=inventory,
        risk=risk,
        ctx=QuoteContext(tick_size=0.01, min_order_size=5.0),
    )
    assert plan.up_ask is not None
    assert plan.up_ask.hold_mode_active is True
    assert plan.up_ask.hold_mode_reason == "sell_churn_hold_mode"
    assert plan.sell_churn_hold_up_active is True
    assert plan.sell_churn_hold_dn_active is False
    assert plan.sell_churn_hold_side == "up"
    assert plan.dual_bid_exception_active is True
    assert plan.dual_bid_exception_reason == "sell_churn_hold_mode"


def test_sell_churn_hold_prefers_locked_side_over_flapping_problem_side():
    cfg = MMConfigV2(session_budget_usd=300.0, base_clip_usd=4.0)
    snapshot = _snapshot(
        market_tradeable=True,
        market_quality_score=0.84,
        up_best_bid=0.37,
        up_best_ask=0.39,
        dn_best_bid=0.61,
        dn_best_ask=0.63,
    )
    inventory = _inventory(
        up_shares=12.96,
        sellable_up_shares=12.96,
        dn_shares=0.0,
        sellable_dn_shares=0.0,
        total_inventory_value_usd=4.8,
        excess_up_qty=12.96,
        excess_up_value_usd=4.8,
        excess_value_usd=4.8,
        signed_excess_value_usd=4.8,
        free_usdc=294.0,
    )
    base_risk = HardSafetyKernel(cfg).evaluate(
        snapshot=snapshot,
        inventory=inventory,
        analytics=AnalyticsState(
            marketability_guard_active=True,
            marketability_guard_reason="sell_skip_cooldown",
            marketability_churn_confirmed=True,
            marketability_problem_side="dn",
            marketability_side_locked="up",
            marketability_side_lock_age_sec=8.0,
        ),
        health=HealthState(),
    )
    risk = replace(
        base_risk,
        marketability_guard_active=True,
        marketability_guard_reason="sell_skip_cooldown",
        marketability_churn_confirmed=True,
        marketability_problem_side="dn",
        marketability_side_locked="up",
        marketability_side_lock_age_sec=8.0,
        marketability_guard_up_active=True,
        marketability_guard_dn_active=True,
        soft_mode="inventory_skewed",
        target_soft_mode="defensive",
        inventory_side="up",
    )
    plan = QuotePolicyV2(cfg).generate(
        snapshot=snapshot,
        inventory=inventory,
        risk=risk,
        ctx=QuoteContext(tick_size=0.01, min_order_size=5.0),
    )
    assert plan.up_ask is not None
    assert plan.up_ask.hold_mode_reason == "sell_churn_hold_mode"
    assert plan.sell_churn_hold_side == "up"


def test_marketability_churn_suppresses_non_helpful_flat_sells():
    cfg = MMConfigV2(session_budget_usd=300.0, base_clip_usd=4.0)
    snapshot = _snapshot(
        market_tradeable=True,
        market_quality_score=0.84,
        fv_up=0.45,
        fv_dn=0.55,
        pm_mid_up=0.45,
        pm_mid_dn=0.55,
        up_best_bid=0.44,
        up_best_ask=0.46,
        dn_best_bid=0.54,
        dn_best_ask=0.56,
    )
    inventory = _inventory(
        up_shares=0.0,
        sellable_up_shares=0.0,
        dn_shares=0.0,
        sellable_dn_shares=0.0,
        total_inventory_value_usd=0.0,
        excess_value_usd=0.0,
        signed_excess_value_usd=0.0,
        free_usdc=300.0,
    )
    base_risk = HardSafetyKernel(cfg).evaluate(
        snapshot=snapshot,
        inventory=inventory,
        analytics=AnalyticsState(
            marketability_guard_active=True,
            marketability_guard_reason="sell_skip_cooldown",
            marketability_churn_confirmed=True,
            marketability_problem_side="up",
        ),
        health=HealthState(),
    )
    risk = replace(
        base_risk,
        marketability_guard_active=True,
        marketability_guard_reason="sell_skip_cooldown",
        marketability_churn_confirmed=True,
        marketability_problem_side="up",
        marketability_guard_up_active=True,
        marketability_guard_dn_active=True,
        soft_mode="defensive",
        target_soft_mode="defensive",
        inventory_side="flat",
    )
    plan = QuotePolicyV2(cfg).generate(
        snapshot=snapshot,
        inventory=inventory,
        risk=risk,
        ctx=QuoteContext(tick_size=0.01, min_order_size=5.0),
    )
    assert plan.up_ask is None
    assert plan.dn_ask is None
    assert plan.suppressed_reasons["up_ask"] == "marketability_churn_confirmed"
    assert plan.suppressed_reasons["dn_ask"] == "marketability_churn_confirmed"


def test_sell_skip_churn_keeps_buy_quarantine_active_in_unwind():
    cfg = MMConfigV2(session_budget_usd=300.0, base_clip_usd=4.0)
    snapshot = _snapshot(
        market_tradeable=True,
        market_quality_score=0.91,
        fv_up=0.55,
        fv_dn=0.45,
        pm_mid_up=0.52,
        pm_mid_dn=0.49,
        up_best_bid=0.53,
        up_best_ask=0.55,
        dn_best_bid=0.45,
        dn_best_ask=0.47,
        time_left_sec=840.0,
    )
    inventory = _inventory(
        up_shares=4.79,
        sellable_up_shares=4.79,
        dn_shares=0.0,
        sellable_dn_shares=0.0,
        total_inventory_value_usd=2.97,
        excess_up_qty=4.79,
        excess_up_value_usd=2.97,
        excess_value_usd=2.97,
        signed_excess_value_usd=2.97,
        free_usdc=297.0,
    )
    base_risk = HardSafetyKernel(cfg).evaluate(
        snapshot=snapshot,
        inventory=inventory,
        analytics=AnalyticsState(
            marketability_guard_active=True,
            marketability_guard_reason="sell_skip_cooldown",
            marketability_churn_confirmed=True,
            marketability_problem_side="up",
        ),
        health=HealthState(),
    )
    risk = replace(
        base_risk,
        soft_mode="unwind",
        target_soft_mode="unwind",
        inventory_side="up",
        marketability_guard_active=True,
        marketability_guard_reason="sell_skip_cooldown",
        marketability_guard_up_active=True,
        marketability_guard_dn_active=False,
        marketability_churn_confirmed=True,
        marketability_problem_side="up",
    )
    plan = QuotePolicyV2(cfg).generate(
        snapshot=snapshot,
        inventory=inventory,
        risk=risk,
        ctx=QuoteContext(tick_size=0.01, min_order_size=5.0),
    )
    assert plan.up_ask is not None
    assert plan.up_ask.hold_mode_active is True
    assert plan.up_ask.hold_mode_reason == "sell_churn_hold_mode"
    assert plan.dn_bid is None
    assert plan.suppressed_reasons["dn_bid"] == "marketability_churn_confirmed"


def test_diagnostic_no_guards_keeps_two_sided_quotes_despite_guard_suppressions():
    cfg = MMConfigV2(session_budget_usd=300.0, base_clip_usd=14.0)
    snapshot = _snapshot(
        fv_up=0.82,
        fv_dn=0.18,
        pm_mid_up=0.53,
        pm_mid_dn=0.47,
        up_best_bid=0.52,
        up_best_ask=0.54,
        dn_best_bid=0.46,
        dn_best_ask=0.48,
        market_tradeable=True,
        market_quality_score=0.91,
    )
    inventory = _inventory(
        up_shares=9.6,
        sellable_up_shares=9.6,
        excess_up_qty=9.6,
        excess_up_value_usd=4.9,
        excess_value_usd=4.9,
        signed_excess_value_usd=4.9,
        total_inventory_value_usd=4.9,
        free_usdc=295.0,
        pair_value_over_target_usd=4.0,
    )
    base_risk = HardSafetyKernel(cfg).evaluate(
        snapshot=snapshot,
        inventory=inventory,
        analytics=AnalyticsState(
            marketability_guard_active=True,
            marketability_guard_reason="sell_skip_cooldown",
            marketability_churn_confirmed=True,
            marketability_problem_side="up",
            marketability_side_locked="up",
            marketability_side_lock_age_sec=15.0,
        ),
        health=HealthState(),
    )
    risk = replace(
        base_risk,
        soft_mode="normal",
        target_soft_mode="normal",
        marketability_guard_active=True,
        marketability_guard_reason="sell_skip_cooldown",
        marketability_guard_up_active=True,
        marketability_guard_dn_active=True,
        marketability_churn_confirmed=True,
        marketability_problem_side="up",
        marketability_side_locked="up",
        marketability_side_lock_age_sec=15.0,
        inventory_side="up",
    )
    plan = QuotePolicyV2(cfg).generate(
        snapshot=snapshot,
        inventory=inventory,
        risk=risk,
        ctx=QuoteContext(tick_size=0.01, min_order_size=5.0, diagnostic_no_guards=True),
    )
    assert plan.up_bid is not None
    assert plan.dn_bid is not None
    assert plan.up_ask is not None
    assert plan.dn_ask is not None
    assert plan.up_bid.price < float(snapshot.up_best_ask)
    assert plan.dn_bid.price < float(snapshot.dn_best_ask)
    assert plan.quote_viability_reason == "diagnostic_no_guards_active"
    assert plan.dual_bid_exception_active is False
    assert plan.dual_bid_exception_reason == ""


def test_sell_skip_guard_holds_inventory_backed_problem_side_sell_even_if_not_helpful():
    cfg = MMConfigV2(session_budget_usd=300.0, base_clip_usd=4.0)
    snapshot = _snapshot(
        market_tradeable=False,
        market_quality_score=0.6,
        fv_up=0.17,
        fv_dn=0.83,
        pm_mid_up=0.20,
        pm_mid_dn=0.81,
        up_best_bid=0.13,
        up_best_ask=0.15,
        dn_best_bid=0.85,
        dn_best_ask=0.87,
    )
    inventory = _inventory(
        up_shares=7.79,
        sellable_up_shares=7.79,
        dn_shares=6.2,
        sellable_dn_shares=6.2,
        total_inventory_value_usd=9.47,
        excess_up_qty=7.79,
        excess_up_value_usd=1.32,
        excess_value_usd=1.32,
        signed_excess_value_usd=1.32,
        free_usdc=293.4,
    )
    base_risk = HardSafetyKernel(cfg).evaluate(
        snapshot=snapshot,
        inventory=inventory,
        analytics=AnalyticsState(
            marketability_guard_active=True,
            marketability_guard_reason="sell_skip_cooldown",
            marketability_problem_side="dn",
        ),
        health=HealthState(),
    )
    risk = replace(
        base_risk,
        soft_mode="defensive",
        target_soft_mode="defensive",
        inventory_side="up",
        marketability_guard_active=True,
        marketability_guard_reason="sell_skip_cooldown",
        marketability_guard_up_active=False,
        marketability_guard_dn_active=True,
        marketability_problem_side="dn",
    )
    plan = QuotePolicyV2(cfg).generate(
        snapshot=snapshot,
        inventory=inventory,
        risk=risk,
        ctx=QuoteContext(tick_size=0.01, min_order_size=5.0),
    )
    assert plan.dn_ask is not None
    assert plan.dn_ask.inventory_effect == "harmful"
    assert plan.dn_ask.hold_mode_active is True
    assert plan.dn_ask.hold_mode_reason == "sell_churn_hold_mode"
    assert plan.sell_churn_hold_dn_active is True
    assert plan.sell_churn_hold_side == "dn"


def test_sell_skip_churn_keeps_problem_side_helpful_sell_alive_below_min_owned_inventory():
    cfg = MMConfigV2(session_budget_usd=300.0, base_clip_usd=4.0)
    snapshot = _snapshot(
        market_tradeable=True,
        market_quality_score=0.88,
        fv_up=0.47,
        fv_dn=0.53,
        pm_mid_up=0.45,
        pm_mid_dn=0.55,
        up_best_bid=0.44,
        up_best_ask=0.46,
        dn_best_bid=0.54,
        dn_best_ask=0.56,
        time_left_sec=840.0,
    )
    inventory = _inventory(
        up_shares=3.95,
        sellable_up_shares=3.95,
        dn_shares=0.0,
        sellable_dn_shares=0.0,
        total_inventory_value_usd=1.78,
        excess_up_qty=3.95,
        excess_up_value_usd=1.78,
        excess_value_usd=1.78,
        signed_excess_value_usd=1.78,
        free_usdc=298.0,
    )
    base_risk = HardSafetyKernel(cfg).evaluate(
        snapshot=snapshot,
        inventory=inventory,
        analytics=AnalyticsState(
            marketability_guard_active=True,
            marketability_guard_reason="sell_skip_cooldown",
            marketability_churn_confirmed=True,
            marketability_problem_side="up",
        ),
        health=HealthState(),
    )
    risk = replace(
        base_risk,
        soft_mode="defensive",
        target_soft_mode="defensive",
        inventory_side="up",
        marketability_guard_active=True,
        marketability_guard_reason="sell_skip_cooldown",
        marketability_guard_up_active=True,
        marketability_guard_dn_active=False,
        marketability_churn_confirmed=True,
        marketability_problem_side="up",
    )
    plan = QuotePolicyV2(cfg).generate(
        snapshot=snapshot,
        inventory=inventory,
        risk=risk,
        ctx=QuoteContext(tick_size=0.01, min_order_size=5.0),
    )
    assert plan.up_ask is not None
    assert plan.up_ask.size >= 5.0
    assert plan.up_ask.hold_mode_active is True
    assert plan.up_ask.hold_mode_reason == "sell_churn_hold_mode"
    assert plan.dn_bid is None
    assert plan.suppressed_reasons["dn_bid"] == "marketability_churn_confirmed"
    assert plan.quote_balance_state == "helpful_only"



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
        excess_dn_value_usd=18.0,
        excess_value_usd=18.0,
        signed_excess_value_usd=-18.0,
        total_inventory_value_usd=18.0,
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
        assert plan.suppressed_reasons["dn_bid"] in {
            "below_min_order_size",
            "harmful_suppressed_in_defensive",
            "dual_bid_guard_viability",
        }


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
        excess_dn_value_usd=18.0,
        excess_value_usd=18.0,
        signed_excess_value_usd=-18.0,
        total_inventory_value_usd=18.0,
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
    assert max(intent.size for intent in helpful_intents) <= 20.0


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


def test_defensive_suppresses_harmful_intents_unconditionally():
    cfg = MMConfigV2(session_budget_usd=15.0, base_clip_usd=6.0)
    snapshot = _snapshot()
    inventory = _inventory(
        dn_shares=6.0,
        excess_dn_qty=6.0,
        excess_dn_value_usd=5.6,
        excess_value_usd=5.6,
        signed_excess_value_usd=-5.6,
        total_inventory_value_usd=5.6,
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
    assert plan.dn_bid is None
    assert plan.up_ask is None
    assert plan.suppressed_reasons.get("dn_bid") == "harmful_suppressed_in_defensive"
    assert plan.suppressed_reasons.get("up_ask") == "harmful_suppressed_in_defensive"
    assert plan.up_bid is not None
    assert plan.dn_ask is not None


def test_unwind_suppresses_harmful_intents_unconditionally():
    cfg = MMConfigV2(session_budget_usd=15.0, base_clip_usd=6.0)
    snapshot = _snapshot()
    inventory = _inventory(
        dn_shares=11.0,
        excess_dn_qty=11.0,
        excess_dn_value_usd=7.2,
        excess_value_usd=7.2,
        signed_excess_value_usd=-7.2,
        total_inventory_value_usd=7.2,
    )
    risk = HardSafetyKernel(cfg).evaluate(
        snapshot=snapshot,
        inventory=inventory,
        analytics=AnalyticsState(),
        health=HealthState(),
    )
    assert risk.soft_mode == "unwind"
    plan = QuotePolicyV2(cfg).generate(
        snapshot=snapshot,
        inventory=inventory,
        risk=risk,
        ctx=QuoteContext(tick_size=0.01, min_order_size=5.0),
    )
    assert plan.dn_bid is None
    assert plan.up_ask is None
    assert plan.suppressed_reasons.get("dn_bid") == "harmful_suppressed_in_unwind"
    assert plan.suppressed_reasons.get("up_ask") == "harmful_suppressed_in_unwind"


def test_protective_modes_emit_no_inventory_expanding_final_plan():
    for excess in (5.6, 7.2):
        cfg = MMConfigV2(session_budget_usd=15.0, base_clip_usd=6.0)
        snapshot = _snapshot()
        inventory = _inventory(
            dn_shares=11.0 if excess > 4.0 else 6.0,
            excess_dn_qty=11.0 if excess > 4.0 else 6.0,
            excess_dn_value_usd=excess,
            excess_value_usd=excess,
            signed_excess_value_usd=-excess,
            total_inventory_value_usd=excess,
        )
        risk = HardSafetyKernel(cfg).evaluate(
            snapshot=snapshot,
            inventory=inventory,
            analytics=AnalyticsState(),
            health=HealthState(),
        )
        assert risk.soft_mode in {"defensive", "unwind"}
        plan = QuotePolicyV2(cfg).generate(
            snapshot=snapshot,
            inventory=inventory,
            risk=risk,
            ctx=QuoteContext(tick_size=0.01, min_order_size=5.0),
        )
        assert plan.dn_bid is None
        assert plan.up_ask is None


def test_target_pair_ratio_cap_applies_only_in_defensive_and_unwind():
    cfg = MMConfigV2(session_budget_usd=30.0, base_clip_usd=6.0, target_pair_value_ratio=0.70)
    snapshot = _snapshot()
    inventory = _inventory(
        up_shares=10.0,
        dn_shares=10.0,
        paired_qty=10.0,
        paired_value_usd=10.0,
        total_inventory_value_usd=24.0,
        target_pair_value_usd=21.0,
        pair_value_ratio=0.80,
        pair_value_over_target_usd=3.0,
        free_usdc=30.0,
    )
    risk = HardSafetyKernel(cfg).evaluate(
        snapshot=snapshot,
        inventory=inventory,
        analytics=AnalyticsState(),
        health=HealthState(),
    )

    risk.soft_mode = "normal"
    risk.target_soft_mode = "normal"
    plan_normal = QuotePolicyV2(cfg).generate(
        snapshot=snapshot,
        inventory=inventory,
        risk=risk,
        ctx=QuoteContext(tick_size=0.01, min_order_size=1.0),
    )
    assert "target_pair_ratio_cap" not in set(plan_normal.suppressed_reasons.values())

    risk.soft_mode = "defensive"
    risk.target_soft_mode = "defensive"
    plan_defensive = QuotePolicyV2(cfg).generate(
        snapshot=snapshot,
        inventory=inventory,
        risk=risk,
        ctx=QuoteContext(tick_size=0.01, min_order_size=1.0),
    )
    assert "target_pair_ratio_cap" in set(plan_defensive.suppressed_reasons.values())

    risk.soft_mode = "unwind"
    risk.target_soft_mode = "unwind"
    plan_unwind = QuotePolicyV2(cfg).generate(
        snapshot=snapshot,
        inventory=inventory,
        risk=risk,
        ctx=QuoteContext(tick_size=0.01, min_order_size=1.0),
    )
    assert "target_pair_ratio_cap" in set(plan_unwind.suppressed_reasons.values())


def test_target_ratio_deadband_prevents_false_inventory_skewed_trigger():
    cfg = MMConfigV2(session_budget_usd=30.0, target_pair_value_ratio=0.50)
    snapshot = _snapshot(
        market_quality_score=0.95,
        market_tradeable=True,
        divergence_up=0.01,
        divergence_dn=0.01,
    )
    inventory_below_deadband = _inventory(
        total_inventory_value_usd=18.0,
        target_pair_value_usd=15.0,
        pair_value_ratio=18.0 / 30.0,
        pair_value_over_target_usd=3.0,
        excess_value_usd=0.2,
        signed_excess_value_usd=0.2,
    )
    risk_below = HardSafetyKernel(cfg).evaluate(
        snapshot=snapshot,
        inventory=inventory_below_deadband,
        analytics=AnalyticsState(),
        health=HealthState(),
    )
    assert cfg.effective_target_ratio_activation_usd() == pytest.approx(4.0)
    assert risk_below.target_soft_mode == "normal"

    inventory_above_deadband = _inventory(
        total_inventory_value_usd=19.5,
        target_pair_value_usd=15.0,
        pair_value_ratio=19.5 / 30.0,
        pair_value_over_target_usd=4.5,
        excess_value_usd=0.2,
        signed_excess_value_usd=0.2,
    )
    risk_above = HardSafetyKernel(cfg).evaluate(
        snapshot=snapshot,
        inventory=inventory_above_deadband,
        analytics=AnalyticsState(),
        health=HealthState(),
    )
    assert risk_above.target_soft_mode == "inventory_skewed"


def test_final_maker_cross_guard_suppresses_unplaceable_endpoint_sell():
    cfg = MMConfigV2(base_clip_usd=4.0)
    snapshot = _snapshot(
        fv_up=0.98,
        pm_mid_up=0.98,
        up_best_bid=0.99,
        up_best_ask=0.99,
        fv_dn=0.02,
        pm_mid_dn=0.02,
        dn_best_bid=0.01,
        dn_best_ask=0.02,
    )
    inventory = _inventory(up_shares=10.0, free_usdc=30.0)
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
    assert plan.up_ask is None
    assert plan.suppressed_reasons.get("up_ask") == "maker_cross_guard"


def test_protective_modes_still_block_gross_inventory_expansion():
    cfg = MMConfigV2(session_budget_usd=30.0, base_clip_usd=6.0, target_pair_value_ratio=0.70)
    snapshot = _snapshot()
    inventory = _inventory(
        up_shares=12.0,
        dn_shares=8.0,
        paired_qty=8.0,
        excess_up_qty=4.0,
        paired_value_usd=8.0,
        excess_up_value_usd=2.0,
        total_inventory_value_usd=24.0,
        target_pair_value_usd=21.0,
        pair_value_ratio=0.80,
        pair_value_over_target_usd=3.0,
        excess_value_usd=2.0,
        signed_excess_value_usd=2.0,
        free_usdc=30.0,
    )
    risk = HardSafetyKernel(cfg).evaluate(
        snapshot=snapshot,
        inventory=inventory,
        analytics=AnalyticsState(),
        health=HealthState(),
    )
    risk.soft_mode = "defensive"
    risk.target_soft_mode = "defensive"
    plan = QuotePolicyV2(cfg).generate(
        snapshot=snapshot,
        inventory=inventory,
        risk=risk,
        ctx=QuoteContext(tick_size=0.01, min_order_size=1.0),
    )
    assert plan.up_bid is None
    assert plan.dn_bid is None
    assert plan.suppressed_reasons.get("up_bid") in {
        "target_pair_ratio_cap",
        "harmful_suppressed_in_defensive",
    }
    assert plan.suppressed_reasons.get("dn_bid") in {
        "target_pair_ratio_cap",
        "harmful_suppressed_in_defensive",
    }


def test_inventory_skewed_blocks_harmful_buy_when_excess_above_guard():
    cfg = MMConfigV2(
        session_budget_usd=50.0,
        base_clip_usd=6.0,
        harmful_buy_suppress_ratio=0.20,
    )
    snapshot = _snapshot()
    inventory = _inventory(
        dn_shares=12.0,
        excess_dn_qty=12.0,
        excess_dn_value_usd=12.0,
        excess_value_usd=12.0,
        signed_excess_value_usd=-12.0,
        total_inventory_value_usd=12.0,
        free_usdc=50.0,
    )
    risk = HardSafetyKernel(cfg).evaluate(
        snapshot=snapshot,
        inventory=inventory,
        analytics=AnalyticsState(),
        health=HealthState(),
    )
    assert risk.soft_mode == "inventory_skewed"
    assert cfg.effective_harmful_buy_suppress_usd() == pytest.approx(10.0)
    plan = QuotePolicyV2(cfg).generate(
        snapshot=snapshot,
        inventory=inventory,
        risk=risk,
        ctx=QuoteContext(tick_size=0.01, min_order_size=1.0),
    )
    assert plan.dn_bid is None
    assert plan.suppressed_reasons.get("dn_bid") == "harmful_buy_blocked_high_skew"
    assert plan.up_bid is not None
    assert plan.dn_ask is not None


def test_inventory_skewed_keeps_harmful_buy_below_guard():
    cfg = MMConfigV2(
        session_budget_usd=50.0,
        base_clip_usd=6.0,
        harmful_buy_suppress_ratio=0.40,
    )
    snapshot = _snapshot()
    inventory = _inventory(
        dn_shares=12.0,
        excess_dn_qty=12.0,
        excess_dn_value_usd=12.0,
        excess_value_usd=12.0,
        signed_excess_value_usd=-12.0,
        total_inventory_value_usd=12.0,
        free_usdc=50.0,
    )
    risk = HardSafetyKernel(cfg).evaluate(
        snapshot=snapshot,
        inventory=inventory,
        analytics=AnalyticsState(),
        health=HealthState(),
    )
    assert risk.soft_mode == "inventory_skewed"
    assert cfg.effective_harmful_buy_suppress_usd() == pytest.approx(20.0)
    plan = QuotePolicyV2(cfg).generate(
        snapshot=snapshot,
        inventory=inventory,
        risk=risk,
        ctx=QuoteContext(tick_size=0.01, min_order_size=1.0),
    )
    assert plan.dn_bid is not None
    assert plan.dn_bid.inventory_effect == "harmful"
    assert plan.suppressed_reasons.get("dn_bid") != "harmful_buy_blocked_high_skew"


def test_pre_protective_target_blocks_harmful_buy_before_soft_mode_flips():
    cfg = MMConfigV2(session_budget_usd=30.0, base_clip_usd=4.0, harmful_buy_suppress_ratio=0.30)
    snapshot = _snapshot()
    inventory = _inventory(
        free_usdc=30.0,
        dn_shares=8.0,
        excess_dn_qty=8.0,
        excess_dn_value_usd=9.2,
        excess_value_usd=9.2,
        signed_excess_value_usd=-9.2,
        total_inventory_value_usd=9.2,
    )
    risk = HardSafetyKernel(cfg).evaluate(
        snapshot=snapshot,
        inventory=inventory,
        analytics=AnalyticsState(),
        health=HealthState(),
    )
    risk = replace(risk, soft_mode="normal", target_soft_mode="unwind")
    plan = QuotePolicyV2(cfg).generate(
        snapshot=snapshot,
        inventory=inventory,
        risk=risk,
        ctx=QuoteContext(tick_size=0.01, min_order_size=1.0),
    )
    assert plan.dn_bid is None
    assert plan.suppressed_reasons.get("dn_bid") == "harmful_buy_blocked_pre_protective"
    assert plan.up_bid is not None


def test_dual_bid_guard_recovers_harmful_bid_near_high_skew_guard():
    cfg = MMConfigV2(session_budget_usd=30.0, base_clip_usd=4.0, harmful_buy_suppress_ratio=0.30)
    snapshot = _snapshot()
    inventory = _inventory(
        free_usdc=30.0,
        dn_shares=8.0,
        excess_dn_qty=8.0,
        excess_dn_value_usd=9.2,
        excess_value_usd=9.2,
        signed_excess_value_usd=-9.2,
        total_inventory_value_usd=9.2,
    )
    risk = HardSafetyKernel(cfg).evaluate(
        snapshot=snapshot,
        inventory=inventory,
        analytics=AnalyticsState(),
        health=HealthState(),
    )
    assert risk.soft_mode == "inventory_skewed"
    assert risk.target_soft_mode == "inventory_skewed"
    plan = QuotePolicyV2(cfg).generate(
        snapshot=snapshot,
        inventory=inventory,
        risk=risk,
        ctx=QuoteContext(tick_size=0.01, min_order_size=5.0),
    )
    assert plan.up_bid is not None
    assert plan.dn_bid is not None
    assert plan.dn_bid.inventory_effect == "harmful"
    assert plan.dn_bid.post_only is True
    assert plan.suppressed_reasons.get("dn_bid") == "dual_bid_guard_applied"


def test_dynamic_side_floor_makes_expensive_bid_pass_min_order_without_raising_default_clip():
    cfg = MMConfigV2(session_budget_usd=30.0, base_clip_usd=4.0)
    snapshot = _snapshot(
        fv_up=0.90,
        fv_dn=0.10,
        pm_mid_up=0.90,
        pm_mid_dn=0.10,
        up_best_bid=0.89,
        up_best_ask=0.91,
        dn_best_bid=0.09,
        dn_best_ask=0.11,
    )
    inventory = _inventory(free_usdc=30.0)
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
    assert cfg.base_clip_usd == pytest.approx(4.0)
    assert plan.up_bid is not None
    assert plan.up_bid.size >= 5.0


def test_inventory_skewed_dual_bid_guard_restores_missing_bid_when_headroom_sufficient(monkeypatch):
    cfg = MMConfigV2(session_budget_usd=30.0, base_clip_usd=4.0, harmful_buy_suppress_ratio=0.40)
    snapshot = _snapshot(
        time_left_sec=600.0,
        pm_mid_up=0.54,
        up_best_bid=0.54,
        up_best_ask=0.56,
        pm_mid_dn=0.46,
        dn_best_bid=0.46,
        dn_best_ask=0.48,
    )
    inventory = _inventory(
        free_usdc=30.0,
        excess_up_qty=12.0,
        excess_up_value_usd=7.0,
        excess_value_usd=7.0,
        signed_excess_value_usd=7.0,
        total_inventory_value_usd=7.0,
    )
    risk = HardSafetyKernel(cfg).evaluate(
        snapshot=snapshot,
        inventory=inventory,
        analytics=AnalyticsState(),
        health=HealthState(),
    )
    assert risk.soft_mode == "inventory_skewed"

    policy = QuotePolicyV2(cfg)
    original = policy._make_intent
    dn_bid_calls = {"count": 0}

    def _patched_make_intent(*args, **kwargs):
        token = kwargs.get("token")
        side = kwargs.get("side")
        if token == snapshot.dn_token_id and side == "BUY":
            dn_bid_calls["count"] += 1
            if dn_bid_calls["count"] == 1:
                return None, "below_min_order_size"
        return original(*args, **kwargs)

    monkeypatch.setattr(policy, "_make_intent", _patched_make_intent)
    plan = policy.generate(
        snapshot=snapshot,
        inventory=inventory,
        risk=risk,
        ctx=QuoteContext(tick_size=0.01, min_order_size=5.0),
    )
    assert dn_bid_calls["count"] >= 2
    assert plan.dn_bid is not None
    assert plan.up_bid is not None
    assert plan.suppressed_reasons.get("dn_bid") == "dual_bid_guard_applied"


def test_dual_bid_guard_recovers_cheap_side_bid_with_side_aware_cap():
    cfg = MMConfigV2(session_budget_usd=30.0, base_clip_usd=4.0)
    snapshot = _snapshot(
        time_left_sec=600.0,
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
        free_usdc=30.0,
        dn_shares=4.5,
        excess_dn_qty=4.5,
        excess_dn_value_usd=4.5,
        excess_value_usd=4.5,
        signed_excess_value_usd=-4.5,
        total_inventory_value_usd=4.5,
    )
    risk = HardSafetyKernel(cfg).evaluate(
        snapshot=snapshot,
        inventory=inventory,
        analytics=AnalyticsState(),
        health=HealthState(),
    )
    assert risk.soft_mode == "normal"
    assert risk.inventory_side == "dn"

    plan = QuotePolicyV2(cfg).generate(
        snapshot=snapshot,
        inventory=inventory,
        risk=risk,
        ctx=QuoteContext(tick_size=0.01, min_order_size=5.0),
    )
    assert plan.up_bid is not None
    assert plan.dn_bid is not None
    assert plan.dn_bid.size >= 5.0
    assert plan.suppressed_reasons.get("dn_bid") == "dual_bid_guard_applied"


def test_dual_bid_guard_does_not_run_in_defensive_unwind(monkeypatch):
    cfg = MMConfigV2(session_budget_usd=30.0, base_clip_usd=4.0)
    snapshot = _snapshot(time_left_sec=600.0)
    inventory = _inventory(
        free_usdc=30.0,
        excess_up_qty=20.0,
        excess_up_value_usd=12.0,
        excess_value_usd=12.0,
        signed_excess_value_usd=12.0,
        total_inventory_value_usd=12.0,
    )
    risk = HardSafetyKernel(cfg).evaluate(
        snapshot=snapshot,
        inventory=inventory,
        analytics=AnalyticsState(),
        health=HealthState(),
    )
    risk.soft_mode = "defensive"
    risk.target_soft_mode = "defensive"

    policy = QuotePolicyV2(cfg)
    original = policy._make_intent
    dn_bid_calls = {"count": 0}

    def _patched_make_intent(*args, **kwargs):
        token = kwargs.get("token")
        side = kwargs.get("side")
        if token == snapshot.dn_token_id and side == "BUY":
            dn_bid_calls["count"] += 1
            return None, "below_min_order_size"
        return original(*args, **kwargs)

    monkeypatch.setattr(policy, "_make_intent", _patched_make_intent)
    plan = policy.generate(
        snapshot=snapshot,
        inventory=inventory,
        risk=risk,
        ctx=QuoteContext(tick_size=0.01, min_order_size=5.0),
    )
    assert dn_bid_calls["count"] <= 1
    assert "dual_bid_guard_applied" not in set(plan.suppressed_reasons.values())
    assert "dual_bid_guard_headroom" not in set(plan.suppressed_reasons.values())
    assert "dual_bid_guard_viability" not in set(plan.suppressed_reasons.values())


def test_dual_bid_guard_emits_explicit_reason_when_not_recoverable(monkeypatch):
    cfg = MMConfigV2(session_budget_usd=30.0, base_clip_usd=4.0)
    snapshot = _snapshot(time_left_sec=600.0)
    inventory = _inventory(free_usdc=1.0)
    risk = HardSafetyKernel(cfg).evaluate(
        snapshot=snapshot,
        inventory=inventory,
        analytics=AnalyticsState(),
        health=HealthState(),
    )
    assert risk.soft_mode == "normal"

    policy = QuotePolicyV2(cfg)
    original = policy._make_intent
    dn_bid_calls = {"count": 0}

    def _patched_make_intent(*args, **kwargs):
        token = kwargs.get("token")
        side = kwargs.get("side")
        if token == snapshot.dn_token_id and side == "BUY":
            dn_bid_calls["count"] += 1
            if dn_bid_calls["count"] == 1:
                return None, "below_min_order_size"
        return original(*args, **kwargs)

    monkeypatch.setattr(policy, "_make_intent", _patched_make_intent)
    plan = policy.generate(
        snapshot=snapshot,
        inventory=inventory,
        risk=risk,
        ctx=QuoteContext(tick_size=0.01, min_order_size=5.0),
    )
    assert dn_bid_calls["count"] == 1
    assert plan.dn_bid is None
    assert plan.suppressed_reasons.get("dn_bid") in {"dual_bid_guard_headroom", "below_min_order_size"}
    assert plan.suppressed_reasons.get("dn_bid") != "dual_bid_guard_applied"


def test_side_soft_brake_keeps_dual_bids_without_hard_block():
    cfg = MMConfigV2(session_budget_usd=30.0, base_clip_usd=4.0)
    snapshot = _snapshot(time_left_sec=600.0, market_tradeable=True)
    inventory = _inventory(free_usdc=30.0)
    risk = HardSafetyKernel(cfg).evaluate(
        snapshot=snapshot,
        inventory=inventory,
        analytics=AnalyticsState(),
        health=HealthState(),
    )
    risk = replace(
        risk,
        soft_mode="normal",
        target_soft_mode="normal",
        toxic_fill_streak_up=1,
        toxic_fill_streak_dn=1,
        negative_spread_capture_streak_up=1,
        negative_spread_capture_streak_dn=1,
        side_soft_brake_up_active=True,
        side_soft_brake_dn_active=True,
        side_reentry_cooldown_up_sec=0.0,
        side_reentry_cooldown_dn_sec=0.0,
        side_hard_block_up_sec=0.0,
        side_hard_block_dn_sec=0.0,
    )
    plan = QuotePolicyV2(cfg).generate(
        snapshot=snapshot,
        inventory=inventory,
        risk=risk,
        ctx=QuoteContext(tick_size=0.01, min_order_size=5.0),
    )
    assert plan.up_bid is not None
    assert plan.dn_bid is not None
    assert plan.quote_viability_reason == "side_reentry_soft_brake"
    assert plan.suppressed_reasons.get("up_bid") is None
    assert plan.suppressed_reasons.get("dn_bid") is None


def test_simultaneous_hard_block_keeps_less_toxic_bid_midpoint_safe():
    cfg = MMConfigV2(session_budget_usd=30.0, base_clip_usd=4.0)
    snapshot = _snapshot(
        time_left_sec=600.0,
        market_tradeable=True,
        up_best_bid=0.52,
        up_best_ask=0.54,
        dn_best_bid=0.46,
        dn_best_ask=0.48,
    )
    inventory = _inventory(free_usdc=30.0)
    risk = HardSafetyKernel(cfg).evaluate(
        snapshot=snapshot,
        inventory=inventory,
        analytics=AnalyticsState(),
        health=HealthState(),
    )
    risk = replace(
        risk,
        soft_mode="normal",
        target_soft_mode="normal",
        toxic_fill_streak_up=1,
        toxic_fill_streak_dn=2,
        negative_spread_capture_streak_up=2,
        negative_spread_capture_streak_dn=3,
        side_soft_brake_up_active=True,
        side_soft_brake_dn_active=True,
        side_reentry_cooldown_up_sec=6.0,
        side_reentry_cooldown_dn_sec=12.0,
        side_hard_block_up_sec=6.0,
        side_hard_block_dn_sec=12.0,
    )
    plan = QuotePolicyV2(cfg).generate(
        snapshot=snapshot,
        inventory=inventory,
        risk=risk,
        ctx=QuoteContext(tick_size=0.01, min_order_size=5.0),
    )
    assert plan.simultaneous_bid_block_prevented == 1
    assert (plan.up_bid is not None) ^ (plan.dn_bid is not None)
    assert plan.up_bid is not None
    assert plan.up_bid.post_only is True
    assert plan.up_bid.price < snapshot.up_best_ask
    assert plan.suppressed_reasons.get("up_bid") == "dual_bid_cooldown_coordination"
    assert plan.suppressed_reasons.get("dn_bid") == "side_reentry_cooldown"


def test_divergence_buy_soft_brake_widens_and_reduces_toxic_bid():
    cfg = MMConfigV2(session_budget_usd=30.0, base_clip_usd=4.0)
    baseline_snapshot = _snapshot(
        time_left_sec=600.0,
        market_tradeable=True,
        midpoint_anchor_up=0.60,
        midpoint_anchor_dn=0.40,
        up_best_bid=0.58,
        up_best_ask=0.70,
        model_anchor_up=0.60,
        model_anchor_dn=0.40,
        buy_edge_gap_up=0.0,
        buy_edge_gap_dn=0.0,
    )
    snapshot = _snapshot(
        time_left_sec=600.0,
        market_tradeable=True,
        midpoint_anchor_up=0.60,
        midpoint_anchor_dn=0.40,
        up_best_bid=0.58,
        up_best_ask=0.70,
        model_anchor_up=0.50,
        model_anchor_dn=0.50,
        buy_edge_gap_up=0.10,
        buy_edge_gap_dn=0.0,
    )
    inventory = _inventory(free_usdc=30.0)
    risk = HardSafetyKernel(cfg).evaluate(
        snapshot=snapshot,
        inventory=inventory,
        analytics=AnalyticsState(),
        health=HealthState(),
    )
    baseline_risk = HardSafetyKernel(cfg).evaluate(
        snapshot=baseline_snapshot,
        inventory=inventory,
        analytics=AnalyticsState(),
        health=HealthState(),
    )
    baseline_plan = QuotePolicyV2(cfg).generate(
        snapshot=baseline_snapshot,
        inventory=inventory,
        risk=baseline_risk,
        ctx=QuoteContext(tick_size=0.01, min_order_size=1.0),
    )
    plan = QuotePolicyV2(cfg).generate(
        snapshot=snapshot,
        inventory=inventory,
        risk=risk,
        ctx=QuoteContext(tick_size=0.01, min_order_size=1.0),
    )

    assert plan.up_bid is not None
    assert baseline_plan.up_bid is not None
    assert plan.divergence_soft_brake_up_active is True
    assert plan.divergence_hard_suppress_up_active is False
    assert plan.divergence_soft_brake_hits >= 1
    assert plan.up_bid.price <= baseline_plan.up_bid.price
    assert plan.up_bid.size < baseline_plan.up_bid.size


def test_divergence_buy_hard_suppress_blocks_toxic_bid_and_sets_dual_bid_exception():
    cfg = MMConfigV2(session_budget_usd=30.0, base_clip_usd=4.0)
    snapshot = _snapshot(
        time_left_sec=600.0,
        market_tradeable=True,
        midpoint_anchor_up=0.68,
        midpoint_anchor_dn=0.32,
        model_anchor_up=0.17,
        model_anchor_dn=0.83,
        buy_edge_gap_up=0.51,
        buy_edge_gap_dn=0.0,
        up_best_bid=0.67,
        up_best_ask=0.69,
        dn_best_bid=0.31,
        dn_best_ask=0.33,
    )
    inventory = _inventory(free_usdc=30.0)
    risk = replace(
        HardSafetyKernel(cfg).evaluate(
            snapshot=snapshot,
            inventory=inventory,
            analytics=AnalyticsState(),
            health=HealthState(),
        ),
        soft_mode="normal",
        target_soft_mode="normal",
    )
    plan = QuotePolicyV2(cfg).generate(
        snapshot=snapshot,
        inventory=inventory,
        risk=risk,
        ctx=QuoteContext(tick_size=0.01, min_order_size=1.0),
    )

    assert plan.up_bid is None
    assert plan.dn_bid is not None
    assert plan.suppressed_reasons.get("up_bid") == "divergence_buy_hard_suppress"
    assert plan.divergence_hard_suppress_up_active is True
    assert plan.dual_bid_exception_active is True
    assert plan.dual_bid_exception_reason == "divergence_buy_hard_suppress"


def test_harmful_buy_brake_reduces_clip_as_excess_grows():
    cfg = MMConfigV2(session_budget_usd=30.0, base_clip_usd=4.0, harmful_buy_suppress_ratio=0.50)
    snapshot = _snapshot()
    low_inventory = _inventory(
        free_usdc=30.0,
        dn_shares=10.0,
        excess_dn_qty=10.0,
        excess_dn_value_usd=6.2,
        excess_value_usd=6.2,
        signed_excess_value_usd=-6.2,
        total_inventory_value_usd=6.2,
    )
    high_inventory = _inventory(
        free_usdc=30.0,
        dn_shares=10.0,
        excess_dn_qty=10.0,
        excess_dn_value_usd=9.5,
        excess_value_usd=9.5,
        signed_excess_value_usd=-9.5,
        total_inventory_value_usd=9.5,
    )
    risk_low = HardSafetyKernel(cfg).evaluate(
        snapshot=snapshot,
        inventory=low_inventory,
        analytics=AnalyticsState(),
        health=HealthState(),
    )
    risk_high = HardSafetyKernel(cfg).evaluate(
        snapshot=snapshot,
        inventory=high_inventory,
        analytics=AnalyticsState(),
        health=HealthState(),
    )
    assert risk_low.soft_mode == "inventory_skewed"
    assert risk_high.soft_mode == "inventory_skewed"
    policy = QuotePolicyV2(cfg)
    low_plan = policy.generate(
        snapshot=snapshot,
        inventory=low_inventory,
        risk=risk_low,
        ctx=QuoteContext(tick_size=0.01, min_order_size=1.0),
    )
    high_plan = policy.generate(
        snapshot=snapshot,
        inventory=high_inventory,
        risk=risk_high,
        ctx=QuoteContext(tick_size=0.01, min_order_size=1.0),
    )
    assert low_plan.dn_bid is not None and low_plan.dn_bid.inventory_effect == "harmful"
    assert high_plan.dn_bid is not None and high_plan.dn_bid.inventory_effect == "harmful"
    high_notional = float(high_plan.dn_bid.size) * float(high_plan.dn_bid.price)
    low_notional = float(low_plan.dn_bid.size) * float(low_plan.dn_bid.price)
    assert high_notional < low_notional
    assert low_plan.harmful_buy_brake_active is True
    assert high_plan.harmful_buy_brake_active is True
    assert high_plan.harmful_buy_brake_hits >= low_plan.harmful_buy_brake_hits
    assert high_plan.harmful_buy_brake_hits >= 1


def test_harmful_buy_brake_does_not_degrade_helpful_intents(monkeypatch):
    cfg = MMConfigV2(session_budget_usd=30.0, base_clip_usd=4.0, harmful_buy_suppress_ratio=0.50)
    snapshot = _snapshot()
    inventory = _inventory(
        free_usdc=30.0,
        dn_shares=10.0,
        excess_dn_qty=10.0,
        excess_dn_value_usd=9.5,
        excess_value_usd=9.5,
        signed_excess_value_usd=-9.5,
        total_inventory_value_usd=9.5,
    )
    risk = HardSafetyKernel(cfg).evaluate(
        snapshot=snapshot,
        inventory=inventory,
        analytics=AnalyticsState(),
        health=HealthState(),
    )
    assert risk.soft_mode == "inventory_skewed"
    policy_braked = QuotePolicyV2(cfg)
    policy_unbraked = QuotePolicyV2(cfg)
    monkeypatch.setattr(policy_braked, "_harmful_buy_brake_mult", lambda **_kwargs: 0.30)
    monkeypatch.setattr(policy_unbraked, "_harmful_buy_brake_mult", lambda **_kwargs: 1.0)
    plan_braked = policy_braked.generate(
        snapshot=snapshot,
        inventory=inventory,
        risk=risk,
        ctx=QuoteContext(tick_size=0.01, min_order_size=1.0),
    )
    plan_unbraked = policy_unbraked.generate(
        snapshot=snapshot,
        inventory=inventory,
        risk=risk,
        ctx=QuoteContext(tick_size=0.01, min_order_size=1.0),
    )
    assert plan_braked.up_bid is not None and plan_unbraked.up_bid is not None
    assert plan_braked.dn_bid is not None and plan_unbraked.dn_bid is not None
    braked_helpful_notional = float(plan_braked.up_bid.size) * float(plan_braked.up_bid.price)
    unbraked_helpful_notional = float(plan_unbraked.up_bid.size) * float(plan_unbraked.up_bid.price)
    braked_harmful_notional = float(plan_braked.dn_bid.size) * float(plan_braked.dn_bid.price)
    unbraked_harmful_notional = float(plan_unbraked.dn_bid.size) * float(plan_unbraked.dn_bid.price)
    assert braked_helpful_notional == pytest.approx(unbraked_helpful_notional)
    assert braked_harmful_notional < unbraked_harmful_notional


def test_harmful_buy_blocked_on_deep_drawdown_in_skewed_mode():
    cfg = MMConfigV2(session_budget_usd=30.0, base_clip_usd=4.0)
    snapshot = _snapshot()
    inventory = _inventory(
        free_usdc=20.0,
        dn_shares=12.0,
        excess_dn_qty=12.0,
        excess_dn_value_usd=7.0,
        excess_value_usd=7.0,
        signed_excess_value_usd=-7.0,
        total_inventory_value_usd=7.0,
    )
    risk = HardSafetyKernel(cfg).evaluate(
        snapshot=snapshot,
        inventory=inventory,
        analytics=AnalyticsState(session_pnl_equity_usd=-7.0, session_pnl=-7.0),
        health=HealthState(),
    )
    assert risk.soft_mode == "inventory_skewed"
    assert risk.early_drawdown_pressure >= 0.75
    plan = QuotePolicyV2(cfg).generate(
        snapshot=snapshot,
        inventory=inventory,
        risk=risk,
        ctx=QuoteContext(tick_size=0.01, min_order_size=1.0),
    )
    assert plan.up_bid is not None and plan.up_bid.inventory_effect == "helpful"
    assert plan.dn_bid is None
    assert plan.suppressed_reasons.get("dn_bid") == "harmful_buy_blocked_drawdown"


def test_gross_inventory_brake_reduces_buy_clip_before_block_threshold():
    cfg = MMConfigV2(session_budget_usd=30.0, base_clip_usd=4.0)
    snapshot = _snapshot()
    base_inventory = _inventory(
        free_usdc=20.0,
        dn_shares=8.0,
        excess_dn_qty=8.0,
        excess_dn_value_usd=6.5,
        excess_value_usd=6.5,
        signed_excess_value_usd=-6.5,
        total_inventory_value_usd=8.0,
        target_pair_value_usd=15.0,
    )
    risk = HardSafetyKernel(cfg).evaluate(
        snapshot=snapshot,
        inventory=base_inventory,
        analytics=AnalyticsState(),
        health=HealthState(),
    )
    unbraked = QuotePolicyV2(cfg).generate(
        snapshot=snapshot,
        inventory=base_inventory,
        risk=risk,
        ctx=QuoteContext(tick_size=0.01, min_order_size=1.0),
    )
    braked_inventory = replace(
        base_inventory,
        pair_value_over_target_usd=2.0,
        pair_value_ratio=0.57,
        total_inventory_value_usd=17.0,
    )
    braked = QuotePolicyV2(cfg).generate(
        snapshot=snapshot,
        inventory=braked_inventory,
        risk=risk,
        ctx=QuoteContext(tick_size=0.01, min_order_size=1.0),
    )
    assert unbraked.up_bid is not None and braked.up_bid is not None
    assert braked.gross_inventory_brake_active is True
    assert braked.up_bid.size < unbraked.up_bid.size


def test_pair_over_target_buy_block_blocks_pair_expanding_buys():
    cfg = MMConfigV2(session_budget_usd=30.0, base_clip_usd=4.0)
    snapshot = _snapshot()
    inventory = _inventory(
        free_usdc=18.0,
        dn_shares=8.0,
        excess_dn_qty=8.0,
        excess_dn_value_usd=6.5,
        excess_value_usd=6.5,
        signed_excess_value_usd=-6.5,
        total_inventory_value_usd=18.5,
        target_pair_value_usd=15.0,
        pair_value_over_target_usd=3.5,
        pair_value_ratio=0.62,
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
        ctx=QuoteContext(tick_size=0.01, min_order_size=1.0),
    )
    assert risk.soft_mode == "inventory_skewed"
    assert plan.up_bid is None
    assert plan.dn_bid is None
    assert plan.pair_over_target_buy_blocks == 2
    assert plan.suppressed_reasons["up_bid"] == "pair_over_target_buy_block"
    assert plan.suppressed_reasons["dn_bid"] == "pair_over_target_buy_block"


def test_dual_bid_guard_preserves_harmful_bid_during_progressive_gross_brake():
    cfg = MMConfigV2(session_budget_usd=30.0, base_clip_usd=4.0)
    snapshot = _snapshot()
    inventory = _inventory(
        free_usdc=20.0,
        dn_shares=14.0,
        excess_dn_qty=14.0,
        excess_dn_value_usd=9.2,
        excess_value_usd=9.2,
        signed_excess_value_usd=-9.2,
        total_inventory_value_usd=18.0,
        target_pair_value_usd=15.0,
        pair_value_over_target_usd=2.0,
        pair_value_ratio=0.60,
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
        ctx=QuoteContext(tick_size=0.01, min_order_size=1.0),
    )
    assert risk.soft_mode == "inventory_skewed"
    assert plan.up_bid is not None and plan.up_bid.inventory_effect == "helpful"
    assert plan.dn_bid is not None
    assert plan.dn_bid.inventory_effect == "harmful"
    assert plan.suppressed_reasons.get("dn_bid") in {None, "dual_bid_guard_applied"}
    assert plan.dual_bid_guard_inventory_budget_hits == 0


def test_dual_bid_guard_preserves_harmful_bid_during_pre_emergency_drawdown(monkeypatch):
    cfg = MMConfigV2(session_budget_usd=30.0, base_clip_usd=4.0)
    snapshot = _snapshot(
        time_left_sec=600.0,
        pm_mid_up=0.78,
        pm_mid_dn=0.22,
        up_best_bid=0.67,
        up_best_ask=0.69,
        dn_best_bid=0.31,
        dn_best_ask=0.33,
        market_tradeable=True,
        market_quality_score=0.95,
    )
    inventory = _inventory(
        free_usdc=18.0,
        up_shares=3.88,
        dn_shares=9.56,
        sellable_up_shares=3.88,
        sellable_dn_shares=9.56,
        excess_dn_qty=1.7929905037459941 / 0.31566734220880166,
        excess_dn_value_usd=1.7929905037459941,
        excess_value_usd=1.7929905037459941,
        signed_excess_value_usd=-1.7929905037459941,
        total_inventory_value_usd=5.0,
        target_pair_value_usd=15.0,
    )
    risk = HardSafetyKernel(cfg).evaluate(
        snapshot=snapshot,
        inventory=inventory,
        analytics=AnalyticsState(session_pnl_equity_usd=-6.66, session_pnl=-6.66),
        health=HealthState(),
    )
    assert 0.50 <= risk.early_drawdown_pressure < 0.75
    assert risk.soft_mode == "normal"

    policy = QuotePolicyV2(cfg)
    original_make_intent = policy._make_intent
    dn_bid_calls = {"count": 0}

    def _patched_make_intent(*args, **kwargs):
        token = kwargs.get("token")
        side = kwargs.get("side")
        if token == snapshot.dn_token_id and side == "BUY":
            dn_bid_calls["count"] += 1
            if dn_bid_calls["count"] == 1:
                return None, "below_min_order_size"
        return original_make_intent(*args, **kwargs)

    monkeypatch.setattr(policy, "_make_intent", _patched_make_intent)
    plan = policy.generate(
        snapshot=snapshot,
        inventory=inventory,
        risk=risk,
        ctx=QuoteContext(tick_size=0.01, min_order_size=5.0),
    )
    assert dn_bid_calls["count"] >= 2
    assert plan.up_bid is not None
    assert plan.dn_bid is not None
    assert plan.dn_bid.inventory_effect == "harmful"
    assert plan.suppressed_reasons.get("dn_bid") == "dual_bid_guard_applied"


def test_dual_bid_guard_allows_harmful_bid_when_only_micro_over_target():
    cfg = MMConfigV2(session_budget_usd=30.0, base_clip_usd=4.0)
    snapshot = _snapshot()
    inventory = _inventory(
        free_usdc=20.0,
        dn_shares=10.2,
        excess_dn_qty=10.2,
        excess_dn_value_usd=6.4,
        excess_value_usd=6.4,
        signed_excess_value_usd=-6.4,
        total_inventory_value_usd=15.8,
        target_pair_value_usd=15.0,
        pair_value_over_target_usd=0.8,
        pair_value_ratio=15.8 / 30.0,
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
        ctx=QuoteContext(tick_size=0.01, min_order_size=1.0),
    )
    assert risk.soft_mode == "inventory_skewed"
    assert plan.up_bid is not None and plan.up_bid.inventory_effect == "helpful"
    assert plan.dn_bid is not None
    assert plan.dn_bid.inventory_effect == "harmful"
    assert plan.suppressed_reasons.get("dn_bid") is None
    assert plan.dual_bid_guard_inventory_budget_hits == 0


def test_dual_bid_guard_does_not_rearm_harmful_bid_after_gross_buy_block_threshold():
    cfg = MMConfigV2(session_budget_usd=30.0, base_clip_usd=4.0)
    snapshot = _snapshot()
    inventory = _inventory(
        free_usdc=20.0,
        dn_shares=14.0,
        excess_dn_qty=14.0,
        excess_dn_value_usd=9.2,
        excess_value_usd=9.2,
        signed_excess_value_usd=-9.2,
        total_inventory_value_usd=18.5,
        target_pair_value_usd=15.0,
        pair_value_over_target_usd=3.5,
        pair_value_ratio=18.5 / 30.0,
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
        ctx=QuoteContext(tick_size=0.01, min_order_size=1.0),
    )
    assert risk.soft_mode == "inventory_skewed"
    assert plan.up_bid is None
    assert plan.dn_bid is None
    assert plan.suppressed_reasons["dn_bid"] in {
        "pair_over_target_buy_block",
        "harmful_buy_blocked_high_skew",
    }
    assert plan.dual_bid_guard_inventory_budget_hits == 0


def test_emergency_taker_forced_disables_post_only_for_emergency_sells():
    cfg = MMConfigV2(session_budget_usd=30.0, base_clip_usd=4.0)
    snapshot = _snapshot(time_left_sec=600.0)
    inventory = _inventory(
        up_shares=5.0,
        dn_shares=5.0,
        excess_up_qty=5.0,
        excess_dn_qty=5.0,
        sellable_up_shares=5.0,
        sellable_dn_shares=5.0,
        excess_up_value_usd=3.0,
        excess_dn_value_usd=3.0,
        excess_value_usd=6.0,
        signed_excess_value_usd=3.0,
        total_inventory_value_usd=5.0,
        free_usdc=30.0,
    )
    risk = HardSafetyKernel(cfg).evaluate(
        snapshot=snapshot,
        inventory=inventory,
        analytics=AnalyticsState(session_pnl_equity_usd=-10.0, session_pnl=-10.0),
        health=HealthState(drawdown_breach_active=True, drawdown_breach_ticks=3, drawdown_breach_age_sec=9.0),
    )
    assert risk.hard_mode == "emergency_unwind"
    maker_plan = QuotePolicyV2(cfg).generate(
        snapshot=snapshot,
        inventory=inventory,
        risk=risk,
        ctx=QuoteContext(tick_size=0.01, min_order_size=1.0),
    )
    forced_plan = QuotePolicyV2(cfg).generate(
        snapshot=snapshot,
        inventory=inventory,
        risk=replace(risk, emergency_taker_forced=True),
        ctx=QuoteContext(tick_size=0.01, min_order_size=1.0),
    )
    assert maker_plan.up_ask is not None and maker_plan.dn_ask is not None
    assert forced_plan.up_ask is not None and forced_plan.dn_ask is not None
    assert maker_plan.up_ask.post_only is True
    assert maker_plan.dn_ask.post_only is True
    assert forced_plan.up_ask.post_only is False
    assert forced_plan.dn_ask.post_only is False
