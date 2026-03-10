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

from mm_v2.config import MMConfigV2, NO_HELPFUL_TICKS_FOR_UNWIND, UNWIND_STUCK_WINDOW_SEC
from mm_v2.quote_policy import QuoteContext, QuotePolicyV2
from mm_v2.risk_kernel import HardSafetyKernel
from mm_v2.state_machine import StateMachineV2
from mm_v2.types import AnalyticsState, HealthState, PairInventoryState, PairMarketSnapshot, QuoteViabilitySummary


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
        free_usdc=50.0,
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


def _evaluate(
    cfg: MMConfigV2,
    snapshot: PairMarketSnapshot,
    inventory: PairInventoryState,
    *,
    min_order_size: float = 1.0,
):
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
        ctx=QuoteContext(tick_size=0.01, min_order_size=min_order_size),
    )
    return risk, plan


def test_one_sided_dn_fill_below_hard_cap_keeps_four_quotes():
    cfg = MMConfigV2(session_budget_usd=50.0, base_clip_usd=6.0)
    snapshot = _snapshot()
    inventory = _inventory(
        dn_shares=12.0,
        excess_dn_qty=12.0,
        excess_dn_value_usd=12.0,
        excess_value_usd=12.0,
        signed_excess_value_usd=-12.0,
        total_inventory_value_usd=12.0,
    )
    risk, plan = _evaluate(cfg, snapshot, inventory)
    assert risk.soft_mode == "inventory_skewed"
    assert risk.hard_mode == "none"
    assert all([plan.up_bid, plan.up_ask, plan.dn_bid, plan.dn_ask])
    assert plan.quote_balance_state == "balanced"
    assert plan.dn_bid.inventory_effect == "harmful"
    assert plan.dn_ask.inventory_effect == "helpful"


def test_repeated_dn_accumulation_enters_defensive_but_keeps_quotes():
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
    risk, plan = _evaluate(cfg, snapshot, inventory)
    assert risk.soft_mode == "defensive"
    assert plan.up_bid is not None
    assert plan.dn_ask is not None
    assert plan.dn_bid is None
    assert plan.up_ask is None
    assert plan.suppressed_reasons.get("dn_bid") == "harmful_suppressed_in_defensive"
    assert plan.suppressed_reasons.get("up_ask") == "harmful_suppressed_in_defensive"
    assert plan.up_bid.inventory_effect == "helpful"
    assert plan.dn_ask.inventory_effect == "helpful"
    assert plan.quote_balance_state != "none"


def test_excess_beyond_hard_cap_moves_into_unwind_without_halt():
    cfg = MMConfigV2(session_budget_usd=15.0, base_clip_usd=6.0)
    sm = StateMachineV2(cfg)
    snapshot = _snapshot()
    inventory = _inventory(
        dn_shares=11.0,
        excess_dn_qty=11.0,
        excess_dn_value_usd=7.2,
        excess_value_usd=7.2,
        signed_excess_value_usd=-7.2,
        total_inventory_value_usd=7.2,
    )
    risk, plan = _evaluate(cfg, snapshot, inventory)
    transition = sm.transition(snapshot=snapshot, inventory=inventory, risk=risk, viability=QuoteViabilitySummary(any_quote=True, four_quotes=False, helpful_count=1))
    assert transition.lifecycle == "quoting"
    transition = sm.transition(snapshot=snapshot, inventory=inventory, risk=risk, viability=QuoteViabilitySummary(any_quote=True, four_quotes=False, helpful_count=1))
    assert transition.lifecycle == "inventory_skewed"
    transition = sm.transition(snapshot=snapshot, inventory=inventory, risk=risk, viability=QuoteViabilitySummary(any_quote=True, four_quotes=False, helpful_count=1))
    assert transition.lifecycle == "defensive"
    transition = sm.transition(snapshot=snapshot, inventory=inventory, risk=risk, viability=QuoteViabilitySummary(any_quote=True, four_quotes=False, helpful_count=0, harmful_count=2, harmful_only=True, four_quote_presence_ratio=0.10))
    assert risk.hard_mode == "none"
    assert risk.target_soft_mode == "unwind"
    assert transition.lifecycle == "defensive"
    sm._excess_baseline_ts = time.time() - (float(UNWIND_STUCK_WINDOW_SEC) + 1.0)
    sm._excess_baseline_value_usd = float(inventory.excess_value_usd)
    for _ in range(int(NO_HELPFUL_TICKS_FOR_UNWIND)):
        transition = sm.transition(
            snapshot=snapshot,
            inventory=inventory,
            risk=risk,
            viability=QuoteViabilitySummary(
                any_quote=True,
                four_quotes=False,
                helpful_count=0,
                harmful_count=2,
                harmful_only=True,
                quote_balance_state="reduced",
                four_quote_presence_ratio=0.10,
            ),
        )
    assert transition.lifecycle == "unwind"
    assert plan.dn_bid is None
    assert plan.up_bid is not None


def test_no_progress_in_unwind_keeps_engine_in_unwind():
    cfg = MMConfigV2(session_budget_usd=15.0, base_clip_usd=6.0)
    sm = StateMachineV2(cfg)
    snapshot = _snapshot()
    inventory = _inventory(
        dn_shares=11.0,
        excess_dn_qty=11.0,
        excess_dn_value_usd=5.8,
        excess_value_usd=5.8,
        signed_excess_value_usd=-5.8,
        total_inventory_value_usd=5.8,
    )
    risk, _ = _evaluate(cfg, snapshot, inventory)
    sm.transition(snapshot=snapshot, inventory=inventory, risk=risk, viability=QuoteViabilitySummary(any_quote=True, four_quotes=False, helpful_count=1))
    sm.transition(snapshot=snapshot, inventory=inventory, risk=risk, viability=QuoteViabilitySummary(any_quote=True, four_quotes=False, helpful_count=1))
    sm._excess_baseline_ts = time.time() - 31.0
    sm._excess_baseline_value_usd = 5.8
    for _ in range(3):
        result = sm.transition(
            snapshot=snapshot,
            inventory=inventory,
            risk=risk,
            viability=QuoteViabilitySummary(
                any_quote=True,
                four_quotes=False,
                helpful_count=0,
                harmful_count=2,
                harmful_only=True,
                four_quote_presence_ratio=0.10,
            ),
        )
    assert result.lifecycle == "unwind"
    assert sm.lifecycle == "unwind"


def test_below_hard_cap_with_helpful_quotes_does_not_enter_unwind():
    cfg = MMConfigV2(session_budget_usd=50.0, base_clip_usd=6.0)
    sm = StateMachineV2(cfg)
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
    )
    risk, plan = _evaluate(cfg, snapshot, inventory)
    viability = QuoteViabilitySummary(
        any_quote=True,
        four_quotes=all([plan.up_bid, plan.up_ask, plan.dn_bid, plan.dn_ask]),
        helpful_count=sum(1 for q in (plan.up_bid, plan.up_ask, plan.dn_bid, plan.dn_ask) if q and q.inventory_effect == "helpful"),
        harmful_count=sum(1 for q in (plan.up_bid, plan.up_ask, plan.dn_bid, plan.dn_ask) if q and q.inventory_effect == "harmful"),
        helpful_only=False,
        harmful_only=False,
        four_quote_presence_ratio=1.0,
    )
    sm.transition(snapshot=snapshot, inventory=inventory, risk=risk, viability=viability)
    sm.transition(snapshot=snapshot, inventory=inventory, risk=risk, viability=viability)
    sm.transition(snapshot=snapshot, inventory=inventory, risk=risk, viability=viability)
    sm._excess_baseline_ts = time.time() - 31.0
    sm._excess_baseline_value_usd = inventory.excess_value_usd
    result = sm.transition(snapshot=snapshot, inventory=inventory, risk=risk, viability=viability)
    assert viability.helpful_count > 0
    assert plan.quote_balance_state != "none"
    assert result.lifecycle != "unwind"


def test_live_like_low_free_usdc_does_not_collapse_to_none_below_hard_cap():
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
        paired_value_usd=5.84,
        excess_dn_value_usd=0.19,
        excess_value_usd=0.19,
        signed_excess_value_usd=-0.19,
        total_inventory_value_usd=6.03,
        free_usdc=9.94,
    )
    risk, plan = _evaluate(cfg, snapshot, inventory, min_order_size=5.0)
    assert risk.hard_mode == "none"
    assert plan.quote_balance_state != "none"
    assert plan.quote_viability_reason != "all_quotes_below_min_size"


def test_unwind_while_target_lower_ratio_is_bounded_outside_expiry():
    cfg = MMConfigV2(session_budget_usd=15.0, base_clip_usd=6.0)
    sm = StateMachineV2(cfg)
    snapshot = _snapshot(time_left_sec=600.0)
    high_inventory = _inventory(
        dn_shares=11.0,
        excess_dn_qty=11.0,
        excess_dn_value_usd=7.2,
        excess_value_usd=7.2,
        signed_excess_value_usd=-7.2,
        total_inventory_value_usd=7.2,
    )
    low_inventory = _inventory(
        dn_shares=6.0,
        excess_dn_qty=6.0,
        excess_dn_value_usd=5.6,
        excess_value_usd=5.6,
        signed_excess_value_usd=-5.6,
        total_inventory_value_usd=5.6,
    )
    high_risk, _ = _evaluate(cfg, snapshot, high_inventory)
    low_risk, _ = _evaluate(cfg, snapshot, low_inventory)
    assert high_risk.target_soft_mode == "unwind"
    assert low_risk.target_soft_mode == "defensive"
    viability = QuoteViabilitySummary(any_quote=True, four_quotes=False, helpful_count=1, four_quote_presence_ratio=0.30)

    samples: list[tuple[str, str, float]] = []
    for _ in range(8):
        result = sm.transition(snapshot=snapshot, inventory=high_inventory, risk=high_risk, viability=viability)
        samples.append((result.effective_soft_mode, result.target_soft_mode, snapshot.time_left_sec))
    sm._unwind_started_at = time.time() - 10.0
    for _ in range(20):
        result = sm.transition(snapshot=snapshot, inventory=low_inventory, risk=low_risk, viability=viability)
        samples.append((result.effective_soft_mode, result.target_soft_mode, snapshot.time_left_sec))

    eligible = [entry for entry in samples if entry[2] > cfg.unwind_window_sec]
    mismatch = [entry for entry in eligible if entry[0] == "unwind" and entry[1] != "unwind"]
    ratio = (len(mismatch) / len(eligible)) if eligible else 0.0
    assert ratio < 0.25


def test_non_expiry_quote_balance_none_not_more_than_three_consecutive():
    cfg = MMConfigV2(session_budget_usd=50.0, base_clip_usd=6.0)
    snapshot = _snapshot(
        time_left_sec=600.0,
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
        free_usdc=12.0,
    )
    risk, _ = _evaluate(cfg, snapshot, inventory, min_order_size=5.0)
    if risk.soft_mode != "unwind":
        risk.soft_mode = "unwind"
        risk.target_soft_mode = "defensive"
    policy = QuotePolicyV2(cfg)
    max_none_streak = 0
    current_none_streak = 0
    for _ in range(12):
        plan = policy.generate(
            snapshot=snapshot,
            inventory=inventory,
            risk=risk,
            ctx=QuoteContext(tick_size=0.01, min_order_size=5.0),
        )
        if snapshot.time_left_sec > cfg.unwind_window_sec and plan.quote_balance_state == "none":
            current_none_streak += 1
            max_none_streak = max(max_none_streak, current_none_streak)
        else:
            current_none_streak = 0
    assert max_none_streak <= 3


def test_skewed_protective_modes_suppress_harmful_intents():
    cfg = MMConfigV2(session_budget_usd=15.0, base_clip_usd=6.0)
    snapshot = _snapshot(time_left_sec=600.0)
    inventory = _inventory(
        dn_shares=6.0,
        excess_dn_qty=6.0,
        excess_dn_value_usd=5.6,
        excess_value_usd=5.6,
        signed_excess_value_usd=-5.6,
        total_inventory_value_usd=5.6,
    )
    risk, _ = _evaluate(cfg, snapshot, inventory, min_order_size=5.0)
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
