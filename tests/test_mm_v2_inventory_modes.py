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

from mm_v2.config import EXIT_CONFIRM_TICKS, MMConfigV2, UNWIND_EXIT_CONFIRM_TICKS
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


def test_soft_mode_normal_below_soft_excess():
    cfg = MMConfigV2(session_budget_usd=15.0)
    risk = HardSafetyKernel(cfg).evaluate(
        snapshot=_snapshot(),
        inventory=_inventory(excess_up_value_usd=1.0, excess_value_usd=1.0, signed_excess_value_usd=1.0),
        analytics=AnalyticsState(),
        health=HealthState(),
    )
    assert risk.soft_mode == "normal"
    assert risk.hard_mode == "none"


def test_micro_excess_is_treated_as_flat_inventory_side():
    cfg = MMConfigV2(session_budget_usd=15.0)
    risk = HardSafetyKernel(cfg).evaluate(
        snapshot=_snapshot(),
        inventory=_inventory(
            excess_up_value_usd=0.02,
            excess_value_usd=0.02,
            signed_excess_value_usd=0.02,
        ),
        analytics=AnalyticsState(),
        health=HealthState(),
    )
    assert risk.inventory_side == "flat"
    assert risk.soft_mode == "normal"
    assert risk.hard_mode == "none"


def test_soft_mode_inventory_skewed_between_soft_and_defensive_thresholds():
    cfg = MMConfigV2(session_budget_usd=15.0)
    risk = HardSafetyKernel(cfg).evaluate(
        snapshot=_snapshot(),
        inventory=_inventory(excess_up_value_usd=2.0, excess_value_usd=2.0, signed_excess_value_usd=2.0),
        analytics=AnalyticsState(),
        health=HealthState(),
    )
    assert risk.soft_mode == "inventory_skewed"
    assert risk.inventory_side == "up"
    assert risk.hard_mode == "none"


def test_flat_bootstrap_ignores_mild_quality_noise():
    cfg = MMConfigV2(session_budget_usd=15.0)
    risk = HardSafetyKernel(cfg).evaluate(
        snapshot=_snapshot(
            market_tradeable=True,
            market_quality_score=0.30,  # below 0.35 but above bootstrap floor
            divergence_up=0.11,
            divergence_dn=0.11,
        ),
        inventory=_inventory(
            excess_value_usd=0.05,
            signed_excess_value_usd=0.05,
        ),
        analytics=AnalyticsState(),
        health=HealthState(),
    )
    assert risk.inventory_side == "flat"
    assert risk.soft_mode == "normal"
    assert risk.hard_mode == "none"


def test_flat_bootstrap_enters_defensive_on_severe_quality():
    cfg = MMConfigV2(session_budget_usd=15.0)
    risk = HardSafetyKernel(cfg).evaluate(
        snapshot=_snapshot(
            market_tradeable=True,
            market_quality_score=0.12,
            divergence_up=0.22,
            divergence_dn=0.22,
        ),
        inventory=_inventory(
            excess_value_usd=0.02,
            signed_excess_value_usd=0.02,
        ),
        analytics=AnalyticsState(),
        health=HealthState(),
    )
    assert risk.inventory_side == "flat"
    assert risk.soft_mode == "defensive"
    assert risk.hard_mode == "none"


def test_soft_mode_defensive_between_defensive_and_hard_thresholds():
    cfg = MMConfigV2(session_budget_usd=15.0)
    risk = HardSafetyKernel(cfg).evaluate(
        snapshot=_snapshot(),
        inventory=_inventory(excess_dn_value_usd=3.0, excess_value_usd=3.0, signed_excess_value_usd=-3.0),
        analytics=AnalyticsState(),
        health=HealthState(),
    )
    assert risk.soft_mode == "defensive"
    assert risk.inventory_side == "dn"
    assert risk.hard_mode == "none"


def test_soft_mode_unwind_above_hard_threshold():
    cfg = MMConfigV2(session_budget_usd=15.0)
    risk = HardSafetyKernel(cfg).evaluate(
        snapshot=_snapshot(),
        inventory=_inventory(excess_dn_value_usd=5.6, excess_value_usd=5.6, signed_excess_value_usd=-5.6),
        analytics=AnalyticsState(),
        health=HealthState(),
    )
    assert risk.soft_mode == "unwind"
    assert risk.hard_mode == "none"


def test_poor_market_quality_only_enters_defensive_not_hard_mode():
    cfg = MMConfigV2(session_budget_usd=15.0)
    risk = HardSafetyKernel(cfg).evaluate(
        snapshot=_snapshot(market_tradeable=False, market_quality_score=0.1, divergence_up=0.2, divergence_dn=0.2),
        inventory=_inventory(),
        analytics=AnalyticsState(),
        health=HealthState(),
    )
    assert risk.soft_mode == "defensive"
    assert risk.hard_mode == "none"
    assert risk.quality_pressure > 0.0


def test_hard_safety_true_drift_bypasses_soft_modes():
    cfg = MMConfigV2(session_budget_usd=15.0)
    risk = HardSafetyKernel(cfg).evaluate(
        snapshot=_snapshot(),
        inventory=_inventory(excess_up_value_usd=3.0, excess_value_usd=3.0, signed_excess_value_usd=3.0),
        analytics=AnalyticsState(),
        health=HealthState(true_drift=True),
    )
    assert risk.hard_mode == "halted"
    assert risk.reason == "true inventory drift"


def test_hard_drawdown_enters_emergency_unwind_not_soft_defensive():
    cfg = MMConfigV2(session_budget_usd=15.0, hard_drawdown_usd=4.0)
    risk = HardSafetyKernel(cfg).evaluate(
        snapshot=_snapshot(),
        inventory=_inventory(up_shares=6.0, excess_up_value_usd=3.0, excess_value_usd=3.0, signed_excess_value_usd=3.0),
        analytics=AnalyticsState(session_pnl=-4.5, session_pnl_equity_usd=-4.5),
        health=HealthState(drawdown_breach_active=True, drawdown_breach_ticks=3, drawdown_breach_age_sec=9.0),
    )
    assert risk.hard_mode == "emergency_unwind"
    assert risk.soft_mode in {"defensive", "inventory_skewed"}


def test_below_hard_cap_no_progress_does_not_force_unwind_while_helpful_quotes_exist():
    cfg = MMConfigV2(session_budget_usd=15.0)
    sm = StateMachineV2(cfg)
    inventory = _inventory(
        dn_shares=6.0,
        excess_dn_qty=6.0,
        excess_dn_value_usd=3.0,
        excess_value_usd=3.0,
        signed_excess_value_usd=-3.0,
        total_inventory_value_usd=3.0,
    )
    risk = HardSafetyKernel(cfg).evaluate(
        snapshot=_snapshot(),
        inventory=inventory,
        analytics=AnalyticsState(),
        health=HealthState(),
    )
    viability = QuoteViabilitySummary(
        any_quote=True,
        four_quotes=True,
        helpful_count=2,
        harmful_count=2,
        helpful_only=False,
        harmful_only=False,
        four_quote_presence_ratio=0.80,
    )
    sm.transition(snapshot=_snapshot(), inventory=inventory, risk=risk, viability=viability)
    sm.transition(snapshot=_snapshot(), inventory=inventory, risk=risk, viability=viability)
    sm.transition(snapshot=_snapshot(), inventory=inventory, risk=risk, viability=viability)
    sm._excess_baseline_ts = time.time() - 31.0
    sm._excess_baseline_value_usd = 3.0
    result = sm.transition(
        snapshot=_snapshot(),
        inventory=inventory,
        risk=risk,
        viability=viability,
    )
    assert result.lifecycle == "defensive"
    assert result.no_progress is True


def test_defensive_enters_unwind_after_no_progress_and_missing_helpful_quotes():
    cfg = MMConfigV2(session_budget_usd=15.0)
    sm = StateMachineV2(cfg)
    inventory = _inventory(
        dn_shares=6.0,
        excess_dn_qty=6.0,
        excess_dn_value_usd=3.0,
        excess_value_usd=3.0,
        signed_excess_value_usd=-3.0,
        total_inventory_value_usd=3.0,
    )
    risk = HardSafetyKernel(cfg).evaluate(
        snapshot=_snapshot(),
        inventory=inventory,
        analytics=AnalyticsState(),
        health=HealthState(),
    )
    bad_viability = QuoteViabilitySummary(
        any_quote=True,
        four_quotes=False,
        helpful_count=0,
        harmful_count=2,
        helpful_only=False,
        harmful_only=True,
        four_quote_presence_ratio=0.10,
    )
    sm.transition(snapshot=_snapshot(), inventory=inventory, risk=risk, viability=bad_viability)
    sm.transition(snapshot=_snapshot(), inventory=inventory, risk=risk, viability=bad_viability)
    sm.transition(snapshot=_snapshot(), inventory=inventory, risk=risk, viability=bad_viability)
    sm._excess_baseline_ts = time.time() - 31.0
    sm._excess_baseline_value_usd = 3.0
    for _ in range(3):
        result = sm.transition(
            snapshot=_snapshot(),
            inventory=inventory,
            risk=risk,
            viability=bad_viability,
        )
    assert result.lifecycle == "unwind"
    assert result.effective_soft_mode == "unwind"


def test_target_soft_mode_can_be_defensive_while_effective_soft_mode_stays_inventory_skewed_during_hysteresis():
    cfg = MMConfigV2(session_budget_usd=15.0)
    sm = StateMachineV2(cfg)
    inv_skewed = _inventory(excess_dn_value_usd=2.0, excess_value_usd=2.0, signed_excess_value_usd=-2.0)
    risk_skewed = HardSafetyKernel(cfg).evaluate(
        snapshot=_snapshot(),
        inventory=inv_skewed,
        analytics=AnalyticsState(),
        health=HealthState(),
    )
    sm.transition(snapshot=_snapshot(), inventory=inv_skewed, risk=risk_skewed)
    result = sm.transition(snapshot=_snapshot(), inventory=inv_skewed, risk=risk_skewed)
    assert result.lifecycle == "inventory_skewed"

    inv_def = _inventory(excess_dn_value_usd=3.0, excess_value_usd=3.0, signed_excess_value_usd=-3.0)
    risk_def = HardSafetyKernel(cfg).evaluate(
        snapshot=_snapshot(),
        inventory=inv_def,
        analytics=AnalyticsState(),
        health=HealthState(),
    )
    result = sm.transition(snapshot=_snapshot(), inventory=inv_def, risk=risk_def)
    assert result.target_soft_mode == "defensive"
    assert result.effective_soft_mode == "inventory_skewed"


def test_effective_soft_mode_matches_lifecycle_mapping():
    cfg = MMConfigV2(session_budget_usd=15.0)
    sm = StateMachineV2(cfg)
    inventory = _inventory(excess_dn_value_usd=2.0, excess_value_usd=2.0, signed_excess_value_usd=-2.0)
    risk = HardSafetyKernel(cfg).evaluate(
        snapshot=_snapshot(),
        inventory=inventory,
        analytics=AnalyticsState(),
        health=HealthState(),
    )
    sm.transition(snapshot=_snapshot(), inventory=inventory, risk=risk)
    result = sm.transition(snapshot=_snapshot(), inventory=inventory, risk=risk)
    assert result.lifecycle == "inventory_skewed"
    assert result.effective_soft_mode == "inventory_skewed"


def test_inventory_skewed_deescalates_to_quoting_after_confirmed_normal_target():
    cfg = MMConfigV2(session_budget_usd=50.0)
    sm = StateMachineV2(cfg)
    sm._set_lifecycle("inventory_skewed")
    inventory = _inventory(
        up_shares=4.0,
        excess_up_qty=4.0,
        excess_up_value_usd=1.8,
        excess_value_usd=1.8,
        signed_excess_value_usd=1.8,
        total_inventory_value_usd=1.8,
    )
    snap = _snapshot(
        market_tradeable=True,
        market_quality_score=0.95,
        divergence_up=0.03,
        divergence_dn=0.03,
        time_left_sec=700.0,
    )
    risk = HardSafetyKernel(cfg).evaluate(
        snapshot=snap,
        inventory=inventory,
        analytics=AnalyticsState(),
        health=HealthState(),
    )
    # Drive target below inventory_skewed to arm de-escalation.
    risk.target_soft_mode = "normal"
    risk.soft_mode = "normal"
    risk.reason = "normal quoting"
    viability = QuoteViabilitySummary(
        any_quote=True,
        four_quotes=True,
        helpful_count=2,
        harmful_count=2,
        helpful_only=False,
        harmful_only=False,
        four_quote_presence_ratio=0.9,
        quote_balance_state="balanced",
    )
    result = None
    for _ in range(EXIT_CONFIRM_TICKS):
        result = sm.transition(snapshot=snap, inventory=inventory, risk=risk, viability=viability)
    assert result is not None
    assert result.lifecycle == "quoting"
    assert result.effective_soft_mode == "normal"


def test_unwind_deescalates_when_target_mode_drops_even_if_quality_is_poor():
    cfg = MMConfigV2(session_budget_usd=15.0)
    sm = StateMachineV2(cfg)
    sm._set_lifecycle("unwind")
    sm._unwind_started_at = time.time() - 10.0
    inventory = _inventory(
        up_shares=4.88,
        dn_shares=1.35,
        paired_qty=1.35,
        excess_up_qty=3.53,
        excess_up_value_usd=0.90,
        excess_value_usd=0.90,
        signed_excess_value_usd=0.90,
        total_inventory_value_usd=2.25,
    )
    snap = _snapshot(
        market_tradeable=False,
        market_quality_score=0.20,
        divergence_up=0.14,
        divergence_dn=0.14,
    )
    risk = HardSafetyKernel(cfg).evaluate(
        snapshot=snap,
        inventory=inventory,
        analytics=AnalyticsState(),
        health=HealthState(),
    )
    assert risk.target_soft_mode == "defensive"
    assert risk.quality_pressure >= 1.0
    viability = QuoteViabilitySummary(
        any_quote=True,
        four_quotes=False,
        helpful_count=1,
        harmful_count=0,
        helpful_only=True,
        harmful_only=False,
        four_quote_presence_ratio=0.30,
    )
    result = None
    for _ in range(UNWIND_EXIT_CONFIRM_TICKS):
        result = sm.transition(snapshot=snap, inventory=inventory, risk=risk, viability=viability)
    assert result is not None
    assert result.lifecycle == "defensive"
    assert result.effective_soft_mode == "defensive"


def test_unwind_deescalates_to_defensive_after_confirmed_lower_target():
    cfg = MMConfigV2(session_budget_usd=50.0)
    sm = StateMachineV2(cfg)
    sm._set_lifecycle("unwind")
    sm._unwind_started_at = time.time() - 10.0
    inventory = _inventory(
        up_shares=5.0,
        excess_up_qty=5.0,
        excess_up_value_usd=2.4,
        excess_value_usd=2.4,
        signed_excess_value_usd=2.4,
        total_inventory_value_usd=2.4,
    )
    snap = _snapshot(
        market_tradeable=False,
        market_quality_score=0.20,
        divergence_up=0.14,
        divergence_dn=0.14,
        time_left_sec=600.0,
    )
    risk = HardSafetyKernel(cfg).evaluate(
        snapshot=snap,
        inventory=inventory,
        analytics=AnalyticsState(),
        health=HealthState(),
    )
    assert risk.target_soft_mode == "defensive"
    viability = QuoteViabilitySummary(any_quote=True, four_quotes=False, helpful_count=1, four_quote_presence_ratio=0.25)
    result = None
    for _ in range(UNWIND_EXIT_CONFIRM_TICKS):
        result = sm.transition(snapshot=snap, inventory=inventory, risk=risk, viability=viability)
    assert result is not None
    assert result.lifecycle == "defensive"
    assert result.effective_soft_mode == "defensive"


def test_unwind_does_not_deescalate_near_expiry_window():
    cfg = MMConfigV2(session_budget_usd=50.0)
    sm = StateMachineV2(cfg)
    sm._set_lifecycle("unwind")
    sm._unwind_started_at = time.time() - 10.0
    inventory = _inventory(
        up_shares=5.0,
        excess_up_qty=5.0,
        excess_up_value_usd=2.4,
        excess_value_usd=2.4,
        signed_excess_value_usd=2.4,
        total_inventory_value_usd=2.4,
    )
    snap = _snapshot(
        market_tradeable=False,
        market_quality_score=0.20,
        divergence_up=0.14,
        divergence_dn=0.14,
        time_left_sec=30.0,
    )
    risk = HardSafetyKernel(cfg).evaluate(
        snapshot=snap,
        inventory=inventory,
        analytics=AnalyticsState(),
        health=HealthState(),
    )
    assert risk.target_soft_mode == "unwind"
    risk.target_soft_mode = "defensive"
    risk.soft_mode = "defensive"
    viability = QuoteViabilitySummary(any_quote=True, four_quotes=False, helpful_count=1, four_quote_presence_ratio=0.25)
    result = None
    for _ in range(UNWIND_EXIT_CONFIRM_TICKS + 1):
        result = sm.transition(snapshot=snap, inventory=inventory, risk=risk, viability=viability)
    assert result is not None
    assert result.lifecycle == "unwind"
    assert result.effective_soft_mode == "unwind"


def test_unwind_does_not_deescalate_when_hard_mode_active():
    cfg = MMConfigV2(session_budget_usd=50.0)
    sm = StateMachineV2(cfg)
    sm._set_lifecycle("unwind")
    sm._unwind_started_at = time.time() - 10.0
    inventory = _inventory(
        up_shares=5.0,
        excess_up_qty=5.0,
        excess_up_value_usd=2.4,
        excess_value_usd=2.4,
        signed_excess_value_usd=2.4,
        total_inventory_value_usd=2.4,
    )
    snap = _snapshot(time_left_sec=600.0)
    risk = HardSafetyKernel(cfg).evaluate(
        snapshot=snap,
        inventory=inventory,
        analytics=AnalyticsState(),
        health=HealthState(true_drift=True, true_drift_no_progress_sec=5.0),
    )
    assert risk.hard_mode == "emergency_unwind"
    result = sm.transition(
        snapshot=snap,
        inventory=inventory,
        risk=risk,
        viability=QuoteViabilitySummary(any_quote=True, four_quotes=False, helpful_count=1, four_quote_presence_ratio=0.25),
    )
    assert result.lifecycle == "emergency_unwind"


def test_unwind_exit_does_not_require_excess_baseline_streak():
    cfg = MMConfigV2(session_budget_usd=50.0)
    sm = StateMachineV2(cfg)
    sm._set_lifecycle("unwind")
    sm._unwind_started_at = time.time() - 10.0
    # Deliberately stale/worse baseline: unwind exit should not depend on it.
    sm._excess_baseline_ts = time.time() - 60.0
    sm._excess_baseline_value_usd = 0.1
    inventory = _inventory(
        up_shares=5.0,
        excess_up_qty=5.0,
        excess_up_value_usd=2.4,
        excess_value_usd=2.4,
        signed_excess_value_usd=2.4,
        total_inventory_value_usd=2.4,
    )
    snap = _snapshot(
        market_tradeable=False,
        market_quality_score=0.20,
        divergence_up=0.14,
        divergence_dn=0.14,
        time_left_sec=600.0,
    )
    risk = HardSafetyKernel(cfg).evaluate(
        snapshot=snap,
        inventory=inventory,
        analytics=AnalyticsState(),
        health=HealthState(),
    )
    assert risk.target_soft_mode == "defensive"
    viability = QuoteViabilitySummary(any_quote=True, four_quotes=False, helpful_count=1, four_quote_presence_ratio=0.25)
    result = None
    for _ in range(UNWIND_EXIT_CONFIRM_TICKS):
        result = sm.transition(snapshot=snap, inventory=inventory, risk=risk, viability=viability)
    assert result is not None
    assert result.lifecycle == "defensive"


def test_defensive_deescalates_when_target_normal_and_quotes_viable_despite_stale_baseline():
    cfg = MMConfigV2(session_budget_usd=15.0)
    sm = StateMachineV2(cfg)
    sm._set_lifecycle("defensive")
    # Stale low baseline should not block defensive de-escalation anymore.
    sm._excess_baseline_ts = time.time() - 60.0
    sm._excess_baseline_value_usd = 0.1
    inventory = _inventory(
        up_shares=5.0,
        excess_up_qty=5.0,
        excess_up_value_usd=1.4,
        excess_value_usd=1.4,
        signed_excess_value_usd=1.4,
        total_inventory_value_usd=1.4,
    )
    snap = _snapshot(time_left_sec=600.0)
    risk = HardSafetyKernel(cfg).evaluate(
        snapshot=snap,
        inventory=inventory,
        analytics=AnalyticsState(),
        health=HealthState(),
    )
    assert risk.target_soft_mode == "normal"
    viability = QuoteViabilitySummary(
        any_quote=True,
        four_quotes=False,
        helpful_count=1,
        harmful_count=0,
        helpful_only=True,
        harmful_only=False,
        quote_balance_state="helpful_only",
        four_quote_presence_ratio=0.2,
    )
    result = None
    for _ in range(EXIT_CONFIRM_TICKS):
        result = sm.transition(snapshot=snap, inventory=inventory, risk=risk, viability=viability)
    assert result is not None
    assert result.lifecycle == "quoting"
    assert result.effective_soft_mode == "normal"


def test_flat_defensive_no_progress_does_not_escalate_to_unwind():
    cfg = MMConfigV2(session_budget_usd=15.0)
    sm = StateMachineV2(cfg)
    sm._set_lifecycle("defensive")
    inventory = _inventory(
        up_shares=4.88,
        dn_shares=1.35,
        paired_qty=1.35,
        excess_up_qty=3.53,
        excess_up_value_usd=0.90,
        excess_value_usd=0.90,
        signed_excess_value_usd=0.40,  # below inventory-side deadband => flat
        total_inventory_value_usd=2.25,
    )
    snap = _snapshot(
        market_tradeable=False,
        market_quality_score=0.20,
        divergence_up=0.14,
        divergence_dn=0.14,
    )
    risk = HardSafetyKernel(cfg).evaluate(
        snapshot=snap,
        inventory=inventory,
        analytics=AnalyticsState(),
        health=HealthState(),
    )
    assert risk.inventory_side == "flat"
    assert risk.target_soft_mode == "defensive"
    sm._excess_baseline_ts = time.time() - 31.0
    sm._excess_baseline_value_usd = float(inventory.excess_value_usd)
    low_viability = QuoteViabilitySummary(
        any_quote=False,
        four_quotes=False,
        helpful_count=0,
        harmful_count=0,
        helpful_only=False,
        harmful_only=False,
        four_quote_presence_ratio=0.0,
    )
    result = sm.transition(snapshot=snap, inventory=inventory, risk=risk, viability=low_viability)
    assert result.no_progress is True
    assert result.lifecycle == "defensive"


def test_defensive_no_progress_requires_degraded_quote_balance_for_unwind():
    cfg = MMConfigV2(session_budget_usd=15.0)
    sm = StateMachineV2(cfg)
    inventory = _inventory(
        dn_shares=6.0,
        excess_dn_qty=6.0,
        excess_dn_value_usd=3.0,
        excess_value_usd=3.0,
        signed_excess_value_usd=-3.0,
        total_inventory_value_usd=3.0,
    )
    snap = _snapshot()
    risk = HardSafetyKernel(cfg).evaluate(
        snapshot=snap,
        inventory=inventory,
        analytics=AnalyticsState(),
        health=HealthState(),
    )
    assert risk.target_soft_mode == "defensive"
    sm._set_lifecycle("defensive")
    sm._excess_baseline_ts = time.time() - 31.0
    sm._excess_baseline_value_usd = 3.0
    viability = QuoteViabilitySummary(
        any_quote=True,
        four_quotes=True,
        helpful_count=0,
        harmful_count=2,
        helpful_only=False,
        harmful_only=True,
        four_quote_presence_ratio=0.9,
        quote_balance_state="balanced",
    )
    for _ in range(3):
        result = sm.transition(snapshot=snap, inventory=inventory, risk=risk, viability=viability)
    assert result.no_progress is True
    assert result.lifecycle == "defensive"
