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
from mm_v2.state_api import serialize_engine_state
from mm_v2.types import AnalyticsState, EngineState, ExecutionState, HealthState, PairInventoryState, PairMarketSnapshot, QuotePlan


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
        target_pair_value_usd=21.0,
        pair_value_ratio=0.8,
        pair_value_over_target_usd=3.0,
        inventory_pressure_abs=0.0,
        inventory_pressure_signed=0.0,
    )
    payload.update(overrides)
    return PairInventoryState(**payload)


def test_target_pair_ratio_overflow_suppresses_inventory_expanding_intents():
    cfg = MMConfigV2(session_budget_usd=30.0, target_pair_value_ratio=0.70)
    snapshot = _snapshot()
    inventory = _inventory(
        up_shares=12.0,
        dn_shares=12.0,
        paired_qty=12.0,
        paired_value_usd=12.0,
        total_inventory_value_usd=24.0,
        pair_value_ratio=0.80,
        pair_value_over_target_usd=3.0,
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
    assert plan.up_bid is None
    assert plan.dn_bid is None
    assert plan.suppressed_reasons.get("up_bid") == "target_pair_ratio_cap"
    assert plan.suppressed_reasons.get("dn_bid") == "target_pair_ratio_cap"


def test_target_pair_ratio_metrics_exposed_in_state():
    cfg = MMConfigV2(session_budget_usd=30.0, target_pair_value_ratio=0.70)
    snapshot = _snapshot()
    inventory = _inventory()
    risk = HardSafetyKernel(cfg).evaluate(
        snapshot=snapshot,
        inventory=inventory,
        analytics=AnalyticsState(),
        health=HealthState(),
    )
    analytics = AnalyticsState(
        target_pair_value_usd=inventory.target_pair_value_usd,
        pair_value_ratio=inventory.pair_value_ratio,
        pair_value_over_target_usd=inventory.pair_value_over_target_usd,
        target_ratio_pressure=risk.target_ratio_pressure,
        harmful_suppressed_count_60s=2,
        target_ratio_breaches_60s=5,
        defensive_to_unwind_count_window=1,
        quote_cancel_to_fill_ratio_60s=1.5,
        mm_effective_ratio_60s=0.72,
    )
    state = EngineState(
        lifecycle="quoting",
        market=snapshot,
        inventory=inventory,
        risk=risk,
        current_quotes=QuotePlan(None, None, None, None, "normal", "test"),
        execution=ExecutionState(),
        analytics=analytics,
        health=HealthState(),
    )
    payload = serialize_engine_state(state, config=cfg)
    assert payload["inventory"]["target_pair_value_usd"] == 21.0
    assert payload["analytics"]["target_ratio_breaches_60s"] == 5
    assert payload["analytics"]["mm_effective_ratio_60s"] == 0.72


def test_target_ratio_does_not_trigger_hard_mode_by_itself():
    cfg = MMConfigV2(session_budget_usd=30.0, target_pair_value_ratio=0.70)
    risk = HardSafetyKernel(cfg).evaluate(
        snapshot=_snapshot(),
        inventory=_inventory(
            total_inventory_value_usd=24.0,
            pair_value_ratio=0.80,
            pair_value_over_target_usd=3.0,
            excess_value_usd=0.0,
            signed_excess_value_usd=0.0,
        ),
        analytics=AnalyticsState(),
        health=HealthState(),
    )
    assert risk.hard_mode == "none"
    assert risk.soft_mode == "inventory_skewed"
    assert risk.target_ratio_pressure > 0.0
