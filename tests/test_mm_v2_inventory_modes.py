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
        inventory=_inventory(excess_dn_value_usd=4.2, excess_value_usd=4.2, signed_excess_value_usd=-4.2),
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
        analytics=AnalyticsState(session_pnl=-4.5),
        health=HealthState(),
    )
    assert risk.hard_mode == "emergency_unwind"
    assert risk.soft_mode in {"defensive", "inventory_skewed"}
