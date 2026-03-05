from __future__ import annotations

import os
import sys
import time
from types import SimpleNamespace


BASE = os.path.dirname(os.path.dirname(__file__))
SRC = os.path.join(BASE, "src")
if SRC not in sys.path:
    sys.path.insert(0, SRC)
if BASE not in sys.path:
    sys.path.insert(0, BASE)

from mm_v2.config import MMConfigV2
from mm_v2.quote_policy import QuoteContext, QuotePolicyV2
from mm_v2.reconcile import ReconcileV2
from mm_v2.risk_kernel import HardSafetyKernel
from mm_v2.runtime import MarketMakerV2
from mm_v2.state_machine import StateMachineV2
from mm_v2.types import AnalyticsState, HealthState, PairInventoryState, PairMarketSnapshot, QuoteViabilitySummary
from mm.types import MarketInfo


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


def _market() -> MarketInfo:
    now = time.time()
    return MarketInfo(
        coin="BTC",
        timeframe="15m",
        question="BTC 15m",
        condition_id="cond",
        up_token_id="up-token",
        dn_token_id="dn-token",
        strike=100000.0,
        tick_size=0.01,
        min_order_size=5.0,
        window_start=now,
        window_end=now + 900.0,
    )


def _risk_and_plan(cfg: MMConfigV2, snapshot: PairMarketSnapshot, inventory: PairInventoryState):
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
    viability = QuoteViabilitySummary(
        any_quote=any([plan.up_bid, plan.up_ask, plan.dn_bid, plan.dn_ask]),
        four_quotes=all([plan.up_bid, plan.up_ask, plan.dn_bid, plan.dn_ask]),
        helpful_count=sum(1 for q in (plan.up_bid, plan.up_ask, plan.dn_bid, plan.dn_ask) if q and q.inventory_effect == "helpful"),
        harmful_count=sum(1 for q in (plan.up_bid, plan.up_ask, plan.dn_bid, plan.dn_ask) if q and q.inventory_effect == "harmful"),
        helpful_only=False,
        harmful_only=False,
        four_quote_presence_ratio=1.0 if all([plan.up_bid, plan.up_ask, plan.dn_bid, plan.dn_ask]) else 0.25,
    )
    viability.helpful_only = viability.helpful_count > 0 and viability.harmful_count == 0
    viability.harmful_only = viability.harmful_count > 0 and viability.helpful_count == 0
    return risk, plan, viability


def test_dn_excess_near_endpoint_below_hard_cap_keeps_safe_plan():
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
        dn_shares=40.0,
        excess_dn_qty=40.0,
        excess_dn_value_usd=8.0,
        excess_value_usd=8.0,
        signed_excess_value_usd=-8.0,
        total_inventory_value_usd=8.0,
    )
    risk, plan, _ = _risk_and_plan(cfg, snapshot, inventory)
    assert risk.hard_mode == "none"
    assert risk.soft_mode in {"inventory_skewed", "defensive"}
    assert plan.quote_balance_state in {"balanced", "helpful_only", "reduced", "harmful_only_blocked"}
    assert not (plan.quote_balance_state == "harmful_only_blocked" and plan.up_bid is None and plan.dn_ask is None)


def test_helpful_quotes_can_be_restored_by_promotion():
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
        excess_dn_value_usd=6.0,
        excess_value_usd=6.0,
        signed_excess_value_usd=-6.0,
        total_inventory_value_usd=6.0,
    )
    _, plan, viability = _risk_and_plan(cfg, snapshot, inventory)
    assert viability.helpful_count >= 1
    assert not viability.harmful_only


def test_no_progress_for_30s_with_helpful_quotes_stays_defensive():
    cfg = MMConfigV2(session_budget_usd=15.0, base_clip_usd=6.0)
    snapshot = _snapshot()
    inventory = _inventory(
        dn_shares=6.0,
        excess_dn_qty=6.0,
        excess_dn_value_usd=3.0,
        excess_value_usd=3.0,
        signed_excess_value_usd=-3.0,
        total_inventory_value_usd=3.0,
    )
    risk, plan, viability = _risk_and_plan(cfg, snapshot, inventory)
    sm = StateMachineV2(cfg)
    sm.transition(snapshot=snapshot, inventory=inventory, risk=risk, viability=viability)
    sm.transition(snapshot=snapshot, inventory=inventory, risk=risk, viability=viability)
    sm.transition(snapshot=snapshot, inventory=inventory, risk=risk, viability=viability)
    sm._excess_baseline_ts = time.time() - 31.0
    sm._excess_baseline_value_usd = 3.0
    result = sm.transition(snapshot=snapshot, inventory=inventory, risk=risk, viability=viability)
    assert result.lifecycle == "defensive"
    assert result.no_progress is True


def test_no_progress_for_30s_and_no_helpful_quotes_enters_unwind():
    cfg = MMConfigV2(session_budget_usd=15.0, base_clip_usd=6.0)
    snapshot = _snapshot()
    inventory = _inventory(
        dn_shares=6.0,
        excess_dn_qty=6.0,
        excess_dn_value_usd=3.0,
        excess_value_usd=3.0,
        signed_excess_value_usd=-3.0,
        total_inventory_value_usd=3.0,
    )
    risk, _, _ = _risk_and_plan(cfg, snapshot, inventory)
    sm = StateMachineV2(cfg)
    bad_viability = QuoteViabilitySummary(
        any_quote=True,
        four_quotes=False,
        helpful_count=0,
        harmful_count=2,
        helpful_only=False,
        harmful_only=True,
        four_quote_presence_ratio=0.10,
    )
    sm.transition(snapshot=snapshot, inventory=inventory, risk=risk, viability=bad_viability)
    sm.transition(snapshot=snapshot, inventory=inventory, risk=risk, viability=bad_viability)
    sm.transition(snapshot=snapshot, inventory=inventory, risk=risk, viability=bad_viability)
    sm._excess_baseline_ts = time.time() - 31.0
    sm._excess_baseline_value_usd = 3.0
    for _ in range(3):
        result = sm.transition(snapshot=snapshot, inventory=inventory, risk=risk, viability=bad_viability)
    assert result.lifecycle == "unwind"


def test_hard_cap_exceeded_disables_pair_expanding_intents_without_halt():
    cfg = MMConfigV2(session_budget_usd=15.0, base_clip_usd=6.0)
    snapshot = _snapshot()
    inventory = _inventory(excess_dn_value_usd=5.8, excess_value_usd=5.8, signed_excess_value_usd=-5.8)
    risk, plan, _ = _risk_and_plan(cfg, snapshot, inventory)
    assert risk.target_soft_mode == "unwind"
    assert risk.hard_mode == "none"
    assert plan.dn_bid is None
    assert plan.up_ask is None


def test_first_skew_fill_below_hard_cap_keeps_helpful_quotes_alive():
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
    risk, plan, viability = _risk_and_plan(cfg, snapshot, inventory)
    assert risk.hard_mode == "none"
    assert viability.helpful_count >= 1
    assert plan.quote_balance_state != "none"


def test_below_hard_cap_no_early_unwind_when_helpful_quotes_alive_after_first_fill():
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
    risk, _, viability = _risk_and_plan(cfg, snapshot, inventory)
    sm = StateMachineV2(cfg)
    sm.transition(snapshot=snapshot, inventory=inventory, risk=risk, viability=viability)
    sm.transition(snapshot=snapshot, inventory=inventory, risk=risk, viability=viability)
    sm.transition(snapshot=snapshot, inventory=inventory, risk=risk, viability=viability)
    sm._excess_baseline_ts = time.time() - 31.0
    sm._excess_baseline_value_usd = inventory.excess_value_usd
    result = sm.transition(snapshot=snapshot, inventory=inventory, risk=risk, viability=viability)
    assert viability.helpful_count >= 1
    assert result.lifecycle in {"inventory_skewed", "defensive"}
    assert result.lifecycle != "unwind"


def test_live_like_paired_inventory_with_low_free_usdc_keeps_helpful_quotes_alive():
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
    risk, plan, viability = _risk_and_plan(cfg, snapshot, inventory)
    assert risk.hard_mode == "none"
    assert risk.inventory_side == "flat"
    assert viability.four_quotes is True
    assert plan.quote_balance_state != "none"
    assert plan.quote_viability_reason != "all_quotes_below_min_size"


def test_live_like_paired_inventory_below_hard_cap_does_not_early_unwind():
    cfg = MMConfigV2(session_budget_usd=15.0, base_clip_usd=6.0)
    sm = StateMachineV2(cfg)
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
    risk, _, viability = _risk_and_plan(cfg, snapshot, inventory)
    sm.transition(snapshot=snapshot, inventory=inventory, risk=risk, viability=viability)
    sm.transition(snapshot=snapshot, inventory=inventory, risk=risk, viability=viability)
    sm._excess_baseline_ts = time.time() - 31.0
    sm._excess_baseline_value_usd = inventory.excess_value_usd
    result = sm.transition(snapshot=snapshot, inventory=inventory, risk=risk, viability=viability)
    assert risk.inventory_side == "flat"
    assert viability.four_quotes is True
    assert result.lifecycle != "unwind"


def test_wallet_stale_does_not_trigger_true_drift():
    cfg = MMConfigV2(reconcile_drift_threshold_shares=1.5)
    reconcile = ReconcileV2(cfg)
    market = _market()
    reconcile.align(0.0, 0.0)
    reconcile.reconcile(
        market=market,
        real_up=8.0,
        real_dn=0.0,
        total_usdc=15.0,
        available_usdc=15.0,
        active_orders={},
        fv_up=0.54,
        fv_dn=0.46,
        wallet_snapshot_stale=True,
    )
    assert reconcile.status == "wallet_stale"
    assert reconcile.true_drift is False
    assert reconcile.drift_evidence.classification == "wallet_stale"


def test_sellability_lag_does_not_trigger_true_drift():
    cfg = MMConfigV2(reconcile_drift_threshold_shares=1.5)
    reconcile = ReconcileV2(cfg)
    market = _market()
    reconcile.align(0.0, 6.0)
    reconcile.reconcile(
        market=market,
        real_up=0.0,
        real_dn=0.0,
        total_usdc=15.0,
        available_usdc=15.0,
        active_orders={},
        fv_up=0.54,
        fv_dn=0.46,
        sellability_lag_active=True,
    )
    assert reconcile.status == "sellability_lag"
    assert reconcile.true_drift is False
    assert reconcile.drift_evidence.classification == "sellability_lag"


def test_true_drift_with_position_enters_emergency_unwind_before_halt():
    cfg = MMConfigV2(session_budget_usd=15.0, base_clip_usd=6.0)
    snapshot = _snapshot()
    inventory = _inventory(
        up_shares=6.0,
        excess_up_qty=6.0,
        excess_up_value_usd=3.24,
        excess_value_usd=3.24,
        signed_excess_value_usd=3.24,
        total_inventory_value_usd=3.24,
    )
    analytics = AnalyticsState()

    risk_emergency = HardSafetyKernel(cfg).evaluate(
        snapshot=snapshot,
        inventory=inventory,
        analytics=analytics,
        health=HealthState(true_drift=True, true_drift_no_progress_sec=5.0),
    )
    assert risk_emergency.hard_mode == "emergency_unwind"

    risk_halted = HardSafetyKernel(cfg).evaluate(
        snapshot=snapshot,
        inventory=inventory,
        analytics=analytics,
        health=HealthState(true_drift=True, true_drift_no_progress_sec=25.0),
    )
    assert risk_halted.hard_mode == "halted"


def test_cancel_repost_sell_lag_does_not_halt_runtime():
    cfg = MMConfigV2(reconcile_drift_threshold_shares=1.5, session_budget_usd=15.0, base_clip_usd=6.0)
    reconcile = ReconcileV2(cfg)
    market = _market()
    snapshot = _snapshot()

    # Start with inventory on DN leg, then emulate PM post-cancel release lag:
    # free DN appears as zero for a few ticks even though wallet truth is not broken.
    reconcile.align(0.0, 6.0)
    inventory = reconcile.reconcile(
        market=market,
        real_up=0.0,
        real_dn=0.0,
        total_usdc=15.0,
        available_usdc=15.0,
        active_orders={},
        fv_up=0.54,
        fv_dn=0.46,
        sellability_lag_active=True,
    )

    assert reconcile.status == "sellability_lag"
    assert reconcile.true_drift is False
    assert reconcile.drift_evidence.classification == "sellability_lag"

    risk = HardSafetyKernel(cfg).evaluate(
        snapshot=snapshot,
        inventory=inventory,
        analytics=AnalyticsState(),
        health=HealthState(
            reconcile_status=reconcile.status,
            true_drift=reconcile.true_drift,
            sellability_lag_active=True,
        ),
    )
    assert risk.hard_mode == "none"


def test_drawdown_requires_persistence_before_hard_mode():
    cfg = MMConfigV2(session_budget_usd=50.0, base_clip_usd=6.0, hard_drawdown_usd=4.0)
    snapshot = _snapshot()
    inventory = _inventory(
        up_shares=6.0,
        excess_up_qty=6.0,
        excess_up_value_usd=3.24,
        excess_value_usd=3.24,
        signed_excess_value_usd=3.24,
        total_inventory_value_usd=3.24,
    )
    analytics = AnalyticsState(session_pnl_equity_usd=-4.8, session_pnl=-4.8)

    risk_pre = HardSafetyKernel(cfg).evaluate(
        snapshot=snapshot,
        inventory=inventory,
        analytics=analytics,
        health=HealthState(drawdown_breach_active=False, drawdown_breach_ticks=1, drawdown_breach_age_sec=1.0),
    )
    assert risk_pre.hard_mode == "none"

    risk_post = HardSafetyKernel(cfg).evaluate(
        snapshot=snapshot,
        inventory=inventory,
        analytics=analytics,
        health=HealthState(drawdown_breach_active=True, drawdown_breach_ticks=3, drawdown_breach_age_sec=9.0),
    )
    assert risk_post.hard_mode == "emergency_unwind"


def test_live_like_window_reports_mm_regime_ratios():
    class _MockClient:
        _orders = {}

    mm = MarketMakerV2(SimpleNamespace(), _MockClient(), MMConfigV2())
    now = time.time()
    mm._lifecycle_history = [
        (now - 50.0, "quoting"),
        (now - 40.0, "quoting"),
        (now - 30.0, "inventory_skewed"),
        (now - 20.0, "defensive"),
        (now - 10.0, "unwind"),
        (now - 5.0, "unwind"),
    ]
    ratios = mm._lifecycle_ratios(window_sec=60.0)
    assert ratios["quoting_ratio_60s"] > 0.30
    assert ratios["unwind_ratio_60s"] > 0.30
    assert ratios["inventory_skewed_ratio_60s"] > 0.0
    assert ratios["defensive_ratio_60s"] > 0.0


def test_mm_regime_degraded_alert_fires_on_unwind_dominance():
    class _MockClient:
        _orders = {}

    mm = MarketMakerV2(SimpleNamespace(), _MockClient(), MMConfigV2())
    # Emulate prolonged degraded regime.
    mm._mm_regime_degraded_started_ts = time.time() - 130.0
    mm._update_mm_regime_alert(quoting_ratio_60s=0.10, unwind_ratio_60s=0.80)
    assert "mm_regime_degraded" in mm._alerts
