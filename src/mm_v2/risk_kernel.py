from __future__ import annotations

from dataclasses import dataclass

from .config import MMConfigV2
from .types import AnalyticsState, HealthState, PairInventoryState, PairMarketSnapshot, RiskRegime

INVENTORY_SIDE_DEADBAND_USD = 0.50
FLAT_BOOTSTRAP_DIVERGENCE_DEFENSIVE = 0.20
FLAT_BOOTSTRAP_QUALITY_MULT = 0.50
DIVERGENCE_PRESSURE_SCALE = 0.30
DIVERGENCE_DEFENSIVE_THRESHOLD = 0.30
UNTRADEABLE_QUALITY_MULT = 0.75
UNTRADEABLE_DIVERGENCE_MULT = 1.50
UNTRADEABLE_BASE_QUALITY_PRESSURE = 0.60
UNTRADEABLE_MATERIAL_INVENTORY_DIVERGENCE = 0.06


@dataclass
class SoftRiskAssessment:
    target_soft_mode: str
    inventory_side: str
    pressure_abs: float
    pressure_signed: float
    quality_pressure: float
    target_ratio_pressure: float
    soft_reason: str


class SoftRiskKernel:
    def __init__(self, config: MMConfigV2):
        self.config = config

    @staticmethod
    def _clamp(value: float, lo: float, hi: float) -> float:
        return max(lo, min(hi, value))

    def evaluate(
        self,
        *,
        snapshot: PairMarketSnapshot,
        inventory: PairInventoryState,
        analytics: AnalyticsState,
    ) -> SoftRiskAssessment:
        budget = max(0.01, float(self.config.session_budget_usd))
        soft_cap = max(0.01, float(self.config.soft_excess_value_ratio) * budget)
        defensive_cap = max(soft_cap, float(self.config.defensive_excess_value_ratio) * budget)
        hard_cap = max(defensive_cap, float(self.config.effective_hard_excess_value_ratio()) * budget)

        excess_value_usd = max(0.0, float(inventory.excess_value_usd))
        signed_excess_value_usd = float(inventory.signed_excess_value_usd)
        deadband_usd = max(0.01, INVENTORY_SIDE_DEADBAND_USD)
        if signed_excess_value_usd > deadband_usd:
            inventory_side = "up"
        elif signed_excess_value_usd < -deadband_usd:
            inventory_side = "dn"
        else:
            inventory_side = "flat"

        pressure_abs = self._clamp(excess_value_usd / hard_cap, 0.0, 1.0)
        pressure_signed = self._clamp(signed_excess_value_usd / hard_cap, -1.0, 1.0)
        target_pair_value_usd = max(0.01, float(inventory.target_pair_value_usd))
        pair_over_target_usd = max(0.0, float(inventory.pair_value_over_target_usd))
        pair_entry_cost = max(0.0, float(getattr(inventory, "pair_entry_cost", 0.0) or 0.0))
        pair_entry_pnl_per_share = float(getattr(inventory, "pair_entry_pnl_per_share", 0.0) or 0.0)
        pair_entry_loss_per_share = max(0.0, -pair_entry_pnl_per_share)
        target_ratio_pressure = self._clamp(pair_over_target_usd / target_pair_value_usd, 0.0, 1.0)
        target_ratio_activation_usd = float(self.config.effective_target_ratio_activation_usd())
        gross_brake_activation_usd = max(1.5, 0.05 * budget)
        material_inventory_usd = max(6.0, 0.20 * budget)
        inventory_nearly_flat_usd = max(2.0, 0.10 * budget)
        marketability_guard_active = bool(getattr(analytics, "marketability_guard_active", False))
        marketability_guard_reason = str(getattr(analytics, "marketability_guard_reason", "") or "")
        marketability_churn_confirmed = bool(getattr(analytics, "marketability_churn_confirmed", False))
        marketability_problem_side = str(getattr(analytics, "marketability_problem_side", "") or "")
        marketability_side_locked = str(getattr(analytics, "marketability_side_locked", "") or "")
        if marketability_side_locked in {"up", "dn"}:
            marketability_problem_side = marketability_side_locked
        up_inventory_value_usd = max(
            0.0,
            float(inventory.up_shares)
            * max(
                0.01,
                float(
                    snapshot.up_best_bid
                    if snapshot.up_best_bid is not None
                    else snapshot.midpoint_anchor_up
                    if snapshot.midpoint_anchor_up is not None
                    else snapshot.fv_up
                ),
            ),
        )
        dn_inventory_value_usd = max(
            0.0,
            float(inventory.dn_shares)
            * max(
                0.01,
                float(
                    snapshot.dn_best_bid
                    if snapshot.dn_best_bid is not None
                    else snapshot.midpoint_anchor_dn
                    if snapshot.midpoint_anchor_dn is not None
                    else snapshot.fv_dn
                ),
            ),
        )
        buy_edge_gap_up = float(getattr(snapshot, "buy_edge_gap_up", 0.0) or 0.0)
        buy_edge_gap_dn = float(getattr(snapshot, "buy_edge_gap_dn", 0.0) or 0.0)
        toxic_buy_gap_threshold = 0.18
        toxic_divergence_inventory_up = bool(
            bool(snapshot.market_tradeable)
            and buy_edge_gap_up >= toxic_buy_gap_threshold
            and up_inventory_value_usd >= material_inventory_usd
        )
        toxic_divergence_inventory_dn = bool(
            bool(snapshot.market_tradeable)
            and buy_edge_gap_dn >= toxic_buy_gap_threshold
            and dn_inventory_value_usd >= material_inventory_usd
        )

        max_divergence = max(float(snapshot.divergence_up), float(snapshot.divergence_dn))
        min_quality = float(self.config.min_market_quality_score)
        flat_bootstrap = inventory_side == "flat" and excess_value_usd < max(soft_cap * 0.5, deadband_usd)
        is_untradeable = not bool(snapshot.market_tradeable)
        severe_untradeable_quality = bool(
            float(snapshot.market_quality_score) < (min_quality * UNTRADEABLE_QUALITY_MULT)
        )
        severe_untradeable_divergence = bool(
            max_divergence > (DIVERGENCE_DEFENSIVE_THRESHOLD * UNTRADEABLE_DIVERGENCE_MULT)
        )
        severe_untradeable = bool(
            is_untradeable and (severe_untradeable_quality or severe_untradeable_divergence)
        )
        material_inventory = float(inventory.total_inventory_value_usd) >= material_inventory_usd
        inventory_nearly_flat = float(inventory.total_inventory_value_usd) < inventory_nearly_flat_usd
        marketability_context_bad = bool(is_untradeable or marketability_guard_active)
        marketability_problem_matches_inventory = bool(
            marketability_problem_side in {"up", "dn"} and marketability_problem_side == inventory_side
        )
        untradeable_inventory_divergence_bad = bool(
            max_divergence >= UNTRADEABLE_MATERIAL_INVENTORY_DIVERGENCE
        )
        untradeable_inventory_quality_bad = bool(
            float(snapshot.market_quality_score) < min_quality
        )

        quality_pressure = 0.0
        if is_untradeable:
            quality_pressure = max(quality_pressure, UNTRADEABLE_BASE_QUALITY_PRESSURE)
        if min_quality > 0:
            quality_deficit = max(
                0.0,
                min_quality - float(snapshot.market_quality_score),
            ) / min_quality
            quality_pressure = max(quality_pressure, self._clamp(quality_deficit, 0.0, 1.0))
        # Divergence is noisy around endpoint liquidity regimes; do not
        # saturate quality pressure too early or MM gets stuck in defensive.
        divergence_pressure = max_divergence / DIVERGENCE_PRESSURE_SCALE
        quality_pressure = max(quality_pressure, self._clamp(divergence_pressure, 0.0, 1.0))
        if severe_untradeable:
            quality_pressure = 1.0

        target_soft_mode = "normal"
        soft_reason = "normal quoting"
        if snapshot.time_left_sec <= float(self.config.unwind_window_sec):
            target_soft_mode = "unwind"
            soft_reason = "expiry unwind window"
        elif excess_value_usd >= hard_cap:
            target_soft_mode = "unwind"
            soft_reason = f"hard excess ${excess_value_usd:.2f}"
        elif excess_value_usd >= defensive_cap:
            target_soft_mode = "defensive"
            soft_reason = "defensive excess regime"
        elif bool(getattr(snapshot, "fast_move_hard_active", False)) or bool(
            getattr(snapshot, "fast_move_pause_active", False)
        ):
            target_soft_mode = "defensive"
            soft_reason = "defensive fast move"
        elif toxic_divergence_inventory_up or toxic_divergence_inventory_dn:
            target_soft_mode = "defensive"
            if toxic_divergence_inventory_up and toxic_divergence_inventory_dn:
                soft_reason = "defensive toxic divergence inventory"
            elif toxic_divergence_inventory_up:
                soft_reason = "defensive toxic divergence inventory (up)"
            else:
                soft_reason = "defensive toxic divergence inventory (dn)"
        else:
            market_quality_bad = float(snapshot.market_quality_score) < min_quality
            divergence_bad = max_divergence > DIVERGENCE_DEFENSIVE_THRESHOLD
            if marketability_churn_confirmed and (
                material_inventory or marketability_problem_matches_inventory or not inventory_nearly_flat
            ):
                target_soft_mode = "defensive"
                if marketability_problem_side in {"up", "dn"}:
                    soft_reason = f"defensive marketability churn ({marketability_problem_side})"
                else:
                    soft_reason = "defensive marketability churn"
            elif is_untradeable and marketability_guard_active:
                target_soft_mode = "defensive"
                if marketability_guard_reason:
                    soft_reason = f"defensive marketability guard ({marketability_guard_reason})"
                else:
                    soft_reason = "defensive marketability guard"
            elif is_untradeable and material_inventory and (
                untradeable_inventory_divergence_bad or untradeable_inventory_quality_bad
            ):
                target_soft_mode = "defensive"
                soft_reason = "defensive untradeable inventory regime"
            elif is_untradeable and pair_over_target_usd >= gross_brake_activation_usd:
                target_soft_mode = "inventory_skewed"
                soft_reason = (
                    f"untradeable pair inventory over target by ${pair_over_target_usd:.2f}"
                )
            elif is_untradeable and severe_untradeable:
                target_soft_mode = "defensive"
                if severe_untradeable_quality:
                    soft_reason = "defensive market regime (untradeable severe quality)"
                else:
                    soft_reason = "defensive market regime (untradeable severe divergence)"
            elif is_untradeable and not inventory_nearly_flat:
                target_soft_mode = "inventory_skewed"
                soft_reason = "inventory_skewed untradeable nonflat"
            elif is_untradeable and not marketability_churn_confirmed:
                soft_reason = "normal quoting (untradeable tolerated)"
            elif flat_bootstrap:
                # When inventory is effectively flat, avoid overreacting to
                # mild quality noise. Keep two-sided MM entry possible unless
                # degradation is severe.
                quality_floor = min_quality * FLAT_BOOTSTRAP_QUALITY_MULT
                if (
                    float(snapshot.market_quality_score) < quality_floor
                    or max_divergence > FLAT_BOOTSTRAP_DIVERGENCE_DEFENSIVE
                ):
                    target_soft_mode = "defensive"
                    soft_reason = "defensive bootstrap regime"
            elif market_quality_bad or divergence_bad:
                target_soft_mode = "defensive"
                soft_reason = "defensive market regime"
            elif bool(getattr(snapshot, "fast_move_soft_active", False)):
                target_soft_mode = "inventory_skewed" if inventory_nearly_flat else "defensive"
                soft_reason = "inventory fast move"
        if target_soft_mode == "normal" and excess_value_usd >= soft_cap:
            target_soft_mode = "inventory_skewed"
            soft_reason = f"soft excess ${excess_value_usd:.2f}"
        if (
            target_soft_mode in {"normal", "inventory_skewed"}
            and pair_entry_loss_per_share > 0.0
            and float(inventory.paired_qty) * pair_entry_loss_per_share >= 1.0
        ):
            target_soft_mode = "defensive"
            soft_reason = f"defensive negative pair carry ${pair_entry_loss_per_share:.4f}/share"
        if target_soft_mode == "normal" and pair_over_target_usd >= target_ratio_activation_usd:
            target_soft_mode = "inventory_skewed"
            soft_reason = (
                f"target pair ratio exceeded by ${pair_over_target_usd:.2f} "
                f"(>=${target_ratio_activation_usd:.2f})"
            )

        return SoftRiskAssessment(
            target_soft_mode=target_soft_mode,
            inventory_side=inventory_side,
            pressure_abs=pressure_abs,
            pressure_signed=pressure_signed,
            quality_pressure=quality_pressure,
            target_ratio_pressure=target_ratio_pressure,
            soft_reason=soft_reason,
        )


class HardSafetyKernel:
    def __init__(self, config: MMConfigV2):
        self.config = config
        self.soft_kernel = SoftRiskKernel(config)

    def evaluate(
        self,
        *,
        snapshot: PairMarketSnapshot,
        inventory: PairInventoryState,
        analytics: AnalyticsState,
        health: HealthState,
    ) -> RiskRegime:
        drawdown_pnl = float(getattr(analytics, "session_pnl_drawdown_usd", 0.0) or 0.0)
        if abs(drawdown_pnl) < 1e-12:
            drawdown_pnl = float(getattr(analytics, "session_pnl_equity_usd", 0.0) or 0.0)
        if abs(drawdown_pnl) < 1e-12 and abs(float(getattr(analytics, "session_pnl", 0.0) or 0.0)) > 0.0:
            # Backward-compatible fallback while tests/fixtures migrate.
            drawdown_pnl = float(getattr(analytics, "session_pnl", 0.0) or 0.0)
        hard_mode = "none"
        hard_reason = ""
        has_material_position = inventory.up_shares > 0.5 or inventory.dn_shares > 0.5
        effective_drawdown_usd = max(0.01, float(self.config.effective_hard_drawdown_usd()))
        drawdown_budget = 1.0
        if self.config.hard_drawdown_usd > 0:
            drawdown_budget = max(
                0.0,
                1.0 - max(0.0, -drawdown_pnl) / effective_drawdown_usd,
            )
        early_drawdown_pressure = max(0.0, -drawdown_pnl / effective_drawdown_usd)

        if health.true_drift:
            if has_material_position:
                if float(getattr(health, "true_drift_no_progress_sec", 0.0) or 0.0) >= 20.0:
                    hard_mode = "halted"
                    hard_reason = "true inventory drift: no unwind progress"
                else:
                    hard_mode = "emergency_unwind"
                    hard_reason = "true inventory drift: controlled unwind"
            else:
                hard_mode = "halted"
                hard_reason = "true inventory drift"
        elif not health.transport_ok and health.last_api_error:
            hard_mode = "emergency_unwind" if has_material_position else "halted"
            hard_reason = f"transport unhealthy: {health.last_api_error}"
        elif not health.heartbeat_ok:
            hard_mode = "emergency_unwind" if has_material_position else "halted"
            hard_reason = "heartbeat failure"
        elif bool(getattr(health, "drawdown_breach_active", False)):
            hard_mode = "emergency_unwind" if has_material_position else "halted"
            hard_reason = (
                f"hard drawdown ${drawdown_pnl:.2f} "
                f"(thr ${effective_drawdown_usd:.2f})"
            )
        elif health.residual_inventory_failure and snapshot.time_left_sec <= float(self.config.emergency_taker_start_sec):
            hard_mode = "emergency_unwind"
            hard_reason = "residual inventory near expiry"
        if bool(getattr(health, "post_terminal_cleanup_grace_active", False)) and hard_mode == "halted":
            hard_mode = "emergency_unwind" if has_material_position else "none"
            hard_reason = (
                f"terminal cleanup grace: {hard_reason}"
                if hard_reason
                else "terminal cleanup grace"
            )

        soft = self.soft_kernel.evaluate(
            snapshot=snapshot,
            inventory=inventory,
            analytics=analytics,
        )
        edge_score = max(0.0, min(1.0, float(snapshot.market_quality_score) * float(snapshot.fv_confidence)))
        return RiskRegime(
            soft_mode=soft.target_soft_mode,  # type: ignore[arg-type]
            hard_mode=hard_mode,  # type: ignore[arg-type]
            target_soft_mode=soft.target_soft_mode,  # type: ignore[arg-type]
            reason=hard_reason or soft.soft_reason,
            inventory_pressure=soft.pressure_signed,
            edge_score=edge_score,
            drawdown_pct_budget=drawdown_budget,
            inventory_side=soft.inventory_side,  # type: ignore[arg-type]
            inventory_pressure_abs=soft.pressure_abs,
            inventory_pressure_signed=soft.pressure_signed,
            quality_pressure=soft.quality_pressure,
            target_ratio_pressure=soft.target_ratio_pressure,
            early_drawdown_pressure=early_drawdown_pressure,
            marketability_guard_active=bool(getattr(analytics, "marketability_guard_active", False)),
            marketability_guard_reason=str(getattr(analytics, "marketability_guard_reason", "") or ""),
            marketability_churn_confirmed=bool(getattr(analytics, "marketability_churn_confirmed", False)),
            marketability_problem_side=str(getattr(analytics, "marketability_problem_side", "") or ""),
            marketability_side_locked=str(getattr(analytics, "marketability_side_locked", "") or ""),
            marketability_side_lock_age_sec=float(getattr(analytics, "marketability_side_lock_age_sec", 0.0) or 0.0),
            pair_entry_cost=float(getattr(analytics, "pair_entry_cost", 0.0) or 0.0),
            pair_entry_pnl_per_share=float(getattr(analytics, "pair_entry_pnl_per_share", 0.0) or 0.0),
            rolling_markout_up_5s=float(getattr(analytics, "rolling_markout_up_5s", 0.0) or 0.0),
            rolling_markout_dn_5s=float(getattr(analytics, "rolling_markout_dn_5s", 0.0) or 0.0),
            rolling_spread_capture_up=float(getattr(analytics, "rolling_spread_capture_up", 0.0) or 0.0),
            rolling_spread_capture_dn=float(getattr(analytics, "rolling_spread_capture_dn", 0.0) or 0.0),
        )
