from __future__ import annotations

from dataclasses import dataclass

from .config import MMConfigV2
from .types import AnalyticsState, HealthState, PairInventoryState, PairMarketSnapshot, RiskRegime


@dataclass
class SoftRiskAssessment:
    target_soft_mode: str
    inventory_side: str
    pressure_abs: float
    pressure_signed: float
    quality_pressure: float
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
        del analytics
        budget = max(0.01, float(self.config.session_budget_usd))
        soft_cap = max(0.01, float(self.config.soft_excess_value_ratio) * budget)
        defensive_cap = max(soft_cap, float(self.config.defensive_excess_value_ratio) * budget)
        hard_cap = max(defensive_cap, float(self.config.hard_excess_value_ratio) * budget)

        excess_value_usd = max(0.0, float(inventory.excess_value_usd))
        signed_excess_value_usd = float(inventory.signed_excess_value_usd)
        if signed_excess_value_usd > 1e-6:
            inventory_side = "up"
        elif signed_excess_value_usd < -1e-6:
            inventory_side = "dn"
        else:
            inventory_side = "flat"

        pressure_abs = self._clamp(excess_value_usd / hard_cap, 0.0, 1.0)
        pressure_signed = self._clamp(signed_excess_value_usd / hard_cap, -1.0, 1.0)

        quality_pressure = 0.0
        if not snapshot.market_tradeable:
            quality_pressure = 1.0
        if float(self.config.min_market_quality_score) > 0:
            quality_deficit = max(
                0.0,
                float(self.config.min_market_quality_score) - float(snapshot.market_quality_score),
            ) / float(self.config.min_market_quality_score)
            quality_pressure = max(quality_pressure, self._clamp(quality_deficit, 0.0, 1.0))
        divergence_pressure = max(float(snapshot.divergence_up), float(snapshot.divergence_dn)) / 0.12
        quality_pressure = max(quality_pressure, self._clamp(divergence_pressure, 0.0, 1.0))

        target_soft_mode = "normal"
        soft_reason = "normal quoting"
        if snapshot.time_left_sec <= float(self.config.unwind_window_sec):
            target_soft_mode = "unwind"
            soft_reason = "expiry unwind window"
        elif excess_value_usd >= hard_cap:
            target_soft_mode = "unwind"
            soft_reason = f"hard excess ${excess_value_usd:.2f}"
        elif (
            excess_value_usd >= defensive_cap
            or not snapshot.market_tradeable
            or float(snapshot.market_quality_score) < float(self.config.min_market_quality_score)
            or max(float(snapshot.divergence_up), float(snapshot.divergence_dn)) > 0.12
        ):
            target_soft_mode = "defensive"
            soft_reason = "defensive market regime"
        elif excess_value_usd >= soft_cap:
            target_soft_mode = "inventory_skewed"
            soft_reason = f"soft excess ${excess_value_usd:.2f}"

        return SoftRiskAssessment(
            target_soft_mode=target_soft_mode,
            inventory_side=inventory_side,
            pressure_abs=pressure_abs,
            pressure_signed=pressure_signed,
            quality_pressure=quality_pressure,
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
        hard_mode = "none"
        hard_reason = ""
        has_material_position = inventory.up_shares > 0.5 or inventory.dn_shares > 0.5
        drawdown_budget = 1.0
        if self.config.hard_drawdown_usd > 0:
            drawdown_budget = max(
                0.0,
                1.0 - max(0.0, -analytics.session_pnl) / float(self.config.hard_drawdown_usd),
            )

        if health.true_drift:
            hard_mode = "halted"
            hard_reason = "true inventory drift"
        elif not health.transport_ok and health.last_api_error:
            hard_mode = "emergency_unwind" if has_material_position else "halted"
            hard_reason = f"transport unhealthy: {health.last_api_error}"
        elif not health.heartbeat_ok:
            hard_mode = "emergency_unwind" if has_material_position else "halted"
            hard_reason = "heartbeat failure"
        elif analytics.session_pnl <= -float(self.config.hard_drawdown_usd):
            hard_mode = "emergency_unwind" if has_material_position else "halted"
            hard_reason = f"hard drawdown ${analytics.session_pnl:.2f}"
        elif health.residual_inventory_failure and snapshot.time_left_sec <= float(self.config.emergency_taker_start_sec):
            hard_mode = "emergency_unwind"
            hard_reason = "residual inventory near expiry"

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
        )
