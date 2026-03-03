from __future__ import annotations

from .config import MMConfigV2
from .types import AnalyticsState, HealthState, PairInventoryState, PairMarketSnapshot, RiskRegime


class HardSafetyKernel:
    def __init__(self, config: MMConfigV2):
        self.config = config

    def evaluate(
        self,
        *,
        snapshot: PairMarketSnapshot,
        inventory: PairInventoryState,
        analytics: AnalyticsState,
        health: HealthState,
    ) -> RiskRegime:
        hard_mode = "none"
        reason = ""
        has_material_position = inventory.up_shares > 0.5 or inventory.dn_shares > 0.5
        drawdown_budget = 1.0
        if self.config.hard_drawdown_usd > 0:
            drawdown_budget = max(
                0.0,
                1.0 - max(0.0, -analytics.session_pnl) / float(self.config.hard_drawdown_usd),
            )

        if health.true_drift:
            hard_mode = "halted"
            reason = "true inventory drift"
        elif not health.transport_ok and health.last_api_error:
            hard_mode = "emergency_unwind" if has_material_position else "halted"
            reason = f"transport unhealthy: {health.last_api_error}"
        elif not health.heartbeat_ok:
            hard_mode = "emergency_unwind" if has_material_position else "halted"
            reason = "heartbeat failure"
        elif analytics.session_pnl <= -float(self.config.hard_drawdown_usd):
            hard_mode = "emergency_unwind" if has_material_position else "halted"
            reason = f"hard drawdown ${analytics.session_pnl:.2f}"
        elif health.residual_inventory_failure and snapshot.time_left_sec <= float(self.config.emergency_taker_start_sec):
            hard_mode = "emergency_unwind"
            reason = "residual inventory near expiry"

        soft_mode = "normal"
        if hard_mode == "none":
            hard_cap = max(0.01, float(self.config.hard_excess_value_ratio) * float(self.config.session_budget_usd))
            soft_cap = max(0.01, float(self.config.soft_excess_value_ratio) * float(self.config.session_budget_usd))
            excess_value = max(inventory.excess_up_value_usd, inventory.excess_dn_value_usd)
            if snapshot.time_left_sec <= float(self.config.unwind_window_sec):
                soft_mode = "unwind"
                reason = "expiry unwind window"
            elif excess_value >= hard_cap:
                soft_mode = "unwind"
                reason = f"hard excess ${excess_value:.2f}"
            elif not snapshot.market_tradeable or snapshot.market_quality_score < float(self.config.min_market_quality_score) or max(snapshot.divergence_up, snapshot.divergence_dn) > 0.12:
                soft_mode = "defensive"
                reason = "defensive market regime"
            elif excess_value >= soft_cap:
                soft_mode = "inventory_skewed"
                reason = f"soft excess ${excess_value:.2f}"
        signed_excess = inventory.excess_up_value_usd - inventory.excess_dn_value_usd
        hard_cap = max(0.01, float(self.config.hard_excess_value_ratio) * float(self.config.session_budget_usd))
        inventory_pressure = max(-1.0, min(1.0, signed_excess / hard_cap))
        edge_score = max(0.0, min(1.0, float(snapshot.market_quality_score) * float(snapshot.fv_confidence)))
        return RiskRegime(
            soft_mode=soft_mode,  # type: ignore[arg-type]
            hard_mode=hard_mode,  # type: ignore[arg-type]
            reason=reason,
            inventory_pressure=inventory_pressure,
            edge_score=edge_score,
            drawdown_pct_budget=drawdown_budget,
        )
