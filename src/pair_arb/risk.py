"""Risk manager for pair arb — budget, exposure, cooldown, drawdown."""
from __future__ import annotations

import time

from .config import PairArbConfig
from .types import ArbOpportunity


class ArbRiskManager:
    """Enforces risk constraints before arb execution."""

    def __init__(self, config: PairArbConfig):
        self.config = config
        self.session_spent: float = 0.0
        self.session_pnl: float = 0.0
        self.last_arb_ts: float = 0.0
        self.leg_risk_losses: float = 0.0
        self.total_arbs: int = 0

    def can_execute(self, opp: ArbOpportunity) -> tuple[bool, str]:
        """Check all risk constraints. Returns (allowed, reason)."""
        # Cooldown
        if time.time() - self.last_arb_ts < self.config.cooldown_after_arb_sec:
            return False, "cooldown"

        # Budget
        cost = opp.total_cost_per_pair * opp.max_arb_shares
        if self.session_spent + cost > self.config.session_budget_usd:
            return False, "budget_exceeded"

        # Per-trade exposure
        if cost > self.config.max_leg_risk_usd:
            return False, "max_leg_risk"

        # Drawdown
        if self.session_pnl < -self.config.hard_drawdown_usd:
            return False, "drawdown_limit"

        return True, "ok"

    def record_execution(self, cost_usd: float) -> None:
        self.session_spent += cost_usd
        self.last_arb_ts = time.time()
        self.total_arbs += 1

    def record_merge_profit(self, proceeds_usd: float, cost_usd: float) -> None:
        """Record profit from a merge. proceeds - cost = net profit."""
        self.session_pnl += (proceeds_usd - cost_usd)

    def record_leg_risk_loss(self, loss_usd: float) -> None:
        self.leg_risk_losses += loss_usd
        self.session_pnl -= loss_usd

    def to_dict(self) -> dict:
        return {
            "session_spent": round(self.session_spent, 4),
            "session_pnl": round(self.session_pnl, 4),
            "leg_risk_losses": round(self.leg_risk_losses, 4),
            "total_arbs": self.total_arbs,
            "last_arb_ts": self.last_arb_ts,
        }
