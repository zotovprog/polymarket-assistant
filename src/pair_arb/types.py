"""Data types for the Pair Arbitrage engine."""
from __future__ import annotations

from dataclasses import dataclass, field
import time


@dataclass
class ArbMarket:
    """A market being scanned for arb opportunities."""
    coin: str
    timeframe: str
    up_token_id: str
    dn_token_id: str
    condition_id: str
    last_discovered_ts: float = 0.0

    @property
    def scope(self) -> str:
        return f"{self.coin}_{self.timeframe}"


@dataclass
class ArbOpportunity:
    """A detected arb opportunity on a specific market."""
    market: ArbMarket
    ask_up: float
    ask_dn: float
    size_up: float            # Available shares at ask UP
    size_dn: float            # Available shares at ask DN
    fee_up_per_share: float   # Fee per share for buying UP (0 if maker)
    fee_dn_per_share: float   # Fee per share for buying DN (0 if maker)
    net_shares_up: float      # Shares received after BUY fee
    net_shares_dn: float      # Shares received after BUY fee
    max_arb_shares: float     # Executable arb size
    gross_cost_per_pair: float  # ask_up + ask_dn (no fees)
    total_cost_per_pair: float  # Including fees and gas
    profit_per_pair: float      # 1.0 - total_cost_per_pair
    profit_usd: float           # profit_per_pair * max_arb_shares
    detected_at: float = field(default_factory=time.time)


@dataclass
class ArbExecution:
    """Tracks a single arb execution attempt."""
    id: str
    opportunity: ArbOpportunity
    target_shares: float
    up_order_id: str | None = None
    dn_order_id: str | None = None
    up_filled: float = 0.0
    dn_filled: float = 0.0
    up_fill_price: float = 0.0
    dn_fill_price: float = 0.0
    merged_shares: float = 0.0
    realized_profit: float = 0.0
    status: str = "pending"
    error: str = ""
    leg_risk_retries: int = 0
    started_at: float = 0.0
    completed_at: float = 0.0

    def to_dict(self) -> dict:
        return {
            "id": self.id,
            "status": self.status,
            "coin": self.opportunity.market.coin,
            "timeframe": self.opportunity.market.timeframe,
            "target_shares": round(self.target_shares, 2),
            "ask_up": self.opportunity.ask_up,
            "ask_dn": self.opportunity.ask_dn,
            "profit_per_pair": round(self.opportunity.profit_per_pair, 4),
            "up_filled": round(self.up_filled, 2),
            "dn_filled": round(self.dn_filled, 2),
            "merged_shares": round(self.merged_shares, 2),
            "realized_profit": round(self.realized_profit, 4),
            "error": self.error,
            "started_at": self.started_at,
            "completed_at": self.completed_at,
        }


@dataclass
class ArbState:
    """Full engine state snapshot for API."""
    is_running: bool = False
    started_at: float = 0.0
    app_version: str = ""
    paper_mode: bool = True
    markets_scanning: int = 0
    market_scopes: list[str] = field(default_factory=list)
    scan_count: int = 0
    opportunities_seen: int = 0
    opportunities_executed: int = 0
    total_merged_pairs: float = 0.0
    total_profit_usd: float = 0.0
    total_leg_risk_loss_usd: float = 0.0
    session_pnl_usd: float = 0.0
    usdc_balance: float = 0.0
    unmerged_up: dict[str, float] = field(default_factory=dict)
    unmerged_dn: dict[str, float] = field(default_factory=dict)
    pending_executions: list[dict] = field(default_factory=list)
    recent_executions: list[dict] = field(default_factory=list)
    last_opportunity_ts: float = 0.0
    leg_risk_events: int = 0
    config: dict = field(default_factory=dict)

    def to_dict(self) -> dict:
        from dataclasses import asdict
        return asdict(self)
