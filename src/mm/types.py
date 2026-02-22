"""Core data types for the Market Making engine."""
from __future__ import annotations
from dataclasses import dataclass, field
from typing import Optional
import time


@dataclass
class Quote:
    """A single quote (bid or ask) to be placed on Polymarket CLOB."""
    side: str              # "BUY" or "SELL"
    token_id: str          # Polymarket token ID
    price: float           # Quote price (0.01 - 0.99)
    size: float            # Size in shares
    order_id: Optional[str] = None  # Filled after placement
    placed_at: float = 0.0       # Unix timestamp when placed on exchange

    @property
    def notional(self) -> float:
        return self.price * self.size


@dataclass
class Fill:
    """A recorded fill (trade execution)."""
    ts: float              # Unix timestamp
    side: str              # "BUY" or "SELL"
    token_id: str
    price: float
    size: float
    fee: float = 0.0
    order_id: str = ""
    is_maker: bool = True

    @property
    def notional(self) -> float:
        return self.price * self.size


@dataclass
class Inventory:
    """Track current inventory across UP and DOWN tokens."""
    up_shares: float = 0.0
    dn_shares: float = 0.0
    usdc: float = 0.0
    initial_usdc: float = 0.0  # Starting balance for PnL calc

    @property
    def net_delta(self) -> float:
        """Positive = long UP, negative = long DN."""
        return self.up_shares - self.dn_shares

    @property
    def abs_exposure(self) -> float:
        """Total absolute share exposure."""
        return self.up_shares + self.dn_shares

    def update_from_fill(self, fill: Fill, token_type: str) -> None:
        """Update inventory after a fill.
        token_type: 'up' or 'dn'
        """
        if fill.side == "BUY":
            if token_type == "up":
                self.up_shares += fill.size
            else:
                self.dn_shares += fill.size
            self.usdc -= fill.notional + fill.fee
        else:  # SELL
            if token_type == "up":
                self.up_shares -= fill.size
            else:
                self.dn_shares -= fill.size
            self.usdc += fill.notional - fill.fee

    def reconcile(self, real_up: float, real_dn: float) -> None:
        """Update shares to match actual PM balance."""
        self.up_shares = real_up
        self.dn_shares = real_dn


@dataclass
class MMState:
    """Snapshot of Market Maker state for dashboard/API."""
    # Current quotes
    bid_up: Optional[Quote] = None
    ask_up: Optional[Quote] = None
    bid_dn: Optional[Quote] = None
    ask_dn: Optional[Quote] = None

    # Inventory
    inventory: Inventory = field(default_factory=Inventory)

    # PnL
    realized_pnl: float = 0.0
    unrealized_pnl: float = 0.0
    total_fees: float = 0.0

    # Session stats
    fill_count: int = 0
    total_volume: float = 0.0
    uptime_sec: float = 0.0
    avg_spread_bps: float = 0.0

    # Risk
    is_paused: bool = False
    pause_reason: str = ""

    # Fair value
    fair_value_up: float = 0.5
    fair_value_dn: float = 0.5
    binance_mid: float = 0.0

    # Rebate
    estimated_daily_rebate: float = 0.0
    orders_scoring_eligible: int = 0

    # Fills history (last N)
    recent_fills: list = field(default_factory=list)

    # Timestamps
    started_at: float = 0.0
    last_quote_ts: float = 0.0
    last_fill_ts: float = 0.0


@dataclass
class MarketInfo:
    """Information about a Polymarket market (window)."""
    coin: str
    timeframe: str
    up_token_id: str
    dn_token_id: str
    strike: float           # Strike price for the window
    window_start: float     # Unix timestamp
    window_end: float       # Unix timestamp
    condition_id: str = ""
    question: str = ""
    min_order_size: float = 5.0  # Default PM minimum

    @property
    def time_remaining(self) -> float:
        """Seconds until window expiry."""
        return max(0.0, self.window_end - time.time())

    @property
    def is_expired(self) -> bool:
        return time.time() >= self.window_end
