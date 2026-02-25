"""Core data types for the Market Making engine."""
from __future__ import annotations
from dataclasses import dataclass, field
from typing import Optional
import time


@dataclass
class CostBasis:
    """Track average entry price for a token position."""
    total_cost: float = 0.0
    total_shares: float = 0.0

    @property
    def avg_entry_price(self) -> float:
        if self.total_shares <= 0:
            return 0.0
        return self.total_cost / self.total_shares

    def record_buy(self, price: float, size: float, fee: float) -> None:
        self.total_cost += price * size + fee
        self.total_shares += size

    def record_sell(self, size: float) -> None:
        if self.total_shares <= 0:
            self.total_cost = 0.0
            self.total_shares = 0.0
            return
        fraction = min(size / self.total_shares, 1.0)
        self.total_cost *= (1.0 - fraction)
        self.total_shares = max(0.0, self.total_shares - size)

    def reset(self) -> None:
        self.total_cost = 0.0
        self.total_shares = 0.0


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
    up_cost: CostBasis = field(default_factory=CostBasis)
    dn_cost: CostBasis = field(default_factory=CostBasis)
    paired: PairedInventory = field(default_factory=lambda: PairedInventory())

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

        PM fee mechanics:
        - BUY taker: fee deducted in shares (receive size * 0.98), USDC cost = size * price only
        - BUY maker: no fee, receive full size, USDC cost = size * price
        - SELL taker: fee deducted from USDC proceeds
        - SELL maker: no fee
        """
        cost = self.up_cost if token_type == "up" else self.dn_cost
        if fill.side == "BUY":
            # For BUY taker: PM deducts fee in shares, not USDC.
            # net_shares = fill.size if maker, fill.size * 0.98 if taker.
            # USDC cost is always size * price (fee NOT deducted from USDC).
            if fill.is_maker or fill.fee == 0:
                received_shares = fill.size
                usdc_cost = fill.notional
            else:
                # Taker: fee is in shares; fill.fee is the USD-equivalent
                from .pm_fees import net_shares_after_buy_fee
                received_shares = net_shares_after_buy_fee(
                    fill.size,
                    fill.price,
                    token_id=fill.token_id,
                )
                usdc_cost = fill.notional  # USDC cost = size * price, no extra fee
            if token_type == "up":
                self.up_shares += received_shares
            else:
                self.dn_shares += received_shares
            self.usdc -= usdc_cost
            if self.usdc < 0:
                self.usdc = 0.0  # Floor at zero — real balance checked via API
            cost.record_buy(fill.price, received_shares, fill.fee)
        else:  # SELL
            if token_type == "up":
                self.up_shares = max(0.0, self.up_shares - fill.size)
            else:
                self.dn_shares = max(0.0, self.dn_shares - fill.size)
            self.usdc += fill.notional - fill.fee
            cost.record_sell(fill.size)

    def reconcile(self, real_up: float, real_dn: float,
                  real_usdc: float | None = None) -> None:
        """Update shares (and optionally USDC) to match actual PM balance."""
        old_up = self.up_shares
        old_dn = self.dn_shares
        self.up_shares = real_up
        self.dn_shares = real_dn
        if real_usdc is not None:
            self.usdc = real_usdc

        # Sync CostBasis.total_shares with actual share count
        # This keeps avg_entry_price correct after reconciliation
        if self.up_cost.total_shares > 0 and real_up != old_up:
            if real_up <= 0:
                self.up_cost.reset()
            elif real_up < self.up_cost.total_shares:
                # Shares decreased externally — reduce cost proportionally
                self.up_cost.record_sell(self.up_cost.total_shares - real_up)

        if self.dn_cost.total_shares > 0 and real_dn != old_dn:
            if real_dn <= 0:
                self.dn_cost.reset()
            elif real_dn < self.dn_cost.total_shares:
                # Shares decreased externally — reduce cost proportionally
                self.dn_cost.record_sell(self.dn_cost.total_shares - real_dn)


@dataclass
class PairedInventory:
    """Track paired UP+DN positions for merge-first strategy.

    q_pair = min(up_shares, dn_shares) — guaranteed mergeable
    q_excess_up = up_shares - q_pair — directional risk
    q_excess_dn = dn_shares - q_pair — directional risk

    Merge is free ($0 fee) and returns $1.00 per pair.
    Profit = $1.00 - avg_cost_up_in_pair - avg_cost_dn_in_pair
    """
    q_pair: float = 0.0
    q_excess_up: float = 0.0
    q_excess_dn: float = 0.0
    total_pair_cost: float = 0.0  # Total USDC spent on paired shares
    merged_count: float = 0.0     # Total pairs merged this session
    merged_profit: float = 0.0    # Total profit from merges

    def update(self, up_shares: float, dn_shares: float,
               up_avg_price: float, dn_avg_price: float) -> None:
        """Recalculate paired/excess from current inventory."""
        self.q_pair = min(up_shares, dn_shares)
        self.q_excess_up = max(0.0, up_shares - self.q_pair)
        self.q_excess_dn = max(0.0, dn_shares - self.q_pair)
        if self.q_pair > 0:
            self.total_pair_cost = self.q_pair * (up_avg_price + dn_avg_price)
        else:
            self.total_pair_cost = 0.0

    @property
    def pair_profit_per_unit(self) -> float:
        """Expected profit per merged pair."""
        if self.q_pair <= 0 or self.total_pair_cost <= 0:
            return 0.0
        avg_cost = self.total_pair_cost / self.q_pair
        return 1.0 - avg_cost  # Merge returns $1, cost is avg_cost

    @property
    def expected_merge_profit(self) -> float:
        """Total expected profit from merging all current pairs."""
        return self.q_pair * self.pair_profit_per_unit

    def record_merge(self, amount: float, profit: float) -> None:
        """Record a completed merge."""
        self.merged_count += amount
        self.merged_profit += profit
        self.q_pair = max(0.0, self.q_pair - amount)

    def to_dict(self) -> dict:
        return {
            "q_pair": round(self.q_pair, 4),
            "q_excess_up": round(self.q_excess_up, 4),
            "q_excess_dn": round(self.q_excess_dn, 4),
            "total_pair_cost": round(self.total_pair_cost, 4),
            "pair_profit_per_unit": round(self.pair_profit_per_unit, 6),
            "expected_merge_profit": round(self.expected_merge_profit, 4),
            "merged_count": round(self.merged_count, 4),
            "merged_profit": round(self.merged_profit, 4),
        }


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
    market_type: str = "up_down"      # "up_down" | "above_below"
    resolution_source: str = "binance"  # "binance" | "chainlink" | "unknown"
    min_order_size: float = 5.0  # Default PM minimum
    tick_size: float = 0.01  # Price tick size from PM API (0.1/0.01/0.001/0.0001)

    @property
    def time_remaining(self) -> float:
        """Seconds until window expiry."""
        return max(0.0, self.window_end - time.time())

    @property
    def is_expired(self) -> bool:
        return time.time() >= self.window_end
