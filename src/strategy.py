"""
Combined trading strategy for Polymarket 15m BTC Up/Down binary options.

Strategy logic (evaluated at minute 7 of each period):
1. REVERSAL: If leader flipped between minute 5 and minute 7, buy new leader (if >= 0.60)
2. TREND_CONFIRM: If no flip, buy leading side if >= 0.60 AND trending from minute 4

Backtest results (296 periods, 96h):
  - 214 trades, 86.9% win rate, +4.4c/trade (maker), t-stat = +1.90
  - REVERSAL: 32 trades, 88% win, +14.7c/trade
  - TREND_CONFIRM: 182 trades, 87% win, +2.6c/trade
  - Split-half stable: +4.9c H1, +4.0c H2
"""

import time
from dataclasses import dataclass, field
from enum import Enum


class Signal(Enum):
    NONE = "none"
    TREND_CONFIRM = "trend_confirm"
    REVERSAL = "reversal"


@dataclass
class TradeSignal:
    signal: Signal
    side: str           # "Up" or "Down"
    entry_price: float  # PM price at entry
    entry_minute: float # minute within the period
    period_ts: int      # period start timestamp
    confidence: str     # description of why


@dataclass
class PriceSnapshot:
    """Stores PM Up price at a specific minute within the period."""
    minute: float
    price_up: float
    timestamp: float


@dataclass
class StrategyState:
    """Per-period state for strategy evaluation."""
    period_ts: int = 0
    snapshots: list = field(default_factory=list)
    signal_fired: bool = False
    last_signal: TradeSignal | None = None

    def reset(self, period_ts: int):
        self.period_ts = period_ts
        self.snapshots = []
        self.signal_fired = False
        self.last_signal = None

    def add_snapshot(self, minute: float, price_up: float):
        self.snapshots.append(PriceSnapshot(
            minute=minute,
            price_up=price_up,
            timestamp=time.time(),
        ))

    def get_price_at(self, target_minute: float, tolerance: float = 1.0) -> float | None:
        """Get price closest to target minute, within tolerance."""
        best = None
        best_dist = float("inf")
        for s in self.snapshots:
            dist = abs(s.minute - target_minute)
            if dist < best_dist and dist <= tolerance:
                best_dist = dist
                best = s.price_up
        return best


# ── Strategy Parameters (from backtest optimization) ──

ENTRY_MINUTE = 7.0      # evaluate signal at minute 7
CHECK_MINUTE = 5.0      # reversal check: compare minute 5 vs 7
LOOKBACK_MINUTE = 4.0   # trend confirm: price must be trending since minute 4
THRESHOLD = 0.60        # minimum price to enter
MIN_SNAPSHOT_MINUTE = 3.5   # start collecting snapshots at minute 3.5
MAX_SIGNAL_MINUTE = 8.0     # don't fire signals after minute 8


def evaluate(state: StrategyState) -> TradeSignal | None:
    """
    Evaluate the combined strategy at the current moment.

    Returns a TradeSignal if conditions are met, None otherwise.
    Only fires ONCE per period.
    """
    if state.signal_fired:
        return None

    # Get prices at key minutes
    p4 = state.get_price_at(LOOKBACK_MINUTE)
    p5 = state.get_price_at(CHECK_MINUTE)
    p7 = state.get_price_at(ENTRY_MINUTE)

    if p7 is None:
        return None  # not enough data yet

    # Current minute
    now = time.time()
    current_minute = (now - state.period_ts) / 60.0

    # Only evaluate in the right window
    if current_minute < ENTRY_MINUTE - 0.5 or current_minute > MAX_SIGNAL_MINUTE:
        return None

    # ── REVERSAL CHECK (higher edge signal) ──
    if p5 is not None:
        leader_at_5 = "Up" if p5 > 0.50 else "Down"
        leader_at_7 = "Up" if p7 > 0.50 else "Down"

        if leader_at_5 != leader_at_7:
            # Leader flipped! Buy new leader if above threshold
            if leader_at_7 == "Up" and p7 >= THRESHOLD:
                state.signal_fired = True
                sig = TradeSignal(
                    signal=Signal.REVERSAL,
                    side="Up",
                    entry_price=p7,
                    entry_minute=current_minute,
                    period_ts=state.period_ts,
                    confidence=f"leader flip {leader_at_5}→Up, p5={p5:.3f} p7={p7:.3f}",
                )
                state.last_signal = sig
                return sig

            elif leader_at_7 == "Down" and (1.0 - p7) >= THRESHOLD:
                state.signal_fired = True
                sig = TradeSignal(
                    signal=Signal.REVERSAL,
                    side="Down",
                    entry_price=1.0 - p7,
                    entry_minute=current_minute,
                    period_ts=state.period_ts,
                    confidence=f"leader flip {leader_at_5}→Down, p5={p5:.3f} p7={p7:.3f}",
                )
                state.last_signal = sig
                return sig

    # ── TREND CONFIRMATION (fallback signal) ──
    if p4 is not None:
        price_dn_7 = 1.0 - p7

        # Buy Up: price_up >= threshold AND price_up increased from minute 4
        if p7 >= THRESHOLD and p7 > p4:
            state.signal_fired = True
            sig = TradeSignal(
                signal=Signal.TREND_CONFIRM,
                side="Up",
                entry_price=p7,
                entry_minute=current_minute,
                period_ts=state.period_ts,
                confidence=f"trend confirm Up, p4={p4:.3f} p7={p7:.3f}",
            )
            state.last_signal = sig
            return sig

        # Buy Down: price_dn >= threshold AND price_dn increased (price_up decreased)
        elif price_dn_7 >= THRESHOLD and p7 < p4:
            state.signal_fired = True
            sig = TradeSignal(
                signal=Signal.TREND_CONFIRM,
                side="Down",
                entry_price=price_dn_7,
                entry_minute=current_minute,
                period_ts=state.period_ts,
                confidence=f"trend confirm Down, p4={p4:.3f} p7={p7:.3f}",
            )
            state.last_signal = sig
            return sig

    return None
