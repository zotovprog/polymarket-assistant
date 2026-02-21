"""MM Strategy Simulator — tick-by-tick backtesting of market making.

Simulates:
1. Fair value computation at each tick
2. Quote generation (bid/ask)
3. Fill simulation (when incoming trades cross our quotes)
4. Inventory tracking and PnL computation
5. Risk limit checks

Does NOT simulate:
- Network latency
- Order book queue priority
- Partial fills
"""
from __future__ import annotations
import logging
import time
from dataclasses import dataclass, field
from typing import Optional

log = logging.getLogger("backtest.sim")

try:
    import pandas as pd
    import numpy as np
except ImportError:
    pd = None
    np = None

import sys
from pathlib import Path
SRC_DIR = Path(__file__).resolve().parent.parent / "src"
if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))

from mm.mm_config import MMConfig
from mm.types import Quote, Fill, Inventory
from mm.quote_engine import QuoteEngine


@dataclass
class BacktestResult:
    """Results from a backtest run."""
    # PnL
    realized_pnl: float = 0.0
    unrealized_pnl: float = 0.0
    total_pnl: float = 0.0
    total_fees: float = 0.0

    # Stats
    total_volume: float = 0.0
    fill_count: int = 0
    quote_count: int = 0
    avg_spread_bps: float = 0.0
    fill_rate: float = 0.0  # fills / quotes

    # Risk
    max_drawdown: float = 0.0
    max_inventory: float = 0.0
    sharpe_ratio: float = 0.0

    # Time series
    pnl_series: list = field(default_factory=list)
    inventory_series: list = field(default_factory=list)
    spread_series: list = field(default_factory=list)
    price_series: list = field(default_factory=list)

    # Config used
    config: dict = field(default_factory=dict)

    # Duration
    duration_sec: float = 0.0
    n_ticks: int = 0


class MMSimulator:
    """Simulate market making strategy on historical data."""

    def __init__(self, config: MMConfig):
        self.config = config
        self.quote_engine = QuoteEngine(config)

    def run(self, data: "pd.DataFrame",
            strike: float = 0.0,
            window_duration_sec: float = 300) -> BacktestResult:
        """Run backtest on tick data.

        Args:
            data: DataFrame with columns:
                - timestamp (unix or datetime)
                - mid_price (Binance mid)
                - trade_price (actual trade price)
                - trade_size (trade size)
                - is_buy (True if buyer-initiated)
                Optional:
                - best_bid, best_ask (PM orderbook)
            strike: Strike price for binary option FV calc
                    (0 = use mid_price, auto-detect windows)
            window_duration_sec: Window length for time-remaining calc

        Returns:
            BacktestResult with all metrics
        """
        if pd is None or np is None:
            raise ImportError("pandas and numpy required")

        start_time = time.time()
        result = BacktestResult()
        result.config = self.config.to_dict()
        result.n_ticks = len(data)

        inventory = Inventory()
        fills = []
        pnl_curve = []
        running_pnl = 0.0
        peak_pnl = 0.0
        max_dd = 0.0
        max_inv = 0.0
        spread_samples = []

        # Current quotes
        cur_bid: Optional[Quote] = None
        cur_ask: Optional[Quote] = None

        # Volatility estimation
        returns = []
        vol_window = 20

        for i, row in data.iterrows():
            mid = row["mid_price"]
            trade_price = row.get("trade_price", mid)
            trade_size = row.get("trade_size", 0)
            is_buy = row.get("is_buy", True)

            if mid <= 0:
                continue

            # Compute log return for vol
            if len(result.price_series) > 0:
                prev = result.price_series[-1]
                if prev > 0:
                    returns.append(np.log(mid / prev))

            result.price_series.append(mid)

            # Estimate volatility
            vol = 0.02
            if len(returns) >= vol_window:
                recent = returns[-vol_window:]
                vol = max(0.005, float(np.std(recent)))

            # Fair value (simplified for backtest)
            if strike > 0:
                # Binary option FV
                time_frac = max(0.1, window_duration_sec / 60.0)
                d2 = (np.log(mid / strike) - 0.5 * vol**2 * time_frac) / \
                     (vol * np.sqrt(time_frac) + 1e-10)
                from mm.fair_value import _norm_cdf
                fv = _norm_cdf(d2)
                fv = max(0.05, min(0.95, fv))
            else:
                fv = 0.5  # Use 0.5 for non-binary backtests

            # Generate quotes
            bid, ask = self.quote_engine.generate_quotes(
                fv, "backtest-token", inventory, vol, vol)
            result.quote_count += 1

            spread_bps = (ask.price - bid.price) / max(ask.price, 0.01) * 10000
            spread_samples.append(spread_bps)

            # Check if incoming trade fills our quotes
            if trade_size > 0:
                # Buy trade crosses our ask
                if is_buy and trade_price >= ask.price and ask.price > 0:
                    fill_size = min(trade_size, ask.size)
                    fill = Fill(
                        ts=float(row.get("timestamp", 0)),
                        side="SELL", token_id="backtest",
                        price=ask.price, size=fill_size,
                        fee=0.0, is_maker=True,
                    )
                    fills.append(fill)
                    inventory.up_shares -= fill_size
                    running_pnl += fill.notional
                    result.fill_count += 1
                    result.total_volume += fill.notional

                # Sell trade crosses our bid
                elif not is_buy and trade_price <= bid.price and bid.price > 0:
                    fill_size = min(trade_size, bid.size)
                    fill = Fill(
                        ts=float(row.get("timestamp", 0)),
                        side="BUY", token_id="backtest",
                        price=bid.price, size=fill_size,
                        fee=0.0, is_maker=True,
                    )
                    fills.append(fill)
                    inventory.up_shares += fill_size
                    running_pnl -= fill.notional
                    result.fill_count += 1
                    result.total_volume += fill.notional

            # Mark-to-market
            unrealized = inventory.up_shares * fv
            total_pnl = running_pnl + unrealized
            pnl_curve.append(total_pnl)

            # Track max drawdown
            peak_pnl = max(peak_pnl, total_pnl)
            dd = peak_pnl - total_pnl
            max_dd = max(max_dd, dd)

            # Track max inventory
            max_inv = max(max_inv, abs(inventory.up_shares))

            # Record series
            result.pnl_series.append(total_pnl)
            result.inventory_series.append(inventory.up_shares)
            result.spread_series.append(spread_bps)

        # Final metrics
        result.realized_pnl = running_pnl
        result.unrealized_pnl = inventory.up_shares * 0.5  # mark at 0.5
        result.total_pnl = result.realized_pnl + result.unrealized_pnl
        result.max_drawdown = max_dd
        result.max_inventory = max_inv
        result.avg_spread_bps = float(np.mean(spread_samples)) if spread_samples else 0
        result.fill_rate = result.fill_count / max(result.quote_count, 1)

        # Sharpe ratio (from PnL changes)
        if len(pnl_curve) > 1:
            pnl_changes = np.diff(pnl_curve)
            if np.std(pnl_changes) > 0:
                result.sharpe_ratio = float(
                    np.mean(pnl_changes) / np.std(pnl_changes) * np.sqrt(252 * 24 * 60)
                )

        result.duration_sec = time.time() - start_time
        log.info(f"Backtest complete: {result.n_ticks} ticks, "
                 f"{result.fill_count} fills, "
                 f"PnL=${result.total_pnl:.4f}, "
                 f"Sharpe={result.sharpe_ratio:.2f}, "
                 f"MaxDD=${result.max_drawdown:.4f}")

        return result

    def parameter_sweep(self, data: "pd.DataFrame",
                        param_grid: dict,
                        strike: float = 0.0) -> "pd.DataFrame":
        """Run backtest over a grid of parameters.

        Args:
            data: Tick data DataFrame
            param_grid: Dict of param_name -> [values]
                e.g. {"half_spread_bps": [100, 150, 200],
                       "order_size_usd": [5, 10, 20]}
            strike: Strike price

        Returns:
            DataFrame with results for each parameter combination
        """
        if pd is None:
            raise ImportError("pandas required")

        import itertools

        keys = list(param_grid.keys())
        values = list(param_grid.values())
        combos = list(itertools.product(*values))

        results = []
        total = len(combos)

        for i, combo in enumerate(combos):
            params = dict(zip(keys, combo))
            cfg = MMConfig(**{**self.config.to_dict(), **params})
            sim = MMSimulator(cfg)
            result = sim.run(data, strike=strike)

            row = {**params,
                   "total_pnl": result.total_pnl,
                   "sharpe_ratio": result.sharpe_ratio,
                   "max_drawdown": result.max_drawdown,
                   "fill_count": result.fill_count,
                   "fill_rate": result.fill_rate,
                   "avg_spread_bps": result.avg_spread_bps,
                   "total_volume": result.total_volume,
                   "max_inventory": result.max_inventory}
            results.append(row)

            if (i + 1) % 10 == 0 or i == total - 1:
                log.info(f"Sweep progress: {i+1}/{total}")

        df = pd.DataFrame(results)
        df = df.sort_values("sharpe_ratio", ascending=False)
        log.info(f"Sweep complete: {total} combinations tested")
        return df
