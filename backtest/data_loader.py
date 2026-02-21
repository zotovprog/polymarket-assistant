"""Data Loader — load and prepare data for MM backtesting.

Supports:
1. Jon Becker's Polymarket dataset (Parquet)
2. Binance klines from REST API
3. Synthetic data for quick testing
"""
from __future__ import annotations
import os
import time
import logging
from typing import Optional
from pathlib import Path

log = logging.getLogger("backtest.data")

try:
    import pandas as pd
    import numpy as np
except ImportError:
    pd = None
    np = None


class DataLoader:
    """Load and align Polymarket + Binance data for backtesting."""

    def load_polymarket_parquet(self, path: str,
                                market_filter: str = "") -> "pd.DataFrame":
        """Load Polymarket trades from Parquet files.

        Args:
            path: Path to directory containing parquet files
                  (e.g., data/polymarket/trades/)
            market_filter: Optional filter string for market/condition_id

        Returns:
            DataFrame with columns: timestamp, maker, taker,
            maker_amount, taker_amount, fee, token_id
        """
        if pd is None:
            raise ImportError("pandas is required: pip install pandas pyarrow")

        path = Path(path)
        if path.is_file():
            df = pd.read_parquet(path)
        elif path.is_dir():
            files = sorted(path.glob("*.parquet"))
            if not files:
                raise FileNotFoundError(f"No parquet files in {path}")
            dfs = [pd.read_parquet(f) for f in files]
            df = pd.concat(dfs, ignore_index=True)
        else:
            raise FileNotFoundError(f"Path not found: {path}")

        log.info(f"Loaded {len(df)} PM trades from {path}")

        if market_filter and "condition_id" in df.columns:
            df = df[df["condition_id"].str.contains(market_filter, na=False)]
            log.info(f"Filtered to {len(df)} trades for {market_filter}")

        return df

    def load_binance_klines(self, symbol: str = "BTCUSDT",
                            interval: str = "1m",
                            start_ms: Optional[int] = None,
                            end_ms: Optional[int] = None,
                            limit: int = 1000) -> "pd.DataFrame":
        """Load Binance klines from REST API.

        Returns DataFrame with columns: timestamp, open, high, low, close, volume
        """
        if pd is None:
            raise ImportError("pandas is required")

        import requests

        url = "https://api.binance.com/api/v3/klines"
        params = {"symbol": symbol, "interval": interval, "limit": limit}
        if start_ms:
            params["startTime"] = start_ms
        if end_ms:
            params["endTime"] = end_ms

        resp = requests.get(url, params=params, timeout=10)
        resp.raise_for_status()
        data = resp.json()

        df = pd.DataFrame(data, columns=[
            "open_time", "open", "high", "low", "close", "volume",
            "close_time", "quote_volume", "trades", "taker_buy_base",
            "taker_buy_quote", "ignore"
        ])

        for col in ["open", "high", "low", "close", "volume"]:
            df[col] = df[col].astype(float)

        df["timestamp"] = pd.to_datetime(df["open_time"], unit="ms")
        df = df[["timestamp", "open", "high", "low", "close", "volume"]]

        log.info(f"Loaded {len(df)} klines for {symbol} {interval}")
        return df

    def generate_synthetic(self, n_ticks: int = 10000,
                           start_price: float = 100000.0,
                           volatility: float = 0.001,
                           spread_pct: float = 0.02,
                           seed: int = 42) -> "pd.DataFrame":
        """Generate synthetic tick data for quick testing.

        Returns DataFrame with: timestamp, mid_price, best_bid, best_ask,
        trade_price, trade_size, is_buy
        """
        if np is None or pd is None:
            raise ImportError("numpy and pandas required")

        rng = np.random.RandomState(seed)
        prices = [start_price]
        for _ in range(n_ticks - 1):
            ret = rng.normal(0, volatility)
            prices.append(prices[-1] * (1 + ret))

        prices = np.array(prices)
        half_spread = prices * spread_pct / 200  # half of spread_pct

        # Random trades crossing the spread
        is_buy = rng.random(n_ticks) > 0.5
        trade_prices = np.where(is_buy, prices + half_spread, prices - half_spread)
        trade_sizes = rng.exponential(10, n_ticks)

        now = time.time()
        timestamps = [now - (n_ticks - i) * 60 for i in range(n_ticks)]

        df = pd.DataFrame({
            "timestamp": timestamps,
            "mid_price": prices,
            "best_bid": prices - half_spread,
            "best_ask": prices + half_spread,
            "trade_price": trade_prices,
            "trade_size": trade_sizes,
            "is_buy": is_buy,
        })

        log.info(f"Generated {n_ticks} synthetic ticks")
        return df

    def generate_pm_synthetic(self, n_ticks: int = 10000,
                              start_fv: float = 0.50,
                              volatility: float = 0.01,
                              spread_bps: float = 200,
                              trade_size_mean: float = 15.0,
                              seed: int = 42) -> "pd.DataFrame":
        """Generate synthetic PM-like tick data in [0.01, 0.99] price space.

        This simulates a Polymarket binary option token where:
        - mid_price fluctuates around fair value (0.01-0.99)
        - Trades cross the spread (aggressive orders)
        - Trade sizes are in shares (typical PM range)

        Args:
            n_ticks: Number of ticks to generate
            start_fv: Starting fair value (0.01-0.99)
            volatility: Per-tick vol of the token price (e.g. 0.01 = 1%)
            spread_bps: Market spread in bps (200 = 2 cents)
            trade_size_mean: Mean trade size in shares
            seed: Random seed

        Returns DataFrame with: timestamp, mid_price, best_bid, best_ask,
        trade_price, trade_size, is_buy
        """
        if np is None or pd is None:
            raise ImportError("numpy and pandas required")

        rng = np.random.RandomState(seed)

        # Generate price path with mean-reversion around start_fv
        prices = [start_fv]
        for _ in range(n_ticks - 1):
            # Mean-reverting random walk (Ornstein-Uhlenbeck-like)
            reversion = 0.01 * (start_fv - prices[-1])  # Pull toward start
            noise = rng.normal(0, volatility)
            new_price = prices[-1] + reversion + noise
            new_price = max(0.02, min(0.98, new_price))  # Keep in valid range
            prices.append(new_price)

        prices = np.array(prices)
        half_spread = spread_bps / 10000.0 / 2  # Convert bps to price

        bids = np.maximum(0.01, prices - half_spread)
        asks = np.minimum(0.99, prices + half_spread)

        # Trades: aggressive orders cross the spread
        is_buy = rng.random(n_ticks) > 0.5
        # Buy = taker hits the ask, Sell = taker hits the bid
        trade_prices = np.where(is_buy, asks, bids)
        trade_sizes = rng.exponential(trade_size_mean, n_ticks)

        now = time.time()
        timestamps = [now - (n_ticks - i) * 60 for i in range(n_ticks)]

        df = pd.DataFrame({
            "timestamp": timestamps,
            "mid_price": prices,
            "best_bid": bids,
            "best_ask": asks,
            "trade_price": trade_prices,
            "trade_size": trade_sizes,
            "is_buy": is_buy,
        })

        log.info(f"Generated {n_ticks} PM-like ticks (FV={start_fv}, "
                 f"spread={spread_bps}bps, range=[{prices.min():.3f}, {prices.max():.3f}])")
        return df

    def align_data(self, pm_df: "pd.DataFrame",
                   binance_df: "pd.DataFrame") -> "pd.DataFrame":
        """Align Polymarket and Binance data by timestamp.

        Merges on nearest timestamp (asof join).
        """
        if pd is None:
            raise ImportError("pandas is required")

        # Ensure both have datetime timestamps
        if not pd.api.types.is_datetime64_any_dtype(pm_df["timestamp"]):
            pm_df["timestamp"] = pd.to_datetime(pm_df["timestamp"], unit="s")
        if not pd.api.types.is_datetime64_any_dtype(binance_df["timestamp"]):
            binance_df["timestamp"] = pd.to_datetime(binance_df["timestamp"])

        pm_df = pm_df.sort_values("timestamp")
        binance_df = binance_df.sort_values("timestamp")

        merged = pd.merge_asof(
            pm_df, binance_df,
            on="timestamp",
            direction="nearest",
            suffixes=("_pm", "_binance"),
        )

        log.info(f"Aligned {len(merged)} records")
        return merged
