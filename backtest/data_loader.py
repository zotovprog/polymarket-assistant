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

    def load_real_polymarket(self, path: str, market_id: str,
                             max_rows: int = 0) -> "pd.DataFrame":
        """Load real Polymarket trades from AiYa1729 Parquet dataset.

        Uses pyarrow predicate pushdown for efficient filtering of the 3.8GB file.

        Args:
            path: Path to the parquet file
            market_id: Full market address string (e.g., '0x9a5b16b2...')
            max_rows: Limit number of rows (0 = no limit)

        Returns:
            DataFrame with columns: timestamp, mid_price, trade_price, trade_size,
            is_buy, best_bid, best_ask, and _maker_pnl, _total_maker_pnl, _end_price
            for validation.
        """
        if pd is None:
            raise ImportError("pandas is required: pip install pandas pyarrow")

        import pyarrow.parquet as pq

        # Columns to load (column pruning for memory efficiency)
        columns = [
            "timeStamp", "price", "fill_price", "volume",
            "taker_side", "maker_price", "taker_price",
            "maker_pnl", "taker_pnl", "total_maker_pnl",
            "tx_value", "end_price", "market"
        ]

        # Predicate pushdown: only read rows matching market_id
        filters = [("market", "=", market_id)]
        log.info(f"Loading market {market_id[:24]}... from {path}")

        table = pq.read_table(path, columns=columns, filters=filters)
        df = table.to_pandas()
        log.info(f"Read {len(df)} rows for market {market_id[:24]}...")

        if len(df) == 0:
            raise ValueError(f"No data found for market_id={market_id}")

        # Sort by timestamp
        df = df.sort_values("timeStamp").reset_index(drop=True)

        # Map columns to simulator schema
        # Convert timestamps to unix float (simulator expects float or numeric)
        ts_unix = df["timeStamp"].astype("int64") / 1e9  # nanoseconds to seconds
        result = pd.DataFrame({
            "timestamp": ts_unix.values,
            "mid_price": df["price"].values,
            "trade_price": df["fill_price"].values,
            "trade_size": df["volume"].values,
            "is_buy": (df["taker_side"] == 1).values,
        })

        # Estimate best_bid / best_ask from maker_price by side
        # When taker_side=1 (buy), the maker was offering the ask
        # When taker_side=0 (sell), the maker was offering the bid
        last_bid = df["maker_price"].where(df["taker_side"] == 0).ffill()
        last_ask = df["maker_price"].where(df["taker_side"] == 1).ffill()
        result["best_bid"] = last_bid.fillna(df["price"] - 0.01).values
        result["best_ask"] = last_ask.fillna(df["price"] + 0.01).values

        # Fix outlier fill_prices: replace values outside [0.001, 0.999] with mid_price
        mask = (result["trade_price"] < 0.001) | (result["trade_price"] > 0.999)
        outlier_count = mask.sum()
        if outlier_count > 0:
            result.loc[mask, "trade_price"] = result.loc[mask, "mid_price"]
            log.info(f"Fixed {outlier_count} outlier fill_price values ({outlier_count/len(result)*100:.2f}%)")

        # Preserve validation columns
        result["_maker_pnl"] = df["maker_pnl"].values
        result["_total_maker_pnl"] = df["total_maker_pnl"].values
        result["_end_price"] = df["end_price"].values
        result["_tx_value"] = df["tx_value"].values
        result["_market"] = df["market"].values

        # Drop rows with NaN in critical columns
        before = len(result)
        result = result.dropna(subset=["mid_price", "trade_price", "trade_size"])
        after = len(result)
        if before != after:
            log.info(f"Dropped {before - after} rows with NaN values")

        # Apply row limit
        if max_rows > 0:
            result = result.head(max_rows).reset_index(drop=True)
            log.info(f"Limited to {max_rows} rows")

        log.info(f"Loaded {len(result)} real PM ticks, "
                 f"price range [{result['mid_price'].min():.4f}, {result['mid_price'].max():.4f}], "
                 f"time span: {result['timestamp'].iloc[0]} to {result['timestamp'].iloc[-1]}")

        return result

    @staticmethod
    def list_markets(path: str, top_n: int = 20) -> "pd.DataFrame":
        """List top markets by trade count from parquet file.

        Uses pyarrow compute for efficient aggregation without loading full dataset.

        Args:
            path: Path to the parquet file
            top_n: Number of top markets to return

        Returns:
            DataFrame with columns: market_id, trade_count
        """
        if pd is None:
            raise ImportError("pandas is required")

        import pyarrow.parquet as pq
        import pyarrow.compute as pc

        log.info(f"Scanning markets in {path}...")
        table = pq.read_table(path, columns=["market"])
        vc = pc.value_counts(table.column("market"))

        records = []
        for entry in vc:
            py = entry.as_py()
            records.append((py["values"], py["counts"]))

        records.sort(key=lambda x: x[1], reverse=True)

        df = pd.DataFrame(records[:top_n], columns=["market_id", "trade_count"])
        log.info(f"Found {len(records)} unique markets, showing top {top_n}")
        return df

    def market_summary(self, path: str, market_id: str) -> dict:
        """Get summary statistics for a specific market.

        Args:
            path: Path to the parquet file
            market_id: Market address string

        Returns:
            Dict with market stats (trade_count, dates, prices, volume, etc.)
        """
        if pd is None:
            raise ImportError("pandas is required")

        import pyarrow.parquet as pq

        cols = ["timeStamp", "price", "volume", "taker_side", "tx_value", "end_price"]
        filters = [("market", "=", market_id)]
        table = pq.read_table(path, columns=cols, filters=filters)
        df = table.to_pandas()

        if len(df) == 0:
            return {"market_id": market_id, "trade_count": 0}

        return {
            "market_id": market_id,
            "trade_count": len(df),
            "time_start": str(df["timeStamp"].min()),
            "time_end": str(df["timeStamp"].max()),
            "duration_days": (df["timeStamp"].max() - df["timeStamp"].min()).days,
            "price_min": float(df["price"].min()),
            "price_max": float(df["price"].max()),
            "price_mean": float(df["price"].mean()),
            "total_volume_usd": float(df["tx_value"].sum()),
            "avg_trade_size": float(df["volume"].mean()),
            "buy_ratio": float((df["taker_side"] == 1).mean()),
            "end_price": float(df["end_price"].iloc[-1]) if not df["end_price"].isna().all() else None,
        }

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
