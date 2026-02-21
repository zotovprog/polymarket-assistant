#!/usr/bin/env python3
"""Comprehensive backtest runner for the MM strategy.

Runs:
1. Synthetic data (10K ticks, default params)
2. Synthetic data (50K ticks, high vol)
3. Binance BTC 1m live klines → tick conversion
4. Parameter sweep (spread × size grid)
5. HTML + JSON report generation
"""
import sys
import os
import json
import logging
import time
from pathlib import Path

# Add project paths
ROOT = Path(__file__).resolve().parent.parent
SRC = ROOT / "src"
sys.path.insert(0, str(SRC))
sys.path.insert(0, str(ROOT))
os.chdir(ROOT)

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(name)s %(message)s")
log = logging.getLogger("backtest.runner")

import numpy as np
import pandas as pd

from backtest.data_loader import DataLoader
from backtest.simulator import MMSimulator, BacktestResult
from backtest.report import BacktestReport
from mm.mm_config import MMConfig

# Path to real Polymarket data (AiYa1729 dataset)
PARQUET_PATH = str(ROOT / "data" / "polymarket-transactions" /
    "polymarket-transactions-including-negrisk-prepped-for-ml.parquet")

# Target markets for real-data backtesting
MARKET_MEDIUM = "0x9a5b16b21b8fd20f985a2a56d9bcbebf847ec334cdc1b2f5f2c4080b23e5170c"
MARKET_LARGE = "0xe3b1bc389210504ebcb9cffe4b0ed06ccac50561e0f24abb6379984cec030f00"
MARKET_RECENT = "0x0e512a7a83b4e4d3f43b073af887341458922dd4b4929b7e0a4722d6c74a3f01"


def run_synthetic_basic():
    """Scenario 1: PM-like synthetic data, 10K ticks around FV=0.50."""
    log.info("=" * 60)
    log.info("SCENARIO 1: PM synthetic 10K ticks, FV=0.50, default config")
    log.info("=" * 60)

    loader = DataLoader()
    data = loader.generate_pm_synthetic(
        n_ticks=10000, start_fv=0.50,
        volatility=0.008, spread_bps=300,
        trade_size_mean=15.0, seed=42
    )

    config = MMConfig(half_spread_bps=100, order_size_usd=10.0, max_inventory_shares=50.0)
    sim = MMSimulator(config)
    result = sim.run(data, strike=0, window_duration_sec=300)

    report = BacktestReport()
    report.print_summary(result)
    return result


def run_synthetic_high_vol():
    """Scenario 2: Higher volatility PM data, 50K ticks."""
    log.info("=" * 60)
    log.info("SCENARIO 2: PM synthetic 50K ticks, high vol, FV=0.45")
    log.info("=" * 60)

    loader = DataLoader()
    data = loader.generate_pm_synthetic(
        n_ticks=50000, start_fv=0.45,
        volatility=0.015, spread_bps=400,
        trade_size_mean=20.0, seed=123
    )

    config = MMConfig(
        half_spread_bps=100, order_size_usd=10.0,
        max_inventory_shares=30.0, skew_bps_per_unit=10.0
    )
    sim = MMSimulator(config)
    result = sim.run(data, strike=0, window_duration_sec=300)

    report = BacktestReport()
    report.print_summary(result)
    return result


def run_binance_derived():
    """Scenario 3: PM ticks with vol derived from real Binance BTC data."""
    log.info("=" * 60)
    log.info("SCENARIO 3: Binance BTC vol → PM synthetic (4000 ticks)")
    log.info("=" * 60)

    loader = DataLoader()

    # Try to load Binance klines to estimate real BTC vol
    btc_vol = 0.003  # default fallback
    try:
        klines = loader.load_binance_klines(
            symbol="BTCUSDT", interval="1m", limit=1000
        )
        # Estimate per-minute vol from close prices
        closes = klines["close"].values
        returns = np.diff(np.log(closes))
        btc_vol = float(np.std(returns))
        log.info(f"Estimated BTC 1m vol: {btc_vol:.6f} ({btc_vol*100:.3f}%)")
    except Exception as e:
        log.warning(f"Failed to load Binance klines: {e}, using default vol={btc_vol}")

    # Scale BTC vol to PM token vol (PM tokens are more volatile)
    pm_vol = btc_vol * 5  # PM binary tokens amplify underlying vol

    data = loader.generate_pm_synthetic(
        n_ticks=4000, start_fv=0.55,
        volatility=max(pm_vol, 0.003), spread_bps=300,
        trade_size_mean=12.0, seed=42
    )

    config = MMConfig(half_spread_bps=100, order_size_usd=10.0, max_inventory_shares=50.0)
    sim = MMSimulator(config)
    result = sim.run(data, strike=0, window_duration_sec=300)

    report = BacktestReport()
    report.print_summary(result)
    return result


def run_parameter_sweep():
    """Run parameter sweep on synthetic data."""
    log.info("=" * 60)
    log.info("PARAMETER SWEEP")
    log.info("=" * 60)

    loader = DataLoader()
    data = loader.generate_pm_synthetic(
        n_ticks=20000, start_fv=0.50,
        volatility=0.008, spread_bps=200,
        trade_size_mean=15.0, seed=42
    )

    config = MMConfig()
    sim = MMSimulator(config)

    param_grid = {
        "half_spread_bps": [50, 100, 150, 200, 300],
        "order_size_usd": [5, 10, 20],
    }

    results_df = sim.parameter_sweep(data, param_grid, strike=0)

    print("\n" + "=" * 80)
    print("  PARAMETER SWEEP RESULTS (sorted by Sharpe)")
    print("=" * 80)
    print(results_df.to_string(index=False))
    print("=" * 80)

    # Save to JSON
    out_path = ROOT / "backtest" / "sweep_results.json"
    results_df.to_json(str(out_path), orient="records", indent=2)
    log.info(f"Sweep results saved to {out_path}")

    return results_df


def discover_markets():
    """Discover and display top markets from the real dataset."""
    log.info("=" * 60)
    log.info("MARKET DISCOVERY")
    log.info("=" * 60)

    if not os.path.exists(PARQUET_PATH):
        log.warning(f"Real data not found at {PARQUET_PATH}, skipping discovery")
        return None

    loader = DataLoader()
    top = DataLoader.list_markets(PARQUET_PATH, top_n=10)

    print("\n  Top 10 markets by trade count:")
    for idx, row in top.iterrows():
        summary = loader.market_summary(PARQUET_PATH, row['market_id'])
        print(f"    {idx+1:2d}. {row['market_id'][:20]}... "
              f"trades={row['trade_count']:>10,}  "
              f"days={summary.get('duration_days', '?'):>4}  "
              f"vol=${summary.get('total_volume_usd', 0):>12,.0f}  "
              f"end={summary.get('end_price', '?')}")
    print()
    return top


def run_real_market(market_id: str, label: str = "real",
                    max_rows: int = 0):
    """Run backtest on real Polymarket data for a specific market.

    Args:
        market_id: Full market address string
        label: Label for logging
        max_rows: Limit rows (0 = all)

    Returns:
        Tuple of (BacktestResult, DataFrame with validation columns)
    """
    log.info("=" * 60)
    log.info(f"REAL DATA [{label}]: {market_id[:24]}...")
    log.info("=" * 60)

    if not os.path.exists(PARQUET_PATH):
        log.warning(f"Real data not found at {PARQUET_PATH}, skipping")
        return None, None

    loader = DataLoader()

    # Get market summary first
    summary = loader.market_summary(PARQUET_PATH, market_id)
    log.info(f"Market: {summary.get('trade_count', 0)} trades, "
             f"{summary.get('duration_days', 0)} days, "
             f"vol=${summary.get('total_volume_usd', 0):,.0f}, "
             f"end_price={summary.get('end_price', '?')}")

    data = loader.load_real_polymarket(PARQUET_PATH, market_id, max_rows=max_rows)

    log.info(f"Loaded {len(data)} ticks, price range "
             f"[{data['mid_price'].min():.4f}, {data['mid_price'].max():.4f}]")

    config = MMConfig(
        half_spread_bps=100,
        order_size_usd=10.0,
        max_inventory_shares=50.0,
        skew_bps_per_unit=5.0,
    )
    sim = MMSimulator(config)
    result = sim.run(data, strike=0, window_duration_sec=300)

    report = BacktestReport()
    report.print_summary(result)
    return result, data


def validate_vs_actual(result, data, label: str = ""):
    """Compare simulated MM PnL with actual maker PnL from dataset.

    Args:
        result: BacktestResult from simulator
        data: DataFrame with _maker_pnl validation columns
        label: Label for logging
    """
    if result is None or data is None:
        return

    log.info("=" * 60)
    log.info(f"VALIDATION {label}: Simulated vs Actual Maker PnL")
    log.info("=" * 60)

    if '_maker_pnl' not in data.columns:
        log.warning("No _maker_pnl column in data, skipping validation")
        return

    # Actual maker aggregate PnL (per-share pnl * volume)
    actual_maker_pnl = float((data['_maker_pnl'] * data['trade_size']).sum())

    # Our simulated PnL
    sim_pnl = result.total_pnl

    # Market outcome
    end_price = float(data['_end_price'].iloc[-1]) if '_end_price' in data.columns else None

    print(f"\n  --- Validation {label} ---")
    print(f"  Market end price:             {end_price}")
    print(f"  Actual aggregate maker PnL:   ${actual_maker_pnl:>14,.2f}")
    print(f"  Simulated MM PnL:             ${sim_pnl:>14.4f}")
    print(f"  Simulated fill count:         {result.fill_count:>14,}")
    print(f"  Simulated fill rate:          {result.fill_rate*100:>13.1f}%")
    print(f"  Total ticks processed:        {result.n_ticks:>14,}")

    # Directional agreement check
    if actual_maker_pnl != 0 and sim_pnl != 0:
        same_sign = (actual_maker_pnl > 0) == (sim_pnl > 0)
        print(f"  PnL sign agreement:           {'YES' if same_sign else 'NO'}")
    print()


def run_real_parameter_sweep():
    """Run parameter sweep on real market data (medium market, limited rows)."""
    log.info("=" * 60)
    log.info("PARAMETER SWEEP ON REAL DATA")
    log.info("=" * 60)

    if not os.path.exists(PARQUET_PATH):
        log.warning(f"Real data not found at {PARQUET_PATH}, skipping sweep")
        return None

    loader = DataLoader()
    # Use medium market, first 50K rows for speed
    data = loader.load_real_polymarket(PARQUET_PATH, MARKET_MEDIUM, max_rows=50000)

    config = MMConfig()
    sim = MMSimulator(config)

    param_grid = {
        "half_spread_bps": [50, 100, 150, 200],
        "order_size_usd": [5, 10, 20],
    }

    results_df = sim.parameter_sweep(data, param_grid, strike=0)

    print("\n" + "=" * 80)
    print("  REAL DATA PARAMETER SWEEP RESULTS (sorted by Sharpe)")
    print("=" * 80)
    print(results_df.to_string(index=False))
    print("=" * 80)

    # Save to JSON
    out_path = ROOT / "backtest" / "sweep_results_real.json"
    results_df.to_json(str(out_path), orient="records", indent=2)
    log.info(f"Real data sweep results saved to {out_path}")

    return results_df


def main():
    t0 = time.time()
    log.info("Starting comprehensive backtest suite")
    log.info(f"numpy={np.__version__}, pandas={pd.__version__}")

    results = {}

    # === SYNTHETIC SCENARIOS ===
    results["synthetic_basic"] = run_synthetic_basic()
    results["synthetic_high_vol"] = run_synthetic_high_vol()
    results["binance_derived"] = run_binance_derived()

    # Synthetic parameter sweep
    sweep_df = run_parameter_sweep()

    # === REAL DATA SCENARIOS ===
    has_real_data = os.path.exists(PARQUET_PATH)

    if has_real_data:
        # Discover markets
        discover_markets()

        # Medium market (fastest, ~168K trades)
        result_med, data_med = run_real_market(MARKET_MEDIUM, "medium")
        if result_med:
            results["real_medium"] = result_med
            validate_vs_actual(result_med, data_med, "medium")

        # Large market (2.75M trades — may take ~60s)
        result_lg, data_lg = run_real_market(MARKET_LARGE, "large",
                                              max_rows=500000)
        if result_lg:
            results["real_large_500k"] = result_lg
            validate_vs_actual(result_lg, data_lg, "large (500K sample)")

        # Recent market (1.35M trades, Nov 2024 - Jan 2025)
        result_rec, data_rec = run_real_market(MARKET_RECENT, "recent",
                                               max_rows=500000)
        if result_rec:
            results["real_recent_500k"] = result_rec
            validate_vs_actual(result_rec, data_rec, "recent (500K sample)")

        # Real data parameter sweep
        real_sweep_df = run_real_parameter_sweep()
    else:
        log.warning(f"Real data not found at {PARQUET_PATH}")
        log.warning("Skipping real-data scenarios. Download with:")
        log.warning("  huggingface-cli download AiYa1729/polymarket-transactions "
                     "--repo-type dataset --local-dir data/polymarket-transactions")

    # Generate HTML report for best result (highest Sharpe)
    best_key = max(results, key=lambda k: results[k].sharpe_ratio)
    best_result = results[best_key]

    report = BacktestReport()
    html_path = str(ROOT / "backtest" / "report.html")
    report.to_html(best_result, html_path)
    log.info(f"HTML report for best scenario ({best_key}) saved to {html_path}")

    # Also save JSON
    json_path = str(ROOT / "backtest" / "report.json")
    report.to_json(best_result, json_path)

    # Final summary
    elapsed = time.time() - t0
    print("\n" + "=" * 80)
    print("  FINAL SUMMARY — ALL SCENARIOS")
    print("=" * 80)
    print(f"  {'Scenario':30s} {'PnL':>12s} {'Sharpe':>8s} {'Fills':>8s} "
          f"{'MaxDD':>10s} {'FillRate':>9s} {'Ticks':>10s}")
    print("-" * 90)
    for name, r in results.items():
        pnl_color = "+" if r.total_pnl >= 0 else ""
        print(f"  {name:30s} ${pnl_color}{r.total_pnl:>10.4f} "
              f"{r.sharpe_ratio:>8.3f} {r.fill_count:>8,} "
              f"${r.max_drawdown:>9.4f} {r.fill_rate*100:>8.1f}% "
              f"{r.n_ticks:>10,}")
    print("-" * 90)
    print(f"  Best scenario: {best_key}")
    print(f"  Total time: {elapsed:.1f}s")
    print(f"  HTML report: {html_path}")
    print(f"  Sweep results: {ROOT / 'backtest' / 'sweep_results.json'}")
    if has_real_data:
        print(f"  Real sweep:    {ROOT / 'backtest' / 'sweep_results_real.json'}")
    print("=" * 80)


if __name__ == "__main__":
    main()
