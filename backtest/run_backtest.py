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


def main():
    t0 = time.time()
    log.info("Starting comprehensive backtest suite")
    log.info(f"numpy={np.__version__}, pandas={pd.__version__}")

    # Run scenarios
    results = {}

    results["synthetic_basic"] = run_synthetic_basic()
    results["synthetic_high_vol"] = run_synthetic_high_vol()
    results["binance_derived"] = run_binance_derived()

    # Parameter sweep
    sweep_df = run_parameter_sweep()

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
    print("\n" + "=" * 60)
    print("  FINAL SUMMARY")
    print("=" * 60)
    for name, r in results.items():
        print(f"  {name:25s}: PnL=${r.total_pnl:>10.4f}  Sharpe={r.sharpe_ratio:>7.3f}  "
              f"Fills={r.fill_count:>6d}  MaxDD=${r.max_drawdown:>8.4f}  "
              f"FillRate={r.fill_rate*100:.1f}%")
    print(f"\n  Best scenario: {best_key}")
    print(f"  Total time: {elapsed:.1f}s")
    print(f"  HTML report: {html_path}")
    print(f"  Sweep results: {ROOT / 'backtest' / 'sweep_results.json'}")
    print("=" * 60)


if __name__ == "__main__":
    main()
