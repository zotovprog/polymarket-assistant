import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'src'))
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

import pytest
from backtest.data_loader import DataLoader
from backtest.report import BacktestReport
from backtest.simulator import BacktestResult, MMSimulator
from mm.mm_config import MMConfig


def _run_pm_backtest(
    *,
    n_ticks: int = 5000,
    seed: int = 42,
    volatility: float = 0.01,
    spread_bps: float = 200,
    trade_size_mean: float = 15.0,
    **config_overrides,
):
    loader = DataLoader()
    data = loader.generate_pm_synthetic(
        n_ticks=n_ticks,
        seed=seed,
        volatility=volatility,
        spread_bps=spread_bps,
        trade_size_mean=trade_size_mean,
    )

    config_kwargs = {
        "half_spread_bps": 100.0,
        "order_size_usd": 10.0,
        "max_inventory_shares": 50.0,
        "skew_bps_per_unit": 5.0,
    }
    config_kwargs.update(config_overrides)

    config = MMConfig(**config_kwargs)
    sim = MMSimulator(config)
    result = sim.run(data, strike=0, window_duration_sec=300)
    return data, result


def test_synthetic_basic_runs() -> None:
    _, result = _run_pm_backtest(n_ticks=5000, seed=42)

    assert result.fill_count > 0
    assert result.total_pnl is not None


def test_inventory_within_limit() -> None:
    _, result = _run_pm_backtest(n_ticks=3000, max_inventory_shares=20.0)

    assert result.max_inventory <= 20.0


def test_pnl_calculation() -> None:
    _, result = _run_pm_backtest(n_ticks=3000, seed=7)

    pnl_gap = abs(result.total_pnl - (result.realized_pnl + result.unrealized_pnl))
    assert pnl_gap < 0.001


def test_synthetic_data_columns() -> None:
    data = DataLoader().generate_pm_synthetic(n_ticks=500, seed=11)

    required_cols = {
        "timestamp",
        "mid_price",
        "trade_price",
        "trade_size",
        "is_buy",
        "best_bid",
        "best_ask",
    }
    assert required_cols.issubset(set(data.columns))


def test_parameter_sweep() -> None:
    loader = DataLoader()
    data = loader.generate_pm_synthetic(n_ticks=1000, seed=42)

    sim = MMSimulator(MMConfig(half_spread_bps=100.0, order_size_usd=10.0))
    results_df = sim.parameter_sweep(
        data,
        param_grid={
            "half_spread_bps": [50.0, 100.0],
            "order_size_usd": [5.0, 10.0],
        },
        strike=0,
    )
    results = results_df.to_dict(orient="records")

    assert isinstance(results, list)
    assert len(results) == 4


def test_report_summary() -> None:
    _, result = _run_pm_backtest(n_ticks=2000, seed=42)
    report = BacktestReport()
    summary = report.summary(result)

    for key in ["total_pnl", "sharpe_ratio", "fill_count", "max_inventory"]:
        assert key in summary


def test_high_vol_scenario() -> None:
    _, basic_result = _run_pm_backtest(n_ticks=5000, seed=42, volatility=0.005)
    _, high_vol_result = _run_pm_backtest(n_ticks=5000, seed=42, volatility=0.02)

    assert high_vol_result.sharpe_ratio < basic_result.sharpe_ratio


def test_zero_fills_extreme_spread() -> None:
    _, result = _run_pm_backtest(
        n_ticks=3000,
        seed=42,
        half_spread_bps=5000.0,
        max_spread_bps=5000.0,
    )

    assert result.fill_count <= 5
