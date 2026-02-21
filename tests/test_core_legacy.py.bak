import os
import sys
from types import SimpleNamespace
from unittest.mock import Mock, patch

import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))

import config
import indicators
import trading


@pytest.fixture(autouse=True)
def _stable_indicator_thresholds(monkeypatch):
    """Keep indicator thresholds deterministic for score_trend tests."""
    monkeypatch.setattr(indicators.config, "OBI_BAND_PCT", 0.5, raising=False)
    monkeypatch.setattr(indicators.config, "OBI_THRESH", 0.2, raising=False)
    monkeypatch.setattr(indicators.config, "WALL_MULT", 3.0, raising=False)
    monkeypatch.setattr(indicators.config, "RSI_OB", 70, raising=False)
    monkeypatch.setattr(indicators.config, "RSI_OS", 30, raising=False)


def _sample_market_inputs():
    bids = [(99.9, 1.0), (99.8, 1.0)]
    asks = [(100.1, 1.0), (100.2, 1.0)]
    mid = 100.0
    trades = [{"qty": 1.0, "price": 100.0, "is_buy": True, "t": 0.0}]
    klines = [{"o": 100.0, "h": 101.0, "l": 99.0, "c": 100.0, "v": 10.0}]
    return bids, asks, mid, trades, klines


# ---------------------------------------------------------------------------
# 1) indicators.score_trend
# ---------------------------------------------------------------------------

def test_score_trend_empty_data_returns_neutral():
    score, label, color = indicators.score_trend([], [], 0.0, [], [])

    assert score == 0
    assert label == "NEUTRAL"
    assert color == "yellow"


def test_score_trend_strong_bullish_returns_bullish():
    bids, asks, mid, trades, klines = _sample_market_inputs()

    with (
        patch("indicators.obi", return_value=0.45),
        patch("indicators.cvd", return_value=10_000.0),
        patch("indicators.rsi", return_value=20.0),
        patch("indicators.macd", return_value=(1.0, 0.5, 0.1)),
        patch("indicators.vwap", return_value=99.5),
        patch("indicators.emas", return_value=(101.0, 99.0)),
        patch("indicators.walls", return_value=([(99.9, 10.0), (99.8, 10.0)], [])),
        patch("indicators.heikin_ashi", return_value=[{"green": True}] * 3),
    ):
        score, label, color = indicators.score_trend(bids, asks, mid, trades, klines)

    assert score >= 3
    assert label == "BULLISH"
    assert color == "green"


def test_score_trend_strong_bearish_returns_bearish():
    bids, asks, mid, trades, klines = _sample_market_inputs()

    with (
        patch("indicators.obi", return_value=-0.45),
        patch("indicators.cvd", return_value=-10_000.0),
        patch("indicators.rsi", return_value=80.0),
        patch("indicators.macd", return_value=(-1.0, -0.5, -0.1)),
        patch("indicators.vwap", return_value=100.5),
        patch("indicators.emas", return_value=(99.0, 101.0)),
        patch("indicators.walls", return_value=([], [(100.1, 10.0), (100.2, 10.0)])),
        patch("indicators.heikin_ashi", return_value=[{"green": False}] * 3),
    ):
        score, label, color = indicators.score_trend(bids, asks, mid, trades, klines)

    assert score <= -3
    assert label == "BEARISH"
    assert color == "red"


def test_score_trend_mixed_signals_returns_neutral():
    bids, asks, mid, trades, klines = _sample_market_inputs()

    with (
        patch("indicators.obi", return_value=0.30),
        patch("indicators.cvd", return_value=-5_000.0),
        patch("indicators.rsi", return_value=50.0),
        patch("indicators.macd", return_value=(1.0, 0.5, 0.05)),
        patch("indicators.vwap", return_value=100.5),
        patch("indicators.emas", return_value=(101.0, 99.0)),
        patch("indicators.walls", return_value=([(99.9, 10.0)], [(100.1, 10.0)])),
        patch("indicators.heikin_ashi", return_value=[{"green": False}, {"green": True}, {"green": False}]),
    ):
        score, label, color = indicators.score_trend(bids, asks, mid, trades, klines)

    assert -3 < score < 3
    assert label == "NEUTRAL"
    assert color == "yellow"


def test_score_trend_custom_trend_threshold_is_applied():
    bids, asks, mid, trades, klines = _sample_market_inputs()

    with (
        patch("indicators.obi", return_value=0.35),
        patch("indicators.cvd", return_value=1_000.0),
        patch("indicators.rsi", return_value=None),
        patch("indicators.macd", return_value=(None, None, None)),
        patch("indicators.vwap", return_value=0.0),
        patch("indicators.emas", return_value=(None, None)),
        patch("indicators.walls", return_value=([], [])),
        patch("indicators.heikin_ashi", return_value=[{"green": True}, {"green": True}]),
    ):
        score_default, label_default, _ = indicators.score_trend(
            bids, asks, mid, trades, klines, trend_thresh=3
        )
        score_custom, label_custom, _ = indicators.score_trend(
            bids, asks, mid, trades, klines, trend_thresh=2
        )

    assert score_default == 2
    assert label_default == "NEUTRAL"
    assert score_custom == 2
    assert label_custom == "BULLISH"


# ---------------------------------------------------------------------------
# 2) TradingConfig dynamic sizing via TradingEngine._compute_position_size
# ---------------------------------------------------------------------------

def _compute_size(bias: float, cfg: trading.TradingConfig) -> float:
    fake_engine = SimpleNamespace(cfg=cfg)
    return trading.TradingEngine._compute_position_size(fake_engine, bias)


def test_compute_position_size_dynamic_disabled_returns_fixed_size():
    cfg = trading.TradingConfig(size_usd=13.0, dynamic_sizing_enabled=False)

    assert _compute_size(0.0, cfg) == pytest.approx(cfg.size_usd)
    assert _compute_size(100.0, cfg) == pytest.approx(cfg.size_usd)


def test_compute_position_size_dynamic_enabled_zero_bias_returns_min_size():
    cfg = trading.TradingConfig(
        dynamic_sizing_enabled=True,
        min_size_usd=5.0,
        max_size_usd=25.0,
        sizing_bias_floor=0.0,
        sizing_bias_ceiling=100.0,
    )

    assert _compute_size(0.0, cfg) == pytest.approx(cfg.min_size_usd)


def test_compute_position_size_dynamic_enabled_full_bias_returns_max_size():
    cfg = trading.TradingConfig(
        dynamic_sizing_enabled=True,
        min_size_usd=5.0,
        max_size_usd=25.0,
        sizing_bias_floor=0.0,
        sizing_bias_ceiling=100.0,
    )

    assert _compute_size(100.0, cfg) == pytest.approx(cfg.max_size_usd)


def test_compute_position_size_dynamic_enabled_mid_bias_scales_between_bounds():
    cfg = trading.TradingConfig(
        dynamic_sizing_enabled=True,
        min_size_usd=5.0,
        max_size_usd=25.0,
        sizing_bias_floor=0.0,
        sizing_bias_ceiling=100.0,
    )

    size = _compute_size(50.0, cfg)

    assert cfg.min_size_usd < size < cfg.max_size_usd
    assert size == pytest.approx(15.0)


# ---------------------------------------------------------------------------
# 3) Complete-set arbitrage edge detection (pure helper)
# ---------------------------------------------------------------------------

def _calc_complete_set_edges(pm_up: float, pm_dn: float, taker_fee: float = config.PM_TAKER_FEE):
    combined = pm_up + pm_dn
    gross_edge_pct = (1.0 - combined) * 100.0 if combined < 1.0 else 0.0
    net_edge_pct = (1.0 - combined - 2.0 * taker_fee) * 100.0
    return combined, gross_edge_pct, net_edge_pct


def _is_arb_actionable(
    pm_up: float,
    pm_dn: float,
    min_edge_pct: float = config.PM_ARB_MIN_EDGE_PCT,
    taker_fee: float = config.PM_TAKER_FEE,
):
    combined, _, net_edge_pct = _calc_complete_set_edges(pm_up, pm_dn, taker_fee)
    if combined >= 1.0:
        return False, combined, net_edge_pct
    return net_edge_pct >= min_edge_pct, combined, net_edge_pct


def test_complete_set_edge_exists_for_048_048():
    actionable, combined, net_edge_pct = _is_arb_actionable(0.48, 0.48)
    _, gross_edge_pct, _ = _calc_complete_set_edges(0.48, 0.48)

    assert combined == pytest.approx(0.96)
    assert gross_edge_pct == pytest.approx(4.0)
    assert net_edge_pct == pytest.approx(3.56)
    assert actionable


def test_complete_set_no_edge_for_052_050():
    actionable, combined, net_edge_pct = _is_arb_actionable(0.52, 0.50)
    _, gross_edge_pct, _ = _calc_complete_set_edges(0.52, 0.50)

    assert combined == pytest.approx(1.02)
    assert gross_edge_pct == pytest.approx(0.0)
    assert net_edge_pct == pytest.approx(-2.44)
    assert not actionable


def test_complete_set_edge_049_050_clears_fee_plus_threshold_budget():
    actionable, combined, net_edge_pct = _is_arb_actionable(0.49, 0.50)
    _, gross_edge_pct, _ = _calc_complete_set_edges(0.49, 0.50)
    min_gross_needed = 2 * config.PM_TAKER_FEE * 100 + config.PM_ARB_MIN_EDGE_PCT

    assert combined == pytest.approx(0.99)
    assert gross_edge_pct == pytest.approx(1.0)
    assert min_gross_needed == pytest.approx(0.94)
    assert gross_edge_pct > min_gross_needed
    assert net_edge_pct == pytest.approx(0.56)
    assert actionable


def test_complete_set_edge_below_threshold_after_fees():
    actionable, combined, net_edge_pct = _is_arb_actionable(0.495, 0.500)
    _, gross_edge_pct, _ = _calc_complete_set_edges(0.495, 0.500)

    assert combined == pytest.approx(0.995)
    assert gross_edge_pct == pytest.approx(0.5)
    assert net_edge_pct == pytest.approx(0.06)
    assert not actionable


# ---------------------------------------------------------------------------
# 4) PM depth quality gate (pure helper)
# ---------------------------------------------------------------------------

def _depth_gate_allows_entry(min_depth_usd: float, fetch_depth_usd):
    if min_depth_usd <= 0:
        return True
    try:
        depth_usd = float(fetch_depth_usd())
        return depth_usd >= min_depth_usd
    except Exception:
        # Fail-open semantics from trading.py
        return True


def test_depth_gate_allows_when_depth_meets_minimum():
    assert _depth_gate_allows_entry(10.0, lambda: 10.0)
    assert _depth_gate_allows_entry(10.0, lambda: 12.5)


def test_depth_gate_blocks_when_depth_below_minimum():
    assert not _depth_gate_allows_entry(10.0, lambda: 9.99)


def test_depth_gate_fail_open_on_fetch_error():
    fetch_depth = Mock(side_effect=RuntimeError("depth endpoint unavailable"))

    assert _depth_gate_allows_entry(10.0, fetch_depth)
    fetch_depth.assert_called_once()


# ---------------------------------------------------------------------------
# 5) Execution analytics (SessionStats properties)
# ---------------------------------------------------------------------------

def test_session_stats_execution_analytics_properties():
    stats = trading.SessionStats(
        fill_attempts=8,
        fill_successes=5,
        total_fill_time_ms=1500.0,
        fill_count_timed=5,
        total_slippage_bps=12.0,
        slippage_count=4,
    )

    assert stats.fill_ratio == pytest.approx(0.625)
    assert stats.avg_fill_time_ms == pytest.approx(300.0)
    assert stats.avg_slippage_bps == pytest.approx(3.0)


def test_session_stats_execution_analytics_zero_counts_are_safe():
    stats = trading.SessionStats()

    assert stats.fill_ratio == 0.0
    assert stats.avg_fill_time_ms == 0.0
    assert stats.avg_slippage_bps == 0.0
