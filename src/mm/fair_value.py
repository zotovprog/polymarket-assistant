"""Fair Value Engine — converts Binance mid-price to binary option fair value.

The key insight: Polymarket crypto markets are binary options.
UP token pays $1 if price > strike at expiry, $0 otherwise.
DN token pays $1 if price <= strike at expiry, $0 otherwise.

Fair value = P(price > strike | current_price, volatility, time_to_expiry)

We use a simplified log-normal model (similar to Black-Scholes digital option)
plus TA signal adjustments.
"""
from __future__ import annotations
import math
from typing import Optional

import indicators as ind


def _norm_cdf(x: float) -> float:
    """Approximation of standard normal CDF (Abramowitz & Stegun)."""
    if x < -10.0:
        return 0.0
    if x > 10.0:
        return 1.0
    # Hart's approximation
    t = 1.0 / (1.0 + 0.2316419 * abs(x))
    d = 0.3989422804014327  # 1/sqrt(2*pi)
    p = d * math.exp(-x * x / 2.0) * (
        t * (0.319381530 + t * (-0.356563782 + t * (1.781477937 +
        t * (-1.821255978 + t * 1.330274429))))
    )
    return 1.0 - p if x > 0 else p


class FairValueEngine:
    """Compute fair value for UP/DN tokens based on Binance data."""

    def __init__(self, vol_window: int = 20, vol_floor: float = 0.0001,
                 vol_cap: float = 0.01, signal_weight: float = 0.03):
        """
        Args:
            vol_window: Number of klines for realized vol calculation
            vol_floor: Minimum per-kline vol (prevents FV collapsing to 0/1)
            vol_cap: Maximum per-kline vol
            signal_weight: Max adjustment from TA signals (±3%)

        Note: vol is per-kline (typically per-minute for 1m klines).
        Typical BTC per-minute vol: 0.0003-0.0006.
        """
        self.vol_window = vol_window
        self.vol_floor = vol_floor
        self.vol_cap = vol_cap
        self.signal_weight = signal_weight
        self._last_vol: float = 0.0005  # fallback vol (typical BTC per-minute)

    def realized_vol(self, klines: list[dict]) -> float:
        """Compute realized volatility from kline closes.
        Returns annualized vol scaled to the kline interval.
        """
        if len(klines) < 2:
            return self._last_vol

        closes = [k["c"] for k in klines[-self.vol_window:]]
        if len(closes) < 2:
            return self._last_vol

        # Log returns
        returns = []
        for i in range(1, len(closes)):
            if closes[i - 1] > 0:
                returns.append(math.log(closes[i] / closes[i - 1]))

        if not returns:
            return self._last_vol

        # Standard deviation of log returns
        mean = sum(returns) / len(returns)
        var = sum((r - mean) ** 2 for r in returns) / len(returns)
        vol = math.sqrt(var)

        # Clamp
        vol = max(self.vol_floor, min(self.vol_cap, vol))
        self._last_vol = vol
        return vol

    def binary_fair_value(self, mid: float, strike: float,
                          time_remaining_sec: float,
                          klines: list[dict]) -> float:
        """P(price > strike at expiry) using log-normal model.

        Args:
            mid: Current Binance mid-price
            strike: Window strike price
            time_remaining_sec: Seconds until window expiry
            klines: Recent klines for vol estimation

        Returns:
            Probability in [0.01, 0.99]
        """
        if mid <= 0 or strike <= 0:
            return 0.5

        # Time to expiry in "kline units" (fraction of window)
        # For 5m window, a kline is 1m, so T = time_remaining / 60
        # We normalize: sigma is per-kline, T is in kline-units
        t_kline = max(time_remaining_sec / 60.0, 0.1)  # in minutes

        sigma = self.realized_vol(klines)

        # d2 for digital option: P(S_T > K) = N(d2)
        # d2 = (ln(S/K) + (r - σ²/2) * T) / (σ * √T)
        # r ≈ 0 for short timeframes
        d2 = (math.log(mid / strike) - 0.5 * sigma * sigma * t_kline) / \
             (sigma * math.sqrt(t_kline) + 1e-10)

        prob = _norm_cdf(d2)
        return max(0.01, min(0.99, prob))

    def adjust_for_signals(self, base_fv: float,
                           bids: list, asks: list, mid: float,
                           trades: list, klines: list) -> float:
        """Adjust fair value based on TA signals.

        Nudges FV by up to ±signal_weight based on:
        - RSI (overbought/oversold)
        - EMA cross direction
        - OBI (order book imbalance)
        - CVD (cumulative volume delta)

        Returns adjusted FV clamped to [0.01, 0.99].
        """
        adj = 0.0
        w = self.signal_weight  # max ±3%

        # RSI
        rsi_v = ind.rsi(klines)
        if rsi_v is not None:
            # RSI 70+ → bearish nudge, RSI 30- → bullish nudge
            if rsi_v > 50:
                adj += (rsi_v - 50) / 50.0 * w * 0.3  # up to +0.9%
            else:
                adj -= (50 - rsi_v) / 50.0 * w * 0.3

        # EMA cross
        es, el = ind.emas(klines)
        if es is not None and el is not None and el > 0:
            separation = (es - el) / el
            ema_adj = max(-1.0, min(1.0, separation / 0.005)) * w * 0.3
            adj += ema_adj

        # OBI
        if mid and bids and asks:
            obi_v = ind.obi(bids, asks, mid)
            adj += obi_v * w * 0.2  # up to ±0.6%

        # CVD 5m
        cvd5 = ind.cvd(trades, 300)
        if cvd5 != 0:
            cvd_adj = max(-1.0, min(1.0, cvd5 / 100000.0)) * w * 0.2
            adj += cvd_adj

        result = base_fv + adj
        return max(0.01, min(0.99, result))

    def compute(self, mid: float, strike: float,
                time_remaining_sec: float,
                klines: list[dict],
                bids: list = None, asks: list = None,
                trades: list = None) -> tuple[float, float]:
        """Full fair value computation.

        Returns (fair_value_up, fair_value_dn).
        """
        base = self.binary_fair_value(mid, strike, time_remaining_sec, klines)

        if bids and asks and trades:
            fv_up = self.adjust_for_signals(base, bids, asks, mid, trades, klines)
        else:
            fv_up = base

        fv_dn = max(0.01, min(0.99, 1.0 - fv_up))
        return fv_up, fv_dn
