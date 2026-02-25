"""Dynamic spread model — Avellaneda-Stoikov lite for binary options."""
from __future__ import annotations

import math
from statistics import NormalDist


_SECONDS_PER_YEAR = 365.0 * 24.0 * 60.0 * 60.0
_STD_NORMAL = NormalDist()


class DynamicSpread:
    """Dynamic half-spread model for Polymarket binary options.

    Standard A-S total spread:
      gamma * sigma^2 * T + (2/gamma) * ln(1 + gamma/k)

    This implementation maps that expression to binary-option price units and
    adds:
    - digital delta sensitivity (spikes as T -> 0)
    - fair-value uncertainty weighting (widest near FV=0.5)
    - inventory risk penalty
    """

    def __init__(
        self,
        gamma: float = 0.1,
        k: float = 1.5,
        min_spread_bps: float = 100,
        max_spread_bps: float = 800,
    ):
        if gamma <= 0:
            raise ValueError("gamma must be > 0")
        if k <= 0:
            raise ValueError("k must be > 0")
        if min_spread_bps <= 0:
            raise ValueError("min_spread_bps must be > 0")
        if max_spread_bps <= min_spread_bps:
            raise ValueError("max_spread_bps must be > min_spread_bps")

        self.gamma = float(gamma)
        self.k = float(k)
        self.min_spread_bps = float(min_spread_bps)
        self.max_spread_bps = float(max_spread_bps)

        self._min_spread_price = self.min_spread_bps / 10000.0
        self._max_spread_price = self.max_spread_bps / 10000.0

        # Internal calibration for binary option price space [0, 1].
        self._as_scale = 0.02
        self._delta_weight = 0.08
        self._volatility_weight = 0.45
        self._inventory_bps_per_delta = 7.5
        self._max_asymmetry = 0.45
        self._asymmetry_inventory_scale = 10.0

    @staticmethod
    def _clamp(value: float, low: float, high: float) -> float:
        return max(low, min(high, value))

    @staticmethod
    def _seconds_to_years(T_seconds: float) -> float:
        return max(0.0, float(T_seconds)) / _SECONDS_PER_YEAR

    def _clamp_half_spread(self, half_spread_price: float) -> float:
        return self._clamp(float(half_spread_price), self._min_spread_price, self._max_spread_price)

    def _as_half_spread(self, sigma: float, T_years: float) -> float:
        """Base A-S half-spread before binary-option multipliers."""
        total_spread = (
            self.gamma * (sigma ** 2) * T_years
            + (2.0 / self.gamma) * math.log1p(self.gamma / self.k)
        )
        return 0.5 * total_spread

    def _digital_delta_multiplier(self, sigma: float, T_years: float, fair_value: float) -> float:
        """Digital option delta sensitivity term.

        delta ~= N'(d2) / (sigma * sqrt(T))
        where d2 ~= N^-1(probability). Here fair_value is used as probability.
        """
        sigma_eff = max(float(sigma), 1e-4)
        T_eff = max(float(T_years), 1.0 / _SECONDS_PER_YEAR)  # 1 second floor.
        fv = self._clamp(float(fair_value), 1e-6, 1.0 - 1e-6)

        d2 = _STD_NORMAL.inv_cdf(fv)
        n_prime_d2 = _STD_NORMAL.pdf(d2)
        digital_delta = n_prime_d2 / (sigma_eff * math.sqrt(T_eff))
        return 1.0 + self._delta_weight * math.log1p(digital_delta)

    def _uncertainty_multiplier(self, fair_value: float) -> float:
        """Binary uncertainty is highest near 0.5 and lowest near 0 or 1."""
        fv = self._clamp(float(fair_value), 0.0, 1.0)
        uncertainty = 4.0 * fv * (1.0 - fv)  # [0,1], peaks at FV=0.5.
        return 0.7 + 0.6 * uncertainty

    def _volatility_multiplier(self, sigma: float) -> float:
        """Ensure spread widens monotonically with realized underlying vol."""
        return self._clamp(1.0 + self._volatility_weight * max(0.0, float(sigma)), 1.0, 3.0)

    def _inventory_penalty(self, inventory_delta: float) -> float:
        """Linear inventory risk penalty in price units."""
        penalty_bps = self._inventory_bps_per_delta * abs(float(inventory_delta))
        return penalty_bps / 10000.0

    def compute_half_spread(
        self,
        sigma: float,
        T_seconds: float,
        inventory_delta: float,
        fair_value: float,
    ) -> float:
        """Return symmetric half-spread in price units.

        Terms:
        1) A-S base half-spread
        2) Volatility multiplier (higher sigma -> wider spread)
        3) Digital delta multiplier (near-expiry -> wider spread)
        4) Uncertainty multiplier (FV near 0.5 -> wider spread)
        5) Inventory penalty (large inventory -> wider spread)
        """
        sigma_val = max(0.0, float(sigma))
        T_years = self._seconds_to_years(T_seconds)
        fair_val = self._clamp(float(fair_value), 1e-6, 1.0 - 1e-6)

        # A-S half-spread in "model space", scaled to PM price units.
        base_half_spread = self._as_half_spread(sigma_val, T_years) * self._as_scale

        # Multiplicative risk adjustments.
        vol_mult = self._volatility_multiplier(sigma_val)
        delta_mult = self._digital_delta_multiplier(sigma_val, T_years, fair_val)
        uncertainty_mult = self._uncertainty_multiplier(fair_val)

        # Additive inventory penalty in price units.
        inventory_addon = self._inventory_penalty(inventory_delta)

        raw_half_spread = base_half_spread * vol_mult * delta_mult * uncertainty_mult + inventory_addon
        return self._clamp_half_spread(raw_half_spread)

    def compute_asymmetric_spread(
        self,
        sigma: float,
        T_seconds: float,
        inventory_delta: float,
        fair_value: float,
    ) -> tuple[float, float]:
        """Return (bid_half_spread, ask_half_spread) with inventory asymmetry.

        Positive inventory_delta means long UP, so ask side is tightened to
        encourage selling and bid side is widened to discourage more buying.
        """
        base = self.compute_half_spread(sigma, T_seconds, inventory_delta, fair_value)
        inv = float(inventory_delta)

        if inv == 0.0:
            return base, base

        tilt = self._max_asymmetry * math.tanh(abs(inv) / self._asymmetry_inventory_scale)
        if inv > 0.0:
            bid_half = base * (1.0 + tilt)
            ask_half = base * (1.0 - tilt)
        else:
            bid_half = base * (1.0 - tilt)
            ask_half = base * (1.0 + tilt)

        return self._clamp_half_spread(bid_half), self._clamp_half_spread(ask_half)

    def suggested_config(self, sigma: float, T_seconds: float) -> dict:
        """Suggest regime-aware parameter tuning for gamma/k/spread bounds."""
        sigma_val = max(0.0, float(sigma))
        t_seconds = max(0.0, float(T_seconds))
        t_hours = t_seconds / 3600.0

        near_expiry_mult = 1.35 if t_hours < 1.0 else 1.15 if t_hours < 6.0 else 1.0
        high_vol_mult = 1.35 if sigma_val >= 1.0 else 1.15 if sigma_val >= 0.6 else 1.0

        gamma_rec = max(0.03, self.gamma * near_expiry_mult * high_vol_mult)
        k_rec = max(0.5, self.k / high_vol_mult)

        min_spread_rec = self._clamp(
            self.min_spread_bps * near_expiry_mult * high_vol_mult,
            self.min_spread_bps,
            2000.0,
        )
        max_spread_rec = max(
            min_spread_rec + 50.0,
            self.max_spread_bps * max(near_expiry_mult, high_vol_mult),
        )

        return {
            "gamma": round(gamma_rec, 4),
            "k": round(k_rec, 4),
            "min_spread_bps": round(min_spread_rec, 1),
            "max_spread_bps": round(max_spread_rec, 1),
            "inventory_bps_per_delta": round(self._inventory_bps_per_delta * near_expiry_mult, 2),
            "regime": "near_expiry" if t_hours < 1.0 else "high_vol" if sigma_val >= 1.0 else "normal",
            "preview_half_spread_mid_fv": round(
                self.compute_half_spread(sigma_val, t_seconds, inventory_delta=0.0, fair_value=0.5),
                6,
            ),
        }
