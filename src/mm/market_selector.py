"""Market selection engine — pick the best coin+timeframe for MM."""

from __future__ import annotations

from dataclasses import dataclass
import math
from typing import Any


@dataclass(frozen=True)
class MarketScore:
    coin: str
    timeframe: str
    spread_score: float
    depth_score: float
    volume_score: float
    fee_score: float
    volatility_score: float
    total_score: float
    recommendation: str  # "strong", "moderate", "weak", "skip"


class MarketSelector:
    WEIGHTS = {
        "spread": 0.3,
        "depth": 0.2,
        "volume": 0.25,
        "fee": 0.15,
        "volatility": 0.1,
    }

    MIN_RECOMMEND_SCORE = 0.4
    MAX_FEE_RATE = 0.25**2  # max of (p * (1 - p))**2 at p = 0.5

    @staticmethod
    def _clamp(value: float, lo: float = 0.0, hi: float = 1.0) -> float:
        return max(lo, min(hi, value))

    @staticmethod
    def _recommendation(score: float) -> str:
        if score >= 0.7:
            return "strong"
        if score >= 0.55:
            return "moderate"
        if score >= 0.4:
            return "weak"
        return "skip"

    def _component_scores(
        self,
        spread_bps: float,
        depth_usd: float,
        volume_24h: float,
        avg_price: float,
        volatility: float,
    ) -> dict[str, float]:
        spread = max(0.0, float(spread_bps))
        depth = max(0.0, float(depth_usd))
        volume = max(0.0, float(volume_24h))
        price = self._clamp(float(avg_price), 0.0, 1.0)
        vol = max(0.0, float(volatility))

        spread_score = math.exp(-((spread - 500.0) / 300.0) ** 2)
        depth_score = 1.0 / (1.0 + math.exp((500.0 - depth) / 200.0))
        volume_score = min(1.0, math.log1p(volume) / math.log1p(10_000.0))

        fee_rate = (price * (1.0 - price)) ** 2
        fee_score = self._clamp(1.0 - (fee_rate / self.MAX_FEE_RATE))

        volatility_score = math.exp(-((vol - 1.5) / 1.0) ** 2)

        return {
            "spread": self._clamp(spread_score),
            "depth": self._clamp(depth_score),
            "volume": self._clamp(volume_score),
            "fee": self._clamp(fee_score),
            "volatility": self._clamp(volatility_score),
        }

    def score_market(
        self,
        spread_bps: float,
        depth_usd: float,
        volume_24h: float,
        avg_price: float,
        volatility: float,
    ) -> float:
        """Return weighted market quality score in [0, 1]."""
        scores = self._component_scores(
            spread_bps=spread_bps,
            depth_usd=depth_usd,
            volume_24h=volume_24h,
            avg_price=avg_price,
            volatility=volatility,
        )
        total = (
            self.WEIGHTS["spread"] * scores["spread"]
            + self.WEIGHTS["depth"] * scores["depth"]
            + self.WEIGHTS["volume"] * scores["volume"]
            + self.WEIGHTS["fee"] * scores["fee"]
            + self.WEIGHTS["volatility"] * scores["volatility"]
        )
        return self._clamp(total)

    def rank_markets(self, markets: list[dict[str, Any]]) -> list[dict[str, Any]]:
        """
        Score and rank markets descending by attractiveness.

        Expected market keys: coin, timeframe, spread_bps, depth_usd, volume_24h,
        avg_price, volatility.
        """
        ranked: list[dict[str, Any]] = []
        for market in markets:
            spread_bps = float(market.get("spread_bps", 0.0))
            depth_usd = float(market.get("depth_usd", 0.0))
            volume_24h = float(market.get("volume_24h", 0.0))
            avg_price = float(market.get("avg_price", 0.5))
            volatility = float(market.get("volatility", 0.0))

            component_scores = self._component_scores(
                spread_bps=spread_bps,
                depth_usd=depth_usd,
                volume_24h=volume_24h,
                avg_price=avg_price,
                volatility=volatility,
            )
            score = self.score_market(
                spread_bps=spread_bps,
                depth_usd=depth_usd,
                volume_24h=volume_24h,
                avg_price=avg_price,
                volatility=volatility,
            )

            enriched = dict(market)
            enriched["score"] = score
            enriched["recommendation"] = self._recommendation(score)
            enriched["spread_score"] = component_scores["spread"]
            enriched["depth_score"] = component_scores["depth"]
            enriched["volume_score"] = component_scores["volume"]
            enriched["fee_score"] = component_scores["fee"]
            enriched["volatility_score"] = component_scores["volatility"]
            ranked.append(enriched)

        ranked.sort(key=lambda item: item["score"], reverse=True)
        return ranked

    def recommend(self, markets: list[dict[str, Any]]) -> dict[str, Any] | None:
        """Return best market if score meets threshold, otherwise None."""
        ranked = self.rank_markets(markets)
        if not ranked:
            return None
        best = ranked[0]
        if float(best.get("score", 0.0)) < self.MIN_RECOMMEND_SCORE:
            return None
        return best
