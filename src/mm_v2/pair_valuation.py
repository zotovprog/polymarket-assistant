from __future__ import annotations

from dataclasses import dataclass
import time
from types import SimpleNamespace
from typing import Any

from mm_shared.fair_value import FairValueEngine
from mm_shared.market_quality import MarketQualityAnalyzer
from mm_shared.types import MarketInfo

from .config import MMConfigV2
from .types import PairMarketSnapshot


@dataclass
class PairValuationResult:
    fv_up: float
    fv_dn: float
    pair_mid: float
    source: str
    divergence_up: float
    divergence_dn: float
    confidence: float
    regime: str
    pm_age_sec: float


class PairValuationEngine:
    def __init__(self, config: MMConfigV2):
        self.config = config
        self.provider = FairValueEngine(vol_floor=0.0003, signal_weight=0.0)
        adapter = SimpleNamespace(
            order_size_usd=self.config.base_clip_usd,
            min_market_quality_score=self.config.min_market_quality_score,
            min_entry_depth_usd=self.config.min_entry_depth_usd,
            max_entry_spread_bps=self.config.max_entry_spread_bps,
            exit_liquidity_threshold=0.10,
        )
        self.quality = MarketQualityAnalyzer(adapter)

    def _safe_depth(self, book: dict[str, Any], side: str) -> float:
        key = f"{side}_depth_usd"
        try:
            return max(0.0, float(book.get(key, 0.0) or 0.0))
        except Exception:
            return 0.0

    def compute(
        self,
        *,
        market: MarketInfo,
        feed_state: Any,
        up_book: dict[str, Any],
        dn_book: dict[str, Any],
    ) -> tuple[PairValuationResult, PairMarketSnapshot]:
        mid = float(getattr(feed_state, "mid", 0.0) or 0.0)
        klines = list(getattr(feed_state, "klines", []) or [])
        pm_up = getattr(feed_state, "pm_up", None)
        pm_dn = getattr(feed_state, "pm_dn", None)
        pm_age = max(0.0, time.time() - float(getattr(feed_state, "pm_last_update_ts", 0.0) or 0.0))
        fv_up, fv_dn = self.provider.compute_with_pm_anchor(
            mid=mid,
            strike=float(market.strike or 0.0),
            time_remaining_sec=float(market.time_remaining),
            klines=klines,
            pm_up=float(pm_up or 0.0),
            pm_dn=float(pm_dn or 0.0),
            pm_age_sec=pm_age,
            bids=list(getattr(feed_state, "bids", []) or []),
            asks=list(getattr(feed_state, "asks", []) or []),
            trades=list(getattr(feed_state, "trades", []) or []),
        )
        total = max(1e-9, fv_up + fv_dn)
        fv_up /= total
        fv_dn /= total
        quality = self.quality.analyze(up_book, dn_book, fv_up, fv_dn)
        pm_mid_up = float(pm_up) if pm_up is not None else None
        pm_mid_dn = float(pm_dn) if pm_dn is not None else None
        divergence_up = abs(fv_up - pm_mid_up) if pm_mid_up is not None else 0.0
        divergence_dn = abs(fv_dn - pm_mid_dn) if pm_mid_dn is not None else 0.0
        divergence = max(divergence_up, divergence_dn)
        confidence = max(0.0, min(1.0, (quality.overall_score * 0.7) + (1.0 - min(1.0, divergence / 0.20)) * 0.3))
        if not quality.tradeable:
            regime = "illiquid"
        elif divergence > 0.10:
            regime = "divergent"
        else:
            regime = "normal"
        result = PairValuationResult(
            fv_up=fv_up,
            fv_dn=fv_dn,
            pair_mid=fv_up,
            source=self.provider.last_source,
            divergence_up=divergence_up,
            divergence_dn=divergence_dn,
            confidence=confidence,
            regime=regime,
            pm_age_sec=pm_age,
        )
        snapshot = PairMarketSnapshot(
            ts=time.time(),
            market_id=f"{market.coin}_{market.timeframe}_{market.up_token_id[:8]}_{market.dn_token_id[:8]}",
            up_token_id=market.up_token_id,
            dn_token_id=market.dn_token_id,
            time_left_sec=float(market.time_remaining),
            fv_up=fv_up,
            fv_dn=fv_dn,
            fv_confidence=confidence,
            pm_mid_up=pm_mid_up,
            pm_mid_dn=pm_mid_dn,
            up_best_bid=up_book.get("best_bid"),
            up_best_ask=up_book.get("best_ask"),
            dn_best_bid=dn_book.get("best_bid"),
            dn_best_ask=dn_book.get("best_ask"),
            up_bid_depth_usd=self._safe_depth(up_book, "bid"),
            up_ask_depth_usd=self._safe_depth(up_book, "ask"),
            dn_bid_depth_usd=self._safe_depth(dn_book, "bid"),
            dn_ask_depth_usd=self._safe_depth(dn_book, "ask"),
            market_quality_score=quality.overall_score,
            market_tradeable=quality.tradeable,
            divergence_up=divergence_up,
            divergence_dn=divergence_dn,
            valuation_source=result.source,
            valuation_regime=result.regime,
            pm_age_sec=pm_age,
        )
        return result, snapshot
