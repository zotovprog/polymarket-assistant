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
    realized_vol_per_min: float = 0.0005


class PairValuationEngine:
    MIDPOINT_FIRST_SHIFT_CAP_ABS = 0.02
    MIDPOINT_FIRST_SHIFT_SPREAD_FRACTION = 0.35

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

    @staticmethod
    def _book_mid(book: dict[str, Any]) -> float | None:
        try:
            best_bid = float(book.get("best_bid") or 0.0)
            best_ask = float(book.get("best_ask") or 0.0)
        except Exception:
            return None
        if best_bid <= 0.0 or best_ask <= 0.0 or best_ask < best_bid:
            return None
        return (best_bid + best_ask) / 2.0

    @classmethod
    def _bounded_market_reference(
        cls,
        *,
        anchor: float,
        model_reference: float,
        best_bid: float | None,
        best_ask: float | None,
    ) -> float:
        spread = None
        try:
            bid = float(best_bid) if best_bid is not None else 0.0
            ask = float(best_ask) if best_ask is not None else 0.0
        except Exception:
            bid = 0.0
            ask = 0.0
        if bid > 0.0 and ask > 0.0 and ask >= bid:
            spread = max(0.0, ask - bid)
        shift_cap = float(cls.MIDPOINT_FIRST_SHIFT_CAP_ABS)
        if spread is not None:
            shift_cap = min(
                float(cls.MIDPOINT_FIRST_SHIFT_CAP_ABS),
                max(0.01, spread * float(cls.MIDPOINT_FIRST_SHIFT_SPREAD_FRACTION)),
            )
        shift = max(-shift_cap, min(shift_cap, float(model_reference) - float(anchor)))
        return max(0.01, min(0.99, float(anchor) + shift))

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
        model_up, model_dn = self.provider.compute(
            mid=mid,
            strike=float(market.strike or 0.0),
            time_remaining_sec=float(market.time_remaining),
            klines=klines,
            bids=list(getattr(feed_state, "bids", []) or []),
            asks=list(getattr(feed_state, "asks", []) or []),
            trades=list(getattr(feed_state, "trades", []) or []),
        )
        total = max(1e-9, model_up + model_dn)
        model_up /= total
        model_dn /= total
        realized_vol_per_min = float(getattr(self.provider, "last_vol", 0.0005) or 0.0005)
        pm_mid_up = float(pm_up) if pm_up is not None else None
        pm_mid_dn = float(pm_dn) if pm_dn is not None else None
        book_mid_up = self._book_mid(up_book)
        book_mid_dn = self._book_mid(dn_book)
        midpoint_anchor_up = book_mid_up
        midpoint_anchor_dn = book_mid_dn
        if midpoint_anchor_up is None and pm_mid_up is not None:
            midpoint_anchor_up = float(pm_mid_up)
        if midpoint_anchor_dn is None and pm_mid_dn is not None:
            midpoint_anchor_dn = float(pm_mid_dn)
        if midpoint_anchor_up is None:
            midpoint_anchor_up = float(model_up)
        if midpoint_anchor_dn is None:
            midpoint_anchor_dn = float(model_dn)

        market_anchor_available = bool(
            (book_mid_up is not None or pm_mid_up is not None)
            and (book_mid_dn is not None or pm_mid_dn is not None)
        )
        preliminary_quality = self.quality.analyze(
            up_book,
            dn_book,
            float(midpoint_anchor_up),
            float(midpoint_anchor_dn),
        )
        if preliminary_quality.tradeable and market_anchor_available:
            fv_up = self._bounded_market_reference(
                anchor=float(midpoint_anchor_up),
                model_reference=float(model_up),
                best_bid=up_book.get("best_bid"),
                best_ask=up_book.get("best_ask"),
            )
            fv_dn = self._bounded_market_reference(
                anchor=float(midpoint_anchor_dn),
                model_reference=float(model_dn),
                best_bid=dn_book.get("best_bid"),
                best_ask=dn_book.get("best_ask"),
            )
            total = max(1e-9, fv_up + fv_dn)
            fv_up /= total
            fv_dn /= total
            if (
                abs(float(fv_up) - float(midpoint_anchor_up)) <= 1e-4
                and abs(float(fv_dn) - float(midpoint_anchor_dn)) <= 1e-4
            ):
                valuation_source = "midpoint_first"
            else:
                valuation_source = "midpoint_bounded_model"
        else:
            fv_up = float(model_up)
            fv_dn = float(model_dn)
            valuation_source = "model_fallback"

        quality = self.quality.analyze(up_book, dn_book, fv_up, fv_dn)
        divergence_up = abs(fv_up - pm_mid_up) if pm_mid_up is not None else abs(fv_up - midpoint_anchor_up)
        divergence_dn = abs(fv_dn - pm_mid_dn) if pm_mid_dn is not None else abs(fv_dn - midpoint_anchor_dn)
        buy_edge_gap_up = float(midpoint_anchor_up) - float(model_up)
        buy_edge_gap_dn = float(midpoint_anchor_dn) - float(model_dn)
        anchor_divergence_up = abs(float(model_up) - float(midpoint_anchor_up))
        anchor_divergence_dn = abs(float(model_dn) - float(midpoint_anchor_dn))
        divergence = max(divergence_up, divergence_dn)
        raw_anchor_divergence = max(anchor_divergence_up, anchor_divergence_dn)
        max_buy_edge_gap = max(0.0, float(buy_edge_gap_up), float(buy_edge_gap_dn))
        confidence = max(
            0.0,
            min(1.0, (quality.overall_score * 0.7) + (1.0 - min(1.0, divergence / 0.20)) * 0.3),
        )
        if valuation_source == "model_fallback":
            regime = "model_fallback"
        elif quality.tradeable and max_buy_edge_gap >= 0.18:
            regime = "toxic_divergence"
        elif raw_anchor_divergence > 0.10:
            regime = "divergent"
        else:
            regime = "normal"
        result = PairValuationResult(
            fv_up=fv_up,
            fv_dn=fv_dn,
            pair_mid=float(midpoint_anchor_up),
            source=valuation_source,
            divergence_up=divergence_up,
            divergence_dn=divergence_dn,
            confidence=confidence,
            regime=regime,
            pm_age_sec=pm_age,
            realized_vol_per_min=realized_vol_per_min,
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
            realized_vol_per_min=realized_vol_per_min,
            midpoint_anchor_up=float(midpoint_anchor_up),
            midpoint_anchor_dn=float(midpoint_anchor_dn),
            model_anchor_up=float(model_up),
            model_anchor_dn=float(model_dn),
            buy_edge_gap_up=float(buy_edge_gap_up),
            buy_edge_gap_dn=float(buy_edge_gap_dn),
            anchor_divergence_up=float(anchor_divergence_up),
            anchor_divergence_dn=float(anchor_divergence_dn),
            quote_anchor_mode="midpoint_first",
            divergence_up=divergence_up,
            divergence_dn=divergence_dn,
            valuation_source=valuation_source,
            valuation_regime=result.regime,
            pm_age_sec=pm_age,
            underlying_mid_price=float(mid),
        )
        return result, snapshot
