"""Market quality analysis for smart entry/exit decisions.

Evaluates order book depth, spread, and liquidity to determine
whether a market is suitable for market-making.
"""
from __future__ import annotations

import logging
from dataclasses import dataclass, field

log = logging.getLogger("mm.quality")


@dataclass
class MarketQuality:
    """Result of market quality analysis."""
    bid_depth_usd: float = 0.0
    ask_depth_usd: float = 0.0
    spread_bps: float = 0.0
    num_bid_levels: int = 0
    num_ask_levels: int = 0
    liquidity_score: float = 0.0   # 0-1
    spread_score: float = 0.0      # 0-1
    overall_score: float = 0.0     # 0-1
    tradeable: bool = False
    reason: str = ""


class MarketQualityAnalyzer:
    """Analyze order books to score market quality."""

    def __init__(self, config):
        """
        Args:
            config: MMConfig instance (needs order_size_usd,
                    min_market_quality_score, min_entry_depth_usd,
                    max_entry_spread_bps, exit_liquidity_threshold)
        """
        self.config = config

    def analyze(self, up_book: dict, dn_book: dict,
                fv_up: float, fv_dn: float) -> MarketQuality:
        """Analyze both order books and return quality assessment.

        Args:
            up_book: Full book dict from get_full_book() for UP token
            dn_book: Full book dict from get_full_book() for DN token
            fv_up: Fair value for UP token
            fv_dn: Fair value for DN token

        Returns:
            MarketQuality with scores and tradeable flag
        """
        mq = MarketQuality()

        # Aggregate depth across both books
        up_bid_depth = up_book.get("bid_depth_usd", 0.0)
        up_ask_depth = up_book.get("ask_depth_usd", 0.0)
        dn_bid_depth = dn_book.get("bid_depth_usd", 0.0)
        dn_ask_depth = dn_book.get("ask_depth_usd", 0.0)

        mq.bid_depth_usd = up_bid_depth + dn_bid_depth
        mq.ask_depth_usd = up_ask_depth + dn_ask_depth
        mq.num_bid_levels = (up_book.get("num_bids", 0)
                             + dn_book.get("num_bids", 0))
        mq.num_ask_levels = (up_book.get("num_asks", 0)
                             + dn_book.get("num_asks", 0))

        # Spread: use UP book as primary (it's the main traded side)
        best_bid = up_book.get("best_bid")
        best_ask = up_book.get("best_ask")
        if best_bid and best_ask and best_ask > 0:
            mq.spread_bps = (best_ask - best_bid) / best_ask * 10000
        else:
            mq.spread_bps = 10000.0  # Max spread if no book

        # Spread score: <200bps = 1.0, >1000bps = 0.0, linear between
        if mq.spread_bps <= 200:
            mq.spread_score = 1.0
        elif mq.spread_bps >= 1000:
            mq.spread_score = 0.0
        else:
            mq.spread_score = 1.0 - (mq.spread_bps - 200) / 800

        # Liquidity score: total depth vs 3x our order size
        required_depth = self.config.order_size_usd * 3
        total_depth = mq.bid_depth_usd + mq.ask_depth_usd
        if required_depth > 0:
            mq.liquidity_score = min(1.0, total_depth / required_depth)
        else:
            mq.liquidity_score = 1.0

        # Overall: weighted combination
        mq.overall_score = 0.4 * mq.spread_score + 0.6 * mq.liquidity_score

        # Tradeable check
        reasons = []
        if mq.overall_score < self.config.min_market_quality_score:
            reasons.append(
                f"score {mq.overall_score:.2f} < {self.config.min_market_quality_score}")
        if total_depth < self.config.min_entry_depth_usd:
            reasons.append(
                f"depth ${total_depth:.0f} < ${self.config.min_entry_depth_usd:.0f}")
        if mq.spread_bps > self.config.max_entry_spread_bps:
            reasons.append(
                f"spread {mq.spread_bps:.0f}bps > {self.config.max_entry_spread_bps:.0f}bps")

        if reasons:
            mq.tradeable = False
            mq.reason = "; ".join(reasons)
        else:
            mq.tradeable = True
            mq.reason = "OK"

        return mq

    def check_exit_conditions(self, up_book: dict, dn_book: dict,
                              fv_up: float, fv_dn: float,
                              inventory) -> tuple[bool, str]:
        """Check whether we should exit early due to deteriorating conditions.

        Args:
            up_book, dn_book: Full book dicts
            fv_up, fv_dn: Fair values
            inventory: Current Inventory object

        Returns:
            (should_exit, reason) tuple
        """
        quality = self.analyze(up_book, dn_book, fv_up, fv_dn)

        has_inventory = (inventory.up_shares > 0.5 or inventory.dn_shares > 0.5)

        if (has_inventory
                and quality.liquidity_score < self.config.exit_liquidity_threshold):
            return True, (
                f"Liquidity dried up: score={quality.liquidity_score:.2f} "
                f"< {self.config.exit_liquidity_threshold}, "
                f"depth=${quality.bid_depth_usd + quality.ask_depth_usd:.0f}"
            )

        return False, ""
