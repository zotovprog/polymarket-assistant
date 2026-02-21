"""Quote Engine — generates bid/ask quotes around fair value.

Core logic:
  bid = fair_value - half_spread - inventory_skew
  ask = fair_value + half_spread - inventory_skew

Spread is widened during high volatility.
Inventory skew pushes quotes to reduce net delta.
Prices are rounded to PM's 0.01 increment.
"""
from __future__ import annotations
import math
from .types import Quote, Inventory
from .mm_config import MMConfig


def _round_price(price: float) -> float:
    """Round to Polymarket's cent increment, clamp to [0.01, 0.99]."""
    return max(0.01, min(0.99, round(price, 2)))


def _bps_to_price(bps: float) -> float:
    """Convert basis points to price delta (100 bps = 0.01 in PM price)."""
    return bps / 10000.0


class QuoteEngine:
    """Generate bid/ask quotes for UP and DN tokens."""

    def __init__(self, config: MMConfig):
        self.config = config

    def _effective_half_spread(self, volatility: float,
                                avg_vol: float) -> float:
        """Compute half-spread, widening in high-vol regime."""
        base = self.config.half_spread_bps

        if avg_vol > 0 and volatility > avg_vol:
            vol_ratio = volatility / avg_vol
            if vol_ratio > self.config.volatility_pause_mult:
                # Very high vol — use max spread
                return self.config.max_spread_bps
            # Linear widening between 1x and pause_mult
            mult = 1.0 + (vol_ratio - 1.0) * (self.config.vol_spread_mult - 1.0)
            base *= mult

        return max(self.config.min_spread_bps,
                   min(self.config.max_spread_bps, base))

    def _inventory_skew(self, inventory: Inventory) -> float:
        """Compute price skew based on net delta.

        Positive delta (long UP) → skew negative → lower bid/ask
        to encourage selling UP / buying DN.
        Returns skew in price units.
        """
        delta = inventory.net_delta
        skew_bps = delta * self.config.skew_bps_per_unit
        return _bps_to_price(skew_bps)

    def generate_quotes(self, fair_value: float,
                        token_id: str,
                        inventory: Inventory,
                        volatility: float = 0.0,
                        avg_volatility: float = 0.0) -> tuple[Quote, Quote]:
        """Generate a bid and ask quote for a single token.

        Args:
            fair_value: Fair price for this token (0.01-0.99)
            token_id: Polymarket token ID
            inventory: Current inventory state
            volatility: Current realized vol
            avg_volatility: Average realized vol (for regime detection)

        Returns:
            (bid_quote, ask_quote)
        """
        half_spread_bps = self._effective_half_spread(volatility, avg_volatility)
        half_spread = _bps_to_price(half_spread_bps)
        skew = self._inventory_skew(inventory)

        bid_price = _round_price(fair_value - half_spread - skew)
        ask_price = _round_price(fair_value + half_spread - skew)

        # Ensure bid < ask (at least 0.01 apart)
        if bid_price >= ask_price:
            mid = (bid_price + ask_price) / 2.0
            bid_price = _round_price(mid - 0.01)
            ask_price = _round_price(mid + 0.01)

        # Size in shares = USD / price
        bid_size = self.config.order_size_usd / bid_price if bid_price > 0 else 0
        ask_size = self.config.order_size_usd / ask_price if ask_price > 0 else 0

        bid = Quote(side="BUY", token_id=token_id,
                    price=bid_price, size=round(bid_size, 2))
        ask = Quote(side="SELL", token_id=token_id,
                    price=ask_price, size=round(ask_size, 2))

        return bid, ask

    def should_requote(self, current: Quote, new: Quote) -> bool:
        """Check if price moved enough to justify cancel-replace.

        Returns True if price difference exceeds threshold.
        """
        if current is None:
            return True
        if current.order_id is None:
            return True

        price_diff_bps = abs(new.price - current.price) / max(current.price, 0.01) * 10000
        return price_diff_bps >= self.config.requote_threshold_bps

    def generate_all_quotes(self, fv_up: float, fv_dn: float,
                            up_token_id: str, dn_token_id: str,
                            inventory: Inventory,
                            volatility: float = 0.0,
                            avg_volatility: float = 0.0) -> dict[str, tuple[Quote, Quote]]:
        """Generate quotes for both UP and DN tokens.

        Returns dict with keys 'up' and 'dn', each containing (bid, ask).
        """
        up_bid, up_ask = self.generate_quotes(
            fv_up, up_token_id, inventory, volatility, avg_volatility)
        dn_bid, dn_ask = self.generate_quotes(
            fv_dn, dn_token_id, inventory, volatility, avg_volatility)

        return {
            "up": (up_bid, up_ask),
            "dn": (dn_bid, dn_ask),
        }
