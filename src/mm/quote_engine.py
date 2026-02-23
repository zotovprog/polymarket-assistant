"""Quote Engine — generates bid/ask quotes around fair value.

Core logic:
  bid = fair_value - half_spread - inventory_skew
  ask = fair_value + half_spread - inventory_skew

Spread is widened during high volatility.
Inventory skew pushes quotes to reduce net delta.
Prices are rounded to PM's configured tick increment.
"""
from __future__ import annotations
import logging
import math
from .types import Quote, Inventory
from .mm_config import MMConfig

log = logging.getLogger("mm.quotes")


def _round_price(price: float, tick_size: float = 0.01) -> float:
    """Round to Polymarket's tick increment, clamp to valid range."""
    rounded = round(round(price / tick_size) * tick_size, 10)
    return max(tick_size, min(1.0 - tick_size, rounded))


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
                        avg_volatility: float = 0.0,
                        tick_size: float = 0.01) -> tuple[Quote, Quote]:
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

        bid_price = _round_price(fair_value - half_spread - skew, tick_size)
        ask_price = _round_price(fair_value + half_spread - skew, tick_size)

        # Ensure bid < ask (at least 1 tick apart)
        if bid_price >= ask_price:
            mid = (bid_price + ask_price) / 2.0
            bid_price = _round_price(mid - tick_size, tick_size)
            ask_price = _round_price(mid + tick_size, tick_size)

        # Size in shares = USD / price
        bid_size = self.config.order_size_usd / bid_price if bid_price > 0 else 0
        ask_size = self.config.order_size_usd / ask_price if ask_price > 0 else 0
        max_shares = self.config.max_inventory_shares
        if bid_size > max_shares:
            bid_size = max_shares
        if ask_size > max_shares:
            ask_size = max_shares

        bid = Quote(side="BUY", token_id=token_id,
                    price=bid_price, size=round(bid_size, 2))
        ask = Quote(side="SELL", token_id=token_id,
                    price=ask_price, size=round(ask_size, 2))

        return bid, ask

    @staticmethod
    def clamp_to_book(
        bid: Quote | None,
        ask: Quote | None,
        best_bid: float | None,
        best_ask: float | None,
        tick_size: float = 0.01,
    ) -> tuple[Quote | None, Quote | None]:
        """Adjust quotes to avoid crossing the Polymarket orderbook.

        Post-only rules:
        - BUY order must have price < best_ask (otherwise it crosses)
        - SELL order must have price > best_bid (otherwise it crosses)

        If we would cross, pull our price back to 1 tick away from the book edge.
        """
        tick = tick_size

        if bid is not None and best_ask is not None and bid.price >= best_ask:
            bid.price = _round_price(best_ask - tick, tick_size)

        if ask is not None and best_bid is not None and ask.price <= best_bid:
            ask.price = _round_price(best_bid + tick, tick_size)

        return bid, ask

    def should_requote(self, current: Quote | None, new: Quote | None) -> bool:
        """Check if price moved enough to justify cancel-replace.

        Returns True if price difference exceeds threshold.
        """
        if new is None:
            return False
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
                            avg_volatility: float = 0.0,
                            usdc_budget: float = 0.0,
                            order_collateral: float = 0.0,
                            tick_size: float = 0.01) -> dict[str, tuple[Quote | None, Quote]]:
        """Generate quotes for both UP and DN tokens.

        Args:
            usdc_budget: Session USDC limit (initial_usdc). 0 = no limit.
            order_collateral: USDC locked in active BUY orders (pending fills).

        Returns dict with keys 'up' and 'dn', each containing (bid, ask).
        """
        up_bid, up_ask = self.generate_quotes(
            fv_up, up_token_id, inventory, volatility, avg_volatility, tick_size=tick_size)
        dn_bid, dn_ask = self.generate_quotes(
            fv_dn, dn_token_id, inventory, volatility, avg_volatility, tick_size=tick_size)

        # Cap BUY size by remaining inventory room per token.
        max_shares = self.config.max_inventory_shares
        up_room = max(0.0, max_shares - inventory.up_shares)
        dn_room = max(0.0, max_shares - inventory.dn_shares)

        up_bid_size = round(min(up_bid.size, up_room), 2)
        dn_bid_size = round(min(dn_bid.size, dn_room), 2)

        # Hard cutoff: if remaining USDC budget <= 0, skip all BUY generation
        if usdc_budget > 0:
            up_locked = inventory.up_shares * inventory.up_cost.avg_entry_price
            dn_locked = inventory.dn_shares * inventory.dn_cost.avg_entry_price
            remaining = max(0.0, usdc_budget - up_locked - dn_locked - order_collateral)
            if remaining <= 0:
                return {
                    "up": (None, up_ask),
                    "dn": (None, dn_ask),
                }

        # Cap BUY size by remaining USDC budget.
        if usdc_budget > 0:
            up_locked = inventory.up_shares * inventory.up_cost.avg_entry_price
            dn_locked = inventory.dn_shares * inventory.dn_cost.avg_entry_price
            remaining = max(0.0, usdc_budget - up_locked - dn_locked - order_collateral)
            half_remaining = remaining / 2.0

            if up_bid_size > 0 and up_bid.price > 0:
                max_up_shares = half_remaining / up_bid.price
                if up_bid_size > max_up_shares:
                    up_bid_size = round(max(0.0, max_up_shares), 2)

            if dn_bid_size > 0 and dn_bid.price > 0:
                max_dn_shares = half_remaining / dn_bid.price
                if dn_bid_size > max_dn_shares:
                    dn_bid_size = round(max(0.0, max_dn_shares), 2)

        if up_bid_size <= 0:
            up_bid = None
        else:
            up_bid.size = up_bid_size

        if dn_bid_size <= 0:
            dn_bid = None
        else:
            dn_bid.size = dn_bid_size

        return {
            "up": (up_bid, up_ask),
            "dn": (dn_bid, dn_ask),
        }
