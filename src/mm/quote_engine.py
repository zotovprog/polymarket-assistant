"""Quote Engine — generates bid/ask quotes around fair value.

Core logic:
  bid = fair_value - half_spread - inventory_skew
  ask = fair_value + half_spread - inventory_skew

For DN tokens, skew is inverted (shifts mid UP when long UP)
so that the bot buys DN aggressively to hedge.

Spread is widened during high volatility and near expiry (gamma-aware).
Prices are rounded to PM's configured tick increment.
"""
from __future__ import annotations
import logging
import random
from .types import Quote, Inventory
from .mm_config import MMConfig
from .dynamic_spread import DynamicSpread

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
        self._quote_price_jitter_ticks: int | None = None
        self._quote_size_jitter_mult: float | None = None
        self._dynamic_spread: DynamicSpread | None = None
        self._dynamic_spread_sig: tuple[float, float, float, float] | None = None

    def _ensure_dynamic_spread(self) -> DynamicSpread:
        """Create/update dynamic spread model when runtime config changes."""
        sig = (
            float(getattr(self.config, "dynamic_spread_gamma", 0.10)),
            float(getattr(self.config, "dynamic_spread_k", 1.5)),
            float(getattr(self.config, "dynamic_spread_min_bps", self.config.min_spread_bps)),
            float(getattr(self.config, "dynamic_spread_max_bps", self.config.max_spread_bps)),
        )
        if self._dynamic_spread is None or self._dynamic_spread_sig != sig:
            self._dynamic_spread = DynamicSpread(
                gamma=sig[0],
                k=sig[1],
                min_spread_bps=sig[2],
                max_spread_bps=sig[3],
            )
            self._dynamic_spread_sig = sig
        return self._dynamic_spread

    def _apply_price_jitter(self, price: float, tick_size: float) -> float:
        """Apply anti-detection price jitter to a single quote level."""
        if not bool(getattr(self.config, "price_jitter_enabled", True)):
            return _round_price(price, tick_size)

        jitter_ticks = self._quote_price_jitter_ticks
        if jitter_ticks is None:
            max_ticks = max(0, int(getattr(self.config, "price_jitter_ticks", 1)))
            if max_ticks == 0:
                jitter_ticks = 0
            else:
                jitter_ticks = random.choice([-max_ticks, 0, 0, max_ticks])

        return _round_price(price + jitter_ticks * tick_size, tick_size)

    def _apply_size_jitter(self, size: float) -> float:
        """Apply anti-detection size jitter and enforce minimum order size."""
        size_val = max(0.0, float(size))
        if bool(getattr(self.config, "size_jitter_enabled", True)):
            size_mult = self._quote_size_jitter_mult
            if size_mult is None:
                jitter_pct = max(0.0, float(getattr(self.config, "size_jitter_pct", 0.20)))
                size_mult = 1.0 + random.uniform(-jitter_pct, jitter_pct)
            size_val *= size_mult

        size_val = max(float(getattr(self.config, "min_quote_size_shares", 1.0)), size_val)
        size_val = min(size_val, float(self.config.max_inventory_shares))
        return round(size_val, 2)

    def jitter_requote_interval(self, base_interval: float) -> float:
        """Apply anti-detection jitter to requote loop sleep interval."""
        interval = float(base_interval)
        if not bool(getattr(self.config, "requote_interval_jitter_enabled", True)):
            return max(1.0, interval)

        max_jitter = max(0.0, float(getattr(self.config, "requote_interval_jitter_sec", 1.5)))
        if max_jitter == 0.0:
            return max(1.0, interval)

        return max(1.0, interval + random.uniform(-max_jitter, max_jitter))

    def _effective_half_spread(self, volatility: float,
                                avg_vol: float,
                                time_remaining: float = -1.0) -> float:
        """Compute half-spread, widening in high-vol regime and near expiry."""
        base = self.config.half_spread_bps

        if avg_vol > 0 and volatility > avg_vol:
            vol_ratio = volatility / avg_vol
            if vol_ratio > self.config.volatility_pause_mult:
                # Very high vol — use max spread
                return self.config.max_spread_bps
            # Linear widening between 1x and pause_mult
            mult = 1.0 + (vol_ratio - 1.0) * (self.config.vol_spread_mult - 1.0)
            base *= mult

        # Gamma-aware widening: binary option gamma explodes as T→0
        # Linearly widen up to 3x in last 2 minutes
        if time_remaining >= 0 and time_remaining < 120:
            time_mult = 1.0 + (120.0 - time_remaining) / 120.0 * 2.0
            base *= time_mult

        return max(self.config.min_spread_bps,
                   min(self.config.max_spread_bps, base))

    def _inventory_skew(
        self,
        inventory: Inventory,
        *,
        tier: int = 0,
        skew_mult: float = 1.0,
    ) -> float:
        """Compute price skew based on net delta.

        Positive delta (long UP) → skew positive → shifts mid down for UP token,
        shifts mid up for DN token (with invert_skew=True).
        Returns skew in price units.
        """
        delta = inventory.net_delta
        if delta == 0:
            return 0.0

        safe_tier = max(0, min(3, int(tier)))
        exponent = 1.0 + 0.5 * (safe_tier / 3.0)
        sign = 1.0 if delta > 0 else -1.0
        skew_bps = sign * self.config.skew_bps_per_unit * (abs(delta) ** exponent) * max(1.0, skew_mult)
        return _bps_to_price(skew_bps)

    def compute_imbalance_adjustments(
        self,
        inventory: Inventory,
        fill_imbalance_shares: float,
        imbalance_duration_sec: float,
    ) -> dict:
        """Compute asymmetric spread/skew adjustments based on fill imbalance."""
        imbalance = max(0.0, float(fill_imbalance_shares))
        duration = max(0.0, float(imbalance_duration_sec))

        # Tier 0 hard gate: very small imbalance gets no adjustments regardless of duration.
        if imbalance < 3.0:
            return {
                "leading_spread_mult": 1.0,
                "lagging_spread_mult": 1.0,
                "skew_mult": 1.0,
                "tier": 0,
                "suppress_leading_buy": False,
                "force_taker_lagging": False,
            }

        # Size-based tier escalation.
        if imbalance >= 15.0:
            size_tier = 3
        elif imbalance >= 8.0:
            size_tier = 2
        else:
            size_tier = 1

        # Time-based tier escalation.
        if duration > 20.0:
            time_tier = 3
        elif duration >= 10.0:
            time_tier = 2
        else:
            time_tier = 1

        tier = max(size_tier, time_tier)

        if tier == 1:
            return {
                "leading_spread_mult": 1.3,
                "lagging_spread_mult": 0.5,
                "skew_mult": 1.5,
                "tier": 1,
                "suppress_leading_buy": False,
                "force_taker_lagging": False,
            }
        if tier == 2:
            return {
                "leading_spread_mult": 2.0,
                "lagging_spread_mult": 0.3,
                "skew_mult": 2.5,
                "tier": 2,
                "suppress_leading_buy": False,
                "force_taker_lagging": False,
            }

        # Tier 3 keeps aggressive spread profile with leading-buy suppression.
        return {
            "leading_spread_mult": 2.0,
            "lagging_spread_mult": 0.3,
            "skew_mult": 3.0,
            "tier": 3,
            "suppress_leading_buy": True,
            "force_taker_lagging": bool(getattr(self.config, "paired_fill_ioc_enabled", False)),
        }

    def generate_quotes(self, fair_value: float,
                        token_id: str,
                        inventory: Inventory,
                        volatility: float = 0.0,
                        avg_volatility: float = 0.0,
                        tick_size: float = 0.01,
                        invert_skew: bool = False,
                        imbalance_adjustments: dict | None = None,
                        time_remaining: float = -1.0) -> tuple[Quote, Quote]:
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
        base_half_spread_bps = self._effective_half_spread(volatility, avg_volatility, time_remaining)
        base_half_spread = _bps_to_price(base_half_spread_bps)
        bid_half_spread = base_half_spread
        ask_half_spread = base_half_spread
        imbalance_adjustments = imbalance_adjustments or {}

        if bool(getattr(self.config, "dynamic_spread_enabled", True)):
            try:
                dyn = self._ensure_dynamic_spread()
                inv_delta = inventory.net_delta
                if invert_skew:
                    inv_delta = -inv_delta
                inv_delta *= max(1.0, float(imbalance_adjustments.get("skew_mult", 1.0)))
                effective_t = (
                    time_remaining
                    if time_remaining is not None and time_remaining >= 0
                    else max(30.0, float(self.config.requote_interval_sec) * 5.0)
                )
                dyn_bid_half, dyn_ask_half = dyn.compute_asymmetric_spread(
                    sigma=max(0.0, float(volatility)),
                    T_seconds=effective_t,
                    inventory_delta=inv_delta,
                    fair_value=fair_value,
                )
                hard_cap = _bps_to_price(self.config.max_spread_bps)
                bid_half_spread = min(hard_cap, max(base_half_spread, dyn_bid_half))
                ask_half_spread = min(hard_cap, max(base_half_spread, dyn_ask_half))
            except Exception as e:
                log.debug("Dynamic spread fallback to static: %s", e)

        skew = self._inventory_skew(
            inventory,
            tier=int(imbalance_adjustments.get("tier", 0)),
            skew_mult=float(imbalance_adjustments.get("skew_mult", 1.0)),
        )
        if invert_skew:
            skew = -skew

        bid_price = _round_price(fair_value - bid_half_spread - skew, tick_size)
        ask_price = _round_price(fair_value + ask_half_spread - skew, tick_size)

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

        # Anti-detection: apply jitter as the final transform before Quote objects.
        if bool(getattr(self.config, "price_jitter_enabled", True)):
            max_ticks = max(0, int(getattr(self.config, "price_jitter_ticks", 1)))
            if max_ticks == 0:
                self._quote_price_jitter_ticks = 0
            else:
                self._quote_price_jitter_ticks = random.choice([-max_ticks, 0, 0, max_ticks])
        else:
            self._quote_price_jitter_ticks = None
        bid_price = self._apply_price_jitter(bid_price, tick_size)
        ask_price = self._apply_price_jitter(ask_price, tick_size)
        self._quote_price_jitter_ticks = None

        # Ensure bid < ask after jitter (at least 1 tick apart).
        if bid_price >= ask_price:
            mid = (bid_price + ask_price) / 2.0
            bid_price = _round_price(mid - tick_size, tick_size)
            ask_price = _round_price(mid + tick_size, tick_size)

        if bool(getattr(self.config, "size_jitter_enabled", True)):
            jitter_pct = max(0.0, float(getattr(self.config, "size_jitter_pct", 0.20)))
            self._quote_size_jitter_mult = 1.0 + random.uniform(-jitter_pct, jitter_pct)
        else:
            self._quote_size_jitter_mult = None
        bid_size = self._apply_size_jitter(bid_size)
        ask_size = self._apply_size_jitter(ask_size)
        self._quote_size_jitter_mult = None

        bid = Quote(side="BUY", token_id=token_id,
                    price=bid_price, size=bid_size)
        ask = Quote(side="SELL", token_id=token_id,
                    price=ask_price, size=ask_size)

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
                            tick_size: float = 0.01,
                            imbalance_adjustments: dict | None = None,
                            time_remaining: float = -1.0) -> dict[str, tuple[Quote | None, Quote]]:
        """Generate quotes for both UP and DN tokens.

        Args:
            usdc_budget: Session USDC limit (initial_usdc). 0 = no limit.
            order_collateral: USDC locked in active BUY orders (pending fills).

        Returns dict with keys 'up' and 'dn', each containing (bid, ask).
        """
        up_bid, up_ask = self.generate_quotes(
            fv_up, up_token_id, inventory, volatility, avg_volatility,
            tick_size=tick_size, imbalance_adjustments=imbalance_adjustments,
            time_remaining=time_remaining)
        dn_bid, dn_ask = self.generate_quotes(
            fv_dn, dn_token_id, inventory, volatility, avg_volatility,
            tick_size=tick_size, invert_skew=True,
            imbalance_adjustments=imbalance_adjustments,
            time_remaining=time_remaining)

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
