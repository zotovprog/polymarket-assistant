from __future__ import annotations

from dataclasses import dataclass
import math
from typing import Literal

from .config import (
    HARMFUL_PRICE_TICKS_MAX,
    HARMFUL_SIZE_MULT_MIN,
    HELPFUL_MIN_PROMOTION_MULT,
    HELPFUL_PRICE_TICKS_MAX,
    HELPFUL_SIZE_MULT_MAX,
    MMConfigV2,
    PAIR_SHARE_CLIP_PRICE_FLOOR,
)
from .types import PairInventoryState, PairMarketSnapshot, QuoteIntent, QuotePlan, RiskRegime


def _round_price(price: float, tick_size: float) -> float:
    rounded = round(round(float(price) / tick_size) * tick_size, 10)
    min_price = max(tick_size, 0.01)
    max_price = min(0.99, 1.0 - tick_size)
    if max_price < min_price:
        max_price = min_price
    return max(min_price, min(max_price, rounded))


def _floor_price(price: float, tick_size: float) -> float:
    tick = max(1e-9, float(tick_size))
    floored = math.floor((float(price) + 1e-9) / tick) * tick
    min_price = max(tick_size, 0.01)
    max_price = min(0.99, 1.0 - tick_size)
    if max_price < min_price:
        max_price = min_price
    return round(max(min_price, min(max_price, floored)), 10)


def _ceil_price(price: float, tick_size: float) -> float:
    tick = max(1e-9, float(tick_size))
    ceiled = math.ceil((float(price) - 1e-9) / tick) * tick
    min_price = max(tick_size, 0.01)
    max_price = min(0.99, 1.0 - tick_size)
    if max_price < min_price:
        max_price = min_price
    return round(max(min_price, min(max_price, ceiled)), 10)


@dataclass
class QuoteContext:
    tick_size: float
    min_order_size: float
    allow_naked_sells: bool = True


class QuotePolicyV2:
    def __init__(self, config: MMConfigV2):
        self.config = config

    @staticmethod
    def _bps_to_price(bps: float) -> float:
        return float(bps) / 10000.0

    def _clip_usd(self, risk: RiskRegime) -> float:
        base = float(self.config.effective_base_clip_usd())
        if risk.soft_mode == "inventory_skewed":
            return base * 0.7
        if risk.soft_mode == "defensive":
            return base * float(self.config.defensive_size_mult)
        if risk.soft_mode == "unwind":
            return max(1.0, base * 0.5)
        return base

    def _min_viable_clip_usd(self, snapshot: PairMarketSnapshot, ctx: QuoteContext) -> float:
        return float(ctx.min_order_size) * self._pair_reference_price(snapshot)

    @staticmethod
    def _buy_size_from_clip(clip_usd: float, price: float) -> float:
        return clip_usd / max(0.01, float(price))

    @staticmethod
    def _sell_size_from_clip(clip_usd: float, price: float) -> float:
        collateral_per_share = max(0.01, 1.0 - float(price))
        return clip_usd / collateral_per_share

    @staticmethod
    def _inventory_backed_sell_size_from_clip(clip_usd: float, price: float) -> float:
        return clip_usd / max(0.01, float(price))

    def _spread(self, risk: RiskRegime) -> float:
        base = self._bps_to_price(self.config.base_half_spread_bps)
        if risk.soft_mode == "defensive":
            base *= float(self.config.defensive_spread_mult)
        elif risk.soft_mode == "unwind":
            base *= max(2.0, float(self.config.defensive_spread_mult))
        return min(self._bps_to_price(self.config.max_half_spread_bps), base)

    @staticmethod
    def _classify_inventory_effect(
        *,
        token: str,
        side: Literal["BUY", "SELL"],
        inventory_side: str,
        up_token_id: str,
        dn_token_id: str,
    ) -> Literal["helpful", "neutral", "harmful"]:
        if inventory_side == "flat":
            return "neutral"
        if inventory_side == "up":
            if token == up_token_id and side == "BUY":
                return "harmful"
            if token == up_token_id and side == "SELL":
                return "helpful"
            if token == dn_token_id and side == "BUY":
                return "helpful"
            if token == dn_token_id and side == "SELL":
                return "harmful"
        if inventory_side == "dn":
            if token == dn_token_id and side == "BUY":
                return "harmful"
            if token == dn_token_id and side == "SELL":
                return "helpful"
            if token == up_token_id and side == "BUY":
                return "helpful"
            if token == up_token_id and side == "SELL":
                return "harmful"
        return "neutral"

    @staticmethod
    def _size_multiplier(effect: str, pressure: float) -> float:
        if effect == "helpful":
            return min(HELPFUL_SIZE_MULT_MAX, 1.0 + 0.8 * pressure)
        if effect == "harmful":
            return max(HARMFUL_SIZE_MULT_MIN, 1.0 - 0.75 * pressure)
        return 1.0

    @staticmethod
    def _price_adjust_ticks(effect: str, pressure: float) -> int:
        if effect == "helpful":
            return int(math.ceil(pressure * HELPFUL_PRICE_TICKS_MAX))
        if effect == "harmful":
            return int(math.ceil(pressure * HARMFUL_PRICE_TICKS_MAX))
        return 0

    @staticmethod
    def _would_expand_excess(
        *,
        token: str,
        side: Literal["BUY", "SELL"],
        inventory_side: str,
        up_token_id: str,
        dn_token_id: str,
    ) -> bool:
        if inventory_side == "up":
            return (token == up_token_id and side == "BUY") or (token == dn_token_id and side == "SELL")
        if inventory_side == "dn":
            return (token == dn_token_id and side == "BUY") or (token == up_token_id and side == "SELL")
        return False

    def _maker_clamp(
        self,
        *,
        side: Literal["BUY", "SELL"],
        price: float,
        best_bid: float | None,
        best_ask: float | None,
        tick_size: float,
    ) -> float:
        if side == "BUY" and best_ask is not None:
            price = min(price, float(best_ask) - tick_size)
        if side == "SELL" and best_bid is not None:
            price = max(price, float(best_bid) + tick_size)
        return price

    @staticmethod
    def _pair_reference_price(snapshot: PairMarketSnapshot) -> float:
        return max(
            PAIR_SHARE_CLIP_PRICE_FLOOR,
            float(snapshot.fv_up or 0.0),
            float(snapshot.fv_dn or 0.0),
        )

    @staticmethod
    def _owned_share_cap(
        *,
        token: str,
        inventory: PairInventoryState,
        up_token_id: str,
        dn_token_id: str,
    ) -> float:
        if token == up_token_id:
            return max(0.0, float(inventory.up_shares) - float(inventory.pending_sell_up))
        if token == dn_token_id:
            return max(0.0, float(inventory.dn_shares) - float(inventory.pending_sell_dn))
        return 0.0

    @staticmethod
    def _sellable_share_cap(
        *,
        token: str,
        inventory: PairInventoryState,
        up_token_id: str,
        dn_token_id: str,
    ) -> float:
        if token == up_token_id:
            return max(
                0.0,
                float(inventory.sellable_up_shares) - float(inventory.pending_sell_up),
            )
        if token == dn_token_id:
            return max(
                0.0,
                float(inventory.sellable_dn_shares) - float(inventory.pending_sell_dn),
            )
        return 0.0

    @staticmethod
    def _count_effects(built: dict[str, QuoteIntent | None]) -> tuple[int, int, int]:
        helpful = 0
        harmful = 0
        neutral = 0
        for intent in built.values():
            if not intent:
                continue
            if intent.inventory_effect == "helpful":
                helpful += 1
            elif intent.inventory_effect == "harmful":
                harmful += 1
            else:
                neutral += 1
        return helpful, harmful, neutral

    @staticmethod
    def _quote_balance_state(
        *,
        built: dict[str, QuoteIntent | None],
        helpful_count: int,
        harmful_count: int,
        harmful_blocked: bool,
    ) -> str:
        active_count = sum(1 for intent in built.values() if intent)
        if harmful_blocked:
            return "harmful_only_blocked"
        if active_count == 4:
            return "balanced"
        if helpful_count > 0 and harmful_count == 0:
            return "helpful_only"
        if active_count > 0:
            return "reduced"
        return "none"

    def _make_intent(
        self,
        *,
        token: str,
        side: Literal["BUY", "SELL"],
        price: float,
        clip_usd: float,
        share_cap: float,
        ctx: QuoteContext,
        role: str,
        post_only: bool,
        inventory_effect: Literal["helpful", "neutral", "harmful"],
        size_mult: float,
        price_adjust_ticks: int,
        inventory_backed_sell: bool = False,
        size_override: float | None = None,
    ) -> tuple[QuoteIntent | None, str | None]:
        if size_override is not None:
            size = float(size_override)
        elif side == "BUY":
            economic_size = self._buy_size_from_clip(clip_usd, price)
            size = min(economic_size, share_cap)
        else:
            if inventory_backed_sell:
                economic_size = self._inventory_backed_sell_size_from_clip(clip_usd, price)
            else:
                economic_size = self._sell_size_from_clip(clip_usd, price)
            size = min(economic_size, share_cap)
        size = round(max(0.0, float(size)), 2)
        if 0.0 < size < ctx.min_order_size and inventory_effect == "helpful":
            promoted = round(float(ctx.min_order_size), 2)
            if promoted <= round(max(0.0, share_cap * HELPFUL_MIN_PROMOTION_MULT), 2):
                size = promoted
        if size < ctx.min_order_size:
            if inventory_effect == "helpful":
                return None, "below_min_order_size_after_helpful_floor"
            return None, "below_min_order_size"
        return (
            QuoteIntent(
                token=token,
                side=side,
                price=_round_price(price, ctx.tick_size),
                size=size,
                quote_role=role,  # type: ignore[arg-type]
                post_only=post_only,
                inventory_effect=inventory_effect,
                size_mult=size_mult,
                price_adjust_ticks=price_adjust_ticks,
                suppressed_reason=None,
            ),
            None,
        )

    def generate(
        self,
        *,
        snapshot: PairMarketSnapshot,
        inventory: PairInventoryState,
        risk: RiskRegime,
        ctx: QuoteContext,
    ) -> QuotePlan:
        base_mid = max(0.01, min(0.99, float(snapshot.fv_up)))
        spread = self._spread(risk)
        clip_usd = self._clip_usd(risk)
        free_usdc = max(0.0, float(inventory.free_usdc))
        budget_headroom_usd = max(1.0, free_usdc * 0.20)
        min_viable_clip_usd = self._min_viable_clip_usd(snapshot, ctx)
        harmful_buy_guard_usd = float(self.config.effective_harmful_buy_suppress_usd())
        pressure = max(0.0, min(1.0, float(risk.inventory_pressure_abs)))
        mid_shift = float(risk.inventory_pressure_signed) * float(self.config.inventory_skew_strength) * 0.0025
        pair_mid = max(0.01, min(0.99, base_mid - mid_shift))
        pair_reference_price = self._pair_reference_price(snapshot)

        up_bid_price = pair_mid - spread
        up_ask_price = pair_mid + spread
        dn_mid = 1.0 - pair_mid
        dn_bid_price = dn_mid - spread
        dn_ask_price = dn_mid + spread

        raw_quotes = {
            "up_bid": ("BUY", snapshot.up_token_id, up_bid_price, snapshot.up_best_bid, snapshot.up_best_ask, "base_bid"),
            "up_ask": ("SELL", snapshot.up_token_id, up_ask_price, snapshot.up_best_bid, snapshot.up_best_ask, "base_ask"),
            "dn_bid": ("BUY", snapshot.dn_token_id, dn_bid_price, snapshot.dn_best_bid, snapshot.dn_best_ask, "base_bid"),
            "dn_ask": ("SELL", snapshot.dn_token_id, dn_ask_price, snapshot.dn_best_bid, snapshot.dn_best_ask, "base_ask"),
        }
        built: dict[str, QuoteIntent | None] = {}
        suppressed_reasons: dict[str, str] = {}
        helpful_floor_applied = False
        neutral_floor_applied = False

        for slot, (side, token, base_price, best_bid, best_ask, role) in raw_quotes.items():
            effect = self._classify_inventory_effect(
                token=token,
                side=side,
                inventory_side=risk.inventory_side,
                up_token_id=snapshot.up_token_id,
                dn_token_id=snapshot.dn_token_id,
            )
            if (
                risk.soft_mode == "inventory_skewed"
                and side == "BUY"
                and effect == "harmful"
                and float(inventory.excess_value_usd) >= harmful_buy_guard_usd
            ):
                built[slot] = None
                suppressed_reasons[slot] = "harmful_buy_blocked_high_skew"
                continue
            if risk.soft_mode in {"defensive", "unwind"} and effect == "harmful":
                built[slot] = None
                suppressed_reasons[slot] = (
                    "harmful_suppressed_in_defensive"
                    if risk.soft_mode == "defensive"
                    else "harmful_suppressed_in_unwind"
                )
                continue
            if risk.soft_mode == "unwind" and self._would_expand_excess(
                token=token,
                side=side,
                inventory_side=risk.inventory_side,
                up_token_id=snapshot.up_token_id,
                dn_token_id=snapshot.dn_token_id,
            ):
                built[slot] = None
                suppressed_reasons[slot] = "pair-expanding intent disabled in unwind"
                continue
            ticks = self._price_adjust_ticks(effect, pressure)
            adjusted_price = base_price
            if effect == "helpful":
                adjusted_price += ctx.tick_size * ticks if side == "BUY" else -(ctx.tick_size * ticks)
            elif effect == "harmful":
                adjusted_price += -(ctx.tick_size * ticks) if side == "BUY" else (ctx.tick_size * ticks)
            adjusted_price = self._maker_clamp(
                side=side,
                price=adjusted_price,
                best_bid=best_bid,
                best_ask=best_ask,
                tick_size=ctx.tick_size,
            )
            size_mult = self._size_multiplier(effect, pressure)
            owned_share_cap = 0.0
            live_sellable_share_cap = 0.0
            inventory_backed_sell = False
            if side == "SELL":
                owned_share_cap = self._owned_share_cap(
                    token=token,
                    inventory=inventory,
                    up_token_id=snapshot.up_token_id,
                    dn_token_id=snapshot.dn_token_id,
                )
                live_sellable_share_cap = self._sellable_share_cap(
                    token=token,
                    inventory=inventory,
                    up_token_id=snapshot.up_token_id,
                    dn_token_id=snapshot.dn_token_id,
                )
                share_cap_for_sell = owned_share_cap if ctx.allow_naked_sells else live_sellable_share_cap
                inventory_backed_sell = share_cap_for_sell >= ctx.min_order_size
                if not ctx.allow_naked_sells and not inventory_backed_sell:
                    built[slot] = None
                    suppressed_reasons[slot] = "live_requires_inventory_backed_sell"
                    continue
            expands_gross_inventory = side == "BUY" or (side == "SELL" and not inventory_backed_sell)
            if (
                risk.soft_mode in {"defensive", "unwind"}
                and inventory.pair_value_over_target_usd > 0.0
                and expands_gross_inventory
            ):
                built[slot] = None
                suppressed_reasons[slot] = "target_pair_ratio_cap"
                continue
            buy_headroom_usd = budget_headroom_usd
            if side == "BUY" and effect == "helpful" and risk.soft_mode == "unwind":
                buy_headroom_usd = max(0.0, free_usdc)
            if side == "BUY":
                nominal_quote_clip_usd = min(clip_usd, buy_headroom_usd) * size_mult
            elif inventory_backed_sell:
                nominal_quote_clip_usd = clip_usd * size_mult
            else:
                nominal_quote_clip_usd = min(clip_usd, budget_headroom_usd) * size_mult
            effective_clip_usd = nominal_quote_clip_usd
            floor_allowed = False
            if (
                effect == "helpful"
                and risk.soft_mode in {"inventory_skewed", "defensive", "unwind"}
                and risk.inventory_side != "flat"
            ):
                helpful_headroom_usd = buy_headroom_usd if side == "BUY" else budget_headroom_usd
                floor_allowed = (
                    (
                        side == "SELL"
                        and inventory_backed_sell
                        and owned_share_cap >= ctx.min_order_size
                    )
                    or min_viable_clip_usd <= helpful_headroom_usd * max(1.0, size_mult)
                )
            elif (
                effect == "neutral"
                and risk.inventory_side == "flat"
                and risk.soft_mode in {"normal", "inventory_skewed", "defensive"}
            ):
                # Flat-start maker quoting should use real free USDC viability,
                # not the tighter BUY headroom cap, otherwise defensive starts
                # can collapse into "all quotes below min size" with cash idle.
                floor_allowed = min_viable_clip_usd <= free_usdc
            if floor_allowed:
                effective_clip_usd = max(effective_clip_usd, min_viable_clip_usd)
                if effective_clip_usd > nominal_quote_clip_usd + 1e-9:
                    if effect == "helpful":
                        helpful_floor_applied = True
                    elif effect == "neutral":
                        neutral_floor_applied = True
            if inventory_backed_sell:
                share_cap = owned_share_cap if ctx.allow_naked_sells else live_sellable_share_cap
            else:
                share_cap = max(0.0, effective_clip_usd / pair_reference_price)
            role_name = role if risk.soft_mode != "unwind" else "unwind"
            intent, suppressed = self._make_intent(
                token=token,
                side=side,
                price=adjusted_price,
                clip_usd=effective_clip_usd,
                share_cap=share_cap,
                ctx=ctx,
                role=role_name,
                post_only=True,
                inventory_effect=effect,
                size_mult=size_mult,
                price_adjust_ticks=ticks,
                inventory_backed_sell=inventory_backed_sell,
            )
            if intent is not None:
                # Final maker guard after rounding. This prevents any residual
                # crossed-book post-only prices in endpoint/tick edge cases.
                if side == "BUY" and best_ask is not None:
                    max_maker_buy = _floor_price(float(best_ask) - ctx.tick_size, ctx.tick_size)
                    if (
                        max_maker_buy < 0.01
                        or max_maker_buy >= float(best_ask) - 1e-9
                        or intent.price > max_maker_buy + 1e-9
                    ):
                        if max_maker_buy >= 0.01 and max_maker_buy < float(best_ask) - 1e-9:
                            intent.price = max_maker_buy
                        else:
                            intent = None
                            suppressed = "maker_cross_guard"
                elif side == "SELL" and best_bid is not None:
                    min_maker_sell = _ceil_price(float(best_bid) + ctx.tick_size, ctx.tick_size)
                    if (
                        min_maker_sell > 0.99
                        or min_maker_sell <= float(best_bid) + 1e-9
                        or intent.price < min_maker_sell - 1e-9
                    ):
                        if min_maker_sell <= 0.99 and min_maker_sell > float(best_bid) + 1e-9:
                            intent.price = min_maker_sell
                        else:
                            intent = None
                            suppressed = "maker_cross_guard"
            built[slot] = intent
            if suppressed:
                suppressed_reasons[slot] = suppressed

        pair_ceiling = 1.0 - ctx.tick_size
        if built["up_bid"] and built["dn_ask"] and built["up_bid"].price + built["dn_ask"].price > pair_ceiling:
            built["up_bid"].price = _floor_price(pair_ceiling - built["dn_ask"].price, ctx.tick_size)
        if built["dn_bid"] and built["up_ask"] and built["dn_bid"].price + built["up_ask"].price > pair_ceiling:
            built["dn_bid"].price = _floor_price(pair_ceiling - built["up_ask"].price, ctx.tick_size)

        regime = risk.soft_mode
        reason = risk.reason
        if risk.hard_mode == "emergency_unwind":
            regime = "emergency_unwind"
            if inventory.up_shares > 0:
                up_post_only = bool(snapshot.time_left_sec > self.config.emergency_taker_start_sec)
                up_emergency_price = float(snapshot.up_best_bid or max(0.01, up_ask_price))
                if up_post_only:
                    up_emergency_price = self._maker_clamp(
                        side="SELL",
                        price=up_emergency_price,
                        best_bid=snapshot.up_best_bid,
                        best_ask=snapshot.up_best_ask,
                        tick_size=ctx.tick_size,
                    )
                built["up_bid"] = None
                built["up_ask"], _ = self._make_intent(
                    token=snapshot.up_token_id,
                    side="SELL",
                    price=up_emergency_price,
                    clip_usd=clip_usd,
                    share_cap=max(0.0, inventory.up_shares),
                    ctx=ctx,
                    role="emergency_unwind",
                    post_only=up_post_only,
                    size_override=inventory.up_shares,
                    inventory_effect="helpful" if risk.inventory_side == "up" else "neutral",
                    size_mult=1.0,
                    price_adjust_ticks=0,
                    inventory_backed_sell=True,
                )
            else:
                built["up_bid"] = None
                built["up_ask"] = None
            if inventory.dn_shares > 0:
                dn_post_only = bool(snapshot.time_left_sec > self.config.emergency_taker_start_sec)
                dn_emergency_price = float(snapshot.dn_best_bid or max(0.01, dn_ask_price))
                if dn_post_only:
                    dn_emergency_price = self._maker_clamp(
                        side="SELL",
                        price=dn_emergency_price,
                        best_bid=snapshot.dn_best_bid,
                        best_ask=snapshot.dn_best_ask,
                        tick_size=ctx.tick_size,
                    )
                built["dn_bid"] = None
                built["dn_ask"], _ = self._make_intent(
                    token=snapshot.dn_token_id,
                    side="SELL",
                    price=dn_emergency_price,
                    clip_usd=clip_usd,
                    share_cap=max(0.0, inventory.dn_shares),
                    ctx=ctx,
                    role="emergency_unwind",
                    post_only=dn_post_only,
                    size_override=inventory.dn_shares,
                    inventory_effect="helpful" if risk.inventory_side == "dn" else "neutral",
                    size_mult=1.0,
                    price_adjust_ticks=0,
                    inventory_backed_sell=True,
                )
            else:
                built["dn_bid"] = None
                built["dn_ask"] = None

        helpful_count, harmful_count, neutral_count = self._count_effects(built)
        harmful_blocked = False
        if (
            risk.hard_mode == "none"
            and risk.inventory_side != "flat"
            and risk.soft_mode in {"normal", "inventory_skewed", "defensive"}
            and helpful_count == 0
            and harmful_count > 0
        ):
            for slot, intent in list(built.items()):
                if intent and intent.inventory_effect == "harmful":
                    built[slot] = None
                    suppressed_reasons[slot] = "harmful blocked without helpful viability"
            harmful_blocked = True
            helpful_count, harmful_count, neutral_count = self._count_effects(built)
            reason = "helpful intents not viable; harmful blocked"

        quote_balance_state = self._quote_balance_state(
            built=built,
            helpful_count=helpful_count,
            harmful_count=harmful_count,
            harmful_blocked=harmful_blocked,
        )
        all_quotes_below_min = (
            not any(built.values())
            and bool(suppressed_reasons)
            and all(
                value in {"below_min_order_size", "below_min_order_size_after_helpful_floor"}
                for value in suppressed_reasons.values()
            )
        )
        quote_viability_reason = quote_balance_state
        if harmful_blocked:
            quote_viability_reason = "harmful_only_blocked"
        elif all_quotes_below_min:
            quote_viability_reason = "all_quotes_below_min_size"
            if risk.hard_mode == "none" and risk.inventory_side != "flat":
                reason = "no viable quotes after min-size checks"
        elif helpful_floor_applied:
            quote_viability_reason = "helpful_floor_applied"
        elif neutral_floor_applied:
            quote_viability_reason = "min_viable_floor_applied"
        elif quote_balance_state == "reduced":
            quote_viability_reason = "reduced"
        elif quote_balance_state == "none":
            quote_viability_reason = "none"

        return QuotePlan(
            up_bid=built["up_bid"],
            up_ask=built["up_ask"],
            dn_bid=built["dn_bid"],
            dn_ask=built["dn_ask"],
            regime=regime,
            reason=reason,
            quote_balance_state=quote_balance_state,  # type: ignore[arg-type]
            quote_viability_reason=quote_viability_reason,
            suppressed_reasons=suppressed_reasons,
        )
