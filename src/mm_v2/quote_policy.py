from __future__ import annotations

from dataclasses import dataclass
import math
from typing import Any, Literal

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

    def _harmful_buy_brake_mult(
        self,
        *,
        inventory: PairInventoryState,
        risk: RiskRegime,
    ) -> float:
        """Throttle harmful BUY sizing as excess approaches hard cap.

        This keeps two-sided maker MM active, but reduces the speed of inventory
        expansion on the harmful side before we are forced into protective modes.
        """
        if risk.soft_mode not in {"normal", "inventory_skewed"}:
            return 1.0
        excess_value = max(0.0, float(inventory.excess_value_usd))
        budget = max(0.01, float(self.config.session_budget_usd))
        soft_cap = max(0.01, float(self.config.soft_excess_value_ratio) * budget)
        defensive_cap = max(soft_cap, float(self.config.defensive_excess_value_ratio) * budget)
        hard_cap = max(defensive_cap, float(self.config.effective_hard_excess_value_ratio()) * budget)
        # Start throttling around soft-cap so harmful BUY flow does not keep
        # accelerating excess into hard/unwind territory.
        brake_start = max(2.0, soft_cap)
        if excess_value <= brake_start + 1e-9:
            return 1.0
        if hard_cap <= brake_start + 1e-9:
            return 0.20
        progress = (excess_value - brake_start) / max(1e-9, hard_cap - brake_start)
        progress = max(0.0, min(1.0, progress))
        return max(0.20, 1.0 - 0.80 * progress)

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
    def _maker_safe_bid_price(*, best_ask: float | None, tick_size: float) -> float | None:
        if best_ask is None:
            return None
        safe = _floor_price(float(best_ask) - tick_size, tick_size)
        if safe < 0.01 or safe >= float(best_ask) - 1e-9:
            return None
        return float(safe)

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
        dual_bid_guard_headroom_usd = max(1.0, free_usdc)
        min_viable_clip_usd = self._min_viable_clip_usd(snapshot, ctx)
        harmful_buy_guard_usd = float(self.config.effective_harmful_buy_suppress_usd())
        budget_usd = max(0.01, float(self.config.session_budget_usd))
        soft_cap_usd = max(0.01, float(self.config.soft_excess_value_ratio) * budget_usd)
        defensive_cap_usd = max(0.01, float(self.config.defensive_excess_value_ratio) * budget_usd)
        hard_cap_usd = max(
            defensive_cap_usd,
            float(self.config.effective_hard_excess_value_ratio()) * budget_usd,
        )
        pre_protective_harmful_buy_guard_usd = max(2.0, 0.60 * defensive_cap_usd)
        harmful_side_floor_block_usd = max(
            pre_protective_harmful_buy_guard_usd,
            0.80 * defensive_cap_usd,
        )
        pressure = max(0.0, min(1.0, float(risk.inventory_pressure_abs)))
        mid_shift = float(risk.inventory_pressure_signed) * float(self.config.inventory_skew_strength) * 0.0025
        pair_mid = max(0.01, min(0.99, base_mid - mid_shift))
        pair_reference_price = self._pair_reference_price(snapshot)
        outside_near_expiry = float(snapshot.time_left_sec) > float(self.config.unwind_window_sec)

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
        bid_slot_meta: dict[str, dict[str, Any]] = {}
        helpful_floor_applied = False
        neutral_floor_applied = False
        harmful_buy_brake_hits = 0

        for slot, (side, token, base_price, best_bid, best_ask, role) in raw_quotes.items():
            effect = self._classify_inventory_effect(
                token=token,
                side=side,
                inventory_side=risk.inventory_side,
                up_token_id=snapshot.up_token_id,
                dn_token_id=snapshot.dn_token_id,
            )
            if (
                risk.soft_mode in {"normal", "inventory_skewed"}
                and risk.target_soft_mode in {"defensive", "unwind"}
                and side == "BUY"
                and effect == "harmful"
                and float(inventory.excess_value_usd) >= pre_protective_harmful_buy_guard_usd
            ):
                built[slot] = None
                suppressed_reasons[slot] = "harmful_buy_blocked_pre_protective"
                continue
            if (
                risk.soft_mode in {"normal", "inventory_skewed"}
                and side == "BUY"
                and effect == "harmful"
                and float(risk.drawdown_pct_budget) <= 0.25
                and (
                    risk.target_soft_mode in {"defensive", "unwind"}
                    or float(inventory.excess_value_usd) >= pre_protective_harmful_buy_guard_usd
                )
            ):
                built[slot] = None
                suppressed_reasons[slot] = "harmful_buy_blocked_drawdown"
                continue
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
            if side == "BUY":
                bid_slot_meta[slot] = {
                    "token": token,
                    "best_bid": best_bid,
                    "best_ask": best_ask,
                    "role": role,
                    "effect": effect,
                    "size_mult": float(self._size_multiplier(effect, pressure)),
                    "ticks": int(ticks),
                    "adjusted_price": float(adjusted_price),
                }
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
            harmful_buy_brake_mult = 1.0
            if side == "BUY" and effect == "harmful":
                harmful_buy_brake_mult = self._harmful_buy_brake_mult(
                    inventory=inventory,
                    risk=risk,
                )
                side_excess_qty = 0.0
                side_mark_price = pair_reference_price
                if token == snapshot.up_token_id:
                    side_excess_qty = max(0.0, float(inventory.excess_up_qty))
                    side_mark_price = max(
                        0.05,
                        float(snapshot.pm_mid_up or snapshot.fv_up or pair_reference_price),
                    )
                elif token == snapshot.dn_token_id:
                    side_excess_qty = max(0.0, float(inventory.excess_dn_qty))
                    side_mark_price = max(
                        0.05,
                        float(snapshot.pm_mid_dn or snapshot.fv_dn or pair_reference_price),
                    )
                soft_cap_qty = soft_cap_usd / max(0.05, side_mark_price)
                hard_cap_qty = hard_cap_usd / max(0.05, side_mark_price)
                qty_brake_start = max(float(ctx.min_order_size), 0.70 * soft_cap_qty)
                if side_excess_qty > qty_brake_start + 1e-9:
                    if hard_cap_qty <= qty_brake_start + 1e-9:
                        qty_brake_mult = 0.20
                    else:
                        qty_progress = (side_excess_qty - qty_brake_start) / max(
                            1e-9,
                            hard_cap_qty - qty_brake_start,
                        )
                        qty_progress = max(0.0, min(1.0, qty_progress))
                        qty_brake_mult = max(0.20, 1.0 - 0.80 * qty_progress)
                    harmful_buy_brake_mult = min(harmful_buy_brake_mult, float(qty_brake_mult))
                if harmful_buy_brake_mult < 0.999:
                    harmful_buy_brake_hits += 1
            if side == "BUY":
                nominal_quote_clip_usd = (
                    min(clip_usd, buy_headroom_usd) * size_mult * harmful_buy_brake_mult
                )
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
            if (
                side == "BUY"
                and outside_near_expiry
                and risk.hard_mode == "none"
                and risk.soft_mode in {"normal", "inventory_skewed"}
            ):
                maker_safe_bid_price = self._maker_safe_bid_price(
                    best_ask=best_ask,
                    tick_size=ctx.tick_size,
                )
                if maker_safe_bid_price is not None:
                    side_min_clip_usd = float(ctx.min_order_size) * float(maker_safe_bid_price)
                    side_floor_headroom_usd = buy_headroom_usd * max(1.0, size_mult)
                    harmful_brake_floor_blocked = (
                        effect == "harmful"
                        and harmful_buy_brake_mult < 0.999
                        and float(inventory.excess_value_usd) >= harmful_side_floor_block_usd
                    )
                    if (not harmful_brake_floor_blocked) and side_min_clip_usd <= side_floor_headroom_usd + 1e-9:
                        effective_clip_usd = max(effective_clip_usd, side_min_clip_usd)
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

        # Dual-bid core guard: outside near-expiry in normal/skewed mode we should
        # avoid sustained one-sided BUY quoting when the opposite bid is recoverable.
        if (
            outside_near_expiry
            and risk.hard_mode == "none"
            and risk.soft_mode in {"normal", "inventory_skewed"}
        ):
            up_bid_active = built["up_bid"] is not None
            dn_bid_active = built["dn_bid"] is not None
            if int(up_bid_active) + int(dn_bid_active) == 1:
                missing_slot = "up_bid" if not up_bid_active else "dn_bid"
                existing_reason = str(suppressed_reasons.get(missing_slot) or "")
                prior_reason = existing_reason
                blocked_by_high_skew = existing_reason == "harmful_buy_blocked_high_skew"
                blocked_by_pre_protective = existing_reason == "harmful_buy_blocked_pre_protective"
                blocked_by_harmful_skew = blocked_by_high_skew or blocked_by_pre_protective
                if existing_reason in {
                    "harmful_suppressed_in_defensive",
                    "harmful_suppressed_in_unwind",
                    "target_pair_ratio_cap",
                    "maker_cross_guard",
                    "live_requires_inventory_backed_sell",
                    "harmful_buy_blocked_drawdown",
                }:
                    pass
                else:
                    meta = bid_slot_meta.get(missing_slot)
                    if not meta:
                        fallback_quote = raw_quotes.get(missing_slot)
                        if fallback_quote:
                            (
                                _fallback_side,
                                fallback_token,
                                fallback_base_price,
                                fallback_best_bid,
                                fallback_best_ask,
                                fallback_role,
                            ) = fallback_quote
                            fallback_effect = self._classify_inventory_effect(
                                token=fallback_token,
                                side="BUY",
                                inventory_side=risk.inventory_side,
                                up_token_id=snapshot.up_token_id,
                                dn_token_id=snapshot.dn_token_id,
                            )
                            fallback_ticks = self._price_adjust_ticks(fallback_effect, pressure)
                            fallback_adjusted = float(fallback_base_price)
                            if fallback_effect == "helpful":
                                fallback_adjusted += ctx.tick_size * fallback_ticks
                            elif fallback_effect == "harmful":
                                fallback_adjusted -= ctx.tick_size * fallback_ticks
                            fallback_adjusted = self._maker_clamp(
                                side="BUY",
                                price=fallback_adjusted,
                                best_bid=fallback_best_bid,
                                best_ask=fallback_best_ask,
                                tick_size=ctx.tick_size,
                            )
                            meta = {
                                "token": fallback_token,
                                "best_bid": fallback_best_bid,
                                "best_ask": fallback_best_ask,
                                "role": fallback_role,
                                "effect": fallback_effect,
                                "size_mult": float(self._size_multiplier(fallback_effect, pressure)),
                                "ticks": int(fallback_ticks),
                                "adjusted_price": float(fallback_adjusted),
                            }
                    if (
                        meta
                        and str(meta.get("effect") or "") == "harmful"
                        and risk.soft_mode in {"normal", "inventory_skewed"}
                        and risk.target_soft_mode in {"defensive", "unwind"}
                        and float(inventory.excess_value_usd) >= pre_protective_harmful_buy_guard_usd
                    ):
                        suppressed_reasons[missing_slot] = prior_reason or "dual_bid_guard_viability"
                        meta = None
                    if not meta:
                        if prior_reason:
                            suppressed_reasons[missing_slot] = prior_reason
                        else:
                            suppressed_reasons[missing_slot] = "dual_bid_guard_viability"
                    else:
                        maker_safe_bid_price = self._maker_safe_bid_price(
                            best_ask=meta.get("best_ask"),
                            tick_size=ctx.tick_size,
                        )
                        if maker_safe_bid_price is None:
                            suppressed_reasons[missing_slot] = "dual_bid_guard_market"
                        else:
                            side_min_clip_usd = float(ctx.min_order_size) * float(maker_safe_bid_price)
                            side_floor_headroom_usd = dual_bid_guard_headroom_usd * max(
                                1.0,
                                float(meta.get("size_mult") or 1.0),
                            )
                            if side_min_clip_usd > side_floor_headroom_usd + 1e-9:
                                suppressed_reasons[missing_slot] = "dual_bid_guard_headroom"
                            elif (
                                risk.soft_mode == "inventory_skewed"
                                and str(meta.get("effect") or "") == "harmful"
                                and float(inventory.excess_value_usd) >= harmful_buy_guard_usd
                                and not blocked_by_harmful_skew
                            ):
                                suppressed_reasons[missing_slot] = "dual_bid_guard_viability"
                            else:
                                guard_price = min(
                                    float(meta.get("adjusted_price") or maker_safe_bid_price),
                                    float(maker_safe_bid_price),
                                )
                                guard_clip_usd = side_min_clip_usd
                                guard_share_cap = max(
                                    max(0.0, side_min_clip_usd / pair_reference_price),
                                    # Side-aware cap: recover missing dual-bid on cheap side
                                    # without opening large endpoint inventory expansion.
                                    float(ctx.min_order_size) * float(HELPFUL_MIN_PROMOTION_MULT),
                                )
                                allow_guard_intent = True
                                if blocked_by_harmful_skew:
                                    # Keep recovery passive near the high-skew guard:
                                    # only re-arm dual-bid with a deeper maker price and
                                    # minimal economically viable size.
                                    if blocked_by_pre_protective:
                                        suppressed_reasons[missing_slot] = prior_reason or "dual_bid_guard_viability"
                                        allow_guard_intent = False
                                    recovery_limit = harmful_buy_guard_usd * 1.15
                                    if allow_guard_intent and float(inventory.excess_value_usd) > recovery_limit:
                                        suppressed_reasons[missing_slot] = prior_reason or "dual_bid_guard_viability"
                                        allow_guard_intent = False
                                    if allow_guard_intent:
                                        conservative_ticks = max(6, int(meta.get("ticks") or 0) + 4)
                                        guard_price = max(
                                            0.01,
                                            _floor_price(
                                                float(guard_price) - (float(ctx.tick_size) * conservative_ticks),
                                                ctx.tick_size,
                                            ),
                                        )
                                        guard_clip_usd = max(
                                            0.01,
                                            float(ctx.min_order_size) * float(guard_price) * 1.02,
                                        )
                                        guard_share_cap = max(
                                            float(ctx.min_order_size),
                                            float(guard_clip_usd) / max(0.01, pair_reference_price),
                                        )
                                if allow_guard_intent:
                                    intent, suppressed = self._make_intent(
                                        token=str(meta.get("token")),
                                        side="BUY",
                                        price=guard_price,
                                        clip_usd=guard_clip_usd,
                                        share_cap=guard_share_cap,
                                        ctx=ctx,
                                        role=str(meta.get("role") or "base_bid"),
                                        post_only=True,
                                        inventory_effect=str(meta.get("effect") or "neutral"),  # type: ignore[arg-type]
                                        size_mult=float(meta.get("size_mult") or 1.0),
                                        price_adjust_ticks=int(meta.get("ticks") or 0),
                                    )
                                    if intent is None:
                                        if blocked_by_harmful_skew and prior_reason:
                                            suppressed_reasons[missing_slot] = prior_reason
                                        else:
                                            suppressed_reasons[missing_slot] = "dual_bid_guard_viability"
                                    else:
                                        intent.price = min(float(intent.price), float(maker_safe_bid_price))
                                        built[missing_slot] = intent
                                        suppressed_reasons[missing_slot] = "dual_bid_guard_applied"

        regime = risk.soft_mode
        reason = risk.reason
        if risk.hard_mode == "emergency_unwind":
            regime = "emergency_unwind"
            emergency_taker_forced = bool(getattr(risk, "emergency_taker_forced", False))
            if inventory.up_shares > 0:
                up_post_only = bool(
                    snapshot.time_left_sec > self.config.emergency_taker_start_sec
                ) and not emergency_taker_forced
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
                dn_post_only = bool(
                    snapshot.time_left_sec > self.config.emergency_taker_start_sec
                ) and not emergency_taker_forced
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
            harmful_buy_brake_active=bool(harmful_buy_brake_hits > 0),
            harmful_buy_brake_hits=int(harmful_buy_brake_hits),
        )
