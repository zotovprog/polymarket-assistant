from __future__ import annotations

from dataclasses import dataclass
import math
from typing import Literal

from .config import MMConfigV2
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


@dataclass
class QuoteContext:
    tick_size: float
    min_order_size: float


class QuotePolicyV2:
    def __init__(self, config: MMConfigV2):
        self.config = config

    @staticmethod
    def _bps_to_price(bps: float) -> float:
        return float(bps) / 10000.0

    def _clip_usd(self, risk: RiskRegime) -> float:
        base = float(self.config.base_clip_usd)
        if risk.soft_mode == "inventory_skewed":
            return base * 0.7
        if risk.soft_mode == "defensive":
            return base * float(self.config.defensive_size_mult)
        if risk.soft_mode == "unwind":
            return max(1.0, base * 0.5)
        return base

    @staticmethod
    def _buy_size_from_clip(clip_usd: float, price: float) -> float:
        return clip_usd / max(0.01, float(price))

    @staticmethod
    def _sell_size_from_clip(clip_usd: float, price: float) -> float:
        collateral_per_share = max(0.01, 1.0 - float(price))
        return clip_usd / collateral_per_share

    def _spread(self, risk: RiskRegime) -> float:
        base = self._bps_to_price(self.config.base_half_spread_bps)
        if risk.soft_mode == "defensive":
            base *= float(self.config.defensive_spread_mult)
        elif risk.soft_mode == "unwind":
            base *= max(2.0, float(self.config.defensive_spread_mult))
        return min(self._bps_to_price(self.config.max_half_spread_bps), base)

    def _excess_bias(self, inventory: PairInventoryState) -> float:
        hard_cap = max(1e-9, float(self.config.hard_excess_value_ratio) * float(self.config.session_budget_usd))
        signed_excess = inventory.excess_up_value_usd - inventory.excess_dn_value_usd
        return max(-1.0, min(1.0, signed_excess / hard_cap))

    def _make_intent(
        self,
        *,
        token: str,
        side: Literal["BUY", "SELL"],
        price: float,
        clip_usd: float,
        ctx: QuoteContext,
        role: str,
        post_only: bool,
        size_override: float | None = None,
    ) -> QuoteIntent | None:
        if size_override is not None:
            size = size_override
        elif side == "BUY":
            size = self._buy_size_from_clip(clip_usd, price)
        else:
            size = self._sell_size_from_clip(clip_usd, price)
        size = round(max(0.0, float(size)), 2)
        if size < ctx.min_order_size:
            return None
        return QuoteIntent(
            token=token,
            side=side,
            price=_round_price(price, ctx.tick_size),
            size=size,
            quote_role=role,  # type: ignore[arg-type]
            post_only=post_only,
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
        per_quote_budget = min(clip_usd, max(1.0, free_usdc * 0.20))
        bias = self._excess_bias(inventory)
        skew = bias * float(self.config.inventory_skew_strength) * 0.0075
        pair_mid = max(0.01, min(0.99, base_mid - skew))

        up_bid_half = spread
        up_ask_half = spread
        dn_bid_half = spread
        dn_ask_half = spread
        if bias > 0:
            up_bid_half *= 1.4
            up_ask_half *= 0.8
            dn_bid_half *= 0.9
            dn_ask_half *= 1.1
        elif bias < 0:
            dn_bid_half *= 1.4
            dn_ask_half *= 0.8
            up_bid_half *= 0.9
            up_ask_half *= 1.1

        up_bid_price = pair_mid - up_bid_half
        up_ask_price = pair_mid + up_ask_half
        dn_mid = 1.0 - pair_mid
        dn_bid_price = dn_mid - dn_bid_half
        dn_ask_price = dn_mid + dn_ask_half

        if snapshot.up_best_ask is not None:
            up_bid_price = min(up_bid_price, float(snapshot.up_best_ask) - ctx.tick_size)
        if snapshot.up_best_bid is not None:
            up_ask_price = max(up_ask_price, float(snapshot.up_best_bid) + ctx.tick_size)
        if snapshot.dn_best_ask is not None:
            dn_bid_price = min(dn_bid_price, float(snapshot.dn_best_ask) - ctx.tick_size)
        if snapshot.dn_best_bid is not None:
            dn_ask_price = max(dn_ask_price, float(snapshot.dn_best_bid) + ctx.tick_size)

        # Preserve pair complementarity with a one-tick safety gap.
        pair_ceiling = 1.0 - ctx.tick_size
        if up_bid_price + dn_ask_price > pair_ceiling:
            up_bid_price = min(up_bid_price, pair_ceiling - dn_ask_price)
        if dn_bid_price + up_ask_price > pair_ceiling:
            dn_bid_price = min(dn_bid_price, pair_ceiling - up_ask_price)
        up_bid_price = max(ctx.tick_size, up_bid_price)
        dn_bid_price = max(ctx.tick_size, dn_bid_price)

        up_bid = self._make_intent(
            token=snapshot.up_token_id,
            side="BUY",
            price=up_bid_price,
            clip_usd=per_quote_budget,
            ctx=ctx,
            role="base_bid",
            post_only=True,
        )
        dn_bid = self._make_intent(
            token=snapshot.dn_token_id,
            side="BUY",
            price=dn_bid_price,
            clip_usd=per_quote_budget,
            ctx=ctx,
            role="base_bid",
            post_only=True,
        )
        up_ask = self._make_intent(
            token=snapshot.up_token_id,
            side="SELL",
            price=up_ask_price,
            clip_usd=per_quote_budget,
            ctx=ctx,
            role="base_ask",
            post_only=True,
        )
        dn_ask = self._make_intent(
            token=snapshot.dn_token_id,
            side="SELL",
            price=dn_ask_price,
            clip_usd=per_quote_budget,
            ctx=ctx,
            role="base_ask",
            post_only=True,
        )

        if up_bid and dn_ask and up_bid.price + dn_ask.price > (1.0 - ctx.tick_size):
            up_bid.price = _floor_price((1.0 - ctx.tick_size) - dn_ask.price, ctx.tick_size)
        if dn_bid and up_ask and dn_bid.price + up_ask.price > (1.0 - ctx.tick_size):
            dn_bid.price = _floor_price((1.0 - ctx.tick_size) - up_ask.price, ctx.tick_size)

        regime = risk.soft_mode
        reason = risk.reason
        if risk.soft_mode == "unwind":
            if inventory.excess_up_qty > 0:
                up_bid = None
                up_ask = self._make_intent(
                    token=snapshot.up_token_id,
                    side="SELL",
                    price=max(up_ask_price - ctx.tick_size, snapshot.up_best_bid + ctx.tick_size if snapshot.up_best_bid else up_ask_price),
                    clip_usd=per_quote_budget,
                    ctx=ctx,
                    role="unwind",
                    post_only=True,
                    size_override=max(
                        inventory.excess_up_qty,
                        round(self._sell_size_from_clip(per_quote_budget, up_ask_price), 2),
                    ),
                )
            elif inventory.excess_dn_qty > 0:
                dn_bid = None
                dn_ask = self._make_intent(
                    token=snapshot.dn_token_id,
                    side="SELL",
                    price=max(dn_ask_price - ctx.tick_size, snapshot.dn_best_bid + ctx.tick_size if snapshot.dn_best_bid else dn_ask_price),
                    clip_usd=per_quote_budget,
                    ctx=ctx,
                    role="unwind",
                    post_only=True,
                    size_override=max(
                        inventory.excess_dn_qty,
                        round(self._sell_size_from_clip(per_quote_budget, dn_ask_price), 2),
                    ),
                )
        if risk.hard_mode == "emergency_unwind":
            regime = "emergency_unwind"
            if inventory.up_shares > 0:
                up_bid = None
                up_ask = self._make_intent(
                    token=snapshot.up_token_id,
                    side="SELL",
                    price=float(snapshot.up_best_bid or max(0.01, up_ask_price)),
                    clip_usd=per_quote_budget,
                    ctx=ctx,
                    role="emergency_unwind",
                    post_only=bool(snapshot.time_left_sec > self.config.emergency_taker_start_sec),
                    size_override=inventory.up_shares,
                )
            else:
                up_bid = None
                up_ask = None
            if inventory.dn_shares > 0:
                dn_bid = None
                dn_ask = self._make_intent(
                    token=snapshot.dn_token_id,
                    side="SELL",
                    price=float(snapshot.dn_best_bid or max(0.01, dn_ask_price)),
                    clip_usd=per_quote_budget,
                    ctx=ctx,
                    role="emergency_unwind",
                    post_only=bool(snapshot.time_left_sec > self.config.emergency_taker_start_sec),
                    size_override=inventory.dn_shares,
                )
            else:
                dn_bid = None
                dn_ask = None

        return QuotePlan(
            up_bid=up_bid,
            up_ask=up_ask,
            dn_bid=dn_bid,
            dn_ask=dn_ask,
            regime=regime,
            reason=reason,
        )
