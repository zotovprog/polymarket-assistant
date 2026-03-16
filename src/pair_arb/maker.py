"""Maker pair arb — posts persistent BUY limits on both sides, merges when filled."""
from __future__ import annotations

import asyncio
import logging
import math
import time

from mm_shared.types import Quote

from .config import PairArbConfig
from .types import ArbMarket

log = logging.getLogger(__name__)


class MakerArbManager:
    """Manages persistent BUY limit orders on both sides of a single market.

    Flow:
      1. Fetch order books → compute target bid prices
      2. Post BUY UP + BUY DN at best-bid (post_only=True, 0 fee)
      3. Monitor fills via token balance check
      4. When min(up_bal, dn_bal) >= min_clip → merge for $1.00
      5. Reprice if book moves significantly
    """

    REPRICE_THRESHOLD = 0.015  # Reprice if best_bid shifts by >1.5¢

    def __init__(self, market: ArbMarket, order_mgr, config: PairArbConfig):
        self.market = market
        self.order_mgr = order_mgr
        self.config = config

        # Active order tracking
        self.up_order_id: str | None = None
        self.dn_order_id: str | None = None
        self.up_price: float = 0.0
        self.dn_price: float = 0.0
        self.up_size: float = 0.0
        self.dn_size: float = 0.0

        # Stats
        self.orders_posted: int = 0
        self.reprices: int = 0
        self.last_post_ts: float = 0.0
        self.last_tick_ts: float = 0.0

    async def tick(self) -> dict | None:
        """One cycle: ensure orders posted at profitable levels, reprice if needed.

        Returns dict with info if orders were posted/repriced, else None.
        """
        self.last_tick_ts = time.time()

        # 1. Fetch books
        try:
            up_book = await self.order_mgr.get_full_book(self.market.up_token_id)
            dn_book = await self.order_mgr.get_full_book(self.market.dn_token_id)
        except Exception as e:
            log.debug("Book fetch failed for %s: %s", self.market.scope, e)
            return None

        best_bid_up = up_book.get("best_bid")
        best_bid_dn = dn_book.get("best_bid")
        best_ask_up = up_book.get("best_ask")
        best_ask_dn = dn_book.get("best_ask")

        if not best_bid_up or not best_bid_dn:
            return None

        # 2. Check profitability at bid levels
        target_up = best_bid_up
        target_dn = best_bid_dn
        total_cost = target_up + target_dn

        gas_per_share = self.config.gas_cost_usd / max(self.config.min_clip_shares, 1)
        min_profit = self.config.min_profit_bps / 10000.0

        if total_cost >= 1.0 - min_profit - gas_per_share:
            # Not profitable even at bid — cancel and wait
            if self.up_order_id or self.dn_order_id:
                log.debug("Maker %s: not profitable (bids %.2f+%.2f=%.4f), cancelling",
                         self.market.scope, target_up, target_dn, total_cost)
                await self._cancel_all()
            return None

        profit_per_pair = 1.0 - total_cost - gas_per_share

        # 3. Determine clip size — ensure each leg meets PM minimum notional ($1.00)
        clip = self.config.max_clip_shares
        min_price = min(target_up, target_dn)
        if min_price > 0:
            min_clip_for_notional = math.ceil(1.0 / min_price)
            if min_clip_for_notional > clip:
                clip = float(min_clip_for_notional)
                log.debug("Clip raised to %.0f for %s (min_price=%.2f, notional=$%.2f)",
                          clip, self.market.scope, min_price, clip * min_price)

        # 4. Check if repricing needed
        need_reprice_up = (
            self.up_order_id is None
            or abs(self.up_price - target_up) >= self.REPRICE_THRESHOLD
        )
        need_reprice_dn = (
            self.dn_order_id is None
            or abs(self.dn_price - target_dn) >= self.REPRICE_THRESHOLD
        )

        # --- Balance pre-check: ensure USDC covers BOTH legs ---
        if need_reprice_up or need_reprice_dn:
            total_needed = target_up * clip + target_dn * clip
            try:
                available = await self.order_mgr.get_usdc_available_balance(force_refresh=True)
            except Exception:
                available = 0.0

            # Collateral freed when we cancel our own orders for reprice
            freeable = 0.0
            if need_reprice_up and self.up_order_id:
                freeable += self.up_size * self.up_price
            if need_reprice_dn and self.dn_order_id:
                freeable += self.dn_size * self.dn_price

            effective = available + freeable
            if total_needed > effective + 0.50:  # $0.50 tolerance
                # Try reducing clip
                max_clip = math.floor((effective / (target_up + target_dn)) * 100) / 100
                if max_clip < self.config.min_clip_shares:
                    log.warning(
                        "Balance pre-check SKIP %s: need $%.2f, available $%.2f (incl freeable $%.2f), min_clip=%.0f",
                        self.market.scope, total_needed, effective, freeable, self.config.min_clip_shares,
                    )
                    return None
                clip = max_clip
                total_needed = target_up * clip + target_dn * clip
                log.info(
                    "Balance pre-check: reduced clip to %.1f for %s (need $%.2f, available $%.2f)",
                    clip, self.market.scope, total_needed, effective,
                )

        result = {"scope": self.market.scope, "action": "none"}

        # 5+6. Cancel stale orders, then place UP+DN in parallel
        if need_reprice_up and self.up_order_id:
            await self._cancel_order(self.up_order_id)
            self.up_order_id = None
            self.reprices += 1
        if need_reprice_dn and self.dn_order_id:
            await self._cancel_order(self.dn_order_id)
            self.dn_order_id = None
            self.reprices += 1

        up_quote = None
        dn_quote = None
        if need_reprice_up:
            up_quote = Quote(
                side="BUY",
                token_id=self.market.up_token_id,
                price=target_up,
                size=clip,
                order_context="pair_arb_maker",
            )
        if need_reprice_dn:
            dn_quote = Quote(
                side="BUY",
                token_id=self.market.dn_token_id,
                price=target_dn,
                size=clip,
                order_context="pair_arb_maker",
            )

        # Place both legs in parallel for speed
        if up_quote and dn_quote:
            up_result, dn_result = await asyncio.gather(
                self._safe_place(up_quote),
                self._safe_place(dn_quote),
            )
            self.up_order_id = up_result
            self.dn_order_id = dn_result
        elif up_quote:
            self.up_order_id = await self._safe_place(up_quote)
        elif dn_quote:
            self.dn_order_id = await self._safe_place(dn_quote)

        if need_reprice_up:
            self.up_price = target_up
            self.up_size = clip
        if need_reprice_dn:
            self.dn_price = target_dn
            self.dn_size = clip

        # --- Orphan cleanup: if one leg failed, cancel the other ---
        if need_reprice_up or need_reprice_dn:
            up_ok = self.up_order_id is not None
            dn_ok = self.dn_order_id is not None
            if up_ok and not dn_ok:
                log.warning(
                    "Maker %s: DN leg failed, cancelling orphaned UP order %s",
                    self.market.scope, self.up_order_id[:12],
                )
                await self._cancel_order(self.up_order_id)
                self.up_order_id = None
                return {"scope": self.market.scope, "action": "orphan_cleanup", "failed_leg": "dn"}
            elif dn_ok and not up_ok:
                log.warning(
                    "Maker %s: UP leg failed, cancelling orphaned DN order %s",
                    self.market.scope, self.dn_order_id[:12],
                )
                await self._cancel_order(self.dn_order_id)
                self.dn_order_id = None
                return {"scope": self.market.scope, "action": "orphan_cleanup", "failed_leg": "up"}

        if (need_reprice_up or need_reprice_dn) and self.up_order_id and self.dn_order_id:
            self.orders_posted += 1
            self.last_post_ts = time.time()
            result["action"] = "posted"
            result["up_price"] = target_up
            result["dn_price"] = target_dn
            result["total"] = round(total_cost, 4)
            result["profit_per_pair"] = round(profit_per_pair, 4)
            result["clip"] = clip
            log.info(
                "Maker %s: BUY UP@%.2f + DN@%.2f = %.4f (profit=%.4f/pair x%.0f)",
                self.market.scope, target_up, target_dn, total_cost,
                profit_per_pair, clip,
            )

        # Also check taker opportunity (asks sum < $1.00)
        if best_ask_up and best_ask_dn:
            ask_total = best_ask_up + best_ask_dn
            if ask_total < 1.0 - min_profit - gas_per_share:
                result["taker_opportunity"] = True
                result["ask_total"] = round(ask_total, 4)
                log.info(
                    "TAKER ARB %s: asks %.2f+%.2f=%.4f (profit=%.4f)",
                    self.market.scope, best_ask_up, best_ask_dn, ask_total,
                    1.0 - ask_total - gas_per_share,
                )

        return result if result["action"] != "none" else None

    async def cancel_all(self) -> None:
        """Cancel all active orders."""
        await self._cancel_all()

    async def _cancel_all(self) -> None:
        if self.up_order_id:
            await self._cancel_order(self.up_order_id)
            self.up_order_id = None
        if self.dn_order_id:
            await self._cancel_order(self.dn_order_id)
            self.dn_order_id = None

    async def _cancel_order(self, order_id: str) -> None:
        try:
            await self.order_mgr.cancel_order(order_id)
        except Exception as e:
            log.debug("Cancel failed for %s: %s", order_id[:12], e)

    async def _safe_place(self, quote: Quote) -> str | None:
        try:
            result = await self.order_mgr.place_order(quote, post_only=True)
            if isinstance(result, str) and result:
                log.info("Maker order placed: %s %s@%.2f x%.1f → %s",
                         quote.side, quote.token_id[:8], quote.price, quote.size,
                         result[:12])
                return result
            log.warning("Maker order returned non-string: %s %s@%.2f → %r",
                       quote.side, quote.token_id[:8], quote.price, result)
            return None
        except Exception as e:
            log.warning("Maker order exception: %s %s@%.2f → %s",
                       quote.side, quote.token_id[:8], quote.price, e)
            return None

    def to_dict(self) -> dict:
        return {
            "scope": self.market.scope,
            "up_order": self.up_order_id[:12] if self.up_order_id else None,
            "dn_order": self.dn_order_id[:12] if self.dn_order_id else None,
            "up_price": self.up_price,
            "dn_price": self.dn_price,
            "total_cost": round(self.up_price + self.dn_price, 4),
            "profit_per_pair": round(1.0 - self.up_price - self.dn_price, 4) if self.up_price and self.dn_price else 0,
            "orders_posted": self.orders_posted,
            "reprices": self.reprices,
            "has_orders": bool(self.up_order_id or self.dn_order_id),
            "has_both_orders": bool(self.up_order_id and self.dn_order_id),
        }
