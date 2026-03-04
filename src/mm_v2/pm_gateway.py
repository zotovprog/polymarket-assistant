from __future__ import annotations

import asyncio
import math
from typing import Any

from mm.order_manager import OrderManager
from mm.types import MarketInfo, Quote, Fill

from .config import MMConfigV2
from .types import QuoteIntent


class PMGateway:
    def __init__(self, clob_client: Any, config: MMConfigV2):
        self.config = config
        self.transport_config = config.to_mm_config()
        self._supports_naked_sells = bool(hasattr(clob_client, "_orders"))
        if not self._supports_naked_sells:
            # Current live transport path does not support naked conditional SELLs
            # reliably; keep live asks inventory-backed until a verified short path exists.
            self.transport_config.allow_short_sells = False
        self.order_mgr = OrderManager(clob_client, self.transport_config)
        self.market: MarketInfo | None = None

    def set_market(self, market: MarketInfo) -> None:
        self.market = market
        self.order_mgr.set_market_context(
            min_order_size=market.min_order_size,
            token_ids={market.up_token_id, market.dn_token_id},
        )

    async def get_books(self) -> tuple[dict[str, Any], dict[str, Any]]:
        assert self.market is not None
        return await asyncio.gather(
            self.order_mgr.get_full_book(self.market.up_token_id),
            self.order_mgr.get_full_book(self.market.dn_token_id),
        )

    async def get_balances(self) -> tuple[float | None, float | None, float | None, float | None]:
        assert self.market is not None
        up_dn = await self.order_mgr.get_all_token_balances(
            self.market.up_token_id,
            self.market.dn_token_id,
        )
        total_usdc, available_usdc = await self.order_mgr.get_usdc_balances()
        return up_dn[0], up_dn[1], total_usdc, available_usdc

    def active_orders(self) -> dict[str, Quote]:
        return self.order_mgr.active_orders

    def active_order_ids(self) -> list[str]:
        return self.order_mgr.active_order_ids

    def supports_naked_sells(self) -> bool:
        return self._supports_naked_sells

    def sync_paper_prices(self, *, fv_up: float, fv_dn: float, pm_prices: dict[str, float | None]) -> None:
        client = self.order_mgr.client
        if hasattr(client, "set_fair_values") and self.market:
            client.set_fair_values(fv_up, fv_dn, self.market, pm_prices=pm_prices)

    @staticmethod
    def _round_post_only_price(*, side: str, raw_price: float, tick_size: float) -> float:
        tick = max(1e-9, float(tick_size))
        min_price = max(tick, 0.01)
        max_price = min(0.99, 1.0 - tick)
        if side == "BUY":
            price = math.floor((float(raw_price) + 1e-9) / tick) * tick
        else:
            price = math.ceil((float(raw_price) - 1e-9) / tick) * tick
        if max_price < min_price:
            max_price = min_price
        return round(max(min_price, min(max_price, price)), 10)

    def _last_reject_crossed_book(self, intent: QuoteIntent) -> bool:
        stats = self.order_mgr.get_api_error_stats()
        recent = list(stats.get("recent") or [])
        if not recent:
            return False
        last = recent[-1]
        if str(last.get("op") or "") != "place_order":
            return False
        if str(last.get("message") or "").lower().find("crosses book") < 0:
            return False
        token_id = str(last.get("token_id") or "")
        if token_id and not str(intent.token).startswith(token_id):
            return False
        details = last.get("details") or {}
        if str(details.get("side") or "") != intent.side:
            return False
        return True

    async def _retry_post_only_reprice(self, intent: QuoteIntent) -> str | None:
        if not self.market:
            return None
        book = await self.order_mgr.get_full_book(intent.token)
        tick_size = float(self.market.tick_size)
        repriced: float | None = None
        if intent.side == "SELL":
            best_bid = book.get("best_bid")
            if best_bid is None:
                return None
            raw = max(float(intent.price), float(best_bid) + tick_size)
            repriced = self._round_post_only_price(side="SELL", raw_price=raw, tick_size=tick_size)
            if repriced <= float(best_bid):
                repriced = self._round_post_only_price(side="SELL", raw_price=float(best_bid) + (2.0 * tick_size), tick_size=tick_size)
        else:
            best_ask = book.get("best_ask")
            if best_ask is None:
                return None
            raw = min(float(intent.price), float(best_ask) - tick_size)
            repriced = self._round_post_only_price(side="BUY", raw_price=raw, tick_size=tick_size)
            if repriced >= float(best_ask):
                repriced = self._round_post_only_price(side="BUY", raw_price=float(best_ask) - (2.0 * tick_size), tick_size=tick_size)
        if repriced is None or abs(repriced - float(intent.price)) < 1e-9:
            return None
        quote = Quote(
            side=intent.side,
            token_id=intent.token,
            price=float(repriced),
            size=float(intent.size),
        )
        return await self.order_mgr.place_order(quote, post_only=True, fallback_taker=False)

    async def place_intent(self, intent: QuoteIntent) -> str | None:
        quote = Quote(
            side=intent.side,
            token_id=intent.token,
            price=float(intent.price),
            size=float(intent.size),
        )
        order_id = await self.order_mgr.place_order(quote, post_only=intent.post_only, fallback_taker=False)
        if order_id is not None or not bool(intent.post_only):
            return order_id
        if not self._last_reject_crossed_book(intent):
            return None
        return await self._retry_post_only_reprice(intent)

    async def cancel(self, order_id: str) -> bool:
        return await self.order_mgr.cancel_order(order_id)

    async def cancel_all(self) -> int:
        return await self.order_mgr.cancel_all(force_exchange=True)

    async def check_fills(self) -> list[Fill]:
        return await self.order_mgr.check_fills()

    def api_error_stats(self) -> dict[str, Any]:
        return self.order_mgr.get_api_error_stats()

    async def ensure_sell_allowances(self) -> None:
        if not self.market:
            return
        await asyncio.gather(
            self.order_mgr.ensure_sell_allowance(self.market.up_token_id, required_shares=0.0),
            self.order_mgr.ensure_sell_allowance(self.market.dn_token_id, required_shares=0.0),
        )
