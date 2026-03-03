from __future__ import annotations

import asyncio
from typing import Any

from mm.order_manager import OrderManager
from mm.types import MarketInfo, Quote, Fill

from .config import MMConfigV2
from .types import QuoteIntent


class PMGateway:
    def __init__(self, clob_client: Any, config: MMConfigV2):
        self.config = config
        self.transport_config = config.to_mm_config()
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

    def sync_paper_prices(self, *, fv_up: float, fv_dn: float, pm_prices: dict[str, float | None]) -> None:
        client = self.order_mgr.client
        if hasattr(client, "set_fair_values") and self.market:
            client.set_fair_values(fv_up, fv_dn, self.market, pm_prices=pm_prices)

    async def place_intent(self, intent: QuoteIntent) -> str | None:
        quote = Quote(
            side=intent.side,
            token_id=intent.token,
            price=float(intent.price),
            size=float(intent.size),
        )
        return await self.order_mgr.place_order(quote, post_only=intent.post_only, fallback_taker=False)

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
