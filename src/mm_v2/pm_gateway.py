from __future__ import annotations

import asyncio
import logging
import math
from typing import Any

from mm_shared.order_manager import OrderManager
from mm_shared.types import MarketInfo, Quote, Fill

from .config import MMConfigV2
from .types import QuoteIntent

log = logging.getLogger("mm.v2.gateway")


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

    async def get_wallet_balances(
        self,
        *,
        reference_balances: tuple[float, float] | None = None,
    ) -> tuple[float | None, float | None, float | None, float | None]:
        assert self.market is not None
        references: dict[str, float] | None = None
        if reference_balances is not None:
            references = {
                self.market.up_token_id: float(reference_balances[0]),
                self.market.dn_token_id: float(reference_balances[1]),
            }
        up_dn = await self.order_mgr.get_all_token_balances(
            self.market.up_token_id,
            self.market.dn_token_id,
            reference_balances=references,
        )
        total_usdc, available_usdc = await self.order_mgr.get_usdc_balances()
        return up_dn[0], up_dn[1], total_usdc, available_usdc

    async def get_sellable_balances(
        self,
        *,
        reference_balances: tuple[float, float] | None = None,
    ) -> tuple[float | None, float | None]:
        assert self.market is not None
        up_reference = None
        dn_reference = None
        if reference_balances is not None:
            up_reference = float(reference_balances[0])
            dn_reference = float(reference_balances[1])
        up_sellable, dn_sellable = await asyncio.gather(
            self.order_mgr.get_sellable_token_balance(
                self.market.up_token_id,
                reference_shares=up_reference,
            ),
            self.order_mgr.get_sellable_token_balance(
                self.market.dn_token_id,
                reference_shares=dn_reference,
            ),
        )
        return up_sellable, dn_sellable

    def sell_release_lag_state(self) -> dict[str, Any]:
        assert self.market is not None
        return self.order_mgr.get_sell_release_lag_snapshot(
            up_token_id=self.market.up_token_id,
            dn_token_id=self.market.dn_token_id,
        )

    def balance_fetch_health_state(self) -> dict[str, Any]:
        return self.order_mgr.get_balance_fetch_health_snapshot()

    def marketability_state(self) -> dict[str, Any]:
        if not self.market:
            return {
                "active": False,
                "reason": "",
                "collateral_warning_hits_60s": 0,
                "sell_skip_cooldown_hits_60s": 0,
                "sell_place_attempts_60s": 0,
                "execution_churn_ratio_60s": 0.0,
                "up_active": False,
                "up_reason": "",
                "up_collateral_warning_hits_60s": 0,
                "up_sell_skip_cooldown_hits_60s": 0,
                "up_collateral_warning_streak": 0,
                "up_sell_skip_cooldown_streak": 0,
                "up_sell_place_attempts_60s": 0,
                "up_execution_churn_ratio_60s": 0.0,
                "dn_active": False,
                "dn_reason": "",
                "dn_collateral_warning_hits_60s": 0,
                "dn_sell_skip_cooldown_hits_60s": 0,
                "dn_collateral_warning_streak": 0,
                "dn_sell_skip_cooldown_streak": 0,
                "dn_sell_place_attempts_60s": 0,
                "dn_execution_churn_ratio_60s": 0.0,
            }
        return self.order_mgr.get_marketability_snapshot(
            up_token_id=self.market.up_token_id,
            dn_token_id=self.market.dn_token_id,
        )

    async def get_balances(self) -> tuple[float | None, float | None, float | None, float | None]:
        """Backward-compatible alias for wallet balances."""
        return await self.get_wallet_balances()

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

    def terminal_liquidation_order_ids(self) -> list[str]:
        return [
            order_id
            for order_id, quote in self.order_mgr.active_orders.items()
            if str(getattr(quote, "order_context", "quote") or "quote") == "terminal_liquidation"
        ]

    async def cancel_terminal_liquidation_orders(self) -> int:
        return await self.order_mgr.cancel_orders_batch(self.terminal_liquidation_order_ids())

    async def run_terminal_liquidation_step(
        self,
        *,
        round_idx: int = 0,
        cancel_existing: bool = True,
    ) -> dict[str, Any]:
        if not self.market:
            return {
                "attempted_orders": 0,
                "placed_orders": 0,
                "remaining_up": 0.0,
                "remaining_dn": 0.0,
                "done": True,
                "reason": "no_market",
                "placed_order_ids": [],
                "cancelled_orders": 0,
            }

        cancelled_orders = 0
        if cancel_existing:
            await self.order_mgr.check_fills()
            cancelled_orders = await self.cancel_terminal_liquidation_orders()

        attempted_orders = 0
        placed_orders = 0
        placed_order_ids: list[str] = []
        had_material_inventory = False
        tick = max(0.0001, float(self.market.tick_size or 0.01))
        min_size = max(0.0, float(self.market.min_order_size or 0.0))

        up_owned, dn_owned, _, _ = await self.get_wallet_balances()
        up_sellable, dn_sellable = await self.get_sellable_balances(
            reference_balances=(
                float(up_owned or 0.0),
                float(dn_owned or 0.0),
            ),
        )
        for token_id, raw_sellable, raw_owned in (
            (self.market.up_token_id, up_sellable, up_owned),
            (self.market.dn_token_id, dn_sellable, dn_owned),
        ):
            sellable = max(0.0, float(raw_sellable or 0.0))
            owned = max(0.0, float(raw_owned or 0.0))
            size = math.floor(max(sellable, owned) * 100.0) / 100.0
            if size < min_size:
                continue
            had_material_inventory = True
            book = await self.order_mgr.get_full_book(token_id)
            best_bid_raw = book.get("best_bid")
            try:
                best_bid = float(best_bid_raw)
            except Exception:
                best_bid = 0.0
            if best_bid <= 0:
                continue
            discount_ticks = 1 + (2 * int(round_idx))
            price = max(0.01, round(best_bid - (tick * discount_ticks), 10))
            quote = Quote(
                side="SELL",
                token_id=token_id,
                price=float(price),
                size=float(size),
                order_context="terminal_liquidation",
            )
            attempted_orders += 1
            order_id = await self.order_mgr.place_order(
                quote,
                post_only=False,
                fallback_taker=False,
                ignore_sell_cooldowns=True,
                ignore_recent_cancelled_reserve=True,
            )
            if order_id:
                placed_orders += 1
                placed_order_ids.append(order_id)
                log.info(
                    "Terminal liquidation placed SELL %s %.2f@%.2f id=%s round=%d",
                    token_id[:8],
                    size,
                    price,
                    order_id[:12],
                    int(round_idx),
                )

        up_remaining, dn_remaining, total_usdc, _ = await self.get_wallet_balances()
        rem_up = max(0.0, float(up_remaining or 0.0))
        rem_dn = max(0.0, float(dn_remaining or 0.0))
        done = rem_up < min_size and rem_dn < min_size
        reason = "ok"
        if not done and had_material_inventory and placed_orders == 0:
            reason = "no_book_liquidity"
        return {
            "attempted_orders": attempted_orders,
            "placed_orders": placed_orders,
            "remaining_up": round(rem_up, 4),
            "remaining_dn": round(rem_dn, 4),
            "wallet_total_usdc": round(max(0.0, float(total_usdc or 0.0)), 4),
            "done": bool(done),
            "reason": reason,
            "placed_order_ids": placed_order_ids,
            "cancelled_orders": int(cancelled_orders),
        }

    async def emergency_flatten_on_stop(
        self,
        *,
        rounds: int = 5,
        round_delay_sec: float = 1.0,
    ) -> dict[str, Any]:
        """Best-effort forced inventory unwind used by manual stop().

        This path is intentionally taker-capable (post_only=False): once the
        operator requests stop, priority is flattening exposure, not passive MM.
        """
        if not self.market:
            return {
                "attempted_orders": 0,
                "placed_orders": 0,
                "remaining_up": 0.0,
                "remaining_dn": 0.0,
                "done": True,
                "reason": "no_market",
            }

        attempted_orders = 0
        placed_orders = 0
        min_size = max(0.0, float(self.market.min_order_size or 0.0))

        for round_idx in range(max(1, int(rounds))):
            step = await self.run_terminal_liquidation_step(
                round_idx=round_idx,
                cancel_existing=False,
            )
            attempted_orders += int(step.get("attempted_orders") or 0)
            placed_orders += int(step.get("placed_orders") or 0)
            per_round_order_ids = list(step.get("placed_order_ids") or [])
            if not per_round_order_ids:
                break

            await asyncio.sleep(max(0.05, float(round_delay_sec)))
            await self.order_mgr.check_fills()
            active_ids = set(self.order_mgr.active_order_ids)
            for order_id in per_round_order_ids:
                if order_id in active_ids:
                    await self.order_mgr.cancel_order(order_id)

        up_remaining, dn_remaining, _, _ = await self.get_wallet_balances()
        rem_up = max(0.0, float(up_remaining or 0.0))
        rem_dn = max(0.0, float(dn_remaining or 0.0))
        done = rem_up < min_size and rem_dn < min_size
        log.info(
            "Stop liquidation summary: attempted=%d placed=%d rem_up=%.4f rem_dn=%.4f done=%s",
            attempted_orders,
            placed_orders,
            rem_up,
            rem_dn,
            done,
        )
        return {
            "attempted_orders": attempted_orders,
            "placed_orders": placed_orders,
            "remaining_up": round(rem_up, 4),
            "remaining_dn": round(rem_dn, 4),
            "done": bool(done),
            "reason": "ok",
        }

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
