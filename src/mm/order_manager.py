"""Order Manager — handles order placement, cancellation, and tracking via CLOB API.

Uses py_clob_client for:
- Post-only orders (maker-only, 0% fee)
- GTD (Good-Til-Date) orders that auto-expire
- Batch post/cancel operations
- Fill tracking via order status polling
"""
from __future__ import annotations
import asyncio
import logging
import time
from typing import Any, Optional

from .types import Quote, Fill
from .mm_config import MMConfig

log = logging.getLogger("mm.orders")


class OrderManager:
    """Manage orders on Polymarket CLOB."""

    def __init__(self, clob_client: Any, config: MMConfig):
        """
        Args:
            clob_client: py_clob_client.ClobClient instance (or mock)
            config: MM configuration
        """
        self.client = clob_client
        self.config = config
        self._active_orders: dict[str, Quote] = {}  # order_id -> Quote
        self._pending_cancels: set[str] = set()

    @property
    def active_order_ids(self) -> list[str]:
        return list(self._active_orders.keys())

    @property
    def active_orders(self) -> dict[str, Quote]:
        return dict(self._active_orders)

    async def place_order(self, quote: Quote) -> Optional[str]:
        """Place a single post-only order.

        Returns order_id on success, None on failure.
        """
        try:
            # Build order args
            order_args = {
                "token_id": quote.token_id,
                "price": quote.price,
                "size": quote.size,
                "side": quote.side,
            }

            # Add GTD expiration if enabled
            if self.config.use_gtd:
                order_args["expiration"] = int(time.time()) + self.config.gtd_duration_sec

            # Create and sign order
            signed_order = await asyncio.to_thread(
                self.client.create_and_sign_order, order_args
            )

            # Post with post_only flag
            resp = await asyncio.to_thread(
                self.client.post_order,
                signed_order,
                "GTC" if not self.config.use_gtd else "GTD",
            )

            order_id = resp.get("orderID") or resp.get("order_id") or resp.get("id")
            if order_id:
                quote.order_id = order_id
                self._active_orders[order_id] = quote
                log.info(f"Placed {quote.side} {quote.size:.1f}@{quote.price:.2f} "
                         f"token={quote.token_id[:8]}... id={order_id[:12]}...")
                return order_id
            else:
                log.warning(f"No order_id in response: {resp}")
                return None

        except Exception as e:
            log.error(f"Failed to place order: {e}")
            return None

    async def place_quotes(self, *quotes: Quote) -> list[str]:
        """Place multiple orders (potentially batch).

        Returns list of order_ids (empty string for failures).
        """
        results = []
        for q in quotes:
            oid = await self.place_order(q)
            results.append(oid or "")
        return results

    async def cancel_order(self, order_id: str) -> bool:
        """Cancel a single order."""
        try:
            await asyncio.to_thread(self.client.cancel, order_id)
            self._active_orders.pop(order_id, None)
            log.info(f"Cancelled order {order_id[:12]}...")
            return True
        except Exception as e:
            log.warning(f"Cancel failed for {order_id[:12]}...: {e}")
            return False

    async def cancel_all(self) -> int:
        """Cancel all active orders. Returns count of cancelled."""
        if not self._active_orders:
            return 0

        ids = list(self._active_orders.keys())
        cancelled = 0
        try:
            # Try batch cancel first
            await asyncio.to_thread(self.client.cancel_all)
            cancelled = len(ids)
            self._active_orders.clear()
            log.info(f"Batch cancelled {cancelled} orders")
        except Exception as e:
            log.warning(f"Batch cancel failed: {e}, trying individual...")
            for oid in ids:
                if await self.cancel_order(oid):
                    cancelled += 1

        return cancelled

    async def cancel_replace(self, old_ids: list[str],
                              new_quotes: list[Quote]) -> list[str]:
        """Cancel old orders and place new ones.

        Returns list of new order_ids.
        """
        # Cancel old orders
        for oid in old_ids:
            if oid:
                await self.cancel_order(oid)

        # Place new orders
        return await self.place_quotes(*new_quotes)

    async def check_fills(self) -> list[Fill]:
        """Poll for fills on active orders.

        Returns list of new fills detected.
        """
        fills = []
        to_remove = []

        for order_id, quote in self._active_orders.items():
            try:
                order = await asyncio.to_thread(
                    self.client.get_order, order_id
                )
                status = order.get("status", "")
                size_matched = float(order.get("size_matched", 0))

                if status in ("MATCHED", "CLOSED") and size_matched > 0:
                    fill = Fill(
                        ts=time.time(),
                        side=quote.side,
                        token_id=quote.token_id,
                        price=quote.price,
                        size=size_matched,
                        fee=0.0,  # Maker fee = 0%
                        order_id=order_id,
                        is_maker=True,
                    )
                    fills.append(fill)
                    to_remove.append(order_id)

                elif status in ("CANCELLED", "EXPIRED"):
                    to_remove.append(order_id)

            except Exception as e:
                log.debug(f"Error checking order {order_id[:12]}...: {e}")

        for oid in to_remove:
            self._active_orders.pop(oid, None)

        return fills

    def get_stats(self) -> dict:
        """Get order manager stats."""
        return {
            "active_orders": len(self._active_orders),
            "active_bids": sum(1 for q in self._active_orders.values() if q.side == "BUY"),
            "active_asks": sum(1 for q in self._active_orders.values() if q.side == "SELL"),
        }
