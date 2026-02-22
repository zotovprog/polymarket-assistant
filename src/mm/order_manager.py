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

# Try importing py_clob_client types (available when real client is used)
try:
    from py_clob_client.clob_types import (
        AssetType,
        BalanceAllowanceParams,
        OrderArgs,
        OrderType,
    )
    _HAS_CLOB_TYPES = True
except ImportError:
    _HAS_CLOB_TYPES = False


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
        self._log = log
        self._active_orders: dict[str, Quote] = {}  # order_id -> Quote
        self._filled_order_ids: set[str] = set()
        self._pending_cancels: set[str] = set()
        self._mock_token_balances: dict[str, float] = {}
        self._allowance_set: set[str] = set()  # token IDs with allowance already set

    @property
    def active_order_ids(self) -> list[str]:
        return list(self._active_orders.keys())

    @property
    def active_orders(self) -> dict[str, Quote]:
        return dict(self._active_orders)

    async def _retry(self, coro_func, *args, max_retries: int = 3,
                     base_delay: float = 0.5, **kwargs):
        """Execute async function with exponential backoff retry."""
        last_exc = None
        for attempt in range(max_retries):
            try:
                return await asyncio.wait_for(
                    coro_func(*args, **kwargs),
                    timeout=10.0,
                )
            except asyncio.TimeoutError:
                last_exc = TimeoutError(f"Timeout after 10s on attempt {attempt + 1}")
                self._log.warning(f"Timeout on attempt {attempt + 1}/{max_retries}")
            except Exception as e:
                last_exc = e
                self._log.warning(f"Error on attempt {attempt + 1}/{max_retries}: {e}")
            if attempt < max_retries - 1:
                delay = base_delay * (2 ** attempt)
                await asyncio.sleep(delay)
        self._log.error(f"All {max_retries} retries failed: {last_exc}")
        return None

    @staticmethod
    def required_collateral(quote: Quote) -> float:
        """USDC needed to place this order on Polymarket CLOB.

        BUY: costs size * price in USDC.
        SELL: requires size * (1 - price) as USDC collateral.
        """
        if quote.side == "BUY":
            return quote.size * quote.price
        else:
            return quote.size * (1.0 - quote.price)

    async def ensure_sell_allowance(self, token_id: str) -> bool:
        """Ensure ERC1155 operator allowance is set for SELL orders on this token.

        Polymarket CLOB requires operator approval before you can SELL conditional tokens.
        This calls update_balance_allowance() which is idempotent (safe to call multiple times).
        Results are cached per token_id to avoid redundant API calls.

        Returns True if allowance is OK, False on failure.
        """
        if hasattr(self.client, "_orders"):  # Mock client
            return True
        if token_id in self._allowance_set:
            return True
        if not _HAS_CLOB_TYPES:
            return False

        try:
            # Check current allowance
            result = await asyncio.to_thread(
                self.client.get_balance_allowance,
                BalanceAllowanceParams(
                    asset_type=AssetType.CONDITIONAL,
                    token_id=token_id,
                ),
            )
            allowance = float(result.get("allowance", 0))
            balance = float(result.get("balance", 0))

            log.info(f"Token {token_id[:12]}... balance={balance/1e6:.2f} allowance={allowance/1e6:.2f}")

            if allowance <= 0 or allowance < balance:
                log.info(f"Setting allowance for {token_id[:12]}...")
                await asyncio.to_thread(
                    self.client.update_balance_allowance,
                    BalanceAllowanceParams(
                        asset_type=AssetType.CONDITIONAL,
                        token_id=token_id,
                    ),
                )
                log.info(f"Allowance updated for {token_id[:12]}...")

            self._allowance_set.add(token_id)
            return True
        except Exception as e:
            log.error(f"Failed to ensure allowance for {token_id[:12]}...: {e}")
            return False

    async def _place_order_inner(self, quote: Quote, post_only: bool) -> str:
        """Create, sign and send an order. Returns order_id or raises on error."""
        is_mock = hasattr(self.client, '_orders')
        if is_mock:
            order_args = {
                "token_id": quote.token_id,
                "price": quote.price,
                "size": quote.size,
                "side": quote.side,
            }
            signed_order = self.client.create_and_sign_order(order_args)
            resp = self.client.post_order(signed_order, "GTC")
        else:
            if not _HAS_CLOB_TYPES:
                raise ImportError("py_clob_client not installed for live trading")

            order_type = OrderType.GTD if self.config.use_gtd else OrderType.GTC
            oa = OrderArgs(
                token_id=quote.token_id,
                price=quote.price,
                size=quote.size,
                side=quote.side,
            )
            if self.config.use_gtd:
                oa.expiration = int(time.time()) + self.config.gtd_duration_sec

            signed_order = await asyncio.to_thread(
                self.client.create_order, oa
            )
            resp = await asyncio.wait_for(
                asyncio.to_thread(
                    self.client.post_order,
                    signed_order,
                    order_type,
                    post_only,
                ),
                timeout=10.0,
            )

        order_id = resp.get("orderID") or resp.get("order_id") or resp.get("id")
        if not order_id:
            raise RuntimeError(f"No order_id in response: {resp}")

        quote.order_id = order_id
        self._active_orders[order_id] = quote
        log.info(f"Placed {quote.side} {quote.size:.1f}@{quote.price:.2f} "
                 f"token={quote.token_id[:8]}... id={order_id[:12]}...")
        return order_id

    async def place_order(self, quote: Quote, *, post_only: bool | None = None) -> Optional[str]:
        """Place an order with retry on insufficient balance.

        Args:
            quote: The quote to place.
            post_only: Override post-only flag. None = use config default.
                       False = allow crossing book (taker, for liquidation).

        Returns order_id on success, None on failure.
        """
        use_post_only = self.config.use_post_only if post_only is None else post_only
        is_mock = hasattr(self.client, '_orders')

        # Ensure allowance for SELL orders on conditional tokens
        if quote.side == "SELL" and not is_mock:
            if not await self.ensure_sell_allowance(quote.token_id):
                log.warning(f"Cannot place SELL — allowance setup failed for {quote.token_id[:12]}...")
                return None
        try:
            return await self._place_order_inner(quote, use_post_only)
        except Exception as e:
            error_msg = str(e)
            if "not enough balance" in error_msg.lower():
                reduced = round(quote.size * 0.9, 2)
                if reduced >= 1.0:
                    log.warning(
                        f"Insufficient balance for {quote.side} "
                        f"{quote.size:.1f}@{quote.price:.2f} — retrying with {reduced:.1f}"
                    )
                    quote.size = reduced
                    try:
                        return await self._place_order_inner(quote, use_post_only)
                    except Exception as e2:
                        log.error(f"Retry also failed: {e2}")
                        return None
                else:
                    log.warning(
                        f"Insufficient balance for {quote.side} "
                        f"{quote.size:.1f}@{quote.price:.2f} — too small to retry"
                    )
            elif "crosses book" in error_msg.lower():
                log.warning(
                    f"Post-only crossed book: {quote.side} "
                    f"{quote.size:.1f}@{quote.price:.2f}"
                )
            else:
                log.error(f"Failed to place order: {e}")
            return None

    async def get_book_summary(self, token_id: str) -> dict[str, float | None]:
        """Fetch best bid/ask from Polymarket CLOB for a token.

        Returns dict with 'best_bid' and 'best_ask' (float or None).

        Note: get_order_book returns an OrderBookSummary object (not dict).
        - book.bids is sorted ascending by price  → best bid = last element
        - book.asks is sorted descending by price → best ask = last element
        - OrderSummary.price is a string
        """
        try:
            is_mock = hasattr(self.client, "_orders")
            if is_mock:
                return {"best_bid": None, "best_ask": None}

            book = await asyncio.to_thread(self.client.get_order_book, token_id)
            best_bid = None
            best_ask = None

            # OrderBookSummary: bids sorted ascending, asks sorted descending
            if book and hasattr(book, "bids") and book.bids:
                best_bid = float(book.bids[-1].price)  # highest bid = last
            if book and hasattr(book, "asks") and book.asks:
                best_ask = float(book.asks[-1].price)   # lowest ask = last

            return {"best_bid": best_bid, "best_ask": best_ask}
        except Exception as e:
            log.debug(f"Failed to get book for {token_id[:12]}...: {e}")
            return {"best_bid": None, "best_ask": None}

    async def get_token_balance(self, token_id: str) -> float:
        """Fetch real PM token balance in shares for a conditional token."""
        try:
            if hasattr(self.client, "_orders"):
                return float(self._mock_token_balances.get(token_id, 0.0))

            if not _HAS_CLOB_TYPES:
                raise ImportError("py_clob_client not installed for live trading")

            result = await asyncio.to_thread(
                self.client.get_balance_allowance,
                BalanceAllowanceParams(
                    asset_type=AssetType.CONDITIONAL,
                    token_id=token_id,
                )
            )
            return float(result["balance"]) / 1e6
        except Exception as e:
            log.warning(f"Failed to fetch token balance for {token_id[:12]}...: {e}")
            return 0.0

    async def get_all_token_balances(self, up_token_id: str, dn_token_id: str) -> tuple[float, float]:
        """Fetch both UP and DN token balances.

        Live mode: fetches PM balances via API.
        Mock mode: returns balances tracked from observed mock fills.
        """
        if hasattr(self.client, "_orders"):
            up = float(self._mock_token_balances.get(up_token_id, 0.0))
            dn = float(self._mock_token_balances.get(dn_token_id, 0.0))
            return up, dn

        up, dn = await asyncio.gather(
            self.get_token_balance(up_token_id),
            self.get_token_balance(dn_token_id),
        )
        return float(up), float(dn)

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
            await asyncio.wait_for(
                asyncio.to_thread(self.client.cancel, order_id),
                timeout=10.0,
            )
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
                order = await self._retry(
                    asyncio.to_thread,
                    self.client.get_order,
                    order_id,
                )
                if order is None:
                    continue
                status = order.get("status", "")
                size_matched = float(order.get("size_matched", 0))

                if status in ("MATCHED", "CLOSED") and size_matched > 0:
                    if order_id in self._filled_order_ids:
                        to_remove.append(order_id)
                        continue

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

                    # Mock balance tracking mirrors fill-side share movements.
                    if hasattr(self.client, "_orders"):
                        signed_size = size_matched if quote.side == "BUY" else -size_matched
                        cur = self._mock_token_balances.get(quote.token_id, 0.0)
                        self._mock_token_balances[quote.token_id] = cur + signed_size

                    self._filled_order_ids.add(order_id)
                    to_remove.append(order_id)

                elif status in ("CANCELLED", "EXPIRED"):
                    to_remove.append(order_id)

            except Exception as e:
                log.debug(f"Error checking order {order_id[:12]}...: {e}")

        for oid in to_remove:
            self._active_orders.pop(oid, None)

        return fills

    def reset(self) -> None:
        """Clear tracked order state between windows."""
        self._active_orders.clear()
        self._filled_order_ids.clear()
        self._mock_token_balances.clear()
        self._allowance_set.clear()

    def get_stats(self) -> dict:
        """Get order manager stats."""
        return {
            "active_orders": len(self._active_orders),
            "active_bids": sum(1 for q in self._active_orders.values() if q.side == "BUY"),
            "active_asks": sum(1 for q in self._active_orders.values() if q.side == "SELL"),
        }
