"""Order Manager — handles order placement, cancellation, and tracking via CLOB API.

Uses py_clob_client for:
- Post-only orders (maker-only, 0% fee)
- GTD (Good-Til-Date) orders that auto-expire
- Batch post/cancel operations
- Fill tracking via order status polling
"""
from __future__ import annotations
import asyncio
import json
import logging
import time
from typing import Any, Optional
import websockets

from .types import Quote, Fill
from .mm_config import MMConfig
from .pm_fees import taker_fee_usd, net_shares_after_buy_fee

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
        self._order_post_only: dict[str, bool] = {}  # order_id -> placement post_only flag
        self._filled_order_ids: set[str] = set()
        self._partial_fill_reported: dict[str, float] = {}  # order_id -> last reported size_matched
        self._pending_cancels: set[str] = set()
        self._mock_token_balances: dict[str, float] = {}
        self._allowance_set: set[str] = set()  # token IDs with allowance already set
        self._session_budget: float = 0.0  # Hard USDC budget cap (0 = no limit)
        self._session_spent: float = 0.0   # Total USDC committed to BUY orders this session
        self._warn_cooldowns: dict[str, float] = {}
        self._fill_ws_task: asyncio.Task | None = None
        self._fill_ws_running = False
        self._ws_fills_queue: asyncio.Queue = asyncio.Queue()
        self._last_fill_check_ts: float = 0.0
        self._usdc_balance_cache: float | None = None
        self._usdc_balance_cache_ts: float = 0.0
        self._usdc_cache_ttl: float = 5.0  # Cache USDC balance for 5 seconds
        self._on_fill_callback: Any = None  # Callable or None — called on WS fill
        self._reconcile_requested: bool = False
        self._on_heartbeat_id: Any = None  # Callable(str) — notify heartbeat of new ID

    def set_heartbeat_id_callback(self, callback) -> None:
        """Set callback to notify HeartbeatManager of new heartbeat_id from PM responses."""
        self._on_heartbeat_id = callback

    @staticmethod
    def _extract_heartbeat_id(resp: Any) -> str | None:
        """Extract heartbeat_id from PM API response if present."""
        if isinstance(resp, dict):
            hb_id = resp.get("heartbeat_id")
            if hb_id and isinstance(hb_id, str) and len(hb_id) >= 32:
                return hb_id
        return None

    def _notify_heartbeat_id(self, resp: Any) -> None:
        """If PM response contains a heartbeat_id, notify the HeartbeatManager."""
        if self._on_heartbeat_id:
            hb_id = self._extract_heartbeat_id(resp)
            if hb_id:
                try:
                    self._on_heartbeat_id(hb_id)
                except Exception:
                    pass

    def _throttled_warn(self, key: str, msg: str, cooldown: float = 30.0):
        """Log a warning at most once per cooldown period."""
        now = time.time()
        if now - self._warn_cooldowns.get(key, 0) >= cooldown:
            self._warn_cooldowns[key] = now
            self._log.warning(msg)

    @property
    def active_order_ids(self) -> list[str]:
        return list(self._active_orders.keys())

    @property
    def active_orders(self) -> dict[str, Quote]:
        return dict(self._active_orders)

    @property
    def reconcile_requested(self) -> bool:
        """Whether an external inventory reconcile should be forced soon."""
        return self._reconcile_requested

    def clear_reconcile_request(self) -> None:
        """Clear a previously raised reconcile request."""
        self._reconcile_requested = False

    @staticmethod
    def _safe_float(value: Any) -> Optional[float]:
        try:
            return float(value)
        except (TypeError, ValueError):
            return None

    @classmethod
    def _extract_fill_price(cls, order: dict[str, Any], fallback: float) -> float:
        """Best-effort fill price from exchange payload, with quote fallback."""
        for key in (
            "fill_price",
            "matched_price",
            "last_trade_price",
            "avg_price",
            "average_price",
            "price",
        ):
            px = cls._safe_float(order.get(key))
            if px is not None and px > 0:
                return px
        return fallback

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
        quote.placed_at = time.time()
        self._active_orders[order_id] = quote
        self._order_post_only[order_id] = post_only
        self._notify_heartbeat_id(resp)
        self.invalidate_usdc_cache()
        log.info(f"Placed {quote.side} {quote.size:.1f}@{quote.price:.2f} "
                 f"token={quote.token_id[:8]}... id={order_id[:12]}...")
        return order_id

    async def place_order(self, quote: Quote, *, post_only: bool | None = None, fallback_taker: bool = False) -> Optional[str]:
        """Place an order with retry on insufficient balance.

        Args:
            quote: The quote to place.
            post_only: Override post-only flag. None = use config default.
                       False = allow crossing book (taker, for liquidation).

        Returns order_id on success, None on failure.
        """
        use_post_only = self.config.use_post_only if post_only is None else post_only
        is_mock = hasattr(self.client, '_orders')

        # Hard budget cap: reject BUY orders that exceed session budget
        collateral = self.required_collateral(quote)
        if quote.side == "BUY" and self._session_budget > 0:
            active_buy_collateral = sum(
                self.required_collateral(q)
                for q in self._active_orders.values()
                if q.side == "BUY"
            )
            budget_remaining = self._session_budget - self._session_spent - active_buy_collateral
            if collateral > budget_remaining + 0.01:
                # Try to reduce size to fit budget
                if budget_remaining > 1.0 and quote.price > 0:
                    max_size = budget_remaining / quote.price
                    if max_size >= 1.0:
                        self._throttled_warn(
                            "budget_cap",
                            f"Budget cap: {quote.token_id[:8]} {quote.size:.1f}@{quote.price:.2f} "
                            f"needs ${collateral:.2f} but only ${budget_remaining:.2f} remaining — "
                            f"reducing to {max_size:.1f}",
                        )
                        quote.size = round(max_size, 2)
                        collateral = self.required_collateral(quote)
                    else:
                        self._throttled_warn(
                            "budget_reject",
                            f"Budget cap: rejecting {quote.token_id[:8]} BUY "
                            f"{quote.size:.1f}@{quote.price:.2f} — only ${budget_remaining:.2f} remaining",
                        )
                        return None
                else:
                    self._throttled_warn(
                        "budget_exhausted",
                        f"Budget cap: rejecting {quote.token_id[:8]} BUY "
                        f"{quote.size:.1f}@{quote.price:.2f} — budget exhausted (${budget_remaining:.2f} remaining)",
                    )
                    return None

        # Log warning if collateral exceeds available USDC (mock only)
        if is_mock:
            usdc_avail = float(getattr(self.client, '_usdc_balance', 0.0))
            if collateral > usdc_avail:
                log.warning(
                    "Collateral warning: %s %s %.1f@%.2f needs $%.2f but only $%.2f USDC available",
                    quote.side, quote.token_id[:8], quote.size, quote.price,
                    collateral, usdc_avail,
                )

        # BUY balance pre-check (live only): avoid spamming PM API with 400 errors
        if quote.side == "BUY" and not is_mock:
            usdc_bal = await self.get_usdc_balance()
            if usdc_bal is None:
                log.warning("BUY pre-check skipped: failed to fetch USDC balance")
            else:
                # Subtract collateral already locked in active BUY orders
                active_buy_collateral = sum(
                    self.required_collateral(q)
                    for q in self._active_orders.values()
                    if q.side == "BUY"
                )
                available = usdc_bal - active_buy_collateral
                if collateral > available + 0.50:
                    if available > 1.0 and quote.price > 0:
                        max_size = available / quote.price
                        pm_min = 5.0
                        if max_size >= pm_min:
                            self._throttled_warn(
                                "buy_balance_cap",
                                f"BUY balance cap: need ${collateral:.2f} but only "
                                f"${available:.2f} available — reducing {quote.size:.1f} → {max_size:.1f}",
                            )
                            quote.size = round(max_size, 2)
                        else:
                            self._throttled_warn(
                                "buy_balance_skip",
                                f"BUY skipped: need ${collateral:.2f} but only "
                                f"${available:.2f} available (max_size={max_size:.1f} < min {pm_min})",
                            )
                            return None
                    else:
                        self._throttled_warn(
                            "buy_no_usdc",
                            f"BUY skipped: need ${collateral:.2f} but only "
                            f"${available:.2f} USDC available",
                        )
                        return None

        # Ensure allowance for SELL orders on conditional tokens
        if quote.side == "SELL" and not is_mock:
            if not await self.ensure_sell_allowance(quote.token_id):
                log.warning(f"Cannot place SELL — allowance setup failed for {quote.token_id[:12]}...")
                return None

            # SELL pre-check:
            # - Inventory-backed close: require enough free tokens, skip USDC collateral check.
            # - True short SELL: only short remainder requires USDC collateral.
            free_inventory = 0.0
            token_bal = await self.get_token_balance(quote.token_id)
            active_sell_exposure = sum(
                q.size
                for q in self._active_orders.values()
                if q.side == "SELL" and q.token_id == quote.token_id
            )
            local_inventory = quote.size + active_sell_exposure
            # If token balance unavailable, fall back to local inventory for close detection
            if token_bal is None:
                # Use local inventory estimate — safer than assuming short
                token_balance_for_close_check = local_inventory if local_inventory > 0 else 0.0
            else:
                token_balance_for_close_check = token_bal
            free_inventory = max(0.0, token_balance_for_close_check - active_sell_exposure)
            if free_inventory + 0.01 >= quote.size:
                log.debug(
                    "SELL pre-check: inventory-backed close %.2f shares "
                    "(token=%.2f active_sell=%.2f) — skipping USDC collateral check",
                    quote.size,
                    token_balance_for_close_check,
                    active_sell_exposure,
                )

            short_size = max(0.0, quote.size - free_inventory)
            short_collateral = short_size * (1.0 - quote.price)

            if short_collateral > 0.01:
                usdc_bal = await self.get_usdc_balance()
                if usdc_bal is None:
                    log.warning("SELL pre-check skipped: failed to fetch USDC balance")
                elif usdc_bal < short_collateral:
                    if usdc_bal > 0.01 and quote.price < 1.0:
                        max_short_affordable = usdc_bal / (1.0 - quote.price)
                        max_total_size = free_inventory + max_short_affordable
                        pm_min = 5.0
                        if max_total_size >= pm_min:
                            self._throttled_warn(
                                "sell_balance_cap",
                                f"SELL balance cap (short leg): need ${short_collateral:.2f} but only "
                                f"${usdc_bal:.2f} USDC — reducing {quote.size:.1f} → {max_total_size:.1f}",
                            )
                            quote.size = round(max_total_size, 2)
                        else:
                            self._throttled_warn(
                                "sell_balance_skip",
                                f"SELL skipped: short leg needs ${short_collateral:.2f} USDC but only "
                                f"${usdc_bal:.2f} (max_size={max_total_size:.1f} < min {pm_min})",
                            )
                            return None
                    else:
                        self._throttled_warn(
                            "sell_no_usdc",
                            f"SELL skipped: no USDC for short collateral "
                            f"(need ${short_collateral:.2f}, have ${usdc_bal:.2f})",
                        )
                        return None

        try:
            return await self._place_order_inner(quote, use_post_only)
        except Exception as e:
            error_msg = str(e)
            if "not enough balance" in error_msg.lower():
                reduced = round(quote.size * 0.9, 2)
                if reduced >= 1.0:
                    self._throttled_warn(
                        "insufficient_balance",
                        f"Insufficient balance for {quote.side} "
                        f"{quote.size:.1f}@{quote.price:.2f} — retrying with {reduced:.1f}",
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
                if fallback_taker:
                    log.info(
                        "Post-only crossed book, retrying as taker: %s "
                        "%.1f@%.2f",
                        quote.side, quote.size, quote.price,
                    )
                    try:
                        return await self._place_order_inner(quote, False)
                    except Exception as e2:
                        log.error(f"Taker fallback also failed: {e2}")
                        return None
                else:
                    self._throttled_warn(
                        "crosses_book",
                        f"Post-only crossed book: {quote.side} "
                        f"{quote.size:.1f}@{quote.price:.2f}",
                        cooldown=10.0,
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
            book = None

            if is_mock:
                book = self.client.get_order_book(token_id)
            else:
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

    async def get_full_book(self, token_id: str) -> dict:
        """Full order book depth for a token.

        Returns:
            {'bids': [{'price': float, 'size': float}, ...],  # desc by price
             'asks': [{'price': float, 'size': float}, ...],  # asc by price
             'best_bid': float|None, 'best_ask': float|None,
             'bid_depth_usd': float, 'ask_depth_usd': float,
             'num_bids': int, 'num_asks': int}
        """
        empty = {"bids": [], "asks": [], "best_bid": None, "best_ask": None,
                 "bid_depth_usd": 0.0, "ask_depth_usd": 0.0,
                 "num_bids": 0, "num_asks": 0}
        try:
            is_mock = hasattr(self.client, "_orders")
            if is_mock:
                book = self.client.get_order_book(token_id)
            else:
                book = await asyncio.to_thread(self.client.get_order_book, token_id)
            if not book:
                return empty

            bids = []
            asks = []
            bid_depth = 0.0
            ask_depth = 0.0

            if hasattr(book, "bids") and book.bids:
                for entry in book.bids:
                    p = float(entry.price)
                    s = float(entry.size)
                    bids.append({"price": p, "size": s})
                    bid_depth += p * s
                bids.sort(key=lambda x: x["price"], reverse=True)

            if hasattr(book, "asks") and book.asks:
                for entry in book.asks:
                    p = float(entry.price)
                    s = float(entry.size)
                    asks.append({"price": p, "size": s})
                    ask_depth += p * s
                asks.sort(key=lambda x: x["price"])

            best_bid = bids[0]["price"] if bids else None
            best_ask = asks[0]["price"] if asks else None

            return {
                "bids": bids, "asks": asks,
                "best_bid": best_bid, "best_ask": best_ask,
                "bid_depth_usd": bid_depth, "ask_depth_usd": ask_depth,
                "num_bids": len(bids), "num_asks": len(asks),
            }
        except Exception as e:
            log.debug(f"Failed to get full book for {token_id[:12]}...: {e}")
            return empty

    async def get_token_balance(self, token_id: str) -> Optional[float]:
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
            return None

    async def get_usdc_balance(self) -> Optional[float]:
        """Fetch real USDC (collateral) balance on Polymarket."""
        now = time.time()
        if (
            self._usdc_balance_cache is not None
            and (now - self._usdc_balance_cache_ts) < self._usdc_cache_ttl
        ):
            return self._usdc_balance_cache

        try:
            if hasattr(self.client, "_orders"):
                balance = float(getattr(self.client, '_usdc_balance', 0.0))
                self._usdc_balance_cache = balance
                self._usdc_balance_cache_ts = now
                return balance
            if not _HAS_CLOB_TYPES:
                self._usdc_balance_cache = 0.0
                self._usdc_balance_cache_ts = now
                return 0.0
            result = await asyncio.to_thread(
                self.client.get_balance_allowance,
                BalanceAllowanceParams(asset_type=AssetType.COLLATERAL),
            )
            balance = float(result.get("balance", 0)) / 1e6
            self._usdc_balance_cache = balance
            self._usdc_balance_cache_ts = now
            return balance
        except Exception as e:
            log.warning(f"Failed to fetch USDC balance: {e}")
            return None

    def invalidate_usdc_cache(self) -> None:
        """Force next USDC balance read to refresh from source."""
        self._usdc_balance_cache = None
        self._usdc_balance_cache_ts = 0.0

    async def get_all_token_balances(
        self, up_token_id: str, dn_token_id: str
    ) -> tuple[Optional[float], Optional[float]]:
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
        return up, dn

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
            resp = await asyncio.wait_for(
                asyncio.to_thread(self.client.cancel, order_id),
                timeout=10.0,
            )
            self._active_orders.pop(order_id, None)
            self._order_post_only.pop(order_id, None)
            self._notify_heartbeat_id(resp)
            self.invalidate_usdc_cache()
            log.info(f"Cancelled order {order_id[:12]}...")
            return True
        except Exception as e:
            log.warning(f"Cancel failed for {order_id[:12]}...: {e}")
            return False

    def clear_local_order_tracking(self) -> None:
        """Drop in-memory order-tracking state without touching balances."""
        self._active_orders.clear()
        self._order_post_only.clear()
        self._filled_order_ids.clear()
        self._partial_fill_reported.clear()
        self._pending_cancels.clear()
        self._reconcile_requested = False
        while not self._ws_fills_queue.empty():
            try:
                self._ws_fills_queue.get_nowait()
            except asyncio.QueueEmpty:
                break

    async def cancel_all(self, *, force_exchange: bool = False) -> int:
        """Cancel active orders.

        Args:
            force_exchange: If True, call exchange `cancel_all` even when local
                tracking is empty.

        Returns count of locally tracked orders cancelled.
        """
        ids = list(self._active_orders.keys())
        if not ids and not force_exchange:
            return 0

        cancelled = 0
        try:
            # Try batch cancel first
            batch_resp = await asyncio.to_thread(self.client.cancel_all)
            if isinstance(batch_resp, dict) and batch_resp.get("error"):
                raise RuntimeError(f"batch cancel response error: {batch_resp.get('error')}")
            self._notify_heartbeat_id(batch_resp)
            cancelled = len(ids)
            self.clear_local_order_tracking()
            if cancelled > 0:
                log.info(f"Batch cancelled {cancelled} orders")
            elif force_exchange:
                log.info("Forced batch cancel submitted with no locally tracked orders")
        except Exception as e:
            if not ids:
                log.error("Forced batch cancel failed with no local order IDs: %s", e)
                raise
            log.warning(f"Batch cancel failed: {e}, trying individual...")
            for oid in ids:
                if await self.cancel_order(oid):
                    cancelled += 1

        return cancelled

    async def cancel_replace(self, old_ids: list[str],
                              new_quotes: list[Quote]) -> list[str] | None:
        """Cancel old orders and place new ones.

        Cancels run in parallel. Placements run sequentially to avoid
        budget accounting races (two BUY orders could both pass the
        budget check before either is recorded in _active_orders).

        Returns list of new order_ids.
        """
        # Cancel old orders in parallel
        if old_ids:
            cancel_results = await asyncio.gather(
                *(self.cancel_order(oid) for oid in old_ids if oid)
            )
            # If ANY cancel failed, don't place replacements — wait for next tick
            if not all(cancel_results):
                failed = sum(1 for r in cancel_results if not r)
                self._log.warning(
                    "cancel_replace: %d/%d cancels failed, skipping new placements",
                    failed,
                    len(cancel_results),
                )
                return None

        # Place new orders sequentially (budget safety)
        results = []
        for q in new_quotes:
            if q:
                oid = await self.place_order(q)
                results.append(oid or "")
        return results

    def set_fill_callback(self, callback) -> None:
        """Set a callback to be called when a fill is detected via WS.

        Used by MarketMaker to trigger immediate requote on fill.
        """
        self._on_fill_callback = callback

    async def start_fill_ws(self, api_key: str = "", api_secret: str = "",
                            api_passphrase: str = "") -> None:
        """Start WebSocket connection for real-time fill notifications.

        This supplements polling — fills detected via WS are queued and
        consumed by check_fills() on next call. Polling remains as reconciliation.
        """
        if self._fill_ws_running:
            return
        self._fill_ws_running = True
        self._fill_ws_task = asyncio.ensure_future(
            self._fill_ws_loop(api_key, api_secret, api_passphrase))

    async def _fill_ws_loop(self, api_key: str, api_secret: str,
                            api_passphrase: str) -> None:
        """Background loop for user WebSocket channel."""
        url = "wss://ws-subscriptions-clob.polymarket.com/ws/user"
        log.info("Fill WS: connecting to %s", url)
        while self._fill_ws_running:
            try:
                async with websockets.connect(url, ping_interval=10) as ws:
                    auth = {}
                    if api_key:
                        auth = {
                            "apiKey": api_key,
                            "secret": api_secret,
                            "passphrase": api_passphrase,
                        }
                    sub_msg = {
                        "type": "subscribe",
                        "channel": "user",
                        "auth": auth,
                    }
                    await ws.send(json.dumps(sub_msg))
                    log.info("Fill WS: subscribed to user channel")

                    async for raw in ws:
                        try:
                            msg = json.loads(raw)
                            event_type = msg.get("event_type", "")
                            if event_type == "trade":
                                # PM docs: order ids are in taker_order_id and maker_orders[].order_id
                                ids = set()
                                tid = msg.get("taker_order_id")
                                if tid:
                                    ids.add(tid)
                                for mo in (msg.get("maker_orders") or []):
                                    oid = mo.get("order_id")
                                    if oid:
                                        ids.add(oid)
                                if not ids:
                                    # Can't map trade to our orders — request reconcile
                                    self._reconcile_requested = True
                                for oid in ids:
                                    await self._ws_fills_queue.put({"order_id": oid, "raw": msg})
                                log.info(
                                    "Fill WS: trade event — matched_orders=%d size=%s price=%s",
                                    len(ids),
                                    msg.get("size", "?"),
                                    msg.get("price", "?"),
                                )
                                if self._on_fill_callback:
                                    try:
                                        self._on_fill_callback()
                                    except Exception:
                                        pass
                            elif event_type == "order" and msg.get("status") in ("MATCHED", "CLOSED"):
                                await self._ws_fills_queue.put(msg)
                        except json.JSONDecodeError:
                            continue
            except Exception as e:
                if self._fill_ws_running:
                    log.warning("Fill WS disconnected: %s — reconnecting in 5s", e)
                    await asyncio.sleep(5)
        log.info("Fill WS: stopped")

    async def stop_fill_ws(self) -> None:
        """Stop the fill WebSocket."""
        self._fill_ws_running = False
        if self._fill_ws_task:
            self._fill_ws_task.cancel()
            try:
                await self._fill_ws_task
            except asyncio.CancelledError:
                pass
            self._fill_ws_task = None

    async def check_fills(self) -> list[Fill]:
        """Poll for fills on active orders.

        Returns list of new fills detected, including partial fills.
        Tracks previously reported size_matched per order to emit
        incremental fill events for partial fills.
        """
        now = time.time()
        has_ws_notifications = not self._ws_fills_queue.empty()
        if not has_ws_notifications and (now - self._last_fill_check_ts) < 2.0:
            return []

        # Drain WS fill events — collect order IDs that definitely have updates
        ws_priority_ids: set[str] = set()
        while not self._ws_fills_queue.empty():
            try:
                ws_msg = self._ws_fills_queue.get_nowait()
                oid = ws_msg.get("order_id", "")
                if oid and oid in self._active_orders:
                    ws_priority_ids.add(oid)
            except asyncio.QueueEmpty:
                break
        if ws_priority_ids:
            log.info("Fill WS: %d orders with pending updates", len(ws_priority_ids))

        fills = []
        to_remove = []

        # Check WS-notified orders first, then remaining active orders
        check_order = list(ws_priority_ids)
        check_order.extend(oid for oid in self._active_orders if oid not in ws_priority_ids)

        order_data = []
        for order_id in check_order:
            quote = self._active_orders.get(order_id)
            if quote:
                order_data.append((order_id, quote))

        if order_data:
            self._last_fill_check_ts = time.time()
            fetch_results = await asyncio.gather(
                *(
                    self._retry(
                        asyncio.to_thread,
                        self.client.get_order,
                        order_id,
                    )
                    for order_id, _ in order_data
                ),
                return_exceptions=True,
            )

            for (order_id, quote), order_result in zip(order_data, fetch_results):
                if isinstance(order_result, Exception):
                    log.debug(f"Error checking order {order_id[:12]}...: {order_result}")
                    continue

                try:
                    order = order_result
                    if order is None:
                        continue
                    status = order.get("status", "")
                    size_matched_raw = order.get("size_matched", 0)
                    size_matched = self._safe_float(size_matched_raw)
                    if size_matched is None:
                        self._reconcile_requested = True
                        log.error(
                            "Anomalous fill: invalid size_matched (%s) for order %s",
                            size_matched_raw,
                            order_id[:12],
                        )
                        continue

                    # Track how much we already reported for this order
                    prev_matched = self._partial_fill_reported.get(order_id, 0.0)
                    remaining_size = max(0.0, quote.size - prev_matched)
                    new_fill_size = size_matched - prev_matched

                    if new_fill_size > remaining_size + 0.1:
                        self._reconcile_requested = True
                        log.error(
                            "Anomalous fill: size_matched exceeds order remainder "
                            "(order=%s size_matched=%.4f already_filled=%.4f remaining=%.4f original=%.4f)",
                            order_id[:12],
                            size_matched,
                            prev_matched,
                            remaining_size,
                            quote.size,
                        )
                        # Still clean up completed/cancelled orders to prevent drift
                        if status in ("MATCHED", "CLOSED", "CANCELLED", "EXPIRED"):
                            to_remove.append(order_id)
                            self._partial_fill_reported.pop(order_id, None)
                        continue
                    if new_fill_size < -0.1:
                        self._reconcile_requested = True
                        log.error(
                            "Anomalous fill: size_matched regressed "
                            "(order=%s size_matched=%.4f already_filled=%.4f)",
                            order_id[:12],
                            size_matched,
                            prev_matched,
                        )
                        if status in ("MATCHED", "CLOSED", "CANCELLED", "EXPIRED"):
                            to_remove.append(order_id)
                            self._partial_fill_reported.pop(order_id, None)
                        continue

                    if new_fill_size >= 0.01:
                        fill_price = self._extract_fill_price(order, quote.price)
                        is_maker = self._order_post_only.get(order_id, True)
                        fee = 0.0 if is_maker else taker_fee_usd(fill_price, new_fill_size, quote.side)
                        fill = Fill(
                            ts=time.time(),
                            side=quote.side,
                            token_id=quote.token_id,
                            price=fill_price,
                            size=round(new_fill_size, 4),
                            fee=fee,
                            order_id=order_id,
                            is_maker=is_maker,
                        )
                        fills.append(fill)
                        self._partial_fill_reported[order_id] = size_matched

                        # Track session spending for budget cap
                        if quote.side == "BUY":
                            self._session_spent += new_fill_size * fill_price
                        elif quote.side == "SELL":
                            self._session_spent = max(0.0, self._session_spent - new_fill_size * fill_price)

                        # Mock balance tracking mirrors fill-side share movements.
                        if hasattr(self.client, "_orders"):
                            if quote.side == "BUY":
                                # BUY taker: fee deducted in shares
                                actual_size = net_shares_after_buy_fee(new_fill_size, fill_price) if not is_maker else new_fill_size
                                cur = self._mock_token_balances.get(quote.token_id, 0.0)
                                self._mock_token_balances[quote.token_id] = cur + actual_size
                            else:  # SELL
                                cur = self._mock_token_balances.get(quote.token_id, 0.0)
                                self._mock_token_balances[quote.token_id] = cur - new_fill_size

                    if status in ("MATCHED", "CLOSED"):
                        self._filled_order_ids.add(order_id)
                        to_remove.append(order_id)
                        self._partial_fill_reported.pop(order_id, None)
                    elif status in ("CANCELLED", "EXPIRED"):
                        to_remove.append(order_id)
                        self._partial_fill_reported.pop(order_id, None)

                except Exception as e:
                    log.debug(f"Error checking order {order_id[:12]}...: {e}")

        for oid in to_remove:
            self._active_orders.pop(oid, None)
            self._order_post_only.pop(oid, None)

        return fills

    async def merge_positions(self, condition_id: str, amount_shares: float,
                              private_key: str) -> dict:
        """Merge YES+NO conditional token pairs back into USDC.

        Paper mode: instantly credits USDC balance.
        Live mode: skipped when using funder/proxy (tokens on Safe, not EOA).
                   On-chain merge requires tokens on msg.sender's address.
        """
        if hasattr(self.client, "_orders"):  # Paper mode
            self.client._usdc_balance += amount_shares
            log.info("[MOCK] Merge %.2f pairs -> $%.2f USDC", amount_shares, amount_shares)
            return {"success": True, "amount_usdc": amount_shares}

        # Live mode: check if using funder/proxy (tokens on Safe, not EOA)
        funder = getattr(self.client, 'funder', None)
        signer_addr = None
        if hasattr(self.client, 'signer'):
            signer_addr = getattr(self.client.signer, 'address', lambda: None)()
        if funder and signer_addr and funder.lower() != signer_addr.lower():
            log.warning(
                "Merge skipped: tokens on funder (%s), not EOA (%s). "
                "Using SELL liquidation instead.",
                funder[:10], signer_addr[:10],
            )
            return {"success": False, "error": "funder mode — merge not supported"}

        from .approvals import merge_positions as _merge
        return await asyncio.to_thread(_merge, private_key, condition_id, amount_shares)

    def reset(self) -> None:
        """Clear tracked order state between windows."""
        self.clear_local_order_tracking()
        self._mock_token_balances.clear()
        self._allowance_set.clear()
        self.invalidate_usdc_cache()
        self._last_fill_check_ts = 0.0
        self._session_spent = 0.0
        self._warn_cooldowns = {}

    def get_stats(self) -> dict:
        """Get order manager stats."""
        return {
            "active_orders": len(self._active_orders),
            "active_bids": sum(1 for q in self._active_orders.values() if q.side == "BUY"),
            "active_asks": sum(1 for q in self._active_orders.values() if q.side == "SELL"),
        }

    def get_active_orders_detail(self, liquidation_ids: set[str] | None = None,
                                  up_token_id: str = "",
                                  dn_token_id: str = "") -> list[dict]:
        """Get details of all active orders for dashboard display.

        Returns list of dicts with: order_id, side, price, size, notional,
        token (UP/DN/??), age_sec, type (quote/liquidation).
        """
        liq_ids = liquidation_ids or set()
        now = time.time()
        result = []
        for oid, quote in self._active_orders.items():
            if quote.token_id == up_token_id:
                token_label = "UP"
            elif quote.token_id == dn_token_id:
                token_label = "DN"
            else:
                token_label = "??"
            age = now - quote.placed_at if quote.placed_at > 0 else 0.0
            order_type = "liquidation" if oid in liq_ids else "quote"
            result.append({
                "order_id": oid[:12] + "..." if len(oid) > 12 else oid,
                "side": quote.side,
                "price": quote.price,
                "size": quote.size,
                "notional": round(quote.price * quote.size, 2),
                "token": token_label,
                "age_sec": round(age, 1),
                "type": order_type,
            })
        return result
