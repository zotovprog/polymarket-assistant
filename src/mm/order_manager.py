"""Order Manager — handles order placement, cancellation, and tracking via CLOB API.

Uses py_clob_client for:
- Post-only orders (maker-only, 0% fee)
- GTD (Good-Til-Date) orders that auto-expire
- Batch post/cancel operations
- Fill tracking via order status polling
"""
from __future__ import annotations
import asyncio
from collections import deque
import inspect
import json
import logging
import math
import time
from dataclasses import dataclass
from typing import Any, Optional
import websockets

from .types import Quote, Fill
from .mm_config import MMConfig
from .pm_fees import (
    fetch_fee_rate,
    invalidate_fee_rate_cache,
    net_shares_after_buy_fee,
    taker_fee_usd,
)
from .runtime_metrics import runtime_metrics

log = logging.getLogger("mm.orders")

# Try importing py_clob_client types (available when real client is used)
try:
    from py_clob_client.clob_types import (
        AssetType,
        BalanceAllowanceParams,
        OrderArgs,
        OrderType,
        PostOrdersArgs,
    )
    _HAS_CLOB_TYPES = True
    _HAS_POST_ORDERS_ARGS = True
except ImportError:
    try:
        from py_clob_client.clob_types import (  # type: ignore[no-redef]
            AssetType,
            BalanceAllowanceParams,
            OrderArgs,
            OrderType,
        )
        PostOrdersArgs = None  # type: ignore[assignment]
        _HAS_CLOB_TYPES = True
        _HAS_POST_ORDERS_ARGS = False
    except ImportError:
        PostOrdersArgs = None  # type: ignore[assignment]
        _HAS_CLOB_TYPES = False
        _HAS_POST_ORDERS_ARGS = False


class TradeLedger:
    """Persistent trade history with backfill from API."""

    def __init__(self, max_entries: int = 1000):
        self._trades: list[dict] = []  # All recorded trades
        self._trade_ids: set[str] = set()  # Dedup by trade_id
        self._max_entries = max_entries

    def record(self, trade: dict) -> bool:
        """Record a trade. Returns True if new, False if duplicate."""
        trade_id = trade.get("id") or trade.get("trade_id") or ""
        if trade_id and trade_id in self._trade_ids:
            return False
        if trade_id:
            self._trade_ids.add(trade_id)
        self._trades.append(trade)
        if len(self._trades) > self._max_entries:
            # Remove oldest, update trade_ids set
            removed = self._trades[:len(self._trades) - self._max_entries]
            self._trades = self._trades[-self._max_entries:]
            for r in removed:
                rid = r.get("id") or r.get("trade_id") or ""
                if rid:
                    self._trade_ids.discard(rid)
        return True

    @property
    def trades(self) -> list[dict]:
        return list(self._trades)

    @property
    def count(self) -> int:
        return len(self._trades)

    def summary(self) -> dict:
        """Return summary stats."""
        buys = [t for t in self._trades if t.get("side") == "BUY"]
        sells = [t for t in self._trades if t.get("side") == "SELL"]
        return {
            "total_trades": len(self._trades),
            "buys": len(buys),
            "sells": len(sells),
            "unique_trade_ids": len(self._trade_ids),
        }


@dataclass
class RecentOrderState:
    quote: Quote
    removed_ts: float
    token_id: str
    reason: str
    last_polled_ts: float
    generation: int


@dataclass
class RecentCancelledSellReserve:
    order_id: str
    token_id: str
    remaining_size: float
    cancelled_ts: float
    grace_until: float


@dataclass
class APIErrorEvent:
    ts: float
    op: str
    token_id: str | None
    order_id: str | None
    status_code: int | None
    message: str
    details: dict[str, Any]

    def to_dict(self) -> dict[str, Any]:
        return {
            "ts": round(float(self.ts), 3),
            "op": self.op,
            "token_id": self.token_id[:12] if self.token_id else None,
            "order_id": self.order_id[:12] if self.order_id else None,
            "status_code": self.status_code,
            "message": self.message,
            "details": dict(self.details),
        }


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
        self._recent_orders: dict[str, RecentOrderState] = {}
        self._filled_order_ids: set[str] = set()
        self._partial_fill_reported: dict[str, float] = {}  # order_id -> last reported size_matched
        self._pending_cancels: set[str] = set()
        self._mock_token_balances: dict[str, float] = {}
        self._allowance_set: set[str] = set()  # token IDs with allowance already set
        self._allowance_cap_shares: dict[str, float] = {}  # token_id -> last confirmed sellable size via allowance
        self._session_budget: float = 0.0  # Hard USDC budget cap (0 = no limit)
        self._session_spent: float = 0.0   # Total USDC committed to BUY orders this session
        self._warn_cooldowns: dict[str, float] = {}
        self._fill_ws_task: asyncio.Task | None = None
        self._fill_ws_running = False
        self._ws_fills_queue: asyncio.Queue = asyncio.Queue()
        self.trade_ledger = TradeLedger()
        self._last_fill_check_ts: float = 0.0
        self._last_recent_poll_ts: float = 0.0
        self._usdc_balance_cache: float | None = None
        self._usdc_available_cache: float | None = None
        self._usdc_balance_cache_ts: float = 0.0
        self._usdc_cache_ttl: float = 5.0  # Cache USDC balance for 5 seconds
        self._recent_order_retention_sec: float = max(
            5.0,
            float(getattr(config, "recent_order_retention_sec", 20.0) or 20.0),
        )
        self._recent_order_max_per_token: int = max(
            2,
            int(getattr(config, "recent_order_max_per_token", 8) or 8),
        )
        self._fallback_poll_cap: int = max(
            4,
            int(getattr(config, "fallback_poll_cap", 12) or 12),
        )
        self._recent_poll_interval_sec: float = 3.0
        self._on_fill_callback: Any = None  # Callable or None — called on WS fill
        self._reconcile_requested: bool = False
        self._on_heartbeat_id: Any = None  # Callable(str) — notify heartbeat of new ID
        self._on_ws_reconnect: Any = None  # Callable() — notify WS reconnect event
        self._post_orders_mode: str | None = None  # runtime-resolved py-clob-client signature mode
        self._sell_reject_cooldown_until: dict[str, float] = {}
        self._sell_reject_cooldown_sec: float = max(
            2.0,
            float(getattr(config, "sell_reject_cooldown_sec", 8.0) or 8.0),
        )
        self._cancel_repost_cooldown_until: dict[str, float] = {}
        self._market_min_order_size: float = 5.0
        self._current_token_ids: set[str] = set()
        self._tracking_generation: int = 0
        self._last_fallback_poll_count: int = 0
        self._order_args_supports_fee_rate: bool = self._detect_order_args_fee_rate_support()
        self._recent_api_errors: deque[APIErrorEvent] = deque(maxlen=50)
        self._api_error_counts: dict[str, int] = {}
        self._transport_error_counts: dict[str, int] = {}
        self._last_api_error_ts: float = 0.0
        self._recent_cancelled_sell_reserves: dict[str, RecentCancelledSellReserve] = {}
        self._last_sellability_lag_reason: str = ""
        if hasattr(clob_client, "_mock_token_balances") and isinstance(
            getattr(clob_client, "_mock_token_balances", None),
            dict,
        ):
            self._mock_token_balances = clob_client._mock_token_balances

    def set_heartbeat_id_callback(self, callback) -> None:
        """Set callback to notify HeartbeatManager of new heartbeat_id from PM responses."""
        self._on_heartbeat_id = callback

    def set_ws_reconnect_callback(self, callback) -> None:
        """Set callback invoked when fill WS disconnects/reconnect loop kicks in."""
        self._on_ws_reconnect = callback

    def set_market_context(
        self,
        *,
        min_order_size: float | None = None,
        token_ids: set[str] | list[str] | tuple[str, ...] | None = None,
    ) -> None:
        """Set market-dependent runtime parameters used by pre-trade checks."""
        if min_order_size is not None:
            try:
                self._market_min_order_size = max(0.01, float(min_order_size))
            except (TypeError, ValueError):
                self._market_min_order_size = 5.0
        if token_ids is not None:
            new_token_ids = {
                str(token_id)
                for token_id in token_ids
                if token_id
            }
            if new_token_ids != self._current_token_ids:
                self._tracking_generation += 1
                self._current_token_ids = new_token_ids
                self._prune_recent_orders(prune_all_out_of_scope=True)

    def _pm_min_order_size(self) -> float:
        """Current PM minimum size from market context (safe default = 5)."""
        return max(0.01, float(self._market_min_order_size or 5.0))

    @staticmethod
    def _detect_order_args_fee_rate_support() -> bool:
        """Whether current OrderArgs supports explicit fee rate field."""
        if not _HAS_CLOB_TYPES:
            return False
        try:
            sig = inspect.signature(OrderArgs)
            return "fee_rate_bps" in sig.parameters
        except Exception:
            return hasattr(OrderArgs, "fee_rate_bps")

    @staticmethod
    def _parse_fee_rate_bps(payload: Any) -> Optional[int]:
        """Extract fee rate in bps from /fee-rate payload."""
        if not isinstance(payload, dict):
            return None
        # Prefer explicit bps keys when available.
        for key in ("fee_rate_bps", "feeRateBps", "base_fee_bps", "baseFeeBps"):
            raw_bps = payload.get(key)
            try:
                if raw_bps is None:
                    continue
                bps = int(round(float(raw_bps)))
                if bps >= 0:
                    return bps
            except (TypeError, ValueError):
                continue

        # Some CLOB deployments return ratio-like keys (feeRate/base_fee).
        for key in ("feeRate", "base_fee", "baseFee"):
            raw_rate = payload.get(key)
            try:
                if raw_rate is None:
                    continue
                fee_rate = float(raw_rate)
                if fee_rate < 0:
                    continue
                # API may return ratio (0.1) or bps (1000).
                bps = int(round(fee_rate * 10000)) if fee_rate <= 1.0 else int(round(fee_rate))
                return max(0, bps)
            except (TypeError, ValueError):
                continue
        return None

    async def _resolve_order_fee_rate_bps(self, token_id: str) -> Optional[int]:
        """Fetch fee rate for order signing on live fee-enabled markets.

        Returns:
            int bps when resolved (including 0 for fee-disabled markets),
            None when fee rate is unavailable and order must not be sent.
        """
        if hasattr(self.client, "_orders"):
            return 0

        # Test stubs / non-official clients may not expose full API surface.
        # Keep old behavior for these synthetic clients to avoid false failures in tests.
        if not hasattr(self.client, "get_balance_allowance"):
            return 0

        if not self._order_args_supports_fee_rate:
            self._throttled_warn(
                "fee_rate_unsupported_order_args",
                (
                    "OrderArgs does not support fee_rate_bps on live client. "
                    "Refusing to place orders to avoid fee-enabled market rejects."
                ),
                cooldown=15.0,
            )
            return None

        payload = await fetch_fee_rate(token_id)
        bps = self._parse_fee_rate_bps(payload)
        if bps is None:
            self._throttled_warn(
                f"fee_rate_unavailable:{token_id}",
                f"Fee-rate unavailable for token {token_id[:12]}... — skipping order",
                cooldown=5.0,
            )
            return None
        return bps

    def _build_order_args(self, quote: Quote, *, fee_rate_bps: Optional[int]) -> Any:
        """Construct OrderArgs with explicit fee rate when supported."""
        kwargs: dict[str, Any] = {
            "token_id": quote.token_id,
            "price": quote.price,
            "size": quote.size,
            "side": quote.side,
        }
        if fee_rate_bps is not None and self._order_args_supports_fee_rate:
            kwargs["fee_rate_bps"] = int(max(0, fee_rate_bps))
        return OrderArgs(**kwargs)

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

    @staticmethod
    def _is_signature_type_error(exc: TypeError) -> bool:
        """Heuristic: distinguish call-signature TypeError from runtime TypeError."""
        msg = str(exc).lower()
        markers = (
            "positional argument",
            "keyword argument",
            "required positional argument",
            "unexpected keyword",
        )
        return any(m in msg for m in markers)

    def _post_orders_mode_candidates(self) -> list[str]:
        """Return compatible post_orders invocation modes, most likely first."""
        if self._post_orders_mode:
            return [self._post_orders_mode]

        modes: list[str] = []
        try:
            sig = inspect.signature(self.client.post_orders)
            params = list(sig.parameters.values())
            names = {p.name for p in params}
            positional = [
                p for p in params
                if p.kind in (inspect.Parameter.POSITIONAL_ONLY, inspect.Parameter.POSITIONAL_OR_KEYWORD)
            ]
            if "args" in names:
                modes.append("args_kw")
            if positional:
                modes.append("batch_only")
            if "orders" in names:
                modes.append("orders_kw")
            if "signed_orders" in names:
                modes.append("signed_orders_kw")
            if "order_type" in names or "post_only" in names:
                modes.append("legacy_keywords")
            if len(positional) >= 3:
                modes.append("legacy_positional")
        except Exception:
            pass

        # Safety fallbacks for unknown versions.
        for mode in (
            "batch_only",
            "orders_kw",
            "signed_orders_kw",
            "legacy_keywords",
            "legacy_positional",
        ):
            if mode not in modes:
                modes.append(mode)
        return modes

    @staticmethod
    def _is_payload_shape_error(exc: Exception) -> bool:
        """Errors indicating payload format mismatch for current client version."""
        msg = str(exc).lower()
        return "has no attribute" in msg and "ordertype" in msg

    def _build_post_orders_args(self, batch: list[Any], order_type: Any, post_only: bool) -> list[Any]:
        """Wrap signed orders into PostOrdersArgs when supported by client version."""
        if not _HAS_POST_ORDERS_ARGS or PostOrdersArgs is None:
            return batch

        wrapped: list[Any] = []
        try:
            for signed in batch:
                wrapped.append(
                    PostOrdersArgs(
                        order=signed,
                        orderType=order_type,
                        postOnly=bool(post_only),
                    )
                )
            return wrapped
        except Exception:
            return batch

    async def _post_orders_compat(self, batch: list[Any], order_type: Any, post_only: bool) -> Any:
        """Call client.post_orders across py-clob-client API variants."""
        modes = self._post_orders_mode_candidates()
        has_args_mode = "args_kw" in modes
        last_type_error: TypeError | None = None
        last_payload_error: Exception | None = None
        batch_args = self._build_post_orders_args(batch, order_type, post_only)

        for mode in modes:
            try:
                if mode == "args_kw":
                    resp = await asyncio.to_thread(self.client.post_orders, args=batch_args)
                elif mode == "batch_only":
                    payload = batch_args if has_args_mode else batch
                    resp = await asyncio.to_thread(self.client.post_orders, payload)
                elif mode == "orders_kw":
                    resp = await asyncio.to_thread(self.client.post_orders, orders=batch)
                elif mode == "signed_orders_kw":
                    resp = await asyncio.to_thread(self.client.post_orders, signed_orders=batch)
                elif mode == "legacy_keywords":
                    resp = await asyncio.to_thread(
                        self.client.post_orders,
                        batch,
                        order_type=order_type,
                        post_only=post_only,
                    )
                elif mode == "legacy_positional":
                    resp = await asyncio.to_thread(
                        self.client.post_orders,
                        batch,
                        order_type,
                        post_only,
                    )
                else:
                    continue

                if self._post_orders_mode != mode:
                    log.info("Resolved post_orders mode: %s", mode)
                self._post_orders_mode = mode
                return resp
            except Exception as exc:
                if isinstance(exc, TypeError) and self._is_signature_type_error(exc):
                    last_type_error = exc
                    continue
                if self._is_payload_shape_error(exc):
                    last_payload_error = exc
                    continue
                raise

        if last_type_error is not None:
            raise last_type_error
        if last_payload_error is not None:
            raise last_payload_error
        raise RuntimeError("Failed to resolve compatible post_orders signature")

    @property
    def active_order_ids(self) -> list[str]:
        return list(self._active_orders.keys())

    @property
    def active_orders(self) -> dict[str, Quote]:
        return dict(self._active_orders)

    @property
    def trade_stats(self) -> dict:
        return self.trade_ledger.summary()

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

    @staticmethod
    def _safe_str(value: Any) -> str:
        if value is None:
            return ""
        if isinstance(value, str):
            return value.strip()
        return str(value).strip()

    @staticmethod
    def _compact_raw(payload: Any, max_len: int = 1200) -> str:
        """Compact payload for logs without blowing up line length."""
        try:
            raw = json.dumps(payload, ensure_ascii=True, separators=(",", ":"), default=str)
        except Exception:
            raw = repr(payload)
        if len(raw) > max_len:
            return f"{raw[:max_len]}...<truncated>"
        return raw

    @staticmethod
    def _extract_status_code(payload: Any) -> int | None:
        if payload is None:
            return None
        if isinstance(payload, dict):
            for key in ("status_code", "statusCode", "code"):
                raw = payload.get(key)
                try:
                    if raw is None:
                        continue
                    return int(raw)
                except (TypeError, ValueError):
                    continue
            error = payload.get("error")
            if isinstance(error, dict):
                for key in ("status_code", "statusCode", "code"):
                    raw = error.get(key)
                    try:
                        if raw is None:
                            continue
                        return int(raw)
                    except (TypeError, ValueError):
                        continue
            return None

        for attr in ("status_code", "status"):
            raw = getattr(payload, attr, None)
            try:
                if raw is None:
                    continue
                return int(raw)
            except (TypeError, ValueError):
                continue
        response = getattr(payload, "response", None)
        if response is not None:
            for attr in ("status_code", "status"):
                raw = getattr(response, attr, None)
                try:
                    if raw is None:
                        continue
                    return int(raw)
                except (TypeError, ValueError):
                    continue
        return None

    def _record_api_error(
        self,
        *,
        op: str,
        message: str,
        token_id: str | None = None,
        order_id: str | None = None,
        status_code: int | None = None,
        details: dict[str, Any] | None = None,
        transient: bool = False,
    ) -> None:
        safe_message = self._safe_str(message) or "unknown api error"
        safe_details = dict(details or {})
        if transient:
            safe_details["transient"] = True
        event = APIErrorEvent(
            ts=time.time(),
            op=op,
            token_id=self._safe_str(token_id) or None,
            order_id=self._safe_str(order_id) or None,
            status_code=status_code,
            message=safe_message,
            details=safe_details,
        )
        self._recent_api_errors.append(event)
        self._api_error_counts[op] = int(self._api_error_counts.get(op, 0)) + 1
        if self._is_transport_error_event(op=op, message=safe_message, details=safe_details, status_code=status_code):
            self._transport_error_counts[op] = int(self._transport_error_counts.get(op, 0)) + 1
        self._last_api_error_ts = event.ts

    def _is_transport_error_event(
        self,
        *,
        op: str,
        message: str,
        details: dict[str, Any] | None,
        status_code: int | None,
    ) -> bool:
        del status_code
        payload = details or {}
        if bool(payload.get("transient")):
            return False
        message_l = str(message or "").lower()
        if "crosses book" in message_l or "invalid post-only order" in message_l:
            return False
        if self._is_balance_or_allowance_reject(message_l):
            return False
        return True

    def get_api_error_stats(self) -> dict[str, Any]:
        return {
            "total_by_op": dict(sorted(self._api_error_counts.items())),
            "transport_total_by_op": dict(sorted(self._transport_error_counts.items())),
            "recent": [event.to_dict() for event in list(self._recent_api_errors)[-10:]],
            "last_error_ts": round(self._last_api_error_ts, 3) if self._last_api_error_ts > 0 else 0.0,
        }

    def get_placement_error_total(self) -> int:
        """Count real placement/signing failures recorded through API-error paths."""
        placement_ops = (
            "place_order",
            "place_order_taker_fallback",
            "place_batch",
            "place_batch_item",
            "sign_order",
        )
        return sum(int(self._api_error_counts.get(op, 0)) for op in placement_ops)

    @staticmethod
    def _clone_quote(quote: Quote) -> Quote:
        """Detach quote snapshot for post-cancel fill reconciliation."""
        return Quote(
            side=quote.side,
            token_id=quote.token_id,
            price=float(quote.price),
            size=float(quote.size),
            order_id=quote.order_id,
            placed_at=float(quote.placed_at or 0.0),
        )

    def _track_recent_order(
        self,
        order_id: str,
        quote: Quote | None,
        *,
        reason: str = "cancelled",
    ) -> None:
        """Keep recently removed order for late fill accounting."""
        if not order_id or quote is None:
            return
        if order_id in self._filled_order_ids or reason in {"matched", "closed"}:
            self._recent_orders.pop(order_id, None)
            self._partial_fill_reported.pop(order_id, None)
            return
        token_id = self._safe_str(getattr(quote, "token_id", "")) or ""
        if self._current_token_ids and token_id and token_id not in self._current_token_ids:
            return
        self._recent_orders[order_id] = RecentOrderState(
            quote=self._clone_quote(quote),
            removed_ts=time.time(),
            token_id=token_id,
            reason=reason,
            last_polled_ts=0.0,
            generation=self._tracking_generation,
        )
        self._prune_recent_orders()

    def _prune_recent_cancelled_sell_reserves(self, now: float | None = None) -> None:
        ts = now if now is not None else time.time()
        stale = [
            order_id
            for order_id, reserve in self._recent_cancelled_sell_reserves.items()
            if reserve.grace_until <= ts or reserve.remaining_size <= 0.0
        ]
        for order_id in stale:
            self._recent_cancelled_sell_reserves.pop(order_id, None)

    def _set_cancel_repost_cooldown(self, token_id: str) -> None:
        token = self._safe_str(token_id)
        if not token:
            return
        until = time.time() + float(getattr(self.config, "sell_release_grace_sec", 3.0) or 3.0)
        prev = float(self._cancel_repost_cooldown_until.get(token, 0.0))
        if until > prev:
            self._cancel_repost_cooldown_until[token] = until

    def _cancel_repost_cooldown_left(self, token_id: str) -> float:
        token = self._safe_str(token_id)
        if not token:
            return 0.0
        until = float(self._cancel_repost_cooldown_until.get(token, 0.0))
        if until <= 0:
            return 0.0
        now = time.time()
        if until <= now:
            self._cancel_repost_cooldown_until.pop(token, None)
            return 0.0
        return until - now

    def _active_sell_inventory(self, token_id: str) -> float:
        reserved = 0.0
        for order_id, quote in self._active_orders.items():
            if quote.token_id != token_id:
                continue
            reserved += self._remaining_active_sell_size(order_id, quote)
        return reserved

    def _recent_cancelled_sell_inventory(self, token_id: str, now: float | None = None) -> float:
        if bool(getattr(self.config, "allow_short_sells", False)):
            return 0.0
        self._prune_recent_cancelled_sell_reserves(now)
        reserved = 0.0
        for reserve in self._recent_cancelled_sell_reserves.values():
            if reserve.token_id != token_id:
                continue
            reserved += max(0.0, float(reserve.remaining_size))
        return reserved

    def _add_recent_cancelled_sell_reserve(self, order_id: str, quote: Quote | None) -> None:
        if bool(getattr(self.config, "allow_short_sells", False)):
            return
        if not order_id or quote is None or (quote.side or "").upper() != "SELL":
            return
        remaining = self._remaining_active_sell_size(order_id, quote)
        if remaining <= 0.0:
            return
        now = time.time()
        self._recent_cancelled_sell_reserves[order_id] = RecentCancelledSellReserve(
            order_id=order_id,
            token_id=quote.token_id,
            remaining_size=float(remaining),
            cancelled_ts=now,
            grace_until=now + float(getattr(self.config, "sell_release_grace_sec", 3.0) or 3.0),
        )

    def _clear_recent_cancelled_sell_reserves_for_token(self, token_id: str) -> None:
        if not token_id:
            return
        stale = [
            order_id
            for order_id, reserve in self._recent_cancelled_sell_reserves.items()
            if reserve.token_id == token_id
        ]
        for order_id in stale:
            self._recent_cancelled_sell_reserves.pop(order_id, None)

    def _reduce_recent_cancelled_sell_reserves(self, token_id: str, filled_size: float) -> None:
        remaining_fill = max(0.0, float(filled_size))
        if remaining_fill <= 0.0:
            return
        self._prune_recent_cancelled_sell_reserves()
        ordered = sorted(
            (
                (order_id, reserve)
                for order_id, reserve in self._recent_cancelled_sell_reserves.items()
                if reserve.token_id == token_id
            ),
            key=lambda item: item[1].cancelled_ts,
        )
        for order_id, reserve in ordered:
            if remaining_fill <= 0.0:
                break
            if reserve.remaining_size <= remaining_fill + 1e-9:
                remaining_fill -= reserve.remaining_size
                self._recent_cancelled_sell_reserves.pop(order_id, None)
            else:
                reserve.remaining_size = max(0.0, reserve.remaining_size - remaining_fill)
                remaining_fill = 0.0

    async def get_sellable_token_balance(self, token_id: str) -> Optional[float]:
        """Best-effort estimate of how many shares can be safely sold right now.

        For live CONDITIONAL balances PM may lag when releasing inventory after a
        SELL cancel. We therefore keep recently cancelled SELL reserve separate
        from wallet truth and expose a conservative sellable balance to V2 asks.
        """
        raw_balance = await self.get_token_balance(token_id)
        if raw_balance is None:
            return None
        if hasattr(self.client, "_orders"):
            return max(0.0, float(await self.get_reconcile_token_balance(token_id) or 0.0))
        active_sell_reserve = self._active_sell_inventory(token_id)
        recent_cancelled_reserve = self._recent_cancelled_sell_inventory(token_id)
        return max(0.0, float(raw_balance) + active_sell_reserve - recent_cancelled_reserve)

    def get_sell_release_lag_snapshot(
        self,
        *,
        up_token_id: str | None = None,
        dn_token_id: str | None = None,
    ) -> dict[str, Any]:
        now = time.time()
        self._prune_recent_cancelled_sell_reserves(now)

        def _seconds_left(token_id: str | None) -> float:
            if not token_id:
                return 0.0
            left = 0.0
            for reserve in self._recent_cancelled_sell_reserves.values():
                if reserve.token_id != token_id:
                    continue
                left = max(left, max(0.0, reserve.grace_until - now))
            return left

        up_reserve = self._recent_cancelled_sell_inventory(up_token_id or "", now) if up_token_id else 0.0
        dn_reserve = self._recent_cancelled_sell_inventory(dn_token_id or "", now) if dn_token_id else 0.0
        up_cooldown = self._cancel_repost_cooldown_left(up_token_id or "")
        dn_cooldown = self._cancel_repost_cooldown_left(dn_token_id or "")
        has_reserve = up_reserve > 0.0 or dn_reserve > 0.0
        has_cooldown = up_cooldown > 0.0 or dn_cooldown > 0.0
        if has_reserve and has_cooldown:
            active_reason = "both"
        elif has_reserve:
            active_reason = "recent_cancelled_reserve"
        elif has_cooldown:
            active_reason = "post_cancel_cooldown"
        else:
            active_reason = ""
        return {
            "active": bool(has_reserve or has_cooldown),
            "up_reserve": round(float(up_reserve), 4),
            "dn_reserve": round(float(dn_reserve), 4),
            "up_seconds_left": round(_seconds_left(up_token_id), 3),
            "dn_seconds_left": round(_seconds_left(dn_token_id), 3),
            "up_cooldown_sec": round(float(up_cooldown), 3),
            "dn_cooldown_sec": round(float(dn_cooldown), 3),
            "active_reason": active_reason,
            "reason": self._last_sellability_lag_reason or active_reason,
        }

    def _prune_recent_orders(
        self,
        now: float | None = None,
        *,
        prune_all_out_of_scope: bool = False,
    ) -> None:
        """Drop expired recently removed orders."""
        ts = now if now is not None else time.time()
        ttl = max(5.0, float(self._recent_order_retention_sec))
        stale_ids: list[str] = []
        scoped_recent: list[tuple[str, RecentOrderState]] = []
        for oid, state in self._recent_orders.items():
            if prune_all_out_of_scope and (
                state.generation != self._tracking_generation
                or (self._current_token_ids and state.token_id not in self._current_token_ids)
            ):
                stale_ids.append(oid)
                continue
            if (ts - state.removed_ts) > ttl:
                stale_ids.append(oid)
                continue
            if state.generation != self._tracking_generation:
                stale_ids.append(oid)
                continue
            if self._current_token_ids and state.token_id and state.token_id not in self._current_token_ids:
                stale_ids.append(oid)
                continue
            scoped_recent.append((oid, state))

        if scoped_recent:
            by_token: dict[str, list[tuple[str, RecentOrderState]]] = {}
            for oid, state in scoped_recent:
                by_token.setdefault(state.token_id or "", []).append((oid, state))
            for items in by_token.values():
                items.sort(key=lambda entry: entry[1].removed_ts, reverse=True)
                for oid, _state in items[self._recent_order_max_per_token:]:
                    stale_ids.append(oid)
            scoped_recent.sort(key=lambda entry: entry[1].removed_ts, reverse=True)
            for oid, _state in scoped_recent[max(16, self._fallback_poll_cap + 4):]:
                stale_ids.append(oid)

        for oid in stale_ids:
            self._recent_orders.pop(oid, None)
            self._partial_fill_reported.pop(oid, None)

    def _get_tracked_quote(self, order_id: str) -> Quote | None:
        """Lookup order in active set first, then recently cancelled set."""
        quote = self._active_orders.get(order_id)
        if quote is not None:
            return quote
        recent = self._recent_orders.get(order_id)
        if recent is not None:
            return recent.quote
        return None

    @classmethod
    def _extract_batch_reject_reason(cls, payload: Any) -> str:
        """Best-effort reject reason extraction from post_orders item payload."""
        if isinstance(payload, str):
            return payload.strip() or "empty response"
        if not isinstance(payload, dict):
            return f"unexpected payload type: {type(payload).__name__}"

        error = payload.get("error")
        if isinstance(error, dict):
            for key in ("message", "error", "reason", "code"):
                msg = cls._safe_str(error.get(key))
                if msg:
                    return msg
        elif error is not None:
            msg = cls._safe_str(error)
            if msg:
                return msg

        for key in (
            "errorMsg",
            "error_msg",
            "error_message",
            "message",
            "reason",
            "status",
            "failureReason",
            "rejectReason",
        ):
            msg = cls._safe_str(payload.get(key))
            if msg:
                return msg

        return "missing order id / unknown reject reason"

    @staticmethod
    def _is_balance_or_allowance_reject(reason: str) -> bool:
        """Detect PM rejects that indicate balance/allowance mismatch."""
        text = (reason or "").strip().lower()
        if not text:
            return False
        markers = (
            "not enough balance",
            "insufficient balance",
            "insufficient collateral",
            "not enough collateral",
            "allowance",
        )
        return any(marker in text for marker in markers)

    @staticmethod
    def _is_fee_or_signature_reject(reason: str) -> bool:
        """Detect rejects likely caused by stale fee rate / signing payload mismatch."""
        text = (reason or "").strip().lower()
        if not text:
            return False
        markers = (
            "feerate",
            "fee rate",
            "fee_rate_bps",
            "feeratebps",
            "signature",
            "invalid auth",
            "invalid order",
        )
        return any(marker in text for marker in markers)

    def _handle_fee_or_signature_reject(
        self,
        quote: Quote,
        *,
        reason: str,
        source: str,
    ) -> None:
        """Invalidate token fee cache after probable fee/signature rejects."""
        if not self._is_fee_or_signature_reject(reason):
            return
        invalidate_fee_rate_cache(quote.token_id)
        self._throttled_warn(
            f"fee_reject_refresh:{source}:{quote.token_id}",
            (
                f"Fee/signature reject ({source}) for {quote.token_id[:8]}: "
                f"{reason}. Invalidated fee-rate cache."
            ),
            cooldown=2.0,
        )

    def _sell_reject_cooldown_left(self, token_id: str) -> float:
        """Seconds left for SELL cooldown after balance/allowance reject."""
        until = float(self._sell_reject_cooldown_until.get(token_id, 0.0))
        if until <= 0:
            return 0.0
        now = time.time()
        if until <= now:
            self._sell_reject_cooldown_until.pop(token_id, None)
            return 0.0
        return until - now

    def _should_skip_sell_after_reject(self, quote: Quote, *, source: str) -> bool:
        """Block repeated SELL attempts for same token right after reject."""
        if quote.side != "SELL":
            return False
        cooldown_left = self._sell_reject_cooldown_left(quote.token_id)
        if cooldown_left <= 0:
            return False

        self._throttled_warn(
            f"sell_reject_cooldown:{source}:{quote.token_id}",
            (
                f"SELL skipped ({source}) {quote.token_id[:8]} "
                f"{quote.size:.1f}@{quote.price:.2f}: cooldown {cooldown_left:.1f}s "
                f"after balance/allowance reject"
            ),
            cooldown=2.0,
        )
        return True

    def _should_skip_sell_after_cancel(self, quote: Quote, *, source: str) -> bool:
        """Block immediate SELL repost right after SELL cancel for same token."""
        if quote.side != "SELL":
            return False
        cooldown_left = self._cancel_repost_cooldown_left(quote.token_id)
        if cooldown_left <= 0:
            return False
        self._throttled_warn(
            f"sell_post_cancel_cooldown:{source}:{quote.token_id}",
            (
                f"SELL skipped ({source}) {quote.token_id[:8]} "
                f"{quote.size:.1f}@{quote.price:.2f}: post-cancel cooldown "
                f"{cooldown_left:.1f}s"
            ),
            cooldown=2.0,
        )
        return True

    def _mark_reconcile_on_balance_reject(
        self,
        quote: Quote,
        *,
        reason: str,
        source: str,
        raw: Any = None,
    ) -> None:
        """Raise reconcile request + local cooldown/cache reset after hard reject."""
        if not self._is_balance_or_allowance_reject(reason):
            return

        self._reconcile_requested = True
        self.invalidate_usdc_cache()
        extra = ""
        if quote.side == "SELL":
            recent_cancelled = self._recent_cancelled_sell_inventory(quote.token_id)
            sellability_lag = recent_cancelled > 0.0
            if sellability_lag:
                self._last_sellability_lag_reason = (
                    f"{source}: sellability_lag ({recent_cancelled:.2f} recently cancelled shares)"
                )
            else:
                self._last_sellability_lag_reason = ""
                self._allowance_set.discard(quote.token_id)
                self._allowance_cap_shares.pop(quote.token_id, None)
            until = time.time() + self._sell_reject_cooldown_sec
            prev = float(self._sell_reject_cooldown_until.get(quote.token_id, 0.0))
            if until > prev:
                self._sell_reject_cooldown_until[quote.token_id] = until
            if sellability_lag:
                extra = (
                    f", classify=sellability_lag, recent_cancelled={recent_cancelled:.2f}, "
                    f"sell_cooldown={self._sell_reject_cooldown_sec:.1f}s"
                )
            else:
                extra = f", sell_cooldown={self._sell_reject_cooldown_sec:.1f}s"

        raw_suffix = ""
        if raw is not None:
            raw_suffix = f", raw={self._compact_raw(raw, max_len=240)}"
        self._throttled_warn(
            f"balance_reject_reconcile:{source}:{quote.side}:{quote.token_id}",
            (
                f"Balance/allowance reject ({source}): {quote.side} {quote.token_id[:8]} "
                f"{quote.size:.1f}@{quote.price:.2f}, reason={reason} "
                f"-> forcing reconcile{extra}{raw_suffix}"
            ),
            cooldown=2.0,
        )

    async def _retry_buy_after_balance_reject(
        self,
        quote: Quote,
        *,
        use_post_only: bool,
    ) -> Optional[str]:
        """Retry BUY after balance reject using fresh available balance and PM min size guard."""
        if quote.side != "BUY":
            return None
        if quote.price <= 0:
            return None

        usdc_bal = await self.get_usdc_available_balance(force_refresh=True)
        if usdc_bal is None:
            self._throttled_warn(
                "buy_balance_retry_no_usdc",
                (
                    f"BUY retry skipped: failed to refresh available USDC after reject "
                    f"for {quote.token_id[:8]}"
                ),
                cooldown=3.0,
            )
            return None

        active_buy_collateral = sum(
            self.required_collateral(q)
            for q in self._active_orders.values()
            if q.side == "BUY"
        )
        available = max(0.0, usdc_bal - active_buy_collateral)
        max_size = math.floor((available / quote.price) * 100.0) / 100.0
        pm_min = self._pm_min_order_size()

        if max_size < pm_min:
            self._throttled_warn(
                "buy_balance_retry_below_min",
                (
                    f"BUY retry skipped: refreshed max_size={max_size:.2f} < PM min {pm_min:.1f} "
                    f"(available=${available:.2f}, price={quote.price:.2f})"
                ),
                cooldown=3.0,
            )
            return None

        new_size = min(max_size, float(quote.size))
        if new_size >= quote.size - 0.01:
            self._throttled_warn(
                "buy_balance_retry_not_reducing",
                (
                    f"BUY retry skipped: no meaningful size reduction after reject "
                    f"(size={quote.size:.2f}, max={max_size:.2f})"
                ),
                cooldown=3.0,
            )
            return None

        old_size = quote.size
        quote.size = round(new_size, 2)
        self._throttled_warn(
            "buy_balance_retry_clamp",
            (
                f"BUY balance retry clamp: {quote.token_id[:8]} "
                f"{old_size:.1f}→{quote.size:.1f} (available=${available:.2f})"
            ),
            cooldown=2.0,
        )

        try:
            return await self._place_order_inner(quote, use_post_only)
        except Exception as e:
            self._mark_reconcile_on_balance_reject(
                quote,
                reason=str(e),
                source="single_place_retry",
            )
            log.error("BUY retry after balance reject failed: %s", e)
            return None

    async def _retry_batch_reject_as_taker(
        self,
        quote: Quote,
        *,
        reason: str,
    ) -> Optional[str]:
        """Retry a batch post-only reject as taker with guardrails.

        Guardrails:
        - only for crossing-book rejects,
        - block retries when quote is too aggressive vs current BBO,
        - block retries when estimated taker fee exceeds max_loss_per_fill_usd.
        """
        # Safety policy: in maker mode we do not silently convert rejected
        # post-only quotes into taker orders.
        if bool(getattr(self.config, "use_post_only", True)):
            return None

        reason_l = (reason or "").lower()
        if "crosses book" not in reason_l:
            return None

        max_cross_bps = max(
            5.0,
            float(getattr(self.config, "requote_threshold_bps", 25.0) or 25.0),
        )
        max_fee_usd = max(0.0, float(getattr(self.config, "max_loss_per_fill_usd", 0.0) or 0.0))

        best_bid: float | None = None
        best_ask: float | None = None
        try:
            book = await self.get_book_summary(quote.token_id)
            if isinstance(book, dict):
                best_bid = self._safe_float(book.get("best_bid"))
                best_ask = self._safe_float(book.get("best_ask"))
        except Exception:
            best_bid = None
            best_ask = None

        estimated_fill_price = quote.price
        if quote.side == "BUY" and best_ask is not None and best_ask > 0:
            max_allowed = best_ask * (1.0 + max_cross_bps / 10000.0)
            if quote.price > max_allowed + 1e-9:
                log.warning(
                    "Batch taker retry blocked BUY %s %.1f@%.2f: "
                    "cross too wide vs ask=%.2f (max=%0.4f, limit=%0.4f)",
                    quote.token_id[:8],
                    quote.size,
                    quote.price,
                    best_ask,
                    max_cross_bps,
                    max_allowed,
                )
                return None
            estimated_fill_price = min(quote.price, best_ask)
        elif quote.side == "SELL" and best_bid is not None and best_bid > 0:
            min_allowed = best_bid * (1.0 - max_cross_bps / 10000.0)
            if quote.price < min_allowed - 1e-9:
                log.warning(
                    "Batch taker retry blocked SELL %s %.1f@%.2f: "
                    "cross too wide vs bid=%.2f (max=%0.4f, limit=%0.4f)",
                    quote.token_id[:8],
                    quote.size,
                    quote.price,
                    best_bid,
                    max_cross_bps,
                    min_allowed,
                )
                return None
            estimated_fill_price = max(quote.price, best_bid)

        est_fee = taker_fee_usd(
            estimated_fill_price,
            quote.size,
            quote.side,
            token_id=quote.token_id,
        )
        if max_fee_usd > 0 and est_fee > max_fee_usd + 1e-9:
            log.warning(
                "Batch taker retry blocked %s %s %.1f@%.2f: "
                "estimated fee $%.4f > max_loss_per_fill_usd $%.4f",
                quote.side,
                quote.token_id[:8],
                quote.size,
                quote.price,
                est_fee,
                max_fee_usd,
            )
            return None

        log.info(
            "Batch reject fallback: retrying as taker %s %s %.1f@%.2f (reason=%s)",
            quote.side,
            quote.token_id[:8],
            quote.size,
            quote.price,
            reason,
        )
        return await self.place_order(quote, post_only=False, fallback_taker=False)

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

    @classmethod
    def _extract_trade_id(cls, payload: dict[str, Any], order_id: str = "") -> str:
        """Extract trade-level identifier from PM payload (not order_id)."""
        if not isinstance(payload, dict):
            return ""
        order_ref = cls._safe_str(order_id)
        for key in (
            "trade_id",
            "tradeID",
            "id",
            "match_id",
            "matchId",
            "fill_id",
            "fillId",
            "transaction_hash",
            "transactionHash",
            "tx_hash",
            "txHash",
            "hash",
        ):
            candidate = cls._safe_str(payload.get(key))
            if candidate and candidate != order_ref:
                return candidate
        return ""

    @classmethod
    def _build_ledger_trade_id(
        cls,
        source: str,
        payload: dict[str, Any],
        order_id: str,
        fill_size: float,
        fill_price: float,
        prev_matched: float = 0.0,
    ) -> str:
        """Return a stable dedup key for ledger records."""
        trade_id = cls._extract_trade_id(payload, order_id=order_id)
        if trade_id:
            return trade_id

        data = payload if isinstance(payload, dict) else {}
        ts = cls._safe_str(
            data.get("timestamp")
            or data.get("time")
            or data.get("created_at")
            or data.get("updated_at")
            or data.get("transacted_at")
        )
        seq = cls._safe_str(
            data.get("sequence")
            or data.get("seq")
            or data.get("offset")
            or data.get("nonce")
        )
        status = cls._safe_str(data.get("status"))
        matched = cls._safe_str(
            data.get("size_matched")
            or data.get("matched_size")
            or data.get("filled_size")
        )
        return (
            f"{source}:{cls._safe_str(order_id)}:{ts}:{seq}:{status}:{matched}:"
            f"{prev_matched:.4f}:{fill_size:.4f}:{fill_price:.6f}"
        )

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

    async def _enforce_close_only_sell(self, quote: Quote) -> bool:
        """Enforce close-only SELL behavior when shorting is disabled.

        Returns True when SELL can proceed (possibly with reduced size),
        False when SELL must be skipped.
        """
        if quote.side != "SELL":
            return True
        if bool(getattr(self.config, "allow_short_sells", False)):
            return True

        token_bal = await self.get_token_balance(quote.token_id)
        if token_bal is None:
            self._throttled_warn(
                "sell_close_only_balance_unavailable",
                (
                    f"SELL blocked (close-only): failed to fetch token balance for "
                    f"{quote.token_id[:12]}..."
                ),
                cooldown=10.0,
            )
            return False

        active_sell_exposure = self._active_sell_inventory(quote.token_id)
        recent_cancelled_exposure = self._recent_cancelled_sell_inventory(quote.token_id)
        free_inventory = max(
            0.0,
            float(token_bal) - active_sell_exposure - recent_cancelled_exposure,
        )

        pm_min = self._pm_min_order_size()
        # Keep a tiny safety buffer for live SELLs to avoid exchange-side
        # precision/race rejects when local balance reads equal requested size.
        safety_buffer = 0.02
        raw_free = math.floor(free_inventory * 100.0) / 100.0
        if free_inventory >= (pm_min + safety_buffer):
            effective_free = math.floor(max(0.0, free_inventory - safety_buffer) * 100.0) / 100.0
        else:
            effective_free = raw_free

        if effective_free + 1e-9 >= quote.size:
            return True

        if effective_free >= pm_min:
            old = quote.size
            quote.size = round(effective_free, 2)
            self._throttled_warn(
                "sell_close_only_trimmed",
                (
                    f"SELL trimmed (close-only) {quote.token_id[:8]} "
                    f"{old:.1f}→{quote.size:.1f} shares "
                    f"(free={free_inventory:.2f}, active={active_sell_exposure:.2f}, "
                    f"recent_cancelled={recent_cancelled_exposure:.2f}, effective={effective_free:.2f})"
                ),
                cooldown=5.0,
            )
            return True

        self._throttled_warn(
            "sell_close_only_blocked",
            (
                f"SELL blocked (close-only) {quote.token_id[:8]} "
                f"need={quote.size:.1f} free={free_inventory:.2f} "
                f"active={active_sell_exposure:.2f} recent_cancelled={recent_cancelled_exposure:.2f} "
                f"effective={effective_free:.2f}"
            ),
            cooldown=5.0,
        )
        return False

    def estimate_reserved_collateral(
        self,
        token_balances: Optional[dict[str, float]] = None,
    ) -> dict[str, float]:
        """Estimate USDC collateral currently reserved by active orders.

        BUY orders always reserve `size * price`.
        SELL orders only reserve short leg collateral:
        `max(0, sell_size - available_token_balance) * (1 - price)`.
        """
        balances = {
            token_id: max(0.0, float(balance))
            for token_id, balance in (token_balances or {}).items()
        }

        buy_reserved = 0.0
        short_reserved = 0.0

        for quote in self._active_orders.values():
            side = (quote.side or "").upper()
            size = max(0.0, float(quote.size))
            price = max(0.0, min(1.0, float(quote.price)))
            if side == "BUY":
                buy_reserved += size * price
                continue
            if side != "SELL":
                continue

            token_id = quote.token_id
            available = balances.get(token_id, 0.0)
            close_size = min(size, available)
            balances[token_id] = max(0.0, available - close_size)
            short_size = max(0.0, size - close_size)
            short_reserved += short_size * (1.0 - price)

        total_reserved = buy_reserved + short_reserved
        return {
            "buy_reserved": buy_reserved,
            "short_reserved": short_reserved,
            "total_reserved": total_reserved,
        }

    @staticmethod
    def _decode_usdc_amount(raw: Any) -> Optional[float]:
        """Decode PM balance value to USDC units.

        PM usually returns base units (1e6). If a decimal string/float is returned,
        keep it as-is.
        """
        if raw is None:
            return None
        try:
            if isinstance(raw, str):
                s = raw.strip()
                if not s:
                    return None
                val = float(s)
                return val if "." in s else (val / 1e6)
            if isinstance(raw, int):
                return float(raw) / 1e6
            val = float(raw)
            # If float is integer-like and large enough, treat as base units.
            if val.is_integer() and abs(val) >= 1_000:
                return val / 1e6
            return val
        except (TypeError, ValueError):
            return None

    @classmethod
    def _extract_usdc_balances(cls, payload: Any) -> tuple[Optional[float], Optional[float]]:
        """Extract total and available USDC from PM balance payload."""
        if not isinstance(payload, dict):
            return None, None

        total = None
        available = None

        for key in ("balance", "total", "total_balance", "totalBalance"):
            total = cls._decode_usdc_amount(payload.get(key))
            if total is not None:
                break

        for key in ("available", "available_balance", "availableBalance", "free", "free_balance"):
            available = cls._decode_usdc_amount(payload.get(key))
            if available is not None:
                break

        if total is None and available is not None:
            total = available
        if available is None and total is not None:
            available = total
        return total, available

    async def ensure_sell_allowance(
        self,
        token_id: str,
        *,
        required_shares: float | None = None,
        force_refresh: bool = False,
    ) -> bool:
        """Ensure ERC1155 operator allowance is set for SELL orders on this token.

        Polymarket CLOB requires operator approval before you can SELL conditional tokens.
        This calls update_balance_allowance() which is idempotent (safe to call multiple times).
        Results are cached per token_id to avoid redundant API calls, but cache is
        bypassed when required size exceeds last confirmed allowance capacity.

        Returns True if allowance is OK, False on failure.
        """
        if hasattr(self.client, "_orders"):  # Mock client
            return True
        if not _HAS_CLOB_TYPES:
            return False

        req_shares = max(0.0, float(required_shares or 0.0))
        if token_id in self._allowance_set and not force_refresh:
            cached_cap = max(0.0, float(self._allowance_cap_shares.get(token_id, 0.0)))
            if req_shares <= 0.0 or req_shares <= (cached_cap + 0.01):
                return True

        try:
            # Check current allowance
            result = await asyncio.to_thread(
                self.client.get_balance_allowance,
                BalanceAllowanceParams(
                    asset_type=AssetType.CONDITIONAL,
                    token_id=token_id,
                ),
            )
            allowance_raw = max(0.0, float(result.get("allowance", 0)))
            balance_raw = max(0.0, float(result.get("balance", 0)))
            req_raw = max(0.0, req_shares * 1e6)
            required_raw = max(balance_raw, req_raw)

            log.info(
                "Token %s... balance=%.2f allowance=%.2f required=%.2f",
                token_id[:12],
                balance_raw / 1e6,
                allowance_raw / 1e6,
                required_raw / 1e6,
            )

            if allowance_raw <= 0 or allowance_raw + 1 < required_raw:
                log.info(
                    "Setting allowance for %s... (required %.2f shares)",
                    token_id[:12],
                    required_raw / 1e6,
                )
                await asyncio.to_thread(
                    self.client.update_balance_allowance,
                    BalanceAllowanceParams(
                        asset_type=AssetType.CONDITIONAL,
                        token_id=token_id,
                    ),
                )
                log.info(f"Allowance updated for {token_id[:12]}...")
                # update_balance_allowance is async on-chain; assume at least required amount
                allowance_raw = max(allowance_raw, required_raw)

            cap_shares = max(0.0, allowance_raw / 1e6)
            self._allowance_set.add(token_id)
            self._allowance_cap_shares[token_id] = cap_shares
            return True
        except Exception as e:
            self._record_api_error(
                op="ensure_sell_allowance",
                token_id=token_id,
                status_code=self._extract_status_code(e),
                message=str(e),
                details={
                    "required_shares": req_shares,
                    "force_refresh": force_refresh,
                    "transient": True,
                },
            )
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
            fee_rate_bps = await self._resolve_order_fee_rate_bps(quote.token_id)
            if fee_rate_bps is None:
                raise RuntimeError(
                    f"fee rate unavailable for token {quote.token_id[:12]}..."
                )
            oa = self._build_order_args(quote, fee_rate_bps=fee_rate_bps)
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
        self.invalidate_usdc_cache()
        if quote.side == "SELL":
            self._sell_reject_cooldown_until.pop(quote.token_id, None)
            self._cancel_repost_cooldown_until.pop(quote.token_id, None)
            self._clear_recent_cancelled_sell_reserves_for_token(quote.token_id)
            self._last_sellability_lag_reason = ""
        self._notify_heartbeat_id(resp)
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
        if quote.side == "SELL" and self._should_skip_sell_after_cancel(quote, source="single_place"):
            return None
        if quote.side == "SELL" and self._should_skip_sell_after_reject(quote, source="single_place"):
            return None

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
            # Force fresh balance read for live BUY pre-check to avoid stale cache overspend windows.
            usdc_bal = await self.get_usdc_available_balance(force_refresh=True)
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
                        pm_min = self._pm_min_order_size()
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
            if not await self.ensure_sell_allowance(
                quote.token_id,
                required_shares=quote.size,
            ):
                log.warning(f"Cannot place SELL — allowance setup failed for {quote.token_id[:12]}...")
                return None
            if not await self._enforce_close_only_sell(quote):
                return None

            # SELL pre-check:
            # - Inventory-backed close: require enough free tokens, skip USDC collateral check.
            # - True short SELL: only short remainder requires USDC collateral.
            free_inventory = 0.0
            token_bal = await self.get_token_balance(quote.token_id)
            active_sell_exposure = self._active_sell_inventory(quote.token_id)
            recent_cancelled_exposure = self._recent_cancelled_sell_inventory(quote.token_id)
            local_inventory = quote.size + active_sell_exposure + recent_cancelled_exposure
            # If token balance unavailable, fall back to local inventory for close detection
            if token_bal is None:
                # Use local inventory estimate — safer than assuming short
                token_balance_for_close_check = local_inventory if local_inventory > 0 else 0.0
            else:
                token_balance_for_close_check = token_bal
            free_inventory = max(
                0.0,
                token_balance_for_close_check - active_sell_exposure - recent_cancelled_exposure,
            )
            if free_inventory + 0.01 >= quote.size:
                log.debug(
                    "SELL pre-check: inventory-backed close %.2f shares "
                    "(token=%.2f active_sell=%.2f recent_cancelled=%.2f) — skipping USDC collateral check",
                    quote.size,
                    token_balance_for_close_check,
                    active_sell_exposure,
                    recent_cancelled_exposure,
                )

            allow_short_sells = bool(getattr(self.config, "allow_short_sells", False))
            short_size = max(0.0, quote.size - free_inventory)
            short_collateral = short_size * (1.0 - quote.price)

            if allow_short_sells and short_collateral > 0.01:
                # Force fresh balance read for live short-collateral check.
                usdc_bal = await self.get_usdc_available_balance(force_refresh=True)
                if usdc_bal is None:
                    log.warning("SELL pre-check skipped: failed to fetch USDC balance")
                elif usdc_bal < short_collateral:
                    if usdc_bal > 0.01 and quote.price < 1.0:
                        max_short_affordable = usdc_bal / (1.0 - quote.price)
                        max_total_size = free_inventory + max_short_affordable
                        pm_min = self._pm_min_order_size()
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
            self._record_api_error(
                op="place_order",
                token_id=quote.token_id,
                status_code=self._extract_status_code(e),
                message=error_msg,
                details={
                    "side": quote.side,
                    "price": quote.price,
                    "size": quote.size,
                    "post_only": use_post_only,
                },
            )
            self._handle_fee_or_signature_reject(
                quote,
                reason=error_msg,
                source="single_place",
            )
            if self._is_balance_or_allowance_reject(error_msg):
                self._mark_reconcile_on_balance_reject(
                    quote,
                    reason=error_msg,
                    source="single_place",
                )
                # For SELL-side rejects, stop immediately and let reconcile repair state.
                if quote.side == "SELL":
                    return None
            if "not enough balance" in error_msg.lower():
                if quote.side == "BUY":
                    return await self._retry_buy_after_balance_reject(
                        quote,
                        use_post_only=use_post_only,
                    )
                return None
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
                        self._record_api_error(
                            op="place_order_taker_fallback",
                            token_id=quote.token_id,
                            status_code=self._extract_status_code(e2),
                            message=str(e2),
                            details={
                                "side": quote.side,
                                "price": quote.price,
                                "size": quote.size,
                                "post_only": False,
                            },
                        )
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

    async def place_orders_batch(self, quotes: list[Quote], *, post_only: bool | None = None) -> list[str | None]:
        """Place multiple orders in a single API call using post_orders.

        Polymarket supports up to 15 orders per batch.

        Args:
            quotes: List of Quote objects to place.
            post_only: Override post-only flag. None = use config default.

        Returns:
            List of order_ids (None for failed orders).
        """
        if not quotes:
            return []

        use_post_only = self.config.use_post_only if post_only is None else post_only
        is_mock = hasattr(self.client, "_orders")

        # Mock/paper client does not expose batch API; place individually.
        if is_mock:
            results: list[str | None] = []
            for q in quotes:
                oid = await self.place_order(q, post_only=use_post_only)
                results.append(oid)
            return results

        if not _HAS_CLOB_TYPES:
            raise ImportError("py_clob_client not installed for live trading")

        signed_orders: list[Any | None] = []
        order_type = OrderType.GTD if self.config.use_gtd else OrderType.GTC
        planned_buy_collateral = 0.0
        token_fee_rate_bps: dict[str, int] = {}

        for quote in quotes:
            if quote.side == "SELL" and self._should_skip_sell_after_cancel(quote, source="batch_precheck"):
                signed_orders.append(None)
                continue
            if quote.side == "SELL" and self._should_skip_sell_after_reject(quote, source="batch_precheck"):
                signed_orders.append(None)
                continue
            # Hard budget cap for BUY orders.
            collateral = self.required_collateral(quote)
            if quote.side == "BUY" and self._session_budget > 0:
                active_buy_collateral = sum(
                    self.required_collateral(q)
                    for q in self._active_orders.values()
                    if q.side == "BUY"
                )
                budget_remaining = (
                    self._session_budget
                    - self._session_spent
                    - active_buy_collateral
                    - planned_buy_collateral
                )
                if collateral > budget_remaining + 0.01:
                    self._throttled_warn(
                        "batch_budget_reject",
                        f"Batch BUY rejected: {quote.size:.1f}@{quote.price:.2f} needs "
                        f"${collateral:.2f}, remaining ${budget_remaining:.2f}",
                    )
                    signed_orders.append(None)
                    continue
                planned_buy_collateral += collateral

            # Ensure allowance for SELL orders.
            if quote.side == "SELL":
                if not await self.ensure_sell_allowance(
                    quote.token_id,
                    required_shares=quote.size,
                ):
                    self._throttled_warn(
                        "batch_sell_allowance",
                        f"Batch SELL skipped — allowance setup failed for {quote.token_id[:12]}...",
                    )
                    signed_orders.append(None)
                    continue
                if not await self._enforce_close_only_sell(quote):
                    signed_orders.append(None)
                    continue

            fee_rate_bps = token_fee_rate_bps.get(quote.token_id)
            if fee_rate_bps is None:
                resolved = await self._resolve_order_fee_rate_bps(quote.token_id)
                if resolved is None:
                    self._throttled_warn(
                        f"batch_fee_rate_skip:{quote.token_id}",
                        f"Batch order skipped: fee-rate unavailable for {quote.token_id[:12]}...",
                        cooldown=3.0,
                    )
                    signed_orders.append(None)
                    continue
                fee_rate_bps = resolved
                token_fee_rate_bps[quote.token_id] = fee_rate_bps
            oa = self._build_order_args(quote, fee_rate_bps=fee_rate_bps)
            if self.config.use_gtd:
                oa.expiration = int(time.time()) + self.config.gtd_duration_sec

            try:
                signed = await asyncio.to_thread(self.client.create_order, oa)
                signed_orders.append(signed)
            except Exception as e:
                self._record_api_error(
                    op="sign_order",
                    token_id=quote.token_id,
                    status_code=self._extract_status_code(e),
                    message=str(e),
                    details={
                        "side": quote.side,
                        "price": quote.price,
                        "size": quote.size,
                    },
                )
                log.warning("Failed to sign order %s: %s", quote.side, e)
                signed_orders.append(None)

        valid_indices = [i for i, s in enumerate(signed_orders) if s is not None]
        valid_signed = [signed_orders[i] for i in valid_indices]

        if not valid_signed:
            return [None] * len(quotes)

        results: list[str | None] = [None] * len(quotes)
        for batch_start in range(0, len(valid_signed), 15):
            batch = valid_signed[batch_start:batch_start + 15]
            batch_indices = valid_indices[batch_start:batch_start + 15]

            try:
                resp = await asyncio.wait_for(
                    self._post_orders_compat(batch, order_type, use_post_only),
                    timeout=15.0,
                )
                self._notify_heartbeat_id(resp)

                # post_orders may return list directly or wrapped payload.
                if isinstance(resp, list):
                    order_results = resp
                elif isinstance(resp, dict):
                    order_results = resp.get("orders", [resp])
                else:
                    order_results = [resp]

                for pos, idx in enumerate(batch_indices):
                    quote = quotes[idx]
                    order_resp = order_results[pos] if pos < len(order_results) else None
                    if not isinstance(order_resp, dict):
                        reason = self._extract_batch_reject_reason(order_resp)
                        self._record_api_error(
                            op="place_batch_item",
                            token_id=quote.token_id,
                            status_code=self._extract_status_code(order_resp),
                            message=reason,
                            details={
                                "side": quote.side,
                                "price": quote.price,
                                "size": quote.size,
                                "raw": self._compact_raw(order_resp),
                                "transient": False,
                            },
                        )
                        self._handle_fee_or_signature_reject(
                            quote,
                            reason=reason,
                            source="batch_place_payload",
                        )
                        self._mark_reconcile_on_balance_reject(
                            quote,
                            reason=reason,
                            source="batch_place",
                            raw=order_resp,
                        )
                        log.error(
                            "Batch reject %s %s %.1f@%.2f: reason=%s raw=%s",
                            quote.side,
                            quote.token_id[:8],
                            quote.size,
                            quote.price,
                            reason,
                            self._compact_raw(order_resp),
                        )
                        continue
                    order_id = (
                        order_resp.get("orderID")
                        or order_resp.get("order_id")
                        or order_resp.get("id")
                    )
                    if order_id:
                        quote.order_id = order_id
                        quote.placed_at = time.time()
                        self._active_orders[order_id] = quote
                        self._order_post_only[order_id] = use_post_only
                        if quote.side == "SELL":
                            self._sell_reject_cooldown_until.pop(quote.token_id, None)
                            self._cancel_repost_cooldown_until.pop(quote.token_id, None)
                            self._clear_recent_cancelled_sell_reserves_for_token(quote.token_id)
                            self._last_sellability_lag_reason = ""
                        results[idx] = order_id
                        log.info(
                            "Batch placed %s %s %.1f@%.2f id=%s...",
                            quote.side, quote.token_id[:8], quote.size, quote.price, order_id[:12],
                        )
                    else:
                        reason = self._extract_batch_reject_reason(order_resp)
                        self._record_api_error(
                            op="place_batch_item",
                            token_id=quote.token_id,
                            status_code=self._extract_status_code(order_resp),
                            message=reason,
                            details={
                                "side": quote.side,
                                "price": quote.price,
                                "size": quote.size,
                                "raw": self._compact_raw(order_resp),
                                "transient": False,
                            },
                        )
                        self._handle_fee_or_signature_reject(
                            quote,
                            reason=reason,
                            source="batch_place_reject",
                        )
                        self._mark_reconcile_on_balance_reject(
                            quote,
                            reason=reason,
                            source="batch_place",
                            raw=order_resp,
                        )
                        log.error(
                            "Batch reject %s %s %.1f@%.2f: reason=%s raw=%s",
                            quote.side,
                            quote.token_id[:8],
                            quote.size,
                            quote.price,
                            reason,
                            self._compact_raw(order_resp),
                        )
            except Exception as e:
                self._record_api_error(
                    op="place_batch",
                    status_code=self._extract_status_code(e),
                    message=str(e),
                    details={
                        "batch_size": len(batch_indices),
                        "post_only": use_post_only,
                        "transient": self._is_balance_or_allowance_reject(str(e)),
                    },
                )
                log.error("Batch post_orders failed: %s", e)
                if self._is_balance_or_allowance_reject(str(e)):
                    for idx in batch_indices:
                        self._mark_reconcile_on_balance_reject(
                            quotes[idx],
                            reason=str(e),
                            source="batch_post_orders_error",
                        )
                for idx in batch_indices:
                    self._handle_fee_or_signature_reject(
                        quotes[idx],
                        reason=str(e),
                        source="batch_post_orders_error",
                    )
                # Fallback to individual placement for failed batch.
                for idx in batch_indices:
                    quote = quotes[idx]
                    log.warning(
                        "Batch fallback to single order for %s %s %.1f@%.2f (cause=%s)",
                        quote.side,
                        quote.token_id[:8],
                        quote.size,
                        quote.price,
                        str(e),
                    )
                    try:
                        oid = await self.place_order(quote, post_only=use_post_only)
                        results[idx] = oid
                    except Exception:
                        pass

        return results

    async def cancel_orders_batch(self, order_ids: list[str]) -> int:
        """Cancel multiple orders in a single API call.

        Polymarket supports up to 3000 cancels per batch.

        Returns number of successfully cancelled orders.
        """
        if not order_ids:
            return 0

        is_mock = hasattr(self.client, "_orders")
        cancelled_ids: set[str] = set()

        if is_mock:
            for oid in order_ids:
                try:
                    self.client.cancel(oid)
                    cancelled_ids.add(oid)
                except Exception:
                    pass
            for oid in cancelled_ids:
                quote = self._active_orders.get(oid)
                self._add_recent_cancelled_sell_reserve(oid, quote)
                if quote is not None and (quote.side or "").upper() == "SELL":
                    self._set_cancel_repost_cooldown(quote.token_id)
                self._track_recent_order(oid, quote)
                self._active_orders.pop(oid, None)
                self._order_post_only.pop(oid, None)
            return len(cancelled_ids)

        for batch_start in range(0, len(order_ids), 3000):
            batch = order_ids[batch_start:batch_start + 3000]
            try:
                resp = await asyncio.wait_for(
                    asyncio.to_thread(self.client.cancel_orders, batch),
                    timeout=10.0,
                )
                self._notify_heartbeat_id(resp)
                # Assume full batch success if API didn't raise.
                cancelled_ids.update(batch)
            except Exception as e:
                self._record_api_error(
                    op="cancel_orders_batch",
                    status_code=self._extract_status_code(e),
                    message=str(e),
                    details={
                        "count": len(batch),
                        "order_ids": list(batch),
                        "transient": True,
                    },
                )
                log.warning("Batch cancel failed: %s, falling back to individual", e)
                for oid in batch:
                    try:
                        await asyncio.wait_for(
                            asyncio.to_thread(self.client.cancel, oid),
                            timeout=5.0,
                        )
                        cancelled_ids.add(oid)
                    except Exception as inner_exc:
                        self._record_api_error(
                            op="cancel_order",
                            order_id=oid,
                            status_code=self._extract_status_code(inner_exc),
                            message=str(inner_exc),
                            details={"transient": True, "fallback": "batch_cancel"},
                        )

        for oid in cancelled_ids:
            quote = self._active_orders.get(oid)
            self._add_recent_cancelled_sell_reserve(oid, quote)
            if quote is not None and (quote.side or "").upper() == "SELL":
                self._set_cancel_repost_cooldown(quote.token_id)
            self._track_recent_order(oid, quote)
            self._active_orders.pop(oid, None)
            self._order_post_only.pop(oid, None)
            self._pending_cancels.discard(oid)

        return len(cancelled_ids)

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
            self._record_api_error(
                op="get_book_summary",
                token_id=token_id,
                message=str(e),
                status_code=self._extract_status_code(e),
                details={"token_id": token_id[:12]},
                transient=True,
            )
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
            self._record_api_error(
                op="get_full_book",
                token_id=token_id,
                message=str(e),
                status_code=self._extract_status_code(e),
                details={"token_id": token_id[:12]},
                transient=True,
            )
            log.debug(f"Failed to get full book for {token_id[:12]}...: {e}")
            return empty

    async def get_token_balance(self, token_id: str) -> Optional[float]:
        """Fetch free/tradable PM token balance in shares for a conditional token.

        For live CONDITIONAL balances, PM may report only currently available
        inventory, excluding shares reserved by open SELL orders. This method is
        therefore suitable for pre-trade close-only checks, but not for
        reconcile/startup wallet-truth inventory snapshots.
        """
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
            self._record_api_error(
                op="get_token_balance",
                token_id=token_id,
                message=str(e),
                status_code=self._extract_status_code(e),
                details={"token_id": token_id[:12]},
                transient=True,
            )
            log.warning(f"Failed to fetch token balance for {token_id[:12]}...: {e}")
            return None

    def _remaining_active_sell_size(self, order_id: str, quote: Quote) -> float:
        """Remaining live SELL size after partial fills for a tracked order."""
        if (quote.side or "").upper() != "SELL":
            return 0.0
        matched = max(0.0, float(self._partial_fill_reported.get(order_id, 0.0)))
        total = max(0.0, float(quote.size))
        return max(0.0, total - matched)

    def _reserved_sell_inventory(self, token_id: str) -> float:
        """Shares reserved by active and just-cancelled close-only SELL orders.

        When short selling is disabled, PM's CONDITIONAL balance endpoint may
        return free inventory with open SELL sizes deducted. Reconcile/startup
        need wallet-total inventory, so we add back our still-open SELL size.
        """
        if bool(getattr(self.config, "allow_short_sells", False)):
            return 0.0

        return (
            self._active_sell_inventory(token_id)
            + self._recent_cancelled_sell_inventory(token_id)
        )

    async def get_reconcile_token_balance(
        self,
        token_id: str,
        *,
        reference_shares: float | None = None,
    ) -> Optional[float]:
        """Fetch wallet-total token inventory for reconcile/startup/PnL paths.

        PM's CONDITIONAL endpoint is inconsistent in live mode: sometimes it
        reports free balance (open SELL inventory already deducted), sometimes
        total wallet inventory. Reconcile therefore evaluates both candidates:
        the raw endpoint response, and raw + tracked SELL reserve
        (active + recently cancelled within release-grace window).

        When a reference inventory is available from the MM state, prefer the
        candidate closer to that expected total. This prevents both under-counts
        and double-counts during live reconcile.
        """
        free_balance = await self.get_token_balance(token_id)
        if free_balance is None:
            return None

        raw_balance = max(0.0, float(free_balance))
        reserved_balance = self._reserved_sell_inventory(token_id)
        adjusted_balance = raw_balance + reserved_balance
        if reference_shares is None:
            return adjusted_balance

        try:
            reference = max(0.0, float(reference_shares))
        except (TypeError, ValueError):
            return adjusted_balance

        raw_diff = abs(raw_balance - reference)
        adjusted_diff = abs(adjusted_balance - reference)
        if raw_diff <= adjusted_diff:
            return raw_balance
        return adjusted_balance

    async def get_usdc_balances(
        self,
        *,
        force_refresh: bool = False,
    ) -> tuple[Optional[float], Optional[float]]:
        """Fetch real USDC balances on Polymarket: (total, available)."""
        now = time.time()
        if (
            not force_refresh
            and
            self._usdc_balance_cache is not None
            and self._usdc_available_cache is not None
            and (now - self._usdc_balance_cache_ts) < self._usdc_cache_ttl
        ):
            return self._usdc_balance_cache, self._usdc_available_cache

        try:
            if hasattr(self.client, "_orders"):
                total = None
                get_balance = getattr(self.client, "get_balance", None)
                if callable(get_balance):
                    try:
                        total = float(get_balance())
                    except Exception:
                        total = None
                available = float(getattr(self.client, "_usdc_balance", 0.0))
                if total is None:
                    total = available
                self._usdc_balance_cache = total
                self._usdc_available_cache = available
                self._usdc_balance_cache_ts = now
                return total, available
            if not _HAS_CLOB_TYPES:
                self._usdc_balance_cache = 0.0
                self._usdc_available_cache = 0.0
                self._usdc_balance_cache_ts = now
                return 0.0, 0.0
            result = await asyncio.to_thread(
                self.client.get_balance_allowance,
                BalanceAllowanceParams(asset_type=AssetType.COLLATERAL),
            )
            total, available = self._extract_usdc_balances(result)
            if total is None:
                total = 0.0
            if available is None:
                available = total
            self._usdc_balance_cache = total
            self._usdc_available_cache = available
            self._usdc_balance_cache_ts = now
            return total, available
        except Exception as e:
            self._record_api_error(
                op="get_usdc_balances",
                message=str(e),
                status_code=self._extract_status_code(e),
                transient=True,
            )
            log.warning(f"Failed to fetch USDC balance: {e}")
            return None, None

    async def get_usdc_balance(self, *, force_refresh: bool = False) -> Optional[float]:
        """Fetch total USDC collateral balance on Polymarket."""
        total, _available = await self.get_usdc_balances(force_refresh=force_refresh)
        return total

    async def get_usdc_available_balance(self, *, force_refresh: bool = False) -> Optional[float]:
        """Fetch available/free USDC balance on Polymarket."""
        _total, available = await self.get_usdc_balances(force_refresh=force_refresh)
        return available

    def invalidate_usdc_cache(self) -> None:
        """Force next USDC balance read to refresh from source."""
        self._usdc_balance_cache = None
        self._usdc_available_cache = None
        self._usdc_balance_cache_ts = 0.0

    async def get_all_token_balances(
        self,
        up_token_id: str,
        dn_token_id: str,
        *,
        reference_balances: dict[str, float] | None = None,
    ) -> tuple[Optional[float], Optional[float]]:
        """Fetch both UP and DN token balances.

        Live mode: fetches wallet-total inventory for reconcile/startup paths.
        PM CONDITIONAL balances can exclude shares reserved by our open SELL
        orders, or sometimes already include them. `reference_balances` lets us
        choose the candidate that best matches the MM's expected wallet-total
        inventory for each token.
        Mock mode: returns balances tracked from observed mock fills.
        """
        if hasattr(self.client, "_orders"):
            up = float(self._mock_token_balances.get(up_token_id, 0.0))
            dn = float(self._mock_token_balances.get(dn_token_id, 0.0))
            return up, dn

        up_reference = None
        dn_reference = None
        if reference_balances:
            up_reference = reference_balances.get(up_token_id)
            dn_reference = reference_balances.get(dn_token_id)

        up, dn = await asyncio.gather(
            self.get_reconcile_token_balance(up_token_id, reference_shares=up_reference),
            self.get_reconcile_token_balance(dn_token_id, reference_shares=dn_reference),
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
            quote = self._active_orders.get(order_id)
            if quote is not None:
                self._add_recent_cancelled_sell_reserve(order_id, quote)
                if (quote.side or "").upper() == "SELL":
                    self._set_cancel_repost_cooldown(quote.token_id)
                self._track_recent_order(order_id, quote, reason="cancelled")
            self._active_orders.pop(order_id, None)
            self._order_post_only.pop(order_id, None)
            self.invalidate_usdc_cache()
            self._notify_heartbeat_id(resp)
            log.info(f"Cancelled order {order_id[:12]}...")
            return True
        except Exception as e:
            self._record_api_error(
                op="cancel_order",
                order_id=order_id,
                message=str(e),
                status_code=self._extract_status_code(e),
                details={"order_id": order_id[:12]},
            )
            log.warning(f"Cancel failed for {order_id[:12]}...: {e}")
            return False

    def clear_local_order_tracking(
        self,
        *,
        clear_recent: bool = False,
        clear_ws_queue: bool = False,
    ) -> None:
        """Drop in-memory order-tracking state without touching balances."""
        self._active_orders.clear()
        self._order_post_only.clear()
        self._filled_order_ids.clear()
        self._partial_fill_reported.clear()
        self._pending_cancels.clear()
        self._recent_cancelled_sell_reserves.clear()
        self._last_sellability_lag_reason = ""
        self._reconcile_requested = False
        self._sell_reject_cooldown_until.clear()
        self._cancel_repost_cooldown_until.clear()
        self._last_fallback_poll_count = 0
        if clear_recent:
            self._recent_orders.clear()
        if clear_ws_queue:
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
        existing = dict(self._active_orders)
        try:
            # Try batch cancel first
            batch_resp = await asyncio.to_thread(self.client.cancel_all)
            if isinstance(batch_resp, dict) and batch_resp.get("error"):
                raise RuntimeError(f"batch cancel response error: {batch_resp.get('error')}")
            self._notify_heartbeat_id(batch_resp)
            cancelled = len(ids)
            for oid, quote in existing.items():
                self._add_recent_cancelled_sell_reserve(oid, quote)
                if quote is not None and (quote.side or "").upper() == "SELL":
                    self._set_cancel_repost_cooldown(quote.token_id)
                self._track_recent_order(oid, quote, reason="batch_cancel")
            for oid in ids:
                self._active_orders.pop(oid, None)
                self._order_post_only.pop(oid, None)
                self._pending_cancels.discard(oid)
            self.invalidate_usdc_cache()
            if cancelled > 0:
                log.info(f"Batch cancelled {cancelled} orders")
            elif force_exchange:
                log.info("Forced batch cancel submitted with no locally tracked orders")
        except Exception as e:
            if not ids:
                self._record_api_error(
                    op="cancel_all",
                    message=str(e),
                    status_code=self._extract_status_code(e),
                    details={"force_exchange": force_exchange, "tracked_orders": len(ids)},
                )
                log.error("Forced batch cancel failed with no local order IDs: %s", e)
                raise
            self._record_api_error(
                op="cancel_all",
                message=str(e),
                status_code=self._extract_status_code(e),
                details={"force_exchange": force_exchange, "tracked_orders": len(ids)},
            )
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
            runtime_metrics.incr("orders.fill_ws.loop")
            try:
                async with websockets.connect(
                    url,
                    ping_interval=10,
                    max_queue=256,
                ) as ws:
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
                    msg_since_yield = 0

                    async for raw in ws:
                        runtime_metrics.incr("orders.fill_ws.msg_recv")
                        msg_since_yield += 1
                        if msg_since_yield >= 200:
                            msg_since_yield = 0
                            await asyncio.sleep(0)
                        try:
                            msg = json.loads(raw)
                            event_type = msg.get("event_type", "")
                            if event_type == "trade":
                                # PM WS uses maker_order_id / taker_order_id (not order_id)
                                oid = (msg.get("maker_order_id") or msg.get("taker_order_id")
                                       or msg.get("order_id") or "")
                                msg["_resolved_order_id"] = oid
                                is_ours = bool(oid and self._get_tracked_quote(oid) is not None)
                                if is_ours:
                                    runtime_metrics.incr("orders.fill_ws.ours_fill")
                                    await self._ws_fills_queue.put(msg)
                                    log.info(
                                        "Fill WS: OUR fill — order=%s size=%s price=%s",
                                        str(oid)[:12],
                                        msg.get("size", "?"),
                                        msg.get("price", "?"),
                                    )
                                    if self._on_fill_callback:
                                        try:
                                            self._on_fill_callback()
                                        except Exception:
                                            pass
                            elif event_type == "order":
                                oid = msg.get("order_id", "")
                                status = msg.get("status", "")
                                if oid and self._get_tracked_quote(oid) is not None:
                                    runtime_metrics.incr("orders.fill_ws.order_event")
                                    await self._ws_fills_queue.put(msg)
                                    if status in ("MATCHED", "CLOSED", "CANCELLED", "EXPIRED"):
                                        log.info("Fill WS: order status — id=%s status=%s",
                                                 str(oid)[:12], status)
                        except json.JSONDecodeError:
                            runtime_metrics.incr("orders.fill_ws.decode_error")
                            continue
            except Exception as e:
                runtime_metrics.incr("orders.fill_ws.error")
                if self._fill_ws_running:
                    log.warning("Fill WS disconnected: %s — reconnecting in 5s", e)
                    if self._on_ws_reconnect:
                        try:
                            self._on_ws_reconnect()
                        except Exception:
                            pass
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
        """Detect fills via WS events (primary) with HTTP fallback for stale orders.

        WS trade events provide real-time fill data — no HTTP needed.
        HTTP polling only runs every 30s for orders with no WS activity.
        """
        now = time.time()
        is_mock_client = hasattr(self.client, "_orders")
        self._prune_recent_orders(now)
        fills = []
        to_remove = []
        ws_processed_oids: set[str] = set()

        # ── 1. Process WS events directly (primary — zero HTTP) ──────
        while not self._ws_fills_queue.empty():
            try:
                ws_msg = self._ws_fills_queue.get_nowait()
            except asyncio.QueueEmpty:
                break

            event_type = ws_msg.get("event_type", "")

            if event_type == "trade":
                oid = ws_msg.get("_resolved_order_id", "")
                quote = self._get_tracked_quote(oid)
                if not oid or quote is None:
                    continue
                ws_processed_oids.add(oid)

                fill_size = self._safe_float(ws_msg.get("size", 0))
                fill_price = self._safe_float(ws_msg.get("price", 0))
                if not fill_size or fill_size <= 0:
                    continue
                if not fill_price or fill_price <= 0:
                    fill_price = quote.price

                prev_matched = self._partial_fill_reported.get(oid, 0.0)
                self._partial_fill_reported[oid] = prev_matched + fill_size

                is_maker = self._order_post_only.get(oid, True)
                fee = (
                    0.0
                    if is_maker
                    else taker_fee_usd(fill_price, fill_size, quote.side, token_id=quote.token_id)
                )
                fill = Fill(
                    ts=now, side=quote.side, token_id=quote.token_id,
                    price=fill_price, size=round(fill_size, 4),
                    fee=fee, order_id=oid, is_maker=is_maker,
                )
                fills.append(fill)
                self.invalidate_usdc_cache()
                trade_id = self._build_ledger_trade_id(
                    "ws_trade", ws_msg, oid, fill_size, fill_price, prev_matched
                )
                self.trade_ledger.record({
                    "id": trade_id,
                    "order_id": fill.order_id,
                    "ts": fill.ts,
                    "side": fill.side,
                    "token_id": fill.token_id,
                    "price": fill.price,
                    "size": fill.size,
                    "fee": fill.fee,
                    "is_maker": fill.is_maker,
                    "source": "ws",
                })

                if quote.side == "BUY":
                    self._session_spent += fill_size * fill_price
                elif quote.side == "SELL":
                    self._session_spent = max(0.0, self._session_spent - fill_size * fill_price)
                    self._reduce_recent_cancelled_sell_reserves(quote.token_id, fill_size)

                if hasattr(self.client, "_orders") and not getattr(self.client, "_manages_mock_balances", False):
                    if quote.side == "BUY":
                        actual_size = (
                            net_shares_after_buy_fee(fill_size, fill_price, token_id=quote.token_id)
                            if not is_maker
                            else fill_size
                        )
                        cur = self._mock_token_balances.get(quote.token_id, 0.0)
                        self._mock_token_balances[quote.token_id] = cur + actual_size
                    else:
                        cur = self._mock_token_balances.get(quote.token_id, 0.0)
                        self._mock_token_balances[quote.token_id] = cur - fill_size

                if (prev_matched + fill_size) >= quote.size - 0.01:
                    self._filled_order_ids.add(oid)
                    to_remove.append(oid)
                    self._partial_fill_reported.pop(oid, None)

            elif event_type == "order":
                oid = ws_msg.get("order_id", "")
                quote = self._get_tracked_quote(oid)
                if not oid or quote is None:
                    continue
                ws_processed_oids.add(oid)
                status = ws_msg.get("status", "")

                if status in ("MATCHED", "CLOSED"):
                    # Catch any missed fill volume via size_matched
                    size_matched = self._safe_float(ws_msg.get("size_matched", 0))
                    if size_matched and size_matched > 0:
                        prev = self._partial_fill_reported.get(oid, 0.0)
                        missed = size_matched - prev
                        if missed >= 0.01:
                            fill_price = self._extract_fill_price(ws_msg, quote.price)
                            is_maker = self._order_post_only.get(oid, True)
                            fee = (
                                0.0
                                if is_maker
                                else taker_fee_usd(fill_price, missed, quote.side, token_id=quote.token_id)
                            )
                            fill = Fill(
                                ts=now, side=quote.side, token_id=quote.token_id,
                                price=fill_price, size=round(missed, 4),
                                fee=fee, order_id=oid, is_maker=is_maker,
                            )
                            fills.append(fill)
                            self.invalidate_usdc_cache()
                            trade_id = self._build_ledger_trade_id(
                                "ws_order", ws_msg, oid, missed, fill_price, prev
                            )
                            self.trade_ledger.record({
                                "id": trade_id,
                                "order_id": fill.order_id,
                                "ts": fill.ts,
                                "side": fill.side,
                                "token_id": fill.token_id,
                                "price": fill.price,
                                "size": fill.size,
                                "fee": fill.fee,
                                "is_maker": fill.is_maker,
                                "source": "ws",
                            })
                            if quote.side == "BUY":
                                self._session_spent += missed * fill_price
                            elif quote.side == "SELL":
                                self._session_spent = max(0.0, self._session_spent - missed * fill_price)
                                self._reduce_recent_cancelled_sell_reserves(quote.token_id, missed)
                    self._filled_order_ids.add(oid)
                    to_remove.append(oid)
                    self._partial_fill_reported.pop(oid, None)
                elif status in ("CANCELLED", "EXPIRED"):
                    self._add_recent_cancelled_sell_reserve(oid, quote)
                    to_remove.append(oid)
                    self._partial_fill_reported.pop(oid, None)

        if ws_processed_oids:
            log.info("WS fills processed: %d orders", len(ws_processed_oids))

        # ── 2. HTTP fallback:
        # active orders (stale >30s) + recently cancelled orders (short-interval polling)
        stale_cutoff = 30.0
        removed_set = set(to_remove)
        poll_map: dict[str, Quote] = {}

        if is_mock_client and self._active_orders:
            for oid, quote in self._active_orders.items():
                if oid in ws_processed_oids or oid in removed_set:
                    continue
                poll_map[oid] = quote
        elif (now - self._last_fill_check_ts) >= stale_cutoff and self._active_orders:
            self._last_fill_check_ts = now
            for oid, quote in self._active_orders.items():
                if (
                    oid in ws_processed_oids
                    or oid in removed_set
                    or (now - (quote.placed_at or now)) <= stale_cutoff
                ):
                    continue
                poll_map[oid] = quote

        recent_due = (
            bool(self._recent_orders)
            and (now - self._last_recent_poll_ts) >= max(1.0, float(self._recent_poll_interval_sec))
        )
        if recent_due:
            self._last_recent_poll_ts = now
            recent_candidates: list[tuple[str, RecentOrderState]] = []
            for oid, state in self._recent_orders.items():
                if oid in ws_processed_oids or oid in removed_set:
                    continue
                if state.generation != self._tracking_generation:
                    continue
                if self._current_token_ids and state.token_id not in self._current_token_ids:
                    continue
                if (now - state.last_polled_ts) < max(1.0, float(self._recent_poll_interval_sec)):
                    continue
                recent_candidates.append((oid, state))
            recent_candidates.sort(key=lambda entry: entry[1].removed_ts, reverse=True)
            for oid, state in recent_candidates:
                if len(poll_map) >= self._fallback_poll_cap:
                    break
                poll_map.setdefault(oid, state.quote)
                state.last_polled_ts = now

        if poll_map:
            poll_items = list(poll_map.items())[:self._fallback_poll_cap]
            self._last_fallback_poll_count = len(poll_items)
            if is_mock_client:
                log.info("Paper fill poll: checking %d tracked orders", len(poll_items))
            else:
                log.info("HTTP fallback: polling %d tracked orders", len(poll_items))
            fetch_results = await asyncio.gather(
                *(
                    asyncio.wait_for(
                        asyncio.to_thread(self.client.get_order, oid),
                        timeout=10.0,
                    )
                    for oid, _ in poll_items
                ),
                return_exceptions=True,
            )
            for (oid, quote), result in zip(poll_items, fetch_results):
                if isinstance(result, Exception):
                    self._record_api_error(
                        op="fallback_poll",
                        token_id=quote.token_id,
                        order_id=oid,
                        status_code=self._extract_status_code(result),
                        message=str(result),
                        details={"transient": True},
                    )
                    continue
                if result is None:
                    self._record_api_error(
                        op="fallback_poll",
                        token_id=quote.token_id,
                        order_id=oid,
                        message="empty order status response",
                        details={"transient": True},
                    )
                    continue
                status = result.get("status", "")
                size_matched = self._safe_float(result.get("size_matched", 0)) or 0.0
                prev = self._partial_fill_reported.get(oid, 0.0)
                new_fill = size_matched - prev

                if new_fill >= 0.01:
                    fill_price = self._extract_fill_price(result, quote.price)
                    is_maker = self._order_post_only.get(oid, True)
                    fee = (
                        0.0
                        if is_maker
                        else taker_fee_usd(fill_price, new_fill, quote.side, token_id=quote.token_id)
                    )
                    fill = Fill(
                        ts=now, side=quote.side, token_id=quote.token_id,
                        price=fill_price, size=round(new_fill, 4),
                        fee=fee, order_id=oid, is_maker=is_maker,
                    )
                    if quote.side == "SELL":
                        inventory_backed_total = float(result.get("inventory_backed_size", 0.0) or 0.0)
                        inv_prev = min(inventory_backed_total, prev)
                        inv_new = min(inventory_backed_total, size_matched)
                        fill.inventory_backed_size = round(max(0.0, inv_new - inv_prev), 4)
                        fill.short_backed_size = round(max(0.0, new_fill - fill.inventory_backed_size), 4)
                    fills.append(fill)
                    self.invalidate_usdc_cache()
                    trade_id = self._build_ledger_trade_id(
                        "http_poll", result, oid, new_fill, fill_price, prev
                    )
                    self.trade_ledger.record({
                        "id": trade_id,
                        "order_id": fill.order_id,
                        "ts": fill.ts,
                        "side": fill.side,
                        "token_id": fill.token_id,
                        "price": fill.price,
                        "size": fill.size,
                        "fee": fill.fee,
                        "is_maker": fill.is_maker,
                        "source": "http",
                    })
                    self._partial_fill_reported[oid] = size_matched
                    if quote.side == "BUY":
                        self._session_spent += new_fill * fill_price
                    elif quote.side == "SELL":
                        self._session_spent = max(0.0, self._session_spent - new_fill * fill_price)
                        self._reduce_recent_cancelled_sell_reserves(quote.token_id, new_fill)
                    if hasattr(self.client, "_orders") and not getattr(self.client, "_manages_mock_balances", False):
                        if quote.side == "BUY":
                            actual = (
                                net_shares_after_buy_fee(new_fill, fill_price, token_id=quote.token_id)
                                if not is_maker
                                else new_fill
                            )
                            cur = self._mock_token_balances.get(quote.token_id, 0.0)
                            self._mock_token_balances[quote.token_id] = cur + actual
                        else:
                            cur = self._mock_token_balances.get(quote.token_id, 0.0)
                            self._mock_token_balances[quote.token_id] = cur - new_fill

                if status in ("MATCHED", "CLOSED"):
                    self._filled_order_ids.add(oid)
                    to_remove.append(oid)
                    self._recent_orders.pop(oid, None)
                    self._partial_fill_reported.pop(oid, None)
                elif status in ("CANCELLED", "EXPIRED"):
                    self._add_recent_cancelled_sell_reserve(oid, quote)
                    to_remove.append(oid)
                    self._partial_fill_reported.pop(oid, None)
        else:
            self._last_fallback_poll_count = 0

        for oid in set(to_remove):
            self._active_orders.pop(oid, None)
            self._order_post_only.pop(oid, None)
            self._recent_orders.pop(oid, None)

        self._prune_recent_orders(now)

        return fills

    async def backfill_trades(self, market_id: str = "", token_id: str = "") -> int:
        """Backfill trade history from getTrades API.

        Uses getTradesPaginated if available, falls back to getTrades.
        Returns number of new trades added.
        """
        if hasattr(self.client, "_orders"):  # Mock/paper mode
            return 0

        new_count = 0
        try:
            # Try paginated endpoint first
            cursor = ""
            for page in range(10):  # Max 10 pages
                params = {}
                if market_id:
                    params["market"] = market_id
                if cursor:
                    params["cursor"] = cursor

                paginated_getter = (
                    getattr(self.client, "get_trades_paginated", None)
                    or getattr(self.client, "getTradesPaginated", None)
                )
                trades_getter = (
                    getattr(self.client, "get_trades", None)
                    or getattr(self.client, "getTrades", None)
                )
                getter = paginated_getter or trades_getter
                if not getter:
                    break

                result = await asyncio.to_thread(
                    getter,
                    **params
                )

                trades = result if isinstance(result, list) else result.get("data", [])
                next_cursor = result.get("next_cursor", "") if isinstance(result, dict) else ""

                for trade in trades:
                    trade_doc = trade if isinstance(trade, dict) else {"raw": trade}
                    if isinstance(trade_doc, dict):
                        order_id = self._safe_str(
                            trade_doc.get("order_id")
                            or trade_doc.get("maker_order_id")
                            or trade_doc.get("taker_order_id")
                        )
                        has_trade_id = bool(self._extract_trade_id(trade_doc, order_id))
                        if not has_trade_id:
                            price = self._safe_float(trade_doc.get("price")) or 0.0
                            size = self._safe_float(
                                trade_doc.get("size")
                                or trade_doc.get("amount")
                                or trade_doc.get("matched_size")
                            ) or 0.0
                            synthetic = self._build_ledger_trade_id(
                                "api_backfill",
                                trade_doc,
                                order_id,
                                size,
                                price,
                            )
                            trade_doc = dict(trade_doc)
                            trade_doc["id"] = synthetic
                    if self.trade_ledger.record(trade_doc):
                        new_count += 1

                if not next_cursor or not trades or not paginated_getter:
                    break
                cursor = next_cursor
        except Exception as e:
            self._record_api_error(
                op="backfill_trades",
                status_code=self._extract_status_code(e),
                message=str(e),
                details={
                    "market_id": market_id,
                    "token_id": token_id,
                    "transient": True,
                },
            )
            log.warning("Trade backfill failed: %s", e)

        if new_count > 0:
            log.info("Backfilled %d trades from API", new_count)
        return new_count

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

    async def redeem_positions(self, condition_id: str, private_key: str) -> dict:
        """Redeem resolved winning tokens back to USDC."""
        if hasattr(self.client, "_orders"):  # Paper mode
            return {"success": False, "error": "paper mode"}
        if not condition_id:
            return {"success": False, "error": "missing condition_id"}
        if not private_key:
            return {"success": False, "error": "missing private key"}

        funder = getattr(self.client, 'funder', None)
        signer_addr = None
        if hasattr(self.client, 'signer'):
            signer_addr = getattr(self.client.signer, 'address', lambda: None)()
        if funder and signer_addr and funder.lower() != signer_addr.lower():
            log.warning(
                "Redeem skipped: tokens on funder (%s), not EOA (%s)",
                funder[:10], signer_addr[:10],
            )
            return {"success": False, "error": "funder mode — redeem not supported"}

        from .approvals import redeem_positions as _redeem
        return await asyncio.to_thread(_redeem, private_key, condition_id)

    def reset(self) -> None:
        """Clear tracked order state between windows."""
        self.clear_local_order_tracking(clear_recent=True, clear_ws_queue=True)
        self._mock_token_balances.clear()
        self._allowance_set.clear()
        self._allowance_cap_shares.clear()
        self.invalidate_usdc_cache()
        self._last_fill_check_ts = 0.0
        self._last_recent_poll_ts = 0.0
        self._session_spent = 0.0
        self._last_fallback_poll_count = 0
        self._warn_cooldowns = {}
        self._recent_api_errors.clear()
        self._api_error_counts = {}
        self._transport_error_counts = {}
        self._last_api_error_ts = 0.0

    def get_stats(self) -> dict:
        """Get order manager stats."""
        return {
            "active_orders": len(self._active_orders),
            "active_bids": sum(1 for q in self._active_orders.values() if q.side == "BUY"),
            "active_asks": sum(1 for q in self._active_orders.values() if q.side == "SELL"),
            "recent_orders": len(self._recent_orders),
            "tracking_generation": self._tracking_generation,
            "fallback_poll_count": self._last_fallback_poll_count,
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
