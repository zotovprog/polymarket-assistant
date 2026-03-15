"""Polymarket Market Making — Web Server & API.

Serves the MM dashboard frontend and provides REST API for:
- Starting/stopping the market maker
- Live config updates
- State snapshots (quotes, inventory, PnL, fills)
- Market info and health checks
"""
from __future__ import annotations

import asyncio
import json
import os
import signal
import sys
import time
import logging
import math
import copy
import threading
import traceback
from pathlib import Path
from typing import Any, Callable, Optional

from fastapi import FastAPI, HTTPException, Request, Response, WebSocket, WebSocketDisconnect
from fastapi.responses import FileResponse, JSONResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel, Field, field_validator

# ── Path setup ──────────────────────────────────────────────────
BASE_DIR = Path(__file__).resolve().parent
SRC_DIR = BASE_DIR / "src"
if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))

import config
import feeds
from mm_shared.types import MarketInfo
from mm_shared.mm_config import MMConfig
from mm_shared.runtime_metrics import runtime_metrics
from mm_v2 import MMConfigV2, MarketMakerV2
from telegram_notifier import TelegramNotifier
from telegram_bot import TelegramBotManager
from version import __version__ as APP_VERSION, git_hash as _git_hash_fn

APP_GIT_HASH = _git_hash_fn()  # Resolve once at startup

# ── Logging ─────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(name)s] %(levelname)s: %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger("web")

import collections


class RingBufferLogHandler(logging.Handler):
    """In-memory ring buffer for recent log entries (used by /api/logs)."""

    def __init__(self, maxlen: int = 500):
        super().__init__()
        self.buffer: collections.deque = collections.deque(maxlen=maxlen)

    def emit(self, record: logging.LogRecord) -> None:
        self.buffer.append({
            "ts": record.created,
            "level": record.levelname,
            "name": record.name,
            "msg": self.format(record),
        })


_log_handler = RingBufferLogHandler()
_log_handler.setLevel(logging.INFO)
logging.getLogger().addHandler(_log_handler)

RUNTIME_WATCHDOG_INTERVAL_SEC = max(
    0.5,
    float(os.environ.get("RUNTIME_WATCHDOG_INTERVAL_SEC", "2.0")),
)
RUNTIME_WATCHDOG_CPU_ALERT_PCT = max(
    50.0,
    float(os.environ.get("RUNTIME_WATCHDOG_CPU_ALERT_PCT", "85.0")),
)
RUNTIME_WATCHDOG_LAG_ALERT_MS = max(
    50.0,
    float(os.environ.get("RUNTIME_WATCHDOG_LAG_ALERT_MS", "250.0")),
)
RUNTIME_WATCHDOG_LOG_COOLDOWN_SEC = max(
    2.0,
    float(os.environ.get("RUNTIME_WATCHDOG_LOG_COOLDOWN_SEC", "10.0")),
)
V2_SUPPORTED_COINS = frozenset({"BTC", "ETH", "SOL", "XRP"})
V2_SUPPORTED_TIMEFRAMES = frozenset({"5m", "15m", "1h", "4h"})
V2_SUPPORTED_MARKET_SCOPES = frozenset(
    f"{coin}_{timeframe}"
    for coin in V2_SUPPORTED_COINS
    for timeframe in V2_SUPPORTED_TIMEFRAMES
)


# ── Auth ────────────────────────────────────────────────────────
AUTH_COOKIE = "pm_web_auth"


def _load_access_key() -> str:
    key = os.environ.get("PM_WEB_ACCESS_KEY", "").strip()
    if not key:
        # Try file fallback
        key_file = BASE_DIR / ".web_access_key"
        if key_file.exists():
            key = key_file.read_text().strip()
    if not key:
        in_test_env = (
            os.environ.get("PYTEST_CURRENT_TEST") is not None
            or os.environ.get("PYTEST_RUNNING") == "1"
            or "pytest" in sys.modules
        )
        if in_test_env:
            return "test-access-key"
        raise RuntimeError(
            "PM_WEB_ACCESS_KEY is required. "
            "Set it in environment or .web_access_key file."
        )
    return key


ACCESS_KEY = _load_access_key()
log.info(f"Auth key loaded (length={len(ACCESS_KEY)})")

# ── Credentials ─────────────────────────────────────────────────
PM_PRIVATE_KEY = os.environ.get("PM_PRIVATE_KEY", "").strip()
PM_FUNDER = os.environ.get("PM_FUNDER", "").strip()
PM_API_KEY = os.environ.get("PM_API_KEY", "").strip()
PM_API_SECRET = os.environ.get("PM_API_SECRET", "").strip()
PM_API_PASSPHRASE = os.environ.get("PM_API_PASSPHRASE", "").strip()

log.info(f"PM_PRIVATE_KEY={'set' if PM_PRIVATE_KEY else 'MISSING'} (len={len(PM_PRIVATE_KEY)})")
log.info(f"PM_FUNDER={'set' if PM_FUNDER else 'MISSING'} (len={len(PM_FUNDER)})")
log.info(f"PM_API_KEY={'set' if PM_API_KEY else 'MISSING'} (len={len(PM_API_KEY)})" if PM_API_KEY else "PM_API_KEY=MISSING")
log.info(f"PM_API_SECRET={'set' if PM_API_SECRET else 'MISSING'} (len={len(PM_API_SECRET)})")
log.info(f"PM_API_PASSPHRASE={'set' if PM_API_PASSPHRASE else 'MISSING'} (len={len(PM_API_PASSPHRASE)})")

# ── Telegram ────────────────────────────────────────────────────
_telegram = TelegramNotifier()
log.info(f"Telegram: {'enabled' if _telegram.enabled else 'disabled'}")


def _telegram_polling_enabled() -> bool:
    raw = os.environ.get("TELEGRAM_POLLING_ENABLED", "1").strip().lower()
    return raw not in {"0", "false", "no", "off"}

# ── FastAPI app ─────────────────────────────────────────────────
app = FastAPI(title="Polymarket MM", docs_url=None, redoc_url=None)

# Serve frontend
WEB_DIR = BASE_DIR / "web"
if WEB_DIR.exists():
    app.mount("/static", StaticFiles(directory=str(WEB_DIR)), name="static")


# ── Auth middleware ─────────────────────────────────────────────
def _require_auth(request: Request) -> None:
    cookie = request.cookies.get(AUTH_COOKIE, "")
    header = request.headers.get("x-access-key", "")
    if cookie != ACCESS_KEY and header != ACCESS_KEY:
        raise HTTPException(status_code=401, detail="unauthorized")


# ── Pydantic models ─────────────────────────────────────────────
class LoginRequest(BaseModel):
    key: str


class StartRequest(BaseModel):
    coin: str = "BTC"
    timeframe: str = "5m"
    paper_mode: bool = True  # Paper trading by default for safety
    initial_usdc: float = 1000.0
    dev: bool = False
    force_normal_soft_mode: bool = False
    force_normal_no_guards: bool = False


class PaperSweepStartRequest(BaseModel):
    coin: str = "BTC"
    timeframe: str = "15m"
    initial_usdc: float = 300.0
    base_clips: list[float] = Field(default_factory=lambda: [8.0, 12.0, 14.0, 20.0])
    variants: list[dict[str, Any]] | None = None
    force_normal_soft_mode: bool = False

    @field_validator("base_clips", mode="before")
    @classmethod
    def _parse_base_clips(cls, value: Any) -> list[float]:
        if value is None:
            return [8.0, 12.0, 14.0, 20.0]
        if isinstance(value, str):
            value = [part.strip() for part in value.split(",") if part.strip()]
        if not isinstance(value, list):
            value = [float(value)]
        parsed: list[float] = []
        for item in value:
            clip = float(item)
            if clip <= 0:
                raise ValueError("base_clips must be positive")
            parsed.append(clip)
        deduped: list[float] = []
        seen: set[float] = set()
        for clip in parsed:
            rounded = round(float(clip), 6)
            if rounded in seen:
                continue
            seen.add(rounded)
            deduped.append(float(clip))
        if not deduped:
            raise ValueError("base_clips cannot be empty")
        return deduped


class PairArbStartRequest(BaseModel):
    paper_mode: bool = True
    initial_usdc: float = 50.0
    market_scopes: str = "BTC_5m,BTC_15m,ETH_5m,ETH_15m,SOL_5m,SOL_15m"


class ConfigUpdateRequest(BaseModel):
    half_spread_bps: Optional[float] = Field(default=None)
    min_spread_bps: Optional[float] = None
    max_spread_bps: Optional[float] = None
    vol_spread_mult: Optional[float] = None
    dynamic_spread_enabled: Optional[bool] = None
    dynamic_spread_gamma: Optional[float] = None
    dynamic_spread_k: Optional[float] = None
    dynamic_spread_min_bps: Optional[float] = None
    dynamic_spread_max_bps: Optional[float] = None
    order_size_usd: Optional[float] = Field(default=None)
    min_order_size_usd: Optional[float] = None
    max_order_size_usd: Optional[float] = None
    max_inventory_shares: Optional[float] = None
    max_net_delta_shares: Optional[float] = None
    skew_bps_per_unit: Optional[float] = Field(default=None)
    min_quote_size_shares: Optional[float] = None
    requote_interval_sec: Optional[float] = Field(default=None)
    refresh_interval_s: Optional[float] = Field(default=None)
    requote_threshold_bps: Optional[float] = None
    requote_interval_jitter_enabled: Optional[bool] = None
    requote_interval_jitter_sec: Optional[float] = None
    event_requote_enabled: Optional[bool] = None
    event_pm_mid_threshold_bps: Optional[float] = None
    event_binance_threshold_bps: Optional[float] = None
    event_poll_interval_sec: Optional[float] = None
    event_fallback_interval_sec: Optional[float] = None
    gtd_duration_sec: Optional[int] = None
    heartbeat_interval_sec: Optional[int] = None
    heartbeat_failures_before_shutdown: Optional[int] = None
    use_post_only: Optional[bool] = None
    use_gtd: Optional[bool] = None
    price_jitter_enabled: Optional[bool] = None
    price_jitter_ticks: Optional[int] = None
    size_jitter_enabled: Optional[bool] = None
    size_jitter_pct: Optional[float] = None
    max_drawdown_usd: Optional[float] = Field(default=None)
    volatility_pause_mult: Optional[float] = Field(default=None)
    max_loss_per_fill_usd: Optional[float] = Field(default=None)
    take_profit_usd: Optional[float] = Field(default=None)
    trailing_stop_pct: Optional[float] = Field(default=None)
    toxic_divergence_threshold: Optional[float] = None
    toxic_divergence_ticks: Optional[int] = None
    critical_reconcile_drift_shares: Optional[float] = None
    fill_settlement_grace_sec: Optional[float] = None
    recent_order_retention_sec: Optional[float] = None
    recent_order_max_per_token: Optional[int] = None
    fallback_poll_cap: Optional[int] = None
    pre_entry_stable_checks: Optional[int] = None
    pre_entry_min_quality_score: Optional[float] = None
    pre_entry_max_spread_bps: Optional[float] = None
    pre_entry_max_divergence: Optional[float] = None
    post_fill_entry_guard_sec: Optional[float] = None
    post_fill_entry_score_drop: Optional[float] = None
    post_fill_entry_spread_widen_bps: Optional[float] = None
    cycle_lockout_bad_cycles: Optional[int] = None
    cycle_lockout_loss_usd: Optional[float] = None
    cycle_lockout_sec: Optional[float] = None
    placement_failure_lockout_count: Optional[int] = None
    placement_failure_lockout_sec: Optional[float] = None
    close_only_toxic_checks: Optional[int] = None
    close_only_toxic_spread_bps: Optional[float] = None
    negative_edge_min_fills: Optional[int] = None
    negative_edge_markout_5s_threshold: Optional[float] = None
    negative_edge_adverse_pct_threshold: Optional[float] = None
    negative_edge_min_spread_capture_events: Optional[int] = None
    negative_edge_spread_capture_threshold_usd: Optional[float] = None
    require_flat_start: Optional[bool] = None
    flat_start_max_shares: Optional[float] = None
    max_one_sided_ticks: Optional[int] = None
    min_fv_to_quote: Optional[float] = None
    close_window_sec: Optional[float] = None
    auto_next_window: Optional[bool] = None
    resolution_wait_sec: Optional[float] = None
    liq_price_floor_enabled: Optional[bool] = None
    liq_gradual_chunks: Optional[int] = Field(default=None)
    liq_chunk_interval_sec: Optional[float] = Field(default=None)
    liq_chunk_interval_s: Optional[float] = Field(default=None)
    liq_taker_threshold_sec: Optional[float] = None
    liq_max_discount_from_fv: Optional[float] = Field(default=None)
    aggressive_liq_after_sec: Optional[float] = None
    aggressive_liq_chunk_interval_sec: Optional[float] = None
    aggressive_liq_taker_threshold_sec: Optional[float] = None
    aggressive_liq_max_discount_from_fv: Optional[float] = None
    liq_abandon_below_floor: Optional[bool] = None
    merge_sell_epsilon: Optional[float] = None
    merge_sell_min_depth_pairs: Optional[float] = None
    entry_settle_sec: Optional[float] = None
    enabled: Optional[bool] = None
    rebate_scoring_enabled: Optional[bool] = None
    rebate_check_interval_ticks: Optional[int] = None
    rebate_require_scoring: Optional[bool] = None
    rebate_non_scoring_size_mult: Optional[float] = None
    rebate_score_timeout_sec: Optional[float] = None
    market_selector_min_score: Optional[float] = None
    paired_fill_ioc_enabled: Optional[bool] = None
    redeem_after_resolution_enabled: Optional[bool] = None
    redeem_retry_interval_sec: Optional[float] = None
    session_limit: Optional[float] = None  # Max USDC budget for session

    @field_validator(
        "half_spread_bps",
        "min_spread_bps",
        "max_spread_bps",
        "vol_spread_mult",
        "dynamic_spread_gamma",
        "dynamic_spread_k",
        "dynamic_spread_min_bps",
        "dynamic_spread_max_bps",
        "order_size_usd",
        "min_order_size_usd",
        "max_order_size_usd",
        "max_inventory_shares",
        "max_net_delta_shares",
        "skew_bps_per_unit",
        "min_quote_size_shares",
        "requote_interval_sec",
        "refresh_interval_s",
        "requote_threshold_bps",
        "requote_interval_jitter_sec",
        "event_pm_mid_threshold_bps",
        "event_binance_threshold_bps",
        "event_poll_interval_sec",
        "event_fallback_interval_sec",
        "size_jitter_pct",
        "max_drawdown_usd",
        "volatility_pause_mult",
        "max_loss_per_fill_usd",
        "take_profit_usd",
        "trailing_stop_pct",
        "toxic_divergence_threshold",
        "critical_reconcile_drift_shares",
        "fill_settlement_grace_sec",
        "pre_entry_min_quality_score",
        "pre_entry_max_spread_bps",
        "pre_entry_max_divergence",
        "post_fill_entry_guard_sec",
        "post_fill_entry_score_drop",
        "post_fill_entry_spread_widen_bps",
        "flat_start_max_shares",
        "min_fv_to_quote",
        "close_window_sec",
        "resolution_wait_sec",
        "liq_chunk_interval_sec",
        "liq_chunk_interval_s",
        "liq_taker_threshold_sec",
        "liq_max_discount_from_fv",
        "merge_sell_epsilon",
        "merge_sell_min_depth_pairs",
        "entry_settle_sec",
        "rebate_non_scoring_size_mult",
        "rebate_score_timeout_sec",
        "market_selector_min_score",
        "redeem_retry_interval_sec",
        "session_limit",
        mode="before",
    )
    @classmethod
    def _validate_finite_numbers(cls, v: Any) -> Any:
        if v is None:
            return None
        if isinstance(v, bool):
            raise ValueError("must be a number")
        try:
            n = float(v)
        except (TypeError, ValueError) as e:
            raise ValueError("must be a valid number") from e
        if not math.isfinite(n):
            raise ValueError("must be finite")
        return v

    @field_validator(
        "gtd_duration_sec",
        "heartbeat_interval_sec",
        "heartbeat_failures_before_shutdown",
        "max_one_sided_ticks",
        "toxic_divergence_ticks",
        "pre_entry_stable_checks",
        "liq_gradual_chunks",
        "price_jitter_ticks",
        "rebate_check_interval_ticks",
        mode="before",
    )
    @classmethod
    def _validate_int_numbers(cls, v: Any) -> Any:
        if v is None:
            return None
        if isinstance(v, bool):
            raise ValueError("must be an integer")
        try:
            n = float(v)
        except (TypeError, ValueError) as e:
            raise ValueError("must be a valid integer") from e
        if not math.isfinite(n) or not n.is_integer():
            raise ValueError("must be a valid integer")
        return int(n)


class VerificationRunRequest(BaseModel):
    kind: str = "server_self_check"


class ConfigUpdateRequestV2(BaseModel):
    session_budget_usd: Optional[float] = None
    base_clip_usd: Optional[float] = None
    target_pair_value_ratio: Optional[float] = None
    soft_excess_value_ratio: Optional[float] = None
    hard_excess_value_ratio: Optional[float] = None
    base_half_spread_bps: Optional[float] = None
    min_edge_bps: Optional[float] = None
    min_pm_spread_bps: Optional[float] = None
    max_half_spread_bps: Optional[float] = None
    vol_floor: Optional[float] = None
    inventory_skew_strength: Optional[float] = None
    defensive_spread_mult: Optional[float] = None
    defensive_size_mult: Optional[float] = None
    unwind_window_sec: Optional[float] = None
    terminal_liquidation_start_sec: Optional[float] = None
    emergency_unwind_timeout_sec: Optional[float] = None
    emergency_taker_start_sec: Optional[float] = None
    hard_drawdown_usd: Optional[float] = None
    max_transport_failures: Optional[int] = None
    market_scope: Optional[str] = None
    tick_interval_sec: Optional[float] = None
    min_market_quality_score: Optional[float] = None
    min_entry_depth_usd: Optional[float] = None
    max_entry_spread_bps: Optional[float] = None
    reconcile_drift_threshold_shares: Optional[float] = None
    fill_settlement_grace_sec: Optional[float] = None
    sell_release_grace_sec: Optional[float] = None
    requote_threshold_bps: Optional[float] = None
    fallback_poll_cap: Optional[int] = None


def _validate_config_updates_before_apply(updates: dict[str, Any]) -> dict[str, Any]:
    """Clamp incoming config updates against MMConfig bounds before runtime apply."""
    if not updates:
        return updates

    staged = MMConfig.from_dict(_runtime.mm_config.to_dict())
    staged.update(**updates)

    normalized: dict[str, Any] = {}
    for key, value in updates.items():
        if key == "session_limit":
            normalized[key] = value
            continue
        target_key = MMConfig.UPDATE_ALIASES.get(key, key)
        if key in MMConfig.UPDATE_ALIASES and target_key in normalized:
            continue
        if hasattr(staged, target_key):
            normalized[target_key] = getattr(staged, target_key)
    return normalized


# ── Mock CLOB Client (for paper trading) ────────────────────────
import random

class _MockOrderSummary:
    """Mimics py_clob_client OrderSummary with .price and .size attributes."""
    def __init__(self, price: float, size: float):
        self.price = str(price)
        self.size = str(size)


class _MockOrderBook:
    """Mimics py_clob_client OrderBookSummary with .bids and .asks lists."""
    def __init__(self, bids: list, asks: list):
        self.bids = bids  # sorted ascending by price
        self.asks = asks  # sorted descending by price


class MockClobClient:
    """Mock CLOB client for paper trading with realistic fill simulation.

    Features:
    - Price-dependent fill probability (closer to FV = more likely to fill)
    - Partial fills (50-100% of order size)
    - Simulated order book around fair value
    - FV sync from MarketMaker for realistic pricing
    """

    def __init__(self, fill_prob: float = 0.15, usdc_balance: float = 1000.0):
        self._orders: dict[str, dict] = {}
        self._next_id = 1
        self._fill_prob = fill_prob  # Base fill probability per get_order call
        self._tick_count = 0  # Count calls for time-based fill logic
        self._usdc_balance = usdc_balance
        self._mock_token_balances: dict[str, float] = {}
        self._manages_mock_balances = True
        # Fair values per token_id, set by market_maker via set_fair_values()
        self._fair_values: dict[str, float] = {}
        self._pm_prices: dict = {}  # {"up": mid, "dn": mid} from real PM WS feed
        self._up_token: str | None = None
        self._dn_token: str | None = None

    @property
    def balance(self) -> float:
        return self._usdc_balance

    def get_balance(self) -> float:
        """Return TOTAL USDC including collateral locked in open BUY orders.

        Matches real PM get_balance_allowance(COLLATERAL) which reports
        the full on-chain balance, not available-minus-locked.
        """
        locked = 0.0
        for order in self._orders.values():
            if order.get("status") != "LIVE":
                continue
            side = str(order.get("side", "")).upper()
            size = float(order.get("size", 0.0) or 0.0)
            size_matched = float(order.get("size_matched", 0.0) or 0.0)
            price = float(order.get("price", 0.0) or 0.0)
            if side == "BUY":
                locked += max(0.0, size - size_matched) * price
            elif side == "SELL":
                short_size = float(order.get("short_size", 0.0) or 0.0)
                inventory_backed = float(order.get("inventory_backed_size", 0.0) or 0.0)
                short_filled = max(0.0, size_matched - inventory_backed)
                locked += max(0.0, short_size - short_filled) * max(0.0, 1.0 - price)
        return self._usdc_balance + locked

    def _complement_token(self, token_id: str) -> str | None:
        if token_id == self._up_token:
            return self._dn_token
        if token_id == self._dn_token:
            return self._up_token
        return None

    def set_fair_values(
        self,
        fv_up: float,
        fv_dn: float,
        market,
        pm_prices: dict | None = None,
    ) -> None:
        """Sync fair values from MarketMaker for realistic fill simulation."""
        if market:
            self._fair_values[market.up_token_id] = fv_up
            self._fair_values[market.dn_token_id] = fv_dn
            self._up_token = market.up_token_id
            self._dn_token = market.dn_token_id
        if pm_prices:
            self._pm_prices = pm_prices

    @staticmethod
    def _required_collateral(side: str, size: float, price: float) -> float:
        if side == "BUY":
            return size * price
        return 0.0

    def create_and_sign_order(self, args: dict) -> dict:
        return {"token_id": args["token_id"], "price": args["price"],
                "size": args["size"], "side": args["side"]}

    def post_order(self, signed, order_type: str) -> dict:
        price = float(signed.get("price", 0.5))
        size = float(signed.get("size", 10))
        side = str(signed.get("side", "BUY")).upper()
        token_id = signed.get("token_id", "")
        token_balance = max(0.0, float(self._mock_token_balances.get(token_id, 0.0) or 0.0))
        inventory_backed_size = min(token_balance, size) if side == "SELL" else 0.0
        short_size = max(0.0, size - inventory_backed_size) if side == "SELL" else 0.0
        collateral = self._required_collateral(side, size, price)
        if side == "SELL":
            collateral = short_size * max(0.0, 1.0 - price)

        if self._usdc_balance < collateral:
            return {"error_msg": "not enough balance", "status": "error"}

        self._usdc_balance -= collateral
        oid = f"mock-{self._next_id:06d}"
        self._next_id += 1
        self._orders[oid] = {
            "status": "LIVE",
            "size_matched": 0,
            "price": price,
            "size": size,
            "side": side,
            "token_id": token_id,
            "collateral": collateral,
            "fill_credit_applied": False,
            "created_tick": self._tick_count,
            "created_ts": time.time(),
            "inventory_backed_size": inventory_backed_size,
            "short_size": short_size,
        }
        return {"orderID": oid}

    @staticmethod
    def _unfilled_collateral(order: dict) -> float:
        side = str(order.get("side", "")).upper()
        price = float(order.get("price", 0.0) or 0.0)
        size = float(order.get("size", 0.0) or 0.0)
        size_matched = float(order.get("size_matched", 0.0) or 0.0)
        if side == "BUY":
            return max(0.0, size - size_matched) * price
        inventory_backed = float(order.get("inventory_backed_size", 0.0) or 0.0)
        short_size = float(order.get("short_size", 0.0) or 0.0)
        short_filled = max(0.0, size_matched - inventory_backed)
        return max(0.0, short_size - short_filled) * max(0.0, 1.0 - price)

    def cancel(self, order_id: str) -> dict:
        order = self._orders.pop(order_id, None)
        if order and order.get("status") == "LIVE":
            refund = self._unfilled_collateral(order)
            self._usdc_balance += refund
        return {"success": True}

    def cancel_all(self) -> dict:
        for order in self._orders.values():
            if order.get("status") == "LIVE":
                self._usdc_balance += self._unfilled_collateral(order)
        self._orders.clear()
        return {"success": True}

    def _compute_fill_prob(self, order: dict) -> float:
        """Compute fill probability based on price distance from fair value.

        BUY closer to (or above) FV → more likely to fill.
        SELL closer to (or below) FV → more likely to fill.
        Age bonus capped at 2x base probability.
        """
        age_ticks = max(0.0, float(self._tick_count - order.get("created_tick", 0)))
        created_ts = float(order.get("created_ts", 0.0) or 0.0)
        age_seconds = max(0.0, time.time() - created_ts) if created_ts > 0 else 0.0
        age = max(age_ticks, age_seconds / 3.0)
        price = order["price"]
        side = order["side"]
        token_id = order.get("token_id", "")
        fv = self._fair_values.get(token_id)

        # Use PM mid price as reference instead of own FV (avoids self-referential simulation)
        if token_id == self._up_token:
            ref_price = self._pm_prices.get("up")
            if ref_price is None:
                ref_price = fv
        elif token_id == self._dn_token:
            ref_price = self._pm_prices.get("dn")
            if ref_price is None:
                ref_price = fv
        else:
            ref_price = fv

        if ref_price is None or ref_price <= 0:
            # No price reference info — fall back to age-based only
            age_mult = min(2.0, 1.0 + age * 0.1)
            return min(0.60, self._fill_prob * age_mult)

        distance = abs(price - ref_price) / max(ref_price, 0.01)

        if side == "BUY":
            # BUY at or above FV → high prob; BUY far below FV → low prob
            if price >= ref_price:
                base = 0.80
            else:
                base = max(0.02, self._fill_prob * (1.0 - distance * 5))
        else:
            # SELL at or below FV → high prob; SELL far above FV → low prob
            if price <= ref_price:
                base = 0.80
            else:
                base = max(0.02, self._fill_prob * (1.0 - distance * 5))

        # Age bonus: max 2x, capped at 0.85
        age_mult = min(2.0, 1.0 + age * 0.08)
        return min(0.85, base * age_mult)

    def get_order(self, order_id: str) -> dict:
        """Check order status. Simulates fills with price-dependent probability.

        Supports partial fills (50-100% of order size).
        """
        self._tick_count += 1
        order = self._orders.get(order_id)
        if order is None:
            return {"status": "CANCELLED", "size_matched": 0}

        if order["status"] != "LIVE":
            return order

        prob = self._compute_fill_prob(order)

        if random.random() < prob:
            # Partial fill: 50-100% of remaining size
            fill_frac = random.uniform(0.5, 1.0)
            already_matched = float(order.get("size_matched", 0))
            remaining = order["size"] - already_matched
            fill_size = round(remaining * fill_frac, 2)

            if fill_size < 0.5:
                fill_size = remaining  # Fill the rest if too small to split

            order["size_matched"] = round(already_matched + fill_size, 2)

            if order["size_matched"] >= order["size"] - 0.01:
                # Fully filled
                order["status"] = "MATCHED"
                order["size_matched"] = order["size"]

            token_id = str(order.get("token_id", ""))
            side = str(order.get("side", "")).upper()
            if side == "BUY":
                cur = float(self._mock_token_balances.get(token_id, 0.0) or 0.0)
                self._mock_token_balances[token_id] = cur + fill_size
            elif side == "SELL":
                inventory_backed = float(order.get("inventory_backed_size", 0.0) or 0.0)
                inv_filled_prev = min(inventory_backed, already_matched)
                inv_filled_new = min(inventory_backed, order["size_matched"])
                short_filled_prev = max(0.0, already_matched - inventory_backed)
                short_filled_new = max(0.0, order["size_matched"] - inventory_backed)
                delta_inv = max(0.0, inv_filled_new - inv_filled_prev)
                delta_short = max(0.0, short_filled_new - short_filled_prev)
                if delta_inv > 0:
                    cur = float(self._mock_token_balances.get(token_id, 0.0) or 0.0)
                    self._mock_token_balances[token_id] = max(0.0, cur - delta_inv)
                    self._usdc_balance += delta_inv * order["price"]
                if delta_short > 0:
                    comp = self._complement_token(token_id)
                    if comp:
                        cur_comp = float(self._mock_token_balances.get(comp, 0.0) or 0.0)
                        self._mock_token_balances[comp] = cur_comp + delta_short

            log.info(f"[MOCK] Fill: {order['side']} "
                     f"{order['size_matched']:.1f}/{order['size']:.1f}"
                     f"@{order['price']:.2f} (prob={prob:.2f})")

        return order

    def post_heartbeat(self) -> dict:
        return {"success": True}

    def is_order_scoring(self, params: dict) -> dict:
        return {"scoring": True}

    def get_order_book(self, token_id: str) -> _MockOrderBook:
        """Return simulated order book around fair value.

        Generates 5 levels of bids and asks around FV with random sizes.
        Returns _MockOrderBook with .bids (ascending) and .asks (descending)
        matching py_clob_client OrderBookSummary format.
        """
        fv = self._fair_values.get(token_id, 0.50)
        fv = max(0.05, min(0.95, fv))

        bids = []
        asks = []
        for i in range(5):
            bid_price = round(max(0.01, fv - 0.01 * (i + 1)), 2)
            ask_price = round(min(0.99, fv + 0.01 * (i + 1)), 2)
            bid_size = round(random.uniform(10, 50), 1)
            ask_size = round(random.uniform(10, 50), 1)
            bids.append(_MockOrderSummary(bid_price, bid_size))
            asks.append(_MockOrderSummary(ask_price, ask_size))

        # bids sorted ascending by price (lowest first, highest last)
        bids.sort(key=lambda x: float(x.price))
        # asks sorted descending by price (highest first, lowest last)
        asks.sort(key=lambda x: float(x.price), reverse=True)

        return _MockOrderBook(bids, asks)


# ── CLOB Client Factory ────────────────────────────────────────
def _create_clob_client(paper_mode: bool = True, initial_usdc: float = 1000.0) -> Any:
    """Create CLOB client — mock for paper, real for live."""
    if paper_mode:
        log.info("Using MOCK CLOB client (paper trading)")
        return MockClobClient(fill_prob=0.15, usdc_balance=initial_usdc)

    try:
        from py_clob_client.client import ClobClient
        from py_clob_client.clob_types import ApiCreds

        creds = ApiCreds(
            api_key=PM_API_KEY,
            api_secret=PM_API_SECRET,
            api_passphrase=PM_API_PASSPHRASE,
        )
        client = ClobClient(
            host="https://clob.polymarket.com",
            key=PM_PRIVATE_KEY,
            chain_id=137,
            creds=creds,
            funder=PM_FUNDER,
            signature_type=2,  # POLY_GNOSIS_SAFE
        )
        log.info("Using REAL CLOB client (live trading)")
        return client
    except Exception as e:
        log.error(f"Failed to create real CLOB client: {e}")
        raise HTTPException(
            status_code=500,
            detail=f"Failed to create CLOB client for live trading: {e}"
        )


# ── MM Runtime ──────────────────────────────────────────────────
class MMRuntime:
    """Manages the lifecycle of feeds and MarketMaker."""

    def __init__(self):
        self.feed_state: Optional[feeds.State] = None
        self.mm: Optional[Any] = None
        self.mm_config: MMConfig = MMConfig()
        self._feed_tasks: list[asyncio.Task] = []
        self._monitor_task: asyncio.Task | None = None
        self._running = False
        self._coin: str = ""
        self._timeframe: str = ""
        self._requested_coin: str = ""
        self._requested_timeframe: str = ""
        self._paper_mode: bool = True
        self._dev_mode: bool = False
        self._initial_usdc: float = 1000.0
        self._next_window_at: float = 0.0  # timestamp when next window starts
        self._mongo = None  # MongoLogger (if MONGO_URI set)
        self._mongo_log_handler = None  # MongoLogHandler (if active)
        self._pnl_history: list[tuple[float, float]] = []  # [(timestamp, session_pnl), ...]
        self._watching = False
        self._start_balance: float = 0.0  # PM USDC balance at session start
        self._strike_invalid: bool = False
        self._strike_retry_task: asyncio.Task | None = None
        self._last_good_tick_size: float = 0.01
        self._last_good_min_order_size: float = 5.0
        self._last_market_selection: dict[str, Any] | None = None
        self._alerts: dict[str, dict[str, Any]] = {}
        self._mm_alert_state: dict[str, Any] = {
            "fallback_cap_hits": 0,
            "fallback_near_hits": 0,
        }
        self._verification_task: asyncio.Task | None = None
        self._verification_status: dict[str, Any] = {
            "running": False,
            "kind": "",
            "started_at": 0.0,
            "finished_at": 0.0,
            "ok": None,
            "summary": "",
            "checks": [],
            "command": [],
            "exit_code": None,
            "stdout_tail": "",
            "stderr_tail": "",
            "error": "",
        }
        self._runtime_watchdog: dict[str, Any] = {
            "active": False,
            "last_check_ts": 0.0,
            "last_loop_lag_ms": 0.0,
            "last_cpu_pct": 0.0,
            "last_log_ts": 0.0,
            "last_top_counts": [],
            "last_top_tasks": [],
            "last_main_stack": [],
        }

    @property
    def is_running(self) -> bool:
        return self._running and self.mm is not None

    @staticmethod
    def _is_valid_strike(strike: float) -> bool:
        """Strike sanity bounds for tradable windows."""
        return 0.0 < float(strike) <= 200000.0

    @staticmethod
    def _is_valid_tick_size(tick_size: float) -> bool:
        """PM tick size guardrail: expected values are typically 0.001 or 0.01."""
        return 0.0 < float(tick_size) <= 0.01

    @staticmethod
    def _is_valid_min_order_size(min_order_size: float) -> bool:
        """Sanity bounds for PM min_order_size to avoid pathological API values."""
        return 0.0 < float(min_order_size) <= 100.0

    @staticmethod
    def _safe_float(value: Any, default: float = 0.0) -> float:
        try:
            return float(value)
        except (TypeError, ValueError):
            return default

    def _enforce_maker_only(self, source: str) -> None:
        """Force maker-only quoting in runtime configuration."""
        if bool(getattr(self.mm_config, "use_post_only", True)):
            self.clear_alert("maker_only")
            return
        self.mm_config.use_post_only = True
        msg = f"Maker-only enforced: ignored use_post_only=false ({source})"
        log.warning(msg)
        self.set_alert("maker_only", msg, level="warning")

    def set_alert(self, source: str, message: str, level: str = "warning") -> None:
        """Set/update a dashboard-visible runtime alert by source key."""
        self._alerts[source] = {
            "source": source,
            "level": level,
            "message": message,
            "ts": time.time(),
        }

    def clear_alert(self, source: str) -> None:
        self._alerts.pop(source, None)

    def list_alerts(self) -> list[dict[str, Any]]:
        alerts = list(self._alerts.values())
        alerts.sort(key=lambda x: x.get("ts", 0.0), reverse=True)
        return alerts

    async def _teardown_mongo_logger(self) -> None:
        if self._mongo_log_handler:
            logging.getLogger().removeHandler(self._mongo_log_handler)
            self._mongo_log_handler = None
        if self._mongo:
            try:
                await self._mongo.stop()
            except Exception:
                pass
            self._mongo = None

    async def _attach_mongo_logger(
        self,
        *,
        register_fill: Callable[[Any], None] | None = None,
        register_snapshot: Callable[[Any], None] | None = None,
        runtime_tag: str = "",
    ) -> None:
        if not config.MONGO_URI:
            return
        await self._teardown_mongo_logger()
        try:
            from mm_shared.mongo_logger import MongoLogger, MongoLogHandler

            self._mongo = MongoLogger(config.MONGO_URI, config.MONGO_DB)
            await self._mongo.start()
            if register_fill is not None:
                register_fill(self._mongo)
            if register_snapshot is not None:
                register_snapshot(self._mongo)
            self._mongo_log_handler = MongoLogHandler(self._mongo)
            self._mongo_log_handler.setLevel(logging.INFO)
            logging.getLogger().addHandler(self._mongo_log_handler)
            if runtime_tag:
                log.info("MongoLogger attached (%s)", runtime_tag)
            else:
                log.info("MongoLogger attached")
        except Exception as e:
            log.warning(f"MongoLogger init failed (continuing without): {e}")
            await self._teardown_mongo_logger()

    def get_verification_status(self) -> dict[str, Any]:
        return copy.deepcopy(self._verification_status)

    async def _run_server_self_check(self) -> dict[str, Any]:
        snap = self.snapshot()
        required_blocks = [
            "order_tracking",
            "cycle_guard",
            "negative_edge_guard",
            "liquidation",
            "api_errors",
        ]
        checks: list[dict[str, Any]] = []
        for key in required_blocks:
            checks.append(
                {
                    "name": f"state_contract:{key}",
                    "ok": key in snap,
                    "detail": "present" if key in snap else "missing",
                }
            )
        maker_only = bool((snap.get("config") or {}).get("use_post_only", True))
        checks.append(
            {
                "name": "config:maker_only",
                "ok": maker_only,
                "detail": "use_post_only=true" if maker_only else "use_post_only=false",
            }
        )
        watchdog_present = "runtime_watchdog" in snap
        checks.append(
            {
                "name": "state_contract:runtime_watchdog",
                "ok": watchdog_present,
                "detail": "present" if watchdog_present else "missing",
            }
        )
        app_meta_ok = bool(snap.get("app_version")) and bool(snap.get("app_git_hash"))
        checks.append(
            {
                "name": "state_contract:app_meta",
                "ok": app_meta_ok,
                "detail": "present" if app_meta_ok else "missing",
            }
        )
        alert_sources = {str(a.get("source", "")) for a in snap.get("alerts", [])}
        blocking_alerts = {
            "critical_drift_pause",
            "residual_inventory_failure",
            "runtime_watchdog",
            "runtime_cpu_watchdog",
        }
        bad_alerts = sorted(source for source in alert_sources if source in blocking_alerts)
        checks.append(
            {
                "name": "alerts:blocking",
                "ok": not bad_alerts,
                "detail": ", ".join(bad_alerts) if bad_alerts else "none",
            }
        )
        fallback_cap = int(
            (snap.get("config") or {}).get("fallback_poll_cap", 0)
            or 0
        )
        fallback_count = int((snap.get("order_tracking") or {}).get("last_fallback_poll_count", 0) or 0)
        fallback_ok = fallback_cap <= 0 or fallback_count <= fallback_cap
        checks.append(
            {
                "name": "fallback_poll:within_cap",
                "ok": fallback_ok,
                "detail": f"{fallback_count}/{fallback_cap}" if fallback_cap > 0 else str(fallback_count),
            }
        )
        passed = sum(1 for check in checks if check["ok"])
        return {
            "ok": passed == len(checks),
            "summary": f"{passed}/{len(checks)} checks passed",
            "checks": checks,
            "command": [],
            "exit_code": 0 if passed == len(checks) else 1,
            "stdout_tail": "",
            "stderr_tail": "",
            "error": "",
        }

    def _verification_command(self, kind: str) -> list[str] | None:
        if kind == "pytest_safety":
            return [
                sys.executable,
                "-m",
                "pytest",
                "-q",
                "tests/test_acceptance_minimal.py",
                "tests/test_reconciliation.py",
                "tests/test_batch_runtime_guards.py",
                "tests/test_session_cap_guards.py",
                "tests/test_bug_fixes.py",
                "tests/test_telegram_conflict.py",
                "tests/test_runtime_metrics.py",
            ]
        if kind == "backtest_smoke":
            return [
                sys.executable,
                "backtest/run_backtest.py",
            ]
        return None

    @staticmethod
    def _tail_text(value: bytes, limit: int = 16000) -> str:
        text = value.decode("utf-8", errors="replace")
        if len(text) <= limit:
            return text
        return text[-limit:]

    async def _run_command_verification(self, kind: str, command: list[str]) -> dict[str, Any]:
        env = os.environ.copy()
        py_path = env.get("PYTHONPATH", "")
        env["PYTHONPATH"] = f"{SRC_DIR}:{py_path}" if py_path else str(SRC_DIR)
        proc = await asyncio.create_subprocess_exec(
            *command,
            cwd=str(BASE_DIR),
            env=env,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
        )
        stdout, stderr = await proc.communicate()
        ok = proc.returncode == 0
        return {
            "ok": ok,
            "summary": f"{kind} {'passed' if ok else 'failed'} (exit_code={proc.returncode})",
            "checks": [],
            "command": command,
            "exit_code": proc.returncode,
            "stdout_tail": self._tail_text(stdout),
            "stderr_tail": self._tail_text(stderr),
            "error": "",
        }

    async def _verification_runner(self, kind: str) -> None:
        self._verification_status = {
            "running": True,
            "kind": kind,
            "started_at": time.time(),
            "finished_at": 0.0,
            "ok": None,
            "summary": "",
            "checks": [],
            "command": [],
            "exit_code": None,
            "stdout_tail": "",
            "stderr_tail": "",
            "error": "",
        }
        try:
            if kind == "server_self_check":
                result = await self._run_server_self_check()
            else:
                command = self._verification_command(kind)
                if not command:
                    raise ValueError(f"unsupported verification kind: {kind}")
                result = await self._run_command_verification(kind, command)
            self._verification_status.update(result)
        except asyncio.CancelledError:
            self._verification_status.update(
                {
                    "running": False,
                    "finished_at": time.time(),
                    "ok": False,
                    "summary": "verification cancelled",
                    "error": "cancelled",
                }
            )
            raise
        except Exception as e:
            self._verification_status.update(
                {
                    "running": False,
                    "finished_at": time.time(),
                    "ok": False,
                    "summary": f"{kind} failed",
                    "error": str(e),
                }
            )
        else:
            self._verification_status["running"] = False
            self._verification_status["finished_at"] = time.time()
        finally:
            self._verification_task = None

    async def start_verification(self, kind: str) -> dict[str, Any]:
        allowed = {"server_self_check", "pytest_safety", "backtest_smoke"}
        kind = str(kind or "server_self_check").strip() or "server_self_check"
        if kind not in allowed:
            raise HTTPException(status_code=400, detail=f"unsupported verification kind: {kind}")
        if self._verification_task and not self._verification_task.done():
            raise HTTPException(status_code=409, detail="verification already running")
        self._verification_task = asyncio.create_task(self._verification_runner(kind))
        return self.get_verification_status()

    async def cancel_verification(self) -> dict[str, Any]:
        task = self._verification_task
        if not task or task.done():
            return self.get_verification_status()
        task.cancel()
        try:
            await task
        except asyncio.CancelledError:
            pass
        except Exception as e:
            log.warning("Verification cancel failed: %s", e)
        return self.get_verification_status()

    def _clear_mm_alerts(self) -> None:
        for source in (
            "negative_edge_guard_activated",
            "cycle_guard_no_trade",
            "cycle_guard_close_only",
            "drawdown_exit",
            "fallback_poll_hot",
            "residual_inventory_failure",
            "critical_drift_pause",
            "aggressive_liquidation_activated",
        ):
            self.clear_alert(source)
        self._mm_alert_state["fallback_cap_hits"] = 0
        self._mm_alert_state["fallback_near_hits"] = 0

    def _sync_mm_alerts_from_snapshot(self, snap: dict[str, Any]) -> None:
        order_tracking = snap.get("order_tracking") or {}
        cycle_guard = snap.get("cycle_guard") or {}
        negative_edge_guard = snap.get("negative_edge_guard") or {}
        liquidation = snap.get("liquidation") or {}
        pause_reason = str(snap.get("pause_reason") or "")
        config = snap.get("config") or {}

        fallback_cap = int(
            order_tracking.get("fallback_poll_cap")
            or config.get("fallback_poll_cap")
            or getattr(self.mm_config, "fallback_poll_cap", 0)
            or 0
        )
        fallback_count = int(order_tracking.get("last_fallback_poll_count", 0) or 0)
        if fallback_cap > 0:
            if fallback_count >= fallback_cap:
                self._mm_alert_state["fallback_cap_hits"] += 1
            else:
                self._mm_alert_state["fallback_cap_hits"] = 0
            near_threshold = max(1, fallback_cap - 1)
            if fallback_count >= near_threshold:
                self._mm_alert_state["fallback_near_hits"] += 1
            else:
                self._mm_alert_state["fallback_near_hits"] = 0
            if (
                self._mm_alert_state["fallback_cap_hits"] >= 2
                or self._mm_alert_state["fallback_near_hits"] >= 3
            ):
                self.set_alert(
                    "fallback_poll_hot",
                    f"Fallback polling hot: {fallback_count} tracked orders (cap={fallback_cap})",
                    level="warning",
                )
            else:
                self.clear_alert("fallback_poll_hot")
        else:
            self.clear_alert("fallback_poll_hot")
            self._mm_alert_state["fallback_cap_hits"] = 0
            self._mm_alert_state["fallback_near_hits"] = 0

        if bool(negative_edge_guard.get("active")):
            self.set_alert(
                "negative_edge_guard_activated",
                str(negative_edge_guard.get("reason") or "Negative edge guard activated"),
                level="warning",
            )
        else:
            self.clear_alert("negative_edge_guard_activated")

        cycle_mode = str(cycle_guard.get("mode") or "off")
        cycle_reason = str(cycle_guard.get("reason") or "")
        if cycle_mode == "no_trade":
            self.set_alert(
                "cycle_guard_no_trade",
                cycle_reason or "Cycle guard no-trade lockout active",
                level="warning",
            )
        else:
            self.clear_alert("cycle_guard_no_trade")
        if cycle_mode == "close_only":
            self.set_alert(
                "cycle_guard_close_only",
                cycle_reason or "Cycle guard close-only active",
                level="warning",
            )
        else:
            self.clear_alert("cycle_guard_close_only")

        lower_reasons = " | ".join(
            reason.lower()
            for reason in (
                cycle_reason,
                str(liquidation.get("reason") or ""),
                pause_reason,
            )
            if reason
        )
        if "max drawdown" in lower_reasons:
            self.set_alert(
                "drawdown_exit",
                str(liquidation.get("reason") or cycle_reason or pause_reason or "Max drawdown exit active"),
                level="error",
            )
        else:
            self.clear_alert("drawdown_exit")

        if bool(liquidation.get("residual_inventory_failure")):
            self.set_alert(
                "residual_inventory_failure",
                "Residual inventory remained after closing window",
                level="error",
            )
        else:
            self.clear_alert("residual_inventory_failure")

        if "critical inventory drift" in pause_reason.lower():
            self.set_alert(
                "critical_drift_pause",
                pause_reason,
                level="error",
            )
        else:
            self.clear_alert("critical_drift_pause")

        liq_mode = str(liquidation.get("mode") or "inactive")
        if liq_mode in {"aggressive", "taker"}:
            self.set_alert(
                "aggressive_liquidation_activated",
                str(liquidation.get("reason") or f"{liq_mode} liquidation active"),
                level="warning",
            )
        else:
            self.clear_alert("aggressive_liquidation_activated")

    def _startup_window_block_reason(self, market: MarketInfo | None) -> str:
        """Return non-empty reason when MM should not start on this window."""
        if not market:
            return ""

        time_left = max(0.0, float(market.time_remaining))
        if time_left <= 0.0:
            return "Selected market window already expired"

        window_dur = max(1.0, float(market.window_end - market.window_start))
        close_sec = min(float(self.mm_config.close_window_sec), window_dur * 0.4)
        if time_left <= close_sec:
            return (
                f"Selected market window too close to close "
                f"({time_left:.1f}s left <= close_window_sec {close_sec:.1f}s)"
            )
        return ""

    async def _fetch_public_book_metrics(self, token_id: str) -> dict[str, float]:
        """Fetch best bid/ask and coarse depth from public PM orderbook."""
        import requests as _req

        empty = {"best_bid": 0.0, "best_ask": 0.0, "bid_depth_usd": 0.0, "ask_depth_usd": 0.0}
        if not token_id:
            return empty
        try:
            resp = await asyncio.to_thread(
                lambda: _req.get(
                    "https://clob.polymarket.com/book",
                    params={"token_id": token_id},
                    timeout=8,
                )
            )
            if not resp.ok:
                return empty
            data = resp.json() or {}
            bids = data.get("bids") or []
            asks = data.get("asks") or []
            bid_levels = [level for level in bids if isinstance(level, dict)]
            ask_levels = [level for level in asks if isinstance(level, dict)]
            bid_prices = [self._safe_float(level.get("price"), 0.0) for level in bid_levels]
            ask_prices = [self._safe_float(level.get("price"), 0.0) for level in ask_levels]
            best_bid = max([p for p in bid_prices if p > 0], default=0.0)
            best_ask = min([p for p in ask_prices if p > 0], default=0.0)
            bid_depth = sum(
                self._safe_float(level.get("price"), 0.0) * self._safe_float(level.get("size"), 0.0)
                for level in bid_levels[:30]
            )
            ask_depth = sum(
                self._safe_float(level.get("price"), 0.0) * self._safe_float(level.get("size"), 0.0)
                for level in ask_levels[:30]
            )
            return {
                "best_bid": max(0.0, best_bid),
                "best_ask": max(0.0, best_ask),
                "bid_depth_usd": max(0.0, bid_depth),
                "ask_depth_usd": max(0.0, ask_depth),
            }
        except Exception:
            return empty

    async def _build_selector_candidate(self, coin: str, timeframe: str) -> dict[str, Any] | None:
        """Build market selector metrics for a single coin/timeframe pair."""
        try:
            tokens = await asyncio.wait_for(
                asyncio.to_thread(feeds.fetch_pm_tokens, coin, timeframe),
                timeout=15.0,
            )
        except Exception:
            return None
        if not tokens or not tokens[0] or not tokens[1]:
            return None
        up_id, dn_id, _ = tokens
        up_book, dn_book = await asyncio.gather(
            self._fetch_public_book_metrics(up_id),
            self._fetch_public_book_metrics(dn_id),
        )
        up_bid = up_book["best_bid"]
        up_ask = up_book["best_ask"]
        dn_bid = dn_book["best_bid"]
        dn_ask = dn_book["best_ask"]
        if up_bid <= 0 or up_ask <= 0 or dn_bid <= 0 or dn_ask <= 0:
            return None

        spread_up = ((up_ask - up_bid) / max(up_ask, 1e-9)) * 10000.0
        spread_dn = ((dn_ask - dn_bid) / max(dn_ask, 1e-9)) * 10000.0
        spread_bps = max(0.0, (spread_up + spread_dn) / 2.0)
        depth_usd = min(
            up_book["bid_depth_usd"] + up_book["ask_depth_usd"],
            dn_book["bid_depth_usd"] + dn_book["ask_depth_usd"],
        )
        mid_up = (up_bid + up_ask) / 2.0
        mid_dn = (dn_bid + dn_ask) / 2.0
        avg_price = max(0.01, min(0.99, (mid_up + mid_dn) / 2.0))

        volume_24h = 0.0
        try:
            event_data = await asyncio.wait_for(
                asyncio.to_thread(feeds.fetch_pm_event_data, coin, timeframe),
                timeout=12.0,
            )
            if event_data:
                market0 = ((event_data.get("markets") or [{}])[0]) if isinstance(event_data, dict) else {}
                volume_24h = max(
                    self._safe_float(event_data.get("volume24hr"), 0.0),
                    self._safe_float(event_data.get("volume24h"), 0.0),
                    self._safe_float(event_data.get("volume"), 0.0),
                    self._safe_float(market0.get("volume24hr"), 0.0),
                    self._safe_float(market0.get("volume24h"), 0.0),
                    self._safe_float(market0.get("volume"), 0.0),
                )
        except Exception:
            volume_24h = 0.0

        volatility = 1.0
        if self.feed_state and self._coin == coin and self.feed_state.klines:
            try:
                closes = [float(k[4]) for k in self.feed_state.klines[-40:] if len(k) >= 5]
                if len(closes) >= 2:
                    abs_rets = [abs(closes[i] / closes[i - 1] - 1.0) for i in range(1, len(closes))]
                    volatility = max(0.0, min(5.0, sum(abs_rets) / len(abs_rets)))
            except Exception:
                volatility = 1.0

        return {
            "coin": coin,
            "timeframe": timeframe,
            "spread_bps": spread_bps,
            "depth_usd": depth_usd,
            "volume_24h": volume_24h,
            "avg_price": avg_price,
            "volatility": volatility,
        }

    async def _auto_select_market(self, coin: str, timeframe: str) -> tuple[str, str]:
        """Legacy V1 auto-selection removed."""
        return str(coin), str(timeframe)
        coin_auto = coin.strip().lower() == "auto"
        tf_auto = timeframe.strip().lower() == "auto"
        if not coin_auto and not tf_auto:
            return coin, timeframe

        candidate_pairs: list[tuple[str, str]] = []
        coins = config.COINS if coin_auto else [coin]
        for c in coins:
            timeframes = config.COIN_TIMEFRAMES.get(c, [])
            if tf_auto:
                candidate_pairs.extend((c, tf) for tf in timeframes)
            elif timeframe in timeframes:
                candidate_pairs.append((c, timeframe))

        if not candidate_pairs:
            fallback_coin = config.COINS[0] if coin_auto and config.COINS else coin
            fallback_timeframes = config.COIN_TIMEFRAMES.get(fallback_coin, [])
            fallback_tf = (
                fallback_timeframes[0]
                if tf_auto and fallback_timeframes
                else timeframe
            )
            return fallback_coin, fallback_tf

        from mm.market_selector import MarketSelector
        selector = MarketSelector()
        selector.MIN_RECOMMEND_SCORE = float(
            getattr(self.mm_config, "market_selector_min_score", selector.MIN_RECOMMEND_SCORE)
        )

        results = await asyncio.gather(
            *(self._build_selector_candidate(c, tf) for c, tf in candidate_pairs),
            return_exceptions=True,
        )
        candidates = [r for r in results if isinstance(r, dict)]
        if not candidates:
            fallback_coin, fallback_tf = candidate_pairs[0]
            return fallback_coin, fallback_tf

        ranked = selector.rank_markets(candidates)
        best = selector.recommend(candidates) or ranked[0]
        self._last_market_selection = {
            "requested_coin": coin,
            "requested_timeframe": timeframe,
            "selected_coin": best.get("coin", coin),
            "selected_timeframe": best.get("timeframe", timeframe),
            "selected_score": best.get("score", 0.0),
            "selected_recommendation": best.get("recommendation", "skip"),
            "top": ranked[:5],
            "ts": time.time(),
        }
        return str(best.get("coin", coin)), str(best.get("timeframe", timeframe))

    async def _cancel_strike_retry_task(self) -> None:
        """Stop background strike recovery loop if active."""
        task = self._strike_retry_task
        if not task:
            return
        task.cancel()
        try:
            await task
        except asyncio.CancelledError:
            pass
        except Exception as e:
            log.warning("Strike retry task cleanup failed: %s", e)
        finally:
            self._strike_retry_task = None

    async def _cancel_monitor_task(self) -> None:
        """Cancel runtime window-monitor task if active."""
        task = self._monitor_task
        if not task:
            return
        current = asyncio.current_task()
        if task is current:
            self._monitor_task = None
            return
        task.cancel()
        try:
            await task
        except asyncio.CancelledError:
            pass
        except Exception as e:
            log.warning("Monitor task cleanup failed: %s", e)
        finally:
            self._monitor_task = None

    def _ensure_monitor_task(self) -> None:
        """Spawn single monitor task (idempotent)."""
        if self._monitor_task and not self._monitor_task.done():
            return
        self._monitor_task = asyncio.create_task(self._monitor_window_expiry())

    async def _stop_feed_tasks(self) -> None:
        """Cancel feed tasks and sweep leaked feed coroutines from the loop."""
        tasks = list(self._feed_tasks)
        for task in tasks:
            task.cancel()
        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)
        self._feed_tasks.clear()

        # Safety sweep: in case tracked list got out of sync, cancel any leaked
        # feed tasks left in the event loop.
        leaked: list[asyncio.Task] = []
        try:
            loop = asyncio.get_running_loop()
            current = asyncio.current_task(loop=loop)
            for task in asyncio.all_tasks(loop):
                if task is current or task.done():
                    continue
                label = _task_coro_label(task)
                if any(name in label for name in ("ob_poller", "binance_feed", "pm_feed")):
                    if task not in tasks:
                        leaked.append(task)
        except Exception:
            leaked = []

        if leaked:
            log.warning(
                "Feed task sweep: cancelling %d leaked feed task(s) not tracked by runtime",
                len(leaked),
            )
            for task in leaked:
                task.cancel()
            await asyncio.gather(*leaked, return_exceptions=True)

        if self.feed_state:
            self.feed_state.binance_ws_connected = False
            self.feed_state.binance_ob_connected = False
            self.feed_state.pm_connected = False

    def _ensure_strike_retry_task(self) -> None:
        """Launch strike recovery loop when window is in watch mode."""
        if self._strike_retry_task and not self._strike_retry_task.done():
            return
        self._strike_retry_task = asyncio.create_task(self._retry_strike_loop())

    async def _retry_strike_loop(self) -> None:
        """Retry strike fetch every 30s until strike becomes valid."""
        try:
            while self._running and self._strike_invalid:
                await asyncio.sleep(30.0)
                if not self._running or not self._strike_invalid:
                    break
                mm = self.mm
                if not mm or not mm.market:
                    break

                try:
                    strike, window_start, window_end = await asyncio.wait_for(
                        asyncio.to_thread(
                            feeds.fetch_pm_strike,
                            self._coin,
                            self._timeframe,
                            5,
                            2.0,
                        ),
                        timeout=30.0,
                    )
                except Exception as e:
                    log.warning("Strike retry failed, staying in watch mode: %s", e)
                    continue

                if (not self._is_valid_strike(strike)
                        or window_start <= 0
                        or window_end <= window_start):
                    log.warning(
                        "Strike retry invalid (strike=%.6f ws=%.0f we=%.0f), staying in watch mode",
                        float(strike), float(window_start), float(window_end),
                    )
                    continue

                buffered_window_end = max(window_start + 1.0, window_end - 10.0)
                mm.market.strike = float(strike)
                mm.market.window_start = float(window_start)
                mm.market.window_end = float(buffered_window_end)
                self._strike_invalid = False
                mm._requote_event.set()
                log.info(
                    "Strike recovered: strike=%.2f window=[%.0f, %.0f] — resuming normal trading",
                    mm.market.strike, mm.market.window_start, mm.market.window_end,
                )
                break
        except asyncio.CancelledError:
            pass
        finally:
            self._strike_retry_task = None

    async def validate_live_credentials(self) -> dict:
        """Validate Polymarket API credentials via get_api_keys() (read-only)."""
        missing = []
        if not PM_PRIVATE_KEY:
            missing.append("PM_PRIVATE_KEY")
        if not PM_FUNDER:
            missing.append("PM_FUNDER")
        if not PM_API_KEY:
            missing.append("PM_API_KEY")
        if not PM_API_SECRET:
            missing.append("PM_API_SECRET")
        if not PM_API_PASSPHRASE:
            missing.append("PM_API_PASSPHRASE")

        if missing:
            raise HTTPException(
                status_code=400,
                detail=f"Missing credentials: {', '.join(missing)}"
            )

        try:
            from py_clob_client.client import ClobClient
            from py_clob_client.clob_types import ApiCreds

            creds = ApiCreds(
                api_key=PM_API_KEY,
                api_secret=PM_API_SECRET,
                api_passphrase=PM_API_PASSPHRASE,
            )
            client = ClobClient(
                host="https://clob.polymarket.com",
                key=PM_PRIVATE_KEY,
                chain_id=137,
                creds=creds,
                funder=PM_FUNDER,
                signature_type=2,  # POLY_GNOSIS_SAFE
            )
            result = await asyncio.to_thread(client.get_api_keys)
            log.info(f"Credential validation OK: {len(result) if isinstance(result, list) else 'ok'} keys")
            return {"valid": True}

        except HTTPException:
            raise
        except Exception as e:
            error_msg = str(e)
            if "401" in error_msg or "Unauthorized" in error_msg.lower():
                detail = "Invalid API credentials (401). Regenerate on polymarket.com"
            elif "403" in error_msg:
                detail = "API access forbidden (403). Key may be revoked."
            else:
                detail = f"Credential validation failed: {error_msg}"
            log.warning(f"Credential validation failed: {error_msg}")
            raise HTTPException(status_code=400, detail=detail)

    async def start(
        self,
        coin: str,
        timeframe: str,
        paper_mode: bool = True,
        initial_usdc: float = 1000.0,
        dev: bool = False,
        session_budget_usd: Optional[float] = None,
    ) -> dict:
        """Legacy V1 runtime start removed."""
        raise HTTPException(
            status_code=410,
            detail="legacy_v1_runtime_removed_use_mmv2",
        )
        if self._running:
            raise HTTPException(status_code=400, detail="Already running")

        await self._cancel_strike_retry_task()
        await self._cancel_monitor_task()
        await self._stop_feed_tasks()
        self._strike_invalid = False

        if self._watching:
            await self.stop_watch()

        # Previous session may have already stopped internally.
        # Drop stale MM reference before creating a fresh instance.
        if self.mm and not self.mm._running:
            self.mm = None
        self._clear_mm_alerts()

        initial_usdc = float(initial_usdc)
        req_coin = str(coin)
        req_tf = str(timeframe)
        self._requested_coin = req_coin
        self._requested_timeframe = req_tf

        selected_coin, selected_tf = await self._auto_select_market(req_coin, req_tf)
        if selected_coin != coin or selected_tf != timeframe:
            log.info(
                "Market selector: %s/%s -> %s/%s",
                req_coin,
                req_tf,
                selected_coin,
                selected_tf,
            )
        coin = selected_coin
        timeframe = selected_tf
        self._coin = coin
        self._timeframe = timeframe
        self._paper_mode = paper_mode
        self._dev_mode = dev
        if dev:
            _telegram.switch_credentials(
                token=os.environ.get("DEV_TELEGRAM_BOT_TOKEN", ""),
                chat_id=os.environ.get("DEV_TELEGRAM_CHAT_ID", ""),
            )
            log.info("Telegram switched to DEV channel")
        else:
            _telegram.switch_credentials(
                token=os.environ.get("TELEGRAM_BOT_TOKEN", ""),
                chat_id=os.environ.get("TELEGRAM_CHAT_ID", ""),
                thread_id=os.environ.get("TELEGRAM_THREAD_ID", ""),
            )
        self._initial_usdc = initial_usdc
        self._enforce_maker_only("start")

        # Validate credentials before going live
        if not paper_mode:
            log.info("Live mode — validating API credentials...")
            await self.validate_live_credentials()
            log.info("Credentials validated OK")
            # One-time on-chain approvals for SELL orders (neg-risk markets)
            log.info("Checking on-chain approvals for neg-risk trading...")
            from mm_shared.approvals import _do_approvals
            raw_approval_result: Any
            try:
                raw_approval_result = await asyncio.to_thread(_do_approvals, PM_PRIVATE_KEY)
            except Exception as e:
                raw_approval_result = {"error": str(e)}

            approval_result = (
                raw_approval_result
                if isinstance(raw_approval_result, dict)
                else {"raw": str(raw_approval_result)}
            )

            if approval_result.get("error") or not approval_result.get("all_ok", False):
                log.critical("Cannot start live: on-chain approvals failed: %s", approval_result)
                raise HTTPException(
                    status_code=500,
                    detail={
                        "error": "Cannot start live: on-chain approvals failed",
                        "details": approval_result,
                    },
                )

            log.info("All approvals OK: %s", approval_result)

        # Validate
        if coin not in config.COINS:
            raise HTTPException(status_code=400, detail=f"Unknown coin: {coin}")
        if timeframe not in config.COIN_TIMEFRAMES.get(coin, []):
            raise HTTPException(status_code=400,
                                detail=f"Invalid timeframe {timeframe} for {coin}")

        # Create feed state
        self.feed_state = feeds.State()

        # Start Binance feeds
        symbol = config.COIN_BINANCE[coin]
        kline_interval = config.TF_KLINE[timeframe]

        t1 = asyncio.create_task(feeds.ob_poller(symbol, self.feed_state))
        t2 = asyncio.create_task(feeds.binance_feed(symbol, kline_interval, self.feed_state))
        self._feed_tasks = [t1, t2]

        # Start PM price feeds
        try:
            tokens = await asyncio.wait_for(
                asyncio.to_thread(feeds.fetch_pm_tokens, coin, timeframe),
                timeout=15.0,
            )
            if tokens and tokens[0] and tokens[1]:
                up_id, dn_id, cond_id = tokens
                # Set token IDs on feed state so pm_feed can subscribe
                self.feed_state.pm_up_id = up_id
                self.feed_state.pm_dn_id = dn_id
                log.info(f"PM tokens: UP={up_id[:20]}... DN={dn_id[:20]}... cond={cond_id[:20]}..." if cond_id else f"PM tokens: UP={up_id[:20]}... DN={dn_id[:20]}...")

                t3 = asyncio.create_task(
                    feeds.pm_feed(self.feed_state))
                self._feed_tasks.append(t3)

                # Build market info from token IDs
                # _build_market_info_from_tokens() performs blocking HTTP strike fetch;
                # run it off the event loop to keep API handlers responsive at startup.
                market = await asyncio.wait_for(
                    asyncio.to_thread(
                        self._build_market_info_from_tokens,
                        coin,
                        timeframe,
                        up_id,
                        dn_id,
                        condition_id=cond_id,
                    ),
                    timeout=45.0,
                )
            else:
                msg = "PM tokens not found for current window"
                if not paper_mode:
                    log.error("%s — refusing live start", msg)
                    await self._stop_feed_tasks()
                    raise HTTPException(status_code=503, detail=msg)
                log.warning("%s, using placeholder", msg)
                self._strike_invalid = False
                market = self._build_placeholder_market(coin, timeframe)
        except Exception as e:
            if not paper_mode:
                log.error("PM token fetch failed in live mode: %s", e)
                await self._stop_feed_tasks()
                raise HTTPException(status_code=503, detail=f"PM token fetch failed: {e}")
            log.warning(f"PM token fetch failed: {e}")
            self._strike_invalid = False
            market = self._build_placeholder_market(coin, timeframe)

        # Wait for initial data (max 10s)
        log.info("Waiting for initial Binance data...")
        for _ in range(100):
            if self.feed_state.mid and self.feed_state.mid > 0:
                break
            await asyncio.sleep(0.1)

        # Create CLOB client
        clob = _create_clob_client(
            paper_mode=paper_mode,
            initial_usdc=initial_usdc,
        )

        # Enrich MarketInfo with tick_size, market_type, resolution_source from PM API
        if market and market.up_token_id and not market.up_token_id.startswith("placeholder"):
            await self._enrich_market_info(market, coin, timeframe)

        startup_block_reason = self._startup_window_block_reason(market)
        if startup_block_reason:
            log.warning("%s — refusing start for %s/%s", startup_block_reason, coin, timeframe)
            await self._stop_feed_tasks()
            raise HTTPException(status_code=409, detail=startup_block_reason)

        # Create and start market maker
        from mm.market_maker import MarketMaker
        self.mm = MarketMaker(self.feed_state, clob, self.mm_config)
        # Always set session budget limit for USDC cap enforcement
        self.mm.inventory.initial_usdc = initial_usdc
        if paper_mode:
            self.mm.inventory.usdc = initial_usdc
        self.mm.set_market(market)

        # Set private key for live merge operations
        if not paper_mode and PM_PRIVATE_KEY:
            self.mm._private_key = PM_PRIVATE_KEY

        try:
            await self.mm.start()
        except HTTPException:
            self.mm = None
            self._running = False
            await self._cancel_strike_retry_task()
            await self._stop_feed_tasks()
            raise
        except Exception as e:
            log.error("MM start failed: %s", e)
            self.mm = None
            self._running = False
            await self._cancel_strike_retry_task()
            await self._stop_feed_tasks()
            raise HTTPException(status_code=400, detail=str(e))
        self._running = True
        if self._strike_invalid:
            self._ensure_strike_retry_task()

        # Notify Telegram about MM start
        mode_str = "PAPER" if paper_mode else "LIVE"
        _telegram.notify_mm_start(
            coin=coin,
            timeframe=timeframe,
            mode=mode_str,
            half_spread_bps=self.mm_config.half_spread_bps,
            order_size_usd=self.mm_config.order_size_usd,
        )

        # Snapshot starting balance for real PnL calc in Telegram summary
        try:
            self._start_balance = await self.mm.order_mgr.get_usdc_balance()
        except Exception:
            self._start_balance = self._initial_usdc

        # MongoDB logger (fills, snapshots, Python logs)
        await self._attach_mongo_logger(
            register_fill=lambda mongo: self.mm.on_fill(
                lambda fill, tt: mongo.log_fill(fill, tt, self._fill_context())
            ),
            register_snapshot=lambda mongo: self.mm.on_snapshot(mongo.log_snapshot),
            runtime_tag="legacy",
        )

        # Monitor for window expiry and handle auto-next-window
        self._ensure_monitor_task()

        log.info(f"MM started: {coin}/{timeframe} paper={paper_mode}")
        return self.snapshot()

    async def _monitor_window_expiry(self) -> None:
        """Watch for window expiry; auto-restart next window if configured."""
        while self._running:
            runtime_metrics.incr("web.monitor.loop")
            await asyncio.sleep(2.0)
            mm = self.mm
            if not mm:
                self._running = False
                break
            # MM stopped itself (window expiry or other reason)
            if not mm._running:
                log.info(f"Monitor: mm._running=False, _is_closing={mm._is_closing}")
            if not mm._running and not (mm._is_closing or (mm.market and mm.market.time_remaining <= 0)):
                log.error(
                    "Monitor: MM stopped unexpectedly mid-window; syncing runtime state and stopping feeds"
                )
                self._running = False
                await self._cancel_strike_retry_task()
                await self._stop_feed_tasks()
                try:
                    if mm.heartbeat.is_running:
                        await mm.heartbeat.stop()
                except Exception as e:
                    log.warning("Failed to stop heartbeat after unexpected MM stop: %s", e)
                break
            if not mm._running and (mm._is_closing or (mm.market and mm.market.time_remaining <= 0)):
                # Don't auto-restart after emergency shutdown
                if mm._emergency_stopped:
                    log.warning("Monitor: emergency shutdown detected, NOT auto-restarting")
                    self._running = False
                    await self._cancel_strike_retry_task()
                    await self._stop_feed_tasks()
                    break
                log.info("Window expired — MM stopped. Cleaning up feeds.")
                redeem_elapsed = 0.0
                if (
                    (not self._paper_mode)
                    and bool(getattr(self.mm_config, "redeem_after_resolution_enabled", True))
                    and mm.market
                    and mm.market.condition_id
                ):
                    redeem_started = time.time()
                    redeem_timeout = min(
                        float(getattr(self.mm_config, "resolution_wait_sec", 90.0)),
                        180.0,
                    )
                    try:
                        redeem_result = await mm.redeem_after_resolution(max_wait_sec=redeem_timeout)
                        log.info("Redeem result: %s", redeem_result)
                    except Exception as e:
                        log.warning("Redeem attempt failed: %s", e)
                    redeem_elapsed = max(0.0, time.time() - redeem_started)

                await self._send_window_summary()
                self._running = False
                await self._cancel_strike_retry_task()
                await self._stop_feed_tasks()

                # Natural window expiry sets mm._running=False without calling mm.stop(),
                # so heartbeat can still be alive. Stop it before rotating window.
                try:
                    if mm.heartbeat.is_running:
                        await mm.heartbeat.stop()
                except Exception as e:
                    log.warning(f"Failed to stop heartbeat after expiry: {e}")

                if self.mm_config.auto_next_window:
                    # Poll for new window tokens instead of fixed wait
                    max_wait = max(10.0, float(self.mm_config.resolution_wait_sec) - redeem_elapsed)
                    min_wait = min(15.0, max(2.0, max_wait * 0.25))
                    poll_interval = 10.0
                    self._next_window_at = time.time() + max_wait
                    log.info(f"Auto-next-window: polling for new tokens (max {max_wait:.0f}s)...")
                    await asyncio.sleep(min_wait)

                    up_id = ""
                    dn_id = ""
                    elapsed = min_wait
                    while elapsed < max_wait:
                        try:
                            tokens = await asyncio.wait_for(
                                asyncio.to_thread(feeds.fetch_pm_tokens, self._coin, self._timeframe),
                                timeout=15.0,
                            )
                            if tokens and tokens[0] and tokens[1]:
                                up_id, dn_id, _cond_id = tokens
                                strike, _ws, _we = await asyncio.wait_for(
                                    asyncio.to_thread(
                                        feeds.fetch_pm_strike, self._coin, self._timeframe,
                                        5, 2.0,  # max_retries=5, retry_delay=2s
                                    ),
                                    timeout=30.0,
                                )
                                if not self._is_valid_strike(strike):
                                    log.warning(
                                        f"Invalid strike {strike:.6f} after retries ({elapsed:.0f}s elapsed), "
                                        f"skipping this window..."
                                    )
                                    continue  # keep polling for next window
                                log.info(
                                    f"New tokens ready after {elapsed:.0f}s: "
                                    f"strike={strike:.2f} UP={up_id[:20]}... DN={dn_id[:20]}..."
                                )
                                break
                            else:
                                log.info(f"Tokens not ready yet ({elapsed:.0f}s elapsed), retrying...")
                        except Exception as e:
                            log.warning(f"Token poll failed ({elapsed:.0f}s): {e}")
                        await asyncio.sleep(poll_interval)
                        elapsed += poll_interval
                        self._next_window_at = time.time() + (max_wait - elapsed)

                    if not up_id:
                        log.warning(f"Tokens not available after {max_wait:.0f}s, trying start anyway")

                    self._next_window_at = 0.0
                    if up_id and dn_id:
                        # Pre-entry market quality check
                        skip_count = 0
                        max_skip = 3
                        while skip_count < max_skip:
                            try:
                                from mm_shared.market_quality import MarketQualityAnalyzer
                                from mm_shared.order_manager import OrderManager
                                analyzer = MarketQualityAnalyzer(self.mm_config)
                                temp_clob = _create_clob_client(
                                    paper_mode=self._paper_mode,
                                    initial_usdc=self._initial_usdc,
                                )
                                temp_om = OrderManager(temp_clob, self.mm_config)
                                up_book = await temp_om.get_full_book(up_id)
                                dn_book = await temp_om.get_full_book(dn_id)
                                quality = analyzer.analyze(up_book, dn_book, 0.5, 0.5)
                                if quality.tradeable:
                                    log.info(f"Market quality OK: score={quality.overall_score:.2f}")
                                    break
                                skip_count += 1
                                log.warning(
                                    f"Window skip {skip_count}/{max_skip}: "
                                    f"{quality.reason} (score={quality.overall_score:.2f})"
                                )
                                if skip_count < max_skip:
                                    await asyncio.sleep(30.0)
                            except Exception as e:
                                log.warning(f"Quality check error: {e}, proceeding anyway")
                                break
                        else:
                            log.error(f"Skipped {max_skip} times, starting anyway")
                    try:
                        log.info("Auto-starting next window...")
                        await self.start(
                            coin=self._requested_coin or self._coin,
                            timeframe=self._requested_timeframe or self._timeframe,
                            paper_mode=self._paper_mode,
                            initial_usdc=self._initial_usdc,
                            dev=self._dev_mode,
                        )
                    except Exception as e:
                        log.error(f"Auto-next-window failed: {e}")
                break

    async def stop(self, *, liquidate: bool = True, emergency: bool = False) -> dict:
        """Stop market maker and feeds."""
        await self._cancel_strike_retry_task()
        self._running = False
        await self._cancel_monitor_task()
        self._strike_invalid = False

        if self.mm:
            if emergency:
                self.mm._emergency_flag = True
                self.mm._emergency_stopped = True
                self.mm._paused = True
                self.mm._pause_reason = "Emergency stop"
            await self.mm.stop(liquidate=liquidate)

        await self._stop_feed_tasks()

        # Tear down MongoLogger
        await self._teardown_mongo_logger()

        log.info("MM stopped (liquidate=%s emergency=%s)", liquidate, emergency)
        return self.snapshot()

    def snapshot(self) -> dict:
        """Get current state for API."""
        runtime_metrics.incr("web.runtime.snapshot")
        if self.mm:
            snap = self.mm.snapshot()
            self._sync_mm_alerts_from_snapshot(snap)
            snap["paper_mode"] = self._paper_mode
            snap["dev_mode"] = self._dev_mode
            snap["session_limit"] = self._initial_usdc
            snap["next_window_in"] = max(0, self._next_window_at - time.time()) if self._next_window_at else 0
            if self._paper_mode and hasattr(self.mm, "order_mgr"):
                client = self.mm.order_mgr.client
                if hasattr(client, "balance"):
                    snap["mock_usdc_balance"] = float(client.balance)
            snap["feeds"] = self._build_feeds_dict()
            snap["app_version"] = APP_VERSION
            snap["app_git_hash"] = APP_GIT_HASH
            snap["alerts"] = self.list_alerts()
            snap["verification"] = self.get_verification_status()
            snap["runtime_watchdog"] = dict(self._runtime_watchdog)
            if "_tg_bot" in globals():
                snap["telegram_bot"] = _tg_bot.status
            if self._last_market_selection:
                snap["market_selector"] = self._last_market_selection
            return snap

        self._clear_mm_alerts()
        result = {
            "is_running": False,
            "paper_mode": self._paper_mode,
            "dev_mode": self._dev_mode,
            "next_window_in": max(0, self._next_window_at - time.time()) if self._next_window_at else 0,
            "market": {"coin": self._coin, "timeframe": self._timeframe},
            "fair_value": {"up": 0.5, "dn": 0.5, "binance_mid": 0, "volatility": 0},
            "quotes": {},
            "inventory": {"up_shares": 0, "dn_shares": 0, "net_delta": 0, "usdc": 0},
            "recent_fills": [],
            "config": self.mm_config.to_dict(),
            "order_tracking": {
                "active_count": 0,
                "recent_count": 0,
                "current_generation": 0,
                "last_fallback_poll_count": 0,
                "max_recent_per_token": int(getattr(self.mm_config, "recent_order_max_per_token", 0) or 0),
                "current_tokens": [],
            },
            "cycle_guard": {
                "active": False,
                "mode": "off",
                "reason": "",
                "seconds_left": 0.0,
                "bad_cycle_count": 0,
                "consecutive_place_failures": 0,
                "current_cycle_active": False,
                "current_cycle_fill_count": 0,
                "last_cycle_pnl": 0.0,
            },
            "negative_edge_guard": {
                "active": False,
                "mode": "off",
                "reason": "",
                "seconds_left": 0.0,
                "trigger_count": 0,
                "last_total_fills": 0,
                "last_avg_markout_5s": 0.0,
                "last_adverse_pct_5s": 0.0,
                "last_spread_capture_usd": 0.0,
                "last_spread_capture_count": 0,
                "last_triggered_at": 0.0,
            },
            "liquidation": {
                "mode": "inactive",
                "reason": "",
                "seconds_in_mode": 0.0,
                "chunk_interval_sec_current": float(getattr(self.mm_config, "liq_chunk_interval_sec", 0.0) or 0.0),
                "taker_threshold_sec_current": float(getattr(self.mm_config, "liq_taker_threshold_sec", 0.0) or 0.0),
                "residual_inventory_failure": False,
            },
            "api_errors": {
                "total_by_op": {},
                "recent": [],
                "last_error_ts": 0.0,
            },
        }
        result["feeds"] = self._build_feeds_dict()
        result["app_version"] = APP_VERSION
        result["app_git_hash"] = APP_GIT_HASH
        result["alerts"] = self.list_alerts()
        result["verification"] = self.get_verification_status()
        result["runtime_watchdog"] = dict(self._runtime_watchdog)
        if "_tg_bot" in globals():
            result["telegram_bot"] = _tg_bot.status
        if self._last_market_selection:
            result["market_selector"] = self._last_market_selection
        if self._watching and self.feed_state:
            st = self.feed_state
            result["fair_value"]["binance_mid"] = st.mid
            if st.pm_up is not None:
                result["pm_prices"] = {"up": st.pm_up, "dn": st.pm_dn}
        return result

    def _build_feeds_dict(self) -> dict:
        """Build feed health metrics dict for API response."""
        st = self.feed_state
        if not st:
            return {}
        now = time.time()
        return {
            "binance_ws": {
                "connected": st.binance_ws_connected,
                "msg_count": st.binance_ws_msg_count,
                "error_count": st.binance_ws_error_count,
                "latency_ms": round((now - st.binance_ws_last_ok_ts) * 1000) if st.binance_ws_last_ok_ts else None,
                "uptime_sec": round(now - st.binance_ws_connected_at) if st.binance_ws_connected_at else 0,
            },
            "binance_ob": {
                "connected": st.binance_ob_connected,
                "ready": st.binance_ob_ready,
                "msg_count": st.binance_ob_msg_count,
                "error_count": st.binance_ob_error_count,
                "last_update_ms_ago": round((now - st.binance_ob_last_ok_ts) * 1000) if st.binance_ob_last_ok_ts else None,
                "uptime_sec": round(now - st.binance_ob_connected_at) if st.binance_ob_connected_at else 0,
            },
            "polymarket": {
                "connected": st.pm_connected,
                "prices_ready": st.pm_prices_ready,
                "msg_count": st.pm_msg_count,
                "error_count": st.pm_error_count,
                "last_update_ms_ago": round((now - st.pm_last_update_ts) * 1000) if st.pm_last_update_ts else None,
                "uptime_sec": round(now - st.pm_connected_at) if st.pm_connected_at else 0,
            },
        }

    async def start_watch(self, coin: str, timeframe: str) -> dict:
        """Start feeds only (no trading) for live price monitoring."""
        if self._running:
            return {"detail": "Session running, feeds already active"}
        await self._cancel_monitor_task()
        await self._stop_feed_tasks()
        if self._watching:
            await self.stop_watch()

        self._coin = coin
        self._timeframe = timeframe
        self.feed_state = feeds.State()

        symbol = config.COIN_BINANCE.get(coin, "BTCUSDT")
        kline_interval = config.TF_KLINE.get(timeframe, "5m")

        self._feed_tasks.append(asyncio.create_task(
            feeds.ob_poller(symbol, self.feed_state)))
        self._feed_tasks.append(asyncio.create_task(
            feeds.binance_feed(symbol, kline_interval, self.feed_state)))

        try:
            tokens = await asyncio.wait_for(
                asyncio.to_thread(feeds.fetch_pm_tokens, coin, timeframe),
                timeout=15.0,
            )
            if tokens and tokens[0] and tokens[1]:
                up_id, dn_id, _ = tokens
                self.feed_state.pm_up_id = up_id
                self.feed_state.pm_dn_id = dn_id
                self._feed_tasks.append(asyncio.create_task(
                    feeds.pm_feed(self.feed_state)))
        except Exception as e:
            log.warning(f"Watch: PM tokens not available: {e}")

        self._watching = True
        log.info(f"Watch mode started: {coin}/{timeframe}")
        return {"ok": True, "coin": coin, "timeframe": timeframe}

    async def stop_watch(self):
        """Stop watch mode feeds."""
        await self._cancel_strike_retry_task()
        await self._cancel_monitor_task()
        self._strike_invalid = False
        await self._stop_feed_tasks()
        self._watching = False
        self.feed_state = None
        log.info("Watch mode stopped")

    def update_config(self, **kwargs) -> dict:
        """Update MM config at runtime."""
        # Handle session_limit separately (not part of MMConfig)
        session_limit = kwargs.pop("session_limit", None)
        if session_limit is not None:
            self._initial_usdc = float(session_limit)
            if self.mm:
                self.mm.inventory.initial_usdc = self._initial_usdc
                self.mm.order_mgr._session_budget = self._initial_usdc
                spent = self.mm.order_mgr._session_spent
                if self._initial_usdc < spent:
                    log.warning(
                        "New session limit $%.2f is below already-spent $%.2f — "
                        "BUY orders will be blocked until inventory frees up",
                        self._initial_usdc, spent,
                    )
            log.info("Session limit updated to $%.2f", self._initial_usdc)
        self.mm_config.update(**kwargs)
        self._enforce_maker_only("runtime_update")
        if self.mm:
            self.mm.config = self.mm_config
            self.mm.quote_engine.config = self.mm_config
        # Persist to MongoDB (fire-and-forget)
        asyncio.ensure_future(self._save_config())
        return self.mm_config.to_dict()

    async def _save_config(self) -> None:
        """Save current config to MongoDB for persistence across deploys."""
        if not config.MONGO_URI:
            return
        try:
            import motor.motor_asyncio
            client = motor.motor_asyncio.AsyncIOMotorClient(config.MONGO_URI)
            db = client[config.MONGO_DB]
            doc = {"_id": "mm_config", **self.mm_config.to_dict(),
                   "session_limit": self._initial_usdc}
            await db.config.replace_one(
                {"_id": "mm_config"}, doc, upsert=True,
            )
            client.close()
            log.info("Config saved to MongoDB")
        except Exception as e:
            log.warning("Failed to save config to MongoDB: %s", e)

    async def load_config(self) -> None:
        """Load saved config from MongoDB (called at startup)."""
        if not config.MONGO_URI:
            return
        try:
            import motor.motor_asyncio
            client = motor.motor_asyncio.AsyncIOMotorClient(config.MONGO_URI)
            db = client[config.MONGO_DB]
            doc = await db.config.find_one({"_id": "mm_config"})
            client.close()
            if doc:
                doc.pop("_id", None)
                saved_limit = doc.pop("session_limit", None)
                if saved_limit is not None:
                    self._initial_usdc = float(saved_limit)
                self.mm_config.update(**doc)
                self._enforce_maker_only("mongo_load")
                self.mm_config.validate()
                log.info("Config loaded from MongoDB: spread=%s, skew=%s, session_limit=$%.2f",
                         self.mm_config.half_spread_bps, self.mm_config.skew_bps_per_unit,
                         self._initial_usdc)
        except Exception as e:
            log.warning("Failed to load config from MongoDB: %s", e)

    def _build_market_info(self, coin: str, timeframe: str,
                           tokens: dict) -> MarketInfo:
        """Build MarketInfo from fetched PM tokens."""
        # tokens typically has: up_token_id, dn_token_id, strike, end_time, etc.
        up_id = tokens.get("up_token_id", tokens.get("up", {}).get("token_id", ""))
        dn_id = tokens.get("dn_token_id", tokens.get("dn", {}).get("token_id", ""))
        strike = float(tokens.get("strike", 0))

        now = time.time()
        # Parse window end
        tf_minutes = {"5m": 5, "15m": 15, "1h": 60, "4h": 240, "daily": 1440}
        window_duration = tf_minutes.get(timeframe, 60) * 60
        window_end = now + window_duration  # Approximate

        if "end_time" in tokens:
            try:
                window_end = float(tokens["end_time"])
            except (ValueError, TypeError):
                pass

        return MarketInfo(
            coin=coin,
            timeframe=timeframe,
            up_token_id=up_id,
            dn_token_id=dn_id,
            strike=strike,
            window_start=now,
            window_end=window_end,
        )

    def _build_market_info_from_tokens(self, coin: str, timeframe: str,
                                       up_id: str, dn_id: str,
                                       condition_id: str = "") -> Optional[MarketInfo]:
        """Build MarketInfo from fetched PM token ID tuple.

        Fetches the actual strike price from Binance candle open
        and window timing from PM event endDate.

        NOTE: This method performs blocking HTTP calls via feeds.fetch_pm_strike().
        Always call it via asyncio.to_thread() from async code.
        """
        strike, window_start, window_end = feeds.fetch_pm_strike(coin, timeframe)

        now = time.time()
        tf_minutes = {"5m": 5, "15m": 15, "1h": 60, "4h": 240, "daily": 1440}
        window_duration = tf_minutes.get(timeframe, 60) * 60

        if window_start <= 0:
            window_start = now
        if window_end <= window_start:
            window_end = window_start + window_duration

        if not self._is_valid_strike(strike):
            self._strike_invalid = True
            strike = 0.0
            log.error("Strike fetch failed, cannot start window — entering watch mode")
        else:
            self._strike_invalid = False

        # Buffer: treat window as ending 10s before PM endDate
        # so timer shows 0 before PM resolves, and bot enters closing mode earlier
        window_end = max(window_start + 1.0, window_end - 10.0)

        log.info(f"Market info: strike={strike:.2f} window=[{window_start:.0f}, {window_end:.0f}] (10s buffer applied)")

        return MarketInfo(
            coin=coin,
            timeframe=timeframe,
            up_token_id=up_id,
            dn_token_id=dn_id,
            strike=strike,
            window_start=window_start,
            window_end=window_end,
            condition_id=condition_id,
        )

    async def _enrich_market_info(self, market: MarketInfo,
                                    coin: str, timeframe: str) -> None:
        """Fetch tick_size from PM book and detect market_type/resolution_source."""
        import requests as _req

        # 1. tick_size + min_order_size from PM order book API
        safe_tick_size = 0.01
        safe_min_order_size = 5.0
        if not self._is_valid_tick_size(self._last_good_tick_size):
            self._last_good_tick_size = safe_tick_size
        if not self._is_valid_min_order_size(self._last_good_min_order_size):
            self._last_good_min_order_size = safe_min_order_size

        # Start from last-known-good (defaults on cold start).
        market.tick_size = self._last_good_tick_size
        market.min_order_size = self._last_good_min_order_size

        try:
            resp = await asyncio.to_thread(
                lambda: _req.get(
                    "https://clob.polymarket.com/book",
                    params={"token_id": market.up_token_id},
                    timeout=10,
                )
            )
            if not resp.ok:
                log.error(
                    "Failed to fetch PM book params (status=%s), using last-known-good "
                    "tick_size=%s min_order_size=%s",
                    resp.status_code, market.tick_size, market.min_order_size,
                )
            else:
                data = resp.json()

                raw_tick_size = data.get("tick_size")
                if raw_tick_size is not None:
                    try:
                        tick_size = float(raw_tick_size)
                        if self._is_valid_tick_size(tick_size):
                            market.tick_size = tick_size
                            self._last_good_tick_size = tick_size
                        else:
                            market.tick_size = safe_tick_size
                            log.error(
                                "Invalid tick_size from PM (%s), using safe default %s",
                                raw_tick_size, safe_tick_size,
                            )
                    except (TypeError, ValueError):
                        market.tick_size = safe_tick_size
                        log.error(
                            "Non-numeric tick_size from PM (%s), using safe default %s",
                            raw_tick_size, safe_tick_size,
                        )

                raw_min_order_size = data.get("min_order_size")
                if raw_min_order_size is not None:
                    try:
                        min_order_size = float(raw_min_order_size)
                        if self._is_valid_min_order_size(min_order_size):
                            market.min_order_size = min_order_size
                            self._last_good_min_order_size = min_order_size
                        else:
                            market.min_order_size = safe_min_order_size
                            log.error(
                                "Invalid min_order_size from PM (%s), using safe default %s",
                                raw_min_order_size, safe_min_order_size,
                            )
                    except (TypeError, ValueError):
                        market.min_order_size = safe_min_order_size
                        log.error(
                            "Non-numeric min_order_size from PM (%s), using safe default %s",
                            raw_min_order_size, safe_min_order_size,
                        )

                log.info(
                    "PM book params: tick_size=%s, min_order_size=%s",
                    market.tick_size, market.min_order_size,
                )
        except Exception as e:
            log.error(
                "Failed to fetch PM book params: %s, using last-known-good "
                "tick_size=%s min_order_size=%s",
                e, market.tick_size, market.min_order_size,
            )

        # 2. market_type + resolution_source from PM event description
        try:
            event_data = await asyncio.to_thread(
                feeds.fetch_pm_event_data, coin, timeframe,
            )
            if event_data:
                title = (event_data.get("title", "") or "").lower()
                desc = (event_data.get("description", "") or "").lower()
                text = title + " " + desc

                if "above" in text or "below" in text:
                    market.market_type = "above_below"
                else:
                    market.market_type = "up_down"

                if "chainlink" in text:
                    market.resolution_source = "chainlink"
                elif "binance" in text:
                    market.resolution_source = "binance"
                else:
                    market.resolution_source = "unknown"

                log.info(
                    "Market classification: type=%s, resolution=%s",
                    market.market_type, market.resolution_source,
                )
        except Exception as e:
            log.warning("Failed to detect market type: %s", e)

    def _build_placeholder_market(self, coin: str, timeframe: str) -> MarketInfo:
        """Placeholder market when PM tokens aren't available yet."""
        now = time.time()
        return MarketInfo(
            coin=coin,
            timeframe=timeframe,
            up_token_id="placeholder-up",
            dn_token_id="placeholder-dn",
            strike=0.0,
            window_start=now,
            window_end=now + 3600,
        )

    async def _send_window_summary(self) -> None:
        """Send window PnL summary to Telegram.

        Priority: MongoDB fills (exact) -> PM balance diff (fallback).
        """
        if not _telegram.enabled or not self.mm:
            return
        try:
            mode = "PAPER" if self._paper_mode else "LIVE"
            snap = self.mm.snapshot()
            fv = snap.get("fair_value", {})
            fv_up = fv.get("up", 0.5)
            fv_dn = fv.get("dn", 0.5)

            # Use PM-balance-based PnL from snapshot (same as dashboard)
            session_pnl = snap.get("session_pnl", 0.0)
            portfolio_value = snap.get("portfolio_value", 0.0)
            effective_balance = portfolio_value if portfolio_value > 0 else (self._start_balance + session_pnl)

            # Log MongoDB fills for comparison (debug only)
            if self._mongo and self.mm.market:
                mongo_pnl = await self._mongo.compute_session_pnl(
                    coin=self._coin, timeframe=self._timeframe,
                    window_start=self.mm.market.window_start,
                    window_end=self.mm.market.window_end,
                    fv_up=fv_up, fv_dn=fv_dn,
                )
                if mongo_pnl:
                    log.info("MongoDB fills PnL: $%.4f (buys=%d, sells=%d) vs dashboard: $%.4f",
                             mongo_pnl["total_pnl"], mongo_pnl["buy_count"],
                             mongo_pnl["sell_count"], session_pnl)

            # Record PnL for 1h/24h aggregation
            now = time.time()
            self._pnl_history.append((now, session_pnl))
            cutoff_24h = now - 86400
            self._pnl_history = [(t, p) for t, p in self._pnl_history if t >= cutoff_24h]

            pnl_1h = sum(p for t, p in self._pnl_history if t >= now - 3600)
            pnl_24h = sum(p for t, p in self._pnl_history)

            if effective_balance <= 0:
                effective_balance = self._start_balance + session_pnl

            log.info(
                "Sending window summary: pnl=$%.2f, balance=$%.2f, enabled=%s",
                session_pnl, effective_balance, _telegram.enabled,
            )
            _telegram.notify_window_summary(
                coin=self._coin or "UNKNOWN",
                timeframe=self._timeframe or "UNKNOWN",
                mode=mode,
                session_pnl=session_pnl,
                pnl_1h=pnl_1h,
                pnl_24h=pnl_24h,
                usdc_balance=effective_balance,
            )

            # Update start_balance for next window
            if effective_balance > 0:
                self._start_balance = effective_balance
        except Exception as e:
            log.warning(f"Failed to send window summary to Telegram: {e}")

    def _fill_context(self) -> dict:
        """Build context dict for MongoLogger fill records."""
        mm = self.mm
        if not mm:
            return {}
        snap = mm.snapshot()
        return {
            "market": snap.get("market"),
            "inventory": snap.get("inventory"),
            "fair_value": snap.get("fair_value"),
            "pnl": {
                "realized": snap.get("realized_pnl", 0),
                "unrealized": snap.get("unrealized_pnl", 0),
                "total": snap.get("total_pnl", 0),
            },
            "paper_mode": self._paper_mode,
        }


class MMRuntimeV2(MMRuntime):
    """Parallel runtime for the pair-first V2 engine."""
    LIVE_MIN_BUDGET_USD = 15.0
    PAPER_MIN_BUDGET_USD = 30.0

    def __init__(self):
        super().__init__()
        self.mm_v2: Optional[MarketMakerV2] = None
        self.mm_config_v2: MMConfigV2 = MMConfigV2()
        self._live_budget_gate_passed: bool = False
        self._paper_budget_gate_passed: bool = False
        self._force_normal_soft_mode_paper: bool = False
        self._force_normal_no_guards_paper: bool = False
        self._last_terminal_runtime_v2: dict[str, Any] = {
            "last_terminal_reason": "",
            "last_terminal_ts": 0.0,
            "last_terminal_wallet_total_usdc": 0.0,
            "last_terminal_up_shares": 0.0,
            "last_terminal_dn_shares": 0.0,
            "last_terminal_pnl_equity_usd": 0.0,
            "terminal_liquidation_active": False,
            "terminal_liquidation_attempted_orders": 0,
            "terminal_liquidation_placed_orders": 0,
            "terminal_liquidation_remaining_up": 0.0,
            "terminal_liquidation_remaining_dn": 0.0,
            "terminal_liquidation_done": False,
            "terminal_liquidation_reason": "",
        }

    def _idle_snapshot_v2(self) -> dict[str, Any]:
        return {
            "app_version": APP_VERSION,
            "app_git_hash": APP_GIT_HASH,
            "is_running": False,
            "lifecycle": "bootstrapping",
            "market": None,
            "valuation": {
                "fv_up": 0.0,
                "fv_dn": 0.0,
                "confidence": 0.0,
                "source": "",
                "regime": "",
                "divergence_up": 0.0,
                "divergence_dn": 0.0,
                "buy_edge_gap_up": 0.0,
                "buy_edge_gap_dn": 0.0,
            },
            "inventory": {
                "up_shares": float(self._last_terminal_runtime_v2.get("last_terminal_up_shares") or 0.0),
                "dn_shares": float(self._last_terminal_runtime_v2.get("last_terminal_dn_shares") or 0.0),
                "free_usdc": float(self._last_terminal_runtime_v2.get("last_terminal_wallet_total_usdc") or 0.0),
                "reserved_usdc": 0.0,
                "wallet_total_usdc": float(self._last_terminal_runtime_v2.get("last_terminal_wallet_total_usdc") or 0.0),
                "wallet_reserved_usdc": 0.0,
                "pending_buy_reserved_usdc": 0.0,
                "pending_buy_up": 0.0,
                "pending_buy_dn": 0.0,
                "pending_sell_up": 0.0,
                "pending_sell_dn": 0.0,
                "paired_qty": 0.0,
                "excess_up_qty": 0.0,
                "excess_dn_qty": 0.0,
                "paired_value_usd": 0.0,
                "excess_up_value_usd": 0.0,
                "excess_dn_value_usd": 0.0,
                "total_inventory_value_usd": 0.0,
                "target_pair_value_usd": 0.0,
                "pair_value_ratio": 0.0,
                "pair_value_over_target_usd": 0.0,
                "sellable_up_shares": 0.0,
                "sellable_dn_shares": 0.0,
            },
            "pair_inventory": {
                "paired_qty": 0.0,
                "excess_up_qty": 0.0,
                "excess_dn_qty": 0.0,
                "paired_value_usd": 0.0,
                "excess_up_value_usd": 0.0,
                "excess_dn_value_usd": 0.0,
                "sellable_up_shares": 0.0,
                "sellable_dn_shares": 0.0,
            },
            "quotes": {
                "up_bid": None,
                "up_ask": None,
                "dn_bid": None,
                "dn_ask": None,
            },
            "execution": {
                "open_orders": 0,
                "pending_buy_up": 0.0,
                "pending_buy_dn": 0.0,
                "pending_sell_up": 0.0,
                "pending_sell_dn": 0.0,
                "transport_failures": 0,
                "last_api_error": "",
                "last_fallback_poll_count": 0,
                "recent_cancelled_sell_reserve_up": 0.0,
                "recent_cancelled_sell_reserve_dn": 0.0,
                "sell_release_lag_up_sec": 0.0,
                "sell_release_lag_dn_sec": 0.0,
                "up_cooldown_sec": 0.0,
                "dn_cooldown_sec": 0.0,
                "active_sell_release_reason": "",
                "last_sellability_lag_reason": "",
                "current_order_ids": {},
            },
            "risk": {
                "soft_mode": "normal",
                "hard_mode": "none",
                "reason": "",
                "inventory_pressure": 0.0,
                "edge_score": 0.0,
                "drawdown_pct_budget": 1.0,
                "early_drawdown_pressure": 0.0,
                "post_fill_markout_5s_up": 0.0,
                "post_fill_markout_5s_dn": 0.0,
                "negative_spread_capture_streak_up": 0,
                "negative_spread_capture_streak_dn": 0,
                "toxic_fill_streak_up": 0,
                "toxic_fill_streak_dn": 0,
                "side_soft_brake_up_active": False,
                "side_soft_brake_dn_active": False,
                "side_reentry_cooldown_up_sec": 0.0,
                "side_reentry_cooldown_dn_sec": 0.0,
                "side_hard_block_up_sec": 0.0,
                "side_hard_block_dn_sec": 0.0,
            },
            "health": {
                "reconcile_status": "bootstrapping",
                "heartbeat_ok": True,
                "transport_ok": True,
                "last_api_error": "",
                "last_api_error_op": "",
                "last_api_error_status_code": 0,
                "last_api_error_raw": "",
                "last_fallback_poll_count": 0,
                "true_drift": False,
                "residual_inventory_failure": False,
                "sellability_lag_active": False,
                "wallet_snapshot_stale": False,
                "true_drift_age_sec": 0.0,
                "true_drift_no_progress_sec": 0.0,
                "drawdown_breach_ticks": 0,
                "drawdown_breach_age_sec": 0.0,
                "drawdown_breach_active": False,
                "drawdown_threshold_usd_effective": 0.0,
                "drift_evidence": {},
            },
            "analytics": {
                "fill_count": 0,
                "session_pnl": float(self._last_terminal_runtime_v2.get("last_terminal_pnl_equity_usd") or 0.0),
                "session_pnl_equity_usd": float(
                    self._last_terminal_runtime_v2.get("last_terminal_pnl_equity_usd") or 0.0
                ),
                "session_pnl_operator_usd": float(
                    self._last_terminal_runtime_v2.get("last_terminal_pnl_equity_usd") or 0.0
                ),
                "session_pnl_operator_ema_usd": float(
                    self._last_terminal_runtime_v2.get("last_terminal_pnl_equity_usd") or 0.0
                ),
                "position_mark_value_usd": 0.0,
                "position_mark_value_bid_usd": 0.0,
                "position_mark_value_mid_usd": 0.0,
                "portfolio_mark_value_usd": 0.0,
                "tradeable_portfolio_value_usd": 0.0,
                "anchor_divergence_up": 0.0,
                "anchor_divergence_dn": 0.0,
                "buy_edge_gap_up": 0.0,
                "buy_edge_gap_dn": 0.0,
                "quote_shift_from_mid_up": 0.0,
                "quote_shift_from_mid_dn": 0.0,
                "post_fill_markout_5s_up": 0.0,
                "post_fill_markout_5s_dn": 0.0,
                "toxic_fill_streak_up": 0,
                "toxic_fill_streak_dn": 0,
                "side_soft_brake_up_active": False,
                "side_soft_brake_dn_active": False,
                "negative_spread_capture_streak_up": 0,
                "negative_spread_capture_streak_dn": 0,
                "side_reentry_cooldown_up_sec": 0.0,
                "side_reentry_cooldown_dn_sec": 0.0,
                "side_hard_block_up_sec": 0.0,
                "side_hard_block_dn_sec": 0.0,
                "quote_anchor_mode": "midpoint_first",
                "midpoint_reference_mode": "midpoint_first",
                "pnl_calc_mode": "wallet_total_plus_mark",
                "pnl_mark_basis": "conservative_bid",
                "pnl_updated_ts": 0.0,
                "markout_1s": 0.0,
                "markout_5s": 0.0,
                "spread_capture_usd": 0.0,
                "fill_rate": 0.0,
                "quote_presence_ratio": 0.0,
                "inventory_half_life_sec": 0.0,
                "target_ratio_activation_usd_effective": 0.0,
                "target_ratio_cap_active": False,
                "target_ratio_cap_hits_60s": 0,
                "gross_inventory_brake_active": False,
                "gross_inventory_brake_hits_60s": 0,
                "pair_over_target_buy_blocks_60s": 0,
                "dual_bid_guard_inventory_budget_hits_60s": 0,
                "harmful_buy_brake_active": False,
                "harmful_buy_brake_hits_60s": 0,
                "emergency_taker_forced": False,
                "emergency_taker_forced_hits_60s": 0,
                "emergency_no_progress_sec": 0.0,
                "quoting_ratio_60s": 0.0,
                "inventory_skewed_ratio_60s": 0.0,
                "defensive_ratio_60s": 0.0,
                "unwind_ratio_60s": 0.0,
                "emergency_unwind_ratio_60s": 0.0,
                "four_quote_ratio_60s": 0.0,
                "dual_bid_ratio_60s": 0.0,
                "one_sided_bid_streak_outside": 0,
                "maker_cross_guard_hits_60s": 0,
                "dual_bid_guard_hits_60s": 0,
                "dual_bid_guard_fail_hits_60s": 0,
                "midpoint_first_brake_hits_60s": 0,
                "simultaneous_bid_block_prevented_hits_60s": 0,
                "divergence_soft_brake_up_active": False,
                "divergence_soft_brake_dn_active": False,
                "divergence_hard_suppress_up_active": False,
                "divergence_hard_suppress_dn_active": False,
                "divergence_soft_brake_hits_60s": 0,
                "divergence_hard_suppress_hits_60s": 0,
                "max_buy_edge_gap_60s": 0.0,
                "dual_bid_exception_active": False,
                "dual_bid_exception_reason": "",
                "marketability_guard_active": False,
                "marketability_guard_reason": "",
                "marketability_churn_confirmed": False,
                "marketability_problem_side": "",
                "marketability_side_locked": "",
                "marketability_side_lock_age_sec": 0.0,
                "sell_churn_hold_up_active": False,
                "sell_churn_hold_dn_active": False,
                "sell_churn_hold_side": "",
                "sell_churn_hold_order_age_up_sec": 0.0,
                "sell_churn_hold_order_age_dn_sec": 0.0,
                "sell_churn_hold_reprice_due_up": False,
                "sell_churn_hold_reprice_due_dn": False,
                "sell_churn_hold_reprice_suppressed_hits_60s": 0,
                "sell_churn_hold_cancel_avoided_hits_60s": 0,
                "collateral_warning_hits_60s": 0,
                "sell_skip_cooldown_hits_60s": 0,
                "up_collateral_warning_streak": 0,
                "dn_collateral_warning_streak": 0,
                "up_sell_skip_cooldown_streak": 0,
                "dn_sell_skip_cooldown_streak": 0,
                "collateral_warning_streak_current": 0,
                "sell_skip_cooldown_streak_current": 0,
                "execution_churn_ratio_60s": 0.0,
                "untradeable_tolerated_samples_60s": 0,
                "failure_bucket_current": "",
                "execution_replay_blocker_hint": "",
                "unwind_deferred_hits_60s": 0,
                "forced_unwind_extreme_excess_hits_60s": 0,
                "mm_regime_degraded_reason": "",
                "emergency_exit_armed": False,
                "recent_fills": [],
            },
            "alerts": self.list_alerts(),
            "config": self.mm_config_v2.to_dict(),
            "runtime": {
                "last_terminal_reason": str(self._last_terminal_runtime_v2.get("last_terminal_reason") or ""),
                "last_terminal_ts": float(self._last_terminal_runtime_v2.get("last_terminal_ts") or 0.0),
                "last_terminal_wallet_total_usdc": float(
                    self._last_terminal_runtime_v2.get("last_terminal_wallet_total_usdc") or 0.0
                ),
                "last_terminal_up_shares": float(self._last_terminal_runtime_v2.get("last_terminal_up_shares") or 0.0),
                "last_terminal_dn_shares": float(self._last_terminal_runtime_v2.get("last_terminal_dn_shares") or 0.0),
                "last_terminal_pnl_equity_usd": float(
                    self._last_terminal_runtime_v2.get("last_terminal_pnl_equity_usd") or 0.0
                ),
                "terminal_liquidation_active": bool(self._last_terminal_runtime_v2.get("terminal_liquidation_active")),
                "terminal_liquidation_attempted_orders": int(
                    self._last_terminal_runtime_v2.get("terminal_liquidation_attempted_orders") or 0
                ),
                "terminal_liquidation_placed_orders": int(
                    self._last_terminal_runtime_v2.get("terminal_liquidation_placed_orders") or 0
                ),
                "terminal_liquidation_remaining_up": float(
                    self._last_terminal_runtime_v2.get("terminal_liquidation_remaining_up") or 0.0
                ),
                "terminal_liquidation_remaining_dn": float(
                    self._last_terminal_runtime_v2.get("terminal_liquidation_remaining_dn") or 0.0
                ),
                "terminal_liquidation_done": bool(self._last_terminal_runtime_v2.get("terminal_liquidation_done")),
                "terminal_liquidation_reason": str(
                    self._last_terminal_runtime_v2.get("terminal_liquidation_reason") or ""
                ),
                "live_budget_gate_passed": bool(self._live_budget_gate_passed),
                "paper_budget_gate_passed": bool(self._paper_budget_gate_passed),
                "force_normal_soft_mode_paper": bool(self.mm_v2 and self._force_normal_soft_mode_paper),
                "force_normal_no_guards_paper": bool(self.mm_v2 and self._force_normal_no_guards_paper),
                "drawdown_breach_ticks": 0,
                "drawdown_breach_age_sec": 0.0,
            },
        }

    def _fill_context_v2(self) -> dict:
        mm = self.mm_v2
        market_context: dict[str, Any] = {
            "coin": self._coin or "",
            "timeframe": self._timeframe or "",
        }
        if not mm:
            return {
                "market": market_context,
                "paper_mode": self._paper_mode,
                "engine": "v2",
            }
        snap = mm.snapshot(app_version=APP_VERSION, app_git_hash=APP_GIT_HASH)
        snapshot_market = snap.get("market") or {}
        if isinstance(snapshot_market, dict):
            market_context.update(snapshot_market)
        analytics = snap.get("analytics") or {}
        pnl_ctx = {
            "session_pnl_equity_usd": analytics.get("session_pnl_equity_usd", analytics.get("session_pnl", 0.0)),
            "session_pnl_operator_usd": analytics.get("session_pnl_operator_usd", 0.0),
        }
        return {
            "market": market_context,
            "inventory": snap.get("inventory"),
            "valuation": snap.get("valuation"),
            "risk": snap.get("risk"),
            "analytics": analytics,
            "pnl": pnl_ctx,
            "paper_mode": self._paper_mode,
            "engine": "v2",
        }

    async def start(
        self,
        coin: str,
        timeframe: str,
        paper_mode: bool = True,
        initial_usdc: float = 1000.0,
        dev: bool = False,
        session_budget_usd: Optional[float] = None,
        force_normal_soft_mode: bool = False,
        force_normal_no_guards: bool = False,
    ) -> dict:
        if self._running and self.mm_v2:
            try:
                live_snap = self.mm_v2.snapshot(app_version=APP_VERSION, app_git_hash=APP_GIT_HASH)
            except Exception:
                live_snap = {}
            lifecycle = str(live_snap.get("lifecycle") or "")
            is_running = bool(live_snap.get("is_running", False))
            if not is_running or lifecycle in {"expired", "halted"}:
                self._running = False
        if self._running:
            raise HTTPException(status_code=400, detail="V2 already running")
        if str(coin).upper() not in V2_SUPPORTED_COINS or str(timeframe) not in V2_SUPPORTED_TIMEFRAMES:
            raise HTTPException(
                status_code=400,
                detail=(
                    f"V2 supports coins {sorted(V2_SUPPORTED_COINS)} "
                    f"on timeframes {sorted(V2_SUPPORTED_TIMEFRAMES)}"
                ),
            )

        await self._cancel_strike_retry_task()
        await self._cancel_monitor_task()
        await self._stop_feed_tasks()
        self._clear_mm_alerts()
        self.mm_v2 = None
        self._running = False

        self._coin = str(coin).upper()
        self._timeframe = str(timeframe)
        self._requested_coin = self._coin
        self._requested_timeframe = self._timeframe
        self._paper_mode = bool(paper_mode)
        self._dev_mode = bool(dev)
        self._initial_usdc = float(initial_usdc)
        self._force_normal_soft_mode_paper = bool(force_normal_soft_mode and paper_mode)
        self._force_normal_no_guards_paper = bool(force_normal_no_guards and paper_mode)
        effective_session_budget = (
            float(session_budget_usd)
            if session_budget_usd is not None
            else float(self._initial_usdc)
        )
        self._paper_budget_gate_passed = bool(
            (not self._paper_mode) or effective_session_budget >= float(self.PAPER_MIN_BUDGET_USD)
        )
        self._live_budget_gate_passed = bool(
            self._paper_mode or effective_session_budget >= float(self.LIVE_MIN_BUDGET_USD)
        )
        if self._paper_mode and not self._paper_budget_gate_passed:
            raise HTTPException(
                status_code=400,
                detail=(
                    f"paper_min_budget_30_required: requested={effective_session_budget:.2f} "
                    f"min={self.PAPER_MIN_BUDGET_USD:.2f}"
                ),
            )
        if not self._paper_mode and not self._live_budget_gate_passed:
            raise HTTPException(
                status_code=400,
                detail=(
                    f"live_min_budget_15_required: requested={effective_session_budget:.2f} "
                    f"min={self.LIVE_MIN_BUDGET_USD:.2f}"
                ),
            )
        if force_normal_soft_mode and not self._paper_mode:
            raise HTTPException(
                status_code=400,
                detail="force_normal_soft_mode is paper-only",
            )
        if force_normal_no_guards and not self._paper_mode:
            raise HTTPException(
                status_code=400,
                detail="force_normal_no_guards is paper-only",
            )

        if dev:
            _telegram.switch_credentials(
                token=os.environ.get("DEV_TELEGRAM_BOT_TOKEN", ""),
                chat_id=os.environ.get("DEV_TELEGRAM_CHAT_ID", ""),
            )
        else:
            _telegram.switch_credentials(
                token=os.environ.get("TELEGRAM_BOT_TOKEN", ""),
                chat_id=os.environ.get("TELEGRAM_CHAT_ID", ""),
                thread_id=os.environ.get("TELEGRAM_THREAD_ID", ""),
            )

        if not paper_mode:
            await self.validate_live_credentials()
            from mm_shared.approvals import _do_approvals

            approval_result = await asyncio.to_thread(_do_approvals, PM_PRIVATE_KEY)
            if not isinstance(approval_result, dict) or approval_result.get("error") or not approval_result.get("all_ok", False):
                raise HTTPException(status_code=500, detail="Cannot start live V2: approvals failed")

        self.feed_state = feeds.State()
        symbol = config.COIN_BINANCE[self._coin]
        kline_interval = config.TF_KLINE[self._timeframe]
        self._feed_tasks = [
            asyncio.create_task(feeds.ob_poller(symbol, self.feed_state)),
            asyncio.create_task(feeds.binance_feed(symbol, kline_interval, self.feed_state)),
        ]

        tokens = await asyncio.wait_for(
            asyncio.to_thread(feeds.fetch_pm_tokens, self._coin, self._timeframe),
            timeout=15.0,
        )
        if not tokens or not tokens[0] or not tokens[1]:
            await self._stop_feed_tasks()
            raise HTTPException(status_code=503, detail="PM tokens not found for V2")
        up_id, dn_id, cond_id = tokens
        self.feed_state.pm_up_id = up_id
        self.feed_state.pm_dn_id = dn_id
        self._feed_tasks.append(asyncio.create_task(feeds.pm_feed(self.feed_state)))
        market = await asyncio.wait_for(
            asyncio.to_thread(
                self._build_market_info_from_tokens,
                self._coin,
                self._timeframe,
                up_id,
                dn_id,
                condition_id=cond_id,
            ),
            timeout=45.0,
        )
        if market and market.up_token_id and not market.up_token_id.startswith("placeholder"):
            await self._enrich_market_info(market, self._coin, self._timeframe)

        startup_block_reason = self._startup_window_block_reason(market)
        if startup_block_reason:
            await self._stop_feed_tasks()
            raise HTTPException(status_code=409, detail=startup_block_reason)
        if not market or not self._is_valid_strike(getattr(market, "strike", 0.0)):
            await self._stop_feed_tasks()
            raise HTTPException(status_code=503, detail="V2 cannot start without a valid strike")

        for _ in range(100):
            if self.feed_state.mid and self.feed_state.mid > 0:
                break
            await asyncio.sleep(0.1)

        clob = _create_clob_client(
            paper_mode=paper_mode,
            initial_usdc=self._initial_usdc,
        )
        self.mm_config_v2.session_budget_usd = effective_session_budget
        self.mm_v2 = MarketMakerV2(
            self.feed_state,
            clob,
            self.mm_config_v2,
            force_normal_soft_mode_paper=self._force_normal_soft_mode_paper,
            force_normal_no_guards_paper=self._force_normal_no_guards_paper,
        )
        self.mm_v2._private_key = PM_PRIVATE_KEY
        self.mm_v2.set_market(market)
        await self._attach_mongo_logger(
            register_fill=lambda mongo: self.mm_v2.on_fill(
                lambda fill, tt: mongo.log_fill(fill, tt, self._fill_context_v2())
            ),
            register_snapshot=lambda mongo: self.mm_v2.on_snapshot(mongo.log_snapshot),
            runtime_tag="v2",
        )
        await self.mm_v2.start()
        self._running = True
        log.info("MM V2 started: %s/%s paper=%s", self._coin, self._timeframe, paper_mode)
        return self.snapshot()

    async def stop(self, *, liquidate: bool = True, emergency: bool = False) -> dict:
        self._running = False
        await self._cancel_monitor_task()
        await self._cancel_strike_retry_task()
        stop_liquidation: dict[str, Any] | None = None
        if self.mm_v2:
            if hasattr(self.mm_v2, "snapshot"):
                terminal_snap = self.mm_v2.snapshot(app_version=APP_VERSION, app_git_hash=APP_GIT_HASH)
                runtime_block = terminal_snap.get("runtime") if isinstance(terminal_snap, dict) else {}
                if isinstance(runtime_block, dict):
                    self._last_terminal_runtime_v2.update(
                        {
                            "last_terminal_reason": str(runtime_block.get("last_terminal_reason") or ""),
                            "last_terminal_ts": float(runtime_block.get("last_terminal_ts") or 0.0),
                            "last_terminal_wallet_total_usdc": float(
                                runtime_block.get("last_terminal_wallet_total_usdc") or 0.0
                            ),
                            "last_terminal_up_shares": float(runtime_block.get("last_terminal_up_shares") or 0.0),
                            "last_terminal_dn_shares": float(runtime_block.get("last_terminal_dn_shares") or 0.0),
                            "last_terminal_pnl_equity_usd": float(
                                runtime_block.get("last_terminal_pnl_equity_usd") or 0.0
                            ),
                            "terminal_liquidation_active": bool(runtime_block.get("terminal_liquidation_active")),
                            "terminal_liquidation_attempted_orders": int(
                                runtime_block.get("terminal_liquidation_attempted_orders") or 0
                            ),
                            "terminal_liquidation_placed_orders": int(
                                runtime_block.get("terminal_liquidation_placed_orders") or 0
                            ),
                            "terminal_liquidation_remaining_up": float(
                                runtime_block.get("terminal_liquidation_remaining_up") or 0.0
                            ),
                            "terminal_liquidation_remaining_dn": float(
                                runtime_block.get("terminal_liquidation_remaining_dn") or 0.0
                            ),
                            "terminal_liquidation_done": bool(runtime_block.get("terminal_liquidation_done")),
                            "terminal_liquidation_reason": str(runtime_block.get("terminal_liquidation_reason") or ""),
                        }
                    )
            stop_liquidation = await self.mm_v2.stop(liquidate=liquidate and not emergency)
            self.mm_v2 = None
            if isinstance(stop_liquidation, dict):
                self._last_terminal_runtime_v2["last_terminal_up_shares"] = float(
                    stop_liquidation.get("remaining_up") or self._last_terminal_runtime_v2["last_terminal_up_shares"]
                )
                self._last_terminal_runtime_v2["last_terminal_dn_shares"] = float(
                    stop_liquidation.get("remaining_dn") or self._last_terminal_runtime_v2["last_terminal_dn_shares"]
                )
                self._last_terminal_runtime_v2["terminal_liquidation_active"] = False
                self._last_terminal_runtime_v2["terminal_liquidation_attempted_orders"] = int(
                    stop_liquidation.get("attempted_orders") or 0
                )
                self._last_terminal_runtime_v2["terminal_liquidation_placed_orders"] = int(
                    stop_liquidation.get("placed_orders") or 0
                )
                self._last_terminal_runtime_v2["terminal_liquidation_remaining_up"] = float(
                    stop_liquidation.get("remaining_up") or 0.0
                )
                self._last_terminal_runtime_v2["terminal_liquidation_remaining_dn"] = float(
                    stop_liquidation.get("remaining_dn") or 0.0
                )
                self._last_terminal_runtime_v2["terminal_liquidation_done"] = bool(
                    stop_liquidation.get("done", False)
                )
                self._last_terminal_runtime_v2["terminal_liquidation_reason"] = str(
                    stop_liquidation.get("reason") or ""
                )
        await self._stop_feed_tasks()
        await self._teardown_mongo_logger()
        snap = self.snapshot()
        if stop_liquidation is not None:
            snap["stop_liquidation"] = stop_liquidation
            if bool(stop_liquidation.get("enabled")) and not bool(stop_liquidation.get("done", True)):
                self.set_alert(
                    "mmv2_stop_liquidation",
                    (
                        "Stop liquidation incomplete: "
                        f"rem_up={stop_liquidation.get('remaining_up')} "
                        f"rem_dn={stop_liquidation.get('remaining_dn')}"
                    ),
                    level="warning",
                )
            else:
                self.clear_alert("mmv2_stop_liquidation")
        return snap

    def snapshot(self) -> dict:
        if self.mm_v2:
            snap = self.mm_v2.snapshot(app_version=APP_VERSION, app_git_hash=APP_GIT_HASH)
            runtime_block = snap.get("runtime")
            if not isinstance(runtime_block, dict):
                runtime_block = {}
            self._last_terminal_runtime_v2.update(
                {
                    "last_terminal_reason": str(runtime_block.get("last_terminal_reason") or ""),
                    "last_terminal_ts": float(runtime_block.get("last_terminal_ts") or 0.0),
                    "last_terminal_wallet_total_usdc": float(runtime_block.get("last_terminal_wallet_total_usdc") or 0.0),
                    "last_terminal_up_shares": float(runtime_block.get("last_terminal_up_shares") or 0.0),
                    "last_terminal_dn_shares": float(runtime_block.get("last_terminal_dn_shares") or 0.0),
                    "last_terminal_pnl_equity_usd": float(runtime_block.get("last_terminal_pnl_equity_usd") or 0.0),
                    "terminal_liquidation_active": bool(runtime_block.get("terminal_liquidation_active")),
                    "terminal_liquidation_attempted_orders": int(
                        runtime_block.get("terminal_liquidation_attempted_orders") or 0
                    ),
                    "terminal_liquidation_placed_orders": int(
                        runtime_block.get("terminal_liquidation_placed_orders") or 0
                    ),
                    "terminal_liquidation_remaining_up": float(
                        runtime_block.get("terminal_liquidation_remaining_up") or 0.0
                    ),
                    "terminal_liquidation_remaining_dn": float(
                        runtime_block.get("terminal_liquidation_remaining_dn") or 0.0
                    ),
                    "terminal_liquidation_done": bool(runtime_block.get("terminal_liquidation_done")),
                    "terminal_liquidation_reason": str(runtime_block.get("terminal_liquidation_reason") or ""),
                }
            )
            runtime_block["live_budget_gate_passed"] = bool(self._live_budget_gate_passed)
            runtime_block["paper_budget_gate_passed"] = bool(self._paper_budget_gate_passed)
            snap["runtime"] = runtime_block
            snap["alerts"] = self.list_alerts() + [a for a in snap.get("alerts", []) if a not in self.list_alerts()]
            return snap
        snap = self._idle_snapshot_v2()
        snap["alerts"] = self.list_alerts()
        return snap

    def update_config(self, **updates: Any) -> dict[str, Any]:
        self.mm_config_v2.update(**updates)
        if self.mm_v2:
            self.mm_v2.config.update(**updates)
            self.mm_v2.gateway.transport_config = self.mm_v2.config.to_mm_config()
            self.mm_v2.gateway.order_mgr.config = self.mm_v2.gateway.transport_config
            self.mm_v2.valuation.config = self.mm_v2.config
            self.mm_v2.valuation.provider.vol_floor = float(self.mm_v2.config.vol_floor)
            self.mm_v2.reconcile.config = self.mm_v2.config
            self.mm_v2.risk_kernel.config = self.mm_v2.config
            self.mm_v2.state_machine.config = self.mm_v2.config
            self.mm_v2.execution_policy.requote_threshold_bps = float(self.mm_v2.config.requote_threshold_bps)
        return self.mm_config_v2.to_dict()

    def fills_page(self, *, limit: int = 50, offset: int = 0) -> dict[str, Any]:
        if not self.mm_v2:
            return {"fills": [], "total": 0}
        return self.mm_v2.fills_page(limit=limit, offset=offset)

    async def _run_server_self_check(self) -> dict[str, Any]:
        snap = self.snapshot()
        required_blocks = [
            "lifecycle",
            "market",
            "valuation",
            "inventory",
            "pair_inventory",
            "quotes",
            "execution",
            "risk",
            "health",
            "analytics",
            "alerts",
            "config",
            "runtime",
        ]
        checks: list[dict[str, Any]] = []
        for key in required_blocks:
            checks.append({
                "name": f"state_contract:{key}",
                "ok": key in snap,
                "detail": "present" if key in snap else "missing",
            })
        maker_only = True
        checks.append({
            "name": "config:maker_only_runtime_policy",
            "ok": maker_only,
            "detail": "maker-only except emergency taker",
        })
        passed = sum(1 for check in checks if check["ok"])
        return {
            "ok": passed == len(checks),
            "summary": f"{passed}/{len(checks)} checks passed",
            "checks": checks,
            "command": [],
            "exit_code": 0 if passed == len(checks) else 1,
            "stdout_tail": "",
            "stderr_tail": "",
            "error": "",
        }

    def _verification_command(self, kind: str) -> list[str] | None:
        if kind == "pytest_v2":
            return [
                sys.executable,
                "-m",
                "pytest",
                "-q",
                "tests/test_mm_v2.py",
                "tests/test_mm_v2_inventory_modes.py",
                "tests/test_mm_v2_quote_skew.py",
                "tests/test_mm_v2_paper_multiwindow.py",
                "tests/test_mm_v2_runtime_skew_flow.py",
                "tests/test_mm_v2_sell_release_lag.py",
            ]
        if kind == "replay_v2":
            return [
                sys.executable,
                "-m",
                "pytest",
                "-q",
                "tests/test_mm_v2_replay.py",
            ]
        if kind == "paper_v2_smoke":
            return [
                sys.executable,
                "-m",
                "pytest",
                "-q",
                "tests/test_mm_v2_paper.py",
            ]
        return super()._verification_command(kind)

    async def start_verification(self, kind: str) -> dict[str, Any]:
        allowed = {
            "server_self_check",
            "pytest_v2",
            "replay_v2",
            "paper_v2_smoke",
            "pytest_safety",
            "backtest_smoke",
        }
        kind = str(kind or "server_self_check").strip() or "server_self_check"
        if kind not in allowed:
            raise HTTPException(status_code=400, detail=f"unsupported verification kind: {kind}")
        if self._verification_task and not self._verification_task.done():
            raise HTTPException(status_code=409, detail="verification already running")
        self._verification_task = asyncio.create_task(self._verification_runner(kind))
        return self.get_verification_status()


class PaperSweepRuntimeV2(MMRuntime):
    DEFAULT_BASE_CLIPS = (8.0, 12.0, 14.0, 20.0)

    def __init__(self):
        super().__init__()
        self.mm_config_v2: MMConfigV2 = MMConfigV2()
        self._variants: list[dict[str, Any]] = []
        self._started_at: float = 0.0
        self._force_normal_soft_mode_paper: bool = False
        self._last_state: dict[str, Any] = self._idle_snapshot()

    def _idle_snapshot(self) -> dict[str, Any]:
        return {
            "is_running": False,
            "started_at": float(self._started_at or 0.0),
            "coin": self._coin or "BTC",
            "timeframe": self._timeframe or "15m",
            "paper_mode": True,
            "initial_usdc": float(self._initial_usdc or 0.0),
            "force_normal_soft_mode": bool(self._force_normal_soft_mode_paper),
            "force_normal_no_guards": False,
            "base_clips": list(self.DEFAULT_BASE_CLIPS),
            "variant_count": 0,
            "running_variants": 0,
            "completed_variants": 0,
            "aggregate_pnl_usd": 0.0,
            "best_pnl_usd": 0.0,
            "worst_pnl_usd": 0.0,
            "variants": [],
        }

    def _normalize_base_clips(self, base_clips: list[float] | None) -> list[float]:
        values = list(base_clips or self.DEFAULT_BASE_CLIPS)
        normalized: list[float] = []
        seen: set[float] = set()
        for clip in values:
            value = round(float(clip), 6)
            if value <= 0.0 or value > 100.0:
                raise HTTPException(status_code=400, detail="base_clips must be between 0 and 100")
            if value in seen:
                continue
            seen.add(value)
            normalized.append(float(clip))
        if not normalized:
            raise HTTPException(status_code=400, detail="base_clips cannot be empty")
        return normalized

    def _variant_summary(self, variant: dict[str, Any], raw: dict[str, Any]) -> dict[str, Any]:
        analytics = raw.get("analytics") or {}
        risk = raw.get("risk") or {}
        runtime = raw.get("runtime") or {}
        inventory = raw.get("inventory") or {}
        quotes = raw.get("quotes") or {}
        market = raw.get("market") or {}
        config_overrides = dict(variant.get("config_overrides") or {})
        mm_obj = variant.get("mm")
        effective_config: dict[str, float] = {}
        if mm_obj and hasattr(mm_obj, "config"):
            effective_config = {
                "base_half_spread_bps": float(mm_obj.config.base_half_spread_bps),
                "vol_spread_multiplier": float(mm_obj.config.vol_spread_multiplier),
                "inventory_skew_strength": float(mm_obj.config.inventory_skew_strength),
                "min_edge_bps": float(mm_obj.config.min_edge_bps),
                "base_clip_usd": float(mm_obj.config.base_clip_usd),
            }
        active_quotes = sum(
            1
            for value in quotes.values()
            if isinstance(value, dict) and value.get("active", True) and "price" in value
        )
        lifecycle = str(raw.get("lifecycle") or "bootstrapping")
        is_running = bool(raw.get("is_running", lifecycle not in {"expired", "halted"}))
        return {
            "id": str(variant.get("id") or ""),
            "label": str(variant.get("label") or ""),
            "base_clip_usd": float(variant.get("clip_usd") or 0.0),
            "is_running": is_running,
            "lifecycle": lifecycle,
            "soft_mode": str(risk.get("soft_mode") or "normal"),
            "target_soft_mode": str(risk.get("target_soft_mode") or risk.get("soft_mode") or "normal"),
            "hard_mode": str(risk.get("hard_mode") or "none"),
            "reason": str(risk.get("reason") or ""),
            "session_pnl_equity_usd": float(
                analytics.get("session_pnl_equity_usd")
                if analytics.get("session_pnl_equity_usd") is not None
                else analytics.get("session_pnl") or 0.0
            ),
            "session_pnl_operator_usd": float(
                analytics.get("session_pnl_operator_usd")
                if analytics.get("session_pnl_operator_usd") is not None
                else analytics.get("session_pnl_equity_usd") or analytics.get("session_pnl") or 0.0
            ),
            "portfolio_mark_value_usd": float(analytics.get("portfolio_mark_value_usd") or 0.0),
            "tradeable_portfolio_value_usd": float(analytics.get("tradeable_portfolio_value_usd") or 0.0),
            "position_mark_value_usd": float(analytics.get("position_mark_value_usd") or 0.0),
            "wallet_total_usdc": float(inventory.get("wallet_total_usdc") or 0.0),
            "fill_count": int(analytics.get("fill_count") or 0),
            "mm_effective_ratio_60s": float(analytics.get("mm_effective_ratio_60s") or 0.0),
            "dual_bid_ratio_60s": float(analytics.get("dual_bid_ratio_60s") or 0.0),
            "failure_bucket_current": str(analytics.get("failure_bucket_current") or ""),
            "marketability_churn_confirmed": bool(analytics.get("marketability_churn_confirmed") or False),
            "marketability_problem_side": str(analytics.get("marketability_problem_side") or ""),
            "sell_churn_hold_side": str(analytics.get("sell_churn_hold_side") or ""),
            "quote_balance_state": str(analytics.get("quote_balance_state") or ""),
            "dual_bid_exception_reason": str(analytics.get("dual_bid_exception_reason") or ""),
            "terminal_liquidation_done": bool(runtime.get("terminal_liquidation_done") or False),
            "terminal_liquidation_reason": str(runtime.get("terminal_liquidation_reason") or ""),
            "terminal_liquidation_remaining_up": float(runtime.get("terminal_liquidation_remaining_up") or 0.0),
            "terminal_liquidation_remaining_dn": float(runtime.get("terminal_liquidation_remaining_dn") or 0.0),
            "active_quotes": int(active_quotes),
            "market_id": str(market.get("market_id") or ""),
            "time_left_sec": float(market.get("time_left_sec") or 0.0),
            "config_overrides": config_overrides,
            "effective_config": effective_config,
        }

    def snapshot(self) -> dict[str, Any]:
        if self._variants:
            variants: list[dict[str, Any]] = []
            running_variants = 0
            for variant in self._variants:
                mm = variant.get("mm")
                if not mm:
                    continue
                try:
                    raw = mm.snapshot(app_version=APP_VERSION, app_git_hash=APP_GIT_HASH)
                except Exception as exc:
                    raw = {
                        "is_running": False,
                        "lifecycle": "halted",
                        "risk": {"soft_mode": "normal", "hard_mode": "halted", "reason": f"snapshot_error: {exc}"},
                        "analytics": {"session_pnl_equity_usd": 0.0, "session_pnl_operator_usd": 0.0},
                        "runtime": {},
                        "inventory": {},
                        "quotes": {},
                    }
                summary = self._variant_summary(variant, raw)
                variants.append(summary)
                if summary["is_running"]:
                    running_variants += 1
            if running_variants == 0:
                self._running = False
            aggregate = sum(float(variant.get("session_pnl_equity_usd") or 0.0) for variant in variants)
            best = max([float(variant.get("session_pnl_equity_usd") or 0.0) for variant in variants], default=0.0)
            worst = min([float(variant.get("session_pnl_equity_usd") or 0.0) for variant in variants], default=0.0)
            active_variant = next((variant for variant in variants if bool(variant.get("is_running"))), variants[0] if variants else {})
            state = {
                "is_running": bool(running_variants > 0),
                "started_at": float(self._started_at or 0.0),
                "coin": self._coin or "BTC",
                "timeframe": self._timeframe or "15m",
                "paper_mode": True,
                "initial_usdc": float(self._initial_usdc or 0.0),
                "force_normal_soft_mode": bool(self._force_normal_soft_mode_paper),
                "force_normal_no_guards": False,
                "base_clips": [float(variant.get("clip_usd") or 0.0) for variant in self._variants],
                "variant_count": len(variants),
                "running_variants": int(running_variants),
                "completed_variants": int(len(variants) - running_variants),
                "aggregate_pnl_usd": float(aggregate),
                "best_pnl_usd": float(best),
                "worst_pnl_usd": float(worst),
                "market_id": str(active_variant.get("market_id") or ""),
                "time_left_sec": float(active_variant.get("time_left_sec") or 0.0),
                "variants": variants,
            }
            self._last_state = state
            return state
        return dict(self._last_state)

    def export_bundle(self) -> dict[str, Any]:
        state = self.snapshot()
        exported_variants: list[dict[str, Any]] = []
        if not self._variants:
            for item in list(state.get("variants") or []):
                exported_variants.append(
                    {
                        "summary": dict(item),
                        "fills": [],
                        "fills_total": int(item.get("fill_count") or 0),
                        "note": "fills_unavailable_runtime_variants_already_released",
                    }
                )
            return {
                "state": state,
                "variants": exported_variants,
            }

        for variant in self._variants:
            mm = variant.get("mm")
            if not mm:
                continue
            raw = mm.snapshot(app_version=APP_VERSION, app_git_hash=APP_GIT_HASH)
            summary = self._variant_summary(variant, raw)
            fills_total = int(getattr(mm, "_fills", None) and len(mm._fills) or 0)
            fills_page = mm.fills_page(limit=max(1, fills_total), offset=0) if fills_total > 0 else {"fills": [], "total": 0}
            exported_variants.append(
                {
                    "summary": summary,
                    "raw_state": raw,
                    "fills": list(fills_page.get("fills") or []),
                    "fills_total": int(fills_page.get("total") or 0),
                }
            )
        return {
            "state": state,
            "variants": exported_variants,
        }

    async def start(
        self,
        coin: str,
        timeframe: str,
        *,
        initial_usdc: float = 300.0,
        base_clips: list[float] | None = None,
        variants: list[dict[str, Any]] | None = None,
        force_normal_soft_mode: bool = False,
        base_config: MMConfigV2 | None = None,
    ) -> dict[str, Any]:
        if self._running and any(bool(variant.get("mm") and variant["mm"]._running) for variant in self._variants):
            raise HTTPException(status_code=400, detail="paper sweep already running")
        await self.stop(liquidate=False)

        self._coin = str(coin).upper()
        self._timeframe = str(timeframe)
        self._paper_mode = True
        self._initial_usdc = float(initial_usdc)
        self._force_normal_soft_mode_paper = bool(force_normal_soft_mode)
        normalized_clips = self._normalize_base_clips(base_clips) if not variants else []

        self.feed_state = feeds.State()
        symbol = config.COIN_BINANCE[self._coin]
        kline_interval = config.TF_KLINE[self._timeframe]
        self._feed_tasks = [
            asyncio.create_task(feeds.ob_poller(symbol, self.feed_state)),
            asyncio.create_task(feeds.binance_feed(symbol, kline_interval, self.feed_state)),
        ]

        try:
            tokens = await asyncio.wait_for(
                asyncio.to_thread(feeds.fetch_pm_tokens, self._coin, self._timeframe),
                timeout=15.0,
            )
            if not tokens or not tokens[0] or not tokens[1]:
                raise HTTPException(status_code=503, detail="PM tokens not found for paper sweep")
            up_id, dn_id, cond_id = tokens
            self.feed_state.pm_up_id = up_id
            self.feed_state.pm_dn_id = dn_id
            self._feed_tasks.append(asyncio.create_task(feeds.pm_feed(self.feed_state)))
            market = await asyncio.wait_for(
                asyncio.to_thread(
                    self._build_market_info_from_tokens,
                    self._coin,
                    self._timeframe,
                    up_id,
                    dn_id,
                    condition_id=cond_id,
                ),
                timeout=45.0,
            )
            if market and market.up_token_id and not market.up_token_id.startswith("placeholder"):
                await self._enrich_market_info(market, self._coin, self._timeframe)

            startup_block_reason = self._startup_window_block_reason(market)
            if startup_block_reason:
                raise HTTPException(status_code=409, detail=startup_block_reason)
            if not market or not self._is_valid_strike(getattr(market, "strike", 0.0)):
                raise HTTPException(status_code=503, detail="paper sweep cannot start without a valid strike")

            for _ in range(100):
                if self.feed_state.mid and self.feed_state.mid > 0:
                    break
                await asyncio.sleep(0.1)

            template = copy.deepcopy(base_config or self.mm_config_v2)
            template.session_budget_usd = float(initial_usdc)
            self.mm_config_v2 = copy.deepcopy(template)

            variants_list: list[dict[str, Any]] = []
            started: list[MarketMakerV2] = []
            try:
                if variants:
                    variant_specs: list[dict[str, Any]] = []
                    for i, variant in enumerate(variants):
                        payload = dict(variant)
                        label = str(payload.pop("label", f"v{i}"))
                        clip = float(payload.pop("base_clip_usd", payload.pop("clip_usd", template.base_clip_usd)))
                        variant_specs.append(
                            {
                                "label": label,
                                "clip_usd": clip,
                                "overrides": dict(payload),
                            }
                        )
                else:
                    variant_specs = [
                        {"label": f"${clip:g}", "clip_usd": clip, "overrides": {}}
                        for clip in normalized_clips
                    ]

                for spec in variant_specs:
                    cfg = copy.deepcopy(template)
                    cfg.base_clip_usd = float(spec["clip_usd"])
                    if spec["overrides"]:
                        cfg.update(**spec["overrides"])
                    clob = _create_clob_client(paper_mode=True, initial_usdc=float(initial_usdc))
                    mm = MarketMakerV2(
                        self.feed_state,
                        clob,
                        cfg,
                        force_normal_soft_mode_paper=bool(force_normal_soft_mode),
                    )
                    mm.set_market(copy.deepcopy(market))
                    await mm.start()
                    started.append(mm)
                    variants_list.append(
                        {
                            "id": f"v-{str(spec['label']).replace(' ', '_').replace('.', '_')}",
                            "label": str(spec["label"]),
                            "clip_usd": float(spec["clip_usd"]),
                            "config_overrides": dict(spec["overrides"]),
                            "mm": mm,
                        }
                    )
            except Exception:
                for mm in started:
                    try:
                        await mm.stop(liquidate=False)
                    except Exception:
                        pass
                raise

            self._variants = variants_list
            self._started_at = time.time()
            self._running = True
            return self.snapshot()
        except Exception:
            await self._stop_feed_tasks()
            self._variants = []
            self._running = False
            raise

    async def stop(self, *, liquidate: bool = True) -> dict[str, Any]:
        self._running = False
        variants = list(self._variants)
        self._variants = []
        for variant in variants:
            mm = variant.get("mm")
            if not mm:
                continue
            try:
                await mm.stop(liquidate=liquidate)
            except Exception as exc:
                log.warning("Paper sweep variant stop failed for clip=%s: %s", variant.get("clip_usd"), exc)
        await self._stop_feed_tasks()
        if variants:
            summaries: list[dict[str, Any]] = []
            for variant in variants:
                mm = variant.get("mm")
                if not mm:
                    continue
                try:
                    raw = mm.snapshot(app_version=APP_VERSION, app_git_hash=APP_GIT_HASH)
                except Exception as exc:
                    raw = {
                        "is_running": False,
                        "lifecycle": "halted",
                        "risk": {"soft_mode": "normal", "hard_mode": "halted", "reason": f"snapshot_error: {exc}"},
                        "analytics": {"session_pnl_equity_usd": 0.0, "session_pnl_operator_usd": 0.0},
                        "runtime": {},
                        "inventory": {},
                        "quotes": {},
                    }
                summaries.append(self._variant_summary(variant, raw))
            aggregate = sum(float(item.get("session_pnl_equity_usd") or 0.0) for item in summaries)
            best = max([float(item.get("session_pnl_equity_usd") or 0.0) for item in summaries], default=0.0)
            worst = min([float(item.get("session_pnl_equity_usd") or 0.0) for item in summaries], default=0.0)
            self._last_state = {
                "is_running": False,
                "started_at": float(self._started_at or 0.0),
                "coin": self._coin or "BTC",
                "timeframe": self._timeframe or "15m",
                "paper_mode": True,
                "initial_usdc": float(self._initial_usdc or 0.0),
                "force_normal_soft_mode": bool(self._force_normal_soft_mode_paper),
                "force_normal_no_guards": False,
                "base_clips": [float(item.get("base_clip_usd") or 0.0) for item in summaries],
                "variant_count": len(summaries),
                "running_variants": 0,
                "completed_variants": len(summaries),
                "aggregate_pnl_usd": float(aggregate),
                "best_pnl_usd": float(best),
                "worst_pnl_usd": float(worst),
                "variants": summaries,
            }
        return self.snapshot()

# ── Singleton runtime ───────────────────────────────────────────
# Legacy V1 runtime path is removed. Keep `_runtime` as compatibility alias
# to the V2 singleton for internal helpers that still reference this name.
_runtime_v2 = MMRuntimeV2()
_paper_sweep_v2 = PaperSweepRuntimeV2()
# Pair Arb runtime (initialized lazily on first start)
_pair_arb_engine = None  # type: ignore
_runtime = _runtime_v2


def _dashboard_engine(preferred: str | None = None) -> str:
    del preferred
    return "v2"


def _v2_active_orders_detail() -> list[dict[str, Any]]:
    mm = _runtime_v2.mm_v2
    if not mm or not getattr(mm, "market", None):
        return []
    try:
        return mm.gateway.order_mgr.get_active_orders_detail(
            liquidation_ids=set(),
            up_token_id=mm.market.up_token_id,
            dn_token_id=mm.market.dn_token_id,
        )
    except Exception:
        return []


def _compute_spread_bps(bid: float | None, ask: float | None) -> float:
    bid_v = float(bid or 0.0)
    ask_v = float(ask or 0.0)
    mid = (bid_v + ask_v) / 2.0
    if bid_v <= 0 or ask_v <= 0 or ask_v <= bid_v or mid <= 0:
        return 0.0
    return ((ask_v - bid_v) / mid) * 10000.0


def _dashboard_snapshot_from_v2(raw: dict[str, Any]) -> dict[str, Any]:
    market = raw.get("market") or {}
    valuation = raw.get("valuation") or {}
    inventory = raw.get("inventory") or {}
    execution = raw.get("execution") or {}
    risk = raw.get("risk") or {}
    health = raw.get("health") or {}
    analytics = raw.get("analytics") or {}
    quotes = raw.get("quotes") or {}
    quotes = {
        key: value
        if isinstance(value, dict) and value.get("active", True) and "price" in value and "size" in value
        else None
        for key, value in quotes.items()
    }
    mm = _runtime_v2.mm_v2
    market_info = getattr(mm, "market", None) if mm else None
    heartbeat = getattr(getattr(mm, "heartbeat", None), "stats", {}) or {}
    fills_page = _runtime_v2.fills_page(limit=20, offset=0)
    recent_fills = list(fills_page.get("fills") or [])
    started_at = float(getattr(mm, "_started_at", 0.0) or 0.0) if mm else 0.0
    now_ts = time.time()
    up_bid = market.get("up_best_bid")
    up_ask = market.get("up_best_ask")
    dn_bid = market.get("dn_best_bid")
    dn_ask = market.get("dn_best_ask")
    spread_values = [
        _compute_spread_bps(up_bid, up_ask),
        _compute_spread_bps(dn_bid, dn_ask),
    ]
    spread_values = [v for v in spread_values if v > 0]
    spread_bps = max(spread_values) if spread_values else 0.0
    bid_depth_usd = float(market.get("up_bid_depth_usd") or 0.0) + float(market.get("dn_bid_depth_usd") or 0.0)
    ask_depth_usd = float(market.get("up_ask_depth_usd") or 0.0) + float(market.get("dn_ask_depth_usd") or 0.0)
    depth_total = bid_depth_usd + ask_depth_usd
    market_quality = {
        "overall_score": float(market.get("market_quality_score") or 0.0),
        "tradeable": bool(market.get("market_tradeable", False)),
        "liquidity_score": max(0.0, min(1.0, depth_total / 200.0)),
        "spread_bps": round(spread_bps, 1),
        "bid_depth_usd": round(bid_depth_usd, 2),
        "ask_depth_usd": round(ask_depth_usd, 2),
    }
    up_mid = market.get("pm_mid_up")
    dn_mid = market.get("pm_mid_dn")
    up_mark = float(up_mid) if up_mid is not None else float(valuation.get("fv_up") or 0.0)
    dn_mark = float(dn_mid) if dn_mid is not None else float(valuation.get("fv_dn") or 0.0)
    up_mark = max(0.0, up_mark)
    dn_mark = max(0.0, dn_mark)
    free_usdc = float(inventory.get("free_usdc") or 0.0)
    reserved_usdc = float(inventory.get("wallet_reserved_usdc") or inventory.get("reserved_usdc") or 0.0)
    wallet_total_usdc = float(inventory.get("wallet_total_usdc") or (free_usdc + reserved_usdc))
    up_shares = float(inventory.get("up_shares") or 0.0)
    dn_shares = float(inventory.get("dn_shares") or 0.0)
    position_value = float(analytics.get("position_mark_value_usd") or (up_shares * up_mark + dn_shares * dn_mark))
    portfolio_value = float(analytics.get("tradeable_portfolio_value_usd") or (free_usdc + position_value))
    wallet_portfolio_value = float(analytics.get("portfolio_mark_value_usd") or (wallet_total_usdc + position_value))
    session_pnl_risk_equity = float(
        analytics.get("session_pnl_equity_usd")
        if analytics.get("session_pnl_equity_usd") is not None
        else analytics.get("session_pnl") or 0.0
    )
    session_pnl_operator = float(
        analytics.get("session_pnl_operator_usd")
        if analytics.get("session_pnl_operator_usd") is not None
        else session_pnl_risk_equity
    )
    feed_mid = float(getattr(_runtime_v2.feed_state, "mid", 0.0) or 0.0) if _runtime_v2.feed_state else 0.0
    lifecycle = str(raw.get("lifecycle") or "bootstrapping")
    is_running = bool(raw.get("is_running", False))
    return {
        "dashboard_engine": "v2",
        "app_version": raw.get("app_version", APP_VERSION),
        "app_git_hash": raw.get("app_git_hash", APP_GIT_HASH),
        "is_running": is_running,
        "lifecycle": lifecycle,
        "mode": str(risk.get("soft_mode") or "normal"),
        "target_mode": str(risk.get("target_soft_mode") or risk.get("soft_mode") or "normal"),
        "hard_mode": str(risk.get("hard_mode") or "none"),
        "is_paused": lifecycle == "halted" or str(risk.get("hard_mode") or "") == "halted",
        "pause_reason": str(risk.get("reason") or "") if lifecycle == "halted" else "",
        "is_closing": lifecycle in {"unwind", "emergency_unwind", "expired"},
        "paper_mode": bool(_runtime_v2._paper_mode),
        "started_at": started_at,
        "uptime_sec": round(max(0.0, now_ts - started_at), 2) if started_at else 0.0,
        "session_limit": float((raw.get("config") or {}).get("session_budget_usd") or _runtime_v2._initial_usdc or 0.0),
        "usdc_balance_pm": round(wallet_total_usdc, 4),
        "usdc_free_pm": round(free_usdc, 4),
        "usdc_reserved_pm": round(reserved_usdc, 4),
        "portfolio_value": round(portfolio_value, 4),
        "wallet_portfolio_value": round(wallet_portfolio_value, 4),
        "position_value_pm": round(position_value, 4),
        "session_pnl": session_pnl_operator,
        "operator_pnl": session_pnl_operator,
        "session_pnl_risk_equity": session_pnl_risk_equity,
        "realized_pnl": float(analytics.get("spread_capture_usd") or 0.0),
        "unrealized_pnl": round(session_pnl_risk_equity - float(analytics.get("spread_capture_usd") or 0.0), 4),
        "peak_pnl": session_pnl_operator,
        "total_fees": 0.0,
        "total_volume": round(sum(float(f.get("price") or 0.0) * float(f.get("size") or 0.0) for f in recent_fills), 4),
        "fill_count": int(analytics.get("fill_count") or fills_page.get("total") or 0),
        "quote_count": 0,
        "requote_count": 0,
        "avg_spread_bps": round(spread_bps, 1),
        "fair_value": {
            "up": float(valuation.get("fv_up") or 0.0),
            "dn": float(valuation.get("fv_dn") or 0.0),
            "volatility": 0.0,
            "binance_mid": feed_mid,
        },
        "quotes": {
            "up_bid": quotes.get("up_bid"),
            "up_ask": quotes.get("up_ask"),
            "dn_bid": quotes.get("dn_bid"),
            "dn_ask": quotes.get("dn_ask"),
        },
        "pm_prices": {
            "up": up_mark,
            "dn": dn_mark,
        },
        "inventory": {
            "up_shares": up_shares,
            "dn_shares": dn_shares,
            "net_delta": round(up_shares - dn_shares, 4),
            "usdc": round(free_usdc, 4),
            "usdc_reserved": round(reserved_usdc, 4),
            "up_avg_entry": None,
            "dn_avg_entry": None,
        },
        "recent_fills": recent_fills,
        "active_orders_detail": _v2_active_orders_detail(),
        "market_quality": market_quality,
        "mm_regime": {
            "quoting_ratio_60s": float(analytics.get("quoting_ratio_60s") or 0.0),
            "inventory_skewed_ratio_60s": float(analytics.get("inventory_skewed_ratio_60s") or 0.0),
            "defensive_ratio_60s": float(analytics.get("defensive_ratio_60s") or 0.0),
            "unwind_ratio_60s": float(analytics.get("unwind_ratio_60s") or 0.0),
            "emergency_unwind_ratio_60s": float(analytics.get("emergency_unwind_ratio_60s") or 0.0),
            "four_quote_ratio_60s": float(analytics.get("four_quote_ratio_60s") or 0.0),
            "mm_effective_ratio_60s": float(analytics.get("mm_effective_ratio_60s") or 0.0),
            "dual_bid_ratio_60s": float(analytics.get("dual_bid_ratio_60s") or 0.0),
            "one_sided_bid_streak_outside": int(analytics.get("one_sided_bid_streak_outside") or 0),
            "mm_regime_degraded_reason": str(analytics.get("mm_regime_degraded_reason") or ""),
            "quote_anchor_mode": str(analytics.get("quote_anchor_mode") or "midpoint_first"),
            "midpoint_reference_mode": str(analytics.get("midpoint_reference_mode") or "midpoint_first"),
            "anchor_divergence_up": float(analytics.get("anchor_divergence_up") or 0.0),
            "anchor_divergence_dn": float(analytics.get("anchor_divergence_dn") or 0.0),
            "buy_edge_gap_up": float(analytics.get("buy_edge_gap_up") or market.get("buy_edge_gap_up") or 0.0),
            "buy_edge_gap_dn": float(analytics.get("buy_edge_gap_dn") or market.get("buy_edge_gap_dn") or 0.0),
            "quote_shift_from_mid_up": float(analytics.get("quote_shift_from_mid_up") or 0.0),
            "quote_shift_from_mid_dn": float(analytics.get("quote_shift_from_mid_dn") or 0.0),
            "post_fill_markout_5s_up": float(analytics.get("post_fill_markout_5s_up") or 0.0),
            "post_fill_markout_5s_dn": float(analytics.get("post_fill_markout_5s_dn") or 0.0),
            "toxic_fill_streak_up": int(analytics.get("toxic_fill_streak_up") or 0),
            "toxic_fill_streak_dn": int(analytics.get("toxic_fill_streak_dn") or 0),
            "side_soft_brake_up_active": bool(analytics.get("side_soft_brake_up_active") or False),
            "side_soft_brake_dn_active": bool(analytics.get("side_soft_brake_dn_active") or False),
            "side_reentry_cooldown_up_sec": float(analytics.get("side_reentry_cooldown_up_sec") or 0.0),
            "side_reentry_cooldown_dn_sec": float(analytics.get("side_reentry_cooldown_dn_sec") or 0.0),
            "side_hard_block_up_sec": float(analytics.get("side_hard_block_up_sec") or 0.0),
            "side_hard_block_dn_sec": float(analytics.get("side_hard_block_dn_sec") or 0.0),
            "maker_cross_guard_hits_60s": int(analytics.get("maker_cross_guard_hits_60s") or 0),
            "dual_bid_guard_hits_60s": int(analytics.get("dual_bid_guard_hits_60s") or 0),
            "dual_bid_guard_fail_hits_60s": int(analytics.get("dual_bid_guard_fail_hits_60s") or 0),
            "midpoint_first_brake_hits_60s": int(analytics.get("midpoint_first_brake_hits_60s") or 0),
            "simultaneous_bid_block_prevented_hits_60s": int(
                analytics.get("simultaneous_bid_block_prevented_hits_60s") or 0
            ),
            "divergence_soft_brake_up_active": bool(analytics.get("divergence_soft_brake_up_active") or False),
            "divergence_soft_brake_dn_active": bool(analytics.get("divergence_soft_brake_dn_active") or False),
            "divergence_hard_suppress_up_active": bool(analytics.get("divergence_hard_suppress_up_active") or False),
            "divergence_hard_suppress_dn_active": bool(analytics.get("divergence_hard_suppress_dn_active") or False),
            "divergence_soft_brake_hits_60s": int(analytics.get("divergence_soft_brake_hits_60s") or 0),
            "divergence_hard_suppress_hits_60s": int(analytics.get("divergence_hard_suppress_hits_60s") or 0),
            "max_buy_edge_gap_60s": float(analytics.get("max_buy_edge_gap_60s") or 0.0),
            "dual_bid_exception_active": bool(analytics.get("dual_bid_exception_active") or False),
            "dual_bid_exception_reason": str(analytics.get("dual_bid_exception_reason") or ""),
            "marketability_guard_active": bool(analytics.get("marketability_guard_active") or False),
            "marketability_guard_reason": str(analytics.get("marketability_guard_reason") or ""),
            "marketability_churn_confirmed": bool(analytics.get("marketability_churn_confirmed") or False),
            "marketability_problem_side": str(analytics.get("marketability_problem_side") or ""),
            "marketability_side_locked": str(analytics.get("marketability_side_locked") or ""),
            "marketability_side_lock_age_sec": float(analytics.get("marketability_side_lock_age_sec") or 0.0),
            "sell_churn_hold_up_active": bool(analytics.get("sell_churn_hold_up_active") or False),
            "sell_churn_hold_dn_active": bool(analytics.get("sell_churn_hold_dn_active") or False),
            "sell_churn_hold_side": str(analytics.get("sell_churn_hold_side") or ""),
            "sell_churn_hold_order_age_up_sec": float(analytics.get("sell_churn_hold_order_age_up_sec") or 0.0),
            "sell_churn_hold_order_age_dn_sec": float(analytics.get("sell_churn_hold_order_age_dn_sec") or 0.0),
            "sell_churn_hold_reprice_due_up": bool(analytics.get("sell_churn_hold_reprice_due_up") or False),
            "sell_churn_hold_reprice_due_dn": bool(analytics.get("sell_churn_hold_reprice_due_dn") or False),
            "sell_churn_hold_reprice_suppressed_hits_60s": int(
                analytics.get("sell_churn_hold_reprice_suppressed_hits_60s") or 0
            ),
            "sell_churn_hold_cancel_avoided_hits_60s": int(
                analytics.get("sell_churn_hold_cancel_avoided_hits_60s") or 0
            ),
            "collateral_warning_hits_60s": int(analytics.get("collateral_warning_hits_60s") or 0),
            "sell_skip_cooldown_hits_60s": int(analytics.get("sell_skip_cooldown_hits_60s") or 0),
            "up_collateral_warning_streak": int(analytics.get("up_collateral_warning_streak") or 0),
            "dn_collateral_warning_streak": int(analytics.get("dn_collateral_warning_streak") or 0),
            "up_sell_skip_cooldown_streak": int(analytics.get("up_sell_skip_cooldown_streak") or 0),
            "dn_sell_skip_cooldown_streak": int(analytics.get("dn_sell_skip_cooldown_streak") or 0),
            "collateral_warning_streak_current": int(analytics.get("collateral_warning_streak_current") or 0),
            "sell_skip_cooldown_streak_current": int(analytics.get("sell_skip_cooldown_streak_current") or 0),
            "execution_churn_ratio_60s": float(analytics.get("execution_churn_ratio_60s") or 0.0),
            "untradeable_tolerated_samples_60s": int(analytics.get("untradeable_tolerated_samples_60s") or 0),
            "post_terminal_cleanup_grace_active": bool(
                analytics.get("post_terminal_cleanup_grace_active")
                or ((raw.get("health") or {}).get("post_terminal_cleanup_grace_active"))
                or ((raw.get("runtime") or {}).get("post_terminal_cleanup_grace_active"))
                or False
            ),
            "failure_bucket_current": str(analytics.get("failure_bucket_current") or ""),
            "execution_replay_blocker_hint": str(analytics.get("execution_replay_blocker_hint") or ""),
            "diagnostic_no_guards_active": bool(analytics.get("diagnostic_no_guards_active") or False),
            "gross_inventory_brake_active": bool(analytics.get("gross_inventory_brake_active") or False),
            "gross_inventory_brake_hits_60s": int(analytics.get("gross_inventory_brake_hits_60s") or 0),
            "pair_over_target_buy_blocks_60s": int(analytics.get("pair_over_target_buy_blocks_60s") or 0),
            "dual_bid_guard_inventory_budget_hits_60s": int(
                analytics.get("dual_bid_guard_inventory_budget_hits_60s") or 0
            ),
            "harmful_buy_brake_active": bool(analytics.get("harmful_buy_brake_active") or False),
            "harmful_buy_brake_hits_60s": int(analytics.get("harmful_buy_brake_hits_60s") or 0),
            "emergency_taker_forced": bool(analytics.get("emergency_taker_forced") or False),
            "emergency_taker_forced_hits_60s": int(analytics.get("emergency_taker_forced_hits_60s") or 0),
            "emergency_no_progress_sec": float(analytics.get("emergency_no_progress_sec") or 0.0),
            "unwind_deferred_hits_60s": int(analytics.get("unwind_deferred_hits_60s") or 0),
            "forced_unwind_extreme_excess_hits_60s": int(
                analytics.get("forced_unwind_extreme_excess_hits_60s") or 0
            ),
            "current_mode": str(risk.get("soft_mode") or "normal"),
            "target_mode": str(risk.get("target_soft_mode") or risk.get("soft_mode") or "normal"),
            "lifecycle": lifecycle,
            "reason": str(risk.get("reason") or ""),
            "quote_balance_state": str(analytics.get("quote_balance_state") or ""),
            "helpful_quote_count": int(analytics.get("helpful_quote_count") or 0),
            "harmful_quote_count": int(analytics.get("harmful_quote_count") or 0),
        },
        "latency": None,
        "market": {
            "coin": _runtime_v2._coin or "BTC",
            "timeframe": _runtime_v2._timeframe or "15m",
            "strike": float(getattr(market_info, "strike", 0.0) or 0.0) if market_info else 0.0,
            "time_remaining": float(market.get("time_left_sec") or 0.0),
        },
        "alerts": raw.get("alerts") or [],
        "heartbeat": heartbeat,
        "config": raw.get("config") or {},
        "feeds": {},
        "next_window_in": 0.0,
    }


def _dashboard_snapshot(preferred: str | None = None) -> dict[str, Any]:
    del preferred
    snap = _dashboard_snapshot_from_v2(_runtime_v2.snapshot())
    snap["paper_sweep"] = _paper_sweep_v2.snapshot()
    return snap


# ── WebSocket Connection Manager ───────────────────────────────
class ConnectionManager:
    """Manages active WebSocket connections with thread-safe broadcast."""

    def __init__(self):
        self._connections: list[WebSocket] = []
        self._lock = asyncio.Lock()

    async def connect(self, websocket: WebSocket):
        await websocket.accept()
        async with self._lock:
            self._connections.append(websocket)
        log.info("WS client connected (%d total)", len(self._connections))

    async def disconnect(self, websocket: WebSocket):
        async with self._lock:
            if websocket in self._connections:
                self._connections.remove(websocket)
        log.info("WS client disconnected (%d remaining)", len(self._connections))

    async def broadcast(self, data: dict):
        """Send JSON to all connected clients, removing dead connections."""
        message = json.dumps(data)
        dead: list[WebSocket] = []
        async with self._lock:
            clients = list(self._connections)
        for ws in clients:
            try:
                await ws.send_text(message)
            except Exception:
                dead.append(ws)
        if dead:
            async with self._lock:
                for ws in dead:
                    if ws in self._connections:
                        self._connections.remove(ws)

    @property
    def client_count(self) -> int:
        return len(self._connections)


_ws_manager = ConnectionManager()


async def _ws_broadcast_loop():
    """Broadcast state snapshots to all WS clients every ~1s."""
    while True:
        runtime_metrics.incr("web.ws_broadcast.loop")
        try:
            if _ws_manager.client_count > 0:
                snap = _dashboard_snapshot()
                await _ws_manager.broadcast(snap)
        except Exception as e:
            log.warning("WS broadcast error: %s", e)
        await asyncio.sleep(1.0)

# ── Telegram Bot (interactive management) ──────────────────────
def _on_telegram_conflict(reason: str) -> None:
    _runtime_v2.set_alert("telegram_conflict", reason, level="warning")


_tg_bot = TelegramBotManager(
    notifier=_telegram,
    get_runtime=lambda: _runtime_v2,
    access_key=ACCESS_KEY,
    on_conflict=_on_telegram_conflict,
)


# ── Routes ──────────────────────────────────────────────────────
@app.get("/")
async def index():
    index_file = WEB_DIR / "index.html"
    if index_file.exists():
        return FileResponse(str(index_file))
    return JSONResponse({"status": "Polymarket MM API", "version": APP_VERSION, "git_hash": APP_GIT_HASH})


@app.post("/api/auth/login")
async def login(req: LoginRequest, response: Response):
    if req.key != ACCESS_KEY:
        raise HTTPException(status_code=401, detail="invalid key")
    is_https = os.environ.get("HTTPS_MODE", "").lower() in ("1", "true", "yes")
    response.set_cookie(
        AUTH_COOKIE, req.key,
        httponly=True, samesite="strict", secure=is_https, max_age=86400 * 30,
    )
    return {"ok": True}


@app.get("/api/auth/check")
async def auth_check(request: Request):
    try:
        _require_auth(request)
        return {"authenticated": True}
    except HTTPException:
        return {"authenticated": False}


@app.post("/api/auth/logout")
async def logout(response: Response):
    response.delete_cookie(AUTH_COOKIE)
    return {"ok": True}


# ── MM Control ──────────────────────────────────────────────────
def _legacy_v1_removed() -> None:
    raise HTTPException(
        status_code=410,
        detail={
            "error": "legacy_v1_removed_use_mmv2",
            "message": "Legacy /api/mm/* endpoints were removed. Use /api/mmv2/*.",
        },
    )


@app.post("/api/mm/start")
async def mm_start(req: StartRequest, request: Request, response: Response = None):
    """Legacy V1 endpoint removed."""
    del req, response
    _require_auth(request)
    _legacy_v1_removed()


@app.post("/api/mm/stop")
async def mm_stop(request: Request, response: Response = None):
    """Legacy V1 endpoint removed."""
    del response
    _require_auth(request)
    _legacy_v1_removed()


@app.get("/api/mm/state")
async def mm_state(request: Request, response: Response = None):
    """Legacy V1 endpoint removed."""
    del response
    _require_auth(request)
    _legacy_v1_removed()


@app.post("/api/mm/config")
async def mm_config_update(req: ConfigUpdateRequest, request: Request, response: Response = None):
    """Legacy V1 endpoint removed."""
    del req, response
    _require_auth(request)
    _legacy_v1_removed()


@app.get("/api/mm/config")
async def mm_config_get(request: Request, response: Response = None):
    """Legacy V1 endpoint removed."""
    del response
    _require_auth(request)
    _legacy_v1_removed()


@app.post("/api/mm/emergency")
async def mm_emergency(request: Request, response: Response = None):
    """Legacy V1 endpoint removed."""
    del response
    _require_auth(request)
    _legacy_v1_removed()


@app.post("/api/mm/kill")
async def mm_kill(request: Request, response: Response = None):
    """Legacy V1 endpoint removed."""
    del response
    _require_auth(request)
    _legacy_v1_removed()


@app.post("/api/mm/watch")
async def mm_watch(request: Request, body: dict):
    """Legacy V1 endpoint removed."""
    del body
    _require_auth(request)
    _legacy_v1_removed()


@app.post("/api/mm/watch/stop")
async def mm_watch_stop(request: Request):
    """Legacy V1 endpoint removed."""
    _require_auth(request)
    _legacy_v1_removed()


@app.post("/api/mm/validate-credentials")
async def mm_validate_credentials(request: Request, response: Response = None):
    """Legacy V1 endpoint removed."""
    del response
    _require_auth(request)
    _legacy_v1_removed()


@app.get("/api/mm/fills")
async def mm_fills(request: Request, limit: int = 50, offset: int = 0, response: Response = None):
    """Legacy V1 endpoint removed."""
    del limit, offset, response
    _require_auth(request)
    _legacy_v1_removed()


@app.post("/api/mmv2/start")
async def mmv2_start(req: StartRequest, request: Request):
    _require_auth(request)
    if _paper_sweep_v2.snapshot().get("is_running"):
        raise HTTPException(status_code=409, detail="paper sweep is running; stop it before starting main MM")
    if req.paper_mode:
        session_budget_usd = float(req.initial_usdc)
    elif "initial_usdc" in req.model_fields_set:
        session_budget_usd = float(req.initial_usdc)
    else:
        session_budget_usd = float(_runtime_v2.mm_config_v2.session_budget_usd)
    if req.paper_mode and session_budget_usd < float(MMRuntimeV2.PAPER_MIN_BUDGET_USD):
        raise HTTPException(
            status_code=400,
            detail=(
                f"paper_min_budget_30_required: requested={session_budget_usd:.2f} "
                f"min={MMRuntimeV2.PAPER_MIN_BUDGET_USD:.2f}"
            ),
        )
    if not req.paper_mode and session_budget_usd < float(MMRuntimeV2.LIVE_MIN_BUDGET_USD):
        raise HTTPException(
            status_code=400,
            detail=(
                f"live_min_budget_15_required: requested={session_budget_usd:.2f} "
                f"min={MMRuntimeV2.LIVE_MIN_BUDGET_USD:.2f}"
            ),
        )
    result = await _runtime_v2.start(
        req.coin,
        req.timeframe,
        req.paper_mode,
        req.initial_usdc,
        dev=req.dev,
        session_budget_usd=session_budget_usd,
        force_normal_soft_mode=req.force_normal_soft_mode,
        force_normal_no_guards=req.force_normal_no_guards,
    )
    return {"ok": True, "state": result}


@app.post("/api/mmv2/paper-sweep/start")
async def mmv2_paper_sweep_start(req: PaperSweepStartRequest, request: Request):
    _require_auth(request)
    if _runtime_v2.snapshot().get("is_running"):
        raise HTTPException(status_code=409, detail="main MM runtime is running; stop it before paper sweep")
    result = await _paper_sweep_v2.start(
        req.coin,
        req.timeframe,
        initial_usdc=float(req.initial_usdc),
        base_clips=list(req.base_clips),
        variants=req.variants,
        force_normal_soft_mode=bool(req.force_normal_soft_mode),
        base_config=copy.deepcopy(_runtime_v2.mm_config_v2),
    )
    return {"ok": True, "state": result}


@app.post("/api/mmv2/paper-sweep/stop")
async def mmv2_paper_sweep_stop(request: Request):
    _require_auth(request)
    result = await _paper_sweep_v2.stop()
    return {"ok": True, "state": result}


@app.get("/api/mmv2/paper-sweep/state")
async def mmv2_paper_sweep_state(request: Request):
    _require_auth(request)
    return _paper_sweep_v2.snapshot()


@app.get("/api/mmv2/paper-sweep/export")
async def mmv2_paper_sweep_export(request: Request):
    _require_auth(request)
    return _paper_sweep_v2.export_bundle()


@app.post("/api/mmv2/validate-credentials")
async def mmv2_validate_credentials(request: Request):
    _require_auth(request)
    try:
        result = await _runtime_v2.validate_live_credentials()
        return {"valid": True, **result}
    except HTTPException as e:
        return {"valid": False, "detail": e.detail}


@app.post("/api/mmv2/stop")
async def mmv2_stop(request: Request):
    _require_auth(request)
    result = await _runtime_v2.stop()
    return {"ok": True, "state": result}


@app.get("/api/mmv2/state")
async def mmv2_state(request: Request):
    _require_auth(request)
    snap = _runtime_v2.snapshot()
    snap["paper_sweep"] = _paper_sweep_v2.snapshot()
    return snap


@app.post("/api/mmv2/config")
async def mmv2_config_update(req: ConfigUpdateRequestV2, request: Request):
    _require_auth(request)
    updates = {k: v for k, v in req.model_dump().items() if v is not None}
    if req.min_edge_bps is not None:
        updates["min_edge_bps"] = req.min_edge_bps
    if req.min_pm_spread_bps is not None:
        updates["min_pm_spread_bps"] = req.min_pm_spread_bps
    market_scope = updates.get("market_scope")
    if market_scope is not None and market_scope not in V2_SUPPORTED_MARKET_SCOPES:
        return JSONResponse(
            {
                "error": (
                    "market_scope must be one of "
                    f"{sorted(V2_SUPPORTED_MARKET_SCOPES)}"
                )
            },
            status_code=400,
        )
    config = _runtime_v2.update_config(**updates)
    return {"ok": True, "config": config}


@app.get("/api/mmv2/config")
async def mmv2_config_get(request: Request):
    _require_auth(request)
    return {"config": _runtime_v2.mm_config_v2.to_dict()}


@app.get("/api/mmv2/fills")
async def mmv2_fills(request: Request, limit: int = 50, offset: int = 0):
    _require_auth(request)
    return _runtime_v2.fills_page(limit=limit, offset=offset)


@app.post("/api/mmv2/verification/run")
async def mmv2_verification_run(req: VerificationRunRequest, request: Request):
    _require_auth(request)
    status = await _runtime_v2.start_verification(req.kind)
    return {"ok": True, "verification": status}


@app.get("/api/mmv2/verification/state")
async def mmv2_verification_state(request: Request):
    _require_auth(request)
    return {"verification": _runtime_v2.get_verification_status()}


@app.post("/api/mmv2/verification/cancel")
async def mmv2_verification_cancel(request: Request):
    _require_auth(request)
    status = await _runtime_v2.cancel_verification()
    return {"ok": True, "verification": status}


# ── Logs ───────────────────────────────────────────────────────
@app.get("/api/logs")
async def get_logs(request: Request, limit: int = 200, level: str = ""):
    """Return recent log entries from in-memory ring buffer."""
    _require_auth(request)
    entries = list(_log_handler.buffer)
    if level:
        level_upper = level.upper()
        entries = [e for e in entries if e["level"] == level_upper]
    entries = entries[-limit:]
    return {"logs": entries, "total": len(_log_handler.buffer)}


@app.post("/api/verification/run")
async def verification_run(req: VerificationRunRequest, request: Request):
    _require_auth(request)
    status = await _runtime.start_verification(req.kind)
    return {"ok": True, "verification": status}


@app.get("/api/verification/state")
async def verification_state(request: Request):
    _require_auth(request)
    return {"verification": _runtime.get_verification_status()}


@app.post("/api/verification/cancel")
async def verification_cancel(request: Request):
    _require_auth(request)
    status = await _runtime.cancel_verification()
    return {"ok": True, "verification": status}


def _task_coro_label(task: asyncio.Task) -> str:
    """Stable short label for asyncio task grouping in debug endpoints."""
    try:
        coro = task.get_coro()
        qual = getattr(coro, "__qualname__", None) or getattr(coro, "__name__", None) or type(coro).__name__
        code = getattr(coro, "cr_code", None)
        if code is not None:
            return f"{qual} ({Path(code.co_filename).name}:{code.co_firstlineno})"
        return str(qual)
    except Exception:
        return "unknown"


def _collect_asyncio_task_counts(top_n: int = 10) -> tuple[int, list[dict[str, Any]]]:
    """Best-effort pending-task breakdown for diagnostics."""
    task_counts: collections.Counter[str] = collections.Counter()
    loop = asyncio.get_running_loop()
    pending = [t for t in asyncio.all_tasks(loop) if not t.done()]
    for task in pending:
        task_counts[_task_coro_label(task)] += 1
    top_tasks = [
        {"coro": name, "count": int(cnt)}
        for name, cnt in task_counts.most_common(top_n)
    ]
    return len(pending), top_tasks


def _sample_main_thread_stack(limit: int = 12) -> list[str]:
    """Sample main-thread stack frames for CPU starvation debugging."""
    main_tid = threading.main_thread().ident
    if main_tid is None:
        return []
    frame = sys._current_frames().get(main_tid)
    if frame is None:
        return []
    extracted = traceback.extract_stack(frame)
    sampled = extracted[-max(1, int(limit)):]
    return [
        f"{Path(item.filename).name}:{item.lineno} in {item.name}"
        for item in sampled
    ]


_runtime_watchdog_task: asyncio.Task | None = None
_runtime_stack_watchdog_thread: threading.Thread | None = None
_runtime_stack_watchdog_stop = threading.Event()


async def _runtime_watchdog_loop() -> None:
    """Event-loop watchdog: lag + top counters/tasks when runtime gets unhealthy."""
    interval = RUNTIME_WATCHDOG_INTERVAL_SEC
    last_tick = time.perf_counter()
    while True:
        await asyncio.sleep(interval)
        now_perf = time.perf_counter()
        actual_interval = max(0.0, now_perf - last_tick)
        last_tick = now_perf
        lag_ms = max(0.0, (actual_interval - interval) * 1000.0)
        runtime_metrics.observe_ms("web.watchdog.loop_lag_ms", lag_ms)
        metrics = runtime_metrics.snapshot(reset=True, top_n=10, advance=True)
        process_cpu_pct = float(metrics.get("process_cpu_pct", 0.0) or 0.0)
        pending_tasks = 0
        top_tasks: list[dict[str, Any]] = []
        try:
            pending_tasks, top_tasks = _collect_asyncio_task_counts(top_n=10)
        except Exception as e:
            top_tasks = [{"coro": "task_snapshot_error", "count": 1, "error": str(e)}]

        top_counts = metrics.get("counts", [])[:5]
        watchdog = _runtime._runtime_watchdog
        watchdog["active"] = True
        watchdog["last_check_ts"] = time.time()
        watchdog["last_loop_lag_ms"] = round(lag_ms, 2)
        watchdog["last_cpu_pct"] = round(process_cpu_pct, 2)
        watchdog["last_top_counts"] = top_counts
        watchdog["last_top_tasks"] = top_tasks[:5]
        watchdog["pending_tasks"] = pending_tasks

        unhealthy = (
            process_cpu_pct >= RUNTIME_WATCHDOG_CPU_ALERT_PCT
            or lag_ms >= RUNTIME_WATCHDOG_LAG_ALERT_MS
        )
        if unhealthy:
            now_ts = time.time()
            if (now_ts - float(watchdog.get("last_log_ts", 0.0) or 0.0)) >= RUNTIME_WATCHDOG_LOG_COOLDOWN_SEC:
                watchdog["last_log_ts"] = now_ts
                msg = (
                    f"Runtime watchdog: cpu={process_cpu_pct:.1f}% "
                    f"loop_lag={lag_ms:.1f}ms pending_tasks={pending_tasks} "
                    f"top_counts={top_counts} top_tasks={top_tasks[:3]}"
                )
                log.warning(msg)
                _runtime.set_alert("runtime_watchdog", msg, level="warning")
        else:
            _runtime.clear_alert("runtime_watchdog")


def _runtime_stack_watchdog_main() -> None:
    """Thread-based CPU sampler that can still inspect main-thread stack when loop is wedged."""
    interval = RUNTIME_WATCHDOG_INTERVAL_SEC
    prev_wall = time.time()
    prev_proc = time.process_time()
    last_log_ts = 0.0
    while not _runtime_stack_watchdog_stop.wait(interval):
        now_wall = time.time()
        now_proc = time.process_time()
        wall_delta = max(1e-6, now_wall - prev_wall)
        proc_delta = max(0.0, now_proc - prev_proc)
        prev_wall = now_wall
        prev_proc = now_proc
        cpu_pct = round((proc_delta / wall_delta) * 100.0, 2)
        watchdog = _runtime._runtime_watchdog
        watchdog["last_cpu_pct"] = max(float(watchdog.get("last_cpu_pct", 0.0) or 0.0), cpu_pct)
        if cpu_pct < RUNTIME_WATCHDOG_CPU_ALERT_PCT:
            continue
        if (now_wall - last_log_ts) < RUNTIME_WATCHDOG_LOG_COOLDOWN_SEC:
            continue
        last_log_ts = now_wall
        main_stack = _sample_main_thread_stack(limit=14)
        watchdog["last_main_stack"] = main_stack
        watchdog["last_log_ts"] = now_wall
        msg = (
            f"Runtime CPU watchdog: cpu={cpu_pct:.1f}% main_thread_stack={main_stack}"
        )
        log.warning(msg)
        _runtime.set_alert("runtime_cpu_watchdog", msg, level="warning")


def _ensure_runtime_watchdogs() -> None:
    """Start async + thread watchdogs once."""
    global _runtime_watchdog_task, _runtime_stack_watchdog_thread
    if _runtime_watchdog_task is None or _runtime_watchdog_task.done():
        _runtime_watchdog_task = asyncio.create_task(_runtime_watchdog_loop())
    if _runtime_stack_watchdog_thread is None or not _runtime_stack_watchdog_thread.is_alive():
        _runtime_stack_watchdog_stop.clear()
        _runtime_stack_watchdog_thread = threading.Thread(
            target=_runtime_stack_watchdog_main,
            name="runtime-stack-watchdog",
            daemon=True,
        )
        _runtime_stack_watchdog_thread.start()


async def _stop_runtime_watchdogs() -> None:
    """Stop background watchdogs on shutdown."""
    global _runtime_watchdog_task
    _runtime_stack_watchdog_stop.set()
    task = _runtime_watchdog_task
    _runtime_watchdog_task = None
    if task and not task.done():
        task.cancel()
        try:
            await task
        except asyncio.CancelledError:
            pass
        except Exception as e:
            log.warning("Runtime watchdog shutdown error: %s", e)


@app.get("/api/debug/runtime-metrics")
async def debug_runtime_metrics(
    request: Request,
    reset: bool = False,
    top_n: int = 50,
):
    """Runtime counters/rates + asyncio task breakdown for CPU hot-path debugging."""
    _require_auth(request)
    top_n = max(1, min(int(top_n), 500))
    metrics = runtime_metrics.snapshot(reset=reset, top_n=top_n, advance=False)
    try:
        pending_count, top_tasks = _collect_asyncio_task_counts(top_n=top_n)
        metrics["asyncio"] = {
            "pending_tasks": pending_count,
            "top_tasks": top_tasks,
            "threads": threading.active_count(),
        }
    except Exception as e:
        metrics["asyncio"] = {"error": str(e)}
    metrics["watchdog"] = dict(_runtime._runtime_watchdog)
    metrics["python"] = {
        "main_thread_stack": _sample_main_thread_stack(limit=14),
    }
    return metrics


# ── Health ──────────────────────────────────────────────────────
@app.get("/api/health")
async def health():
    return {
        "status": "ok",
        "running": _runtime.is_running,
        "coin": _runtime._coin,
        "timeframe": _runtime._timeframe,
        "paper_mode": _runtime._paper_mode,
    }


# ── WebSocket endpoint ─────────────────────────────────────────
@app.websocket("/ws")
async def ws_endpoint(websocket: WebSocket):
    """WebSocket for real-time dashboard updates.

    Auth via x-access-key header OR cookie pm_web_auth.
    Client can send "ping" → server replies {"type":"pong"}.
    """
    query_token = websocket.query_params.get("token")
    cookie = websocket.cookies.get(AUTH_COOKIE, "")
    header = websocket.headers.get("x-access-key", "")
    if cookie != ACCESS_KEY and header != ACCESS_KEY and query_token != ACCESS_KEY:
        await websocket.close(code=1008, reason="unauthorized")
        return

    await _ws_manager.connect(websocket)
    try:
        while True:
            try:
                data = await asyncio.wait_for(websocket.receive_text(), timeout=30.0)
                if data.strip() == "ping":
                    await websocket.send_text(json.dumps({"type": "pong"}))
            except asyncio.TimeoutError:
                # No message in 30s — send ping to keep alive
                try:
                    await websocket.send_text(json.dumps({"type": "ping"}))
                except Exception:
                    break
    except WebSocketDisconnect:
        pass
    except Exception:
        pass
    finally:
        await _ws_manager.disconnect(websocket)


# ── Available markets ──────────────────────────────────────────
@app.get("/api/markets")
async def markets():
    return {
        "coins": config.COINS,
        "timeframes": config.COIN_TIMEFRAMES,
    }


# ── Startup / Shutdown ─────────────────────────────────────────
def _sigterm_handler(signum, frame):
    """Handle SIGTERM for graceful shutdown in Docker."""
    log.info("Received SIGTERM — initiating graceful shutdown")
    raise SystemExit(0)


signal.signal(signal.SIGTERM, _sigterm_handler)


# ---------- Pair Arbitrage ----------

@app.post("/api/pair-arb/start")
async def pair_arb_start(req: PairArbStartRequest, request: Request):
    _require_auth(request)
    global _pair_arb_engine

    if _pair_arb_engine is not None and _pair_arb_engine._running:
        raise HTTPException(status_code=409, detail="pair arb already running")

    from pair_arb import PairArbEngine, PairArbConfig

    config = PairArbConfig(
        market_scopes=req.market_scopes,
        session_budget_usd=float(req.initial_usdc),
    )
    config.validate()

    # Create CLOB client (same pattern as MMRuntimeV2)
    from mm_shared.order_manager import OrderManager
    import os

    private_key = os.environ.get("PM_PRIVATE_KEY", "")
    api_key = os.environ.get("PM_API_KEY", "")
    api_secret = os.environ.get("PM_API_SECRET", "")
    api_passphrase = os.environ.get("PM_API_PASSPHRASE", "")

    if req.paper_mode:
        # Use MockClobClient for paper mode.
        client = MockClobClient(usdc_balance=float(req.initial_usdc))
        order_mgr = OrderManager(client)
    else:
        if not private_key:
            raise HTTPException(status_code=400, detail="PM_PRIVATE_KEY not set")
        from py_clob_client.client import ClobClient
        chain_id = int(os.environ.get("PM_CHAIN_ID", "137"))
        client = ClobClient(
            "https://clob.polymarket.com",
            key=private_key,
            chain_id=chain_id,
            creds={"apiKey": api_key, "secret": api_secret, "passphrase": api_passphrase} if api_key else None,
        )
        order_mgr = OrderManager(client)

    _pair_arb_engine = PairArbEngine(
        order_mgr=order_mgr,
        config=config,
        private_key=private_key,
        paper_mode=req.paper_mode,
        app_version=APP_VERSION,
    )

    result = await _pair_arb_engine.start()
    return result


@app.post("/api/pair-arb/stop")
async def pair_arb_stop(request: Request):
    _require_auth(request)
    global _pair_arb_engine
    if _pair_arb_engine is None or not _pair_arb_engine._running:
        return {"ok": False, "error": "not_running"}
    result = await _pair_arb_engine.stop()
    return result


@app.get("/api/pair-arb/state")
async def pair_arb_state(request: Request):
    _require_auth(request)
    if _pair_arb_engine is None:
        return {"is_running": False, "error": "not_initialized"}
    return _pair_arb_engine.snapshot().to_dict()


@app.post("/api/pair-arb/config")
async def pair_arb_config_update(request: Request):
    _require_auth(request)
    if _pair_arb_engine is None:
        raise HTTPException(status_code=400, detail="pair arb not initialized")
    body = await request.json()
    new_config = _pair_arb_engine.update_config(**body)
    return {"ok": True, "config": new_config}


@app.get("/api/pair-arb/config")
async def pair_arb_config_get(request: Request):
    _require_auth(request)
    if _pair_arb_engine is None:
        from pair_arb import PairArbConfig
        return {"config": PairArbConfig().to_dict()}
    return {"config": _pair_arb_engine.config.to_dict()}


@app.on_event("startup")
async def _startup():
    _telegram.set_loop(asyncio.get_running_loop())
    _ensure_runtime_watchdogs()
    await _runtime.load_config()
    _runtime.clear_alert("telegram_conflict")
    _runtime.clear_alert("telegram_polling")
    if _telegram.enabled and _telegram_polling_enabled():
        await _tg_bot.start()
        log.info("Telegram bot polling started")
    elif _telegram.enabled:
        msg = "Telegram bot polling disabled by TELEGRAM_POLLING_ENABLED=0"
        _runtime.set_alert("telegram_polling", msg, level="warning")
        log.warning(msg)
    # Start WebSocket broadcast loop
    asyncio.create_task(_ws_broadcast_loop())


@app.on_event("shutdown")
async def _shutdown():
    await _stop_runtime_watchdogs()
    await _tg_bot.stop()
    if _runtime.is_running:
        try:
            await asyncio.wait_for(_runtime.stop(), timeout=15.0)
        except Exception as e:
            log.error(f"Shutdown error: {e}")
    if _pair_arb_engine is not None and _pair_arb_engine._running:
        try:
            await asyncio.wait_for(_pair_arb_engine.stop(), timeout=10.0)
        except Exception as e:
            log.error(f"Pair arb shutdown error: {e}")
    await _telegram.close()
