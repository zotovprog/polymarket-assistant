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
import sys
import time
import logging
from pathlib import Path
from typing import Any, Optional

from fastapi import FastAPI, HTTPException, Request, Response
from fastapi.responses import FileResponse, JSONResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel

# ── Path setup ──────────────────────────────────────────────────
BASE_DIR = Path(__file__).resolve().parent
SRC_DIR = BASE_DIR / "src"
if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))

import config
import feeds
from mm.types import MarketInfo, Inventory
from mm.mm_config import MMConfig
from mm.market_maker import MarketMaker
from telegram_notifier import TelegramNotifier
from telegram_bot import TelegramBotManager

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
log.info(f"PM_API_KEY={'set' if PM_API_KEY else 'MISSING'} (len={len(PM_API_KEY)}, prefix={PM_API_KEY[:8]}...)" if PM_API_KEY else "PM_API_KEY=MISSING")
log.info(f"PM_API_SECRET={'set' if PM_API_SECRET else 'MISSING'} (len={len(PM_API_SECRET)})")
log.info(f"PM_API_PASSPHRASE={'set' if PM_API_PASSPHRASE else 'MISSING'} (len={len(PM_API_PASSPHRASE)})")

# ── Telegram ────────────────────────────────────────────────────
_telegram = TelegramNotifier()
log.info(f"Telegram: {'enabled' if _telegram.enabled else 'disabled'}")

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


class ConfigUpdateRequest(BaseModel):
    half_spread_bps: Optional[float] = None
    min_spread_bps: Optional[float] = None
    max_spread_bps: Optional[float] = None
    vol_spread_mult: Optional[float] = None
    order_size_usd: Optional[float] = None
    min_order_size_usd: Optional[float] = None
    max_order_size_usd: Optional[float] = None
    max_inventory_shares: Optional[float] = None
    skew_bps_per_unit: Optional[float] = None
    requote_interval_sec: Optional[float] = None
    requote_threshold_bps: Optional[float] = None
    gtd_duration_sec: Optional[int] = None
    heartbeat_interval_sec: Optional[int] = None
    use_post_only: Optional[bool] = None
    use_gtd: Optional[bool] = None
    max_drawdown_usd: Optional[float] = None
    volatility_pause_mult: Optional[float] = None
    max_loss_per_fill_usd: Optional[float] = None
    take_profit_usd: Optional[float] = None
    trailing_stop_pct: Optional[float] = None
    max_one_sided_ticks: Optional[int] = None
    close_window_sec: Optional[float] = None
    auto_next_window: Optional[bool] = None
    resolution_wait_sec: Optional[float] = None
    liq_price_floor_enabled: Optional[bool] = None
    liq_gradual_chunks: Optional[int] = None
    liq_chunk_interval_sec: Optional[float] = None
    liq_taker_threshold_sec: Optional[float] = None
    liq_max_discount_from_fv: Optional[float] = None
    liq_abandon_below_floor: Optional[bool] = None
    enabled: Optional[bool] = None


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
        # Fair values per token_id, set by market_maker via set_fair_values()
        self._fair_values: dict[str, float] = {}
        self._pm_prices: dict = {}  # {"up": mid, "dn": mid} from real PM WS feed
        self._up_token: str | None = None
        self._dn_token: str | None = None

    @property
    def balance(self) -> float:
        return self._usdc_balance

    def get_balance(self) -> float:
        return self._usdc_balance

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
        collateral = self._required_collateral(side, size, price)

        if side == "BUY" and self._usdc_balance < collateral:
            return {"error_msg": "not enough balance", "status": "error"}

        if side == "BUY":
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
        }
        return {"orderID": oid}

    def cancel(self, order_id: str) -> dict:
        order = self._orders.pop(order_id, None)
        if (order and order.get("status") == "LIVE"
                and order.get("side") == "BUY"):
            # Refund only unfilled portion's collateral
            filled = float(order.get("size_matched", 0))
            unfilled = max(0.0, order["size"] - filled)
            refund = unfilled * order["price"]
            self._usdc_balance += refund
        return {"success": True}

    def cancel_all(self) -> dict:
        for order in self._orders.values():
            if order.get("status") == "LIVE" and order.get("side") == "BUY":
                filled = float(order.get("size_matched", 0))
                unfilled = max(0.0, order["size"] - filled)
                refund = unfilled * order["price"]
                self._usdc_balance += refund
        self._orders.clear()
        return {"success": True}

    def _compute_fill_prob(self, order: dict) -> float:
        """Compute fill probability based on price distance from fair value.

        BUY closer to (or above) FV → more likely to fill.
        SELL closer to (or below) FV → more likely to fill.
        Age bonus capped at 2x base probability.
        """
        age = self._tick_count - order.get("created_tick", 0)
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

            if order["side"] == "SELL" and not order.get("fill_credit_applied"):
                self._usdc_balance += order["size_matched"] * order["price"]
                order["fill_credit_applied"] = True
            elif (order["side"] == "SELL" and order.get("fill_credit_applied")
                  and order["status"] == "MATCHED"):
                # Additional credit for partial → full transition
                prev_credit = (order["size_matched"] - fill_size) * order["price"]
                total_credit = order["size"] * order["price"]
                self._usdc_balance += total_credit - prev_credit

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
        self.mm: Optional[MarketMaker] = None
        self.mm_config: MMConfig = MMConfig()
        self._feed_tasks: list[asyncio.Task] = []
        self._running = False
        self._coin: str = ""
        self._timeframe: str = ""
        self._paper_mode: bool = True
        self._dev_mode: bool = False
        self._initial_usdc: float = 1000.0
        self._next_window_at: float = 0.0  # timestamp when next window starts
        self._mongo = None  # MongoLogger (if MONGO_URI set)
        self._mongo_log_handler = None  # MongoLogHandler (if active)
        self._pnl_history: list[tuple[float, float]] = []  # [(timestamp, session_pnl), ...]
        self._watching = False
        self._start_balance: float = 0.0  # PM USDC balance at session start

    @property
    def is_running(self) -> bool:
        return self._running and self.mm is not None

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

    async def start(self, coin: str, timeframe: str, paper_mode: bool = True,
                    initial_usdc: float = 1000.0, dev: bool = False) -> dict:
        """Start feeds and market maker."""
        if self._running:
            raise HTTPException(status_code=400, detail="Already running")

        if self._watching:
            await self.stop_watch()

        initial_usdc = float(initial_usdc)
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

        # Validate credentials before going live
        if not paper_mode:
            log.info("Live mode — validating API credentials...")
            await self.validate_live_credentials()
            log.info("Credentials validated OK")
            # One-time on-chain approvals for SELL orders (neg-risk markets)
            log.info("Checking on-chain approvals for neg-risk trading...")
            from mm.approvals import _do_approvals
            approval_result = await asyncio.to_thread(_do_approvals, PM_PRIVATE_KEY)
            if approval_result.get("error"):
                raise HTTPException(status_code=500, detail=f"Approval setup failed: {approval_result['error']}")
            if not approval_result.get("all_ok", False):
                log.warning(f"Some approvals failed: {approval_result}")
            else:
                log.info(f"All approvals OK: {approval_result}")

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
                market = self._build_market_info_from_tokens(
                    coin, timeframe, up_id, dn_id, condition_id=cond_id)
                if market is None:
                    raise HTTPException(
                        status_code=500,
                        detail="Failed to determine strike price — cannot trade this window"
                    )
            else:
                log.warning("PM tokens not found, using placeholder")
                market = self._build_placeholder_market(coin, timeframe)
        except Exception as e:
            log.warning(f"PM token fetch failed: {e}")
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

        # Create and start market maker
        self.mm = MarketMaker(self.feed_state, clob, self.mm_config)
        # Always set session budget limit for USDC cap enforcement
        self.mm.inventory.initial_usdc = initial_usdc
        if paper_mode:
            self.mm.inventory.usdc = initial_usdc
        self.mm.set_market(market)

        # Set private key for live merge operations
        if not paper_mode and PM_PRIVATE_KEY:
            self.mm._private_key = PM_PRIVATE_KEY

        await self.mm.start()
        self._running = True

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
        if config.MONGO_URI:
            try:
                from mm.mongo_logger import MongoLogger, MongoLogHandler
                self._mongo = MongoLogger(config.MONGO_URI, config.MONGO_DB)
                await self._mongo.start()
                # Fill callback
                self.mm.on_fill(lambda fill, tt: self._mongo.log_fill(
                    fill, tt, self._fill_context()))
                # Snapshot callback
                self.mm.on_snapshot(self._mongo.log_snapshot)
                # Python log handler
                self._mongo_log_handler = MongoLogHandler(self._mongo)
                self._mongo_log_handler.setLevel(logging.INFO)
                logging.getLogger().addHandler(self._mongo_log_handler)
                log.info("MongoLogger attached")
            except Exception as e:
                log.warning(f"MongoLogger init failed (continuing without): {e}")
                self._mongo = None

        # Monitor for window expiry and handle auto-next-window
        asyncio.create_task(self._monitor_window_expiry())

        log.info(f"MM started: {coin}/{timeframe} paper={paper_mode}")
        return self.snapshot()

    async def _monitor_window_expiry(self) -> None:
        """Watch for window expiry; auto-restart next window if configured."""
        while self._running:
            await asyncio.sleep(2.0)
            mm = self.mm
            if not mm:
                break
            # MM stopped itself (window expiry or other reason)
            if not mm._running:
                log.info(f"Monitor: mm._running=False, _is_closing={mm._is_closing}")
            if not mm._running and (mm._is_closing or (mm.market and mm.market.time_remaining <= 0)):
                log.info("Window expired — MM stopped. Cleaning up feeds.")
                await self._send_window_summary()
                self._running = False
                for t in self._feed_tasks:
                    t.cancel()
                self._feed_tasks.clear()

                # Natural window expiry sets mm._running=False without calling mm.stop(),
                # so heartbeat can still be alive. Stop it before rotating window.
                try:
                    if mm.heartbeat.is_running:
                        await mm.heartbeat.stop()
                except Exception as e:
                    log.warning(f"Failed to stop heartbeat after expiry: {e}")

                if self.mm_config.auto_next_window:
                    # Poll for new window tokens instead of fixed wait
                    min_wait = 15.0  # minimum cooldown after expiry
                    max_wait = self.mm_config.resolution_wait_sec
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
                                if strike <= 0:
                                    log.warning(
                                        f"Strike=0 after retries ({elapsed:.0f}s elapsed), "
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
                                from mm.market_quality import MarketQualityAnalyzer
                                from mm.order_manager import OrderManager
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
                            coin=self._coin,
                            timeframe=self._timeframe,
                            paper_mode=self._paper_mode,
                            initial_usdc=self._initial_usdc,
                            dev=self._dev_mode,
                        )
                    except Exception as e:
                        log.error(f"Auto-next-window failed: {e}")
                break

    async def stop(self) -> dict:
        """Stop market maker and feeds."""
        snap = self.snapshot()

        if self.mm:
            await self.mm.stop()

        for t in self._feed_tasks:
            t.cancel()
        self._feed_tasks.clear()

        # Tear down MongoLogger
        if self._mongo_log_handler:
            logging.getLogger().removeHandler(self._mongo_log_handler)
            self._mongo_log_handler = None
        if self._mongo:
            try:
                await self._mongo.stop()
            except Exception:
                pass
            self._mongo = None

        self._running = False
        log.info("MM stopped")
        return snap

    def snapshot(self) -> dict:
        """Get current state for API."""
        if self.mm:
            snap = self.mm.snapshot()
            snap["paper_mode"] = self._paper_mode
            snap["dev_mode"] = self._dev_mode
            snap["session_limit"] = self._initial_usdc
            snap["next_window_in"] = max(0, self._next_window_at - time.time()) if self._next_window_at else 0
            if self._paper_mode:
                client = self.mm.order_mgr.client
                if hasattr(client, "balance"):
                    snap["mock_usdc_balance"] = float(client.balance)
            snap["feeds"] = self._build_feeds_dict()
            return snap

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
        }
        result["feeds"] = self._build_feeds_dict()
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
                "latency_ms": round((now - st.binance_ob_last_ok_ts) * 1000) if st.binance_ob_last_ok_ts else None,
                "uptime_sec": round(now - st.binance_ws_connected_at) if st.binance_ws_connected_at else 0,
            },
            "binance_ob": {
                "ready": st.binance_ob_ready,
                "msg_count": st.binance_ob_msg_count,
                "error_count": st.binance_ob_error_count,
                "last_update_ms_ago": round((now - st.binance_ob_last_ok_ts) * 1000) if st.binance_ob_last_ok_ts else None,
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
        for t in self._feed_tasks:
            t.cancel()
        self._feed_tasks.clear()
        self._watching = False
        self.feed_state = None
        log.info("Watch mode stopped")

    def update_config(self, **kwargs) -> dict:
        """Update MM config at runtime."""
        self.mm_config.update(**kwargs)
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
            await db.config.replace_one(
                {"_id": "mm_config"},
                {"_id": "mm_config", **self.mm_config.to_dict()},
                upsert=True,
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
                self.mm_config.update(**doc)
                log.info("Config loaded from MongoDB: spread=%s, skew=%s",
                         self.mm_config.half_spread_bps, self.mm_config.skew_bps_per_unit)
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

        Returns None if strike cannot be determined (prevents trading with FV=0.5).
        """
        strike, window_start, window_end = feeds.fetch_pm_strike(coin, timeframe)

        if strike <= 0 or window_start <= 0:
            # Fallback: use current Binance mid as strike (imperfect but better than 0)
            log.warning("Could not fetch strike from Binance, trying current mid price")
            now = time.time()
            tf_minutes = {"5m": 5, "15m": 15, "1h": 60, "4h": 240, "daily": 1440}
            window_duration = tf_minutes.get(timeframe, 60) * 60
            strike = self.feed_state.mid if self.feed_state and self.feed_state.mid else 0.0
            if strike <= 0:
                log.error("Strike is 0 and no Binance mid available — cannot trade this window")
                return None
            window_start = now
            window_end = now + window_duration

        log.info(f"Market info: strike={strike:.2f} window=[{window_start:.0f}, {window_end:.0f}]")

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
        try:
            resp = await asyncio.to_thread(
                lambda: _req.get(
                    "https://clob.polymarket.com/book",
                    params={"token_id": market.up_token_id},
                    timeout=10,
                )
            )
            if resp.ok:
                data = resp.json()
                if "tick_size" in data:
                    market.tick_size = float(data["tick_size"])
                if "min_order_size" in data:
                    market.min_order_size = float(data["min_order_size"])
                log.info(
                    "PM book params: tick_size=%s, min_order_size=%s",
                    market.tick_size, market.min_order_size,
                )
        except Exception as e:
            log.warning("Failed to fetch tick_size from PM: %s — using default %s", e, market.tick_size)

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

            # Resolution value for unredeemed shares
            up_resolution = 1.0 if fv_up >= 0.5 else 0.0
            dn_resolution = 1.0 if fv_dn >= 0.5 else 0.0

            session_pnl = 0.0
            effective_balance = 0.0

            # ── Priority 1: MongoDB fills (exact transaction history) ──
            mongo_pnl = None
            if self._mongo and self.mm.market:
                mongo_pnl = await self._mongo.compute_session_pnl(
                    coin=self._coin,
                    timeframe=self._timeframe,
                    window_start=self.mm.market.window_start,
                    window_end=self.mm.market.window_end,
                    fv_up=up_resolution,
                    fv_dn=dn_resolution,
                )

            if mongo_pnl and (mongo_pnl["buy_count"] + mongo_pnl["sell_count"]) > 0:
                session_pnl = mongo_pnl["total_pnl"]
                # Balance = start + pnl
                effective_balance = self._start_balance + session_pnl
                log.info(
                    "Window PnL (MongoDB): $%.2f (buys=%d, sells=%d, fees=$%.4f)",
                    session_pnl, mongo_pnl["buy_count"],
                    mongo_pnl["sell_count"], mongo_pnl["fees"],
                )
            else:
                # ── Priority 2: PM balance diff (fallback) ──
                if self._paper_mode:
                    client = self.mm.order_mgr.client
                    end_balance = float(client.balance) if hasattr(client, "balance") else self._initial_usdc
                    up_shares = self.mm.inventory.up_shares
                    dn_shares = self.mm.inventory.dn_shares
                else:
                    try:
                        end_balance = await self.mm.order_mgr.get_usdc_balance()
                    except Exception:
                        end_balance = self.mm._cached_usdc_balance or 0
                    try:
                        up_shares, dn_shares = await self.mm.order_mgr.get_all_token_balances(
                            self.mm.market.up_token_id, self.mm.market.dn_token_id)
                    except Exception:
                        up_shares = max(0, self.mm.inventory.up_shares)
                        dn_shares = max(0, self.mm.inventory.dn_shares)

                up_shares = max(0.0, up_shares)
                dn_shares = max(0.0, dn_shares)
                unredeemed_value = up_shares * up_resolution + dn_shares * dn_resolution
                effective_balance = end_balance + unredeemed_value
                session_pnl = effective_balance - self._start_balance if self._start_balance else 0.0

            # Record PnL for 1h/24h aggregation
            now = time.time()
            self._pnl_history.append((now, session_pnl))
            cutoff_24h = now - 86400
            self._pnl_history = [(t, p) for t, p in self._pnl_history if t >= cutoff_24h]

            pnl_1h = sum(p for t, p in self._pnl_history if t >= now - 3600)
            pnl_24h = sum(p for t, p in self._pnl_history)

            if effective_balance <= 0:
                effective_balance = self._start_balance + session_pnl

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


# ── Singleton runtime ───────────────────────────────────────────
_runtime = MMRuntime()

# ── Telegram Bot (interactive management) ──────────────────────
_tg_bot = TelegramBotManager(
    notifier=_telegram,
    get_runtime=lambda: _runtime,
    access_key=ACCESS_KEY,
)


# ── Routes ──────────────────────────────────────────────────────
@app.get("/")
async def index():
    index_file = WEB_DIR / "index.html"
    if index_file.exists():
        return FileResponse(str(index_file))
    return JSONResponse({"status": "Polymarket MM API", "version": "2.0"})


@app.post("/api/auth/login")
async def login(req: LoginRequest, response: Response):
    if req.key != ACCESS_KEY:
        raise HTTPException(status_code=401, detail="invalid key")
    response.set_cookie(
        AUTH_COOKIE, req.key,
        httponly=True, samesite="strict", max_age=86400 * 30,
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
@app.post("/api/mm/start")
async def mm_start(req: StartRequest, request: Request):
    _require_auth(request)
    # Re-enable after Kill All
    _runtime.mm_config.enabled = True
    _runtime.mm_config.auto_next_window = True
    result = await _runtime.start(
        req.coin,
        req.timeframe,
        req.paper_mode,
        req.initial_usdc,
        dev=req.dev,
    )
    return {"ok": True, "state": result}


@app.post("/api/mm/stop")
async def mm_stop(request: Request):
    _require_auth(request)
    result = await _runtime.stop()
    return {"ok": True, "state": result}


@app.get("/api/mm/state")
async def mm_state(request: Request):
    _require_auth(request)
    return _runtime.snapshot()


@app.post("/api/mm/config")
async def mm_config_update(req: ConfigUpdateRequest, request: Request):
    _require_auth(request)
    updates = {k: v for k, v in req.model_dump().items() if v is not None}
    new_config = _runtime.update_config(**updates)
    return {"ok": True, "config": new_config}


@app.get("/api/mm/config")
async def mm_config_get(request: Request):
    _require_auth(request)
    return {"config": _runtime.mm_config.to_dict()}


@app.post("/api/mm/emergency")
async def mm_emergency(request: Request):
    """Emergency stop — cancel all orders immediately."""
    _require_auth(request)
    if _runtime.mm:
        cancelled = await _runtime.mm.order_mgr.cancel_all()
        await _runtime.mm.heartbeat.stop()
        _runtime.mm._paused = True
        _runtime.mm._pause_reason = "Emergency stop"
        return {"ok": True, "cancelled": cancelled}
    return {"ok": True, "cancelled": 0}


@app.post("/api/mm/kill")
async def mm_kill(request: Request):
    """Kill all: stop MM, liquidate inventory, disable auto-restart."""
    _require_auth(request)
    _runtime.mm_config.auto_next_window = False
    _runtime.mm_config.enabled = False
    snap = await _runtime.stop()
    return {"ok": True, "state": snap}


@app.post("/api/mm/watch")
async def mm_watch(request: Request, body: dict):
    """Start feeds without trading for live price monitoring."""
    _require_auth(request)
    coin = body.get("coin", "BTC")
    timeframe = body.get("timeframe", "5m")
    return await _runtime.start_watch(coin, timeframe)


@app.post("/api/mm/watch/stop")
async def mm_watch_stop(request: Request):
    """Stop watch mode feeds."""
    _require_auth(request)
    await _runtime.stop_watch()
    return {"ok": True}


@app.post("/api/mm/validate-credentials")
async def mm_validate_credentials(request: Request):
    """Validate Polymarket API credentials without starting MM."""
    _require_auth(request)
    try:
        result = await _runtime.validate_live_credentials()
        return {"valid": True, **result}
    except HTTPException as e:
        return {"valid": False, "detail": e.detail}


@app.get("/api/mm/fills")
async def mm_fills(request: Request, limit: int = 50, offset: int = 0):
    _require_auth(request)
    if _runtime.mm:
        fills = _runtime.mm.risk_mgr.fills
        total = len(fills)
        page = fills[-(offset + limit):-offset or None] if offset else fills[-limit:]
        return {
            "fills": [
                {
                    "ts": f.ts, "side": f.side,
                    "token_id": f.token_id[:16] + "...",
                    "price": f.price, "size": f.size,
                    "fee": f.fee, "is_maker": f.is_maker,
                }
                for f in reversed(page)
            ],
            "total": total,
        }
    return {"fills": [], "total": 0}


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


# ── Available markets ──────────────────────────────────────────
@app.get("/api/markets")
async def markets():
    return {
        "coins": config.COINS,
        "timeframes": config.COIN_TIMEFRAMES,
    }


# ── Startup / Shutdown ─────────────────────────────────────────
@app.on_event("startup")
async def _startup():
    _telegram.set_loop(asyncio.get_running_loop())
    await _runtime.load_config()
    if _telegram.enabled:
        await _tg_bot.start()
        log.info("Telegram bot polling started")


@app.on_event("shutdown")
async def _shutdown():
    await _tg_bot.stop()
    if _runtime.is_running:
        try:
            await asyncio.wait_for(_runtime.stop(), timeout=15.0)
        except Exception as e:
            log.error(f"Shutdown error: {e}")
    await _telegram.close()
