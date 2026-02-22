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

# ── Logging ─────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(name)s] %(levelname)s: %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger("web")


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
    enabled: Optional[bool] = None


# ── Mock CLOB Client (for paper trading) ────────────────────────
import random

class MockClobClient:
    """Mock CLOB client for paper trading with fill simulation.

    Simulates order fills based on probability + price proximity to fair value.
    When get_order() is called, each LIVE order has a chance of being filled:
    - Base fill probability per check: ~15%
    - Orders closer to fair value are more likely to fill
    - Simulates realistic paper trading behavior
    """

    def __init__(self, fill_prob: float = 0.15, usdc_balance: float = 1000.0):
        self._orders: dict[str, dict] = {}
        self._next_id = 1
        self._fill_prob = fill_prob  # Base fill probability per get_order call
        self._tick_count = 0  # Count calls for time-based fill logic
        self._usdc_balance = usdc_balance

    @property
    def balance(self) -> float:
        return self._usdc_balance

    def get_balance(self) -> float:
        return self._usdc_balance

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
            "collateral": collateral,
            "fill_credit_applied": False,
            "created_tick": self._tick_count,
        }
        return {"orderID": oid}

    def cancel(self, order_id: str) -> dict:
        order = self._orders.pop(order_id, None)
        if (order and order.get("status") == "LIVE"
                and order.get("side") == "BUY"):
            self._usdc_balance += float(order.get("collateral", 0.0))
        return {"success": True}

    def cancel_all(self) -> dict:
        for order in self._orders.values():
            if order.get("status") == "LIVE" and order.get("side") == "BUY":
                self._usdc_balance += float(order.get("collateral", 0.0))
        self._orders.clear()
        return {"success": True}

    def get_order(self, order_id: str) -> dict:
        """Check order status. Simulates fills probabilistically.

        Orders that have been live for more ticks are more likely to fill.
        """
        self._tick_count += 1
        order = self._orders.get(order_id)
        if order is None:
            return {"status": "CANCELLED", "size_matched": 0}

        if order["status"] != "LIVE":
            return order

        # Calculate fill probability based on age
        age = self._tick_count - order.get("created_tick", 0)
        # Probability increases with age: starts at fill_prob, grows ~2x over 10 ticks
        prob = min(0.95, self._fill_prob * (1 + age * 0.1))

        if random.random() < prob:
            # Fill the order (full fill)
            order["status"] = "MATCHED"
            order["size_matched"] = order["size"]
            if order["side"] == "SELL" and not order.get("fill_credit_applied"):
                self._usdc_balance += order["size"] * order["price"]
                order["fill_credit_applied"] = True
            log.info(f"[MOCK] Simulated fill: {order['side']} "
                     f"{order['size']:.1f}@{order['price']:.2f}")

        return order

    def post_heartbeat(self) -> dict:
        return {"success": True}

    def is_order_scoring(self, params: dict) -> dict:
        return {"scoring": True}

    def get_order_book(self, token_id: str) -> dict:
        return {"bids": [], "asks": [], "min_order_size": 5.0}


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
        self._initial_usdc: float = 1000.0
        self._next_window_at: float = 0.0  # timestamp when next window starts

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
                    initial_usdc: float = 1000.0) -> dict:
        """Start feeds and market maker."""
        if self._running:
            raise HTTPException(status_code=400, detail="Already running")

        initial_usdc = float(initial_usdc)
        self._coin = coin
        self._timeframe = timeframe
        self._paper_mode = paper_mode
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
                up_id, dn_id = tokens
                # Set token IDs on feed state so pm_feed can subscribe
                self.feed_state.pm_up_id = up_id
                self.feed_state.pm_dn_id = dn_id
                log.info(f"PM tokens: UP={up_id[:20]}... DN={dn_id[:20]}...")

                t3 = asyncio.create_task(
                    feeds.pm_feed(self.feed_state))
                self._feed_tasks.append(t3)

                # Build market info from token IDs
                market = self._build_market_info_from_tokens(
                    coin, timeframe, up_id, dn_id)
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

        # Create and start market maker
        self.mm = MarketMaker(self.feed_state, clob, self.mm_config)
        if paper_mode:
            self.mm.inventory.usdc = initial_usdc
            self.mm.inventory.initial_usdc = initial_usdc
        self.mm.set_market(market)

        # Register fill callback for telegram
        if _telegram.enabled:
            self.mm.on_fill(self._on_fill_telegram)

        await self.mm.start()
        self._running = True

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
            # MM stopped itself due to window expiry
            if not mm._running and mm._is_closing:
                log.info("Window expired — MM stopped. Cleaning up feeds.")
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
                                up_id, dn_id = tokens
                                strike, _ws, _we = await asyncio.wait_for(
                                    asyncio.to_thread(feeds.fetch_pm_strike, self._coin, self._timeframe),
                                    timeout=15.0,
                                )
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

        self._running = False
        log.info("MM stopped")
        return snap

    def snapshot(self) -> dict:
        """Get current state for API."""
        if self.mm:
            snap = self.mm.snapshot()
            snap["paper_mode"] = self._paper_mode
            snap["session_limit"] = self._initial_usdc
            snap["next_window_in"] = max(0, self._next_window_at - time.time()) if self._next_window_at else 0
            if self._paper_mode:
                client = self.mm.order_mgr.client
                if hasattr(client, "balance"):
                    snap["mock_usdc_balance"] = float(client.balance)
            return snap

        return {
            "is_running": False,
            "paper_mode": self._paper_mode,
            "next_window_in": max(0, self._next_window_at - time.time()) if self._next_window_at else 0,
            "market": {"coin": self._coin, "timeframe": self._timeframe},
            "fair_value": {"up": 0.5, "dn": 0.5, "binance_mid": 0, "volatility": 0},
            "quotes": {},
            "inventory": {"up_shares": 0, "dn_shares": 0, "net_delta": 0, "usdc": 0},
            "recent_fills": [],
            "config": self.mm_config.to_dict(),
        }

    def update_config(self, **kwargs) -> dict:
        """Update MM config at runtime."""
        self.mm_config.update(**kwargs)
        if self.mm:
            self.mm.config = self.mm_config
            self.mm.quote_engine.config = self.mm_config
        return self.mm_config.to_dict()

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
                                       up_id: str, dn_id: str) -> MarketInfo:
        """Build MarketInfo from fetched PM token ID tuple.

        Fetches the actual strike price from Binance candle open
        and window timing from PM event endDate.
        """
        strike, window_start, window_end = feeds.fetch_pm_strike(coin, timeframe)

        if strike <= 0 or window_start <= 0:
            # Fallback: use current Binance mid as strike (imperfect but better than 0)
            log.warning("Could not fetch strike from Binance, using current mid price")
            now = time.time()
            tf_minutes = {"5m": 5, "15m": 15, "1h": 60, "4h": 240, "daily": 1440}
            window_duration = tf_minutes.get(timeframe, 60) * 60
            # Use feed_state.mid if available
            strike = self.feed_state.mid if self.feed_state and self.feed_state.mid else 0.0
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
        )

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

    def _on_fill_telegram(self, fill, token_type: str) -> None:
        """Send fill notification via Telegram."""
        try:
            _telegram.notify_fill(
                coin=self._coin or "UNKNOWN",
                timeframe=self._timeframe or "UNKNOWN",
                side=fill.side,
                price=fill.price,
                size=fill.size,
                fee=fill.fee,
                is_maker=fill.is_maker,
            )
        except Exception:
            pass


# ── Singleton runtime ───────────────────────────────────────────
_runtime = MMRuntime()


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


# ── Shutdown ────────────────────────────────────────────────────
@app.on_event("shutdown")
async def _shutdown():
    if _runtime.is_running:
        try:
            await asyncio.wait_for(_runtime.stop(), timeout=15.0)
        except Exception as e:
            log.error(f"Shutdown error: {e}")
    await _telegram.close()
