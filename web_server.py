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

log.info(f"PM_PRIVATE_KEY={'set' if PM_PRIVATE_KEY else 'MISSING'}")
log.info(f"PM_FUNDER={'set' if PM_FUNDER else 'MISSING'}")
log.info(f"PM_API credentials={'set' if PM_API_KEY else 'MISSING'}")

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


class ConfigUpdateRequest(BaseModel):
    half_spread_bps: Optional[float] = None
    order_size_usd: Optional[float] = None
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
    enabled: Optional[bool] = None


# ── Mock CLOB Client (for paper trading) ────────────────────────
class MockClobClient:
    """Mock CLOB client for paper trading / testing without real orders."""

    def __init__(self):
        self._orders: dict[str, dict] = {}
        self._next_id = 1

    def create_and_sign_order(self, args: dict) -> dict:
        return {"token_id": args["token_id"], "price": args["price"],
                "size": args["size"], "side": args["side"]}

    def post_order(self, signed, order_type: str) -> dict:
        oid = f"mock-{self._next_id:06d}"
        self._next_id += 1
        self._orders[oid] = {"status": "LIVE", "size_matched": 0}
        return {"orderID": oid}

    def cancel(self, order_id: str) -> dict:
        self._orders.pop(order_id, None)
        return {"success": True}

    def cancel_all(self) -> dict:
        self._orders.clear()
        return {"success": True}

    def get_order(self, order_id: str) -> dict:
        return self._orders.get(order_id, {"status": "CANCELLED", "size_matched": 0})

    def post_heartbeat(self) -> dict:
        return {"success": True}

    def is_order_scoring(self, params: dict) -> dict:
        return {"scoring": True}

    def get_order_book(self, token_id: str) -> dict:
        return {"bids": [], "asks": [], "min_order_size": 5.0}


# ── CLOB Client Factory ────────────────────────────────────────
def _create_clob_client(paper_mode: bool = True) -> Any:
    """Create CLOB client — mock for paper, real for live."""
    if paper_mode:
        log.info("Using MOCK CLOB client (paper trading)")
        return MockClobClient()

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
            signature_type=2,
        )
        log.info("Using REAL CLOB client (live trading)")
        return client
    except Exception as e:
        log.error(f"Failed to create real CLOB client: {e}")
        log.info("Falling back to MOCK client")
        return MockClobClient()


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

    @property
    def is_running(self) -> bool:
        return self._running and self.mm is not None

    async def start(self, coin: str, timeframe: str, paper_mode: bool = True) -> dict:
        """Start feeds and market maker."""
        if self._running:
            raise HTTPException(status_code=400, detail="Already running")

        self._coin = coin
        self._timeframe = timeframe
        self._paper_mode = paper_mode

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

        t1 = asyncio.create_task(feeds.ob_poller(self.feed_state, symbol))
        t2 = asyncio.create_task(feeds.binance_feed(self.feed_state, symbol, kline_interval))
        self._feed_tasks = [t1, t2]

        # Start PM price feeds
        try:
            tokens = await asyncio.wait_for(
                asyncio.to_thread(feeds.fetch_pm_tokens, coin, timeframe),
                timeout=15.0,
            )
            if tokens:
                t3 = asyncio.create_task(
                    feeds.pm_feed(self.feed_state))
                self._feed_tasks.append(t3)

                # Create market info from tokens
                market = self._build_market_info(coin, timeframe, tokens)
            else:
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
        clob = _create_clob_client(paper_mode)

        # Create and start market maker
        self.mm = MarketMaker(self.feed_state, clob, self.mm_config)
        self.mm.set_market(market)

        # Register fill callback for telegram
        if _telegram.enabled:
            self.mm.on_fill(self._on_fill_telegram)

        await self.mm.start()
        self._running = True

        log.info(f"MM started: {coin}/{timeframe} paper={paper_mode}")
        return self.snapshot()

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
            return self.mm.snapshot()

        return {
            "is_running": False,
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
    result = await _runtime.start(req.coin, req.timeframe, req.paper_mode)
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
