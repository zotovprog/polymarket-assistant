import asyncio
import json
import time
from collections import deque
import os
import re

import aiohttp
import requests
import websockets
from datetime import datetime, timezone, timedelta

import config
from mm.runtime_metrics import runtime_metrics


class State:
    def __init__(self):
        self.bids: list[tuple[float, float]] = []
        self.asks: list[tuple[float, float]] = []
        self.mid: float = 0.0

        self.trades = deque(maxlen=5000)

        self.klines: list[dict] = []
        self.cur_kline: dict | None = None

        self.pm_up_id:  str | None = None
        self.pm_dn_id:  str | None = None
        self.pm_up:     float | None = None
        self.pm_dn:     float | None = None
        self.pm_up_bid: float | None = None
        self.pm_dn_bid: float | None = None

        self.binance_ws_connected: bool = False
        self.binance_ob_connected: bool = False
        self.binance_ob_ready: bool = False
        self.binance_ob_last_ok_ts: float = 0.0
        self.pm_connected: bool = False
        self.pm_prices_ready: bool = False
        self.pm_last_update_ts: float = 0.0
        self.pm_reconnect_requested: bool = False
        self.pm_all_filtered: bool = False
        self.pm_all_filtered_ts: float = 0.0

        # ── Feed health metrics ────────────────────────
        self.binance_ws_msg_count: int = 0
        self.binance_ws_error_count: int = 0
        self.binance_ws_connected_at: float = 0.0
        self.binance_ws_last_ok_ts: float = 0.0
        self.binance_ob_connected_at: float = 0.0
        self.binance_last_trade_sample_ts: float = 0.0

        self.binance_ob_msg_count: int = 0
        self.binance_ob_error_count: int = 0

        self.pm_msg_count: int = 0
        self.pm_error_count: int = 0
        self.pm_connected_at: float = 0.0


OB_POLL_INTERVAL = 2
BINANCE_OB_IDLE_RECONNECT_SEC = 12.0
BINANCE_WS_IDLE_RECONNECT_SEC = 20.0
PM_WS_IDLE_RECONNECT_SEC = 30.0
BINANCE_OB_PROCESS_MIN_INTERVAL_SEC = max(
    0.01,
    float(os.environ.get("BINANCE_OB_PROCESS_MIN_INTERVAL_SEC", "0.05")),
)
PM_WS_PROCESS_MIN_INTERVAL_SEC = max(
    0.01,
    float(os.environ.get("PM_WS_PROCESS_MIN_INTERVAL_SEC", "0.20")),
)
BINANCE_TRADE_MIN_INTERVAL_SEC = 0.05
BINANCE_ENDPOINT_CACHE_TTL_SEC = 300.0
BINANCE_WS_MAX_QUEUE = max(1, int(os.environ.get("BINANCE_WS_MAX_QUEUE", "128")))
PM_WS_MAX_QUEUE = max(1, int(os.environ.get("PM_WS_MAX_QUEUE", "64")))
BINANCE_TRADE_STREAM_ENABLED = os.environ.get(
    "BINANCE_TRADE_STREAM_ENABLED", "0"
).lower() in ("1", "true", "yes", "on")

_binance_rest_cache: tuple[str, float] | None = None
_binance_ws_cache: tuple[str, float] | None = None


def _fetch_binance_depth(url: str, symbol: str) -> dict:
    return requests.get(url, params={"symbol": symbol, "limit": 20}, timeout=3).json()


def _pick_binance_rest() -> str:
    """Test primary Binance REST, fall back to .us if blocked."""
    for base in [config.BINANCE_REST, config.BINANCE_REST_FALLBACK]:
        try:
            r = requests.get(f"{base}/ping", timeout=3)
            if r.status_code == 200:
                print(f"  [Binance] REST endpoint OK: {base}")
                return base
        except Exception:
            pass
    print("  [Binance] WARNING: no REST endpoint reachable, using fallback")
    return config.BINANCE_REST_FALLBACK


def _pick_binance_ws() -> str:
    """Return primary or fallback WS base URL."""
    # Quick test: try REST ping on .com — if blocked, assume WS is too
    try:
        r = requests.get(f"{config.BINANCE_REST}/ping", timeout=3)
        if r.status_code == 200:
            return config.BINANCE_WS
    except Exception:
        pass
    print("  [Binance] WS falling back to binance.us")
    return config.BINANCE_WS_FALLBACK


async def _pick_binance_rest_async(force_refresh: bool = False) -> str:
    """Resolve Binance REST endpoint without blocking event loop."""
    global _binance_rest_cache
    now = time.time()
    if (
        not force_refresh
        and _binance_rest_cache
        and (now - _binance_rest_cache[1]) < BINANCE_ENDPOINT_CACHE_TTL_SEC
    ):
        return _binance_rest_cache[0]
    rest = await asyncio.to_thread(_pick_binance_rest)
    _binance_rest_cache = (rest, now)
    return rest


async def _pick_binance_ws_async(force_refresh: bool = False) -> str:
    """Resolve Binance WS endpoint without blocking event loop."""
    global _binance_ws_cache
    now = time.time()
    if (
        not force_refresh
        and _binance_ws_cache
        and (now - _binance_ws_cache[1]) < BINANCE_ENDPOINT_CACHE_TTL_SEC
    ):
        return _binance_ws_cache[0]
    ws_base = await asyncio.to_thread(_pick_binance_ws)
    _binance_ws_cache = (ws_base, now)
    return ws_base


async def ob_poller(symbol: str, state: State):
    binance_rest = await _pick_binance_rest_async()
    rest_url = f"{binance_rest}/depth"
    ws_base = "wss://stream.binance.us:9443" if "binance.us" in binance_rest else "wss://stream.binance.com:9443"
    ws_url = f"{ws_base}/ws/{symbol.lower()}@depth20@100ms"
    print(f"  [Binance OB] streaming {symbol} via {ws_url}")

    async def _apply_depth(bids_raw, asks_raw):
        state.bids = [(float(p), float(q)) for p, q in bids_raw]
        state.asks = [(float(p), float(q)) for p, q in asks_raw]
        if state.bids and state.asks:
            state.mid = (state.bids[0][0] + state.asks[0][0]) / 2
            state.binance_ob_ready = True
            state.binance_ob_last_ok_ts = time.time()
            state.binance_ob_msg_count += 1

    reconnect_delay = 1
    while True:
        try:
            # Fallback snapshot before opening stream, so we have initial OB quickly.
            try:
                resp = await asyncio.to_thread(_fetch_binance_depth, rest_url, symbol)
                bids_raw = resp.get("bids", [])
                asks_raw = resp.get("asks", [])
                if bids_raw and asks_raw:
                    await _apply_depth(bids_raw, asks_raw)
            except Exception as e:
                print(f"  [Binance OB] snapshot fallback failed: {e}")

            timeout = aiohttp.ClientTimeout(total=None, sock_connect=10, sock_read=70)
            async with aiohttp.ClientSession(timeout=timeout) as session:
                async with session.ws_connect(ws_url, heartbeat=20, receive_timeout=70) as ws:
                    print(f"  [Binance OB] connected – {symbol}")
                    reconnect_delay = 1
                    state.binance_ob_connected = True
                    state.binance_ob_connected_at = time.time()
                    msg_since_yield = 0
                    dropped_since_yield = 0
                    last_processed_ts = 0.0

                    while True:
                        try:
                            msg = await asyncio.wait_for(
                                ws.receive(),
                                timeout=BINANCE_OB_IDLE_RECONNECT_SEC,
                            )
                        except asyncio.TimeoutError as e:
                            raise ConnectionError(
                                f"orderbook stream idle for >{BINANCE_OB_IDLE_RECONNECT_SEC:.0f}s"
                            ) from e

                        if msg.type == aiohttp.WSMsgType.TEXT:
                            runtime_metrics.incr("feeds.binance_ob.msg_recv")
                            now = time.time()
                            msg_since_yield += 1
                            if msg_since_yield >= 200:
                                msg_since_yield = 0
                                await asyncio.sleep(0)
                            if (now - last_processed_ts) < BINANCE_OB_PROCESS_MIN_INTERVAL_SEC:
                                runtime_metrics.incr("feeds.binance_ob.msg_drop")
                                dropped_since_yield += 1
                                if dropped_since_yield >= 200:
                                    dropped_since_yield = 0
                                    await asyncio.sleep(0)
                                continue
                            dropped_since_yield = 0
                            data = json.loads(msg.data)
                            bids_raw = data.get("bids") or data.get("b") or []
                            asks_raw = data.get("asks") or data.get("a") or []
                            if bids_raw and asks_raw:
                                await _apply_depth(bids_raw, asks_raw)
                                runtime_metrics.incr("feeds.binance_ob.msg_process")
                                last_processed_ts = now
                        elif msg.type in (
                            aiohttp.WSMsgType.CLOSE,
                            aiohttp.WSMsgType.CLOSING,
                            aiohttp.WSMsgType.CLOSED,
                            aiohttp.WSMsgType.ERROR,
                        ):
                            raise ConnectionError("orderbook stream disconnected")
        except asyncio.CancelledError:
            state.binance_ob_connected = False
            raise
        except Exception as e:
            runtime_metrics.incr("feeds.binance_ob.error")
            state.binance_ob_error_count += 1
            state.binance_ob_connected = False
            delay = reconnect_delay or OB_POLL_INTERVAL
            print(f"  [Binance OB] disconnected: {e}. Reconnecting in {delay}s...")
            await asyncio.sleep(delay)
            reconnect_delay = min(max(delay * 2, 1), 10)


async def binance_feed(symbol: str, kline_iv: str, state: State):
    ws_base = await _pick_binance_ws_async()
    sym = symbol.lower()
    streams = [f"{sym}@kline_{kline_iv}"]
    if BINANCE_TRADE_STREAM_ENABLED:
        streams.insert(0, f"{sym}@trade")
    streams = "/".join(streams)
    url = f"{ws_base}?streams={streams}"
    reconnect_delay = 1

    while True:
        try:
            async with websockets.connect(
                url,
                ping_interval=20,
                ping_timeout=60,
                close_timeout=10,
                max_queue=BINANCE_WS_MAX_QUEUE,
            ) as ws:
                state.binance_ws_connected = True
                state.binance_ws_connected_at = time.time()
                reconnect_delay = 1
                if BINANCE_TRADE_STREAM_ENABLED:
                    print(f"  [Binance WS] connected – {symbol} (trade+kline)")
                else:
                    print(f"  [Binance WS] connected – {symbol} (kline-only)")
                msg_since_yield = 0
                dropped_trade_since_yield = 0

                while True:
                    raw = await asyncio.wait_for(
                        ws.recv(),
                        timeout=BINANCE_WS_IDLE_RECONNECT_SEC,
                    )
                    runtime_metrics.incr("feeds.binance_ws.msg_recv")
                    data = json.loads(raw)
                    stream = data.get("stream", "")
                    pay = data["data"]
                    state.binance_ws_msg_count += 1
                    state.binance_ws_last_ok_ts = time.time()
                    msg_since_yield += 1
                    if msg_since_yield >= 200:
                        # Avoid starving the event loop under burst traffic.
                        msg_since_yield = 0
                        await asyncio.sleep(0)

                    if "@trade" in stream:
                        ts = pay["T"] / 1000.0
                        trade_price = float(pay["p"])
                        if state.mid <= 0 and trade_price > 0:
                            state.mid = trade_price
                        # Downsample ultra-fast trade stream to avoid CPU saturation.
                        if (ts - state.binance_last_trade_sample_ts) < BINANCE_TRADE_MIN_INTERVAL_SEC:
                            runtime_metrics.incr("feeds.binance_ws.trade_drop")
                            dropped_trade_since_yield += 1
                            if dropped_trade_since_yield >= 200:
                                dropped_trade_since_yield = 0
                                await asyncio.sleep(0)
                            continue
                        dropped_trade_since_yield = 0
                        state.binance_last_trade_sample_ts = ts
                        state.trades.append({
                            "t": ts,
                            "price": trade_price,
                            "qty": float(pay["q"]),
                            "is_buy": not pay["m"],
                        })
                        runtime_metrics.incr("feeds.binance_ws.trade_process")

                    elif "@kline" in stream:
                        k = pay["k"]
                        candle = {
                            "t": k["t"] / 1000.0,
                            "o": float(k["o"]), "h": float(k["h"]),
                            "l": float(k["l"]), "c": float(k["c"]),
                            "v": float(k["v"]),
                        }
                        state.cur_kline = candle
                        runtime_metrics.incr("feeds.binance_ws.kline_process")
                        if k["x"]:
                            state.klines.append(candle)
                            state.klines = state.klines[-config.KLINE_MAX:]

        except asyncio.CancelledError:
            state.binance_ws_connected = False
            raise
        except websockets.exceptions.ConnectionClosed as e:
            runtime_metrics.incr("feeds.binance_ws.error")
            state.binance_ws_error_count += 1
            state.binance_ws_connected = False
            delay = reconnect_delay
            print(f"  [Binance WS] connection closed: {e}, reconnecting in {delay}s...")
            await asyncio.sleep(delay)
            reconnect_delay = min(max(delay * 2, 1), 10)
        except asyncio.TimeoutError:
            runtime_metrics.incr("feeds.binance_ws.error")
            state.binance_ws_error_count += 1
            state.binance_ws_connected = False
            delay = reconnect_delay
            print(
                f"  [Binance WS] idle for >{BINANCE_WS_IDLE_RECONNECT_SEC:.0f}s, "
                f"reconnecting in {delay}s..."
            )
            await asyncio.sleep(delay)
            reconnect_delay = min(max(delay * 2, 1), 10)
        except Exception as e:
            runtime_metrics.incr("feeds.binance_ws.error")
            state.binance_ws_error_count += 1
            state.binance_ws_connected = False
            delay = reconnect_delay
            print(f"  [Binance WS] connection error: {e}, reconnecting in {delay}s...")
            await asyncio.sleep(delay)
            reconnect_delay = min(max(delay * 2, 1), 10)


async def bootstrap(symbol: str, interval: str, state: State):
    for attempt in range(1, 6):
        try:
            # Run blocking HTTP call off the event loop.
            resp = await asyncio.to_thread(
                lambda: requests.get(
                    f"{config.BINANCE_REST}/klines",
                    params={"symbol": symbol, "interval": interval, "limit": config.KLINE_BOOT},
                    timeout=5,
                ).json()
            )
            state.klines = [
                {
                    "t": r[0] / 1e3,
                    "o": float(r[1]), "h": float(r[2]),
                    "l": float(r[3]), "c": float(r[4]),
                    "v": float(r[5]),
                }
                for r in resp
            ]
            print(f"  [Binance] loaded {len(state.klines)} historical candles")
            return
        except Exception as e:
            if attempt >= 5:
                print(f"  [Binance] bootstrap failed after {attempt} attempts: {e}")
                state.klines = []
                return
            print(f"  [Binance] bootstrap attempt {attempt} failed: {e} (retrying)")
            await asyncio.sleep(min(2 * attempt, 8))


_MONTHS = ["", "january", "february", "march", "april", "may", "june",
           "july", "august", "september", "october", "november", "december"]


def _et_now() -> datetime:
    utc = datetime.now(timezone.utc)
    year = utc.year

    mar1_dow  = datetime(year, 3, 1).weekday()
    mar_sun   = 1 + (6 - mar1_dow) % 7
    dst_start = datetime(year, 3, mar_sun + 7, 2, 0, 0, tzinfo=timezone.utc)

    nov1_dow = datetime(year, 11, 1).weekday()
    nov_sun  = 1 + (6 - nov1_dow) % 7
    dst_end  = datetime(year, 11, nov_sun, 6, 0, 0, tzinfo=timezone.utc)

    offset = timedelta(hours=-4) if dst_start <= utc < dst_end else timedelta(hours=-5)
    return utc + offset


def _to_12h(hour24: int) -> str:
    if hour24 == 0:
        return "12am"
    if hour24 < 12:
        return f"{hour24}am"
    if hour24 == 12:
        return "12pm"
    return f"{hour24 - 12}pm"


def _build_slug(coin: str, tf: str) -> str | None:
    now_utc = datetime.now(timezone.utc)
    now_ts  = int(now_utc.timestamp())
    et      = _et_now()

    if tf == "5m":
        ts = (now_ts // 300) * 300
        return f"{config.COIN_PM[coin]}-updown-5m-{ts}"

    if tf == "15m":
        ts = (now_ts // 900) * 900
        return f"{config.COIN_PM[coin]}-updown-15m-{ts}"

    if tf == "4h":
        ts = ((now_ts - 3600) // 14400) * 14400 + 3600
        return f"{config.COIN_PM[coin]}-updown-4h-{ts}"

    if tf == "1h":
        return (f"{config.COIN_PM_LONG[coin]}-up-or-down-"
                f"{_MONTHS[et.month]}-{et.day}-{_to_12h(et.hour)}-et")

    if tf == "daily":
        resolution = et.replace(hour=12, minute=0, second=0, microsecond=0)
        target      = et if et < resolution else et + timedelta(days=1)
        return (f"{config.COIN_PM_LONG[coin]}-up-or-down-on-"
                f"{_MONTHS[target.month]}-{target.day}")

    return None


def fetch_pm_event_data(coin: str, tf: str) -> dict | None:
    """Fetch full event data from Polymarket API."""
    slug = _build_slug(coin, tf)
    if slug is None:
        return None
    try:
        resp = requests.get(config.PM_GAMMA, params={"slug": slug, "limit": 1}, timeout=5)
        data = resp.json()
        if isinstance(data, list) and data and data[0].get("ticker") == slug:
            return data[0]
    except Exception as e:
        print(f"  [PM] gamma event fetch failed ({slug}): {e}")

    # Fallback: CLOB active sampling markets (Gamma REST can be unavailable/blocked).
    try:
        sm_resp = requests.get("https://clob.polymarket.com/sampling-markets", timeout=8)
        sm_data = sm_resp.json()
        markets = sm_data.get("data", []) if isinstance(sm_data, dict) else []
        if not isinstance(markets, list):
            markets = []

        for m in markets:
            if isinstance(m, dict) and m.get("market_slug") == slug:
                return m

        prefix = f"{config.COIN_PM[coin]}-updown-{tf}-"
        candidates = [
            m for m in markets
            if isinstance(m, dict) and str(m.get("market_slug", "")).startswith(prefix)
        ]
        if candidates:
            def _slug_ts(m: dict) -> int:
                ms = str(m.get("market_slug", ""))
                mat = re.search(r"-(\d{9,12})$", ms)
                return int(mat.group(1)) if mat else 0

            best = max(candidates, key=_slug_ts)
            print(
                f"  [PM] slug miss ({slug}); using nearest active market {best.get('market_slug')}"
            )
            return best
    except Exception as e:
        print(f"  [PM] sampling-markets fallback failed ({slug}): {e}")

    print(f"  [PM] no active market for slug: {slug}")
    return None


def fetch_pm_tokens(coin: str, tf: str) -> tuple:
    """Fetch PM token IDs and condition ID for up/down markets.

    Returns:
        (up_token_id, dn_token_id, condition_id) on success,
        (None, None, None) on failure.
    """
    event_data = fetch_pm_event_data(coin, tf)
    if event_data is None:
        return None, None, None
    try:
        # Gamma legacy shape.
        if "markets" in event_data:
            market = event_data["markets"][0]
            ids = json.loads(market["clobTokenIds"])
            condition_id = market.get("conditionId", "")
            return ids[0], ids[1], condition_id

        # CLOB sampling-markets shape.
        tokens = event_data.get("tokens", [])
        if not isinstance(tokens, list) or len(tokens) < 2:
            raise ValueError("sampling market has no token pair")

        up_id: str | None = None
        dn_id: str | None = None
        for t in tokens:
            if not isinstance(t, dict):
                continue
            tid = str(t.get("token_id", "")).strip()
            outcome = str(t.get("outcome", "")).strip().lower()
            if not tid:
                continue
            if outcome in ("yes", "up", "above", "higher"):
                up_id = tid
            elif outcome in ("no", "down", "below", "lower"):
                dn_id = tid

        if not up_id:
            up_id = str(tokens[0].get("token_id", "")).strip()
        if not dn_id:
            dn_id = str(tokens[1].get("token_id", "")).strip()
        if not up_id or not dn_id:
            raise ValueError("sampling market token ids missing")

        condition_id = (
            event_data.get("condition_id")
            or event_data.get("conditionId")
            or ""
        )
        return up_id, dn_id, condition_id
    except Exception as e:
        print(f"  [PM] token extraction failed: {e}")
        return None, None, None


def fetch_pm_strike(coin: str, tf: str, max_retries: int = 3,
                    retry_delay: float = 2.0) -> tuple[float, float, float]:
    """Fetch strike price and window timing from PM event + Binance.

    For Up/Down markets, strike = Binance candle open price.
    Retries up to max_retries times with retry_delay between attempts.

    Returns:
        (strike, window_start, window_end) — strike is the Binance open price,
        window_start/end are Unix timestamps.
        Returns (0.0, 0, 0) on failure.
    """
    for attempt in range(1, max_retries + 1):
        strike, ws, we = _fetch_pm_strike_once(coin, tf)
        if strike > 0:
            return strike, ws, we
        if attempt < max_retries:
            print(f"  [PM] strike=0 on attempt {attempt}/{max_retries}, retrying in {retry_delay}s...")
            time.sleep(retry_delay)
    print(f"  [PM] strike=0 after {max_retries} attempts")
    return 0.0, 0, 0


def _fetch_pm_strike_once(coin: str, tf: str) -> tuple[float, float, float]:
    """Single attempt to fetch strike price."""
    event_data = fetch_pm_event_data(coin, tf)
    if event_data is None:
        return 0.0, 0, 0

    try:
        from datetime import datetime, timezone, timedelta

        end_date_str = (
            event_data.get("endDate")
            or event_data.get("end_date_iso")
            or event_data.get("endDateIso")
            or ""
        )
        if not end_date_str:
            return 0.0, 0, 0

        # Parse end date
        end_dt = datetime.fromisoformat(end_date_str.replace("Z", "+00:00"))

        # Candle duration from timeframe
        tf_minutes = {"5m": 5, "15m": 15, "1h": 60, "4h": 240, "daily": 1440}
        duration_min = tf_minutes.get(tf, 60)

        # Open time = end - duration
        open_dt = end_dt - timedelta(minutes=duration_min)

        window_start = open_dt.timestamp()
        window_end = end_dt.timestamp()

        # Fetch Binance kline to get the candle open price
        symbol = config.COIN_BINANCE.get(coin, "BTCUSDT")
        kline_interval = config.TF_KLINE.get(tf, "1h")
        open_ms = int(window_start * 1000)

        # Try primary and fallback Binance REST endpoints
        for base_url in [config.BINANCE_REST, config.BINANCE_REST_FALLBACK]:
            try:
                url = f"{base_url}/klines"
                resp = requests.get(url, params={
                    "symbol": symbol,
                    "interval": kline_interval,
                    "startTime": open_ms,
                    "limit": 1,
                }, timeout=5)
                klines = resp.json()
                if klines and isinstance(klines, list) and len(klines) > 0:
                    strike = float(klines[0][1])  # [1] = open price
                    print(f"  [PM] Strike={strike:.2f} (Binance {symbol} {kline_interval} open at {open_dt}) via {base_url}")
                    return strike, window_start, window_end
            except Exception as e:
                print(f"  [PM] Binance kline fetch failed ({base_url}): {e}")

        print(f"  [PM] No Binance kline found for strike calc (all endpoints failed)")
        return 0.0, window_start, window_end

    except Exception as e:
        print(f"  [PM] strike fetch failed: {e}")
        return 0.0, 0, 0


async def fetch_pm_depth(token_id: str, price: float, side: str = "buy") -> float:
    """Fetch available depth (in USD) at or better than `price` for a PM token.

    Args:
        token_id: The PM conditional token ID
        price: The price threshold
        side: "buy" or "sell"

    Returns:
        Total USD depth available at or better than price. Returns 0.0 on error.
    """
    try:
        import httpx

        url = f"https://clob.polymarket.com/book?token_id={token_id}"
        async with httpx.AsyncClient(timeout=5.0) as client:
            resp = await client.get(url)
            if not resp.is_success:
                return 0.0
            data = resp.json()

        # For BUY orders, we look at asks (we're buying from sellers)
        # For SELL orders, we look at bids (we're selling to buyers)
        if side == "buy":
            levels = data.get("asks", [])
            total = 0.0
            for level in levels:
                lvl_price = float(level.get("price", 0))
                lvl_size = float(level.get("size", 0))
                if lvl_price <= price:
                    total += lvl_price * lvl_size
            return total

        levels = data.get("bids", [])
        total = 0.0
        for level in levels:
            lvl_price = float(level.get("price", 0))
            lvl_size = float(level.get("size", 0))
            if lvl_price >= price:
                total += lvl_price * lvl_size
        return total
    except Exception as e:
        print(f"  [FEEDS] fetch_pm_depth error: {e}")
        return 0.0


async def pm_feed(state: State):
    if not state.pm_up_id:
        print("  [PM] no tokens for this coin/timeframe – skipped")
        return

    _last_msg_log_ts = 0.0
    _last_processed_ts = 0.0
    reconnect_delay = 1

    while True:
        # Pick up current token IDs (may have been updated by trading_loop)
        assets = [state.pm_up_id, state.pm_dn_id]
        if not assets[0] or not assets[1]:
            print("  [PM] waiting for token IDs...")
            await asyncio.sleep(5)
            continue

        state.pm_reconnect_requested = False

        try:
            async with websockets.connect(
                config.PM_WS,
                ping_interval=20,
                ping_timeout=30,
                close_timeout=5,
                open_timeout=15,
                max_queue=PM_WS_MAX_QUEUE,
            ) as ws:
                await ws.send(json.dumps({"assets_ids": assets, "type": "market"}))
                print(
                    f"  [PM] connected, subscribed to {len(assets)} assets "
                    f"(up={assets[0][:12]}.. dn={assets[1][:12]}..)"
                )
                state.pm_connected = True
                state.pm_connected_at = time.time()
                state.pm_msg_count = 0
                reconnect_delay = 1
                last_msg_ts = time.time()
                msg_since_yield = 0
                dropped_burst_since_yield = 0

                while True:
                    # Check if trading_loop requested reconnect (new window = new tokens)
                    if state.pm_reconnect_requested:
                        print("  [PM] reconnect requested (new market window), closing WS...")
                        state.pm_connected = False
                        await ws.close()
                        break

                    try:
                        raw_msg = await asyncio.wait_for(ws.recv(), timeout=5)
                        runtime_metrics.incr("feeds.pm.msg_recv")
                        now = time.time()
                        state.pm_msg_count += 1
                        last_msg_ts = now
                        msg_since_yield += 1
                        if msg_since_yield >= 200:
                            msg_since_yield = 0
                            await asyncio.sleep(0)

                        # Coalesce PM bursts: process at most ~20Hz and drop
                        # intermediate updates under heavy flow to keep API responsive.
                        if (now - _last_processed_ts) < PM_WS_PROCESS_MIN_INTERVAL_SEC:
                            runtime_metrics.incr("feeds.pm.msg_drop")
                            dropped_burst_since_yield += 1
                            if dropped_burst_since_yield >= 200:
                                # Yield periodically under heavy burst drops without
                                # allocating a sleep coroutine for every skipped frame.
                                dropped_burst_since_yield = 0
                                await asyncio.sleep(0)
                            continue

                        dropped_burst_since_yield = 0
                        raw = json.loads(raw_msg)
                        runtime_metrics.incr("feeds.pm.msg_process")
                        _last_processed_ts = now

                        # Throttled debug log: show message stats every 60s
                        if now - _last_msg_log_ts >= 60:
                            if isinstance(raw, list):
                                print(
                                    f"  [PM] snapshot: {len(raw)} entries, "
                                    f"pm_up={state.pm_up}, pm_dn={state.pm_dn} "
                                    f"(msgs={state.pm_msg_count})"
                                )
                            elif isinstance(raw, dict):
                                print(
                                    f"  [PM] event: {raw.get('event_type', '?')}, "
                                    f"pm_up={state.pm_up}, pm_dn={state.pm_dn} "
                                    f"(msgs={state.pm_msg_count})"
                                )
                            else:
                                print(
                                    f"  [PM] heartbeat: pm_up={state.pm_up}, "
                                    f"pm_dn={state.pm_dn} (msgs={state.pm_msg_count})"
                                )
                            _last_msg_log_ts = now

                        if isinstance(raw, list):
                            for entry in raw:
                                _pm_apply(entry.get("asset_id"), entry.get("asks", []), entry.get("bids", []), state)

                        elif isinstance(raw, dict) and raw.get("event_type") == "price_change":
                            for ch in raw.get("price_changes", []):
                                if ch.get("best_ask"):
                                    _pm_set(ch["asset_id"], float(ch["best_ask"]), state)
                                if ch.get("best_bid"):
                                    _pm_set_bid(ch["asset_id"], float(ch["best_bid"]), state)

                    except asyncio.TimeoutError:
                        # Keep loop responsive for reconnect flag, but force a
                        # reconnect if feed is silent for too long.
                        if (time.time() - last_msg_ts) >= PM_WS_IDLE_RECONNECT_SEC:
                            state.pm_error_count += 1
                            print(
                                f"  [PM] idle for >{PM_WS_IDLE_RECONNECT_SEC:.0f}s, reconnecting..."
                            )
                            state.pm_connected = False
                            break
                        continue

                    except websockets.exceptions.ConnectionClosed:
                        runtime_metrics.incr("feeds.pm.error")
                        state.pm_error_count += 1
                        print(f"  [PM] connection closed after {state.pm_msg_count} msgs, reconnecting...")
                        state.pm_connected = False
                        break

        except asyncio.CancelledError:
            state.pm_connected = False
            raise
        except Exception as e:
            runtime_metrics.incr("feeds.pm.error")
            state.pm_error_count += 1
            delay = reconnect_delay
            print(f"  [PM] connection error: {e}, reconnecting in {delay}s...")
            state.pm_connected = False
            await asyncio.sleep(delay)
            reconnect_delay = min(max(delay * 2, 1), 10)


def _pm_apply(asset, asks, bids, state):
    if asks:
        valid = [float(a["price"]) for a in asks if float(a["price"]) < 0.99]
        if valid:
            _pm_set(asset, min(valid), state)
            state.pm_all_filtered = False  # reset — valid prices exist
        elif asks:  # all asks >= 0.99
            if not state.pm_all_filtered:
                state.pm_all_filtered = True
                state.pm_all_filtered_ts = time.time()
                print(f"  [PM] all prices >= 0.99 for {(asset or '')[:12]}.. — market resolved?")
    if bids:
        valid_bids = [float(b["price"]) for b in bids if float(b["price"]) < 0.99]
        if valid_bids:
            _pm_set_bid(asset, max(valid_bids), state)


_pm_filter_log_ts = 0.0


def _pm_set(asset, price, state):
    global _pm_filter_log_ts
    if price >= 0.99:
        now = time.time()
        if now - _pm_filter_log_ts >= 60:
            asset_label = (asset or "")[:12]
            print(f"  [PM] filtered price={price:.4f} for asset={asset_label}.. (>= 0.99)")
            _pm_filter_log_ts = now
        return
    if asset == state.pm_up_id:
        state.pm_up = price
    elif asset == state.pm_dn_id:
        state.pm_dn = price
    state.pm_last_update_ts = time.time()
    state.pm_prices_ready = state.pm_up is not None and state.pm_dn is not None


def _pm_set_bid(asset, price, state):
    if price >= 0.99:
        return
    if asset == state.pm_up_id:
        state.pm_up_bid = price
    elif asset == state.pm_dn_id:
        state.pm_dn_bid = price
