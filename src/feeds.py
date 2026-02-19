import asyncio
import json
import time

import requests
import websockets
from datetime import datetime, timezone, timedelta

import config


class State:
    def __init__(self):
        self.bids: list[tuple[float, float]] = []
        self.asks: list[tuple[float, float]] = []
        self.mid: float = 0.0

        self.trades: list[dict] = []

        self.klines: list[dict] = []
        self.cur_kline: dict | None = None

        self.pm_up_id:  str | None = None
        self.pm_dn_id:  str | None = None
        self.pm_up:     float | None = None
        self.pm_dn:     float | None = None

        self.binance_ws_connected: bool = False
        self.binance_ob_ready: bool = False
        self.binance_ob_last_ok_ts: float = 0.0
        self.pm_connected: bool = False
        self.pm_prices_ready: bool = False
        self.pm_last_update_ts: float = 0.0
        self.pm_reconnect_requested: bool = False


OB_POLL_INTERVAL = 2


def _fetch_binance_depth(url: str, symbol: str) -> dict:
    return requests.get(url, params={"symbol": symbol, "limit": 20}, timeout=3).json()


async def ob_poller(symbol: str, state: State):
    url = f"{config.BINANCE_REST}/depth"
    print(f"  [Binance OB] polling {symbol} every {OB_POLL_INTERVAL}s")
    while True:
        try:
            # Run blocking HTTP call off the event loop.
            resp = await asyncio.to_thread(_fetch_binance_depth, url, symbol)
            state.bids = [(float(p), float(q)) for p, q in resp["bids"]]
            state.asks = [(float(p), float(q)) for p, q in resp["asks"]]
            if state.bids and state.asks:
                state.mid = (state.bids[0][0] + state.asks[0][0]) / 2
                state.binance_ob_ready = True
                state.binance_ob_last_ok_ts = time.time()
        except Exception:
            # Keep last good orderbook state; staleness is handled by trader gate.
            pass
        await asyncio.sleep(OB_POLL_INTERVAL)


async def binance_feed(symbol: str, kline_iv: str, state: State):
    sym = symbol.lower()
    streams = "/".join([
        f"{sym}@trade",
        f"{sym}@kline_{kline_iv}",
    ])
    url = f"{config.BINANCE_WS}?streams={streams}"

    while True:
        try:
            async with websockets.connect(
                url,
                ping_interval=20,
                ping_timeout=60,
                close_timeout=10
            ) as ws:
                print(f"  [Binance WS] connected – {symbol}")
                state.binance_ws_connected = True

                while True:
                    try:
                        data   = json.loads(await ws.recv())
                        stream = data.get("stream", "")
                        pay    = data["data"]

                        if "@trade" in stream:
                            state.trades.append({
                                "t":      pay["T"] / 1000.0,
                                "price":  float(pay["p"]),
                                "qty":    float(pay["q"]),
                                "is_buy": not pay["m"],
                            })
                            if len(state.trades) > 5000:
                                cut = time.time() - config.TRADE_TTL
                                state.trades = [t for t in state.trades if t["t"] >= cut]

                        elif "@kline" in stream:
                            k = pay["k"]
                            candle = {
                                "t": k["t"] / 1000.0,
                                "o": float(k["o"]), "h": float(k["h"]),
                                "l": float(k["l"]), "c": float(k["c"]),
                                "v": float(k["v"]),
                            }
                            state.cur_kline = candle
                            if k["x"]:
                                state.klines.append(candle)
                                state.klines = state.klines[-config.KLINE_MAX:]

                    except websockets.exceptions.ConnectionClosed:
                        print(f"  [Binance WS] connection closed, reconnecting...")
                        state.binance_ws_connected = False
                        break

        except Exception as e:
            print(f"  [Binance WS] connection error: {e}, reconnecting in 5s...")
            state.binance_ws_connected = False
            await asyncio.sleep(5)


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
        data = requests.get(config.PM_GAMMA, params={"slug": slug, "limit": 1}, timeout=5).json()
        if not data or data[0].get("ticker") != slug:
            print(f"  [PM] no active market for slug: {slug}")
            return None
        return data[0]
    except Exception as e:
        print(f"  [PM] event fetch failed ({slug}): {e}")
        return None


def fetch_pm_tokens(coin: str, tf: str) -> tuple:
    """Fetch PM token IDs for up/down markets."""
    event_data = fetch_pm_event_data(coin, tf)
    if event_data is None:
        return None, None
    try:
        ids = json.loads(event_data["markets"][0]["clobTokenIds"])
        return ids[0], ids[1]
    except Exception as e:
        print(f"  [PM] token extraction failed: {e}")
        return None, None


async def pm_feed(state: State):
    if not state.pm_up_id:
        print("  [PM] no tokens for this coin/timeframe – skipped")
        return

    _last_msg_log_ts = 0.0
    _msg_count = 0

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
                ping_timeout=60,
                close_timeout=10
            ) as ws:
                await ws.send(json.dumps({"assets_ids": assets, "type": "market"}))
                print(
                    f"  [PM] connected, subscribed to {len(assets)} assets "
                    f"(up={assets[0][:12]}.. dn={assets[1][:12]}..)"
                )
                state.pm_connected = True
                _msg_count = 0

                while True:
                    # Check if trading_loop requested reconnect (new window = new tokens)
                    if state.pm_reconnect_requested:
                        print("  [PM] reconnect requested (new market window), closing WS...")
                        state.pm_connected = False
                        await ws.close()
                        break

                    try:
                        raw = json.loads(await asyncio.wait_for(ws.recv(), timeout=5))
                        _msg_count += 1

                        # Throttled debug log: show message stats every 60s
                        now = time.time()
                        if now - _last_msg_log_ts >= 60:
                            if isinstance(raw, list):
                                print(
                                    f"  [PM] snapshot: {len(raw)} entries, "
                                    f"pm_up={state.pm_up}, pm_dn={state.pm_dn} "
                                    f"(msgs={_msg_count})"
                                )
                            elif isinstance(raw, dict):
                                print(
                                    f"  [PM] event: {raw.get('event_type', '?')}, "
                                    f"pm_up={state.pm_up}, pm_dn={state.pm_dn} "
                                    f"(msgs={_msg_count})"
                                )
                            else:
                                print(
                                    f"  [PM] heartbeat: pm_up={state.pm_up}, "
                                    f"pm_dn={state.pm_dn} (msgs={_msg_count})"
                                )
                            _last_msg_log_ts = now

                        if isinstance(raw, list):
                            for entry in raw:
                                _pm_apply(entry.get("asset_id"), entry.get("asks", []), state)

                        elif isinstance(raw, dict) and raw.get("event_type") == "price_change":
                            for ch in raw.get("price_changes", []):
                                if ch.get("best_ask"):
                                    _pm_set(ch["asset_id"], float(ch["best_ask"]), state)

                    except asyncio.TimeoutError:
                        # No message in 30s — check reconnect flag and loop
                        continue

                    except websockets.exceptions.ConnectionClosed:
                        print(f"  [PM] connection closed after {_msg_count} msgs, reconnecting...")
                        state.pm_connected = False
                        break

        except Exception as e:
            print(f"  [PM] connection error: {e}, reconnecting in 5s...")
            state.pm_connected = False
            await asyncio.sleep(5)


def _pm_apply(asset, asks, state):
    if asks:
        valid = [float(a["price"]) for a in asks if float(a["price"]) < 0.99]
        if valid:
            _pm_set(asset, min(valid), state)


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
