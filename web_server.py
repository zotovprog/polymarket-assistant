from __future__ import annotations

import asyncio
import os
import sys
import time
from collections import deque
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from fastapi import FastAPI, HTTPException, Request, Response
from fastapi.responses import FileResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel, Field

BASE_DIR = Path(__file__).resolve().parent
SRC_DIR = BASE_DIR / "src"
if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))

import config
import feeds
import indicators as ind
import trading
from telegram_notifier import TelegramNotifier
from telegram_bot import TelegramBot


AUTH_COOKIE = "pm_web_auth"


def _load_access_key_from_env() -> str:
    key = os.environ.get("PM_WEB_ACCESS_KEY", "").strip()
    if not key:
        raise RuntimeError(
            "PM_WEB_ACCESS_KEY is required for web auth. "
            "Set it in environment (Railway Variables / Docker env / shell export)."
        )
    if len(key) < 16:
        raise RuntimeError("PM_WEB_ACCESS_KEY is too short (minimum 16 chars).")
    return key


ACCESS_KEY = _load_access_key_from_env()
print(f"[WEB AUTH] source=env key_length={len(ACCESS_KEY)}")

PM_PRIVATE_KEY = os.environ.get("PM_PRIVATE_KEY", "").strip()
PM_FUNDER = os.environ.get("PM_FUNDER", "").strip()
PM_SIGNATURE_TYPE = 2  # hardcoded: proxy signer
print(
    f"[CREDENTIALS] PM_PRIVATE_KEY={'set' if PM_PRIVATE_KEY else 'MISSING'} "
    f"PM_FUNDER={'set' if PM_FUNDER else 'MISSING'} PM_SIGNATURE_TYPE={PM_SIGNATURE_TYPE}"
)

_telegram = TelegramNotifier()
if _telegram.enabled:
    print(f"[TELEGRAM] notifications enabled for chat_id={_telegram.chat_id}")
else:
    print("[TELEGRAM] notifications disabled (TELEGRAM_BOT_TOKEN or TELEGRAM_CHAT_ID not set)")

_BOT_USERNAME = os.environ.get("TELEGRAM_BOT_USERNAME", "").strip().lstrip("@")
_BOT_SETTINGS_PATH = BASE_DIR / "workspace" / "bot_last_session.json"
_bot: TelegramBot | None = None


def _env_int(name: str, default: int) -> int:
    raw = os.environ.get(name, "").strip()
    if not raw:
        return default
    try:
        return int(raw)
    except Exception:
        return default


def _env_flag(name: str, default: bool) -> bool:
    raw = os.environ.get(name, "").strip().lower()
    if not raw:
        return default
    return raw in {"1", "true", "yes", "on"}


CLIENT_IDLE_FLATTEN_SEC = max(10, _env_int("PM_CLIENT_IDLE_FLATTEN_SEC", 45))
FLATTEN_TIMEOUT_SEC = max(5, _env_int("PM_FLATTEN_TIMEOUT_SEC", 25))
# Default is autonomous run: UI heartbeat is optional unless explicitly enabled.
DEFAULT_CLIENT_IDLE_FLATTEN = _env_flag("PM_CLIENT_IDLE_FLATTEN", False)
ENABLE_FLATTEN_ON_STOP = _env_flag("PM_FLATTEN_ON_STOP", True)

PRESETS: dict[str, dict[str, float | int]] = {
    "safe": {
        "min_bias": 60,
        "min_obi": 0.45,
        "min_price": 0.42,
        "max_price": 0.62,
        "cooldown_sec": 900,
        "max_trades_per_day": 2,
        "eval_interval_sec": 5,
        "tp_pct": 9,
        "sl_pct": 5,
        "max_hold_sec": 1800,
        "reverse_exit_bias": 60,
    },
    "medium": {
        "min_bias": 55,
        "min_obi": 0.40,
        "min_price": 0.40,
        "max_price": 0.68,
        "cooldown_sec": 420,
        "max_trades_per_day": 4,
        "eval_interval_sec": 3,
        "tp_pct": 10,
        "sl_pct": 6,
        "max_hold_sec": 1200,
        "reverse_exit_bias": 55,
    },
    "aggressive": {
        "min_bias": 45,
        "min_obi": 0.30,
        "min_price": 0.35,
        "max_price": 0.80,
        "cooldown_sec": 180,
        "max_trades_per_day": 8,
        "eval_interval_sec": 2,
        "tp_pct": 8,
        "sl_pct": 7,
        "max_hold_sec": 900,
        "reverse_exit_bias": 45,
    },
    "super_aggressive": {
        "min_bias": 12,
        "min_obi": 0.05,
        "min_price": 0.10,
        "max_price": 0.95,
        "cooldown_sec": 5,
        "max_trades_per_day": 300,
        "eval_interval_sec": 1,
        "tp_pct": 4,
        "sl_pct": 7,
        "max_hold_sec": 240,
        "reverse_exit_bias": 20,
    },
    "mega_aggressive": {
        "min_bias": 12,
        "min_obi": 0.05,
        "min_price": 0.10,
        "max_price": 0.95,
        "cooldown_sec": 5,
        "max_trades_per_day": 300,
        "eval_interval_sec": 1,
        "tp_pct": 4,
        "sl_pct": 7,
        "max_hold_sec": 240,
        "reverse_exit_bias": 20,
    },
}


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _mask(value: str) -> str:
    if not value:
        return ""
    if len(value) <= 10:
        return "*" * len(value)
    return f"{value[:6]}...{value[-4:]}"


def _score_trend(st: feeds.State) -> tuple[int, str]:
    score = 0

    obi_v = ind.obi(st.bids, st.asks, st.mid) if st.mid else 0.0
    if obi_v > config.OBI_THRESH:
        score += 1
    elif obi_v < -config.OBI_THRESH:
        score -= 1

    cvd5 = ind.cvd(st.trades, 300)
    score += 1 if cvd5 > 0 else -1 if cvd5 < 0 else 0

    rsi_v = ind.rsi(st.klines)
    if rsi_v is not None:
        if rsi_v > config.RSI_OB:
            score -= 1
        elif rsi_v < config.RSI_OS:
            score += 1

    _, _, hv = ind.macd(st.klines)
    if hv is not None:
        score += 1 if hv > 0 else -1

    vwap_v = ind.vwap(st.klines)
    if vwap_v and st.mid:
        score += 1 if st.mid > vwap_v else -1

    ema_s, ema_l = ind.emas(st.klines)
    if ema_s is not None and ema_l is not None:
        score += 1 if ema_s > ema_l else -1

    bid_walls, ask_walls = ind.walls(st.bids, st.asks)
    score += min(len(bid_walls), 2)
    score -= min(len(ask_walls), 2)

    ha = ind.heikin_ashi(st.klines)
    if len(ha) >= 3:
        tail = ha[-3:]
        if all(c["green"] for c in tail):
            score += 1
        elif all(not c["green"] for c in tail):
            score -= 1

    if score >= 3:
        return score, "BULLISH"
    if score <= -3:
        return score, "BEARISH"
    return score, "NEUTRAL"


def _bias_label(bias: float) -> str:
    if bias > 10:
        return "BULLISH"
    if bias < -10:
        return "BEARISH"
    return "NEUTRAL"


def _profile_rows(klines: list[dict]) -> tuple[float, list[dict]]:
    poc, vp = ind.vol_profile(klines)
    if not vp:
        return poc, []

    max_v = max(v for _, v in vp) or 1.0
    poc_i = min(range(len(vp)), key=lambda i: abs(vp[i][0] - poc))
    half = config.VP_SHOW // 2
    start = max(0, poc_i - half)
    end = min(len(vp), start + config.VP_SHOW)
    start = max(0, end - config.VP_SHOW)

    out: list[dict] = []
    for i in range(end - 1, start - 1, -1):
        price, vol = vp[i]
        out.append(
            {
                "price": price,
                "volume": vol,
                "ratio": vol / max_v if max_v > 0 else 0.0,
                "is_poc": i == poc_i,
            }
        )
    return poc, out


def _signals(st: feeds.State, trend_label: str, trend_score: int, bias: float) -> list[str]:
    out: list[str] = []
    obi_v = ind.obi(st.bids, st.asks, st.mid) if st.mid else 0.0
    if abs(obi_v) > config.OBI_THRESH:
        out.append(f"OBI -> {'BULLISH' if obi_v > 0 else 'BEARISH'} ({obi_v * 100:+.1f}%)")

    cvd5 = ind.cvd(st.trades, 300)
    if cvd5:
        out.append(
            f"CVD 5m -> {'buy pressure' if cvd5 > 0 else 'sell pressure'} (${abs(cvd5):,.2f})"
        )

    rsi_v = ind.rsi(st.klines)
    if rsi_v is not None:
        if rsi_v > config.RSI_OB:
            out.append(f"RSI -> overbought ({rsi_v:.0f})")
        elif rsi_v < config.RSI_OS:
            out.append(f"RSI -> oversold ({rsi_v:.0f})")

    _, _, hv = ind.macd(st.klines)
    if hv is not None:
        out.append(f"MACD hist -> {'bullish' if hv > 0 else 'bearish'}")

    vwap_v = ind.vwap(st.klines)
    if st.mid and vwap_v:
        out.append(f"Price {'above' if st.mid > vwap_v else 'below'} VWAP")

    ema_s, ema_l = ind.emas(st.klines)
    if ema_s is not None and ema_l is not None:
        out.append(f"EMA -> {'golden' if ema_s > ema_l else 'death'} cross")

    ha = ind.heikin_ashi(st.klines)
    if len(ha) >= 3:
        tail = ha[-3:]
        if all(c["green"] for c in tail):
            out.append("HA -> 3+ green candles")
        elif all(not c["green"] for c in tail):
            out.append("HA -> 3+ red candles")

    out.append(f"TREND: {trend_label} ({trend_score:+d})")
    out.append(f"BIAS: {_bias_label(bias)} ({bias:+.1f})")
    return out


class StartRequest(BaseModel):
    mode: str = "observe"
    coin: str = "BTC"
    timeframe: str = "15m"
    preset: str = "medium"
    confirm_live_token: str = ""
    # env field removed: credentials now read from server env vars
    size_usd: float = 5.0
    min_bias: float = 55.0
    min_obi: float = 0.40
    min_price: float = 0.40
    max_price: float = 0.68
    cooldown_sec: int = 420
    max_trades_per_day: int = 4
    eval_interval_sec: int = 3
    tp_pct: float = 10.0
    sl_pct: float = 6.0
    max_hold_sec: int = 1200
    reverse_exit_bias: float = 55.0
    auto_exit_enabled: bool = True
    reverse_exit_enabled: bool = True
    live_entry_require_fill: bool = True
    live_entry_fill_timeout_sec: int = 25
    live_entry_fill_poll_sec: float = 1.0
    keep_unfilled_entry_open: bool = False
    binance_ob_stale_sec: int = 12
    executions_log_file: str = ""
    auto_approve_live: bool = False
    client_watchdog_enabled: bool | None = None


class CommandRequest(BaseModel):
    command: str


class AuthRequest(BaseModel):
    key: str = ""


@dataclass
class SessionRuntime:
    session_id: str
    coin: str = "BTC"
    timeframe: str = "15m"
    mode: trading.TradeMode = trading.TradeMode.OBSERVE
    env: dict[str, str] = field(default_factory=dict)
    engine: trading.TradingEngine | None = None
    feed_state: feeds.State = field(default_factory=feeds.State)
    logs: deque[str] = field(default_factory=lambda: deque(maxlen=500))
    tasks: list[asyncio.Task] = field(default_factory=list)
    events: deque[dict[str, Any]] = field(default_factory=lambda: deque(maxlen=120))
    event_seq: int = 0
    running: bool = False
    started_ts: float = 0.0
    last_error: str = ""
    last_client_seen_ts: float = 0.0
    client_disconnected_mode: bool = False
    client_watchdog_enabled: bool = False
    blocked_max_trades_saved: int | None = None
    lock: asyncio.Lock = field(default_factory=asyncio.Lock)

    def log(self, message: str):
        ts = datetime.now(timezone.utc).strftime("%H:%M:%S")
        self.logs.append(f"[{ts}] {message}")

    def notify(self, level: str, title: str, message: str):
        self.event_seq += 1
        self.events.append(
            {
                "id": self.event_seq,
                "ts": _utc_now_iso(),
                "level": level,
                "title": title,
                "message": message,
            }
        )

    def touch_client(self):
        self.last_client_seen_ts = time.time()
        if (
            self.client_disconnected_mode
            and self.engine is not None
            and self.blocked_max_trades_saved is not None
        ):
            self.engine.cfg.max_trades_per_day = self.blocked_max_trades_saved
            self.blocked_max_trades_saved = None
            self.client_disconnected_mode = False
            self.notify("success", "Client reconnected", "Normal trade limits restored")

    async def _attempt_emergency_flatten(self, reason: str):
        if not self.engine or self.mode != trading.TradeMode.LIVE:
            return
        if self.engine.state.open_position is None:
            return

        self.notify("warning", "Emergency flatten", f"Attempting exit before stop ({reason})")
        self.log(f"[SYS] emergency flatten requested: {reason}")

        deadline = time.time() + FLATTEN_TIMEOUT_SEC
        while time.time() < deadline:
            if self.engine.state.open_position is None:
                self.notify("success", "Emergency flatten", "Position closed")
                return
            self.engine.state.force_close_requested = True
            try:
                self.engine.maybe_close_position(self.feed_state, self.coin, self.timeframe, self.log)
            except Exception as e:
                self.log(f"[SYS] emergency flatten error: {e}")
            await asyncio.sleep(max(0.5, min(2.0, float(self.engine.cfg.eval_interval_sec))))

        if self.engine.state.open_position is not None:
            self.notify(
                "error",
                "Emergency flatten failed",
                f"Could not close within {FLATTEN_TIMEOUT_SEC}s; position may still be open",
            )

    async def _client_watchdog_loop(self):
        while True:
            try:
                await asyncio.sleep(2)
                if (
                    not self.running
                    or self.mode != trading.TradeMode.LIVE
                    or self.engine is None
                    or not self.client_watchdog_enabled
                ):
                    continue

                idle = time.time() - (self.last_client_seen_ts or self.started_ts or time.time())
                if idle < CLIENT_IDLE_FLATTEN_SEC:
                    continue

                if not self.client_disconnected_mode:
                    self.client_disconnected_mode = True
                    if self.blocked_max_trades_saved is None:
                        self.blocked_max_trades_saved = self.engine.cfg.max_trades_per_day
                    # Freeze new entries while client is disconnected.
                    self.engine.cfg.max_trades_per_day = self.engine.state.trades_today
                    self.engine.state.pending_decision = None
                    self.engine.state.pending_sig = ""
                    self.engine.state.pending_key = ""
                    self.engine.state.approval_armed = False
                    self.notify(
                        "warning",
                        "Client disconnected",
                        f"No client heartbeat for {int(idle)}s; flatten protection is active",
                    )
                    self.log(f"[SYS] client idle {int(idle)}s: block new entries and try flatten")

                if self.engine.state.open_position is not None:
                    self.engine.state.force_close_requested = True
            except asyncio.CancelledError:
                raise
            except Exception as e:
                self.log(f"[SYS] client watchdog error: {e}")

    async def _stop_locked(self):
        if (
            ENABLE_FLATTEN_ON_STOP
            and self.mode == trading.TradeMode.LIVE
            and self.engine is not None
            and self.engine.state.open_position is not None
        ):
            if self.blocked_max_trades_saved is None:
                self.blocked_max_trades_saved = self.engine.cfg.max_trades_per_day
            self.engine.cfg.max_trades_per_day = self.engine.state.trades_today
            self.engine.state.pending_decision = None
            self.engine.state.pending_sig = ""
            self.engine.state.pending_key = ""
            self.engine.state.approval_armed = False
            await self._attempt_emergency_flatten("stop")

        if not self.tasks:
            self.running = False
            return
        for task in self.tasks:
            task.cancel()
        await asyncio.gather(*self.tasks, return_exceptions=True)
        self.tasks = []
        self.running = False
        self.log("[SYS] session stopped")
        _telegram.notify_session_stop(coin=self.coin, timeframe=self.timeframe)

    async def stop(self):
        async with self.lock:
            await self._stop_locked()

    async def submit_command(self, command: str) -> bool:
        async with self.lock:
            if not self.engine:
                return False
            ok = self.engine.enqueue_command(command)
            if ok:
                self.engine.process_control_commands(self.feed_state, self.log)
            return ok

    def _validate_start(self, payload: StartRequest) -> trading.TradeMode:
        if payload.preset not in PRESETS:
            raise ValueError(f"unknown preset: {payload.preset}")
        if payload.preset in {"mega_aggressive", "super_aggressive"} and payload.mode.lower().strip() != "paper":
            raise ValueError("SUPER AGGRESSIVE preset is paper-only")

        coin = payload.coin.upper()
        if coin not in config.COINS:
            raise ValueError(f"invalid coin: {coin}")
        if payload.timeframe not in config.COIN_TIMEFRAMES[coin]:
            raise ValueError(
                f"invalid timeframe {payload.timeframe} for {coin}. "
                f"allowed: {', '.join(config.COIN_TIMEFRAMES[coin])}"
            )

        mode_raw = payload.mode.lower().strip()
        if mode_raw == "observe":
            return trading.TradeMode.OBSERVE
        if mode_raw == "paper":
            return trading.TradeMode.PAPER
        if mode_raw == "live":
            if payload.confirm_live_token != trading.LIVE_CONFIRM_TOKEN:
                raise ValueError(
                    f'live mode requires confirm token "{trading.LIVE_CONFIRM_TOKEN}"'
                )
            return trading.TradeMode.LIVE
        raise ValueError("mode must be one of: observe, paper, live")

    async def start(self, payload: StartRequest):
        async with self.lock:
            await self._stop_locked()
            self.logs.clear()
            self.events.clear()
            self.event_seq = 0
            self.last_error = ""
            self.last_client_seen_ts = time.time()
            self.client_disconnected_mode = False
            self.client_watchdog_enabled = (
                DEFAULT_CLIENT_IDLE_FLATTEN
                if payload.client_watchdog_enabled is None
                else bool(payload.client_watchdog_enabled)
            )
            self.blocked_max_trades_saved = None

            self.mode = self._validate_start(payload)
            self.coin = payload.coin.upper()
            self.timeframe = payload.timeframe

            runtime_env: dict[str, str] = {}
            if self.mode == trading.TradeMode.LIVE:
                runtime_env["PM_ENABLE_LIVE"] = "1"
                if not PM_PRIVATE_KEY:
                    raise ValueError("PM_PRIVATE_KEY environment variable is required for live mode")
                if not PM_FUNDER:
                    raise ValueError("PM_FUNDER environment variable is required for live mode")
                runtime_env["PM_PRIVATE_KEY"] = PM_PRIVATE_KEY
                runtime_env["PM_FUNDER"] = PM_FUNDER
                runtime_env["PM_SIGNATURE_TYPE"] = str(PM_SIGNATURE_TYPE)
            self.env = runtime_env

            # Notify credential state at startup for both PAPER and LIVE modes.
            preflight_report: dict[str, Any] | None = None
            if self.mode in {trading.TradeMode.PAPER, trading.TradeMode.LIVE}:
                preflight_report = await asyncio.to_thread(
                    self._run_live_credentials_preflight,
                    runtime_env,
                    self.mode == trading.TradeMode.LIVE,
                )

            exec_log = payload.executions_log_file.strip()
            if not exec_log:
                exec_log = str(BASE_DIR / f"executions.{self.session_id}.jsonl")

            cfg = trading.TradingConfig(
                size_usd=max(5.0, payload.size_usd),
                min_abs_bias=max(0.0, payload.min_bias),
                min_abs_obi=max(0.0, payload.min_obi),
                min_price=max(0.01, payload.min_price),
                max_price=min(0.99, payload.max_price),
                cooldown_sec=max(10, payload.cooldown_sec),
                max_trades_per_day=max(1, payload.max_trades_per_day),
                eval_interval_sec=max(1, payload.eval_interval_sec),
                control_file=f"/tmp/pm_traderctl_{self.session_id}",
                executions_log_file=exec_log,
                binance_ob_stale_sec=max(3, payload.binance_ob_stale_sec),
                live_manual_approval=not payload.auto_approve_live,
                approval_beep_enabled=False,
                approval_sound_command="",
                live_entry_require_fill=payload.live_entry_require_fill,
                live_entry_fill_timeout_sec=max(1, payload.live_entry_fill_timeout_sec),
                live_entry_fill_poll_sec=max(0.2, payload.live_entry_fill_poll_sec),
                live_cancel_unfilled_entry=not payload.keep_unfilled_entry_open,
                auto_exit_enabled=payload.auto_exit_enabled,
                tp_pct=max(0.1, payload.tp_pct),
                sl_pct=max(0.1, payload.sl_pct),
                max_hold_sec=max(30, payload.max_hold_sec),
                reverse_exit_enabled=payload.reverse_exit_enabled,
                reverse_exit_bias=max(1.0, payload.reverse_exit_bias),
            )

            self.feed_state = feeds.State()
            self.feed_state.pm_up_id, self.feed_state.pm_dn_id = await asyncio.to_thread(
                feeds.fetch_pm_tokens, self.coin, self.timeframe
            )

            binance_sym = config.COIN_BINANCE[self.coin]
            kline_iv = config.TF_KLINE[self.timeframe]
            await feeds.bootstrap(binance_sym, kline_iv, self.feed_state)

            def _on_entry(rec):
                _telegram.notify_entry(
                    mode=rec.mode, coin=rec.coin, timeframe=rec.timeframe,
                    side=rec.side, price=rec.price, size_usd=rec.size_usd,
                    status=rec.status, reason=rec.reason, order_id=getattr(rec, "order_id", None),
                )

            def _on_exit(rec):
                _telegram.notify_exit(
                    mode=rec.mode, coin=rec.coin, timeframe=rec.timeframe,
                    side=rec.side, price=rec.price, size_usd=rec.size_usd,
                    status=rec.status, reason=rec.reason,
                    pnl_usd=getattr(rec, "pnl_usd", None),
                    pnl_pct=getattr(rec, "pnl_pct", None),
                    order_id=getattr(rec, "order_id", None),
                )
                if "window_close" in (rec.reason or ""):
                    import re
                    m = re.search(r"(\d+)s remaining", rec.reason or "")
                    remaining = int(m.group(1)) if m else 0
                    _telegram.notify_window_exit(
                        coin=rec.coin, timeframe=rec.timeframe,
                        side=rec.side, remaining_sec=remaining,
                    )

            self.engine = None
            if self.mode != trading.TradeMode.OBSERVE:
                self.engine = trading.TradingEngine(
                    self.mode, cfg, runtime_env=self.env,
                    timeframe=self.timeframe,
                    on_entry_callback=_on_entry,
                    on_exit_callback=_on_exit,
                )
                if self.mode == trading.TradeMode.LIVE:
                    if not preflight_report or not preflight_report.get("ok", False):
                        raise ValueError("live preflight failed: check toast details")

            self.tasks = [
                asyncio.create_task(feeds.ob_poller(binance_sym, self.feed_state)),
                asyncio.create_task(feeds.binance_feed(binance_sym, kline_iv, self.feed_state)),
                asyncio.create_task(feeds.pm_feed(self.feed_state)),
            ]
            if self.engine:
                self.tasks.append(
                    asyncio.create_task(
                        trading.trading_loop(
                            self.feed_state,
                            self.engine,
                            self.coin,
                            self.timeframe,
                            self.log,
                        )
                    )
                )
                if self.mode == trading.TradeMode.LIVE and self.client_watchdog_enabled:
                    self.tasks.append(asyncio.create_task(self._client_watchdog_loop()))

            self.running = True
            self.started_ts = time.time()
            self.log(
                f"[SYS] started mode={self.mode.value} {self.coin} {self.timeframe} "
                f"size=${cfg.size_usd:.2f} eval={cfg.eval_interval_sec}s"
            )
            self.notify(
                "success",
                "Session started",
                f"{self.mode.value.upper()} on {self.coin} {self.timeframe}",
            )
            _telegram.notify_session_start(
                mode=self.mode.value, coin=self.coin,
                timeframe=self.timeframe, size_usd=cfg.size_usd,
                preset=payload.preset,
            )
            if self.mode == trading.TradeMode.LIVE and payload.auto_approve_live:
                self.notify(
                    "warning",
                    "Auto-approve enabled",
                    "All live entries will be sent automatically without manual approve",
                )
            if self.mode == trading.TradeMode.LIVE and not self.client_watchdog_enabled:
                self.notify(
                    "info",
                    "Autonomous run",
                    "Client disconnect watchdog is disabled; strategy keeps running until Stop",
                )

    def _notify_preflight_checks(self, checks: list[dict], prefix: str = "Credentials check"):
        for c in checks:
            status = str(c.get("status", "info")).lower()
            name = c.get("name", "check")
            detail = c.get("detail", "")
            self.notify(
                "error" if status == "error" else ("warning" if status == "warn" else "success"),
                f"{prefix}: {name}",
                str(detail),
            )

    def _run_live_credentials_preflight(
        self, runtime_env: dict[str, str], strict: bool
    ) -> dict[str, Any]:
        key_present = bool(runtime_env.get("PM_PRIVATE_KEY"))
        funder_present = bool(runtime_env.get("PM_FUNDER"))
        sig_type = runtime_env.get("PM_SIGNATURE_TYPE", "0")

        if not key_present and not funder_present:
            if strict:
                self.notify("error", "Credentials", "PM_PRIVATE_KEY and PM_FUNDER are required for live mode")
                return {"ok": False, "checks": []}
            self.notify("warning", "Credentials", "Live credentials not provided (paper mode, check skipped)")
            return {"ok": True, "checks": []}

        if key_present != funder_present:
            missing = "PM_FUNDER" if key_present else "PM_PRIVATE_KEY"
            self.notify("warning", "Credentials", f"Missing {missing}; full preflight skipped")
            return {"ok": not strict, "checks": []}

        probe_env = dict(runtime_env)
        probe_env["PM_ENABLE_LIVE"] = "1"
        probe_env.setdefault("PM_SIGNATURE_TYPE", sig_type or "0")
        try:
            probe = trading.LiveExecutor(trading.TradingConfig(), runtime_env=probe_env)
            report = probe.preflight()
            self._notify_preflight_checks(report.get("checks", []), prefix="Live preflight")
            if report.get("ok", False):
                self.notify(
                    "success",
                    "Credentials OK",
                    f"Signer/funder and API checks passed (sig_type={probe_env.get('PM_SIGNATURE_TYPE')})",
                )
            else:
                self.notify(
                    "error",
                    "Credentials issue",
                    "Some checks failed; review preflight toasts",
                )
            return report
        except Exception as e:
            self.notify("error", "Credentials preflight failed", str(e))
            return {"ok": not strict, "checks": [{"name": "preflight", "status": "error", "detail": str(e)}]}

    def _feed_gate(self) -> dict[str, Any]:
        if not self.engine:
            return {"ready": True, "reason": "trader disabled (observe mode)"}

        st = self.feed_state
        now = time.time()
        if not st.binance_ws_connected:
            return {"ready": False, "reason": "waiting Binance WS connection"}
        if not st.binance_ob_ready or st.mid <= 0:
            return {"ready": False, "reason": "waiting Binance orderbook"}
        if st.binance_ob_last_ok_ts <= 0:
            return {"ready": False, "reason": "waiting Binance orderbook"}
        if (now - st.binance_ob_last_ok_ts) > self.engine.cfg.binance_ob_stale_sec:
            return {"ready": False, "reason": "waiting Binance orderbook (stale)"}
        if not st.klines:
            return {"ready": False, "reason": "waiting Binance candles"}
        if not st.pm_connected:
            return {"ready": False, "reason": "waiting Polymarket WS connection"}
        if not st.pm_prices_ready:
            return {"ready": False, "reason": "waiting Polymarket prices"}
        return {"ready": True, "reason": "ready (Binance + Polymarket)"}

    def snapshot(self) -> dict[str, Any]:
        st = self.feed_state
        trend_score, trend_label = _score_trend(st)
        bias = (
            ind.bias_score(st.bids, st.asks, st.mid, st.trades, st.klines)
            if st.mid and st.klines
            else 0.0
        )
        obi_v = ind.obi(st.bids, st.asks, st.mid) if st.mid else 0.0
        bid_walls, ask_walls = ind.walls(st.bids, st.asks)
        depths = ind.depth_usd(st.bids, st.asks, st.mid) if st.mid else {}
        rsi_v = ind.rsi(st.klines)
        macd_v, sig_v, hist_v = ind.macd(st.klines)
        vwap_v = ind.vwap(st.klines) if st.klines else 0.0
        ema_s, ema_l = ind.emas(st.klines)
        cvd1 = ind.cvd(st.trades, 60)
        cvd3 = ind.cvd(st.trades, 180)
        cvd5 = ind.cvd(st.trades, 300)
        delta1 = ind.cvd(st.trades, config.DELTA_WINDOW)
        poc, profile = _profile_rows(st.klines)
        ha = ind.heikin_ashi(st.klines)

        trader_state = self.engine.snapshot() if self.engine else None
        feed_gate = self._feed_gate()

        # Window timing info
        import window as window_mod
        window_info = window_mod.get_window_info(self.timeframe) if self.running else None

        return {
            "session_id": self.session_id,
            "ts": _utc_now_iso(),
            "running": self.running,
            "started_ts": self.started_ts,
            "last_client_seen_ts": self.last_client_seen_ts,
            "client_disconnected_mode": self.client_disconnected_mode,
            "client_watchdog_enabled": self.client_watchdog_enabled,
            "mode": self.mode.value,
            "coin": self.coin,
            "timeframe": self.timeframe,
            "feed_gate": feed_gate,
            "window": {
                "timeframe": window_info.timeframe,
                "remaining_sec": window_info.remaining_sec,
                "start_ts": window_info.start_ts,
                "end_ts": window_info.end_ts,
                "entry_blocked": window_info.entry_blocked,
                "exit_forced": window_info.exit_forced,
            } if window_info else None,
            "connections": {
                "binance_ws_connected": st.binance_ws_connected,
                "binance_ob_ready": st.binance_ob_ready,
                "binance_ob_age_sec": (
                    max(0.0, time.time() - st.binance_ob_last_ok_ts)
                    if st.binance_ob_last_ok_ts > 0
                    else None
                ),
                "pm_connected": st.pm_connected,
                "pm_prices_ready": st.pm_prices_ready,
            },
            "market": {
                "price": st.mid,
                "pm_up": st.pm_up,
                "pm_down": st.pm_dn,
                "trend": {"score": trend_score, "label": trend_label},
                "bias": {"value": bias, "label": _bias_label(bias), "pct": abs(bias)},
                "summary": self._market_summary(trend_score, trend_label, bias),
                "orderbook": {
                    "obi": obi_v,
                    "depth": depths,
                    "buy_walls": bid_walls[:3],
                    "sell_walls": ask_walls[:3],
                },
                "technical": {
                    "rsi": rsi_v,
                    "macd": macd_v,
                    "signal": sig_v,
                    "macd_hist": hist_v,
                    "vwap": vwap_v,
                    "ema5": ema_s,
                    "ema20": ema_l,
                    "ha_last8": [c["green"] for c in ha[-8:]] if ha else [],
                },
                "flow": {
                    "cvd_1m": cvd1,
                    "cvd_3m": cvd3,
                    "cvd_5m": cvd5,
                    "delta_1m": delta1,
                    "poc": poc,
                    "profile": profile,
                },
                "signals": _signals(st, trend_label, trend_score, bias),
            },
            "trader": trader_state,
            "logs": list(self.logs)[-200:],
            "events": list(self.events),
            "env_meta": {
                "PM_ENABLE_LIVE": self.env.get("PM_ENABLE_LIVE", ""),
                "PM_PRIVATE_KEY_SET": bool(self.env.get("PM_PRIVATE_KEY")),
                "PM_FUNDER_SET": bool(self.env.get("PM_FUNDER")),
                "PM_SIGNATURE_TYPE": self.env.get("PM_SIGNATURE_TYPE", str(PM_SIGNATURE_TYPE)),
            },
            "error": self.last_error,
        }

    def _market_summary(self, trend_score: int, trend_label: str, bias: float) -> str:
        st = self.feed_state
        if st.mid <= 0 or not st.klines:
            return (
                f"{self.coin} {self.timeframe} is warming up. "
                "Waiting for a stable feed from Binance and Polymarket."
            )

        cvd5 = ind.cvd(st.trades, 300)
        obi_v = ind.obi(st.bids, st.asks, st.mid) if st.mid else 0.0
        up = st.pm_up
        dn = st.pm_dn
        pressure = "buy pressure" if cvd5 > 0 else ("sell pressure" if cvd5 < 0 else "flat flow")
        skew = "bullish" if obi_v > 0 else ("bearish" if obi_v < 0 else "neutral")
        summary = (
            f"{self.coin} {self.timeframe}: {trend_label.lower()} structure "
            f"(trend {trend_score:+d}, bias {bias:+.1f}%). "
            f"Orderbook skew is {skew} ({obi_v * 100:+.1f}%), with {pressure} on 5m CVD "
            f"(${abs(cvd5):,.0f}). "
        )
        if up is not None and dn is not None:
            summary += f"Polymarket quotes are Up {up:.3f} / Down {dn:.3f}."
        else:
            summary += "Polymarket quotes are still loading."
        return summary


# Singleton session: all clients (web, Telegram bot) share one session
SESSIONS: dict[str, SessionRuntime] = {"primary": SessionRuntime(session_id="primary")}


def _get_primary_session() -> SessionRuntime | None:
    """Return the singleton session."""
    return SESSIONS.get("primary")


async def _bot_start_session(params: dict) -> None:
    """Start a session from Telegram bot with given params dict."""
    session = SESSIONS["primary"]

    preset_name = params.get("preset", "medium")
    preset_vals = PRESETS.get(preset_name, PRESETS["medium"])

    payload = StartRequest(
        mode=params.get("mode", "paper"),
        coin=params.get("coin", "BTC"),
        timeframe=params.get("timeframe", "15m"),
        preset=preset_name,
        confirm_live_token=trading.LIVE_CONFIRM_TOKEN if params.get("mode") == "live" else "",
        size_usd=params.get("size_usd", 5.0),
        min_bias=preset_vals.get("min_bias", 55),
        min_obi=preset_vals.get("min_obi", 0.40),
        min_price=preset_vals.get("min_price", 0.40),
        max_price=preset_vals.get("max_price", 0.68),
        cooldown_sec=int(preset_vals.get("cooldown_sec", 420)),
        max_trades_per_day=int(preset_vals.get("max_trades_per_day", 4)),
        eval_interval_sec=int(preset_vals.get("eval_interval_sec", 3)),
        tp_pct=preset_vals.get("tp_pct", 10),
        sl_pct=preset_vals.get("sl_pct", 6),
        max_hold_sec=int(preset_vals.get("max_hold_sec", 1200)),
        reverse_exit_bias=preset_vals.get("reverse_exit_bias", 55),
        auto_approve_live=True,
    )
    await session.start(payload)


async def _bot_stop_session() -> None:
    """Stop the primary running session."""
    session = _get_primary_session()
    if session and session.running:
        await session.stop()


def _is_authorized(request: Request) -> bool:
    return request.cookies.get(AUTH_COOKIE, "") == ACCESS_KEY


def _require_auth(request: Request):
    if not _is_authorized(request):
        raise HTTPException(status_code=401, detail="unauthorized: provide access key")


def _set_auth_cookie(response: Response):
    response.set_cookie(
        AUTH_COOKIE,
        ACCESS_KEY,
        httponly=True,
        samesite="lax",
        max_age=60 * 60 * 24 * 30,
    )


def _get_singleton_session() -> SessionRuntime:
    """Return the single shared session used by all clients."""
    return SESSIONS["primary"]


def _load_saved_settings() -> dict:
    """Load last-used settings from bot_last_session.json (survives restarts)."""
    import json as _json
    try:
        if _BOT_SETTINGS_PATH.exists():
            return _json.loads(_BOT_SETTINGS_PATH.read_text())
    except Exception:
        pass
    return {}


app = FastAPI(title="Polymarket Assistant Web")
app.mount("/static", StaticFiles(directory=str(BASE_DIR / "web")), name="static")


@app.on_event("startup")
async def _startup():
    global _bot
    if _telegram.enabled and _BOT_USERNAME:
        _bot = TelegramBot(
            notifier=_telegram,
            get_session=_get_primary_session,
            start_session=_bot_start_session,
            stop_session=_bot_stop_session,
            presets=PRESETS,
            coin_timeframes=config.COIN_TIMEFRAMES,
            settings_path=_BOT_SETTINGS_PATH,
        )
        await _bot.start_polling()
        print(f"[TELEGRAM BOT] interactive bot started for @{_BOT_USERNAME}")
    else:
        if not _BOT_USERNAME:
            print("[TELEGRAM BOT] disabled (TELEGRAM_BOT_USERNAME not set)")



@app.get("/")
async def root() -> FileResponse:
    return FileResponse(BASE_DIR / "web" / "index.html")


@app.post("/api/auth")
async def api_auth(payload: AuthRequest, response: Response):
    if payload.key.strip() != ACCESS_KEY:
        raise HTTPException(status_code=401, detail="invalid access key")
    _set_auth_cookie(response)
    return {"ok": True}


@app.get("/api/bootstrap")
async def api_bootstrap(request: Request):
    _require_auth(request)
    session = _get_singleton_session()
    session.touch_client()
    return {
        "ok": True,
        "session_id": session.session_id,
        "live_confirm_token": trading.LIVE_CONFIRM_TOKEN,
        "coins": config.COINS,
        "coin_timeframes": config.COIN_TIMEFRAMES,
        "presets": PRESETS,
        "defaults": {
            "client_watchdog_enabled": DEFAULT_CLIENT_IDLE_FLATTEN,
        },
        "credentials_available": bool(PM_PRIVATE_KEY and PM_FUNDER),
        "saved_settings": _load_saved_settings(),
        "state": session.snapshot(),
    }


@app.get("/api/state")
async def api_state(request: Request):
    _require_auth(request)
    session = _get_singleton_session()
    session.touch_client()
    return {"ok": True, "state": session.snapshot()}


@app.post("/api/start")
async def api_start(payload: StartRequest, request: Request):
    _require_auth(request)
    session = _get_singleton_session()
    session.touch_client()
    try:
        await session.start(payload)
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    if _bot:
        _bot.save_settings_from_web(
            mode=payload.mode, coin=payload.coin, timeframe=payload.timeframe,
            preset=payload.preset, size_usd=payload.size_usd,
        )
    return {"ok": True, "state": session.snapshot()}


@app.post("/api/stop")
async def api_stop(request: Request):
    _require_auth(request)
    session = _get_singleton_session()
    session.touch_client()
    await session.stop()
    return {"ok": True, "state": session.snapshot()}


@app.post("/api/command")
async def api_command(payload: CommandRequest, request: Request):
    _require_auth(request)
    session = _get_singleton_session()
    session.touch_client()
    if not session.engine:
        raise HTTPException(status_code=400, detail="trader is disabled in observe mode")
    ok = await session.submit_command(payload.command)
    if not ok:
        raise HTTPException(status_code=400, detail="empty/invalid command")
    return {"ok": True, "state": session.snapshot()}


@app.on_event("shutdown")
async def _shutdown():
    if _bot:
        await _bot.stop()
    session = SESSIONS.get("primary")
    if session:
        await session.stop()
    await _telegram.close()
