"""Interactive Telegram bot for controlling trading sessions via inline keyboards.

Usage: instantiated and started from web_server.py when TELEGRAM_BOT_USERNAME is set.
Polling runs inside the same event loop as FastAPI.
"""

from __future__ import annotations

import asyncio
import json
import os
import time
import traceback
from pathlib import Path
from typing import Any, Callable

from telegram_notifier import TelegramNotifier


def _fetch_polymarket_balance() -> float | None:
    """Fetch USDC collateral balance from Polymarket. Returns None if unavailable."""
    try:
        private_key = os.environ.get("PM_PRIVATE_KEY", "").strip()
        funder = os.environ.get("PM_FUNDER", "").strip()
        if not private_key or not funder:
            return None

        from py_clob_client.client import ClobClient
        from py_clob_client.clob_types import BalanceAllowanceParams, AssetType

        host = "https://clob.polymarket.com"
        chain_id = 137
        sig_type = int(os.environ.get("PM_SIGNATURE_TYPE", "2"))

        client = ClobClient(host, key=private_key, chain_id=chain_id)
        api_creds = client.create_or_derive_api_creds()
        client = ClobClient(
            host,
            key=private_key,
            chain_id=chain_id,
            creds=api_creds,
            signature_type=sig_type,
            funder=funder,
        )

        params = BalanceAllowanceParams(asset_type=AssetType.COLLATERAL, signature_type=-1)
        data = client.get_balance_allowance(params)
        bal_raw = str((data or {}).get("balance", "")).strip()
        if not bal_raw:
            return None
        # Balance is in raw units (6 decimals for USDC)
        val = float(bal_raw)
        if val > 1_000_000:
            val = val / 1_000_000  # convert from raw to USDC
        return val
    except Exception as e:
        print(f"  [TELEGRAM BOT] balance fetch error: {e}")
        return None

# Preset descriptions (short labels for keyboard buttons)
PRESET_LABELS: dict[str, str] = {
    "safe": "Safe (conservative)",
    "medium": "Medium (balanced)",
    "aggressive": "Aggressive (high risk)",
}

AMOUNT_OPTIONS: list[float] = [5, 10, 25, 50, 100]

# Allowed Telegram usernames (without @, lowercased)
ALLOWED_USERS: set[str] = {"collideadron"}


def _btn(text: str, data: str) -> dict:
    return {"text": text, "callback_data": data}


class TelegramBot:
    """Long-polling Telegram bot with inline keyboard controls."""

    def __init__(
        self,
        notifier: TelegramNotifier,
        get_session: Callable,
        start_session: Callable,
        stop_session: Callable,
        presets: dict[str, dict],
        coin_timeframes: dict[str, list[str]],
        settings_path: Path,
    ):
        self._n = notifier
        self._get_session = get_session
        self._start_session = start_session
        self._stop_session = stop_session
        self._presets = presets
        self._coin_timeframes = coin_timeframes
        self._settings_path = settings_path
        self._bot_username: str = ""
        self._bot_id: int = 0
        self._offset: int = 0
        self._poll_task: asyncio.Task | None = None
        self._waiting_custom_amount_from: int | None = None  # user_id expecting amount text

    # ---- session logging helper ----

    def _log(self, msg: str) -> None:
        """Log a message to the active session so it shows in the web UI terminal."""
        session = self._get_session()
        if session:
            session.log(f"[TG] {msg}")

    def _notify(self, level: str, title: str, message: str) -> None:
        """Push a toast notification to the web UI."""
        session = self._get_session()
        if session:
            session.notify(level, f"[Telegram] {title}", message)

    # ---- lifecycle ----

    async def start_polling(self) -> None:
        me = await self._n.get_me()
        if me:
            self._bot_username = me.get("username", "")
            self._bot_id = me.get("id", 0)
            print(f"  [TELEGRAM BOT] logged in as @{self._bot_username} (id={self._bot_id})")
        else:
            print("  [TELEGRAM BOT] WARNING: getMe failed, mention detection may not work")
        self._poll_task = asyncio.get_running_loop().create_task(self._poll_loop())

    async def stop(self) -> None:
        if self._poll_task:
            self._poll_task.cancel()
            try:
                await self._poll_task
            except asyncio.CancelledError:
                pass
            self._poll_task = None

    # ---- polling ----

    async def _poll_loop(self) -> None:
        print("  [TELEGRAM BOT] polling started")
        while True:
            try:
                updates = await self._n.get_updates(offset=self._offset, timeout=30)
                for upd in updates:
                    self._offset = upd["update_id"] + 1
                    await self._handle_update(upd)
            except asyncio.CancelledError:
                raise
            except Exception as e:
                print(f"  [TELEGRAM BOT] poll error: {e}")
                traceback.print_exc()
                await asyncio.sleep(5)

    # ---- routing ----

    def _is_allowed_user(self, user: dict) -> bool:
        """Check if user is in the allowed list."""
        username = (user.get("username") or "").lower()
        return username in ALLOWED_USERS

    async def _handle_update(self, upd: dict) -> None:
        # Callback query (button press)
        cb = upd.get("callback_query")
        if cb:
            if not self._is_allowed_user(cb.get("from", {})):
                await self._n.answer_callback_query(cb["id"], text="\u26d4 Access denied", alert=True)
                return
            await self._handle_callback(cb)
            return

        # Message (check for @mention)
        msg = upd.get("message")
        if msg and self._is_mention(msg):
            if not self._is_allowed_user(msg.get("from", {})):
                return  # silently ignore unauthorized mentions
            # If waiting for custom amount text
            if self._waiting_custom_amount_from and msg.get("from", {}).get("id") == self._waiting_custom_amount_from:
                await self._handle_custom_amount_text(msg)
                return
            await self._send_main_menu(msg)
            return

        # Plain text message — check if we're waiting for custom amount
        if msg and self._waiting_custom_amount_from:
            if msg.get("from", {}).get("id") == self._waiting_custom_amount_from:
                if not self._is_allowed_user(msg.get("from", {})):
                    return
                await self._handle_custom_amount_text(msg)
                return

    def _is_mention(self, msg: dict) -> bool:
        """Check if the bot is mentioned in the message."""
        chat = msg.get("chat", {})
        chat_id = str(chat.get("id", ""))

        # Check for thread_id match if configured (group with topics)
        if self._n.thread_id:
            if msg.get("message_thread_id") != self._n.thread_id:
                return False
        else:
            if chat_id != str(self._n.chat_id):
                return False

        # In private chat, every message is directed at the bot
        if chat.get("type") == "private":
            return True

        # Check entities for @mention (group chats)
        text = msg.get("text", "")
        for ent in msg.get("entities", []):
            if ent.get("type") == "mention":
                offset = ent["offset"]
                length = ent["length"]
                mention = text[offset:offset + length].lstrip("@").lower()
                if mention == self._bot_username.lower():
                    return True

        # Fallback: check if message is a reply to the bot
        reply = msg.get("reply_to_message", {})
        if reply.get("from", {}).get("id") == self._bot_id:
            return True

        return False

    # ---- main menu ----

    def _default_quick_start_settings(self) -> dict:
        return {
            "mode": "live",
            "coin": "BTC",
            "timeframe": "15m",
            "preset": "medium",
            "size_usd": 5.0,
        }

    def _format_size_compact(self, value: Any) -> str:
        try:
            amount = float(value)
        except (TypeError, ValueError):
            amount = 5.0
        if abs(amount - round(amount)) < 1e-9:
            return f"${int(round(amount))}"
        return f"${amount:.2f}".rstrip("0").rstrip(".")

    def _preset_short_label(self, preset: Any) -> str:
        raw = str(preset or "medium").strip().lower()
        if not raw:
            raw = "medium"
        return raw.capitalize()

    def _main_menu_text(self) -> str:
        return f"{self._status_line()}\nВыберите действие:"

    def _start_button_text(self) -> str:
        params = self._load_settings()
        if not isinstance(params, dict):
            params = {}
        defaults = self._default_quick_start_settings()
        for k, v in defaults.items():
            params.setdefault(k, v)

        coin = str(params.get("coin", "BTC")).upper()
        tf = str(params.get("timeframe", "15m"))
        preset = self._preset_short_label(params.get("preset", "medium"))
        size = self._format_size_compact(params.get("size_usd", 5.0))
        return f"\u25b6\ufe0f Старт: {coin} {tf} {preset} {size}"

    def _main_keyboard(self) -> list[list[dict]]:
        session = self._get_session()
        if session and session.running:
            return [
                [_btn("\U0001f4ca Статус", "status")],
                [_btn("\u23f9 Стоп", "stop")],
                [_btn("\u2699\ufe0f Настройки", "settings_menu")],
            ]
        return [
            [_btn(self._start_button_text(), "quick_start")],
            [_btn("\U0001f4ca Статус", "status")],
            [_btn("\u2699\ufe0f Настройки", "settings_menu")],
        ]

    def _status_line(self) -> str:
        session = self._get_session()
        if session and session.running:
            mode = session.mode.value.upper()
            return f"\U0001f7e2 <b>Бот запущен</b> — {mode}"
        return "\U0001f534 <b>Бот остановлен</b>"

    async def _send_main_menu(self, msg: dict) -> None:
        await self._n.send_with_keyboard(self._main_menu_text(), self._main_keyboard())

    # ---- callback handlers ----

    async def _handle_callback(self, cb: dict) -> None:
        data = cb.get("data", "")
        cb_id = cb["id"]
        msg = cb.get("message", {})
        msg_id = msg.get("message_id", 0)

        if data == "status":
            await self._cb_status(cb_id, msg_id)
        elif data == "stop":
            await self._cb_stop(cb_id, msg_id)
        elif data == "quick_start":
            await self._cb_quick_start(cb_id, msg_id)
        elif data == "settings_menu":
            await self._cb_settings_menu(cb_id, msg_id)
        elif data == "start_menu":
            await self._cb_start_menu(cb_id, msg_id)
        elif data.startswith("start:"):
            mode = data.split(":", 1)[1]
            await self._cb_start(cb_id, msg_id, mode)
        elif data == "restart_menu":
            await self._cb_restart_menu(cb_id, msg_id)
        elif data.startswith("restart:"):
            mode = data.split(":", 1)[1]
            await self._cb_restart(cb_id, msg_id, mode)
        elif data == "strategy_menu":
            await self._cb_strategy_menu(cb_id, msg_id)
        elif data.startswith("strategy:"):
            preset = data.split(":", 1)[1]
            await self._cb_strategy(cb_id, msg_id, preset)
        elif data == "amount_menu":
            await self._cb_amount_menu(cb_id, msg_id)
        elif data.startswith("amount:"):
            val = data.split(":", 1)[1]
            if val == "custom":
                await self._cb_amount_custom(cb_id, msg_id, cb)
            else:
                await self._cb_amount(cb_id, msg_id, float(val))
        elif data == "coin_menu":
            await self._cb_coin_menu(cb_id, msg_id)
        elif data.startswith("coin:"):
            coin = data.split(":", 1)[1]
            await self._cb_coin(cb_id, msg_id, coin)
        elif data == "tf_menu":
            await self._cb_tf_menu(cb_id, msg_id)
        elif data.startswith("tf:"):
            tf = data.split(":", 1)[1]
            await self._cb_tf(cb_id, msg_id, tf)
        elif data == "back_settings":
            await self._cb_back_settings(cb_id, msg_id)
        elif data == "back":
            await self._cb_back(cb_id, msg_id)
        else:
            await self._n.answer_callback_query(cb_id)

    # ---- Status ----

    async def _cb_status(self, cb_id: str, msg_id: int) -> None:
        await self._n.answer_callback_query(cb_id)

        # Fetch real Polymarket balance in a thread (blocking I/O)
        loop = asyncio.get_running_loop()
        pm_balance = await loop.run_in_executor(None, _fetch_polymarket_balance)

        session = self._get_session()
        if not session or not session.running:
            params = self._load_settings()
            preset = str(params.get("preset", "medium"))
            size = float(params.get("size_usd", 5.0))
            coin = str(params.get("coin", "BTC")).upper()
            tf = str(params.get("timeframe", "15m"))
            min_bias = float(self._presets.get(preset, {}).get("min_bias", 55.0))
            bal_line = f"\n\U0001f4b0 Баланс Polymarket: <code>${pm_balance:.2f}</code>" if pm_balance is not None else ""

            # Window info for saved timeframe
            import window as window_mod
            winfo = window_mod.get_window_info(tf)
            w_m, w_s = divmod(winfo.remaining_sec, 60)
            win_line = f"\n\u23f1 Окно {tf}: <code>{w_m}m {w_s:02d}s</code> до закрытия"

            html = (
                "\U0001f4a4 Бот остановлен\n"
                "\U0001f534 <b>Trading Bot — STOPPED</b>\n"
                f"Пара: <code>{coin} {tf}</code>\n"
                f"Стратегия: <code>{self._preset_short_label(preset)}</code> | Размер: <code>${size:.2f}</code>\n"
                f"Следующий вход при bias > {min_bias:.1f}{bal_line}{win_line}"
            )
        else:
            mode = session.mode.value.upper()
            coin = session.coin
            tf = session.timeframe
            preset = self._detect_preset(session)
            cfg = session.engine.cfg if session.engine else None
            size = float(cfg.size_usd) if cfg else 0.0
            min_bias = float(cfg.min_abs_bias) if cfg else 0.0

            snap = session.engine.snapshot() if session.engine else {}
            trades_today = snap.get("trades_today", 0)
            max_trades = snap.get("max_trades_per_day", 0)

            # Market data from feed state
            st = session.feed_state
            price = st.mid if st.mid else 0

            from indicators import bias_score
            bias = bias_score(st.bids, st.asks, st.mid, st.trades, st.klines) if st.mid and st.klines else 0.0

            gate_reason = self._feed_gate_block_reason(session)
            open_pos_obj = session.engine.state.open_position if session.engine else None
            cb_active = bool(session.engine and session.engine.state.circuit_breaker_active)
            cb_reason = session.engine.state.circuit_breaker_reason if cb_active and session.engine else ""

            if gate_reason:
                activity_line = f"\u23f3 Ожидание данных ({gate_reason})"
            elif open_pos_obj:
                hold_price = self._position_display_price(session, open_pos_obj.side, open_pos_obj.entry_price)
                if hold_price:
                    activity_line = f"\U0001f4c8 Держит позицию {open_pos_obj.side} @ {hold_price:.3f}"
                else:
                    activity_line = f"\U0001f4c8 Держит позицию {open_pos_obj.side}"
            elif cb_active:
                activity_line = "\U0001f6ab Circuit breaker активен"
            else:
                activity_line = f"\U0001f50d Ищет сигнал (bias: {bias:+.1f})"

            html = (
                f"{activity_line}\n"
                f"\U0001f7e2 <b>Trading Bot — RUNNING</b>\n"
                f"<code>{mode}</code> | <b>{coin} {tf}</b> | Стратегия: <code>{self._preset_short_label(preset)}</code>\n"
                f"Размер: <code>${size:.2f}</code> | Сделки: <code>{trades_today}/{max_trades}</code>\n"
                f"Следующий вход при bias > {min_bias:.1f}\n"
            )
            if price:
                html += f"Цена: <code>{price:.3f}</code> | Bias: <code>{bias:+.1f}</code>\n"

            if cb_active and cb_reason:
                html += f"Причина circuit breaker: <code>{cb_reason}</code>\n"

            # Polymarket USDC balance
            if pm_balance is not None:
                html += f"\U0001f4b0 Баланс Polymarket: <code>${pm_balance:.2f}</code>\n"

            # Window info
            import window as window_mod
            winfo = window_mod.get_window_info(tf)
            w_m, w_s = divmod(winfo.remaining_sec, 60)
            entry_status = "ЗАБЛОКИРОВАН \u26d4" if winfo.entry_blocked else "ОТКРЫТ \u2705"
            html += f"\u23f1 Окно: <code>{w_m}m {w_s:02d}s</code> до закрытия | Вход: {entry_status}\n"

            # Total PnL from completed trades
            trades_list = snap.get("trades", [])
            total_pnl_usd = 0.0
            wins = 0
            losses = 0
            for t in trades_list:
                if t.get("action") != "exit":
                    continue
                if t.get("is_execution") is False:
                    continue
                pnl = t.get("pnl_usd")
                if pnl is not None:
                    total_pnl_usd += pnl
                    if pnl >= 0:
                        wins += 1
                    else:
                        losses += 1
            closed_count = wins + losses
            if closed_count > 0:
                pnl_emoji = "\U0001f7e2" if total_pnl_usd >= 0 else "\U0001f534"
                winrate = wins / closed_count * 100
                html += (
                    f"\n\U0001f4b5 <b>PnL сессии:</b> {pnl_emoji} <code>${total_pnl_usd:+.2f}</code>\n"
                    f"W/L: <code>{wins}/{losses}</code> ({winrate:.0f}% win rate)\n"
                )

            # Open position
            pos = snap.get("open_position")
            if pos:
                entry_price = float(pos.get("entry_price", 0) or 0)
                side = str(pos.get("side", "?"))
                mark_price = self._position_display_price(session, side, entry_price)
                if entry_price and mark_price:
                    side_up = side.upper() in {"UP", "YES", "LONG"}
                    if side_up:
                        pnl_pct = ((mark_price - entry_price) / entry_price) * 100
                    else:
                        pnl_pct = ((entry_price - mark_price) / entry_price) * 100
                    pnl_emoji = "\U0001f7e2" if pnl_pct >= 0 else "\U0001f534"
                    html += (
                        f"\n<b>Открытая позиция:</b> {side} @ <code>{entry_price:.3f}</code>\n"
                        f"Текущая цена: <code>{mark_price:.3f}</code> | PnL: {pnl_emoji} <code>{pnl_pct:+.1f}%</code>"
                    )

        keyboard = [[_btn("\u25c0 Back", "back")]]
        await self._n.edit_message_text(msg_id, html, keyboard)

    # ---- Stop ----

    async def _cb_stop(self, cb_id: str, msg_id: int) -> None:
        await self._n.answer_callback_query(cb_id)
        session = self._get_session()
        if not session or not session.running:
            html = "\u26a0\ufe0f Бот уже остановлен."
            keyboard = [[_btn("\u25c0 Back", "back")]]
            await self._n.edit_message_text(msg_id, html, keyboard)
            return

        self._log("Stop requested via Telegram")
        self._notify("warning", "Stop", "Session stop requested via Telegram")
        await self._stop_session()
        self._log("Session stopped via Telegram")
        html = "\u2705 <b>Сессия остановлена.</b>"
        keyboard = [[_btn("\u25c0 Back", "back")]]
        await self._n.edit_message_text(msg_id, html, keyboard)

    # ---- Quick start ----

    async def _cb_quick_start(self, cb_id: str, msg_id: int) -> None:
        await self._n.answer_callback_query(cb_id)
        session = self._get_session()
        if session and session.running:
            html = "\u26a0\ufe0f Бот уже запущен. Остановите сессию перед новым стартом."
            keyboard = [[_btn("\u25c0 Back", "back")]]
            await self._n.edit_message_text(msg_id, html, keyboard)
            return

        env_raw = os.environ.get("PM_DEFAULT_SETTINGS", "").strip()
        has_env_settings = False
        if env_raw:
            try:
                json.loads(env_raw)
                has_env_settings = True
            except Exception:
                has_env_settings = False

        has_saved_settings = self._settings_path.exists() or has_env_settings
        params = self._load_settings()
        if not isinstance(params, dict) or not has_saved_settings:
            params = self._default_quick_start_settings()

        defaults = self._default_quick_start_settings()
        for k, v in defaults.items():
            params.setdefault(k, v)

        params["mode"] = "live"
        params["auto_approve_live"] = True

        try:
            self._log(
                f"Quick start via Telegram: LIVE {params.get('coin','BTC')} "
                f"{params.get('timeframe','15m')} strategy={params.get('preset','medium')} "
                f"size=${float(params.get('size_usd', 5.0)):.2f}"
            )
            await self._start_session(params)
            self._save_settings(params)

            coin = str(params.get("coin", "BTC")).upper()
            tf = str(params.get("timeframe", "15m"))
            preset = self._preset_short_label(params.get("preset", "medium"))
            size = float(params.get("size_usd", 5.0))
            self._notify("success", "Quick start", f"LIVE {coin} {tf} | {preset} | ${size:.2f}")

            loop = asyncio.get_running_loop()
            pm_balance = await loop.run_in_executor(None, _fetch_polymarket_balance)
            bal_line = f"\n\U0001f4b0 Баланс: <code>${pm_balance:.2f}</code>" if pm_balance is not None else ""

            html = (
                "\u2705 <b>Сессия запущена — LIVE</b>\n"
                f"<b>{coin} {tf}</b> | Стратегия: <code>{preset}</code> | Размер: <code>${size:.2f}</code>"
                f"{bal_line}"
            )
        except Exception as e:
            self._log(f"Quick start failed: {e}")
            self._notify("error", "Quick start failed", str(e))
            html = f"\u274c <b>Ошибка запуска:</b> {e}"

        keyboard = [[_btn("\u25c0 Back", "back")]]
        await self._n.edit_message_text(msg_id, html, keyboard)

    # ---- Settings menu ----

    def _settings_keyboard(self) -> list[list[dict]]:
        params = self._load_settings()
        if not isinstance(params, dict):
            params = {}

        coin = str(params.get("coin", "BTC")).upper()
        tf = str(params.get("timeframe", "15m"))
        preset = self._preset_short_label(params.get("preset", "medium"))
        size = self._format_size_compact(params.get("size_usd", 5.0))

        return [
            [_btn(f"\u041c\u043e\u043d\u0435\u0442\u0430: {coin} \u2705", "coin_menu"), _btn(f"\u0422\u0430\u0439\u043c\u0444\u0440\u0435\u0439\u043c: {tf} \u2705", "tf_menu")],
            [_btn(f"\u0421\u0442\u0440\u0430\u0442\u0435\u0433\u0438\u044f: {preset} \u2705", "strategy_menu"), _btn(f"\u0420\u0430\u0437\u043c\u0435\u0440: {size} \u2705", "amount_menu")],
            [_btn("\u25c0 \u041d\u0430\u0437\u0430\u0434", "back")],
        ]

    def _settings_menu_text(self, note: str = "") -> str:
        html = "\u2699\ufe0f <b>\u041d\u0430\u0441\u0442\u0440\u043e\u0439\u043a\u0438</b>"
        if note:
            html += f"\n\n{note}"
        return html

    async def _show_settings_menu(self, msg_id: int, note: str = "") -> None:
        await self._n.edit_message_text(msg_id, self._settings_menu_text(note), self._settings_keyboard())

    async def _cb_settings_menu(self, cb_id: str, msg_id: int) -> None:
        await self._n.answer_callback_query(cb_id)
        self._waiting_custom_amount_from = None
        await self._show_settings_menu(msg_id)

    async def _cb_back_settings(self, cb_id: str, msg_id: int) -> None:
        await self._n.answer_callback_query(cb_id)
        self._waiting_custom_amount_from = None
        await self._show_settings_menu(msg_id)

    async def _restart_running_session(self, params: dict) -> tuple[bool, str]:
        session = self._get_session()
        if not session or not session.running:
            return True, ""

        mode_obj = getattr(session, "mode", None)
        mode = getattr(mode_obj, "value", str(mode_obj or "live")).strip().lower()
        if "." in mode:
            mode = mode.split(".")[-1]
        if not mode:
            mode = "live"

        restart_params = dict(params)
        restart_params["mode"] = mode
        if mode == "live":
            restart_params["auto_approve_live"] = True

        try:
            self._log(
                f"Auto-restart via Telegram: {mode.upper()} {restart_params.get('coin','BTC')} "
                f"{restart_params.get('timeframe','15m')}"
            )
            await self._stop_session()
            await self._start_session(restart_params)
            self._save_settings(restart_params)
            return True, mode
        except Exception as e:
            self._log(f"Auto-restart failed: {e}")
            self._notify("error", "Auto-restart failed", str(e))
            return False, str(e)

    # ---- Start menu (mode selection) ----

    async def _cb_start_menu(self, cb_id: str, msg_id: int) -> None:
        await self._n.answer_callback_query(cb_id)
        session = self._get_session()
        if session and session.running:
            html = "\u26a0\ufe0f Bot is already running. Stop it first or use Restart."
            keyboard = [[_btn("\u25c0 Back", "back")]]
            await self._n.edit_message_text(msg_id, html, keyboard)
            return

        html = "\u25b6\ufe0f <b>Start — select mode:</b>"
        keyboard = [
            [_btn("\U0001f4dd Paper", "start:paper"), _btn("\U0001f4b5 Live", "start:live")],
            [_btn("\u25c0 Back", "back")],
        ]
        await self._n.edit_message_text(msg_id, html, keyboard)

    async def _cb_start(self, cb_id: str, msg_id: int, mode: str) -> None:
        await self._n.answer_callback_query(cb_id)
        params = self._load_settings()
        params["mode"] = mode
        if mode == "live":
            params["auto_approve_live"] = True

        try:
            self._log(f"Start requested via Telegram: {mode.upper()} {params.get('coin','BTC')} {params.get('timeframe','15m')} strategy={params.get('preset','medium')} size=${params.get('size_usd',5.0):.2f}")
            await self._start_session(params)
            self._save_settings(params)
            preset = params.get("preset", "medium")
            coin = params.get("coin", "BTC")
            tf = params.get("timeframe", "15m")
            size = params.get("size_usd", 5.0)
            self._notify("success", "Started", f"{mode.upper()} {coin} {tf} | {preset} | ${size:.2f}")

            # Fetch balance in background thread
            loop = asyncio.get_running_loop()
            pm_balance = await loop.run_in_executor(None, _fetch_polymarket_balance)
            bal_line = f"\n\U0001f4b0 Balance: <code>${pm_balance:.2f}</code>" if pm_balance is not None else ""

            html = (
                f"\u2705 <b>Session started — {mode.upper()}</b>\n"
                f"<b>{coin} {tf}</b> | Strategy: <code>{preset}</code> | Size: <code>${size:.2f}</code>"
                f"{bal_line}"
            )
        except Exception as e:
            self._log(f"Start failed: {e}")
            self._notify("error", "Start failed", str(e))
            html = f"\u274c <b>Start failed:</b> {e}"

        keyboard = [[_btn("\u25c0 Back", "back")]]
        await self._n.edit_message_text(msg_id, html, keyboard)

    # ---- Restart menu (mode selection) ----

    async def _cb_restart_menu(self, cb_id: str, msg_id: int) -> None:
        await self._n.answer_callback_query(cb_id)
        html = "\U0001f504 <b>Restart — select mode:</b>"
        keyboard = [
            [_btn("\U0001f4dd Paper", "restart:paper"), _btn("\U0001f4b5 Live", "restart:live")],
            [_btn("\u25c0 Back", "back")],
        ]
        await self._n.edit_message_text(msg_id, html, keyboard)

    async def _cb_restart(self, cb_id: str, msg_id: int, mode: str) -> None:
        await self._n.answer_callback_query(cb_id)
        # Stop first if running
        session = self._get_session()
        if session and session.running:
            self._log("Restart: stopping current session via Telegram")
            await self._stop_session()

        params = self._load_settings()
        params["mode"] = mode
        if mode == "live":
            params["auto_approve_live"] = True

        try:
            self._log(f"Restart requested via Telegram: {mode.upper()} {params.get('coin','BTC')} {params.get('timeframe','15m')} strategy={params.get('preset','medium')} size=${params.get('size_usd',5.0):.2f}")
            await self._start_session(params)
            self._save_settings(params)
            preset = params.get("preset", "medium")
            coin = params.get("coin", "BTC")
            tf = params.get("timeframe", "15m")
            size = params.get("size_usd", 5.0)
            self._notify("success", "Restarted", f"{mode.upper()} {coin} {tf} | {preset} | ${size:.2f}")

            # Fetch balance in background thread
            loop = asyncio.get_running_loop()
            pm_balance = await loop.run_in_executor(None, _fetch_polymarket_balance)
            bal_line = f"\n\U0001f4b0 Balance: <code>${pm_balance:.2f}</code>" if pm_balance is not None else ""

            html = (
                f"\u2705 <b>Restarted — {mode.upper()}</b>\n"
                f"<b>{coin} {tf}</b> | Strategy: <code>{preset}</code> | Size: <code>${size:.2f}</code>"
                f"{bal_line}"
            )
        except Exception as e:
            self._log(f"Restart failed: {e}")
            self._notify("error", "Restart failed", str(e))
            html = f"\u274c <b>Restart failed:</b> {e}"

        keyboard = [[_btn("\u25c0 Back", "back")]]
        await self._n.edit_message_text(msg_id, html, keyboard)

    # ---- Strategy menu ----

    async def _cb_strategy_menu(self, cb_id: str, msg_id: int) -> None:
        await self._n.answer_callback_query(cb_id)
        current = self._get_current_preset()
        html = f"\U0001f3af <b>Стратегия</b>\nТекущая: <code>{self._preset_short_label(current)}</code>"
        keyboard = []
        for key, label in PRESET_LABELS.items():
            marker = " \u2705" if key == current else ""
            keyboard.append([_btn(f"{label}{marker}", f"strategy:{key}")])
        keyboard.append([_btn("\u25c0 Назад", "back_settings")])
        await self._n.edit_message_text(msg_id, html, keyboard)

    async def _cb_strategy(self, cb_id: str, msg_id: int, preset: str) -> None:
        await self._n.answer_callback_query(cb_id)
        if preset not in self._presets:
            html = f"\u274c Неизвестная стратегия: {preset}"
            keyboard = [[_btn("\u25c0 Назад", "back_settings")]]
            await self._n.edit_message_text(msg_id, html, keyboard)
            return

        preset_values = self._presets[preset]

        # Apply to running session
        session = self._get_session()
        if session and session.running and session.engine:
            cfg = session.engine.cfg
            cfg.min_abs_bias = preset_values.get("min_bias", cfg.min_abs_bias)
            cfg.min_abs_obi = preset_values.get("min_obi", cfg.min_abs_obi)
            cfg.min_price = preset_values.get("min_price", cfg.min_price)
            cfg.max_price = preset_values.get("max_price", cfg.max_price)
            cfg.cooldown_sec = int(preset_values.get("cooldown_sec", cfg.cooldown_sec))
            cfg.max_trades_per_day = int(preset_values.get("max_trades_per_day", cfg.max_trades_per_day))
            cfg.eval_interval_sec = int(preset_values.get("eval_interval_sec", cfg.eval_interval_sec))
            cfg.tp_pct = preset_values.get("tp_pct", cfg.tp_pct)
            cfg.sl_pct = preset_values.get("sl_pct", cfg.sl_pct)
            cfg.max_hold_sec = int(preset_values.get("max_hold_sec", cfg.max_hold_sec))
            cfg.reverse_exit_bias = preset_values.get("reverse_exit_bias", cfg.reverse_exit_bias)

        self._log(f"Strategy changed to {preset.upper()} via Telegram")
        self._notify("info", "Strategy changed", f"Set to {preset.upper()}")

        # Save
        params = self._load_settings()
        params["preset"] = preset
        self._save_settings(params)

        await self._show_settings_menu(
            msg_id,
            f"\u2705 Стратегия обновлена: <code>{self._preset_short_label(preset)}</code>",
        )

    # ---- Amount menu ----

    async def _cb_amount_menu(self, cb_id: str, msg_id: int) -> None:
        await self._n.answer_callback_query(cb_id)
        current = self._get_current_amount()
        html = f"\U0001f4b0 <b>Размер позиции</b>\nТекущий: <code>${current:.2f}</code>"
        row1 = [_btn(f"${v:.0f}", f"amount:{v}") for v in AMOUNT_OPTIONS[:3]]
        row2 = [_btn(f"${v:.0f}", f"amount:{v}") for v in AMOUNT_OPTIONS[3:]]
        row2.append(_btn("\u270d Свой", "amount:custom"))
        keyboard = [row1, row2, [_btn("\u25c0 Назад", "back_settings")]]
        await self._n.edit_message_text(msg_id, html, keyboard)

    async def _cb_amount(self, cb_id: str, msg_id: int, amount: float) -> None:
        await self._n.answer_callback_query(cb_id)
        amount = max(5.0, amount)
        self._apply_amount(amount)
        self._log(f"Amount changed to ${amount:.2f} via Telegram")
        self._notify("info", "Amount changed", f"Set to ${amount:.2f}")
        await self._show_settings_menu(msg_id, f"\u2705 Размер обновлён: <code>${amount:.2f}</code>")

    async def _cb_amount_custom(self, cb_id: str, msg_id: int, cb: dict) -> None:
        user_id = cb.get("from", {}).get("id")
        await self._n.answer_callback_query(cb_id)
        self._waiting_custom_amount_from = user_id
        html = "\u270d <b>Введите размер</b>\nОтправьте число в USD, например <code>15</code>"
        keyboard = [[_btn("\u25c0 Отмена", "back_settings")]]
        await self._n.edit_message_text(msg_id, html, keyboard)

    async def _handle_custom_amount_text(self, msg: dict) -> None:
        self._waiting_custom_amount_from = None
        text = (msg.get("text") or "").strip().replace("$", "").replace(",", ".")
        try:
            amount = float(text)
            if amount < 5:
                raise ValueError("минимум $5")
            self._apply_amount(amount)
            self._log(f"Amount changed to ${amount:.2f} via Telegram (custom)")
            self._notify("info", "Amount changed", f"Set to ${amount:.2f}")
            html = self._settings_menu_text(f"\u2705 Размер обновлён: <code>${amount:.2f}</code>")
        except (ValueError, TypeError) as e:
            html = self._settings_menu_text(f"\u274c Некорректный размер: {e}")
        await self._n.send_with_keyboard(html, self._settings_keyboard())

    # ---- Coin menu ----

    async def _cb_coin_menu(self, cb_id: str, msg_id: int) -> None:
        await self._n.answer_callback_query(cb_id)
        params = self._load_settings()
        current_coin = params.get("coin", "BTC")
        html = f"\U0001fa99 <b>Монета</b>\nТекущая: <code>{current_coin}</code>"
        keyboard = []
        coins = list(self._coin_timeframes.keys())
        # Show 3 per row: BTC, ETH, SOL (skip XRP for cleaner layout, but include all)
        row = []
        for c in coins:
            marker = " \u2705" if c == current_coin else ""
            row.append(_btn(f"{c}{marker}", f"coin:{c}"))
            if len(row) == 3:
                keyboard.append(row)
                row = []
        if row:
            keyboard.append(row)
        keyboard.append([_btn("\u25c0 Назад", "back_settings")])
        await self._n.edit_message_text(msg_id, html, keyboard)

    async def _cb_coin(self, cb_id: str, msg_id: int, coin: str) -> None:
        await self._n.answer_callback_query(cb_id)
        if coin not in self._coin_timeframes:
            html = f"\u274c Неизвестная монета: {coin}"
            keyboard = [[_btn("\u25c0 Назад", "back_settings")]]
            await self._n.edit_message_text(msg_id, html, keyboard)
            return

        params = self._load_settings()
        old_coin = params.get("coin", "BTC")
        old_tf = params.get("timeframe", "15m")
        params["coin"] = coin

        # If current timeframe is not available for new coin, reset to first available
        available_tfs = self._coin_timeframes[coin]
        if params.get("timeframe", "15m") not in available_tfs:
            params["timeframe"] = available_tfs[0]
        new_tf = params.get("timeframe", "15m")
        tf_adjusted = new_tf != old_tf

        self._save_settings(params)
        self._log(f"Coin changed to {coin} via Telegram (was {old_coin})")
        self._notify("info", "Coin changed", f"Set to {coin} (timeframe: {params['timeframe']})")

        notes: list[str] = [f"\u2705 Монета: <code>{coin}</code>"]
        if tf_adjusted:
            notes.append(f"\u2139\ufe0f Таймфрейм автоматически изменён на <code>{new_tf}</code>")

        session = self._get_session()
        was_running = bool(session and session.running)
        restarted, restart_info = await self._restart_running_session(params)
        if was_running:
            if restarted:
                notes.append(f"\U0001f504 Сессия перезапущена в режиме <code>{restart_info.upper()}</code>")
            else:
                notes.append(f"\u274c Ошибка автоперезапуска: <code>{restart_info}</code>")

        await self._show_settings_menu(msg_id, "\n".join(notes))

    # ---- Timeframe menu ----

    async def _cb_tf_menu(self, cb_id: str, msg_id: int) -> None:
        await self._n.answer_callback_query(cb_id)
        params = self._load_settings()
        current_coin = params.get("coin", "BTC")
        current_tf = params.get("timeframe", "15m")
        available_tfs = self._coin_timeframes.get(current_coin, ["15m"])

        html = f"\u23f1 <b>Таймфрейм</b>\nМонета: <code>{current_coin}</code> | Текущий: <code>{current_tf}</code>"
        keyboard = []
        row = []
        for tf in available_tfs:
            marker = " \u2705" if tf == current_tf else ""
            row.append(_btn(f"{tf}{marker}", f"tf:{tf}"))
            if len(row) == 3:
                keyboard.append(row)
                row = []
        if row:
            keyboard.append(row)
        keyboard.append([_btn("\u25c0 Назад", "back_settings")])
        await self._n.edit_message_text(msg_id, html, keyboard)

    async def _cb_tf(self, cb_id: str, msg_id: int, tf: str) -> None:
        await self._n.answer_callback_query(cb_id)
        params = self._load_settings()
        current_coin = params.get("coin", "BTC")
        available_tfs = self._coin_timeframes.get(current_coin, [])

        if tf not in available_tfs:
            html = f"\u274c Таймфрейм <code>{tf}</code> недоступен для {current_coin}.\nДоступно: {', '.join(available_tfs)}"
            keyboard = [[_btn("\u25c0 Назад", "back_settings")]]
            await self._n.edit_message_text(msg_id, html, keyboard)
            return

        old_tf = params.get("timeframe", "15m")
        params["timeframe"] = tf
        self._save_settings(params)
        self._log(f"Timeframe changed to {tf} via Telegram (was {old_tf})")
        self._notify("info", "Timeframe changed", f"Set to {tf}")

        notes: list[str] = [f"\u2705 Таймфрейм: <code>{tf}</code>"]
        session = self._get_session()
        was_running = bool(session and session.running)
        restarted, restart_info = await self._restart_running_session(params)
        if was_running:
            if restarted:
                notes.append(f"\U0001f504 Сессия перезапущена в режиме <code>{restart_info.upper()}</code>")
            else:
                notes.append(f"\u274c Ошибка автоперезапуска: <code>{restart_info}</code>")

        await self._show_settings_menu(msg_id, "\n".join(notes))

    # ---- Back (main menu) ----

    async def _cb_back(self, cb_id: str, msg_id: int) -> None:
        await self._n.answer_callback_query(cb_id)
        self._waiting_custom_amount_from = None
        await self._n.edit_message_text(msg_id, self._main_menu_text(), self._main_keyboard())

    # ---- helpers ----

    def _feed_gate_block_reason(self, session: Any) -> str:
        engine = getattr(session, "engine", None)
        st = getattr(session, "feed_state", None)
        if not engine or st is None:
            return ""

        now = time.time()
        if not st.binance_ws_connected:
            return "waiting Binance WS connection"
        if not st.binance_ob_ready or st.mid <= 0:
            return "waiting Binance orderbook"
        if st.binance_ob_last_ok_ts <= 0:
            return "waiting Binance orderbook"
        if (now - st.binance_ob_last_ok_ts) > engine.cfg.binance_ob_stale_sec:
            return "waiting Binance orderbook (stale)"
        if not st.klines:
            return "waiting Binance candles"
        if not st.pm_connected:
            return "waiting Polymarket WS connection"
        if not st.pm_prices_ready:
            return "waiting Polymarket prices"
        return ""

    def _position_display_price(self, session: Any, side: str, fallback: float = 0.0) -> float:
        st = getattr(session, "feed_state", None)
        if st is None:
            return fallback

        side_u = str(side or "").strip().upper()
        if side_u in {"UP", "YES", "LONG"} and st.pm_up is not None:
            return float(st.pm_up)
        if side_u in {"DOWN", "NO", "SHORT"} and st.pm_dn is not None:
            return float(st.pm_dn)
        return float(fallback or 0.0)

    def _apply_amount(self, amount: float) -> None:
        session = self._get_session()
        if session and session.running and session.engine:
            session.engine.cfg.size_usd = amount

        params = self._load_settings()
        params["size_usd"] = amount
        self._save_settings(params)

    def _get_current_amount(self) -> float:
        session = self._get_session()
        if session and session.running and session.engine:
            return session.engine.cfg.size_usd
        params = self._load_settings()
        return params.get("size_usd", 5.0)

    def _get_current_preset(self) -> str:
        session = self._get_session()
        if session and session.running and session.engine:
            return self._detect_preset(session)
        params = self._load_settings()
        return params.get("preset", "medium")

    def _detect_preset(self, session: Any) -> str:
        """Compare current cfg values against known presets."""
        if not session.engine:
            return "unknown"
        cfg = session.engine.cfg
        check_keys = [
            ("min_bias", "min_abs_bias"),
            ("min_obi", "min_abs_obi"),
            ("cooldown_sec", "cooldown_sec"),
            ("max_trades_per_day", "max_trades_per_day"),
            ("tp_pct", "tp_pct"),
            ("sl_pct", "sl_pct"),
        ]
        for name, preset_vals in self._presets.items():
            if name not in PRESET_LABELS:
                continue  # skip super/mega aggressive
            match = True
            for preset_key, cfg_attr in check_keys:
                if preset_key in preset_vals:
                    if abs(float(getattr(cfg, cfg_attr, 0)) - float(preset_vals[preset_key])) > 0.01:
                        match = False
                        break
            if match:
                return name
        return "custom"

    def _load_settings(self) -> dict:
        try:
            if self._settings_path.exists():
                return json.loads(self._settings_path.read_text())
        except Exception:
            pass
        # Fallback: PM_DEFAULT_SETTINGS env var (survives container re-deploys)
        env_raw = os.environ.get("PM_DEFAULT_SETTINGS", "").strip()
        if env_raw:
            try:
                settings = json.loads(env_raw)
                self._save_settings(settings)
                return settings
            except Exception:
                pass
        return {
            "mode": "paper",
            "coin": "BTC",
            "timeframe": "15m",
            "preset": "medium",
            "size_usd": 5.0,
        }

    def _save_settings(self, params: dict) -> None:
        try:
            self._settings_path.parent.mkdir(parents=True, exist_ok=True)
            self._settings_path.write_text(json.dumps(params, indent=2))
        except Exception as e:
            print(f"  [TELEGRAM BOT] save settings error: {e}")

    def save_settings_from_web(self, settings: dict[str, Any] | None = None, **kwargs: Any) -> None:
        """Called from web_server to sync full web UI params into Telegram settings storage."""
        params = self._load_settings()
        if isinstance(settings, dict):
            params.update(settings)
        if kwargs:
            params.update(kwargs)
        self._save_settings(params)
