"""Interactive Telegram bot for controlling trading sessions via inline keyboards.

Usage: instantiated and started from web_server.py when TELEGRAM_BOT_USERNAME is set.
Polling runs inside the same event loop as FastAPI.
"""

from __future__ import annotations

import asyncio
import json
import os
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
        # Check for thread_id match if configured
        if self._n.thread_id:
            if msg.get("message_thread_id") != self._n.thread_id:
                return False
        else:
            chat_id = str(msg.get("chat", {}).get("id", ""))
            if chat_id != str(self._n.chat_id):
                return False

        # Check entities for @mention
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

    def _main_keyboard(self) -> list[list[dict]]:
        return [
            [_btn("\U0001f4ca Status", "status"), _btn("\U0001f6d1 Stop", "stop")],
            [_btn("\u25b6\ufe0f Start", "start_menu"), _btn("\U0001f504 Restart", "restart_menu")],
            [_btn("\U0001f3af Strategy", "strategy_menu"), _btn("\U0001f4b0 Amount", "amount_menu")],
            [_btn("\U0001fa99 Coin", "coin_menu"), _btn("\u23f1 Timeframe", "tf_menu")],
        ]

    def _status_line(self) -> str:
        session = self._get_session()
        if session and session.running:
            mode = session.mode.value.upper()
            return f"\U0001f7e2 <b>Bot is RUNNING</b> — {mode}"
        return "\U0001f534 <b>Bot is STOPPED</b>"

    async def _send_main_menu(self, msg: dict) -> None:
        html = f"{self._status_line()}\nSelect an action:"
        await self._n.send_with_keyboard(html, self._main_keyboard())

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
            preset = params.get("preset", "medium")
            size = params.get("size_usd", 5.0)
            coin = params.get("coin", "BTC")
            tf = params.get("timeframe", "15m")
            bal_line = f"\n\U0001f4b0 Polymarket balance: <code>${pm_balance:.2f}</code>" if pm_balance is not None else ""

            # Window info for saved timeframe
            import window as window_mod
            winfo = window_mod.get_window_info(tf)
            w_m, w_s = divmod(winfo.remaining_sec, 60)
            win_line = f"\n\u23f1 Window {tf}: <code>{w_m}m {w_s:02d}s</code> left"

            html = (
                "\U0001f534 <b>Trading Bot — STOPPED</b>\n"
                f"Coin: <code>{coin} {tf}</code>\n"
                f"Last strategy: <code>{preset}</code> | Size: <code>${size:.2f}</code>\n"
                f"No active session.{bal_line}{win_line}"
            )
        else:
            mode = session.mode.value.upper()
            coin = session.coin
            tf = session.timeframe
            preset = self._detect_preset(session)
            cfg = session.engine.cfg if session.engine else None
            size = cfg.size_usd if cfg else 0

            snap = session.engine.snapshot() if session.engine else {}
            trades_today = snap.get("trades_today", 0)
            max_trades = snap.get("max_trades_per_day", 0)

            # Market data from feed state
            st = session.feed_state
            price = st.mid if st.mid else 0

            from indicators import bias_score
            bias = bias_score(st.bids, st.asks, st.mid, st.trades, st.klines) if st.mid and st.klines else 0.0

            html = (
                f"\U0001f7e2 <b>Trading Bot — RUNNING</b>\n"
                f"<code>{mode}</code> | <b>{coin} {tf}</b> | Strategy: <code>{preset}</code>\n"
                f"Size: <code>${size:.2f}</code> | Trades: <code>{trades_today}/{max_trades}</code>\n"
            )
            if price:
                html += f"Price: <code>{price:.3f}</code> | Bias: <code>{bias:+.1f}</code>\n"

            # Polymarket USDC balance
            if pm_balance is not None:
                html += f"\U0001f4b0 Polymarket balance: <code>${pm_balance:.2f}</code>\n"

            # Window info
            import window as window_mod
            winfo = window_mod.get_window_info(tf)
            w_m, w_s = divmod(winfo.remaining_sec, 60)
            entry_status = "BLOCKED \u26d4" if winfo.entry_blocked else "OK \u2705"
            html += f"\u23f1 Window: <code>{w_m}m {w_s:02d}s</code> left | Entry: {entry_status}\n"

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
                    f"\n\U0001f4b5 <b>Session PnL:</b> {pnl_emoji} <code>${total_pnl_usd:+.2f}</code>\n"
                    f"W/L: <code>{wins}/{losses}</code> ({winrate:.0f}% win rate)\n"
                )

            # Open position
            pos = snap.get("open_position")
            if pos:
                entry_price = pos.get("entry_price", 0)
                side = pos.get("side", "?")
                if entry_price and price:
                    if side == "YES":
                        pnl_pct = ((price - entry_price) / entry_price) * 100
                    else:
                        pnl_pct = ((entry_price - price) / entry_price) * 100
                    pnl_emoji = "\U0001f7e2" if pnl_pct >= 0 else "\U0001f534"
                    html += (
                        f"\n<b>Open Position:</b> {side} @ <code>{entry_price:.3f}</code>\n"
                        f"Current: <code>{price:.3f}</code> | PnL: {pnl_emoji} <code>{pnl_pct:+.1f}%</code>"
                    )

        keyboard = [[_btn("\u25c0 Back", "back")]]
        await self._n.edit_message_text(msg_id, html, keyboard)

    # ---- Stop ----

    async def _cb_stop(self, cb_id: str, msg_id: int) -> None:
        await self._n.answer_callback_query(cb_id)
        session = self._get_session()
        if not session or not session.running:
            html = "\u26a0\ufe0f Bot is not running."
            keyboard = [[_btn("\u25c0 Back", "back")]]
            await self._n.edit_message_text(msg_id, html, keyboard)
            return

        self._log("Stop requested via Telegram")
        self._notify("warning", "Stop", "Session stop requested via Telegram")
        await self._stop_session()
        self._log("Session stopped via Telegram")
        html = "\u2705 <b>Session stopped.</b>"
        keyboard = [[_btn("\u25c0 Back", "back")]]
        await self._n.edit_message_text(msg_id, html, keyboard)

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
        html = f"\U0001f3af <b>Change Strategy</b>\nCurrent: <code>{current}</code>"
        keyboard = []
        for key, label in PRESET_LABELS.items():
            marker = " \u2705" if key == current else ""
            keyboard.append([_btn(f"{label}{marker}", f"strategy:{key}")])
        keyboard.append([_btn("\u25c0 Back", "back")])
        await self._n.edit_message_text(msg_id, html, keyboard)

    async def _cb_strategy(self, cb_id: str, msg_id: int, preset: str) -> None:
        await self._n.answer_callback_query(cb_id)
        if preset not in self._presets:
            html = f"\u274c Unknown preset: {preset}"
            keyboard = [[_btn("\u25c0 Back", "back")]]
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

        label = PRESET_LABELS.get(preset, preset)
        html = f"\u2705 <b>Strategy set to {preset.upper()}</b>\n{label}"
        keyboard = [[_btn("\u25c0 Back", "back")]]
        await self._n.edit_message_text(msg_id, html, keyboard)

    # ---- Amount menu ----

    async def _cb_amount_menu(self, cb_id: str, msg_id: int) -> None:
        await self._n.answer_callback_query(cb_id)
        current = self._get_current_amount()
        html = f"\U0001f4b0 <b>Change Amount</b>\nCurrent: <code>${current:.2f}</code>"
        row1 = [_btn(f"${v:.0f}", f"amount:{v}") for v in AMOUNT_OPTIONS[:3]]
        row2 = [_btn(f"${v:.0f}", f"amount:{v}") for v in AMOUNT_OPTIONS[3:]]
        row2.append(_btn("\u270d Custom", "amount:custom"))
        keyboard = [row1, row2, [_btn("\u25c0 Back", "back")]]
        await self._n.edit_message_text(msg_id, html, keyboard)

    async def _cb_amount(self, cb_id: str, msg_id: int, amount: float) -> None:
        await self._n.answer_callback_query(cb_id)
        amount = max(5.0, amount)
        self._apply_amount(amount)
        self._log(f"Amount changed to ${amount:.2f} via Telegram")
        self._notify("info", "Amount changed", f"Set to ${amount:.2f}")
        html = f"\u2705 <b>Input amount set to ${amount:.2f}</b>"
        keyboard = [[_btn("\u25c0 Back", "back")]]
        await self._n.edit_message_text(msg_id, html, keyboard)

    async def _cb_amount_custom(self, cb_id: str, msg_id: int, cb: dict) -> None:
        user_id = cb.get("from", {}).get("id")
        await self._n.answer_callback_query(cb_id)
        self._waiting_custom_amount_from = user_id
        html = "\u270d <b>Enter custom amount</b>\nSend a number (USD), e.g. <code>15</code>"
        keyboard = [[_btn("\u25c0 Cancel", "back")]]
        await self._n.edit_message_text(msg_id, html, keyboard)

    async def _handle_custom_amount_text(self, msg: dict) -> None:
        self._waiting_custom_amount_from = None
        text = (msg.get("text") or "").strip().replace("$", "").replace(",", ".")
        try:
            amount = float(text)
            if amount < 5:
                raise ValueError("minimum $5")
            self._apply_amount(amount)
            self._log(f"Amount changed to ${amount:.2f} via Telegram (custom)")
            self._notify("info", "Amount changed", f"Set to ${amount:.2f}")
            html = f"\u2705 <b>Input amount set to ${amount:.2f}</b>"
        except (ValueError, TypeError) as e:
            html = f"\u274c Invalid amount: {e}\nPlease use the menu to try again."
        await self._n.send_with_keyboard(html, [[_btn("\u25c0 Menu", "back")]])

    # ---- Coin menu ----

    async def _cb_coin_menu(self, cb_id: str, msg_id: int) -> None:
        await self._n.answer_callback_query(cb_id)
        params = self._load_settings()
        current_coin = params.get("coin", "BTC")
        html = f"\U0001fa99 <b>Change Coin</b>\nCurrent: <code>{current_coin}</code>"
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
        keyboard.append([_btn("\u25c0 Back", "back")])
        await self._n.edit_message_text(msg_id, html, keyboard)

    async def _cb_coin(self, cb_id: str, msg_id: int, coin: str) -> None:
        await self._n.answer_callback_query(cb_id)
        if coin not in self._coin_timeframes:
            html = f"\u274c Unknown coin: {coin}"
            keyboard = [[_btn("\u25c0 Back", "back")]]
            await self._n.edit_message_text(msg_id, html, keyboard)
            return

        params = self._load_settings()
        old_coin = params.get("coin", "BTC")
        params["coin"] = coin

        # If current timeframe is not available for new coin, reset to first available
        available_tfs = self._coin_timeframes[coin]
        if params.get("timeframe", "15m") not in available_tfs:
            params["timeframe"] = available_tfs[0]

        self._save_settings(params)
        self._log(f"Coin changed to {coin} via Telegram (was {old_coin})")
        self._notify("info", "Coin changed", f"Set to {coin} (timeframe: {params['timeframe']})")

        note = ""
        if params.get("timeframe") != self._load_settings().get("timeframe"):
            note = f"\nTimeframe adjusted to <code>{params['timeframe']}</code>"

        session = self._get_session()
        restart_hint = ""
        if session and session.running:
            restart_hint = "\n\n\u26a0\ufe0f <i>Restart the session to apply coin change.</i>"

        html = f"\u2705 <b>Coin set to {coin}</b>{note}{restart_hint}"
        keyboard = [[_btn("\u25c0 Back", "back")]]
        await self._n.edit_message_text(msg_id, html, keyboard)

    # ---- Timeframe menu ----

    async def _cb_tf_menu(self, cb_id: str, msg_id: int) -> None:
        await self._n.answer_callback_query(cb_id)
        params = self._load_settings()
        current_coin = params.get("coin", "BTC")
        current_tf = params.get("timeframe", "15m")
        available_tfs = self._coin_timeframes.get(current_coin, ["15m"])

        html = f"\u23f1 <b>Change Timeframe</b>\nCoin: <code>{current_coin}</code> | Current: <code>{current_tf}</code>"
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
        keyboard.append([_btn("\u25c0 Back", "back")])
        await self._n.edit_message_text(msg_id, html, keyboard)

    async def _cb_tf(self, cb_id: str, msg_id: int, tf: str) -> None:
        await self._n.answer_callback_query(cb_id)
        params = self._load_settings()
        current_coin = params.get("coin", "BTC")
        available_tfs = self._coin_timeframes.get(current_coin, [])

        if tf not in available_tfs:
            html = f"\u274c Timeframe <code>{tf}</code> not available for {current_coin}.\nAvailable: {', '.join(available_tfs)}"
            keyboard = [[_btn("\u25c0 Back", "back")]]
            await self._n.edit_message_text(msg_id, html, keyboard)
            return

        old_tf = params.get("timeframe", "15m")
        params["timeframe"] = tf
        self._save_settings(params)
        self._log(f"Timeframe changed to {tf} via Telegram (was {old_tf})")
        self._notify("info", "Timeframe changed", f"Set to {tf}")

        session = self._get_session()
        restart_hint = ""
        if session and session.running:
            restart_hint = "\n\n\u26a0\ufe0f <i>Restart the session to apply timeframe change.</i>"

        html = f"\u2705 <b>Timeframe set to {tf}</b>{restart_hint}"
        keyboard = [[_btn("\u25c0 Back", "back")]]
        await self._n.edit_message_text(msg_id, html, keyboard)

    # ---- Back (main menu) ----

    async def _cb_back(self, cb_id: str, msg_id: int) -> None:
        await self._n.answer_callback_query(cb_id)
        self._waiting_custom_amount_from = None
        html = f"{self._status_line()}\nSelect an action:"
        await self._n.edit_message_text(msg_id, html, self._main_keyboard())

    # ---- helpers ----

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
