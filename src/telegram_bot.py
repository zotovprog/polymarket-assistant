"""Interactive Telegram Bot for MM control.

Uses edit-in-place pattern: one live message per chat, updated via
editMessageText on every interaction. No message spam.

Commands: /start, /kill
Buttons: Refresh, Kill All, Settings, Start Bot
"""
from __future__ import annotations

import asyncio
import logging
import time
from typing import Any, Callable

from mm_shared.runtime_metrics import runtime_metrics
from telegram_notifier import TelegramAPIError

log = logging.getLogger("tg.bot")


class TelegramBotManager:
    """Interactive Telegram bot with single-message UI."""

    _MIN_POLL_INTERVAL_SEC = 0.25

    def __init__(
        self,
        notifier,
        get_runtime,
        access_key: str = "",
        on_conflict: Callable[[str], None] | None = None,
    ):
        self._tg = notifier
        self._get_runtime = get_runtime
        self._access_key = access_key
        self._on_conflict = on_conflict
        self._running = False
        self._task: asyncio.Task | None = None
        self._offset: int | None = None
        self._disabled_reason: str = ""
        self._disabled_at: float = 0.0
        self._min_poll_interval_sec = self._MIN_POLL_INTERVAL_SEC
        # Track the live message per chat for edit-in-place
        self._live_msg: dict[int, int] = {}  # chat_id -> message_id
        # Current view per chat: "status" | "settings"
        self._view: dict[int, str] = {}

        self._settings: dict[str, Any] = {
            "coin": "BTC",
            "timeframe": "15m",
            "mode": "paper",
            "limit": 25.0,
            "dev": True,
        }

    @property
    def status(self) -> dict[str, Any]:
        return {
            "running": self._running,
            "disabled_reason": self._disabled_reason,
            "disabled_at": self._disabled_at,
        }

    async def start(self) -> None:
        if self._running:
            return
        self._running = True
        self._task = asyncio.create_task(self._poll_loop())
        log.info("TelegramBotManager started")

    async def stop(self) -> None:
        self._running = False
        if self._task:
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass
        log.info("TelegramBotManager stopped")

    # ── Poll loop ───────────────────────────────────────────────

    @staticmethod
    def _is_conflict_error(error: Exception) -> bool:
        status_code = getattr(error, "status_code", None)
        error_code = getattr(error, "error_code", None)
        if status_code == 409 or error_code == 409:
            return True
        err_str = str(error)
        return "409" in err_str or "Conflict" in err_str

    async def _yield_after_poll(self, poll_started: float) -> None:
        elapsed = time.monotonic() - poll_started
        runtime_metrics.observe_ms("tg.poll.cycle_ms", elapsed * 1000.0)
        remaining = self._min_poll_interval_sec - elapsed
        if remaining > 0:
            runtime_metrics.incr("tg.poll.min_yield")
            await asyncio.sleep(remaining)

    async def _poll_loop(self) -> None:
        while self._running:
            runtime_metrics.incr("tg.poll.loop")
            poll_started = time.monotonic()
            applied_backoff = False
            try:
                updates = await self._tg.get_updates(
                    offset=self._offset, timeout=30,
                )
                runtime_metrics.incr("tg.poll.success")
                if updates:
                    runtime_metrics.incr("tg.poll.updates", n=len(updates))
                for upd in updates:
                    self._offset = upd["update_id"] + 1
                    try:
                        await self._handle(upd)
                    except Exception as e:
                        runtime_metrics.incr("tg.poll.update_error")
                        log.warning("Update error: %s", e)
            except asyncio.CancelledError:
                break
            except Exception as e:
                runtime_metrics.incr("tg.poll.error")
                if self._is_conflict_error(e):
                    runtime_metrics.incr("tg.poll.conflict_409")
                    self._disabled_reason = (
                        "Telegram polling disabled: 409 Conflict "
                        "(another bot instance is using getUpdates)"
                    )
                    self._disabled_at = time.time()
                    self._running = False
                    # Disable outgoing notifications too; avoid further API pressure.
                    self._tg.enabled = False
                    log.error(self._disabled_reason)
                    if self._on_conflict:
                        try:
                            self._on_conflict(self._disabled_reason)
                        except Exception as cb_err:
                            log.warning("Telegram conflict callback error: %s", cb_err)
                    break
                else:
                    log.warning("Poll error: %s", e)
                    applied_backoff = True
                    await asyncio.sleep(5.0)
            finally:
                if self._running and not applied_backoff:
                    await self._yield_after_poll(poll_started)

    async def _handle(self, upd: dict) -> None:
        if "callback_query" in upd:
            await self._on_callback(upd["callback_query"])
        elif "message" in upd:
            msg = upd["message"]
            text = (msg.get("text") or "").strip()
            chat_id = msg.get("chat", {}).get("id")
            if text.startswith("/"):
                await self._on_command(text, chat_id)

    # ── Commands ────────────────────────────────────────────────

    async def _on_command(self, text: str, chat_id: int | None) -> None:
        cmd = text.split()[0].lower().split("@")[0]
        if cmd in ("/start", "/menu", "/status"):
            await self._show_status(chat_id)
        elif cmd == "/kill":
            await self._do_kill(chat_id)

    # ── Callbacks ───────────────────────────────────────────────

    async def _on_callback(self, cq: dict) -> None:
        data = cq.get("data", "")
        cq_id = cq["id"]
        msg = cq.get("message", {})
        chat_id = msg.get("chat", {}).get("id")
        msg_id = msg.get("message_id")

        # Store this message as the live message for edits
        if chat_id and msg_id:
            self._live_msg[chat_id] = msg_id

        if data == "refresh":
            await self._tg.answer_callback_query(cq_id)
            await self._edit_status(chat_id, msg_id)

        elif data == "settings":
            await self._tg.answer_callback_query(cq_id)
            self._view[chat_id] = "settings"
            await self._edit_settings(chat_id, msg_id)

        elif data == "back":
            await self._tg.answer_callback_query(cq_id)
            self._view[chat_id] = "status"
            await self._edit_status(chat_id, msg_id)

        elif data == "kill_all":
            await self._tg.answer_callback_query(cq_id, text="Stopping...")
            await self._do_kill(chat_id, msg_id)

        elif data == "start_bot":
            await self._tg.answer_callback_query(cq_id, text="Starting...")
            await self._do_start(chat_id, msg_id)

        # Settings mutations
        elif data.startswith("s:"):
            await self._handle_setting(data[2:], cq_id, chat_id, msg_id)

        else:
            await self._tg.answer_callback_query(cq_id)

    async def _handle_setting(self, param: str, cq_id: str,
                              chat_id: int, msg_id: int) -> None:
        key, val = param.split("=", 1)
        if key == "coin":
            self._settings["coin"] = val
        elif key == "tf":
            self._settings["timeframe"] = val
        elif key == "mode":
            self._settings["mode"] = val
        elif key == "limit":
            self._settings["limit"] = float(val)
        elif key == "dev":
            self._settings["dev"] = val == "1"

        await self._tg.answer_callback_query(cq_id)
        await self._edit_settings(chat_id, msg_id)

    # ── Views ───────────────────────────────────────────────────

    def _build_status_html(self) -> str:
        rt = self._get_runtime()
        is_running = rt.is_running if rt else False

        if not is_running:
            return (
                "\U0001f534 <b>MM Bot — Stopped</b>\n"
                "━━━━━━━━━━━━━━━━\n"
                "Press <b>Settings</b> to configure, then <b>Start</b>."
            )

        snap = rt.snapshot()
        inv = snap.get("inventory", {})
        fv = snap.get("fair_value", {})
        mkt = snap.get("market", {})
        pnl = snap.get("total_pnl", 0)
        session_pnl = snap.get("session_pnl", 0)

        mode = "PAPER" if snap.get("paper_mode") else "LIVE"
        dev_tag = " [DEV]" if snap.get("dev_mode") else ""
        tl = mkt.get("time_remaining", 0)
        mins = int(tl // 60)
        secs = int(tl % 60)

        pnl_emoji = "\U0001f4c8" if session_pnl >= 0 else "\U0001f4c9"
        fills = snap.get("fill_count", 0)
        orders = snap.get("active_orders", 0)

        return (
            f"\U0001f7e2 <b>MM Bot — Running</b>{dev_tag}\n"
            f"━━━━━━━━━━━━━━━━\n"
            f"<b>Market:</b> <code>{mkt.get('coin', '?')}/{mkt.get('timeframe', '?')}</code>"
            f"  |  <code>{mode}</code>\n"
            f"<b>Time:</b> <code>{mins}m {secs}s</code>"
            f"  |  <b>Orders:</b> <code>{orders}</code>\n\n"
            f"<b>FV</b>  UP: <code>{fv.get('up', 0.5):.3f}</code>"
            f"  DN: <code>{fv.get('dn', 0.5):.3f}</code>\n"
            f"<b>BTC:</b> <code>${fv.get('binance_mid', 0):,.0f}</code>\n\n"
            f"<b>Inventory</b>\n"
            f"  UP: <code>{inv.get('up_shares', 0):.1f}</code>"
            f"  DN: <code>{inv.get('dn_shares', 0):.1f}</code>"
            f"  \u0394: <code>{inv.get('net_delta', 0):+.1f}</code>\n\n"
            f"{pnl_emoji} <b>Session PnL:</b> <code>${session_pnl:+.2f}</code>\n"
            f"<b>Fills:</b> <code>{fills}</code>"
        )

    def _status_keyboard(self) -> list:
        rt = self._get_runtime()
        is_running = rt.is_running if rt else False

        if is_running:
            return [
                [
                    {"text": "\U0001f504 Refresh", "callback_data": "refresh"},
                    {"text": "\u2699\ufe0f Settings", "callback_data": "settings"},
                ],
                [{"text": "\U0001f6d1 Kill All", "callback_data": "kill_all"}],
            ]
        return [
            [
                {"text": "\u2699\ufe0f Settings", "callback_data": "settings"},
                {"text": "\U0001f680 Start", "callback_data": "start_bot"},
            ],
        ]

    def _build_settings_html(self) -> str:
        s = self._settings
        dev_str = "ON" if s["dev"] else "OFF"
        return (
            "\u2699\ufe0f <b>Settings</b>\n"
            "━━━━━━━━━━━━━━━━\n"
            f"<b>Coin:</b> <code>{s['coin']}</code>  "
            f"<b>TF:</b> <code>{s['timeframe']}</code>  "
            f"<b>Mode:</b> <code>{s['mode'].upper()}</code>\n"
            f"<b>Limit:</b> <code>${s['limit']:.0f}</code>  "
            f"<b>Dev:</b> <code>{dev_str}</code>"
        )

    def _settings_keyboard(self) -> list:
        s = self._settings
        check = "\u2705 "

        def _c(cur, val):
            return check + val if cur == val else val

        dev_text = "Dev: ON \u2705" if s["dev"] else "Dev: OFF"
        dev_data = "s:dev=0" if s["dev"] else "s:dev=1"

        return [
            # Coin
            [
                {"text": _c(s["coin"], "BTC"), "callback_data": "s:coin=BTC"},
                {"text": _c(s["coin"], "ETH"), "callback_data": "s:coin=ETH"},
                {"text": _c(s["coin"], "SOL"), "callback_data": "s:coin=SOL"},
                {"text": _c(s["coin"], "XRP"), "callback_data": "s:coin=XRP"},
            ],
            # Timeframe
            [
                {"text": _c(s["timeframe"], "5m"), "callback_data": "s:tf=5m"},
                {"text": _c(s["timeframe"], "15m"), "callback_data": "s:tf=15m"},
                {"text": _c(s["timeframe"], "1h"), "callback_data": "s:tf=1h"},
                {"text": _c(s["timeframe"], "4h"), "callback_data": "s:tf=4h"},
            ],
            # Mode
            [
                {"text": _c(s["mode"], "paper"), "callback_data": "s:mode=paper"},
                {"text": _c(s["mode"], "live"), "callback_data": "s:mode=live"},
            ],
            # Limit
            [
                {"text": "$25" + (" \u2705" if s["limit"] == 25 else ""), "callback_data": "s:limit=25"},
                {"text": "$50" + (" \u2705" if s["limit"] == 50 else ""), "callback_data": "s:limit=50"},
                {"text": "$100" + (" \u2705" if s["limit"] == 100 else ""), "callback_data": "s:limit=100"},
                {"text": "$250" + (" \u2705" if s["limit"] == 250 else ""), "callback_data": "s:limit=250"},
            ],
            # Dev + actions
            [
                {"text": dev_text, "callback_data": dev_data},
                {"text": "\U0001f680 Start", "callback_data": "start_bot"},
            ],
            [{"text": "\u2b05\ufe0f Back", "callback_data": "back"}],
        ]

    # ── Send / Edit ─────────────────────────────────────────────

    async def _show_status(self, chat_id: int | None) -> None:
        """Send a new status message (replaces old live msg)."""
        html = self._build_status_html()
        kb = self._status_keyboard()
        result = await self._tg.send_with_keyboard(html, kb)
        if result and chat_id:
            self._live_msg[chat_id] = result.get("message_id")
            self._view[chat_id] = "status"

    async def _edit_status(self, chat_id: int, msg_id: int) -> None:
        """Edit existing message with fresh status."""
        html = self._build_status_html()
        kb = self._status_keyboard()
        await self._tg.edit_message_text(msg_id, html, kb)

    async def _edit_settings(self, chat_id: int, msg_id: int) -> None:
        html = self._build_settings_html()
        kb = self._settings_keyboard()
        await self._tg.edit_message_text(msg_id, html, kb)

    # ── Actions ─────────────────────────────────────────────────

    async def _do_kill(self, chat_id: int | None, msg_id: int | None = None) -> None:
        rt = self._get_runtime()
        if not rt or not rt.is_running:
            text = "\U0001f534 <b>Bot is already stopped</b>"
            if msg_id:
                await self._tg.edit_message_text(msg_id, text)
            else:
                await self._tg._send(text)
            return

        try:
            rt.mm_config.auto_next_window = False
            rt.mm_config.enabled = False
            await rt.stop()
            html = "\U0001f6d1 <b>Killed.</b> Auto-restart OFF."
            kb = [[
                {"text": "\u2699\ufe0f Settings", "callback_data": "settings"},
                {"text": "\U0001f680 Start", "callback_data": "start_bot"},
            ]]
            if msg_id:
                await self._tg.edit_message_text(msg_id, html, kb)
            else:
                await self._tg.send_with_keyboard(html, kb)
        except Exception as e:
            await self._tg._send(f"\U0001f6a8 Kill failed: {e}")

    async def _do_start(self, chat_id: int | None, msg_id: int | None = None) -> None:
        rt = self._get_runtime()
        if not rt:
            await self._tg._send("\U0001f534 Runtime not available")
            return
        if rt.is_running:
            if msg_id:
                await self._edit_status(chat_id, msg_id)
            return

        s = self._settings
        paper = s["mode"] == "paper"
        mode_str = "PAPER" if paper else "LIVE"

        # Show "starting..." in the message
        if msg_id:
            await self._tg.edit_message_text(
                msg_id,
                f"\u23f3 Starting {s['coin']}/{s['timeframe']} {mode_str}...",
            )

        try:
            rt.mm_config.enabled = True
            rt.mm_config.auto_next_window = True
            await rt.start(
                coin=s["coin"],
                timeframe=s["timeframe"],
                paper_mode=paper,
                initial_usdc=s["limit"],
                dev=s["dev"],
            )
            # Show live status after start
            if msg_id:
                await self._edit_status(chat_id, msg_id)
            else:
                await self._show_status(chat_id)
        except Exception as e:
            err_html = f"\U0001f6a8 <b>Start failed</b>\n<code>{e}</code>"
            if msg_id:
                await self._tg.edit_message_text(msg_id, err_html)
            else:
                await self._tg._send(err_html)
