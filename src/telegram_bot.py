"""Interactive Telegram Bot for MM control.

Long-polls getUpdates and handles:
- /status  — current bot state
- /start   — show main menu
- Kill All — emergency stop
- Settings — timeframe, mode, coin, limit selection
- Start Bot — launch with selected settings
"""
from __future__ import annotations

import asyncio
import logging
import time
from typing import Any, Callable, Awaitable

log = logging.getLogger("tg.bot")


class TelegramBotManager:
    """Interactive Telegram bot using long-polling via TelegramNotifier."""

    def __init__(
        self,
        notifier,  # TelegramNotifier instance
        get_runtime,  # callable() -> MMRuntime
        access_key: str = "",
    ):
        self._tg = notifier
        self._get_runtime = get_runtime
        self._access_key = access_key
        self._running = False
        self._task: asyncio.Task | None = None
        self._offset: int | None = None

        # User settings (persisted per chat session)
        self._settings: dict[str, Any] = {
            "coin": "BTC",
            "timeframe": "15m",
            "mode": "paper",
            "limit": 25.0,
            "dev": True,
        }

    async def start(self) -> None:
        """Start the long-polling loop."""
        if self._running:
            return
        self._running = True
        self._task = asyncio.create_task(self._poll_loop())
        log.info("TelegramBotManager started")

    async def stop(self) -> None:
        """Stop polling."""
        self._running = False
        if self._task:
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass
        log.info("TelegramBotManager stopped")

    # ── Main poll loop ──────────────────────────────────────────

    async def _poll_loop(self) -> None:
        while self._running:
            try:
                updates = await self._tg.get_updates(
                    offset=self._offset, timeout=30,
                )
                for update in updates:
                    self._offset = update["update_id"] + 1
                    try:
                        await self._handle_update(update)
                    except Exception as e:
                        log.warning("Error handling update: %s", e)
            except asyncio.CancelledError:
                break
            except Exception as e:
                log.warning("Poll error: %s", e)
                await asyncio.sleep(5.0)

    async def _handle_update(self, update: dict) -> None:
        if "callback_query" in update:
            await self._handle_callback(update["callback_query"])
        elif "message" in update:
            msg = update["message"]
            text = (msg.get("text") or "").strip()
            if text.startswith("/"):
                await self._handle_command(text, msg)

    # ── Command handlers ────────────────────────────────────────

    async def _handle_command(self, text: str, msg: dict) -> None:
        cmd = text.split()[0].lower().split("@")[0]  # strip @botname
        if cmd in ("/start", "/menu"):
            await self._send_main_menu()
        elif cmd == "/status":
            await self._send_status()
        elif cmd == "/kill":
            await self._do_kill_all()
        elif cmd == "/help":
            await self._tg._send(
                "<b>Commands:</b>\n"
                "/status — Bot status\n"
                "/menu — Main menu\n"
                "/kill — Kill All\n"
            )

    # ── Callback handlers ───────────────────────────────────────

    async def _handle_callback(self, cq: dict) -> None:
        data = cq.get("data", "")
        cq_id = cq["id"]

        if data == "status":
            await self._tg.answer_callback_query(cq_id)
            await self._send_status()
        elif data == "kill_all":
            await self._tg.answer_callback_query(cq_id, text="Stopping...")
            await self._do_kill_all()
        elif data == "settings":
            await self._tg.answer_callback_query(cq_id)
            await self._send_settings()
        elif data == "start_bot":
            await self._tg.answer_callback_query(cq_id, text="Starting...")
            await self._do_start_bot()
        elif data == "menu":
            await self._tg.answer_callback_query(cq_id)
            await self._send_main_menu()

        # ── Settings callbacks ──
        elif data.startswith("set_coin:"):
            val = data.split(":")[1]
            self._settings["coin"] = val
            await self._tg.answer_callback_query(cq_id, text=f"Coin: {val}")
            await self._send_settings()
        elif data.startswith("set_tf:"):
            val = data.split(":")[1]
            self._settings["timeframe"] = val
            await self._tg.answer_callback_query(cq_id, text=f"TF: {val}")
            await self._send_settings()
        elif data.startswith("set_mode:"):
            val = data.split(":")[1]
            self._settings["mode"] = val
            await self._tg.answer_callback_query(cq_id, text=f"Mode: {val}")
            await self._send_settings()
        elif data.startswith("set_limit:"):
            val = float(data.split(":")[1])
            self._settings["limit"] = val
            await self._tg.answer_callback_query(cq_id, text=f"Limit: ${val:.0f}")
            await self._send_settings()
        elif data.startswith("set_dev:"):
            val = data.split(":")[1] == "1"
            self._settings["dev"] = val
            await self._tg.answer_callback_query(cq_id, text=f"Dev: {'ON' if val else 'OFF'}")
            await self._send_settings()
        else:
            await self._tg.answer_callback_query(cq_id)

    # ── UI builders ─────────────────────────────────────────────

    async def _send_main_menu(self) -> None:
        rt = self._get_runtime()
        is_running = rt.is_running if rt else False
        status_emoji = "\U0001f7e2" if is_running else "\U0001f534"

        html = (
            f"{status_emoji} <b>MM Bot</b>\n"
            f"━━━━━━━━━━━━━━━━\n"
        )
        if is_running:
            html += f"<b>Running:</b> {rt._coin}/{rt._timeframe}\n"
            html += f"<b>Mode:</b> {'PAPER' if rt._paper_mode else 'LIVE'}\n"
        else:
            html += "<b>Status:</b> Stopped\n"

        keyboard = [
            [{"text": "\U0001f4ca Status", "callback_data": "status"}],
            [
                {"text": "\u2699\ufe0f Settings", "callback_data": "settings"},
                {"text": "\U0001f680 Start Bot", "callback_data": "start_bot"},
            ],
            [{"text": "\U0001f6d1 Kill All", "callback_data": "kill_all"}],
        ]
        await self._tg.send_with_keyboard(html, keyboard)

    async def _send_status(self) -> None:
        rt = self._get_runtime()
        if not rt or not rt.is_running:
            await self._tg._send(
                "\U0001f534 <b>Bot is stopped</b>\n"
                "Use /menu to start."
            )
            return

        snap = rt.snapshot()
        inv = snap.get("inventory", {})
        fv = snap.get("fair_value", {})
        market = snap.get("market", {})
        pnl = snap.get("total_pnl", 0)

        mode = "PAPER" if snap.get("paper_mode") else "LIVE"
        dev = " [DEV]" if snap.get("dev_mode") else ""
        time_left = market.get("time_remaining", 0)
        minutes = int(time_left // 60)
        seconds = int(time_left % 60)

        html = (
            f"\U0001f7e2 <b>MM Status</b>{dev}\n"
            f"━━━━━━━━━━━━━━━━\n"
            f"<b>Market:</b> <code>{market.get('coin', '?')}/{market.get('timeframe', '?')}</code>\n"
            f"<b>Mode:</b> <code>{mode}</code>\n"
            f"<b>Time left:</b> <code>{minutes}m {seconds}s</code>\n\n"
            f"<b>Inventory</b>\n"
            f"  UP: <code>{inv.get('up_shares', 0):.2f}</code>\n"
            f"  DN: <code>{inv.get('dn_shares', 0):.2f}</code>\n"
            f"  Delta: <code>{inv.get('net_delta', 0):+.2f}</code>\n\n"
            f"<b>Fair Value</b>\n"
            f"  UP: <code>{fv.get('up', 0.5):.4f}</code>\n"
            f"  DN: <code>{fv.get('dn', 0.5):.4f}</code>\n"
            f"  BTC: <code>${fv.get('binance_mid', 0):,.2f}</code>\n\n"
            f"<b>PnL:</b> <code>${pnl:+.2f}</code>\n"
        )

        # Check if paused
        if snap.get("is_paused"):
            html += f"\n\u26a0\ufe0f <b>PAUSED:</b> {snap.get('pause_reason', 'unknown')}\n"

        keyboard = [
            [
                {"text": "\U0001f504 Refresh", "callback_data": "status"},
                {"text": "\U0001f3e0 Menu", "callback_data": "menu"},
            ],
            [{"text": "\U0001f6d1 Kill All", "callback_data": "kill_all"}],
        ]
        await self._tg.send_with_keyboard(html, keyboard)

    async def _send_settings(self) -> None:
        s = self._settings
        coin_mark = lambda c: f"\u2705 {c}" if c == s["coin"] else c
        tf_mark = lambda t: f"\u2705 {t}" if t == s["timeframe"] else t
        mode_mark = lambda m: f"\u2705 {m.upper()}" if m == s["mode"] else m.upper()

        html = (
            f"\u2699\ufe0f <b>Settings</b>\n"
            f"━━━━━━━━━━━━━━━━\n"
            f"<b>Coin:</b> <code>{s['coin']}</code>\n"
            f"<b>Timeframe:</b> <code>{s['timeframe']}</code>\n"
            f"<b>Mode:</b> <code>{s['mode'].upper()}</code>\n"
            f"<b>Limit:</b> <code>${s['limit']:.0f}</code>\n"
            f"<b>Dev:</b> <code>{'ON' if s['dev'] else 'OFF'}</code>\n"
        )

        keyboard = [
            # Coin row
            [
                {"text": coin_mark("BTC"), "callback_data": "set_coin:BTC"},
                {"text": coin_mark("ETH"), "callback_data": "set_coin:ETH"},
                {"text": coin_mark("SOL"), "callback_data": "set_coin:SOL"},
                {"text": coin_mark("XRP"), "callback_data": "set_coin:XRP"},
            ],
            # Timeframe row
            [
                {"text": tf_mark("5m"), "callback_data": "set_tf:5m"},
                {"text": tf_mark("15m"), "callback_data": "set_tf:15m"},
                {"text": tf_mark("1h"), "callback_data": "set_tf:1h"},
                {"text": tf_mark("4h"), "callback_data": "set_tf:4h"},
            ],
            # Mode row
            [
                {"text": mode_mark("paper"), "callback_data": "set_mode:paper"},
                {"text": mode_mark("live"), "callback_data": "set_mode:live"},
            ],
            # Limit row
            [
                {"text": "$25" + ("\u2705" if s["limit"] == 25 else ""), "callback_data": "set_limit:25"},
                {"text": "$50" + ("\u2705" if s["limit"] == 50 else ""), "callback_data": "set_limit:50"},
                {"text": "$100" + ("\u2705" if s["limit"] == 100 else ""), "callback_data": "set_limit:100"},
                {"text": "$250" + ("\u2705" if s["limit"] == 250 else ""), "callback_data": "set_limit:250"},
            ],
            # Dev toggle
            [
                {"text": f"Dev: {'ON \u2705' if s['dev'] else 'OFF'}", "callback_data": f"set_dev:{'0' if s['dev'] else '1'}"},
            ],
            # Action row
            [
                {"text": "\U0001f680 Start Bot", "callback_data": "start_bot"},
                {"text": "\U0001f3e0 Menu", "callback_data": "menu"},
            ],
        ]
        await self._tg.send_with_keyboard(html, keyboard)

    # ── Actions ─────────────────────────────────────────────────

    async def _do_kill_all(self) -> None:
        rt = self._get_runtime()
        if not rt:
            await self._tg._send("\U0001f534 Runtime not available")
            return

        if not rt.is_running:
            await self._tg._send("\U0001f534 Bot is already stopped")
            return

        try:
            rt.mm_config.auto_next_window = False
            rt.mm_config.enabled = False
            await rt.stop()
            await self._tg._send(
                "\U0001f6d1 <b>Kill All executed</b>\n"
                "Auto-restart disabled. Bot stopped."
            )
        except Exception as e:
            await self._tg._send(f"\U0001f6a8 Kill All failed: {e}")

    async def _do_start_bot(self) -> None:
        rt = self._get_runtime()
        if not rt:
            await self._tg._send("\U0001f534 Runtime not available")
            return

        if rt.is_running:
            await self._tg._send("\u26a0\ufe0f Bot is already running. Kill first.")
            return

        s = self._settings
        paper = s["mode"] == "paper"

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
            mode_str = "PAPER" if paper else "LIVE"
            await self._tg._send(
                f"\U0001f680 <b>Bot started</b>\n"
                f"<code>{s['coin']}/{s['timeframe']}</code> | "
                f"<code>{mode_str}</code> | "
                f"<code>${s['limit']:.0f}</code>"
            )
        except Exception as e:
            await self._tg._send(f"\U0001f6a8 Start failed: {e}")
