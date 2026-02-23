from __future__ import annotations

import asyncio
import html
import os
import time
from datetime import datetime, timezone
from typing import Any

import httpx


class TelegramNotifier:
    """Fire-and-forget Telegram notifications for market-making notifications.
    Uses httpx (already a project dependency) for HTTP calls.
    """

    def __init__(
        self,
        token: str | None = None,
        chat_id: str | None = None,
        thread_id: str | int | None = None,
    ):
        self.token = token or os.environ.get("TELEGRAM_BOT_TOKEN", "").strip()
        self.chat_id = chat_id or os.environ.get("TELEGRAM_CHAT_ID", "").strip()
        _tid = thread_id if thread_id is not None else os.environ.get("TELEGRAM_THREAD_ID", "").strip()
        self.thread_id = int(_tid) if _tid else None
        self.enabled = bool(self.token and self.chat_id)
        self._background_tasks: set[asyncio.Task] = set()
        self._client: httpx.AsyncClient | None = None
        if self.enabled:
            self._client = httpx.AsyncClient(timeout=10.0)
        self._main_loop: asyncio.AbstractEventLoop | None = None
        try:
            self._main_loop = asyncio.get_running_loop()
        except RuntimeError:
            pass  # Will be set later via set_loop()

    @property
    def base_url(self) -> str:
        return f"https://api.telegram.org/bot{self.token}"

    @property
    def api_url(self) -> str:
        return f"{self.base_url}/sendMessage"

    async def _send(self, html: str) -> None:
        """Send a message. Swallows all exceptions (fire-and-forget)."""
        if not self.enabled or not self._client:
            return
        try:
            payload: dict[str, Any] = {
                "chat_id": self.chat_id,
                "text": html,
                "parse_mode": "HTML",
                "disable_web_page_preview": True,
            }
            if self.thread_id:
                payload["message_thread_id"] = self.thread_id
            resp = await self._client.post(self.api_url, json=payload)
            if not resp.is_success:
                print(f"  [TELEGRAM] send http {resp.status_code}: {resp.text[:300]}")
                return
            try:
                data = resp.json()
                if not data.get("ok", False):
                    print(f"  [TELEGRAM] send api error: {data}")
            except Exception:
                pass
        except Exception as e:
            print(f"  [TELEGRAM] send error: {e}")

    def set_loop(self, loop: asyncio.AbstractEventLoop) -> None:
        """Capture the main event loop for cross-thread notification dispatch."""
        self._main_loop = loop
        print(f"  [TELEGRAM] main loop captured (id={id(loop)})")

    def _fire(self, html: str) -> None:
        """Schedule _send as a background task with cleanup callback.
        Works both from async context and from worker threads (asyncio.to_thread).
        """
        if not self.enabled:
            return
        try:
            loop = asyncio.get_running_loop()
            # We're in an async context — create task directly
            task = loop.create_task(self._send(html))
            self._background_tasks.add(task)
            task.add_done_callback(self._task_done)
        except RuntimeError:
            # Called from a worker thread (e.g. asyncio.to_thread).
            # Use the captured main event loop.
            main = self._main_loop
            if main is not None and main.is_running():
                print(f"  [TELEGRAM] _fire: scheduling from worker thread (loop={id(self._main_loop)})")
                main.call_soon_threadsafe(self._schedule_from_thread, main, html)
            else:
                print("  [TELEGRAM] _fire: no main loop available, message dropped")

    def _schedule_from_thread(self, loop: asyncio.AbstractEventLoop, html: str) -> None:
        """Create an async task from the main event loop thread."""
        task = loop.create_task(self._send(html))
        self._background_tasks.add(task)
        task.add_done_callback(self._task_done)

    def _task_done(self, task: asyncio.Task) -> None:
        self._background_tasks.discard(task)
        if task.cancelled():
            return
        exc = task.exception()
        if exc:
            print(f"  [TELEGRAM] background task error: {exc}")

    def _ts(self) -> str:
        return datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")

    @staticmethod
    def _esc(value: Any) -> str:
        return html.escape(str(value), quote=False)

    @staticmethod
    def _clip(value: str, max_len: int = 240) -> str:
        if len(value) <= max_len:
            return value
        return value[: max_len - 1] + "…"

    @staticmethod
    def _short_order(order_id: str | None) -> str:
        if not order_id:
            return ""
        s = str(order_id)
        if len(s) <= 20:
            return s
        return f"{s[:10]}…{s[-8:]}"

    # ---- Public API: Market Making notifications ----

    def notify_mm_start(
        self,
        coin: str,
        timeframe: str,
        mode: str,
        half_spread_bps: float,
        order_size_usd: float,
    ) -> None:
        html = (
            f"\U0001f680 <b>MM STARTED</b>\n"
            f"━━━━━━━━━━━━━━━━\n"
            f"<b>Market:</b> <code>{self._esc(coin)} {self._esc(timeframe)}</code>\n"
            f"<b>Mode:</b> <code>{self._esc(mode.upper())}</code>\n"
            f"<b>Spread:</b> <code>{half_spread_bps:.0f} bps</code> half\n"
            f"<b>Size:</b> <code>${order_size_usd:.2f}</code>/side\n"
            f"<i>{self._ts()}</i>"
        )
        self._fire(html)

    def notify_mm_stop(self, coin: str, timeframe: str, reason: str = "manual") -> None:
        html = (
            f"\U0001f6d1 <b>MM STOPPED</b>\n"
            f"<b>{self._esc(coin)} {self._esc(timeframe)}</b>\n"
            f"<b>Reason:</b> <code>{self._esc(reason)}</code>\n"
            f"<i>{self._ts()}</i>"
        )
        self._fire(html)

    def notify_fill(
        self,
        coin: str,
        timeframe: str,
        side: str,
        price: float,
        size: float,
        fee: float,
        is_maker: bool,
    ) -> None:
        emoji = "\U0001f7e2" if side.upper() == "BUY" else "\U0001f534"
        maker_tag = "MAKER" if is_maker else "TAKER"
        html = (
            f"{emoji} <b>FILL • {maker_tag}</b>\n"
            f"<b>{self._esc(coin)} {self._esc(timeframe)}</b>\n"
            f"<b>Side:</b> <code>{self._esc(side.upper())}</code> @ <code>{price:.4f}</code>\n"
            f"<b>Size:</b> <code>{size:.2f}</code> shares\n"
            f"<b>Fee:</b> <code>${fee:.4f}</code>\n"
            f"<i>{self._ts()}</i>"
        )
        self._fire(html)

    def notify_pnl_update(
        self,
        coin: str,
        timeframe: str,
        realized_pnl: float,
        unrealized_pnl: float,
        total_fills: int,
        net_delta: float,
    ) -> None:
        total = realized_pnl + unrealized_pnl
        emoji = "\U0001f4c8" if total >= 0 else "\U0001f4c9"
        html = (
            f"{emoji} <b>PnL UPDATE</b>\n"
            f"━━━━━━━━━━━━━━━━\n"
            f"<b>{self._esc(coin)} {self._esc(timeframe)}</b>\n"
            f"<b>Realized:</b> <code>${realized_pnl:+.4f}</code>\n"
            f"<b>Unrealized:</b> <code>${unrealized_pnl:+.4f}</code>\n"
            f"<b>Total:</b> <code>${total:+.4f}</code>\n"
            f"<b>Fills:</b> <code>{total_fills}</code> | "
            f"<b>Delta:</b> <code>{net_delta:+.2f}</code>\n"
            f"<i>{self._ts()}</i>"
        )
        self._fire(html)

    def notify_risk_pause(
        self,
        coin: str,
        timeframe: str,
        reason: str,
        net_delta: float,
        drawdown_usd: float,
    ) -> None:
        html = (
            f"\u26a0\ufe0f <b>MM PAUSED</b>\n"
            f"━━━━━━━━━━━━━━━━\n"
            f"<b>{self._esc(coin)} {self._esc(timeframe)}</b>\n"
            f"<b>Reason:</b> <code>{self._esc(reason)}</code>\n"
            f"<b>Net Delta:</b> <code>{net_delta:+.2f}</code>\n"
            f"<b>Drawdown:</b> <code>${drawdown_usd:.2f}</code>\n"
            f"<i>{self._ts()}</i>"
        )
        self._fire(html)

    def notify_rebate_update(
        self,
        estimated_daily: float,
        total_volume: float,
        qualifying_orders: int,
    ) -> None:
        html = (
            f"\U0001f4b0 <b>REBATE UPDATE</b>\n"
            f"━━━━━━━━━━━━━━━━\n"
            f"<b>Est. Daily:</b> <code>${estimated_daily:.4f}</code>\n"
            f"<b>Volume:</b> <code>${total_volume:.2f}</code>\n"
            f"<b>Qualifying:</b> <code>{qualifying_orders}</code> orders\n"
            f"<i>{self._ts()}</i>"
        )
        self._fire(html)

    def notify_window_transition(
        self,
        coin: str,
        timeframe: str,
        old_window: str,
        new_window: str,
    ) -> None:
        html = (
            f"\U0001f504 <b>WINDOW TRANSITION</b>\n"
            f"<b>{self._esc(coin)} {self._esc(timeframe)}</b>\n"
            f"<code>{self._esc(old_window)}</code> \u2192 <code>{self._esc(new_window)}</code>\n"
            f"Orders cancelled, re-quoting...\n"
            f"<i>{self._ts()}</i>"
        )
        self._fire(html)

    def notify_window_summary(
        self,
        coin: str,
        timeframe: str,
        mode: str,
        session_pnl: float,
        pnl_1h: float,
        pnl_24h: float,
        usdc_balance: float,
    ) -> None:
        """Summary sent after each window closes."""
        emoji = "\U0001f3c6" if session_pnl >= 0 else "\U0001f4a8"
        is_paper = mode.upper() == "PAPER"
        header = f"{emoji} <b>WINDOW CLOSED</b>"
        if is_paper:
            header += "  \u2139\ufe0f <i>[TEST MODE]</i>"
        html = (
            f"{header}\n"
            f"━━━━━━━━━━━━━━━━\n"
            f"<b>Market:</b> <code>{self._esc(coin)} {self._esc(timeframe)}</code>\n\n"
            f"<b>PnL</b>\n"
            f"  Last window: <code>${session_pnl:+.2f}</code>\n"
            f"  1h: <code>${pnl_1h:+.2f}</code>\n"
            f"  24h: <code>${pnl_24h:+.2f}</code>\n\n"
            f"\U0001f4b0 <b>Balance:</b> <code>${usdc_balance:.2f}</code> USDC"
        )
        self._fire(html)

    def notify_error(self, source: str, message: str, detail: str = "") -> None:
        detail_line = (
            f"\n<b>Detail:</b> <code>{self._clip(self._esc(detail))}</code>"
            if detail else ""
        )
        html = (
            f"\U0001f6a8 <b>ERROR • {self._esc(source)}</b>\n"
            f"━━━━━━━━━━━━━━━━\n"
            f"<b>Message:</b> <code>{self._clip(self._esc(message))}</code>"
            f"{detail_line}\n"
            f"<i>{self._ts()}</i>"
        )
        self._fire(html)

    def notify_daily_summary(
        self,
        coin: str,
        timeframe: str,
        total_fills: int,
        realized_pnl: float,
        rebates_earned: float,
        avg_spread_bps: float,
        uptime_pct: float,
        mode: str,
    ) -> None:
        total = realized_pnl + rebates_earned
        html = (
            f"\U0001f4ca <b>DAILY MM SUMMARY</b>\n"
            f"━━━━━━━━━━━━━━━━\n"
            f"<b>{self._esc(coin)} {self._esc(timeframe)}</b> | <code>{self._esc(mode.upper())}</code>\n"
            f"<b>Fills:</b> <code>{total_fills}</code>\n"
            f"<b>PnL:</b> <code>${realized_pnl:+.4f}</code>\n"
            f"<b>Rebates:</b> <code>${rebates_earned:+.4f}</code>\n"
            f"<b>Net:</b> <code>${total:+.4f}</code>\n"
            f"<b>Avg Spread:</b> <code>{avg_spread_bps:.1f} bps</code>\n"
            f"<b>Uptime:</b> <code>{uptime_pct:.1f}%</code>\n"
            f"<i>{self._ts()}</i>"
        )
        self._fire(html)

    # ---- Direct async API (for interactive bot) ----

    async def send_with_keyboard(
        self,
        html: str,
        keyboard: list[list[dict]],
        reply_to_message_id: int | None = None,
    ) -> dict | None:
        """Send a message with inline keyboard. Returns the API result or None."""
        if not self.enabled or not self._client:
            return None
        try:
            payload: dict[str, Any] = {
                "chat_id": self.chat_id,
                "text": html,
                "parse_mode": "HTML",
                "disable_web_page_preview": True,
                "reply_markup": {"inline_keyboard": keyboard},
            }
            if self.thread_id:
                payload["message_thread_id"] = self.thread_id
            if reply_to_message_id:
                payload["reply_to_message_id"] = reply_to_message_id
            resp = await self._client.post(self.api_url, json=payload)
            data = resp.json()
            return data.get("result")
        except Exception as e:
            print(f"  [TELEGRAM] send_with_keyboard error: {e}")
            return None

    async def answer_callback_query(
        self, callback_query_id: str, text: str = "", alert: bool = False
    ) -> None:
        """Answer a callback query (removes spinner on button)."""
        if not self.enabled or not self._client:
            return
        try:
            payload: dict[str, Any] = {"callback_query_id": callback_query_id}
            if text:
                payload["text"] = text
            if alert:
                payload["show_alert"] = True
            await self._client.post(f"{self.base_url}/answerCallbackQuery", json=payload)
        except Exception as e:
            print(f"  [TELEGRAM] answer_callback_query error: {e}")

    async def edit_message_text(
        self,
        message_id: int,
        html: str,
        keyboard: list[list[dict]] | None = None,
    ) -> None:
        """Edit an existing message text (and optionally keyboard)."""
        if not self.enabled or not self._client:
            return
        try:
            payload: dict[str, Any] = {
                "chat_id": self.chat_id,
                "message_id": message_id,
                "text": html,
                "parse_mode": "HTML",
                "disable_web_page_preview": True,
            }
            if keyboard is not None:
                payload["reply_markup"] = {"inline_keyboard": keyboard}
            await self._client.post(f"{self.base_url}/editMessageText", json=payload)
        except Exception as e:
            print(f"  [TELEGRAM] edit_message_text error: {e}")

    async def delete_message(self, message_id: int) -> None:
        """Delete a message."""
        if not self.enabled or not self._client:
            return
        try:
            await self._client.post(
                f"{self.base_url}/deleteMessage",
                json={"chat_id": self.chat_id, "message_id": message_id},
            )
        except Exception as e:
            print(f"  [TELEGRAM] delete_message error: {e}")

    async def get_me(self) -> dict | None:
        """Call getMe to retrieve bot info."""
        if not self.enabled or not self._client:
            return None
        try:
            resp = await self._client.get(f"{self.base_url}/getMe")
            data = resp.json()
            return data.get("result")
        except Exception as e:
            print(f"  [TELEGRAM] get_me error: {e}")
            return None

    async def get_updates(
        self, offset: int | None = None, timeout: int = 30
    ) -> list[dict]:
        """Long-poll getUpdates."""
        if not self.enabled or not self._client:
            return []
        try:
            params: dict[str, Any] = {
                "timeout": timeout,
                "allowed_updates": ["message", "callback_query"],
            }
            if offset is not None:
                params["offset"] = offset
            resp = await self._client.post(
                f"{self.base_url}/getUpdates",
                json=params,
                timeout=timeout + 10,
            )
            data = resp.json()
            return data.get("result", [])
        except Exception as e:
            print(f"  [TELEGRAM] get_updates error: {e}")
            return []

    async def close(self) -> None:
        """Graceful shutdown: wait for pending messages, close HTTP client."""
        if self._background_tasks:
            await asyncio.gather(*self._background_tasks, return_exceptions=True)
        if self._client:
            await self._client.aclose()
