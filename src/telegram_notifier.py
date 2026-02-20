from __future__ import annotations

import asyncio
import html
import os
import time
from datetime import datetime, timezone
from typing import Any

import httpx


class TelegramNotifier:
    """Fire-and-forget Telegram notifications for trading events.
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
            # Schedule the coroutine on the main event loop.
            try:
                loop = asyncio.get_event_loop()
                if loop.is_running():
                    loop.call_soon_threadsafe(self._schedule_from_thread, loop, html)
                # else: no running loop at all, drop silently
            except RuntimeError:
                pass  # No event loop available at all

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

    # ---- Public API ----

    def notify_entry(
        self,
        mode: str,
        coin: str,
        timeframe: str,
        side: str,
        price: float,
        size_usd: float,
        status: str,
        reason: str,
        order_id: str | None = None,
    ) -> None:
        status_l = status.lower()
        is_ok = status_l in {"paper", "filled", "partial_filled", "partial_filled_cancelled", "partial_filled_open", "posted"}
        emoji = "\U0001f7e2" if is_ok else "\U0001f534"
        reason_txt = self._clip(self._esc(reason))
        order_txt = self._short_order(order_id)
        html = (
            f"{emoji} <b>ENTRY • {self._esc(mode.upper())}</b>\n"
            f"━━━━━━━━━━━━━━━━\n"
            f"<b>Market:</b> <code>{self._esc(coin)} {self._esc(timeframe)}</code>\n"
            f"<b>Side:</b> <code>{self._esc(side)}</code> @ <code>{price:.3f}</code>\n"
            f"<b>Size:</b> <code>${size_usd:.2f}</code>\n"
            f"<b>Status:</b> <code>{self._esc(status)}</code>\n"
            f"<b>Reason:</b> <code>{reason_txt}</code>\n"
        )
        if order_txt:
            html += f"<b>Order:</b> <code>{self._esc(order_txt)}</code>\n"
        html += f"<i>{self._ts()}</i>"
        self._fire(html)

    def notify_exit(
        self,
        mode: str,
        coin: str,
        timeframe: str,
        side: str,
        price: float,
        size_usd: float,
        status: str,
        reason: str,
        pnl_usd: float | None = None,
        pnl_pct: float | None = None,
        order_id: str | None = None,
    ) -> None:
        emoji = "\u2705" if pnl_pct is not None and pnl_pct >= 0 else "\u274c"
        reason_txt = self._clip(self._esc(reason))
        order_txt = self._short_order(order_id)
        pnl_line = ""
        if pnl_pct is not None and pnl_usd is not None:
            pnl_line = (
                f"<b>PnL:</b> <code>{pnl_pct:+.1f}%</code> "
                f"(<code>${pnl_usd:+.2f}</code>)\n"
            )
        html = (
            f"{emoji} <b>EXIT • {self._esc(mode.upper())}</b>\n"
            f"━━━━━━━━━━━━━━━━\n"
            f"<b>Market:</b> <code>{self._esc(coin)} {self._esc(timeframe)}</code>\n"
            f"<b>Side:</b> <code>{self._esc(side)}</code> @ <code>{price:.3f}</code>\n"
            f"<b>Size:</b> <code>${size_usd:.2f}</code>\n"
            f"<b>Status:</b> <code>{self._esc(status)}</code>\n"
            f"{pnl_line}"
            f"<b>Reason:</b> <code>{reason_txt}</code>\n"
        )
        if order_txt:
            html += f"<b>Order:</b> <code>{self._esc(order_txt)}</code>\n"
        html += f"<i>{self._ts()}</i>"
        self._fire(html)

    def notify_window_exit(
        self,
        coin: str,
        timeframe: str,
        side: str,
        remaining_sec: int,
    ) -> None:
        html = (
            f"\u23f0 <b>WINDOW EXIT</b>\n"
            f"<b>{coin} {timeframe}</b> | {side}\n"
            f"Window closing in <code>{remaining_sec}s</code>. Forced exit triggered.\n"
            f"<i>{self._ts()}</i>"
        )
        self._fire(html)

    def notify_daily_summary(
        self,
        coin: str,
        timeframe: str,
        trades_today: int,
        total_pnl_usd: float,
        total_pnl_pct: float,
        wins: int,
        losses: int,
        mode: str,
    ) -> None:
        winrate = (wins / (wins + losses) * 100) if (wins + losses) > 0 else 0.0
        html = (
            f"\U0001f4ca <b>DAILY SUMMARY</b>\n"
            f"<b>{coin} {timeframe}</b> | Mode: <code>{mode.upper()}</code>\n"
            f"Trades: <code>{trades_today}</code> | "
            f"W/L: <code>{wins}/{losses}</code> ({winrate:.0f}%)\n"
            f"Total PnL: <code>{total_pnl_pct:+.1f}%</code> "
            f"(<code>${total_pnl_usd:+.2f}</code>)\n"
            f"<i>{self._ts()}</i>"
        )
        self._fire(html)

    def notify_session_start(
        self,
        mode: str,
        coin: str,
        timeframe: str,
        size_usd: float,
        preset: str = "",
        balance: float | None = None,
    ) -> None:
        strategy_line = f"Strategy: <code>{preset}</code>\n" if preset else ""
        bal_line = f"\U0001f4b0 Balance: <code>${balance:.2f}</code>\n" if balance is not None else ""
        html = (
            f"\U0001f680 <b>SESSION STARTED</b>\n"
            f"<b>{coin} {timeframe}</b> | Mode: <code>{mode.upper()}</code>\n"
            f"Size: <code>${size_usd:.2f}</code>\n"
            f"{strategy_line}"
            f"{bal_line}"
            f"<i>{self._ts()}</i>"
        )
        self._fire(html)

    def notify_session_stop(self, coin: str, timeframe: str) -> None:
        html = (
            f"\U0001f6d1 <b>SESSION STOPPED</b>\n"
            f"<b>{coin} {timeframe}</b>\n"
            f"<i>{self._ts()}</i>"
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
