from __future__ import annotations

import asyncio
import html
import logging
import os
import time
from datetime import datetime, timezone
from typing import Any

import httpx

log = logging.getLogger("tg.notify")


class TelegramAPIError(RuntimeError):
    """Structured Telegram API error with optional HTTP/API codes."""

    def __init__(
        self,
        message: str,
        *,
        status_code: int | None = None,
        error_code: int | None = None,
    ) -> None:
        super().__init__(message)
        self.status_code = status_code
        self.error_code = error_code


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

    def switch_credentials(self, token: str, chat_id: str, thread_id: str | int | None = None) -> None:
        """Switch to different Telegram credentials (e.g. dev channel)."""
        self.token = token
        self.chat_id = chat_id
        self.thread_id = int(thread_id) if thread_id else None
        self.enabled = bool(self.token and self.chat_id)
        if self.enabled and not self._client:
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
                log.warning("send http %s: %s", resp.status_code, resp.text[:300])
                return
            try:
                data = resp.json()
                if not data.get("ok", False):
                    log.warning("send api error: %s", data)
            except Exception:
                pass
        except Exception as e:
            log.warning("send error: %s", e)

    def set_loop(self, loop: asyncio.AbstractEventLoop) -> None:
        """Capture the main event loop for cross-thread notification dispatch."""
        self._main_loop = loop
        log.info("main loop captured (id=%s)", id(loop))

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
                log.info("_fire: scheduling from worker thread")
                main.call_soon_threadsafe(self._schedule_from_thread, main, html)
            else:
                log.warning("_fire: no main loop available, message dropped")

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
            log.warning("background task error: %s", exc)

    def _ts(self) -> str:
        return datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")

    @staticmethod
    def _esc(value: Any) -> str:
        return html.escape(str(value), quote=False)

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
        log.info("notify_window_summary: firing message (enabled=%s, chat=%s)",
                 self.enabled, self.chat_id)
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
            log.warning("send_with_keyboard error: %s", e)
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
            log.warning("answer_callback_query error: %s", e)

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
            log.warning("edit_message_text error: %s", e)

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
            log.warning("delete_message error: %s", e)

    async def get_me(self) -> dict | None:
        """Call getMe to retrieve bot info."""
        if not self.enabled or not self._client:
            return None
        try:
            resp = await self._client.get(f"{self.base_url}/getMe")
            data = resp.json()
            return data.get("result")
        except Exception as e:
            log.warning("get_me error: %s", e)
            return None

    async def get_updates(
        self, offset: int | None = None, timeout: int = 30
    ) -> list[dict]:
        """Long-poll getUpdates."""
        if not self.enabled or not self._client:
            return []
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
        data: dict[str, Any] = {}
        try:
            data = resp.json()
        except Exception:
            data = {}
        if not resp.is_success:
            description = data.get("description") or resp.text[:300]
            error_code = data.get("error_code")
            raise TelegramAPIError(
                f"Telegram getUpdates HTTP {resp.status_code}: {description}",
                status_code=resp.status_code,
                error_code=error_code if isinstance(error_code, int) else None,
            )
        if not data.get("ok"):
            error_code = data.get("error_code")
            raise TelegramAPIError(
                f"Telegram getUpdates failed: {error_code} {data.get('description', '')}",
                status_code=resp.status_code,
                error_code=error_code if isinstance(error_code, int) else None,
            )
        return data.get("result", [])

    async def close(self) -> None:
        """Graceful shutdown: wait for pending messages, close HTTP client."""
        if self._background_tasks:
            await asyncio.gather(*self._background_tasks, return_exceptions=True)
        if self._client:
            await self._client.aclose()
