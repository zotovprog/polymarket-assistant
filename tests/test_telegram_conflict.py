import asyncio
import os
import sys

BASE = os.path.dirname(os.path.dirname(__file__))
SRC = os.path.join(BASE, "src")
if SRC not in sys.path:
    sys.path.insert(0, SRC)

from telegram_bot import TelegramBotManager
from telegram_notifier import TelegramAPIError


class _DummyNotifier:
    def __init__(self):
        self.enabled = True

    async def get_updates(self, offset=None, timeout=30):
        raise RuntimeError("409 Conflict: terminated by other getUpdates request")


class _TypedConflictNotifier:
    def __init__(self):
        self.enabled = True

    async def get_updates(self, offset=None, timeout=30):
        raise TelegramAPIError(
            "Telegram getUpdates HTTP 409: Conflict",
            status_code=409,
            error_code=409,
        )


class _FastEmptyNotifier:
    def __init__(self):
        self.enabled = True
        self.calls = 0

    async def get_updates(self, offset=None, timeout=30):
        self.calls += 1
        return []


def test_telegram_polling_disables_on_409_conflict():
    async def _run() -> None:
        notifier = _DummyNotifier()
        seen_reason = {"value": ""}

        def _on_conflict(reason: str) -> None:
            seen_reason["value"] = reason

        mgr = TelegramBotManager(
            notifier=notifier,
            get_runtime=lambda: None,
            access_key="",
            on_conflict=_on_conflict,
        )

        await mgr.start()
        # Let background loop run one iteration.
        await asyncio.sleep(0.05)

        status = mgr.status
        assert status["running"] is False
        assert "409 Conflict" in status["disabled_reason"]
        assert notifier.enabled is False
        assert "409 Conflict" in seen_reason["value"]

        await mgr.stop()

    asyncio.run(_run())


def test_telegram_polling_disables_on_typed_409_conflict():
    async def _run() -> None:
        notifier = _TypedConflictNotifier()
        seen_reason = {"value": ""}

        def _on_conflict(reason: str) -> None:
            seen_reason["value"] = reason

        mgr = TelegramBotManager(
            notifier=notifier,
            get_runtime=lambda: None,
            access_key="",
            on_conflict=_on_conflict,
        )

        await mgr.start()
        await asyncio.sleep(0.05)

        status = mgr.status
        assert status["running"] is False
        assert "409 Conflict" in status["disabled_reason"]
        assert notifier.enabled is False
        assert "409 Conflict" in seen_reason["value"]

        await mgr.stop()

    asyncio.run(_run())


def test_telegram_poll_loop_enforces_min_yield_on_fast_empty_polls():
    async def _run() -> None:
        notifier = _FastEmptyNotifier()
        mgr = TelegramBotManager(
            notifier=notifier,
            get_runtime=lambda: None,
            access_key="",
        )
        mgr._min_poll_interval_sec = 0.05

        sleep_calls: list[float] = []
        original_sleep = asyncio.sleep

        async def fake_sleep(delay: float) -> None:
            sleep_calls.append(delay)
            mgr._running = False
            await original_sleep(0)

        asyncio.sleep = fake_sleep
        try:
            mgr._running = True
            await mgr._poll_loop()
        finally:
            asyncio.sleep = original_sleep

        assert notifier.calls == 1
        assert any(delay >= 0.04 for delay in sleep_calls)

    asyncio.run(_run())
