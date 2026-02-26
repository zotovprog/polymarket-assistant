import asyncio

from telegram_bot import TelegramBotManager


class _DummyNotifier:
    def __init__(self):
        self.enabled = True

    async def get_updates(self, offset=None, timeout=30):
        raise RuntimeError("409 Conflict: terminated by other getUpdates request")


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
