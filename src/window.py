from __future__ import annotations

import time
from dataclasses import dataclass
from datetime import datetime, timezone, timedelta


# Seconds per timeframe
WINDOW_DURATION: dict[str, int] = {
    "5m": 300,
    "15m": 900,
    "1h": 3600,
    "4h": 14400,
    "daily": 86400,
}

# Entry buffer: block new entries if remaining_sec < this value
WINDOW_ENTRY_BUFFER: dict[str, int] = {
    "5m": 60,
    "15m": 120,
    "1h": 300,
    "4h": 600,
    "daily": 1800,
}

# Exit buffer: force exit if remaining_sec < this value
WINDOW_EXIT_BUFFER: dict[str, int] = {
    "5m": 30,
    "15m": 60,
    "1h": 180,
    "4h": 300,
    "daily": 900,
}


def _daily_window_boundaries() -> tuple[int, int]:
    """Daily window: ET noon to ET noon (24h)."""
    utc = datetime.now(timezone.utc)
    year = utc.year

    # DST logic (same as feeds._et_now)
    mar1_dow = datetime(year, 3, 1).weekday()
    mar_sun = 1 + (6 - mar1_dow) % 7
    dst_start = datetime(year, 3, mar_sun + 7, 2, 0, 0, tzinfo=timezone.utc)
    nov1_dow = datetime(year, 11, 1).weekday()
    nov_sun = 1 + (6 - nov1_dow) % 7
    dst_end = datetime(year, 11, nov_sun, 6, 0, 0, tzinfo=timezone.utc)

    et_offset_hours = -4 if dst_start <= utc < dst_end else -5

    # ET noon = UTC (12 - et_offset_hours) = UTC 16:00 or 17:00
    noon_utc_hour = 12 - et_offset_hours  # 16 or 17
    today_noon_utc = utc.replace(
        hour=noon_utc_hour, minute=0, second=0, microsecond=0
    )

    if utc < today_noon_utc:
        start_utc = today_noon_utc - timedelta(days=1)
    else:
        start_utc = today_noon_utc
    end_utc = start_utc + timedelta(days=1)

    return int(start_utc.timestamp()), int(end_utc.timestamp())


def window_boundaries(tf: str) -> tuple[int, int]:
    """Return (start_ts, end_ts) in UTC epoch seconds for the current window."""
    now_ts = int(time.time())

    if tf == "5m":
        start = (now_ts // 300) * 300
        return start, start + 300

    if tf == "15m":
        start = (now_ts // 900) * 900
        return start, start + 900

    if tf == "1h":
        start = (now_ts // 3600) * 3600
        return start, start + 3600

    if tf == "4h":
        # Matches feeds.py _build_slug: ((now_ts - 3600) // 14400) * 14400 + 3600
        start = ((now_ts - 3600) // 14400) * 14400 + 3600
        return start, start + 14400

    if tf == "daily":
        return _daily_window_boundaries()

    # Fallback: no window constraint
    return 0, now_ts + 999999


def window_time_remaining(tf: str) -> int:
    """Seconds remaining in the current window for timeframe tf."""
    _, end_ts = window_boundaries(tf)
    return max(0, end_ts - int(time.time()))


@dataclass
class WindowInfo:
    timeframe: str
    remaining_sec: int
    start_ts: int
    end_ts: int
    entry_blocked: bool
    exit_forced: bool


def get_window_info(tf: str) -> WindowInfo:
    """Full window status including safety flags."""
    start_ts, end_ts = window_boundaries(tf)
    remaining = max(0, end_ts - int(time.time()))
    entry_buf = WINDOW_ENTRY_BUFFER.get(tf, 0)
    exit_buf = WINDOW_EXIT_BUFFER.get(tf, 0)
    return WindowInfo(
        timeframe=tf,
        remaining_sec=remaining,
        start_ts=start_ts,
        end_ts=end_ts,
        entry_blocked=remaining < entry_buf,
        exit_forced=remaining < exit_buf,
    )
