"""Event-driven requote triggers for the MM engine."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import List, Optional
import time


@dataclass
class RequoteEvent:
    """A single requote trigger event."""

    event_type: str
    timestamp: float
    detail: str = field(default="")


class EventRequoter:
    """Monitor market events and trigger requotes on meaningful changes.

    Event types:
    - `pm_mid_shift`: PM mid-price moved by at least `pm_mid_threshold_bps`.
    - `binance_move`: Binance reference moved by at least `binance_threshold_bps`.
    - `book_change`: PM best bid or best ask changed.
    - `fill_event`: One of our orders was filled.
    - `inventory_change`: Inventory imbalance tier changed.
    - `timer_fallback`: No events fired for `fallback_interval_sec`.

    This class is designed for single-threaded asyncio flows and uses plain
    instance state (no locks).
    """

    EVENT_TYPES = (
        "pm_mid_shift",
        "binance_move",
        "book_change",
        "fill_event",
        "inventory_change",
        "timer_fallback",
    )

    def __init__(
        self,
        pm_mid_threshold_bps: float = 25,
        binance_threshold_bps: float = 50,
        fallback_interval_sec: float = 10.0,
    ):
        self.pm_mid_threshold_bps = float(pm_mid_threshold_bps)
        self.binance_threshold_bps = float(binance_threshold_bps)
        self.fallback_interval_sec = float(fallback_interval_sec)

        self._last_pm_mid: Optional[float] = None
        self._last_binance_price: Optional[float] = None
        self._last_book_state: Optional[tuple[float, float]] = None
        self._last_inventory_tier: Optional[int] = None

        now = time.time()
        self._started_at = now
        self._last_event_ts = now
        self._last_trigger_ts: Optional[float] = None

        self._event_counts: dict[str, int] = {event: 0 for event in self.EVENT_TYPES}
        self._last_trigger_by_event: dict[str, Optional[float]] = {
            event: None for event in self.EVENT_TYPES
        }

    @staticmethod
    def _move_bps(previous: float, current: float) -> float:
        """Return absolute move in basis points (1 bps = 0.01%)."""
        if previous <= 0:
            return 0.0
        return abs((current - previous) / previous) * 10000.0

    def _record_event(
        self,
        events: List[RequoteEvent],
        event_type: str,
        timestamp: float,
        detail: str = "",
    ) -> None:
        events.append(RequoteEvent(event_type=event_type, timestamp=timestamp, detail=detail))
        self._event_counts[event_type] += 1
        self._last_trigger_by_event[event_type] = timestamp
        self._last_trigger_ts = timestamp
        self._last_event_ts = timestamp

    def check_events(
        self,
        current_pm_mid: Optional[float],
        current_binance_price: Optional[float],
        current_best_bid: Optional[float],
        current_best_ask: Optional[float],
        had_fill: bool = False,
        inventory_imbalance_tier: int = 0,
        inventory_tier: Optional[int] = None,
    ) -> List[RequoteEvent]:
        """Check all event sources and return triggered requote events.

        Inputs:
        - `current_pm_mid`: Latest PM mid price.
        - `current_binance_price`: Latest Binance reference price.
        - `current_best_bid` / `current_best_ask`: PM top-of-book best prices.
        - `had_fill`: Set `True` when at least one of our orders filled.
        - `inventory_imbalance_tier`: Current inventory tier bucket.
        - `inventory_tier`: Alias for `inventory_imbalance_tier`.
        """

        now = time.time()
        events: List[RequoteEvent] = []

        if inventory_tier is not None:
            inventory_imbalance_tier = int(inventory_tier)

        if had_fill:
            self._record_event(events, "fill_event", now, detail="order fill detected")

        if current_pm_mid is not None and current_pm_mid > 0:
            if self._last_pm_mid is not None and self._last_pm_mid > 0:
                pm_move_bps = self._move_bps(self._last_pm_mid, float(current_pm_mid))
                if pm_move_bps >= self.pm_mid_threshold_bps:
                    self._record_event(
                        events,
                        "pm_mid_shift",
                        now,
                        detail=f"move_bps={pm_move_bps:.2f}",
                    )
            self._last_pm_mid = float(current_pm_mid)

        if current_binance_price is not None and current_binance_price > 0:
            if self._last_binance_price is not None and self._last_binance_price > 0:
                binance_move_bps = self._move_bps(
                    self._last_binance_price, float(current_binance_price)
                )
                if binance_move_bps >= self.binance_threshold_bps:
                    self._record_event(
                        events,
                        "binance_move",
                        now,
                        detail=f"move_bps={binance_move_bps:.2f}",
                    )
            self._last_binance_price = float(current_binance_price)

        if current_best_bid is not None and current_best_ask is not None:
            current_book_state = (float(current_best_bid), float(current_best_ask))
            if self._last_book_state is not None and current_book_state != self._last_book_state:
                prev_bid, prev_ask = self._last_book_state
                self._record_event(
                    events,
                    "book_change",
                    now,
                    detail=(
                        f"bid {prev_bid:.6f}->{current_book_state[0]:.6f}, "
                        f"ask {prev_ask:.6f}->{current_book_state[1]:.6f}"
                    ),
                )
            self._last_book_state = current_book_state

        if self._last_inventory_tier is not None:
            if inventory_imbalance_tier != self._last_inventory_tier:
                self._record_event(
                    events,
                    "inventory_change",
                    now,
                    detail=f"tier {self._last_inventory_tier}->{inventory_imbalance_tier}",
                )
        self._last_inventory_tier = int(inventory_imbalance_tier)

        if not events:
            idle_sec = now - self._last_event_ts
            if idle_sec >= self.fallback_interval_sec:
                self._record_event(
                    events,
                    "timer_fallback",
                    now,
                    detail=f"idle_sec={idle_sec:.2f}",
                )

        return events

    def should_requote(
        self,
        current_pm_mid: Optional[float],
        current_binance_price: Optional[float],
        current_best_bid: Optional[float],
        current_best_ask: Optional[float],
        had_fill: bool = False,
        inventory_imbalance_tier: int = 0,
        inventory_tier: Optional[int] = None,
    ) -> bool:
        """Return True when at least one requote event is triggered."""

        events = self.check_events(
            current_pm_mid=current_pm_mid,
            current_binance_price=current_binance_price,
            current_best_bid=current_best_bid,
            current_best_ask=current_best_ask,
            had_fill=had_fill,
            inventory_imbalance_tier=inventory_imbalance_tier,
            inventory_tier=inventory_tier,
        )
        return bool(events)

    @property
    def stats(self) -> dict:
        """Return event counts, frequencies, and last trigger timestamps."""

        now = time.time()
        elapsed_sec = max(now - self._started_at, 1e-9)
        frequencies_hz = {
            event_type: count / elapsed_sec
            for event_type, count in self._event_counts.items()
        }
        total_triggers = sum(self._event_counts.values())

        return {
            "counts": dict(self._event_counts),
            "frequencies_hz": frequencies_hz,
            "total_triggers": total_triggers,
            "elapsed_sec": elapsed_sec,
            "last_trigger_time": self._last_trigger_ts,
            "last_trigger_by_event": dict(self._last_trigger_by_event),
        }
