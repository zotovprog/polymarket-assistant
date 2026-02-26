"""Lightweight runtime loop metrics for diagnosing CPU hot paths."""
from __future__ import annotations

from collections import defaultdict
from threading import Lock
import os
import time
from typing import Any


class RuntimeMetrics:
    """In-memory counters with interval-rate snapshots."""

    def __init__(self) -> None:
        self._lock = Lock()
        self._counts_total: dict[str, int] = defaultdict(int)
        self._counts_interval: dict[str, int] = defaultdict(int)
        self._dur_total_ms: dict[str, float] = defaultdict(float)
        self._dur_total_n: dict[str, int] = defaultdict(int)
        self._dur_interval_ms: dict[str, float] = defaultdict(float)
        self._dur_interval_n: dict[str, int] = defaultdict(int)
        self._last_reset_ts: float = time.time()
        self._last_cpu_sample_ts: float = self._last_reset_ts
        self._last_cpu_sample_proc_sec: float = time.process_time()

    def incr(self, name: str, n: int = 1) -> None:
        if n <= 0:
            return
        with self._lock:
            self._counts_total[name] += n
            self._counts_interval[name] += n

    def observe_ms(self, name: str, value_ms: float) -> None:
        if value_ms < 0:
            return
        with self._lock:
            self._dur_total_ms[name] += float(value_ms)
            self._dur_total_n[name] += 1
            self._dur_interval_ms[name] += float(value_ms)
            self._dur_interval_n[name] += 1

    def mark(self, name: str, *, duration_ms: float | None = None, n: int = 1) -> None:
        self.incr(name, n=n)
        if duration_ms is not None:
            self.observe_ms(name, duration_ms)

    def snapshot(self, *, reset: bool = False, top_n: int = 50) -> dict[str, Any]:
        now = time.time()
        with self._lock:
            window_sec = max(1e-6, now - self._last_reset_ts)
            cpu_window_sec = max(1e-6, now - self._last_cpu_sample_ts)
            cpu_now_sec = time.process_time()
            cpu_delta_sec = max(0.0, cpu_now_sec - self._last_cpu_sample_proc_sec)
            # >100% is expected on multi-core hosts.
            process_cpu_pct = round((cpu_delta_sec / cpu_window_sec) * 100.0, 2)

            counts = []
            for name, interval_count in self._counts_interval.items():
                if interval_count <= 0:
                    continue
                counts.append({
                    "name": name,
                    "interval_count": int(interval_count),
                    "rate_hz": round(interval_count / window_sec, 2),
                    "total_count": int(self._counts_total.get(name, 0)),
                })
            counts.sort(key=lambda x: x["interval_count"], reverse=True)

            durations = []
            for name, interval_n in self._dur_interval_n.items():
                if interval_n <= 0:
                    continue
                interval_sum = float(self._dur_interval_ms.get(name, 0.0))
                total_n = int(self._dur_total_n.get(name, 0))
                total_sum = float(self._dur_total_ms.get(name, 0.0))
                durations.append({
                    "name": name,
                    "interval_samples": int(interval_n),
                    "interval_avg_ms": round(interval_sum / interval_n, 3),
                    "total_samples": total_n,
                    "total_avg_ms": round(total_sum / max(1, total_n), 3),
                })
            durations.sort(key=lambda x: x["interval_samples"], reverse=True)

            out: dict[str, Any] = {
                "window_sec": round(window_sec, 3),
                "process_cpu_pct": process_cpu_pct,
                "pid": os.getpid(),
                "counts": counts[:max(1, int(top_n))],
                "durations": durations[:max(1, int(top_n))],
                "now_ts": now,
            }

            self._last_cpu_sample_ts = now
            self._last_cpu_sample_proc_sec = cpu_now_sec

            if reset:
                self._counts_interval.clear()
                self._dur_interval_ms.clear()
                self._dur_interval_n.clear()
                self._last_reset_ts = now

            return out


runtime_metrics = RuntimeMetrics()
