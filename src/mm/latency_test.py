"""Latency test for Polymarket CLOB API endpoints.

Usage:
    python src/mm/latency_test.py
    python src/mm/latency_test.py --count 50
    python src/mm/latency_test.py --json
"""
from __future__ import annotations

import argparse
import json
import math
import sys
import time
from dataclasses import dataclass

import httpx


ENDPOINTS = (
    "https://clob.polymarket.com/time",
    "https://clob.polymarket.com/markets?limit=1",
)


@dataclass
class LatencyResult:
    endpoint: str
    rtt_ms: float
    status_code: int = 0
    error: str = ""


class LatencySummary:
    def __init__(self, results: list[LatencyResult]):
        self.results = results
        self.successes = [r for r in results if not r.error and r.status_code > 0]
        self._rtt_values = sorted(r.rtt_ms for r in self.successes)
        self.errors = [r.error for r in results if r.error]

    @property
    def request_count(self) -> int:
        return len(self.results)

    @property
    def success_count(self) -> int:
        return len(self.successes)

    @property
    def success_rate(self) -> float:
        if self.request_count == 0:
            return 0.0
        return (100.0 * self.success_count) / self.request_count

    @property
    def min_ms(self) -> float | None:
        if not self._rtt_values:
            return None
        return self._rtt_values[0]

    @property
    def max_ms(self) -> float | None:
        if not self._rtt_values:
            return None
        return self._rtt_values[-1]

    @property
    def mean_ms(self) -> float | None:
        if not self._rtt_values:
            return None
        return sum(self._rtt_values) / len(self._rtt_values)

    @property
    def median_ms(self) -> float | None:
        if not self._rtt_values:
            return None
        n = len(self._rtt_values)
        mid = n // 2
        if n % 2 == 1:
            return self._rtt_values[mid]
        return (self._rtt_values[mid - 1] + self._rtt_values[mid]) / 2.0

    @property
    def p95_ms(self) -> float | None:
        return self._percentile(95.0)

    @property
    def p99_ms(self) -> float | None:
        return self._percentile(99.0)

    def _percentile(self, percentile: float) -> float | None:
        values = self._rtt_values
        if not values:
            return None
        if len(values) == 1:
            return values[0]

        rank = (percentile / 100.0) * (len(values) - 1)
        lower_index = int(math.floor(rank))
        upper_index = int(math.ceil(rank))

        if lower_index == upper_index:
            return values[lower_index]

        lower_value = values[lower_index]
        upper_value = values[upper_index]
        weight = rank - lower_index
        return lower_value + (upper_value - lower_value) * weight

    def to_dict(self) -> dict[str, float | int | list[str] | None]:
        return {
            "request_count": self.request_count,
            "success_count": self.success_count,
            "success_rate": self.success_rate,
            "min_ms": self.min_ms,
            "max_ms": self.max_ms,
            "mean_ms": self.mean_ms,
            "median_ms": self.median_ms,
            "p95_ms": self.p95_ms,
            "p99_ms": self.p99_ms,
            "errors": self.errors,
        }


def test_http_endpoints(count: int) -> dict[str, LatencySummary]:
    summaries: dict[str, LatencySummary] = {}
    timeout = httpx.Timeout(10.0)

    with httpx.Client(timeout=timeout) as client:
        for endpoint in ENDPOINTS:
            results: list[LatencyResult] = []
            for _ in range(count):
                start = time.perf_counter()
                try:
                    response = client.get(endpoint)
                    elapsed_ms = (time.perf_counter() - start) * 1000.0
                    results.append(
                        LatencyResult(
                            endpoint=endpoint,
                            rtt_ms=elapsed_ms,
                            status_code=response.status_code,
                        )
                    )
                except Exception as exc:
                    elapsed_ms = (time.perf_counter() - start) * 1000.0
                    results.append(
                        LatencyResult(
                            endpoint=endpoint,
                            rtt_ms=elapsed_ms,
                            error=str(exc),
                        )
                    )
            summaries[endpoint] = LatencySummary(results)

    return summaries


def _format_value(value: float | None) -> str:
    if value is None:
        return "n/a"
    return f"{value:.2f}"


def print_table(summaries: dict[str, LatencySummary]) -> None:
    headers = [
        "Endpoint",
        "Requests",
        "Success",
        "Success%",
        "Min(ms)",
        "Max(ms)",
        "Mean(ms)",
        "Median(ms)",
        "P95(ms)",
        "P99(ms)",
    ]
    rows = []
    for endpoint, summary in summaries.items():
        rows.append(
            [
                endpoint,
                str(summary.request_count),
                str(summary.success_count),
                f"{summary.success_rate:.1f}",
                _format_value(summary.min_ms),
                _format_value(summary.max_ms),
                _format_value(summary.mean_ms),
                _format_value(summary.median_ms),
                _format_value(summary.p95_ms),
                _format_value(summary.p99_ms),
            ]
        )

    widths = []
    for idx, header in enumerate(headers):
        width = len(header)
        for row in rows:
            width = max(width, len(row[idx]))
        widths.append(width)

    header_line = "  ".join(header.ljust(widths[idx]) for idx, header in enumerate(headers))
    separator = "  ".join("-" * widths[idx] for idx in range(len(headers)))
    print(header_line)
    print(separator)
    for row in rows:
        print("  ".join(value.ljust(widths[idx]) for idx, value in enumerate(row)))


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Measure HTTP round-trip latency to public Polymarket CLOB API endpoints."
    )
    parser.add_argument(
        "--count",
        type=int,
        default=10,
        help="Number of requests per endpoint (default: 10).",
    )
    parser.add_argument(
        "--json",
        action="store_true",
        dest="json_output",
        help="Print output as JSON instead of a table.",
    )
    args = parser.parse_args()

    if args.count <= 0:
        parser.error("--count must be a positive integer")

    summaries = test_http_endpoints(args.count)
    payload = {endpoint: summary.to_dict() for endpoint, summary in summaries.items()}

    if args.json_output:
        json.dump(payload, sys.stdout, indent=2)
        sys.stdout.write("\n")
        return 0

    print_table(summaries)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
