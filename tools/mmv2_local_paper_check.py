#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import os
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen


def _read_key(explicit: str | None, repo_root: Path) -> str:
    if explicit:
        return explicit.strip()
    env_key = os.environ.get("PM_WEB_ACCESS_KEY", "").strip()
    if env_key:
        return env_key
    key_file = repo_root / ".web_access_key"
    if key_file.exists():
        return key_file.read_text(encoding="utf-8").strip()
    raise RuntimeError("PM_WEB_ACCESS_KEY is required (env, --key, or .web_access_key)")


def _http_json(
    base_url: str,
    path: str,
    key: str,
    *,
    method: str = "GET",
    payload: dict[str, Any] | None = None,
    timeout_sec: float = 15.0,
) -> dict[str, Any]:
    data = None
    headers = {
        "x-access-key": key,
        "Accept": "application/json",
    }
    if payload is not None:
        data = json.dumps(payload).encode("utf-8")
        headers["Content-Type"] = "application/json"
    req = Request(
        url=f"{base_url.rstrip('/')}{path}",
        data=data,
        headers=headers,
        method=method,
    )
    try:
        with urlopen(req, timeout=max(1.0, float(timeout_sec))) as resp:
            raw = resp.read().decode("utf-8")
            if not raw:
                return {}
            return json.loads(raw)
    except HTTPError as exc:
        body = exc.read().decode("utf-8", errors="replace")
        raise RuntimeError(f"HTTP {exc.code} {method} {path}: {body}") from exc
    except URLError as exc:
        raise RuntimeError(f"Transport {method} {path}: {exc}") from exc


def _append_jsonl(path: Path, payload: dict[str, Any]) -> None:
    with path.open("a", encoding="utf-8") as fh:
        fh.write(json.dumps(payload, ensure_ascii=True, separators=(",", ":")) + "\n")


def _start_with_retry(
    *,
    base_url: str,
    key: str,
    payload: dict[str, Any],
    timeout_sec: float,
    sleep_sec: float,
    http_timeout_sec: float,
) -> dict[str, Any]:
    deadline = time.time() + max(1.0, float(timeout_sec))
    last_error = ""
    while time.time() < deadline:
        try:
            return _http_json(
                base_url,
                "/api/mmv2/start",
                key,
                method="POST",
                payload=payload,
                timeout_sec=http_timeout_sec,
            )
        except Exception as exc:  # noqa: BLE001
            text = str(exc)
            last_error = text
            # Normal transient start cases: near-close, strike fetch timeout/fail,
            # temporary network stalls while feeds reconnect after expiry.
            if any(
                marker in text.lower()
                for marker in (
                    "too close to close",
                    "http 409",
                    "http 503",
                    "valid strike",
                    "pm tokens not found",
                    "timed out",
                    "timeout",
                    "strike fetch failed",
                    "watch mode",
                    "transport",
                    "temporarily unavailable",
                )
            ):
                time.sleep(max(0.5, float(sleep_sec)))
                continue
            raise
    raise RuntimeError(f"Unable to start mmv2 within retry timeout: {last_error}")


def main() -> int:
    parser = argparse.ArgumentParser(description="Local-first MMV2 paper verification runner")
    parser.add_argument("--base-url", default="http://127.0.0.1:8000")
    parser.add_argument("--key", default=None)
    parser.add_argument("--coin", default="BTC")
    parser.add_argument("--timeframe", default="15m")
    parser.add_argument("--budget", type=float, default=50.0)
    mode = parser.add_mutually_exclusive_group()
    mode.add_argument("--paper-mode", dest="paper_mode", action="store_true", default=True)
    mode.add_argument("--live-mode", dest="paper_mode", action="store_false")
    parser.add_argument("--duration-sec", type=int, default=1800)
    parser.add_argument("--poll-sec", type=float, default=5.0)
    parser.add_argument("--start-retry-timeout-sec", type=float, default=180.0)
    parser.add_argument("--http-timeout-sec", type=float, default=30.0)
    parser.add_argument("--epsilon", type=float, default=1e-3)
    parser.add_argument("--paper-max-loss-usd", type=float, default=1.0)
    parser.add_argument("--live-max-loss-usd", type=float, default=3.0)
    parser.add_argument("--stop-at-end", action="store_true")
    parser.add_argument("--output-root", default="audit/local-paper")
    parser.add_argument("--auto-roll-expired", action="store_true", default=True)
    parser.add_argument("--no-auto-roll-expired", dest="auto_roll_expired", action="store_false")
    args = parser.parse_args()

    repo_root = Path(__file__).resolve().parents[1]
    key = _read_key(args.key, repo_root)

    ts = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    out_dir = repo_root / args.output_root / ts
    out_dir.mkdir(parents=True, exist_ok=True)

    state_jsonl = out_dir / "state.jsonl"
    fills_jsonl = out_dir / "fills.jsonl"
    logs_jsonl = out_dir / "logs.jsonl"

    failures: list[str] = []
    failed_criteria: list[str] = []
    samples = 0
    max_fill_count = 0
    restart_count = 0
    baseline_portfolio = None
    prev_portfolio_mark = None
    prev_session_pnl = None
    prev_fill_count = None
    terminal_expected = False

    # Gate metrics
    outside_near_expiry_samples = 0
    min_mm_effective_ratio_60s_outside = float("inf")
    max_unwind_ratio_60s_outside = 0.0
    max_emergency_unwind_ratio_60s_outside = 0.0
    max_quote_none_streak_outside = 0
    quote_none_streak_outside = 0
    final_pnl_usd = 0.0
    window_final_pnls_by_start: dict[float, float] = {}

    # Ensure stale run is not hanging.
    try:
        stop_resp = _http_json(
            args.base_url,
            "/api/mmv2/stop",
            key,
            method="POST",
            timeout_sec=float(args.http_timeout_sec),
        )
        (out_dir / "pre_stop.json").write_text(json.dumps(stop_resp, indent=2), encoding="utf-8")
    except Exception as exc:  # noqa: BLE001
        (out_dir / "pre_stop_error.txt").write_text(str(exc), encoding="utf-8")

    start_payload = {
        "coin": args.coin,
        "timeframe": args.timeframe,
        "paper_mode": bool(args.paper_mode),
        "initial_usdc": float(args.budget),
        "dev": True,
    }
    start_resp = _start_with_retry(
        base_url=args.base_url,
        key=key,
        payload=start_payload,
        timeout_sec=float(args.start_retry_timeout_sec),
        sleep_sec=max(1.0, float(args.poll_sec)),
        http_timeout_sec=float(args.http_timeout_sec),
    )
    (out_dir / "start.json").write_text(json.dumps(start_resp, indent=2), encoding="utf-8")

    started_at = time.time()
    target_loss_floor = -abs(float(args.paper_max_loss_usd if args.paper_mode else args.live_max_loss_usd))

    def _ratio(value: Any) -> float:
        try:
            return max(0.0, min(1.0, float(value or 0.0)))
        except Exception:
            return 0.0

    try:
        while (time.time() - started_at) < float(args.duration_sec):
            now = time.time()
            state = _http_json(
                args.base_url,
                "/api/mmv2/state",
                key,
                method="GET",
                timeout_sec=float(args.http_timeout_sec),
            )
            fills = _http_json(
                args.base_url,
                "/api/mmv2/fills?limit=100",
                key,
                method="GET",
                timeout_sec=float(args.http_timeout_sec),
            )
            logs = _http_json(
                args.base_url,
                "/api/logs?limit=200",
                key,
                method="GET",
                timeout_sec=float(args.http_timeout_sec),
            )

            _append_jsonl(state_jsonl, {"ts": now, "state": state})
            _append_jsonl(fills_jsonl, {"ts": now, "fills": fills})
            _append_jsonl(logs_jsonl, {"ts": now, "logs": logs})

            samples += 1
            health = state.get("health") or {}
            risk = state.get("risk") or {}
            analytics = state.get("analytics") or {}
            lifecycle = str(state.get("lifecycle") or "")
            is_running = bool(state.get("is_running"))
            hard_mode = str(risk.get("hard_mode") or "")
            market = state.get("market") or {}
            cfg = state.get("config") or {}
            time_left_sec = float(market.get("time_left_sec") or 0.0)
            unwind_window_sec = float(cfg.get("unwind_window_sec") or 90.0)
            outside_near_expiry = time_left_sec > unwind_window_sec

            if lifecycle == "halted" or hard_mode == "halted":
                failures.append(
                    f"runtime halted sample={samples}: lifecycle={lifecycle} hard_mode={hard_mode} "
                    f"reason={risk.get('reason') or ''}"
                )
                break
            if not is_running:
                # Graceful end-of-window terminal in paper/live checks.
                if lifecycle == "expired" and hard_mode == "none":
                    terminal_expected = True
                    break
                if args.auto_roll_expired:
                    try:
                        _start_with_retry(
                            base_url=args.base_url,
                            key=key,
                            payload=start_payload,
                            timeout_sec=float(args.start_retry_timeout_sec),
                            sleep_sec=max(1.0, float(args.poll_sec)),
                            http_timeout_sec=float(args.http_timeout_sec),
                        )
                        restart_count += 1
                        baseline_portfolio = None
                        prev_portfolio_mark = None
                        prev_session_pnl = None
                        prev_fill_count = None
                        time.sleep(max(0.5, min(2.0, float(args.poll_sec))))
                        continue
                    except Exception as exc:  # noqa: BLE001
                        failures.append(f"runtime not running and restart failed sample={samples}: {exc}")
                failures.append(
                    f"runtime halted sample={samples}: lifecycle={lifecycle} hard_mode={hard_mode} "
                    f"reason={risk.get('reason') or ''}"
                )
                break

            # Window ended: roll to next one automatically so long runs can
            # really cover multiple windows.
            if args.auto_roll_expired and is_running and lifecycle == "expired":
                try:
                    _http_json(
                        args.base_url,
                        "/api/mmv2/stop",
                        key,
                        method="POST",
                        timeout_sec=float(args.http_timeout_sec),
                    )
                    _start_with_retry(
                        base_url=args.base_url,
                        key=key,
                        payload=start_payload,
                        timeout_sec=float(args.start_retry_timeout_sec),
                        sleep_sec=max(1.0, float(args.poll_sec)),
                        http_timeout_sec=float(args.http_timeout_sec),
                    )
                    restart_count += 1
                    # New session baseline will be recomputed from next sample.
                    baseline_portfolio = None
                    prev_portfolio_mark = None
                    prev_session_pnl = None
                    prev_fill_count = None
                    time.sleep(max(0.5, min(2.0, float(args.poll_sec))))
                    continue
                except Exception as exc:  # noqa: BLE001
                    # Treat first auto-roll miss as transient and let the regular
                    # restart branch handle recovery on the next samples.
                    _append_jsonl(
                        logs_jsonl,
                        {
                            "ts": now,
                            "local_check_warning": f"auto-roll transient failure sample={samples}: {exc}",
                        },
                    )
            elif lifecycle == "expired" and hard_mode == "none" and not args.auto_roll_expired:
                terminal_expected = True
                break

            if bool(health.get("true_drift")):
                failures.append(f"true_drift at sample={samples}")
            if hard_mode == "halted":
                failures.append(f"hard_mode_halted at sample={samples}")

            fill_count = int(analytics.get("fill_count") or 0)
            session_pnl = float(
                analytics.get("session_pnl_equity_usd")
                if analytics.get("session_pnl_equity_usd") is not None
                else (analytics.get("session_pnl") or 0.0)
            )
            final_pnl_usd = session_pnl
            started_key = float(state.get("started_at") or 0.0)
            if started_key > 0.0:
                window_final_pnls_by_start[started_key] = session_pnl
            portfolio_mark = analytics.get("portfolio_mark_value_usd")
            if portfolio_mark is not None:
                portfolio_mark = float(portfolio_mark)
                if baseline_portfolio is None:
                    baseline_portfolio = portfolio_mark - session_pnl
                expected_pnl = portfolio_mark - float(baseline_portfolio)
                if abs(expected_pnl - session_pnl) > float(args.epsilon):
                    failures.append(
                        f"pnl mismatch sample={samples}: expected={expected_pnl:.6f} actual={session_pnl:.6f}"
                    )
                if (
                    prev_portfolio_mark is not None
                    and prev_session_pnl is not None
                    and prev_fill_count is not None
                    and fill_count == prev_fill_count
                ):
                    delta_portfolio = portfolio_mark - prev_portfolio_mark
                    delta_pnl = session_pnl - prev_session_pnl
                    if abs(delta_pnl - delta_portfolio) > float(args.epsilon):
                        failures.append(
                            f"pnl delta mismatch without new fills sample={samples}: "
                            f"delta_pnl={delta_pnl:.6f} delta_portfolio={delta_portfolio:.6f}"
                        )

            if outside_near_expiry:
                outside_near_expiry_samples += 1
                mm_effective_ratio_60s = _ratio(analytics.get("mm_effective_ratio_60s"))
                unwind_ratio_60s = _ratio(analytics.get("unwind_ratio_60s"))
                emergency_unwind_ratio_60s = _ratio(analytics.get("emergency_unwind_ratio_60s"))
                min_mm_effective_ratio_60s_outside = min(min_mm_effective_ratio_60s_outside, mm_effective_ratio_60s)
                max_unwind_ratio_60s_outside = max(max_unwind_ratio_60s_outside, unwind_ratio_60s)
                max_emergency_unwind_ratio_60s_outside = max(
                    max_emergency_unwind_ratio_60s_outside,
                    emergency_unwind_ratio_60s,
                )
                quote_balance_state = str(analytics.get("quote_balance_state") or state.get("quote_balance_state") or "")
                if quote_balance_state == "none":
                    quote_none_streak_outside += 1
                    max_quote_none_streak_outside = max(max_quote_none_streak_outside, quote_none_streak_outside)
                else:
                    quote_none_streak_outside = 0

            prev_portfolio_mark = portfolio_mark
            prev_session_pnl = session_pnl
            prev_fill_count = fill_count
            max_fill_count = max(max_fill_count, fill_count)
            time.sleep(max(0.1, float(args.poll_sec)))
    finally:
        if args.stop_at_end:
            try:
                stop_resp = _http_json(
                    args.base_url,
                    "/api/mmv2/stop",
                    key,
                    method="POST",
                    timeout_sec=float(args.http_timeout_sec),
                )
                (out_dir / "stop.json").write_text(json.dumps(stop_resp, indent=2), encoding="utf-8")
            except Exception as exc:  # noqa: BLE001
                (out_dir / "stop_error.txt").write_text(str(exc), encoding="utf-8")

    window_final_pnls = [
        float(window_final_pnls_by_start[k]) for k in sorted(window_final_pnls_by_start.keys())
    ]
    if not window_final_pnls:
        window_final_pnls = [float(final_pnl_usd)]

    if any(float(pnl) < target_loss_floor for pnl in window_final_pnls):
        failed_criteria.append(
            f"window_final_pnl_below_floor (floor={target_loss_floor:.2f}, values={window_final_pnls})"
        )
    if failures:
        failed_criteria.append("runtime_failures_present")
    if outside_near_expiry_samples <= 0:
        failed_criteria.append("no_samples_outside_near_expiry")
    else:
        if min_mm_effective_ratio_60s_outside < 0.65:
            failed_criteria.append(
                f"mm_effective_ratio_60s_below_0.65 (min={min_mm_effective_ratio_60s_outside:.4f})"
            )
        if max_unwind_ratio_60s_outside > 0.35:
            failed_criteria.append(
                f"unwind_ratio_60s_above_0.35 (max={max_unwind_ratio_60s_outside:.4f})"
            )
        if max_emergency_unwind_ratio_60s_outside > 0.10:
            failed_criteria.append(
                f"emergency_unwind_ratio_60s_above_0.10 (max={max_emergency_unwind_ratio_60s_outside:.4f})"
            )
        if max_quote_none_streak_outside > 3:
            failed_criteria.append(
                f"quote_balance_none_streak_above_3 (max={max_quote_none_streak_outside})"
            )

    gate_verdict = "go" if not failed_criteria else "no_go"
    summary = {
        "ok": gate_verdict == "go",
        "gate_verdict": gate_verdict,
        "failed_criteria": failed_criteria,
        "terminal_expected": bool(terminal_expected),
        "samples": samples,
        "max_fill_count": max_fill_count,
        "restart_count": restart_count,
        "final_pnl_usd": float(final_pnl_usd),
        "window_final_pnls": window_final_pnls,
        "outside_near_expiry": {
            "samples": int(outside_near_expiry_samples),
            "min_mm_effective_ratio_60s": (
                0.0 if min_mm_effective_ratio_60s_outside == float("inf") else float(min_mm_effective_ratio_60s_outside)
            ),
            "max_unwind_ratio_60s": float(max_unwind_ratio_60s_outside),
            "max_emergency_unwind_ratio_60s": float(max_emergency_unwind_ratio_60s_outside),
            "max_quote_balance_none_streak": int(max_quote_none_streak_outside),
        },
        "failures": failures,
        "params": {
            "base_url": args.base_url,
            "coin": args.coin,
            "timeframe": args.timeframe,
            "budget": args.budget,
            "paper_mode": bool(args.paper_mode),
            "duration_sec": args.duration_sec,
            "poll_sec": args.poll_sec,
            "epsilon": args.epsilon,
            "paper_max_loss_usd": float(args.paper_max_loss_usd),
            "live_max_loss_usd": float(args.live_max_loss_usd),
            "auto_roll_expired": bool(args.auto_roll_expired),
            "start_retry_timeout_sec": float(args.start_retry_timeout_sec),
            "http_timeout_sec": float(args.http_timeout_sec),
        },
        "output_dir": str(out_dir),
    }
    (out_dir / "summary.json").write_text(json.dumps(summary, indent=2), encoding="utf-8")

    print(json.dumps(summary, indent=2))
    return 0 if summary["ok"] else 2


if __name__ == "__main__":
    raise SystemExit(main())
