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


def _http_json(base_url: str, path: str, key: str, *, method: str = "GET", payload: dict[str, Any] | None = None) -> dict[str, Any]:
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
        with urlopen(req, timeout=15) as resp:
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
) -> dict[str, Any]:
    deadline = time.time() + max(1.0, float(timeout_sec))
    last_error = ""
    while time.time() < deadline:
        try:
            return _http_json(base_url, "/api/mmv2/start", key, method="POST", payload=payload)
        except Exception as exc:  # noqa: BLE001
            text = str(exc)
            last_error = text
            # Normal transient near-close case: wait for the next window.
            if "too close to close" in text or "HTTP 409" in text:
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
    parser.add_argument("--duration-sec", type=int, default=1800)
    parser.add_argument("--poll-sec", type=float, default=5.0)
    parser.add_argument("--start-retry-timeout-sec", type=float, default=180.0)
    parser.add_argument("--epsilon", type=float, default=1e-3)
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
    samples = 0
    max_fill_count = 0
    restart_count = 0
    baseline_portfolio = None
    prev_portfolio_mark = None
    prev_session_pnl = None
    prev_fill_count = None

    # Ensure stale run is not hanging.
    try:
        stop_resp = _http_json(args.base_url, "/api/mmv2/stop", key, method="POST")
        (out_dir / "pre_stop.json").write_text(json.dumps(stop_resp, indent=2), encoding="utf-8")
    except Exception as exc:  # noqa: BLE001
        (out_dir / "pre_stop_error.txt").write_text(str(exc), encoding="utf-8")

    start_payload = {
        "coin": args.coin,
        "timeframe": args.timeframe,
        "paper_mode": True,
        "initial_usdc": float(args.budget),
        "dev": True,
    }
    start_resp = _start_with_retry(
        base_url=args.base_url,
        key=key,
        payload=start_payload,
        timeout_sec=float(args.start_retry_timeout_sec),
        sleep_sec=max(1.0, float(args.poll_sec)),
    )
    (out_dir / "start.json").write_text(json.dumps(start_resp, indent=2), encoding="utf-8")

    started_at = time.time()

    try:
        while (time.time() - started_at) < float(args.duration_sec):
            now = time.time()
            state = _http_json(args.base_url, "/api/mmv2/state", key, method="GET")
            fills = _http_json(args.base_url, "/api/mmv2/fills?limit=100", key, method="GET")
            logs = _http_json(args.base_url, "/api/logs?limit=200", key, method="GET")

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

            if lifecycle == "halted" or hard_mode == "halted" or not is_running:
                failures.append(
                    f"runtime halted sample={samples}: lifecycle={lifecycle} hard_mode={hard_mode} "
                    f"reason={risk.get('reason') or ''}"
                )
                break

            # Window ended: roll to next one automatically so long runs can
            # really cover multiple windows.
            if args.auto_roll_expired and is_running and lifecycle == "expired":
                try:
                    _http_json(args.base_url, "/api/mmv2/stop", key, method="POST")
                    _start_with_retry(
                        base_url=args.base_url,
                        key=key,
                        payload=start_payload,
                        timeout_sec=float(args.start_retry_timeout_sec),
                        sleep_sec=max(1.0, float(args.poll_sec)),
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
                    failures.append(f"auto-roll failed sample={samples}: {exc}")

            if bool(health.get("true_drift")):
                failures.append(f"true_drift at sample={samples}")

            fill_count = int(analytics.get("fill_count") or 0)
            session_pnl = float(analytics.get("session_pnl") or 0.0)
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

            prev_portfolio_mark = portfolio_mark
            prev_session_pnl = session_pnl
            prev_fill_count = fill_count
            max_fill_count = max(max_fill_count, fill_count)
            time.sleep(max(0.1, float(args.poll_sec)))
    finally:
        if args.stop_at_end:
            try:
                stop_resp = _http_json(args.base_url, "/api/mmv2/stop", key, method="POST")
                (out_dir / "stop.json").write_text(json.dumps(stop_resp, indent=2), encoding="utf-8")
            except Exception as exc:  # noqa: BLE001
                (out_dir / "stop_error.txt").write_text(str(exc), encoding="utf-8")

    summary = {
        "ok": len(failures) == 0,
        "samples": samples,
        "max_fill_count": max_fill_count,
        "restart_count": restart_count,
        "failures": failures,
        "params": {
            "base_url": args.base_url,
            "coin": args.coin,
            "timeframe": args.timeframe,
            "budget": args.budget,
            "duration_sec": args.duration_sec,
            "poll_sec": args.poll_sec,
            "epsilon": args.epsilon,
            "auto_roll_expired": bool(args.auto_roll_expired),
            "start_retry_timeout_sec": float(args.start_retry_timeout_sec),
        },
        "output_dir": str(out_dir),
    }
    (out_dir / "summary.json").write_text(json.dumps(summary, indent=2), encoding="utf-8")

    print(json.dumps(summary, indent=2))
    return 0 if summary["ok"] else 2


if __name__ == "__main__":
    raise SystemExit(main())
