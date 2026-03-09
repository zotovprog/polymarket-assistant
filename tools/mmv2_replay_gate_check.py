#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


def _clamp_ratio(value: Any) -> float:
    try:
        return max(0.0, min(1.0, float(value or 0.0)))
    except Exception:
        return 0.0


def _load_snapshots(path: Path) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for line in path.read_text(encoding="utf-8").splitlines():
        raw_line = line.strip()
        if not raw_line:
            continue
        payload = json.loads(raw_line)
        if isinstance(payload, dict) and isinstance(payload.get("state"), dict):
            rows.append(payload["state"])
        else:
            rows.append(payload)
    return rows


def _resolve_input_dir(audit_root: Path, explicit: str | None) -> Path:
    if explicit:
        target = Path(explicit).expanduser().resolve()
        if not target.exists():
            raise RuntimeError(f"input directory not found: {target}")
        return target
    candidates = [
        p
        for p in audit_root.iterdir()
        if p.is_dir() and p.name.startswith("mongo-last-run-") and (p / "snapshots.jsonl").exists()
    ]
    if not candidates:
        raise RuntimeError(f"no mongo-last-run-* directories with snapshots.jsonl in {audit_root}")
    return max(candidates, key=lambda p: p.stat().st_mtime)


def main() -> int:
    parser = argparse.ArgumentParser(description="Deterministic replay gate checker for latest mongo run artifacts")
    parser.add_argument("--audit-root", default="audit")
    parser.add_argument("--input-dir", default=None)
    parser.add_argument("--output-root", default="audit/replay-check")
    parser.add_argument("--mm-effective-min", type=float, default=0.65)
    parser.add_argument("--unwind-max", type=float, default=0.35)
    parser.add_argument("--emergency-max", type=float, default=0.10)
    parser.add_argument("--quote-none-streak-max", type=int, default=3)
    args = parser.parse_args()

    repo_root = Path(__file__).resolve().parents[1]
    audit_root = (repo_root / args.audit_root).resolve()
    input_dir = _resolve_input_dir(audit_root, args.input_dir)
    snapshots_path = input_dir / "snapshots.jsonl"
    rows = _load_snapshots(snapshots_path)
    if not rows:
        raise RuntimeError(f"no snapshots in {snapshots_path}")

    true_drift_any = False
    hard_halted_any = False
    outside_samples = 0
    outside_lifecycle = Counter()
    min_mm_effective_ratio_60s_outside = float("inf")
    max_unwind_ratio_60s_outside = 0.0
    max_emergency_unwind_ratio_60s_outside = 0.0
    max_quote_none_streak_outside = 0
    quote_none_streak = 0
    unwind_target_lower_count = 0
    unwind_outside_count = 0

    for state in rows:
        if not isinstance(state, dict):
            continue
        health = state.get("health") or {}
        risk = state.get("risk") or {}
        analytics = state.get("analytics") or {}
        market = state.get("market") or {}
        cfg = state.get("config") or {}
        lifecycle = str(state.get("lifecycle") or "")
        hard_mode = str(risk.get("hard_mode") or "")

        if bool(health.get("true_drift")):
            true_drift_any = True
        if lifecycle == "halted" or hard_mode == "halted":
            hard_halted_any = True

        time_left_sec = float(market.get("time_left_sec") or 0.0)
        unwind_window_sec = float(cfg.get("unwind_window_sec") or 90.0)
        if time_left_sec <= unwind_window_sec:
            continue

        outside_samples += 1
        outside_lifecycle[lifecycle] += 1
        target_soft_mode = str(risk.get("target_soft_mode") or "")
        if lifecycle == "unwind":
            unwind_outside_count += 1
            if target_soft_mode != "unwind":
                unwind_target_lower_count += 1

        mm_effective_ratio_60s = analytics.get("mm_effective_ratio_60s")
        if mm_effective_ratio_60s is None:
            mm_effective_ratio_60s = (
                _clamp_ratio(analytics.get("quoting_ratio_60s"))
                + _clamp_ratio(analytics.get("inventory_skewed_ratio_60s"))
                + _clamp_ratio(analytics.get("defensive_ratio_60s"))
            )
        mm_effective_ratio_60s = _clamp_ratio(mm_effective_ratio_60s)
        unwind_ratio_60s = _clamp_ratio(analytics.get("unwind_ratio_60s"))
        emergency_ratio_60s = _clamp_ratio(analytics.get("emergency_unwind_ratio_60s"))

        min_mm_effective_ratio_60s_outside = min(min_mm_effective_ratio_60s_outside, mm_effective_ratio_60s)
        max_unwind_ratio_60s_outside = max(max_unwind_ratio_60s_outside, unwind_ratio_60s)
        max_emergency_unwind_ratio_60s_outside = max(max_emergency_unwind_ratio_60s_outside, emergency_ratio_60s)

        quote_balance_state = str(analytics.get("quote_balance_state") or state.get("quote_balance_state") or "")
        if quote_balance_state == "none":
            quote_none_streak += 1
            max_quote_none_streak_outside = max(max_quote_none_streak_outside, quote_none_streak)
        else:
            quote_none_streak = 0

    failed_criteria: list[str] = []
    if true_drift_any:
        failed_criteria.append("true_drift_present")
    if hard_halted_any:
        failed_criteria.append("halted_present")
    if outside_samples <= 0:
        failed_criteria.append("no_samples_outside_near_expiry")
    else:
        if min_mm_effective_ratio_60s_outside < float(args.mm_effective_min):
            failed_criteria.append(
                f"mm_effective_ratio_60s_below_{float(args.mm_effective_min):.2f} "
                f"(min={min_mm_effective_ratio_60s_outside:.4f})"
            )
        if max_unwind_ratio_60s_outside > float(args.unwind_max):
            failed_criteria.append(
                f"unwind_ratio_60s_above_{float(args.unwind_max):.2f} "
                f"(max={max_unwind_ratio_60s_outside:.4f})"
            )
        if max_emergency_unwind_ratio_60s_outside > float(args.emergency_max):
            failed_criteria.append(
                f"emergency_unwind_ratio_60s_above_{float(args.emergency_max):.2f} "
                f"(max={max_emergency_unwind_ratio_60s_outside:.4f})"
            )
        if max_quote_none_streak_outside > int(args.quote_none_streak_max):
            failed_criteria.append(
                f"quote_balance_none_streak_above_{int(args.quote_none_streak_max)} "
                f"(max={max_quote_none_streak_outside})"
            )

    outside_unwind_lower_ratio = (
        float(unwind_target_lower_count) / float(outside_samples) if outside_samples > 0 else 0.0
    )
    unwind_target_lower_ratio_within_unwind = (
        float(unwind_target_lower_count) / float(unwind_outside_count) if unwind_outside_count > 0 else 0.0
    )
    outside_mode_ratios = {}
    if outside_samples > 0:
        for mode, count in outside_lifecycle.items():
            outside_mode_ratios[mode] = float(count) / float(outside_samples)
    mm_effective_share_outside = (
        outside_mode_ratios.get("quoting", 0.0)
        + outside_mode_ratios.get("inventory_skewed", 0.0)
        + outside_mode_ratios.get("defensive", 0.0)
    )

    gate_verdict = "go" if not failed_criteria else "no_go"
    ts = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    out_dir = (repo_root / args.output_root / ts).resolve()
    out_dir.mkdir(parents=True, exist_ok=True)

    summary = {
        "gate_verdict": gate_verdict,
        "failed_criteria": failed_criteria,
        "input_dir": str(input_dir),
        "samples": len(rows),
        "outside_near_expiry_samples": outside_samples,
        "outside_mode_ratios": outside_mode_ratios,
        "mm_effective_share_outside": mm_effective_share_outside,
        "min_mm_effective_ratio_60s_outside": (
            0.0 if min_mm_effective_ratio_60s_outside == float("inf") else min_mm_effective_ratio_60s_outside
        ),
        "max_unwind_ratio_60s_outside": max_unwind_ratio_60s_outside,
        "max_emergency_unwind_ratio_60s_outside": max_emergency_unwind_ratio_60s_outside,
        "max_quote_none_streak_outside": max_quote_none_streak_outside,
        "unwind_when_target_lower": {
            "count": unwind_target_lower_count,
            "ratio_outside": outside_unwind_lower_ratio,
            "ratio_within_unwind": unwind_target_lower_ratio_within_unwind,
        },
        "true_drift_present": true_drift_any,
        "halted_present": hard_halted_any,
        "output_dir": str(out_dir),
    }
    (out_dir / "summary.json").write_text(json.dumps(summary, indent=2), encoding="utf-8")
    print(json.dumps(summary, indent=2))
    return 0 if gate_verdict == "go" else 2


if __name__ == "__main__":
    raise SystemExit(main())
