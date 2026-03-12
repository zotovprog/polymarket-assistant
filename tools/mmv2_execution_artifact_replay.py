#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import re
import sys
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from tools.mmv2_replay_gate_check import (  # type: ignore
    _clamp_ratio,
    _derive_failure_bucket,
    _pick_primary_blocker,
    _resolve_input_dir,
    _resolve_snapshots_path,
)


def _load_snapshots(path: Path) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for line in path.read_text(encoding="utf-8").splitlines():
        raw_line = line.strip()
        if not raw_line:
            continue
        payload = json.loads(raw_line)
        if isinstance(payload, dict) and isinstance(payload.get("state"), dict):
            rows.append(payload["state"])
        elif isinstance(payload, dict):
            rows.append(payload)
    return rows


def _load_jsonl_optional(path: Path) -> list[dict[str, Any]]:
    if not path.exists():
        return []
    rows: list[dict[str, Any]] = []
    for line in path.read_text(encoding="utf-8").splitlines():
        raw_line = line.strip()
        if not raw_line:
            continue
        try:
            payload = json.loads(raw_line)
        except Exception:
            payload = {"message": raw_line}
        if isinstance(payload, dict):
            rows.append(payload)
    return rows


def _resolve_optional_jsonl(input_dir: Path, *names: str) -> Path | None:
    for name in names:
        candidate = input_dir / name
        if candidate.exists():
            return candidate
    return None


def _log_message(row: dict[str, Any]) -> str:
    for key in ("message", "msg", "text", "event", "detail"):
        value = row.get(key)
        if isinstance(value, str) and value.strip():
            return value.strip()
    return ""


def _build_summary(args: argparse.Namespace) -> dict[str, Any]:
    input_dir = _resolve_input_dir((REPO_ROOT / args.audit_root).resolve(), args.input_dir)
    snapshots_path = _resolve_snapshots_path(input_dir)
    rows = _load_snapshots(snapshots_path)
    if not rows:
        return {
            "ok": False,
            "gate_verdict": "no_go",
            "failed_criteria": ["no_snapshots_found"],
            "failure_buckets": [],
            "primary_blocker": "",
            "input_dir": str(input_dir),
            "samples": 0,
            "final_pnl_usd": 0.0,
            "aggregate_pnl_usd": 0.0,
            "max_sell_skip_cooldown_streak_outside": 0,
            "max_collateral_warning_streak_outside": 0,
            "max_quote_balance_none_streak": 0,
            "execution_churn_ratio_60s_max": 0.0,
            "outside_mode_ratios": {},
            "terminal_ok": False,
            "execution_paths": [],
            "execution_replay_blocker_hint": "",
        }

    logs_path = _resolve_optional_jsonl(input_dir, "logs.jsonl", "log.jsonl")
    fills_path = _resolve_optional_jsonl(input_dir, "fills.jsonl", "fill.jsonl")
    logs = _load_jsonl_optional(logs_path) if logs_path else []
    fills = _load_jsonl_optional(fills_path) if fills_path else []

    true_drift_any = False
    hard_halted_any = False
    outside_samples = 0
    outside_lifecycle: Counter[str] = Counter()
    failure_bucket_counts: Counter[str] = Counter()
    min_mm_effective_ratio_60s_outside = float("inf")
    max_unwind_ratio_60s_outside = 0.0
    max_emergency_unwind_ratio_60s_outside = 0.0
    max_quote_none_streak = 0
    quote_none_streak = 0
    max_collateral_warning_streak_outside = 0
    max_sell_skip_cooldown_streak_outside = 0
    collateral_warning_streak_outside = 0
    sell_skip_cooldown_streak_outside = 0
    execution_churn_ratio_60s_max = 0.0
    untradeable_tolerated_material_samples_outside = 0
    window_final_pnls_by_start: dict[float, float] = {}
    execution_paths: set[str] = set()
    post_terminal_false_drift = False

    for state in rows:
        if not isinstance(state, dict):
            continue
        analytics = state.get("analytics") or {}
        health = state.get("health") or {}
        risk = state.get("risk") or {}
        runtime = state.get("runtime") or {}
        market = state.get("market") or {}
        inventory = state.get("inventory") or {}
        config = state.get("config") or {}
        lifecycle = str(state.get("lifecycle") or "")
        bucket = _derive_failure_bucket(state)
        if bucket:
            failure_bucket_counts[bucket] += 1
        session_pnl = float(
            analytics.get("session_pnl_equity_usd")
            if analytics.get("session_pnl_equity_usd") is not None
            else analytics.get("session_pnl") or 0.0
        )
        started_key = float(state.get("started_at") or 0.0)
        if started_key > 0.0:
            window_final_pnls_by_start[started_key] = session_pnl
        if bool(health.get("true_drift")):
            true_drift_any = True
        if lifecycle == "halted" or str(risk.get("hard_mode") or "") == "halted":
            hard_halted_any = True
        if bool(analytics.get("sell_churn_hold_up_active") or analytics.get("sell_churn_hold_dn_active")):
            execution_paths.add("sell_churn_hold_mode")
        dual_bid_exception_reason = str(analytics.get("dual_bid_exception_reason") or "")
        if dual_bid_exception_reason == "sell_churn_hold_mode":
            execution_paths.add("sell_churn_hold_mode")
        if dual_bid_exception_reason == "divergence_buy_hard_suppress" or bool(
            analytics.get("divergence_hard_suppress_up_active") or analytics.get("divergence_hard_suppress_dn_active")
        ):
            execution_paths.add("divergence hard suppress")
        if (
            bool(runtime.get("terminal_liquidation_done"))
            and (
                bool(health.get("true_drift"))
                or lifecycle == "halted"
                or str(risk.get("hard_mode") or "") == "halted"
            )
        ):
            post_terminal_false_drift = True

        time_left_sec = float(market.get("time_left_sec") or 0.0)
        unwind_window_sec = float(config.get("unwind_window_sec") or 90.0)
        if time_left_sec <= unwind_window_sec:
            continue
        outside_samples += 1
        outside_lifecycle[lifecycle] += 1
        mm_effective_ratio_60s = analytics.get("mm_effective_ratio_60s")
        if mm_effective_ratio_60s is None:
            mm_effective_ratio_60s = (
                _clamp_ratio(analytics.get("quoting_ratio_60s"))
                + _clamp_ratio(analytics.get("inventory_skewed_ratio_60s"))
                + _clamp_ratio(analytics.get("defensive_ratio_60s"))
            )
        unwind_ratio_60s = _clamp_ratio(analytics.get("unwind_ratio_60s"))
        emergency_ratio_60s = _clamp_ratio(analytics.get("emergency_unwind_ratio_60s"))
        min_mm_effective_ratio_60s_outside = min(min_mm_effective_ratio_60s_outside, _clamp_ratio(mm_effective_ratio_60s))
        max_unwind_ratio_60s_outside = max(max_unwind_ratio_60s_outside, unwind_ratio_60s)
        max_emergency_unwind_ratio_60s_outside = max(max_emergency_unwind_ratio_60s_outside, emergency_ratio_60s)
        execution_churn_ratio_60s_max = max(
            execution_churn_ratio_60s_max,
            max(0.0, float(analytics.get("execution_churn_ratio_60s") or 0.0)),
        )
        quote_balance_state = str(analytics.get("quote_balance_state") or state.get("quote_balance_state") or "")
        if quote_balance_state == "none":
            quote_none_streak += 1
            max_quote_none_streak = max(max_quote_none_streak, quote_none_streak)
        else:
            quote_none_streak = 0
        has_live_streaks = (
            "collateral_warning_streak_current" in analytics
            or "sell_skip_cooldown_streak_current" in analytics
            or "up_collateral_warning_streak" in analytics
            or "up_sell_skip_cooldown_streak" in analytics
        )
        if has_live_streaks:
            collateral_warning_streak = max(
                int(analytics.get("collateral_warning_streak_current") or 0),
                int(analytics.get("up_collateral_warning_streak") or 0),
                int(analytics.get("dn_collateral_warning_streak") or 0),
            )
            sell_skip_streak = max(
                int(analytics.get("sell_skip_cooldown_streak_current") or 0),
                int(analytics.get("up_sell_skip_cooldown_streak") or 0),
                int(analytics.get("dn_sell_skip_cooldown_streak") or 0),
            )
            max_collateral_warning_streak_outside = max(max_collateral_warning_streak_outside, collateral_warning_streak)
            max_sell_skip_cooldown_streak_outside = max(max_sell_skip_cooldown_streak_outside, sell_skip_streak)
        else:
            collateral_warning_hits = int(analytics.get("collateral_warning_hits_60s") or 0)
            sell_skip_cooldown_hits = int(analytics.get("sell_skip_cooldown_hits_60s") or 0)
            if collateral_warning_hits > 0:
                collateral_warning_streak_outside += 1
                max_collateral_warning_streak_outside = max(
                    max_collateral_warning_streak_outside,
                    collateral_warning_streak_outside,
                )
            else:
                collateral_warning_streak_outside = 0
            if sell_skip_cooldown_hits > 0:
                sell_skip_cooldown_streak_outside += 1
                max_sell_skip_cooldown_streak_outside = max(
                    max_sell_skip_cooldown_streak_outside,
                    sell_skip_cooldown_streak_outside,
                )
            else:
                sell_skip_cooldown_streak_outside = 0
        material_inventory_usd = max(6.0, 0.20 * float(config.get("session_budget_usd") or 300.0))
        if (
            not bool(market.get("market_tradeable"))
            and str(risk.get("soft_mode") or "") == "normal"
            and str(risk.get("reason") or "").startswith("normal quoting (untradeable tolerated)")
            and float(inventory.get("total_inventory_value_usd") or 0.0) >= material_inventory_usd
        ):
            untradeable_tolerated_material_samples_outside += 1

    for row in logs:
        message = _log_message(row)
        if not message:
            continue
        if "post-cancel cooldown" in message or "SELL skipped" in message or "Cancelled" in message:
            execution_paths.add("cancel/repost churn")
        if "divergence_buy_hard_suppress" in message:
            execution_paths.add("divergence hard suppress")
        if "sell_churn_hold_mode" in message:
            execution_paths.add("sell_churn_hold_mode")
        if "post-terminal" in message and "drift" in message:
            execution_paths.add("post-terminal false drift")
    if post_terminal_false_drift:
        execution_paths.add("post-terminal false drift")

    final_state = rows[-1]
    final_runtime = final_state.get("runtime") or {}
    final_market = final_state.get("market") or {}
    final_cfg = final_state.get("config") or {}
    final_min_order_size = float(final_cfg.get("min_order_size") or 5.0)
    terminal_ok = True
    if bool(final_runtime.get("terminal_liquidation_active")) and float(final_market.get("time_left_sec") or 0.0) <= 0.0:
        terminal_ok = bool(final_runtime.get("terminal_liquidation_done")) or max(
            float(final_runtime.get("terminal_liquidation_remaining_up") or 0.0),
            float(final_runtime.get("terminal_liquidation_remaining_dn") or 0.0),
        ) < final_min_order_size

    failed_criteria: list[str] = []
    if true_drift_any:
        failed_criteria.append("true_drift_present")
    if hard_halted_any:
        failed_criteria.append("halted_present")
    if not terminal_ok:
        failed_criteria.append("terminal_execution_incomplete_present")
    if outside_samples <= 0:
        failed_criteria.append("no_samples_outside_near_expiry")
    else:
        if min_mm_effective_ratio_60s_outside < float(args.mm_effective_min):
            failed_criteria.append(
                f"mm_effective_ratio_60s_below_{float(args.mm_effective_min):.2f} (min={min_mm_effective_ratio_60s_outside:.4f})"
            )
        if max_unwind_ratio_60s_outside > float(args.unwind_max):
            failed_criteria.append(
                f"unwind_ratio_60s_above_{float(args.unwind_max):.2f} (max={max_unwind_ratio_60s_outside:.4f})"
            )
        if max_emergency_unwind_ratio_60s_outside > float(args.emergency_max):
            failed_criteria.append(
                f"emergency_unwind_ratio_60s_above_{float(args.emergency_max):.2f} (max={max_emergency_unwind_ratio_60s_outside:.4f})"
            )
        if max_quote_none_streak > int(args.quote_none_streak_max):
            failed_criteria.append(
                f"quote_balance_none_streak_above_{int(args.quote_none_streak_max)} (max={max_quote_none_streak})"
            )
        if max_collateral_warning_streak_outside > 3:
            failed_criteria.append(
                f"collateral_warning_streak_outside_above_3 (max={max_collateral_warning_streak_outside})"
            )
        if max_sell_skip_cooldown_streak_outside > 3:
            failed_criteria.append(
                f"sell_skip_cooldown_streak_outside_above_3 (max={max_sell_skip_cooldown_streak_outside})"
            )
        if untradeable_tolerated_material_samples_outside > 0:
            failed_criteria.append(
                f"untradeable_tolerated_with_material_inventory_present (count={untradeable_tolerated_material_samples_outside})"
            )

    outside_mode_ratios = {
        mode: (float(count) / float(outside_samples))
        for mode, count in outside_lifecycle.items()
        if outside_samples > 0
    }
    window_final_pnls = [float(window_final_pnls_by_start[k]) for k in sorted(window_final_pnls_by_start.keys())]
    final_pnl_usd = float(window_final_pnls[-1]) if window_final_pnls else 0.0
    aggregate_pnl_usd = float(sum(window_final_pnls)) if window_final_pnls else final_pnl_usd
    gate_verdict = "go" if not failed_criteria else "no_go"
    failure_buckets = sorted(bucket for bucket, count in failure_bucket_counts.items() if int(count) > 0) if failed_criteria else []
    primary_blocker = _pick_primary_blocker(failure_bucket_counts) if failed_criteria else ""
    blocker_hint = ""
    if primary_blocker == "marketability_churn":
        if "sell_churn_hold_mode" in execution_paths:
            blocker_hint = "sell_churn_hold_mode"
        elif "cancel/repost churn" in execution_paths:
            blocker_hint = "cancel/repost churn"
        else:
            blocker_hint = "marketability_churn"
    elif primary_blocker == "edge_divergence":
        blocker_hint = "divergence hard suppress" if "divergence hard suppress" in execution_paths else "edge_divergence"
    elif primary_blocker == "drift_transport":
        blocker_hint = "post-terminal false drift" if "post-terminal false drift" in execution_paths else "drift_transport"
    elif primary_blocker == "terminal_execution":
        blocker_hint = "terminal_execution"
    elif primary_blocker == "inventory_regime":
        blocker_hint = "inventory_regime"
    if failed_criteria and not primary_blocker:
        failed_criteria.append("unknown_failure_bucket")

    ts = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    out_dir = (REPO_ROOT / args.output_root / ts).resolve()
    out_dir.mkdir(parents=True, exist_ok=True)
    summary = {
        "ok": gate_verdict == "go",
        "gate_verdict": gate_verdict,
        "failed_criteria": failed_criteria,
        "failure_buckets": failure_buckets,
        "primary_blocker": primary_blocker,
        "input_dir": str(input_dir),
        "samples": len(rows),
        "logs_samples": len(logs),
        "fills_samples": len(fills),
        "final_pnl_usd": final_pnl_usd,
        "aggregate_pnl_usd": aggregate_pnl_usd,
        "max_sell_skip_cooldown_streak_outside": max_sell_skip_cooldown_streak_outside,
        "max_collateral_warning_streak_outside": max_collateral_warning_streak_outside,
        "max_quote_balance_none_streak": max_quote_none_streak,
        "execution_churn_ratio_60s_max": execution_churn_ratio_60s_max,
        "outside_mode_ratios": outside_mode_ratios,
        "terminal_ok": bool(terminal_ok),
        "execution_paths": sorted(execution_paths),
        "execution_replay_blocker_hint": blocker_hint,
        "output_dir": str(out_dir),
    }
    (out_dir / "summary.json").write_text(json.dumps(summary, ensure_ascii=False, indent=2), encoding="utf-8")
    return summary


def main() -> int:
    parser = argparse.ArgumentParser(description="Execution-aware replay for MM V2 artifacts")
    parser.add_argument("--audit-root", default="audit")
    parser.add_argument("--input-dir", default=None)
    parser.add_argument("--output-root", default="audit/execution-replay")
    parser.add_argument("--mm-effective-min", type=float, default=0.65)
    parser.add_argument("--unwind-max", type=float, default=0.35)
    parser.add_argument("--emergency-max", type=float, default=0.10)
    parser.add_argument("--quote-none-streak-max", type=int, default=3)
    args = parser.parse_args()

    summary = _build_summary(args)
    print(json.dumps(summary, ensure_ascii=False, indent=2))
    return 0 if summary.get("gate_verdict") == "go" else 2


if __name__ == "__main__":
    raise SystemExit(main())
