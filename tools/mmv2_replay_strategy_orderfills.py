#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import math
import sys
import time
from dataclasses import replace
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import pandas as pd

REPO_ROOT = Path(__file__).resolve().parents[1]
SRC_ROOT = REPO_ROOT / "src"
if str(SRC_ROOT) not in sys.path:
    sys.path.insert(0, str(SRC_ROOT))

from mm_v2.config import (  # noqa: E402
    DRAWDOWN_CONFIRM_MIN_AGE_SEC,
    DRAWDOWN_CONFIRM_TICKS,
    DRAWDOWN_RESET_HYSTERESIS_USD,
    MMConfigV2,
)
from mm_v2.pair_inventory import build_pair_inventory  # noqa: E402
from mm_v2.quote_policy import QuoteContext, QuotePolicyV2  # noqa: E402
from mm_v2.risk_kernel import HardSafetyKernel  # noqa: E402
from mm_v2.state_machine import StateMachineV2  # noqa: E402
from mm_v2.types import (  # noqa: E402
    AnalyticsState,
    HealthState,
    PairMarketSnapshot,
    QuoteIntent,
    QuotePlan,
    QuoteViabilitySummary,
)
import mm_v2.state_machine as state_machine_module  # noqa: E402

TOKEN_RAW_SCALE = 1_000_000.0
DEFAULT_MIN_ORDER_SIZE = 5.0
DEFAULT_TICK_SIZE = 0.01
DEFAULT_DEPTH_USD = 250.0
PRIMARY_BLOCKER_PRIORITY = [
    "drift_transport",
    "terminal_execution",
    "marketability_churn",
    "edge_divergence",
    "inventory_regime",
]
CURATED_REPLAY_SLICE = REPO_ROOT / "data" / "replay" / "poly_replay_slice_test.parquet"


class _ReplayClock:
    def __init__(self, start_ts: float):
        self.ts = float(start_ts)

    def time(self) -> float:
        return float(self.ts)


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run offline MM V2 replay over normalized Polymarket orderfills.")
    parser.add_argument(
        "--dataset-root",
        default=str(REPO_ROOT / "data" / "normalized" / "poly_data_orderfills"),
        help="Path to normalized dataset root (partitioned by trade_day=YYYY-MM-DD).",
    )
    parser.add_argument("--manifest", default="", help="Optional JSON manifest of replay scenarios.")
    parser.add_argument(
        "--manifest-mode",
        choices=("quick", "full"),
        default="full",
        help="Subset of manifest scenarios to run when --manifest is provided.",
    )
    parser.add_argument("--up-token-id", default="", help="UP token asset id for replay pair.")
    parser.add_argument("--dn-token-id", default="", help="DN token asset id for replay pair.")
    parser.add_argument("--date-from", default="", help="Inclusive start date (YYYY-MM-DD).")
    parser.add_argument("--date-to", default="", help="Inclusive end date (YYYY-MM-DD).")
    parser.add_argument("--ts-from", type=int, default=0, help="Inclusive start timestamp (epoch sec).")
    parser.add_argument("--ts-to", type=int, default=0, help="Inclusive end timestamp (epoch sec).")
    parser.add_argument("--tick-sec", type=int, default=2, help="Replay tick interval in seconds.")
    parser.add_argument("--window-sec", type=float, default=900.0, help="Synthetic market window length in seconds.")
    parser.add_argument("--session-budget-usd", type=float, default=50.0, help="Replay start USDC.")
    parser.add_argument(
        "--allow-naked-sells",
        action="store_true",
        help="Allow synthetic naked sells by minting complementary shares with collateral accounting.",
    )
    parser.add_argument(
        "--output-dir",
        default="",
        help="Output directory for replay artifacts. Default: audit/replay/<timestamp>/",
    )
    parser.add_argument("--max-ticks", type=int, default=0, help="Optional cap on processed ticks (0 = all).")
    args = parser.parse_args()
    if not args.manifest and (not str(args.up_token_id).strip() or not str(args.dn_token_id).strip()):
        parser.error("--up-token-id and --dn-token-id are required when --manifest is not provided")
    return args


def _trade_days(dataset_root: Path, date_from: str, date_to: str) -> list[Path]:
    days = sorted([p for p in dataset_root.glob("trade_day=*") if p.is_dir()])
    if not days:
        return []
    if not date_from and not date_to:
        return days
    out: list[Path] = []
    for day_dir in days:
        day = day_dir.name.split("=", 1)[1]
        if date_from and day < date_from:
            continue
        if date_to and day > date_to:
            continue
        out.append(day_dir)
    return out


def _resolve_dataset_source(dataset_root_arg: str) -> Path:
    dataset_root = Path(dataset_root_arg).expanduser().resolve()
    if dataset_root.exists():
        return dataset_root
    if CURATED_REPLAY_SLICE.exists():
        return CURATED_REPLAY_SLICE.resolve()
    raise FileNotFoundError(f"Dataset root not found: {dataset_root}")


def _load_pair_events(
    dataset_root: Path,
    *,
    up_token_id: str,
    dn_token_id: str,
    date_from: str,
    date_to: str,
    ts_from: int = 0,
    ts_to: int = 0,
) -> pd.DataFrame:
    cols = [
        "timestamp_sec",
        "trade_day",
        "token_asset_id",
        "taker_side",
        "price_prob",
        "token_amount_raw",
        "self_trade",
    ]
    frames: list[pd.DataFrame] = []
    token_set = {str(up_token_id), str(dn_token_id)}
    if dataset_root.is_file():
        frame = pd.read_parquet(dataset_root)
        if frame.empty or "token_asset_id" not in frame.columns or "timestamp_sec" not in frame.columns:
            return pd.DataFrame(columns=cols + ["shares", "tick_ts"])
        frame = frame.copy()
        frame["token_asset_id"] = frame["token_asset_id"].astype(str)
        frame = frame[frame["token_asset_id"].isin(token_set)]
        if frame.empty:
            return pd.DataFrame(columns=cols + ["shares", "tick_ts"])
        if "trade_day" not in frame.columns:
            frame["trade_day"] = pd.to_datetime(frame["timestamp_sec"], unit="s", utc=True).dt.strftime("%Y-%m-%d")
        if "taker_side" not in frame.columns:
            frame["taker_side"] = "BUY"
        if "self_trade" not in frame.columns:
            frame["self_trade"] = False
        if int(ts_from) > 0:
            frame = frame[frame["timestamp_sec"].astype(int) >= int(ts_from)]
        if frame.empty:
            return pd.DataFrame(columns=cols + ["shares", "tick_ts"])
        if int(ts_to) > 0:
            frame = frame[frame["timestamp_sec"].astype(int) <= int(ts_to)]
        if frame.empty:
            return pd.DataFrame(columns=cols + ["shares", "tick_ts"])
        frame = frame[frame["self_trade"] == False]  # noqa: E712
        if frame.empty:
            return pd.DataFrame(columns=cols + ["shares", "tick_ts"])
        frame = frame.reindex(columns=cols)
        frame["price_prob"] = frame["price_prob"].astype(float).clip(0.01, 0.99)
        frame["shares"] = (frame["token_amount_raw"].astype(float) / TOKEN_RAW_SCALE).clip(lower=0.0)
        frame = frame[frame["shares"] > 0.0]
        return frame.sort_values("timestamp_sec").reset_index(drop=True)
    for day_dir in _trade_days(dataset_root, date_from, date_to):
        for part in sorted(day_dir.glob("*.parquet")):
            frame = pd.read_parquet(part, columns=cols)
            if frame.empty:
                continue
            frame["token_asset_id"] = frame["token_asset_id"].astype(str)
            frame = frame[frame["token_asset_id"].isin(token_set)]
            if frame.empty:
                continue
            if int(ts_from) > 0:
                frame = frame[frame["timestamp_sec"].astype(int) >= int(ts_from)]
            if frame.empty:
                continue
            if int(ts_to) > 0:
                frame = frame[frame["timestamp_sec"].astype(int) <= int(ts_to)]
            if frame.empty:
                continue
            frame = frame[frame["self_trade"] == False]  # noqa: E712
            if frame.empty:
                continue
            frames.append(frame)
    if not frames:
        return pd.DataFrame(columns=cols + ["shares", "tick_ts"])
    events = pd.concat(frames, ignore_index=True)
    events["price_prob"] = events["price_prob"].astype(float).clip(0.01, 0.99)
    events["shares"] = (events["token_amount_raw"].astype(float) / TOKEN_RAW_SCALE).clip(lower=0.0)
    events = events[events["shares"] > 0.0]
    return events.sort_values("timestamp_sec").reset_index(drop=True)


def _weighted_mids(events: pd.DataFrame, tick_sec: int) -> pd.DataFrame:
    df = events.copy()
    df["tick_ts"] = (df["timestamp_sec"].astype(int) // tick_sec) * tick_sec
    df["weighted_price"] = df["price_prob"] * df["shares"]
    agg = (
        df.groupby(["tick_ts", "token_asset_id"], as_index=False)[["weighted_price", "shares"]]
        .sum()
        .rename(columns={"shares": "shares_sum"})
    )
    agg["mid"] = agg["weighted_price"] / agg["shares_sum"].clip(lower=1e-9)
    return agg[["tick_ts", "token_asset_id", "mid", "shares_sum"]]


def _tick_side_stats(events: pd.DataFrame, tick_sec: int) -> pd.DataFrame:
    df = events.copy()
    df["tick_ts"] = (df["timestamp_sec"].astype(int) // tick_sec) * tick_sec
    grouped = (
        df.groupby(["tick_ts", "token_asset_id", "taker_side"], as_index=False)
        .agg(
            side_price_min=("price_prob", "min"),
            side_price_max=("price_prob", "max"),
            side_shares=("shares", "sum"),
        )
        .reset_index(drop=True)
    )
    grouped["taker_side"] = grouped["taker_side"].astype(str)
    return grouped


def _viability(plan: QuotePlan) -> QuoteViabilitySummary:
    intents = [plan.up_bid, plan.up_ask, plan.dn_bid, plan.dn_ask]
    active = [i for i in intents if i]
    helpful = sum(1 for i in active if i.inventory_effect == "helpful")
    harmful = sum(1 for i in active if i.inventory_effect == "harmful")
    return QuoteViabilitySummary(
        any_quote=bool(active),
        four_quotes=len(active) == 4,
        helpful_count=helpful,
        harmful_count=harmful,
        helpful_only=helpful > 0 and harmful == 0,
        harmful_only=harmful > 0 and helpful == 0,
        four_quote_presence_ratio=1.0 if len(active) == 4 else (0.25 if active else 0.0),
        quote_balance_state=plan.quote_balance_state,
    )


def _safe_bid_ask(mid: float, half_spread: float = 0.01) -> tuple[float, float]:
    bid = max(0.01, min(0.99, mid - half_spread))
    ask = max(0.01, min(0.99, mid + half_spread))
    if ask <= bid:
        ask = min(0.99, bid + DEFAULT_TICK_SIZE)
    return bid, ask


def _fill_buy(
    *,
    intent: QuoteIntent,
    side_stats: dict[str, dict[str, float]],
    cash_usd: float,
    current_shares: float,
) -> tuple[float, float, float, float]:
    sell_flow = side_stats.get("SELL")
    if not sell_flow:
        return cash_usd, current_shares, 0.0, 0.0
    if float(intent.price) + 1e-9 < float(sell_flow["price_min"]):
        return cash_usd, current_shares, 0.0, 0.0
    max_affordable = cash_usd / max(0.01, float(intent.price))
    fill_size = max(0.0, min(float(intent.size), float(sell_flow["shares"]), max_affordable))
    if fill_size <= 1e-9:
        return cash_usd, current_shares, 0.0, 0.0
    notional = fill_size * float(intent.price)
    return cash_usd - notional, current_shares + fill_size, fill_size, notional


def _fill_sell(
    *,
    intent: QuoteIntent,
    token: str,
    up_token_id: str,
    dn_token_id: str,
    side_stats: dict[str, dict[str, float]],
    cash_usd: float,
    up_shares: float,
    dn_shares: float,
    allow_naked_sells: bool,
) -> tuple[float, float, float, float, float]:
    buy_flow = side_stats.get("BUY")
    if not buy_flow:
        return cash_usd, up_shares, dn_shares, 0.0, 0.0
    if float(intent.price) - 1e-9 > float(buy_flow["price_max"]):
        return cash_usd, up_shares, dn_shares, 0.0, 0.0
    fill_cap = max(0.0, min(float(intent.size), float(buy_flow["shares"])))
    if fill_cap <= 1e-9:
        return cash_usd, up_shares, dn_shares, 0.0, 0.0

    if token == up_token_id:
        owned = up_shares
        other = dn_shares
    else:
        owned = dn_shares
        other = up_shares

    regular = min(owned, fill_cap)
    naked = 0.0
    if allow_naked_sells and fill_cap > regular:
        per_share_collateral = max(0.01, 1.0 - float(intent.price))
        max_naked = cash_usd / per_share_collateral
        naked = max(0.0, min(fill_cap - regular, max_naked))
    fill_size = regular + naked
    if fill_size <= 1e-9:
        return cash_usd, up_shares, dn_shares, 0.0, 0.0

    # Inventory-backed leg.
    cash_after = cash_usd + regular * float(intent.price)
    owned_after = owned - regular
    # Naked short leg via mint+sell decomposition:
    # collateral(1) - proceeds(price) => net cash delta = -(1-price), opposite inventory +1.
    cash_after -= naked * max(0.01, 1.0 - float(intent.price))
    other_after = other + naked

    if token == up_token_id:
        up_after = owned_after
        dn_after = other_after
    else:
        dn_after = owned_after
        up_after = other_after
    notional = fill_size * float(intent.price)
    return cash_after, up_after, dn_after, fill_size, notional


def _update_drawdown_state(
    *,
    equity_pnl: float,
    hard_drawdown_usd: float,
    now_ts: float,
    breach_ticks: int,
    breach_started_ts: float,
    breach_active: bool,
) -> tuple[int, float, bool]:
    if hard_drawdown_usd <= 0:
        return 0, 0.0, False
    threshold = -abs(float(hard_drawdown_usd))
    if equity_pnl <= threshold:
        if breach_started_ts <= 0.0:
            breach_started_ts = now_ts
            breach_ticks = 1
        else:
            breach_ticks += 1
    elif equity_pnl >= threshold + float(DRAWDOWN_RESET_HYSTERESIS_USD):
        breach_ticks = 0
        breach_started_ts = 0.0
        breach_active = False
    age = max(0.0, now_ts - breach_started_ts) if breach_started_ts > 0.0 else 0.0
    breach_active = bool(
        breach_ticks >= int(DRAWDOWN_CONFIRM_TICKS) and age >= float(DRAWDOWN_CONFIRM_MIN_AGE_SEC)
    )
    return int(breach_ticks), float(age), breach_active


def _scenario_failure_bucket(summary: dict[str, Any], scenario_category: str) -> str:
    if bool(summary.get("true_drift_present")):
        return "drift_transport"
    if bool(summary.get("terminal_execution_failed")):
        return "terminal_execution"
    if scenario_category in {"marketability_churn", "edge_divergence", "inventory_regime", "terminal_execution"}:
        return str(scenario_category)
    if bool(summary.get("halted_present")):
        return "inventory_regime"
    if float(summary.get("max_emergency_unwind_ratio_60s_outside") or 0.0) > 0.10:
        return "inventory_regime"
    if float(summary.get("max_unwind_ratio_60s_outside") or 0.0) > 0.35:
        return "inventory_regime"
    return ""


def _pick_primary_blocker(buckets: list[str]) -> str:
    bucket_set = {str(bucket) for bucket in buckets if str(bucket)}
    for bucket in PRIMARY_BLOCKER_PRIORITY:
        if bucket in bucket_set:
            return bucket
    return ""


def _scenario_error_summary(
    *,
    scenario_id: str,
    scenario_category: str,
    scenario_dir: Path,
    error: Exception,
    dataset_root: str,
) -> dict[str, Any]:
    failure_bucket = str(scenario_category) if str(scenario_category) in {
        "marketability_churn",
        "edge_divergence",
        "inventory_regime",
        "terminal_execution",
    } else ""
    summary = {
        "ok": False,
        "gate_verdict": "no_go",
        "failed_criteria": [f"scenario_runtime_error:{type(error).__name__}"],
        "failure_buckets": [failure_bucket] if failure_bucket else [],
        "primary_blocker": failure_bucket,
        "dataset_root": str(dataset_root),
        "scenario_id": str(scenario_id),
        "scenario_category": str(scenario_category),
        "final_pnl_usd": 0.0,
        "aggregate_pnl_usd": 0.0,
        "outside_near_expiry_samples": 0,
        "outside_mode_ratios": {},
        "mm_effective_share_outside": 0.0,
        "max_unwind_ratio_60s_outside": 0.0,
        "max_emergency_unwind_ratio_60s_outside": 0.0,
        "max_quote_none_streak_outside": 0,
        "execution_churn_ratio_60s": 0.0,
        "runtime_sec": 0.0,
        "error_type": type(error).__name__,
        "error": str(error),
    }
    if summary["failed_criteria"] and not summary["primary_blocker"]:
        summary["failed_criteria"].append("unknown_failure_bucket")
    (scenario_dir / "summary.json").write_text(json.dumps(summary, ensure_ascii=False, indent=2), encoding="utf-8")
    return summary


def _single_run_failed_criteria(summary: dict[str, Any]) -> list[str]:
    failed: list[str] = []
    if bool(summary.get("true_drift_present")):
        failed.append("true_drift_present")
    if bool(summary.get("halted_present")):
        failed.append("halted_present")
    if int(summary.get("outside_near_expiry_samples") or 0) <= 0:
        failed.append("no_samples_outside_near_expiry")
        return failed
    if float(summary.get("mm_effective_share_outside") or 0.0) < 0.65:
        failed.append(
            "mm_effective_ratio_60s_below_0.65 "
            f"(min={float(summary.get('mm_effective_share_outside') or 0.0):.4f})"
        )
    if float(summary.get("max_unwind_ratio_60s_outside") or 0.0) > 0.35:
        failed.append(
            "unwind_ratio_60s_above_0.35 "
            f"(max={float(summary.get('max_unwind_ratio_60s_outside') or 0.0):.4f})"
        )
    if float(summary.get("max_emergency_unwind_ratio_60s_outside") or 0.0) > 0.10:
        failed.append(
            "emergency_unwind_ratio_60s_above_0.10 "
            f"(max={float(summary.get('max_emergency_unwind_ratio_60s_outside') or 0.0):.4f})"
        )
    if int(summary.get("max_quote_none_streak_outside") or 0) > 3:
        failed.append(
            "quote_balance_none_streak_above_3 "
            f"(max={int(summary.get('max_quote_none_streak_outside') or 0)})"
        )
    if int(summary.get("untradeable_tolerated_material_samples_outside") or 0) > 0:
        failed.append(
            "untradeable_tolerated_with_material_inventory_present "
            f"(count={int(summary.get('untradeable_tolerated_material_samples_outside') or 0)})"
        )
    if bool(summary.get("toxic_buy_present_outside")):
        failed.append("toxic_side_buy_present_outside")
    dual_bid_ratio_outside = float(summary.get("dual_bid_ratio_outside") or 0.0)
    if dual_bid_ratio_outside < 0.70:
        failed.append(f"dual_bid_ratio_outside_below_0.70 (min={dual_bid_ratio_outside:.4f})")
    return failed


def _load_manifest(manifest_path: Path) -> dict[str, Any]:
    payload = json.loads(manifest_path.read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        raise RuntimeError(f"manifest must be a JSON object: {manifest_path}")
    scenarios = payload.get("scenarios")
    if not isinstance(scenarios, list) or not scenarios:
        raise RuntimeError(f"manifest has no scenarios: {manifest_path}")
    payload["scenarios"] = [dict(item) for item in scenarios if isinstance(item, dict)]
    if not payload["scenarios"]:
        raise RuntimeError(f"manifest has no valid scenario objects: {manifest_path}")
    return payload


def run_replay(args: argparse.Namespace) -> dict[str, Any]:
    started = time.time()
    dataset_root = _resolve_dataset_source(str(args.dataset_root))

    events = _load_pair_events(
        dataset_root,
        up_token_id=args.up_token_id,
        dn_token_id=args.dn_token_id,
        date_from=args.date_from,
        date_to=args.date_to,
        ts_from=int(getattr(args, "ts_from", 0) or 0),
        ts_to=int(getattr(args, "ts_to", 0) or 0),
    )
    if events.empty:
        raise RuntimeError("No pair events found for selected token ids/date range.")

    tick_sec = max(1, int(args.tick_sec))
    mids = _weighted_mids(events, tick_sec=tick_sec)
    side_stats = _tick_side_stats(events, tick_sec=tick_sec)

    tick_mids: dict[int, dict[str, float]] = {}
    for row in mids.itertuples(index=False):
        tick_mids.setdefault(int(row.tick_ts), {})[str(row.token_asset_id)] = float(row.mid)

    tick_flow: dict[int, dict[str, dict[str, dict[str, float]]]] = {}
    for row in side_stats.itertuples(index=False):
        tick = int(row.tick_ts)
        token = str(row.token_asset_id)
        side = str(row.taker_side).upper()
        tick_flow.setdefault(tick, {}).setdefault(token, {})[side] = {
            "price_min": float(row.side_price_min),
            "price_max": float(row.side_price_max),
            "shares": float(row.side_shares),
        }

    cfg = MMConfigV2(session_budget_usd=float(args.session_budget_usd))
    cfg.validate()
    policy = QuotePolicyV2(cfg)
    kernel = HardSafetyKernel(cfg)
    ctx = QuoteContext(
        tick_size=DEFAULT_TICK_SIZE,
        min_order_size=DEFAULT_MIN_ORDER_SIZE,
        allow_naked_sells=bool(args.allow_naked_sells),
    )

    all_ticks = sorted(set(tick_mids.keys()) | set(tick_flow.keys()))
    if args.max_ticks and args.max_ticks > 0:
        all_ticks = all_ticks[: int(args.max_ticks)]
    if not all_ticks:
        raise RuntimeError("No replay ticks built from events.")
    clock = _ReplayClock(float(all_ticks[0]))
    original_state_machine_time = state_machine_module.time.time
    state_machine_module.time.time = clock.time
    sm = StateMachineV2(cfg)

    # Initial state.
    first_mid_map = tick_mids.get(all_ticks[0], {})
    up_mid = float(first_mid_map.get(args.up_token_id, 0.50))
    dn_mid = float(first_mid_map.get(args.dn_token_id, 1.0 - up_mid))
    if not (0.01 <= dn_mid <= 0.99):
        dn_mid = max(0.01, min(0.99, 1.0 - up_mid))
    cash_usd = float(cfg.session_budget_usd)
    up_shares = 0.0
    dn_shares = 0.0
    start_portfolio = cash_usd
    fill_count = 0
    filled_notional_usd = 0.0
    drawdown_breach_ticks = 0
    drawdown_breach_started_ts = 0.0
    drawdown_breach_active = False

    lifecycle_counts: dict[str, int] = {}
    quote_balance_counts: dict[str, int] = {}
    unwind_target_mismatch_ticks = 0
    unwind_target_mismatch_sec = 0.0
    harmful_suppressed_ticks = 0
    near_expiry_none_streak = 0
    non_expiry_none_streak = 0
    max_non_expiry_none_streak = 0
    outside_near_expiry_samples = 0
    outside_lifecycle_counts: dict[str, int] = {}
    max_quote_none_streak_outside = 0
    quote_none_streak_outside = 0
    dual_bid_ticks_outside = 0
    one_sided_bid_streak_outside = 0
    max_one_sided_bid_streak_outside = 0
    halted_present = False
    true_drift_present = False

    tick_rows: list[dict[str, Any]] = []
    first_ts = int(all_ticks[0])
    try:
        for tick in all_ticks:
            clock.ts = float(tick)
            mid_map = tick_mids.get(tick, {})
            if args.up_token_id in mid_map:
                up_mid = float(mid_map[args.up_token_id])
            if args.dn_token_id in mid_map:
                dn_mid = float(mid_map[args.dn_token_id])
            if args.up_token_id not in mid_map and args.dn_token_id in mid_map:
                up_mid = max(0.01, min(0.99, 1.0 - dn_mid))
            if args.dn_token_id not in mid_map and args.up_token_id in mid_map:
                dn_mid = max(0.01, min(0.99, 1.0 - up_mid))

            up_bid, up_ask = _safe_bid_ask(up_mid)
            dn_bid, dn_ask = _safe_bid_ask(dn_mid)
            time_left = max(0.0, float(args.window_sec) - float((tick - first_ts) % max(1, int(args.window_sec))))

            position_mark_bid = up_shares * up_bid + dn_shares * dn_bid
            equity = cash_usd + position_mark_bid
            pnl = equity - start_portfolio
            drawdown_breach_ticks, drawdown_breach_age, drawdown_breach_active = _update_drawdown_state(
                equity_pnl=pnl,
                hard_drawdown_usd=float(cfg.hard_drawdown_usd),
                now_ts=float(tick),
                breach_ticks=drawdown_breach_ticks,
                breach_started_ts=drawdown_breach_started_ts,
                breach_active=drawdown_breach_active,
            )
            if drawdown_breach_ticks > 0 and drawdown_breach_started_ts <= 0.0:
                drawdown_breach_started_ts = float(tick)
            if drawdown_breach_ticks == 0:
                drawdown_breach_started_ts = 0.0

            inventory = build_pair_inventory(
                up_shares=up_shares,
                dn_shares=dn_shares,
                total_usdc=cash_usd,
                available_usdc=cash_usd,
                active_orders={},
                fv_up=up_mid,
                fv_dn=dn_mid,
                up_token_id=args.up_token_id,
                dn_token_id=args.dn_token_id,
                session_budget_usd=float(cfg.session_budget_usd),
                target_pair_value_ratio=float(cfg.target_pair_value_ratio),
            )
            inventory.sellable_up_shares = max(0.0, up_shares)
            inventory.sellable_dn_shares = max(0.0, dn_shares)

            analytics = AnalyticsState(
                fill_count=int(fill_count),
                session_pnl=float(pnl),
                session_pnl_equity_usd=float(pnl),
                session_pnl_operator_usd=float(pnl),
                session_pnl_operator_ema_usd=float(pnl),
                position_mark_value_usd=float(up_shares * up_mid + dn_shares * dn_mid),
                position_mark_value_bid_usd=float(position_mark_bid),
                position_mark_value_mid_usd=float(up_shares * up_mid + dn_shares * dn_mid),
                portfolio_mark_value_usd=float(equity),
                tradeable_portfolio_value_usd=float(cash_usd + up_shares * up_mid + dn_shares * dn_mid),
                pnl_updated_ts=float(tick),
                excess_value_usd=float(inventory.excess_value_usd),
                target_pair_value_usd=float(inventory.target_pair_value_usd),
                pair_value_ratio=float(inventory.pair_value_ratio),
                pair_value_over_target_usd=float(inventory.pair_value_over_target_usd),
                inventory_pressure_abs=float(inventory.inventory_pressure_abs),
                inventory_pressure_signed=float(inventory.inventory_pressure_signed),
            )
            health = HealthState(
                reconcile_status="ok",
                heartbeat_ok=True,
                transport_ok=True,
                true_drift=False,
                drawdown_breach_ticks=int(drawdown_breach_ticks),
                drawdown_breach_age_sec=float(drawdown_breach_age),
                drawdown_breach_active=bool(drawdown_breach_active),
            )

            snapshot = PairMarketSnapshot(
                ts=float(tick),
                market_id="offline-replay",
                up_token_id=args.up_token_id,
                dn_token_id=args.dn_token_id,
                time_left_sec=float(time_left),
                fv_up=float(up_mid),
                fv_dn=float(dn_mid),
                fv_confidence=0.9,
                pm_mid_up=float(up_mid),
                pm_mid_dn=float(dn_mid),
                up_best_bid=float(up_bid),
                up_best_ask=float(up_ask),
                dn_best_bid=float(dn_bid),
                dn_best_ask=float(dn_ask),
                up_bid_depth_usd=DEFAULT_DEPTH_USD,
                up_ask_depth_usd=DEFAULT_DEPTH_USD,
                dn_bid_depth_usd=DEFAULT_DEPTH_USD,
                dn_ask_depth_usd=DEFAULT_DEPTH_USD,
                market_quality_score=0.9,
                market_tradeable=True,
                divergence_up=0.0,
                divergence_dn=0.0,
            )

            risk = kernel.evaluate(snapshot=snapshot, inventory=inventory, analytics=analytics, health=health)
            provisional = policy.generate(snapshot=snapshot, inventory=inventory, risk=risk, ctx=ctx)
            provisional_viability = _viability(provisional)
            transition = sm.transition(
                snapshot=snapshot,
                inventory=inventory,
                risk=risk,
                viability=provisional_viability,
            )
            effective_risk = replace(
                risk,
                soft_mode=transition.effective_soft_mode,  # type: ignore[arg-type]
                target_soft_mode=transition.target_soft_mode,  # type: ignore[arg-type]
            )
            plan = policy.generate(snapshot=snapshot, inventory=inventory, risk=effective_risk, ctx=ctx)
            viability = _viability(plan)

            lifecycle_counts[transition.lifecycle] = lifecycle_counts.get(transition.lifecycle, 0) + 1
            quote_balance_counts[plan.quote_balance_state] = quote_balance_counts.get(plan.quote_balance_state, 0) + 1
            if transition.effective_soft_mode == "unwind" and transition.target_soft_mode != "unwind":
                unwind_target_mismatch_ticks += 1
                unwind_target_mismatch_sec += float(tick_sec)
            if any(v.startswith("harmful_suppressed_in_") for v in plan.suppressed_reasons.values()):
                harmful_suppressed_ticks += 1
            near_expiry = time_left <= float(cfg.unwind_window_sec)
            if plan.quote_balance_state == "none":
                if near_expiry:
                    near_expiry_none_streak += 1
                else:
                    non_expiry_none_streak += 1
                    max_non_expiry_none_streak = max(max_non_expiry_none_streak, non_expiry_none_streak)
            else:
                near_expiry_none_streak = 0
                non_expiry_none_streak = 0
            if transition.lifecycle == "halted" or str(getattr(effective_risk, "hard_mode", "") or "") == "halted":
                halted_present = True
            if not near_expiry:
                outside_near_expiry_samples += 1
                outside_lifecycle_counts[transition.lifecycle] = outside_lifecycle_counts.get(transition.lifecycle, 0) + 1
                active_up_bid = bool(plan.up_bid)
                active_dn_bid = bool(plan.dn_bid)
                if active_up_bid and active_dn_bid:
                    dual_bid_ticks_outside += 1
                    one_sided_bid_streak_outside = 0
                else:
                    one_sided_bid_streak_outside += 1
                    max_one_sided_bid_streak_outside = max(
                        max_one_sided_bid_streak_outside,
                        one_sided_bid_streak_outside,
                    )
                if plan.quote_balance_state == "none":
                    quote_none_streak_outside += 1
                    max_quote_none_streak_outside = max(
                        max_quote_none_streak_outside,
                        quote_none_streak_outside,
                    )
                else:
                    quote_none_streak_outside = 0

            token_tick_flow = tick_flow.get(tick, {})
            for key, intent in (
                ("up_bid", plan.up_bid),
                ("up_ask", plan.up_ask),
                ("dn_bid", plan.dn_bid),
                ("dn_ask", plan.dn_ask),
            ):
                if not intent:
                    continue
                token_flow = token_tick_flow.get(intent.token, {})
                if not token_flow:
                    continue
                if intent.side == "BUY":
                    if intent.token == args.up_token_id:
                        cash_usd, up_shares, filled, notional = _fill_buy(
                            intent=intent,
                            side_stats=token_flow,
                            cash_usd=cash_usd,
                            current_shares=up_shares,
                        )
                    else:
                        cash_usd, dn_shares, filled, notional = _fill_buy(
                            intent=intent,
                            side_stats=token_flow,
                            cash_usd=cash_usd,
                            current_shares=dn_shares,
                        )
                else:
                    cash_usd, up_shares, dn_shares, filled, notional = _fill_sell(
                        intent=intent,
                        token=intent.token,
                        up_token_id=args.up_token_id,
                        dn_token_id=args.dn_token_id,
                        side_stats=token_flow,
                        cash_usd=cash_usd,
                        up_shares=up_shares,
                        dn_shares=dn_shares,
                        allow_naked_sells=bool(args.allow_naked_sells),
                    )
                if filled > 0.0:
                    fill_count += 1
                    filled_notional_usd += notional

            tick_rows.append(
                {
                    "tick_ts": int(tick),
                    "lifecycle": transition.lifecycle,
                    "target_soft_mode": transition.target_soft_mode,
                    "effective_soft_mode": transition.effective_soft_mode,
                    "quote_balance_state": plan.quote_balance_state,
                    "helpful_count": viability.helpful_count,
                    "harmful_count": viability.harmful_count,
                    "time_left_sec": float(time_left),
                    "cash_usd": float(cash_usd),
                    "up_shares": float(up_shares),
                    "dn_shares": float(dn_shares),
                    "up_mid": float(up_mid),
                    "dn_mid": float(dn_mid),
                    "equity_pnl": float((cash_usd + up_shares * up_bid + dn_shares * dn_bid) - start_portfolio),
                }
            )
    finally:
        state_machine_module.time.time = original_state_machine_time

    total_ticks = max(1, len(all_ticks))
    quoting_ratio = lifecycle_counts.get("quoting", 0) / total_ticks
    inventory_skewed_ratio = lifecycle_counts.get("inventory_skewed", 0) / total_ticks
    defensive_ratio = lifecycle_counts.get("defensive", 0) / total_ticks
    unwind_ratio = lifecycle_counts.get("unwind", 0) / total_ticks
    emergency_ratio = lifecycle_counts.get("emergency_unwind", 0) / total_ticks
    mm_effective_ratio = quoting_ratio + inventory_skewed_ratio + defensive_ratio
    mismatch_ratio = unwind_target_mismatch_ticks / total_ticks
    final_up_bid, _ = _safe_bid_ask(up_mid)
    final_dn_bid, _ = _safe_bid_ask(dn_mid)
    final_equity = cash_usd + up_shares * final_up_bid + dn_shares * final_dn_bid
    outside_mode_ratios: dict[str, float] = {}
    if outside_near_expiry_samples > 0:
        for mode, count in outside_lifecycle_counts.items():
            outside_mode_ratios[mode] = float(count) / float(outside_near_expiry_samples)
    mm_effective_share_outside = (
        outside_mode_ratios.get("quoting", 0.0)
        + outside_mode_ratios.get("inventory_skewed", 0.0)
        + outside_mode_ratios.get("defensive", 0.0)
    )
    dual_bid_ratio_outside = (
        float(dual_bid_ticks_outside) / float(outside_near_expiry_samples)
        if outside_near_expiry_samples > 0
        else 0.0
    )

    result = {
        "ok": True,
        "dataset_root": str(dataset_root),
        "scenario_id": str(getattr(args, "scenario_id", "") or ""),
        "scenario_category": str(getattr(args, "scenario_category", "") or ""),
        "up_token_id": str(args.up_token_id),
        "dn_token_id": str(args.dn_token_id),
        "date_from": str(args.date_from or ""),
        "date_to": str(args.date_to or ""),
        "ts_from": int(getattr(args, "ts_from", 0) or 0),
        "ts_to": int(getattr(args, "ts_to", 0) or 0),
        "tick_sec": int(tick_sec),
        "window_sec": float(args.window_sec),
        "session_budget_usd": float(cfg.session_budget_usd),
        "allow_naked_sells": bool(args.allow_naked_sells),
        "processed_ticks": int(total_ticks),
        "processed_events": int(len(events)),
        "fills": int(fill_count),
        "filled_notional_usd": float(filled_notional_usd),
        "final_cash_usd": float(cash_usd),
        "final_up_shares": float(up_shares),
        "final_dn_shares": float(dn_shares),
        "final_equity_usd": float(final_equity),
        "final_pnl_usd": float(final_equity - start_portfolio),
        "lifecycle_counts": lifecycle_counts,
        "quote_balance_counts": quote_balance_counts,
        "quoting_ratio": float(quoting_ratio),
        "inventory_skewed_ratio": float(inventory_skewed_ratio),
        "defensive_ratio": float(defensive_ratio),
        "unwind_ratio": float(unwind_ratio),
        "emergency_unwind_ratio": float(emergency_ratio),
        "mm_effective_ratio": float(mm_effective_ratio),
        "unwind_target_mismatch_ticks": int(unwind_target_mismatch_ticks),
        "unwind_target_mismatch_sec": float(unwind_target_mismatch_sec),
        "unwind_target_mismatch_ratio": float(mismatch_ratio),
        "harmful_suppressed_ticks": int(harmful_suppressed_ticks),
        "max_non_expiry_none_streak": int(max_non_expiry_none_streak),
        "outside_near_expiry_samples": int(outside_near_expiry_samples),
        "outside_mode_ratios": outside_mode_ratios,
        "mm_effective_share_outside": float(mm_effective_share_outside),
        "max_unwind_ratio_60s_outside": float(outside_mode_ratios.get("unwind", 0.0)),
        "max_emergency_unwind_ratio_60s_outside": float(outside_mode_ratios.get("emergency_unwind", 0.0)),
        "max_quote_none_streak_outside": int(max_quote_none_streak_outside),
        "dual_bid_ratio_outside": float(dual_bid_ratio_outside),
        "max_one_sided_bid_streak_outside": int(max_one_sided_bid_streak_outside),
        "max_collateral_warning_streak_outside": 0,
        "max_sell_skip_cooldown_streak_outside": 0,
        "execution_churn_ratio_60s": 0.0,
        "untradeable_tolerated_material_samples_outside": 0,
        "toxic_buy_present_outside": False,
        "true_drift_present": bool(true_drift_present),
        "halted_present": bool(halted_present),
        "terminal_execution_failed": False,
        "runtime_sec": float(time.time() - started),
        "pass_checks": {
            "quoting_ratio_ge_0_25": bool(quoting_ratio >= 0.25),
            "mm_effective_ratio_ge_0_60": bool(mm_effective_ratio >= 0.60),
            "unwind_ratio_le_0_35": bool(unwind_ratio <= 0.35),
            "unwind_target_mismatch_ratio_lt_0_25": bool(mismatch_ratio < 0.25),
            "max_non_expiry_none_streak_le_3": bool(max_non_expiry_none_streak <= 3),
        },
    }
    result["failed_criteria"] = _single_run_failed_criteria(result)
    result["failure_buckets"] = []
    scenario_bucket = _scenario_failure_bucket(result, str(getattr(args, "scenario_category", "") or ""))
    if result["failed_criteria"] and scenario_bucket:
        result["failure_buckets"] = [scenario_bucket]
    result["primary_blocker"] = _pick_primary_blocker(result["failure_buckets"])
    if result["failed_criteria"] and not result["primary_blocker"]:
        result["failed_criteria"].append("unknown_failure_bucket")
    result["gate_verdict"] = "go" if not result["failed_criteria"] else "no_go"
    result["ok"] = result["gate_verdict"] == "go"

    out_dir = (
        Path(args.output_dir).expanduser().resolve()
        if args.output_dir
        else (REPO_ROOT / "audit" / "replay" / datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ"))
    )
    out_dir.mkdir(parents=True, exist_ok=True)
    (out_dir / "summary.json").write_text(json.dumps(result, ensure_ascii=False, indent=2))
    pd.DataFrame(tick_rows).to_parquet(out_dir / "tick_metrics.parquet", index=False)
    return result


def _namespace_for_scenario(base_args: argparse.Namespace, scenario: dict[str, Any], output_dir: Path) -> argparse.Namespace:
    return argparse.Namespace(
        dataset_root=str(base_args.dataset_root),
        manifest="",
        manifest_mode=str(base_args.manifest_mode),
        up_token_id=str(scenario["up_token_id"]),
        dn_token_id=str(scenario["dn_token_id"]),
        date_from=str(scenario.get("date_from") or base_args.date_from or ""),
        date_to=str(scenario.get("date_to") or base_args.date_to or ""),
        ts_from=int(scenario.get("ts_from") or 0),
        ts_to=int(scenario.get("ts_to") or 0),
        tick_sec=int(scenario.get("tick_sec") or base_args.tick_sec),
        window_sec=float(scenario.get("window_sec") or base_args.window_sec),
        session_budget_usd=float(scenario.get("session_budget_usd") or base_args.session_budget_usd),
        allow_naked_sells=bool(scenario.get("allow_naked_sells", base_args.allow_naked_sells)),
        output_dir=str(output_dir),
        max_ticks=int(scenario.get("max_ticks") or base_args.max_ticks),
        scenario_id=str(scenario.get("id") or output_dir.name),
        scenario_category=str(scenario.get("category") or ""),
    )


def run_manifest_suite(args: argparse.Namespace) -> dict[str, Any]:
    started = time.time()
    manifest_path = Path(args.manifest).expanduser().resolve()
    if not manifest_path.exists():
        raise FileNotFoundError(f"manifest not found: {manifest_path}")
    manifest = _load_manifest(manifest_path)
    scenarios = list(manifest.get("scenarios") or [])
    if str(args.manifest_mode) == "quick":
        quick_scenarios = [scenario for scenario in scenarios if bool(scenario.get("quick", False))]
        scenarios = quick_scenarios or scenarios[: min(3, len(scenarios))]
    output_dir = (
        Path(args.output_dir).expanduser().resolve()
        if args.output_dir
        else (REPO_ROOT / "audit" / "replay" / datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ"))
    )
    output_dir.mkdir(parents=True, exist_ok=True)
    scenario_results: list[dict[str, Any]] = []
    failure_buckets: list[str] = []
    failed_criteria: list[str] = []
    aggregate_pnl_usd = 0.0
    outside_samples_total = 0
    outside_mode_weighted: dict[str, float] = {}
    max_quote_none_streak_outside = 0
    max_unwind_ratio_60s_outside = 0.0
    max_emergency_unwind_ratio_60s_outside = 0.0

    for idx, scenario in enumerate(scenarios, start=1):
        scenario_id = str(scenario.get("id") or f"scenario_{idx:02d}")
        scenario_dir = output_dir / "scenarios" / scenario_id
        scenario_dir.mkdir(parents=True, exist_ok=True)
        scenario_args = _namespace_for_scenario(args, scenario, scenario_dir)
        try:
            summary = run_replay(scenario_args)
        except Exception as exc:
            summary = _scenario_error_summary(
                scenario_id=scenario_id,
                scenario_category=str(scenario.get("category") or ""),
                scenario_dir=scenario_dir,
                error=exc,
                dataset_root=str(scenario_args.dataset_root),
            )
        scenario_results.append(summary)
        aggregate_pnl_usd += float(summary.get("final_pnl_usd") or 0.0)
        outside_samples = int(summary.get("outside_near_expiry_samples") or 0)
        outside_samples_total += outside_samples
        for mode, ratio in (summary.get("outside_mode_ratios") or {}).items():
            outside_mode_weighted[mode] = outside_mode_weighted.get(str(mode), 0.0) + float(ratio) * outside_samples
        max_quote_none_streak_outside = max(
            max_quote_none_streak_outside,
            int(summary.get("max_quote_none_streak_outside") or 0),
        )
        max_unwind_ratio_60s_outside = max(
            max_unwind_ratio_60s_outside,
            float(summary.get("max_unwind_ratio_60s_outside") or 0.0),
        )
        max_emergency_unwind_ratio_60s_outside = max(
            max_emergency_unwind_ratio_60s_outside,
            float(summary.get("max_emergency_unwind_ratio_60s_outside") or 0.0),
        )
        if summary.get("gate_verdict") != "go":
            failed_criteria.append(f"scenario_failed:{scenario_id}")
            failure_buckets.extend(list(summary.get("failure_buckets") or []))

    outside_mode_ratios = {
        mode: (value / float(outside_samples_total) if outside_samples_total > 0 else 0.0)
        for mode, value in outside_mode_weighted.items()
    }
    mm_effective_share_outside = (
        outside_mode_ratios.get("quoting", 0.0)
        + outside_mode_ratios.get("inventory_skewed", 0.0)
        + outside_mode_ratios.get("defensive", 0.0)
    )
    summary = {
        "ok": not failed_criteria,
        "gate_verdict": "go" if not failed_criteria else "no_go",
        "failed_criteria": failed_criteria,
        "failure_buckets": sorted({bucket for bucket in failure_buckets if bucket}),
        "primary_blocker": _pick_primary_blocker(failure_buckets),
        "manifest": str(manifest_path),
        "manifest_mode": str(args.manifest_mode),
        "scenario_count": len(scenario_results),
        "scenario_results": scenario_results,
        "final_pnl_usd": float(scenario_results[-1].get("final_pnl_usd") or 0.0) if scenario_results else 0.0,
        "aggregate_pnl_usd": float(aggregate_pnl_usd),
        "outside_near_expiry_samples": int(outside_samples_total),
        "outside_mode_ratios": outside_mode_ratios,
        "mm_effective_share_outside": float(mm_effective_share_outside),
        "max_unwind_ratio_60s_outside": float(max_unwind_ratio_60s_outside),
        "max_emergency_unwind_ratio_60s_outside": float(max_emergency_unwind_ratio_60s_outside),
        "max_quote_none_streak_outside": int(max_quote_none_streak_outside),
        "execution_churn_ratio_60s": 0.0,
        "runtime_sec": float(time.time() - started),
    }
    if summary["failed_criteria"] and not summary["primary_blocker"]:
        summary["failed_criteria"].append("unknown_failure_bucket")
        summary["ok"] = False
        summary["gate_verdict"] = "no_go"
    (output_dir / "summary.json").write_text(json.dumps(summary, ensure_ascii=False, indent=2), encoding="utf-8")
    return summary


def main() -> int:
    args = _parse_args()
    summary = run_manifest_suite(args) if args.manifest else run_replay(args)
    print(json.dumps(summary, ensure_ascii=False, indent=2))
    return 0 if summary.get("gate_verdict") == "go" else 2


if __name__ == "__main__":
    raise SystemExit(main())
