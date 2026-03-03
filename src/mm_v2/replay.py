from __future__ import annotations

import json
import re
from dataclasses import dataclass
from pathlib import Path
from typing import Any


@dataclass
class ReplayClassification:
    negative_edge_confirmed: bool
    residual_inventory_failure: bool
    one_sided_inventory: bool
    fallback_poll_hot: bool
    flatten_blocked: bool
    summary: str


def load_state_artifact(path: str | Path) -> dict[str, Any]:
    return json.loads(Path(path).read_text())


def discover_artifact_dirs(root: str | Path = "audit") -> list[Path]:
    base = Path(root)
    if not base.exists():
        return []
    dirs: list[Path] = []
    for entry in sorted(base.iterdir()):
        if not entry.is_dir():
            continue
        raw = entry / "last_failed_window_state_raw.json"
        fills = entry / "last_failed_window_recent_fills.tsv"
        if raw.exists() and fills.exists():
            dirs.append(entry)
    return dirs


def load_replay_bundle(path: str | Path) -> dict[str, Any]:
    base = Path(path)
    if base.is_file():
        base = base.parent
    bundle: dict[str, Any] = {"base": str(base)}
    files = {
        "state_raw": "last_failed_window_state_raw.json",
        "state_summary": "last_failed_window_state_summary.json",
        "fills_tsv": "last_failed_window_recent_fills.tsv",
        "logs_tsv": "last_failed_window_logs_window_slice.tsv",
    }
    for key, name in files.items():
        target = base / name
        if not target.exists():
            bundle[key] = None
            continue
        if target.suffix == ".json":
            bundle[key] = json.loads(target.read_text())
        else:
            bundle[key] = target.read_text()
    return bundle


def classify_state_artifact(payload: dict[str, Any]) -> ReplayClassification:
    analytics = payload.get("analytics") or {}
    negative_edge = payload.get("negative_edge_guard") or {}
    liquidation = payload.get("liquidation") or {}
    pair_inventory = payload.get("pair_inventory") or {}
    order_tracking = payload.get("order_tracking") or {}

    if not pair_inventory:
        legacy_paired = payload.get("paired_inventory") or {}
        pair_inventory = {
            "excess_up_qty": legacy_paired.get("q_excess_up", 0.0),
            "excess_dn_qty": legacy_paired.get("q_excess_dn", 0.0),
        }

    one_sided = bool(
        abs(float(pair_inventory.get("excess_up_qty", 0.0) or 0.0)) > 0.5
        or abs(float(pair_inventory.get("excess_dn_qty", 0.0) or 0.0)) > 0.5
        or abs(float(payload.get("net_delta", 0.0) or 0.0)) > 0.5
    )

    fallback_count = int(order_tracking.get("last_fallback_poll_count", 0) or 0)
    fallback_poll_hot = fallback_count >= 12

    spread_capture = float(analytics.get("spread_capture_usd", 0.0) or 0.0)
    if spread_capture == 0.0:
        pnl_decomp = payload.get("pnl_decomposition") or {}
        spread_capture = float(
            (((pnl_decomp.get("components") or {}).get("spread_capture") or {}).get("total_usd", 0.0) or 0.0)
        )
    markout = payload.get("markout_tca") or {}
    avg_markout_5s = float(markout.get("avg_markout_5s", 0.0) or 0.0)
    adverse_pct_5s = float(markout.get("adverse_pct_5s", 0.0) or 0.0)
    session_pnl = float(payload.get("session_pnl", analytics.get("session_pnl", 0.0)) or 0.0)
    negative_edge_confirmed = bool(negative_edge.get("active")) or (
        session_pnl < 0.0
        and spread_capture < 0.0
        and (avg_markout_5s < 0.0 or adverse_pct_5s >= 25.0)
    )

    inventory = payload.get("inventory") or {}
    inv_up = float(inventory.get("up_shares", 0.0) or 0.0)
    inv_dn = float(inventory.get("dn_shares", 0.0) or 0.0)
    active_orders = int(payload.get("active_orders", 0) or 0)
    residual_failure = bool(liquidation.get("residual_inventory_failure")) or (
        active_orders == 0 and (inv_up > 0.5 or inv_dn > 0.5)
    )
    flatten_blocked = active_orders == 0 and one_sided and (inv_up > 0.5 or inv_dn > 0.5)
    summary = (
        f"negative_edge={negative_edge_confirmed} "
        f"residual={residual_failure} one_sided={one_sided} "
        f"flatten_blocked={flatten_blocked} fallback_hot={fallback_poll_hot}"
    )
    return ReplayClassification(
        negative_edge_confirmed=negative_edge_confirmed,
        residual_inventory_failure=residual_failure,
        one_sided_inventory=one_sided,
        fallback_poll_hot=fallback_poll_hot,
        flatten_blocked=flatten_blocked,
        summary=summary,
    )


def classify_replay_bundle(bundle: dict[str, Any]) -> ReplayClassification:
    payload = bundle.get("state_raw") or {}
    result = classify_state_artifact(payload)
    logs_tsv = str(bundle.get("logs_tsv") or "")
    polled_counts = [int(m.group(1)) for m in re.finditer(r"HTTP fallback: polling (\d+) tracked orders", logs_tsv)]
    if not result.fallback_poll_hot and polled_counts and max(polled_counts) >= 12:
        result = ReplayClassification(
            negative_edge_confirmed=result.negative_edge_confirmed,
            residual_inventory_failure=result.residual_inventory_failure,
            one_sided_inventory=result.one_sided_inventory,
            fallback_poll_hot=True,
            flatten_blocked=result.flatten_blocked,
            summary=result.summary + " fallback_hot=True(logs)",
        )
    return result
