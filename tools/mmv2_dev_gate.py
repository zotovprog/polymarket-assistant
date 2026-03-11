#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import subprocess
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

PRIMARY_BLOCKER_PRIORITY = [
    "drift_transport",
    "terminal_execution",
    "marketability_churn",
    "edge_divergence",
    "inventory_regime",
]


def _parse_args() -> argparse.Namespace:
    repo_root = Path(__file__).resolve().parents[1]
    parser = argparse.ArgumentParser(description="Unified dataset-first dev gate for MM V2.")
    parser.add_argument("--mode", choices=("quick", "full"), default="quick")
    parser.add_argument(
        "--fixture-manifest",
        default=str(repo_root / "tests" / "fixtures" / "mm_v2_failure_matrix" / "manifest.json"),
    )
    parser.add_argument(
        "--dataset-manifest",
        default=str(repo_root / "data" / "replay" / "mmv2_dataset_scenarios.json"),
    )
    parser.add_argument("--audit-root", default=str(repo_root / "audit"))
    parser.add_argument("--output-root", default=str(repo_root / "audit" / "dev-gate"))
    parser.add_argument("--base-url", default="http://127.0.0.1:8000")
    parser.add_argument("--key", default="")
    parser.add_argument("--coin", default="BTC")
    parser.add_argument("--timeframe", default="15m")
    parser.add_argument("--budget", type=float, default=30.0)
    parser.add_argument("--poll-sec", type=float, default=5.0)
    parser.add_argument("--local-paper-duration-sec", type=int, default=2700)
    parser.add_argument("--skip-local-paper", action="store_true")
    return parser.parse_args()


def _run_json_command(cmd: list[str], *, cwd: Path) -> tuple[int, dict[str, Any]]:
    proc = subprocess.run(cmd, cwd=str(cwd), capture_output=True, text=True)
    if proc.returncode not in (0, 2):
        raise RuntimeError(
            f"command failed ({proc.returncode}): {' '.join(cmd)}\nSTDOUT:\n{proc.stdout}\nSTDERR:\n{proc.stderr}"
        )
    stdout = proc.stdout.strip()
    if not stdout:
        raise RuntimeError(f"command produced no JSON output: {' '.join(cmd)}")
    try:
        return proc.returncode, json.loads(stdout)
    except json.JSONDecodeError as exc:
        raise RuntimeError(
            f"command did not return JSON: {' '.join(cmd)}\nSTDOUT:\n{stdout}\nSTDERR:\n{proc.stderr}"
        ) from exc


def _pick_primary_blocker(buckets: list[str]) -> str:
    bucket_set = {str(bucket) for bucket in buckets if str(bucket)}
    for bucket in PRIMARY_BLOCKER_PRIORITY:
        if bucket in bucket_set:
            return bucket
    return ""


def _latest_dir(root: Path, prefix: str) -> Path | None:
    candidates = [p for p in root.iterdir() if p.is_dir() and p.name.startswith(prefix)]
    if not candidates:
        return None
    return max(candidates, key=lambda p: p.stat().st_mtime)


def _load_fixture_manifest(path: Path) -> list[dict[str, Any]]:
    payload = json.loads(path.read_text(encoding="utf-8"))
    cases = payload.get("cases")
    if not isinstance(cases, list) or not cases:
        raise RuntimeError(f"fixture manifest has no cases: {path}")
    return [dict(case) for case in cases if isinstance(case, dict)]


def _run_fixture_stage(repo_root: Path, out_dir: Path, manifest_path: Path) -> dict[str, Any]:
    cases = _load_fixture_manifest(manifest_path)
    script = repo_root / "tools" / "mmv2_replay_gate_check.py"
    case_results: list[dict[str, Any]] = []
    failed_criteria: list[str] = []

    for case in cases:
        case_id = str(case["id"])
        input_dir = (manifest_path.parent / str(case["input_dir"])).resolve()
        cmd = [
            sys.executable,
            str(script),
            "--input-dir",
            str(input_dir),
            "--output-root",
            str(out_dir / case_id),
        ]
        _, summary = _run_json_command(cmd, cwd=repo_root)
        matched = (
            str(summary.get("gate_verdict") or "") == str(case.get("expected_gate_verdict") or "")
            and str(summary.get("primary_blocker") or "") == str(case.get("expected_primary_blocker") or "")
            and "unknown_failure_bucket" not in list(summary.get("failed_criteria") or [])
        )
        case_results.append(
            {
                "id": case_id,
                "input_dir": str(input_dir),
                "expected_gate_verdict": str(case.get("expected_gate_verdict") or ""),
                "expected_primary_blocker": str(case.get("expected_primary_blocker") or ""),
                "actual_gate_verdict": str(summary.get("gate_verdict") or ""),
                "actual_primary_blocker": str(summary.get("primary_blocker") or ""),
                "matched": bool(matched),
            }
        )
        if not matched:
            failed_criteria.append(f"fixture_matrix_mismatch:{case_id}")

    return {
        "stage": "fixture_replay",
        "ok": not failed_criteria,
        "gate_verdict": "go" if not failed_criteria else "no_go",
        "failed_criteria": failed_criteria,
        "failure_buckets": [],
        "primary_blocker": "",
        "case_results": case_results,
    }


def _run_artifact_stage(repo_root: Path, out_dir: Path, audit_root: Path) -> dict[str, Any]:
    script = repo_root / "tools" / "mmv2_replay_gate_check.py"
    targets: list[tuple[str, Path]] = []
    latest_mongo = _latest_dir(audit_root, "mongo-last-run-")
    local_paper_root = audit_root / "local-paper"
    latest_local = _latest_dir(local_paper_root, "") if local_paper_root.exists() else None
    if latest_mongo:
        targets.append(("latest_mongo", latest_mongo))
    if latest_local:
        targets.append(("latest_local_paper", latest_local))
    if not targets:
        return {
            "stage": "artifact_replay",
            "ok": False,
            "gate_verdict": "no_go",
            "failed_criteria": ["no_artifact_inputs_found"],
            "failure_buckets": [],
            "primary_blocker": "",
            "artifact_results": [],
        }

    artifact_results: list[dict[str, Any]] = []
    failed_criteria: list[str] = []
    for name, input_dir in targets:
        cmd = [
            sys.executable,
            str(script),
            "--input-dir",
            str(input_dir),
            "--output-root",
            str(out_dir / name),
        ]
        _, summary = _run_json_command(cmd, cwd=repo_root)
        artifact_results.append({"name": name, "input_dir": str(input_dir), "summary": summary})
        if "unknown_failure_bucket" in list(summary.get("failed_criteria") or []):
            failed_criteria.append(f"unknown_failure_bucket:{name}")
        if summary.get("gate_verdict") == "no_go" and not str(summary.get("primary_blocker") or ""):
            failed_criteria.append(f"missing_primary_blocker:{name}")

    return {
        "stage": "artifact_replay",
        "ok": not failed_criteria,
        "gate_verdict": "go" if not failed_criteria else "no_go",
        "failed_criteria": failed_criteria,
        "failure_buckets": [],
        "primary_blocker": "",
        "artifact_results": artifact_results,
    }


def _run_dataset_stage(repo_root: Path, out_dir: Path, args: argparse.Namespace) -> dict[str, Any]:
    script = repo_root / "tools" / "mmv2_replay_strategy_orderfills.py"
    cmd = [
        sys.executable,
        str(script),
        "--manifest",
        str(Path(args.dataset_manifest).expanduser().resolve()),
        "--manifest-mode",
        str(args.mode),
        "--output-dir",
        str(out_dir),
    ]
    _, summary = _run_json_command(cmd, cwd=repo_root)
    summary = dict(summary)
    summary["stage"] = "dataset_replay"
    return summary


def _run_local_paper_stage(repo_root: Path, out_dir: Path, args: argparse.Namespace) -> dict[str, Any]:
    script = repo_root / "tools" / "mmv2_local_paper_check.py"
    cmd = [
        sys.executable,
        str(script),
        "--base-url",
        str(args.base_url),
        "--coin",
        str(args.coin),
        "--timeframe",
        str(args.timeframe),
        "--budget",
        str(float(args.budget)),
        "--duration-sec",
        str(int(args.local_paper_duration_sec)),
        "--poll-sec",
        str(float(args.poll_sec)),
        "--stop-at-end",
        "--output-root",
        str(out_dir),
    ]
    if str(args.key or "").strip():
        cmd.extend(["--key", str(args.key).strip()])
    _, summary = _run_json_command(cmd, cwd=repo_root)
    summary = dict(summary)
    summary["stage"] = "local_paper"
    return summary


def main() -> int:
    args = _parse_args()
    repo_root = Path(__file__).resolve().parents[1]
    audit_root = Path(args.audit_root).expanduser().resolve()
    output_root = Path(args.output_root).expanduser().resolve()
    ts = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    out_dir = output_root / ts
    out_dir.mkdir(parents=True, exist_ok=True)

    stages: list[dict[str, Any]] = []
    failed_criteria: list[str] = []
    failure_buckets: list[str] = []

    fixture_stage = _run_fixture_stage(
        repo_root,
        out_dir / "fixture-replay",
        Path(args.fixture_manifest).expanduser().resolve(),
    )
    stages.append(fixture_stage)
    if fixture_stage.get("gate_verdict") != "go":
        failed_criteria.extend([f"fixture_replay:{item}" for item in fixture_stage.get("failed_criteria", [])])

    artifact_stage = _run_artifact_stage(repo_root, out_dir / "artifact-replay", audit_root)
    stages.append(artifact_stage)
    if artifact_stage.get("gate_verdict") != "go":
        failed_criteria.extend([f"artifact_replay:{item}" for item in artifact_stage.get("failed_criteria", [])])

    dataset_stage = _run_dataset_stage(repo_root, out_dir / "dataset-replay", args)
    stages.append(dataset_stage)
    if dataset_stage.get("gate_verdict") != "go":
        failed_criteria.extend([f"dataset_replay:{item}" for item in dataset_stage.get("failed_criteria", [])])
        failure_buckets.extend(list(dataset_stage.get("failure_buckets") or []))

    if args.mode == "full" and not args.skip_local_paper and not failed_criteria:
        local_stage = _run_local_paper_stage(repo_root, out_dir / "local-paper", args)
        stages.append(local_stage)
        if local_stage.get("gate_verdict") != "go":
            failed_criteria.extend([f"local_paper:{item}" for item in local_stage.get("failed_criteria", [])])
            failure_buckets.extend(list(local_stage.get("failure_buckets") or []))
    elif args.mode == "full" and not args.skip_local_paper:
        stages.append(
            {
                "stage": "local_paper",
                "ok": False,
                "gate_verdict": "no_go",
                "failed_criteria": ["skipped_due_to_prior_stage_failure"],
                "failure_buckets": [],
                "primary_blocker": "",
            }
        )

    primary_blocker = _pick_primary_blocker(failure_buckets)
    summary = {
        "ok": not failed_criteria,
        "gate_verdict": "go" if not failed_criteria else "no_go",
        "mode": str(args.mode),
        "failed_criteria": failed_criteria,
        "failure_buckets": sorted({bucket for bucket in failure_buckets if bucket}),
        "primary_blocker": primary_blocker,
        "stages": stages,
        "final_pnl_usd": float(stages[-1].get("final_pnl_usd") or 0.0) if stages else 0.0,
        "aggregate_pnl_usd": float(
            sum(float(stage.get("aggregate_pnl_usd") or 0.0) for stage in stages if isinstance(stage, dict))
        ),
        "output_dir": str(out_dir),
    }
    if summary["failed_criteria"] and not summary["primary_blocker"] and summary["failure_buckets"]:
        summary["failed_criteria"].append("unknown_failure_bucket")
        summary["ok"] = False
        summary["gate_verdict"] = "no_go"

    (out_dir / "summary.json").write_text(json.dumps(summary, ensure_ascii=False, indent=2), encoding="utf-8")
    print(json.dumps(summary, ensure_ascii=False, indent=2))
    return 0 if summary["gate_verdict"] == "go" else 2


if __name__ == "__main__":
    raise SystemExit(main())
