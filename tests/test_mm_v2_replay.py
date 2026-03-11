from __future__ import annotations

import json
import os
import subprocess
import sys
from pathlib import Path


BASE = Path(__file__).resolve().parent.parent
SRC = BASE / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))
if str(BASE) not in sys.path:
    sys.path.insert(0, str(BASE))

from mm_v2.replay import classify_replay_bundle, discover_artifact_dirs, load_replay_bundle


FIXTURES = BASE / "tests" / "fixtures" / "mm_v2_replay"
FAILURE_MATRIX_FIXTURES = BASE / "tests" / "fixtures" / "mm_v2_failure_matrix"
REPLAY_GATE_TOOL = BASE / "tools" / "mmv2_replay_gate_check.py"
DATASET_REPLAY_TOOL = BASE / "tools" / "mmv2_replay_strategy_orderfills.py"
DATASET_MANIFEST = BASE / "data" / "replay" / "mmv2_dataset_scenarios.json"


def test_replay_discovers_committed_fixture_artifacts():
    dirs = discover_artifact_dirs(FIXTURES)
    assert dirs, "expected at least one audit artifact directory"
    assert any(d.name == "2026-03-02_23-23-54" for d in dirs)


def test_replay_classifies_known_bad_window_from_committed_fixtures():
    bundle = load_replay_bundle(FIXTURES / "2026-03-02_23-23-54")
    result = classify_replay_bundle(bundle)
    assert result.negative_edge_confirmed is True
    assert result.one_sided_inventory is True
    assert result.flatten_blocked is True
    assert result.residual_inventory_failure is True
    assert result.fallback_poll_hot is True
    assert "negative_edge=True" in result.summary


def test_failure_matrix_fixture_manifest_buckets_as_expected(tmp_path):
    manifest = json.loads((FAILURE_MATRIX_FIXTURES / "manifest.json").read_text(encoding="utf-8"))
    for case in manifest["cases"]:
        proc = subprocess.run(
            [
                sys.executable,
                str(REPLAY_GATE_TOOL),
                "--input-dir",
                str((FAILURE_MATRIX_FIXTURES / case["input_dir"]).resolve()),
                "--output-root",
                str((tmp_path / case["id"]).resolve()),
            ],
            cwd=str(BASE),
            capture_output=True,
            text=True,
        )
        assert proc.returncode in (0, 2), proc.stderr
        summary = json.loads(proc.stdout)
        assert summary["gate_verdict"] == case["expected_gate_verdict"]
        assert summary["primary_blocker"] == case["expected_primary_blocker"]
        assert "unknown_failure_bucket" not in summary.get("failed_criteria", [])


def test_dataset_manifest_quick_suite_runs_and_writes_unified_summary(tmp_path):
    out_dir = tmp_path / "dataset-suite"
    proc = subprocess.run(
        [
            sys.executable,
            str(DATASET_REPLAY_TOOL),
            "--manifest",
            str(DATASET_MANIFEST),
            "--manifest-mode",
            "quick",
            "--output-dir",
            str(out_dir),
        ],
        cwd=str(BASE),
        capture_output=True,
        text=True,
    )
    assert proc.returncode in (0, 2), proc.stderr
    summary = json.loads(proc.stdout)
    assert summary["scenario_count"] >= 1
    assert "gate_verdict" in summary
    assert "failed_criteria" in summary
    assert "failure_buckets" in summary
    assert "primary_blocker" in summary
    assert "aggregate_pnl_usd" in summary
    assert "outside_mode_ratios" in summary
    assert (out_dir / "summary.json").exists()


def test_dataset_manifest_quick_suite_with_missing_dataset_root_returns_structured_summary(tmp_path):
    out_dir = tmp_path / "dataset-suite-missing-root"
    proc = subprocess.run(
        [
            sys.executable,
            str(DATASET_REPLAY_TOOL),
            "--dataset-root",
            str((tmp_path / "missing-dataset-root").resolve()),
            "--manifest",
            str(DATASET_MANIFEST),
            "--manifest-mode",
            "quick",
            "--output-dir",
            str(out_dir),
        ],
        cwd=str(BASE),
        capture_output=True,
        text=True,
    )
    assert proc.returncode in (0, 2), proc.stderr
    summary = json.loads(proc.stdout)
    assert "gate_verdict" in summary
    assert "failed_criteria" in summary
    assert "failure_buckets" in summary
    assert "primary_blocker" in summary
    assert (out_dir / "summary.json").exists()
