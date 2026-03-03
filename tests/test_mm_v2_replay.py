from __future__ import annotations

import os
import sys
from pathlib import Path


BASE = Path(__file__).resolve().parent.parent
SRC = BASE / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))
if str(BASE) not in sys.path:
    sys.path.insert(0, str(BASE))

from mm_v2.replay import classify_replay_bundle, discover_artifact_dirs, load_replay_bundle


def test_replay_discovers_real_audit_artifacts():
    dirs = discover_artifact_dirs(BASE / "audit")
    assert dirs, "expected at least one audit artifact directory"
    assert any(d.name == "2026-03-02_23-23-54" for d in dirs)


def test_replay_classifies_known_bad_window_from_real_artifacts():
    bundle = load_replay_bundle(BASE / "audit" / "2026-03-02_23-23-54")
    result = classify_replay_bundle(bundle)
    assert result.negative_edge_confirmed is True
    assert result.one_sided_inventory is True
    assert result.flatten_blocked is True
    assert result.residual_inventory_failure is True
    assert result.fallback_poll_hot is True
    assert "negative_edge=True" in result.summary
