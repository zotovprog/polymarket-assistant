"""Heartbeat ID synchronization regressions."""

from __future__ import annotations

import os
import sys
import uuid

import pytest

BASE = os.path.dirname(os.path.dirname(__file__))
SRC = os.path.join(BASE, "src")
if SRC not in sys.path:
    sys.path.insert(0, SRC)

from mm.heartbeat import HeartbeatManager


class _HeartbeatResponseClient:
    """Client that rotates heartbeat_id in successful responses."""

    def __init__(self, returned_ids: list[str]):
        self.returned_ids = list(returned_ids)
        self.calls: list[str] = []

    def post_heartbeat(self, heartbeat_id: str):
        self.calls.append(heartbeat_id)
        idx = min(len(self.calls) - 1, len(self.returned_ids) - 1)
        return {"heartbeat_id": self.returned_ids[idx]}


class _NoCallClient:
    """Client that should not be called when heartbeat is intentionally skipped."""

    def __init__(self):
        self.calls = 0

    def post_heartbeat(self, *_args, **_kwargs):
        self.calls += 1
        raise AssertionError("post_heartbeat should not be called")


@pytest.mark.anyio
async def test_heartbeat_adopts_id_from_success_response():
    hb_next_1 = str(uuid.uuid4())
    hb_next_2 = str(uuid.uuid4())
    client = _HeartbeatResponseClient([hb_next_1, hb_next_2])

    hb = HeartbeatManager(client, interval_sec=5)
    initial_id = hb._heartbeat_id

    ok1 = await hb._send_heartbeat()
    ok2 = await hb._send_heartbeat()

    assert ok1 is True
    assert ok2 is True
    assert client.calls[0] == initial_id
    assert client.calls[1] == hb_next_1
    assert hb._heartbeat_id == hb_next_2


@pytest.mark.anyio
async def test_heartbeat_skip_when_disabled_by_callback():
    client = _NoCallClient()
    hb = HeartbeatManager(client, interval_sec=5, should_send=lambda: False)

    ok = await hb._send_heartbeat()

    assert ok is True
    assert client.calls == 0
    assert hb.stats["skip_count"] == 1
