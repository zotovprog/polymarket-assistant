from __future__ import annotations

import time

from .config import MMConfigV2
from .types import PairInventoryState, PairMarketSnapshot, RiskRegime


class StateMachineV2:
    def __init__(self, config: MMConfigV2):
        self.config = config
        self.lifecycle = "bootstrapping"
        self._entered_at = time.time()
        self._emergency_started_at = 0.0
        self._unwind_started_at = 0.0

    def seconds_in_mode(self) -> float:
        return max(0.0, time.time() - self._entered_at)

    def emergency_seconds(self) -> float:
        if self._emergency_started_at <= 0:
            return 0.0
        return max(0.0, time.time() - self._emergency_started_at)

    def transition(
        self,
        *,
        snapshot: PairMarketSnapshot | None,
        inventory: PairInventoryState,
        risk: RiskRegime,
    ) -> str:
        next_state = self.lifecycle
        has_material_position = inventory.up_shares > 0.5 or inventory.dn_shares > 0.5
        if snapshot is None:
            next_state = "bootstrapping"
        elif snapshot.time_left_sec <= 0:
            next_state = "expired"
        elif risk.hard_mode == "halted":
            next_state = "halted"
        elif risk.hard_mode == "emergency_unwind":
            next_state = "emergency_unwind"
        elif risk.soft_mode == "unwind":
            next_state = "unwind"
        elif risk.soft_mode == "defensive":
            next_state = "defensive"
        elif risk.soft_mode == "inventory_skewed":
            next_state = "inventory_skewed"
        else:
            next_state = "quoting"
        if not has_material_position and next_state in {"unwind", "emergency_unwind"}:
            next_state = "quoting"
        if next_state != self.lifecycle:
            self.lifecycle = next_state
            self._entered_at = time.time()
            if next_state == "unwind":
                self._unwind_started_at = time.time()
            if next_state == "emergency_unwind":
                self._emergency_started_at = time.time()
        return self.lifecycle
