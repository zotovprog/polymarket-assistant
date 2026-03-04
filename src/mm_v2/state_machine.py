from __future__ import annotations

import time

from .config import (
    ENTER_CONFIRM_TICKS,
    EXIT_CONFIRM_TICKS,
    FOUR_QUOTE_MIN_RATIO_FOR_MM,
    MMConfigV2,
    NO_HELPFUL_TICKS_FOR_UNWIND,
    UNWIND_MIN_PROGRESS_RATIO,
    UNWIND_STUCK_WINDOW_SEC,
)
from .types import PairInventoryState, PairMarketSnapshot, QuoteViabilitySummary, RiskRegime, SoftTransitionResult


class StateMachineV2:
    _SOFT_ORDER = {
        "quoting": 0,
        "inventory_skewed": 1,
        "defensive": 2,
        "unwind": 3,
    }

    def __init__(self, config: MMConfigV2):
        self.config = config
        self.lifecycle = "bootstrapping"
        self._entered_at = time.time()
        self._emergency_started_at = 0.0
        self._unwind_started_at = 0.0
        self._target_soft_mode = "normal"
        self._target_soft_mode_ticks = 0
        self._healthy_ticks = 0
        self._excess_baseline_value_usd = 0.0
        self._excess_baseline_ts = 0.0
        self._no_helpful_ticks = 0

    def seconds_in_mode(self) -> float:
        return max(0.0, time.time() - self._entered_at)

    def emergency_seconds(self) -> float:
        if self._emergency_started_at <= 0:
            return 0.0
        return max(0.0, time.time() - self._emergency_started_at)

    @classmethod
    def _target_lifecycle(cls, soft_mode: str) -> str:
        if soft_mode == "inventory_skewed":
            return "inventory_skewed"
        if soft_mode == "defensive":
            return "defensive"
        if soft_mode == "unwind":
            return "unwind"
        return "quoting"

    @classmethod
    def _soft_level(cls, lifecycle: str) -> int:
        return cls._SOFT_ORDER.get(lifecycle, 0)

    def _set_lifecycle(self, next_state: str) -> None:
        if next_state == self.lifecycle:
            return
        self.lifecycle = next_state
        self._entered_at = time.time()
        if next_state == "unwind":
            self._unwind_started_at = time.time()
        if next_state == "emergency_unwind":
            self._emergency_started_at = time.time()
        if next_state in {"quoting", "inventory_skewed", "defensive"}:
            self._healthy_ticks = 0

    @staticmethod
    def _effective_soft_mode(lifecycle: str) -> str:
        if lifecycle == "inventory_skewed":
            return "inventory_skewed"
        if lifecycle == "defensive":
            return "defensive"
        if lifecycle == "unwind":
            return "unwind"
        return "normal"

    def _refresh_progress_baseline(self, inventory: PairInventoryState) -> None:
        now = time.time()
        self._excess_baseline_value_usd = float(inventory.excess_value_usd)
        self._excess_baseline_ts = now

    def transition(
        self,
        *,
        snapshot: PairMarketSnapshot | None,
        inventory: PairInventoryState,
        risk: RiskRegime,
        viability: QuoteViabilitySummary | None = None,
    ) -> SoftTransitionResult:
        now = time.time()
        next_state = self.lifecycle
        viability = viability or QuoteViabilitySummary()
        progress_ratio = 0.0
        no_progress = False
        reason = ""
        has_material_position = inventory.up_shares > 0.5 or inventory.dn_shares > 0.5
        if snapshot is None:
            self._set_lifecycle("bootstrapping")
            return SoftTransitionResult(
                lifecycle=self.lifecycle,  # type: ignore[arg-type]
                effective_soft_mode="normal",
                target_soft_mode=getattr(risk, "target_soft_mode", risk.soft_mode),  # type: ignore[arg-type]
            )
        if snapshot.time_left_sec <= 0:
            self._set_lifecycle("expired")
            return SoftTransitionResult(
                lifecycle=self.lifecycle,  # type: ignore[arg-type]
                effective_soft_mode="normal",
                target_soft_mode=getattr(risk, "target_soft_mode", risk.soft_mode),  # type: ignore[arg-type]
            )
        if risk.hard_mode == "halted":
            self._set_lifecycle("halted")
            return SoftTransitionResult(
                lifecycle=self.lifecycle,  # type: ignore[arg-type]
                effective_soft_mode=self._effective_soft_mode(self.lifecycle),  # type: ignore[arg-type]
                target_soft_mode=getattr(risk, "target_soft_mode", risk.soft_mode),  # type: ignore[arg-type]
                reason=risk.reason,
            )
        if risk.hard_mode == "emergency_unwind":
            self._set_lifecycle("emergency_unwind")
            return SoftTransitionResult(
                lifecycle=self.lifecycle,  # type: ignore[arg-type]
                effective_soft_mode=self._effective_soft_mode(self.lifecycle),  # type: ignore[arg-type]
                target_soft_mode=getattr(risk, "target_soft_mode", risk.soft_mode),  # type: ignore[arg-type]
                reason=risk.reason,
            )

        target_soft_mode = getattr(risk, "target_soft_mode", risk.soft_mode)
        target_lifecycle = self._target_lifecycle(target_soft_mode)
        if target_lifecycle != self._target_soft_mode:
            self._target_soft_mode = target_lifecycle
            self._target_soft_mode_ticks = 1
        else:
            self._target_soft_mode_ticks += 1

        current_level = self._soft_level(self.lifecycle)
        target_level = self._soft_level(target_lifecycle)

        if self.lifecycle == "bootstrapping":
            next_state = "quoting"
            self._refresh_progress_baseline(inventory)
        elif target_level > current_level:
            self._healthy_ticks = 0
            if self._target_soft_mode_ticks >= ENTER_CONFIRM_TICKS:
                for name, level in self._SOFT_ORDER.items():
                    if level == current_level + 1:
                        next_state = name
                        break
        elif target_level < current_level:
            baseline = self._excess_baseline_value_usd if self._excess_baseline_ts > 0 else float(inventory.excess_value_usd)
            # Unwind should be able to de-escalate when soft target is already lower.
            # Quality pressure may remain high in defensive markets, but if target mode
            # dropped below unwind and excess is not worsening, do not pin lifecycle.
            quality_allows_exit = (
                float(risk.quality_pressure) < 1.0
                or self.lifecycle == "unwind"
            )
            is_healthy = (
                target_level < current_level
                and float(inventory.excess_value_usd) <= max(0.0, baseline * 1.02)
                and quality_allows_exit
            )
            if is_healthy:
                self._healthy_ticks += 1
            else:
                self._healthy_ticks = 0
            if self._healthy_ticks >= EXIT_CONFIRM_TICKS:
                for name, level in self._SOFT_ORDER.items():
                    if level == current_level - 1:
                        next_state = name
                        break
        else:
            self._healthy_ticks = 0

        if next_state in {"inventory_skewed", "defensive", "unwind"}:
            if self._excess_baseline_ts <= 0:
                self._refresh_progress_baseline(inventory)
            elif float(inventory.excess_value_usd) < self._excess_baseline_value_usd * 0.95:
                self._refresh_progress_baseline(inventory)
            elif (
                next_state == "defensive"
                and now - self._excess_baseline_ts >= UNWIND_STUCK_WINDOW_SEC
            ):
                if self._excess_baseline_value_usd > 0:
                    progress_ratio = max(
                        0.0,
                        (self._excess_baseline_value_usd - float(inventory.excess_value_usd))
                        / self._excess_baseline_value_usd,
                    )
                no_progress = progress_ratio < UNWIND_MIN_PROGRESS_RATIO
                # For flat inventory there is no "helpful" side by design.
                # Missing helpful quotes must not force unwind escalation.
                missing_helpful_actionable = (
                    risk.inventory_side != "flat"
                    and viability.helpful_count == 0
                )
                if missing_helpful_actionable:
                    self._no_helpful_ticks += 1
                else:
                    self._no_helpful_ticks = 0
                if no_progress and (
                    self._no_helpful_ticks >= NO_HELPFUL_TICKS_FOR_UNWIND
                    or (
                        missing_helpful_actionable
                        and viability.four_quote_presence_ratio < FOUR_QUOTE_MIN_RATIO_FOR_MM
                    )
                ):
                    next_state = "unwind"
                    self._target_soft_mode = "unwind"
                    self._target_soft_mode_ticks = ENTER_CONFIRM_TICKS
                    self._healthy_ticks = 0
                    self._refresh_progress_baseline(inventory)
                    reason = "no progress in defensive mode"

        if not has_material_position and next_state in {"unwind", "emergency_unwind"}:
            next_state = "quoting"
            self._no_helpful_ticks = 0
        self._set_lifecycle(next_state)
        if self.lifecycle == "quoting":
            self._refresh_progress_baseline(inventory)
        return SoftTransitionResult(
            lifecycle=self.lifecycle,  # type: ignore[arg-type]
            effective_soft_mode=self._effective_soft_mode(self.lifecycle),  # type: ignore[arg-type]
            target_soft_mode=target_soft_mode,  # type: ignore[arg-type]
            progress_ratio=progress_ratio,
            no_progress=no_progress,
            reason=reason or risk.reason,
        )
