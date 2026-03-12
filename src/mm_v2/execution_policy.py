from __future__ import annotations

import time
from typing import Any

from .order_tracker import OrderTrackerV2
from .pm_gateway import PMGateway
from .types import QuoteIntent, QuotePlan


class ExecutionPolicyV2:
    SELL_HOLD_REASONS = {"sell_churn_hold_mode", "sell_reprice_hold_mode"}

    def __init__(self, gateway: PMGateway, tracker: OrderTrackerV2, *, requote_threshold_bps: float = 15.0):
        self.gateway = gateway
        self.tracker = tracker
        self.requote_threshold_bps = max(1.0, float(requote_threshold_bps))
        self._sell_churn_hold_reprice_suppressed_hits = 0
        self._sell_churn_hold_cancel_avoided_hits = 0

    def _materially_different(self, current: QuoteIntent, new: QuoteIntent) -> bool:
        if current.side != new.side or current.token != new.token or current.post_only != new.post_only:
            return True
        effective_threshold_bps = float(self.requote_threshold_bps)
        if current.side == "BUY":
            effective_threshold_bps *= 0.6
        price_diff_bps = abs(float(current.price) - float(new.price)) / max(0.01, float(current.price)) * 10000.0
        size_diff = abs(float(current.size) - float(new.size))
        return price_diff_bps >= effective_threshold_bps or size_diff >= 0.5

    @staticmethod
    def _should_hold_existing(existing: Any, desired: QuoteIntent) -> bool:
        min_rest_sec = float(getattr(desired, "min_rest_sec", 0.0) or 0.0)
        current_intent = getattr(existing, "intent", None)
        created_at = float(getattr(existing, "created_at", 0.0) or 0.0)
        if current_intent is None or created_at <= 0.0:
            return False
        if current_intent.side != desired.side or current_intent.token != desired.token:
            return False
        if current_intent.post_only != desired.post_only:
            return False
        if desired.side != "SELL":
            return False
        if str(getattr(current_intent, "quote_role", "") or "") != str(getattr(desired, "quote_role", "") or ""):
            return False
        age_sec = max(0.0, float(time.time()) - created_at)
        if bool(getattr(desired, "hold_mode_active", False)):
            current_hold_reason = str(getattr(current_intent, "hold_mode_reason", "") or "")
            desired_hold_reason = str(getattr(desired, "hold_mode_reason", "") or "")
            sell_hold_mode = (
                current_hold_reason in ExecutionPolicyV2.SELL_HOLD_REASONS
                or desired_hold_reason in ExecutionPolicyV2.SELL_HOLD_REASONS
            )
            if not sell_hold_mode:
                if str(getattr(current_intent, "inventory_effect", "") or "") != "helpful":
                    return False
                if str(getattr(desired, "inventory_effect", "") or "") != "helpful":
                    return False
            hold_max_age_sec = float(getattr(desired, "hold_max_age_sec", 0.0) or 0.0)
            if hold_max_age_sec > 0.0 and age_sec >= hold_max_age_sec:
                return False
            # For SELLs a higher desired price means the current order is no longer maker-safe.
            if (
                not sell_hold_mode
                and float(current_intent.price) + 1e-9 < float(desired.price)
            ):
                return False
            hold_reprice_threshold_ticks = int(getattr(desired, "hold_reprice_threshold_ticks", 0) or 0)
            hold_tick_size = float(getattr(desired, "hold_tick_size", 0.0) or 0.0)
            if hold_reprice_threshold_ticks > 0 and hold_tick_size > 0.0:
                max_price_delta = float(hold_reprice_threshold_ticks) * hold_tick_size
                if abs(float(current_intent.price) - float(desired.price)) > max_price_delta + 1e-9:
                    return False
            return True
        if min_rest_sec <= 0.0:
            return False
        if str(getattr(current_intent, "inventory_effect", "") or "") != "helpful":
            return False
        if str(getattr(desired, "inventory_effect", "") or "") != "helpful":
            return False
        return age_sec < min_rest_sec

    @staticmethod
    def _should_hold_existing_without_desired(existing: Any) -> bool:
        current_intent = getattr(existing, "intent", None)
        created_at = float(getattr(existing, "created_at", 0.0) or 0.0)
        if current_intent is None or created_at <= 0.0:
            return False
        if current_intent.side != "SELL":
            return False
        if not bool(getattr(current_intent, "hold_mode_active", False)):
            return False
        if str(getattr(current_intent, "hold_mode_reason", "") or "") != "sell_churn_hold_mode":
            if str(getattr(current_intent, "inventory_effect", "") or "") != "helpful":
                return False
        hold_max_age_sec = float(getattr(current_intent, "hold_max_age_sec", 0.0) or 0.0)
        age_sec = max(0.0, float(time.time()) - created_at)
        if hold_max_age_sec > 0.0 and age_sec >= hold_max_age_sec:
            return False
        return True

    def consume_sync_metrics(self) -> dict[str, int]:
        metrics = {
            "sell_churn_hold_reprice_suppressed_hits": int(self._sell_churn_hold_reprice_suppressed_hits),
            "sell_churn_hold_cancel_avoided_hits": int(self._sell_churn_hold_cancel_avoided_hits),
        }
        self._sell_churn_hold_reprice_suppressed_hits = 0
        self._sell_churn_hold_cancel_avoided_hits = 0
        return metrics

    def hold_order_state(
        self,
        slot_key: str,
        *,
        desired: QuoteIntent | None = None,
        now: float | None = None,
    ) -> dict[str, Any]:
        existing = self.tracker.get(slot_key)
        if existing is None:
            return {"active": False, "age_sec": 0.0, "reprice_due": False}
        current_intent = getattr(existing, "intent", None)
        if current_intent is None:
            return {"active": False, "age_sec": 0.0, "reprice_due": False}
        if not bool(getattr(current_intent, "hold_mode_active", False)):
            return {"active": False, "age_sec": 0.0, "reprice_due": False}
        if str(getattr(current_intent, "hold_mode_reason", "") or "") != "sell_churn_hold_mode":
            return {"active": False, "age_sec": 0.0, "reprice_due": False}
        age_sec = self.tracker.age_sec(slot_key, now=now)
        if desired is None:
            reprice_due = not self._should_hold_existing_without_desired(existing)
        else:
            reprice_due = not self._should_hold_existing(existing, desired)
        return {
            "active": True,
            "age_sec": float(age_sec),
            "reprice_due": bool(reprice_due),
        }

    async def _sync_slot(self, slot_key: str, desired: QuoteIntent | None) -> None:
        existing = self.tracker.get(slot_key)
        if desired is None:
            if existing:
                if self._should_hold_existing_without_desired(existing):
                    self._sell_churn_hold_cancel_avoided_hits += 1
                    return
                await self.gateway.cancel(existing.order_id)
                self.tracker.delete(slot_key)
            return
        if existing and not self._materially_different(existing.intent, desired):
            return
        if existing and self._should_hold_existing(existing, desired):
            if bool(getattr(desired, "hold_mode_active", False)):
                self._sell_churn_hold_cancel_avoided_hits += 1
                if abs(float(existing.intent.price) - float(desired.price)) > 1e-9:
                    self._sell_churn_hold_reprice_suppressed_hits += 1
            return
        if existing:
            await self.gateway.cancel(existing.order_id)
            self.tracker.delete(slot_key)
        order_id = await self.gateway.place_intent(desired)
        if order_id:
            self.tracker.set(slot_key, order_id, desired)

    async def sync(self, plan: QuotePlan) -> None:
        active = self.gateway.active_orders()
        self.tracker.refresh_from_active(active)
        await self._sync_slot("up_buy", plan.up_bid)
        await self._sync_slot("up_sell", plan.up_ask)
        await self._sync_slot("dn_buy", plan.dn_bid)
        await self._sync_slot("dn_sell", plan.dn_ask)
