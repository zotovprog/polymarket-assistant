from __future__ import annotations

import time
from typing import Any

from .order_tracker import OrderTrackerV2
from .pm_gateway import PMGateway
from .types import QuoteIntent, QuotePlan


class ExecutionPolicyV2:
    def __init__(self, gateway: PMGateway, tracker: OrderTrackerV2, *, requote_threshold_bps: float = 15.0):
        self.gateway = gateway
        self.tracker = tracker
        self.requote_threshold_bps = max(1.0, float(requote_threshold_bps))
        self._sell_churn_hold_reprice_suppressed_hits = 0
        self._sell_churn_hold_cancel_avoided_hits = 0

    def _materially_different(self, current: QuoteIntent, new: QuoteIntent) -> bool:
        if current.side != new.side or current.token != new.token or current.post_only != new.post_only:
            return True
        price_diff_bps = abs(float(current.price) - float(new.price)) / max(0.01, float(current.price)) * 10000.0
        size_diff = abs(float(current.size) - float(new.size))
        return price_diff_bps >= self.requote_threshold_bps or size_diff >= 0.5

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
            if str(getattr(current_intent, "inventory_effect", "") or "") != "helpful":
                return False
            if str(getattr(desired, "inventory_effect", "") or "") != "helpful":
                return False
            hold_max_age_sec = float(getattr(desired, "hold_max_age_sec", 0.0) or 0.0)
            if hold_max_age_sec > 0.0 and age_sec >= hold_max_age_sec:
                return False
            # For SELLs a higher desired price means the current order is no longer maker-safe.
            if float(current_intent.price) + 1e-9 < float(desired.price):
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

    def consume_sync_metrics(self) -> dict[str, int]:
        metrics = {
            "sell_churn_hold_reprice_suppressed_hits": int(self._sell_churn_hold_reprice_suppressed_hits),
            "sell_churn_hold_cancel_avoided_hits": int(self._sell_churn_hold_cancel_avoided_hits),
        }
        self._sell_churn_hold_reprice_suppressed_hits = 0
        self._sell_churn_hold_cancel_avoided_hits = 0
        return metrics

    async def _sync_slot(self, slot_key: str, desired: QuoteIntent | None) -> None:
        existing = self.tracker.get(slot_key)
        if desired is None:
            if existing:
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
