from __future__ import annotations

from dataclasses import dataclass, field
import time
from typing import Any

from mm_shared.types import Quote

from .types import ExecutionState, QuoteIntent


@dataclass
class SlotOrder:
    order_id: str
    intent: QuoteIntent
    created_at: float = field(default_factory=time.time)


@dataclass
class OrderTrackerV2:
    slots: dict[str, SlotOrder] = field(default_factory=dict)

    def slot_key(self, token_key: str, side: str) -> str:
        return f"{token_key}_{side.lower()}"

    def get(self, slot_key: str) -> SlotOrder | None:
        return self.slots.get(slot_key)

    def set(self, slot_key: str, order_id: str, intent: QuoteIntent) -> None:
        self.slots[slot_key] = SlotOrder(order_id=order_id, intent=intent)

    def delete(self, slot_key: str) -> None:
        self.slots.pop(slot_key, None)

    def refresh_from_active(self, active_orders: dict[str, Quote]) -> None:
        live_ids = set(active_orders.keys())
        stale = [slot_key for slot_key, entry in self.slots.items() if entry.order_id not in live_ids]
        for slot_key in stale:
            self.slots.pop(slot_key, None)

    def order_ids(self) -> list[str]:
        return [entry.order_id for entry in self.slots.values()]

    def execution_state(
        self,
        *,
        active_orders: dict[str, Quote],
        transport_failures: int,
        last_api_error: str,
        last_fallback_poll_count: int,
        up_token_id: str,
        dn_token_id: str,
    ) -> ExecutionState:
        pending_buy_up = 0.0
        pending_buy_dn = 0.0
        pending_sell_up = 0.0
        pending_sell_dn = 0.0
        for quote in active_orders.values():
            if quote.token_id == up_token_id:
                if quote.side == "BUY":
                    pending_buy_up += float(quote.size)
                else:
                    pending_sell_up += float(quote.size)
            elif quote.token_id == dn_token_id:
                if quote.side == "BUY":
                    pending_buy_dn += float(quote.size)
                else:
                    pending_sell_dn += float(quote.size)
        return ExecutionState(
            open_orders=len(active_orders),
            pending_buy_up=pending_buy_up,
            pending_buy_dn=pending_buy_dn,
            pending_sell_up=pending_sell_up,
            pending_sell_dn=pending_sell_dn,
            transport_failures=transport_failures,
            last_api_error=last_api_error,
            last_fallback_poll_count=last_fallback_poll_count,
            current_order_ids={slot: entry.order_id for slot, entry in self.slots.items()},
        )
