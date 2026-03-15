"""Arb order executor — places BUY orders for both sides, handles leg risk."""
from __future__ import annotations

import asyncio
import logging
import time

from mm_shared.types import Quote

from .config import PairArbConfig
from .types import ArbExecution, ArbOpportunity

log = logging.getLogger(__name__)


class ArbExecutor:
    """Places pair arb orders and manages partial fills / leg risk."""

    def __init__(self, order_mgr, config: PairArbConfig):
        self.order_mgr = order_mgr
        self.config = config
        self.pending: list[ArbExecution] = []
        self._exec_counter: int = 0

    async def execute(self, opp: ArbOpportunity) -> ArbExecution:
        """Execute an arb: buy UP and DN simultaneously."""
        self._exec_counter += 1
        exec_id = f"arb_{int(time.time())}_{self._exec_counter}"

        execution = ArbExecution(
            id=exec_id,
            opportunity=opp,
            target_shares=opp.max_arb_shares,
            started_at=time.time(),
            status="buying",
        )

        up_quote = Quote(
            side="BUY",
            token_id=opp.market.up_token_id,
            price=opp.ask_up,
            size=opp.max_arb_shares,
            order_context="pair_arb",
        )
        dn_quote = Quote(
            side="BUY",
            token_id=opp.market.dn_token_id,
            price=opp.ask_dn,
            size=opp.max_arb_shares,
            order_context="pair_arb",
        )

        post_only = self.config.use_maker_orders

        # Place both orders concurrently
        try:
            up_result, dn_result = await asyncio.gather(
                self._safe_place(up_quote, post_only),
                self._safe_place(dn_quote, post_only),
            )
        except Exception as e:
            execution.status = "failed"
            execution.error = f"order_placement_error: {e}"
            execution.completed_at = time.time()
            return execution

        execution.up_order_id = up_result
        execution.dn_order_id = dn_result

        if not execution.up_order_id and not execution.dn_order_id:
            execution.status = "failed"
            execution.error = "both_orders_failed"
            execution.completed_at = time.time()
            log.warning("Arb %s: both orders failed", exec_id)
            return execution

        if not execution.up_order_id or not execution.dn_order_id:
            execution.status = "leg_risk"
            log.warning(
                "Arb %s: LEG RISK — UP=%s DN=%s",
                exec_id,
                "OK" if execution.up_order_id else "FAILED",
                "OK" if execution.dn_order_id else "FAILED",
            )
        else:
            execution.status = "filled"
            log.info(
                "Arb %s: both orders placed — UP@%.2f DN@%.2f x%.1f",
                exec_id, opp.ask_up, opp.ask_dn, opp.max_arb_shares,
            )

        self.pending.append(execution)
        return execution

    async def handle_leg_risk(self) -> list[ArbExecution]:
        """Process pending executions with leg risk."""
        resolved: list[ArbExecution] = []
        still_pending: list[ArbExecution] = []

        for ex in self.pending:
            if ex.status == "leg_risk":
                age = time.time() - ex.started_at

                if age > self.config.leg_risk_max_hold_sec:
                    # Timeout: sell back the filled leg
                    await self._unwind_filled_leg(ex)
                    resolved.append(ex)
                elif ex.leg_risk_retries < self.config.leg_risk_retry_count:
                    # Retry the missing leg
                    success = await self._retry_missing_leg(ex)
                    if success:
                        ex.status = "filled"
                        still_pending.append(ex)
                    else:
                        ex.leg_risk_retries += 1
                        still_pending.append(ex)
                else:
                    # Max retries exhausted: sell back
                    await self._unwind_filled_leg(ex)
                    resolved.append(ex)

            elif ex.status in ("merged", "failed"):
                resolved.append(ex)
            else:
                still_pending.append(ex)

        self.pending = still_pending
        return resolved

    async def _retry_missing_leg(self, ex: ArbExecution) -> bool:
        """Retry placing the missing side order."""
        missing_up = ex.up_order_id is None
        opp = ex.opportunity

        token_id = opp.market.up_token_id if missing_up else opp.market.dn_token_id
        price = opp.ask_up if missing_up else opp.ask_dn

        retry_quote = Quote(
            side="BUY",
            token_id=token_id,
            price=price,
            size=ex.target_shares,
            order_context="pair_arb_retry",
        )

        # Use taker for retries (speed over cost)
        result = await self._safe_place(retry_quote, post_only=False)
        if result:
            if missing_up:
                ex.up_order_id = result
            else:
                ex.dn_order_id = result
            log.info("Arb %s: retry succeeded for %s side", ex.id, "UP" if missing_up else "DN")
            return True

        log.debug("Arb %s: retry %d failed for %s side", ex.id, ex.leg_risk_retries + 1,
                   "UP" if missing_up else "DN")
        return False

    async def _unwind_filled_leg(self, ex: ArbExecution) -> None:
        """Sell back the filled leg at best bid to cut losses."""
        missing_up = ex.up_order_id is None
        opp = ex.opportunity

        # Sell the side that WAS filled
        filled_token = opp.market.dn_token_id if missing_up else opp.market.up_token_id

        sell_quote = Quote(
            side="SELL",
            token_id=filled_token,
            price=0.01,  # Floor price — will fill at market
            size=ex.target_shares,
            order_context="pair_arb_unwind",
        )

        result = await self._safe_place(sell_quote, post_only=False)
        ex.status = "failed"
        ex.error = "leg_risk_unwind"
        ex.completed_at = time.time()
        log.warning("Arb %s: unwound filled leg (sold %s), order=%s",
                     ex.id, "DN" if missing_up else "UP", result)

    async def _safe_place(self, quote: Quote, post_only: bool) -> str | None:
        """Place an order, returning order_id or None on failure."""
        try:
            result = await self.order_mgr.place_order(quote, post_only=post_only)
            if isinstance(result, str) and result:
                return result
            return None
        except Exception as e:
            log.debug("Order placement failed: %s", e)
            return None

    def get_pending_count(self) -> int:
        return len(self.pending)

    def get_leg_risk_count(self) -> int:
        return sum(1 for ex in self.pending if ex.status == "leg_risk")
