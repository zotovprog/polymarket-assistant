"""Merge trigger — monitors token balances and executes on-chain merges."""
from __future__ import annotations

import logging
import math
import time

from .config import PairArbConfig
from .types import ArbMarket

log = logging.getLogger(__name__)


class MergeTrigger:
    """Checks for mergeable UP+DN pairs and triggers on-chain merge."""

    def __init__(self, order_mgr, config: PairArbConfig, private_key: str):
        self.order_mgr = order_mgr
        self.config = config
        self.private_key = private_key
        self.last_merge_ts: float = 0.0
        self.total_merged: float = 0.0
        self.total_merge_proceeds: float = 0.0
        self.merge_count: int = 0

    async def check_and_merge(self, market: ArbMarket) -> dict | None:
        """Check balances and merge if sufficient paired inventory exists.

        Returns:
            dict with merge result, or None if no merge attempted.
        """
        now = time.time()
        if now - self.last_merge_ts < self.config.merge_check_interval_sec:
            return None

        try:
            up_bal, dn_bal, usdc_bal, _ = await self.order_mgr.get_all_token_balances(
                market.up_token_id, market.dn_token_id,
            )
        except Exception as e:
            log.debug("Balance fetch failed for %s: %s", market.scope, e)
            return None

        up_bal = float(up_bal or 0)
        dn_bal = float(dn_bal or 0)
        mergeable = min(up_bal, dn_bal)

        # Floor to 2 decimal places (PM precision)
        mergeable = math.floor(mergeable * 100) / 100.0

        if mergeable < self.config.min_clip_shares:
            return None

        log.info(
            "Merge trigger: %s UP=%.2f DN=%.2f mergeable=%.2f",
            market.scope, up_bal, dn_bal, mergeable,
        )

        try:
            result = await self.order_mgr.merge_positions(
                market.condition_id, mergeable, self.private_key,
            )
        except Exception as e:
            log.error("Merge call failed for %s: %s", market.scope, e)
            return {"merged": 0, "error": str(e)}

        self.last_merge_ts = time.time()

        if result and result.get("success"):
            proceeds = float(result.get("amount_usdc", mergeable))
            self.total_merged += mergeable
            self.total_merge_proceeds += proceeds
            self.merge_count += 1
            log.info(
                "MERGE OK: %.2f pairs on %s → $%.2f USDC (tx=%s)",
                mergeable, market.scope, proceeds,
                str(result.get("tx_hash", ""))[:16],
            )
            return {
                "merged": mergeable,
                "proceeds": proceeds,
                "tx_hash": result.get("tx_hash", ""),
            }

        error = result.get("error", "unknown") if result else "no_result"
        log.warning("Merge failed for %s: %s", market.scope, error)
        return {"merged": 0, "error": error}
