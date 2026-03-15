"""Pair Arbitrage Engine — main loop scanning, executing, and merging."""
from __future__ import annotations

import asyncio
import logging
import time
from typing import Any

from .config import PairArbConfig
from .executor import ArbExecutor
from .maker import MakerArbManager
from .merger import MergeTrigger
from .risk import ArbRiskManager
from .scanner import ArbScanner
from .types import ArbExecution, ArbMarket, ArbState

log = logging.getLogger(__name__)

# Market discovery refresh interval
MARKET_REFRESH_SEC = 60.0


class PairArbEngine:
    """Core arb engine: discover markets → post maker orders → merge.

    Two modes:
      - Maker (default): posts BUY limits on both sides at best-bid, waits for fills, merges.
      - Taker (fallback): scans for asks summing < $1.00, takes both sides, merges.
    """

    def __init__(
        self,
        order_mgr: Any,
        config: PairArbConfig,
        private_key: str,
        paper_mode: bool = True,
        app_version: str = "",
    ):
        self.order_mgr = order_mgr
        self.config = config
        self.private_key = private_key
        self._private_key = private_key
        self.paper_mode = paper_mode
        self.app_version = app_version

        self.scanner = ArbScanner(order_mgr, config)
        self.executor = ArbExecutor(order_mgr, config)
        self.merger = MergeTrigger(order_mgr, config, private_key)
        self.risk = ArbRiskManager(config)

        self._running = False
        self._started_at = 0.0
        self._markets: list[ArbMarket] = []
        self._maker_managers: dict[str, MakerArbManager] = {}  # scope -> manager
        self._last_market_refresh: float = 0.0
        self._scan_count = 0
        self._opportunities_seen = 0
        self._opportunities_executed = 0
        self._last_opportunity_ts = 0.0
        self._recent_executions: list[ArbExecution] = []
        self._pending_redeems: list[str] = []  # condition_ids to redeem
        self._redeemed: set[str] = set()       # already redeemed
        self._last_redeem_attempt: float = 0.0
        self._task: asyncio.Task | None = None

    async def start(self) -> dict:
        """Start the arb engine. Returns initial state."""
        if self._running:
            return {"ok": False, "error": "already_running"}

        self._running = True
        self._started_at = time.time()

        # Initial market discovery
        await self._refresh_markets()

        self._task = asyncio.create_task(self._run_loop())
        log.info(
            "PairArbEngine started: paper=%s scopes=%s markets=%d maker=%s",
            self.paper_mode, self.config.market_scopes, len(self._markets),
            self.config.use_maker_orders,
        )
        return {"ok": True, "markets": len(self._markets), "state": self.snapshot().to_dict()}

    async def stop(self) -> dict:
        """Stop the engine and cancel open orders."""
        self._running = False
        if self._task:
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass
            self._task = None

        # Cancel maker orders
        for mgr in self._maker_managers.values():
            try:
                await mgr.cancel_all()
            except Exception as e:
                log.debug("Error cancelling maker orders for %s: %s", mgr.market.scope, e)

        # Cancel any remaining orders
        try:
            await self.order_mgr.cancel_all()
        except Exception as e:
            log.warning("Error cancelling orders on stop: %s", e)

        log.info(
            "PairArbEngine stopped: pnl=%.4f merged=%.2f leg_risk_loss=%.4f",
            self.risk.session_pnl, self.merger.total_merged, self.risk.leg_risk_losses,
        )
        return {"ok": True, "state": self.snapshot().to_dict()}

    async def _refresh_markets(self) -> list[ArbMarket]:
        """Discover active markets from configured scopes."""
        from feeds import fetch_pm_tokens

        old_conditions = {m.scope: m.condition_id for m in self._markets}
        markets: list[ArbMarket] = []
        now = time.time()

        for coin, tf in self.config.parsed_scopes():
            try:
                up_id, dn_id, cond_id = fetch_pm_tokens(coin, tf)
                if up_id and dn_id and cond_id:
                    markets.append(ArbMarket(
                        coin=coin,
                        timeframe=tf,
                        up_token_id=up_id,
                        dn_token_id=dn_id,
                        condition_id=cond_id,
                        last_discovered_ts=now,
                    ))
            except Exception as e:
                log.debug("Market discovery failed for %s_%s: %s", coin, tf, e)

        for m in markets:
            old_cid = old_conditions.get(m.scope)
            if old_cid and old_cid != m.condition_id and old_cid not in self._redeemed:
                self._pending_redeems.append(old_cid)
                log.info("Market %s rotated: queuing redeem for %s", m.scope, old_cid[:16])

        self._markets = markets
        self._last_market_refresh = now

        # Create/update maker managers
        if self.config.use_maker_orders:
            new_managers: dict[str, MakerArbManager] = {}
            for m in markets:
                if m.scope in self._maker_managers:
                    mgr = self._maker_managers[m.scope]
                    mgr.market = m  # Update token IDs if rotated
                    new_managers[m.scope] = mgr
                else:
                    new_managers[m.scope] = MakerArbManager(m, self.order_mgr, self.config)
            # Cancel managers for markets no longer active
            for scope, mgr in self._maker_managers.items():
                if scope not in new_managers:
                    try:
                        await mgr.cancel_all()
                    except Exception:
                        pass
            self._maker_managers = new_managers

        if markets:
            log.info("Discovered %d markets: %s", len(markets),
                     ", ".join(m.scope for m in markets))
        return markets

    async def _run_loop(self) -> None:
        """Main arb loop."""
        while self._running:
            try:
                # Refresh markets periodically
                if time.time() - self._last_market_refresh > MARKET_REFRESH_SEC:
                    await self._refresh_markets()

                if not self._markets:
                    await asyncio.sleep(5.0)
                    continue

                self._scan_count += 1

                # === MAKER MODE: manage persistent BUY limits ===
                # Only post orders on the BEST market (most profitable spread)
                # to avoid exceeding USDC budget across too many markets.
                if self.config.use_maker_orders:
                    # First: scan all markets to find best spread
                    best_scope = None
                    best_spread = 1.0
                    for scope, mgr in self._maker_managers.items():
                        try:
                            up_book = await self.order_mgr.get_full_book(mgr.market.up_token_id)
                            dn_book = await self.order_mgr.get_full_book(mgr.market.dn_token_id)
                            bid_up = up_book.get("best_bid") or 0
                            bid_dn = dn_book.get("best_bid") or 0
                            total = bid_up + bid_dn
                            if total > 0 and total < best_spread:
                                best_spread = total
                                best_scope = scope
                        except Exception:
                            pass

                    # Post orders only on best market, cancel others
                    for scope, mgr in self._maker_managers.items():
                        try:
                            if scope == best_scope:
                                result = await mgr.tick()
                                if result and result.get("taker_opportunity"):
                                    self._opportunities_seen += 1
                                    self._last_opportunity_ts = time.time()
                            else:
                                # Cancel orders on non-best markets
                                if mgr.up_order_id or mgr.dn_order_id:
                                    await mgr.cancel_all()
                        except Exception as e:
                            log.debug("Maker tick error for %s: %s", scope, e)

                # === TAKER MODE: scan asks for immediate arb ===
                else:
                    opportunities = await self.scanner.scan_all(self._markets)
                    if opportunities:
                        self._opportunities_seen += len(opportunities)
                        self._last_opportunity_ts = time.time()
                        best = opportunities[0]

                        can_exec, reason = self.risk.can_execute(best)
                        if can_exec:
                            log.info(
                                "ARB: %s UP@%.2f+DN@%.2f=%.4f profit=%.4f x%.1f",
                                best.market.scope, best.ask_up, best.ask_dn,
                                best.total_cost_per_pair, best.profit_per_pair,
                                best.max_arb_shares,
                            )
                            execution = await self.executor.execute(best)
                            self._opportunities_executed += 1
                            cost = best.total_cost_per_pair * best.max_arb_shares
                            self.risk.record_execution(cost)
                            self._recent_executions.append(execution)
                            if len(self._recent_executions) > 100:
                                self._recent_executions = self._recent_executions[-100:]

                # --- Handle leg risk ---
                resolved = await self.executor.handle_leg_risk()
                for ex in resolved:
                    if ex.status == "failed" and "unwind" in (ex.error or ""):
                        estimated_loss = 0.03 * ex.target_shares
                        self.risk.record_leg_risk_loss(estimated_loss)

                # --- Check merges (both modes) ---
                for market in self._markets:
                    result = await self.merger.check_and_merge(market)
                    if result and result.get("merged", 0) > 0:
                        merged = result["merged"]
                        proceeds = result.get("proceeds", merged)
                        estimated_cost = merged * 0.97
                        self.risk.record_merge_profit(proceeds, estimated_cost)
                        self._opportunities_executed += 1
                        log.info(
                            "MERGED %.2f pairs on %s → $%.2f",
                            merged, market.scope, proceeds,
                        )

                # --- Auto-redeem resolved markets ---
                if self._pending_redeems and not self.paper_mode:
                    now = time.time()
                    if now - self._last_redeem_attempt >= 30.0:
                        self._last_redeem_attempt = now
                        cond_id = self._pending_redeems[0]
                        try:
                            result = await self.order_mgr.redeem_positions(
                                condition_id=cond_id,
                                private_key=self._private_key,
                            )
                            if result.get("success"):
                                self._redeemed.add(cond_id)
                                self._pending_redeems.pop(0)
                                log.info(
                                    "Auto-redeemed %s: tx=%s",
                                    cond_id[:16],
                                    result.get("tx_hash", "")[:16],
                                )
                            else:
                                log.debug(
                                    "Redeem not ready for %s: %s",
                                    cond_id[:16],
                                    result.get("error"),
                                )
                        except Exception as e:
                            log.debug("Redeem failed for %s: %s", cond_id[:16], e)

                await asyncio.sleep(self.config.scan_interval_sec)

            except asyncio.CancelledError:
                break
            except Exception as e:
                log.error("Arb loop error: %s", e, exc_info=True)
                await asyncio.sleep(5.0)

    def snapshot(self) -> ArbState:
        """Return full state snapshot."""
        state = ArbState(
            is_running=self._running,
            started_at=self._started_at,
            app_version=self.app_version,
            paper_mode=self.paper_mode,
            markets_scanning=len(self._markets),
            market_scopes=[m.scope for m in self._markets],
            scan_count=self._scan_count,
            opportunities_seen=self._opportunities_seen,
            opportunities_executed=self._opportunities_executed,
            total_merged_pairs=self.merger.total_merged,
            total_profit_usd=self.risk.session_pnl,
            total_leg_risk_loss_usd=self.risk.leg_risk_losses,
            session_pnl_usd=self.risk.session_pnl,
            last_opportunity_ts=self._last_opportunity_ts,
            leg_risk_events=sum(
                1 for e in self._recent_executions if e.status == "failed"
            ),
            pending_redeems=len(self._pending_redeems),
            total_redeemed=len(self._redeemed),
            config=self.config.to_dict(),
            recent_executions=[
                e.to_dict() for e in self._recent_executions[-20:]
            ],
            pending_executions=[
                e.to_dict() for e in self.executor.pending
            ],
        )
        # Add maker manager info
        if self._maker_managers:
            state.config["maker_managers"] = {
                scope: mgr.to_dict() for scope, mgr in self._maker_managers.items()
            }
        return state

    def update_config(self, **kwargs: Any) -> dict:
        """Update config at runtime."""
        self.config.update(**kwargs)
        self.scanner.config = self.config
        self.executor.config = self.config
        self.merger.config = self.config
        self.risk.config = self.config
        for mgr in self._maker_managers.values():
            mgr.config = self.config
        return self.config.to_dict()
