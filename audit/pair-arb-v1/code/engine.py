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
        self._current_best_scope: str | None = None
        self._last_scope_switch_ts: float = 0.0
        self._last_usdc_balance: float = 0.0

    async def start(self) -> dict:
        """Start the arb engine. Returns initial state."""
        if self._running:
            return {"ok": False, "error": "already_running"}

        self._running = True
        self._started_at = time.time()

        # Initial market discovery
        await self._refresh_markets()

        # Fetch initial USDC balance before first snapshot
        try:
            _, avail = await self.order_mgr.get_usdc_balances()
            self._last_usdc_balance = float(avail or 0)
        except Exception:
            pass

        self._task = asyncio.create_task(self._run_loop())
        log.info(
            "PairArbEngine started: paper=%s scopes=%s markets=%d maker=%s usdc=%.2f",
            self.paper_mode, self.config.market_scopes, len(self._markets),
            self.config.use_maker_orders, self._last_usdc_balance,
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
            await self.order_mgr.cancel_all(force_exchange=True)
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

                # Update cached USDC balance for snapshot
                try:
                    _, avail = await self.order_mgr.get_usdc_balances()
                    self._last_usdc_balance = float(avail or 0)
                except Exception:
                    pass

                # === MAKER MODE: manage persistent BUY limits ===
                # Only post orders on the SINGLE best market to concentrate USDC.
                if self.config.use_maker_orders:
                    # Find best market (lowest total bid = highest profit)
                    best_scope = None
                    best_total = 2.0  # impossibly high
                    for scope, mgr in self._maker_managers.items():
                        try:
                            up_book = await self.order_mgr.get_full_book(mgr.market.up_token_id)
                            dn_book = await self.order_mgr.get_full_book(mgr.market.dn_token_id)
                            bid_up = up_book.get("best_bid") or 0
                            bid_dn = dn_book.get("best_bid") or 0
                            total = bid_up + bid_dn
                            if 0 < total < best_total:
                                best_total = total
                                best_scope = scope
                        except Exception:
                            pass

                    # If best market changed, cancel ALL orders first
                    # Sticky: don't switch more than once per 30 seconds
                    STICKY_SEC = 30.0
                    should_switch = (
                        best_scope
                        and best_scope != self._current_best_scope
                        and (
                            self._current_best_scope is None
                            or time.time() - self._last_scope_switch_ts >= STICKY_SEC
                        )
                    )
                    if should_switch:
                        if self._current_best_scope is not None:
                            log.info(
                                "Switching best market: %s -> %s (total=%.4f)",
                                self._current_best_scope, best_scope, best_total,
                            )
                        try:
                            await self.order_mgr.cancel_all(force_exchange=True)
                        except Exception as e:
                            log.warning("cancel_all on market switch failed: %s", e)
                        for mgr in self._maker_managers.values():
                            mgr.up_order_id = None
                            mgr.dn_order_id = None
                        self._current_best_scope = best_scope
                        self._last_scope_switch_ts = time.time()
                        # Invalidate USDC cache so next tick gets fresh balance
                        self.order_mgr.invalidate_usdc_cache()
                        await asyncio.sleep(0.5)  # Let exchange process cancellations

                    # Tick only the best market
                    if best_scope and best_scope in self._maker_managers:
                        try:
                            result = await self._maker_managers[best_scope].tick()
                            if result and result.get("taker_opportunity"):
                                self._opportunities_seen += 1
                                self._last_opportunity_ts = time.time()
                        except Exception as e:
                            log.debug("Maker tick error for %s: %s", best_scope, e)

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
                        # Use actual fill price for loss estimate (5% slippage on unwind)
                        fill_price = getattr(ex, 'up_fill_price', 0) or getattr(ex, 'dn_fill_price', 0) or 0.5
                        estimated_loss = fill_price * ex.target_shares * 0.05
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

                # --- Detect asymmetric fills (maker mode leg risk) ---
                if self.config.use_maker_orders:
                    for market in self._markets:
                        asym = await self.merger.check_asymmetric_fills(
                            market, threshold=self.config.min_clip_shares,
                        )
                        if asym:
                            log.warning(
                                "ASYMMETRIC FILL %s: UP=%.2f DN=%.2f diff=%.2f (heavy=%s)",
                                asym["scope"], asym["up_bal"], asym["dn_bal"],
                                asym["diff"], asym["heavy_side"],
                            )
                            # Cancel maker orders on this market to stop further one-sided fills
                            mgr = self._maker_managers.get(asym["scope"])
                            if mgr:
                                await mgr.cancel_all()

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
        state.usdc_balance = self._last_usdc_balance
        state.config["current_best_scope"] = self._current_best_scope
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
