"""Market Maker Orchestrator — main loop coordinating all MM components.

Lifecycle:
1. Initialize with feed_state, CLOB client, and config
2. Set market info (token IDs, strike, window timing)
3. Start: launch heartbeat, begin quote loop
4. Quote loop runs every requote_interval_sec:
   a. Read Binance data from feed_state
   b. Compute fair value (FairValueEngine)
   c. Generate quotes (QuoteEngine)
   d. Place/update orders if price moved enough (OrderManager)
   e. Check fills, update inventory (RiskManager)
   f. Check risk limits, pause if needed
5. On window transition: cancel all, update token IDs, resume
6. Stop: cancel all orders, stop heartbeat
"""
from __future__ import annotations
import asyncio
import logging
import time
from dataclasses import asdict
from typing import Any, Optional

from .types import Quote, Fill, Inventory, MMState, MarketInfo
from .mm_config import MMConfig
from .fair_value import FairValueEngine
from .quote_engine import QuoteEngine
from .order_manager import OrderManager
from .risk_manager import RiskManager, LiquidationLock
from .heartbeat import HeartbeatManager
from .rebate_tracker import RebateTracker
from .market_quality import MarketQualityAnalyzer, MarketQuality

log = logging.getLogger("mm.engine")


class MarketMaker:
    """Main Market Making engine."""

    def __init__(self, feed_state: Any, clob_client: Any, config: MMConfig):
        """
        Args:
            feed_state: feeds.State object with Binance + PM data
            clob_client: py_clob_client.ClobClient (or mock for paper trading)
            config: MMConfig with all parameters
        """
        self.feed_state = feed_state
        self.config = config
        self._log = log

        # Sub-engines
        self.fair_value = FairValueEngine()
        self.quote_engine = QuoteEngine(config)
        self.order_mgr = OrderManager(clob_client, config)
        self.risk_mgr = RiskManager(config)
        self.heartbeat = HeartbeatManager(
            clob_client,
            config.heartbeat_interval_sec,
            on_failure=self._on_heartbeat_failure,
        )
        self.rebate = RebateTracker(clob_client)
        self.quality_analyzer = MarketQualityAnalyzer(config)

        # State
        self.inventory = Inventory()
        self.market: Optional[MarketInfo] = None
        self._running = False
        self._paused = False
        self._pause_reason = ""
        self._task: Optional[asyncio.Task] = None
        self._started_at: float = 0.0
        self._is_closing = False
        self._liquidation_attempted = False
        self._liquidation_order_ids: set[str] = set()
        self._cached_usdc_balance: float = 0.0
        self._starting_usdc_pm: float = 0.0
        self._last_quality: MarketQuality | None = None
        self._liq_lock: LiquidationLock | None = None
        self._liq_chunk_index: int = 0
        self._liq_last_chunk_time: float = 0.0
        self._one_sided_counter: int = 0
        self._warn_cooldowns: dict[str, float] = {}
        self._private_key: str = ""

        # Current quotes (for dashboard)
        self._current_quotes: dict[str, tuple[Optional[Quote], Optional[Quote]]] = {
            "up": (None, None),
            "dn": (None, None),
        }

        # Stats
        self._quote_count: int = 0
        self._requote_count: int = 0
        self._tick_count: int = 0
        self._spread_samples: list[float] = []

        # Callbacks
        self._on_fill_callbacks: list = []
        self._on_state_change_callbacks: list = []
        self._on_snapshot_callbacks: list = []

    def _throttled_warn(self, key: str, msg: str, cooldown: float = 30.0):
        """Log a warning at most once per cooldown period."""
        now = time.time()
        if now - self._warn_cooldowns.get(key, 0) >= cooldown:
            self._warn_cooldowns[key] = now
            log.warning(msg)

    def _on_heartbeat_failure(self) -> None:
        log.warning("Heartbeat lost — orders likely cancelled by PM")
        self._current_quotes = {
            "up": (None, None),
            "dn": (None, None),
        }

    def set_market(self, market: MarketInfo) -> None:
        """Set the current market (token IDs, strike, window)."""
        self.market = market
        log.info(f"Market set: {market.coin} {market.timeframe} "
                 f"strike={market.strike:.2f} "
                 f"UP={market.up_token_id[:12]}... "
                 f"DN={market.dn_token_id[:12]}...")

    def on_fill(self, callback) -> None:
        """Register callback for fill events: callback(fill, token_type)."""
        self._on_fill_callbacks.append(callback)

    def on_snapshot(self, callback) -> None:
        """Register callback for periodic snapshots: callback(state_dict)."""
        self._on_snapshot_callbacks.append(callback)

    async def start(self) -> None:
        """Start the market maker."""
        if self._running:
            log.warning("MarketMaker already running")
            return

        if not self.market:
            raise ValueError("Market info not set — call set_market() first")

        self._running = True
        # Snapshot starting USDC for real session PnL
        try:
            self._starting_usdc_pm = await self.order_mgr.get_usdc_balance()
            self._cached_usdc_balance = self._starting_usdc_pm
        except Exception:
            self._starting_usdc_pm = 0.0
            self._cached_usdc_balance = 0.0
        self._started_at = time.time()
        self._paused = False
        self._pause_reason = ""
        self._tick_count = 0
        self._is_closing = False
        self._liquidation_attempted = False
        self._liquidation_order_ids = set()
        self._liq_lock = None
        self._liq_chunk_index = 0
        self._liq_last_chunk_time = 0.0
        self._one_sided_counter = 0

        # Set budget cap on order manager (enforced at placement time)
        self.order_mgr._session_budget = self.inventory.initial_usdc
        self.order_mgr._session_spent = 0.0

        # Start heartbeat
        self.heartbeat.start()

        # Start user WebSocket for real-time fill detection (live only)
        is_live = not hasattr(self.order_mgr.client, "_orders")
        if is_live:
            creds = getattr(self.order_mgr.client, 'creds', None)
            if creds and hasattr(creds, 'api_key'):
                await self.order_mgr.start_fill_ws(
                    api_key=creds.api_key,
                    api_secret=creds.api_secret,
                    api_passphrase=getattr(creds, 'api_passphrase', ''),
                )
            else:
                log.info("No API creds for fill WS — using polling only")

        # Start main loop
        self._task = asyncio.create_task(self._run_loop())
        log.info("MarketMaker started")

    async def stop(self, liquidate: bool = True) -> None:
        """Graceful shutdown: liquidate inventory, cancel orders, stop heartbeat."""
        if not self._running:
            return

        self._running = False
        log.info("MarketMaker stopping...")

        # Cancel main loop
        if self._task:
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass

        # Cancel all orders first
        cancelled = await self.order_mgr.cancel_all()
        log.info(f"Cancelled {cancelled} orders on shutdown")

        # Liquidate remaining inventory before full stop
        if liquidate and self.market:
            self._liquidation_order_ids = set()
            for attempt in range(3):
                await self._liquidate_inventory()
                if not self._liquidation_order_ids:
                    break
                # Wait for fills
                log.info(f"Stop liquidation attempt {attempt+1}/3, waiting for fills...")
                await asyncio.sleep(3.0)
                await self.order_mgr.check_fills()

        # Final cancel of any remaining orders
        await self.order_mgr.cancel_all()

        # Stop fill WebSocket
        await self.order_mgr.stop_fill_ws()

        # Stop heartbeat
        await self.heartbeat.stop()

        log.info("MarketMaker stopped")

    async def _run_loop(self) -> None:
        """Main quoting loop."""
        log.info("Quote loop started")
        try:
            while self._running:
                try:
                    try:
                        await asyncio.wait_for(self._tick(), timeout=5.0)
                    except asyncio.TimeoutError:
                        self._log.warning("_tick() timed out after 5s, skipping iteration")
                except asyncio.CancelledError:
                    break
                except Exception as e:
                    log.error(f"Tick error: {e}", exc_info=True)

                await asyncio.sleep(self.config.requote_interval_sec)
        except asyncio.CancelledError:
            pass
        log.info("Quote loop ended")

    async def _tick(self) -> None:
        """Single iteration of the quote loop."""
        if not self.market:
            return
        self._tick_count += 1

        # ── Periodic snapshot (every 10 ticks ≈ 20s) ────────────
        if self._on_snapshot_callbacks and self._tick_count % 10 == 0:
            try:
                snap = self.snapshot()
                for cb in self._on_snapshot_callbacks:
                    try:
                        cb(snap)
                    except Exception:
                        pass
            except Exception:
                pass

        # ── End-of-window management ─────────────────────────────
        time_left = self.market.time_remaining

        if time_left <= 0:
            # Window expired — liquidate and stop
            if not self._is_closing:
                self._is_closing = True
                fv_up, fv_dn = self._compute_fv()
                self._liq_lock = self.risk_mgr.lock_pnl(
                    self.inventory, fv_up, fv_dn,
                    self.config.liq_price_floor_margin)
                self._liq_chunk_index = 0
                self._liq_last_chunk_time = 0.0
                await self.order_mgr.cancel_all()
                self._current_quotes = {"up": (None, None), "dn": (None, None)}
                log.info("Window expired — cancelled all orders, liquidating inventory")
                await self._liquidate_inventory()
            self._running = False
            return

        # Adaptive close window: min(config, 40% of window) — so 5m=120s, 15m=120s, 1h=120s
        window_dur = self.market.window_end - self.market.window_start
        close_sec = min(self.config.close_window_sec, window_dur * 0.4)

        if time_left <= close_sec and not self._is_closing:
            self._is_closing = True
            fv_up_close, fv_dn_close = self._compute_fv()
            self._liq_lock = self.risk_mgr.lock_pnl(
                self.inventory, fv_up_close, fv_dn_close,
                self.config.liq_price_floor_margin)
            self._liq_chunk_index = 0
            self._liq_last_chunk_time = 0.0
            log.info(f"Closing mode: {time_left:.0f}s remaining — cancelling all orders "
                     f"(lock: pnl=${self._liq_lock.trigger_pnl:.2f})")
            await self.order_mgr.cancel_all()
            self._current_quotes = {"up": (None, None), "dn": (None, None)}

        # 2. Check for fills (always, including closing mode)
        fills = await self.order_mgr.check_fills()
        for fill in fills:
            token_type = "up" if fill.token_id == self.market.up_token_id else "dn"
            self.inventory.update_from_fill(fill, token_type)
            self.risk_mgr.record_fill(fill)
            log.info(f"FILL: {fill.side} {fill.size:.1f}@{fill.price:.2f} "
                     f"({token_type.upper()}) fee={fill.fee:.4f}")
            for cb in self._on_fill_callbacks:
                try:
                    cb(fill, token_type)
                except Exception:
                    pass

        # Live mode: periodically reconcile internal shares with PM balances.
        is_live = not hasattr(self.order_mgr.client, "_orders")
        if is_live and self._tick_count % 5 == 0:
            real_up, real_dn = await self.order_mgr.get_all_token_balances(
                self.market.up_token_id,
                self.market.dn_token_id,
            )
            self._cached_usdc_balance = await self.order_mgr.get_usdc_balance()
            up_diff = abs(real_up - self.inventory.up_shares)
            dn_diff = abs(real_dn - self.inventory.dn_shares)
            if up_diff > 1.0 or dn_diff > 1.0:
                log.warning(
                    "Inventory reconcile: internal UP=%.2f DN=%.2f, PM UP=%.2f DN=%.2f",
                    self.inventory.up_shares,
                    self.inventory.dn_shares,
                    real_up,
                    real_dn,
                )
                self.inventory.reconcile(real_up, real_dn, self._cached_usdc_balance)

        if self._is_closing:
            # Continuously try to sell remaining inventory each tick
            await self._liquidate_inventory()
            return

        st = self.feed_state

        # 1. Defensive copies of feed data
        mid = st.mid
        bids = list(st.bids) if st.bids else []
        asks = list(st.asks) if st.asks else []
        trades = list(st.trades) if st.trades else []
        klines = list(st.klines) if st.klines else []

        if not mid or mid <= 0:
            return

        # 3. Compute fair value
        fv_up, fv_dn = self.fair_value.compute(
            mid=mid,
            strike=self.market.strike,
            time_remaining_sec=self.market.time_remaining,
            klines=klines,
            bids=bids,
            asks=asks,
            trades=trades,
        )

        # 3b. Sync FV to mock client for realistic paper trading
        if hasattr(self.order_mgr.client, 'set_fair_values'):
            pm_prices = (
                {"up": self.feed_state.pm_up, "dn": self.feed_state.pm_dn}
                if hasattr(self.feed_state, "pm_up") else None
            )
            self.order_mgr.client.set_fair_values(
                fv_up,
                fv_dn,
                self.market,
                pm_prices=pm_prices,
            )

        # 4. Compute volatility
        vol = self.fair_value.realized_vol(klines)
        self.risk_mgr.record_vol(vol)

        # 5. Check risk limits
        should_pause, reason = self.risk_mgr.should_pause(
            self.inventory, vol, fv_up, fv_dn)

        # Exit triggers (TP, trailing stop, drawdown) ALWAYS take priority — even if already paused
        if should_pause and ("Take profit" in reason or "Trailing stop" in reason or "Max drawdown" in reason):
            log.warning(f"Exit trigger: {reason}")
            self._paused = False
            self._pause_reason = ""
            # Lock prices at trigger time
            self._liq_lock = self.risk_mgr.lock_pnl(
                self.inventory, fv_up, fv_dn,
                self.config.liq_price_floor_margin)
            log.info(
                "LIQ LOCK: trigger_pnl=$%.2f UP_avg=%.4f DN_avg=%.4f "
                "UP_floor=%.2f DN_floor=%.2f",
                self._liq_lock.trigger_pnl,
                self._liq_lock.up_avg_entry, self._liq_lock.dn_avg_entry,
                self._liq_lock.min_sell_price_up, self._liq_lock.min_sell_price_dn,
            )
            self._liq_chunk_index = 0
            self._liq_last_chunk_time = 0.0
            self._is_closing = True
            await self.order_mgr.cancel_all()
            self._current_quotes = {"up": (None, None), "dn": (None, None)}
            return

        # Inventory limit: don't fully pause — suppress BUY on overloaded side
        _inv_limit_suppress: set[str] = set()  # token keys to suppress BUY
        if should_pause and "Inventory limit" in reason:
            max_sh = self.config.max_inventory_shares
            if self.inventory.up_shares > max_sh:
                _inv_limit_suppress.add("up")
            if self.inventory.dn_shares > max_sh:
                _inv_limit_suppress.add("dn")
            # Mark as "soft pause" for UI, but keep quoting
            self._paused = True
            self._pause_reason = reason + " (selling to reduce)"
            should_pause = False  # don't block quoting below
            log.info(f"Inventory limit — suppressing BUY on {_inv_limit_suppress}, continuing to quote")

        if should_pause and not self._paused:
            self._paused = True
            self._pause_reason = reason
            await self.order_mgr.cancel_all()
            log.warning(f"MM PAUSED: {reason}")
            return
        elif not should_pause and self._paused:
            self._paused = False
            self._pause_reason = ""
            log.info("MM RESUMED")

        if self._paused and not _inv_limit_suppress:
            return

        # ── One-sided exposure check ──────────────────────────────
        up_sh = self.inventory.up_shares
        dn_sh = self.inventory.dn_shares
        has_position = (up_sh > 0.5 or dn_sh > 0.5)
        is_one_sided = has_position and (up_sh < 0.5 or dn_sh < 0.5)

        if is_one_sided:
            self._one_sided_counter += 1
            # Only trigger one-sided close after:
            # 1. Bot has been running for a minimum time (120s warmup)
            # 2. Less than 50% of window remains (don't close too early)
            min_run_time = 120.0  # seconds — give bot time to fill both sides
            elapsed = time.time() - self._started_at
            window_dur = (self.market.window_end - self.market.window_start) if self.market else 900
            time_left = self.market.time_remaining if self.market else 0
            past_halfway = time_left < (window_dur * 0.5)
            if (self._one_sided_counter >= self.config.max_one_sided_ticks
                    and elapsed >= min_run_time and past_halfway):
                log.warning(
                    f"One-sided exposure for {self._one_sided_counter} ticks "
                    f"({time_left:.0f}s left): "
                    f"UP={up_sh:.1f} DN={dn_sh:.1f} — early close")
                self._liq_lock = self.risk_mgr.lock_pnl(
                    self.inventory, fv_up, fv_dn, self.config.liq_price_floor_margin)
                self._liq_chunk_index = 0
                self._liq_last_chunk_time = 0.0
                self._is_closing = True
                await self.order_mgr.cancel_all()
                self._current_quotes = {"up": (None, None), "dn": (None, None)}
                return
        else:
            self._one_sided_counter = 0

        # ── Market quality check (every N ticks, live only) ─────────
        is_live = not hasattr(self.order_mgr.client, "_orders")
        if is_live and self._tick_count % self.config.quality_check_interval == 0:
            try:
                up_book = await self.order_mgr.get_full_book(self.market.up_token_id)
                dn_book = await self.order_mgr.get_full_book(self.market.dn_token_id)
                self._last_quality = self.quality_analyzer.analyze(
                    up_book, dn_book, fv_up, fv_dn)

                should_exit, reason = self.quality_analyzer.check_exit_conditions(
                    up_book, dn_book, fv_up, fv_dn, self.inventory)
                if should_exit:
                    log.warning(f"Early exit: {reason}")
                    self._liq_lock = self.risk_mgr.lock_pnl(
                        self.inventory, fv_up, fv_dn,
                        self.config.liq_price_floor_margin)
                    self._liq_chunk_index = 0
                    self._liq_last_chunk_time = 0.0
                    self._is_closing = True
                    await self.order_mgr.cancel_all()
                    self._current_quotes = {"up": (None, None), "dn": (None, None)}
                    return
            except Exception as e:
                log.debug(f"Quality check error: {e}")

        # 6. Generate quotes (with USDC budget cap)
        order_collateral = sum(
            self.order_mgr.required_collateral(q)
            for q in self.order_mgr.active_orders.values()
            if q.side == "BUY"
        )
        all_quotes = self.quote_engine.generate_all_quotes(
            fv_up, fv_dn,
            self.market.up_token_id, self.market.dn_token_id,
            self.inventory,
            vol, self.risk_mgr.avg_volatility,
            usdc_budget=self.inventory.initial_usdc,
            order_collateral=order_collateral,
            tick_size=self.market.tick_size if self.market else 0.01,
        )

        self._quote_count += 1

        # 6a-pre. Suppress BUY on inventory-overloaded sides
        if _inv_limit_suppress:
            for tk in _inv_limit_suppress:
                bid, ask = all_quotes[tk]
                if bid is not None:
                    log.info(f"Suppressing BUY {tk.upper()} due to inventory limit")
                    all_quotes[tk] = (None, ask)

        # 6a. Skip quoting sides where FV is too extreme (market already decided)
        min_fv = self.config.min_fv_to_quote
        if min_fv > 0:
            if fv_up < min_fv and all_quotes["up"][0] is not None:
                log.info(f"Skipping UP bid: FV={fv_up:.3f} < min_fv={min_fv}")
                all_quotes["up"] = (None, all_quotes["up"][1])
            if fv_dn < min_fv and all_quotes["dn"][0] is not None:
                log.info(f"Skipping DN bid: FV={fv_dn:.3f} < min_fv={min_fv}")
                all_quotes["dn"] = (None, all_quotes["dn"][1])

        # 6b. Fetch Polymarket book and clamp quotes to avoid crossing.
        for token_key, token_id in [
            ("up", self.market.up_token_id),
            ("dn", self.market.dn_token_id),
        ]:
            book = await self.order_mgr.get_book_summary(token_id)
            bid, ask = all_quotes[token_key]
            bid, ask = self.quote_engine.clamp_to_book(
                bid, ask, book["best_bid"], book["best_ask"],
                tick_size=self.market.tick_size,
            )
            # Re-apply max inventory cap after clamping (safety net)
            max_sh = self.config.max_inventory_shares
            if bid is not None and bid.size > max_sh:
                bid.size = round(max_sh, 2)
            if ask is not None and ask.size > max_sh:
                ask.size = round(max_sh, 2)
            all_quotes[token_key] = (bid, ask)

        # 7. Place or update orders
        # On Polymarket: SELL requires token inventory. Use BUY-only strategy:
        #   BUY UP @ bid_up  +  BUY DN @ bid_dn
        # BUY DN @ P implicitly provides ask-side for UP at (1 - P).
        # When both fill: hold UP+DN = $1, paid bid_up+bid_dn < $1 → profit.
        for token_key in ("up", "dn"):
            new_bid, new_ask = all_quotes[token_key]
            cur_bid, cur_ask = self._current_quotes.get(token_key, (None, None))

            need_bid = self.quote_engine.should_requote(cur_bid, new_bid)
            bid_notional = (new_bid.size * new_bid.price) if new_bid else 0.0
            if need_bid and bid_notional < self.config.min_order_size_usd:
                need_bid = False

            # Cancel stale bid when new bid is too small or None
            stale_bid = (not need_bid and cur_bid and cur_bid.order_id
                         and (new_bid is None
                              or bid_notional < self.config.min_order_size_usd))

            # SELL: cap size at available inventory (partial sells OK)
            token_bal = (self.inventory.up_shares if token_key == "up"
                         else self.inventory.dn_shares)
            if new_ask and token_bal > 0:
                new_ask.size = round(min(new_ask.size, token_bal), 2)
            ask_size = new_ask.size if new_ask else 0
            ask_price = new_ask.price if new_ask else 0
            need_ask = (self.quote_engine.should_requote(cur_ask, new_ask)
                        and token_bal > 0 and new_ask is not None)
            if need_ask and (ask_size * ask_price) < self.config.min_order_size_usd:
                need_ask = False

            # Also cancel stale ask if we no longer have inventory
            stale_ask = (cur_ask and cur_ask.order_id
                         and token_bal <= 0 and not need_ask)

            if need_bid or need_ask or stale_bid or stale_ask:
                self._requote_count += 1
                # Cancel old orders
                old_ids = []
                if (need_bid or stale_bid) and cur_bid and cur_bid.order_id:
                    old_ids.append(cur_bid.order_id)
                if (need_ask or stale_ask) and cur_ask and cur_ask.order_id:
                    old_ids.append(cur_ask.order_id)

                new_quotes = []
                if need_bid:
                    new_quotes.append(new_bid)
                if need_ask:
                    new_quotes.append(new_ask)

                new_ids = await self.order_mgr.cancel_replace(old_ids, new_quotes)

                # Update current quotes tracking
                updated_bid = new_bid if need_bid else (None if stale_bid else cur_bid)
                updated_ask = new_ask if need_ask else (None if stale_ask else cur_ask)
                self._current_quotes[token_key] = (updated_bid, updated_ask)

        # 8. Track spread for stats
        up_bid, up_ask = self._current_quotes.get("up", (None, None))
        if up_bid and up_ask and up_ask.price > 0:
            spread_bps = (up_ask.price - up_bid.price) / up_ask.price * 10000
            self._spread_samples.append(spread_bps)
            if len(self._spread_samples) > 1000:
                self._spread_samples = self._spread_samples[-500:]

    def _compute_fv(self) -> tuple[float, float]:
        """Compute current fair values from feed state. Returns (fv_up, fv_dn)."""
        st = self.feed_state
        mid = st.mid if st.mid and st.mid > 0 else 0
        if mid <= 0 or not self.market:
            return 0.5, 0.5
        klines = list(st.klines) if st.klines else []
        bids = list(st.bids) if st.bids else []
        asks = list(st.asks) if st.asks else []
        trades = list(st.trades) if st.trades else []
        return self.fair_value.compute(
            mid, self.market.strike,
            self.market.time_remaining, klines,
            bids, asks, trades,
        )

    async def _liquidate_inventory(self) -> None:
        """Smart liquidation with merge + 3-phase SELL exit.

        Phase 0 (Merge): Merge YES+NO pairs → $1 USDC via CTF contract.
            - Instant, no slippage, only gas cost.
        Phase 1 (Gradual Limit): time_left > taker_threshold
            - Sell in chunks (position / remaining_chunks)
            - Price = max(floor, FV - discount), improved to best_bid if higher
            - Post-only (maker)
        Phase 2 (Taker): time_left <= taker_threshold
            - Sell everything at best_bid, but only if best_bid >= floor
        Phase 3 (Abandon): best_bid < floor
            - Don't sell — let token expire (may resolve to $1)
        """
        if not self.market:
            return

        # ── Phase 0: Merge YES+NO pairs → USDC ──────────────────
        up_bal = await self.order_mgr.get_token_balance(self.market.up_token_id)
        dn_bal = await self.order_mgr.get_token_balance(self.market.dn_token_id)
        merge_amount = min(up_bal, dn_bal)

        if merge_amount >= 1.0 and self.market.condition_id:
            try:
                result = await self.order_mgr.merge_positions(
                    self.market.condition_id, merge_amount, self._private_key)
            except Exception as e:
                log.warning("Merge exception: %s", e)
                result = {"success": False, "error": str(e)}
            if result.get("success"):
                log.info(
                    "MERGE: %.2f pairs → $%.2f USDC",
                    merge_amount, merge_amount,
                )
                self.inventory.up_shares = max(0.0, self.inventory.up_shares - merge_amount)
                self.inventory.dn_shares = max(0.0, self.inventory.dn_shares - merge_amount)
                self.inventory.usdc += merge_amount
                self.inventory.up_cost.record_sell(merge_amount)
                self.inventory.dn_cost.record_sell(merge_amount)
                # Sync mock token balances for paper mode
                if hasattr(self.order_mgr.client, "_orders"):
                    for tid in [self.market.up_token_id, self.market.dn_token_id]:
                        cur = self.order_mgr._mock_token_balances.get(tid, 0.0)
                        self.order_mgr._mock_token_balances[tid] = max(0.0, cur - merge_amount)
            else:
                log.warning("Merge failed: %s", result.get("error", "unknown"))

        time_left = self.market.time_remaining
        cfg = self.config
        taker_threshold = cfg.liq_taker_threshold_sec
        use_taker = time_left <= taker_threshold

        # 1. Prune filled/cancelled liquidation orders
        active = set(self.order_mgr.active_order_ids)
        self._liquidation_order_ids &= active

        # 2. If switching to taker phase, cancel existing limit orders first
        if use_taker and self._liquidation_order_ids:
            log.info("Switching to taker liquidation — cancelling %d limit orders",
                     len(self._liquidation_order_ids))
            for oid in list(self._liquidation_order_ids):
                await self.order_mgr.cancel_order(oid)
            self._liquidation_order_ids.clear()

        # 3. If limit orders are still live, wait for chunk interval then cancel & re-place
        now = time.time()
        if self._liquidation_order_ids and not use_taker:
            elapsed = now - self._liq_last_chunk_time if self._liq_last_chunk_time else 0
            if elapsed < cfg.liq_chunk_interval_sec:
                log.info("Liquidation limit orders active (%d), waiting (%.0fs left, %.1fs since last)",
                         len(self._liquidation_order_ids), time_left, elapsed)
                return
            # Stale limit orders — cancel and re-place with updated price
            log.info("Cancelling %d stale liq orders after %.1fs, re-placing",
                     len(self._liquidation_order_ids), elapsed)
            for oid in list(self._liquidation_order_ids):
                await self.order_mgr.cancel_order(oid)
            self._liquidation_order_ids.clear()

        # 5. Pre-flight: ensure allowance (one-time, cached)
        is_live = not hasattr(self.order_mgr.client, "_orders")
        if is_live:
            await self.order_mgr.ensure_sell_allowance(self.market.up_token_id)
            await self.order_mgr.ensure_sell_allowance(self.market.dn_token_id)

        # 6. Compute fair values for pricing
        fv_up, fv_dn = self._compute_fv()

        has_real_balance = False
        placed_any = False

        for label, token_id in [
            ("UP", self.market.up_token_id),
            ("DN", self.market.dn_token_id),
        ]:
            real_balance = await self.order_mgr.get_token_balance(token_id)
            if real_balance <= 0.1:
                log.info(f"No real balance for {label}")
                continue

            has_real_balance = True
            is_up = token_id == self.market.up_token_id
            fv = fv_up if is_up else fv_dn
            book = await self.order_mgr.get_book_summary(token_id)
            best_bid = book["best_bid"]

            # Determine price floor from lock or cost basis
            floor = 0.01
            if cfg.liq_price_floor_enabled and self._liq_lock:
                floor = (self._liq_lock.min_sell_price_up if is_up
                         else self._liq_lock.min_sell_price_dn)
            elif cfg.liq_price_floor_enabled:
                cost = self.inventory.up_cost if is_up else self.inventory.dn_cost
                floor = max(0.01, cost.avg_entry_price + cfg.liq_price_floor_margin)

            # Adaptive floor decay: floor → 0.01 as time_left → 0
            base_floor = floor
            window_dur = self.market.window_end - self.market.window_start
            close_sec = min(cfg.close_window_sec, window_dur * 0.4)
            decay_ratio = max(0.0, min(1.0, time_left / close_sec)) if close_sec > 0 else 0.0
            floor = max(0.01, base_floor * decay_ratio)
            if floor != base_floor:
                log.info(f"{label}: adaptive floor {base_floor:.2f} → {floor:.2f} "
                         f"(decay={decay_ratio:.2f}, {time_left:.0f}s left)")

            # Adjust floor for taker fee when in taker mode
            if use_taker:
                floor = floor + self.config.taker_fee_rate * floor

            if use_taker:
                # ── Phase 2: Taker ──
                if not best_bid or best_bid <= 0:
                    if time_left < 5:
                        log.warning(f"{label}: No best_bid, last resort sell at $0.01 ({time_left:.0f}s left)")
                        best_bid = 0.01
                    else:
                        self._throttled_warn(
                            f"no_bid_{label}",
                            f"{label}: No best_bid for taker liquidation ({time_left:.0f}s left)",
                        )
                        continue

                if best_bid < floor and cfg.liq_abandon_below_floor:
                    if time_left > 5:
                        # Still time — wait for price recovery
                        log.warning(
                            f"{label}: best_bid={best_bid:.2f} < floor={floor:.2f}, "
                            f"waiting ({time_left:.0f}s left, {real_balance:.1f} shares)")
                        continue
                    else:
                        # < 5 seconds — force sell to avoid frozen tokens
                        log.warning(
                            f"{label} FORCE SELL: best_bid={best_bid:.2f} < floor={floor:.2f}, "
                            f"but only {time_left:.0f}s left ({real_balance:.1f} shares)")
                        sell_price = best_bid
                        post_only = False
                        phase = "FORCE_TAKER"
                        sell_size = round(real_balance, 2)
                else:
                    sell_price = best_bid
                    post_only = False
                    phase = "TAKER"
                    # Sell everything remaining (no buffer in taker mode)
                    sell_size = round(real_balance, 2)
            else:
                # ── Phase 1: Gradual Limit ──
                # Price: max(floor, FV - discount), improve to best_bid if higher
                sell_price = max(floor, fv - cfg.liq_max_discount_from_fv)
                if best_bid and best_bid > sell_price:
                    sell_price = best_bid  # improve, don't worsen

                if sell_price < 0.03:
                    sell_price = fv * 0.8 if fv > 0.05 else 0.03

                post_only = True

                # Chunk sizing: split remaining balance
                remaining_chunks = max(1, cfg.liq_gradual_chunks - self._liq_chunk_index)
                chunk_size = round(max(0, real_balance - 0.5) / remaining_chunks, 2)
                # PM minimum order size = 5 shares; if chunk < min, sell all at once
                pm_min_size = self.market.min_order_size if self.market else 5.0
                if chunk_size < pm_min_size and real_balance >= pm_min_size:
                    chunk_size = round(real_balance, 2)
                    log.info(f"{label}: chunk {chunk_size:.1f} < PM min {pm_min_size}, selling all at once")
                sell_size = chunk_size
                phase = f"LIMIT_CHUNK_{self._liq_chunk_index + 1}/{cfg.liq_gradual_chunks}"

            sell_price = round(max(0.01, min(0.99, sell_price)), 2)

            # Min sell: PM minimum order size (5 shares) or notional >= $0.50
            pm_min_size = self.market.min_order_size if self.market else 5.0
            min_sell = max(pm_min_size, 0.50 / sell_price) if sell_price > 0 else pm_min_size
            if sell_size < min_sell:
                if real_balance >= pm_min_size:
                    # Bump up to full balance to meet PM minimum
                    sell_size = round(real_balance, 2)
                    log.info(f"{label}: bumped sell_size to {sell_size:.1f} to meet PM min {pm_min_size}")
                else:
                    log.info(f"{label}: sell_size={sell_size:.1f} < PM min {pm_min_size} and balance too low — skipping")
                    continue

            sell_quote = Quote(
                side="SELL",
                price=sell_price,
                size=sell_size,
                token_id=token_id,
            )
            order_id = await self.order_mgr.place_order(sell_quote, post_only=post_only, fallback_taker=True)
            if order_id:
                self._liquidation_order_ids.add(order_id)
                placed_any = True
                log.info(
                    f"Liquidating {label}: SELL {sell_size:.1f}@{sell_price:.2f} "
                    f"({phase}, FV={fv:.2f}, floor={floor:.2f}, "
                    f"best_bid={best_bid or 0:.2f}, {time_left:.0f}s left)")
            else:
                self._throttled_warn(
                    f"liq_fail_{label}",
                    f"Liquidation {label} failed ({real_balance:.1f} shares) — retrying next tick",
                )

        # Advance chunk counter only when we actually placed orders
        if not use_taker and placed_any:
            self._liq_chunk_index = min(self._liq_chunk_index + 1,
                                        cfg.liq_gradual_chunks)
            self._liq_last_chunk_time = now

        if not has_real_balance:
            log.info("Liquidation complete — no inventory remaining")
            self._is_closing = False
            return

        # Check if all remaining balances are "dust" — below PM minimum, can't sell
        if has_real_balance and not placed_any and not self._liquidation_order_ids:
            pm_min = self.market.min_order_size if self.market else 5.0
            up_rem = await self.order_mgr.get_token_balance(self.market.up_token_id)
            dn_rem = await self.order_mgr.get_token_balance(self.market.dn_token_id)
            all_dust = (up_rem < pm_min and dn_rem < pm_min)
            if all_dust:
                fv_up_d, fv_dn_d = self._compute_fv()
                dust_value = up_rem * fv_up_d + dn_rem * fv_dn_d
                self._throttled_warn(
                    "liq_dust",
                    f"Liquidation dust: UP={up_rem:.2f} DN={dn_rem:.2f} "
                    f"(all < PM min {pm_min}). ~${dust_value:.2f} locked. "
                    f"Waiting for expiry.",
                    cooldown=30.0,
                )
                # Stay in _is_closing=True so bot doesn't resume quoting,
                # but stop spamming sell attempts — just wait for window end

    async def on_window_transition(self, new_market: MarketInfo) -> None:
        """Handle window transition — cancel all, switch tokens, resume."""
        log.info(f"Window transition: {new_market.coin} {new_market.timeframe}")

        # Cancel all existing orders
        await self.order_mgr.cancel_all()
        self._current_quotes = {"up": (None, None), "dn": (None, None)}

        # Reset cost basis and liquidation state for new window
        self.inventory.up_cost.reset()
        self.inventory.dn_cost.reset()
        self._liq_lock = None
        self._liq_chunk_index = 0
        self._liq_last_chunk_time = 0.0
        self._is_closing = False
        self._liquidation_order_ids.clear()
        self._one_sided_counter = 0

        # Update market info
        self.market = new_market
        log.info(f"New window: strike={new_market.strike:.2f} "
                 f"UP={new_market.up_token_id[:12]}... "
                 f"DN={new_market.dn_token_id[:12]}...")

    def snapshot(self) -> dict:
        """Get current state for dashboard API."""
        st = self.feed_state

        # Fair value
        fv_up, fv_dn = 0.5, 0.5
        vol = 0.0
        if self.market and st.mid and st.mid > 0:
            klines = list(st.klines) if st.klines else []
            bids = list(st.bids) if st.bids else []
            asks = list(st.asks) if st.asks else []
            trades = list(st.trades) if st.trades else []
            try:
                fv_up, fv_dn = self.fair_value.compute(
                    st.mid, self.market.strike,
                    self.market.time_remaining, klines,
                    bids, asks, trades)
                vol = self.fair_value.realized_vol(klines)
            except Exception:
                pass

        # PnL
        risk_stats = self.risk_mgr.get_stats(self.inventory, fv_up, fv_dn)

        # Order stats
        order_stats = self.order_mgr.get_stats()

        # Avg spread
        avg_spread = (sum(self._spread_samples) / len(self._spread_samples)
                      if self._spread_samples else 0.0)

        # Current quotes
        up_bid, up_ask = self._current_quotes.get("up", (None, None))
        dn_bid, dn_ask = self._current_quotes.get("dn", (None, None))

        # Recent fills (last 50)
        recent = self.risk_mgr.fills[-50:]
        fills_data = [
            {
                "ts": f.ts,
                "side": f.side,
                "token_id": f.token_id[:12] + "...",
                "price": f.price,
                "size": f.size,
                "fee": f.fee,
                "is_maker": f.is_maker,
            }
            for f in reversed(recent)
        ]

        # Real session PnL based on PM balances
        # = (current_usdc + position_value_at_pm_prices) - starting_usdc
        _pm_up_price = st.pm_up if hasattr(st, "pm_up") and st.pm_up else fv_up
        _pm_dn_price = st.pm_dn if hasattr(st, "pm_dn") and st.pm_dn else fv_dn
        _position_value = (self.inventory.up_shares * _pm_up_price +
                           self.inventory.dn_shares * _pm_dn_price)
        _session_pnl = (self._cached_usdc_balance + _position_value) - self._starting_usdc_pm

        return {
            # Market info
            "market": {
                "coin": self.market.coin if self.market else "",
                "timeframe": self.market.timeframe if self.market else "",
                "strike": self.market.strike if self.market else 0,
                "time_remaining": self.market.time_remaining if self.market else 0,
                "up_token": self.market.up_token_id[:12] + "..." if self.market else "",
                "dn_token": self.market.dn_token_id[:12] + "..." if self.market else "",
                "tick_size": self.market.tick_size if self.market else 0.01,
                "min_order_size": self.market.min_order_size if self.market else 5.0,
                "market_type": self.market.market_type if self.market else "up_down",
                "resolution_source": self.market.resolution_source if self.market else "unknown",
            },

            # Fair value
            "fair_value": {
                "up": round(fv_up, 4),
                "dn": round(fv_dn, 4),
                "binance_mid": round(st.mid, 2) if st.mid else 0,
                "volatility": round(vol, 6),
            },

            # Current quotes
            "quotes": {
                "up_bid": {"price": up_bid.price, "size": up_bid.size} if up_bid else None,
                "up_ask": {"price": up_ask.price, "size": up_ask.size} if up_ask else None,
                "dn_bid": {"price": dn_bid.price, "size": dn_bid.size} if dn_bid else None,
                "dn_ask": {"price": dn_ask.price, "size": dn_ask.size} if dn_ask else None,
            },

            # Inventory
            "inventory": {
                "up_shares": round(self.inventory.up_shares, 2),
                "dn_shares": round(self.inventory.dn_shares, 2),
                "net_delta": round(self.inventory.net_delta, 2),
                "usdc": round(self.inventory.usdc, 2),
                "up_avg_entry": round(self.inventory.up_cost.avg_entry_price, 4),
                "dn_avg_entry": round(self.inventory.dn_cost.avg_entry_price, 4),
            },

            # Liquidation lock
            "liquidation_lock": {
                "active": self._liq_lock is not None,
                "trigger_pnl": round(self._liq_lock.trigger_pnl, 4) if self._liq_lock else 0,
                "up_floor": round(self._liq_lock.min_sell_price_up, 2) if self._liq_lock else 0,
                "dn_floor": round(self._liq_lock.min_sell_price_dn, 2) if self._liq_lock else 0,
                "chunk_index": self._liq_chunk_index,
                "total_chunks": self.config.liq_gradual_chunks,
            },

            # PnL & Risk
            **risk_stats,
            "avg_spread_bps": round(avg_spread, 1),
            "session_pnl": round(_session_pnl, 4),
            "starting_usdc_pm": round(self._starting_usdc_pm, 2),
            "is_paused": self._paused,
            "pause_reason": self._pause_reason,
            "is_closing": self._is_closing,
            "is_running": self._running,

            # Orders
            **order_stats,

            # Fills
            "recent_fills": fills_data,

            # Session
            "quote_count": self._quote_count,
            "requote_count": self._requote_count,
            "started_at": self._started_at,
            "uptime_sec": round(time.time() - self._started_at, 1) if self._started_at else 0,

            # Heartbeat
            "heartbeat": self.heartbeat.stats,

            # Rebate
            "rebate": self.rebate.stats,

            # Config (current)
            "config": self.config.to_dict(),

            # PM prices from feed
            "pm_prices": {
                "up": st.pm_up if hasattr(st, "pm_up") else 0,
                "dn": st.pm_dn if hasattr(st, "pm_dn") else 0,
            },

            # Real USDC balance on Polymarket
            "usdc_balance_pm": round(self._cached_usdc_balance, 2),

            # Active orders detail
            "active_orders_detail": self.order_mgr.get_active_orders_detail(
                liquidation_ids=self._liquidation_order_ids,
                up_token_id=self.market.up_token_id if self.market else "",
                dn_token_id=self.market.dn_token_id if self.market else "",
            ),

            # Market quality
            "market_quality": {
                "overall_score": round(self._last_quality.overall_score, 3) if self._last_quality else None,
                "liquidity_score": round(self._last_quality.liquidity_score, 3) if self._last_quality else None,
                "spread_score": round(self._last_quality.spread_score, 3) if self._last_quality else None,
                "spread_bps": round(self._last_quality.spread_bps, 1) if self._last_quality else None,
                "bid_depth_usd": round(self._last_quality.bid_depth_usd, 2) if self._last_quality else None,
                "ask_depth_usd": round(self._last_quality.ask_depth_usd, 2) if self._last_quality else None,
                "tradeable": self._last_quality.tradeable if self._last_quality else None,
                "reason": self._last_quality.reason if self._last_quality else "",
            },
        }
