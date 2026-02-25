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
            on_failure=self._schedule_heartbeat_failure,
        )
        self.rebate = RebateTracker(clob_client)
        self.quality_analyzer = MarketQualityAnalyzer(config)

        # State
        self.inventory = Inventory()
        self.market: Optional[MarketInfo] = None
        self._running = False
        self._emergency_flag = False
        self._emergency_stopped = False
        self._paused = False
        self._pause_reason = ""
        self._task: Optional[asyncio.Task] = None
        self._heartbeat_failure_task: Optional[asyncio.Task] = None
        self._started_at: float = 0.0
        self._is_closing = False
        self._liquidation_attempted = False
        self._liquidation_order_ids: set[str] = set()
        self._cached_usdc_balance: float = 0.0
        self._cached_pm_up_shares: float = 0.0
        self._cached_pm_dn_shares: float = 0.0
        self._starting_usdc_pm: float = 0.0
        self._starting_portfolio_pm: float = 0.0  # USDC + token values at start
        self._pnl_grace_until: float = 0.0  # Skip PnL risk checks until this timestamp
        self._catastrophic_count: int = 0  # Consecutive CATASTROPHIC readings
        self._catastrophic_threshold: int = 3  # Readings required before shutdown
        self._last_quality: MarketQuality | None = None
        self._quality_error_count: int = 0
        self._quality_success_count: int = 0
        self._quality_pause_active: bool = False
        self._liq_lock: LiquidationLock | None = None
        self._liq_chunk_index: int = 0
        self._liq_last_chunk_time: float = 0.0
        self._one_sided_counter: int = 0
        self._merge_failed_this_cycle: bool = False
        self._closing_start_time_left: float = 0.0
        self._requote_event: asyncio.Event = asyncio.Event()
        self._imbalance_start_ts: float = 0.0
        self._imbalance_adjustments: dict = {
            "leading_spread_mult": 1.0,
            "lagging_spread_mult": 1.0,
            "skew_mult": 1.0,
            "tier": 0,
            "suppress_leading_buy": False,
            "force_taker_lagging": False,
        }
        self._taker_quotes: list[Quote] = []
        self._reconcile_prev_pm: tuple[float, float] | None = None
        self._reconcile_stable_count: int = 0
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

        # Latency metrics
        self._last_tick_ms: float = 0.0
        self._avg_tick_ms: float = 0.0
        self._tick_ms_samples: list[float] = []
        self._last_book_ms: float = 0.0
        self._last_order_ms: float = 0.0
        self._last_fills_ms: float = 0.0
        self._last_reconcile_ms: float = 0.0
        self._last_fv_ms: float = 0.0
        self._last_quotes_ms: float = 0.0
        self._last_orders_ms: float = 0.0

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

    def _schedule_heartbeat_failure(self) -> None:
        """Schedule async heartbeat failure handler from sync callback context."""
        if self._heartbeat_failure_task and not self._heartbeat_failure_task.done():
            return
        try:
            loop = asyncio.get_running_loop()
        except RuntimeError:
            log.error("Heartbeat failure callback invoked without a running event loop")
            return
        self._heartbeat_failure_task = loop.create_task(self._on_heartbeat_failure())

    async def _on_heartbeat_failure(self) -> None:
        """Handle fatal heartbeat failure (triggered after 3 consecutive misses)."""
        reason = "Heartbeat failed 3 consecutive times"
        log.warning("Heartbeat failure callback triggered: %s", reason)
        try:
            await self._emergency_shutdown(reason)
        finally:
            # Always clear local order/quote state even if exchange cancel raised.
            self.order_mgr.clear_local_order_tracking()
            self._current_quotes = {
                "up": (None, None),
                "dn": (None, None),
            }

    async def _emergency_shutdown(self, reason: str) -> None:
        """Best-effort fatal shutdown with explicit order and heartbeat cleanup."""
        log.critical("EMERGENCY SHUTDOWN: %s", reason)
        self._emergency_flag = True  # Signal in-flight tick to stop placing orders
        self._emergency_stopped = True
        self._is_closing = True
        try:
            await self.order_mgr.cancel_all(force_exchange=True)
        except Exception as e:
            log.error("Emergency cancel_all failed: %s", e, exc_info=True)

        try:
            await self.heartbeat.stop()
        except Exception as e:
            log.error("Emergency heartbeat stop failed: %s", e, exc_info=True)

        self._running = False
        self._paused = True
        self._pause_reason = reason
        self._is_closing = True

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
        self._emergency_flag = False
        self._emergency_stopped = False

        # Cancel ALL existing orders first — prevents stale orders from previous
        # crashed sessions (GTD orders can survive up to 5 minutes after crash)
        try:
            cancelled = await self.order_mgr.cancel_all(force_exchange=True)
            if cancelled:
                log.info("Startup: cancelled %d stale orders from previous session", cancelled)
                await asyncio.sleep(1.0)  # Wait for PM to settle after cancels
        except Exception as e:
            log.warning("Startup: cancel_all failed: %s", e)

        # Snapshot starting portfolio (USDC + token values) for real session PnL
        try:
            (real_up, real_dn), starting_usdc = await asyncio.gather(
                self.order_mgr.get_all_token_balances(
                    self.market.up_token_id, self.market.dn_token_id,
                ),
                self.order_mgr.get_usdc_balance(),
            )
            if starting_usdc is None:
                log.warning("Failed to fetch starting USDC balance, defaulting to 0.0")
                starting_usdc = 0.0
            self._starting_usdc_pm = starting_usdc
            self._cached_usdc_balance = starting_usdc
            self._cached_pm_up_shares = real_up if real_up is not None else 0.0
            self._cached_pm_dn_shares = real_dn if real_dn is not None else 0.0
            # Include pre-existing tokens in starting portfolio
            # Wait for valid PM prices (WS feed) before computing starting portfolio
            _fv_up, _fv_dn = 0.0, 0.0
            for _price_attempt in range(10):
                _fv_up = getattr(self.feed_state, "pm_up", 0.0) or 0.0
                _fv_dn = getattr(self.feed_state, "pm_dn", 0.0) or 0.0
                if _fv_up > 0 and _fv_dn > 0:
                    break
                log.info("Waiting for PM prices before starting (attempt %d/10)...", _price_attempt + 1)
                await asyncio.sleep(1.0)
            if _fv_up <= 0 or _fv_dn <= 0:
                log.critical(
                    "PM prices not available after 10s! Using 0.5 fallback. "
                    "Starting PnL will be UNRELIABLE until prices arrive."
                )
                _fv_up = _fv_up if _fv_up > 0 else 0.5
                _fv_dn = _fv_dn if _fv_dn > 0 else 0.5
            _token_value = self._cached_pm_up_shares * _fv_up + self._cached_pm_dn_shares * _fv_dn
            self._starting_portfolio_pm = starting_usdc + _token_value
            # Initialize internal inventory from PM balances (pre-existing tokens
            # from previous sessions must be tracked to avoid phantom PnL)
            if self._cached_pm_up_shares > 0 or self._cached_pm_dn_shares > 0:
                self.inventory.up_shares = self._cached_pm_up_shares
                self.inventory.dn_shares = self._cached_pm_dn_shares
                self.inventory.usdc = starting_usdc
                # Set cost basis from current prices (best guess for pre-existing)
                if self._cached_pm_up_shares > 0:
                    self.inventory.up_cost.total_shares = self._cached_pm_up_shares
                    self.inventory.up_cost.total_cost = self._cached_pm_up_shares * _fv_up
                if self._cached_pm_dn_shares > 0:
                    self.inventory.dn_cost.total_shares = self._cached_pm_dn_shares
                    self.inventory.dn_cost.total_cost = self._cached_pm_dn_shares * _fv_dn
                log.warning(
                    "Pre-existing tokens found: UP=%.2f DN=%.2f — initialized inventory from PM",
                    self._cached_pm_up_shares, self._cached_pm_dn_shares,
                )
            log.info(
                "Starting portfolio: USDC=$%.2f + tokens=$%.2f (UP=%.2f@%.3f DN=%.2f@%.3f) = $%.2f",
                starting_usdc, _token_value,
                self._cached_pm_up_shares, _fv_up,
                self._cached_pm_dn_shares, _fv_dn,
                self._starting_portfolio_pm,
            )
        except Exception:
            self._starting_usdc_pm = 0.0
            self._starting_portfolio_pm = 0.0
            self._cached_usdc_balance = 0.0
            self._cached_pm_up_shares = 0.0
            self._cached_pm_dn_shares = 0.0
        self._started_at = time.time()
        self._pnl_grace_until = self._started_at + 30.0  # 30s grace period for PnL checks
        self._catastrophic_count = 0
        self._paused = False
        self._pause_reason = ""
        self._heartbeat_failure_task = None
        self._tick_count = 0
        self._is_closing = False
        self._liquidation_attempted = False
        self._liquidation_order_ids = set()
        self._liq_lock = None
        self._liq_chunk_index = 0
        self._liq_last_chunk_time = 0.0
        self._one_sided_counter = 0
        self._quality_error_count = 0
        self._quality_success_count = 0
        self._quality_pause_active = False
        self._imbalance_start_ts = 0.0
        self._imbalance_adjustments = {
            "leading_spread_mult": 1.0,
            "lagging_spread_mult": 1.0,
            "skew_mult": 1.0,
            "tier": 0,
            "suppress_leading_buy": False,
            "force_taker_lagging": False,
        }
        self._taker_quotes = []
        self.risk_mgr.reset()

        # Set budget cap on order manager (enforced at placement time)
        self.order_mgr._session_budget = self.inventory.initial_usdc
        self.order_mgr._session_spent = 0.0

        # Wire fill callback → trigger immediate requote
        self.order_mgr.set_fill_callback(lambda: self._requote_event.set())

        # Wire heartbeat ID sync: order/cancel responses may contain new ID
        self.order_mgr.set_heartbeat_id_callback(self.heartbeat.update_id)

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
        """Main quoting loop — event-driven with timeout fallback."""
        log.info("Quote loop started")
        try:
            while self._running:
                try:
                    try:
                        tick_timeout = 15.0 if self._is_closing else 10.0
                        await asyncio.wait_for(self._tick(), timeout=tick_timeout)
                    except asyncio.TimeoutError:
                        self._log.warning("_tick() timed out after %.0fs, skipping iteration",
                                          15.0 if self._is_closing else 10.0)
                except asyncio.CancelledError:
                    break
                except Exception as e:
                    log.error(f"Tick error: {e}", exc_info=True)

                # Wait for event OR timeout (whichever comes first)
                try:
                    await asyncio.wait_for(
                        self._requote_event.wait(),
                        timeout=self.config.requote_interval_sec,
                    )
                    self._requote_event.clear()
                except asyncio.TimeoutError:
                    pass  # Normal tick on timeout
        except asyncio.CancelledError:
            pass
        log.info("Quote loop ended")

    async def _tick(self) -> None:
        """Single iteration of the quote loop."""
        if not self.market:
            return
        self._tick_count += 1
        _t0 = time.perf_counter()

        # ── Periodic snapshot (every 10 ticks ≈ 20s) ────────────
        if self._on_snapshot_callbacks and self._tick_count % 10 == 0:
            try:
                snap = self.snapshot()
                for cb in self._on_snapshot_callbacks:
                    try:
                        cb(snap)
                    except Exception as e:
                        log.warning("Snapshot callback error: %s", e)
            except Exception as e:
                log.warning("Snapshot build error: %s", e)

        # ── End-of-window management ─────────────────────────────
        time_left = self.market.time_remaining

        if time_left <= 0:
            # Window expired — liquidate and stop
            if not self._is_closing:
                self._is_closing = True
                self._closing_start_time_left = max(time_left, 1.0)
                self._merge_failed_this_cycle = False
                fv_up, fv_dn = self._compute_fv()
                self._liq_lock = self.risk_mgr.lock_pnl(
                    self.inventory, fv_up, fv_dn,
                    self.config.liq_price_floor_margin)
                self._liq_chunk_index = 0
                self._liq_last_chunk_time = 0.0
                await self.order_mgr.cancel_all()
                self._current_quotes = {"up": (None, None), "dn": (None, None)}
                log.warning("Window expired — entering closing mode")

            # Retry liquidation up to 3 times with 3s gaps.
            for attempt in range(3):
                await self._liquidate_inventory()
                await asyncio.sleep(3.0)
                has_up = self.inventory.up_shares > 0.5
                has_dn = self.inventory.dn_shares > 0.5
                if not has_up and not has_dn:
                    log.info("Liquidation complete after %d attempts", attempt + 1)
                    break
                log.warning(
                    "Liquidation attempt %d: still holding UP=%.1f DN=%.1f",
                    attempt + 1, self.inventory.up_shares, self.inventory.dn_shares
                )
            self._running = False
            return

        # Adaptive close window: min(config, 40% of window) — so 5m=120s, 15m=120s, 1h=120s
        window_dur = self.market.window_end - self.market.window_start
        close_sec = min(self.config.close_window_sec, window_dur * 0.4)

        if time_left <= close_sec and not self._is_closing:
            self._is_closing = True
            self._closing_start_time_left = time_left
            self._merge_failed_this_cycle = False
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
                except Exception as e:
                    log.warning("Fill callback error: %s", e)
        _t_fills = time.perf_counter()

        # Live mode: periodically reconcile internal shares with PM balances.
        # Skip during closing to save HTTP calls (liquidation does its own balance checks).
        # Uses debounce: only reconcile if PM values are stable for 3+ consecutive checks
        # to avoid oscillation from PM balance API lagging behind fill detection.
        is_live = not hasattr(self.order_mgr.client, "_orders")
        reconcile_requested = self.order_mgr.reconcile_requested
        if is_live and not self._is_closing and (self._tick_count % 5 == 0 or reconcile_requested):
            (real_up, real_dn), usdc_bal = await asyncio.gather(
                self.order_mgr.get_all_token_balances(
                    self.market.up_token_id,
                    self.market.dn_token_id,
                ),
                self.order_mgr.get_usdc_balance(),
            )
            if usdc_bal is not None:
                self._cached_usdc_balance = usdc_bal
            else:
                log.warning("Failed to refresh USDC balance, keeping previous cached value")

            if real_up is None or real_dn is None:
                log.error("Skipping inventory reconcile: failed to fetch PM token balances")
                self._reconcile_stable_count = 0
                self._reconcile_prev_pm = None
            else:
                # Cache real PM token balances for accurate PnL calculation
                self._cached_pm_up_shares = real_up
                self._cached_pm_dn_shares = real_dn
                if reconcile_requested:
                    log.warning(
                        "Forced reconcile after anomalous fill: internal UP=%.2f DN=%.2f "
                        "→ PM UP=%.2f DN=%.2f",
                        self.inventory.up_shares, self.inventory.dn_shares,
                        real_up, real_dn,
                    )
                    self.inventory.reconcile(real_up, real_dn, self._cached_usdc_balance)
                    self._reconcile_stable_count = 0
                    self._reconcile_prev_pm = None
                    self.order_mgr.clear_reconcile_request()
                else:
                    up_diff = abs(real_up - self.inventory.up_shares)
                    dn_diff = abs(real_dn - self.inventory.dn_shares)
                    if up_diff > 1.0 or dn_diff > 1.0:
                        prev = self._reconcile_prev_pm
                        pm_stable = (prev is not None
                                     and abs(real_up - prev[0]) < 0.5
                                     and abs(real_dn - prev[1]) < 0.5)
                        self._reconcile_prev_pm = (real_up, real_dn)

                        if pm_stable:
                            self._reconcile_stable_count += 1
                        else:
                            self._reconcile_stable_count = 1

                        if self._reconcile_stable_count >= 3:
                            log.warning(
                                "Inventory reconcile (confirmed %d checks): "
                                "internal UP=%.2f DN=%.2f → PM UP=%.2f DN=%.2f",
                                self._reconcile_stable_count,
                                self.inventory.up_shares, self.inventory.dn_shares,
                                real_up, real_dn,
                            )
                            self.inventory.reconcile(real_up, real_dn, self._cached_usdc_balance)
                            self._reconcile_stable_count = 0
                            self._reconcile_prev_pm = None
                        else:
                            log.info(
                                "Inventory drift (%d/3): internal UP=%.2f DN=%.2f, PM UP=%.2f DN=%.2f",
                                self._reconcile_stable_count,
                                self.inventory.up_shares, self.inventory.dn_shares,
                                real_up, real_dn,
                            )
                    else:
                        self._reconcile_stable_count = 0
                        self._reconcile_prev_pm = None

        _t_reconcile = time.perf_counter()

        if self._is_closing:
            self._imbalance_start_ts = 0.0
            self._imbalance_adjustments = {
                "leading_spread_mult": 1.0,
                "lagging_spread_mult": 1.0,
                "skew_mult": 1.0,
                "tier": 0,
                "suppress_leading_buy": False,
                "force_taker_lagging": False,
            }
            self._taker_quotes = []
            # Continuously try to sell remaining inventory each tick
            await self._liquidate_inventory()
            return

        st = self.feed_state

        # 1. Defensive copies of feed data
        mid = st.mid
        now = time.time()
        last_ok_ts = getattr(st, "binance_ob_last_ok_ts", 0.0) or 0.0
        if last_ok_ts > 0:
            staleness = now - last_ok_ts
            is_stale = staleness > 5.0
        else:
            staleness = now - self._started_at if self._started_at > 0 else 0.0
            is_stale = staleness > 10.0

        if is_stale:
            log.warning("Binance feed stale (%.1fs), cancelling orders and skipping tick", staleness)
            await self.order_mgr.cancel_all()
            self._current_quotes = {"up": (None, None), "dn": (None, None)}
            return

        bids = list(st.bids) if st.bids else []
        asks = list(st.asks) if st.asks else []
        trades = list(st.trades) if st.trades else []
        klines = list(st.klines) if st.klines else []

        if not mid or mid <= 0:
            return

        try:
            strike = float(self.market.strike)
        except (TypeError, ValueError):
            strike = 0.0
        if strike <= 0 or strike > 200000:
            self._throttled_warn(
                "invalid_strike",
                "Strike invalid/unavailable — cancelling all orders and staying in watch mode",
            )
            await self.order_mgr.cancel_all()
            self._current_quotes = {"up": (None, None), "dn": (None, None)}
            return

        # 3. Compute fair value
        fv_up, fv_dn = self.fair_value.compute(
            mid=mid,
            strike=strike,
            time_remaining_sec=self.market.time_remaining,
            klines=klines,
            bids=bids,
            asks=asks,
            trades=trades,
        )

        _t_fv = time.perf_counter()

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
        # Compute session PnL using REAL PM balances (not internal inventory which may drift).
        # USDC balance is refreshed after order placement each tick, so it's always current.
        _pm_up = st.pm_up if hasattr(st, "pm_up") and st.pm_up else fv_up
        _pm_dn = st.pm_dn if hasattr(st, "pm_dn") and st.pm_dn else fv_dn
        _pos_value = self._cached_pm_up_shares * _pm_up + self._cached_pm_dn_shares * _pm_dn
        _current_portfolio = self._cached_usdc_balance + _pos_value
        _session_pnl = (_current_portfolio - self._starting_portfolio_pm) if self._starting_portfolio_pm > 0 else None

        # Grace period: skip PnL-based risk checks for first 30s while balances settle
        _in_grace = time.time() < self._pnl_grace_until
        _risk_pnl = None if _in_grace else _session_pnl
        if _in_grace and self._tick_count % 10 == 0:
            log.info("PnL grace period active (%.0fs left), risk checks use fill-based PnL",
                     self._pnl_grace_until - time.time())

        should_pause, reason = self.risk_mgr.should_pause(
            self.inventory, vol, fv_up, fv_dn, session_pnl=_risk_pnl)

        # Exit triggers (TP, trailing stop, drawdown) ALWAYS take priority — even if already paused
        if should_pause and ("Take profit" in reason or "Trailing stop" in reason or "Max drawdown" in reason):
            # For Max drawdown: require confirmation (grace period + consecutive readings)
            if "Max drawdown" in reason:
                if _in_grace:
                    log.info("Max drawdown trigger skipped (grace period): %s", reason)
                    should_pause = False
                else:
                    self._catastrophic_count += 1
                    if self._catastrophic_count < self._catastrophic_threshold:
                        log.warning(
                            "Max drawdown reading %d/%d: %s",
                            self._catastrophic_count, self._catastrophic_threshold, reason,
                        )
                        should_pause = False  # Don't act yet, wait for confirmation
                    else:
                        log.warning("Max drawdown CONFIRMED (%d readings): %s",
                                    self._catastrophic_count, reason)
                        self._catastrophic_count = 0
            if should_pause:
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
                self._closing_start_time_left = self.market.time_remaining
                self._merge_failed_this_cycle = False
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
            # Net delta breach: suppress BUY on the leading (higher shares) side
            if "net delta" in reason:
                if self.inventory.up_shares >= self.inventory.dn_shares:
                    _inv_limit_suppress.add("up")
                else:
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
        elif not should_pause and self._paused and not self._quality_pause_active:
            self._paused = False
            self._pause_reason = ""
            log.info("MM RESUMED")

        if self._paused and not _inv_limit_suppress and not self._quality_pause_active:
            return

        # Reset catastrophic counter when PnL is healthy.
        # Only reset if we're NOT in the middle of a drawdown confirmation cycle.
        # The exit trigger block sets should_pause=False while waiting for confirmation,
        # so we check _session_pnl directly instead of relying on should_pause.
        if _session_pnl is not None and _session_pnl > -self.config.max_drawdown_usd:
            if self._catastrophic_count > 0:
                log.info("PnL recovered ($%.2f), resetting catastrophic counter", _session_pnl)
            self._catastrophic_count = 0

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
                self._closing_start_time_left = time_left
                self._merge_failed_this_cycle = False
                await self.order_mgr.cancel_all()
                self._current_quotes = {"up": (None, None), "dn": (None, None)}
                return
        else:
            self._one_sided_counter = 0

        # ── Market quality check (every N ticks, live only) ─────────
        is_live = not hasattr(self.order_mgr.client, "_orders")
        if is_live and self._tick_count % self.config.quality_check_interval == 0:
            try:
                up_book, dn_book = await asyncio.gather(
                    self.order_mgr.get_full_book(self.market.up_token_id),
                    self.order_mgr.get_full_book(self.market.dn_token_id),
                )
                self._last_quality = self.quality_analyzer.analyze(
                    up_book, dn_book, fv_up, fv_dn)
                self._quality_error_count = 0
                if self._quality_pause_active:
                    self._quality_success_count += 1
                    log.info(
                        "Market quality recovery check %d/3 passed",
                        self._quality_success_count,
                    )
                    if self._quality_success_count >= 3:
                        self._quality_pause_active = False
                        self._quality_success_count = 0
                        if not should_pause and self._pause_reason == "Market quality degraded":
                            self._paused = False
                            self._pause_reason = ""
                            log.info("MM RESUMED: market quality recovered")
                else:
                    self._quality_success_count = 0

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
                    self._closing_start_time_left = self.market.time_remaining
                    self._merge_failed_this_cycle = False
                    await self.order_mgr.cancel_all()
                    self._current_quotes = {"up": (None, None), "dn": (None, None)}
                    return
            except Exception as e:
                self._quality_error_count += 1
                self._quality_success_count = 0
                log.error(
                    "Quality check error (%d/3): %s",
                    self._quality_error_count, e,
                )
                if self._quality_error_count >= 3:
                    if not self._quality_pause_active:
                        self._quality_pause_active = True
                        self._paused = True
                        self._pause_reason = "Market quality degraded"
                        await self.order_mgr.cancel_all()
                        self._current_quotes = {"up": (None, None), "dn": (None, None)}
                    log.critical("Market quality degraded, pausing")
                return

        if self._quality_pause_active:
            return

        # 5b. Paired filling imbalance escalation (skip during closing mode).
        imbalance = abs(self.inventory.up_shares - self.inventory.dn_shares)
        now_ts = time.time()
        if imbalance > 2.0:
            if self._imbalance_start_ts <= 0:
                self._imbalance_start_ts = now_ts
            imbalance_duration = max(0.0, now_ts - self._imbalance_start_ts)
        else:
            self._imbalance_start_ts = 0.0
            imbalance_duration = 0.0

        prev_tier = int(self._imbalance_adjustments.get("tier", 0))
        self._imbalance_adjustments = self.quote_engine.compute_imbalance_adjustments(
            self.inventory,
            imbalance,
            imbalance_duration,
        )
        tier = int(self._imbalance_adjustments.get("tier", 0))
        if tier != prev_tier:
            leading_key = "up" if self.inventory.up_shares > self.inventory.dn_shares else (
                "dn" if self.inventory.dn_shares > self.inventory.up_shares else "none"
            )
            lagging_key = "dn" if leading_key == "up" else ("up" if leading_key == "dn" else "none")
            log.info(
                "Paired filling tier %d: imbalance=%.2f duration=%.1fs leading=%s lagging=%s",
                tier,
                imbalance,
                imbalance_duration,
                leading_key.upper(),
                lagging_key.upper(),
            )

        if fills and imbalance > 2.0:
            self._requote_event.set()

        leading_token: str | None = None
        lagging_token: str | None = None
        if self.inventory.up_shares > self.inventory.dn_shares:
            leading_token, lagging_token = "up", "dn"
        elif self.inventory.dn_shares > self.inventory.up_shares:
            leading_token, lagging_token = "dn", "up"

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
            imbalance_adjustments=self._imbalance_adjustments,
            time_remaining=time_left,
        )

        self._quote_count += 1

        tick_size = self.market.tick_size if self.market else 0.01

        def _round_to_tick(price: float) -> float:
            rounded = round(round(price / tick_size) * tick_size, 10)
            return max(tick_size, min(1.0 - tick_size, rounded))

        def _apply_spread_multiplier(token_key: str, spread_mult: float) -> None:
            if spread_mult <= 0:
                return
            bid, ask = all_quotes[token_key]
            if bid is None or ask is None:
                return

            mid_px = (bid.price + ask.price) / 2.0
            half = max(tick_size * 0.5, ((ask.price - bid.price) / 2.0) * spread_mult)
            bid.price = _round_to_tick(mid_px - half)
            ask.price = _round_to_tick(mid_px + half)

            # Maintain at least one tick spread after rounding.
            if bid.price >= ask.price:
                bid.price = _round_to_tick(ask.price - tick_size)
                if bid.price >= ask.price:
                    ask.price = _round_to_tick(bid.price + tick_size)

            all_quotes[token_key] = (bid, ask)

        # 6a-pre. Paired filling spread asymmetry and hard side suppression.
        if leading_token:
            _apply_spread_multiplier(
                leading_token,
                float(self._imbalance_adjustments.get("leading_spread_mult", 1.0)),
            )
        if lagging_token:
            _apply_spread_multiplier(
                lagging_token,
                float(self._imbalance_adjustments.get("lagging_spread_mult", 1.0)),
            )
        if leading_token and self._imbalance_adjustments.get("suppress_leading_buy", False):
            lead_bid, lead_ask = all_quotes[leading_token]
            all_quotes[leading_token] = (None, lead_ask)

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

        self._taker_quotes = []
        taker_lagging_key: str | None = None
        if lagging_token and self._imbalance_adjustments.get("force_taker_lagging", False):
            lag_bid, _ = all_quotes[lagging_token]
            if lag_bid is not None:
                self._taker_quotes.append(lag_bid)
                taker_lagging_key = lagging_token

        # 6b. Fetch Polymarket book and clamp quotes to avoid crossing.
        # Use cached WS prices when both sides are fresh (0ms), fallback to HTTP.
        ws_fresh = (
            getattr(st, "pm_connected", False)
            and (time.time() - getattr(st, "pm_last_update_ts", 0)) < 10
        )
        # Pre-fetch book summaries in parallel if WS data is stale.
        books_cache = {}
        if not ws_fresh:
            up_summary, dn_summary = await asyncio.gather(
                self.order_mgr.get_book_summary(self.market.up_token_id),
                self.order_mgr.get_book_summary(self.market.dn_token_id),
            )
            books_cache["up"] = up_summary
            books_cache["dn"] = dn_summary
        for token_key, token_id in [
            ("up", self.market.up_token_id),
            ("dn", self.market.dn_token_id),
        ]:
            ws_bid = st.pm_up_bid if token_key == "up" else st.pm_dn_bid
            ws_ask = st.pm_up if token_key == "up" else st.pm_dn
            if ws_fresh and ws_bid is not None and ws_ask is not None:
                book = {"best_bid": ws_bid, "best_ask": ws_ask}
            else:
                book = (
                    books_cache[token_key]
                    if token_key in books_cache
                    else await self.order_mgr.get_book_summary(token_id)
                )
            bid, ask = all_quotes[token_key]
            bid, ask = self.quote_engine.clamp_to_book(
                bid, ask, book["best_bid"], book["best_ask"],
                tick_size=self.market.tick_size,
            )
            if taker_lagging_key == token_key and bid is not None and book["best_ask"] is not None:
                # Force taker cross on lagging side to accelerate balancing fills.
                bid.price = _round_to_tick(float(book["best_ask"]))
            # Re-apply max inventory cap after clamping (safety net)
            max_sh = self.config.max_inventory_shares
            if bid is not None and bid.size > max_sh:
                bid.size = round(max_sh, 2)
            if ask is not None and ask.size > max_sh:
                ask.size = round(max_sh, 2)
            all_quotes[token_key] = (bid, ask)
        _t_quotes = time.perf_counter()

        # Abort if emergency shutdown was triggered during this tick
        if self._emergency_flag:
            return

        # 7. Place or update orders
        # On Polymarket: SELL requires token inventory. Use BUY-only strategy:
        #   BUY UP @ bid_up  +  BUY DN @ bid_dn
        # BUY DN @ P implicitly provides ask-side for UP at (1 - P).
        # When both fill: hold UP+DN = $1, paid bid_up+bid_dn < $1 → profit.
        for token_key in ("up", "dn"):
            new_bid, new_ask = all_quotes[token_key]
            cur_bid, cur_ask = self._current_quotes.get(token_key, (None, None))
            use_taker_bid = bool(new_bid is not None and any(q is new_bid for q in self._taker_quotes))

            need_bid = self.quote_engine.should_requote(cur_bid, new_bid)
            if use_taker_bid and new_bid is not None:
                # Force refresh while in taker mode so lagging-side BUY keeps crossing.
                need_bid = True
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
            ask_size = new_ask.size if new_ask else 0.0
            ask_price = new_ask.price if new_ask else 0.0
            current_ask_size = cur_ask.size if (cur_ask and cur_ask.order_id) else 0.0
            material_ask_size_change = (
                new_ask is not None
                and cur_ask is not None
                and cur_ask.order_id is not None
                and abs(ask_size - current_ask_size) > 2.0
            )
            need_ask = ((self.quote_engine.should_requote(cur_ask, new_ask)
                         or material_ask_size_change)
                        and token_bal > 0 and new_ask is not None)

            oversized_live_ask = bool(
                cur_ask and cur_ask.order_id and cur_ask.size > (token_bal + 0.5)
            )
            if oversized_live_ask:
                log.warning(
                    "Cancelling oversized %s ask: live_size=%.2f inventory=%.2f",
                    token_key.upper(),
                    cur_ask.size if cur_ask else 0.0,
                    token_bal,
                )
                if token_bal > 0 and new_ask is not None:
                    need_ask = True

            if need_ask and (ask_size * ask_price) < self.config.min_order_size_usd:
                need_ask = False
            # Also cancel stale ask if we no longer have inventory
            stale_ask = (cur_ask and cur_ask.order_id
                         and (token_bal <= 0 or oversized_live_ask)
                         and not need_ask)

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

                if use_taker_bid and need_bid:
                    # cancel_replace cannot set per-order post_only, so handle taker bid manually.
                    if old_ids:
                        cancel_results = await asyncio.gather(
                            *(self.order_mgr.cancel_order(oid) for oid in old_ids if oid)
                        )
                        if not all(cancel_results):
                            failed = sum(1 for r in cancel_results if not r)
                            log.warning(
                                "taker cancel/replace: %d/%d cancels failed, skipping new placements",
                                failed,
                                len(cancel_results),
                            )
                            continue
                    if need_bid and new_bid is not None:
                        await self.order_mgr.place_order(
                            new_bid,
                            post_only=False,
                            fallback_taker=True,
                        )
                    if need_ask and new_ask is not None:
                        await self.order_mgr.place_order(new_ask)
                    updated_bid = new_bid if need_bid else (None if stale_bid else cur_bid)
                    updated_ask = new_ask if need_ask else (None if stale_ask else cur_ask)
                    self._current_quotes[token_key] = (updated_bid, updated_ask)
                else:
                    new_ids = await self.order_mgr.cancel_replace(old_ids, new_quotes)

                    # Only update tracking when cancel_replace actually placed orders
                    if new_ids is not None:
                        updated_bid = new_bid if need_bid else (None if stale_bid else cur_bid)
                        updated_ask = new_ask if need_ask else (None if stale_ask else cur_ask)
                        self._current_quotes[token_key] = (updated_bid, updated_ask)
                    # If cancel_replace failed (returned []), leave _current_quotes unchanged
                    # so next tick retries the cancel+replace

        _t_orders = time.perf_counter()

        # 8. Track spread for stats
        up_bid, up_ask = self._current_quotes.get("up", (None, None))
        if up_bid and up_ask and up_ask.price > 0:
            spread_bps = (up_ask.price - up_bid.price) / up_ask.price * 10000
            self._spread_samples.append(spread_bps)
            if len(self._spread_samples) > 1000:
                self._spread_samples = self._spread_samples[-500:]

        # 8b. Refresh PM balances AFTER order placement for accurate PnL.
        # USDC changes when orders are placed/filled; token balances change on fills.
        is_live = not hasattr(self.order_mgr.client, "_orders")
        if is_live:
            try:
                (real_up, real_dn), usdc_bal = await asyncio.gather(
                    self.order_mgr.get_all_token_balances(
                        self.market.up_token_id, self.market.dn_token_id,
                    ),
                    self.order_mgr.get_usdc_balance(),
                )
                if usdc_bal is not None:
                    self._cached_usdc_balance = usdc_bal
                if real_up is not None:
                    self._cached_pm_up_shares = real_up
                if real_dn is not None:
                    self._cached_pm_dn_shares = real_dn
            except Exception as e:
                log.warning("Post-order balance refresh failed: %s", e)

        # 9. Latency metrics
        total_ms = (_t_orders - _t0) * 1000
        self._last_tick_ms = total_ms
        self._last_book_ms = (_t_quotes - _t_reconcile) * 1000  # fv + risk + quotes + book clamp
        self._last_order_ms = (_t_orders - _t_quotes) * 1000
        self._last_fills_ms = (_t_fills - _t0) * 1000
        self._last_reconcile_ms = (_t_reconcile - _t_fills) * 1000
        self._last_fv_ms = (_t_fv - _t_reconcile) * 1000
        self._last_quotes_ms = (_t_quotes - _t_fv) * 1000
        self._last_orders_ms = (_t_orders - _t_quotes) * 1000
        self._tick_ms_samples.append(total_ms)
        if len(self._tick_ms_samples) > 100:
            self._tick_ms_samples = self._tick_ms_samples[-50:]
        self._avg_tick_ms = sum(self._tick_ms_samples) / len(self._tick_ms_samples)

        if self._tick_count % 10 == 0:
            log.info(
                "TICK latency: fills=%.0fms reconcile=%.0fms fv=%.0fms "
                "quotes+book=%.0fms orders=%.0fms total=%.0fms",
                (_t_fills - _t0) * 1000,
                (_t_reconcile - _t_fills) * 1000,
                (_t_fv - _t_reconcile) * 1000,
                (_t_quotes - _t_fv) * 1000,
                (_t_orders - _t_quotes) * 1000,
                total_ms,
            )

    def _compute_fv(self) -> tuple[float, float]:
        """Compute current fair values from feed state. Returns (fv_up, fv_dn)."""
        st = self.feed_state
        mid = st.mid if st.mid and st.mid > 0 else 0
        if mid <= 0 or not self.market:
            log.warning(
                "_compute_fv fallback: mid=%s, market=%s — returning 0.5/0.5",
                mid, bool(self.market)
            )
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
        if up_bal is None or dn_bal is None:
            log.error("Liquidation: failed to fetch token balances; retrying next chunk")
            return
        merge_amount = min(up_bal, dn_bal)

        if merge_amount >= 1.0 and self.market.condition_id and not self._merge_failed_this_cycle:
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
                self._merge_failed_this_cycle = True
                log.warning("Merge failed (will not retry this cycle): %s",
                            result.get("error", "unknown"))

        time_left = self.market.time_remaining
        cfg = self.config
        taker_threshold = cfg.liq_taker_threshold_sec
        use_taker = time_left <= taker_threshold
        # Emergency drawdown check during liquidation
        # Use real PM balances (not internal inventory) and starting portfolio (with tokens)
        if self._starting_portfolio_pm > 0 and self._cached_usdc_balance > 0:
            _pm_up = self.feed_state.pm_up if hasattr(self.feed_state, "pm_up") else 0.5
            _pm_dn = self.feed_state.pm_dn if hasattr(self.feed_state, "pm_dn") else 0.5
            _pos_val = self._cached_pm_up_shares * _pm_up + self._cached_pm_dn_shares * _pm_dn
            _liq_pnl = (self._cached_usdc_balance + _pos_val) - self._starting_portfolio_pm
            if _liq_pnl < -2 * self.config.max_drawdown_usd:
                self._catastrophic_count += 1
                log.warning(
                    "CATASTROPHIC reading %d/%d: sPnL=$%.2f (USDC=$%.2f, pos=$%.2f, start=$%.2f)",
                    self._catastrophic_count, self._catastrophic_threshold,
                    _liq_pnl, self._cached_usdc_balance, _pos_val,
                    self._starting_portfolio_pm,
                )
                if self._catastrophic_count >= self._catastrophic_threshold:
                    # Fresh recheck: re-fetch balances before final decision
                    try:
                        fresh_usdc = await self.order_mgr.get_usdc_balance()
                        (fresh_up, fresh_dn) = await self.order_mgr.get_all_token_balances(
                            self.market.up_token_id, self.market.dn_token_id)
                        if fresh_usdc is not None:
                            self._cached_usdc_balance = fresh_usdc
                        if fresh_up is not None:
                            self._cached_pm_up_shares = fresh_up
                        if fresh_dn is not None:
                            self._cached_pm_dn_shares = fresh_dn
                        _fresh_pos = (self._cached_pm_up_shares * _pm_up +
                                      self._cached_pm_dn_shares * _pm_dn)
                        _fresh_pnl = (self._cached_usdc_balance + _fresh_pos) - self._starting_portfolio_pm
                        log.warning("CATASTROPHIC fresh recheck: sPnL=$%.2f (was $%.2f)", _fresh_pnl, _liq_pnl)
                        if _fresh_pnl < -2 * self.config.max_drawdown_usd:
                            await self._emergency_shutdown(
                                f"CATASTROPHIC LOSS confirmed ({self._catastrophic_threshold} readings + fresh recheck): "
                                f"sPnL=${_fresh_pnl:.2f}"
                            )
                            return
                        else:
                            log.warning("CATASTROPHIC averted after fresh recheck: sPnL=$%.2f", _fresh_pnl)
                            self._catastrophic_count = 0
                    except Exception as e:
                        log.error("CATASTROPHIC recheck failed: %s — shutting down as precaution", e)
                        await self._emergency_shutdown(
                            f"CATASTROPHIC LOSS (recheck failed): sPnL=${_liq_pnl:.2f}"
                        )
                        return
                else:
                    return  # Wait for more readings before deciding
            else:
                self._catastrophic_count = 0  # Reset counter on non-catastrophic reading
            if _liq_pnl < -self.config.max_drawdown_usd and not use_taker:
                log.warning("Max drawdown exceeded during liquidation: sPnL=$%.2f, forcing taker mode", _liq_pnl)
                use_taker = True

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

        # ── Pre-flight dust check: if both sides < PM min, try merge dust then skip ──
        pm_min_pf = self.market.min_order_size if self.market else 5.0
        if up_bal < pm_min_pf and dn_bal < pm_min_pf and (up_bal > 0.1 or dn_bal > 0.1):
            # Try merging dust pairs first (if both > 0)
            dust_merge = min(up_bal, dn_bal)
            if dust_merge >= 0.5 and self.market.condition_id and not self._merge_failed_this_cycle:
                try:
                    r = await self.order_mgr.merge_positions(
                        self.market.condition_id, dust_merge, self._private_key)
                    if r.get("success"):
                        log.info("Dust merge: %.2f pairs → $%.2f USDC", dust_merge, dust_merge)
                        self.inventory.up_shares = max(0.0, self.inventory.up_shares - dust_merge)
                        self.inventory.dn_shares = max(0.0, self.inventory.dn_shares - dust_merge)
                        self.inventory.usdc += dust_merge
                    else:
                        self._merge_failed_this_cycle = True
                except Exception:
                    self._merge_failed_this_cycle = True

            # Re-check after potential merge
            up_rem_pf = await self.order_mgr.get_token_balance(self.market.up_token_id)
            dn_rem_pf = await self.order_mgr.get_token_balance(self.market.dn_token_id)
            if up_rem_pf is None or dn_rem_pf is None:
                log.error("Liquidation: failed to refresh token balances after merge; retrying next chunk")
                return
            if up_rem_pf < pm_min_pf and dn_rem_pf < pm_min_pf:
                fv_up_pf, fv_dn_pf = self._compute_fv()
                dust_val = up_rem_pf * fv_up_pf + dn_rem_pf * fv_dn_pf
                self._throttled_warn(
                    "liq_dust_preflight",
                    f"Liquidation dust: UP={up_rem_pf:.2f} DN={dn_rem_pf:.2f} "
                    f"(all < PM min {pm_min_pf}). ~${dust_val:.2f} locked. Awaiting expiry.",
                    cooldown=120.0,
                )
                return

        has_real_balance = False
        placed_any = False
        balance_fetch_failed = False
        completion_threshold = 0.5

        for label, token_id in [
            ("UP", self.market.up_token_id),
            ("DN", self.market.dn_token_id),
        ]:
            real_balance = await self.order_mgr.get_token_balance(token_id)
            if real_balance is None:
                log.error("%s balance fetch failed during liquidation; retrying next chunk", label)
                balance_fetch_failed = True
                continue
            if real_balance <= completion_threshold:
                log.info(f"No real balance for {label}")
                continue

            has_real_balance = True
            is_up = token_id == self.market.up_token_id
            fv = fv_up if is_up else fv_dn
            book = await self.order_mgr.get_book_summary(token_id)
            best_bid = book["best_bid"]

            # Determine price floor from lock or cost basis
            floor = 0.01
            cost = self.inventory.up_cost if is_up else self.inventory.dn_cost
            cost_basis = cost.avg_entry_price if cost else 0.0
            hard_min_floor = max(0.05, cost_basis * 0.5) if cost_basis > 0 else 0.05
            if cfg.liq_price_floor_enabled and self._liq_lock:
                floor = (self._liq_lock.min_sell_price_up if is_up
                         else self._liq_lock.min_sell_price_dn)
            elif cfg.liq_price_floor_enabled:
                floor = max(0.01, cost.avg_entry_price + cfg.liq_price_floor_margin)

            # Adaptive floor decay with hard floor guardrail
            base_floor = floor
            ref = self._closing_start_time_left if self._closing_start_time_left > 0 else 30.0
            decay_ratio = max(0.0, min(1.0, time_left / ref))
            floor = max(hard_min_floor, base_floor * decay_ratio)
            if floor != base_floor:
                log.info(f"{label}: adaptive floor {base_floor:.2f} → {floor:.2f} "
                         f"(decay={decay_ratio:.2f}, {time_left:.0f}s left)")
            if floor <= hard_min_floor:
                self._throttled_warn(
                    f"liq_floor_hard_min_{label}",
                    f"{label}: liquidation floor hit hard minimum {hard_min_floor:.2f} "
                    f"(base={base_floor:.2f}, decay={decay_ratio:.2f}, cost_basis={cost_basis:.2f})",
                    cooldown=30.0,
                )

            # Adjust floor for taker fee when in taker mode
            if use_taker:
                floor = floor + self.config.taker_fee_rate * floor

            if use_taker:
                # ── Phase 2: Taker ──
                if not best_bid or best_bid <= 0:
                    if time_left < 5:
                        log.critical(
                            f"{label}: No bid available for liquidation, holding to resolution "
                            f"({time_left:.0f}s left)"
                        )
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
                        # < 5 seconds — force sell but enforce absolute minimum
                        absolute_min = max(0.03, floor * 0.8)
                        if best_bid < absolute_min:
                            log.critical(
                                f"{label} HOLD: best_bid={best_bid:.2f} < absolute_min={absolute_min:.2f}, "
                                f"holding to resolution ({time_left:.0f}s left, {real_balance:.1f} shares)")
                            continue
                        log.warning(
                            f"{label} FORCE SELL: best_bid={best_bid:.2f} >= absolute_min={absolute_min:.2f}, "
                            f"({time_left:.0f}s left, {real_balance:.1f} shares)")
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
                is_last_chunk = (self._liq_chunk_index >= cfg.liq_gradual_chunks - 1)
                if is_last_chunk:
                    chunk_size = round(real_balance, 2)
                else:
                    chunk_size = round(real_balance / remaining_chunks, 2)
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

        if balance_fetch_failed and not placed_any and not self._liquidation_order_ids:
            log.error("Liquidation deferred: token balance fetch failed, retrying next chunk")
            return

        if not has_real_balance:
            real_up_chk, real_dn_chk = await self.order_mgr.get_all_token_balances(
                self.market.up_token_id,
                self.market.dn_token_id,
            )
            if real_up_chk is None or real_dn_chk is None:
                log.warning("Liquidation completion check deferred: failed to fetch PM balances")
                return

            internal_up = self.inventory.up_shares
            internal_dn = self.inventory.dn_shares
            internal_clear = internal_up <= completion_threshold and internal_dn <= completion_threshold
            real_clear = real_up_chk <= completion_threshold and real_dn_chk <= completion_threshold

            if internal_clear and not real_clear:
                log.warning(
                    "Internal inventory shows 0 but PM has real balance: "
                    "internal UP=%.2f DN=%.2f, PM UP=%.2f DN=%.2f",
                    internal_up,
                    internal_dn,
                    real_up_chk,
                    real_dn_chk,
                )
                return

            if internal_clear and real_clear:
                log.info("Liquidation complete — no inventory remaining")
                self._is_closing = False
                return

        # Check if all remaining balances are "dust" — below PM minimum, can't sell
        if has_real_balance and not placed_any and not self._liquidation_order_ids:
            pm_min = self.market.min_order_size if self.market else 5.0
            up_rem = await self.order_mgr.get_token_balance(self.market.up_token_id)
            dn_rem = await self.order_mgr.get_token_balance(self.market.dn_token_id)
            if up_rem is None or dn_rem is None:
                log.error("Liquidation: failed to fetch remaining balances for dust check; retrying next chunk")
                return
            all_dust = (up_rem < pm_min and dn_rem < pm_min)
            if all_dust:
                fv_up_d, fv_dn_d = self._compute_fv()
                dust_value = up_rem * fv_up_d + dn_rem * fv_dn_d
                self._throttled_warn(
                    "liq_dust",
                    f"Liquidation dust: UP={up_rem:.2f} DN={dn_rem:.2f} "
                    f"(all < PM min {pm_min}). ~${dust_value:.2f} locked. "
                    f"Waiting for expiry.",
                    cooldown=120.0,
                )
                # Stay in _is_closing=True so bot doesn't resume quoting,
                # but stop spamming sell attempts — just wait for window end

    async def on_window_transition(self, new_market: MarketInfo) -> None:
        """Handle window transition — cancel all, switch tokens, resume."""
        log.info(f"Window transition: {new_market.coin} {new_market.timeframe}")

        # Cancel all existing orders
        await self.order_mgr.cancel_all()
        self._current_quotes = {"up": (None, None), "dn": (None, None)}

        # Reset cost basis, risk state, and liquidation state for new window
        self.inventory.up_cost.reset()
        self.inventory.dn_cost.reset()
        self.risk_mgr.reset()
        self._liq_lock = None
        self._liq_chunk_index = 0
        self._liq_last_chunk_time = 0.0
        self._is_closing = False
        self._merge_failed_this_cycle = False
        self._reconcile_prev_pm = None
        self._reconcile_stable_count = 0
        self._liquidation_order_ids.clear()
        self._one_sided_counter = 0
        self._imbalance_start_ts = 0.0
        self._imbalance_adjustments = {
            "leading_spread_mult": 1.0,
            "lagging_spread_mult": 1.0,
            "skew_mult": 1.0,
            "tier": 0,
            "suppress_leading_buy": False,
            "force_taker_lagging": False,
        }
        self._taker_quotes = []

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

        # Real session PnL based on PM balances (not internal inventory)
        # USDC is refreshed every tick after order placement, so no need to add order collateral.
        _pm_up_price = st.pm_up if hasattr(st, "pm_up") and st.pm_up else fv_up
        _pm_dn_price = st.pm_dn if hasattr(st, "pm_dn") and st.pm_dn else fv_dn
        _position_value = (self._cached_pm_up_shares * _pm_up_price +
                           self._cached_pm_dn_shares * _pm_dn_price)
        _current_portfolio = self._cached_usdc_balance + _position_value
        _session_pnl = _current_portfolio - self._starting_portfolio_pm if self._starting_portfolio_pm > 0 else 0.0

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

            # Paired filling state
            "paired_filling": {
                "imbalance_shares": round(abs(self.inventory.up_shares - self.inventory.dn_shares), 2),
                "imbalance_duration_sec": round(
                    max(0.0, time.time() - self._imbalance_start_ts), 2
                ) if self._imbalance_start_ts > 0 else 0.0,
                **self._imbalance_adjustments,
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

            # PnL & Risk (override fill-based with PM-balance-based)
            **risk_stats,
            "total_pnl": round(_session_pnl, 4),
            "unrealized_pnl": round(_position_value, 4),
            "realized_pnl": round(_session_pnl - _position_value, 4),
            "avg_spread_bps": round(avg_spread, 1),
            "session_pnl": round(_session_pnl, 4),
            "starting_usdc_pm": round(self._starting_usdc_pm, 2),
            "starting_portfolio_pm": round(self._starting_portfolio_pm, 2),
            "portfolio_value": round(self._cached_usdc_balance + _position_value, 2),
            "is_paused": self._paused,
            "pause_reason": self._pause_reason,
            "is_closing": self._is_closing,
            "is_running": self._running,

            # Orders
            **order_stats,

            # Fills
            "recent_fills": fills_data,

            # Latency
            "latency": {
                "last_tick_ms": round(self._last_tick_ms, 1),
                "avg_tick_ms": round(self._avg_tick_ms, 1),
                "fills_ms": round(self._last_fills_ms, 1),
                "reconcile_ms": round(self._last_reconcile_ms, 1),
                "fv_ms": round(self._last_fv_ms, 1),
                "quotes_ms": round(self._last_quotes_ms, 1),
                "orders_ms": round(self._last_orders_ms, 1),
            },

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
