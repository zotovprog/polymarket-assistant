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
import math
import time
from dataclasses import asdict, dataclass
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
from .markout_tca import MarkoutTracker
from .pnl_decomposition import PnLDecomposition
from .event_requote import EventRequoter
from .runtime_metrics import runtime_metrics

log = logging.getLogger("mm.engine")
BINANCE_FEED_STALE_SEC = 15.0
BINANCE_FEED_STARTUP_GRACE_SEC = 20.0


class StartBlockedError(RuntimeError):
    """Raised when session safety checks block MM startup."""


class SREMetrics:
    """Track operational health metrics for the MM bot."""

    def __init__(self, max_samples: int = 500):
        self._max = max_samples
        self._tick_durations_ms: list[float] = []
        self._order_rtts_ms: list[float] = []
        self._fill_check_ms: list[float] = []
        self._api_success: int = 0
        self._api_failure: int = 0
        self._tick_timeouts: int = 0
        self._ws_reconnects: int = 0
        self._heartbeat_errors: int = 0
        self._last_reset: float = 0.0

    def record_tick(self, duration_ms: float) -> None:
        self._tick_durations_ms.append(duration_ms)
        if len(self._tick_durations_ms) > self._max:
            self._tick_durations_ms = self._tick_durations_ms[-self._max:]

    def record_order_rtt(self, rtt_ms: float) -> None:
        self._order_rtts_ms.append(rtt_ms)
        if len(self._order_rtts_ms) > self._max:
            self._order_rtts_ms = self._order_rtts_ms[-self._max:]

    def record_fill_check(self, duration_ms: float) -> None:
        self._fill_check_ms.append(duration_ms)
        if len(self._fill_check_ms) > self._max:
            self._fill_check_ms = self._fill_check_ms[-self._max:]

    def record_api_call(self, success: bool) -> None:
        if success:
            self._api_success += 1
        else:
            self._api_failure += 1

    def record_tick_timeout(self) -> None:
        self._tick_timeouts += 1

    def record_ws_reconnect(self) -> None:
        self._ws_reconnects += 1

    def record_heartbeat_error(self) -> None:
        self._heartbeat_errors += 1

    @staticmethod
    def _percentile(data: list[float], pct: float) -> float:
        if not data:
            return 0.0
        sorted_data = sorted(data)
        idx = int(len(sorted_data) * pct / 100.0)
        idx = min(idx, len(sorted_data) - 1)
        return sorted_data[idx]

    @property
    def stats(self) -> dict:
        total_api = self._api_success + self._api_failure
        return {
            "tick_p50_ms": round(self._percentile(self._tick_durations_ms, 50), 1),
            "tick_p95_ms": round(self._percentile(self._tick_durations_ms, 95), 1),
            "tick_p99_ms": round(self._percentile(self._tick_durations_ms, 99), 1),
            "order_rtt_p50_ms": round(self._percentile(self._order_rtts_ms, 50), 1),
            "order_rtt_p95_ms": round(self._percentile(self._order_rtts_ms, 95), 1),
            "fill_check_p50_ms": round(self._percentile(self._fill_check_ms, 50), 1),
            "fill_check_p95_ms": round(self._percentile(self._fill_check_ms, 95), 1),
            "api_success_rate": round(self._api_success / max(total_api, 1) * 100, 1),
            "api_total_calls": total_api,
            "tick_timeouts_total": self._tick_timeouts,
            "ws_reconnects": self._ws_reconnects,
            "heartbeat_errors": self._heartbeat_errors,
            "tick_samples": len(self._tick_durations_ms),
        }

    def reset(self) -> None:
        self._tick_durations_ms.clear()
        self._order_rtts_ms.clear()
        self._fill_check_ms.clear()
        self._api_success = 0
        self._api_failure = 0
        self._tick_timeouts = 0
        self._ws_reconnects = 0
        self._heartbeat_errors = 0
        self._last_reset = time.time()


@dataclass
class SettlementLagState:
    token_id: str
    pending_delta_shares: float
    grace_until: float
    last_fill_ts: float
    last_fill_side: str
    last_fill_size: float
    last_internal_shares: float
    last_pm_shares: float
    source: str


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
        self.fair_value = FairValueEngine(
            vol_floor=self.config.fv_vol_floor,
            signal_weight=self.config.fv_signal_weight,
        )
        self.quote_engine = QuoteEngine(config)
        self.order_mgr = OrderManager(clob_client, config)
        self.risk_mgr = RiskManager(config)
        self.heartbeat = HeartbeatManager(
            clob_client,
            config.heartbeat_interval_sec,
            failure_threshold=max(1, int(getattr(config, "heartbeat_failures_before_shutdown", 3))),
            on_failure=self._schedule_heartbeat_failure,
            should_send=lambda: bool(self.order_mgr.active_order_ids),
        )
        self.rebate = RebateTracker(clob_client)
        self.quality_analyzer = MarketQualityAnalyzer(config)
        self.markout_tracker = MarkoutTracker(self._get_token_mid)
        self.pnl_decomp = PnLDecomposition()
        self.event_requoter = EventRequoter(
            pm_mid_threshold_bps=self.config.event_pm_mid_threshold_bps,
            binance_threshold_bps=self.config.event_binance_threshold_bps,
            fallback_interval_sec=self.config.event_fallback_interval_sec,
        )

        # State
        self.inventory = Inventory()
        # Merge-first tracking
        self._merge_check_interval: int = 10  # Check for merge opportunity every N ticks
        self._merge_check_counter: int = 0
        self.market: Optional[MarketInfo] = None
        self._running = False
        self._emergency_flag = False
        self._emergency_stopped = False
        self._paused = False
        self._pause_reason = ""
        self._task: Optional[asyncio.Task] = None
        self._heartbeat_failure_task: Optional[asyncio.Task] = None
        self._order_ops_lock: asyncio.Lock = asyncio.Lock()
        self._started_at: float = 0.0
        self._is_closing = False
        self._liquidation_attempted = False
        self._liquidation_order_ids: set[str] = set()
        self._cached_usdc_balance: float = 0.0
        self._cached_usdc_available_balance: float | None = None
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
        self._post_fill_entry_guard_until: float = 0.0
        self._post_fill_entry_guard_anchor: MarketQuality | None = None
        self._post_fill_entry_guard_active: bool = False
        self._post_fill_entry_guard_reason: str = ""
        self._post_fill_entry_guard_trigger_count: int = 0
        self._liq_lock: LiquidationLock | None = None
        self._liq_chunk_index: int = 0
        self._liq_last_chunk_time: float = 0.0
        self._liq_last_attempt_time: float = 0.0
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
        self._reconcile_guard_until: float = 0.0
        self._critical_drift_pause_active: bool = False
        self._critical_drift_recovery_streak: int = 0
        self._critical_drift_recovery_required: int = 2
        self._settlement_lag: dict[str, SettlementLagState] = {}
        self._settlement_guard_tokens: set[str] = set()
        self._settlement_lag_suppressed_total: int = 0
        self._settlement_lag_escalated_total: int = 0
        self._warn_cooldowns: dict[str, float] = {}
        self._private_key: str = ""
        self.sre_metrics = SREMetrics()
        self._last_trade_backfill_ts: float = 0.0
        self._trade_backfill_interval_sec: float = 60.0
        self._toxicity_spread_mult: float = 1.0
        self._toxicity_mode: str = "normal"
        self._toxic_divergence_count: int = 0
        self._last_requote_events: list[dict[str, Any]] = []

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

    @staticmethod
    def _safe_non_negative(value: Any) -> float:
        """Best-effort float conversion with non-negative, finite clamp."""
        try:
            val = float(value)
        except (TypeError, ValueError):
            return 0.0
        if not math.isfinite(val):
            return 0.0
        return max(0.0, val)

    def _is_live_mode(self) -> bool:
        """Heuristic: mock paper client exposes in-memory `_orders`."""
        return not hasattr(self.order_mgr.client, "_orders")

    @staticmethod
    def _should_block_bid_for_extreme_fv(fv: float, min_fv: float) -> bool:
        """Whether new BUY quotes should be blocked in FV tail zones."""
        if min_fv <= 0:
            return False
        upper = max(0.0, min(1.0, 1.0 - float(min_fv)))
        value = float(fv)
        return value < float(min_fv) or value > upper

    def _arm_reconcile_guard(self, up_diff: float, dn_diff: float, source: str) -> None:
        """Briefly pause merge/liquidation after large inventory reconcile jumps."""
        max_diff = max(float(up_diff), float(dn_diff))
        total_diff = float(up_diff) + float(dn_diff)
        if max_diff < 3.0 and total_diff < 6.0:
            return

        # Short cooldown to let PM balances converge before acting on reconciled inventory.
        guard_sec = min(20.0, max(4.0, 0.25 * total_diff))
        until = time.time() + guard_sec
        if until > self._reconcile_guard_until:
            self._reconcile_guard_until = until
        log.warning(
            "Reconcile drift guard armed for %.1fs (%s): up_diff=%.2f dn_diff=%.2f",
            guard_sec,
            source,
            up_diff,
            dn_diff,
        )

    def _reconcile_guard_active(self) -> bool:
        """Whether temporary reconcile cooldown is active for merge/liquidation."""
        if self._reconcile_guard_until <= 0:
            return False
        if not self._running:
            return False
        if not self.market:
            return False

        now = time.time()
        if now >= self._reconcile_guard_until:
            self._reconcile_guard_until = 0.0
            return False

        # Never block close-out very close to expiry.
        time_left = self.market.time_remaining
        if time_left <= max(5.0, float(self.config.liq_taker_threshold_sec)):
            self._reconcile_guard_until = 0.0
            return False
        return True

    def _critical_drift_threshold(self) -> float:
        return max(
            0.5,
            float(getattr(self.config, "critical_reconcile_drift_shares", 1.5) or 1.5),
        )

    def _settlement_clear_threshold(self) -> float:
        return max(0.25, 0.25 * self._critical_drift_threshold())

    def _token_key_for_id(self, token_id: str) -> str | None:
        if not self.market:
            return None
        if token_id == self.market.up_token_id:
            return "up"
        if token_id == self.market.dn_token_id:
            return "dn"
        return None

    def _internal_shares_for_token(self, token_id: str) -> float:
        token_key = self._token_key_for_id(token_id)
        if token_key == "up":
            return float(self.inventory.up_shares)
        if token_key == "dn":
            return float(self.inventory.dn_shares)
        return 0.0

    def _pm_shares_for_token(self, token_id: str, *, real_up: float, real_dn: float) -> float:
        token_key = self._token_key_for_id(token_id)
        if token_key == "up":
            return float(real_up)
        if token_key == "dn":
            return float(real_dn)
        return 0.0

    def _balance_reference_snapshot(self) -> dict[str, float]:
        if not self.market:
            return {}
        return {
            self.market.up_token_id: float(self.inventory.up_shares),
            self.market.dn_token_id: float(self.inventory.dn_shares),
        }

    def _clone_quality(self, quality: MarketQuality | None) -> MarketQuality | None:
        if quality is None:
            return None
        return MarketQuality(**asdict(quality))

    def _has_material_inventory(self, *, threshold: float = 0.5) -> bool:
        return (
            float(self.inventory.up_shares) > threshold
            or float(self.inventory.dn_shares) > threshold
        )

    def _post_fill_entry_guard_window_active(self) -> bool:
        return time.time() < self._post_fill_entry_guard_until

    def _should_force_post_fill_quality_check(self) -> bool:
        return (
            self._post_fill_entry_guard_window_active()
            and abs(float(self.inventory.up_shares) - float(self.inventory.dn_shares)) > 2.0
        )

    def _clear_post_fill_entry_guard(self, *, reset_anchor: bool) -> None:
        self._post_fill_entry_guard_active = False
        self._post_fill_entry_guard_reason = ""
        if reset_anchor:
            self._post_fill_entry_guard_until = 0.0
            self._post_fill_entry_guard_anchor = None

    def _arm_post_fill_entry_guard(self, fill: Fill) -> None:
        if not self._is_live_mode() or fill.side != "BUY":
            return

        guard_sec = max(
            5.0,
            float(getattr(self.config, "post_fill_entry_guard_sec", 45.0) or 45.0),
        )
        self._post_fill_entry_guard_until = time.time() + guard_sec
        self._post_fill_entry_guard_anchor = self._clone_quality(self._last_quality)
        self._post_fill_entry_guard_active = False
        self._post_fill_entry_guard_reason = ""

        token_key = self._token_key_for_id(fill.token_id) or fill.token_id
        anchor = self._post_fill_entry_guard_anchor
        log.info(
            "Post-fill entry guard armed: token=%s size=%.2f window=%.1fs anchor_score=%s anchor_spread=%s",
            token_key.upper(),
            float(fill.size),
            guard_sec,
            f"{anchor.overall_score:.3f}" if anchor is not None else "n/a",
            f"{anchor.spread_bps:.0f}bps" if anchor is not None else "n/a",
        )

    def _update_post_fill_entry_guard(self) -> None:
        if not self._has_material_inventory():
            self._clear_post_fill_entry_guard(reset_anchor=True)
            return

        if not self._post_fill_entry_guard_window_active():
            self._clear_post_fill_entry_guard(reset_anchor=True)
            return

        if self._last_quality is None:
            return

        if self._post_fill_entry_guard_anchor is None:
            self._post_fill_entry_guard_anchor = self._clone_quality(self._last_quality)
            return

        anchor = self._post_fill_entry_guard_anchor
        current = self._last_quality
        score_drop = float(anchor.overall_score) - float(current.overall_score)
        spread_widen = float(current.spread_bps) - float(anchor.spread_bps)

        degraded = False
        reason = ""
        if not bool(current.tradeable):
            degraded = True
            reason = current.reason or "market no longer tradeable"
        elif score_drop >= float(getattr(self.config, "post_fill_entry_score_drop", 0.20) or 0.20):
            degraded = True
            reason = (
                f"quality score drop {score_drop:.2f} >= "
                f"{float(getattr(self.config, 'post_fill_entry_score_drop', 0.20) or 0.20):.2f}"
            )
        elif spread_widen >= float(
            getattr(self.config, "post_fill_entry_spread_widen_bps", 250.0) or 250.0
        ):
            degraded = True
            reason = (
                f"spread widened {spread_widen:.0f}bps >= "
                f"{float(getattr(self.config, 'post_fill_entry_spread_widen_bps', 250.0) or 250.0):.0f}bps"
            )

        if degraded:
            if (not self._post_fill_entry_guard_active) or reason != self._post_fill_entry_guard_reason:
                self._post_fill_entry_guard_active = True
                self._post_fill_entry_guard_reason = reason
                self._post_fill_entry_guard_trigger_count += 1
                self._throttled_warn(
                    "post_fill_entry_guard_activated",
                    (
                        "Post-fill entry guard activated: %s "
                        "(anchor score=%.3f spread=%.0fbps -> current score=%.3f spread=%.0fbps)"
                    )
                    % (
                        reason,
                        anchor.overall_score,
                        anchor.spread_bps,
                        current.overall_score,
                        current.spread_bps,
                    ),
                    cooldown=2.0,
                )
            return

        if self._post_fill_entry_guard_active:
            log.info("Post-fill entry guard cleared: market quality recovered before guard expiry")
        self._post_fill_entry_guard_active = False
        self._post_fill_entry_guard_reason = ""

    def _apply_post_fill_entry_buy_block(
        self,
        all_quotes: dict[str, tuple[Quote | None, Quote | None]],
    ) -> None:
        if not self._post_fill_entry_guard_active:
            return

        blocked_any = False
        for token_key in ("up", "dn"):
            bid, ask = all_quotes[token_key]
            if bid is not None:
                blocked_any = True
                all_quotes[token_key] = (None, ask)

        if blocked_any:
            self._throttled_warn(
                "post_fill_entry_guard_block",
                (
                    "Post-fill entry guard: blocking new BUYs for %.1fs (%s)"
                    % (
                        max(0.0, self._post_fill_entry_guard_until - time.time()),
                        self._post_fill_entry_guard_reason or "quality degraded after fill",
                    )
                ),
                cooldown=2.0,
            )

    def _drop_settlement_lag(self, token_id: str) -> None:
        self._settlement_lag.pop(token_id, None)
        self._settlement_guard_tokens.discard(token_id)

    def _record_live_fill_settlement(self, fill: Fill) -> None:
        if not self._is_live_mode() or not self.market:
            return
        token_key = self._token_key_for_id(fill.token_id)
        if token_key is None:
            return

        fill_size = max(0.0, float(fill.size))
        if fill_size <= 0:
            return

        delta = fill_size if fill.side == "BUY" else -fill_size
        now = time.time()
        grace_sec = max(1.0, float(getattr(self.config, "fill_settlement_grace_sec", 6.0) or 6.0))
        current = self._settlement_lag.get(fill.token_id)
        if current is not None and current.grace_until > now:
            pending_delta = current.pending_delta_shares + delta
        else:
            pending_delta = delta

        if abs(pending_delta) <= 1e-9:
            self._drop_settlement_lag(fill.token_id)
            return

        self._settlement_lag[fill.token_id] = SettlementLagState(
            token_id=fill.token_id,
            pending_delta_shares=pending_delta,
            grace_until=now + grace_sec,
            last_fill_ts=float(fill.ts or now),
            last_fill_side=str(fill.side),
            last_fill_size=fill_size,
            last_internal_shares=self._internal_shares_for_token(fill.token_id),
            last_pm_shares=(
                float(self._cached_pm_up_shares)
                if token_key == "up"
                else float(self._cached_pm_dn_shares)
            ),
            source="fill",
        )

    def _settlement_lag_explains_diff(
        self,
        *,
        token_id: str,
        internal_shares: float,
        pm_shares: float,
        threshold: float,
    ) -> bool:
        state = self._settlement_lag.get(token_id)
        if state is None:
            return False
        if time.time() > state.grace_until:
            return False

        diff = float(internal_shares) - float(pm_shares)
        pending = float(state.pending_delta_shares)
        if abs(diff) < float(threshold) or abs(pending) <= 1e-9 or abs(diff) <= 1e-9:
            return False
        if (diff > 0) != (pending > 0):
            return False

        tolerance = max(0.25, 0.25 * float(threshold))
        return abs(diff) <= abs(pending) + tolerance

    def _classify_drift_snapshot(
        self,
        *,
        real_up: float,
        real_dn: float,
        source: str,
    ) -> tuple[set[str], bool]:
        if not self.market:
            return set(), False

        threshold = self._critical_drift_threshold()
        protected_tokens: set[str] = set()
        has_unexplained_critical_drift = False
        now = time.time()
        token_snapshots = (
            (self.market.up_token_id, float(self.inventory.up_shares), float(real_up)),
            (self.market.dn_token_id, float(self.inventory.dn_shares), float(real_dn)),
        )
        for token_id, internal_shares, pm_shares in token_snapshots:
            diff = internal_shares - pm_shares
            if abs(diff) < threshold:
                continue
            if self._settlement_lag_explains_diff(
                token_id=token_id,
                internal_shares=internal_shares,
                pm_shares=pm_shares,
                threshold=threshold,
            ):
                protected_tokens.add(token_id)
                continue

            state = self._settlement_lag.get(token_id)
            if state is not None:
                self._settlement_lag_escalated_total += 1
                reason = "expired" if now > state.grace_until else "mismatch"
                log.warning(
                    "Settlement lag guard escalated (%s): token=%s reason=%s diff=%.2f pending=%.2f",
                    source,
                    (self._token_key_for_id(token_id) or token_id).upper(),
                    reason,
                    diff,
                    state.pending_delta_shares,
                )
                self._drop_settlement_lag(token_id)

            has_unexplained_critical_drift = True

        return protected_tokens, has_unexplained_critical_drift

    async def _activate_settlement_guard(
        self,
        *,
        protected_tokens: set[str],
        real_up: float,
        real_dn: float,
        source: str,
    ) -> None:
        if not protected_tokens:
            return

        now = time.time()
        grace_sec = max(1.0, float(getattr(self.config, "fill_settlement_grace_sec", 6.0) or 6.0))
        newly_engaged: set[str] = set()
        for token_id in protected_tokens:
            token_key = self._token_key_for_id(token_id)
            if token_key is None:
                continue
            internal_shares = self._internal_shares_for_token(token_id)
            pm_shares = self._pm_shares_for_token(token_id, real_up=real_up, real_dn=real_dn)
            diff = internal_shares - pm_shares
            state = self._settlement_lag.get(token_id)
            if state is None:
                state = SettlementLagState(
                    token_id=token_id,
                    pending_delta_shares=diff,
                    grace_until=now + grace_sec,
                    last_fill_ts=now,
                    last_fill_side="UNKNOWN",
                    last_fill_size=abs(diff),
                    last_internal_shares=internal_shares,
                    last_pm_shares=pm_shares,
                    source=source,
                )
                self._settlement_lag[token_id] = state
            else:
                state.last_internal_shares = internal_shares
                state.last_pm_shares = pm_shares
                state.source = source

            if token_id not in self._settlement_guard_tokens:
                newly_engaged.add(token_id)
            self._settlement_guard_tokens.add(token_id)
            self._throttled_warn(
                f"settlement_guard_armed:{source}:{token_id}",
                (
                    "Settlement lag guard armed (%s): token=%s internal=%.2f pm=%.2f "
                    "diff=%.2f grace_left=%.1fs"
                )
                % (
                    source,
                    token_key.upper(),
                    internal_shares,
                    pm_shares,
                    diff,
                    max(0.0, state.grace_until - now),
                ),
                cooldown=1.0,
            )
            self._current_quotes[token_key] = (None, None)

        if newly_engaged:
            self._settlement_lag_suppressed_total += len(newly_engaged)
            if self.order_mgr.active_order_ids:
                await self._cancel_all_guarded()

    def _update_settlement_lag_progress(self, *, real_up: float, real_dn: float, source: str) -> None:
        if not self._settlement_lag:
            return

        now = time.time()
        clear_threshold = self._settlement_clear_threshold()
        for token_id, state in list(self._settlement_lag.items()):
            token_key = self._token_key_for_id(token_id)
            if token_key is None:
                self._drop_settlement_lag(token_id)
                continue

            internal_shares = self._internal_shares_for_token(token_id)
            pm_shares = self._pm_shares_for_token(token_id, real_up=real_up, real_dn=real_dn)
            diff = internal_shares - pm_shares
            if abs(diff) <= clear_threshold:
                if token_id in self._settlement_guard_tokens:
                    log.info(
                        "Settlement lag guard cleared (%s): token=%s diff=%.3f",
                        source,
                        token_key.upper(),
                        diff,
                    )
                self._drop_settlement_lag(token_id)
                continue

            if now > state.grace_until:
                self._drop_settlement_lag(token_id)
                continue

            if abs(state.pending_delta_shares) > 1e-9 and abs(diff) > 1e-9:
                if (diff > 0) != (state.pending_delta_shares > 0):
                    self._drop_settlement_lag(token_id)
                    continue

            state.last_internal_shares = internal_shares
            state.last_pm_shares = pm_shares
            state.pending_delta_shares = diff
            state.source = source

    def _active_settlement_guard_tokens(self) -> set[str]:
        if not self._settlement_guard_tokens:
            return set()

        now = time.time()
        active: set[str] = set()
        for token_id in list(self._settlement_guard_tokens):
            state = self._settlement_lag.get(token_id)
            if state is None or now > state.grace_until or abs(state.pending_delta_shares) <= 1e-9:
                self._settlement_guard_tokens.discard(token_id)
                continue
            active.add(token_id)
        return active

    def _update_critical_drift_recovery(self, *, real_up: float, real_dn: float, source: str) -> None:
        """Keep MM paused until drift stays reconciled across multiple checks."""
        if not self._critical_drift_pause_active:
            return

        clear_threshold = max(0.25, 0.25 * self._critical_drift_threshold())
        up_diff = abs(float(self.inventory.up_shares) - float(real_up))
        dn_diff = abs(float(self.inventory.dn_shares) - float(real_dn))
        if up_diff <= clear_threshold and dn_diff <= clear_threshold:
            self._critical_drift_recovery_streak += 1
            if self._critical_drift_recovery_streak >= self._critical_drift_recovery_required:
                self._critical_drift_pause_active = False
                self._critical_drift_recovery_streak = 0
                if self._paused and self._pause_reason.startswith("Critical inventory drift"):
                    self._paused = False
                    self._pause_reason = ""
                log.warning(
                    "Critical drift pause cleared (%s): UP diff=%.3f DN diff=%.3f",
                    source,
                    up_diff,
                    dn_diff,
                )
            return

        if self._critical_drift_recovery_streak:
            log.warning(
                "Critical drift still unstable (%s): UP diff=%.3f DN diff=%.3f (reset recovery streak)",
                source,
                up_diff,
                dn_diff,
            )
        self._critical_drift_recovery_streak = 0

    async def _handle_critical_inventory_drift(
        self,
        *,
        real_up: float,
        real_dn: float,
        source: str,
    ) -> bool:
        """Hard-stop when internal inventory diverges materially from PM balances.

        This prevents trading decisions on stale state in both directions
        (internal > PM and internal < PM).
        Returns True when emergency drift handling was applied and tick should abort.
        """
        threshold = self._critical_drift_threshold()
        up_diff = abs(float(self.inventory.up_shares) - float(real_up))
        dn_diff = abs(float(self.inventory.dn_shares) - float(real_dn))
        if up_diff < threshold and dn_diff < threshold:
            return False

        log.error(
            "CRITICAL inventory drift (%s): internal UP=%.2f DN=%.2f, PM UP=%.2f DN=%.2f "
            "(abs_diff up=%.2f dn=%.2f, threshold=%.2f)",
            source,
            self.inventory.up_shares,
            self.inventory.dn_shares,
            real_up,
            real_dn,
            up_diff,
            dn_diff,
            threshold,
        )
        self._critical_drift_pause_active = True
        self._critical_drift_recovery_streak = 0
        self.inventory.reconcile(real_up, real_dn, self._cached_usdc_balance)
        self._arm_reconcile_guard(up_diff, dn_diff, f"critical-{source}")
        self._paused = True
        self._pause_reason = "Critical inventory drift: waiting for full reconcile"
        await self._cancel_all_guarded()
        self._current_quotes = {"up": (None, None), "dn": (None, None)}
        self._one_sided_counter = 0
        return True

    async def _confirm_critical_drift_snapshot(
        self,
        *,
        real_up: float,
        real_dn: float,
        source: str,
    ) -> tuple[float, float, bool]:
        """Re-check PM balances before hard critical-drift pause.

        PM balance endpoint can briefly return transient zero/lagging values right
        after fills. We only allow critical pause when immediate recheck confirms
        the same snapshot.
        """
        threshold = self._critical_drift_threshold()
        up_diff = abs(float(self.inventory.up_shares) - float(real_up))
        dn_diff = abs(float(self.inventory.dn_shares) - float(real_dn))
        if up_diff < threshold and dn_diff < threshold:
            return real_up, real_dn, True
        if not self.market:
            return real_up, real_dn, True

        try:
            confirm_up, confirm_dn = await asyncio.wait_for(
                self.order_mgr.get_all_token_balances(
                    self.market.up_token_id,
                    self.market.dn_token_id,
                    reference_balances=self._balance_reference_snapshot(),
                ),
                timeout=5.0,
            )
        except Exception as e:
            self._throttled_warn(
                f"critical_drift_recheck_failed:{source}",
                f"Critical drift recheck failed ({source}): {e}",
                cooldown=3.0,
            )
            return real_up, real_dn, True

        if confirm_up is None or confirm_dn is None:
            return real_up, real_dn, True

        confirm_up_f = float(confirm_up)
        confirm_dn_f = float(confirm_dn)
        # Snapshot changed too much on immediate recheck -> likely transient PM lag.
        if abs(confirm_up_f - float(real_up)) > 0.5 or abs(confirm_dn_f - float(real_dn)) > 0.5:
            self._throttled_warn(
                f"critical_drift_recheck_unstable:{source}",
                (
                    "Critical drift recheck unstable (%s): first UP=%.2f DN=%.2f, "
                    "recheck UP=%.2f DN=%.2f — skipping hard pause this tick"
                )
                % (source, float(real_up), float(real_dn), confirm_up_f, confirm_dn_f),
                cooldown=2.0,
            )
            return confirm_up_f, confirm_dn_f, False

        return confirm_up_f, confirm_dn_f, True

    def _session_budget_cap(self) -> float:
        """Configured session USDC cap (0 means unlimited)."""
        try:
            return max(0.0, float(self.inventory.initial_usdc or 0.0))
        except (TypeError, ValueError):
            return 0.0

    def _effective_max_drawdown_usd(self) -> float:
        """Drawdown cap bounded by session budget when configured."""
        configured = max(0.0, float(getattr(self.config, "max_drawdown_usd", 0.0) or 0.0))
        budget_cap = self._session_budget_cap()
        if budget_cap > 0:
            return min(configured, budget_cap)
        return configured

    def _maybe_exit_inventory_close_mode_after_clear(self) -> None:
        """Exit closing/liquidation mode once PM balances are fully cleared."""
        if not self.market or not self._is_closing:
            return
        if self._critical_drift_pause_active or self._quality_pause_active:
            return

        window_dur = max(1.0, float(self.market.window_end - self.market.window_start))
        close_sec = min(float(self.config.close_window_sec), window_dur * 0.4)
        time_left = float(self.market.time_remaining)
        if time_left <= close_sec:
            log.info(
                "Inventory liquidation cleared near close window (%.0fs left <= %.0fs close window) — "
                "staying in closing mode",
                time_left,
                close_sec,
            )
            return

        self._is_closing = False
        self._paused = False
        self._pause_reason = ""
        self._liq_lock = None
        self._liq_chunk_index = 0
        self._liq_last_chunk_time = 0.0
        self._liq_last_attempt_time = 0.0
        self._closing_start_time_left = 0.0
        self._liquidation_order_ids.clear()
        self._merge_failed_this_cycle = False
        self._one_sided_counter = 0
        self._requote_event.set()
        log.info("Liquidation complete — exiting closing mode and resuming MM")

    def _session_reserved_collateral(self) -> tuple[float, str]:
        """Reserved collateral estimate used for session exposure checks."""
        total_usdc = self._safe_non_negative(self._cached_usdc_balance)
        avail_usdc = self._cached_usdc_available_balance
        if avail_usdc is not None:
            avail_val = self._safe_non_negative(avail_usdc)
            reserved = max(0.0, total_usdc - avail_val)
            return reserved, "pm_available"

        if not self.market:
            return 0.0, "none"

        token_balances = {
            self.market.up_token_id: self._safe_non_negative(self._cached_pm_up_shares),
            self.market.dn_token_id: self._safe_non_negative(self._cached_pm_dn_shares),
        }
        est = self.order_mgr.estimate_reserved_collateral(token_balances)
        return self._safe_non_negative(est.get("total_reserved", 0.0)), "estimated"

    def _enforce_session_exposure_cap(
        self,
        all_quotes: dict[str, tuple[Quote | None, Quote | None]],
        *,
        pm_up_price: float,
        pm_dn_price: float,
    ) -> None:
        """Clamp BUY quotes so PM-fact exposure never exceeds session cap."""
        budget_cap = self._session_budget_cap()
        if budget_cap <= 0 or not self.market:
            return

        up_px = self._safe_non_negative(pm_up_price)
        dn_px = self._safe_non_negative(pm_dn_price)
        position_value = (
            self._safe_non_negative(self._cached_pm_up_shares) * up_px
            + self._safe_non_negative(self._cached_pm_dn_shares) * dn_px
        )
        reserved, reserved_source = self._session_reserved_collateral()
        exposure_used = position_value + reserved
        headroom = budget_cap - exposure_used

        if headroom <= 0.01:
            suppressed_any = False
            for token_key in ("up", "dn"):
                bid, ask = all_quotes[token_key]
                if bid is not None:
                    suppressed_any = True
                    all_quotes[token_key] = (None, ask)
            if suppressed_any:
                self._throttled_warn(
                    "session_cap_exhausted",
                    "Session cap exhausted: used=$%.2f (pos=$%.2f reserved=$%.2f/%s) >= cap=$%.2f; suppressing BUYs"
                    % (exposure_used, position_value, reserved, reserved_source, budget_cap),
                    cooldown=8.0,
                )
            return

        bid_refs: list[tuple[str, Quote]] = []
        planned_buy_notional = 0.0
        for token_key in ("up", "dn"):
            bid, _ask = all_quotes[token_key]
            if bid is None:
                continue
            if bid.price <= 0 or bid.size <= 0:
                continue
            bid_refs.append((token_key, bid))
            planned_buy_notional += bid.size * bid.price

        if not bid_refs or planned_buy_notional <= headroom + 0.01:
            return

        scale = max(0.0, min(1.0, headroom / planned_buy_notional))
        adjusted = False
        for token_key, bid in bid_refs:
            _cur_bid, ask = all_quotes[token_key]
            new_size = round(max(0.0, bid.size * scale), 2)
            if new_size <= 0:
                all_quotes[token_key] = (None, ask)
                adjusted = True
                continue
            if new_size < bid.size:
                adjusted = True
            bid.size = new_size
            all_quotes[token_key] = (bid, ask)

        if adjusted:
            self._throttled_warn(
                "session_cap_clamp",
                "Session cap clamp: cap=$%.2f used=$%.2f headroom=$%.2f planned_buy=$%.2f (reserved=%s)"
                % (budget_cap, exposure_used, headroom, planned_buy_notional, reserved_source),
                cooldown=8.0,
            )

    async def _refresh_fee_rate_cache(self) -> None:
        """Best-effort refresh of dynamic fee params for current market tokens."""
        if not self.market or hasattr(self.order_mgr.client, "_orders"):
            return
        try:
            from .pm_fees import fetch_fee_rate
        except Exception:
            return

        token_ids = [tid for tid in (self.market.up_token_id, self.market.dn_token_id) if tid]
        if not token_ids:
            return

        results = await asyncio.gather(
            *(fetch_fee_rate(token_id) for token_id in token_ids),
            return_exceptions=True,
        )
        for token_id, result in zip(token_ids, results):
            if isinstance(result, Exception):
                log.warning("Fee-rate refresh failed for %s...: %s", token_id[:12], result)

    def _get_token_mid(self, token_id: str) -> float | None:
        """Best-effort current PM mid for a specific token."""
        if not self.market:
            return None
        st = self.feed_state
        if token_id == self.market.up_token_id:
            bid = float(getattr(st, "pm_up_bid", 0.0) or 0.0)
            ask = float(getattr(st, "pm_up", 0.0) or 0.0)
        elif token_id == self.market.dn_token_id:
            bid = float(getattr(st, "pm_dn_bid", 0.0) or 0.0)
            ask = float(getattr(st, "pm_dn", 0.0) or 0.0)
        else:
            return None
        if bid > 0 and ask > 0:
            return (bid + ask) / 2.0
        return ask if ask > 0 else None

    async def _maybe_backfill_trades(self, *, triggered_by_fills: bool = False) -> None:
        """Run periodic trade-ledger backfill in live mode."""
        if hasattr(self.order_mgr.client, "_orders"):
            return
        now = time.time()
        due = triggered_by_fills or ((now - self._last_trade_backfill_ts) >= self._trade_backfill_interval_sec)
        if not due:
            return
        try:
            added = await asyncio.wait_for(self.order_mgr.backfill_trades(), timeout=8.0)
            self._last_trade_backfill_ts = now
            self.sre_metrics.record_api_call(True)
            if added > 0:
                log.info("Trade backfill: +%d ledger entries", added)
        except Exception as e:
            self._last_trade_backfill_ts = now
            self.sre_metrics.record_api_call(False)
            self._throttled_warn(
                "trade_backfill_failed",
                f"Trade backfill failed: {e}",
                cooldown=30.0,
            )

    def _update_toxicity_mode(self) -> None:
        """Set spread multiplier based on markout toxicity metrics."""
        stats = self.markout_tracker.stats
        total = int(stats.get("total_fills", 0) or 0)
        avg_5s = float(stats.get("avg_markout_5s", 0.0) or 0.0)
        adverse_5s = float(stats.get("adverse_pct_5s", 0.0) or 0.0)

        prev_mode = self._toxicity_mode
        if total < 8:
            self._toxicity_mode = "normal"
            self._toxicity_spread_mult = 1.0
        elif avg_5s <= -0.0030 or adverse_5s >= 70.0:
            self._toxicity_mode = "high"
            self._toxicity_spread_mult = 1.50
        elif avg_5s <= -0.0015 or adverse_5s >= 55.0:
            self._toxicity_mode = "elevated"
            self._toxicity_spread_mult = 1.25
        else:
            self._toxicity_mode = "normal"
            self._toxicity_spread_mult = 1.0

        if self._toxicity_mode != prev_mode:
            log.info(
                "Toxicity mode %s → %s (fills=%d avg5s=%.5f adverse5s=%.1f%% spread_x=%.2f)",
                prev_mode,
                self._toxicity_mode,
                total,
                avg_5s,
                adverse_5s,
                self._toxicity_spread_mult,
            )

    @staticmethod
    def _best_bid_price_size(book: dict[str, Any] | None) -> tuple[float | None, float]:
        """Extract top bid price/size from get_full_book payload."""
        if not isinstance(book, dict):
            return None, 0.0
        bids = book.get("bids") or []
        if not bids:
            return None, 0.0
        top = bids[0] if isinstance(bids[0], dict) else {}
        try:
            return float(top.get("price", 0.0)), float(top.get("size", 0.0))
        except Exception:
            return None, 0.0

    async def _should_prefer_pair_sell_over_merge(self, merge_pairs: float) -> bool:
        """Use live top-book premium/depth to decide sell-over-merge."""
        if not self.market or merge_pairs <= 0:
            return False
        try:
            up_book, dn_book = await asyncio.gather(
                self.order_mgr.get_full_book(self.market.up_token_id),
                self.order_mgr.get_full_book(self.market.dn_token_id),
            )
            up_bid_px, up_bid_sz = self._best_bid_price_size(up_book)
            dn_bid_px, dn_bid_sz = self._best_bid_price_size(dn_book)
            if not up_bid_px or not dn_bid_px:
                return False

            bid_sum = up_bid_px + dn_bid_px
            premium = bid_sum - 1.0
            depth_pairs = min(max(0.0, up_bid_sz), max(0.0, dn_bid_sz))
            min_depth = min(float(self.config.merge_sell_min_depth_pairs), float(merge_pairs))
            epsilon = float(self.config.merge_sell_epsilon)
            prefer_sell = premium >= epsilon and depth_pairs >= min_depth
            if prefer_sell:
                log.info(
                    "Merge skipped: sell-over-merge premium=%.4f (bid_sum=%.4f) depth_pairs=%.2f >= %.2f",
                    premium,
                    bid_sum,
                    depth_pairs,
                    min_depth,
                )
            return prefer_sell
        except Exception as e:
            log.debug("Merge break-even check failed: %s", e)
            return False

    async def _apply_rebate_scoring_filters(
        self,
        all_quotes: dict[str, tuple[Quote | None, Quote | None]],
        *,
        is_live: bool,
    ) -> None:
        """Use PM order scoring to adjust quote selection/sizing."""
        if not is_live or not bool(getattr(self.config, "rebate_scoring_enabled", True)):
            return
        interval = max(1, int(getattr(self.config, "rebate_check_interval_ticks", 20)))
        if self._tick_count % interval != 0:
            return

        quote_refs: list[tuple[str, str, Quote]] = []
        for token_key in ("up", "dn"):
            bid, ask = all_quotes[token_key]
            if bid is not None and not any(q is bid for q in self._taker_quotes):
                quote_refs.append((token_key, "BUY", bid))
            if ask is not None:
                quote_refs.append((token_key, "SELL", ask))
        if not quote_refs:
            return

        payload = [
            {"token_id": q.token_id, "price": q.price, "size": q.size, "side": side}
            for _, side, q in quote_refs
        ]
        timeout = max(0.5, float(getattr(self.config, "rebate_score_timeout_sec", 3.0)))
        try:
            results = await asyncio.wait_for(self.rebate.check_batch(payload), timeout=timeout)
        except Exception as e:
            self._throttled_warn("rebate_scoring", f"Rebate scoring check failed: {e}", cooldown=30.0)
            return

        require_scoring = bool(getattr(self.config, "rebate_require_scoring", False))
        non_scoring_mult = float(getattr(self.config, "rebate_non_scoring_size_mult", 0.7))
        non_scoring_mult = max(0.1, min(1.0, non_scoring_mult))
        pm_min_size = float(self.market.min_order_size) if self.market else 5.0
        min_size = max(float(getattr(self.config, "min_quote_size_shares", 1.0)), pm_min_size)
        dropped = 0
        resized = 0
        eligible = 0

        for (token_key, side, quote), result in zip(quote_refs, results):
            scoring_ok = bool(result and result.get("scoring", False))
            if scoring_ok:
                eligible += 1
                continue

            if require_scoring:
                cur_bid, cur_ask = all_quotes[token_key]
                if side == "BUY":
                    all_quotes[token_key] = (None, cur_ask)
                else:
                    all_quotes[token_key] = (cur_bid, None)
                dropped += 1
                continue

            old_size = quote.size
            quote.size = round(max(min_size, quote.size * non_scoring_mult), 2)
            if quote.size < old_size:
                resized += 1

        log.info(
            "Rebate scoring: eligible=%d/%d dropped=%d resized=%d require_scoring=%s",
            eligible,
            len(quote_refs),
            dropped,
            resized,
            require_scoring,
        )

    def _schedule_heartbeat_failure(self) -> None:
        """Schedule async heartbeat failure handler from sync callback context."""
        self.sre_metrics.record_heartbeat_error()
        if self._heartbeat_failure_task and not self._heartbeat_failure_task.done():
            return
        try:
            loop = asyncio.get_running_loop()
        except RuntimeError:
            log.error("Heartbeat failure callback invoked without a running event loop")
            return
        self._heartbeat_failure_task = loop.create_task(self._on_heartbeat_failure())

    async def _cancel_all_guarded(self, *, force_exchange: bool = False) -> int:
        """Serialize cancel-all calls with placement flow to avoid race conditions."""
        async with self._order_ops_lock:
            return await self.order_mgr.cancel_all(force_exchange=force_exchange)

    async def _cancel_order_guarded(self, order_id: str) -> bool:
        """Serialize single-order cancels with placement flow."""
        async with self._order_ops_lock:
            return await self.order_mgr.cancel_order(order_id)

    async def _check_fills_guarded(self) -> list[Fill]:
        """Serialize fill processing with cancel/place flow."""
        async with self._order_ops_lock:
            return await self.order_mgr.check_fills()

    async def _place_order_guarded(
        self,
        quote: Quote,
        *,
        post_only: bool | None = None,
        fallback_taker: bool = False,
    ) -> Optional[str]:
        """Serialize order placement and skip if emergency shutdown is active."""
        async with self._order_ops_lock:
            if self._emergency_flag:
                return None
            return await self.order_mgr.place_order(
                quote, post_only=post_only, fallback_taker=fallback_taker
            )

    async def _on_heartbeat_failure(self) -> None:
        """Handle fatal heartbeat failure (triggered after configured threshold)."""
        threshold = int(self.heartbeat.stats.get("failure_threshold", 3) or 3)
        reason = f"Heartbeat failed {threshold} consecutive times"
        log.warning("Heartbeat failure callback triggered: %s", reason)
        try:
            await self._emergency_shutdown(reason)
        finally:
            # Always clear local order/quote state even if exchange cancel raised.
            async with self._order_ops_lock:
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
        async with self._order_ops_lock:
            try:
                await self.order_mgr.cancel_all(force_exchange=True)
            except Exception as e:
                log.error("Emergency cancel_all failed: %s", e, exc_info=True)
            self.order_mgr.clear_local_order_tracking()
            self._current_quotes = {
                "up": (None, None),
                "dn": (None, None),
            }

        try:
            await self.heartbeat.stop()
        except Exception as e:
            log.error("Emergency heartbeat stop failed: %s", e, exc_info=True)
        self.markout_tracker.stop()

        self._running = False
        self._paused = True
        self._pause_reason = reason
        self._is_closing = True

    def _event_requote_snapshot(self, had_fill: bool = False) -> list[Any]:
        """Evaluate event-driven requote conditions from current feed state."""
        if not bool(getattr(self.config, "event_requote_enabled", True)):
            return []
        if self._is_closing:
            return []
        st = self.feed_state

        # Keep thresholds hot-reloaded from runtime config updates.
        self.event_requoter.pm_mid_threshold_bps = float(
            getattr(self.config, "event_pm_mid_threshold_bps", self.event_requoter.pm_mid_threshold_bps)
        )
        self.event_requoter.binance_threshold_bps = float(
            getattr(self.config, "event_binance_threshold_bps", self.event_requoter.binance_threshold_bps)
        )
        self.event_requoter.fallback_interval_sec = float(
            getattr(self.config, "event_fallback_interval_sec", self.event_requoter.fallback_interval_sec)
        )

        pm_bid = float(getattr(st, "pm_up_bid", 0.0) or 0.0)
        pm_ask = float(getattr(st, "pm_up", 0.0) or 0.0)
        pm_mid = ((pm_bid + pm_ask) / 2.0) if (pm_bid > 0 and pm_ask > 0) else None
        if pm_mid is None and pm_ask > 0:
            pm_mid = pm_ask

        best_bid = pm_bid if pm_bid > 0 else None
        best_ask = pm_ask if pm_ask > 0 else None
        binance_mid = float(getattr(st, "mid", 0.0) or 0.0)
        if binance_mid <= 0:
            binance_mid = None

        tier = int(self._imbalance_adjustments.get("tier", 0))
        events = self.event_requoter.check_events(
            current_pm_mid=pm_mid,
            current_binance_price=binance_mid,
            current_best_bid=best_bid,
            current_best_ask=best_ask,
            had_fill=had_fill,
            inventory_imbalance_tier=tier,
        )
        if events:
            self._last_requote_events = [
                {"event_type": e.event_type, "timestamp": e.timestamp, "detail": e.detail}
                for e in events[-10:]
            ]
        return events

    def set_market(self, market: MarketInfo) -> None:
        """Set the current market (token IDs, strike, window)."""
        self.market = market
        self.order_mgr.set_market_context(min_order_size=market.min_order_size)
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
            cancelled = await self._cancel_all_guarded(force_exchange=True)
            if cancelled:
                log.info("Startup: cancelled %d stale orders from previous session", cancelled)
                await asyncio.sleep(1.0)  # Wait for PM to settle after cancels
        except Exception as e:
            if self._is_live_mode():
                self._running = False
                raise StartBlockedError(
                    f"Start blocked: startup cancel_all failed in live mode: {e}"
                ) from e
            log.warning("Startup: cancel_all failed: %s", e)

        # Snapshot starting portfolio (USDC + token values) for real session PnL
        try:
            (real_up, real_dn), usdc_pair = await asyncio.gather(
                self.order_mgr.get_all_token_balances(
                    self.market.up_token_id,
                    self.market.dn_token_id,
                    reference_balances=self._balance_reference_snapshot(),
                ),
                self.order_mgr.get_usdc_balances(),
            )
            starting_usdc, starting_usdc_available = usdc_pair
            if starting_usdc is None:
                log.warning("Failed to fetch starting USDC balance, defaulting to 0.0")
                starting_usdc = 0.0
            requested_session_budget = self._session_budget_cap()
            if requested_session_budget > 0:
                session_budget = min(requested_session_budget, starting_usdc)
                if session_budget + 0.01 < requested_session_budget:
                    log.warning(
                        "Session budget clipped to wallet balance: requested=$%.2f available=$%.2f -> using=$%.2f",
                        requested_session_budget,
                        starting_usdc,
                        session_budget,
                    )
            else:
                session_budget = starting_usdc
            self._starting_usdc_pm = starting_usdc
            self._cached_usdc_balance = starting_usdc
            self._cached_usdc_available_balance = (
                starting_usdc_available if starting_usdc_available is not None else starting_usdc
            )
            self._cached_pm_up_shares = real_up if real_up is not None else 0.0
            self._cached_pm_dn_shares = real_dn if real_dn is not None else 0.0
            # Always initialize internal inventory from real PM balances to avoid stale state.
            self.inventory.up_shares = self._cached_pm_up_shares
            self.inventory.dn_shares = self._cached_pm_dn_shares
            self.inventory.usdc = starting_usdc
            self.inventory.initial_usdc = session_budget
            self.inventory.up_cost.reset()
            self.inventory.dn_cost.reset()
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
            reserved_collateral = max(
                0.0,
                starting_usdc - (
                    starting_usdc_available
                    if starting_usdc_available is not None
                    else starting_usdc
                ),
            )
            preexisting_exposure = _token_value + reserved_collateral
            flat_start_enabled = bool(getattr(self.config, "require_flat_start", True))
            flat_start_max_shares = max(
                0.0,
                float(getattr(self.config, "flat_start_max_shares", 0.25) or 0.25),
            )
            if flat_start_enabled and (
                self._cached_pm_up_shares > flat_start_max_shares
                or self._cached_pm_dn_shares > flat_start_max_shares
            ):
                raise StartBlockedError(
                    "Start blocked: non-flat wallet inventory detected "
                    "(UP=%.2f DN=%.2f, allowed<=%.2f). "
                    "Close/merge/redeem positions before starting or disable require_flat_start."
                    % (
                        self._cached_pm_up_shares,
                        self._cached_pm_dn_shares,
                        flat_start_max_shares,
                    )
                )
            if session_budget > 0 and preexisting_exposure > (session_budget + 0.01):
                raise StartBlockedError(
                    "Start blocked: pre-existing exposure $%.2f (tokens=$%.2f reserved=$%.2f) "
                    "exceeds session cap $%.2f. Close/redeem positions before starting."
                    % (preexisting_exposure, _token_value, reserved_collateral, session_budget)
                )
            self._starting_portfolio_pm = starting_usdc + _token_value
            # Initialize internal inventory from PM balances (pre-existing tokens
            # from previous sessions must be tracked to avoid phantom PnL)
            if self._cached_pm_up_shares > 0 or self._cached_pm_dn_shares > 0:
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
            if session_budget > 0:
                log.info(
                    "Session budget cap active: $%.2f (wallet total USDC: $%.2f)",
                    session_budget,
                    starting_usdc,
                )
        except StartBlockedError:
            self._running = False
            raise
        except Exception:
            self._starting_usdc_pm = 0.0
            self._starting_portfolio_pm = 0.0
            self._cached_usdc_balance = 0.0
            self._cached_usdc_available_balance = 0.0
            self._cached_pm_up_shares = 0.0
            self._cached_pm_dn_shares = 0.0
            self.inventory.up_shares = 0.0
            self.inventory.dn_shares = 0.0
            self.inventory.usdc = 0.0
            self.inventory.initial_usdc = 0.0
            self.inventory.up_cost.reset()
            self.inventory.dn_cost.reset()
        self._started_at = time.time()
        self._pnl_grace_until = self._started_at + 30.0  # 30s grace period for PnL checks
        self._catastrophic_count = 0
        self._paused = False
        self._pause_reason = ""
        self._critical_drift_pause_active = False
        self._critical_drift_recovery_streak = 0
        self._settlement_lag = {}
        self._settlement_guard_tokens = set()
        self._settlement_lag_suppressed_total = 0
        self._settlement_lag_escalated_total = 0
        self._heartbeat_failure_task = None
        self._tick_count = 0
        self._merge_check_counter = 0
        self._reconcile_guard_until = 0.0
        self._last_trade_backfill_ts = 0.0
        self._toxicity_spread_mult = 1.0
        self._toxicity_mode = "normal"
        self._toxic_divergence_count = 0
        self._is_closing = False
        self._liquidation_attempted = False
        self._liquidation_order_ids = set()
        self._liq_lock = None
        self._liq_chunk_index = 0
        self._liq_last_chunk_time = 0.0
        self._liq_last_attempt_time = 0.0
        self._one_sided_counter = 0
        self._quality_error_count = 0
        self._quality_success_count = 0
        self._quality_pause_active = False
        self._post_fill_entry_guard_until = 0.0
        self._post_fill_entry_guard_anchor = None
        self._post_fill_entry_guard_active = False
        self._post_fill_entry_guard_reason = ""
        self._post_fill_entry_guard_trigger_count = 0
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
        self.pnl_decomp.reset()
        self.rebate.reset()
        self.markout_tracker.stop()
        self.markout_tracker = MarkoutTracker(self._get_token_mid)
        self.event_requoter = EventRequoter(
            pm_mid_threshold_bps=self.config.event_pm_mid_threshold_bps,
            binance_threshold_bps=self.config.event_binance_threshold_bps,
            fallback_interval_sec=self.config.event_fallback_interval_sec,
        )
        self._last_requote_events = []

        # Warm fee-rate cache for both tokens (best-effort, non-fatal).
        await self._refresh_fee_rate_cache()

        # Wire fill callback → trigger immediate requote
        self.order_mgr.set_fill_callback(lambda: self._requote_event.set())
        self.order_mgr.set_ws_reconnect_callback(self.sre_metrics.record_ws_reconnect)

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
        if not self._running and not liquidate:
            return

        was_running = self._running
        self._running = False
        if was_running:
            log.info("MarketMaker stopping...")
        else:
            log.info("MarketMaker stop requested while loop already stopped — cleanup continues")

        # Cancel main loop
        if self._task and not self._task.done():
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass
        self._task = None

        # Cancel all orders first
        cancelled = await self._cancel_all_guarded()
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
                await self._check_fills_guarded()

        # Final cancel of any remaining orders
        await self._cancel_all_guarded()

        # Stop fill WebSocket
        await self.order_mgr.stop_fill_ws()
        self.markout_tracker.stop()

        # Stop heartbeat
        await self.heartbeat.stop()

        log.info("MarketMaker stopped")

    async def _run_loop(self) -> None:
        """Main quoting loop — event-driven with timeout fallback."""
        log.info("Quote loop started")
        try:
            while self._running:
                runtime_metrics.incr("mm.run_loop.iter")
                loop_started_at = time.monotonic()
                try:
                    try:
                        tick_timeout = 15.0 if self._is_closing else 10.0
                        tick_started_at = time.monotonic()
                        await asyncio.wait_for(self._tick(), timeout=tick_timeout)
                        runtime_metrics.incr("mm.run_loop.tick_ok")
                        runtime_metrics.observe_ms(
                            "mm.run_loop.tick_duration_ms",
                            (time.monotonic() - tick_started_at) * 1000.0,
                        )
                    except asyncio.TimeoutError:
                        runtime_metrics.incr("mm.run_loop.tick_timeout")
                        self.sre_metrics.record_tick_timeout()
                        self._log.warning("_tick() timed out after %.0fs, skipping iteration",
                                          15.0 if self._is_closing else 10.0)
                except asyncio.CancelledError:
                    break
                except Exception as e:
                    runtime_metrics.incr("mm.run_loop.error")
                    log.error(f"Tick error: {e}", exc_info=True)

                # Closing mode does not need event-driven requote bursts.
                # Keep a bounded cadence so API handlers remain responsive.
                if self._is_closing:
                    if self.market and self.market.time_remaining <= float(
                        getattr(self.config, "liq_taker_threshold_sec", 10.0)
                    ):
                        close_sleep = 0.5
                    else:
                        close_sleep = max(
                            1.0,
                            min(float(getattr(self.config, "liq_chunk_interval_sec", 5.0)), 3.0),
                        )
                    elapsed = time.monotonic() - loop_started_at
                    if elapsed < close_sleep:
                        await asyncio.sleep(close_sleep - elapsed)
                    continue

                # Wait until explicit trigger, event-driven market movement, or timeout.
                sleep_budget = self.quote_engine.jitter_requote_interval(
                    self.config.requote_interval_sec
                )
                poll_interval = max(0.05, float(getattr(self.config, "event_poll_interval_sec", 0.25)))
                deadline = time.monotonic() + max(0.1, float(sleep_budget))
                while self._running:
                    if self._requote_event.is_set():
                        self._requote_event.clear()
                        break

                    events = self._event_requote_snapshot(had_fill=False)
                    if events:
                        non_timer = [e for e in events if e.event_type != "timer_fallback"]
                        if non_timer:
                            event_names = ",".join(sorted({e.event_type for e in non_timer}))
                            log.debug("Event-driven requote trigger: %s", event_names)
                        break

                    remaining = deadline - time.monotonic()
                    if remaining <= 0:
                        break
                    await asyncio.sleep(min(poll_interval, remaining))

                # Bound max loop frequency under event storms to keep API responsive.
                min_loop_interval = max(0.50, poll_interval)
                elapsed = time.monotonic() - loop_started_at
                if elapsed < min_loop_interval:
                    await asyncio.sleep(min_loop_interval - elapsed)
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
                best_bid_up, best_bid_dn = await self._get_liq_lock_best_bids()
                self._liq_lock = self.risk_mgr.lock_pnl(
                    self.inventory, fv_up, fv_dn,
                    margin=self.config.liq_price_floor_margin,
                    best_bid_up=best_bid_up,
                    best_bid_dn=best_bid_dn,
                )
                self._liq_chunk_index = 0
                self._liq_last_chunk_time = 0.0
                await self._cancel_all_guarded()
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
            best_bid_up, best_bid_dn = await self._get_liq_lock_best_bids()
            self._liq_lock = self.risk_mgr.lock_pnl(
                self.inventory, fv_up_close, fv_dn_close,
                margin=self.config.liq_price_floor_margin,
                best_bid_up=best_bid_up,
                best_bid_dn=best_bid_dn,
            )
            self._liq_chunk_index = 0
            self._liq_last_chunk_time = 0.0
            self._liq_last_attempt_time = 0.0
            log.info(f"Closing mode: {time_left:.0f}s remaining — cancelling all orders "
                     f"(lock: pnl=${self._liq_lock.trigger_pnl:.2f})")
            await self._cancel_all_guarded()
            self._current_quotes = {"up": (None, None), "dn": (None, None)}

        # 2. Check for fills (always, including closing mode)
        fills = await self._check_fills_guarded()
        if fills:
            # Fills change available collateral; force fresh read on next USDC check.
            self.order_mgr.invalidate_usdc_cache()
        for fill in fills:
            token_type = "up" if fill.token_id == self.market.up_token_id else "dn"
            self.inventory.update_from_fill(fill, token_type)
            self.risk_mgr.record_fill(fill)
            self._record_live_fill_settlement(fill)
            self._arm_post_fill_entry_guard(fill)
            self.pnl_decomp.record_fill(
                fill.side, fill.token_id, fill.price, fill.size, fill.fee, fill.is_maker
            )
            self.markout_tracker.record_fill(
                fill.side, fill.token_id, fill.price, fill.size, fill.is_maker
            )
            log.info(f"FILL: {fill.side} {fill.size:.1f}@{fill.price:.2f} "
                     f"({token_type.upper()}) fee={fill.fee:.4f}")
            for cb in self._on_fill_callbacks:
                try:
                    cb(fill, token_type)
                except Exception as e:
                    log.warning("Fill callback error: %s", e)
        await self._maybe_backfill_trades(triggered_by_fills=bool(fills))
        await self.markout_tracker.check_markouts()
        self._update_toxicity_mode()
        _t_fills = time.perf_counter()

        merge_reconcile_requested = False
        # Merge-first: check for merge opportunity periodically (skip during closing).
        if not self._is_closing:
            guarded_tokens = self._active_settlement_guard_tokens()
            if guarded_tokens:
                guard_left = max(
                    0.0,
                    max(
                        (self._settlement_lag[token_id].grace_until - time.time())
                        for token_id in guarded_tokens
                        if token_id in self._settlement_lag
                    ),
                )
                guard_names = ",".join(
                    sorted(
                        (self._token_key_for_id(token_id) or token_id).upper()
                        for token_id in guarded_tokens
                    )
                )
                self._throttled_warn(
                    "merge_settlement_guard",
                    f"Merge check paused for {guard_left:.1f}s during settlement lag ({guard_names})",
                    cooldown=2.0,
                )
            elif self._reconcile_guard_active():
                guard_left = max(0.0, self._reconcile_guard_until - time.time())
                self._throttled_warn(
                    "merge_reconcile_guard",
                    f"Merge check paused for {guard_left:.1f}s after reconcile drift",
                    cooldown=3.0,
                )
            else:
                self._merge_check_counter += 1
                if self._merge_check_counter >= self._merge_check_interval:
                    self._merge_check_counter = 0
                    if not self._merge_failed_this_cycle:
                        merge_profit = await self._try_merge_pairs()
                        if merge_profit > 0:
                            merge_reconcile_requested = True
                            self.order_mgr.invalidate_usdc_cache()

        # Live mode: periodically reconcile internal shares with PM balances.
        # During closing we still run safety reconcile (faster cadence) so
        # liquidation logic always sees wallet-truth inventory.
        # Normal mode uses debounce (3+ stable checks) to avoid oscillation from
        # PM balance API lagging behind fill detection.
        is_live = not hasattr(self.order_mgr.client, "_orders")
        reconcile_requested = self.order_mgr.reconcile_requested or merge_reconcile_requested
        reconcile_interval_ticks = 15
        if self._is_closing:
            reconcile_interval_ticks = max(
                1,
                int(getattr(self.config, "closing_reconcile_interval_ticks", 2) or 2),
            )
        should_reconcile_now = reconcile_requested or (self._tick_count % reconcile_interval_ticks == 0)
        if is_live and should_reconcile_now:
            try:
                (real_up, real_dn), usdc_pair = await asyncio.gather(
                    self.order_mgr.get_all_token_balances(
                        self.market.up_token_id,
                        self.market.dn_token_id,
                        reference_balances=self._balance_reference_snapshot(),
                    ),
                    self.order_mgr.get_usdc_balances(),
                )
            except Exception as e:
                self.sre_metrics.record_api_call(False)
                log.error(
                    "Skipping inventory reconcile: failed to fetch PM balances (%s)",
                    e,
                )
                self._reconcile_stable_count = 0
                self._reconcile_prev_pm = None
                if self.order_mgr.active_order_ids:
                    await self._cancel_all_guarded()
                    self._current_quotes = {"up": (None, None), "dn": (None, None)}
                return

            self.sre_metrics.record_api_call(True)
            usdc_bal, usdc_available = usdc_pair
            if usdc_bal is not None:
                self._cached_usdc_balance = usdc_bal
                self.inventory.usdc = usdc_bal
            else:
                log.warning("Failed to refresh USDC balance, keeping previous cached value")
            if usdc_available is not None:
                self._cached_usdc_available_balance = usdc_available

            if real_up is None or real_dn is None:
                log.error("Skipping inventory reconcile: failed to fetch PM token balances")
                self._reconcile_stable_count = 0
                self._reconcile_prev_pm = None
            else:
                real_up = float(real_up)
                real_dn = float(real_dn)
                protected_tokens, has_unexplained_critical_drift = self._classify_drift_snapshot(
                    real_up=real_up,
                    real_dn=real_dn,
                    source="periodic_reconcile",
                )
                # Cache real PM token balances for accurate PnL calculation.
                self._cached_pm_up_shares = real_up
                self._cached_pm_dn_shares = real_dn

                if protected_tokens and not has_unexplained_critical_drift:
                    await self._activate_settlement_guard(
                        protected_tokens=protected_tokens,
                        real_up=real_up,
                        real_dn=real_dn,
                        source="periodic_reconcile",
                    )
                    self._update_settlement_lag_progress(
                        real_up=real_up,
                        real_dn=real_dn,
                        source="periodic_reconcile",
                    )
                    self._update_critical_drift_recovery(
                        real_up=real_up,
                        real_dn=real_dn,
                        source="periodic_reconcile",
                    )
                    self._reconcile_stable_count = 0
                    self._reconcile_prev_pm = None
                    if self._critical_drift_pause_active:
                        self._current_quotes = {"up": (None, None), "dn": (None, None)}
                        return
                else:
                    if has_unexplained_critical_drift:
                        real_up, real_dn, drift_confirmed = await self._confirm_critical_drift_snapshot(
                            real_up=real_up,
                            real_dn=real_dn,
                            source="periodic_reconcile",
                        )
                        self._cached_pm_up_shares = real_up
                        self._cached_pm_dn_shares = real_dn
                        if drift_confirmed:
                            if await self._handle_critical_inventory_drift(
                                real_up=real_up,
                                real_dn=real_dn,
                                source="periodic_reconcile",
                            ):
                                self._reconcile_stable_count = 0
                                self._reconcile_prev_pm = None
                                self.order_mgr.clear_reconcile_request()
                                return

                    self._update_settlement_lag_progress(
                        real_up=real_up,
                        real_dn=real_dn,
                        source="periodic_reconcile",
                    )
                    self._update_critical_drift_recovery(
                        real_up=real_up,
                        real_dn=real_dn,
                        source="periodic_reconcile",
                    )
                    if self._critical_drift_pause_active:
                        self.order_mgr.clear_reconcile_request()
                        self._current_quotes = {"up": (None, None), "dn": (None, None)}
                        return
                    if reconcile_requested:
                        forced_up_diff = abs(real_up - self.inventory.up_shares)
                        forced_dn_diff = abs(real_dn - self.inventory.dn_shares)
                        log.warning(
                            "Forced reconcile after anomalous fill: internal UP=%.2f DN=%.2f "
                            "→ PM UP=%.2f DN=%.2f",
                            self.inventory.up_shares, self.inventory.dn_shares,
                            real_up, real_dn,
                        )
                        self.inventory.reconcile(real_up, real_dn, self._cached_usdc_balance)
                        self._arm_reconcile_guard(forced_up_diff, forced_dn_diff, "forced")
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

                            stable_required = 1 if self._is_closing else 3
                            if self._reconcile_stable_count >= stable_required:
                                log.warning(
                                    "Inventory reconcile (%d/%d stable checks): "
                                    "internal UP=%.2f DN=%.2f → PM UP=%.2f DN=%.2f",
                                    self._reconcile_stable_count,
                                    stable_required,
                                    self.inventory.up_shares, self.inventory.dn_shares,
                                    real_up, real_dn,
                                )
                                self.inventory.reconcile(real_up, real_dn, self._cached_usdc_balance)
                                self._arm_reconcile_guard(up_diff, dn_diff, "debounced")
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
        elif not is_live:
            # Paper mode: sync from mock client balance + internal inventory
            self._cached_usdc_balance = self.order_mgr.client.get_balance()
            self._cached_usdc_available_balance = self._cached_usdc_balance
            self.inventory.usdc = self._cached_usdc_balance
            self._cached_pm_up_shares = self.inventory.up_shares
            self._cached_pm_dn_shares = self.inventory.dn_shares

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
            if self._reconcile_guard_active():
                guard_left = max(0.0, self._reconcile_guard_until - time.time())
                self._throttled_warn(
                    "liquidation_reconcile_guard",
                    f"Liquidation paused for {guard_left:.1f}s after reconcile drift",
                    cooldown=2.0,
                )
                return
            guarded_tokens = self._active_settlement_guard_tokens()
            if guarded_tokens and time_left > max(5.0, float(self.config.liq_taker_threshold_sec)):
                guard_names = ",".join(
                    sorted(
                        (self._token_key_for_id(token_id) or token_id).upper()
                        for token_id in guarded_tokens
                    )
                )
                guard_left = max(
                    0.0,
                    max(
                        (self._settlement_lag[token_id].grace_until - time.time())
                        for token_id in guarded_tokens
                        if token_id in self._settlement_lag
                    ),
                )
                self._throttled_warn(
                    "liquidation_settlement_guard",
                    f"Liquidation paused for {guard_left:.1f}s during settlement lag ({guard_names})",
                    cooldown=2.0,
                )
                return
            # Continuously try to sell remaining inventory each tick
            await self._liquidate_inventory()
            return

        if self._reconcile_guard_active():
            guard_left = max(0.0, self._reconcile_guard_until - time.time())
            self._throttled_warn(
                "quote_reconcile_guard",
                f"Quote placement paused for {guard_left:.1f}s after reconcile drift",
                cooldown=2.0,
            )
            if self.order_mgr.active_order_ids:
                await self._cancel_all_guarded()
                self._current_quotes = {"up": (None, None), "dn": (None, None)}
            return

        st = self.feed_state

        if is_live:
            available_usdc = (
                self._cached_usdc_available_balance
                if self._cached_usdc_available_balance is not None
                else self._cached_usdc_balance
            )
            min_usdc_to_quote = max(0.5, float(getattr(self.config, "min_order_size_usd", 2.0)))
            has_sellable_inventory = (
                (
                    self._cached_pm_up_shares if is_live else self.inventory.up_shares
                ) > 0.5
                or (
                    self._cached_pm_dn_shares if is_live else self.inventory.dn_shares
                ) > 0.5
            )
            if available_usdc < min_usdc_to_quote and not has_sellable_inventory:
                self._throttled_warn(
                    "low_usdc_quote_skip",
                    (
                        f"Skipping quote: free USDC ${available_usdc:.2f} "
                        f"< min_order_size_usd ${min_usdc_to_quote:.2f}"
                    ),
                    cooldown=5.0,
                )
                if self.order_mgr.active_order_ids:
                    await self._cancel_all_guarded()
                    self._current_quotes = {"up": (None, None), "dn": (None, None)}
                return

        # 1. Defensive copies of feed data
        mid = st.mid
        now = time.time()
        last_ob_ok_ts = getattr(st, "binance_ob_last_ok_ts", 0.0) or 0.0
        last_ws_ok_ts = getattr(st, "binance_ws_last_ok_ts", 0.0) or 0.0
        last_ok_ts = max(last_ob_ok_ts, last_ws_ok_ts)
        if last_ok_ts > 0:
            staleness = now - last_ok_ts
            is_stale = staleness > BINANCE_FEED_STALE_SEC
        else:
            staleness = now - self._started_at if self._started_at > 0 else 0.0
            is_stale = staleness > BINANCE_FEED_STARTUP_GRACE_SEC

        if is_stale:
            ob_age = (now - last_ob_ok_ts) if last_ob_ok_ts > 0 else None
            ws_age = (now - last_ws_ok_ts) if last_ws_ok_ts > 0 else None
            ob_age_s = f"{ob_age:.1f}s" if ob_age is not None else "n/a"
            ws_age_s = f"{ws_age:.1f}s" if ws_age is not None else "n/a"
            self._throttled_warn(
                "binance_feed_stale",
                (
                    f"Binance feed stale ({staleness:.1f}s): "
                    f"ob_age={ob_age_s} ws_age={ws_age_s} "
                    f"ob_connected={bool(getattr(st, 'binance_ob_connected', False))} "
                    f"ws_connected={bool(getattr(st, 'binance_ws_connected', False))} "
                    f"ob_msgs={int(getattr(st, 'binance_ob_msg_count', 0) or 0)} "
                    f"ws_msgs={int(getattr(st, 'binance_ws_msg_count', 0) or 0)} "
                    "— cancelling orders and skipping tick"
                ),
                cooldown=2.0,
            )
            await self._cancel_all_guarded()
            self._current_quotes = {"up": (None, None), "dn": (None, None)}
            return

        # Also check PM price staleness — stale PM mids cause incorrect quoting
        pm_last_update = getattr(st, "pm_last_update_ts", 0.0) or 0.0
        pm_staleness = now - pm_last_update if pm_last_update > 0 else 0.0
        if pm_last_update > 0 and pm_staleness > 15.0:
            self._throttled_warn(
                "pm_stale",
                f"PM prices stale ({pm_staleness:.0f}s) — will use Binance FV as fallback",
                cooldown=10.0,
            )

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
            await self._cancel_all_guarded()
            self._current_quotes = {"up": (None, None), "dn": (None, None)}
            return

        # 3. Compute fair value (PM-anchored when PM data is fresh).
        _t_fv_start = time.perf_counter()
        fv_up, fv_dn = self._compute_fv()

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

        # 3c. PM-anchored mids for quote generation (fallback to model FV).
        # Only use PM prices if they are fresh (< 15s old).
        pm_fresh = pm_last_update > 0 and pm_staleness < 15.0
        pm_up_bid = getattr(st, "pm_up_bid", None)
        pm_up_ask = getattr(st, "pm_up", None)
        pm_dn_bid = getattr(st, "pm_dn_bid", None)
        pm_dn_ask = getattr(st, "pm_dn", None)

        if pm_fresh and pm_up_bid is not None and pm_up_ask is not None and pm_up_bid > 0 and pm_up_ask > 0:
            pm_mid_up = (pm_up_bid + pm_up_ask) / 2.0
        else:
            pm_mid_up = fv_up

        if pm_fresh and pm_dn_bid is not None and pm_dn_ask is not None and pm_dn_bid > 0 and pm_dn_ask > 0:
            pm_mid_dn = (pm_dn_bid + pm_dn_ask) / 2.0
        else:
            pm_mid_dn = fv_dn

        up_divergence = abs(fv_up - pm_mid_up)
        dn_divergence = abs(fv_dn - pm_mid_dn)
        widen_spread_tick = (up_divergence > 0.05) or (dn_divergence > 0.05)
        if widen_spread_tick:
            diverged = []
            if up_divergence > 0.05:
                diverged.append(f"UP={up_divergence:.3f}")
            if dn_divergence > 0.05:
                diverged.append(f"DN={dn_divergence:.3f}")
            log.warning(
                "Model/PM mid divergence > 5%% (%s); widening quotes x2 for this tick",
                ", ".join(diverged),
            )
        max_divergence = max(up_divergence, dn_divergence)
        toxic_divergence_threshold = max(
            0.02,
            float(getattr(self.config, "toxic_divergence_threshold", 0.10) or 0.10),
        )
        toxic_divergence_ticks = max(
            1,
            int(getattr(self.config, "toxic_divergence_ticks", 8) or 8),
        )
        if max_divergence >= toxic_divergence_threshold:
            self._toxic_divergence_count += 1
        else:
            self._toxic_divergence_count = 0
        toxic_entry_block = self._toxic_divergence_count >= toxic_divergence_ticks
        if toxic_entry_block:
            has_inventory_for_unwind = (
                self._cached_pm_up_shares > 0.5
                or self._cached_pm_dn_shares > 0.5
                or self.inventory.up_shares > 0.5
                or self.inventory.dn_shares > 0.5
            )
            self._throttled_warn(
                "toxic_divergence_no_trade",
                (
                    "Toxic divergence no-trade: max_div=%.3f threshold=%.3f "
                    "(%d/%d ticks)"
                )
                % (
                    max_divergence,
                    toxic_divergence_threshold,
                    self._toxic_divergence_count,
                    toxic_divergence_ticks,
                ),
                cooldown=3.0,
            )
            if not has_inventory_for_unwind:
                if self.order_mgr.active_order_ids:
                    await self._cancel_all_guarded()
                self._current_quotes = {"up": (None, None), "dn": (None, None)}
                return

        # 4. Compute volatility
        vol = self.fair_value.realized_vol(klines)
        self.risk_mgr.record_vol(vol)

        # 5. Check risk limits
        # Compute session PnL using REAL PM balances (not internal inventory which may drift).
        # USDC balance is refreshed after order placement each tick, so it's always current.
        # Use BID prices for valuation — that's where we'd actually sell.
        _pm_up = getattr(st, "pm_up_bid", None) or (st.pm_up if hasattr(st, "pm_up") and st.pm_up else fv_up)
        _pm_dn = getattr(st, "pm_dn_bid", None) or (st.pm_dn if hasattr(st, "pm_dn") and st.pm_dn else fv_dn)
        _pos_value = self._cached_pm_up_shares * _pm_up + self._cached_pm_dn_shares * _pm_dn
        _current_portfolio = self._cached_usdc_balance + _pos_value
        _session_pnl = (_current_portfolio - self._starting_portfolio_pm) if self._starting_portfolio_pm > 0 else None

        # Grace period: skip PnL-based risk checks for first 30s while balances settle
        _in_grace = time.time() < self._pnl_grace_until
        _risk_pnl = None if _in_grace else _session_pnl
        effective_drawdown = self._effective_max_drawdown_usd()
        if _in_grace and self._tick_count % 10 == 0:
            log.info("PnL grace period active (%.0fs left), risk checks use fill-based PnL",
                     self._pnl_grace_until - time.time())

        should_pause, reason = self.risk_mgr.should_pause(
            self.inventory, vol, fv_up, fv_dn, session_pnl=_risk_pnl)
        if _risk_pnl is not None and effective_drawdown > 0 and _risk_pnl < -effective_drawdown:
            should_pause = True
            reason = f"Max drawdown exceeded: PnL=${_risk_pnl:.2f}"

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
                best_bid_up, best_bid_dn = await self._get_liq_lock_best_bids()
                self._liq_lock = self.risk_mgr.lock_pnl(
                    self.inventory, fv_up, fv_dn,
                    margin=self.config.liq_price_floor_margin,
                    best_bid_up=best_bid_up,
                    best_bid_dn=best_bid_dn,
                )
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
                await self._cancel_all_guarded()
                self._current_quotes = {"up": (None, None), "dn": (None, None)}
                return

        # Inventory limit: hard-stop and enter closing/liquidation mode.
        if should_pause and "Inventory limit" in reason:
            log.warning("Inventory hard-stop triggered: %s", reason)
            self._paused = True
            self._pause_reason = reason
            best_bid_up, best_bid_dn = await self._get_liq_lock_best_bids()
            self._liq_lock = self.risk_mgr.lock_pnl(
                self.inventory, fv_up, fv_dn,
                margin=self.config.liq_price_floor_margin,
                best_bid_up=best_bid_up,
                best_bid_dn=best_bid_dn,
            )
            self._liq_chunk_index = 0
            self._liq_last_chunk_time = 0.0
            self._is_closing = True
            self._closing_start_time_left = self.market.time_remaining
            self._merge_failed_this_cycle = False
            await self._cancel_all_guarded()
            self._current_quotes = {"up": (None, None), "dn": (None, None)}
            return

        if should_pause and not self._paused:
            self._paused = True
            self._pause_reason = reason
            await self._cancel_all_guarded()
            log.warning(f"MM PAUSED: {reason}")
            return
        elif (
            not should_pause
            and self._paused
            and not self._quality_pause_active
            and not self._critical_drift_pause_active
        ):
            self._paused = False
            self._pause_reason = ""
            log.info("MM RESUMED")

        if self._paused and not self._quality_pause_active:
            return

        # Reset catastrophic counter when PnL is healthy.
        # Only reset if we're NOT in the middle of a drawdown confirmation cycle.
        # The exit trigger block sets should_pause=False while waiting for confirmation,
        # so we check _session_pnl directly instead of relying on should_pause.
        if _session_pnl is not None and (_session_pnl > -effective_drawdown or effective_drawdown <= 0):
            if self._catastrophic_count > 0:
                log.info("PnL recovered ($%.2f), resetting catastrophic counter", _session_pnl)
            self._catastrophic_count = 0

        # ── One-sided exposure check ──────────────────────────────
        up_sh = self._cached_pm_up_shares if is_live else self.inventory.up_shares
        dn_sh = self._cached_pm_dn_shares if is_live else self.inventory.dn_shares
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
                best_bid_up, best_bid_dn = await self._get_liq_lock_best_bids()
                self._liq_lock = self.risk_mgr.lock_pnl(
                    self.inventory, fv_up, fv_dn,
                    margin=self.config.liq_price_floor_margin,
                    best_bid_up=best_bid_up,
                    best_bid_dn=best_bid_dn,
                )
                self._liq_chunk_index = 0
                self._liq_last_chunk_time = 0.0
                self._is_closing = True
                self._closing_start_time_left = time_left
                self._merge_failed_this_cycle = False
                await self._cancel_all_guarded()
                self._current_quotes = {"up": (None, None), "dn": (None, None)}
                return
        else:
            self._one_sided_counter = 0
        one_sided_protect_ticks = max(
            1,
            int(
                getattr(
                    self.config,
                    "one_sided_protect_ticks",
                    max(1, int(self.config.max_one_sided_ticks * 0.25)),
                )
                or max(1, int(self.config.max_one_sided_ticks * 0.25))
            ),
        )
        protect_one_sided_entries = bool(
            is_one_sided and self._one_sided_counter >= one_sided_protect_ticks
        )

        # ── Market quality check (every N ticks, live only) ─────────
        is_live = not hasattr(self.order_mgr.client, "_orders")
        should_check_quality = (
            is_live
            and (
                self._tick_count % self.config.quality_check_interval == 0
                or self._should_force_post_fill_quality_check()
            )
        )
        if should_check_quality:
            try:
                up_book, dn_book = await asyncio.gather(
                    self.order_mgr.get_full_book(self.market.up_token_id),
                    self.order_mgr.get_full_book(self.market.dn_token_id),
                )
                self._last_quality = self.quality_analyzer.analyze(
                    up_book, dn_book, fv_up, fv_dn)
                self._update_post_fill_entry_guard()
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
                    best_bid_up, best_bid_dn = await self._get_liq_lock_best_bids()
                    self._liq_lock = self.risk_mgr.lock_pnl(
                        self.inventory, fv_up, fv_dn,
                        margin=self.config.liq_price_floor_margin,
                        best_bid_up=best_bid_up,
                        best_bid_dn=best_bid_dn,
                    )
                    self._liq_chunk_index = 0
                    self._liq_last_chunk_time = 0.0
                    self._is_closing = True
                    self._closing_start_time_left = self.market.time_remaining
                    self._merge_failed_this_cycle = False
                    await self._cancel_all_guarded()
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
                        await self._cancel_all_guarded()
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
        if not bool(getattr(self.config, "paired_fill_ioc_enabled", False)):
            # Default: passive-only paired-filling management.
            self._imbalance_adjustments["force_taker_lagging"] = False
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

        # Update event-requote tracker with latest state, including fill triggers.
        self._event_requote_snapshot(had_fill=bool(fills))

        if fills and imbalance > 2.0:
            self._requote_event.set()

        leading_token: str | None = None
        lagging_token: str | None = None
        if self.inventory.up_shares > self.inventory.dn_shares:
            leading_token, lagging_token = "up", "dn"
        elif self.inventory.dn_shares > self.inventory.up_shares:
            leading_token, lagging_token = "dn", "up"

        # Entry settle window: skip initial quoting right after a new window opens.
        entry_settle_sec = max(0.0, float(getattr(self.config, "entry_settle_sec", 0.0)))
        if entry_settle_sec > 0 and time.time() < (self.market.window_start + entry_settle_sec):
            remaining = (self.market.window_start + entry_settle_sec) - time.time()
            if self._tick_count % 5 == 0:
                log.info(
                    "Entry settle active: waiting %.1fs before quoting this window",
                    max(0.0, remaining),
                )
            await self._cancel_all_guarded()
            self._current_quotes = {"up": (None, None), "dn": (None, None)}
            return

        # 6. Generate quotes (with USDC budget cap)
        order_collateral = sum(
            self.order_mgr.required_collateral(q)
            for q in self.order_mgr.active_orders.values()
            if q.side == "BUY"
        )
        all_quotes = self.quote_engine.generate_all_quotes(
            pm_mid_up, pm_mid_dn,
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
            min_price = max(tick_size, 0.01)
            max_price = min(0.99, 1.0 - tick_size)
            if max_price < min_price:
                max_price = min_price
            return max(min_price, min(max_price, rounded))

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
        if self._toxicity_spread_mult > 1.0:
            _apply_spread_multiplier("up", self._toxicity_spread_mult)
            _apply_spread_multiplier("dn", self._toxicity_spread_mult)
        if widen_spread_tick:
            _apply_spread_multiplier("up", 2.0)
            _apply_spread_multiplier("dn", 2.0)
        if leading_token and self._imbalance_adjustments.get("suppress_leading_buy", False):
            lead_bid, lead_ask = all_quotes[leading_token]
            all_quotes[leading_token] = (None, lead_ask)
        if protect_one_sided_entries and leading_token in ("up", "dn"):
            lead_bid, lead_ask = all_quotes[leading_token]
            if lead_bid is not None:
                self._throttled_warn(
                    "one_sided_entry_block",
                    (
                        f"One-sided protection: blocking {leading_token.upper()} BUY "
                        f"(ticks={self._one_sided_counter}, threshold={one_sided_protect_ticks})"
                    ),
                    cooldown=5.0,
                )
                all_quotes[leading_token] = (None, lead_ask)
        if toxic_entry_block:
            # In toxic regime, do not add new exposure; keep asks for inventory unwind.
            all_quotes["up"] = (None, all_quotes["up"][1])
            all_quotes["dn"] = (None, all_quotes["dn"][1])
        self._apply_post_fill_entry_buy_block(all_quotes)

        # 6a. Skip quoting sides where FV is too extreme (market already decided)
        min_fv = self.config.min_fv_to_quote
        if min_fv > 0:
            upper_fv = max(0.0, min(1.0, 1.0 - min_fv))
            if self._should_block_bid_for_extreme_fv(fv_up, min_fv) and all_quotes["up"][0] is not None:
                log.info(
                    "Skipping UP bid: FV=%.3f outside [%.3f, %.3f]",
                    fv_up,
                    min_fv,
                    upper_fv,
                )
                all_quotes["up"] = (None, all_quotes["up"][1])
            if self._should_block_bid_for_extreme_fv(fv_dn, min_fv) and all_quotes["dn"][0] is not None:
                log.info(
                    "Skipping DN bid: FV=%.3f outside [%.3f, %.3f]",
                    fv_dn,
                    min_fv,
                    upper_fv,
                )
                all_quotes["dn"] = (None, all_quotes["dn"][1])

        self._taker_quotes = []
        taker_lagging_key: str | None = None
        if (
            lagging_token
            and self._imbalance_adjustments.get("force_taker_lagging", False)
            and not bool(getattr(self.config, "use_post_only", True))
        ):
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
            try:
                up_summary, dn_summary = await asyncio.gather(
                    self.order_mgr.get_book_summary(self.market.up_token_id),
                    self.order_mgr.get_book_summary(self.market.dn_token_id),
                )
                books_cache["up"] = up_summary
                books_cache["dn"] = dn_summary
            except Exception as e:
                self.sre_metrics.record_api_call(False)
                log.error("Book fetch failed (prefetch): %s", e)
                await self._cancel_all_guarded()
                self._current_quotes = {"up": (None, None), "dn": (None, None)}
                return
        for token_key, token_id in [
            ("up", self.market.up_token_id),
            ("dn", self.market.dn_token_id),
        ]:
            ws_bid = st.pm_up_bid if token_key == "up" else st.pm_dn_bid
            ws_ask = st.pm_up if token_key == "up" else st.pm_dn
            if ws_fresh and ws_bid is not None and ws_ask is not None:
                book = {"best_bid": ws_bid, "best_ask": ws_ask}
            else:
                try:
                    book = (
                        books_cache[token_key]
                        if token_key in books_cache
                        else await self.order_mgr.get_book_summary(token_id)
                    )
                except Exception as e:
                    self.sre_metrics.record_api_call(False)
                    log.error("Book fetch failed for %s: %s", token_key.upper(), e)
                    await self._cancel_all_guarded()
                    self._current_quotes = {"up": (None, None), "dn": (None, None)}
                    return
            bid, ask = all_quotes[token_key]
            bid, ask = self.quote_engine.clamp_to_book(
                bid, ask, book["best_bid"], book["best_ask"],
                tick_size=self.market.tick_size,
            )
            if taker_lagging_key == token_key and bid is not None and book["best_ask"] is not None:
                # Force taker cross on lagging side to accelerate balancing fills.
                bid.price = _round_to_tick(float(book["best_ask"]))
            elif self._toxicity_mode == "high":
                # Toxic flow defense: stand one tick off BBO instead of queue-front.
                if bid is not None and book["best_bid"] is not None:
                    safer_bid = _round_to_tick(float(book["best_bid"]) - tick_size)
                    bid.price = min(bid.price, safer_bid)
                if ask is not None and book["best_ask"] is not None:
                    safer_ask = _round_to_tick(float(book["best_ask"]) + tick_size)
                    ask.price = max(ask.price, safer_ask)
            # Re-apply max inventory cap after clamping (safety net)
            max_sh = self.config.max_inventory_shares
            if bid is not None and bid.size > max_sh:
                bid.size = round(max_sh, 2)
            if ask is not None and ask.size > max_sh:
                ask.size = round(max_sh, 2)
            all_quotes[token_key] = (bid, ask)

        self._enforce_session_exposure_cap(
            all_quotes,
            pm_up_price=_pm_up,
            pm_dn_price=_pm_dn,
        )

        await self._apply_rebate_scoring_filters(all_quotes, is_live=is_live)
        guarded_tokens = self._active_settlement_guard_tokens()
        if guarded_tokens:
            for token_key, token_id in (
                ("up", self.market.up_token_id),
                ("dn", self.market.dn_token_id),
            ):
                if token_id in guarded_tokens:
                    if all_quotes[token_key] != (None, None):
                        self._throttled_warn(
                            f"quote_settlement_guard:{token_id}",
                            f"Settlement lag guard: suppressing {token_key.upper()} quotes until PM balances catch up",
                            cooldown=2.0,
                        )
                    all_quotes[token_key] = (None, None)
        _t_quotes = time.perf_counter()

        # Abort if emergency shutdown was triggered during this tick
        if self._emergency_flag:
            return

        # 7. Place or update orders — parallel cancel+place across UP and DN
        # Collect all operations first, then execute cancels→places in parallel
        # to cut tick latency in half (was sequential UP then DN).
        pending_cancels: list[str] = []
        pending_places: list[tuple[Quote, bool | None, bool, bool]] = []  # (quote, post_only, fallback_taker, ioc_like)
        pending_updates: dict[str, tuple] = {}  # token_key -> (bid, ask)

        for token_key in ("up", "dn"):
            new_bid, new_ask = all_quotes[token_key]
            cur_bid, cur_ask = self._current_quotes.get(token_key, (None, None))
            use_taker_bid = bool(new_bid is not None and any(q is new_bid for q in self._taker_quotes))
            use_ioc_like_bid = bool(use_taker_bid and getattr(self.config, "paired_fill_ioc_enabled", False))
            pm_min_size = float(self.market.min_order_size) if self.market else 5.0

            if new_bid is not None and 0 < new_bid.size < pm_min_size:
                new_bid.size = round(pm_min_size, 2)

            need_bid = self.quote_engine.should_requote(cur_bid, new_bid)
            if use_taker_bid and new_bid is not None:
                need_bid = True
            bid_size = new_bid.size if new_bid else 0.0
            bid_notional = (new_bid.size * new_bid.price) if new_bid else 0.0
            if need_bid and (bid_notional < self.config.min_order_size_usd or bid_size < pm_min_size):
                need_bid = False

            stale_bid = (not need_bid and cur_bid and cur_bid.order_id
                         and (new_bid is None
                              or bid_notional < self.config.min_order_size_usd
                              or bid_size < pm_min_size))

            if is_live:
                token_bal = (
                    self._cached_pm_up_shares
                    if token_key == "up"
                    else self._cached_pm_dn_shares
                )
            else:
                token_bal = (
                    self.inventory.up_shares
                    if token_key == "up"
                    else self.inventory.dn_shares
                )
            token_bal = max(0.0, float(token_bal))
            if new_ask and token_bal > 0:
                new_ask.size = round(min(new_ask.size, token_bal), 2)
            if new_ask and 0 < new_ask.size < pm_min_size and token_bal >= pm_min_size:
                new_ask.size = round(pm_min_size, 2)
            ask_size = new_ask.size if new_ask else 0.0
            ask_price = new_ask.price if new_ask else 0.0
            ask_notional = ask_size * ask_price
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

            if need_ask and (ask_notional < self.config.min_order_size_usd or ask_size < pm_min_size):
                need_ask = False
            stale_ask = (cur_ask and cur_ask.order_id
                         and (token_bal <= 0 or oversized_live_ask
                              or new_ask is None
                              or ask_size < pm_min_size
                              or ask_notional < self.config.min_order_size_usd)
                         and not need_ask)

            if need_bid or need_ask or stale_bid or stale_ask:
                self._requote_count += 1
                if (need_bid or stale_bid) and cur_bid and cur_bid.order_id:
                    pending_cancels.append(cur_bid.order_id)
                if (need_ask or stale_ask) and cur_ask and cur_ask.order_id:
                    pending_cancels.append(cur_ask.order_id)

                if need_bid and new_bid is not None:
                    po = False if use_taker_bid else None
                    ft = True if use_taker_bid else False
                    pending_places.append((new_bid, po, ft, use_ioc_like_bid))
                if need_ask and new_ask is not None:
                    pending_places.append((new_ask, None, False, False))

                updated_bid = (
                    None
                    if (need_bid and use_ioc_like_bid)
                    else (new_bid if need_bid else (None if stale_bid else cur_bid))
                )
                updated_ask = new_ask if need_ask else (None if stale_ask else cur_ask)
                pending_updates[token_key] = (updated_bid, updated_ask)

        if pending_cancels or pending_places:
            async with self._order_ops_lock:
                if self._emergency_flag:
                    return

                # Cancel old orders via batch API.
                if pending_cancels:
                    pending_cancels = list(dict.fromkeys(oid for oid in pending_cancels if oid))
                    cancelled = await self.order_mgr.cancel_orders_batch(pending_cancels)
                    self.sre_metrics.record_api_call(cancelled == len(pending_cancels))
                    if self._emergency_flag:
                        return
                    if cancelled < len(pending_cancels):
                        failed = len(pending_cancels) - cancelled
                        log.warning(
                            "Batch cancel: %d/%d failed, skipping placements",
                            failed, len(pending_cancels),
                        )
                        pending_places.clear()
                        pending_updates.clear()

                # Place new orders safely:
                # - maker orders via batch API (fewer HTTP calls),
                # - taker/special orders sequentially (avoid budget races).
                if pending_places:
                    maker_batch_quotes: list[Quote] = []
                    sequential_places: list[tuple[Quote, bool | None, bool, bool]] = []
                    for q, po, ft, ioc_like in pending_places:
                        if (po is None or po is True) and not ft and not ioc_like:
                            maker_batch_quotes.append(q)
                        else:
                            sequential_places.append((q, po, ft, ioc_like))

                    place_failed = 0
                    place_total = 0

                    if maker_batch_quotes:
                        batch_ids = await self.order_mgr.place_orders_batch(
                            maker_batch_quotes,
                            post_only=self.config.use_post_only,
                        )
                        place_total += len(batch_ids)
                        place_failed += sum(1 for oid in batch_ids if not oid)
                        if self._emergency_flag:
                            return

                    for q, po, ft, ioc_like in sequential_places:
                        oid = await self.order_mgr.place_order(q, post_only=po, fallback_taker=ft)
                        place_total += 1
                        if self._emergency_flag:
                            return
                        if not oid:
                            place_failed += 1
                            continue
                        if ioc_like:
                            # IOC-like behavior: taker-second-leg should not rest in book.
                            try:
                                await self.order_mgr.cancel_order(oid)
                            except Exception:
                                pass
                            if self._emergency_flag:
                                return

                    if place_total > 0:
                        self.sre_metrics.record_api_call(place_failed == 0)

                    if place_failed:
                        log.warning("Order placement: %d/%d orders failed", place_failed, place_total)

                # Update quote tracking while order lock is held to avoid state races.
                for tk, (bid, ask) in pending_updates.items():
                    self._current_quotes[tk] = (bid, ask)
        else:
            for tk, (bid, ask) in pending_updates.items():
                self._current_quotes[tk] = (bid, ask)

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
        if is_live and (fills or self._tick_count % 15 == 0):
            try:
                (real_up, real_dn), usdc_pair = await asyncio.gather(
                    self.order_mgr.get_all_token_balances(
                        self.market.up_token_id,
                        self.market.dn_token_id,
                        reference_balances=self._balance_reference_snapshot(),
                    ),
                    self.order_mgr.get_usdc_balances(),
                )
                usdc_bal, usdc_available = usdc_pair
                if usdc_bal is not None:
                    self._cached_usdc_balance = usdc_bal
                    self.inventory.usdc = usdc_bal
                if usdc_available is not None:
                    self._cached_usdc_available_balance = usdc_available
                if real_up is not None:
                    self._cached_pm_up_shares = real_up
                if real_dn is not None:
                    self._cached_pm_dn_shares = real_dn
                if real_up is not None and real_dn is not None:
                    real_up = float(real_up)
                    real_dn = float(real_dn)
                    protected_tokens, has_unexplained_critical_drift = self._classify_drift_snapshot(
                        real_up=real_up,
                        real_dn=real_dn,
                        source="post_order_refresh",
                    )
                    if protected_tokens and not has_unexplained_critical_drift:
                        await self._activate_settlement_guard(
                            protected_tokens=protected_tokens,
                            real_up=real_up,
                            real_dn=real_dn,
                            source="post_order_refresh",
                        )
                    else:
                        if has_unexplained_critical_drift:
                            real_up, real_dn, drift_confirmed = await self._confirm_critical_drift_snapshot(
                                real_up=real_up,
                                real_dn=real_dn,
                                source="post_order_refresh",
                            )
                            self._cached_pm_up_shares = real_up
                            self._cached_pm_dn_shares = real_dn
                            if drift_confirmed:
                                if await self._handle_critical_inventory_drift(
                                    real_up=real_up,
                                    real_dn=real_dn,
                                    source="post_order_refresh",
                                ):
                                    return
                    self._update_settlement_lag_progress(
                        real_up=real_up,
                        real_dn=real_dn,
                        source="post_order_refresh",
                    )
                    self._update_critical_drift_recovery(
                        real_up=real_up,
                        real_dn=real_dn,
                        source="post_order_refresh",
                    )
                    if self._critical_drift_pause_active:
                        self._current_quotes = {"up": (None, None), "dn": (None, None)}
                        return
            except Exception as e:
                log.warning("Post-order balance refresh failed: %s", e)
        elif not is_live:
            # Paper mode: sync from mock client balance + internal inventory
            self._cached_usdc_balance = self.order_mgr.client.get_balance()
            self._cached_usdc_available_balance = self._cached_usdc_balance
            self.inventory.usdc = self._cached_usdc_balance
            self._cached_pm_up_shares = self.inventory.up_shares
            self._cached_pm_dn_shares = self.inventory.dn_shares

        # 9. Latency metrics
        total_ms = (_t_orders - _t0) * 1000
        self._last_tick_ms = total_ms
        self._last_book_ms = (_t_quotes - _t_reconcile) * 1000  # fv + risk + quotes + book clamp
        self._last_order_ms = (_t_orders - _t_quotes) * 1000
        self._last_fills_ms = (_t_fills - _t0) * 1000
        self._last_reconcile_ms = (_t_reconcile - _t_fills) * 1000
        self._last_fv_ms = (_t_fv - _t_fv_start) * 1000
        self._last_quotes_ms = (_t_quotes - _t_fv) * 1000
        self._last_orders_ms = (_t_orders - _t_quotes) * 1000
        self.sre_metrics.record_fill_check(self._last_fills_ms)
        self.sre_metrics.record_order_rtt(self._last_orders_ms)
        self._tick_ms_samples.append(total_ms)
        if len(self._tick_ms_samples) > 100:
            self._tick_ms_samples = self._tick_ms_samples[-50:]
        self._avg_tick_ms = sum(self._tick_ms_samples) / len(self._tick_ms_samples)
        self.sre_metrics.record_tick(self._last_tick_ms)

        if self._tick_count % 10 == 0:
            log.info(
                "TICK latency: fills=%.0fms reconcile=%.0fms fv=%.0fms "
                "quotes+book=%.0fms orders=%.0fms total=%.0fms",
                (_t_fills - _t0) * 1000,
                (_t_reconcile - _t_fills) * 1000,
                self._last_fv_ms,
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

        pm_up_bid = float(getattr(st, "pm_up_bid", 0.0) or 0.0)
        pm_up_ask = float(getattr(st, "pm_up", 0.0) or 0.0)
        pm_dn_bid = float(getattr(st, "pm_dn_bid", 0.0) or 0.0)
        pm_dn_ask = float(getattr(st, "pm_dn", 0.0) or 0.0)
        pm_up = ((pm_up_bid + pm_up_ask) / 2.0) if (pm_up_bid > 0 and pm_up_ask > 0) else pm_up_ask
        pm_dn = ((pm_dn_bid + pm_dn_ask) / 2.0) if (pm_dn_bid > 0 and pm_dn_ask > 0) else pm_dn_ask
        pm_last_update = float(getattr(st, "pm_last_update_ts", 0.0) or 0.0)
        pm_age_sec = (time.time() - pm_last_update) if pm_last_update > 0 else 999.0

        return self.fair_value.compute_with_pm_anchor(
            mid, self.market.strike,
            self.market.time_remaining, klines,
            pm_up=pm_up,
            pm_dn=pm_dn,
            pm_age_sec=pm_age_sec,
            bids=bids, asks=asks, trades=trades,
        )

    async def _get_liq_lock_best_bids(self) -> tuple[float | None, float | None]:
        """Fetch current best bids for both tokens to derive liquidation floors."""
        if not self.market:
            return None, None
        try:
            up_book, dn_book = await asyncio.gather(
                self.order_mgr.get_book_summary(self.market.up_token_id),
                self.order_mgr.get_book_summary(self.market.dn_token_id),
            )
            return up_book.get("best_bid"), dn_book.get("best_bid")
        except Exception as e:
            log.warning("Failed to fetch best bids for liquidation lock: %s", e)
            return None, None

    async def _try_merge_pairs(self) -> float:
        """Attempt to merge paired UP+DN inventory.

        Returns profit from merge (0.0 if no merge done).
        """
        if not self.market or self._is_closing:
            return 0.0

        # Update paired inventory tracking
        self.inventory.paired.update(
            self.inventory.up_shares,
            self.inventory.dn_shares,
            self.inventory.up_cost.avg_entry_price,
            self.inventory.dn_cost.avg_entry_price,
        )

        q_pair = self.inventory.paired.q_pair
        if q_pair < 1.0:  # Need at least 1 pair
            return 0.0

        merge_amount = int(q_pair)  # Merge whole units only
        if merge_amount < 1:
            return 0.0

        if await self._should_prefer_pair_sell_over_merge(merge_amount):
            self._requote_event.set()
            return 0.0

        # Only merge if profitable (cost < $1.00 per pair)
        if self.inventory.paired.pair_profit_per_unit <= 0:
            log.info(
                "Merge skip: pair cost %.4f >= $1.00",
                self.inventory.paired.total_pair_cost / max(q_pair, 0.01),
            )
            return 0.0

        expected_profit = merge_amount * self.inventory.paired.pair_profit_per_unit
        log.info("Merge attempt: %d pairs, expected profit $%.4f", merge_amount, expected_profit)

        try:
            # Use the existing merge_positions method in order_manager
            result = await self.order_mgr.merge_positions(
                condition_id=self.market.condition_id,
                amount_shares=merge_amount,
                private_key="",  # OrderManager handles key internally
            )
            if result.get("success"):
                self.inventory.paired.record_merge(merge_amount, expected_profit)
                merge_total_cost = max(0.0, merge_amount - expected_profit)
                self.pnl_decomp.record_merge(merge_amount, merge_total_cost)
                # Update inventory — merge removes both UP and DN shares, adds USDC.
                self.inventory.up_shares = max(0.0, self.inventory.up_shares - merge_amount)
                self.inventory.dn_shares = max(0.0, self.inventory.dn_shares - merge_amount)
                self.inventory.usdc += merge_amount  # $1 per pair
                self.inventory.up_cost.record_sell(merge_amount)
                self.inventory.dn_cost.record_sell(merge_amount)
                # Keep paper-mode balances in sync with merged inventory.
                if hasattr(self.order_mgr.client, "_orders"):
                    for tid in [self.market.up_token_id, self.market.dn_token_id]:
                        cur = self.order_mgr._mock_token_balances.get(tid, 0.0)
                        self.order_mgr._mock_token_balances[tid] = max(0.0, cur - merge_amount)
                log.info(
                    "Merge SUCCESS: %d pairs -> $%d USDC, profit $%.4f",
                    merge_amount,
                    merge_amount,
                    expected_profit,
                )
                return expected_profit
            log.warning("Merge failed: %s", result.get("error", "unknown"))
            self._merge_failed_this_cycle = True
            return 0.0
        except Exception as e:
            log.error("Merge exception: %s", e)
            self._merge_failed_this_cycle = True
            return 0.0

    async def _liquidate_inventory(self) -> None:
        """Smart liquidation with merge + 3-phase SELL exit.

        Phase 0 (Merge): Merge YES+NO pairs → $1 USDC via CTF contract.
            - Instant, no slippage, only gas cost.
        Phase 1 (Gradual Limit): time_left > taker_threshold
            - Sell in chunks (position / remaining_chunks)
            - Price = max(floor, FV - discount), improved to best_bid if higher
            - Post-only (maker)
        Phase 2 (Taker): time_left <= taker_threshold
            - Sell everything at best_bid (fail-closed), including below floor
        Phase 3 (Abandon): disabled
            - We do not intentionally hold risk in liquidation mode.
        """
        if not self.market:
            return

        cfg = self.config
        time_left = self.market.time_remaining
        min_attempt_interval = (
            0.5
            if time_left <= float(getattr(cfg, "liq_taker_threshold_sec", 10.0))
            else max(1.0, min(float(getattr(cfg, "liq_chunk_interval_sec", 5.0)), 3.0))
        )
        now = time.time()
        if self._liq_last_attempt_time > 0 and (now - self._liq_last_attempt_time) < min_attempt_interval:
            return
        self._liq_last_attempt_time = now

        # ── Phase 0: Merge YES+NO pairs → USDC ──────────────────
        up_bal = await self.order_mgr.get_token_balance(self.market.up_token_id)
        dn_bal = await self.order_mgr.get_token_balance(self.market.dn_token_id)
        if up_bal is None or dn_bal is None:
            log.error("Liquidation: failed to fetch token balances; retrying next chunk")
            return
        # Keep PM-cache inventory in sync during closing; regular reconcile is disabled in _is_closing.
        self._cached_pm_up_shares = max(0.0, up_bal)
        self._cached_pm_dn_shares = max(0.0, dn_bal)
        merge_amount = min(up_bal, dn_bal)

        # Merge first when both sides have meaningful size, then liquidate only leftovers.
        if merge_amount > 0.5 and self.market.condition_id and not self._merge_failed_this_cycle:
            try:
                result = await self.order_mgr.merge_positions(
                    self.market.condition_id, merge_amount, self._private_key)
            except Exception as e:
                log.warning("Merge exception: %s", e)
                result = {"success": False, "error": str(e)}
            if result.get("success"):
                pre_up_avg = self.inventory.up_cost.avg_entry_price
                pre_dn_avg = self.inventory.dn_cost.avg_entry_price
                merge_total_cost = merge_amount * max(0.0, pre_up_avg + pre_dn_avg)
                self.pnl_decomp.record_merge(merge_amount, merge_total_cost)
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
                # Keep local phase balances in sync to avoid double-merge in dust preflight.
                up_bal = max(0.0, up_bal - merge_amount)
                dn_bal = max(0.0, dn_bal - merge_amount)
                self._cached_pm_up_shares = up_bal
                self._cached_pm_dn_shares = dn_bal
            else:
                self._merge_failed_this_cycle = True
                log.warning("Merge failed (will not retry this cycle): %s",
                            result.get("error", "unknown"))

        taker_threshold = cfg.liq_taker_threshold_sec
        use_taker = time_left <= taker_threshold
        # Emergency drawdown check during liquidation
        # Use real PM balances (not internal inventory) and starting portfolio (with tokens)
        if self._starting_portfolio_pm > 0 and self._cached_usdc_balance > 0:
            _pm_up = getattr(self.feed_state, "pm_up_bid", None) or (self.feed_state.pm_up if hasattr(self.feed_state, "pm_up") else 0.5)
            _pm_dn = getattr(self.feed_state, "pm_dn_bid", None) or (self.feed_state.pm_dn if hasattr(self.feed_state, "pm_dn") else 0.5)
            _pos_val = self._cached_pm_up_shares * _pm_up + self._cached_pm_dn_shares * _pm_dn
            _liq_pnl = (self._cached_usdc_balance + _pos_val) - self._starting_portfolio_pm
            effective_drawdown = self._effective_max_drawdown_usd()
            catastrophic_limit = -effective_drawdown if effective_drawdown > 0 else float("-inf")
            if _liq_pnl < catastrophic_limit:
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
                        fresh_usdc, fresh_usdc_available = await self.order_mgr.get_usdc_balances()
                        (fresh_up, fresh_dn) = await self.order_mgr.get_all_token_balances(
                            self.market.up_token_id,
                            self.market.dn_token_id,
                            reference_balances=self._balance_reference_snapshot(),
                        )
                        if fresh_usdc is not None:
                            self._cached_usdc_balance = fresh_usdc
                            self.inventory.usdc = fresh_usdc
                        if fresh_usdc_available is not None:
                            self._cached_usdc_available_balance = fresh_usdc_available
                        if fresh_up is not None:
                            self._cached_pm_up_shares = fresh_up
                        if fresh_dn is not None:
                            self._cached_pm_dn_shares = fresh_dn
                        _fresh_pos = (self._cached_pm_up_shares * _pm_up +
                                      self._cached_pm_dn_shares * _pm_dn)
                        _fresh_pnl = (self._cached_usdc_balance + _fresh_pos) - self._starting_portfolio_pm
                        log.warning("CATASTROPHIC fresh recheck: sPnL=$%.2f (was $%.2f)", _fresh_pnl, _liq_pnl)
                        if _fresh_pnl < catastrophic_limit:
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
            if effective_drawdown > 0 and _liq_pnl < -effective_drawdown and not use_taker:
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
                await self._cancel_order_guarded(oid)
            self._liquidation_order_ids.clear()

        # 3. If limit orders are still live, wait for chunk interval then cancel & re-place
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
                await self._cancel_order_guarded(oid)
            self._liquidation_order_ids.clear()

        # 5. Pre-flight: ensure allowance (one-time, cached)
        is_live = not hasattr(self.order_mgr.client, "_orders")
        if is_live:
            await self.order_mgr.ensure_sell_allowance(
                self.market.up_token_id,
                required_shares=max(0.0, float(up_bal)),
            )
            await self.order_mgr.ensure_sell_allowance(
                self.market.dn_token_id,
                required_shares=max(0.0, float(dn_bal)),
            )

        # 6. Compute fair values for pricing
        fv_up, fv_dn = self._compute_fv()

        # ── Pre-flight dust check: if both sides < PM min, try merge dust then skip ──
        pm_min_pf = self.market.min_order_size if self.market else 5.0
        if up_bal < pm_min_pf and dn_bal < pm_min_pf and (up_bal > 0.1 or dn_bal > 0.1):
            # Try merging dust pairs first (if both > 0)
            dust_merge = min(up_bal, dn_bal)
            # Keep dust merge threshold aligned with regular merge logic (>= 1 whole pair).
            if dust_merge >= 1.0 and self.market.condition_id and not self._merge_failed_this_cycle:
                try:
                    r = await self.order_mgr.merge_positions(
                        self.market.condition_id, dust_merge, self._private_key)
                    if r.get("success"):
                        log.info("Dust merge: %.2f pairs → $%.2f USDC", dust_merge, dust_merge)
                        self.inventory.up_shares = max(0.0, self.inventory.up_shares - dust_merge)
                        self.inventory.dn_shares = max(0.0, self.inventory.dn_shares - dust_merge)
                        self.inventory.usdc += dust_merge
                        pre_up_avg = self.inventory.up_cost.avg_entry_price
                        pre_dn_avg = self.inventory.dn_cost.avg_entry_price
                        merge_total_cost = dust_merge * max(0.0, pre_up_avg + pre_dn_avg)
                        self.pnl_decomp.record_merge(dust_merge, merge_total_cost)
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
            # Keep PM-cache inventory fresh even when balance is tiny/zero.
            if token_id == self.market.up_token_id:
                self._cached_pm_up_shares = max(0.0, real_balance)
            else:
                self._cached_pm_dn_shares = max(0.0, real_balance)
            if real_balance <= completion_threshold:
                log.info(f"No real balance for {label}")
                continue

            has_real_balance = True
            is_up = token_id == self.market.up_token_id
            fv = fv_up if is_up else fv_dn
            book = await self.order_mgr.get_book_summary(token_id)
            best_bid = book["best_bid"]
            best_ask = book.get("best_ask")
            if best_bid is None or best_bid <= 0:
                log.warning("no liquidity for %s", label)
                continue
            tick_size = max(0.001, float(self.market.tick_size if self.market else 0.01))

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
                from .pm_fees import taker_fee_usd
                taker_fee = taker_fee_usd(floor, 1.0, "SELL", token_id=token_id)
                taker_fee_ratio = taker_fee / max(floor, 1e-9)
                floor = floor * (1.0 + taker_fee_ratio)

            if use_taker:
                # ── Phase 2: Taker ──
                if best_bid < floor:
                    # Fail-closed liquidation: never hold inventory by floor logic.
                    # In closing mode we prioritize risk reduction over entry-floor protection.
                    log.warning(
                        f"{label}: best_bid={best_bid:.2f} < floor={floor:.2f} — "
                        f"forced taker exit ({time_left:.0f}s left, {real_balance:.1f} shares)"
                    )
                    phase = "FORCE_TAKER_BELOW_FLOOR"
                else:
                    phase = "TAKER"
                sell_price = best_bid
                post_only = False
                # Sell everything remaining (no buffer in taker mode)
                sell_size = round(real_balance, 2)
            else:
                # ── Phase 1: Gradual Limit ──
                # Price: max(floor, FV - discount), improve to best_bid if higher
                sell_price = max(floor, fv - cfg.liq_max_discount_from_fv)
                if best_bid and best_bid > sell_price:
                    resting_min = best_bid + tick_size
                    if best_ask is not None and best_ask > 0:
                        sell_price = max(resting_min, min(best_ask, sell_price))
                        if sell_price < resting_min:
                            sell_price = resting_min
                    else:
                        sell_price = max(resting_min, sell_price)

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

            quantized = round(round(float(sell_price) / tick_size) * tick_size, 10)
            min_price = max(0.01, tick_size)
            max_price = min(0.99, 1.0 - tick_size)
            if max_price < min_price:
                max_price = min_price
            sell_price = max(min_price, min(max_price, quantized))

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
            order_id = await self._place_order_guarded(sell_quote, post_only=post_only, fallback_taker=False)
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
                reference_balances=self._balance_reference_snapshot(),
            )
            if real_up_chk is None or real_dn_chk is None:
                log.warning("Liquidation completion check deferred: failed to fetch PM balances")
                return

            self._cached_pm_up_shares = max(0.0, real_up_chk)
            self._cached_pm_dn_shares = max(0.0, real_dn_chk)
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

            if real_clear:
                if not internal_clear:
                    log.warning(
                        "Liquidation complete by PM balance, forcing internal reconcile to zero: "
                        "internal UP=%.2f DN=%.2f, PM UP=%.2f DN=%.2f",
                        internal_up,
                        internal_dn,
                        real_up_chk,
                        real_dn_chk,
                    )
                    self.inventory.reconcile(0.0, 0.0, self._cached_usdc_balance)
                if self._liquidation_order_ids:
                    for oid in list(self._liquidation_order_ids):
                        await self._cancel_order_guarded(oid)
                    self._liquidation_order_ids.clear()
                log.info("Liquidation complete — PM inventory cleared")
                self._maybe_exit_inventory_close_mode_after_clear()
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

    async def redeem_after_resolution(self, max_wait_sec: float | None = None) -> dict:
        """Best-effort winner token redemption after market resolution."""
        if hasattr(self.order_mgr.client, "_orders"):
            return {"success": False, "error": "paper mode"}
        if not self.market or not self.market.condition_id:
            return {"success": False, "error": "missing condition_id"}
        if not self._private_key:
            return {"success": False, "error": "missing private key"}

        timeout_sec = (
            float(max_wait_sec)
            if max_wait_sec is not None
            else float(getattr(self.config, "resolution_wait_sec", 90.0))
        )
        retry_interval = max(2.0, float(getattr(self.config, "redeem_retry_interval_sec", 15.0)))
        deadline = time.time() + max(0.0, timeout_sec)
        attempts = 0
        last_result: dict[str, Any] = {"success": False, "error": "not_attempted"}

        while True:
            attempts += 1
            try:
                last_result = await self.order_mgr.redeem_positions(
                    condition_id=self.market.condition_id,
                    private_key=self._private_key,
                )
            except Exception as e:
                last_result = {"success": False, "error": str(e)}

            if last_result.get("success"):
                self.order_mgr.invalidate_usdc_cache()
                log.info(
                    "Redeem success for condition %s... (attempt %d)",
                    self.market.condition_id[:12],
                    attempts,
                )
                return {**last_result, "attempts": attempts}

            err = str(last_result.get("error", "")).lower()
            terminal = (
                "funder mode" in err
                or "missing private key" in err
                or "missing condition_id" in err
                or "not supported" in err
            )
            if terminal or time.time() >= deadline:
                break
            await asyncio.sleep(retry_interval)

        log.warning(
            "Redeem not completed for condition %s... after %d attempt(s): %s",
            self.market.condition_id[:12],
            attempts,
            last_result.get("error", "unknown"),
        )
        return {**last_result, "attempts": attempts}

    async def on_window_transition(self, new_market: MarketInfo) -> None:
        """Handle window transition — cancel all, switch tokens, resume."""
        log.info(f"Window transition: {new_market.coin} {new_market.timeframe}")

        # Cancel all existing orders
        await self._cancel_all_guarded()
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
        self._merge_check_counter = 0
        self._reconcile_prev_pm = None
        self._reconcile_stable_count = 0
        self._reconcile_guard_until = 0.0
        self._critical_drift_pause_active = False
        self._critical_drift_recovery_streak = 0
        self._settlement_lag = {}
        self._settlement_guard_tokens = set()
        self._post_fill_entry_guard_until = 0.0
        self._post_fill_entry_guard_anchor = None
        self._post_fill_entry_guard_active = False
        self._post_fill_entry_guard_reason = ""
        self._post_fill_entry_guard_trigger_count = 0
        self._liquidation_order_ids.clear()
        self._one_sided_counter = 0
        self._toxicity_spread_mult = 1.0
        self._toxicity_mode = "normal"
        self._toxic_divergence_count = 0
        self.event_requoter = EventRequoter(
            pm_mid_threshold_bps=self.config.event_pm_mid_threshold_bps,
            binance_threshold_bps=self.config.event_binance_threshold_bps,
            fallback_interval_sec=self.config.event_fallback_interval_sec,
        )
        self._last_requote_events = []
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
        await self._refresh_fee_rate_cache()

    def snapshot(self) -> dict:
        """Get current state for dashboard API."""
        st = self.feed_state

        # Fair value
        fv_up, fv_dn = 0.5, 0.5
        vol = 0.0
        if self.market and st.mid and st.mid > 0:
            klines = list(st.klines) if st.klines else []
            try:
                fv_up, fv_dn = self._compute_fv()
                vol = self.fair_value.realized_vol(klines)
            except Exception:
                pass

        pm_up_bid = getattr(st, "pm_up_bid", None)
        pm_up_ask = getattr(st, "pm_up", None)
        pm_dn_bid = getattr(st, "pm_dn_bid", None)
        pm_dn_ask = getattr(st, "pm_dn", None)

        if pm_up_bid is not None and pm_up_ask is not None and pm_up_bid > 0 and pm_up_ask > 0:
            pm_mid_up = (pm_up_bid + pm_up_ask) / 2.0
        else:
            pm_mid_up = fv_up

        if pm_dn_bid is not None and pm_dn_ask is not None and pm_dn_bid > 0 and pm_dn_ask > 0:
            pm_mid_dn = (pm_dn_bid + pm_dn_ask) / 2.0
        else:
            pm_mid_dn = fv_dn

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
        # Use BID prices — that's the exit price for long positions (conservative valuation).
        _pm_up_price = getattr(st, "pm_up_bid", None) or (st.pm_up if hasattr(st, "pm_up") and st.pm_up else fv_up)
        _pm_dn_price = getattr(st, "pm_dn_bid", None) or (st.pm_dn if hasattr(st, "pm_dn") and st.pm_dn else fv_dn)
        _position_value = (self._cached_pm_up_shares * _pm_up_price +
                           self._cached_pm_dn_shares * _pm_dn_price)
        _current_portfolio = self._cached_usdc_balance + _position_value
        _session_pnl = _current_portfolio - self._starting_portfolio_pm if self._starting_portfolio_pm > 0 else 0.0
        inventory_unrealized = self.pnl_decomp.update_inventory_cost(
            self.inventory.up_shares,
            self.inventory.up_cost.avg_entry_price,
            _pm_up_price,
            self.inventory.dn_shares,
            self.inventory.dn_cost.avg_entry_price,
            _pm_dn_price,
        )
        is_live = not hasattr(self.order_mgr.client, "_orders")
        inv_up_display = self._cached_pm_up_shares if is_live else self.inventory.up_shares
        inv_dn_display = self._cached_pm_dn_shares if is_live else self.inventory.dn_shares
        inv_usdc_total = self._cached_usdc_balance if is_live else self.inventory.usdc
        inv_net_delta_display = inv_up_display - inv_dn_display
        up_drift = abs(inv_up_display - self.inventory.up_shares)
        dn_drift = abs(inv_dn_display - self.inventory.dn_shares)
        up_avg_entry_display = (
            round(self.inventory.up_cost.avg_entry_price, 4)
            if inv_up_display > 0 and up_drift <= 0.25
            else None
        )
        dn_avg_entry_display = (
            round(self.inventory.dn_cost.avg_entry_price, 4)
            if inv_dn_display > 0 and dn_drift <= 0.25
            else None
        )
        token_balances = {}
        if self.market:
            token_balances[self.market.up_token_id] = max(0.0, inv_up_display)
            token_balances[self.market.dn_token_id] = max(0.0, inv_dn_display)
        reserved_est = self.order_mgr.estimate_reserved_collateral(token_balances)
        if self._cached_usdc_available_balance is not None:
            usdc_free_pm = max(0.0, float(self._cached_usdc_available_balance))
            reserved_collateral_pm = max(0.0, inv_usdc_total - usdc_free_pm)
            usdc_free_source = "pm_api_available"
        else:
            reserved_collateral_pm = max(0.0, reserved_est.get("total_reserved", 0.0))
            usdc_free_pm = max(0.0, inv_usdc_total - reserved_collateral_pm)
            usdc_free_source = "estimated_from_active_orders"
        active_settlement_tokens = self._active_settlement_guard_tokens()
        settlement_guard_tokens = []
        for token_id in sorted(active_settlement_tokens):
            state = self._settlement_lag.get(token_id)
            if state is None:
                continue
            token_key = self._token_key_for_id(token_id)
            settlement_guard_tokens.append(
                {
                    "token": (token_key or token_id).upper(),
                    "token_id": token_id,
                    "pending_delta_shares": round(state.pending_delta_shares, 4),
                    "seconds_left": round(max(0.0, state.grace_until - time.time()), 2),
                    "last_fill_side": state.last_fill_side,
                    "last_fill_size": round(state.last_fill_size, 4),
                    "internal_shares": round(state.last_internal_shares, 4),
                    "pm_shares": round(state.last_pm_shares, 4),
                    "source": state.source,
                }
            )

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
                "model_fv_up": round(fv_up, 4),
                "model_fv_dn": round(fv_dn, 4),
                "pm_mid_up": round(pm_mid_up, 4),
                "pm_mid_dn": round(pm_mid_dn, 4),
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
                "up_shares": round(inv_up_display, 2),
                "dn_shares": round(inv_dn_display, 2),
                "net_delta": round(inv_net_delta_display, 2),
                "usdc": round(inv_usdc_total, 2),
                "up_avg_entry": up_avg_entry_display,
                "dn_avg_entry": dn_avg_entry_display,
            },
            "inventory_internal": {
                "up_shares": round(self.inventory.up_shares, 2),
                "dn_shares": round(self.inventory.dn_shares, 2),
                "net_delta": round(self.inventory.net_delta, 2),
                "usdc": round(self.inventory.usdc, 2),
                "up_avg_entry": round(self.inventory.up_cost.avg_entry_price, 4),
                "dn_avg_entry": round(self.inventory.dn_cost.avg_entry_price, 4),
                "drift_up_shares": round(up_drift, 3),
                "drift_dn_shares": round(dn_drift, 3),
            },

            # Paired filling state
            "paired_filling": {
                "imbalance_shares": round(abs(inv_up_display - inv_dn_display), 2),
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
            "settlement_guard": {
                "active": bool(settlement_guard_tokens),
                "tokens": settlement_guard_tokens,
                "suppressed_total": self._settlement_lag_suppressed_total,
                "escalated_total": self._settlement_lag_escalated_total,
            },
            "post_fill_entry_guard": {
                "active": self._post_fill_entry_guard_active,
                "seconds_left": round(
                    max(0.0, self._post_fill_entry_guard_until - time.time()),
                    2,
                ) if self._post_fill_entry_guard_window_active() else 0.0,
                "reason": self._post_fill_entry_guard_reason,
                "trigger_count": self._post_fill_entry_guard_trigger_count,
                "anchor_score": round(self._post_fill_entry_guard_anchor.overall_score, 3)
                if self._post_fill_entry_guard_anchor
                else None,
                "anchor_spread_bps": round(self._post_fill_entry_guard_anchor.spread_bps, 1)
                if self._post_fill_entry_guard_anchor
                else None,
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
            "position_value_pm": round(_position_value, 4),
            "is_paused": self._paused,
            "pause_reason": self._pause_reason,
            "is_closing": self._is_closing,
            "is_running": self._running,
            "reconcile_guard_sec": round(max(0.0, self._reconcile_guard_until - time.time()), 1),

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

            # Merge-first paired inventory
            "paired_inventory": self.inventory.paired.to_dict(),

            # Rebate
            "rebate": self.rebate.stats,

            # Execution quality / toxicity
            "markout_tca": {
                **self.markout_tracker.stats,
                "toxicity_mode": self._toxicity_mode,
                "toxicity_spread_mult": round(self._toxicity_spread_mult, 2),
                "recent": self.markout_tracker.recent_records,
            },

            # Event-driven requote telemetry
            "event_requote": {
                **self.event_requoter.stats,
                "recent": self._last_requote_events[-10:],
            },

            # PnL decomposition
            "pnl_decomposition": {
                **self.pnl_decomp.stats,
                "inventory_unrealized_usd": round(inventory_unrealized, 4),
            },

            # SRE metrics
            "sre_metrics": self.sre_metrics.stats,

            # Config (current)
            "config": self.config.to_dict(),

            # PM prices from feed
            "pm_prices": {
                "up": st.pm_up if hasattr(st, "pm_up") else 0,
                "dn": st.pm_dn if hasattr(st, "pm_dn") else 0,
            },

            # Real USDC balance on Polymarket
            "usdc_balance_pm": round(self._cached_usdc_balance, 2),
            "usdc_free_pm": round(usdc_free_pm, 2),
            "usdc_reserved_collateral_pm": round(reserved_collateral_pm, 2),
            "usdc_reserved_collateral_est_pm": {
                "buy_reserved": round(reserved_est.get("buy_reserved", 0.0), 2),
                "short_reserved": round(reserved_est.get("short_reserved", 0.0), 2),
                "total_reserved": round(reserved_est.get("total_reserved", 0.0), 2),
            },
            "usdc_free_source": usdc_free_source,

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
