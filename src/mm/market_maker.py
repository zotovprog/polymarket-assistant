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
from .risk_manager import RiskManager
from .heartbeat import HeartbeatManager
from .rebate_tracker import RebateTracker

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

        # Sub-engines
        self.fair_value = FairValueEngine()
        self.quote_engine = QuoteEngine(config)
        self.order_mgr = OrderManager(clob_client, config)
        self.risk_mgr = RiskManager(config)
        self.heartbeat = HeartbeatManager(clob_client, config.heartbeat_interval_sec)
        self.rebate = RebateTracker(clob_client)

        # State
        self.inventory = Inventory()
        self.market: Optional[MarketInfo] = None
        self._running = False
        self._paused = False
        self._pause_reason = ""
        self._task: Optional[asyncio.Task] = None
        self._started_at: float = 0.0

        # Current quotes (for dashboard)
        self._current_quotes: dict[str, tuple[Optional[Quote], Optional[Quote]]] = {
            "up": (None, None),
            "dn": (None, None),
        }

        # Stats
        self._quote_count: int = 0
        self._requote_count: int = 0
        self._spread_samples: list[float] = []

        # Callbacks
        self._on_fill_callbacks: list = []
        self._on_state_change_callbacks: list = []

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

    async def start(self) -> None:
        """Start the market maker."""
        if self._running:
            log.warning("MarketMaker already running")
            return

        if not self.market:
            raise ValueError("Market info not set — call set_market() first")

        self._running = True
        self._started_at = time.time()
        self._paused = False
        self._pause_reason = ""

        # Start heartbeat
        self.heartbeat.start()

        # Start main loop
        self._task = asyncio.create_task(self._run_loop())
        log.info("MarketMaker started")

    async def stop(self) -> None:
        """Graceful shutdown: cancel all orders, stop heartbeat."""
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

        # Cancel all orders
        cancelled = await self.order_mgr.cancel_all()
        log.info(f"Cancelled {cancelled} orders on shutdown")

        # Stop heartbeat
        await self.heartbeat.stop()

        log.info("MarketMaker stopped")

    async def _run_loop(self) -> None:
        """Main quoting loop."""
        log.info("Quote loop started")
        try:
            while self._running:
                try:
                    await self._tick()
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
        if not self.market or self.market.is_expired:
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

        # 2. Check for fills
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

        # 4. Compute volatility
        vol = self.fair_value.realized_vol(klines)
        self.risk_mgr.record_vol(vol)

        # 5. Check risk limits
        should_pause, reason = self.risk_mgr.should_pause(
            self.inventory, vol, fv_up, fv_dn)

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

        if self._paused:
            return

        # 6. Generate quotes
        all_quotes = self.quote_engine.generate_all_quotes(
            fv_up, fv_dn,
            self.market.up_token_id, self.market.dn_token_id,
            self.inventory,
            vol, self.risk_mgr.avg_volatility,
        )

        self._quote_count += 1

        # 7. Place or update orders
        for token_key in ("up", "dn"):
            new_bid, new_ask = all_quotes[token_key]
            cur_bid, cur_ask = self._current_quotes.get(token_key, (None, None))

            need_bid = self.quote_engine.should_requote(cur_bid, new_bid)
            need_ask = self.quote_engine.should_requote(cur_ask, new_ask)

            if need_bid or need_ask:
                self._requote_count += 1
                # Cancel old orders
                old_ids = []
                if need_bid and cur_bid and cur_bid.order_id:
                    old_ids.append(cur_bid.order_id)
                if need_ask and cur_ask and cur_ask.order_id:
                    old_ids.append(cur_ask.order_id)

                new_quotes = []
                if need_bid:
                    new_quotes.append(new_bid)
                if need_ask:
                    new_quotes.append(new_ask)

                new_ids = await self.order_mgr.cancel_replace(old_ids, new_quotes)

                # Update current quotes tracking
                updated_bid = new_bid if need_bid else cur_bid
                updated_ask = new_ask if need_ask else cur_ask
                self._current_quotes[token_key] = (updated_bid, updated_ask)

        # 8. Track spread for stats
        up_bid, up_ask = self._current_quotes.get("up", (None, None))
        if up_bid and up_ask and up_ask.price > 0:
            spread_bps = (up_ask.price - up_bid.price) / up_ask.price * 10000
            self._spread_samples.append(spread_bps)
            if len(self._spread_samples) > 1000:
                self._spread_samples = self._spread_samples[-500:]

    async def on_window_transition(self, new_market: MarketInfo) -> None:
        """Handle window transition — cancel all, switch tokens, resume."""
        log.info(f"Window transition: {new_market.coin} {new_market.timeframe}")

        # Cancel all existing orders
        await self.order_mgr.cancel_all()
        self._current_quotes = {"up": (None, None), "dn": (None, None)}

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

        return {
            # Market info
            "market": {
                "coin": self.market.coin if self.market else "",
                "timeframe": self.market.timeframe if self.market else "",
                "strike": self.market.strike if self.market else 0,
                "time_remaining": self.market.time_remaining if self.market else 0,
                "up_token": self.market.up_token_id[:12] + "..." if self.market else "",
                "dn_token": self.market.dn_token_id[:12] + "..." if self.market else "",
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
            },

            # PnL & Risk
            **risk_stats,
            "avg_spread_bps": round(avg_spread, 1),
            "is_paused": self._paused,
            "pause_reason": self._pause_reason,
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
        }
