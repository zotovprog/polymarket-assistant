# Polymarket Market Maker — Audit Report

**Date:** 2026-02-24
**Codebase:** polymarket-assistant
**Total LOC:** ~3,500 across 15 modules

---

## 1. Executive Summary

The Polymarket Market Maker is an async Python system that provides automated two-sided liquidity on Polymarket's binary options (UP/DOWN tokens). It runs as a FastAPI web server on Railway with a REST API for control, Telegram notifications, and MongoDB logging.

**Key capabilities:**
- Fair value calculation via Black-Scholes digital option model + TA signal adjustment
- Adaptive quote generation with inventory-based skew and volatility widening
- Smart 3-phase liquidation (merge -> gradual limit -> taker -> abandon)
- PnL-based exits (take profit, trailing stop, max drawdown)
- Paper/live mode with unified mock client
- Real-time fill detection (WebSocket + polling hybrid)
- On-chain operations: ERC1155 approvals, position merging

---

## 2. Architecture Overview

```
                         FastAPI Web Server (web_server.py)
                         ├── REST API: /api/mm/*
                         ├── Auth: Cookie/header-based (x-access-key)
                         └── Global: TelegramNotifier, MMRuntime singleton
                                │
            ┌───────────────────┼───────────────────┐
            ▼                   ▼                   ▼
    MarketMaker            MongoLogger         TelegramNotifier
    (market_maker.py)      (mongo_logger.py)   (telegram_notifier.py)
    ├── QuoteEngine        ├── fills collection  ├── Fire-and-forget
    ├── OrderManager       ├── snapshots coll.   ├── Window summaries
    ├── RiskManager        └── logs coll.        └── Interactive API
    ├── FairValueEngine                              (keyboards, callbacks)
    ├── HeartbeatManager
    └── RebateTracker

    Data Feeds (feeds.py)
    ├── Binance WS: trades + klines
    ├── Binance REST: order book polling
    └── Polymarket WS: token prices
```

### File Structure
| File | Lines | Purpose |
|------|-------|---------|
| `web_server.py` | ~1400 | FastAPI app, MMRuntime lifecycle, REST API |
| `src/mm/market_maker.py` | ~1180 | Main trading loop, liquidation, reconciliation |
| `src/mm/order_manager.py` | ~794 | Order placement, fill detection, balance queries |
| `src/mm/quote_engine.py` | ~224 | Quote generation, spread, skew |
| `src/mm/fair_value.py` | ~186 | Binary option pricing (Black-Scholes + TA) |
| `src/mm/risk_manager.py` | ~188 | PnL tracking, pause conditions |
| `src/mm/market_quality.py` | ~143 | Market quality scoring |
| `src/mm/types.py` | ~189 | Data types (Fill, Inventory, Quote, MarketInfo) |
| `src/mm/mm_config.py` | ~100 | 40+ runtime-adjustable parameters |
| `src/mm/heartbeat.py` | ~148 | Keep-alive pings to CLOB |
| `src/mm/rebate_tracker.py` | ~127 | Maker rebate program tracking |
| `src/mm/approvals.py` | ~200 | On-chain ERC1155/ERC20 approvals, merge |
| `src/mm/mongo_logger.py` | ~172 | Async buffered MongoDB writes |
| `src/feeds.py` | ~604 | Binance + Polymarket data feeds |
| `src/config.py` | ~83 | Configuration constants |
| `src/telegram_notifier.py` | ~463 | Telegram notifications + interactive API |

---

## 3. Trading Strategy

### 3.1 Fair Value Model

The bot prices Polymarket binary options using a **log-normal digital option model** (simplified Black-Scholes for binary outcomes).

**UP token** pays $1 if `price > strike` at expiry, $0 otherwise.
**DN token** pays $1 if `price <= strike` at expiry, $0 otherwise.

```
FV_up = N(d2)

where:
  d2 = (ln(S/K) - 0.5 * sigma^2 * T) / (sigma * sqrt(T))
  S = current Binance mid-price
  K = strike price (candle open at window start)
  sigma = realized volatility (from klines)
  T = time to expiry (in kline periods)
  N() = standard normal CDF
```

**Realized volatility** is computed from the last 20 klines as:
```
returns[i] = ln(close[i] / close[i-1])
sigma = std_dev(returns)
clamped to [0.0001, 0.01] per kline period
```

### 3.2 TA Signal Adjustment

Base FV is adjusted by up to +/-3% based on 4 signals:

| Signal | Weight | Logic |
|--------|--------|-------|
| RSI (14-period) | 30% | Bullish if RSI > 50, bearish if < 50 |
| EMA Cross (5/20) | 30% | Separation normalized by 0.5% threshold |
| Order Book Imbalance | 20% | (asks_vol - bids_vol) / total |
| CVD (5m) | 20% | Cumulative volume delta, normalized by 100K |

```
fv_up = clamp(base_fv + adjustment, 0.01, 0.99)
fv_dn = clamp(1.0 - fv_up, 0.01, 0.99)
```

### 3.3 Quote Generation

**Effective half-spread** adapts to volatility:
```
if vol/avg_vol > volatility_pause_mult (3.0):
    spread = max_spread_bps (500)
else:
    mult = 1.0 + (vol_ratio - 1.0) * (vol_spread_mult - 1.0)
    spread = base_spread * mult
clamped to [min_spread_bps, max_spread_bps]
```

**Inventory skew** encourages mean-reversion:
```
delta = up_shares - dn_shares
skew = delta * skew_bps_per_unit / 10000

bid = FV - half_spread - skew
ask = FV + half_spread - skew
```
Positive delta (long UP) lowers quotes to encourage selling UP / buying DN.

**USDC budget constraint**: Remaining budget split 50/50 across UP/DN BUY sides. BUY size capped by available budget.

**Book clamping**: Post-only enforcement prevents crossing (bid < best_ask, ask > best_bid).

### 3.4 Order Placement

- Orders are Good-Till-Date (GTD, 5min default)
- Post-only by default (maker, 0% fee)
- Requote if price moves > 5 bps from current quote
- Cancel-replace: atomic cancel old + place new
- Session budget cap enforced per order

---

## 4. Risk Management

### 4.1 Exit Triggers (Priority Order)

| Trigger | Condition | Action |
|---------|-----------|--------|
| Take Profit | `pnl >= take_profit_usd` | Stop + liquidate |
| Max Drawdown | `pnl < -max_drawdown_usd` | Stop + liquidate |
| Trailing Stop | `pnl < peak_pnl * (1 - trailing_stop_pct)` | Stop + liquidate |
| Inventory Limit | `shares > max_inventory_shares` | Suppress BUY only |
| Volatility Spike | `vol > avg_vol * volatility_pause_mult` | Full pause |
| Disabled | `config.enabled == False` | Full pause |

**Session PnL preference**: Uses PM-balance-based PnL (more reliable than internal fill tracking). Falls back to computed PnL if unavailable.

**Trailing stop activation**: Only triggers after peak PnL reaches max($2, 25% of take_profit).

### 4.2 Liquidation Phases

When closing is triggered (window expiry or risk exit):

**Phase 0 — Merge**: If `min(up_shares, dn_shares) >= 1.0`, merge YES+NO pairs on-chain. Instant, no slippage, ~$0.50 gas.

**Phase 1 — Gradual Limit** (time_left > liq_taker_threshold_sec):
- Split remaining into `liq_gradual_chunks` (default 3)
- Price = max(floor, FV - liq_max_discount_from_fv)
- Post-only (maker, 0% fee)
- Wait liq_chunk_interval_sec (5s) between chunks

**Phase 2 — Taker** (time_left <= liq_taker_threshold_sec):
- Sell at best_bid, non-post-only
- Only if best_bid >= adaptive floor

**Phase 3 — Abandon**:
- If best_bid < floor, don't sell
- Token may resolve to $1 (natural recovery)

**Adaptive Floor Decay**:
```
floor = max(0.01, base_floor * time_left / closing_start_time_left)
```
Floor decays from entry price margin toward $0.01 as window approaches expiry.

**Emergency drawdown during liquidation**:
- If session_pnl < -1x max_drawdown: force taker mode
- If session_pnl < -2x max_drawdown: abandon liquidation entirely, stop bot

### 4.3 One-Sided Exposure Detection

After 50% of window duration and 120s warmup:
- Track consecutive ticks where only one side has inventory
- If one-sided for `max_one_sided_ticks` (90 ticks, ~3 min): trigger closing

### 4.4 Market Quality Gate

Every 5 ticks (live mode only):
- Fetch order books for both UP and DN tokens
- Compute quality score: 40% spread + 60% liquidity depth
- If `score < min_market_quality_score` (0.3) or depth < $50: exit position

---

## 5. Inventory & Reconciliation

### 5.1 Internal Tracking

`Inventory` dataclass tracks:
- `up_shares`, `dn_shares`: Current position
- `usdc`: USDC balance
- `up_cost`, `dn_cost`: CostBasis (total_cost, total_shares, avg_entry_price)

Updated on every fill via `update_from_fill()`. SELL fills clamp shares to `max(0.0, shares - fill.size)` to prevent negative inventory.

### 5.2 Debounced Reconciliation (Live Mode)

Every 5 ticks (~10s), the bot:
1. Fetches real PM token balances
2. Compares to internal tracking
3. If drift detected (>0.5 shares difference):
   - Records snapshot, starts stability window
   - Waits for 3 consecutive checks with stable PM values
   - Only then reconciles internal state to match PM

This prevents oscillation from PM API latency or in-flight orders.

### 5.3 Window Transitions

On new market window:
- Reset reconciliation state
- Reset risk manager (fills, vol history, peak PnL)
- Cancel all orders
- Start fresh with new token IDs

---

## 6. Data Feeds

### 6.1 Binance

**WebSocket** (`/stream`): Trades + klines in real-time
- Multi-stream: `{symbol}@trade` + `{symbol}@kline_{interval}`
- Reconnects with exponential backoff (1s-10s)

**REST** (`/api/v3`): Order book snapshots
- 20 price levels, 1000ms update interval
- Dual endpoint: primary (global) + fallback (US)

**Bootstrap**: 100 historical klines fetched at startup for TA indicators.

### 6.2 Polymarket

**WebSocket** (`ws-subscriptions-clob`): Real-time token prices
- Subscribes to UP and DN token price changes
- Filters prices >= 0.99 (resolved market detection)
- Supports reconnect on window transition (new token IDs)

**REST** (`gamma-api`): Event metadata, token IDs, strike calculation
- Slug-based market identification
- Handles 5m/15m (Unix timestamp slugs), 1h/4h/daily (calendar slugs)
- DST-aware ET time for daily/hourly markets

---

## 7. On-Chain Operations

### 7.1 Approvals (Polygon)

Before SELL orders work, the bot sets approvals for 3 operators:
- CTF_Exchange (0x4bFb41...)
- Neg_Risk_Exchange (0xC5d563...)
- Neg_Risk_Adapter (0xd91E80...)

Both ERC1155 (setApprovalForAll) and ERC20 USDC (approve unlimited) are required.

Gas params: maxFeePerGas = max(100 gwei, current * 2), gas limit = 100K.

### 7.2 Merge Positions

Combines equal UP+DN token pairs back into USDC (1:1, no slippage).
- Calls CTF.mergePositions() on Polygon
- Gas limit: 200K
- Paper mode: instant credit, no on-chain call
- Skipped for funder/proxy (Safe multisig) accounts

---

## 8. Configuration Parameters

### Spread & Sizing
| Parameter | Default | Description |
|-----------|---------|-------------|
| `half_spread_bps` | 150 | Base half-spread (1.5%) |
| `min_spread_bps` | 50 | Floor spread |
| `max_spread_bps` | 500 | Ceiling spread |
| `vol_spread_mult` | 1.5 | Spread multiplier in high-vol |
| `order_size_usd` | 10.0 | Target USD per side |
| `max_inventory_shares` | 25.0 | Max shares one-sided |
| `skew_bps_per_unit` | 15.0 | Skew per share of net delta |

### Risk
| Parameter | Default | Description |
|-----------|---------|-------------|
| `max_drawdown_usd` | 50.0 | Max session drawdown |
| `take_profit_usd` | 0.0 | Exit trigger (0 = disabled) |
| `trailing_stop_pct` | 0.0 | Trail from peak (0 = disabled) |
| `volatility_pause_mult` | 3.0 | Pause if vol > Nx avg |
| `max_one_sided_ticks` | 90 | ~3min one-sided before close |

### Liquidation
| Parameter | Default | Description |
|-----------|---------|-------------|
| `liq_price_floor_enabled` | true | Use entry+margin as floor |
| `liq_gradual_chunks` | 3 | Split liquidation into N chunks |
| `liq_chunk_interval_sec` | 5.0 | Wait between chunks |
| `liq_taker_threshold_sec` | 20.0 | Switch to taker when <20s |
| `liq_max_discount_from_fv` | 0.03 | Max discount from FV |
| `liq_abandon_below_floor` | true | Don't sell below floor |

### Window Management
| Parameter | Default | Description |
|-----------|---------|-------------|
| `close_window_sec` | 30.0 | Seconds before expiry to close |
| `auto_next_window` | true | Auto-restart next window |
| `resolution_wait_sec` | 90.0 | Wait for new tokens after expiry |

All parameters updatable at runtime via `POST /api/mm/config`.

---

## 9. API Endpoints

| Method | Path | Description |
|--------|------|-------------|
| POST | `/api/auth/login` | Authenticate with access key |
| GET | `/api/auth/check` | Check auth status |
| POST | `/api/mm/start` | Start MM (coin, timeframe, paper_mode, initial_usdc, dev) |
| POST | `/api/mm/stop` | Stop MM + liquidate |
| GET | `/api/mm/state` | Current snapshot (quotes, inventory, PnL, feeds) |
| POST | `/api/mm/config` | Update config at runtime |
| GET | `/api/mm/config` | Get current config |
| POST | `/api/mm/emergency` | Cancel all orders, pause |
| POST | `/api/mm/kill` | Full shutdown, disable auto-restart |
| POST | `/api/mm/watch` | Start feeds only (no trading) |
| POST | `/api/mm/watch/stop` | Stop watch mode |
| GET | `/api/mm/fills` | Recent fills (paginated) |
| GET | `/api/logs` | In-memory log buffer |
| GET | `/api/health` | Health check |
| GET | `/api/markets` | Available coins + timeframes |

Auth: `x-access-key` header or `pm_web_auth` cookie.

---

## 10. MongoDB Schema

### fills collection
```json
{
  "ts": 1708800000.0,
  "side": "BUY",
  "token_type": "up",
  "token_id": "0x...",
  "price": 0.55,
  "size": 10.0,
  "fee": 0.0,
  "order_id": "abc-123",
  "is_maker": true,
  "market": {"coin": "BTC", "timeframe": "15m", "strike": 97000},
  "inventory": {"up_shares": 10, "dn_shares": 0, "net_delta": 10},
  "fair_value": {"up": 0.55, "dn": 0.45, "binance_mid": 97500},
  "pnl": {"realized": 0.0, "unrealized": 5.5, "total": 5.5},
  "paper_mode": false
}
```

### snapshots collection
```json
{
  "ts": 1708800020.0,
  "is_running": true,
  "market": {...},
  "quotes": {...},
  "inventory": {...},
  "recent_fills": [...]
}
```

### logs collection
```json
{
  "ts": 1708800000.0,
  "level": "INFO",
  "name": "mm.engine",
  "msg": "FILL: BUY 10.0@0.55 (UP) fee=0.0000"
}
```
TTL index: auto-delete after 30 days.

---

## 11. Telegram Integration

### Notification Types
- MM Started/Stopped
- Fill executed (disabled by default, window summary preferred)
- PnL update
- Risk pause triggered
- Window summary (real PnL from PM balances)
- Error notifications

### Dev Mode
`dev: bool` parameter on `/api/mm/start` switches to `DEV_TELEGRAM_BOT_TOKEN` / `DEV_TELEGRAM_CHAT_ID` for testing.

### Interactive API
Available for bot management (not yet wired to polling loop):
- `send_with_keyboard()` — inline keyboards
- `answer_callback_query()` — button clicks
- `edit_message_text()` — update messages
- `get_updates()` — long-polling

---

## 12. Deployment

**Platform:** Railway (auto-deploy from GitHub)
**Runtime:** Python 3.11+, FastAPI + Uvicorn
**Dependencies:** py_clob_client, web3.py, motor, websockets, httpx, aiohttp

### Environment Variables
| Variable | Purpose |
|----------|---------|
| `PM_PRIVATE_KEY` | Polygon account private key |
| `PM_FUNDER` | Funder address (for Safe multisig) |
| `PM_API_KEY` / `PM_API_SECRET` / `PM_API_PASSPHRASE` | CLOB API credentials |
| `PM_WEB_ACCESS_KEY` | Web server auth key |
| `TELEGRAM_BOT_TOKEN` / `TELEGRAM_CHAT_ID` | Telegram notifications |
| `DEV_TELEGRAM_BOT_TOKEN` / `DEV_TELEGRAM_CHAT_ID` | Dev channel |
| `MONGO_URI` / `MONGO_DB` | MongoDB connection |
| `BINANCE_WS` / `BINANCE_REST` | Binance endpoint overrides |

---

## 13. Known Issues & Recommendations

### Fixed Issues (This Session)
1. **Negative inventory**: SELL > internal shares led to DN=-11.9. Fixed with `max(0.0, shares - fill.size)` clamp.
2. **Floor price stuck**: Decay formula used wrong reference. Fixed with `_closing_start_time_left` tracking.
3. **Drawdown ignored during closing**: `_is_closing` early return skipped risk checks. Added emergency drawdown check in liquidation.
4. **False Telegram notifications**: Window summary sent on manual stop. Fixed — only on expiry.
5. **Wrong PnL in Telegram**: Used internal tracking. Fixed — uses real PM API balances.

### Remaining Considerations
1. **Merge availability**: Requires EOA private key. Funder/proxy (multisig) accounts skip merge — must rely on SELL liquidation.
2. **Budget split**: 50/50 across UP/DN may leave budget unused if one side is heavily skewed.
3. **Reconciliation**: One-shot sync (not gradual blending). PM balance swings cause jumps.
4. **Heartbeat ID**: Auto-regenerates on rejection. If PM criteria change, may need manual intervention.
5. **Dust positions**: Final dust check at PM minimum (5 shares). Very cheap tokens may lock small amounts.

---

*Report generated: 2026-02-24*
