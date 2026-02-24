# Polymarket Market Maker — Architecture & Strategy

## Overview

Automated market maker for Polymarket binary options on cryptocurrency price movements. The bot quotes bid/ask on UP and DN tokens for time-windowed markets (5m, 15m, 1h, 4h, daily), earning the bid-ask spread and Polymarket maker rebates.

**Core principle:** Buy UP and DN tokens simultaneously at prices that sum to less than $1. When both sides fill, the combined position is worth exactly $1 at expiry regardless of outcome, locking in the spread as profit.

---

## 1. Market Structure

### Polymarket Binary Options

Each market is a binary question: *"Will BTC be above $66,280 in 15 minutes?"*

- **UP token**: pays $1 if price > strike at expiry, $0 otherwise
- **DN token**: pays $1 if price ≤ strike at expiry, $0 otherwise
- Prices range from $0.01 to $0.99
- UP + DN ≈ $1.00 (always, by arbitrage)
- Minimum order size: 5 shares

### Time Windows

Markets operate in fixed windows. When a window expires, the outcome is resolved, and a new window with a fresh strike price opens. Supported timeframes: 5m, 15m, 1h, 4h, daily.

---

## 2. System Architecture

```
┌─────────────────────────────────────────────────┐
│                   Web Dashboard                  │
│              (HTML/CSS/JS, port 8000)            │
└──────────────────────┬──────────────────────────┘
                       │ HTTP API
┌──────────────────────▼──────────────────────────┐
│               web_server.py (FastAPI)            │
│  - Auth, Start/Stop, Config, State polling       │
│  - Window expiry monitor + auto-next-window      │
└──────────┬───────────────────────┬──────────────┘
           │                       │
┌──────────▼──────────┐  ┌────────▼───────────────┐
│   MarketMaker       │  │    Data Feeds           │
│   (market_maker.py) │  │  - Binance WS (klines,  │
│                     │  │    trades, book)         │
│  ┌─────────────┐    │  │  - Polymarket REST       │
│  │ FairValue   │    │  │    (tokens, prices,      │
│  │ Engine      │    │  │     strike, book)        │
│  ├─────────────┤    │  └────────────────────────┘
│  │ QuoteEngine │    │
│  ├─────────────┤    │
│  │ RiskManager │    │
│  ├─────────────┤    │
│  │ QualityAnl. │    │
│  ├─────────────┤    │
│  │ OrderManager│────┼──── Polymarket CLOB API
│  ├─────────────┤    │
│  │ Heartbeat   │────┼──── Polymarket Heartbeat API
│  └─────────────┘    │
└─────────────────────┘
```

### Key Files

| File | Responsibility |
|------|---------------|
| `web_server.py` | FastAPI server, dashboard, MM lifecycle, auto-next-window |
| `market_maker.py` | Main loop (`_tick`), orchestrates all components |
| `fair_value.py` | Binary option pricing (log-normal model + TA signals) |
| `quote_engine.py` | Bid/ask generation, spread, skew, book clamping |
| `risk_manager.py` | PnL tracking, pause conditions, liquidation locks |
| `market_quality.py` | Order book depth/spread analysis for entry/exit |
| `order_manager.py` | Order placement, cancellation, fill tracking, CLOB API |
| `types.py` | Core data types: Quote, Fill, Inventory, MarketInfo |
| `mm_config.py` | All configurable parameters (runtime-adjustable) |
| `indicators.py` | Technical indicators: RSI, EMA, OBI, CVD |

---

## 3. Fair Value Model

### Base Model: Log-Normal Binary Pricing

The fair value of the UP token is the probability that BTC will be above the strike at expiry:

```
P(UP) = N(d₂)

d₂ = (ln(S/K) - ½σ²T) / (σ√T)

Where:
  S = Current Binance mid-price
  K = Strike price
  σ = Realized volatility (per-kline, from log-returns of last 20 closes)
  T = Time remaining (in kline units, i.e., minutes)
  N = Standard normal CDF
```

Fair value is clamped to [0.01, 0.99]. DN fair value = 1 - UP fair value.

**Source:** `fair_value.py:86-117`, method `binary_fair_value()`

### Volatility Estimation

Realized volatility computed from 1-minute kline closes:

```python
returns = [log(close[i] / close[i-1]) for i in range(1, N)]
vol = sqrt(variance(returns))
vol = clamp(vol, vol_floor=0.0001, vol_cap=0.01)
```

Typical BTC per-minute vol: 0.0003–0.0006.

**Source:** `fair_value.py:56-84`, method `realized_vol()`

### TA Signal Adjustments

Base fair value is nudged ±3% by four signals:

| Signal | Weight | Logic |
|--------|--------|-------|
| **RSI** (14-period) | 30% | RSI > 50 → bullish nudge, < 50 → bearish |
| **EMA Cross** (short/long) | 30% | EMA separation as fraction of price |
| **OBI** (Order Book Imbalance) | 20% | Bid depth vs ask depth ratio |
| **CVD** (Cumulative Volume Delta, 5m) | 20% | Net buy vs sell volume |

Maximum adjustment: ±`signal_weight` (default 3%).

**Source:** `fair_value.py:119-163`, method `adjust_for_signals()`

---

## 4. Quoting Strategy

### Quote Generation

For each token (UP and DN):

```
bid = FV - half_spread - inventory_skew
ask = FV + half_spread - inventory_skew
```

All prices rounded to $0.01 (Polymarket tick size), clamped to [0.01, 0.99].

**Source:** `quote_engine.py:64-108`, method `generate_quotes()`

### Adaptive Spread

Half-spread (default 150 bps) is widened during high volatility:

```
If current_vol > avg_vol:
  vol_ratio = current_vol / avg_vol
  If vol_ratio > volatility_pause_mult (3x): use max_spread (500 bps)
  Otherwise: spread *= 1 + (vol_ratio - 1) * (vol_spread_mult - 1)

Final spread = clamp(spread, min_spread=50 bps, max_spread=500 bps)
```

**Source:** `quote_engine.py:36-51`, method `_effective_half_spread()`

### Inventory Skew

Quotes are shifted to rebalance inventory:

```
skew = net_delta × skew_bps_per_unit / 10000

net_delta = UP_shares - DN_shares
```

Positive delta (excess UP) → lower both bid and ask → encourages selling UP / buying DN.

Default: 15 bps per share of net delta.

**Source:** `quote_engine.py:53-62`, method `_inventory_skew()`

### Book Clamping (Post-Only Protection)

Before placement, quotes are clamped to avoid crossing the Polymarket book:

- BUY price must be < best_ask (otherwise would cross and execute as taker)
- SELL price must be > best_bid

If crossing would occur, price is pulled back to 1 tick from book edge.

**Source:** `quote_engine.py:110-133`, method `clamp_to_book()`

### Budget Cap

BUY orders are constrained by the session USDC budget:

```
remaining = session_limit - UP_locked - DN_locked - order_collateral
Each side gets half of remaining USDC
```

When budget is exhausted, only SELL orders are generated (ask-only mode).

**Source:** `quote_engine.py:150-219`, method `generate_all_quotes()`

---

## 5. Order Management

### Order Types

- **BUY**: Post-only (maker), Good-Till-Date (GTD, 300s default)
- **SELL**: Post-only when quoting; fallback to taker during liquidation
- **Heartbeat**: Every 55s, keeps GTD orders alive on Polymarket

### Requoting

Every `requote_interval_sec` (2s), the bot:

1. Computes new fair values
2. Generates new quotes
3. Compares to current live quotes
4. If price difference > `requote_threshold_bps` (5 bps): cancel-and-replace

### Polymarket-Specific: BUY-Only Strategy

On Polymarket, SELL requires holding token inventory. The natural market-making strategy uses BUY on both sides:

- BUY UP @ bid_up → provides bid-side for UP
- BUY DN @ bid_dn → implicitly provides ask-side for UP at (1 - bid_dn)

When both fill: you hold UP + DN = $1 at expiry, paid bid_up + bid_dn < $1 → profit = spread.

SELL orders are only placed when the bot has inventory to sell.

**Source:** `market_maker.py:473-528`

---

## 6. Risk Management

### Priority Order

Risk checks run every tick in this order (highest priority first):

1. **Take Profit** — if total PnL ≥ `take_profit_usd` → close all positions
2. **Max Drawdown** — if total PnL < -`max_drawdown_usd` → close all positions
3. **Trailing Stop** — if PnL drops `trailing_stop_pct` from peak → close all positions
4. **Inventory Limit** — if UP or DN shares > `max_inventory_shares` → pause quoting
5. **Volatility Spike** — if vol > avg_vol × `volatility_pause_mult` → pause quoting
6. **Config Disabled** — if `enabled = false` → pause quoting

Exit triggers (1–3) **always override pauses** — even if the bot is already paused due to inventory limit, a take-profit hit will unpause and enter closing mode.

**Source:** `risk_manager.py:88-132`, `market_maker.py:350-382`

### PnL Calculation

```
Realized PnL = Σ(sell_notional - fee) - Σ(buy_notional + fee)
Unrealized PnL = UP_shares × FV_up + DN_shares × FV_dn
Total PnL = Realized + Unrealized

Session PnL = (current_USDC_balance + position_value_at_PM_prices) - starting_USDC
```

Two PnL methods:
- **Internal** (realized + unrealized): based on fill records, uses fair value for mark-to-market
- **Session** (balance-based): compares current portfolio value against starting USDC, uses actual Polymarket prices

**Source:** `risk_manager.py:53-86`, `market_maker.py:848-855`

### Liquidation Lock

When an exit trigger fires, prices are locked at that moment:

```python
LiquidationLock:
  trigger_pnl       # PnL at trigger time
  up_avg_entry       # Average entry price for UP
  dn_avg_entry       # Average entry price for DN
  min_sell_price_up  # avg_entry + margin (1¢)
  min_sell_price_dn  # avg_entry + margin (1¢)
```

This prevents selling below cost basis during liquidation.

**Source:** `risk_manager.py:155-168`, method `lock_pnl()`

---

## 7. Liquidation Engine

When closing mode is triggered (take profit, drawdown, window expiry, etc.), the bot enters a multi-phase exit.

### Phase 0: Merge (YES+NO → USDC)

If the bot holds both UP and DN tokens, it can merge pairs via the CTF (Conditional Token Framework) contract:

```
min(UP_balance, DN_balance) pairs → equal USDC
```

- Instant settlement, no slippage, only gas cost
- Reduces inventory before market sells

**Source:** `market_maker.py:571-599`

### Phase 1: Gradual Limit Orders

While `time_left > taker_threshold_sec` (20s):

- Split remaining inventory into `liq_gradual_chunks` (3) chunks
- Price: `max(floor, FV - liq_max_discount_from_fv)`
- Improve to `best_bid` if it's higher
- Post-only (maker)
- Re-place every `liq_chunk_interval_sec` (5s) with updated prices

**Source:** `market_maker.py:714-734`

### Phase 2: Taker Liquidation

When `time_left ≤ taker_threshold_sec` (20s):

- Cancel all limit orders
- Sell everything at `best_bid`
- Only if `best_bid ≥ floor`

### Phase 3: Force Sell / Abandon

- If `best_bid < floor` and `time_left > 5s`: wait for price recovery
- If `best_bid < floor` and `time_left ≤ 5s`: force sell at any price
- If `liq_abandon_below_floor = true` and no buyer: let token expire (may resolve to $1)

**Source:** `market_maker.py:679-757`

### Adaptive Floor Decay

The price floor decays linearly toward $0.01 as time runs out:

```
decay_ratio = time_left / close_window_sec
floor = max(0.01, base_floor × decay_ratio)
```

This prevents the bot from getting stuck holding tokens it refuses to sell.

**Source:** `market_maker.py:669-677`

---

## 8. Window Management

### Close Window

`close_window_sec` before expiry (default 30s, max 40% of window duration):

1. Cancel all active quotes
2. Lock PnL (liquidation lock)
3. Enter closing mode → liquidation engine takes over

### Auto-Next-Window

After a window expires:

1. Stop feeds and heartbeat
2. Wait `min_wait` (15s) cooldown
3. Poll for new tokens (up to `resolution_wait_sec` = 90s)
4. Pre-entry market quality check (up to 3 skip attempts)
5. If quality OK → auto-start new session with same parameters

**Source:** `web_server.py:644-759`

### One-Sided Exposure Detection

If inventory is one-sided (e.g., only DN, no UP) for `max_one_sided_ticks` (30) consecutive ticks AND bot has been running > 120s → early close triggered. Prevents accumulating directional risk when only one side fills.

**Source:** `market_maker.py:384-406`

---

## 9. Market Quality Analysis

### Entry Conditions

Before entering a new window, the bot checks:

| Metric | Threshold | Default |
|--------|-----------|---------|
| Overall quality score | ≥ `min_market_quality_score` | 0.3 |
| Total book depth (USD) | ≥ `min_entry_depth_usd` | $50 |
| Best spread (bps) | ≤ `max_entry_spread_bps` | 800 bps |

### Quality Scoring

```
spread_score:    <200 bps → 1.0, >1000 bps → 0.0, linear between
liquidity_score: total_depth / (order_size × 3), capped at 1.0
overall_score:   0.4 × spread_score + 0.6 × liquidity_score
```

### Exit Conditions (During Trading)

Every `quality_check_interval` (5) ticks, if the bot has inventory:

- If `liquidity_score < exit_liquidity_threshold` (0.15) → early close

**Source:** `market_quality.py:29-142`

---

## 10. Configuration Reference

All parameters are runtime-adjustable via API (`POST /api/mm/config`).

### Spread

| Parameter | Default | Description |
|-----------|---------|-------------|
| `half_spread_bps` | 150 | Base half-spread in basis points |
| `min_spread_bps` | 50 | Minimum half-spread |
| `max_spread_bps` | 500 | Maximum half-spread |
| `vol_spread_mult` | 1.5 | Spread widening factor during high vol |

### Sizing

| Parameter | Default | Description |
|-----------|---------|-------------|
| `order_size_usd` | 10 | Target order notional (USD) |
| `min_order_size_usd` | 2 | Skip quote if notional below this |
| `max_order_size_usd` | 100 | Max single order notional |

### Inventory

| Parameter | Default | Description |
|-----------|---------|-------------|
| `max_inventory_shares` | 25 | Max shares per token before pause |
| `skew_bps_per_unit` | 15 | Inventory skew (bps per share of net delta) |

### Requoting

| Parameter | Default | Description |
|-----------|---------|-------------|
| `requote_interval_sec` | 2.0 | Quote loop interval |
| `requote_threshold_bps` | 5 | Min price change to trigger requote |

### Order Types

| Parameter | Default | Description |
|-----------|---------|-------------|
| `gtd_duration_sec` | 300 | Good-Till-Date duration |
| `heartbeat_interval_sec` | 55 | Heartbeat keep-alive interval |
| `use_post_only` | true | Maker-only orders |
| `use_gtd` | true | Use GTD order type |

### Risk

| Parameter | Default | Description |
|-----------|---------|-------------|
| `max_drawdown_usd` | 50 | Close all if PnL drops below -$X |
| `volatility_pause_mult` | 3.0 | Pause if vol > avg_vol × X |
| `max_loss_per_fill_usd` | 5 | Max acceptable loss on single fill |
| `take_profit_usd` | 0 | Close all at +$X PnL (0 = disabled) |
| `trailing_stop_pct` | 0 | Close if PnL drops X% from peak (0 = disabled) |

### Liquidation

| Parameter | Default | Description |
|-----------|---------|-------------|
| `liq_price_floor_enabled` | true | Don't sell below avg entry + margin |
| `liq_price_floor_margin` | 0.01 | Min margin above cost basis ($) |
| `liq_gradual_chunks` | 3 | Split liquidation into N chunks |
| `liq_chunk_interval_sec` | 5.0 | Seconds between chunks |
| `liq_taker_threshold_sec` | 20.0 | Switch to taker with ≤ Ns left |
| `liq_max_discount_from_fv` | 0.03 | Max discount from FV for limit orders |
| `liq_abandon_below_floor` | true | Skip sell if best_bid < floor |

### Window Management

| Parameter | Default | Description |
|-----------|---------|-------------|
| `close_window_sec` | 30 | Enter closing mode N sec before expiry |
| `auto_next_window` | true | Auto-start next window after resolution |
| `resolution_wait_sec` | 90 | Max wait for new tokens after expiry |
| `max_one_sided_ticks` | 30 | Close if one-sided for N ticks |

### Market Quality

| Parameter | Default | Description |
|-----------|---------|-------------|
| `min_market_quality_score` | 0.3 | Min score to enter window |
| `min_entry_depth_usd` | 50 | Min book depth to enter |
| `max_entry_spread_bps` | 800 | Max spread to enter |
| `exit_liquidity_threshold` | 0.15 | Exit if liquidity drops below |
| `quality_check_interval` | 5 | Check every N ticks |

---

## 11. Main Loop (`_tick`)

Executed every `requote_interval_sec` (2s):

```
1. Read feed state (Binance mid, klines, trades, book)
2. Compute fair values (log-normal + TA signals)
3. Check window expiry
   └─ time_left ≤ 0 → liquidate & stop
   └─ time_left ≤ close_window_sec → enter closing mode
4. If closing → run liquidation engine, return
5. Check fills → update inventory, PnL, cost basis
6. Reconcile with PM balances (every 5 ticks, live mode)
7. Check risk limits (TP > drawdown > trailing > inventory > vol)
   └─ Exit trigger → closing mode
   └─ Pause trigger → cancel orders, pause
   └─ Resume → unpause
8. Check one-sided exposure
9. Check market quality (every N ticks)
   └─ Liquidity dried up → early close
10. Generate quotes (bid/ask for UP and DN)
11. Clamp quotes to book (post-only protection)
12. Cap sizes by budget and inventory room
13. Place or cancel-replace orders
14. Track spread statistics
```

**Source:** `market_maker.py:221-536`

---

## 12. Data Feeds

### Binance WebSocket

- **Klines** (1m): used for volatility estimation and TA indicators
- **Trades**: used for CVD calculation
- **Order book** (depth20): used for OBI and book imbalance

### Polymarket REST

- **Token lookup**: fetch UP/DN token IDs for current window
- **Strike/window info**: fetch strike price and window timing
- **Order book**: fetch best bid/ask and depth for quote clamping
- **Token balances**: reconcile inventory with actual PM balances
- **USDC balance**: track available funds

---

## 13. Dashboard

Web interface at `http://localhost:8000`:

- **Settings panel**: coin, timeframe, paper/live, stake, start/stop
- **PnL display**: session PnL (large), positions worth, free USDC, PnL chart
- **Active orders**: with distance-from-mid column (color-coded)
- **Price chart**: fair value and PM prices over time
- **Quotes panel**: live bid/ask/FV for UP and DN with inventory gauge
- **Collapsible sections**: spread chart, recent fills, market quality, session stats, MM config

All data refreshes via polling every 1.5s.

---

## 14. Known Risks & Limitations

1. **Directional risk**: If only one side fills (e.g., accumulate DN but no UP), the position has directional exposure. Mitigated by inventory skew and one-sided detection.

2. **Liquidity risk**: Low-liquidity markets may have wide spreads and thin books, making it hard to exit. Mitigated by market quality checks and entry thresholds.

3. **Window expiry risk**: Tokens become worthless if the market resolves against the held position. Mitigated by closing mode, merge, and gradual liquidation.

4. **Price floor vs expiry**: Refusing to sell below cost (floor) can result in holding tokens through expiry. Mitigated by adaptive floor decay and force-sell at <5s.

5. **Latency**: REST-based order management (not WebSocket) introduces latency. Requote interval of 2s is the effective tick rate.

6. **Single-threaded**: One market at a time. No cross-market hedging.
