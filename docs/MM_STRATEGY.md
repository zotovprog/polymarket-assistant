# Polymarket Binary Options Market Maker: Full Strategy Documentation

## Table of Contents

1. [Overview](#1-overview)
2. [Architecture](#2-architecture)
3. [Fair Value Engine](#3-fair-value-engine)
4. [Quote Engine](#4-quote-engine)
5. [Order Management](#5-order-management)
6. [Inventory Management](#6-inventory-management)
7. [Risk Management](#7-risk-management)
8. [Liquidation System](#8-liquidation-system)
9. [Window Lifecycle](#9-window-lifecycle)
10. [Market Quality Analysis](#10-market-quality-analysis)
11. [Paper Trading Mode](#11-paper-trading-mode)
12. [USDC Budget Enforcement](#12-usdc-budget-enforcement)
13. [Feed System & Data Sources](#13-feed-system--data-sources)
14. [Configuration Reference](#14-configuration-reference)
15. [Edge Cases & Known Limitations](#15-edge-cases--known-limitations)

---

## 1. Overview

### What Is This Bot?

A market maker for **Polymarket binary option markets** on crypto assets (BTC, ETH, etc.). Polymarket offers short-duration (5m, 15m, 1h) binary contracts that resolve to $1 or $0 based on whether a crypto asset's price is above or below a strike price at expiry.

Each market has two tokens:
- **UP token** pays $1 if `price > strike` at expiry, $0 otherwise
- **DN token** pays $1 if `price <= strike` at expiry, $0 otherwise

Since UP + DN always = $1 (complementary), their prices are linked: `P(UP) + P(DN) = 1`.

### How the Bot Makes Money

The bot acts as a **market maker** — it simultaneously posts BUY orders for both UP and DN tokens, capturing the bid-ask spread. The core profit mechanism:

1. **Spread capture**: Buy UP at 0.48, buy DN at 0.48. If both fill, total cost = $0.96 for a guaranteed $1.00 payout at resolution = $0.04 profit.
2. **Directional profit**: If fair value changes favorably before expiry, unrealized gains from holding tokens.
3. **Maker rebates**: Polymarket's Maker Rebates Program pays USDC to qualifying market makers.

### Key Constraint: BUY-Only Strategy

On Polymarket, **SELL orders require holding the token** (you need token inventory to sell). This limits the bot to a **BUY-only quoting strategy**:

- Post `BUY UP @ bid_up` (our bid for the UP token)
- Post `BUY DN @ bid_dn` (our bid for the DN token)

`BUY DN @ P` implicitly provides the ask-side for UP at `1 - P`. When both sides fill, we hold UP+DN = guaranteed $1.00 resolution.

SELL orders are only used during **liquidation** (closing positions before expiry).

---

## 2. Architecture

### Component Diagram

```
                    Binance WebSocket
                    ├── Order Book Depth
                    ├── Klines (1m candles)
                    └── Trades
                          │
                          ▼
                    ┌─────────────┐
                    │ feeds.State  │ ← Real-time market data
                    └──────┬──────┘
                           │
            ┌──────────────┼──────────────┐
            ▼              ▼              ▼
    ┌──────────────┐ ┌──────────┐ ┌──────────────┐
    │ FairValueEng │ │ Indicators│ │ PM Price Feed│
    │              │ │ RSI, EMA  │ │ UP/DN prices │
    │ Black-Scholes│ │ OBI, CVD  │ └──────────────┘
    │ + TA Signals │ │ VWAP, MACD│
    └──────┬───────┘ └─────┬────┘
           │               │
           ▼               ▼
    ┌───────────────────────────┐
    │       QuoteEngine          │
    │  spread, skew, sizing      │
    │  USDC budget cap           │
    └─────────┬─────────────────┘
              │
              ▼
    ┌───────────────────────────┐
    │      MarketMaker           │ ← Main orchestrator
    │  _tick() loop every 2s     │
    │  window management         │
    │  liquidation coordination  │
    └──────┬────────────────────┘
           │
           ▼
    ┌───────────────┐    ┌──────────────┐
    │ OrderManager   │───│ ClobClient    │
    │ place, cancel  │   │ (real or mock)│
    │ fill tracking  │   └──────────────┘
    └───────────────┘
           │
    ┌──────┴──────┐
    │RiskManager   │
    │PnL, drawdown │
    │TP, trailing  │
    └─────────────┘
```

### File Map

| File | Responsibility |
|------|----------------|
| `web_server.py` | FastAPI server, session lifecycle, MockClobClient, API endpoints |
| `src/mm/market_maker.py` | Main orchestrator, tick loop, liquidation, window transitions |
| `src/mm/fair_value.py` | Black-Scholes digital option pricing + TA signal adjustments |
| `src/mm/quote_engine.py` | Bid/ask generation, spread, skew, USDC budget cap |
| `src/mm/order_manager.py` | Order placement/cancellation, fill detection, book queries |
| `src/mm/risk_manager.py` | PnL tracking, pause conditions, take-profit, trailing stop |
| `src/mm/types.py` | Core data types: Quote, Fill, Inventory, MarketInfo, CostBasis |
| `src/mm/mm_config.py` | All configuration parameters (runtime-adjustable) |
| `src/mm/market_quality.py` | Order book depth/spread analysis for entry/exit decisions |
| `src/mm/heartbeat.py` | CLOB heartbeat to keep orders alive |
| `src/mm/rebate_tracker.py` | Polymarket maker rebate eligibility tracking |
| `src/indicators.py` | Technical indicators: RSI, EMA, OBI, CVD, VWAP, MACD, etc. |
| `src/feeds.py` | Binance & Polymarket WebSocket data feeds |
| `src/config.py` | Global app configuration (coin list, Binance symbols, defaults) |

---

## 3. Fair Value Engine

**File:** `src/mm/fair_value.py`

### Core Model: Black-Scholes Digital Option

The UP token is priced as a digital call option:

```
P(UP) = N(d2)

where:
  d2 = (ln(S/K) - 0.5 * σ² * T) / (σ * √T)

  S = current Binance mid-price
  K = window strike price
  σ = realized volatility (per-kline, computed from log returns)
  T = time to expiry in kline units (minutes for 1m klines)
  N() = standard normal CDF
```

`P(DN) = 1 - P(UP)` (complementary binary outcome).

### Realized Volatility Calculation

```python
# From last 20 kline closes
log_returns = [ln(close[i] / close[i-1]) for i in range(1, len(closes))]
vol = stddev(log_returns)
vol = clamp(vol, floor=0.0001, cap=0.01)
```

- **vol_floor = 0.0001**: Prevents FV collapsing to 0 or 1 when price is flat
- **vol_cap = 0.01**: Prevents extreme FV swings during flash crashes
- Typical BTC per-minute vol: 0.0003-0.0006

### TA Signal Adjustments

After computing the base FV from Black-Scholes, it's adjusted by ±3% max based on 4 indicators:

| Indicator | Weight | Effect |
|-----------|--------|--------|
| **RSI** (14-period) | 30% | >50 → bullish nudge, <50 → bearish |
| **EMA Cross** (fast vs slow) | 30% | EMA separation → directional nudge |
| **OBI** (Order Book Imbalance) | 20% | Bid/ask volume ratio at mid |
| **CVD 5m** (Cumulative Volume Delta) | 20% | Net buy/sell pressure over 5 min |

```
signal_weight = 0.03  # max ±3% adjustment
adj = rsi_component + ema_component + obi_component + cvd_component
fv_up = clamp(base_fv + adj, 0.01, 0.99)
fv_dn = 1 - fv_up
```

### Behavior At Extremes

- **Price far above strike**: FV_UP → 0.99, FV_DN → 0.01 (high confidence UP wins)
- **Price far below strike**: FV_UP → 0.01, FV_DN → 0.99
- **Price at strike**: FV ≈ 0.50 (depends on vol and time)
- **Low time remaining + price near strike**: High vol makes FV more uncertain (stays near 0.50)
- **Low time remaining + price far from strike**: FV moves decisively toward 0 or 1

---

## 4. Quote Engine

**File:** `src/mm/quote_engine.py`

### Quote Generation Formula

For each token (UP and DN):

```
bid_price = FV - half_spread - inventory_skew
ask_price = FV + half_spread - inventory_skew
```

Prices are rounded to Polymarket's cent increment and clamped to [0.01, 0.99].

### Spread Calculation

```python
effective_half_spread = base_spread_bps

if current_vol > avg_vol:
    vol_ratio = current_vol / avg_vol
    if vol_ratio > volatility_pause_mult:
        effective_half_spread = max_spread_bps  # Maximum spread in extreme vol
    else:
        # Linear widening between 1x and vol_pause_mult
        mult = 1.0 + (vol_ratio - 1.0) * (vol_spread_mult - 1.0)
        effective_half_spread = base * mult

effective_half_spread = clamp(effective_half_spread, min_spread_bps, max_spread_bps)
```

Defaults: `half_spread_bps=150`, `min=50`, `max=500`, `vol_spread_mult=1.5`

100 bps = 0.01 in Polymarket price (1 cent).

### Inventory Skew

Pushes quotes to reduce net delta (directional exposure):

```python
skew_price = net_delta * skew_bps_per_unit / 10000

# net_delta = up_shares - dn_shares
# Positive delta (long UP) → negative skew → lower bid/ask
# This encourages:
#   - selling UP (ask moves down = more attractive)
#   - buying DN (bid moves down = less aggressive = less buying UP)
```

Default: `skew_bps_per_unit=30` (3 bps per share of net delta).

### Size Calculation

```python
bid_size = order_size_usd / bid_price  # in shares
ask_size = order_size_usd / ask_price  # in shares

# Cap by max_inventory_shares
bid_size = min(bid_size, max_inventory_shares)
```

Example: `order_size_usd=20`, `bid_price=0.50` → `bid_size=40 shares` ($20 notional).

### USDC Budget Cap

Added to prevent overspending session limit:

```python
if usdc_budget > 0:
    up_locked = up_shares * up_avg_entry_price
    dn_locked = dn_shares * dn_avg_entry_price
    remaining = max(0, usdc_budget - up_locked - dn_locked)
    half_remaining = remaining / 2  # Split budget between UP and DN

    # Cap UP bid
    max_up_shares = half_remaining / up_bid_price
    up_bid_size = min(up_bid_size, max_up_shares)

    # Cap DN bid
    max_dn_shares = half_remaining / dn_bid_price
    dn_bid_size = min(dn_bid_size, max_dn_shares)
```

### Book Clamping (Anti-Crossing)

Post-only orders must not cross the existing book:

```python
if bid_price >= best_ask:
    bid_price = best_ask - 0.01  # Pull back 1 tick

if ask_price <= best_bid:
    ask_price = best_bid + 0.01  # Push forward 1 tick
```

### Requoting Logic

Orders are only replaced if price moved significantly:

```python
price_diff_bps = |new_price - current_price| / current_price * 10000
should_requote = price_diff_bps >= requote_threshold_bps  # default: 50 bps
```

Minimum order notional check: orders below `min_order_size_usd` ($2) are not placed.

---

## 5. Order Management

**File:** `src/mm/order_manager.py`

### Order Types

- **Post-only (maker)**: Default for quoting. 0% fee. Rejected if would cross book.
- **GTD (Good-Til-Date)**: Auto-expires after `gtd_duration_sec` (default: 55s). Used to avoid stale orders.
- **GTC (Good-Til-Cancelled)**: For liquidation taker orders.

### Order Lifecycle

```
generate_quote() → place_order() → active → check_fills() → fill detected → remove
                                     ↓
                              cancel_order() → removed
```

### Collateral Model

```python
BUY order collateral = size * price        # USDC locked
SELL order collateral = size * (1 - price)  # USDC locked as guarantee
```

Example: BUY 40@0.50 = $20 USDC locked. SELL 40@0.60 = $16 USDC locked.

### Fill Detection

Orders are polled via `get_order(order_id)`:
- Status `MATCHED` or `CLOSED` with `size_matched > 0` → fill
- Status `CANCELLED` or `EXPIRED` → remove from tracking

For mock mode, fills also update `_mock_token_balances` to simulate real PM balances.

### Retry Logic

All API calls use exponential backoff retry (3 attempts, 0.5s base delay, 10s timeout):

```python
for attempt in range(3):
    try:
        result = await asyncio.wait_for(call(), timeout=10.0)
    except (TimeoutError, Exception):
        await asyncio.sleep(0.5 * 2^attempt)
```

### Insufficient Balance Handling

If `post_order` returns "not enough balance":
1. Reduce order size by 10% (`size * 0.9`)
2. Retry once with reduced size
3. If still fails or size < 1.0, give up

### Sell Allowance

Before placing SELL orders (live mode only), `ensure_sell_allowance()` calls PM's `update_balance_allowance()` to set ERC1155 operator approval. This is idempotent and cached per token_id.

---

## 6. Inventory Management

**File:** `src/mm/types.py`

### Inventory State

```python
@dataclass
class Inventory:
    up_shares: float = 0.0      # Current UP token holdings
    dn_shares: float = 0.0      # Current DN token holdings
    usdc: float = 0.0           # Tracked USDC balance
    initial_usdc: float = 0.0   # Session budget limit
    up_cost: CostBasis           # Average entry tracking for UP
    dn_cost: CostBasis           # Average entry tracking for DN
```

### Net Delta

```python
net_delta = up_shares - dn_shares
# Positive → long UP (bullish exposure)
# Negative → long DN (bearish exposure)
# Zero → delta-neutral
```

### Cost Basis Tracking

Weighted average entry price, updated on each fill:

```python
# On BUY:
total_cost += price * size + fee
total_shares += size
avg_entry = total_cost / total_shares

# On SELL:
fraction = size / total_shares  # proportional reduction
total_cost *= (1 - fraction)
total_shares -= size
```

This gives accurate cost basis for PnL calculation and liquidation floor pricing.

### Fill Processing

When a fill is detected:

```python
inventory.update_from_fill(fill, token_type="up"|"dn")
risk_mgr.record_fill(fill)  # For PnL tracking
```

BUY fills: increase shares, decrease USDC.
SELL fills: decrease shares, increase USDC.

### Reconciliation (Live Mode)

Every 5 ticks, live mode fetches actual PM balances and reconciles:

```python
real_up, real_dn = await order_mgr.get_all_token_balances(up_id, dn_id)
cached_usdc = await order_mgr.get_usdc_balance()

if abs(real_up - internal_up) > 1.0 or abs(real_dn - internal_dn) > 1.0:
    inventory.reconcile(real_up, real_dn, cached_usdc)
```

This handles edge cases where fills were missed or internal state drifted.

---

## 7. Risk Management

**File:** `src/mm/risk_manager.py`

### PnL Calculation

```python
realized_pnl = sum(SELL.notional - SELL.fee) - sum(BUY.notional + BUY.fee)
unrealized_pnl = up_shares * fv_up + dn_shares * fv_dn
total_pnl = realized_pnl + unrealized_pnl
```

### Pause Conditions

The bot pauses quoting (cancels all orders, stops placing new ones) when:

| Condition | Default Threshold | Behavior |
|-----------|-------------------|----------|
| **Inventory limit** | `max_inventory_shares=50` | Pause if `up_shares` or `dn_shares` exceeds limit |
| **Max drawdown** | `max_drawdown_usd=5.0` | Pause if `total_pnl < -$5` |
| **Volatility spike** | `volatility_pause_mult=3.0` | Pause if `current_vol > 3x avg_vol` |
| **Config disabled** | `enabled=False` | Master kill switch |

### Take-Profit (TP)

When `total_pnl >= take_profit_usd`:
- Does NOT just pause — triggers **closing mode** with liquidation
- Creates `LiquidationLock` snapshot
- Transitions to sell-down inventory

Default: `take_profit_usd=0` (disabled).

### Trailing Stop

Activated only after peak PnL reaches a meaningful threshold:

```python
min_peak = max($2.0, take_profit_usd * 0.25)

if peak_pnl >= min_peak:
    trail_threshold = peak_pnl * (1 - trailing_stop_pct)
    if total_pnl < trail_threshold:
        trigger closing mode
```

Default: `trailing_stop_pct=0.0` (disabled). Typical: `0.3` (exit if PnL drops 30% from peak).

Example: TP=$5, peak PnL=$4 (>$1.25 min), trailing=30%. If PnL drops to $2.80 ($4 * 0.7), trailing stop triggers.

### LiquidationLock

Snapshot taken at the moment any exit trigger fires:

```python
@dataclass
class LiquidationLock:
    triggered_at: float        # Unix timestamp
    trigger_pnl: float         # PnL at trigger time
    up_avg_entry: float        # Cost basis UP at trigger
    dn_avg_entry: float        # Cost basis DN at trigger
    min_sell_price_up: float   # Floor = avg_entry + margin (1 cent)
    min_sell_price_dn: float   # Floor = avg_entry + margin
```

This lock ensures liquidation won't sell below cost basis, preserving the PnL that triggered the exit.

---

## 8. Liquidation System

**File:** `src/mm/market_maker.py` — `_liquidate_inventory()`

### 3-Phase Smart Liquidation

When the bot enters closing mode, it sells all inventory through 3 phases:

```
Phase 1: Gradual Limit     Phase 2: Taker        Phase 3: Force/Abandon
(time_left > 20s)          (time_left <= 20s)     (best_bid < floor)
┌──────────────────────┐  ┌───────────────────┐  ┌───────────────────┐
│ Sell in chunks        │  │ Sell all at        │  │ If >5s: wait      │
│ Post-only (maker)     │→ │ best_bid (taker)   │→ │ If <=5s: force    │
│ Price = max(floor,    │  │ Only if bid>=floor │  │   sell at best_bid│
│   FV - discount)      │  │                    │  │ Ignores floor     │
│ Chunk interval: 5s    │  │ Full remaining     │  │                   │
└──────────────────────┘  └───────────────────┘  └───────────────────┘
```

### Phase 1: Gradual Limit Orders

- Sells in `liq_gradual_chunks` (default: 3) chunks
- Each chunk = `(real_balance - 0.5) / remaining_chunks` shares
- The `-0.5` buffer prevents residual from being too small (PM minimum ~5 shares)
- Price = `max(floor, FV - liq_max_discount_from_fv)`, improved to `best_bid` if higher
- Post-only (0% fee)
- Chunks placed every `liq_chunk_interval_sec` (default: 5s)
- Stale limit orders are cancelled and re-placed with updated pricing

### Phase 2: Taker

Triggers when `time_left <= liq_taker_threshold_sec` (default: 20s):

1. Cancel all existing limit orders
2. Sell everything at `best_bid` (crosses book, pays taker fee)
3. Only proceeds if `best_bid >= floor`
4. No `-0.5` buffer — sells full `real_balance`

### Phase 3: Force-Sell / Abandon

When `best_bid < floor` (selling would lose money vs cost basis):

- **If `time_left > 5s`**: Wait. Log warning. Hope price recovers.
- **If `time_left <= 5s`**: **Force sell** at `best_bid` regardless of floor. This prevents frozen tokens (which are worthless if they expire unresolved or resolve to $0).

The force-sell override replaced the old "abandon" behavior that would let tokens expire, which could freeze significant capital.

### Minimum Sell Size

```python
min_sell = max(0.5 shares, $0.50 / sell_price)
```

At price 0.50 → min 1.0 share ($0.50 notional).
At price 0.10 → min 5.0 shares ($0.50 notional).
At price 0.01 → min 50 shares ($0.50 notional).

This prevents trying to sell quantities too small for PM to accept.

### Closing Mode Triggers

Five conditions trigger closing mode (all create `LiquidationLock`):

| Trigger | Code Location | Description |
|---------|---------------|-------------|
| **Time-based** | `time_left <= close_sec` | Default: 120s before expiry (adaptive: min of config and 40% of window) |
| **Window expired** | `time_left <= 0` | Market expired, forced liquidation |
| **Take-profit** | `total_pnl >= TP` | Profit target hit |
| **Trailing stop** | `PnL < peak * (1-pct)` | PnL dropped from peak |
| **Quality exit** | `liquidity_score < threshold` | Book dried up with open inventory |

All 5 triggers:
1. Set `_is_closing = True`
2. Create `LiquidationLock` (snapshot prices)
3. Cancel all existing orders
4. Begin `_liquidate_inventory()` each tick

---

## 9. Window Lifecycle

### Window = One Trading Session

A "window" is a single binary option market with:
- Coin (BTC, ETH)
- Timeframe (5m, 15m, 1h)
- Strike price (e.g., $97,500)
- Start/end timestamps
- UP and DN token IDs

### Full Lifecycle

```
1. START
   ├── Fetch PM tokens (UP/DN IDs, strike)
   ├── Wait for Binance data
   ├── Create CLOB client (mock or real)
   ├── Initialize MarketMaker
   └── Begin tick loop

2. ACTIVE TRADING (tick loop, every 2s)
   ├── Check time remaining
   ├── Check fills
   ├── Reconcile balances (live, every 5 ticks)
   ├── Compute fair value
   ├── Sync FV to mock client (paper mode)
   ├── Check risk limits (pause/TP/trailing)
   ├── Check market quality (live, every 5 ticks)
   ├── Generate quotes (with USDC cap)
   ├── Clamp quotes to book
   └── Place/update orders

3. CLOSING MODE
   ├── Cancel all quoting orders
   ├── Run liquidation each tick
   └── MM stops when inventory = 0 or window expired

4. WINDOW TRANSITION (auto_next_window=True)
   ├── Stop heartbeat
   ├── Poll for new tokens (every 10s, max 90s wait)
   ├── Pre-entry market quality check (skip 3 bad windows max)
   ├── Reset: cost basis, liq lock, order state
   └── Restart with new market info
```

### Auto-Next-Window

When enabled (`auto_next_window=True`):
1. After window expiry, wait 15s minimum cooldown
2. Poll PM API for new tokens every 10s (up to `resolution_wait_sec` = 90s)
3. Once tokens found, perform **pre-entry quality check**:
   - Fetch UP and DN order books
   - Analyze depth, spread, liquidity
   - If `tradeable=False`, skip this window (up to 3 consecutive skips)
4. On quality pass: reset state, create new market, start trading

### Window Transition State Reset

On `on_window_transition()`:
- Cancel all orders
- Reset cost basis (`up_cost.reset()`, `dn_cost.reset()`)
- Clear `_liq_lock`, chunk index, chunk timer
- Clear `_is_closing` flag
- Clear liquidation order IDs
- Set new `MarketInfo`

Note: `inventory.up_shares` and `inventory.dn_shares` are **NOT** reset — any residual tokens from previous window carry over (they may still have value or resolve).

---

## 10. Market Quality Analysis

**File:** `src/mm/market_quality.py`

### Quality Score Computation

```python
spread_score:
    spread <= 200 bps → 1.0
    spread >= 1000 bps → 0.0
    between → linear interpolation

liquidity_score:
    total_depth / (order_size_usd * 3)  # capped at 1.0

overall_score = 0.4 * spread_score + 0.6 * liquidity_score
```

### Entry Conditions

A window is `tradeable` if ALL pass:

| Check | Default |
|-------|---------|
| `overall_score >= min_market_quality_score` | 0.3 |
| `total_depth >= min_entry_depth_usd` | $50 |
| `spread <= max_entry_spread_bps` | 800 bps |

### Exit Conditions

While trading, checked every `quality_check_interval` ticks (default: 5, i.e., every 10s at 2s tick):

```python
if has_inventory and liquidity_score < exit_liquidity_threshold:
    trigger early exit with liquidation
```

Default `exit_liquidity_threshold = 0.15` (exit if book depth drops to 15% of required).

This protects against being stuck with inventory when the book dries up — a common scenario on thin PM markets.

---

## 11. Paper Trading Mode

**File:** `web_server.py` — `MockClobClient`

### Overview

Paper mode uses `MockClobClient` instead of the real PM CLOB client. It simulates:
- Order placement with USDC tracking
- Price-dependent fill probability
- Partial fills
- Simulated order book

### Price-Dependent Fill Probability

```python
def _compute_fill_prob(order):
    fv = fair_values[token_id]  # synced from MarketMaker each tick
    distance = |price - fv| / fv

    if BUY:
        if price >= fv: base = 0.80    # aggressive buy fills often
        else: base = max(0.02, 0.15 * (1 - distance * 5))

    if SELL:
        if price <= fv: base = 0.80    # aggressive sell fills often
        else: base = max(0.02, 0.15 * (1 - distance * 5))

    age_mult = min(2.0, 1.0 + age * 0.08)  # older orders more likely
    return min(0.85, base * age_mult)
```

Examples:
- BUY@0.48 when FV=0.50 → base≈0.09 (9%, far from FV)
- BUY@0.50 when FV=0.50 → base=0.80 (80%, at FV)
- SELL@0.55 when FV=0.50 → base≈0.04 (4%, far from FV)
- SELL@0.48 when FV=0.50 → base=0.80 (80%, below FV = aggressive)

### Partial Fills

On fill, a random 50-100% of the order is filled:

```python
fill_frac = random.uniform(0.5, 1.0)
fill_size = remaining * fill_frac

if fill_size < 0.5:
    fill_size = remaining  # Fill the rest if too small to split
```

Partially filled orders remain LIVE and can fill more on subsequent ticks.

### Simulated Order Book

`get_order_book()` generates 5 levels of bids and asks around current FV:

```python
for i in range(5):
    bid_price = fv - 0.01 * (i + 1)   # 1-5 cents below FV
    ask_price = fv + 0.01 * (i + 1)   # 1-5 cents above FV
    size = random.uniform(10, 50)       # 10-50 shares per level
```

This provides `best_bid` and `best_ask` for book clamping and liquidation pricing.

### USDC Balance Tracking

- Initial balance set via `initial_usdc` parameter
- BUY orders deduct `size * price` on placement
- Cancelled BUY orders refund collateral
- SELL fills credit `size_matched * price`
- `cancel_all()` refunds all live BUY order collateral

### FV Synchronization

Each tick, `MarketMaker._tick()` syncs computed fair values to the mock client:

```python
if hasattr(self.order_mgr.client, 'set_fair_values'):
    self.order_mgr.client.set_fair_values(fv_up, fv_dn, self.market)
```

This ensures mock fills and order book are priced relative to current market conditions.

---

## 12. USDC Budget Enforcement

### Problem Solved

Without budget enforcement, the bot could spend far more than `initial_usdc`. Example: $10 budget but $37 spent, leaving $20+ in frozen tokens.

### Enforcement Points

1. **Quote Engine** (`generate_all_quotes()`): Caps BID sizes so total position cost doesn't exceed `usdc_budget`
2. **Order Manager** (`place_order()`): Logs warning if collateral exceeds available USDC (doesn't block, since reconcile may be stale)
3. **Mock Client** (`post_order()`): Rejects BUY orders if `usdc_balance < collateral`

### Budget Calculation

```python
up_locked = up_shares * up_avg_entry_price
dn_locked = dn_shares * dn_avg_entry_price
remaining = usdc_budget - up_locked - dn_locked
half_remaining = remaining / 2  # split between UP and DN bids

# Each bid capped: bid_size * bid_price <= half_remaining
max_shares = half_remaining / bid_price
```

The budget splits remaining capital equally between UP and DN to maintain balanced quoting.

### Critical Implementation Detail

`initial_usdc` must be set for **both** paper and live modes:

```python
# CORRECT — in web_server.py start():
self.mm.inventory.initial_usdc = initial_usdc  # ALWAYS set
if paper_mode:
    self.mm.inventory.usdc = initial_usdc  # Only set tracked balance in paper
```

If `initial_usdc` stays at 0 in live mode, `usdc_budget=0` disables the cap entirely.

---

## 13. Feed System & Data Sources

### Binance Feeds

Two concurrent feeds:

1. **Order Book Poller** (`ob_poller`): REST depth snapshots for bid/ask lists
2. **WebSocket Feed** (`binance_feed`): Real-time klines (1m candles), trades, mid-price

Feed state (in `feeds.State`):
- `mid`: Current mid-price
- `bids`: List of (price, qty) bid levels
- `asks`: List of (price, qty) ask levels
- `trades`: Recent trades with timestamp, price, qty, is_buy
- `klines`: List of OHLCV candles

### Polymarket Price Feed

Subscribes to PM WebSocket for real-time UP/DN token prices:
- `pm_up`: Last UP token price
- `pm_dn`: Last DN token price

### Market Discovery

`feeds.fetch_pm_tokens(coin, timeframe)`: Queries PM API for current window's UP/DN token IDs.
`feeds.fetch_pm_strike(coin, timeframe)`: Gets strike price and window start/end times.

### Heartbeat

**File:** `src/mm/heartbeat.py`

Polymarket auto-cancels orders if no heartbeat received within ~60s. The `HeartbeatManager` sends heartbeats every 55s (configurable).

```python
while running:
    client.post_heartbeat(heartbeat_id)  # Stable UUID per session
    await asyncio.sleep(55)
```

---

## 14. Configuration Reference

**File:** `src/mm/mm_config.py`

All parameters are runtime-adjustable via API (`PATCH /api/mm/config`).

### Spread Parameters

| Parameter | Default | Description |
|-----------|---------|-------------|
| `half_spread_bps` | 150 | Base half-spread in basis points |
| `min_spread_bps` | 50 | Absolute minimum half-spread |
| `max_spread_bps` | 500 | Absolute maximum half-spread |
| `vol_spread_mult` | 1.5 | Spread widening factor in high vol |

### Sizing Parameters

| Parameter | Default | Description |
|-----------|---------|-------------|
| `order_size_usd` | 5.0 | Notional size per order in USDC |
| `min_order_size_usd` | 2.0 | Below this, don't quote |
| `max_order_size_usd` | 100.0 | Maximum order notional |

### Inventory Parameters

| Parameter | Default | Description |
|-----------|---------|-------------|
| `max_inventory_shares` | 50 | Max shares per token (UP or DN) |
| `skew_bps_per_unit` | 30 | Price skew per share of net delta |

### Requoting Parameters

| Parameter | Default | Description |
|-----------|---------|-------------|
| `requote_interval_sec` | 2.0 | Tick interval (seconds between quote updates) |
| `requote_threshold_bps` | 50 | Min price change to trigger requote |

### Order Type Parameters

| Parameter | Default | Description |
|-----------|---------|-------------|
| `gtd_duration_sec` | 55 | GTD order expiry (seconds) |
| `heartbeat_interval_sec` | 55 | Heartbeat interval |
| `use_post_only` | True | Post-only (maker) orders |
| `use_gtd` | True | Good-Til-Date order type |

### Risk Parameters

| Parameter | Default | Description |
|-----------|---------|-------------|
| `max_drawdown_usd` | 5.0 | Pause if total PnL drops below this |
| `volatility_pause_mult` | 3.0 | Pause if vol exceeds this × average |
| `max_loss_per_fill_usd` | 5.0 | Max acceptable loss per single fill |
| `take_profit_usd` | 0.0 | Exit at this PnL (0=disabled) |
| `trailing_stop_pct` | 0.0 | Exit if PnL drops this fraction from peak |

### Liquidation Parameters

| Parameter | Default | Description |
|-----------|---------|-------------|
| `liq_price_floor_enabled` | True | Don't sell below avg entry |
| `liq_price_floor_margin` | 0.01 | Min margin above cost basis ($0.01) |
| `liq_gradual_chunks` | 3 | Split liquidation into N chunks |
| `liq_chunk_interval_sec` | 5.0 | Seconds between chunks |
| `liq_taker_threshold_sec` | 20.0 | Switch to taker below this time |
| `liq_max_discount_from_fv` | 0.03 | Max discount from FV for limit sell |
| `liq_abandon_below_floor` | True | If True: wait/force-sell instead of selling below floor |

### Window Management Parameters

| Parameter | Default | Description |
|-----------|---------|-------------|
| `close_window_sec` | 120.0 | Enter closing mode N seconds before expiry |
| `auto_next_window` | True | Auto-start next window after resolution |
| `resolution_wait_sec` | 90.0 | Max wait for new tokens after expiry |

### Market Quality Parameters

| Parameter | Default | Description |
|-----------|---------|-------------|
| `min_market_quality_score` | 0.3 | Min quality to enter window |
| `min_entry_depth_usd` | 50.0 | Min book depth to enter |
| `max_entry_spread_bps` | 800.0 | Max spread to enter |
| `exit_liquidity_threshold` | 0.15 | Exit if liquidity score drops below |
| `quality_check_interval` | 5 | Check quality every N ticks |

---

## 15. Edge Cases & Known Limitations

### Residual Tokens After Liquidation

PM has minimum order sizes (~5 shares). After liquidation, 0.5-1.0 shares may remain as residual. These are intentional (the `-0.5` buffer in gradual limit phase) and typically worth <$0.50.

### Post-Only Crossed Book During Liquidation

When placing limit SELL during liquidation, the book may move between quote generation and placement, causing "crosses book" errors. These are logged as warnings and retried next tick (every 2s).

### Inventory Reconciliation Drift

Internal inventory tracking can drift from actual PM balances due to:
- Missed fills (network issues)
- Partial fills not fully tracked
- Token resolution between ticks

Reconciliation every 5 ticks (10s) corrects this, but 1-share tolerance means small discrepancies persist.

### USDC Budget Split Limitations

The budget is split 50/50 between UP and DN bids. If the market is strongly directional, one side may fill quickly while the other sits unfilled, effectively using only 50% of capital. This is a design trade-off for simplicity — a more sophisticated approach could dynamically allocate based on fill rates.

### Extreme FV (Near 0 or 1)

When FV approaches 0.01 or 0.99:
- Spread becomes meaningless (bid at 0.01 = no room for spread)
- DN/UP bid becomes very cheap but also very risky
- The bot naturally reduces exposure here because bid prices are low and fill probability is low

### Flash Crashes / Volatility Spikes

The vol cap (`vol_cap=0.01`) prevents FV from swinging wildly, but the `volatility_pause_mult` (3x avg vol) will pause quoting during extreme moves. This is protective but means the bot misses potential opportunities during high-vol periods.

### Window Transition Token Availability

After a window expires, new tokens may not be immediately available on PM. The polling mechanism (10s intervals, 90s max) handles this, but if PM is slow to create new markets, the bot will skip that window.

### Heartbeat Failure

If heartbeats fail repeatedly, PM will cancel all orders (safety feature). The bot tracks `error_count` but does not currently auto-restart on persistent heartbeat failures.

### Paper vs Live Discrepancies

Paper mode simulates fills probabilistically — actual PM fills depend on real order book depth, other traders' activity, and exact timing. Paper PnL may differ significantly from live results, especially:
- High fill rates in paper vs low liquidity in live
- No slippage in paper vs real slippage in live
- Mock book always has 5 levels vs potentially empty real book
