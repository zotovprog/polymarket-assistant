# Polymarket Crypto Assistant

Real-time terminal dashboard that combines live Binance order flow with Polymarket prediction market prices to surface actionable crypto signals.

**By [@SolSt1ne](https://x.com/SolSt1ne)**

**[Polymarket](https://polymarket.com/?via=SolSt1ne)**
---

## What it does

- Streams live trades and orderbook from **Binance**
- Fetches Up/Down contract prices from **Polymarket** via WebSocket
- Calculates 11 indicators across orderbook, flow, and technical analysis
- Aggregates everything into a single **BULLISH / BEARISH / NEUTRAL** trend score
- Renders the full dashboard in the terminal with live refresh

---

## Supported coins & timeframes

| Coins | Timeframes |
|-------|------------|
| BTC, ETH, SOL, XRP | 5m, 15m, 1h, 4h, daily |

All 16 coin × timeframe combinations are supported on Polymarket.

---

## Indicators

**Order Book**
- OBI (Order Book Imbalance)
- Buy / Sell Walls
- Liquidity Depth (0.1% / 0.5% / 1.0%)

**Flow & Volume**
- CVD (Cumulative Volume Delta) — 1m / 3m / 5m
- Delta (1m)
- Volume Profile with POC

**Technical Analysis**
- RSI (14)
- MACD (12/26/9) + Signal + Histogram
- VWAP
- EMA 5 / EMA 20 crossover
- Heikin Ashi candle streak

---

## Setup

```bash
pip install -r requirements.txt
python main.py
```

## Web Interface (Local Server)

This build includes a local web UI with:

- live market/indicator dashboard
- session-scoped live env inputs (`PM_PRIVATE_KEY`, `PM_FUNDER`, `PM_SIGNATURE_TYPE`)
- live preflight checks on start (key format, signature type, API collateral read test)
- manual approve/reject buttons for pending live trades
- frontend sound alert + toast notifications for pending trades and start/runtime issues
- compact English market summary at the top (current regime snapshot)
- trade history + runtime logs in browser

Access control:

- web API is protected by an access key
- set it with env: `PM_WEB_ACCESS_KEY=...` (required)
- browser must unlock once via key prompt; after that auth cookie is used

Safety behavior in live mode:

- on `Stop`, server attempts emergency flatten (close open position) before shutting session tasks
- if client heartbeat disappears for too long, server blocks new entries and requests emergency close
- tunable by env:
  - `PM_FLATTEN_ON_STOP=1` (default)
  - `PM_CLIENT_IDLE_FLATTEN=1` (default)
  - `PM_CLIENT_IDLE_FLATTEN_SEC=45` (default)
  - `PM_FLATTEN_TIMEOUT_SEC=25` (default)

Run locally:

```bash
export PM_WEB_ACCESS_KEY='your-long-random-key'
pip install -r requirements.txt
pip install -r requirements-trading.txt
uvicorn web_server:app --host 0.0.0.0 --port 8000
```

Open:

```text
http://localhost:8000
```

Important:

- live env values are kept in-memory per browser session (not exported to shell)
- web mode keeps manual approvals enabled by default
- live start requires token: `I_UNDERSTAND_REAL_MONEY_RISK`

### Docker

```bash
export PM_WEB_ACCESS_KEY='your-long-random-key'
docker compose up --build
```

### Railway

In Railway project variables add:

- `PM_WEB_ACCESS_KEY` = your random secret key

After deploy, open app URL and enter this key in auth dialog.

Then open:

```text
http://localhost:8000
```

## Trading Modes (Cautious Defaults)

The app supports three modes:

- `observe` (default): analytics dashboard only, no orders
- `paper`: simulated bets (no real money)
- `live`: real orders to Polymarket CLOB (guarded by explicit safety checks)

### Paper trading

```bash
python main.py --paper --size-usd 5 --tp-pct 15 --sl-pct 8 --max-hold-sec 900
```

### Live trading (requires extra dependency + explicit arming)

```bash
pip install -r requirements-trading.txt
export PM_ENABLE_LIVE=1
export PM_PRIVATE_KEY=...
export PM_FUNDER=...
# optional:
# export PM_SIGNATURE_TYPE=0   # EOA/MetaMask/private key wallet (default)
# export PM_SIGNATURE_TYPE=1   # Magic/email wallet
# export PM_SIGNATURE_TYPE=2   # browser proxy wallet
python main.py --live --confirm-live-token I_UNDERSTAND_REAL_MONEY_RISK --size-usd 5
```

### One-Click Presets (safe / medium / aggressive)

Preset launcher scripts are included:

- `safe.sh`
- `medium.sh`
- `aggressive.sh`
- `run_preset.sh` (generic launcher)

Web preset note:

- `SUPER AGGRESSIVE` exists only in web UI and is paper-only by design.

Quick start:

```bash
bash safe.sh              # safe preset in paper mode (SIZE_USD=5 by default)
bash medium.sh live       # medium preset in live mode
bash aggressive.sh live   # aggressive preset in live mode
AUTO_APPROVE=1 bash medium.sh live   # medium live without manual approve
```

Optional overrides (examples):

```bash
COIN=BTC TIMEFRAME=15m SIZE_USD=5 bash safe.sh live
CONTROL_FILE=/tmp/pm_traderctl bash medium.sh
EXEC_LOG_FILE=/tmp/pm_execs.jsonl bash medium.sh live
```

Notes:

- default mode is `paper`; pass `live` explicitly for real orders
- default timeframe in preset launcher is `15m` (override with `TIMEFRAME=...`)
- minimum order size is `5 USD` (enforced by app)
- live mode requires `PM_ENABLE_LIVE=1`, `PM_PRIVATE_KEY`, `PM_FUNDER`
- on some external drives direct `./safe.sh` may be blocked; use `bash safe.sh`
- set `AUTO_APPROVE=1` to auto-approve entries for preset runs
- successful operations (only) are appended to `executions.log.jsonl` by default

Safety gates in live mode:

- live mode is blocked unless `PM_ENABLE_LIVE=1`
- live mode is blocked unless `--confirm-live-token I_UNDERSTAND_REAL_MONEY_RISK`
- strict defaults: high signal thresholds, cooldown, low trade cap/day
- every live bet requires manual approval (`approve`) before sending order
- optional: `--auto-approve` disables manual approval gate
- entry is considered open only after fill check (`get_order`) confirms execution
- unfilled entry is auto-cancelled after timeout (configurable)
- open positions are auto-exited by TP/SL/time-stop/reversal (configurable)

### Runtime trader commands (without restart)

When `paper` or `live` mode is enabled, trader listens for commands in `.traderctl`
(or custom path via `--control-file`):

```bash
echo "status"  > .traderctl   # show trader state
echo "approve" > .traderctl   # approve current pending live bet
echo "reject"  > .traderctl   # reject current pending live bet
echo "close"   > .traderctl   # force close current open position now
echo "reset"   > .traderctl   # clear local open position state and pending decision
echo "help"    > .traderctl   # print command help in app logs
```

You can also type commands directly in the same running terminal (press Enter):

```text
y  -> approve
n  -> reject
s  -> status
c  -> close
r  -> reset
h  -> help
```

When a new `pending live trade` appears, the app emits a sound by default.
On macOS, `afplay /System/Library/Sounds/Glass.aiff` is used automatically.
Disable it with:

```bash
--disable-approval-beep
```

Set an explicit sound command (optional):

```bash
--approval-sound-command "afplay /System/Library/Sounds/Glass.aiff"
```

### Executions Log (successful operations only)

Each successful entry/exit is appended as one JSON line:

```bash
tail -f executions.log.jsonl
```

You can change the file path:

```bash
--executions-log-file /path/to/executions.log.jsonl
```

### Auto-exit parameters

- `--tp-pct`: take-profit threshold from entry (default `15`)
- `--sl-pct`: stop-loss threshold from entry (default `8`)
- `--max-hold-sec`: max hold time before time-stop exit (default `900`)
- `--reverse-exit-bias`: exit when bias strongly reverses (default `60`)
- `--disable-auto-exit`: disable all automatic exits
- `--disable-reverse-exit`: keep TP/SL/time-stop but disable reverse-bias exit

### Live fill-control parameters

- `--entry-fill-timeout-sec`: wait time for entry fill confirmation (default `20`)
- `--entry-fill-poll-sec`: order-status polling interval (default `1.0`)
- `--allow-posted-entry`: treat posted order as open without fill confirmation (less safe)
- `--keep-unfilled-entry-open`: do not auto-cancel unfilled entry after timeout

---

## Project structure

```
arbi-pred/
├── web/                   # frontend (HTML/CSS/JS)
├── src/
│   ├── config.py          # all constants — coins, URLs, indicator params
│   ├── feeds.py           # Binance + Polymarket data feeds
│   ├── indicators.py      # pure indicator calculations
│   ├── dashboard.py       # Rich terminal UI & trend scoring
│   └── trading.py         # paper/live execution + risk guardrails
├── main.py                # entry point — menu & async orchestration
├── web_server.py          # FastAPI local backend for web UI
├── run_preset.sh          # preset launcher (safe/medium/aggressive)
├── safe.sh                # safe preset wrapper
├── medium.sh              # medium preset wrapper
├── aggressive.sh          # aggressive preset wrapper
├── Dockerfile
├── docker-compose.yml
├── requirements.txt       # Python dependencies
├── requirements-trading.txt # optional trading dependency
└── README.md
```
