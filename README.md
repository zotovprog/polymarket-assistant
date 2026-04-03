# Polymarket Assistant (MM V2 + Pair Arb)

## Current Status

This repository contains two live-capable strategies for Polymarket crypto Up/Down markets:

- `MM V2` (maker-first market making)
- `Pair Arb` (two-leg buy + merge/redeem flow)

Infrastructure is mature (feeds, order manager, CLOB integration, on-chain operations), but **strategy profitability is not consistently stable yet** on fast crypto markets (especially `5m` windows). Treat this as **research/verification code**, not unattended production trading.

## What Is Implemented

### 1) MM V2

MM V2 runs a state machine with explicit risk regimes:

- `normal`
- `inventory_skewed`
- `defensive`
- `unwind`
- `emergency_unwind` / terminal liquidation

Core behavior:

- maker-first quoting (`post_only`) in normal operation
- midpoint-first valuation with bounded model influence
- inventory-aware skew, drawdown controls, edge guards
- terminal liquidation path near expiry
- extensive runtime analytics and failure bucket classification

Key modules:

- `src/mm_v2/runtime.py`
- `src/mm_v2/quote_policy.py`
- `src/mm_v2/risk_kernel.py`
- `src/mm_v2/state_machine.py`
- `src/mm_v2/pair_valuation.py`

### 2) Pair Arb

Pair Arb scans selected scopes (e.g. `BTC_5m`), tries to place both legs safely, and then merge/redeem.

Core behavior:

- maker-mode leg placement with balance and orphan cleanup rules
- risk caps on exposure and drawdown
- merge/redeem support, including Safe-based execution helpers

Key modules:

- `src/pair_arb/engine.py`
- `src/pair_arb/maker.py`
- `src/pair_arb/merger.py`
- `src/mm_shared/order_manager.py`
- `src/mm_shared/safe_exec.py`

## API / UI

Single FastAPI server + dashboard:

- entrypoint: `web_server.py`
- frontend: `web`

Main endpoints:

- `/api/mmv2/start`, `/api/mmv2/stop`, `/api/mmv2/state`
- `/api/mmv2/paper-sweep/start`, `/api/mmv2/paper-sweep/state`
- `/api/pair-arb/start`, `/api/pair-arb/stop`, `/api/pair-arb/state`
- `/api/pair-arb/redeem`, `/api/pair-arb/redeem-all`, `/api/pair-arb/liquidate-positions`

## Security and Sensitive Data Policy

### Required rules

- Never commit `.env`, `.web_access_key`, raw Mongo URIs, or private keys.
- Keep secrets only in runtime env/CI variables.
- Keep generated local artifacts (`audit`, datasets, task dumps, codex outputs) out of git.

Current ignore list includes:

- `.env`, `.env.*`, `.web_access_key`
- `audit/`
- `data/raw/`, `data/normalized/`, large replay datasets
- `tasks/`, `codex_outputs/`

If history was ever contaminated, rewrite git history and force-push cleaned refs.

## Setup

### 1) Install

```bash
cd polymarket-assistant
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

### 2) Configure env

Copy `.env.example` and set values.

Minimum for local API:

- `PM_WEB_ACCESS_KEY`

For live trading you also need:

- `PM_PRIVATE_KEY`
- `PM_FUNDER`
- `PM_API_KEY`
- `PM_API_SECRET`
- `PM_API_PASSPHRASE`

### 3) Run server

```bash
PM_WEB_ACCESS_KEY='your-key' uvicorn web_server:app --host 127.0.0.1 --port 8000
```

Dashboard:

- [http://127.0.0.1:8000](http://127.0.0.1:8000)

## Testing

### Unit/Regression (same suite as CI)

```bash
python -m pytest -q \
  tests/test_mm_v2.py \
  tests/test_mm_v2_pnl_accounting.py \
  tests/test_mm_v2_quote_skew.py \
  tests/test_mm_v2_inventory_modes.py \
  tests/test_mm_v2_runtime_skew_flow.py \
  tests/test_mm_v2_sell_release_lag.py \
  tests/test_mm_v2_paper.py \
  tests/test_mm_v2_paper_multiwindow.py \
  tests/test_mm_v2_replay.py \
  tests/test_pair_arb.py \
  tests/test_safe_exec.py
```

### Dataset-first dev gate

Quick gate:

```bash
python tools/mmv2_dev_gate.py --mode quick
```

Full gate (includes local paper stage):

```bash
python tools/mmv2_dev_gate.py --mode full
```

### Local paper verification

```bash
python tools/mmv2_local_paper_check.py \
  --base-url http://127.0.0.1:8000 \
  --timeframe 15m \
  --budget 300 \
  --duration-sec 2700 \
  --poll-sec 5 \
  --stop-at-end
```

## Why It Still Does Not Work As Expected

Based on the latest research/audits and live-paper artifacts, the main blockers are strategy-level, not infra-level.

### 1) Adverse selection in fast PM crypto windows

Even with midpoint-first anchoring, fills can be toxic when microstructure shifts quickly (especially `5m` markets). Quotes get picked off before the bot re-centers edge.

### 2) Marketability/execution churn

A repeated pattern in bad windows:

- collateral warnings
- cancel/repost loops
- sell skip cooldown streaks

This causes `marketability_churn` as primary failure bucket and can degrade PnL before terminal cleanup.

### 3) Inventory regime stress

When inventory stress escalates (`inventory_skewed -> defensive -> unwind`), PnL can depend on late-stage unwind quality, which is fragile in thin liquidity.

### 4) Pair Arb economics are tighter than they look

On paper, pair merge can look safe, but real constraints reduce edge:

- PM minimum order constraints
- fill asymmetry / orphan risk
- merge/redeem latency and occasional tx failures
- inventory carrying costs during partial fills

## Practical Interpretation of Claude Report

The report is directionally correct:

- infra is working
- both strategies are still under-optimized for stable expected value on PM crypto short windows
- the next step is disciplined verification loops, not random live retries

Recommended process:

1. `quick` dev gate (`dataset -> execution replay -> fixture replay`)
2. `full` dev gate
3. local paper windows
4. server paper windows
5. only then live canary

## Known Non-Goals (for now)

- No claim of guaranteed profitability
- No unattended live by default
- No secret material stored in repo

## Troubleshooting

### “Unauthorized” on API

Set the same `PM_WEB_ACCESS_KEY` in server env and request headers.

### Replay/dataset stage fails

Check:

- `data/replay/mmv2_dataset_scenarios.json`
- required artifact folders under `audit`

### Live start rejected

Verify credential envs and runtime budget gates in `MM V2` start path.

---

If you are deciding whether to run live right now: use gate results (`primary_blocker`, `failure_buckets`, PnL windows), not a single profitable run.
