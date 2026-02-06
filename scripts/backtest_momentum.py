#!/usr/bin/env python3
"""
Momentum strategy backtest using Polymarket prices-history endpoint.

Uses clob.polymarket.com/prices-history for full minute-0 coverage
(no 4000-trade cap like data-api).

Strategy: At minute M of the 15-min period, if PM Up price > threshold,
buy Up (cost = price). If PM Down price > threshold, buy Down.
Settlement: winning side pays $1, losing side pays $0.

Usage:
  python scripts/backtest_momentum.py [--hours 48] [--coin BTC]
"""

import argparse
import json
import math
import requests
import statistics
import sys
import time
from datetime import datetime, timezone, timedelta
from collections import defaultdict


# ── Config ──────────────────────────────────────────────────────

PM_PAST_RESULTS = "https://polymarket.com/api/past-results"
PM_GAMMA = "https://gamma-api.polymarket.com/events"
CLOB_PRICES = "https://clob.polymarket.com/prices-history"
HEADERS = {"User-Agent": "Mozilla/5.0"}


# ── Data Collection ─────────────────────────────────────────────

def fetch_oracle_periods(coin: str, hours: int) -> list[dict]:
    """Collect all resolved 15m oracle periods for the past N hours."""
    all_results = []
    seen = set()
    now = datetime.now(timezone.utc)

    # Step through time in 15-min increments to catch all periods
    # (past-results API returns ~4 results per query, looking backward from query time)
    total_steps = hours * 4 + 1
    for step in range(total_steps):
        dt = now - timedelta(hours=hours) + timedelta(minutes=step * 15)
        ts_aligned = (int(dt.timestamp()) // 900) * 900
        dt_aligned = datetime.fromtimestamp(ts_aligned, tz=timezone.utc)
        start_iso = dt_aligned.strftime("%Y-%m-%dT%H:%M:%SZ")

        try:
            resp = requests.get(PM_PAST_RESULTS, params={
                "symbol": coin,
                "variant": "fifteen",
                "assetType": "crypto",
                "currentEventStartTime": start_iso,
            }, timeout=5, headers=HEADERS)

            if resp.status_code == 200:
                data = resp.json()
                for r in data.get("data", {}).get("results", []):
                    key = r["startTime"]
                    if key not in seen:
                        seen.add(key)
                        all_results.append(r)
        except Exception:
            pass

    all_results.sort(key=lambda x: x["startTime"])
    return all_results


def fetch_pm_token_for_period(coin: str, period_start_ts: int) -> tuple[str | None, str | None]:
    """Get Up and Down token IDs for a specific 15m period."""
    slug = f"{coin.lower()}-updown-15m-{period_start_ts}"
    try:
        resp = requests.get(PM_GAMMA, params={"slug": slug, "limit": 1}, timeout=5)
        data = resp.json()
        if data and data[0].get("markets"):
            m = data[0]["markets"][0]
            ids = json.loads(m["clobTokenIds"])
            return ids[0], ids[1]  # up_token, dn_token
    except Exception:
        pass
    return None, None


def fetch_prices_history(token_id: str, start_ts: int, end_ts: int) -> list[dict]:
    """Fetch price history from CLOB prices-history endpoint.

    Returns list of {t: timestamp, p: price} entries.
    fidelity=1 gives ~1-minute granularity.
    """
    try:
        resp = requests.get(CLOB_PRICES, params={
            "market": token_id,
            "startTs": start_ts,
            "endTs": end_ts,
            "fidelity": 1,
        }, timeout=10)
        if resp.status_code == 200:
            data = resp.json()
            if isinstance(data, dict) and "history" in data:
                return [{"t": int(h["t"]), "p": float(h["p"])} for h in data["history"]]
    except Exception as e:
        pass
    return []


# ── Taker Fee Calculation ───────────────────────────────────────

def pm_taker_fee(price: float, shares: float = 1.0) -> float:
    """Polymarket taker fee for 15m crypto markets.
    Formula: shares * price * 0.25 * (price * (1 - price))^2
    """
    return shares * price * 0.25 * (price * (1 - price)) ** 2


# ── Strategy Logic ──────────────────────────────────────────────

def find_price_at_minute(prices: list[dict], period_start: int, target_min: float) -> float | None:
    """Find the PM price closest to target_min minutes into the period."""
    target_ts = period_start + target_min * 60
    best = None
    best_dist = float("inf")

    for p in prices:
        dist = abs(p["t"] - target_ts)
        if dist < best_dist:
            best_dist = dist
            best = p["p"]

    # Only accept if within 90 seconds of target
    if best_dist <= 90:
        return best
    return None


def find_best_price_in_window(prices: list[dict], period_start: int,
                               min_start: float, min_end: float) -> tuple[float | None, float | None]:
    """Find the price in a time window. Returns (price, minute_into_period).

    For buying the leading side, we want the price closest to the center of the window.
    """
    window_start = period_start + min_start * 60
    window_end = period_start + min_end * 60

    candidates = []
    for p in prices:
        if window_start <= p["t"] <= window_end:
            minute = (p["t"] - period_start) / 60.0
            candidates.append((p["p"], minute))

    if not candidates:
        return None, None

    # Return the price closest to the middle of the window
    mid_min = (min_start + min_end) / 2
    candidates.sort(key=lambda x: abs(x[1] - mid_min))
    return candidates[0]


def evaluate_momentum_strategy(
    prices_up: list[dict],
    period_start: int,
    outcome: str,
    entry_window: tuple[float, float],
    threshold: float,
) -> dict | None:
    """
    Evaluate momentum strategy for one period.

    Strategy: In the entry window, if Up price > threshold -> buy Up.
              If Up price < (1 - threshold) -> buy Down.
              Otherwise, no trade.

    Returns trade result dict or None if no trade.
    """
    min_start, min_end = entry_window
    price_up, entry_min = find_best_price_in_window(prices_up, period_start, min_start, min_end)

    if price_up is None:
        return None

    price_dn = 1.0 - price_up  # PM prices should sum to ~1

    # Decision: buy the leading side if above threshold
    if price_up >= threshold:
        side = "Up"
        entry_price = price_up
    elif price_dn >= threshold:
        side = "Down"
        entry_price = price_dn
    else:
        return None  # No trade — neither side above threshold

    # Settlement
    won = (side == "Up" and outcome == "up") or (side == "Down" and outcome == "down")
    settlement = 1.0 if won else 0.0

    # P&L (taker execution)
    taker_fee = pm_taker_fee(entry_price)
    pnl_taker = settlement - entry_price - taker_fee

    # P&L (maker execution — 0% fee)
    pnl_maker = settlement - entry_price

    return {
        "side": side,
        "entry_price": entry_price,
        "entry_minute": entry_min,
        "outcome": outcome,
        "won": won,
        "settlement": settlement,
        "taker_fee": taker_fee,
        "pnl_taker": pnl_taker,
        "pnl_maker": pnl_maker,
        "price_up_at_entry": price_up,
    }


# ── Reporting ───────────────────────────────────────────────────

def print_strategy_report(
    results: list[dict],
    threshold: float,
    entry_window: tuple[float, float],
    total_periods: int,
):
    """Print strategy backtest results."""
    if not results:
        print(f"\n  threshold={threshold:.2f}, window={entry_window}: NO TRADES")
        return

    wins = [r for r in results if r["won"]]
    losses = [r for r in results if not r["won"]]
    win_rate = len(wins) / len(results)

    pnl_maker = [r["pnl_maker"] for r in results]
    pnl_taker = [r["pnl_taker"] for r in results]
    mean_pnl_maker = statistics.mean(pnl_maker)
    mean_pnl_taker = statistics.mean(pnl_taker)
    total_pnl_maker = sum(pnl_maker)
    total_pnl_taker = sum(pnl_taker)

    std_pnl = statistics.stdev(pnl_maker) if len(pnl_maker) > 1 else 0
    t_stat = (mean_pnl_maker / (std_pnl / math.sqrt(len(pnl_maker)))) if std_pnl > 0 else 0

    # Profit factor
    gross_win = sum(r["pnl_maker"] for r in wins)
    gross_loss = abs(sum(r["pnl_maker"] for r in losses)) if losses else 0
    pf = gross_win / gross_loss if gross_loss > 0 else float("inf")

    # Average entry prices
    avg_entry_up = statistics.mean([r["entry_price"] for r in results if r["side"] == "Up"]) if any(r["side"] == "Up" for r in results) else 0
    avg_entry_dn = statistics.mean([r["entry_price"] for r in results if r["side"] == "Down"]) if any(r["side"] == "Down" for r in results) else 0

    # Side breakdown
    up_trades = [r for r in results if r["side"] == "Up"]
    dn_trades = [r for r in results if r["side"] == "Down"]
    up_wins = sum(1 for r in up_trades if r["won"])
    dn_wins = sum(1 for r in dn_trades if r["won"])

    print(f"\n{'─'*70}")
    print(f"  MOMENTUM STRATEGY: threshold={threshold:.2f}, "
          f"window=min {entry_window[0]:.0f}-{entry_window[1]:.0f}")
    print(f"{'─'*70}")
    print(f"  Periods analyzed: {total_periods}")
    print(f"  Periods traded:   {len(results)} ({len(results)/total_periods*100:.0f}%)")
    print(f"  Win/Loss:         {len(wins)}W / {len(losses)}L  ({win_rate:.1%})")
    print(f"  t-stat:           {t_stat:+.2f}")
    print(f"  Profit factor:    {pf:.2f}")
    print(f"")
    print(f"  Maker P&L:  mean={mean_pnl_maker:+.4f}  total={total_pnl_maker:+.2f}  ({total_pnl_maker*100:+.0f}c)")
    print(f"  Taker P&L:  mean={mean_pnl_taker:+.4f}  total={total_pnl_taker:+.2f}  ({total_pnl_taker*100:+.0f}c)")
    print(f"")
    print(f"  Side split:  Up={len(up_trades)} ({up_wins}W), Down={len(dn_trades)} ({dn_wins}W)")
    if avg_entry_up > 0:
        print(f"  Avg entry:   Up={avg_entry_up:.3f}, Down={avg_entry_dn:.3f}")
    print(f"")

    # Loss analysis
    if losses:
        print(f"  ── LOSSES ──")
        for i, r in enumerate(losses):
            print(f"    #{i+1}: Bought {r['side']} at {r['entry_price']:.3f} "
                  f"(min {r['entry_minute']:.1f}), outcome={r['outcome']}")
        print()


def print_sweep_results(sweep: list[dict], total_periods: int):
    """Print compact table of all threshold/window combos."""
    print(f"\n{'='*90}")
    print(f"  PARAMETER SWEEP (across {total_periods} resolved periods)")
    print(f"{'='*90}")
    print(f"  {'Window':>10}  {'Thresh':>6}  {'Trades':>6}  {'Win%':>6}  "
          f"{'MkrPnL':>8}  {'TkrPnL':>8}  {'t-stat':>7}  {'PF':>6}  {'TotMkr':>8}")
    print(f"  {'─'*10}  {'─'*6}  {'─'*6}  {'─'*6}  {'─'*8}  {'─'*8}  {'─'*7}  {'─'*6}  {'─'*8}")

    for s in sweep:
        if not s["results"]:
            continue
        r = s["results"]
        wins = sum(1 for x in r if x["won"])
        wr = wins / len(r) if r else 0
        pnl_m = [x["pnl_maker"] for x in r]
        pnl_t = [x["pnl_taker"] for x in r]
        mean_m = statistics.mean(pnl_m)
        mean_t = statistics.mean(pnl_t)
        total_m = sum(pnl_m)
        std_m = statistics.stdev(pnl_m) if len(pnl_m) > 1 else 0
        t = (mean_m / (std_m / math.sqrt(len(pnl_m)))) if std_m > 0 else 0
        gw = sum(x for x in pnl_m if x > 0)
        gl = abs(sum(x for x in pnl_m if x < 0))
        pf = gw / gl if gl > 0 else float("inf")

        w = s["window"]
        print(f"  {f'{w[0]:.0f}-{w[1]:.0f}m':>10}  {s['threshold']:>6.2f}  "
              f"{len(r):>6}  {wr:>5.1%}  {mean_m:>+8.4f}  {mean_t:>+8.4f}  "
              f"{t:>+7.2f}  {pf:>6.2f}  {total_m:>+8.2f}")

    print(f"{'='*90}\n")


# ── Main ────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(description="Momentum backtest via prices-history")
    parser.add_argument("--hours", type=int, default=48, help="Hours of history")
    parser.add_argument("--coin", type=str, default="BTC", help="Coin symbol")
    parser.add_argument("--max-periods", type=int, default=200, help="Max periods")
    parser.add_argument("--json-out", type=str, default=None, help="Save raw results")
    parser.add_argument("--verbose", action="store_true", help="Show per-period detail")
    args = parser.parse_args()

    print(f"\n{'='*70}")
    print(f"  MOMENTUM BACKTEST — {args.coin} 15m — {args.hours}h lookback")
    print(f"  Using prices-history endpoint (full minute 0-15 coverage)")
    print(f"{'='*70}\n")

    # 1. Collect resolved periods
    print(f"Collecting oracle periods...")
    periods = fetch_oracle_periods(args.coin, args.hours)
    # Filter to only resolved periods (must have outcome)
    periods = [p for p in periods if p.get("outcome") in ("up", "down")]
    print(f"  Found {len(periods)} resolved periods")

    if not periods:
        print("No resolved periods. Try a larger --hours window.")
        sys.exit(1)

    periods = periods[:args.max_periods]
    total_periods = len(periods)

    # 2. Fetch token IDs and prices-history for each period
    print(f"\nFetching price histories for {total_periods} periods...")
    period_data = []
    skipped = 0

    for i, period in enumerate(periods):
        st = datetime.fromisoformat(period["startTime"].replace("Z", "+00:00"))
        period_start_ts = (int(st.timestamp()) // 900) * 900
        period_end_ts = period_start_ts + 900

        # Get token IDs
        up_token, dn_token = fetch_pm_token_for_period(args.coin, period_start_ts)
        if not up_token:
            skipped += 1
            if args.verbose:
                print(f"  [{i+1}] {period['startTime'][:16]} — no market found, skipping")
            continue

        # Fetch prices-history for Up token
        prices_up = fetch_prices_history(up_token, period_start_ts, period_end_ts)
        if not prices_up:
            skipped += 1
            if args.verbose:
                print(f"  [{i+1}] {period['startTime'][:16]} — no price history, skipping")
            continue

        # Coverage stats
        if prices_up:
            first_min = (prices_up[0]["t"] - period_start_ts) / 60.0
            last_min = (prices_up[-1]["t"] - period_start_ts) / 60.0
            n_points = len(prices_up)
        else:
            first_min = last_min = n_points = 0

        if args.verbose:
            print(f"  [{i+1}] {period['startTime'][:16]} ({period['outcome']:>4}) "
                  f"— {n_points} pts, min {first_min:.1f}-{last_min:.1f}")

        period_data.append({
            "period": period,
            "period_start_ts": period_start_ts,
            "period_end_ts": period_end_ts,
            "prices_up": prices_up,
            "outcome": period["outcome"],
        })

        # Rate limit
        if (i + 1) % 10 == 0:
            print(f"  ... {i+1}/{total_periods} ({len(period_data)} with data, {skipped} skipped)")
        time.sleep(0.3)

    print(f"\n  Total: {len(period_data)} periods with price data, {skipped} skipped")

    if not period_data:
        print("No periods with price data found.")
        sys.exit(1)

    # 3. Coverage analysis
    print(f"\n── PRICE COVERAGE ANALYSIS ──\n")
    coverage_by_minute = defaultdict(int)
    for pd in period_data:
        for p in pd["prices_up"]:
            minute = int((p["t"] - pd["period_start_ts"]) / 60)
            if 0 <= minute <= 14:
                coverage_by_minute[minute] += 1

    total_with_data = len(period_data)
    for m in range(15):
        count = coverage_by_minute.get(m, 0)
        pct = count / total_with_data * 100 if total_with_data > 0 else 0
        bar = "#" * int(pct / 2)
        print(f"  min {m:>2}: {count:>4} ({pct:>5.1f}%) {bar}")

    # 4. Run strategy sweep
    print(f"\n── RUNNING STRATEGY SWEEP ──\n")

    WINDOWS = [
        (0, 3),    # Very early
        (2, 5),    # Early
        (3, 7),    # Early-mid
        (5, 7),    # Mid (best in trade-data backtest)
        (5, 9),    # Mid-wide
        (7, 10),   # Mid-late
        (10, 13),  # Late
    ]

    THRESHOLDS = [0.55, 0.60, 0.65, 0.70, 0.75, 0.80]

    sweep = []
    for window in WINDOWS:
        for threshold in THRESHOLDS:
            results = []
            for pd in period_data:
                trade = evaluate_momentum_strategy(
                    pd["prices_up"],
                    pd["period_start_ts"],
                    pd["outcome"],
                    window,
                    threshold,
                )
                if trade:
                    results.append(trade)

            sweep.append({
                "window": window,
                "threshold": threshold,
                "results": results,
            })

    # Print sweep results
    print_sweep_results(sweep, total_with_data)

    # 5. Detailed report for the most interesting combos
    # Find the combo with highest t-stat that has >= 20 trades
    best = None
    best_t = -999
    for s in sweep:
        r = s["results"]
        if len(r) < 10:
            continue
        pnl = [x["pnl_maker"] for x in r]
        mean_p = statistics.mean(pnl)
        std_p = statistics.stdev(pnl) if len(pnl) > 1 else 0
        t = (mean_p / (std_p / math.sqrt(len(pnl)))) if std_p > 0 else 0
        if t > best_t:
            best_t = t
            best = s

    if best:
        print(f"\n  BEST COMBO (by t-stat):")
        print_strategy_report(best["results"], best["threshold"], best["window"], total_with_data)

    # Also print the specific combos from the trade-data backtest
    key_combos = [
        ((5, 7), 0.55),
        ((5, 7), 0.60),
        ((5, 7), 0.70),
        ((3, 7), 0.60),
        ((0, 3), 0.55),
    ]
    for window, threshold in key_combos:
        for s in sweep:
            if s["window"] == window and s["threshold"] == threshold:
                print_strategy_report(s["results"], threshold, window, total_with_data)
                break

    # 6. Save raw results if requested
    if args.json_out:
        output = {
            "meta": {
                "coin": args.coin,
                "hours": args.hours,
                "total_periods": total_with_data,
                "skipped_periods": skipped,
                "run_time": datetime.now(timezone.utc).isoformat(),
            },
            "sweep": [
                {
                    "window": s["window"],
                    "threshold": s["threshold"],
                    "n_trades": len(s["results"]),
                    "results": s["results"],
                }
                for s in sweep
            ],
        }
        with open(args.json_out, "w") as f:
            json.dump(output, f, indent=2)
        print(f"\nRaw results saved to {args.json_out}")


if __name__ == "__main__":
    main()
