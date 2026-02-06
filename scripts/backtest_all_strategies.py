#!/usr/bin/env python3
"""
Comprehensive strategy backtest using Polymarket prices-history endpoint.

Tests MANY different strategy families:
1. Simple momentum (existing) — buy leading side at minute M if price > threshold
2. Momentum with trend confirmation — price must be increasing over a window
3. Mean reversion — buy the LOSING side when it's cheap
4. Volatility filter — only trade when price is moving fast (or slow)
5. Price acceleration — buy when price is accelerating toward one side
6. Contrarian fade — buy the opposite of what momentum says (bet on reversion)
7. Late-period momentum — wait until min 10+ for high certainty
8. Early-period value — buy at min 0-3 when prices are closest to 50/50
9. Spread/conviction — only trade when price is extreme (>0.85)
10. Trend reversal — detect when leading side flips and trade the new leader

Usage:
  python scripts/backtest_all_strategies.py [--hours 96] [--coin BTC]
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
            return ids[0], ids[1]
    except Exception:
        pass
    return None, None


def fetch_prices_history(token_id: str, start_ts: int, end_ts: int) -> list[dict]:
    """Fetch price history from CLOB prices-history endpoint."""
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
    except Exception:
        pass
    return []


# ── Helper Functions ────────────────────────────────────────────

def pm_taker_fee(price: float) -> float:
    """Polymarket taker fee for 15m crypto markets."""
    return price * 0.25 * (price * (1 - price)) ** 2


def get_price_at_minute(prices: list[dict], period_start: int, minute: float) -> float | None:
    """Get the price closest to a specific minute. Returns None if no data within 90s."""
    target_ts = period_start + minute * 60
    best = None
    best_dist = float("inf")
    for p in prices:
        dist = abs(p["t"] - target_ts)
        if dist < best_dist:
            best_dist = dist
            best = p["p"]
    return best if best_dist <= 90 else None


def get_prices_in_window(prices: list[dict], period_start: int,
                          min_start: float, min_end: float) -> list[tuple[float, float]]:
    """Get all (minute, price) pairs within a time window."""
    window_start = period_start + min_start * 60
    window_end = period_start + min_end * 60
    result = []
    for p in prices:
        if window_start <= p["t"] <= window_end:
            minute = (p["t"] - period_start) / 60.0
            result.append((minute, p["p"]))
    return result


def make_trade(side: str, entry_price: float, entry_minute: float,
               outcome: str, price_up: float) -> dict:
    """Create a trade result dict."""
    won = (side == "Up" and outcome == "up") or (side == "Down" and outcome == "down")
    settlement = 1.0 if won else 0.0
    taker_fee = pm_taker_fee(entry_price)
    return {
        "side": side,
        "entry_price": entry_price,
        "entry_minute": entry_minute,
        "outcome": outcome,
        "won": won,
        "settlement": settlement,
        "taker_fee": taker_fee,
        "pnl_taker": settlement - entry_price - taker_fee,
        "pnl_maker": settlement - entry_price,
        "price_up_at_entry": price_up,
    }


# ── Strategy Definitions ───────────────────────────────────────

def strategy_momentum(prices_up, period_start, outcome, params):
    """Simple momentum: buy leading side if price > threshold at entry window."""
    min_start = params["min_start"]
    min_end = params["min_end"]
    threshold = params["threshold"]

    pts = get_prices_in_window(prices_up, period_start, min_start, min_end)
    if not pts:
        return None

    # Use midpoint of window
    mid = (min_start + min_end) / 2
    pts.sort(key=lambda x: abs(x[0] - mid))
    entry_min, price_up = pts[0]
    price_dn = 1.0 - price_up

    if price_up >= threshold:
        return make_trade("Up", price_up, entry_min, outcome, price_up)
    elif price_dn >= threshold:
        return make_trade("Down", price_dn, entry_min, outcome, price_up)
    return None


def strategy_mean_reversion(prices_up, period_start, outcome, params):
    """Mean reversion: buy the LOSING side when it's cheap (below threshold).
    Bet that the market overreacted and it will come back."""
    min_start = params["min_start"]
    min_end = params["min_end"]
    threshold = params["threshold"]  # buy the underdog below this price

    pts = get_prices_in_window(prices_up, period_start, min_start, min_end)
    if not pts:
        return None

    mid = (min_start + min_end) / 2
    pts.sort(key=lambda x: abs(x[0] - mid))
    entry_min, price_up = pts[0]
    price_dn = 1.0 - price_up

    # Buy the cheap side (the one below threshold)
    if price_up <= threshold and price_up > 0.05:
        return make_trade("Up", price_up, entry_min, outcome, price_up)
    elif price_dn <= threshold and price_dn > 0.05:
        return make_trade("Down", price_dn, entry_min, outcome, price_up)
    return None


def strategy_trend_confirm(prices_up, period_start, outcome, params):
    """Trend confirmation: buy leading side only if price has been INCREASING
    (trending toward it) over the lookback window."""
    entry_min_target = params["entry_minute"]
    threshold = params["threshold"]
    lookback = params["lookback_minutes"]

    price_now = get_price_at_minute(prices_up, period_start, entry_min_target)
    price_before = get_price_at_minute(prices_up, period_start, entry_min_target - lookback)

    if price_now is None or price_before is None:
        return None

    price_dn_now = 1.0 - price_now

    # Buy Up if: price_up > threshold AND price_up has increased
    if price_now >= threshold and price_now > price_before:
        return make_trade("Up", price_now, entry_min_target, outcome, price_now)
    # Buy Down if: price_dn > threshold AND price_dn has increased (= price_up decreased)
    elif price_dn_now >= threshold and price_now < price_before:
        return make_trade("Down", price_dn_now, entry_min_target, outcome, price_now)
    return None


def strategy_trend_reversal(prices_up, period_start, outcome, params):
    """Trend reversal: detect when the leading side FLIPS and trade the new leader.
    If Up was leading at check_min but Down is leading at entry_min, buy Down."""
    check_minute = params["check_minute"]
    entry_minute = params["entry_minute"]
    threshold = params["threshold"]

    price_early = get_price_at_minute(prices_up, period_start, check_minute)
    price_late = get_price_at_minute(prices_up, period_start, entry_minute)

    if price_early is None or price_late is None:
        return None

    # Determine which side was leading at check time
    early_leader = "Up" if price_early > 0.5 else "Down"

    # Determine current leader
    late_leader = "Up" if price_late > 0.5 else "Down"

    # Only trade if leader FLIPPED
    if early_leader == late_leader:
        return None

    # Buy the new leader if above threshold
    if late_leader == "Up" and price_late >= threshold:
        return make_trade("Up", price_late, entry_minute, outcome, price_late)
    elif late_leader == "Down" and (1.0 - price_late) >= threshold:
        return make_trade("Down", 1.0 - price_late, entry_minute, outcome, price_late)
    return None


def strategy_volatility_filter(prices_up, period_start, outcome, params):
    """Momentum + volatility filter. Only trade when intra-period price movement
    (max - min over window) is within a range — not too calm, not too wild."""
    min_start = params["min_start"]
    min_end = params["min_end"]
    threshold = params["threshold"]
    min_range = params["min_range"]  # minimum price swing to trigger
    max_range = params["max_range"]  # maximum price swing (avoid chaos)

    pts = get_prices_in_window(prices_up, period_start, min_start, min_end)
    if len(pts) < 2:
        return None

    prices_only = [p for _, p in pts]
    price_range = max(prices_only) - min(prices_only)

    if price_range < min_range or price_range > max_range:
        return None

    # Use latest price in window
    pts.sort(key=lambda x: x[0])
    entry_min, price_up = pts[-1]
    price_dn = 1.0 - price_up

    if price_up >= threshold:
        return make_trade("Up", price_up, entry_min, outcome, price_up)
    elif price_dn >= threshold:
        return make_trade("Down", price_dn, entry_min, outcome, price_up)
    return None


def strategy_acceleration(prices_up, period_start, outcome, params):
    """Price acceleration: buy when price is moving FASTER toward one side.
    Compare slope of recent window vs earlier window."""
    mid_minute = params["mid_minute"]
    threshold = params["threshold"]
    window = params["window"]  # half-window for slope calculation

    # Get 3 price points: early, mid, late
    p1 = get_price_at_minute(prices_up, period_start, mid_minute - window)
    p2 = get_price_at_minute(prices_up, period_start, mid_minute)
    p3 = get_price_at_minute(prices_up, period_start, mid_minute + window)

    if p1 is None or p2 is None or p3 is None:
        return None

    slope1 = (p2 - p1) / window  # early slope
    slope2 = (p3 - p2) / window  # late slope

    # Acceleration = change in slope
    accel = slope2 - slope1

    price_up = p3
    price_dn = 1.0 - p3

    # Positive acceleration toward Up AND Up is leading
    if accel > 0.01 and price_up >= threshold:
        return make_trade("Up", price_up, mid_minute + window, outcome, price_up)
    # Negative acceleration (toward Down) AND Down is leading
    elif accel < -0.01 and price_dn >= threshold:
        return make_trade("Down", price_dn, mid_minute + window, outcome, price_up)
    return None


def strategy_multi_timepoint(prices_up, period_start, outcome, params):
    """Multi-timepoint confirmation: the leading side must be consistent
    across multiple check points. Buy only if the same side leads at ALL points."""
    check_minutes = params["check_minutes"]  # list of minutes to check
    threshold = params["threshold"]

    prices_at_checks = []
    for m in check_minutes:
        p = get_price_at_minute(prices_up, period_start, m)
        if p is None:
            return None
        prices_at_checks.append(p)

    # Check: is the same side leading at all checkpoints?
    all_up = all(p > 0.5 for p in prices_at_checks)
    all_dn = all(p < 0.5 for p in prices_at_checks)

    if not all_up and not all_dn:
        return None  # Mixed signals — skip

    # Use the last checkpoint for entry
    entry_price_up = prices_at_checks[-1]
    entry_min = check_minutes[-1]

    if all_up and entry_price_up >= threshold:
        return make_trade("Up", entry_price_up, entry_min, outcome, entry_price_up)
    elif all_dn and (1.0 - entry_price_up) >= threshold:
        return make_trade("Down", 1.0 - entry_price_up, entry_min, outcome, entry_price_up)
    return None


def strategy_price_distance(prices_up, period_start, outcome, params):
    """Price distance from open: buy the leading side only if price has moved
    significantly from the opening price (minute 0-1)."""
    entry_minute = params["entry_minute"]
    min_move = params["min_move"]  # minimum distance from 0.50
    threshold = params["threshold"]

    # Get opening price
    open_price = get_price_at_minute(prices_up, period_start, 0.5)
    entry_price = get_price_at_minute(prices_up, period_start, entry_minute)

    if open_price is None or entry_price is None:
        return None

    # How far has price moved from open?
    move = abs(entry_price - open_price)

    if move < min_move:
        return None

    price_dn = 1.0 - entry_price
    if entry_price >= threshold:
        return make_trade("Up", entry_price, entry_minute, outcome, entry_price)
    elif price_dn >= threshold:
        return make_trade("Down", price_dn, entry_minute, outcome, entry_price)
    return None


def strategy_extreme_value(prices_up, period_start, outcome, params):
    """Extreme value: only trade when one side is very cheap (high payout if right).
    Buy the underdog at very low prices — high risk, high reward."""
    min_start = params["min_start"]
    min_end = params["min_end"]
    max_price = params["max_price"]  # buy if below this (e.g. 0.25 = 75% implied against)

    pts = get_prices_in_window(prices_up, period_start, min_start, min_end)
    if not pts:
        return None

    mid = (min_start + min_end) / 2
    pts.sort(key=lambda x: abs(x[0] - mid))
    entry_min, price_up = pts[0]
    price_dn = 1.0 - price_up

    # Buy whichever side is cheapest, if below max_price
    if price_up <= max_price and price_up < price_dn:
        return make_trade("Up", price_up, entry_min, outcome, price_up)
    elif price_dn <= max_price and price_dn < price_up:
        return make_trade("Down", price_dn, entry_min, outcome, price_up)
    return None


# ── Strategy Registry ──────────────────────────────────────────

def build_strategy_configs():
    """Build all strategy configurations to test."""
    configs = []

    # 1. MOMENTUM (the baseline we already tested — include key combos)
    for window in [(3, 7), (5, 9), (5, 7), (7, 10)]:
        for th in [0.55, 0.60, 0.65, 0.70, 0.75, 0.80]:
            configs.append({
                "name": f"momentum_{window[0]}-{window[1]}m_{th:.0%}",
                "family": "momentum",
                "fn": strategy_momentum,
                "params": {"min_start": window[0], "min_end": window[1], "threshold": th},
            })

    # 2. MEAN REVERSION — buy the cheap side
    for window in [(3, 7), (5, 9), (7, 10)]:
        for th in [0.20, 0.25, 0.30, 0.35, 0.40]:
            configs.append({
                "name": f"meanrev_{window[0]}-{window[1]}m_{th:.0%}",
                "family": "mean_reversion",
                "fn": strategy_mean_reversion,
                "params": {"min_start": window[0], "min_end": window[1], "threshold": th},
            })

    # 3. TREND CONFIRMATION — momentum + trend
    for entry in [5, 7, 9]:
        for lookback in [2, 3, 5]:
            for th in [0.60, 0.70, 0.80]:
                if entry - lookback < 0:
                    continue
                configs.append({
                    "name": f"trendconf_m{entry}_lb{lookback}_{th:.0%}",
                    "family": "trend_confirm",
                    "fn": strategy_trend_confirm,
                    "params": {"entry_minute": entry, "threshold": th, "lookback_minutes": lookback},
                })

    # 4. TREND REVERSAL — flip detection
    for check in [2, 3, 5]:
        for entry in [7, 9, 11]:
            if entry <= check:
                continue
            for th in [0.55, 0.60, 0.65]:
                configs.append({
                    "name": f"reversal_c{check}_e{entry}_{th:.0%}",
                    "family": "trend_reversal",
                    "fn": strategy_trend_reversal,
                    "params": {"check_minute": check, "entry_minute": entry, "threshold": th},
                })

    # 5. VOLATILITY FILTER — momentum + vol constraint
    for window in [(3, 9), (5, 9)]:
        for th in [0.60, 0.70]:
            for min_r, max_r in [(0.05, 0.20), (0.10, 0.30), (0.05, 0.15)]:
                configs.append({
                    "name": f"volfilt_{window[0]}-{window[1]}m_{th:.0%}_r{min_r:.2f}-{max_r:.2f}",
                    "family": "vol_filter",
                    "fn": strategy_volatility_filter,
                    "params": {"min_start": window[0], "min_end": window[1],
                              "threshold": th, "min_range": min_r, "max_range": max_r},
                })

    # 6. ACCELERATION
    for mid in [5, 7]:
        for w in [2, 3]:
            for th in [0.60, 0.70]:
                configs.append({
                    "name": f"accel_m{mid}_w{w}_{th:.0%}",
                    "family": "acceleration",
                    "fn": strategy_acceleration,
                    "params": {"mid_minute": mid, "window": w, "threshold": th},
                })

    # 7. MULTI-TIMEPOINT CONFIRMATION
    for checks in [(3, 5, 7), (3, 7, 10), (5, 7, 9), (2, 5, 7, 9)]:
        for th in [0.55, 0.60, 0.65, 0.70]:
            name_pts = "_".join(str(c) for c in checks)
            configs.append({
                "name": f"multipt_{name_pts}_{th:.0%}",
                "family": "multi_timepoint",
                "fn": strategy_multi_timepoint,
                "params": {"check_minutes": list(checks), "threshold": th},
            })

    # 8. PRICE DISTANCE FROM OPEN
    for entry in [5, 7, 9]:
        for min_move in [0.05, 0.10, 0.15, 0.20]:
            for th in [0.60, 0.70]:
                configs.append({
                    "name": f"distance_m{entry}_mv{min_move:.2f}_{th:.0%}",
                    "family": "price_distance",
                    "fn": strategy_price_distance,
                    "params": {"entry_minute": entry, "min_move": min_move, "threshold": th},
                })

    # 9. EXTREME VALUE — buy the underdog cheap
    for window in [(5, 9), (7, 11), (3, 7)]:
        for max_p in [0.15, 0.20, 0.25, 0.30, 0.35]:
            configs.append({
                "name": f"extreme_{window[0]}-{window[1]}m_{max_p:.0%}",
                "family": "extreme_value",
                "fn": strategy_extreme_value,
                "params": {"min_start": window[0], "min_end": window[1], "max_price": max_p},
            })

    return configs


# ── Reporting ───────────────────────────────────────────────────

def compute_stats(results: list[dict]) -> dict:
    """Compute statistics for a list of trade results."""
    if not results:
        return {"n": 0}

    n = len(results)
    wins = sum(1 for r in results if r["won"])
    wr = wins / n
    pnl_m = [r["pnl_maker"] for r in results]
    pnl_t = [r["pnl_taker"] for r in results]
    mean_m = statistics.mean(pnl_m)
    mean_t = statistics.mean(pnl_t)
    std_m = statistics.stdev(pnl_m) if n > 1 else 0
    t_stat = (mean_m / (std_m / math.sqrt(n))) if std_m > 0 else 0

    gross_win = sum(p for p in pnl_m if p > 0)
    gross_loss = abs(sum(p for p in pnl_m if p < 0))
    pf = gross_win / gross_loss if gross_loss > 0 else float("inf")

    avg_entry = statistics.mean([r["entry_price"] for r in results])

    return {
        "n": n, "wins": wins, "wr": wr,
        "mean_maker": mean_m, "mean_taker": mean_t,
        "std": std_m, "t_stat": t_stat, "pf": pf,
        "avg_entry": avg_entry,
        "total_maker": sum(pnl_m), "total_taker": sum(pnl_t),
    }


# ── Main ────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(description="Multi-strategy backtest")
    parser.add_argument("--hours", type=int, default=96, help="Hours of history")
    parser.add_argument("--coin", type=str, default="BTC", help="Coin symbol")
    parser.add_argument("--max-periods", type=int, default=700, help="Max periods")
    parser.add_argument("--json-out", type=str, default=None, help="Save raw results")
    args = parser.parse_args()

    print(f"\n{'='*80}")
    print(f"  MULTI-STRATEGY BACKTEST — {args.coin} 15m — {args.hours}h lookback")
    print(f"{'='*80}\n")

    # 1. Collect periods
    print("Collecting oracle periods...")
    periods = fetch_oracle_periods(args.coin, args.hours)
    periods = [p for p in periods if p.get("outcome") in ("up", "down")]
    print(f"  Found {len(periods)} resolved periods")

    if not periods:
        sys.exit(1)
    periods = periods[:args.max_periods]

    # 2. Fetch price data for all periods
    print(f"\nFetching price histories...")
    period_data = []

    for i, period in enumerate(periods):
        st = datetime.fromisoformat(period["startTime"].replace("Z", "+00:00"))
        period_start_ts = (int(st.timestamp()) // 900) * 900
        period_end_ts = period_start_ts + 900

        up_token, dn_token = fetch_pm_token_for_period(args.coin, period_start_ts)
        if not up_token:
            continue

        prices_up = fetch_prices_history(up_token, period_start_ts, period_end_ts)
        if not prices_up:
            continue

        period_data.append({
            "period": period,
            "period_start_ts": period_start_ts,
            "prices_up": prices_up,
            "outcome": period["outcome"],
        })

        if (i + 1) % 20 == 0:
            print(f"  ... {i+1}/{len(periods)} ({len(period_data)} with data)")
        time.sleep(0.3)

    total_periods = len(period_data)
    print(f"  Total: {total_periods} periods with data")

    # 3. Build strategy configs
    configs = build_strategy_configs()
    print(f"\nTesting {len(configs)} strategy configurations...")

    # 4. Run ALL strategies
    all_results = []
    for config in configs:
        trades = []
        for pd in period_data:
            trade = config["fn"](
                pd["prices_up"], pd["period_start_ts"],
                pd["outcome"], config["params"],
            )
            if trade:
                trades.append(trade)

        stats = compute_stats(trades)
        all_results.append({
            "name": config["name"],
            "family": config["family"],
            "params": config["params"],
            "stats": stats,
            "trades": trades,
        })

    # 5. Print results by family
    print(f"\n{'='*100}")
    print(f"  RESULTS BY STRATEGY FAMILY (sorted by t-stat within each)")
    print(f"{'='*100}\n")

    families = defaultdict(list)
    for r in all_results:
        if r["stats"]["n"] >= 15:
            families[r["family"]].append(r)

    for family in sorted(families.keys()):
        members = families[family]
        members.sort(key=lambda x: -x["stats"]["t_stat"])

        print(f"\n  ── {family.upper()} ({len(members)} configs with n>=15) ──\n")
        print(f"    {'Name':>45}  {'N':>4}  {'Win%':>5}  {'Entry':>5}  "
              f"{'MkrPnL':>8}  {'t-stat':>7}  {'PF':>5}  {'TotPnL':>8}")
        print(f"    {'─'*45}  {'─'*4}  {'─'*5}  {'─'*5}  {'─'*8}  {'─'*7}  {'─'*5}  {'─'*8}")

        for r in members[:8]:  # top 8 per family
            s = r["stats"]
            print(f"    {r['name']:>45}  {s['n']:>4}  {s['wr']:>4.0%}  "
                  f"{s['avg_entry']:>5.3f}  {s['mean_maker']:>+8.4f}  "
                  f"{s['t_stat']:>+7.2f}  {s['pf']:>5.2f}  {s['total_maker']:>+8.2f}")

    # 6. Overall top 20
    qualified = [r for r in all_results if r["stats"]["n"] >= 20]
    qualified.sort(key=lambda x: -x["stats"]["t_stat"])

    print(f"\n{'='*100}")
    print(f"  TOP 20 STRATEGIES OVERALL (n>=20, by t-stat)")
    print(f"{'='*100}\n")
    print(f"  {'#':>3}  {'Name':>45}  {'Family':>15}  {'N':>4}  {'Win%':>5}  "
          f"{'MkrPnL':>8}  {'t-stat':>7}  {'PF':>5}")
    print(f"  {'─'*3}  {'─'*45}  {'─'*15}  {'─'*4}  {'─'*5}  {'─'*8}  {'─'*7}  {'─'*5}")

    for i, r in enumerate(qualified[:20]):
        s = r["stats"]
        print(f"  {i+1:>3}  {r['name']:>45}  {r['family']:>15}  {s['n']:>4}  "
              f"{s['wr']:>4.0%}  {s['mean_maker']:>+8.4f}  {s['t_stat']:>+7.2f}  {s['pf']:>5.2f}")

    # 7. Bottom 10 (worst strategies)
    print(f"\n  BOTTOM 10 (worst t-stat, n>=20):\n")
    for i, r in enumerate(qualified[-10:]):
        s = r["stats"]
        print(f"  {i+1:>3}  {r['name']:>45}  {r['family']:>15}  {s['n']:>4}  "
              f"{s['wr']:>4.0%}  {s['mean_maker']:>+8.4f}  {s['t_stat']:>+7.2f}  {s['pf']:>5.2f}")

    # 8. Split-half stability for top 10
    print(f"\n{'='*100}")
    print(f"  SPLIT-HALF STABILITY (top 10 by t-stat)")
    print(f"{'='*100}\n")
    print(f"  {'Name':>45}  {'H1 N':>5}  {'H1 PnL':>7}  {'H2 N':>5}  {'H2 PnL':>7}  {'Stable':>6}")
    print(f"  {'─'*45}  {'─'*5}  {'─'*7}  {'─'*5}  {'─'*7}  {'─'*6}")

    for r in qualified[:10]:
        trades = r["trades"]
        mid = len(trades) // 2
        h1 = trades[:mid]
        h2 = trades[mid:]
        h1_pnl = statistics.mean([t["pnl_maker"] for t in h1]) if h1 else 0
        h2_pnl = statistics.mean([t["pnl_maker"] for t in h2]) if h2 else 0
        stable = "✓" if h1_pnl > 0 and h2_pnl > 0 else "✗"
        print(f"  {r['name']:>45}  {len(h1):>5}  {h1_pnl:>+7.4f}  "
              f"{len(h2):>5}  {h2_pnl:>+7.4f}  {stable:>6}")

    # 9. Family comparison summary
    print(f"\n{'='*100}")
    print(f"  FAMILY COMPARISON (best config per family)")
    print(f"{'='*100}\n")

    for family in sorted(families.keys()):
        members = [r for r in families[family] if r["stats"]["n"] >= 20]
        if not members:
            continue
        best = max(members, key=lambda x: x["stats"]["t_stat"])
        s = best["stats"]
        # Split-half
        trades = best["trades"]
        mid = len(trades) // 2
        h1_pnl = statistics.mean([t["pnl_maker"] for t in trades[:mid]]) if mid > 0 else 0
        h2_pnl = statistics.mean([t["pnl_maker"] for t in trades[mid:]]) if mid > 0 else 0
        stable = "✓" if h1_pnl > 0 and h2_pnl > 0 else "✗"

        print(f"  {family:>20}: {best['name']:>40}  n={s['n']:>3}  wr={s['wr']:.0%}  "
              f"t={s['t_stat']:+.2f}  pnl={s['mean_maker']:+.4f}  {stable}")

    # 10. Save
    if args.json_out:
        output = {
            "meta": {
                "coin": args.coin, "hours": args.hours,
                "total_periods": total_periods,
                "n_strategies": len(configs),
                "run_time": datetime.now(timezone.utc).isoformat(),
            },
            "results": [
                {
                    "name": r["name"], "family": r["family"],
                    "stats": r["stats"],
                    # Don't save individual trades to keep file small
                }
                for r in qualified[:50]
            ],
        }
        with open(args.json_out, "w") as f:
            json.dump(output, f, indent=2, default=str)
        print(f"\nTop 50 results saved to {args.json_out}")


if __name__ == "__main__":
    main()
