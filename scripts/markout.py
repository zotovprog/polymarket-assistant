#!/usr/bin/env python3
"""
Mark-out analysis for Polymarket BTC 15m Up/Down markets.

For each resolved period, fetches:
  - Chainlink oracle open/close prices (from past-results API)
  - All PM trades (from data-api)
  - Binance 1-second klines around each trade (for mark-out)

Computes:
  - Mark-out P&L at 30s, 60s, 90s after each fill
  - Per-period and aggregate statistics
  - Profit factor, t-stat, Sharpe

Usage:
  python scripts/markout.py [--hours 24] [--coin BTC]
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
PM_DATA_API = "https://data-api.polymarket.com/trades"
BINANCE_REST = "https://api.binance.com/api/v3"
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


def fetch_pm_market_for_period(coin: str, period_start_ts: int) -> dict | None:
    """Get the Gamma API market data for a specific 15m period."""
    slug = f"{coin.lower()}-updown-15m-{period_start_ts}"
    try:
        resp = requests.get(PM_GAMMA, params={"slug": slug, "limit": 1}, timeout=5)
        data = resp.json()
        if data and data[0].get("markets"):
            m = data[0]["markets"][0]
            return {
                "slug": slug,
                "conditionId": m["conditionId"],
                "clobTokenIds": json.loads(m["clobTokenIds"]),
                "outcomes": m.get("outcomes", ["Up", "Down"]),
            }
    except Exception:
        pass
    return None


def fetch_pm_trades(condition_id: str, limit: int = 1000) -> list[dict]:
    """Fetch ALL trades for a market from data-api, paginating with offset.

    The API returns trades newest-first with a max of 1000 per request.
    We paginate until we get fewer than `limit` results or an empty/invalid response.
    """
    all_trades = []
    offset = 0

    while True:
        try:
            resp = requests.get(PM_DATA_API, params={
                "market": condition_id,
                "limit": limit,
                "offset": offset,
            }, timeout=10)
            if resp.status_code != 200:
                break

            data = resp.json()

            # API returns [] when no more trades, or {} at very high offsets
            if not data or not isinstance(data, list):
                break

            all_trades.extend(data)

            # If we got fewer than limit, we've reached the end
            if len(data) < limit:
                break

            offset += limit
            time.sleep(0.2)  # rate limit between pages

        except Exception:
            break

    return all_trades


def fetch_binance_klines_1s(symbol: str, start_ms: int, end_ms: int, limit: int = 20) -> list[dict]:
    """Fetch Binance 1-minute klines for a time range."""
    # Binance doesn't have 1s klines, use 1m as finest granularity
    # For mark-out we'll use linear interpolation within each candle
    klines = []
    try:
        resp = requests.get(f"{BINANCE_REST}/klines", params={
            "symbol": symbol,
            "interval": "1m",
            "startTime": start_ms,
            "endTime": end_ms,
            "limit": limit,
        }, timeout=5)
        if resp.status_code == 200:
            for k in resp.json():
                klines.append({
                    "t": k[0] / 1000.0,  # open time in seconds
                    "o": float(k[1]),
                    "h": float(k[2]),
                    "l": float(k[3]),
                    "c": float(k[4]),
                })
    except Exception:
        pass
    return klines


# ── Fair Value Model ────────────────────────────────────────────

MINUTES_PER_YEAR = 525960


def norm_cdf(x: float) -> float:
    return 0.5 * (1.0 + math.erf(x / math.sqrt(2.0)))


def binary_fv(spot: float, strike: float, sigma: float, t_remaining_sec: float) -> float:
    """P(Up) for a binary option. Returns probability in [0, 1]."""
    if t_remaining_sec <= 0:
        return 1.0 if spot >= strike else 0.0
    if sigma <= 0 or strike <= 0 or spot <= 0:
        return 0.5

    T = t_remaining_sec / 60.0 / MINUTES_PER_YEAR
    sqrt_T = math.sqrt(T)
    d2 = (math.log(spot / strike) - 0.5 * sigma * sigma * T) / (sigma * sqrt_T)
    return norm_cdf(d2)


def estimate_vol_from_klines(klines: list[dict]) -> float | None:
    """Simple close-to-close log-return vol from 1m klines, annualized."""
    if len(klines) < 5:
        return None
    closes = [k["c"] for k in klines]
    log_rets = [math.log(closes[i] / closes[i - 1])
                for i in range(1, len(closes)) if closes[i - 1] > 0]
    if len(log_rets) < 3:
        return None
    mean = sum(log_rets) / len(log_rets)
    var = sum((r - mean) ** 2 for r in log_rets) / (len(log_rets) - 1)
    return math.sqrt(max(var, 0.0) * MINUTES_PER_YEAR)


# ── Mark-out Computation ────────────────────────────────────────

def compute_markout_for_period(
    period: dict,
    coin: str,
    binance_sym: str,
) -> list[dict]:
    """
    For one resolved period, compute mark-out for every trade.

    Mark-out = how did the PM fair value change after the trade?
    - If you BOUGHT Up at 0.45 and 60s later FV(Up) = 0.52, markout = +0.07
    - If you SOLD Up at 0.55 and 60s later FV(Up) = 0.52, markout = +0.03

    Positive markout = the trade was in the right direction.
    """
    start_time = period["startTime"]
    end_time = period["endTime"]
    oracle_open = period["openPrice"]
    oracle_close = period["closePrice"]
    outcome = period["outcome"]

    # Parse period timestamps and align to 15-min boundaries
    # (oracle startTime can be offset by a few seconds from the boundary)
    st = datetime.fromisoformat(start_time.replace("Z", "+00:00"))
    et = datetime.fromisoformat(end_time.replace("Z", "+00:00"))
    period_start_ts = (int(st.timestamp()) // 900) * 900
    period_end_ts = period_start_ts + 900

    # Get PM market data
    market = fetch_pm_market_for_period(coin, period_start_ts)
    if not market:
        return []

    # Get all trades for this market (paginated)
    trades = fetch_pm_trades(market["conditionId"])
    if not trades:
        return []

    # Normalize timestamps: data-api returns Unix seconds as int or string
    for t in trades:
        ts = t.get("timestamp", 0)
        if isinstance(ts, str):
            try:
                ts = int(ts)
            except ValueError:
                # Try ISO format
                try:
                    ts = int(datetime.fromisoformat(ts.replace("Z", "+00:00")).timestamp())
                except Exception:
                    ts = 0
        # If timestamp is in milliseconds (>1e12), convert to seconds
        if ts > 1e12:
            ts = ts / 1000.0
        t["timestamp"] = int(ts)

    # Get Binance klines covering the full period + 2 min buffer
    klines = fetch_binance_klines_1s(
        binance_sym,
        (period_start_ts - 120) * 1000,
        (period_end_ts + 120) * 1000,
    )
    if not klines:
        return []

    # Estimate vol from the 60 klines before the period
    pre_klines = fetch_binance_klines_1s(
        binance_sym,
        (period_start_ts - 3600) * 1000,
        period_start_ts * 1000,
        limit=60,
    )
    sigma = estimate_vol_from_klines(pre_klines)
    if sigma is None:
        sigma = 0.50  # default fallback

    up_token = market["clobTokenIds"][0]

    results = []
    for trade in trades:
        trade_ts = trade.get("timestamp", 0)
        if trade_ts < period_start_ts or trade_ts > period_end_ts:
            continue

        trade_price = trade.get("price", 0)
        trade_side = trade.get("side", "")
        trade_size = trade.get("size", 0)
        trade_asset = trade.get("asset", "")
        is_up = (trade_asset == up_token)

        # Time into period
        t_elapsed = trade_ts - period_start_ts
        t_remaining = period_end_ts - trade_ts

        # Find Binance price at trade time
        spot_at_trade = None
        for k in klines:
            if k["t"] <= trade_ts < k["t"] + 60:
                # Interpolate within the minute
                frac = (trade_ts - k["t"]) / 60.0
                spot_at_trade = k["o"] + frac * (k["c"] - k["o"])
                break
        if spot_at_trade is None:
            continue

        # FV at time of trade
        fv_at_trade = binary_fv(spot_at_trade, oracle_open, sigma, t_remaining)

        # Mark-out: FV at trade_ts + 30s, 60s, 90s
        markouts = {}
        for dt_sec in [30, 60, 90]:
            target_ts = trade_ts + dt_sec
            if target_ts > period_end_ts:
                # Use settlement value
                settled_up = 1.0 if outcome == "up" else 0.0
                mo_fv = settled_up
            else:
                # Find Binance price at target time
                spot_later = None
                for k in klines:
                    if k["t"] <= target_ts < k["t"] + 60:
                        frac = (target_ts - k["t"]) / 60.0
                        spot_later = k["o"] + frac * (k["c"] - k["o"])
                        break
                if spot_later is None:
                    continue
                t_rem_later = period_end_ts - target_ts
                mo_fv = binary_fv(spot_later, oracle_open, sigma, t_rem_later)

            # Mark-out depends on trade direction
            if is_up:
                if trade_side == "BUY":
                    # Bought Up: profit if FV goes up
                    markouts[dt_sec] = mo_fv - fv_at_trade
                else:
                    # Sold Up: profit if FV goes down
                    markouts[dt_sec] = fv_at_trade - mo_fv
            else:
                # Down token
                fv_dn_at_trade = 1.0 - fv_at_trade
                fv_dn_later = 1.0 - mo_fv
                if trade_side == "BUY":
                    markouts[dt_sec] = fv_dn_later - fv_dn_at_trade
                else:
                    markouts[dt_sec] = fv_dn_at_trade - fv_dn_later

        if not markouts:
            continue

        # Edge at entry: how far was trade price from FV?
        if is_up:
            if trade_side == "BUY":
                edge_at_entry = fv_at_trade - trade_price
            else:
                edge_at_entry = trade_price - fv_at_trade
        else:
            fv_dn = 1.0 - fv_at_trade
            if trade_side == "BUY":
                edge_at_entry = fv_dn - trade_price
            else:
                edge_at_entry = trade_price - fv_dn

        results.append({
            "period_start": start_time,
            "trade_ts": trade_ts,
            "t_elapsed_sec": t_elapsed,
            "t_remaining_sec": t_remaining,
            "side": trade_side,
            "outcome_token": "Up" if is_up else "Down",
            "price": trade_price,
            "size": trade_size,
            "spot": spot_at_trade,
            "strike": oracle_open,
            "sigma": sigma,
            "fv_at_trade": fv_at_trade if is_up else 1.0 - fv_at_trade,
            "edge_at_entry": edge_at_entry,
            "markout_30s": markouts.get(30),
            "markout_60s": markouts.get(60),
            "markout_90s": markouts.get(90),
            "period_outcome": outcome,
        })

    return results


# ── Reporting ───────────────────────────────────────────────────

def print_report(all_markouts: list[dict]):
    """Print aggregated mark-out statistics."""
    if not all_markouts:
        print("No trades to analyze.")
        return

    print(f"\n{'='*70}")
    print(f"  MARK-OUT ANALYSIS REPORT")
    print(f"  {len(all_markouts)} trades across "
          f"{len(set(m['period_start'] for m in all_markouts))} periods")
    print(f"{'='*70}\n")

    # Split by BUY vs SELL
    buys = [m for m in all_markouts if m["side"] == "BUY"]
    sells = [m for m in all_markouts if m["side"] == "SELL"]

    for label, subset in [("ALL TRADES", all_markouts), ("BUYS ONLY", buys), ("SELLS ONLY", sells)]:
        if not subset:
            continue

        print(f"  ── {label} ({len(subset)} trades) ──\n")

        for horizon in ["markout_30s", "markout_60s", "markout_90s"]:
            values = [m[horizon] for m in subset if m[horizon] is not None]
            if not values:
                continue

            mean_mo = statistics.mean(values)
            std_mo = statistics.stdev(values) if len(values) > 1 else 0
            t_stat = (mean_mo / (std_mo / math.sqrt(len(values)))) if std_mo > 0 else 0

            positive = sum(1 for v in values if v > 0)
            negative = sum(1 for v in values if v < 0)
            win_rate = positive / len(values) if values else 0

            gross_win = sum(v for v in values if v > 0)
            gross_loss = abs(sum(v for v in values if v < 0))
            profit_factor = gross_win / gross_loss if gross_loss > 0 else float("inf")

            tag = horizon.replace("markout_", "")
            print(f"    {tag:>4}:  mean={mean_mo:+.5f}  t-stat={t_stat:+.2f}  "
                  f"win={win_rate:.0%} ({positive}W/{negative}L)  "
                  f"PF={profit_factor:.2f}")

        # Edge at entry stats
        edges = [m["edge_at_entry"] for m in subset]
        mean_edge = statistics.mean(edges)
        print(f"\n    Edge at entry: mean={mean_edge:+.5f}")

        # Timing breakdown
        early = [m for m in subset if m["t_elapsed_sec"] < 300]
        mid = [m for m in subset if 300 <= m["t_elapsed_sec"] < 600]
        late = [m for m in subset if m["t_elapsed_sec"] >= 600]

        print(f"\n    Timing: {len(early)} early (<5m), {len(mid)} mid (5-10m), {len(late)} late (>10m)")

        for timing_label, timing_subset in [("early", early), ("mid", mid), ("late", late)]:
            mo60 = [m["markout_60s"] for m in timing_subset if m["markout_60s"] is not None]
            if mo60:
                mean_60 = statistics.mean(mo60)
                print(f"      {timing_label:>5}: 60s markout mean={mean_60:+.5f} ({len(mo60)} trades)")

        print()

    # Vol regime breakdown
    print(f"  ── VOL REGIME BREAKDOWN ──\n")
    low = [m for m in all_markouts if m["sigma"] < 0.40]
    normal = [m for m in all_markouts if 0.40 <= m["sigma"] < 0.60]
    high = [m for m in all_markouts if m["sigma"] >= 0.60]

    for vol_label, vol_subset in [("low (<40%)", low), ("normal (40-60%)", normal), ("high (>60%)", high)]:
        mo60 = [m["markout_60s"] for m in vol_subset if m["markout_60s"] is not None]
        if mo60:
            mean_60 = statistics.mean(mo60)
            print(f"    {vol_label:>16}: 60s markout mean={mean_60:+.5f} ({len(mo60)} trades)")

    print(f"\n{'='*70}\n")


# ── Main ────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(description="Mark-out analysis for PM BTC 15m markets")
    parser.add_argument("--hours", type=int, default=12, help="Hours of history to analyze")
    parser.add_argument("--coin", type=str, default="BTC", help="Coin symbol")
    parser.add_argument("--max-periods", type=int, default=50, help="Max periods to analyze")
    parser.add_argument("--json-out", type=str, default=None, help="Save raw data to JSON file")
    args = parser.parse_args()

    print(f"\nCollecting oracle data for {args.coin} 15m ({args.hours}h lookback)...")
    periods = fetch_oracle_periods(args.coin, args.hours)
    print(f"  Found {len(periods)} resolved periods")

    if not periods:
        print("No resolved periods found. Markets may not be active.")
        sys.exit(1)

    periods = periods[:args.max_periods]
    binance_sym = f"{args.coin}USDT"

    all_markouts = []
    for i, period in enumerate(periods):
        pct = (i + 1) / len(periods) * 100
        start = period["startTime"][:16]
        print(f"  [{i+1}/{len(periods)}] {start} ({period['outcome']})... ", end="", flush=True)

        markouts = compute_markout_for_period(period, args.coin, binance_sym)
        all_markouts.extend(markouts)
        print(f"{len(markouts)} trades")

        # Rate limit: be gentle with the APIs
        time.sleep(0.5)

    print(f"\nTotal: {len(all_markouts)} trades analyzed")

    if args.json_out:
        with open(args.json_out, "w") as f:
            json.dump(all_markouts, f, indent=2)
        print(f"Raw data saved to {args.json_out}")

    print_report(all_markouts)


if __name__ == "__main__":
    main()
