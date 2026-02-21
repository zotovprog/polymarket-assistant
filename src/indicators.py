import time
import config


def obi(bids, asks, mid):
    band = mid * config.OBI_BAND_PCT / 100
    bv = sum(q for p, q in bids if p >= mid - band)
    av = sum(q for p, q in asks if p <= mid + band)
    tot = bv + av
    return (bv - av) / tot if tot else 0.0


def walls(bids, asks):
    vols = [q for _, q in bids] + [q for _, q in asks]
    if not vols:
        return [], []
    avg = sum(vols) / len(vols)
    thr = avg * config.WALL_MULT
    return (
        [(p, q) for p, q in bids if q >= thr],
        [(p, q) for p, q in asks if q >= thr],
    )


def depth_usd(bids, asks, mid):
    out = {}
    for pct in config.DEPTH_BANDS:
        band = mid * pct / 100
        out[pct] = (
            sum(p * q for p, q in bids if p >= mid - band)
            + sum(p * q for p, q in asks if p <= mid + band)
        )
    return out


def cvd(trades, secs):
    cut = time.time() - secs
    return sum(
        t["qty"] * t["price"] * (1 if t["is_buy"] else -1)
        for t in trades
        if t["t"] >= cut
    )


def vol_profile(klines, bins=20):
    if not klines:
        return 0.0, []

    lo = min(k["l"] for k in klines)
    hi = max(k["h"] for k in klines)
    if hi == lo:
        return lo, [(lo, sum(k["v"] for k in klines))]

    n   = bins
    bsz = (hi - lo) / n
    bins = [0.0] * n

    for k in klines:
        b_lo = max(0,     int((k["l"] - lo) / bsz))
        b_hi = min(n - 1, int((k["h"] - lo) / bsz))
        share = k["v"] / max(1, b_hi - b_lo + 1)
        for b in range(b_lo, b_hi + 1):
            bins[b] += share

    poci = bins.index(max(bins))
    poc  = lo + (poci + 0.5) * bsz
    data = [(lo + (i + 0.5) * bsz, bins[i]) for i in range(n)]
    return poc, data


def _ema_series(vals, period):
    if len(vals) < period:
        return [None] * len(vals)
    mult = 2.0 / (period + 1)
    out  = [None] * (period - 1)
    out.append(sum(vals[:period]) / period)
    for v in vals[period:]:
        out.append(v * mult + out[-1] * (1 - mult))
    return out


def rsi(klines):
    closes = [k["c"] for k in klines]
    n = config.RSI_PERIOD
    if len(closes) < n + 1:
        return None

    ch = [closes[i] - closes[i - 1] for i in range(1, len(closes))]
    ag = sum(max(c, 0) for c in ch[:n]) / n
    al = sum(max(-c, 0) for c in ch[:n]) / n
    for c in ch[n:]:
        ag = (ag * (n - 1) + max(c, 0))  / n
        al = (al * (n - 1) + max(-c, 0)) / n
    return 100.0 if al == 0 else 100.0 - 100.0 / (1 + ag / al)


def macd(klines):
    closes = [k["c"] for k in klines]
    if len(closes) < config.MACD_SLOW:
        return None, None, None

    ef = _ema_series(closes, config.MACD_FAST)
    es = _ema_series(closes, config.MACD_SLOW)

    ml = [ef[i] - es[i] for i in range(len(closes)) if ef[i] is not None and es[i] is not None]
    if not ml:
        return None, None, None

    sig = _ema_series(ml, config.MACD_SIG)
    m = ml[-1]
    s = sig[-1]
    h = (m - s) if s is not None else None
    return m, s, h


def vwap(klines):
    tp_v = sum((k["h"] + k["l"] + k["c"]) / 3 * k["v"] for k in klines)
    v    = sum(k["v"] for k in klines)
    return tp_v / v if v else 0.0


def emas(klines):
    closes = [k["c"] for k in klines]
    s = _ema_series(closes, config.EMA_S)
    l = _ema_series(closes, config.EMA_L)
    return (
        s[-1] if s and s[-1] is not None else None,
        l[-1] if l and l[-1] is not None else None,
    )


def heikin_ashi(klines):
    ha = []
    for i, k in enumerate(klines):
        c = (k["o"] + k["h"] + k["l"] + k["c"]) / 4
        o = (k["o"] + k["c"]) / 2 if i == 0 else (ha[i - 1]["o"] + ha[i - 1]["c"]) / 2
        ha.append({
            "o": o,
            "h": max(k["h"], o, c),
            "l": min(k["l"], o, c),
            "c": c,
            "green": c >= o,
        })
    return ha


def pm_fair_value(mid, klines, rsi_val=None, vwap_val=None):
    """Estimate fair probability for UP/DOWN based on Binance indicators.
    Returns (prob_up, prob_dn) as floats in [0.1, 0.9].
    Simple heuristic: base 0.5 adjusted by VWAP position and RSI.
    """
    prob_up = 0.5

    # VWAP: above VWAP = slightly bullish, below = slightly bearish
    if vwap_val and mid and vwap_val > 0:
        vwap_dist = (mid - vwap_val) / vwap_val * 100  # % distance
        prob_up += max(-0.15, min(0.15, vwap_dist / 1.0 * 0.15))

    # RSI: overbought/oversold
    if rsi_val is not None:
        if rsi_val > 50:
            prob_up += min(0.10, (rsi_val - 50) / 50 * 0.10)
        else:
            prob_up -= min(0.10, (50 - rsi_val) / 50 * 0.10)

    prob_up = max(0.10, min(0.90, prob_up))
    return prob_up, 1.0 - prob_up

