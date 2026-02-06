import math
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


def vol_profile(klines):
    if not klines:
        return 0.0, []

    lo = min(k["l"] for k in klines)
    hi = max(k["h"] for k in klines)
    if hi == lo:
        return lo, [(lo, sum(k["v"] for k in klines))]

    n   = config.VP_BINS
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


# ── Fair Value ────────────────────────────────────────────────


def _norm_cdf(x: float) -> float:
    """Standard normal cumulative distribution function."""
    return 0.5 * (1.0 + math.erf(x / math.sqrt(2.0)))


def _cc_vol(klines: list[dict]) -> float | None:
    """Close-to-close log-return volatility (fallback)."""
    if len(klines) < 5:
        return None
    closes = [k["c"] for k in klines]
    log_rets = [math.log(closes[i] / closes[i - 1])
                for i in range(1, len(closes)) if closes[i - 1] > 0]
    if len(log_rets) < 3:
        return None
    mean = sum(log_rets) / len(log_rets)
    var = sum((r - mean) ** 2 for r in log_rets) / (len(log_rets) - 1)
    return math.sqrt(max(var, 0.0) * config.MINUTES_PER_YEAR)


def yang_zhang_vol(klines: list[dict], window: int) -> float | None:
    """
    Yang-Zhang OHLC volatility estimator.
    Returns annualized sigma, or None if insufficient data.
    """
    if len(klines) < max(window, config.FV_VOL_MIN):
        return _cc_vol(klines)

    recent = klines[-window:]
    n = len(recent)

    # overnight: log(open / prev_close)
    log_oc = []
    # close-to-open (intraday): log(close / open)
    log_co = []
    log_ho = []
    log_lo = []

    for i in range(1, n):
        prev_c = recent[i - 1]["c"]
        o, h, l, c = recent[i]["o"], recent[i]["h"], recent[i]["l"], recent[i]["c"]
        if prev_c <= 0 or o <= 0:
            continue
        log_oc.append(math.log(o / prev_c))
        log_co.append(math.log(c / o) if c > 0 and o > 0 else 0.0)
        log_ho.append(math.log(h / o) if h > 0 and o > 0 else 0.0)
        log_lo.append(math.log(l / o) if l > 0 and o > 0 else 0.0)

    m = len(log_oc)
    if m < 2:
        return _cc_vol(klines)

    # overnight variance
    mean_oc = sum(log_oc) / m
    var_overnight = sum((x - mean_oc) ** 2 for x in log_oc) / (m - 1)

    # close-to-open variance
    mean_co = sum(log_co) / m
    var_close_open = sum((x - mean_co) ** 2 for x in log_co) / (m - 1)

    # Rogers-Satchell variance
    var_rs = sum(
        log_ho[i] * (log_ho[i] - log_co[i]) + log_lo[i] * (log_lo[i] - log_co[i])
        for i in range(m)
    ) / m

    k_yz = 0.34 / (1.34 + (m + 1) / (m - 1))
    var_yz = var_overnight + k_yz * var_close_open + (1 - k_yz) * var_rs

    # annualize: var_yz is per-bar (1-minute) variance
    sigma = math.sqrt(max(var_yz, 0.0) * config.MINUTES_PER_YEAR)
    return sigma


def fair_value(
    spot: float,
    strike: float | None,
    sigma: float | None,
    time_remaining_sec: float,
) -> tuple[float | None, float | None]:
    """
    Binary option fair value for Up/Down contracts.

    P(Up) = Φ((ln(S/K) + (-σ²/2)·T) / (σ·√T))
    P(Down) = 1 - P(Up)

    Returns (p_up, p_down) or (None, None) if inputs are invalid.
    """
    if strike is None or sigma is None or strike <= 0 or sigma <= 0 or spot <= 0:
        return None, None

    if time_remaining_sec <= 0:
        p_up = 1.0 if spot >= strike else 0.0
        return p_up, 1.0 - p_up

    T = time_remaining_sec / (365.25 * 24 * 3600)
    sqrt_T = math.sqrt(T)

    d = (math.log(spot / strike) + (-sigma ** 2 / 2) * T) / (sigma * sqrt_T)
    p_up = _norm_cdf(d)
    return p_up, 1.0 - p_up
