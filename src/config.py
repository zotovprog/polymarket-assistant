# ── Coins ───────────────────────────────────────────────────────
COINS = ["BTC", "ETH", "SOL", "XRP"]

COIN_BINANCE = {
    "BTC": "BTCUSDT",
    "ETH": "ETHUSDT",
    "SOL": "SOLUSDT",
    "XRP": "XRPUSDT",
}

COIN_PM      = {"BTC": "btc",     "ETH": "eth",      "SOL": "sol",    "XRP": "xrp"}
COIN_PM_LONG = {"BTC": "bitcoin", "ETH": "ethereum", "SOL": "solana", "XRP": "xrp"}

# ── Timeframes ──────────────────────────────────────────────────
# Available timeframes per coin (5m only for BTC)
COIN_TIMEFRAMES = {
    "BTC": ["5m", "15m", "1h", "4h", "daily"],
    "ETH": ["15m", "1h", "4h", "daily"],
    "SOL": ["15m", "1h", "4h", "daily"],
    "XRP": ["15m", "1h", "4h", "daily"],
}

# Binance kline interval used for TA candles
TF_KLINE = {"5m": "1m", "15m": "1m", "1h": "1m", "4h": "15m", "daily": "1h"}

# ── Binance ─────────────────────────────────────────────────────
BINANCE_WS   = "wss://stream.binance.com/stream"
BINANCE_REST = "https://api.binance.com/api/v3"
OB_LEVELS    = 20          # depth levels in stream (Binance: 5 / 10 / 20)
TRADE_TTL    = 600         # keep 10 min of trades
KLINE_MAX    = 150         # max candles in memory
KLINE_BOOT   = 100         # candles fetched on startup

# ── Polymarket ──────────────────────────────────────────────────
PM_GAMMA = "https://gamma-api.polymarket.com/events"
PM_WS    = "wss://ws-subscriptions-clob.polymarket.com/ws/market"
PM_TAKER_FEE = 0.0022    # 0.22% taker fee on Polymarket CLOB
PM_MAKER_FEE = 0.0       # 0% maker fee
PM_MAX_SPREAD_PCT = 5.0     # max allowed (ask - bid) / ask * 100 to enter
PM_MIN_DEPTH_USD = 10.0     # Minimum liquidity depth required for entry
PM_COMPLETE_SET_ALERT = 0.98   # alert when UP + DN < this (implies arb edge)
PM_DIVERGENCE_MAX_PCT = 25.0  # block entry if PM price > fair value by this %

# Complete-set arbitrage
PM_ARB_MIN_EDGE_PCT = 0.5    # minimum net edge (after 2x taker fees) to execute arb
PM_ARB_MAX_SIZE_USD = 25.0   # max USD size per arb trade per leg
PM_ARB_COOLDOWN_SEC = 30     # seconds between arb attempts
PM_ARB_ENABLED = True         # master switch for arb execution

# ── Orderbook indicators ───────────────────────────────────────
OBI_BAND_PCT = 1.0          # % band around mid for OBI calc
OBI_THRESH   = 0.10         # ±10 % = signal
WALL_MULT    = 5            # wall = level qty > N × avg level qty
DEPTH_BANDS  = [0.1, 0.5, 1.0]   # % from mid for depth calc

# ── Flow indicators ────────────────────────────────────────────
CVD_WINDOWS  = [60, 180, 300]    # 1m / 3m / 5m in seconds
DELTA_WINDOW = 60                # short delta window (seconds)

# ── TA indicators ──────────────────────────────────────────────
RSI_PERIOD = 14
RSI_OB     = 70
RSI_OS     = 30
MACD_FAST  = 12
MACD_SLOW  = 26
MACD_SIG   = 9
EMA_S      = 5
EMA_L      = 20

# ── Bias Score weights (max absolute contribution of each indicator) ──
BIAS_WEIGHTS = {
    "ema":   10,   # EMA5/EMA20 cross  – strongest trend proxy
    "obi":    8,   # Order Book Imbalance
    "macd":   8,   # MACD histogram sign
    "cvd":    7,   # CVD 5m sign
    "ha":     6,   # Heikin-Ashi streak (up to 3 candles)
    "vwap":   5,   # Price vs VWAP
    "rsi":    5,   # RSI overbought/oversold
    "poc":    3,   # Price vs POC
    "walls":  4,   # bid walls − ask walls (capped ±4)
}
# sum of all weights = 56; bias = (raw_sum / 56) * 100, clamped to ±100

# ── Dashboard ──────────────────────────────────────────────────
HA_COUNT   = 8          # Heikin Ashi candles shown
VP_BINS    = 30         # volume profile price buckets
VP_SHOW    = 9          # VP rows visible
REFRESH    = 10         # seconds between dashboard redraws
REFRESH_5M = 3          # faster refresh for 5m timeframe (seconds)
