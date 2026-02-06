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
TIMEFRAMES = ["15m", "1h", "4h", "daily"]

# Binance kline interval used for TA candles
TF_KLINE   = {"15m": "1m", "1h": "1m", "4h": "15m", "daily": "1h"}
# Polymarket past-results variant name
TF_VARIANT = {"15m": "fifteen", "1h": "hourly", "4h": "four_hour", "daily": "daily"}

# ── Binance ─────────────────────────────────────────────────────
BINANCE_WS   = "wss://stream.binance.com/stream"
BINANCE_REST = "https://api.binance.com/api/v3"
OB_LEVELS    = 20          # depth levels in stream (Binance: 5 / 10 / 20)
TRADE_TTL    = 600         # keep 10 min of trades
KLINE_MAX    = 150         # max candles in memory
KLINE_BOOT   = 100         # candles fetched on startup

# ── Polymarket ──────────────────────────────────────────────────
PM_GAMMA        = "https://gamma-api.polymarket.com/events"
PM_WS           = "wss://ws-subscriptions-clob.polymarket.com/ws/market"
PM_PAST_RESULTS = "https://polymarket.com/api/past-results"
PM_PRICE_POLL   = 10         # seconds between REST price polls

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

# ── Fair Value ─────────────────────────────────────────────────
FV_VOL_WINDOW    = 60         # 1m klines for volatility estimate
FV_VOL_MIN       = 20         # minimum klines for Yang-Zhang
FV_EDGE_THRESH   = 0.02       # 2¢ edge to trigger signal
MINUTES_PER_YEAR = 525960     # 365.25 × 24 × 60

# ── Dashboard ──────────────────────────────────────────────────
HA_COUNT   = 8          # Heikin Ashi candles shown
VP_BINS    = 30         # volume profile price buckets
VP_SHOW    = 9          # VP rows visible
REFRESH    = 10         # seconds between dashboard redraws
