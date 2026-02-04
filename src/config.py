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
TF_KLINE = {"15m": "1m", "1h": "1m", "4h": "15m", "daily": "1h"}

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

# ── Dashboard ──────────────────────────────────────────────────
HA_COUNT   = 8          # Heikin Ashi candles shown
VP_BINS    = 30         # volume profile price buckets
VP_SHOW    = 9          # VP rows visible
REFRESH    = 10         # seconds between dashboard redraws
