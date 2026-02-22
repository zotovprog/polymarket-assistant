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
REFRESH    = 10         # seconds between dashboard redraws
REFRESH_5M = 3          # faster refresh for 5m timeframe (seconds)

# ── Market Making ─────────────────────────────────────────────────
MM_HALF_SPREAD_BPS   = 150       # 1.5% half-spread default
MM_ORDER_SIZE_USD    = 10.0      # USD per side
MM_MAX_INVENTORY     = 25.0      # max shares one-sided (reduced to limit liquidation losses)
MM_SKEW_BPS_PER_UNIT = 15.0      # skew per share of net delta (aggressive rebalancing)
MM_REQUOTE_SEC       = 2.0       # seconds between requote checks
MM_REQUOTE_THRESH_BPS = 5.0      # min price move to requote
MM_GTD_DURATION_SEC  = 300       # GTD order lifetime (5 min)
MM_HEARTBEAT_SEC     = 55        # heartbeat interval
MM_MAX_DRAWDOWN_USD  = 50.0      # max session drawdown
MM_VOL_PAUSE_MULT    = 3.0       # pause if vol > N × avg
MM_USE_POST_ONLY     = True      # force post-only (maker) orders
MM_USE_GTD           = True       # use GTD order type
