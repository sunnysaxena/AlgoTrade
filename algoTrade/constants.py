TIME_ZONE = 'Asia/Kolkata'
OPTION_PATH = 'data/options'
BANKS_PATH = 'data/banks'


OPTION_SYMBOLS_FYERS = {
    'indiavix': 'NSE:INDIAVIX-INDEX',
    'nifty50': 'NSE:NIFTY50-INDEX',
    'niftybank': 'NSE:NIFTYBANK-INDEX',
    'finnifty': 'NSE:FINNIFTY-INDEX',
    'midnifty': 'NSE:MIDCPNIFTY-INDEX',
    'sensex': 'BSE:SENSEX-INDEX'
}

TABLE_NAMES = {
    'nifty50_1m': 'nifty50_1m',
    'nifty50_1d': 'nifty50_1d',
    'sensex_1m': 'sensex_1m',
    'sensex_1d': 'sensex_1d'
}

OPTION_SYMBOLS_YAHOO = {
    'indiavix': '^INDIAVIX',
    'nifty50': '^NSEI',
    'niftybank': '^NSEBANK',
    'finnifty': 'NIFTY_FIN_SERVICE.NS'
}

TREND_TYPES = [
    'Uptrend',
    'Downtrend',
    'Sideways',
    'Reversal',
    'Volatility',
    'Seasonal',
    'Random or Chaotic',
    'Range Bound'
]

# EMA Strategy Parameters
EMA_SETTINGS_9_21 = {
    "short_period": 9,  # Short-term EMA
    "long_period": 21   # Long-term EMA
}

# Order Placement

# type
LIMIT_ORDER = 1
MARKET_ORDER = 2
STOP_ORDER = 3
STOP_LIMIT_ORDER = 4

# 3 => Stop Order (SL-M)
# 4 => Stoplimit Order (SL-L)

# side
BUY = 1
SELL = 2

# product type
INTRADAY = 'INTRADAY' # Applicable for all segments.
MARGIN = 'MARGIN'     # Applicable only for derivatives
CO = 'CO' # Cover Order
BO = 'BO' # Bracket Order

# capital
CAPITAL = 5200
LOT_SIZE = 20
