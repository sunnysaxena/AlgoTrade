import warnings

warnings.filterwarnings("ignore")

import os
import sys

import time
import pygame
import threading
import logging
from collections import defaultdict
from datetime import datetime, timedelta

import pandas as pd
import pandas_ta as ta

import utility
from historical_data import get_historical_data
import fyers_utility
from constants import *
from my_fyers_model import MyFyersModel
from my_fyers_model import client_id
from my_fyers_model import get_access_token
from fyers_apiv3.FyersWebsocket import data_ws


# Redirect stdout temporarily to suppress Pygame messages
class SuppressPygameOutput:
    def __enter__(self):
        self._original_stdout = sys.stdout
        sys.stdout = open(os.devnull, 'w')

    def __exit__(self, exc_type, exc_val, exc_tb):
        sys.stdout.close()
        sys.stdout = self._original_stdout


# Suppress the Pygame initialization message
with SuppressPygameOutput():
    pygame.init()

# Show all rows and columns
pd.set_option('display.max_rows', None)  # Show all rows
pd.set_option('display.max_columns', None)  # Show all columns

# Logging configuration
logging.basicConfig(
    filename="trading_script_sensex_5min.log",
    level=logging.DEBUG,
    format="%(asctime)s [%(levelname)s]: %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S"
)

# Initialize Fyers model
fy_model = MyFyersModel()

# Global variables
signal = None  # Track open position
order_id = None  # Track order position
candlestick_data = {}
current_interval = None  # Track the active 5-minute interval
data = defaultdict(list)
data_lock = threading.Lock()
interval = timedelta(minutes=5)

ce_symbol = 'BSE:SENSEX2521179400CE'
pe_symbol = 'BSE:SENSEX2521175100PE'

# Initialize pygame for sound notifications
pygame.mixer.init()
buy_for_upside_sound = pygame.mixer.Sound("sound/buy_for_upside.wav")
buy_for_downside_sound = pygame.mixer.Sound("sound/buy_for_downside.wav")

# Load historical data from the database
try:
    ohlc_df = get_historical_data(table_name=TABLE_NAMES['sensex_1m'], time_interval='5')
    logging.info("Historical data loaded.")
except FileNotFoundError:
    ohlc_df = pd.DataFrame(columns=['timestamp', 'close'])
    logging.warning("No historical data found. Starting fresh.")

# Get latest data from 9:30 to the symbol next hours
def get_latest_data():
    global ohlc_df
    try:
        latest_df = fyers_utility.get_todays_data(symbol=OPTION_SYMBOLS_FYERS['sensex'], time_interval='5')
        ohlc_df = pd.concat([ohlc_df, latest_df], ignore_index=True).drop_duplicates(subset=['timestamp'], keep='last')
        logging.info("Latest data loaded.")
    except ValueError as e:
        logging.error(f"Failed to load latest data: {e}")
        ohlc_df = pd.DataFrame(columns=['timestamp', 'close'])

get_latest_data()

def do_place_order(symbol, side):
    """Place a market order with error handling."""
    global order_id
    strike_price_ltp = fyers_utility.strike_price_to_symbol(symbol)
    total_quantity = utility.lots_to_buy(capital=CAPITAL, lot_size=LOT_SIZE, price_per_share=strike_price_ltp)
    try:
        order = {
            "symbol": symbol,
            "qty": total_quantity,
            "type": LIMIT_ORDER,
            "side": side,  # 'BUY' or 'SELL'
            "productType": INTRADAY,
            "limitPrice": 0,
            "stopPrice": 0,
            "validity": "DAY",
            "disclosedQty": 0,
            "offlineOrder": False,
            "orderTag": f"Total quantity : {total_quantity}, Capital : {CAPITAL}"
        }

        # get the current time
        current_time = datetime.now()

        # Uncomment when you want to use the actual API
        response = fy_model.punch_place_order(order)
        order_id = response['id']
        logging.info(f"Order Id : {response['id']}")
        logging.info(f"Placing order at : {current_time}")
        logging.info(f"Order request attributes : {order}")
    except Exception as e:
        logging.error(f"Failed to place order: {e}")

def process_candlesticks():
    global ohlc_df, current_interval, data, signal
    while True:
        try:
            time.sleep(1)
            now = datetime.now()
            interval_start = now.replace(second=0, microsecond=0, minute=(now.minute // 5) * 5)

            if current_interval is None:
                current_interval = interval_start  # initialize on startup

            if interval_start > current_interval:
                with data_lock:
                    new_ohlc_list = []
                    for symbol, ticks in data.items():
                        try:
                            df = pd.DataFrame(ticks, columns=['timestamp', 'ltp'])
                            if not df.empty:
                                ohlc = df.groupby('timestamp')['ltp'].agg(['first', 'max', 'min', 'last']).reset_index()
                                ohlc.columns = ['timestamp', 'open', 'high', 'low', 'close']
                                new_ohlc_list.append(ohlc)
                        except Exception as e:
                            logging.error(f"Error processing ticks for symbol {symbol}: {e}")

                    if new_ohlc_list:
                        new_ohlc_df = pd.concat(new_ohlc_list, ignore_index=True)
                        ohlc_df = pd.concat([ohlc_df, new_ohlc_df], ignore_index=True).drop_duplicates(
                            subset=['timestamp'], keep='last')

                        if len(ohlc_df) >= 20:
                            try:
                                ohlc_df['ema_short'] = ta.ema(ohlc_df['close'], length=9)
                                ohlc_df['ema_long'] = ta.ema(ohlc_df['close'], length=21)

                                # Calculate RSI
                                ohlc_df['RSI'] = ta.rsi(ohlc_df['close'], timeperiod=14)
                                # Calculate ADX
                                # ohlc_df['ADX'] = ta.adx(ohlc_df['high'], ohlc_df['low'], ohlc_df['close'], timeperiod=14)

                                last_row = ohlc_df.iloc[-1]
                                prev_row = ohlc_df.iloc[-2]

                                # Buy CE
                                if prev_row['ema_short'] < prev_row['ema_long'] and last_row['ema_short'] > last_row[
                                    'ema_long'] and ohlc_df['RSI'].iloc[-1] > 50: # and ohlc_df['ADX'].iloc[-1] > 25
                                    '''
                                        EMA Bullish Crossover, RSI Confirmation, Strong Trend
                                    '''
                                    if order_id is None:
                                        if signal != "BUY":
                                            buy_for_upside_sound.play()
                                            do_place_order(ce_symbol, BUY)
                                            signal = "BUY"
                                    else:
                                        pass

                                # Buy PE
                                elif prev_row['ema_short'] > prev_row['ema_long'] and last_row['ema_short'] < last_row[
                                    'ema_long'] and ohlc_df['RSI'].iloc[-1] < 50: #  and ohlc_df['ADX'].iloc[-1] > 25
                                    '''
                                        EMA Bearish Crossover, RSI Confirmation, Strong Trend
                                    '''
                                    if order_id is None:
                                        if signal != "SELL":
                                            buy_for_downside_sound.play()
                                            do_place_order(pe_symbol, BUY)
                                            signal = "SELL"
                                    else:
                                        pass
                            except Exception as e:
                                logging.error(f"Error calculating EMA or handling crossover logic: {e}")

                        # Filter rows for today's date
                        today = pd.Timestamp.now().normalize()
                        ohlc_df1 = ohlc_df.copy()
                        ohlc_df1 = ohlc_df[ohlc_df1['timestamp'].dt.normalize() == today]

                        # print(ohlc_df1)

                        logging.info("\nOHLC Data (5-minute interval):")
                        logging.info(ohlc_df1[['timestamp', 'close', 'ema_short', 'ema_long', 'RSI']])

                    data.clear()
                current_interval = interval_start
        except Exception as e:
            logging.error(f"Error in process_candlesticks loop: {e}")

def on_message(message):
    global data
    try:
        symbol = message.get('symbol')
        ltp = message.get('ltp')
        timestamp = datetime.fromtimestamp(message.get('exch_feed_time', time.time()))
        interval_start = timestamp.replace(second=0, microsecond=0, minute=(timestamp.minute // 5) * 5)

        with data_lock:
            data[symbol].append((interval_start, ltp))
    except Exception as e:
        logging.error(f"Error in on_message: {e}")

def on_error(message):
    logging.error(f"WebSocket error: {message}")

def on_close(message):
    logging.warning(f"WebSocket connection closed: {message}")

def on_open():
    try:
        symbols = OPTION_SYMBOLS_FYERS['sensex']
        fyers.subscribe(symbols=symbols, data_type="SymbolUpdate")
        fyers.keep_running()
    except Exception as e:
        logging.error(f"Error during WebSocket connection: {e}")

try:
    access_token = f"{client_id}:{get_access_token()}"
    fyers = data_ws.FyersDataSocket(
        access_token=access_token,
        log_path="",
        litemode=False,
        write_to_file=True,
        reconnect=True,
        on_connect=on_open,
        on_close=on_close,
        on_error=on_error,
        on_message=on_message,
        reconnect_retry=10
    )

    thread = threading.Thread(target=process_candlesticks, daemon=True)
    thread.start()
    fyers.connect()
except Exception as e:
    logging.error(f"Error initializing WebSocket or starting thread: {e}")
