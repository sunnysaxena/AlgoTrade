import warnings

warnings.filterwarnings("ignore")

import time
import pygame
import threading
import logging
from collections import defaultdict
from datetime import datetime, timedelta

import pandas as pd
import pandas_ta as ta

from backup import db_utility
import fyers_utility
from constants import *
from my_fyers_model import MyFyersModel
from my_fyers_model import client_id
from my_fyers_model import get_access_token
from fyers_apiv3.FyersWebsocket import data_ws

# Show all rows and columns
pd.set_option('display.max_rows', None)  # Show all rows
pd.set_option('display.max_columns', None)  # Show all columns

# Logging configuration
logging.basicConfig(
    filename="trading_script.log",
    level=logging.DEBUG,
    format="%(asctime)s [%(levelname)s]: %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S"
)

# Initialize Fyers model
fy_model = MyFyersModel()

# Global variables
signal = None  # Track open position
candlestick_data = {}
current_interval = None  # Track the active 1-minute interval
data = defaultdict(list)
data_lock = threading.Lock()
interval = timedelta(minutes=1)

# Initialize pygame for sound notifications
pygame.mixer.init()
buy_for_upside_sound = pygame.mixer.Sound("sound/buy_for_upside.wav")
buy_for_downside_sound = pygame.mixer.Sound("sound/buy_for_downside.wav")

# Load historical data from the database
try:
    ohlc_df = db_utility.get_historical_data_1min()
    logging.info("Historical data loaded.")
except FileNotFoundError:
    ohlc_df = pd.DataFrame(columns=['timestamp', 'close'])
    logging.warning("No historical data found. Starting fresh.")

# Get latest data from 9:30 to the next hours
def get_latest_data():
    global ohlc_df
    try:
        latest_df = fyers_utility.get_todays_data(time_interval='5')
        ohlc_df = pd.concat([ohlc_df, latest_df], ignore_index=True).drop_duplicates(subset=['timestamp'], keep='last')
        logging.info("Latest data loaded.")
    except ValueError as e:
        logging.error(f"Failed to load latest data: {e}")
        ohlc_df = pd.DataFrame(columns=['timestamp', 'close'])

get_latest_data()


# Get latest data from 9:30 to the next hours
def get_latest_data():
    global ohlc_df
    try:
        latest_df = fyers_utility.get_todays_data()
        ohlc_df = pd.concat([ohlc_df, latest_df], ignore_index=True).drop_duplicates(subset=['timestamp'], keep='last')
        logging.info("Latest data loaded.")
    except ValueError as e:
        logging.error(f"Failed to load latest data: {e}")
        ohlc_df = pd.DataFrame(columns=['timestamp', 'close'])

get_latest_data()

def do_place_order(symbol, side):
    """Place a market order with error handling."""
    try:
        order = {
            "symbol": symbol,
            "qty": 75,  # Modify quantity as needed
            "side": side,  # 'BUY' or 'SELL'
            "type": LIMIT_ORDER,  # Market order
            "productType": INTRADAY,
            "limitPrice": 0,
            "stopPrice": 0,
            "validity": "DAY",
            "disclosedQty": 0,
            "offlineOrder": False
        }

        # get the current time
        current_time = datetime.now()
        logging.info(f"Placing order at {current_time}: {order}")

        # Uncomment when you want to use the actual API
        # response = fy_model.punch_place_order(order)
        # logging.info(f"Order response: {response}")

        print(f"Order placed: {order}")
    except Exception as e:
        logging.error(f"Failed to place order: {e}")

def process_candlesticks():
    global ohlc_df, current_interval, data, signal
    while True:
        try:
            time.sleep(1)
            now = datetime.now()
            interval_start = now.replace(second=0, microsecond=0)

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
                                # Calculate EMA, RSI, and MACD
                                ohlc_df['ema_short'] = ta.ema(ohlc_df['close'], length=9)
                                ohlc_df['ema_long'] = ta.ema(ohlc_df['close'], length=21)
                                ohlc_df['RSI'] = ta.rsi(ohlc_df['close'], timeperiod=14)

                                macd = ta.macd(ohlc_df['close'], fast=12, slow=26, signal=9)
                                ohlc_df['MACD'] = macd['MACD_12_26_9']
                                ohlc_df['Signal_Line'] = macd['MACDs_12_26_9']

                                # Generate buy/sell signals
                                ohlc_df['Buy_Signal'] = ((ohlc_df['ema_short'].shift(1) < ohlc_df['ema_long'].shift(1)) &
                                                         (ohlc_df['ema_short'] > ohlc_df['ema_long']) &
                                                         (ohlc_df['RSI'] > 50) &
                                                         (ohlc_df['MACD'] > ohlc_df['Signal_Line']))

                                ohlc_df['Sell_Signal'] = ((ohlc_df['ema_short'].shift(1) > ohlc_df['ema_long'].shift(1)) &
                                                          (ohlc_df['ema_short'] < ohlc_df['ema_long']) &
                                                          (ohlc_df['RSI'] < 50) &
                                                          (ohlc_df['MACD'] < ohlc_df['Signal_Line']))

                                last_row = ohlc_df.iloc[-1]
                                prev_row = ohlc_df.iloc[-2]

                                # Buy condition
                                if ohlc_df['Buy_Signal'].iloc[-1]:
                                    if position != "BUY":
                                        buy_for_upside_sound.play()
                                        do_place_order(symbol, BUY)
                                        position = "BUY"

                                # Sell condition
                                if ohlc_df['Sell_Signal'].iloc[-1]:
                                    if position != "SELL":
                                        buy_for_downside_sound.play()
                                        do_place_order(symbol, SELL)
                                        position = "SELL"
                            except Exception as e:
                                logging.error(f"Error calculating indicators or handling crossover logic: {e}")

                        # Filter rows for today's date
                        today = pd.Timestamp.now().normalize()
                        ohlc_df1 = ohlc_df.copy()
                        ohlc_df1 = ohlc_df1[ohlc_df1['timestamp'].dt.normalize() == today]

                        print(ohlc_df1)

                        logging.info("\nOHLC Data (1-minute interval):")
                        logging.info(ohlc_df1[['timestamp', 'close', 'ema_short', 'ema_long', 'RSI', 'MACD', 'Signal_Line', 'Buy_Signal', 'Sell_Signal']])

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
        interval_start = timestamp.replace(second=0, microsecond=0)

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
        symbols = ['NSE:NIFTY50-INDEX']
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
