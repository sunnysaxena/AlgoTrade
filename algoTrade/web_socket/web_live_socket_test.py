import time
import json
import utility
import threading
import pandas as pd
import pandas_ta as ta
from constants import *
from collections import defaultdict
from datetime import datetime, timedelta
from today_latest_data import optimize_data

from my_fyers_model import client_id
from my_fyers_model import MyFyersModel
from my_fyers_model import get_access_token
from fyers_apiv3.FyersWebsocket import data_ws

# Initialize Fyers model
fy_model = MyFyersModel()

# Global variables
signal = None  # Track open position
candlestick_data = {}
current_interval = None  # Track the active 5-minute interval
data = defaultdict(list)
data_lock = threading.Lock()
interval = timedelta(minutes=5)


# Load historical data from CSV
try:
    ohlc_df = utility.get_historical_data()
    print("Historical data loaded.")
except FileNotFoundError:
    ohlc_df = pd.DataFrame(columns=['timestamp', 'open', 'high', 'low', 'close'])
    print("No historical data found. Starting fresh.")

def get_latest_data():
    global ohlc_df
    latest_df = optimize_data()
    ohlc_df = pd.concat([ohlc_df, latest_df], ignore_index=True).drop_duplicates(subset=['timestamp'], keep='last')
get_latest_data()



def do_place_order(symbol, side):
    """Place a market order."""
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

    # Get the current time
    current_time = datetime.now()

    print("Current Time:", current_time)
    # response = fy_model.punch_place_order(order)
    print(f"Time : ==== Order placed: {current_time, order}")


def process_candlesticks():
    global ohlc_df, current_interval, data, signal
    while True:
        time.sleep(1)
        now = datetime.now()
        interval_start = now.replace(second=0, microsecond=0, minute=(now.minute // 5) * 5)

        if current_interval is None:
            current_interval = interval_start  # Initialize on startup

        if interval_start > current_interval:
            with data_lock:
                new_ohlc_list = []
                for symbol, ticks in data.items():
                    df = pd.DataFrame(ticks, columns=['timestamp', 'ltp'])
                    if not df.empty:
                        ohlc = df.groupby('timestamp')['ltp'].agg(['first', 'max', 'min', 'last']).reset_index()
                        ohlc.columns = ['timestamp', 'open', 'high', 'low', 'close']
                        new_ohlc_list.append(ohlc)

                if new_ohlc_list:
                    new_ohlc_df = pd.concat(new_ohlc_list, ignore_index=True)
                    ohlc_df = pd.concat([ohlc_df, new_ohlc_df], ignore_index=True).drop_duplicates(subset=['timestamp'],
                                                                                                   keep='last')
                    # Ensure at least 20 candles exist for EMA calculation
                    if len(ohlc_df) >= 20:
                        ohlc_df['ema_short'] = ta.ema(ohlc_df['close'], length=9)
                        ohlc_df['ema_long'] = ta.ema(ohlc_df['close'], length=21)

                        last_row = ohlc_df.iloc[-1]
                        prev_row = ohlc_df.iloc[-2]

                        if prev_row['ema_short'] < prev_row['ema_long'] and last_row['ema_short'] > last_row[
                            'ema_long']:
                            if position != "BUY":
                                do_place_order(symbol, BUY)
                                position = "BUY"
                        elif prev_row['ema_short'] > prev_row['ema_long'] and last_row['ema_short'] < last_row[
                            'ema_long']:
                            if position != "SELL":
                                do_place_order(symbol, SELL)
                                position = "SELL"

                    print("\nOHLC Data (5-minute interval):")
                    # print(ohlc_df['timestamp', 'close', 'ema_short', 'ema_long'].tail(100))
                    #
                    print(ohlc_df.columns)
                    print(ohlc_df[['timestamp', 'close', 'ema_short', 'ema_long']].tail(100))

                    # Save updated OHLC data back to CSV
                    # ohlc_df.to_csv(historical_data_file, index=False)
                data.clear()
            current_interval = interval_start


def on_message(message):
    global data
    symbol = message['symbol']
    ltp = message['ltp']
    timestamp = datetime.fromtimestamp(message['exch_feed_time'])
    interval_start = timestamp.replace(second=0, microsecond=0, minute=(timestamp.minute // 5) * 5)

    with data_lock:
        data[symbol].append((interval_start, ltp))


def on_error(message):
    print(f"Error: {message}")


def on_close(message):
    print("Connection closed:", message)


def on_open():
    symbols = ['NSE:NIFTY50-INDEX']
    fyers.subscribe(symbols=symbols, data_type="SymbolUpdate")
    fyers.keep_running()


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
