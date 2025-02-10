import logging
import pandas as pd
from constants import *
from my_fyers_model import MyFyersModel
from datetime import datetime, timedelta

fy_model = MyFyersModel()
today_date = datetime.today().strftime("%Y-%m-%d")

# Cache dictionary to store data
cache = {}
cache_expiry = timedelta(minutes=5)  # Cache expiry time


def get_timestamp(start_time="09:15:00", end_time=None):
    string1 = today_date + f" {start_time}"
    object1 = datetime.strptime(string1, "%Y-%m-%d %H:%M:%S")
    start_epoch_time = int(object1.timestamp())
    logging.info(f"Start Epoch Time: {string1}")

    if end_time:
        string2 = today_date + f" {end_time}"
    else:
        current_time = datetime.now()
        string2 = current_time.strftime("%Y-%m-%d %H:%M:%S")
    object2 = datetime.strptime(string2, "%Y-%m-%d %H:%M:%S")
    end_epoch_time = int(object2.timestamp())
    logging.info(f"End Epoch Time: {string2}")

    return start_epoch_time, end_epoch_time


def get_todays_data(symbol, time_interval='5', start_time="09:15:00", end_time=None):
    global cache
    cache_key = (symbol, time_interval, start_time, end_time)
    current_time = datetime.now()

    # Check if data is in cache and not expired
    if cache_key in cache:
        cached_data, timestamp = cache[cache_key]
        if current_time - timestamp < cache_expiry:
            logging.info("Returning cached data.")
            return cached_data
    try:
        range_from, range_to = get_timestamp(start_time, end_time)
        data = {
            "symbol": symbol,
            "resolution": time_interval,
            "date_format": "0",
            "range_from": range_from,
            "range_to": range_to,
            "cont_flag": "1"
        }

        response = fy_model.get_history(data=data)
        if 'candles' not in response:
            raise ValueError("Response does not contain 'candles' key")

        r1 = response['candles']
        df = pd.DataFrame(r1, columns=["epoc", "open", "high", "low", "close", "volume"])
        df['timestamp'] = pd.to_datetime(df['epoc'], unit='s', utc=True).map(lambda x: x.tz_convert(TIME_ZONE))
        df['timestamp'] = pd.to_datetime(df['timestamp'], format='%Y-%m-%d %H:%M:%S')
        df['timestamp'] = df['timestamp'].dt.tz_localize(None)
        df = df[["timestamp", "close"]]
        df.drop_duplicates(inplace=True)

        # Store data in cache
        cache[cache_key] = (df, current_time)
        logging.info("Data successfully retrieved and processed.")
        return df

    except Exception as e:
        logging.error(f"An error occurred: {e}")
        return pd.DataFrame()  # Return an empty DataFrame in case of error


def strike_price_to_symbol(symbol=None):
    """
    Fetch the low price of the given symbol from the Fyers API.

    Args:
        symbol (str): The symbol for which to fetch the low price. Default is 'BSE:SENSEX2521179400CE'.

    Returns:
        float: The low price of the symbol, or None if an error occurs.
    """
    try:
        data = {
            "symbols": symbol
        }

        response = fy_model.get_quotes(data=data)
        # print(response)
        ltp = response['d'][0]['v']['lp']
        low_price = response['d'][0]['v']['low_price']
        print(f"symbol : {symbol} ==> Ltp : {ltp} ==> Low Price : {low_price}")

        return low_price
    except Exception as e:
        logging.error(f"An error occurred while fetching the low price for symbol {symbol}: {e}")
        return None


if __name__ == '__main__':
    # get_todays_data(symbol=OPTION_SYMBOLS_FYERS['nifty50'], time_interval='5')
    ce_symbol = 'BSE:SENSEX2521179400CE'

    strike_price_to_symbol(ce_symbol)
