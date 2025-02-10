import pandas as pd
from constants import *
from datetime import datetime
from my_fyers_model import MyFyersModel

fy_model = MyFyersModel()
today_date = datetime.today().strftime("%Y-%m-%d")


def update_all_tables():
    data = {
        "symbol": 'BSE:SENSEX-INDEX',
        "resolution": "1",
        "date_format": "1",
        "range_from": '2019-05-28',
        "range_to": '2019-06-10',
        "cont_flag": "1"
    }

    response = fy_model.get_history(data=data)
    master_data = response['candles']

    df = pd.DataFrame(master_data, columns=["epoc", "open", "high", "low", "close", "volume"])
    df['timestamp'] = pd.to_datetime(df['epoc'], unit='s', utc=True).map(lambda x: x.tz_convert(TIME_ZONE))
    df['timestamp'] = pd.to_datetime(df['timestamp'], format='%Y-%m-%d %H:%M:%S')
    df['timestamp'] = df['timestamp'].dt.tz_localize(None)
    df = df[["timestamp", "open", "high", "low", "close", "volume"]]
    df.drop_duplicates(inplace=True)
    df['volume'] = 0
    print(df.head())
    print(df.tail())

update_all_tables()
