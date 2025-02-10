import pandas as pd
import mysql.connector
from influxdb_client import InfluxDBClient, Point
from influxdb_client.client.write_api import SYNCHRONOUS

from trade_utils import utils
from trade_utils import timeframe_converter as tc

mysql_config = {
    "host": "localhost",
    "user": "root",
    "password": "Root@000###",
    "database": "fnodatabase",
}

# InfluxDB Connection Details
influxdb_config = {
    "url": "http://localhost:8086",
    "token": "ZRyjI3l560XKSi5ORVEXSPKtExYfm3gvoaFCBbM5qoxzEqXvwbvt_9NzBF_hFxWXhu2nkYjYGaIlb3ljIt5h-w==",
    "org": "trading",
    "bucket": "fnodatabase",
}

# Query TOHLCV data from MySQL
def fetch_mysql_data():
    query = "SELECT timestamp, open, high, low, close, volume FROM nifty50_1d"
    connection = mysql.connector.connect(**mysql_config)
    df = pd.read_sql(query, con=connection)
    connection.close()
    df = utils.delete_duplicate_rows(df, False)
    # df = tc.minute_1_to_five5(df)
    df.dropna(how='all', inplace=True, subset=['open', 'high', 'low', 'close'])
    df.reset_index(inplace=True)
    return df

# Write DataFrame to InfluxDB
def write_to_influxdb(df):
    with InfluxDBClient(url=influxdb_config["url"], token=influxdb_config["token"], org=influxdb_config["org"]) as client:
        write_api = client.write_api(write_options=SYNCHRONOUS)

        for _, row in df.iterrows():
            point = (
                Point("nifty50_1d")
                .field("open", row["open"])
                .field("high", row["high"])
                .field("low", row["low"])
                .field("close", row["close"])
                .field("volume", row["volume"])
                .time(row["timestamp"])  # Ensure timestamp is in datetime format
            )
            write_api.write(bucket=influxdb_config["bucket"], org=influxdb_config["org"], record=point)

# Main Execution
if __name__ == "__main__":
    # Step 1: Fetch data from MySQL
    df = fetch_mysql_data()

    # Ensure 'timestamp' is in datetime format
    df["timestamp"] = pd.to_datetime(df["timestamp"])

    # Step 2: Write to InfluxDB
    write_to_influxdb(df)

    print("Data written to InfluxDB successfully!")
