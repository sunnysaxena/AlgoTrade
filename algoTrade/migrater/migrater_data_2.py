import mysql.connector
import pandas as pd

import trade_utils
from trade_utils import utils
from trade_utils import timeframe_converter as tc
from trade_utils import feature_extraction as fe
from trade_utils.db_connection import MySQLConnector
from influxdb_client import InfluxDBClient, Point
from influxdb_client.client.write_api import SYNCHRONOUS

# MySQL Connection Details
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


# Step 1: Connect to MySQL
def fetch_mysql_data():
    connection = mysql.connector.connect(**mysql_config)
    cursor = connection.cursor(dictionary=True)  # Return rows as dictionaries
    query = """
    SELECT timestamp, open, high, low, close, volume
    FROM nifty50_1d
    """
    cursor.execute(query)
    data = cursor.fetchall()
    cursor.close()
    connection.close()
    return data

def get_historical_data():
    mysql = MySQLConnector('root', 'Root@000###')
    connection = mysql.get_mysql_connection()
    engine = connection
    df = None
    query = f"SELECT * FROM nifty50_1d;"

    try:
        with engine.connect() as connection:
            df = pd.read_sql(query, engine)
    except Exception as e:
        print('Something went wrong : ', e)

    df = df[["timestamp", "open", "high", "low", "close", "volume"]]
    df = utils.delete_duplicate_rows(df, False)
    df = tc.minute_1_to_five5(df)
    df.dropna(how='all', inplace=True, subset=['open', 'high', 'low', 'close'])
    df.reset_index(inplace=True)
    # df = df[["timestamp", "open", "high", "low", "close"]]
    print(df.tail())
    print(df.dtypes)
    return df


# Step 2: Write to InfluxDB
def write_to_influxdb(data):
    client = InfluxDBClient(url=influxdb_config["url"], token=influxdb_config["token"])
    write_api = client.write_api(write_options=SYNCHRONOUS)

    # Prepare and write data points
    for row in data:
        print((row))
        print(type(row))
        point = (
            Point("nifty50_1d")
            .tag("symbol", 'nifty50_1d')  # Use tags for searchable dimensions
            .field("open", float(row["open"]))
            .field("high", float(row["high"]))
            .field("low", float(row["low"]))
            .field("close", float(row["close"]))
            .field("volume", int(row["volume"]))
            .time(row["timestamp"])  # Assuming `date` is in ISO 8601 format or datetime
        )
        write_api.write(bucket=influxdb_config["bucket"], org=influxdb_config["org"], record=point)

    client.close()


# Step 3: Migrate Data
def migrate_data():
    print("Fetching data from MySQL...")
    data = get_historical_data()
    print(f"Fetched {len(data)} rows.")

    print("Writing data to InfluxDB...")
    write_to_influxdb(data)
    print("Migration completed.")


if __name__ == "__main__":
    migrate_data()
