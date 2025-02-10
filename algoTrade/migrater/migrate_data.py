import mysql.connector
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
    FROM nifty50_1m
    """
    cursor.execute(query)
    data = cursor.fetchall()
    cursor.close()
    connection.close()
    return data


# Step 2: Write to InfluxDB
def write_to_influxdb(data):
    client = InfluxDBClient(url=influxdb_config["url"], token=influxdb_config["token"])
    write_api = client.write_api(write_options=SYNCHRONOUS)

    # Prepare and write data points
    for row in data:
        point = (
            Point("nifty50_1m")
            .tag("symbol", 'nifty50_1m')  # Use tags for searchable dimensions
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
    data = fetch_mysql_data()
    print(f"Fetched {len(data)} rows.")

    print("Writing data to InfluxDB...")
    write_to_influxdb(data)
    print("Migration completed.")


if __name__ == "__main__":
    migrate_data()
