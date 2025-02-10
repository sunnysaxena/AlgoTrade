import os
import pandas as pd
from constants import *
from dotenv import load_dotenv
from urllib.parse import quote_plus
from sqlalchemy.pool import QueuePool
from sqlalchemy import create_engine, text
from trade_utils import timeframe_converter as tc

# Load environment variables
load_dotenv(dotenv_path='.env')

# Use environment variables for credentials
db_user = os.getenv('DB_USER').strip()  # Strip to remove unwanted spaces
db_password = os.getenv('DB_PASSWORD')
db_host = os.getenv('DB_HOST', 'localhost').strip()
db_name = os.getenv('DB_NAME')

# URL-encode the password
db_password = quote_plus(db_password)

# Create engine with connection pooling
try:
    connection_string = f"mysql+mysqldb://{db_user}:{db_password}@{db_host}/{db_name}"
    engine = create_engine(
        connection_string,
        poolclass=QueuePool,
        pool_size=10,
        max_overflow=20
    )
    print("Database connection established successfully.")
except Exception as e:
    print(f"Failed to connect to the database: {e}")

def get_historical_data(table_name=None, time_interval='5'):
    query = text(f"SELECT timestamp, open, high, low, close, volume FROM {table_name}")
    try:
        with engine.connect() as connection:
            df = pd.read_sql(query, connection)
            if time_interval == '5':
                df = tc.minute_1_to_five5(df)
            df.reset_index(drop=False, inplace=True)
            df = df.drop_duplicates(subset=['timestamp'])
            df = df.dropna(how='all', subset=['open', 'high', 'low', 'close'])
            return df
    except Exception as e:
        print(f"Something went wrong while executing the query: {e}")
        return pd.DataFrame()  # Return an empty DataFrame in case of error

if __name__ == '__main__':
    get_historical_data('nifty50_1m', time_interval='5')
