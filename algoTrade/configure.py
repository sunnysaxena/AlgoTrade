import os
import pandas as pd
from constants import *
from dotenv import load_dotenv
from sqlalchemy.pool import QueuePool
from sqlalchemy import create_engine, text
from trade_utils import timeframe_converter as tc

engine = None
# Load environment variables
load_dotenv(dotenv_path='.env')

# Use environment variables for credentials
db_user = os.getenv('DB_USER').strip()  # Strip to remove unwanted spaces
db_password = os.getenv('DB_PASSWORD')
db_host = os.getenv('DB_HOST', 'localhost').strip()
db_name = os.getenv('DB_NAME')




engine = create_engine('mysql://root:Root@###000@localhost/test')