import time
import pandas as pd
from my_fyers_model import MyFyersModel

# Show all rows and columns
pd.set_option('display.max_rows', None)  # Show all rows
pd.set_option('display.max_columns', None)  # Show all columns

fyers = MyFyersModel()

data = {
    "symbol":"NSE:NIFTY50-INDEX",
    "strikecount":1,
    "timestamp": ""
}

response = fyers.get_option_chain(data=data)
# print(response['data'])

io_data = pd.DataFrame.from_dict(response['data']['optionsChain'])
print(io_data.columns)
print(io_data)

