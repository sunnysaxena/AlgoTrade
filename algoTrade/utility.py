import datetime
import pandas as pd
from datetime import datetime


def generate_dates(end='2023-09-30', periods=1095 + 366 + 365 + 365 + 80):
    df = pd.date_range(end=end, periods=periods).to_pydatetime().tolist()
    dates = [d.strftime("%Y-%m-%d") for d in df]
    return dates


def epoc_to_timestamp(epoch_time):
    if issubclass(type(epoch_time), list):
        return [datetime.fromtimestamp(ep_time).strftime('%Y-%m-%d %H:%M:%S') for ep_time in epoch_time]
    else:
        return datetime.fromtimestamp(epoch_time).strftime('%Y-%m-%d %H:%M:%S')


def epoc_to_timestamp1(epoch_time):
    print(epoch_time)
    if issubclass(type(epoch_time), list):
        return [datetime.fromtimestamp(ep_time).strftime('%Y-%m-%d') for ep_time in epoch_time]
    else:
        return datetime.fromtimestamp(epoch_time).strftime('%Y-%m-%d')


def timestamp_to_epoc(df):
    df['timestamp'] = pd.to_datetime(df['timestamp'])

    # Convert datetime to Unix timestamp
    df['epoch'] = (df['timestamp'] - pd.Timestamp("1970-01-01")) // pd.Timedelta('1ms')
    return df


def get_today_date():
    return datetime.today().strftime("%Y-%m-%d")


def delete_duplicate_rows(df, verbose=False):
    number = df.duplicated(subset=["open", "high", "low", "close", "volume"], keep=False).sum()

    # check for duplicated indexes
    duplicated_indexes = df.duplicated(keep=False, subset=["open", "high", "low", "close", "volume"])
    duplicated_rows = df[duplicated_indexes]

    # drop duplicated indexes
    df = df[~df.duplicated(keep=False, subset=["open", "high", "low", "close", "volume"])]
    df.drop_duplicates(inplace=True)

    if verbose:
        print(duplicated_rows)
    return df


def delete_duplicate_rows_subset(df, subset, verbose=False):
    number = df.duplicated(subset=subset, keep=False).sum()

    # check for duplicated indexes
    duplicated_indexes = df.duplicated(keep=False, subset=subset)
    duplicated_rows = df[duplicated_indexes]

    # drop duplicated indexes
    df = df[~df.duplicated(keep=False, subset=subset)]
    df.drop_duplicates(inplace=True)

    if verbose:
        print(duplicated_rows)
    return df


def merge_df_left_timestamp(df1, df2):
    return pd.merge(df1, df2, how='left', left_on="timestamp", right_on="timestamp")


def lots_to_buy(capital, lot_size, price_per_share, transaction_fee=0):
    """
    Calculate the number of lots you can buy with a fixed capital.

    Args:
        capital (float): The total amount of capital available.
        lot_size (int): The number of shares in one lot.
        price_per_share (float): The price of a single share.
        transaction_fee (float): Any fixed transaction fee (default is 0).

    Returns:
        int: The number of lots you can buy.
        float: The remaining capital after the purchase.

    Usage:
        capital = 21000  # Fixed capital
        lot_size = 75  # Number of shares in one lot
        price_per_share = 20  # Price of a single share

        lots, remaining_capital = calculate_lots_to_buy(capital, lot_size, price_per_share)

        print(f"Lots you can buy: {lots}")
        print(f"Total share : {lots*lot_size:}")
        print(f"Used capital: {(lots*lot_size)*price_per_share:}")
        print(f"Remaining capital: {remaining_capital:.2f}")
    """

    if price_per_share <= 0 or lot_size <= 0:
        raise ValueError("Price per share and lot size must be greater than zero.")
    if capital <= transaction_fee:
        return 0, capital  # Not enough to buy any lots after fees.

    # Calculate the price of one lot
    price_per_lot = lot_size * price_per_share

    # Capital available after deducting the transaction fee
    available_capital = capital - transaction_fee

    # Calculate the number of lots
    lots = int(available_capital // price_per_lot)  # Floor division to get whole lots

    # Calculate remaining capital
    # remaining_capital = available_capital - (lots * price_per_lot)

    total_share = lots * lot_size

    # return lots
    return total_share


if __name__ == '__main__':
    capital = 20000  # Fixed capital
    lot_size = 75  # Number of shares in one lot
    price_per_share = 20  # Price of a single share

    lots = lots_to_buy(capital, lot_size, price_per_share)

    print(f"Current LTP : {price_per_share}")
    print(f"Lots you can buy : {lots}")
    print(f"Total share : {lots * lot_size:}")
    print(f"Used capital : {(lots * lot_size) * price_per_share:}")
    # print(f"Remaining capital: {remaining_capital:.2f}")
