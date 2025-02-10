### EMA Traging Strategy

import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
from pandas_datareader import data
from datetime import date
from matplotlib import gridspec


# Function signals
def buy_sell_function(data):
    buy_list = []
    sell_list = []
    Advice = []
    flag_long = False
    flag_short = False

    for i in range(0, len(data)):
        if data['Middle'][i] < data['Long'][i] and data['Short'][i] < data['Middle'][
            i] and flag_long == False and flag_short == False:
            buy_list.append(data['Close'][i])
            sell_list.append(np.nan)
            Advice.append("Buy")
            flag_short = True
        elif flag_short == True and data['Short'][i] > data['Middle'][i]:
            sell_list.append(data['Close'][i])
            buy_list.append(np.nan)
            Advice.append("Sell")
            flag_short = False
        elif data['Middle'][i] > data['Long'][i] and data['Short'][i] > data['Middle'][
            i] and flag_long == False and flag_short == False:
            buy_list.append(data['Close'][i])
            Advice.append("Buy")
            sell_list.append(np.nan)
            flag_long = True
        elif flag_long == True and data['Short'][i] < data['Middle'][i]:
            sell_list.append(data['Close'][i])
            buy_list.append(np.nan)
            Advice.append("Sell")
            flag_long = False
        else:
            buy_list.append(np.nan)
            sell_list.append(np.nan)
            Advice.append("Hold")
    return buy_list, sell_list, Advice


if __name__ == '__main__':
    # plt.style.use('seaborn')
    ticker = input('Introduzca su ticker: ')
    df = data.DataReader(ticker, 'yahoo', '2020-01-01')

    # Calculate EMAS
    ShortEMA = df.Close.ewm(span=5, adjust=False).mean()
    MidEMA = df.Close.ewm(span=21, adjust=False).mean()
    LongEMA = df.Close.ewm(span=63, adjust=False).mean()

    # Add EMA's to the dataset
    df['Short'] = ShortEMA
    df['Middle'] = MidEMA
    df['Long'] = LongEMA
    df['Buy'] = buy_sell_function(df)[0]
    df['Sell'] = buy_sell_function(df)[1]
    df['Advice'] = buy_sell_function(df)[2]

    Fecha = date.today().strftime("%d-%m-%Y")
    n = len(df['Advice']) - 1
    advice = df['Advice'].iloc[n:].to_string(header=False, index=False)

    # Chart Title - Recommendation
    titulo = 'Hoy  ' + Fecha + '  ' + advice + '  ' + ticker

    plt.figure(figsize=(8, 4))
    plt.title(titulo, fontsize=14, fontweight="bold", x=0.12, horizontalalignment='left')
    plt.plot(df.index, df.Close, label='Close Price', color='Blue', alpha=0.85)
    plt.plot(df.index, ShortEMA, label='Short EMA', color='red', alpha=0.35)
    plt.plot(df.index, MidEMA, label='Mid EMA', color='orange', alpha=0.35)
    plt.plot(df.index, LongEMA, label='Long EMA', color='green', alpha=0.35)
    plt.scatter(df.index, df['Buy'], marker="^", color='green', alpha=1)
    plt.scatter(df.index, df['Sell'], marker="v", color='red', alpha=1)
    plt.ylabel('Precio de cierre')
    plt.xticks(rotation=90)
    plt.legend(loc=0)
    plt.show()