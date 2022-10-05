import ftx
import datetime
from datetime import timedelta
import numpy as np
import time
import pandas as pd
import math
from statistics import mean
from time import *
from csv import DictWriter
from os.path import exists


# On Init
S_api_key = '511V4FYAfBEBSCi4AOH9FL4YRQTSSBs8SIBGdzFz'
S_api_secret = '8IQFj_oKryBQGl87UoJFi1QzFgOKLDMaZuJw-4rG'
SignIn = ftx.FtxClient(api_key=S_api_key, api_secret=S_api_secret)
AccInfo = SignIn.get_account_info()

orderbook_depth_set = 20  # max 20
trade_history_depth = 100  # max 100

pairs = ['ETH', 'BTC', 'ADA', 'XRP', 'WAVES']
for predict in pairs:

    pair = f'{predict}-PERP'
    file_exists = exists(f'{pair}_ticks.csv')
    if  not file_exists:
        column_names = ['current_time', 'time', 'open', 'high', 'low', 'close', 'volume', 'bid_price_L0', 'bid_volume_L0', 'ask_price_L0', 'ask_volume_L0',
                        'spread_L0', 'bid_price_L1', 'bid_volume_L1', 'ask_price_L1', 'ask_volume_L1', 'spread_L1', 'bid_price_L2', 'bid_volume_L2',
                        'ask_price_L2', 'ask_volume_L2', 'spread_L2', 'bid_price_L3', 'bid_volume_L3', 'ask_price_L3', 'ask_volume_L3', 'spread_L3',
                        'bid_price_L4', 'bid_volume_L4', 'ask_price_L4', 'ask_volume_L4', 'spread_L4', 'bid_price_L5', 'bid_volume_L5', 'ask_price_L5',
                        'ask_volume_L5', 'spread_L5', 'bid_price_L6', 'bid_volume_L6', 'ask_price_L6', 'ask_volume_L6', 'spread_L6', 'bid_price_L7', 'bid_volume_L7',
                        'ask_price_L7', 'ask_volume_L7', 'spread_L7', 'bid_price_L8', 'bid_volume_L8', 'ask_price_L8', 'ask_volume_L8', 'spread_L8', 'bid_price_L9',
                        'bid_volume_L9', 'ask_price_L9', 'ask_volume_L9', 'spread_L9', 'bid_price_L10', 'bid_volume_L10', 'ask_price_L10', 'ask_volume_L10', 'spread_L10',
                        'bid_price_L11', 'bid_volume_L11', 'ask_price_L11', 'ask_volume_L11', 'spread_L11', 'bid_price_L12', 'bid_volume_L12', 'ask_price_L12', 'ask_volume_L12',
                        'spread_L12', 'bid_price_L13', 'bid_volume_L13', 'ask_price_L13', 'ask_volume_L13', 'spread_L13', 'bid_price_L14', 'bid_volume_L14', 'ask_price_L14',
                        'ask_volume_L14', 'spread_L14', 'bid_price_L15', 'bid_volume_L15', 'ask_price_L15', 'ask_volume_L15', 'spread_L15', 'bid_price_L16', 'bid_volume_L16',
                        'ask_price_L16', 'ask_volume_L16', 'spread_L16', 'bid_price_L17', 'bid_volume_L17', 'ask_price_L17', 'ask_volume_L17', 'spread_L17', 'bid_price_L18',
                        'bid_volume_L18', 'ask_price_L18', 'ask_volume_L18', 'spread_L18', 'bid_price_L19', 'bid_volume_L19', 'ask_price_L19', 'ask_volume_L19', 'spread_L19',
                        'sum_bid_volume', 'sum_ask_volume', 'number_buys', 'number_sells', 'total_volume_buys', 'total_volume_sells', 'avg_volume_buys', 'avg_volume_sell',
                        'avg_buy', 'buy_range', 'avg_sell', 'sell_range', 'avg_spread', 'open_interest', 'openusd_interest', 'index_price','mark_price', '1h_change',
                        '24h_change', 'bod_change', '24usd_volume', 'margin_price', 'funding_rate', 'fut_volume']

        with open(f'{pair}_ticks.csv', 'w', newline='') as f_object:
                dictwriter_object = DictWriter(f_object, fieldnames=column_names)
                dictwriter_object.writeheader()

                # Close the file object
                f_object.close()



for i in range(0, 2):
    if i == 1:
        # runs every 30 seconds
        # sleep(30 - time() % 30)
        sleep(30)
    else:
        pass

    for predict in pairs:

        pair = f'{predict}-PERP'

        try:
            trade_history = SignIn.get_trades(f'{pair}')
            orderbook = SignIn.get_orderbook(f'{pair}')
        except Exception:
            trade_history = SignIn.get_trades(f'{pair}')
            orderbook = SignIn.get_orderbook(f'{pair}')

        # Getting a new row of data
        new_row = {}

        # OHLC
        latest_time = (datetime.datetime.today()).timestamp()
        latest_close_time = latest_time - 60
        sec_latest_close_time = latest_time - 2*60
        current_row = pd.DataFrame(SignIn.get_historical_data(f'{pair}', 60, 1,
                                                                  sec_latest_close_time, latest_close_time))


        new_row['current_time'] = int(time())
        new_row['time'] = int(current_row['time'])
        new_row['open'] = float(current_row['open'])
        new_row['high'] = float(current_row['high'])
        new_row['low'] = float(current_row['low'])
        new_row['close'] = float(current_row['close'])
        new_row['volume'] = float(current_row['volume'])


        # Orderbook
        bid_prices, bid_volumes, ask_prices, ask_volumes, level_spreads = [], [], [], [], []


        for i in range(0, orderbook_depth_set):
            new_row[f'bid_price_L{i}'] = orderbook['bids'][i][0]
            new_row[f'bid_volume_L{i}'] = orderbook['bids'][i][1]
            new_row[f'ask_price_L{i}'] = orderbook['asks'][i][0]
            new_row[f'ask_volume_L{i}'] = orderbook['asks'][i][1]
            new_row[f'spread_L{i}'] = orderbook['bids'][i][0] - orderbook['asks'][i][0]

            bid_prices.append(orderbook['bids'][i][0])
            bid_volumes.append(orderbook['bids'][i][1])
            ask_prices.append(orderbook['asks'][i][0])
            ask_volumes.append(orderbook['asks'][i][1])


        new_row['sum_bid_volume'] = sum(bid_volumes)
        new_row['sum_ask_volume'] = sum(ask_volumes)


        # Trade history
        buy_prices =[]
        buy_volume = []
        buy_count = 0

        sell_prices =[]
        sell_volume = []
        sell_count = 0


        for i in range(0, trade_history_depth):
            if trade_history[i]['side'] == 'buy':
                buy_prices.append(trade_history[i]['price'])
                buy_volume.append(trade_history[i]['size'])
                buy_count += 1
            else:
                sell_prices.append(trade_history[i]['price'])
                sell_volume.append(trade_history[i]['size'])
                sell_count += 1

        new_row['number_buys'] = buy_count
        new_row['number_sells'] = sell_count

        new_row['total_volume_buys'] = sum(buy_volume)
        new_row['total_volume_sells'] = sum(sell_volume)

        if buy_count != 0:
            new_row['avg_volume_buys'] = mean(buy_volume)
            new_row['avg_buy'] = mean(buy_prices)
            new_row['buy_range'] = max(buy_prices) - min(buy_prices)
        elif buy_count == 0:
            new_row['avg_volume_buys'] = 0
            new_row['avg_buy'] = 0
            new_row['buy_range'] = 0

        if sell_count != 0:
            new_row['avg_volume_sell'] = mean(sell_volume)
            new_row['avg_sell'] = mean(sell_prices)
            new_row['sell_range'] = max(sell_prices) - min(sell_prices)
        elif sell_count == 0:
            new_row['avg_volume_sell'] = 0
            new_row['avg_sell'] = 0
            new_row['sell_range'] = 0

        new_row['avg_spread'] = new_row['avg_buy'] - new_row['avg_sell']




        # Future stats
        # TODO: Check column names
        future_stats = SignIn.get_future(f'{pair}')
        new_row['open_interest'] = future_stats['openInterest']
        new_row['openusd_interest'] = future_stats['openInterestUsd']
        new_row['index_price'] = future_stats['index']
        new_row['mark_price'] = future_stats['mark']
        new_row['1h_change'] = future_stats['change1h']
        new_row['24h_change'] = future_stats['change24h']
        new_row['bod_change'] = future_stats['changeBod']
        new_row['24usd_volume'] = future_stats['volumeUsd24h']
        new_row['margin_price']= future_stats['marginPrice']


        future_rates = SignIn.get_future_stats(f'{pair}')
        new_row['funding_rate'] = future_rates['nextFundingRate']
        new_row['fut_volume'] = future_rates['volume']


        field_names = list(new_row.keys())

        with open(f'{pair}_ticks.csv', 'a', newline='') as f_object:
            dictwriter_object = DictWriter(f_object, fieldnames=field_names)
            dictwriter_object.writerow(new_row)
            f_object.close()

            print(f'done FTX time: {int(new_row["time"])}, current_time: {int(new_row["current_time"])}')
