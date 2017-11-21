import datetime
import os, os.path

import pandas as pd
from pandas.util.testing import assert_frame_equal
import queue
from unittest import TestCase

from price_handler.yahoo_daily_csv_bar import YahooDailyCsvBarPriceHandler
from price_parser import PriceParser


class TestYahooDailyCsvBarPriceHandler(TestCase):
    """

    """
    def setUp(self):
        self.csv_dir = './sampledata/'
        self.events_queue = queue.Queue()
        self.init_tickers = ['SPY', 'N^225']

        fieldnames = ['Date', 'Open', 'High', 'Low', 'Close', 'Adj Close', 'Volume']

        ls_us_calendar = pd.to_datetime(['2017-01-03', '2017-01-04', '2017-01-05', '2017-01-06', '2017-01-09'])
        dict_us_data = {
            'Date': ls_us_calendar,
            'Open': [2, 2.5, 3.0, 3.5, 4],
            'High': [3, 3.5, 4.0, 4.5, 5],
            'Low': [1, 1.5, 2.0, 2.5, 3],
            'Close': [2.5, 3.0, 3.5, 4, 4.5],
            'Adj Close': [2.75, 3.25, 3.75, 4.25, 4.75],
            'Volume': [1000, 1050, 1050.5, 1200, 800]
        }
        df_us_data = pd.DataFrame(dict_us_data, columns=fieldnames)
        df_us_data[fieldnames].to_csv(os.path.join(self.csv_dir, 'SPY.csv'), index=False)
        df_us_data.set_index('Date', inplace=True)
        df_us_data['Ticker'] = 'SPY'
        self.df_us_data = df_us_data

        ls_jp_calendar = pd.to_datetime(['2017-01-04', '2017-01-05', '2017-01-06', '2017-01-10', '2017-01-11'])
        dict_jp_data = {
            'Date': ls_jp_calendar,
            'Open': [5, 4.5, 4, 3.5, 3],
            'High': [6, 5.5, 5, 4.5, 4],
            'Low': [4, 3.5, 3, 2.5, 2],
            'Close': [5.5, 5, 4.5, 4, 3.5],
            'Adj Close': [5, 4.5, 4, 3.5, 3],
            'Volume': [2000, 500, 1000, 1250.50, 1500]
        }
        df_jp_data = pd.DataFrame(dict_jp_data, columns=fieldnames)
        df_jp_data[fieldnames].to_csv(os.path.join(self.csv_dir, 'N^225.csv'), index=False)
        df_jp_data.set_index('Date', inplace=True)
        df_jp_data['Ticker'] = 'N^225'
        self.df_jp_data = df_jp_data

        self.price_handler = YahooDailyCsvBarPriceHandler(self.csv_dir, self.events_queue, self.init_tickers)

    def tearDown(self):
        ls_path = [os.path.join(self.csv_dir, '%s.csv' % s) for s in self.init_tickers]
        for path in ls_path:
            os.remove(path)

    def test_subscribe_ticker(self):
        self.assertTrue(isinstance(self.price_handler.tickers_data, dict))
        assert_frame_equal(self.df_us_data, self.price_handler.tickers_data['SPY'])

        self.assertTrue(isinstance(self.price_handler.tickers, dict))
        dict_us_symbol_price = {
            'close': 2.5,
            'adj_close': 2.75,
            'timestamp': datetime.datetime.strptime('2017-01-03', '%Y-%m-%d')
        }
        self.assertEqual(
            dict_us_symbol_price['close'],
            PriceParser.display(self.price_handler.tickers['SPY']['close'], 1)
        )
        self.assertEqual(
            dict_us_symbol_price['adj_close'],
            PriceParser.display(self.price_handler.tickers['SPY']['adj_close'], 2)
        )
        self.assertEqual(dict_us_symbol_price['timestamp'], self.price_handler.tickers['SPY']['timestamp'])

    def test_merge_sort_ticker_data(self):
        # Case1:
        df_comb_data = pd.concat([self.df_us_data, self.df_jp_data]).sort_index()

        for key, actual_row in self.price_handler.bar_stream:
            expected_row = df_comb_data.loc[key]
            if len(expected_row.shape) > 1:
                expected_row = expected_row[expected_row['Ticker'].isin([actual_row['Ticker']])]
                expected_row = expected_row.stack()
                expected_row = expected_row[key]
            self.assertEqual(expected_row['Open'], actual_row['Open'])
            self.assertEqual(expected_row['High'], actual_row['High'])
            self.assertEqual(expected_row['Low'], actual_row['Low'])
            self.assertEqual(expected_row['Close'], actual_row['Close'])
            self.assertEqual(expected_row['Adj Close'], actual_row['Adj Close'])
            self.assertEqual(expected_row['Volume'], actual_row['Volume'])

    def test_stream_next(self):
        self.price_handler.stream_next()
        event = self.events_queue.get(False)
        self.assertEqual(
            PriceParser.display(event.close_price, 1),
            2.5
        )
        self.assertEqual(
            PriceParser.display(event.adj_close_price, 2),
            2.75
        )
        self.assertEqual(
            event.time.strftime(
                '%Y-%m-%d %H:%M:%S'
            ),
            '2017-01-03 00:00:00'
        )

        self.price_handler.stream_next()
        event = self.events_queue.get(False)
        self.assertEqual(
            PriceParser.display(event.close_price, 1),
            5.5
        )
        self.assertEqual(
            PriceParser.display(event.adj_close_price, 1),
            5.0
        )
        self.assertEqual(
            event.time.strftime(
                '%Y-%m-%d %H:%M:%S'
            ),
            '2017-01-04 00:00:00'
        )

        for i in range(2, 10):
            self.price_handler.stream_next()
            event = self.events_queue.get(False)
        self.assertEqual(
            PriceParser.display(event.close_price, 1),
            3.5
        )