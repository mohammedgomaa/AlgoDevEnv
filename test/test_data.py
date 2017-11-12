import datetime
import os, os.path
import numpy as np
import pandas as pd
from pandas.util.testing import assert_frame_equal
import queue
import csv
from unittest import TestCase
from nose.tools import ok_, eq_

from data import HistoricCSVDataHandler


class TestHistoricalCSVDataHandler(TestCase):
    """

    """
    def setUp(self):
        self.events = queue.Queue()
        self.csv_dir = './sampledata/'
        self.symbol_list = ['SPY', 'N^225']

        fieldnames = ['datetime', 'open', 'high', 'low', 'close', 'adj close', 'volume']

        ls_us_calendar = pd.to_datetime(['2017-01-03', '2017-01-04', '2017-01-05', '2017-01-06', '2017-01-09'])
        dict_us_data = {
            'datetime': ls_us_calendar,
            'open': [2, 2.5, 3.0, 3.5, 4],
            'high': [3, 3.5, 4.0, 4.5, 5],
            'low': [1, 1.5, 2.0, 2.5, 3],
            'close': [2.5, 3.0, 3.5, 4, 4.5],
            'adj close': [2.75, 3.25, 3.75, 4.25, 4.75],
            'volume': [1000, 1050, 1050.5, 1200, 800]
        }
        df_us_data = pd.DataFrame(dict_us_data, columns=fieldnames)
        df_us_data[fieldnames].to_csv(os.path.join(self.csv_dir, 'SPY.csv'), index=False)
        df_us_data.set_index('datetime', inplace=True)
        df_us_data['Symbol'] = 'SPY'
        self.df_us_data = df_us_data

        ls_jp_calendar = pd.to_datetime(['2017-01-04', '2017-01-05', '2017-01-06', '2017-01-10', '2017-01-11'])
        dict_jp_data = {
            'datetime': ls_jp_calendar,
            'open': [5, 4.5, 4, 3.5, 3],
            'high': [6, 5.5, 5, 4.5, 4],
            'low': [4, 3.5, 3, 2.5, 2],
            'close': [5.5, 5, 4.5, 4, 3.5],
            'adj close': [5, 4.5, 4, 3.5, 3],
            'volume': [2000, 500, 1000, 1250.50, 1500]
        }
        df_jp_data = pd.DataFrame(dict_jp_data, columns=fieldnames)
        df_jp_data[fieldnames].to_csv(os.path.join(self.csv_dir, 'N^225.csv'), index=False)
        df_jp_data.set_index('datetime', inplace=True)
        df_jp_data['Symbol'] = 'N^225'
        self.df_jp_data = df_jp_data

        self.bars = HistoricCSVDataHandler(self.events, self.csv_dir, self.symbol_list)

    def tearDown(self):
        ls_path = [os.path.join(self.csv_dir, '%s.csv' % s) for s in self.symbol_list]
        for path in ls_path:
            os.remove(path)

    def test_subscribe_symbol(self):
        self.assertTrue(isinstance(self.bars.symbol_data, dict))
        assert_frame_equal(self.df_us_data, self.bars.symbol_data['SPY'])

        self.assertTrue(isinstance(self.bars.symbols, dict))
        dict_us_symbol_price = {
            'close': 25000000,
            'adj_close': 27500000,
            'timestamp': datetime.datetime.strptime('2017-01-03', '%Y-%m-%d')
        }
        self.assertEqual(dict_us_symbol_price['close'], self.bars.symbols['SPY']['close'])
        self.assertEqual(dict_us_symbol_price['adj_close'], self.bars.symbols['SPY']['adj_close'])
        self.assertEqual(dict_us_symbol_price['timestamp'], self.bars.symbols['SPY']['timestamp'])

    def test_merge_sort_symbol_data(self):
        # Case1:
        df_comb_data = pd.concat([self.df_us_data, self.df_jp_data]).sort_index()

        for key, actual_row in self.bars.bar_stream:
            expected_row = df_comb_data.loc[key]
            if len(expected_row.shape) > 1:
                expected_row = expected_row[expected_row['Symbol'].isin([actual_row['Symbol']])]
                expected_row = expected_row.stack()
                expected_row = expected_row[key]
            self.assertEqual(expected_row['open'], actual_row['open'])
            self.assertEqual(expected_row['high'], actual_row['high'])
            self.assertEqual(expected_row['low'], actual_row['low'])
            self.assertEqual(expected_row['close'], actual_row['close'])
            self.assertEqual(expected_row['adj close'], actual_row['adj close'])
            self.assertEqual(expected_row['volume'], actual_row['volume'])