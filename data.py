import datetime
import os, os.path
import pandas as pd

from abc import ABCMeta, abstractmethod

from price_parser import PriceParser
from event import MarketEvent

class DataHandler(object):
    """
    DataHandler is an abstract base class providing an interface for
    all subsequent (inherited) data handlers (both live and historic).

    The goal of a (derived) DataHandler object is to output a generated
    set of bars (OLHCVI) for each symbol requested.

    This will replicate how a live strategy would function as current
    market data would be sent "down the pipe". Thus a historic and live
    system will be treated identically by the rest of the backtesting suite.
    """

    __metaclass__ = ABCMeta

    @abstractmethod
    def get_latest_bars(self, symbol, N=1):
        """
        Returns the last N bars from the latest_symbol list,
        or fewer if less bars are available
        :param symbol:
        :param N:
        :return:
        """
        raise NotImplementedError("should implement get_latest_bars()")

    @abstractmethod
    def update_bars(self):
        """
        Pushes the latest bar to the latest symbol structure
        for all symbols in the symbol list.
        :return:
        """
        raise NotImplementedError("Shhould implement update_bars()")

class HistoricCSVDataHandler(DataHandler):
    """
    HistoricCSVDataHandler is designed to read CSV files for
    each requested symbol from disk and provide an interface
    to obtain the "latest" bar in a manner identical to a live
    trading interface.
    """

    def __init__(self, events, csv_dir, symbol_list):
        """
        Initialises the historic data handler by requesting
        the location of the CSV files and a list of symbols.

        It will be assumed that all files are of the form
        "symbol.csv", where symbol is a string in the list.

        :param events: The Event Queue.
        :param csv_dir: Absolute directory path to CSV files.
        :param symbol_list: A list of symbol strings.
        """
        self.events = events
        self.csv_dir = csv_dir
        self.continue_backtest = True

        self.symbols = {}
        self.symbol_data = {}
        if symbol_list is not None:
            for symbol in symbol_list:
                self.subscribe_symbol(symbol)
        self.bar_stream = self._merge_sort_symbol_data()

    def subscribe_symbol(self, symbol):
        """
        Subscribes the price handler to a new symbol
        :param symbol:
        :return:
        """
        if symbol not in self.symbols:
            try:
                self._open_convert_csv_files(symbol)
                dft = self.symbol_data[symbol]
                row0 = dft.iloc[0]

                close = PriceParser.parse(row0['close'])
                adj_close = PriceParser.parse(row0['adj close'])

                symbol_prices = {
                    'close': close,
                    'adj_close': adj_close,
                    'timestamp': dft.index[0]
                }
                self.symbols[symbol] = symbol_prices
            except OSError:
                print(
                    'Could not subscribe symbol %s'
                    'as no data CSV found for pricing.' % symbol
                )
        else:
            print(
                'Could not subscribe symbol %s'
                'as is already subscribed.' % symbol
            )

    def _open_convert_csv_files(self, symbol):
        """
        Opens the CSV files from the data directory, converting
        them into pandas DataFrames within a symbol dictionary.

        For this handler it will be assumed that the data is
        taken from Yahoo finance. Thus its format will be respected.
        :return:
        """
        symbol_path = os.path.join(self.csv_dir, '%s.csv' % symbol)

        # Load the CSV file with no header information, indexed on date
        self.symbol_data[symbol] = pd.read_csv(
                                        symbol_path, header=0,
                                        names=['datetime', 'open', 'high', 'low', 'close', 'adj close', 'volume'],
                                        index_col='datetime', parse_dates=True
                                        )
        self.symbol_data[symbol]['Symbol'] = symbol

    def _merge_sort_symbol_data(self):
        """
        Concatenates all of the separate equities DataFrames
        into a single DataFrame that is time ordered, allowing tick
        data events to be added to the queue in a chronological fashion.
        Note that this is an idealised situation, utilised solely for
        backtesting. In live trading ticks may arrive "out of order".
        """

        df = pd.concat(self.symbol_data.values()).sort_index()
        start = None
        end = None
        if start is None and end is None:
            return df.iterrows()


    def _get_new_bar(self, symbol):
        """
        Returns the latest bar from tha data feed as a tuple of
        (symbol, datetime, open, low, high, close, volume).
        :param symbol:
        :return:
        """
        for b in self.symbol_data[symbol]:
            yield tuple([symbol, datetime.datetime.strptime(b[0], '%Y-%m-%d %H:%M:%S'),
                         b[1][0], b[1][1], b[1][2], b[1][3], b[1][4]])

    def get_latest_bars(self, symbol, N=1):
        """
        Returns the last N bars from the latest_symbol list,
        or N-k if less available.
        :param symbol:
        :param N:
        :return:
        """
        try:
            bars_list = self.latest_symbol_data[symbol]
        except KeyError:
            print("That symbol is not available in the historical data set.")
        else:
            return bars_list[-N:]

    def update_bars(self):
        """
        Pushes the latest bar to the latest_symbol_data structure
        for all symbols in the symbol list.
        :return:
        """
        for s in self.symbol_list:
            try:
                bar = self._get_new_bar(s).next()
            except StopIteration:
                self.continue_backtest = False
            else:
                if bar is not None:
                    self.latest_symbol_data[s].append(bar)
        self.events.put(MarketEvent)