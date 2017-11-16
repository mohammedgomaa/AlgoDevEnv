import datetime
import queue

class BuyAndHoldStrategy(AbstractStrategy):
    """
    A testing strategy that simply purchase (longs) an asset
    upon first receipt of the relevant bar event and
    then holds until the completion of a backtest.
    """

    def __init__(
            self, symbol, events_queue,
            base_quantity=100
    ):
        """
        Initialises the buy and hold strategy.

        :param symbol: The DataHandler object that provides bar information
        :param events_queue: The Event Queue object
        """
        self.symbol = symbol
        self.events_queue = events_queue
        self.base_quantity = base_quantity
        self.bars = 0
        self.invested = False

    def calculate_signals(self, event):
        """
        For "Buy and Hold"  we generate a single signal per symbol
        and then no additional signals. This means we are
        constantly long the market from the date of strategy
        Initialisation.

        :param event: A MarketEvent object.
        :return:
        """
        if ( # TO DO: rename EventType in backtest.py
            event.type in [EventType.BAR, EventType.TICK] and
            event.symbol == self.symbol
        ) :
            if not self.invested and self.bars == 0:
                signal = SignalEvent(
                    self.symbol, "BOT",
                    suggested_quantity = self.bars_quantity
                )
                self.events_queue.put(signal)
                self.invested = True
            self.bars += 1

    def run(config, testing, symbols, filename):
        # Backtest information
        title = ['Buy and Hold Example on %s' % symbols[0]]
        initial_equity = 10000.0
        start_date = datetime.datetime(2000, 1, 1)
        end_date = datetime.datetime(2014, 1, 1)

        # Use the Buy and Hold Strategy
        events_queue = queue.Queue()
        strategy = BuyAndHoldStrategy(symbols[0], events_queue)

        # Set up the backtest
        # To Do: modify backtest.py so that we can set up the backtest.
        #
        pass

    if __name__ == '__main__':
        # Configuration data
        testing = False
        config = settings.from_file(
            settings.DEFAULT_CONFIG_FILENAME, testing
        )
        symbols = ['SPY']
        filename = None
        run(config, testing, symbols, filename)