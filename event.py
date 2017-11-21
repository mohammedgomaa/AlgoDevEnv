from enum import Enum

EventType = Enum('EventType', 'TICK BAR SIGNAL ORDER FILL SENTIMENT')


class Event(object):
    """
    Event is base class providing an interface for all subsequent
    (inherited) event, that will trigger further events in the
    trading infrastructure.
    """
    @property
    def typename(self):
        return self.type.name


class TickEvent(Event):
    """
    Handles the event of receiving a new market update tick,
    which is defined as a ticker symbol and associated best
    bid and ask from the top of the order book.
    """

    def __init__(self, ticker, time, bid, ask):
        """
        Initialises the TickEvent.
        :param ticker: The ticker symbol, e.g. 'GOOG'.
        :param time: The timestamp of the tick.
        :param bid: The best bid price at the time of the tick.
        :param ask: The best ask price at the time of the tick.
        """
        self.type = EventType.TICK
        self.ticker = ticker
        self.time = time
        self.bid = bid
        self.ask = ask

    def __str__(self):
        return 'Type: %s, Ticker: %s, Time: %s, Bid: %s, Ask: %s' %(
            str(self.type), str(self.ticker),
            str(self.time), str(self.bid), str(self.ask)
        )

    def __repr__(self):
        return str(self)


class BarEvent(Event):
    """
    Handles the event of receiving a new market
    open-high-low-close-volume bar, as would be generated
    via common data providers such as Yahoo Finance.
    """
    def __init__(
            self, ticker, time, period,
            open_price, high_price, low_price,
            close_price, volume, adj_close_price=None
    ):
        """
        Initialises the BarEvent.

        :param ticker: The ticker symbol, e.g. 'GOOG'.
        :param time: The timestamp of the bar.
        :param period: The time period covered by the bar in seconds.
        :param open_price: The unadjusted opening price of the bar.
        :param high_price: The unadjusted high price of the bar.
        :param low_price: The unadjusted low price of the bar.
        :param close_price: The unadjusted close price of the bar.
        :param volume: The volume of trading within the bar.
        :param adj_close_price: The vendor adjusted closing price
                    (e.g. back-adjustment) of the bar.

        Note It is not advised to use 'open', 'close' instead
        of 'open_price', 'close_price' as 'open' is a reserved
        word in Python.
        """
        self.type = EventType.BAR
        self.ticker = ticker
        self.time = time
        self.period = period
        self.open_price = open_price
        self.high_price = high_price
        self.low_price = low_price
        self.close_price = close_price
        self.volume = volume
        self.adj_close_price = adj_close_price
        self.period_readable = self._readable_period()

    def _readable_period(self):
        """
        Creates a human-readable period from the number
        of seconds specified for 'period'.

        For instance, converts:
        * 1 -> '1sec'
        * 5 -> '5secs'
        * 60 -> '1min'
        * 300 -> '5min'

        If no period is found in the lookup table, the human
        readable period is simply passed through from period,
        in seconds.

        :return:
        """
        lut = {
            1: '1sec',
            5: '5sec',
            10: '10sec',
            15: '15sec',
            30: '30sec',
            60: '1min',
            300: '5min',
            600: '10min',
            900: '15min',
            1800: '30min',
            3600: '1hr',
            86400: '1day',
            604800: '1wk'
        }
        if self.period in lut:
            return lut[self.period]
        else:
            return '%s sec' % str(self.period)

    def __str__(self):
        format_str = 'Type: %s, Ticker: %s, Time: %s, Period: %s, ' \
            'Open: %s, High: %s, Low: %s, Close: %s, ' \
            'Adj Close: %s, Volume: %s' % (
                str(self.type), str(self.ticker), str(self.time),
                str(self.period_readable), str(self.open_price),
                str(self.high_price), str(self.low_price),
                str(self.close_price), str(self.adj_close_price),
                str(self.volume)
        )
        return format_str

    def __repr__(self):
        return str(self)


class SignalEvent(Event):
    """
    Handles the event of sending a Signal from a Strategy object.
    This is received by a Portfolio object and acted upon.
    """

    def __init__(self, ticker, buy_sell, suggested_quantity=None, datetime=None):
        """
        Initialises the SignalEvent.

        :param str ticker: The ticker symbol, e.g. 'GOOG'.
        :param str buy_sell: 'Buy' or 'Sell'.
        :param int suggested_quantity: Optional positively valued integer
                        representing a suggested absolute quantity of units
                        of an asset to transact in, which is used by the
                        PositionSizer and RiskManger.
        :param timestamp datetime: The timestamp at which the signal was generated.
        """

        self.type = EventType.SIGNAL
        self.ticker = ticker
        self.buy_sell = buy_sell
        self.suggested_quantity = suggested_quantity
        self.datetime = datetime


class OrderEvent(Event):
    """
    Handles the event of sending an Order to an execution system..
    The order contains a symbol (e.g. GOOG), a type(market or limit),
    quantity and a direction
    """

    def __init__(self, ticker, buy_sell, quantity, order_type):
        """
        Initialises the order type, setting whether it is
        a Market order ('MKT') or Limit order('LMT'), has
        a quantity (integral) and its direction ('BUY' or
        'SELL').

        :param str ticker: The ticker symbol, e.g. 'GOOG'.
        :param str buy_sell: 'Buy' or 'Sell'.
        :param int quantity: Non-negative integer for quantity.
        :param str order_type: 'MKT' or 'LMT' for Market or Limit.
        """

        self.type = EventType.ORDER
        self.ticker = ticker
        self.buy_sell = buy_sell
        self.quantity = quantity
        self.order_type = order_type

    def print_order(self):
        """
         Outputs the values within the OrderEvent.
         """
        print(
            "Order: Ticker=%s, BuySell=%s, Quantity=%s, OrderType=%s" % (
                self.ticker, self.buy_sell, self.quantity, self.order_type
            )
        )


class FillEvent(Event):
    """
    Encapsulates the notion of a Fill Order, as returned
    from a brokerage. Stores the quantity of an instrument
    actually filled and at what price. In addition, stores
    the commission of the trade from the brokerage.
    """

    def __init__(self, timeindex, symbol, exchange, quantity,
                 direction, fill_cost, commission=None):
        """
        Initialises the FillEvent objects. Sets the symbol, exchange,
        quantity, direction, cost of fill and an optimal
        commission.

        If commission is not provided, the Fill objects will
        calculate it based on the trade size and Interactive
        Brokers fees.

        :param timeindex: The bar-resolution when thhe order was filled.
        :param symbol: The instrument which was filled.
        :param exchange: The exchange where the order was filled.
        :param quantity: The filled quantity.
        :param direction: The direction of fill ("BUY" or "SELL")
        :param fill_cost: The holdings value in dollars.
        :param commission: An optional commission sent from IB.
        """

        self.type = 'FILL'
        self.timeindex = timeindex
        self.symbol = symbol
        self.exchange = exchange
        self.quantity = quantity
        self.direction = direction
        self.fill_cost = fill_cost

        # Calculate commission
        if commission is None:
            self.commission = self.calculate_ib_commission()
        else:
            self.commission = commission

    def calculate_ib_commission(self):
        """
        Calculates the fees of trading based on an Interactive
        Brokers fee structure for API, in USD.

        This does not include exchange or ECN fees.

        Based on "US API Directed Orders":
        https://www.interactivebrokers.com/en/index.php?f=commission&p=stocks2
        """

        if self.quantity <= 500:
            full_cost = max(1.3, 0.013 * self.quantity)
        else:  # Greater than 500
            full_cost = max(1.3, 0.008 * self.quantity)
        full_cost = min(full_cost, 0.5 / 100 * self.quantity * self.fill_cost)
        return full_cost
