import queue
from unittest import TestCase

from config import OANDA
from price_handler.oanda_streaming import OANDAStreamingPriceHandler
from price_parser import PriceParser


class TestOANDAStreamingPriceHandler(TestCase):

    def setUp(self):
        self.events_queue = queue.Queue()
        self.ticks = 0
        self.tickers = ['EUR_USD', 'USD_JPY']
        self.price_handler = OANDAStreamingPriceHandler(
            OANDA.DOMAIN, OANDA.ACCESS_TOKEN, OANDA.ACCOUNT_ID,
            self.tickers, self.events_queue
        )

    def tearDown(self):
        self.price_handler.disconnect()

    def test_on_success(self):
        self.price_handler.rates(
            account_id=OANDA.ACCOUNT_ID, instruments=','.join(self.tickers), ignore_heartbeat=True)
       # self.price_handler.stream_next()
        event = self.events_queue.get(False)
        print('[%s] %s bid=%s, ask=%s' % (
            event.time, event.ticker, event.bid, event.ask
        ))
