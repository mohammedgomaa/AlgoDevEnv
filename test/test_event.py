import event
import queue
from unittest import TestCase
from nose.tools import ok_, eq_

from event import MarketEvent

class TestMarketEvent(TestCase):

    """

    """
    def setUp(self):
        self.events = queue.Queue()
        self.events.put(event.MarketEvent())

    def test_init(self):
        try:
            market_event = self.events.get(False)
        except queue.Empty:
            print('Error')
        else:
            ok_(market_event is not None)
            ok_(market_event.type == 'MARKET')
