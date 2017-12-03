import pandas as pd
import queue
from unittest import TestCase

from event import EventType, TickEvent


class TestTickEvent(TestCase):

    """

    """
    def setUp(self):
        data = {'tick': {'instrument': 'EUR_USD', 'time': '2015-07-03T11:55:53.198607Z',
                'bid': 1.10999, 'ask': 1.11004}}
        ticker = data['tick']['instrument']
        time = pd.to_datetime(data['tick']['time'])
        bid = data['tick']['bid']
        ask = data['tick']['ask']
        self.events_queue = queue.Queue()
        self.events_queue.put(TickEvent(ticker, time, bid, ask))

    def test_init(self):
        try:
            tick_event = self.events_queue.get(False)
        except queue.Empty:
            print('Error')
        else:
            self.assertTrue(tick_event is not None)
            self.assertTrue(tick_event.type == EventType.TICK)
            self.assertAlmostEqual(tick_event.bid, 1.10999)
            self.assertAlmostEqual(tick_event.ask, 1.11004)


class TestSignalEvent(TestCase):

    """

    """
    def setUp(self):
        data = {'tick': {'instrument': 'EUR_USD', 'time': '2015-07-03T11:55:53.198607Z',
                'bid': 1.10999, 'ask': 1.11004}}
        ticker = data['tick']['instrument']
        time = pd.to_datetime(data['tick']['time'])
        bid = data['tick']['bid']
        ask = data['tick']['ask']
        self.events_queue = queue.Queue()
        self.events_queue.put(TickEvent(ticker, time, bid, ask))

    def test_init(self):
        try:
            tick_event = self.events_queue.get(False)
        except queue.Empty:
            print('Error')
        else:
            self.assertTrue(tick_event is not None)
            self.assertTrue(tick_event.type == EventType.TICK)
            self.assertAlmostEqual(tick_event.bid, 1.10999)
            self.assertAlmostEqual(tick_event.ask, 1.11004)
