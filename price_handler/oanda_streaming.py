import oandapy

from event import TickEvent
from price_handler.base import AbstractTickPriceHandler


class OANDAStreamingPriceHandler(AbstractTickPriceHandler):
    def __init__(
            self, domain, access_token,
            account_id, pairs, events_queue
    ):
