import pandas as pd
import json
import oandapy
from price_parser import PriceParser
from event import TickEvent


# Extend the exceptions to support extra cases

class OandaRequestError(oandapy.OandaError):
    def __init__(self):
        er = dict(code=599, message='Request Error', description='')
        super(self.__class__, self).__init__(er)


class OandaStreamError(oandapy.OandaError):
    def __init__(self, content=''):
        er = dict(code=598, message='Failed Streaming', description=content)
        super(self.__class__, self).__init__(er)


class OandaTimeFrameError(oandapy.OandaError):
    def __init__(self, content):
        er = dict(code=597, message='Not supported TimeFrame', description='')
        super(self.__class__, self).__init__(er)


class OandaNetworkError(oandapy.OandaError):
    def __init__(self):
        er = dict(code=596, message='Network Error', description='')
        super(self.__class__, self).__init__(er)


class OANDAStreamingPriceHandler(oandapy.Streamer):
    def __init__(
        self, domain, access_token,
        account_id, init_tickers, events_queue, headers=None,
    ):
        # Override to provide headers, which is in the standard API interface
        super().__init__(environment=domain, access_token=access_token)

        if headers:
            self.client.headers.update(headers)

        self.account_id = account_id
        self.tickers_list = init_tickers
        self.tickers = {}
        self.tickers_data = {}
        for ticker in self.tickers_list:
            self.tickers[ticker] = {}
        self.events_queue = events_queue
        self.price_event = None

        #self.rates(account_id=self.account_id, instruments=','.join(self.tickers_lst))

    def run(self, endpoint, params=None):
        # Override to better manage exceptions.
        # Kept as much as possible close to the original
        self.connected = True

        params = params or {}

        ignore_heartbeat = None
        if 'ignore_heartbeat' in params:
            ignore_heartbeat = params['ignore_heartbeat']

        request_args = {}
        request_args['params'] = params

        url = '%s/%s' % (self.api_url, endpoint)

        while self.connected:
            # Added exception control here
            try:
                response = self.client.get(url, **request_args)
            except requests.RequestException as e:
                self.events_queue.put(OandaRequestError().error_response)
                break

            if response.status_code != 200:
                self.on_error(response.content)
                break  # added break here

            # Changed chunk_size 90 -> None
            try:
                for line in response.iter_lines(chunk_size=None):
                    if not self.connected:
                        break

                    if line:
                        data = json.loads(line.decode('utf-8'))
                        if not (ignore_heartbeat and 'heartbeat' in data):
                            self.on_success(data)

            except:  # socket.error has been seen
                self.events_queue.put(OandaStreamError().error_response)
                break

    def on_success(self, data):
        """
        {"tick": {"instrument": "EUR_USD", "time": "2014-03-07T20:58:07.461445Z", "bid": 1.38701, "ask": 1.38712}}
        {"tick": {"instrument": "EUR_USD", "time": "2014-03-07T20:58:09.345955Z", "bid": 1.38698, "ask": 1.38709}}
        {"tick": {"instrument": "USD_CAD", "time": "2014-03-07T20:58:12.320218Z", "bid": 1.10906, "ask": 1.10922}}
        {"tick": {"instrument": "USD_CAD", "time": "2014-03-07T20:58:12.360615Z", "bid": 1.10904, "ask": 1.10925}}
        """
        if 'tick' in data:
            tev = self._create_event(data['tick'])
            self.price_event = tev
            self.stream_next()

    def _create_event(self, data):
        """
        ticker = dfr['instrument']
        time = dfr['time']
        bid = dfr['bid']
        ask = dfr['ask']
        """
        ticker = data['instrument']
        time = pd.to_datetime(data['time'])
        bid = PriceParser.parse(data['bid'])
        ask = PriceParser.parse(data['ask'])
        return TickEvent(ticker, time, bid, ask)

    def _store_event(self, event):
        ticker = event.ticker
        self.tickers[ticker]['timestamp'] = event.time
        self.tickers[ticker]['bid'] = event.bid
        self.tickers[ticker]['ask'] = event.ask

    def stream_next(self):
        if self.price_event is not None:
            self._store_event(self.price_event)
            self.events_queue.put(self.price_event)
            self.price_event = None

    def on_error(self, data):
        self.disconnect()