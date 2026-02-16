from __future__ import print_function

from decimal import Decimal, getcontext, ROUND_HALF_DOWN
import json
import logging
import threading

import websocket

from qsCrypto.event.event import TickEvent
from qsCrypto.data.price import PriceHandler
from qsCrypto import settings


class StreamingCryptoPrices(PriceHandler):
    """
    Connects to Binance WebSocket book-ticker streams and
    places TickEvents onto the events queue.
    """

    def __init__(self, pairs, events_queue, test_mode=True, market_type="usdm_futures"):
        self.pairs = pairs
        self.events_queue = events_queue
        self.test_mode = test_mode
        self.market_type = market_type
        self.prices = self._set_up_prices_dict()
        self.logger = logging.getLogger(__name__)

        env_label = "testnet" if test_mode else "production"
        self.ws_base_url = settings.BINANCE_ENVIRONMENTS[market_type]["streaming"][env_label]

    def _set_up_prices_dict(self):
        """
        For crypto we don't need inverse pairs like forex.
        Just set up one entry per pair.
        """
        return {
            p: {"bid": None, "ask": None, "time": None}
            for p in self.pairs
        }

    def _build_ws_url(self):
        """
        Build a combined stream URL like:
        wss://stream.binancefuture.com/stream?streams=btcusdt@bookTicker/ethusdt@bookTicker
        """
        streams = "/".join(
            f"{pair.lower()}@bookTicker" for pair in self.pairs
        )
        return f"{self.ws_base_url}/stream?streams={streams}"

    def _on_message(self, ws, message):
        try:
            msg = json.loads(message)
            data = msg.get("data", msg)

            symbol = data.get("s", "").upper()
            if symbol not in self.prices:
                return

            getcontext().rounding = ROUND_HALF_DOWN
            bid = Decimal(str(data["b"])).quantize(Decimal("0.00001"))
            ask = Decimal(str(data["a"])).quantize(Decimal("0.00001"))
            time_val = str(data.get("T", data.get("E", "")))

            self.prices[symbol]["bid"] = bid
            self.prices[symbol]["ask"] = ask
            self.prices[symbol]["time"] = time_val

            tev = TickEvent(symbol, time_val, bid, ask)
            self.events_queue.put(tev)

        except Exception as e:
            self.logger.error("Error processing WS message: %s", e)

    def _on_error(self, ws, error):
        self.logger.error("WebSocket error: %s", error)

    def _on_close(self, ws, close_status_code, close_msg):
        self.logger.info("WebSocket closed: %s %s", close_status_code, close_msg)

    def _on_open(self, ws):
        self.logger.info("WebSocket connection opened")

    def stream_to_queue(self):
        """
        Connect to the Binance WebSocket and start streaming.
        Blocks the calling thread.
        """
        url = self._build_ws_url()
        self.logger.info("Connecting to WebSocket: %s", url)
        self.ws = websocket.WebSocketApp(
            url,
            on_message=self._on_message,
            on_error=self._on_error,
            on_close=self._on_close,
            on_open=self._on_open,
        )
        self.ws.run_forever()

    def stop(self):
        if hasattr(self, "ws") and self.ws:
            self.ws.close()
