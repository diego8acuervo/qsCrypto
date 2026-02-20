from __future__ import print_function

from decimal import Decimal, getcontext, ROUND_HALF_DOWN
import json
import logging
import threading
import time as _time

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

class StreamingBitgetPrices(PriceHandler):
    """
    Connects to the Bitget public WebSocket (v2) and subscribes to
    the ``ticker`` channel for each requested pair.  Delivers
    TickEvents with the same interface as StreamingCryptoPrices so
    it can be used as a drop-in replacement inside the trading loop.

    Parameters
    ----------
    pairs : list[str]
        Instrument IDs as used on Bitget, e.g. ["BTCUSDT", "ETHUSDT"].
    events_queue : queue.Queue
        Shared events queue.
    inst_type : str
        Bitget product-line type.  One of:
        "USDT-FUTURES", "COIN-FUTURES", "USDC-FUTURES", "SPOT".
        Defaults to "USDT-FUTURES".
    ws_url : str | None
        Override the public WebSocket endpoint (useful for testing).
    ping_interval : int
        Seconds between client-side "ping" keep-alives (default 25).
    """

    WS_PUBLIC_URL = "wss://ws.bitget.com/v2/ws/public"

    def __init__(
        self,
        pairs,
        events_queue,
        inst_type="USDT-FUTURES",
        ws_url=None,
        ping_interval=25,
    ):
        self.pairs = pairs
        self.events_queue = events_queue
        self.inst_type = inst_type
        self.ws_url = ws_url or self.WS_PUBLIC_URL
        self.ping_interval = ping_interval
        self.prices = self._set_up_prices_dict()
        self.logger = logging.getLogger(__name__)
        self.ws = None
        self._ping_thread = None
        self._stop_ping = threading.Event()

    # ── prices dict (same contract as StreamingCryptoPrices) ──────

    def _set_up_prices_dict(self):
        """One entry per pair — no inverse pairs needed for crypto."""
        return {
            p: {"bid": None, "ask": None, "time": None}
            for p in self.pairs
        }

    # ── subscribe message ─────────────────────────────────────────

    def _build_subscribe_message(self):
        """
        Build the Bitget ``subscribe`` op message for the ticker
        channel of every configured pair.
        """
        args = [
            {
                "instType": self.inst_type,
                "channel": "ticker",
                "instId": pair,
            }
            for pair in self.pairs
        ]
        return json.dumps({"op": "subscribe", "args": args})

    # ── keep-alive ping ───────────────────────────────────────────

    def _ping_loop(self, ws):
        """
        Bitget requires a string ``"ping"`` every ≤30 s to keep the
        connection alive; it replies with ``"pong"``.
        """
        while not self._stop_ping.is_set():
            try:
                ws.send("ping")
            except Exception:
                break
            self._stop_ping.wait(self.ping_interval)

    # ── WebSocket callbacks ───────────────────────────────────────

    def _on_open(self, ws):
        self.logger.info("Bitget WebSocket opened — subscribing to ticker")
        ws.send(self._build_subscribe_message())
        # Start the keep-alive ping thread
        self._stop_ping.clear()
        self._ping_thread = threading.Thread(
            target=self._ping_loop, args=(ws,), daemon=True
        )
        self._ping_thread.start()

    def _on_message(self, ws, message):
        # Bitget pong is a plain string, not JSON
        if message == "pong":
            return

        try:
            msg = json.loads(message)
        except json.JSONDecodeError as e:
            self.logger.error("Bitget WS JSON decode error: %s", e)
            return

        # Subscription confirmation / error
        event = msg.get("event")
        if event == "subscribe":
            self.logger.info("Bitget subscribed: %s", msg.get("arg"))
            return
        if event == "error":
            self.logger.error(
                "Bitget WS error code=%s msg=%s",
                msg.get("code"), msg.get("msg"),
            )
            return

        # ── ticker push data ─────────────────────────────────────
        data_list = msg.get("data")
        if not data_list:
            return

        for tick in data_list:
            # instId is always present in the push payload
            symbol = tick.get("instId", tick.get("symbol", "")).upper()
            if symbol not in self.prices:
                continue

            bid_raw = tick.get("bidPr")
            ask_raw = tick.get("askPr")
            if bid_raw is None or ask_raw is None:
                continue

            getcontext().rounding = ROUND_HALF_DOWN
            bid = Decimal(str(bid_raw)).quantize(Decimal("0.00001"))
            ask = Decimal(str(ask_raw)).quantize(Decimal("0.00001"))
            time_val = str(tick.get("ts", ""))

            self.prices[symbol]["bid"] = bid
            self.prices[symbol]["ask"] = ask
            self.prices[symbol]["time"] = time_val

            tev = TickEvent(symbol, time_val, bid, ask)
            self.events_queue.put(tev)

    def _on_error(self, ws, error):
        self.logger.error("Bitget WebSocket error: %s", error)

    def _on_close(self, ws, close_status_code, close_msg):
        self.logger.info(
            "Bitget WebSocket closed: %s %s", close_status_code, close_msg
        )
        self._stop_ping.set()

    # ── public interface (same as StreamingCryptoPrices) ──────────

    def stream_to_queue(self):
        """
        Connect to the Bitget public WebSocket and start streaming.
        Blocks the calling thread — run in a dedicated thread.
        """
        self.logger.info("Connecting to Bitget WebSocket: %s", self.ws_url)
        self.ws = websocket.WebSocketApp(
            self.ws_url,
            on_open=self._on_open,
            on_message=self._on_message,
            on_error=self._on_error,
            on_close=self._on_close,
        )
        self.ws.run_forever()

    def stop(self):
        """Cleanly shut down the WebSocket and the ping thread."""
        self._stop_ping.set()
        if self.ws:
            self.ws.close()
