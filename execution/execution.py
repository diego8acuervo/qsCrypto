from __future__ import print_function

from abc import ABCMeta, abstractmethod
from decimal import Decimal, ROUND_DOWN
import logging
import math
import os

from dotenv import load_dotenv


class ExecutionHandler(object):
    """
    Provides an abstract base class to handle all execution in the
    backtesting and live trading system.
    """

    __metaclass__ = ABCMeta

    @abstractmethod
    def execute_order(self):
        """
        Send the order to the brokerage.
        """
        raise NotImplementedError("Should implement execute_order()")


class SimulatedExecution(object):
    """
    Provides a simulated execution handling environment. This class
    actually does nothing - it simply receives an order to execute.

    Instead, the Portfolio object actually provides fill handling.
    This will be modified in later versions.
    """
    def execute_order(self, event):
        pass


# ── Binance helpers ───────────────────────────────────────────────

def load_api_credentials(test_mode=True):
    """
    Load Binance API credentials from environment variables.
    Returns (api_key, secret_key).
    """
    load_dotenv()
    if test_mode:
        api_key = os.environ.get("BINANCE_TESTNET_API_KEY")
        secret_key = os.environ.get("BINANCE_TESTNET_SECRET_KEY")
    else:
        api_key = os.environ.get("BINANCE_API_KEY")
        secret_key = os.environ.get("BINANCE_SECRET_KEY")
    if not api_key or not secret_key:
        raise ValueError(
            "Binance API credentials not found. "
            "Set BINANCE_TESTNET_API_KEY / BINANCE_TESTNET_SECRET_KEY "
            "(or production equivalents) in your .env file."
        )
    return api_key, secret_key


def create_execution_handler(test_mode=True, market_type="usdm_futures"):
    """
    Factory that returns a BinancePerpetualExecutionHandler
    pre-configured with the correct credentials and base URL.
    """
    from qsCrypto import settings

    api_key, secret_key = load_api_credentials(test_mode)
    env_label = "testnet" if test_mode else "production"
    base_url = settings.BINANCE_ENVIRONMENTS[market_type]["api"][env_label]
    return BinancePerpetualExecutionHandler(
        api_key=api_key,
        secret_key=secret_key,
        base_url=base_url,
        market_type=market_type,
    )


class BinancePerpetualExecutionHandler(ExecutionHandler):
    """
    Handles order execution against Binance USDⓈ-M or COIN-M
    perpetual futures via the official binance-futures-connector.
    """

    def __init__(self, api_key, secret_key, base_url, market_type="usdm_futures"):
        self.api_key = api_key
        self.secret_key = secret_key
        self.base_url = base_url
        self.market_type = market_type
        self.logger = logging.getLogger(__name__)
        self.client = self._create_client()
        self._exchange_info = None  # lazy-loaded cache

    # ── Client creation ───────────────────────────────────────────

    def _create_client(self):
        if self.market_type == "usdm_futures":
            from binance.um_futures import UMFutures
            return UMFutures(
                key=self.api_key,
                secret=self.secret_key,
                base_url=self.base_url,
            )
        elif self.market_type == "coinm_futures":
            from binance.cm_futures import CMFutures
            return CMFutures(
                key=self.api_key,
                secret=self.secret_key,
                base_url=self.base_url,
            )
        else:
            raise ValueError(f"Unsupported market_type: {self.market_type}")

    # ── Exchange info & symbol filters ────────────────────────────

    def _load_exchange_info(self):
        """Fetch and cache /fapi/v1/exchangeInfo (or /dapi equivalent)."""
        if self._exchange_info is None:
            self.logger.info("Fetching exchange info from %s …", self.base_url)
            self._exchange_info = self.client.exchange_info()
        return self._exchange_info

    def get_symbol_filters(self, symbol):
        """
        Return a dict of filter dicts keyed by filterType for *symbol*.
        E.g. filters["PRICE_FILTER"]["tickSize"] -> '0.01'
        """
        info = self._load_exchange_info()
        for s in info["symbols"]:
            if s["symbol"] == symbol.upper():
                return {f["filterType"]: f for f in s["filters"]}
        raise ValueError(f"Symbol {symbol} not found in exchange info")

    def round_quantity(self, symbol, qty):
        """
        Round *qty* down to the nearest valid step size
        according to the LOT_SIZE filter for *symbol*.
        """
        filters = self.get_symbol_filters(symbol)
        step_size = Decimal(filters["LOT_SIZE"]["stepSize"])
        qty = Decimal(str(qty))
        # number of decimals in stepSize
        if step_size == 0:
            return qty
        precision = int(round(-math.log10(float(step_size))))
        rounded = (qty // step_size) * step_size
        return float(round(rounded, precision))

    def round_price(self, symbol, price):
        """
        Round *price* to the nearest valid tick size
        according to the PRICE_FILTER for *symbol*.
        """
        filters = self.get_symbol_filters(symbol)
        tick_size = Decimal(filters["PRICE_FILTER"]["tickSize"])
        price = Decimal(str(price))
        if tick_size == 0:
            return price
        precision = int(round(-math.log10(float(tick_size))))
        rounded = (price // tick_size) * tick_size
        return float(round(rounded, precision))

    def validate_order(self, symbol, qty, price=None):
        """
        Validate that qty and price respect the exchange filters.
        Returns (rounded_qty, rounded_price | None).
        Raises ValueError on min-notional / min-qty violations.
        """
        filters = self.get_symbol_filters(symbol)

        rounded_qty = self.round_quantity(symbol, qty)

        min_qty = float(filters["LOT_SIZE"]["minQty"])
        max_qty = float(filters["LOT_SIZE"]["maxQty"])
        if rounded_qty < min_qty:
            raise ValueError(
                f"Quantity {rounded_qty} below minimum {min_qty} for {symbol}"
            )
        if rounded_qty > max_qty:
            raise ValueError(
                f"Quantity {rounded_qty} above maximum {max_qty} for {symbol}"
            )

        rounded_price = None
        if price is not None:
            rounded_price = self.round_price(symbol, price)

        # MIN_NOTIONAL check (qty * price >= minNotional)
        if "MIN_NOTIONAL" in filters and rounded_price is not None:
            min_notional = float(filters["MIN_NOTIONAL"].get("notional", 0))
            if rounded_qty * rounded_price < min_notional:
                raise ValueError(
                    f"Notional {rounded_qty * rounded_price:.4f} below "
                    f"minimum {min_notional} for {symbol}"
                )

        return rounded_qty, rounded_price

    # ── Order execution ───────────────────────────────────────────

    def execute_order(self, event):
        """
        Execute an OrderEvent as a market order on Binance Futures.
        Applies proper precision rounding before submission.
        """
        symbol = event.instrument.upper()
        side_map = {"buy": "BUY", "sell": "SELL"}
        side = side_map.get(event.side, event.side.upper())

        rounded_qty = self.round_quantity(symbol, event.units)

        self.logger.info(
            "Executing %s %s | qty=%s (raw %s)",
            side, symbol, rounded_qty, event.units,
        )

        try:
            response = self.client.new_order(
                symbol=symbol,
                side=side,
                type="MARKET",
                quantity=rounded_qty,
            )
            self.logger.info("Order response: %s", response)
            return response
        except Exception as e:
            self.logger.error("Order failed for %s: %s", symbol, e)
            raise

    def execute_market_order(self, symbol, side, quantity):
        """
        Convenience method for placing a standalone market order
        outside the event-driven loop.
        """
        symbol = symbol.upper()
        side = side.upper()
        rounded_qty = self.round_quantity(symbol, quantity)
        self.logger.info(
            "Market order %s %s qty=%s", side, symbol, rounded_qty
        )
        try:
            response = self.client.new_order(
                symbol=symbol,
                side=side,
                type="MARKET",
                quantity=rounded_qty,
            )
            self.logger.info("Order response: %s", response)
            return response
        except Exception as e:
            self.logger.error("Order failed: %s", e)
            raise

    def execute_limit_order(self, symbol, side, quantity, price, time_in_force="GTC"):
        """
        Place a limit order with proper precision rounding.
        """
        symbol = symbol.upper()
        side = side.upper()
        rounded_qty = self.round_quantity(symbol, quantity)
        rounded_price = self.round_price(symbol, price)
        self.logger.info(
            "Limit order %s %s qty=%s @ %s", side, symbol, rounded_qty, rounded_price
        )
        try:
            response = self.client.new_order(
                symbol=symbol,
                side=side,
                type="LIMIT",
                quantity=rounded_qty,
                price=rounded_price,
                timeInForce=time_in_force,
            )
            self.logger.info("Order response: %s", response)
            return response
        except Exception as e:
            self.logger.error("Limit order failed: %s", e)
            raise

    # ── Account helpers ───────────────────────────────────────────

    def get_account_info(self):
        return self.client.account()

    def get_position_info(self, symbol=None):
        if symbol:
            return self.client.get_position_risk(symbol=symbol.upper())
        return self.client.get_position_risk()

    def cancel_all_orders(self, symbol):
        return self.client.cancel_open_orders(symbol=symbol.upper())

    def get_open_orders(self, symbol=None):
        if symbol:
            return self.client.get_open_orders(symbol=symbol.upper())
        return self.client.get_open_orders()
