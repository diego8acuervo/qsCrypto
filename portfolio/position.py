from decimal import Decimal, getcontext, ROUND_HALF_DOWN


class Position(object):
    def __init__(
        self, home_currency, position_type, 
        currency_pair, units, ticker
    ):
        self.home_currency = home_currency  # Account denomination (e.g. USDT)
        self.position_type = position_type  # Long or short
        self.currency_pair = currency_pair  # Intended traded currency pair (e.g. BTCUSDT)
        self.units = units
        self.ticker = ticker
        self.set_up_currencies()
        self.profit_base = self.calculate_profit_base()
        self.profit_perc = self.calculate_profit_perc()

    def set_up_currencies(self):
        self.base_currency = self.currency_pair[:3]    # For BTC/USDT, this is BTC
        self.quote_currency = self.currency_pair[3:]   # For BTC/USDT, this is USDT
        # For crypto pairs where quote == home (e.g. BTCUSDT with USDT account),
        # no conversion is needed.
        self.quote_home_currency_pair = "%s%s" % (self.quote_currency, self.home_currency)

        ticker_cur = self.ticker.prices[self.currency_pair]
        if self.position_type == "long":
            self.avg_price = Decimal(str(ticker_cur["ask"]))
            self.cur_price = Decimal(str(ticker_cur["bid"]))    
        else:
            self.avg_price = Decimal(str(ticker_cur["bid"]))
            self.cur_price = Decimal(str(ticker_cur["ask"]))

    def calculate_pips(self):
        mult = Decimal("1")
        if self.position_type == "long":
            mult = Decimal("1")
        elif self.position_type == "short":
            mult = Decimal("-1")
        pips = (mult * (self.cur_price - self.avg_price)).quantize(
            Decimal("0.00001"), ROUND_HALF_DOWN
        )
        return pips

    def calculate_profit_base(self):
        pips = self.calculate_pips()
        # If quote currency == home currency (e.g. BTCUSDT with USDT account),
        # no conversion is needed — qh_close is simply 1.0
        if self.quote_currency == self.home_currency:
            qh_close = Decimal("1.0")
        else:
            ticker_qh = self.ticker.prices[self.quote_home_currency_pair]
            if self.position_type == "long":
                qh_close = ticker_qh["bid"]
            else:
                qh_close = ticker_qh["ask"]
        profit = pips * qh_close * self.units
        return profit.quantize(
            Decimal("0.00001"), ROUND_HALF_DOWN
        )   

    def calculate_profit_perc(self):
        if self.units == 0:
            return Decimal("0.00000")
        return (self.profit_base / self.units * Decimal("100.00")).quantize(
            Decimal("0.00001"), ROUND_HALF_DOWN
        )

    def update_position_price(self):
        ticker_cur = self.ticker.prices[self.currency_pair]
        if self.position_type == "long":
            self.cur_price = Decimal(str(ticker_cur["bid"]))
        else:
            self.cur_price = Decimal(str(ticker_cur["ask"]))
        self.profit_base = self.calculate_profit_base()
        self.profit_perc = self.calculate_profit_perc()

    def add_units(self, units):
        cp = self.ticker.prices[self.currency_pair]
        if self.position_type == "long":
            add_price = cp["ask"]
        else:
            add_price = cp["bid"]
        new_total_units = self.units + units
        new_total_cost = self.avg_price*self.units + add_price*units
        self.avg_price = new_total_cost/new_total_units
        self.units = new_total_units
        self.update_position_price()

    def remove_units(self, units):
        dec_units = Decimal(str(units))
        ticker_cp = self.ticker.prices[self.currency_pair]
        # If quote currency == home currency, no conversion needed
        if self.quote_currency == self.home_currency:
            qh_close = Decimal("1.0")
        else:
            ticker_qh = self.ticker.prices[self.quote_home_currency_pair]
            if self.position_type == "long":
                qh_close = ticker_qh["ask"]
            else:
                qh_close = ticker_qh["bid"]
        if self.position_type == "long":
            remove_price = ticker_cp["bid"]
        else:
            remove_price = ticker_cp["ask"]
        self.units -= dec_units
        self.update_position_price()
        # Calculate PnL
        pnl = self.calculate_pips() * qh_close * dec_units
        getcontext().rounding = ROUND_HALF_DOWN
        return pnl.quantize(Decimal("0.01"))

    def close_position(self):
        ticker_cp = self.ticker.prices[self.currency_pair]
        # If quote currency == home currency, no conversion needed
        if self.quote_currency == self.home_currency:
            qh_close = Decimal("1.0")
        else:
            ticker_qh = self.ticker.prices[self.quote_home_currency_pair]
            if self.position_type == "long":
                qh_close = ticker_qh["ask"]
            else:
                qh_close = ticker_qh["bid"]
        self.update_position_price()
        # Calculate PnL
        pnl = self.calculate_pips() * qh_close * self.units
        getcontext().rounding = ROUND_HALF_DOWN
        return pnl.quantize(Decimal("0.01"))
