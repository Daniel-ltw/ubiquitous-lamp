from datetime import datetime, timedelta

from lumibot.strategies import Strategy
from lumibot.entities import Asset, Order, TradingFee
from pandas import DataFrame
import pandas_ta

from credentials import alpaca, polygon_api

ALPACA_CONFIG = alpaca()

IS_LIVE = False
HISTORY_LENGTH = 50
BBAND_WINDOW = 20
BBAND_STDDEV = 2.3

# 0.01% trading/slippage fee
trading_fee = TradingFee(percent_fee=0.005)



eur = (Asset(symbol='EUR', asset_type='forex'),
        Asset(symbol='USD', asset_type='forex'),
        0.2)

nzd = (Asset(symbol='NZD', asset_type='forex'),
        Asset(symbol='USD', asset_type='forex'),
        0.08)

sgd = (Asset(symbol='SGD', asset_type='forex'),
        Asset(symbol='USD', asset_type='forex'),
        0.08)

aud = (Asset(symbol='AUD', asset_type='forex'),
        Asset(symbol='USD', asset_type='forex'),
        0.15)



# A simple strategy that buys AAPL on the first day and hold it
class DumpOnHighGrabOnLow(Strategy):

    def initialize(self, assets:tuple[tuple[Asset,Asset,float]] = None):
        if assets is None:
            raise ValueError("You must provide a valid assets pair")
        # for crypto, market is 24/7
        self.set_market("24/7")
        # iteration every 5 minutes
        self.sleeptime = "1D"
        self.assets = assets
        self.orders = {}
        self.method_orders_limit = 1

    def _position_sizing(self):
        cash = self.get_cash()
        last_price = self.get_last_price(self.asset, self.quote)
        quantity = round(cash * self.cash_at_risk / last_price, 2)
        return cash, last_price, quantity

    def _place_order(self, side, method):
        cash, last_price, quantity = self._position_sizing()
        method = f"{method}_{self.base}_{self.quote}"
        buy_sell_from_history = self._place_order_direction_correct(side, last_price)

        if cash > 0 and side != None and quantity > 0 and buy_sell_from_history and (method not in self.orders or len(self.orders[method]) <= self.method_orders_limit):
            order = self.create_order(asset=self.base,
                                    quantity=quantity,
                                    side=side,
                                    quote=self.quote)
            self.submit_order(order)
            self.log_message(f"Last {side} trade was at {self.get_datetime()}")
            if method not in self.orders:
                self.orders[method] = []
            self.orders[method].append(order)


    def _get_historical_prices(self):
        return self.get_historical_prices(self.asset,length=HISTORY_LENGTH, timestep='1 day', quote=self.quote).df

    def _double_bbands_trading(self):
        window = BBAND_WINDOW
        std_dev = BBAND_STDDEV
        band_a = self.history_df.ta.bbands(length=window, std=std_dev).iloc[-1]
        band_b = self.history_df.ta.bbands(length=window, std=1.0).iloc[-1]

        a1 = band_a[f'BBU_{window}_{std_dev}']
        b1 = band_b[f'BBU_{window}_1.0']
        ma = band_b[f'BBM_{window}_1.0']
        b2 = band_b[f'BBL_{window}_1.0']
        a2 = band_a[f'BBL_{window}_{std_dev}']
        # self.log_message(f"Buy range is between {b1} to {a1}")
        # self.log_message(f"MA is {ma}")
        # self.log_message(f"Sell range is between {b2} to {a2}")

        if self.last_price > b1 and self.last_price <= a1:
            self.ta['double_bbands_side'] = Order.OrderSide.BUY
        elif self.last_price < b2 and self.last_price >= a2:
            self.ta['double_bbands_side'] = Order.OrderSide.SELL
        elif self.last_price > a1:
            self.ta['double_bbands_side'] = Order.OrderSide.SELL
        elif self.last_price < a2:
            self.ta['double_bbands_side'] = Order.OrderSide.BUY
        else:
            self.ta['double_bbands_side'] = None

    def _counter_trend_trading(self):
        band = self.history_df.ta.bbands(length=BBAND_WINDOW, std=BBAND_STDDEV).iloc[-1]
        stochrsi = self.history_df.ta.stochrsi().iloc[-1]
        rsi = (stochrsi['STOCHRSId_14_14_3_3'] + stochrsi['STOCHRSIk_14_14_3_3']) / 2

        if self.last_price > band[f'BBU_{BBAND_WINDOW}_{BBAND_STDDEV}'] and rsi >= 70:
            self.ta['counter_trend_side'] = Order.OrderSide.SELL
        elif self.last_price < band[f'BBL_{BBAND_WINDOW}_{BBAND_STDDEV}'] and rsi <= 30:
            self.ta['counter_trend_side'] = Order.OrderSide.BUY
        else:
            self.ta['counter_trend_side'] = None

    def _close_winning_open_orders(self):
        positions = self.get_positions()
        self.log_message(f'Open Positions: {positions}', color='yellow')

        for position in positions:
            if position.asset != self.quote:
                continue
            last_price = self.get_last_price(position.asset, self.quote)
            selling_order = position.get_selling_order()
            if len(position.orders) > 0 and selling_order.quantity > 0:
                self.log_message(f'Position possible sale: {position}', color='red')
                sma = self.history_df.ta.sma(length=BBAND_WINDOW).iloc[-1]
                if sma < self.last_price and all(last_price > order.get_fill_price() for order in position.orders):
                    self.submit_order(selling_order)
                    for key in self.orders:
                        if position.orders[-1] in self.orders[key]:
                            self.orders[key] = []
                elif sma > self.last_price and all(last_price < order.get_fill_price() for order in position.orders):
                    self.submit_order(selling_order)
                    for key in self.orders:
                        if position.orders[-1] in self.orders[key]:
                            self.orders[key] = []

    def _place_order_direction_correct(self, side, last_price):
        if side == Order.OrderSide.SELL and last_price > self.previous[-2]:
            return True
        elif side == Order.OrderSide.BUY and last_price < self.previous[-2]:
            return True
        else:
            return False


    def on_trading_iteration(self):
        for asset in self.assets:
            self.ta = {}
            self.base, self.quote, self.cash_at_risk = asset
            self.asset = (self.base, self.quote)
            self.history_df = self._get_historical_prices()
            self.last_price = self.get_last_price(self.base, self.quote)
            self.previous = self.history_df.tail(3)['close'].values

            if not self.first_iteration:
                self._close_winning_open_orders()
            self._double_bbands_trading()
            self._counter_trend_trading()

            if self.ta['counter_trend_side'] == self.ta['double_bbands_side']:
                self._place_order(self.ta['counter_trend_side'], 'combine')
        self.log_message(f'Self Orders: {self.orders}', color='yellow')

    def on_bot_crash(self, error):
        self.log_message(error)

if IS_LIVE:
    from lumibot.brokers import Alpaca
    from lumibot.traders import Trader

    trader = Trader()
    broker = Alpaca(ALPACA_CONFIG)
    strategy = DumpOnHighGrabOnLow(
        broker=broker,
        budget=100.0,
        parameters={ "assets": [eur, nzd, sgd, aud] },
        buy_trading_fees=[trading_fee],
        sell_trading_fees=[trading_fee],
    )
    trader.add_strategy(strategy)
    trader.run_all()
else:
    from lumibot.backtesting import PolygonDataBacktesting

    # Pick the dates that you want to start and end your backtest
    # and the allocated budget
    backtesting_end = datetime.now() - timedelta(days=2)
    backtesting_start = backtesting_end - timedelta(days=630)

    # Run the backtest
    # for asset in :
    DumpOnHighGrabOnLow.backtest(
        PolygonDataBacktesting,
        backtesting_start,
        backtesting_end,
        budget=100.0,
        polygon_api_key=polygon_api(),
        polygon_has_paid_subscription=False,
        # benchmark_asset=asset[0],
        # quote_asset=asset[1],
        parameters={ "assets": [eur, nzd, sgd, aud] },
        buy_trading_fees=[trading_fee],
        sell_trading_fees=[trading_fee],
    )
