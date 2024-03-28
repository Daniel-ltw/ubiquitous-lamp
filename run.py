from datetime import datetime, timedelta

from lumibot.strategies import Strategy
from lumibot.entities import Asset, Order
from pandas import DataFrame
import pandas_ta

from credentials import alpaca, polygon_api

ALPACA_CONFIG = alpaca()

IS_LIVE = False

HISTORY_LENGTH = 100



# A simple strategy that buys AAPL on the first day and hold it
class DumpOnHighGrabOnLow(Strategy):

    def initialize(self, assets:tuple[tuple[Asset,Asset]] = None,
                   cash_at_risk:float=0.05):
        if assets is None:
            raise ValueError("You must provide a valid assets pair")
        # for crypto, market is 24/7
        self.set_market("24/7")
        # iteration every 5 minutes
        self.sleeptime = "1H"
        self.assets = assets
        self.cash_at_risk = cash_at_risk
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

        if cash > 0 and side != None and quantity > 0 and (method not in self.orders or len(self.orders[method]) <= self.method_orders_limit):
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
        return self.get_historical_prices(self.asset,length=HISTORY_LENGTH, timestep='60 minutes', quote=self.quote).df

    def _double_bbands_trading(self):
        window = 20
        std_dev = 2.0
        band_a = self.history_df.ta.bbands(length=window, std=std_dev).iloc[-1]
        band_b = self.history_df.ta.bbands(length=window, std=1.0).iloc[-1]
        close = self.history_df.iloc[-1]['close']

        a1 = band_a[f'BBU_{window}_{std_dev}']
        b1 = band_b[f'BBU_{window}_1.0']
        ma = band_a[f'BBM_{window}_{std_dev}']
        b2 = band_b[f'BBL_{window}_1.0']
        a2 = band_a[f'BBL_{window}_{std_dev}']
        # self.log_message(f"Buy range is between {b1} to {a1}")
        # self.log_message(f"MA is {ma}")
        # self.log_message(f"Sell range is between {b2} to {a2}")

        if close > b1 and close <= a1:
            side = Order.OrderSide.BUY
        elif close < b2 and close >= a2:
            side = Order.OrderSide.SELL
        elif close > a1:
            side = Order.OrderSide.SELL
        elif close < a2:
            side = Order.OrderSide.BUY
        else:
            side = None

        self._place_order(side, 'double_bbands')

    def _counter_trend_trading(self):
        band = self.history_df.ta.bbands(length=20, std=2.0).iloc[-1]
        stochrsi = self.history_df.ta.stochrsi().iloc[-1]
        close = self.history_df.iloc[-1]['close']
        rsi = (stochrsi['STOCHRSId_14_14_3_3'] + stochrsi['STOCHRSIk_14_14_3_3']) / 2

        if close > band[f'BBU_20_2.0'] and rsi > 80:
            side = Order.OrderSide.SELL
        elif close < band[f'BBL_20_2.0'] and rsi < 20:
            side = Order.OrderSide.BUY
        else:
            side = None

        self._place_order(side, 'counter_trend')

    def _close_winning_open_orders(self):
        positions = self.get_positions()
        self.log_message(f'Open Positions: {positions}', color='green')

        previous = self.history_df.tail(3)['close'].values
        for position in positions:
            last_price = self.get_last_price(position.asset, self.quote)
            selling_order = position.get_selling_order()
            if len(position.orders) > 0 and selling_order.quantity > 0:
                self.log_message(f'Position possible sale: {position}', color='red')
                if selling_order.side == 'sell' and previous[-1] < previous[-2] and previous[-2] < previous[-3] and any(last_price > order.get_fill_price() for order in position.orders):
                    self.submit_order(selling_order)
                    for key in self.orders:
                        if position.orders[-1] in self.orders[key]:
                            self.orders[key] = []
                elif selling_order.side == 'buy' and previous[-1] > previous[-2] and previous[-2] > previous[-3] and any(last_price < order.get_fill_price() for order in position.orders):
                    self.submit_order(selling_order)
                    for key in self.orders:
                        if position.orders[-1] in self.orders[key]:
                            self.orders[key] = []

    def on_trading_iteration(self):
        for asset in self.assets:
            self.asset = asset
            self.base, self.quote = asset
            history_df = self._get_historical_prices()
            self.history_df = history_df

            self._close_winning_open_orders()
            self._double_bbands_trading()
            self._counter_trend_trading()
        self.log_message(f'Self Orders: {self.orders}', color='green')

    def on_bot_crash(self, error):
        self.log_message(error)

if IS_LIVE:
    from lumibot.brokers import Alpaca
    from lumibot.traders import Trader

    trader = Trader()
    broker = Alpaca(ALPACA_CONFIG)
    strategy = DumpOnHighGrabOnLow(broker=broker)
    trader.add_strategy(strategy)
    trader.run_all()
else:
    from lumibot.backtesting import PolygonDataBacktesting
    from lumibot.entities import TradingFee

    # 0.01% trading/slippage fee
    trading_fee = TradingFee(percent_fee=0.005)

    # Pick the dates that you want to start and end your backtest
    # and the allocated budget
    backtesting_end = datetime.now() - timedelta(days=2)
    backtesting_start = backtesting_end - timedelta(days=700)


    eur = (Asset(symbol='EUR', asset_type='forex'),
            Asset(symbol='USD', asset_type='forex'))

    nzd = (Asset(symbol='NZD', asset_type='forex'),
            Asset(symbol='USD', asset_type='forex'))

    sgd = (Asset(symbol='SGD', asset_type='forex'),
            Asset(symbol='USD', asset_type='forex'))

    aud = (Asset(symbol='AUD', asset_type='forex'),
            Asset(symbol='USD', asset_type='forex'))

    cad = (Asset(symbol='USD', asset_type='forex'),
            Asset(symbol='CAD', asset_type='forex'))

    # Run the backtest
    backtest = DumpOnHighGrabOnLow.backtest(
        PolygonDataBacktesting,
        backtesting_start,
        backtesting_end,
        budget=100.0,
        polygon_api_key=polygon_api(),
        polygon_has_paid_subscription=False,
        parameters={ "assets": [eur, nzd, sgd, aud] },
        buy_trading_fees=[trading_fee],
        sell_trading_fees=[trading_fee],
    )
