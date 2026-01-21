# Copyright 2021 TradersOfTheUniverse S.A. All Rights Reserved.
#
# [Market Analyzer - This class looks for signals in the market]
#
# Authors:
#   antoniojose.luqueocana@telefonica.com
#   joseluis.roblesurquiza@telefonica.com
#   franciscojavier.gonzalezfernandez1@telefonica.com
#
# Version: 0.1
#
import logging

from datetime import datetime
from typing import List

from core.candles.binance_client import BinanceClient
from core.candles.candlestick import Candlestick
from core.market.technical_indicators import TechnicalIndicators
from core.order.binance_order import BinanceOrder
from core.order.moby_order import MobyOrder, OrderPosition
from core.order.order_simulator import OrderSimulator


class Analyzer5:
    """Market Analyzer - This class looks for signals in the market"""

    def __init__(self, real_orders=False):
        """Default constructor"""
        if real_orders:
            self.__binance_client = BinanceClient(account="binance")
            self.__binance_order = BinanceOrder(account="binance")
        else:
            self.__binance_client = BinanceClient()
            self.__binance_order = None

        self.__order_simulator = None  # Init if needed

        self.__technical_indicators = TechnicalIndicators()

        self.__simulator_entries = False
        self.__real_entries = True

        self.__donchian_days = 20
        self.__1m_candles_in_a_day = 1440

        self.coins_to_analyze = [key for key in self.__binance_client.get_all_available_symbols()]
        self.interval = "1m"
        self.num_candles_to_iterate = 31
        self.candle_index_to_start_backtest = self.__donchian_days * self.__1m_candles_in_a_day + 20
        self.order_label = "[Maximos][v0.0.14]"

        self.stoploss_percentage = 1.5  # 1.5%
        self.trailing_stop_percentage = 0.2  # 0.2%
        self.trailing_stop_activation_percentage = 0.5  # 0.5%

    def analyze_all(self):
        """Analiza todas las monedas para buscar entradas, basándonos en ruptura de maximos"""

        for coin in self.coins_to_analyze.copy():
            print("Processing " + coin)
            try:
                candles = self.__binance_client.get_last_candlesticks(
                    coin=coin,
                    num_candlesticks=self.__1m_candles_in_a_day,
                    interval=self.interval,
                    futures_info=True
                )
                # Aqui no aplica pues en real calculamos maximos y minimos con velas diarias, no con donchian
                # self.prepare_candles(candles)

                moby_order = self.analyze(candles)

                if moby_order is not None:
                    logging.warning("Entrada " + moby_order.order_label + ": " + coin)
                    if self.__real_entries:
                        self.__binance_order.open_position(moby_order)
                    if self.__simulator_entries:
                        self.__order_simulator.open_position_simulation(moby_order)

            except Exception as e:
                msg = "Error procesando " + coin + ". " + repr(e)
                if "Invalid symbol" in msg:
                    msg += ". Quitamos esta moneda del listado"
                    self.coins_to_analyze.remove(coin)
                logging.exception(msg)

    def prepare_candles(self, candles: List[Candlestick]):
        """Añade los indicadores tecnicos necesarios a cada vela"""
        self.__technical_indicators.generate_donchian_channel(candles, self.__1m_candles_in_a_day * self.__donchian_days)

    def analyze(self, candles: List[Candlestick]) -> MobyOrder:
        """Analiza una moneda para buscar entradas, basandonos en ruptura de maximos"""

        moby_order = None
        maximum_price = None
        minimum_price = None
        current_candle = candles[-1]
        previous_candle = candles[-2]

        # Backtesting: many candles of 5m, with donchian channel
        if current_candle.technical_indicators is not None \
                and current_candle.technical_indicators.donchian_high_band is not None:
            maximum_price = candles[-3].technical_indicators.donchian_high_band
            minimum_price = candles[-3].technical_indicators.donchian_low_band

            # Salimos antes de seguir haciendo cosas, para eficientar el backtesting
            if not (previous_candle.high_price > maximum_price or previous_candle.low_price < minimum_price):
                return moby_order

        # 5 minutes volumes
        current_volume = sum(candle.volume for candle in candles[-6:-1])
        previous_volumes = [
            sum(candle.volume for candle in candles[-11:-6]),
            sum(candle.volume for candle in candles[-16:-11]),
            sum(candle.volume for candle in candles[-21:-16]),
            sum(candle.volume for candle in candles[-26:-21]),
            sum(candle.volume for candle in candles[-31:-26])
        ]

        if current_volume > max(previous_volumes):

            current_price = current_candle.close_price

            # Real time: some candles of 1d and some of 1m
            if maximum_price is None:
                day_candles = self.__binance_client.get_last_candlesticks(coin=candles[0].ticker,
                                                                          num_candlesticks=self.__donchian_days,
                                                                          interval="1d", futures_info=True)
                maximum_price = max([candle.high_price for candle in day_candles[:-1] + candles[:-2]])
                minimum_price = min([candle.low_price for candle in day_candles[:-1] + candles[:-2]])
                current_price = day_candles[-1].close_price

            if previous_candle.high_price > maximum_price:
                moby_order = MobyOrder(
                    ticker=current_candle.ticker,
                    order_price=current_price,
                    quantity=1,
                    position=OrderPosition.Long,
                    order_label=self.order_label,
                    leverage=15,
                    stop_loss=current_price * (100 - self.stoploss_percentage) / 100,
                    trailing_stop=self.trailing_stop_percentage,
                    trailing_stop_activation_percent=self.trailing_stop_activation_percentage,
                )
            elif previous_candle.low_price < minimum_price:
                moby_order = MobyOrder(
                    ticker=current_candle.ticker,
                    order_price=current_price,
                    quantity=1,
                    position=OrderPosition.Short,
                    order_label=self.order_label,
                    leverage=15,
                    stop_loss=current_price * (100 + self.stoploss_percentage) / 100,
                    trailing_stop=self.trailing_stop_percentage,
                    trailing_stop_activation_percent=self.trailing_stop_activation_percentage,
                )

        return moby_order


# Local Testing
if __name__ == "__main__":
    from core.backtesting.backtesting import Backtesting

    backtest_start_time = datetime(2021, 1, 1)

    compare_one_parameter = [1, 2]
    compare_two_parameter = [1, 2]

    start = datetime.utcnow()

    if compare_one_parameter:
        backtest_analyzers = list()
        initial_label = Analyzer5().order_label
        for value1_to_try in compare_one_parameter:
            for value2_to_try in (compare_two_parameter if compare_two_parameter else [None]):
                backtest_analyzer = Analyzer5()
                backtest_analyzer.stoploss_percentage = value1_to_try
                backtest_analyzer.order_label += ("[Stoploss{0}]".format(value1_to_try))

                if compare_two_parameter:
                    backtest_analyzer.trailing_stop_activation_percentage = value2_to_try
                    backtest_analyzer.order_label += "[TrailingActivation{0}]".format(value2_to_try)

                backtest_analyzers.append(backtest_analyzer)

        Backtesting.compare_backtests(backtest_analyzers, backtest_start_time, processes=4)

    else:
        Backtesting().backtest(Analyzer5(), backtest_start_time, processes=4)

    end = datetime.utcnow()
    time_elapsed = (end - start).total_seconds()
    print("Finished in", time_elapsed, "seconds.")
