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

from core.candles.candlestick import Candlestick
from core.market.technical_indicators import TechnicalIndicators
from core.candles.binance_client import BinanceClient
from core.order.binance_order import BinanceOrder
from core.order.moby_order import MobyOrder, OrderPosition
from core.order.order_simulator import OrderSimulator


class AnalyzerModelo:
    """Market Analyzer - This class looks for signals in the market"""

    def __init__(self):
        """Default constructor"""

        self.__binance_client = BinanceClient()
        self.__order_simulator = None  # Init if needed
        self.__binance_order = None  # Init if needed
        self.__technical_indicators = TechnicalIndicators()

        self.__simulator_entries = False
        self.__real_entries = False

        # All available coins: [key for key in self.__binance_client.get_all_available_symbols()]
        self.coins_to_analyze = ["IOTAUSDT", "XRPUSDT"]
        self.interval = "1m"
        self.num_candles_to_iterate = 30
        self.candle_index_to_start_backtest = self.num_candles_to_iterate
        self.order_label = "[AnalyzerModelo][v0.0.1]"

    def analyze_all(self):
        """Analiza todas las monedas para buscar entradas"""

        for coin in self.coins_to_analyze.copy():
            print("Processing " + coin)
            try:
                candles = self.__binance_client.get_last_candlesticks(
                    coin=coin,
                    num_candlesticks=self.num_candles_to_iterate,
                    interval=self.interval,
                    futures_info=True
                )
                self.prepare_candles(candles)

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
        self.__technical_indicators.generate_average_true_range(candles)


    def analyze(self, candles: List[Candlestick]) -> MobyOrder:
        """Analiza una moneda para buscar entradas"""

        moby_order = None

        current_candle = candles[-1]
        previous_candle = candles[-2]
        previous_previous_candle = candles[-3]

        if previous_candle.volume > 4 * previous_previous_candle.volume:

            atr = candles[-2].technical_indicators.atr

            if previous_candle.color == "RED":
                moby_order = MobyOrder(
                    ticker=current_candle.ticker,
                    order_price=current_candle.close_price,
                    quantity=5,
                    position=OrderPosition.Long,
                    order_label=self.order_label,
                    leverage=15,
                    stop_loss=current_candle.close_price - 4*atr,
                    take_profit_price=current_candle.close_price + 4*atr
                )

            elif previous_candle.color == "GREEN":
                moby_order = MobyOrder(
                    ticker=current_candle.ticker,
                    order_price=current_candle.close_price,
                    quantity=5,
                    position=OrderPosition.Short,
                    order_label=self.order_label,
                    leverage=15,
                    stop_loss=current_candle.close_price + 4*atr,
                    take_profit_price=current_candle.close_price - 4*atr
                )

        return moby_order


# Local Testing
if __name__ == "__main__":
    from core.backtesting.backtesting import Backtesting

    backtest_analyzer = AnalyzerModelo()
    backtest_start_time = datetime(2021, 1, 1)

    start = datetime.utcnow()
    Backtesting().backtest(backtest_analyzer, backtest_start_time)
    end = datetime.utcnow()

    time_elapsed = (end - start).total_seconds()
    print("Finished in", time_elapsed, "seconds.")
