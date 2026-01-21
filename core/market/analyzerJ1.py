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

from core.backtesting.backtesting import Backtesting
from core.candles.candles_period import CandlesPeriod
from core.candles.candlestick import Candlestick
from core.market.technical_indicators import TechnicalIndicators
from core.candles.binance_client import BinanceClient
from core.order.binance_order import BinanceOrder
from core.order.moby_order import MobyOrder, OrderPosition
from core.order.order_simulator import OrderSimulator
import numpy as np


class AnalyzerJ1:
    """Market Analyzer - This class looks for signals in the market"""

    def __init__(self):
        """Default constructor"""

        self.__binance_client = BinanceClient()
        self.__binance_order = BinanceOrder()
        self.__technical_indicators = TechnicalIndicators()

        self.__simulator_entries = False
        self.__real_entries = False

        # All available coins: [key for key in self.__binance_client.get_all_available_symbols()]
        self.coins_to_analyze = [key for key in self.__binance_client.get_all_available_symbols()] #["IOTAUSDT", "XRPUSDT", "ETHUSDT", "ADAUSDT", "LUNAUSDT"]
        self.interval = "1m"
        self.num_candles_to_iterate = 301
        self.candle_index_to_start_backtest = self.num_candles_to_iterate
        self.order_label = "[AnalyzerJ1][v0.0.1]"

        # Variables del algoritmo
        self.__config = dict({
            "stop_loss_margin": 0.0,
            "trailing_stop": 0.2,
            "trailing_stop_activation": 0.1,
            "take_profit_percent": 4  # 5%
        })

    def prepare_candles(self, candles: List[Candlestick]):
        """Añade los indicadores tecnicos necesarios a cada vela"""
        self.__technical_indicators.generate_average_true_range(candles, 40)

    def analyze(self, candles: List[Candlestick]) -> MobyOrder:
        if len(candles) % 2 != 0:
            return self.backtrack_coin(candles[0].ticker, candles)
        else:
            return self.backtrack_coin(candles[0].ticker, candles[0:len(candles)-2])

    def get_real_shadow_percentile(self, candles: List[Candlestick], percentile):
        arr = []
        for candle in candles:
            arr.append(candle.shadow)

        np_arr = np.array(arr)
        return np.percentile(np_arr, percentile)

    def get_volume_percentile(self, candles: List[Candlestick], percentile):
        arr = []
        for candle in candles:
            arr.append(candle.volume)

        np_arr = np.array(arr)
        return np.percentile(np_arr, percentile)

    def search_high_volume(self, complete_candles: List[Candlestick], color):

        volume_range = range(1, 20)
        volume_candle = None
        volume_candle_index = None
        shadow_candle = None
        shadow_candle_index = None
        volume_percentile_value = self.get_volume_percentile(complete_candles[-300:], 97)
        shadow_percentile_value = self.get_real_shadow_percentile(complete_candles[-300:], 92)

        shadow_avg = sum(candle.shadow for candle in complete_candles) / len(complete_candles)
        volume_avg = sum(candle.volume for candle in complete_candles) / len(complete_candles)

        for i in volume_range:
            complete_candle = complete_candles[-i]

            #Volume
            if complete_candle.volume > volume_percentile_value and complete_candle.color == color \
                    and complete_candle.volume > complete_candles[-i-1].volume * 2\
                    and complete_candle.volume > volume_avg * 2:
                if volume_candle is None or complete_candle.volume > volume_candle.volume:
                    volume_candle = complete_candle
                    volume_candle_index = -i

            #Shadow
            if complete_candle.shadow >= shadow_percentile_value and complete_candle.color == color \
                    and complete_candle.shadow > complete_candles[-i-1].shadow * 2\
                    and complete_candle.shadow > shadow_avg * 2:
                if shadow_candle is None or complete_candle.shadow > shadow_candle.shadow:
                    shadow_candle = complete_candle
                    shadow_candle_index = -i

        if volume_candle is not None and shadow_candle is not None:
            return {
                "volume_candle": volume_candle,
                "volume_candle_index": volume_candle_index,
                "shadow_candle": shadow_candle,
                "shadow_candle_index": shadow_candle_index
            }
        return None

    def determine_high_volume(self, complete_candles):
        high_volume_info_green = self.search_high_volume(complete_candles, color="GREEN")
        high_volume_info_red = self.search_high_volume(complete_candles, color="RED")

        if high_volume_info_red is None and high_volume_info_green is None:
            return None

        if high_volume_info_green is None and high_volume_info_red is not None:
            high_volume_info_red["color"] = "RED"
            return high_volume_info_red

        if high_volume_info_red is None and high_volume_info_green is not None:
            high_volume_info_green["color"] = "GREEN"
            return high_volume_info_green

        if high_volume_info_red["volume_candle"].volume > high_volume_info_green["volume_candle"].volume:
            high_volume_info_red["color"] = "RED"
            return high_volume_info_red
        else:
            high_volume_info_green["color"] = "GREEN"
            return high_volume_info_green

    def backtrack_coin(self, coin, candles: List[Candlestick]):

        current_candle = candles[-1]
        atr = candles[-2].technical_indicators.atr
        complete_candles: List[Candlestick] = candles[:-1]
        high_volume_info = self.determine_high_volume(complete_candles)
        moby_order = None

        ###### Green Volume ######
        if high_volume_info is not None and high_volume_info["volume_candle_index"] >= -4 and high_volume_info["color"] == "GREEN":

            pre_volume_max_price = max([candle.high_price for candle in complete_candles[high_volume_info["volume_candle_index"] - 4:high_volume_info["volume_candle_index"]]])

            if pre_volume_max_price > high_volume_info["volume_candle"].high_price:
                # print("No ha habido subida, no cuenta...")
                return None

            if current_candle.color != "RED" and complete_candles[-1].color != "RED":
                # print("Sigue subiendo")
                return None

            if high_volume_info["volume_candle_index"] < -1:
                post_volume_min_price = min([candle.low_price for candle in complete_candles[high_volume_info["volume_candle_index"]+1:]])
            else:
                post_volume_min_price = current_candle.low_price

            max_price = max([candle.high_price for candle in complete_candles[high_volume_info["volume_candle_index"]:]])
            stoploss = max(max_price, current_candle.high_price)
            take_profit = current_candle.close_price - 7 * atr #high_volume_info["volume_candle"].real_body

            if current_candle.low_price > high_volume_info["volume_candle"].low_price and \
               current_candle.low_price > take_profit and \
               take_profit < post_volume_min_price:

                moby_order = MobyOrder(
                    ticker=coin,
                    order_price=current_candle.close_price,
                    quantity=5,
                    position=OrderPosition.Short,
                    order_label=self.order_label,
                    leverage=15,
                    trailing_stop_activation_price=take_profit,
                    trailing_stop=0.2,
                    stop_loss=stoploss + 7 * atr
                )
                return moby_order


        ###### Red Volume ######
        if 1 == 0 and high_volume_info is not None and high_volume_info["volume_candle_index"] >= -5 and high_volume_info["color"] == "RED":

            pre_volume_min_price = min([candle.low_price for candle in complete_candles[
                                                                       high_volume_info["volume_candle_index"] - 4:
                                                                       high_volume_info["volume_candle_index"]]])

            if pre_volume_min_price < high_volume_info["volume_candle"].low_price:
                # print("No ha habido bajada, no cuenta...")
                return None

            if current_candle.color != "GREEN" and complete_candles[-1].color != "GREEN":
                return None

            if high_volume_info["volume_candle_index"] < -1:
                post_volume_max_price = max([candle.high_price for candle in complete_candles[high_volume_info["volume_candle_index"]+1:]])
            else:
                post_volume_max_price = current_candle.high_price

            min_price = min([candle.low_price for candle in complete_candles[high_volume_info["volume_candle_index"]:]])
            stoploss = min(min_price, current_candle.low_price)
            take_profit = high_volume_info["volume_candle"].open_price

            # Determinar si vamos tarde
            if current_candle.high_price < high_volume_info["volume_candle"].high_price and \
               current_candle.high_price < take_profit and \
               take_profit > post_volume_max_price:

                moby_order = MobyOrder(
                    ticker=coin,
                    order_price=current_candle.close_price,
                    quantity=5,
                    position=OrderPosition.Long,
                    order_label=self.order_label,
                    trailing_stop_activation_price=take_profit,
                    leverage=15,
                    trailing_stop=0.2,
                    stop_loss=stoploss - 2 * atr
                )

        if moby_order is None:
            return None

        return moby_order


# Local Testing
if __name__ == "__main__":
    backtest_analyzer = AnalyzerJ1()
    backtest_start_time = datetime(2021, 1, 1)

    start = datetime.utcnow()
    Backtesting().backtest(backtest_analyzer, backtest_start_time)
    end = datetime.utcnow()

    time_elapsed = (end - start).total_seconds()
    print()
    print("Finished in", time_elapsed, "seconds.")
