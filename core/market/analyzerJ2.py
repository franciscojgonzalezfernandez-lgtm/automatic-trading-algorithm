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
from copy import deepcopy

from datetime import datetime
from typing import List

from core.backtesting.backtesting import Backtesting
from core.candles.candlestick import Candlestick
from core.market.technical_indicators import TechnicalIndicators
from core.candles.binance_client import BinanceClient
from core.order.binance_order import BinanceOrder
from core.order.moby_order import MobyOrder, OrderPosition
import numpy as np


class AnalyzerJ2:
    """Market Analyzer - This class looks for signals in the market"""

    def __init__(self, real_orders=False):
        """Default constructor"""
        self.coins_volume_rank = dict()

        if real_orders:
            self.__binance_client = BinanceClient(account="binance2")
            self.__binance_order = BinanceOrder(account="binance2")
        else:
            self.__binance_client = BinanceClient()
            self.__binance_order = None

        self.__technical_indicators = TechnicalIndicators()
        # All available coins: [key for key in self.__binance_client.get_all_available_symbols()]
        self.coins_to_analyze = [key for key in self.__binance_client.get_all_available_symbols()]
        self.interval = "5m"
        self.num_candles_to_iterate = 501
        self.candle_index_to_start_backtest = self.num_candles_to_iterate
        self.order_label = "J2-1"

    def prepare_candles(self, candles: List[Candlestick]):
        """Añade los indicadores tecnicos necesarios a cada vela"""
        self.__technical_indicators.generate_average_true_range(candles, 25)
        #self.__technical_indicators.generate_donchian_channel(candles, 25)

    def analyze(self, candles: List[Candlestick], previous_moby_order: MobyOrder, external_index_candles: List[Candlestick]) -> MobyOrder:
        return self.red_volume_order(candles, previous_moby_order, external_index_candles)

    def get_percentile(self, percentile: float, metric: str, candles: List[Candlestick]):
        np_arr = np.array([candle.__getattribute__(metric) for candle in candles])
        return np.percentile(np_arr, percentile)

    def search_outliers(self, metric: str, percentile: float, look_back_window: int, candles: List[Candlestick],
                        greater_than: bool = True):
        """Determina si la métrica 'metric' es un outliers para el percentil dado observando hasta
        'look_back_window' velas hacia atrás para encontrar la vela más anómala"""

        look_back_window_range = range(1, look_back_window)
        metric_candle = None
        metric_candle_index = None
        metric_percentile_value = self.get_percentile(percentile, metric, candles)

        for i in look_back_window_range:
            current_candle = candles[-i]

            if greater_than and current_candle.__getattribute__(metric) > metric_percentile_value:
                if metric_candle is None or current_candle.__getattribute__(metric) > metric_candle.__getattribute__(metric):
                    metric_candle = current_candle
                    metric_candle_index = -i

            if not greater_than and current_candle.__getattribute__(metric) < metric_percentile_value:
                if metric_candle is None or current_candle.__getattribute__(metric) < metric_candle.__getattribute__(metric):
                    metric_candle = current_candle
                    metric_candle_index = -i

        if metric_candle is not None:
            return {
                "candle": metric_candle,
                "candle_index": metric_candle_index,
                "candle_acc": self.cummulate_candles(candles[metric_candle_index:])
            }
        return None

    def cummulate_candles(self, candles: List[Candlestick]) -> Candlestick:
        candle_acc = deepcopy(candles[0])
        candle_acc.accumulate_all(candles[1:])
        return candle_acc

    def red_volume_order(self, candles: List[Candlestick], previous_moby_order: MobyOrder, external_index_candles: List[Candlestick]):

        look_back_window = 20

        #0.- Check BTC Status
        btc_acc = self.cummulate_candles(external_index_candles[-100:])
        if btc_acc.price_inc_percent < -6:
            logging.warning("BTC-INDICATOR --> OUT")
            return None


        #1.- High Volume
        high_volume_info = self.search_outliers("quote_asset_volume", 91, look_back_window, candles[-500:])
        if high_volume_info is None:
            return None

        high_volume_candle: Candlestick = high_volume_info["candle"]
        high_volume_candle_index: int = high_volume_info["candle_index"]
        high_volume_candle_acc: Candlestick = high_volume_info["candle_acc"]

        # 1.- High Volume
        if high_volume_candle.color == high_volume_candle_acc.color == "RED": # and high_volume_candle_index > -5:

            #2.- Pre volume situation
            pre_volume_candles_acc = self.cummulate_candles(candles[high_volume_candle_index - 100:high_volume_candle_index])
            if pre_volume_candles_acc.price_inc_percent > 1:
                return None

            #3.- Low Price Situation
            low_price_info = self.search_outliers("low_price", 1, look_back_window, candles[-250:], greater_than=False)
            if low_price_info is None:
                return None

            low_price_index = low_price_info["candle_index"]
            if low_price_index == high_volume_candle_index:
                price_info = high_volume_candle
            else:
                price_info = self.cummulate_candles(candles[min(low_price_index, high_volume_candle_index):max(low_price_index, high_volume_candle_index)])

            #4. Determinar si ya tocó 'fondo'
            if price_info.price_inc >= candles[-look_back_window-1].technical_indicators.atr * -2:
                return None

            #5. Comienza ya la subida
            post_low_price_info_acc = self.cummulate_candles(candles[low_price_index:])
            if post_low_price_info_acc.price_inc_percent > 0:
                current_candle: Candlestick = candles[-1]
                previous_candle: Candlestick = candles[-2]
                previous_candle_atr = previous_candle.technical_indicators.atr

                take_profit = current_candle.close_price + previous_candle_atr * 17
                stop_loss = current_candle.close_price - previous_candle_atr * 10

                moby_order = MobyOrder(
                    ticker=current_candle.ticker,
                    order_price=current_candle.close_price,
                    quantity=1,
                    position=OrderPosition.Long,
                    order_label=self.order_label,
                    leverage=15,
                    take_profit_price=take_profit,
                    stop_loss=stop_loss
                )
                return moby_order

        return None

    def real_orders(self):
        return None

# Local Testing
if __name__ == "__main__":
    import os
    backtest_analyzer = AnalyzerJ2()
    backtest_start_time = datetime(2020, 12, 31)

    start = datetime.utcnow()
    Backtesting().backtest(backtest_analyzer, backtest_start_time, processes=12)
    end = datetime.utcnow()

    time_elapsed = (end - start).total_seconds()
    print()
    print("Finished in", time_elapsed, "seconds.")

    os.system('say "Test ready"')
