# Copyright 2021 TradersOfTheUniverse S.A. All Rights Reserved.
#
# [Candles Period - Comparison of two halves of a period of candlesticks]
#
# Authors:
#   antoniojose.luqueocana@telefonica.com
#   joseluis.roblesurquiza@telefonica.com
#   franciscojavier.gonzalezfernandez1@telefonica.com
#
# Version: 0.1
#

from copy import deepcopy
from datetime import datetime
from core.candles.candlestick import Candlestick


class CandlesPeriod:
    """Comparison of two halves of a period of candlesticks"""

    def __init__(self, candlesticks: list):

        self.ticker: str
        self.color: str

        self.volume_inc: float
        self.volume_inc_factor: float
        self.volume_avg: float
        self.volume_acc: float
        self.shadow_avg: float

        self.open_price: float
        self.close_price: float
        self.price_inc: float
        self.price_inc_factor: float

        self.high_price: float
        self.low_price: float
        self.upper_shadow: float
        self.lower_shadow: float

        self.close_time_analysis: datetime = None
        self.open_time_analysis: datetime = None

        self.candlesticks = candlesticks

        self.first_part_candle_acc: Candlestick
        self.second_part_candle_acc: Candlestick

        self.__update_values()
        self.__init_metrics()

    def __update_values(self):
        """Inicializa las variables de PeriodAnalysis en base a candlesticks"""

        if len(self.candlesticks) % 2 != 0 or len(self.candlesticks) < 2:
            raise Exception("Para el analisis es necesario un listado de candlestics multiplo de 2")

        first_part = self.candlesticks[:len(self.candlesticks)//2]
        second_part = self.candlesticks[len(self.candlesticks)//2:]
        self.first_part_candle_acc: Candlestick = deepcopy(first_part[0])
        self.second_part_candle_acc: Candlestick = deepcopy(second_part[0])

        """Accumulador"""
        for i in range(1, len(first_part)):
            self.first_part_candle_acc.accumulate(first_part[i])
            self.second_part_candle_acc.accumulate(second_part[i])

    def __init_metrics(self):

        if self.first_part_candle_acc.ticker != self.second_part_candle_acc.ticker:
            raise Exception("Nombre de tickers no coincidentes")

        if self.first_part_candle_acc.open_time > self.second_part_candle_acc.open_time or self.first_part_candle_acc.close_time > self.second_part_candle_acc.close_time:
            raise Exception("Periodos de tickers no congruentes")

        # if self.first_part_candle_acc.close_time - self.first_part_candle_acc.open_time != self.second_part_candle_acc.close_time - self.second_part_candle_acc.open_time:
            # raise Exception("Periodos diferentes")

        self.ticker = self.first_part_candle_acc.ticker

        self.volume_inc = self.second_part_candle_acc.volume - self.first_part_candle_acc.volume
        self.volume_inc_factor = self.second_part_candle_acc.volume / self.first_part_candle_acc.volume if self.first_part_candle_acc.volume else None

        self.volume_acc = self.first_part_candle_acc.volume + self.second_part_candle_acc.volume
        self.volume_avg = (self.first_part_candle_acc.volume + self.second_part_candle_acc.volume) / len(self.candlesticks)
        self.shadow_avg = sum(a.shadow for a in self.candlesticks) / len(self.candlesticks)

        self.open_price = self.first_part_candle_acc.open_price
        self.close_price = self.second_part_candle_acc.close_price
        self.price_inc = self.second_part_candle_acc.close_price - self.first_part_candle_acc.close_price
        self.price_inc_factor = self.second_part_candle_acc.close_price / self.first_part_candle_acc.close_price

        self.high_price = max(self.first_part_candle_acc.high_price, self.second_part_candle_acc.high_price)
        self.low_price = min(self.first_part_candle_acc.low_price, self.second_part_candle_acc.low_price)

        self.upper_shadow = max(self.first_part_candle_acc.upper_shadow, self.second_part_candle_acc.upper_shadow)
        self.lower_shadow = min(self.first_part_candle_acc.lower_shadow, self.second_part_candle_acc.lower_shadow)

        self.open_time_analysis = self.first_part_candle_acc.open_time
        self.close_time_analysis = self.second_part_candle_acc.close_time

        self.color = "RED" if self.open_price > self.close_price else "GREEN"
