# Copyright 2021 TradersOfTheUniverse S.A. All Rights Reserved.
#
# [Technical Indicators - This class calculates technical indicators]
#
# Authors:
#   antoniojose.luqueocana@telefonica.com
#   joseluis.roblesurquiza@telefonica.com
#   franciscojavier.gonzalezfernandez1@telefonica.com
#
# Version: 0.1
#
import json
from enum import Enum
from typing import List

import pandas as pd
import numpy as np

# https://github.com/bukosabino/ta
# https://technical-analysis-library-in-python.readthedocs.io/en/latest/
from ta.trend import ADXIndicator, SMAIndicator
from ta.volatility import DonchianChannel, BollingerBands, KeltnerChannel, AverageTrueRange

from core.candles.binance_client import BinanceClient
from core.candles.candlestick import Candlestick, TAData, PriceZone

np.seterr(invalid='ignore')


class Trend(str, Enum):
    UNKNOWN = "UNKNOWN"
    UPTREND = "UPTREND"
    DOWNTREND = "DOWNTREND"


class TechnicalIndicators:
    """Technical Indicators - This class calculates technical indicators"""

    def __init__(self):
        """Default constructor"""

        self.__binance_client = BinanceClient()

    def get_trend(self, coin: str, num_periods_sma=30, interval="5m", adx_min=25) -> Trend:
        """Obtiene tendencia (bajista, alcista, o lateral), descargando velas de Binance API"""

        periods = self.__binance_client.get_last_candlesticks(coin, 200, interval)  # 200 for ADX
        return self.get_trend_from_periods(periods, num_periods_sma, adx_min)

    def get_trend_from_periods(self, periods: List[Candlestick], num_periods_sma=30, adx_min=25) -> Trend:
        """Obtiene tendencia (bajista, alcista, o lateral)"""

        self.generate_simple_moving_average(periods, num_periods_sma)
        sma = periods[-1].technical_indicators.sma

        self.generate_average_directional_index(periods)
        adx1 = periods[-1].technical_indicators.adx
        adx2 = periods[-2].technical_indicators.adx
        adx3 = periods[-3].technical_indicators.adx

        # Only strong Trend: [20,30] < ADX < 60, and increasing
        if adx1 < adx_min or adx1 > 60 or (adx1 < adx2 < adx3):
            return Trend.UNKNOWN

        current_candle = periods[-1]
        previous_candle = periods[-2]

        # TODO Falta identificar la tendencia lateral
        if current_candle.low_price > sma and previous_candle.low_price > sma:
            return Trend.UPTREND
        if current_candle.high_price < sma and previous_candle.high_price < sma:
            return Trend.DOWNTREND
        return Trend.UNKNOWN

    def generate_all_technical_indicators(self, periods: List[Candlestick], num_periods_sma: int = 30):
        """Llama a todos los generate() de esta clase para enriquecer las velas"""
        technical_indicators.generate_simple_moving_average(periods, num_periods_sma)
        technical_indicators.generate_average_true_range(periods)
        technical_indicators.generate_average_directional_index(periods)
        technical_indicators.generate_donchian_channel(periods)
        technical_indicators.generate_bollinger_bands(periods)
        technical_indicators.generate_keltner_channel(periods)

    def generate_simple_moving_average(self, periods: List[Candlestick], num_periods: int):
        """SMA: Media Simple Móvil"""

        close = pd.Series([p.close_price for p in periods])

        sma_series = SMAIndicator(close, num_periods).sma_indicator()

        for i in range(len(periods)):
            period = periods[i]
            if not period.technical_indicators:
                period.technical_indicators = TAData()
            period.technical_indicators.sma = sma_series[i]

    def generate_average_directional_index(self, periods: List[Candlestick]):
        """ADX: Indice medio de movimiento direccional"""

        if len(periods) < 200:
            raise Exception("Hacen falta al menos 200 velas para calcular el ADX")

        high = pd.Series([p.high_price for p in periods])
        low = pd.Series([p.low_price for p in periods])
        close = pd.Series([p.close_price for p in periods])

        adx_series = ADXIndicator(high, low, close).adx()
        for i in range(len(periods)):
            period = periods[i]
            if not period.technical_indicators:
                period.technical_indicators = TAData()
            period.technical_indicators.adx = adx_series[i]

    def generate_average_true_range(self, periods: List[Candlestick], window: int = 14):
        """ATR: Rango Verdadero Medio"""

        if len(periods) < 20:
            raise Exception("Hacen falta al menos 20 velas para calcular el ATR")

        high = pd.Series([p.high_price for p in periods])
        low = pd.Series([p.low_price for p in periods])
        close = pd.Series([p.close_price for p in periods])

        atr_series = AverageTrueRange(high, low, close, window).average_true_range()
        for i in range(len(periods)):
            period = periods[i]
            if not period.technical_indicators:
                period.technical_indicators = TAData()
            period.technical_indicators.atr = atr_series[i]

    def generate_donchian_channel(self, periods: List[Candlestick], num_periods=20):
        """Generate bands and infers if price is in resistance or support zone by using Donchian Channel"""

        if len(periods) < 20:
            raise Exception("Hacen falta al menos 20 velas para calcular el Donchian Channel")

        # TODO afinar este porcentaje
        zone_width = 0.05  # 5% close to the limit to be considered support or resistance

        high = pd.Series([p.high_price for p in periods])
        low = pd.Series([p.low_price for p in periods])
        close = pd.Series([p.close_price for p in periods])

        channel = DonchianChannel(high, low, close, window=num_periods)
        high_band = channel.donchian_channel_hband()
        low_band = channel.donchian_channel_lband()
        percentage_band = channel.donchian_channel_pband()

        for i in range(len(periods)):
            period = periods[i]
            if not period.technical_indicators:
                period.technical_indicators = TAData()
            period.technical_indicators.donchian_high_band = high_band[i]
            period.technical_indicators.donchian_low_band = low_band[i]

            price_percentage = percentage_band[i]
            if price_percentage < zone_width:
                period.technical_indicators.donchian_price_zone = PriceZone.SUPPORT
            elif price_percentage > 1 - zone_width:
                period.technical_indicators.donchian_price_zone = PriceZone.RESISTANCE
            else:
                period.technical_indicators.donchian_price_zone = PriceZone.UNKNOWN

    def generate_bollinger_bands(self, periods: List[Candlestick]):
        """Generate bands and infers if price is in resistance or support zone by using Bollinger Bands"""

        if len(periods) < 20:
            raise Exception("Hacen falta al menos 20 velas para calcular el Bollinger Bands")

        # TODO afinar este porcentaje
        zone_width = 0.05  # 5% close to the limit to be considered support or resistance

        close = pd.Series([p.close_price for p in periods])

        bands = BollingerBands(close)
        high_band = bands.bollinger_hband()
        middle_band = bands.bollinger_mavg()
        low_band = bands.bollinger_lband()
        percentage_band = bands.bollinger_pband()

        for i in range(len(periods)):
            period = periods[i]
            if not period.technical_indicators:
                period.technical_indicators = TAData()
            period.technical_indicators.bollinger_high_band = high_band[i]
            period.technical_indicators.bollinger_middle_band = middle_band[i]
            period.technical_indicators.bollinger_low_band = low_band[i]

            price_percentage = percentage_band[i]
            if price_percentage < zone_width:
                period.technical_indicators.bollinger_price_zone = PriceZone.SUPPORT
            elif price_percentage > 1 - zone_width:
                period.technical_indicators.bollinger_price_zone = PriceZone.RESISTANCE
            else:
                period.technical_indicators.bollinger_price_zone = PriceZone.UNKNOWN

    def generate_keltner_channel(self, periods: List[Candlestick]):
        """Generate bands and infers if price is in resistance or support zone by using Keltner Channel"""

        if len(periods) < 20:
            raise Exception("Hacen falta al menos 20 velas para calcular el Keltner Channel")

        zone_width = 0.05  # 5% close to the limit to be considered support or resistance

        high = pd.Series([p.high_price for p in periods])
        low = pd.Series([p.low_price for p in periods])
        close = pd.Series([p.close_price for p in periods])

        channel = KeltnerChannel(high, low, close)
        high_band = channel.keltner_channel_hband()
        low_band = channel.keltner_channel_lband()
        percentage_band = channel.keltner_channel_pband()

        for i in range(len(periods)):
            period = periods[i]
            if not period.technical_indicators:
                period.technical_indicators = TAData()
            period.technical_indicators.keltner_high_band = high_band[i]
            period.technical_indicators.keltner_low_band = low_band[i]

            price_percentage = percentage_band[i]
            if price_percentage < zone_width:
                period.technical_indicators.keltner_price_zone = PriceZone.SUPPORT
            elif price_percentage > 1 - zone_width:
                period.technical_indicators.keltner_price_zone = PriceZone.RESISTANCE
            else:
                period.technical_indicators.keltner_price_zone = PriceZone.UNKNOWN


# Local Testing
if __name__ == "__main__":
    technical_indicators = TechnicalIndicators()
    trend = technical_indicators.get_trend("XRPUSDT", num_periods_sma=30, interval="5m")
    print(trend)

    periods_test = BinanceClient().get_last_candlesticks("XRPUSDT", num_candlesticks=200, interval="1m")
    technical_indicators.generate_all_technical_indicators(periods_test)
    print(json.dumps(periods_test[-1].technical_indicators.__dict__, indent=4))


