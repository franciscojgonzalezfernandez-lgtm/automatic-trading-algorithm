# Copyright 2021 TradersOfTheUniverse All Rights Reserved.
#
# [Candlestick - Representation of a candlestick]
#
# Authors:
#   antoniojose.luqueocana@telefonica.com
#   joseluis.roblesurquiza@telefonica.com
#   franciscojavier.gonzalezfernandez1@telefonica.com
#
# Version: 0.1
#

from datetime import datetime
from enum import Enum

from core.utils.utils import round_seconds, unix_time_to_datetime_utc


class PriceZone(str, Enum):
    UNKNOWN = "UNKNOWN"
    RESISTANCE = "RESISTANCE"
    SUPPORT = "SUPPORT"


class TAData:
    """Struct with many types of technical indicators"""
    def __init__(self):
        """Default constructor"""
        self.atr: float = None
        self.sma: float = None
        self.adx: float = None

        self.bollinger_high_band: float = None
        self.bollinger_middle_band: float = None
        self.bollinger_low_band: float = None
        self.bollinger_price_zone: PriceZone = None

        self.donchian_high_band: float = None
        self.donchian_low_band: float = None
        self.donchian_price_zone: PriceZone = None

        self.keltner_high_band: float = None
        self.keltner_low_band: float = None
        self.keltner_price_zone: PriceZone = None


class Candlestick:
    """Representation of a candlestick"""

    def __init__(self, ticker_name: str, raw_info: list):

        self.ticker = ticker_name
        self.open_time: datetime = unix_time_to_datetime_utc(raw_info[0])
        self.open_price: float = float(raw_info[1])
        self.high_price: float = float(raw_info[2])
        self.low_price: float = float(raw_info[3])
        self.close_price: float = float(raw_info[4])
        self.volume: float = float(raw_info[5])
        self.close_time: datetime = round_seconds(unix_time_to_datetime_utc(raw_info[6]))
        self.quote_asset_volume: float = float(raw_info[7])
        self.number_of_trades: int = raw_info[8]
        self.taker_buy_base_asset_volume: float = float(raw_info[9])
        self.taker_buy_quote_asset_volume: float = float(raw_info[10])

        self.__init_metrics()

        # Can be filled ad-hoc for each analyzer
        self.technical_indicators: TAData = None

    def __init_metrics(self):
        """Inicializa las metricas sinteticas"""

        self.shadow: float = self.high_price - self.low_price

        if self.open_price > self.close_price:
            self.color: str = "RED"
            self.lower_shadow: float = self.close_price - self.low_price
            self.upper_shadow: float = self.high_price - self.open_price

        else:
            self.color: str = "GREEN"
            self.lower_shadow: float = self.open_price - self.low_price
            self.upper_shadow: float = self.high_price - self.close_price

        self.upper_shadow_percent: float = (self.upper_shadow / self.shadow if (self.shadow > 0.0) else 0.0) * 100
        self.lower_shadow_percent: float = (self.lower_shadow / self.shadow if (self.shadow > 0.0) else 0.0) * 100

        self.price_inc = self.close_price - self.open_price
        self.price_inc_percent = (self.price_inc / self.open_price) * 100
        self.price_inc_factor = self.close_price / self.open_price

        self.real_body = abs(self.price_inc)

        self.is_low_hammer:  bool = self.lower_shadow > 2 * self.real_body and self.lower_shadow > (self.real_body + self.upper_shadow)
        self.is_high_hammer: bool = self.upper_shadow > 2 * self.real_body and self.upper_shadow > (self.real_body + self.lower_shadow)
        self.is_hammer: bool = self.is_low_hammer or self.is_high_hammer

    def accumulate(self, candlestick):

        self.high_price = max(self.high_price, candlestick.high_price)
        self.low_price = min(self.low_price, candlestick.low_price)

        self.close_price = candlestick.close_price if candlestick.close_time > self.close_time else self.close_price
        self.open_price = candlestick.open_price if candlestick.open_time < self.open_time else self.open_price

        self.close_time = max(self.close_time, candlestick.close_time)
        self.open_time = min(self.open_time, candlestick.open_time)

        self.volume += candlestick.volume
        self.quote_asset_volume += candlestick.quote_asset_volume
        self.number_of_trades += candlestick.number_of_trades
        self.taker_buy_base_asset_volume += candlestick.taker_buy_base_asset_volume
        self.taker_buy_quote_asset_volume += candlestick.taker_buy_quote_asset_volume

        self.__init_metrics()

    def accumulate_all(self, candles: list):
        for candle in candles:
            self.accumulate(candle)