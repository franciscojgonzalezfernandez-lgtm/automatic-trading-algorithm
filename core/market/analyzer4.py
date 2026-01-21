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

from core.market.technical_indicators import TechnicalIndicators, Trend
from core.candles.binance_client import BinanceClient
from core.order.binance_order import BinanceOrder
from core.order.moby_order import MobyOrder, OrderPosition
from core.order.order_simulator import OrderSimulator


class Analyzer4:
    """Market Analyzer - This class looks for signals in the market"""

    def __init__(self):
        """Default constructor"""

        self.__binance_client = BinanceClient()
        self.__order_simulator = OrderSimulator()
        self.__binance_order = BinanceOrder()
        self.__technical_indicators = TechnicalIndicators()

        self.coins_to_analyze = ["DOGEUSDT", "XRPUSDT", "ADAUSDT", "IOTAUSDT", "ETHUSDT",
                                 "BTCUSDT", "EOSUSDT", "SOLUSDT", "THETAUSDT",
                                 "MATICUSDT", "LTCUSDT", "BCHUSDT", "ADAUSDT",
                                 "LUNAUSDT", "VETUSDT", "CELRUSDT", "1INCHUSDT",
                                 "DOTUSDT", "LINKUSDT", "ONTUSDT", "NEOUSDT",
                                 "SUSHIUSDT", "TRXUSDT", "CHZUSDT"
                                 ]
        self.coins_to_analyze_no_usdt = ["FRONTBUSD"]
        self.coins_to_analyze_no_futures = ["SHIBUSDT", "CAKEUSDT", "FTTUSDT", "NANOUSDT", "ASRUSDT"]

        self.order_label = "[Bollinger][v0.0.11]"

        self.trailing_stop_distance_multiplier = 0.3
        self.stoploss_distance_multiplier = 0.6
        self.take_profit_distance_multiplier = 0.9

    def analyze_all(self):
        """Analiza todas las monedas para buscar entradas"""

        for coin in self.coins_to_analyze:
            print("Processing " + coin)
            try:
                self.analyze(coin)
            except Exception as e:
                logging.exception("Error procesando " + coin + ". " + repr(e))

    def analyze(self, coin):
        """Analiza una moneda para buscar entradas"""
        candles = self.__binance_client.get_last_candlesticks(coin=coin, num_candlesticks=99, interval="1m")
        trend = self.__technical_indicators.get_trend(coin, num_periods_sma=150, interval="1m", adx_min=25)
        # TODO: Probar con num_periods=30, interval="5m", por eso del ADX a 5m
        # TODO: Probar con num_periods=150, interval="1m", por eso del ADX a 1m
        # TODO: Probar con ADX min [20,30] (mejor 30 si 1m, y mejor 20 si 5m)

        self.__technical_indicators.generate_bollinger_bands(candles)

        current_candle = candles[-1]
        previous_candle = candles[-2]

        distance_to_middle_band = abs(current_candle.close_price - previous_candle.technical_indicators.bollinger_middle_band)
        percentage_distance_to_middle_band = 100 * distance_to_middle_band / current_candle.close_price

        percentage_trailing_stop = round(percentage_distance_to_middle_band * self.trailing_stop_distance_multiplier, 1)
        percentage_trailing_stop = max(percentage_trailing_stop, 0.1)
        percentage_trailing_stop = min(percentage_trailing_stop, 5.0)

        if percentage_distance_to_middle_band < 0.2:
            return

        moby_order = None
        order_label = self.order_label + ("[NO_TREND]" if trend == Trend.UNKNOWN else "[TREND]")

        if previous_candle.open_price < previous_candle.technical_indicators.bollinger_low_band\
                and previous_candle.close_price < previous_candle.technical_indicators.bollinger_low_band\
                and trend != Trend.DOWNTREND\
                and current_candle.color == "GREEN":
            moby_order = MobyOrder(
                ticker=coin,
                order_price=current_candle.close_price,
                quantity=5,
                position=OrderPosition.Long,
                order_label=order_label,
                leverage=15,
                stop_loss=current_candle.close_price - (distance_to_middle_band * self.stoploss_distance_multiplier),
                take_profit_price=current_candle.close_price + (distance_to_middle_band * self.take_profit_distance_multiplier),
                trailing_stop=percentage_trailing_stop,
                trailing_stop_activation_percent=percentage_trailing_stop,
            )

        elif previous_candle.open_price > previous_candle.technical_indicators.bollinger_high_band\
                and previous_candle.close_price > previous_candle.technical_indicators.bollinger_high_band\
                and trend != Trend.UPTREND\
                and current_candle.color == "RED":
            moby_order = MobyOrder(
                ticker=coin,
                order_price=current_candle.close_price,
                quantity=5,
                position=OrderPosition.Short,
                order_label=order_label,
                leverage=15,
                stop_loss=current_candle.close_price + (distance_to_middle_band * self.stoploss_distance_multiplier),
                take_profit_price=current_candle.close_price - (distance_to_middle_band * self.take_profit_distance_multiplier),
                trailing_stop=percentage_trailing_stop,
                trailing_stop_activation_percent=percentage_trailing_stop
            )

        if moby_order is not None:
            logging.warning("Entrada Bollinger: " + coin)
            self.__order_simulator.open_position_simulation(moby_order)


# Local Testing
if __name__ == "__main__":
    import time
    market_analyzer = Analyzer4()
    while True:
        start = datetime.utcnow()
        market_analyzer.analyze_all()
        end = datetime.utcnow()
        time_elapsed = (end - start).total_seconds()
        print("Finished in", time_elapsed, "seconds. Sleep...")
        time.sleep(60 - time_elapsed if time_elapsed < 60 else 59)
