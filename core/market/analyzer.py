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
import json
import logging
import pandas as pd

from datetime import datetime
from typing import List
from google.cloud import bigquery

from core.market.technical_indicators import TechnicalIndicators, Trend
from core.candles.binance_client import BinanceClient
from core.candles.candlestick import Candlestick
from core.candles.candles_period import CandlesPeriod
from core.alerts.telegram import Telegram
from core.order.binance_order import BinanceOrder
from core.order.moby_order import MobyOrder, OrderPosition
from core.order.order_simulator import OrderSimulator


class Analyzer:
    """Market Analyzer - This class looks for signals in the market"""

    def __init__(self):
        """Default constructor"""

        self.__telegram = Telegram()
        self.__binance_client = BinanceClient()
        self.__order_simulator = OrderSimulator()
        self.__binance_order = BinanceOrder()
        self.__technical_indicators = TechnicalIndicators()
        self.__binance_spot_chart_url = "https://www.binance.com/es/trade/{0}"
        self.__binance_futures_chart_url = "https://www.binance.com/es/futures/{0}"
        self.__trading_view_url = "https://es.tradingview.com/chart/?symbol=BINANCE:{0}&interval=1"

        self.coins_to_analyze = ["DOGEUSDT", "XRPUSDT", "ADAUSDT", "IOTAUSDT", "ETHUSDT",
                                 "BTCUSDT", "EOSUSDT", "SOLUSDT", "THETAUSDT",
                                 "MATICUSDT", "LTCUSDT", "BCHUSDT", "ADAUSDT",
                                 "LUNAUSDT", "VETUSDT", "CELRUSDT", "1INCHUSDT",
                                 "DOTUSDT", "LINKUSDT", "ONTUSDT", "NEOUSDT",
                                 "SUSHIUSDT", "TRXUSDT", "CHZUSDT"
                                 ]
        self.coins_to_analyze_no_usdt = ["FRONTBUSD"]
        self.coins_to_analyze_no_futures = ["SHIBUSDT", "CAKEUSDT", "FTTUSDT", "NANOUSDT", "ASRUSDT"]

        self.__bq_client = bigquery.Client(project="fender-310315")

    def dog_analyze(self):
        """Analiza monedas para activar o no el modo rastreo"""

        lock_for: dict = {}
        for coin in self.coins_to_analyze:
            print("Processing " + coin)
            try:
                high_volume_periods = self.dog_analyze_volume(coin)
                if len(high_volume_periods) > 0:
                    lock_for[coin] = high_volume_periods
            except Exception as e:
                logging.exception("Error procesando " + coin + ". " + repr(e))

        if len(lock_for) > 0:
            msg = "DOG_VOLUME\n" + json.dumps(lock_for, indent=3)
            print(msg)
            self.__telegram.send_message_to_group_2(msg)

    def check_integrity(self, coin):
        candles_1min = self.__binance_client.get_last_candlesticks(coin=coin, num_candlesticks=61 + datetime.utcnow().minute % 30, interval="1m")
        candles_1min = candles_1min[0:60]
        candles_30min = self.__binance_client.get_last_candlesticks(coin=coin, num_candlesticks=3, interval="30m")
        candles_30min = candles_30min[:-1]

        candles_1min_period: CandlesPeriod = CandlesPeriod(candles_1min)
        candles_30min_period: CandlesPeriod = CandlesPeriod(candles_30min)

        if round(candles_1min_period.first_part_candle_acc.volume, 2) != candles_30min[0].volume:
            raise Exception("Volume integrity error")
        if round(candles_1min_period.second_part_candle_acc.volume, 2) != candles_30min[1].volume:
            raise Exception("Volume integrity error")
        if candles_1min_period.first_part_candle_acc.open_price != candles_30min[0].open_price:
            raise Exception("Open Price integrity error")
        if candles_1min_period.second_part_candle_acc.open_price != candles_30min[1].open_price:
            raise Exception("Open Price integrity error")
        if candles_1min_period.first_part_candle_acc.close_price != candles_30min[0].close_price:
            raise Exception("Close Price integrity error")
        if candles_1min_period.second_part_candle_acc.close_price != candles_30min[1].close_price:
            raise Exception("Close Price integrity error")
        if candles_1min_period.first_part_candle_acc.high_price != candles_30min[0].high_price:
            raise Exception("high_price integrity error")
        if candles_1min_period.second_part_candle_acc.high_price != candles_30min[1].high_price:
            raise Exception("high_price integrity error")
        if candles_1min_period.first_part_candle_acc.low_price != candles_30min[0].low_price:
            raise Exception("low_price integrity error")
        if candles_1min_period.second_part_candle_acc.low_price != candles_30min[1].low_price:
            raise Exception("low_price integrity error")
        if candles_1min_period.first_part_candle_acc.color != candles_30min[0].color:
            raise Exception("color integrity error")
        if candles_1min_period.second_part_candle_acc.color != candles_30min[1].color:
            raise Exception("color integrity error")
        if candles_1min_period.first_part_candle_acc.lower_shadow != candles_30min[0].lower_shadow:
            raise Exception("lower_shadow integrity error")
        if candles_1min_period.second_part_candle_acc.lower_shadow != candles_30min[1].lower_shadow:
            raise Exception("lower_shadow integrity error")
        if candles_1min_period.first_part_candle_acc.upper_shadow != candles_30min[0].upper_shadow:
            raise Exception("upper_shadow integrity error")
        if candles_1min_period.second_part_candle_acc.upper_shadow != candles_30min[1].upper_shadow:
            raise Exception("upper_shadow integrity error")
        if candles_1min_period.first_part_candle_acc.high_price != candles_30min[0].high_price:
            raise Exception("high_price integrity error")
        if candles_1min_period.second_part_candle_acc.high_price != candles_30min[1].high_price:
            raise Exception("high_price integrity error")
        if candles_1min_period.second_part_candle_acc.low_price != candles_30min[1].low_price:
            raise Exception("low_price integrity error")
        if candles_1min_period.first_part_candle_acc.low_price != candles_30min[0].low_price:
            raise Exception("low_price integrity error")

        if round(candles_1min_period.volume_inc, 2) != round(candles_30min_period.volume_inc, 2):
            raise Exception("volume_inc integrity error")
        if round(candles_1min_period.volume_inc_factor, 10) != round(candles_30min_period.volume_inc_factor, 10):
            raise Exception("volume_inc_factor integrity error")
        if round(candles_1min_period.volume_acc, 2) != round(candles_30min_period.volume_acc, 2):
            raise Exception("volume_acc integrity error")
        if candles_1min_period.upper_shadow != candles_30min_period.upper_shadow:
            raise Exception("upper_shadow integrity error")
        if candles_1min_period.lower_shadow != candles_30min_period.lower_shadow:
            raise Exception("lower_shadow integrity error")
        if candles_1min_period.high_price != candles_30min_period.high_price:
            raise Exception("high_price integrity error")
        if candles_1min_period.low_price != candles_30min_period.low_price:
            raise Exception("low_price integrity error")

        print("Integrity Check OK")

    def dog_analyze_volume(self, coin):
        """Analiza el comportamiento de volumen para lanzar o no el rastreo (DogMode)"""

        minutes = [1, 3, 5, 10]
        global_candles = self.__binance_client.get_last_candlesticks(coin=coin,
                                                                     num_candlesticks=max(minutes) * 200,
                                                                     interval="1m")
        global_candles.sort(key=lambda x: x.open_time)
        global_analysis: CandlesPeriod = CandlesPeriod(global_candles)
        current_minute_candle = global_candles[-1]

        dog_analysis: dict = {}
        for minute in minutes:
            candles_period = CandlesPeriod(global_candles[(-minute * 2) - 1:-1])

            if candles_period.volume_inc_factor is not None and candles_period.volume_inc_factor > 3 \
                    and candles_period.second_part_candle_acc.volume > global_analysis.volume_avg:

                # Red
                if candles_period.second_part_candle_acc.color == "RED":

                    print("Red Volume " + coin)
                    candles_support_range = [-1 * minute * 20, (-1 * minute)]
                    global_candles_support = global_candles[candles_support_range[0]: candles_support_range[1]]
                    global_candles_support.sort(key=lambda x: x.low_price)

                    support_break: bool = False
                    support_break_index: int
                    support_look_back = len(global_candles_support)
                    previous_support = None
                    for i in range(0, support_look_back):
                        if candles_period.second_part_candle_acc.low_price < global_candles_support[i].low_price:
                            support_break = True
                            support_break_index = i
                            previous_support = global_candles_support[i]
                            break

                    if support_break:
                        print("Local Support Break " + coin)
                        price_diff_percent = 100 * (current_minute_candle.close_price - candles_period.close_price) / current_minute_candle.close_price

                        dog_analysis[str(minute) + "m"] = {
                            "support_break_index": str(support_break_index) + " of " + str(support_look_back) + " minutes",

                            "volume_inc_factor": candles_period.volume_inc_factor,
                            "price_inc_factor": candles_period.price_inc_factor,

                            "candles_period_color": candles_period.color,
                            "last_minute_color": current_minute_candle.color,

                            "current_minute_price": current_minute_candle.close_price,
                            "current_period_price": candles_period.second_part_candle_acc.close_price,

                            "previous_support_price": previous_support.low_price,
                            "previous_support_time": str(previous_support.open_time),
                        }
                        break

                # Green
                #else:
                #    global_candles_resistance = global_candles[(-minutes * 100):-1]
                #    global_candles_resistance.sort(key=lambda x: x.high_price)

        return dog_analysis

    def analyze_all(self):
        """Analyze market for signals"""
        minutes = [2, 4, 8, 16]

        # Hypothesis
        for coin in self.coins_to_analyze:
            print("Processing " + coin)
            try:
                global_trend = self.__technical_indicators.get_trend(coin, num_periods_sma=50, interval="30m")
                high_trend = self.__technical_indicators.get_trend(coin, num_periods_sma=30, interval="5m")
                high_trend_small_adx = self.__technical_indicators.get_trend(coin, num_periods_sma=150, interval="1m")
                small_trend = self.__technical_indicators.get_trend(coin, num_periods_sma=15, interval="1m")

                mixed_trend = Trend.UNKNOWN if global_trend != high_trend else global_trend
                mixed_trend_2 = Trend.UNKNOWN if global_trend != small_trend else global_trend

                candles_array = self.__binance_client.get_last_candlesticks(coin, 17)

                global_analysis: CandlesPeriod = CandlesPeriod(candles_array[:-1])

                #Ordenes Reales, desactivadas de momento
                for minute in minutes:
                    self.analyze_volume(coin=coin, trend=small_trend, order_label="[15P_1M_TREND]",
                                        minutes_to_analyze=minute, candlesarray=candles_array[-(minute + 1):],
                                        trailing_stop_percentage=0.4, trailing_stop_activation_percentage=0.35,
                                        real_order=False,
                                        stoploss_factor=0.009,
                                        volume_avg=global_analysis.volume_avg)

                    self.analyze_volume(coin=coin, trend=high_trend, order_label="[30P_5M_TREND]",
                                        minutes_to_analyze=minute, candlesarray=candles_array[-(minute + 1):],
                                        real_order=False,
                                        trailing_stop_percentage=0.4, trailing_stop_activation_percentage=0.35,
                                        stoploss_factor=0.009,
                                        volume_avg=global_analysis.volume_avg)

                #Simulaciones
                for minute in minutes:
                    self.analyze_volume(coin=coin, trend=global_trend, order_label="[50P_30M_TREND]",
                                        minutes_to_analyze=minute, candlesarray=candles_array[-(minute + 1):],
                                        trailing_stop_percentage=0.4, trailing_stop_activation_percentage=0.35,
                                        stoploss_factor=0.009,
                                        volume_avg=global_analysis.volume_avg)

                    self.analyze_volume(coin=coin, trend=mixed_trend, order_label="[MIXED_TREND]",
                                        minutes_to_analyze=minute, candlesarray=candles_array[-(minute + 1):],
                                        trailing_stop_percentage=0.4, trailing_stop_activation_percentage=0.35,
                                        stoploss_factor=0.009,
                                        volume_avg=global_analysis.volume_avg)

                    self.analyze_volume(coin=coin, trend=high_trend_small_adx, order_label="[30P_5M_1ADX_TREND]",
                                        minutes_to_analyze=minute, candlesarray=candles_array[-(minute + 1):],
                                        real_order=False,
                                        trailing_stop_percentage=0.4, trailing_stop_activation_percentage=0.35,
                                        stoploss_factor=0.009,
                                        volume_avg=global_analysis.volume_avg)
                    self.analyze_volume(coin=coin, trend=high_trend_small_adx, order_label="[30P_5M_1ADX_TREND]",
                                        minutes_to_analyze=minute, candlesarray=candles_array[-(minute + 1):],
                                        real_order=False,
                                        trailing_stop_percentage=0.4, trailing_stop_activation_percentage=0.8,
                                        stoploss_factor=0.004,
                                        volume_avg=global_analysis.volume_avg)
                    self.analyze_volume(coin=coin, trend=high_trend_small_adx, order_label="[30P_5M_1ADX_TREND]",
                                        minutes_to_analyze=minute, candlesarray=candles_array[-(minute + 1):],
                                        real_order=False,
                                        trailing_stop_percentage=1.0, trailing_stop_activation_percentage=1.0,
                                        stoploss_factor=0.01,
                                        volume_avg=global_analysis.volume_avg)

                self.analyze_hammer(last_3_candles=candles_array[-3:], trend=mixed_trend,
                                    entry_indicator="[MIXED_TREND]", send_alert=False)
                self.analyze_hammer(last_3_candles=candles_array[-3:], trend=global_trend,
                                    entry_indicator="[50P_30M_TREND]", send_alert=False)
                self.analyze_hammer(last_3_candles=candles_array[-3:], trend=high_trend,
                                    entry_indicator="[30P_5M_TREND]", send_alert=False)
                self.analyze_hammer(last_3_candles=candles_array[-3:], trend=small_trend,
                                    entry_indicator="[15P_1M_TREND]", send_alert=False)

            except Exception as e:
                logging.exception("Error procesando " + coin + ". " + repr(e))

    def analyze_hammer(self, last_3_candles: List[Candlestick], entry_indicator: str = "", trend=Trend.UNKNOWN,
                       send_alert=False):
        """Identifica patrones martillo en el array de velas"""

        complete_candle = last_3_candles[-2]
        previous_candle = last_3_candles[-3]

        volume_inc_factor = complete_candle.volume / previous_candle.volume if previous_candle.volume else 1

        if complete_candle.is_hammer and volume_inc_factor > 3:

            hammer_candle = complete_candle
            current_candle = last_3_candles[-1]
            if hammer_candle.is_low_hammer and trend == Trend.UPTREND:
                logging.warning("Start Long Simulation: " + current_candle.ticker)
                moby_order = MobyOrder(
                    ticker=current_candle.ticker,
                    order_price=current_candle.close_price,
                    quantity=5.0,
                    stop_loss=hammer_candle.low_price,
                    position=OrderPosition.Long,
                    order_label="[LOW_HAMMER][HIGH_VOLUME][UPTREND]" + entry_indicator,
                    leverage=2,
                    trailing_stop=0.4,
                    trailing_stop_activation_percent=0.4)

                self.__order_simulator.open_position_simulation(moby_order)

            elif hammer_candle.is_high_hammer and trend == Trend.DOWNTREND:
                logging.warning("Start Short Simulation: " + current_candle.ticker)
                moby_order = MobyOrder(
                    ticker=current_candle.ticker,
                    order_price=current_candle.close_price,
                    quantity=5.0,
                    stop_loss=hammer_candle.high_price,
                    position=OrderPosition.Short,
                    order_label="[HIGH_HAMMER][HIGH_VOLUME][DOWNTREND]" + entry_indicator,
                    leverage=2,
                    trailing_stop=0.4,
                    trailing_stop_activation_percent=0.4)

                self.__order_simulator.open_position_simulation(moby_order)

            logging.warning("Notify chat_ " + hammer_candle.ticker)
            if send_alert:
                hammer_type = "HIGH" if hammer_candle.is_high_hammer else "LOW"
                message = ("<strong>{0} HAMMER ALERT</strong>".format(hammer_type) +
                           "\n" + hammer_candle.color + ": " + hammer_candle.ticker +
                           "\nVol Inc factor <u>" + str(round(volume_inc_factor, 3)) + "</u>"
                                                                                       "\nPrice: " + str(
                            hammer_candle.close_price) +
                           "\nPrice Inc factor: " + str(round(hammer_candle.price_inc_factor, 3)) +
                           "\nPrice Inc %: " + str(round(hammer_candle.price_inc_percent, 3)) +
                           "\nLower Shadow %: " + str(round(hammer_candle.lower_shadow_percent, 3)) +
                           "\nLast Update %: " + str(hammer_candle.open_time) +
                           "\nTrend: " + trend +
                           "\nTrading View: " + self.__trading_view_url.format(hammer_candle.ticker)
                           )

                if __name__ == "__main__":
                    print(message)

                self.__telegram.send_message_to_group_1(message)

    def analyze_volume(self, coin,
                       candlesarray=None,
                       trend=Trend.UNKNOWN,
                       trailing_stop_percentage=0.4,
                       trailing_stop_activation_percentage=0.4,
                       minutes_to_analyze=4,
                       order_label: str = "",
                       real_order: bool = False,
                       volume_avg: float = None,
                       stoploss_factor: float = 0.02942):
        """Look for signals related to valume. Send alert and returns True if found."""

        if candlesarray is None:
            candlesarray = self.__binance_client.get_last_candlesticks(coin, minutes_to_analyze + 1)

        period_analysis: CandlesPeriod = CandlesPeriod(candlesarray[:-1])
        current_candle = candlesarray[-1]
        complete_candle_acc = period_analysis.second_part_candle_acc
        moby_order = None

        # Entradas rápidas en Long
        if period_analysis.volume_inc_factor > 4 and (volume_avg is None or complete_candle_acc.volume > volume_avg):

            if trend == Trend.UPTREND and complete_candle_acc.color == "RED":
                order_label: str = "[" + complete_candle_acc.color + "_VOLUME][" + str(trend) + "][LONG][SIZE(" + str(
                    len(candlesarray)) + ")][v" + str(stoploss_factor) + "]" + order_label \
                    + "[TRAILING_{0}_{1}]".format(trailing_stop_percentage, trailing_stop_activation_percentage) \
                    + ("[VOL_AVG]" if volume_avg is not None else "")
                moby_order = MobyOrder(
                    ticker=current_candle.ticker,
                    order_price=current_candle.close_price,
                    quantity=15.0,
                    stop_loss=complete_candle_acc.low_price * (1 - stoploss_factor),
                    position=OrderPosition.Long,
                    order_label=order_label,
                    leverage=2,
                    trailing_stop=trailing_stop_percentage,
                    trailing_stop_activation_percent=trailing_stop_activation_percentage)

            elif trend == Trend.DOWNTREND and complete_candle_acc.color == "GREEN":
                order_label: str = "[" + complete_candle_acc.color + "_VOLUME][" + str(trend) + "][SHORT][SIZE(" + str(
                    len(candlesarray)) + ")][v" + str(stoploss_factor) + "]" + order_label \
                    + "[TRAILING_{0}_{1}]".format(trailing_stop_percentage, trailing_stop_activation_percentage) \
                    + ("[VOL_AVG]" if volume_avg is not None else "")
                moby_order = MobyOrder(
                    ticker=current_candle.ticker,
                    order_price=current_candle.close_price,
                    quantity=15.0,
                    stop_loss=complete_candle_acc.high_price * (1 + stoploss_factor),
                    position=OrderPosition.Short,
                    order_label=order_label,
                    leverage=2,
                    trailing_stop=trailing_stop_percentage,
                    trailing_stop_activation_percent=trailing_stop_activation_percentage)

        if moby_order is not None:

            # Entrada en real #
            if real_order:
                try:
                    self.__binance_order.open_position(moby_order)
                except Exception as e:
                    logging.exception(repr(e))

            # Simulación
            self.__order_simulator.open_position_simulation(moby_order)

    def load_candles(self, minutes=60):
        """Carga en BQ la información de candlsticks minutes hacia atrás"""

        for coin in self.coins_to_analyze:
            logging.info("Loading " + coin)
            candles_array = self.__binance_client.get_last_candlesticks(coin, minutes)

            bq_array = []
            for candle in candles_array:
                bq_array.append(candle.__dict__)

            if len(bq_array) > 0:
                df = pd.DataFrame(bq_array)
                job_config = bigquery.LoadJobConfig(write_disposition="WRITE_TRUNCATE")  # Replace table
                job = self.__bq_client.load_table_from_dataframe(
                    df, "fender-310315.MobyDick.CandlesticksHistory", job_config=job_config
                )
                job.result()  # Wait for the job to complete
                if job.errors:
                    for e in job.errors:
                        logging.error('BQ ERROR: {}'.format(e['message']))


# Local Testing
if __name__ == "__main__":
    import time

    market_analyzer = Analyzer()
    market_analyzer.check_integrity("IOTAUSDT")
    market_analyzer.check_integrity("XRPUSDT")
    market_analyzer.check_integrity("DOGEUSDT")

    while True:
        start = datetime.utcnow()
        market_analyzer.dog_analyze()
        end = datetime.utcnow()
        time_elapsed = (end - start).total_seconds()
        print("Finished in", time_elapsed, "seconds. Sleep...")
        time.sleep(60 - time_elapsed if time_elapsed < 60 else 59)
