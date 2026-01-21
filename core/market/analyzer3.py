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
from google.cloud import bigquery
import numpy as np

from core.market.technical_indicators import TechnicalIndicators, Trend
from core.candles.binance_client import BinanceClient
from core.candles.candlestick import Candlestick
from core.candles.candles_period import CandlesPeriod
from core.alerts.telegram import Telegram
from core.order.binance_order import BinanceOrder
from core.candles.binance_client import BinanceClient
from core.order.moby_order import MobyOrder, OrderPosition
from core.order.order_simulator import OrderSimulator
from core.utils.redisclient import RedisClient


class Analyzer3:
    """Market Analyzer - This class looks for signals in the market"""

    def __init__(self):
        """Default constructor"""

        # All available coins: [key for key in self.__binance_client.get_all_available_symbols()]
        self.interval = "1m"
        self.num_candles_to_iterate = 121
        self.candle_index_to_start_backtest = self.num_candles_to_iterate
        self.order_label = "[Percentil_Detector][v0.0.1]"
        #self.coins_to_analyze = [key for key in self.__binance_client.get_all_available_symbols()]


        self.__bq_client = bigquery.Client(project="fender-310315")
        self.__telegram = Telegram()
        self.__binance_client = BinanceClient()
        self.__order_simulator = OrderSimulator()
        self.__redis_client = RedisClient()
        self.__binance_order = BinanceOrder()
        self.__technical_indicators = TechnicalIndicators()



        self.coins_to_analyze = {
            "BTCUSDT": {"interval": "1m", "vol_inc_fact": 2},
            "ETHUSDT": {"interval": "1m", "vol_inc_fact": 2},
            "BCHUSDT": {"interval": "1m", "vol_inc_fact": 2},
            "BNBUSDT": {"interval": "1m", "vol_inc_fact": 4},
            "MATICUSDT": {"interval": "1m", "vol_inc_fact": 4},
            "1INCHUSDT": {"interval": "1m", "vol_inc_fact": 4},
            "ADAUSDT": {"interval": "1m", "vol_inc_fact": 4},
            "IOTAUSDT": {"interval": "1m", "vol_inc_fact": 4},
            "THETAUSDT": {"interval": "1m", "vol_inc_fact": 4},
            "DOGEUSDT": {"interval": "1m", "vol_inc_fact": 4},

            # "XRPUSDT": {"interval": "1m", "vol_inc_fact": 2},
            #"DOTUSDT": {"interval": "1m", "vol_inc_fact": 4},
            #"SOLUSDT": {"interval": "1m", "vol_inc_fact": 4},
            #"EOSUSDT": {"interval": "1m", "vol_inc_fact": 4},

            #"LTCUSDT": {"interval": "1m", "vol_inc_fact": 4},
            #"LUNAUSDT": {"interval": "1m", "vol_inc_fact": 4},
            #"VETUSDT": {"interval": "1m", "vol_inc_fact": 4},

            #"LINKUSDT": {"interval": "1m", "vol_inc_fact": 4},
            #"ONTUSDT": {"interval": "1m", "vol_inc_fact": 4},
            #"NEOUSDT": {"interval": "1m", "vol_inc_fact": 4},
            #"SUSHIUSDT": {"interval": "1m", "vol_inc_fact": 4},
            #"TRXUSDT": {"interval": "1m", "vol_inc_fact": 4}
        }

        self.__config = dict({
            "quantity": 5.0,
            "stop_loss_margin": 0.00,
            "real_positions": True,
            "leverage": 15,
            "trailing_stop": 0.2,
            "trailing_stop_activation": 0.1,
            "take_profit_percent": 4,  # 5%
            "label_suffix": "[v5]",
            "roe_filter": 6,
            "coeficient": 0.8, # Coeficiente por el que multiplicar para establecer stoploss y tp
        })

        self.__init_config()

    def __init_config(self):
        config = self.__redis_client.get_dict("ANALYZER3_CONFIG")
        if config is not None and len(config) > 0:
            self.__config = config

    def set_config(self, config):
        self.__redis_client.save_dict("ANALYZER3_CONFIG", config)
        return self.__redis_client.get_dict("ANALYZER3_CONFIG")

    def analyze_all(self):
        """Analiza todas las monedas para buscar entradas"""

        for coin in self.coins_to_analyze:
            print("Processing " + coin)
            try:
                candles = self.__binance_client.get_last_candlesticks(
                    coin=coin,
                    num_candlesticks=self.num_candles_to_iterate,
                    interval=self.interval
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
                logging.exception("Error procesando " + coin + ". " + repr(e))

    def prepare_candles(self, candles: List[Candlestick]):
        """Añade los indicadores tecnicos necesarios a cada vela"""
        self.__technical_indicators.generate_average_true_range(candles)

    def backtrack(self):

        for coin, config in self.coins_to_analyze.items():

            print("BACKTRACKING - FUTURES -" + coin)
            moby_order = self.backtrack_coin(coin, config, True)

            if moby_order is None:
                print("BACKTRACKING - SPOT -" + coin)
                self.backtrack_coin(coin, config, False)

            self.check_breaks(coin)

    def analyze(self, candles: List[Candlestick]):
        moby_order = None

        current_candle = candles[-1]
        complete_candles: List[Candlestick] = candles[:-1]
        global_candles_period: CandlesPeriod = CandlesPeriod(complete_candles)
        high_volume_info = self.search_high_volume(complete_candles, global_candles_period,
                                                   color="GREEN", volume_factor_inc=self.__config["vol_inc_fact"])
        ###### Green Volume ######
        high_volume_info = self.search_high_volume(complete_candles, global_candles_period,
                                                   color="GREEN", volume_factor_inc=self.__config["vol_inc_fact"])
        if high_volume_info is not None and high_volume_info["volume_index"] >= -4:

            if high_volume_info["volume_index"] < -1:
                post_volume_min_price = min(
                    [candle.low_price for candle in complete_candles[high_volume_info["volume_index"] + 1:]])
            else:
                post_volume_min_price = current_candle.close_price

            stoploss = current_candle.close_price + high_volume_info["volume_candle"].shadow
            take_profit = current_candle.close_price - high_volume_info["volume_candle"].shadow

            # DETERMINAR SI VAMOS TARDE
            if current_candle.close_price > high_volume_info["volume_candle"].low_price and \
                    current_candle.low_price > high_volume_info["volume_candle"].low_price and \
                    current_candle.low_price > take_profit and current_candle.close_price > take_profit and \
                    take_profit < post_volume_min_price:

                moby_order = MobyOrder(
                    ticker=current_candle.ticker,
                    order_price=current_candle.close_price,
                    quantity=self.__config["quantity"],
                    stop_loss=stoploss,
                    position=OrderPosition.Short,
                    leverage=self.__config["leverage"],
                    trailing_stop=None,
                    take_profit_price=take_profit)

                roe = moby_order.get_ROE()

                if roe < self.__config["roe_filter"]:
                    print("Pass, low ROE: " + str(roe))

                else:
                    return moby_order

            # Hacemos ordenes sin filtrar
            moby_order_no_filter = MobyOrder(
                ticker=current_candle.ticker,
                order_price=current_candle.close_price,
                quantity=self.__config["quantity"],
                stop_loss=stoploss,
                position=OrderPosition.Short,
                leverage=self.__config["leverage"],
                trailing_stop=None,
                take_profit_price=take_profit)
            roe = moby_order_no_filter.get_ROE()

            if roe > self.__config["roe_filter"]:
                self.__order_simulator.open_position_simulation(moby_order_no_filter)
                adx_trend = self.__technical_indicators.get_trend(coin, num_periods_sma=30, interval="1m")
                # Vamos a evitar entrar en contratendencia dictada por adx y los periodos de arriba.
                if adx_trend != Trend.UPTREND:
                    moby_order_no_filter.order_label = "[Percentil_Detector_TREND_30P_1m]" + label_info + self.__config[
                        "label_suffix"]
                    self.__order_simulator.open_position_simulation(moby_order_no_filter)

        ###### Red Volume ######
        high_volume_info = self.search_high_volume(complete_candles, global_candles_period,
                                                   color="RED", volume_factor_inc=self.__config["vol_inc_fact"])
        if high_volume_info is not None and high_volume_info["volume_index"] >= -4:

            if high_volume_info["volume_index"] < -1:
                post_volume_max_price = max(
                    [candle.high_price for candle in complete_candles[high_volume_info["volume_index"] + 1:]])
            else:
                post_volume_max_price = current_candle.close_price

            stoploss = current_candle.close_price - high_volume_info["volume_candle"].shadow
            take_profit = current_candle.close_price + high_volume_info["volume_candle"].shadow

            # Determinar si vamos tarde
            if current_candle.close_price < high_volume_info["volume_candle"].high_price and \
                    current_candle.high_price < high_volume_info["volume_candle"].high_price and \
                    current_candle.close_price < take_profit and current_candle.high_price < take_profit and \
                    take_profit > post_volume_max_price:

                moby_order = MobyOrder(
                    ticker=current_candle.ticker,
                    order_price=current_candle.close_price,
                    quantity=self.__config["quantity"],
                    stop_loss=stoploss,
                    position=OrderPosition.Long,
                    leverage=self.__config["leverage"],
                    trailing_stop=None,
                    take_profit_price=take_profit)

                roe = moby_order.get_ROE()

                if roe < self.__config["roe_filter"]:
                    print("Pass, low ROE: " + str(roe))

                else:
                    return moby_order


            # Hacemos órdenes sin filtrar
            moby_order_no_filter = MobyOrder(
                ticker=coin,
                order_price=current_candle.close_price,
                quantity=self.__config["quantity"],
                stop_loss=stoploss,
                position=OrderPosition.Long,
                leverage=self.__config["leverage"],
                trailing_stop=None,
                take_profit_price=take_profit)
            roe = moby_order_no_filter.get_ROE()

            if roe > self.__config["roe_filter"]:
                self.__order_simulator.open_position_simulation(moby_order_no_filter)
                adx_trend = self.__technical_indicators.get_trend(coin, num_periods_sma=30, interval="1m")
                if adx_trend != Trend.DOWNTREND:
                   return moby_order

    def check_breaks(self, coin):
        pass

    def backtrack_coin(self, coin, config, use_futures_info: bool = False):

        candles: List[Candlestick] = self.__binance_client.get_last_candlesticks(coin=coin,
                                                                                 num_candlesticks=501,
                                                                                 interval=config["interval"],
                                                                                 futures_info=use_futures_info)
        candles.sort(key=lambda x: x.open_time)
        label_info = "[FUTURES_INFO]" if use_futures_info else "[SPOT_INFO]"
        current_candle = candles[-1]
        complete_candles:List[Candlestick] = candles[:-1]
        global_candles_period: CandlesPeriod = CandlesPeriod(complete_candles)

        ###### Green Volume ######
        high_volume_info = self.search_high_volume(complete_candles, global_candles_period,
                                                   color="GREEN", volume_factor_inc=config["vol_inc_fact"])
        if high_volume_info is not None and high_volume_info["volume_index"] >= -4:

            if high_volume_info["volume_index"] < -1:
                post_volume_min_price = min([candle.low_price for candle in complete_candles[high_volume_info["volume_index"]+1:]])
            else:
                post_volume_min_price = current_candle.close_price

            stoploss = current_candle.close_price + high_volume_info["volume_candle"].shadow
            take_profit = current_candle.close_price - high_volume_info["volume_candle"].shadow

            #DETERMINAR SI VAMOS TARDE
            if current_candle.close_price > high_volume_info["volume_candle"].low_price and \
                    current_candle.low_price > high_volume_info["volume_candle"].low_price and \
                    current_candle.low_price > take_profit and current_candle.close_price > take_profit and \
                    take_profit < post_volume_min_price:

                moby_order = MobyOrder(
                    ticker=coin,
                    order_price=current_candle.close_price,
                    quantity=self.__config["quantity"],
                    stop_loss=stoploss,
                    position=OrderPosition.Short,
                    order_label="[Percentil_Detector]" + label_info + self.__config["label_suffix"],
                    leverage=self.__config["leverage"],
                    trailing_stop=None,
                    take_profit_price=take_profit)

                roe = moby_order.get_ROE()

                if roe < self.__config["roe_filter"]:
                    print("Pass, low ROE: " + str(roe))

                else:
                    try:
                        # Entrada en real #
                        if __name__ != "__main__":
                            # return self.__binance_order.open_position(moby_order)
                            # Simulamos sin tendencias!
                            self.__order_simulator.open_position_simulation(moby_order)


                    except Exception as e:
                        logging.exception("Error procesando " + coin + ". " + repr(e))

                    msg = "[ENTRADA][" + coin + "][" + moby_order.position.upper() + "]" \
                          "\nPosition Price " + str(moby_order.order_price) + \
                          "\nStop loss " + str(moby_order.stop_loss) + \
                          "\nTake profit " + str(moby_order.take_profit_price) + \
                          "\nTrailing Stop " + str(moby_order.trailing_stop) + \
                          "\nTrailing Stop Activation Price " + str(moby_order.trailing_stop_activation_price) + \
                          "\nPosition " + str(datetime.utcnow()) + \
                          "\nPot. ROE " + str(roe) + \
                          "\nVolume " + str(high_volume_info["volume_candle"].open_time) + \
                          "\nLabel " + moby_order.order_label +\
                          "\nVolume Index: " + str(high_volume_info["volume_index"])

                    print("4.-BUY " + msg)
                    if __name__ == "__main__":
                        self.__telegram.send_message_to_group_2(msg)

            # Hacemos ordenes sin filtrar
            moby_order_no_filter = MobyOrder(
                ticker=coin,
                order_price=current_candle.close_price,
                quantity=self.__config["quantity"],
                stop_loss=stoploss,
                position=OrderPosition.Short,
                order_label="[Percentil_Detector_UNFILTERED]" + label_info + self.__config["label_suffix"],
                leverage=self.__config["leverage"],
                trailing_stop=None,
                take_profit_price=take_profit)
            roe = moby_order_no_filter.get_ROE()

            if roe > self.__config["roe_filter"]:
                self.__order_simulator.open_position_simulation(moby_order_no_filter)
                adx_trend = self.__technical_indicators.get_trend(coin, num_periods_sma=30, interval="1m")
                #Vamos a evitar entrar en contratendencia dictada por adx y los periodos de arriba.
                if adx_trend != Trend.UPTREND:
                    moby_order_no_filter.order_label = "[Percentil_Detector_TREND_30P_1m]" + label_info + self.__config["label_suffix"]
                    self.__order_simulator.open_position_simulation(moby_order_no_filter)



        ###### Red Volume ######
        high_volume_info = self.search_high_volume(complete_candles, global_candles_period,
                                                   color="RED", volume_factor_inc=config["vol_inc_fact"])
        if high_volume_info is not None and high_volume_info["volume_index"] >= -4:

            if high_volume_info["volume_index"] < -1:
                post_volume_max_price = max([candle.high_price for candle in complete_candles[high_volume_info["volume_index"]+1:]])
            else:
                post_volume_max_price = current_candle.close_price

            stoploss = current_candle.close_price - high_volume_info["volume_candle"].shadow
            take_profit = current_candle.close_price + high_volume_info["volume_candle"].shadow


            # Determinar si vamos tarde
            if current_candle.close_price < high_volume_info["volume_candle"].high_price and \
                    current_candle.high_price < high_volume_info["volume_candle"].high_price and \
                    current_candle.close_price < take_profit and current_candle.high_price < take_profit and \
                    take_profit > post_volume_max_price:

                moby_order = MobyOrder(
                    ticker=coin,
                    order_price=current_candle.close_price,
                    quantity=self.__config["quantity"],
                    stop_loss=stoploss,
                    position=OrderPosition.Long,
                    order_label="[Percentil_Detector]" + label_info + self.__config["label_suffix"],
                    leverage=self.__config["leverage"],
                    trailing_stop=None,
                    take_profit_price=take_profit)

                roe = moby_order.get_ROE()

                if roe < self.__config["roe_filter"]:
                    print("Pass, low ROE: " + str(roe))

                else:
                    try:
                        # Entrada en real #
                        if __name__ != "__main__":
                            # return self.__binance_order.open_position(moby_order)
                            # Simulamos sin mezclar con tendencias!
                            self.__order_simulator.open_position_simulation(moby_order)


                    except Exception as e:
                        logging.exception("Error procesando " + coin + ". " + repr(e))

                    msg = "[ENTRADA][" + coin + "][" + moby_order.position.upper() + "]" \
                                                "\nPosition Price " + str(moby_order.order_price) + \
                          "\nStop loss " + str(moby_order.stop_loss) + \
                          "\nTake profit " + str(moby_order.take_profit_price) + \
                          "\nPosition " + str(datetime.utcnow()) + \
                          "\nTrailing Stop " + str(moby_order.trailing_stop) + \
                          "\nTrailing Stop Activation Price " + str(moby_order.trailing_stop_activation_price) + \
                          "\nPot. ROE " + str(roe) + \
                          "\nVolume: " + str(high_volume_info["volume_candle"].open_time) + \
                          "\nLabel: " + moby_order.order_label + \
                          "\nVolume Index: " + str(high_volume_info["volume_index"])

                    print("4.-BUY " + msg)
                    if __name__ == "__main__":
                        self.__telegram.send_message_to_group_2(msg)
            #Hacemos órdenes sin filtrar
            moby_order_no_filter = MobyOrder(
                ticker=coin,
                order_price=current_candle.close_price,
                quantity=self.__config["quantity"],
                stop_loss=stoploss,
                position=OrderPosition.Long,
                order_label="[Percentil_Detector_UNFILTERED]" + label_info + self.__config[
                    "label_suffix"],
                leverage=self.__config["leverage"],
                trailing_stop=None,
                take_profit_price=take_profit)
            roe = moby_order_no_filter.get_ROE()

            if roe > self.__config["roe_filter"]:
                self.__order_simulator.open_position_simulation(moby_order_no_filter)
                adx_trend = self.__technical_indicators.get_trend(coin, num_periods_sma=30, interval="1m")
                if adx_trend != Trend.DOWNTREND:
                    moby_order_no_filter.order_label = "[Percentil_Detector_TREND_30P_1m]" + label_info + self.__config[
                        "label_suffix"]
                    self.__order_simulator.open_position_simulation(moby_order_no_filter)

        return None

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

    def search_high_volume(self, complete_candles, global_candles_period, color, volume_factor_inc):

        volume_range = range(1, 10)

        for i in volume_range:

            local_candles_period_range = [(-i - 2), -i]
            local_candles_period: CandlesPeriod = CandlesPeriod(
                complete_candles[local_candles_period_range[0]:local_candles_period_range[1]])

            if local_candles_period.volume_inc_factor is not None and local_candles_period.volume_inc_factor > volume_factor_inc \
                    and local_candles_period.second_part_candle_acc.volume > self.get_volume_percentile(
                complete_candles[-self.num_candles_to_iterate:],
                98) and local_candles_period.second_part_candle_acc.volume > global_candles_period.volume_avg:

                print("VOL - Percentile OK")
                volume_info = {
                    "volume_period": local_candles_period,
                    "volume_index": local_candles_period_range[0] + 1,
                    "volume_candle": complete_candles[local_candles_period_range[0] + 1]
                }

                if volume_info["volume_candle"].shadow >= self.get_real_shadow_percentile(complete_candles[-60:], 96):

                    if volume_info["volume_candle"].color == color:
                        print("VOL- Volume inc factor found: " + str(local_candles_period.volume_inc_factor))
                        print("VOL- Volume candle index: " + str(volume_info["volume_index"]))
                        print("VOL- Volume candle time: " + str(volume_info["volume_candle"].open_time))
                        print("VOL- Volume candle time: " + str(volume_info["volume_candle"].color))
                        if volume_info["volume_candle"].color == "RED":
                            print("VOL- Potencial subida hasta: " + str(volume_info["volume_candle"].open_price))
                        else:
                            print("VOL- Potencial bajada hasta: " + str(volume_info["volume_candle"].open_price))

                        # Comprobación Integridad
                        if volume_info["volume_candle"].open_time != volume_info[
                            "volume_period"].second_part_candle_acc.open_time or volume_info["volume_candle"].volume != \
                                volume_info["volume_period"].second_part_candle_acc.volume:
                            raise Exception("Error de integridad de indices de volumen")

                        return volume_info
                else:
                    print("VOL - Small shadow")
        return None

    def resistance_breach(self, current_candle: Candlestick, candles: List[Candlestick]):
        """Determina si la vela actual 'current_candle' está rompiendo alguna resistencia de la lista 'candles'."""

        candles_resistance = candles[:]
        candles_resistance.sort(key=lambda x: x.high_price, reverse=True)
        for i in range(0, len(candles_resistance)):
            if current_candle.high_price >= candles_resistance[i].high_price:
                resistance_breach = {
                    "resistance_candle": candles_resistance[i],
                    "resistance_index": i
                }
                return resistance_breach
        return None

    def support_breach(self, current_candle: Candlestick, candles: List[Candlestick]):
        """Determina si la vela actual 'current_candle' está rompiendo algún soporte de la lista 'candles'."""

        candles_support = candles[:]
        candles_support.sort(key=lambda x: x.low_price)
        for i in range(0, len(candles_support)):
            if current_candle.low_price <= candles_support[i].low_price:
                support_breach = {
                    "support_candle": candles_support[i],
                    "support_index": i
                }
                return support_breach
        return None

    def check_integrity(self, coin):
        candles_1min = self.__binance_client.get_last_candlesticks(coin=coin,
                                                                   num_candlesticks=61 + datetime.utcnow().minute % 30,
                                                                   interval="1m")
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


# Local Run
if __name__ == "__main__":
    import time

    market_analyzer = Analyzer3()
    # market_analyzer.check_integrity("IOTAUSDT")
    # market_analyzer.check_integrity("XRPUSDT")
    # market_analyzer.check_integrity("DOGEUSDT")

    while True:
        start = datetime.utcnow()
        market_analyzer.backtrack()
        end = datetime.utcnow()
        time_elapsed = (end - start).total_seconds()
        print("Finished in", time_elapsed, "seconds. Sleep...")
        time.sleep(60 - time_elapsed if time_elapsed < 60 else 59)
