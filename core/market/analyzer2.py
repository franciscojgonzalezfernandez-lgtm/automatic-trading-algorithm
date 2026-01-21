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

from datetime import datetime, timedelta
from typing import List
from google.cloud import bigquery
import numpy as np

from core.candles.binance_client import BinanceClient
from core.candles.candlestick import Candlestick
from core.candles.candles_period import CandlesPeriod
from core.alerts.telegram import Telegram
from core.order.binance_order import BinanceOrder
from core.order.moby_order import MobyOrder, OrderPosition
from core.order.order_simulator import OrderSimulator

from core.utils.utils import unix_time_to_datetime_utc, datetime_utc_to_madrid, datetime_utc_to_unix_time


class Analyzer2:
    """Market Analyzer - This class looks for signals in the market
    ###### 705 RED VOLUME ######
    #stoploss = min(min_price, current_candle.low_price)
    #take_profit = current_candle.close_price + high_volume_info["volume_candle"].shadow

    ###### 705 GREEN VOLUME ######
    #stoploss = max(max_price, current_candle.high_price)
    #take_profit = current_candle.close_price - high_volume_info["volume_candle"].shadow

    """

    def __init__(self):
        """Default constructor"""

        self.__bq_client = bigquery.Client(project="fender-310315")
        self.__telegram = Telegram()
        self.__binance_client = BinanceClient()
        self.__binance_order = BinanceOrder()
        self.__order_simulator = OrderSimulator()

        self.coins_to_analyze = {
            "BTCUSDT":   {"interval": "1m", "vol_inc_fact": 4, "real_order": False},
            "ETHUSDT":   {"interval": "1m", "vol_inc_fact": 4, "real_order": False},
            "BCHUSDT":   {"interval": "1m", "vol_inc_fact": 4, "real_order": False},
            "BNBUSDT":   {"interval": "1m", "vol_inc_fact": 4, "real_order": False},
            "MATICUSDT": {"interval": "1m", "vol_inc_fact": 4, "real_order": False},
            "1INCHUSDT": {"interval": "1m", "vol_inc_fact": 4, "real_order": False},
            "ADAUSDT":   {"interval": "1m", "vol_inc_fact": 4, "real_order": False},
            "IOTAUSDT":  {"interval": "1m", "vol_inc_fact": 4, "real_order": False},
            "THETAUSDT": {"interval": "1m", "vol_inc_fact": 4, "real_order": False},
            "DOGEUSDT":  {"interval": "1m", "vol_inc_fact": 4, "real_order": False},

            "XRPUSDT":   {"interval": "1m", "vol_inc_fact": 4, "real_order": False},
            "NEOUSDT":   {"interval": "1m", "vol_inc_fact": 4, "real_order": False},
            "DOTUSDT":   {"interval": "1m", "vol_inc_fact": 4, "real_order": False},
            "SOLUSDT":   {"interval": "1m", "vol_inc_fact": 4, "real_order": False},
            "EOSUSDT":   {"interval": "1m", "vol_inc_fact": 4, "real_order": False},

            "LTCUSDT":   {"interval": "1m", "vol_inc_fact": 4, "real_order": False},
            "LUNAUSDT":  {"interval": "1m", "vol_inc_fact": 4, "real_order": False},
            "VETUSDT":   {"interval": "1m", "vol_inc_fact": 4, "real_order": False},

            "LINKUSDT":  {"interval": "1m", "vol_inc_fact": 4, "real_order": False},
            "ONTUSDT":   {"interval": "1m", "vol_inc_fact": 4, "real_order": False},
            "SUSHIUSDT": {"interval": "1m", "vol_inc_fact": 4, "real_order": False},
            "TRXUSDT":   {"interval": "1m", "vol_inc_fact": 4, "real_order": False}
        }

        self.__config = dict({
            "quantity": 5.0,
            "stop_loss_margin": 0.0,
            "real_positions": True,
            "leverage": 15,
            "trailing_stop": 0.2,
            "trailing_stop_activation": 0.1,
            "take_profit_percent": 4,  # 5%
            "label_suffix": "[v4.743]"
        })

    def backtrack(self):

        interval = "1m"
        for coin, config in self.coins_to_analyze.items():
            print("BACKTRACKING FUTURES " + coin + " " + interval)
            moby_order = self.backtrack_coin(
                coin=coin,
                interval=interval,
                use_futures_info=True,
                real_order=config["real_order"])

            print("BACKTRACKING SPOT " + coin + " " + interval)
            moby_order = self.backtrack_coin(
                coin=coin,
                interval=interval,
                use_futures_info=False,
                real_order=False)

    def determine_high_volume(self, complete_candles, global_candles_period):
        high_volume_info_green = self.search_high_volume(complete_candles, global_candles_period, color="GREEN")
        high_volume_info_red = self.search_high_volume(complete_candles, global_candles_period, color="RED")

        if high_volume_info_red is None and high_volume_info_green is None:
            return None

        if high_volume_info_green is None and high_volume_info_red is not None:
            high_volume_info_red["color"] = "RED"
            return high_volume_info_red

        if high_volume_info_red is None and high_volume_info_green is not None:
            high_volume_info_green["color"] = "GREEN"
            return high_volume_info_green

        if high_volume_info_red["volume"] > high_volume_info_green["volume"]:
            high_volume_info_red["color"] = "RED"
            return high_volume_info_red
        else:
            high_volume_info_green["color"] = "GREEN"
            return high_volume_info_green

    def backtrack_coin(self, coin, interval, use_futures_info: bool = False, real_order: bool = False):

        candles: List[Candlestick] = self.__binance_client.get_last_candlesticks(coin=coin,
                                                                                 num_candlesticks=499,
                                                                                 interval=interval,
                                                                                 futures_info=use_futures_info)
        candles.sort(key=lambda x: x.open_time)
        info = "[" + interval + "]" + ("[FUTURES_INFO]" if use_futures_info else "[SPOT_INFO]")
        current_candle = candles[-1]
        complete_candles: List[Candlestick] = candles[:-1]
        global_candles_period: CandlesPeriod = CandlesPeriod(complete_candles)
        high_volume_info = self.determine_high_volume(complete_candles, global_candles_period)
        moby_order = None

        ###### Green Volume ######
        if high_volume_info is not None and high_volume_info["volume_candle_index"] >= -5 and high_volume_info["color"] == "GREEN":

            pre_volume_max_price = max([candle.high_price for candle in complete_candles[high_volume_info["volume_candle_index"] - 4:high_volume_info["volume_candle_index"]]])

            if pre_volume_max_price > high_volume_info["volume_candle"].high_price:
                print("No ha habido subida, no cuenta...")
                return None

            if current_candle.color != "RED" and complete_candles[-1].color != "RED":
                print("Sigue subiendo")
                return None

            if high_volume_info["volume_candle_index"] < -1:
                post_volume_min_price = min([candle.low_price for candle in complete_candles[high_volume_info["volume_candle_index"]+1:]])
            else:
                post_volume_min_price = current_candle.low_price

            max_price = max([candle.high_price for candle in complete_candles[high_volume_info["volume_candle_index"]:]])
            stoploss = max(max_price, current_candle.high_price)
            take_profit = current_candle.close_price - high_volume_info["volume_candle"].real_body

            if current_candle.low_price > high_volume_info["volume_candle"].low_price and \
               current_candle.low_price > take_profit and \
               take_profit < post_volume_min_price:

                moby_order = MobyOrder(
                    ticker=coin,
                    order_price=current_candle.close_price,
                    quantity=self.__config["quantity"],
                    position=OrderPosition.Short,
                    order_label="[BT][SHORT]" + info + self.__config["label_suffix"],
                    leverage=self.__config["leverage"],
                    take_profit_price=take_profit,
                    stop_loss=stoploss * (1 + self.__config["stop_loss_margin"]))

                roe = moby_order.get_ROE()
                loss = moby_order.get_LOSS()
                print("ROE: " + str(roe))
                print("LOSS: " + str(loss))

                if roe < 2:
                    print("Pass, low ROE: " + str(roe))
                    return None
                if loss > 50:
                    print("Pass, high LOSS: " + str(loss))
                    return None

            else:
                print("Pasó el momento...")

        ###### Red Volume ######
        if high_volume_info is not None and high_volume_info["volume_candle_index"] >= -5 and high_volume_info["color"] == "RED":

            pre_volume_min_price = min([candle.low_price for candle in complete_candles[
                                                                       high_volume_info["volume_candle_index"] - 4:
                                                                       high_volume_info["volume_candle_index"]]])

            if pre_volume_min_price < high_volume_info["volume_candle"].low_price:
                print("No ha habido bajada, no cuenta...")
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
                    quantity=self.__config["quantity"],
                    position=OrderPosition.Long,
                    order_label="[BT][LONG]" + info + self.__config["label_suffix"],
                    leverage=self.__config["leverage"],
                    trailing_stop_activation_price=take_profit,
                    trailing_stop=0.2,
                    stop_loss=stoploss * (1 - self.__config["stop_loss_margin"]))

            else:
                print("Pasó el momento...")

        if moby_order is None:
            return None

        ###### HYPOTHESIS ######
        msg = "[ENTRADA][" + coin + "]" \
                  "\nPosition " + str(moby_order.position) + \
                  "\nPosition Price " + str(moby_order.order_price) + \
                  "\nStop loss " + str(moby_order.stop_loss) + \
                  "\nTrailing Stop " + str(moby_order.trailing_stop) + \
                  "\nTrailing Stop Activation Price " + str(moby_order.trailing_stop_activation_price) + \
                  "\nPot. ROE " + str(moby_order.get_ROE()) + \
                  "\nPot. LOSS " + str(moby_order.get_LOSS()) + \
                  "\nLabel " + moby_order.order_label + \
                  "\nInfo " + info + \
                  "\nVolume Index: " + str(high_volume_info["volume_candle_index"])

        print("4. HYPOTHESIS " + msg)


        ###### ENTRADA SIMULADA ######
        if __name__ != "__main__":
            # Entrada simulada #
            try:
                self.__order_simulator.open_position_simulation(moby_order)
            except Exception as e:
                logging.exception("Error simulación " + coin + ". " + repr(e))

        ###### ENTRADA REAL ######
        if __name__ != "__main__" and real_order is True and self.go_for_real(moby_order):
            try:
                moby_order = self.__binance_order.open_position(moby_order)
            except Exception as e:
                logging.exception("Error entrada real " + coin + ". " + repr(e))

        return moby_order

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

    def search_high_volume(self, complete_candles: List[Candlestick], global_candles_period: CandlesPeriod, color):

        volume_range = range(1, 10)
        volume_candle = None
        volume_candle_index = None
        shadow_candle = None
        shadow_candle_index = None
        volume_percentile_value = self.get_volume_percentile(complete_candles[-300:], 96)
        shadow_percentile_value = self.get_real_shadow_percentile(complete_candles[-300:], 95)

        for i in volume_range:

            complete_candle = complete_candles[-i]

            #Volume
            if complete_candle.volume > volume_percentile_value and complete_candle.color == color \
                    and complete_candle.volume > global_candles_period.volume_avg * 2\
                    and complete_candle.volume > complete_candles[-i-1].volume * 2:
                print("VOL - Percentile OK: " + complete_candle.ticker)
                if volume_candle is None or complete_candle.volume > volume_candle.volume:
                    volume_candle = complete_candle
                    volume_candle_index = -i

            #Shadow
            if complete_candle.shadow >= shadow_percentile_value and complete_candle.color == color \
                    and complete_candle.shadow > global_candles_period.shadow_avg * 2 \
                    and complete_candle.shadow > complete_candles[-i-1].shadow * 2:
                print("VOL - Shadow OK: " + complete_candle.ticker)
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

    def go_for_real(self, moby_order: MobyOrder):
        try:
            current_datetime = datetime.utcnow()
            profits = self.__order_simulator.get_profits_from_redis(moby_order.order_label,
                                                         int(datetime_utc_to_unix_time(current_datetime - timedelta(hours=1))),
                                                         int(datetime_utc_to_unix_time(current_datetime)))
            if profits is None or len(profits) < 2:
                return False

            for profit in profits[-2:]:
                if float(profit) <= 0:
                    return False
            return True

        except Exception as e:
            logging.exception("Error determinando si entrar en real" + repr(e))

        return False



# Local Run
if __name__ == "__main__":
    import time

    market_analyzer = Analyzer2()
    while True:
        start = datetime.utcnow()
        market_analyzer.backtrack()
        end = datetime.utcnow()
        time_elapsed = (end - start).total_seconds()
        print("Finished in", time_elapsed, "seconds. Sleep...")
        time.sleep(60 - time_elapsed if time_elapsed < 60 else 59)