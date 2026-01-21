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

from core.account.account_manager import AccountManager, Account
from core.alerts.telegram import Telegram
from core.backtesting.backtesting import Backtesting
from core.candles.candlestick import Candlestick
from core.market.technical_indicators import TechnicalIndicators
from core.candles.binance_client import BinanceClient
from core.order.binance_order import BinanceOrder
from core.order.moby_order import MobyOrder, OrderPosition
import numpy as np

from core.utils.datastoreclient import DataStoreClient


class AnalyzerJ3_1m:
    """Market Analyzer - This class looks for signals in the market"""

    def __init__(self, real_orders=False):
        """Default constructor"""
        self.coins_volume_rank = dict()
        if real_orders:
            self.__binance_client: BinanceClient = BinanceClient(account="binance2")
            self.__binance_order: BinanceOrder = BinanceOrder(account="binance2")
            self.__account_manager: AccountManager = AccountManager(account="binance2")
            self.__ds_cli: DataStoreClient = DataStoreClient()
            self.__telegram: Telegram = Telegram()
        else:
            self.__binance_client: BinanceClient = BinanceClient()
            self.__binance_order: BinanceOrder = None
            self.__account_manager: AccountManager = None
            self.__ds_cli: DataStoreClient = None
            self.__telegram: Telegram = None

        self.__technical_indicators = TechnicalIndicators()

        # All available coins: [key for key in self.__binance_client.get_all_available_symbols()]
        self.coins_to_analyze = [key for key in self.__binance_client.get_all_available_symbols()]#["ETHUSDT", "IOTAUSDT", "BTCUSDT", "LUNAUSDT", "XRPUSDT", "EOSUSDT", "DOTUSDT", "SOLUSDT", "MATICUSDT", "TRXUSDT"]
        self.interval = "1m"
        self.num_candles_to_iterate = 1441
        self.candle_index_to_start_backtest = self.num_candles_to_iterate
        self.order_label = "[J3-1m-v0.2.00]"

        # Variables
        self.var1 = 99
        self.var2 = 1

    def prepare_candles(self, candles: List[Candlestick]):
        """Añade los indicadores tecnicos necesarios a cada vela"""
        self.__technical_indicators.generate_average_true_range(candles, 25)

    def analyze(self, candles: List[Candlestick], previous_moby_order: MobyOrder = None, external_index_candles: List[Candlestick] = None) -> MobyOrder:
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

    def red_volume_order(self, candles: List[Candlestick], previous_moby_order: MobyOrder = None, btc_candles: List[Candlestick] = None):

        look_back_window = 20

        #0. BTC Filter
        if btc_candles is not None:
            btc_low_price = self.search_outliers("low_price", 0.9, 5, btc_candles[-1440:])
            if btc_low_price is not None:
                return None

        #1.- High Volume
        high_volume_info = self.search_outliers("quote_asset_volume", 91, look_back_window, candles[-1440:])
        if high_volume_info is None:
            return None

        high_volume_candle: Candlestick = high_volume_info["candle"]
        high_volume_candle_index: int = high_volume_info["candle_index"]
        high_volume_candle_acc: Candlestick = high_volume_info["candle_acc"]

        # 1.- High Volume
        if high_volume_candle.color == high_volume_candle_acc.color == "RED": # and high_volume_candle_index > -5:

            #2.- Pre volume situation
            pre_volume_candles_acc = self.cummulate_candles(candles[high_volume_candle_index - 200:high_volume_candle_index])
            if pre_volume_candles_acc.price_inc_percent > -5:
                return None

            #3.- Low Price Situation
            low_price_info = self.search_outliers("low_price", 1, look_back_window, candles[-720:], greater_than=False)
            if low_price_info is None:
                return None

            low_price_index = low_price_info["candle_index"]
            if low_price_index == high_volume_candle_index:
                price_info = high_volume_candle
            else:
                price_info = self.cummulate_candles(candles[min(low_price_index, high_volume_candle_index):max(low_price_index, high_volume_candle_index)])

            #4. Determinar si ya tocó 'fondo'
            if price_info.color == "GREEN" or price_info.price_inc >= candles[-look_back_window-1].technical_indicators.atr * -1:
                return None

            #5. Comienza ya la subida
            post_low_price_info_acc = self.cummulate_candles(candles[low_price_index:])
            if post_low_price_info_acc.price_inc_percent > 0:
                current_candle: Candlestick = candles[-1]
                previous_candle: Candlestick = candles[-2]
                previous_candle_atr = previous_candle.technical_indicators.atr

                take_profit = current_candle.close_price + previous_candle_atr * 17
                stop_loss = current_candle.close_price - previous_candle_atr * 10

                # 6. Filtro SL
                if stop_loss < 0 or stop_loss / current_candle.close_price - 1 < -0.7:
                    print("Ajuste Stoploss a -0.7")
                    stop_loss = current_candle.close_price * (1 - 0.7)

                moby_order = MobyOrder(
                    ticker=current_candle.ticker,
                    order_price=current_candle.close_price,
                    quantity=3,
                    position=OrderPosition.Long,
                    order_label=self.order_label,
                    leverage=15,
                    take_profit_price=take_profit,
                    stop_loss=stop_loss
                )
                return moby_order

        return None

    def get_position_mode(self, account_info: Account, moby_config):
        """#2. Si tenemos 5 o más ordenes cerradas con profit < 0: ---> Activar modo 'ApuestaMinima_1'
        #3. Si tenemos 3 o más órdenes cerradas con profit > 0: ---> Activar modo 'ApuestaNormal_2'"""

        if moby_config is None:
            raise Exception("Configuración necesaria... se para la ejecución")

        if account_info is None:
            raise Exception("Información de cuenta necesaria... se para la ejecución")

        down_price_streak_count = 0
        up_price_streak_count = 0
        new_mode = None

        closed_positions = sorted(account_info.closed_positions, key=lambda order: order.close_time, reverse=True)

        # Orders Streak
        if len(closed_positions) >= 3:
            for order in closed_positions:
                if order.open_price < order.close_price:
                    up_price_streak_count += 1
                    down_price_streak_count = 0
                    if up_price_streak_count == 3:
                        new_mode = "LONG"
                        break
                else:
                    down_price_streak_count += 1
                    up_price_streak_count = 0
                    if down_price_streak_count == 5:
                        new_mode = "SHORT"
                        break

        if new_mode is not None and new_mode != moby_config["PositionMode"]:
            moby_config["PositionMode"] = new_mode
            moby_config["Updated"] = datetime.utcnow()
            self.__ds_cli.update_entity(moby_config)
            self.__telegram.send_message_to_group_2("Position Mode Updated: " + new_mode)

        return moby_config["PositionMode"]

    def real_orders(self):

        account_info: Account = self.__account_manager.get_account_info(with_opened_positions_prices=False)
        moby_config = self.__ds_cli.get_entity("AnalyzerJ3Config", "1m_config")
        coin_count = 0

        # 1. Determinar Modo
        moby_mode = self.get_position_mode(account_info, moby_config)

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

                # Configuración Orden
                if moby_order is not None:
                    moby_order.quantity = 3
                    moby_order.leverage = 5

                    if moby_mode == "LONG":
                        moby_order.position = OrderPosition.Long

                    else:
                        moby_order.position = OrderPosition.Short
                        sl = moby_order.take_profit_price
                        tp = moby_order.stop_loss
                        moby_order.take_profit_price = tp
                        moby_order.stop_loss = sl

                    logging.warning("ENTRADA REAL" + moby_order.order_label + ": " + coin + " MODE: " + moby_mode)
                    self.__binance_order.open_position(moby_order)

                coin_count += 1

                if coin_count % 50 == 0:
                    account_info: Account = self.__account_manager.get_account_info(with_opened_positions_prices=False)
                    moby_config = self.__ds_cli.get_entity("AnalyzerJ3Config", "1m_config")
                    moby_mode = self.get_position_mode(account_info, moby_config)

            except Exception as e:
                msg = "Error procesando " + coin + ". " + repr(e)
                if "Invalid symbol" in msg:
                    msg += ". Quitamos esta moneda del listado"
                    self.coins_to_analyze.remove(coin)
                logging.exception(msg)


# Local Testing
if __name__ == "__main__":
    import os

    backtesting = Backtesting()
    backtest_analyzers = list()
    backtest_start_time = datetime(2021, 11, 30)

    # vars1 = [0.25, 0.5]
    # vars2 = [-5]
    #
    # for var1 in vars1:
    #     for var2 in vars2:
    #         backtest_analyzer = AnalyzerJ3_1m()
    #         backtest_analyzer.var1 = var1
    #         backtest_analyzer.var2 = var2
    #         backtest_analyzer.order_label += " ALL CANDLES: VAR1(ATR) " + str(var1) + " VAR2(PRE-VOL) " + str(var2)
    #         backtest_analyzers.append(backtest_analyzer)
    #         print(backtest_analyzer.order_label + " CREATED")
    #
    # start = datetime.utcnow()
    # Backtesting.compare_backtests(backtest_analyzers, backtest_start_time, processes=None)
    # end = datetime.utcnow()

    # pares = [
    #     [10, -2],       #Score
    #     [200, -4],      #Profit
    #     [100, -10]      #DD
    # ]
    #
    # for par in pares:
    #     backtest_analyzer = AnalyzerJ3_1m()
    #     backtest_analyzer.var1 = par[0]
    #     backtest_analyzer.var2 = par[1]
    #     backtest_analyzer.order_label += " PREVOLUME VARIATION - ALL COINS: VAR1 " + str(par[0]) + " VAR2 " + str(par[1])
    #     backtest_analyzers.append(backtest_analyzer)
    #     print(backtest_analyzer.order_label + " CREATED")
    #
    # start = datetime.utcnow()
    # Backtesting.compare_backtests(backtest_analyzers, backtest_start_time, processes=14)
    # end = datetime.utcnow()

    start = datetime.utcnow()
    Backtesting().backtest(AnalyzerJ3_1m(), backtest_start_time, processes=10)
    end = datetime.utcnow()

    time_elapsed = (end - start).total_seconds()
    print()
    print("Finished in", time_elapsed, "seconds.")

    os.system('say "Test ready"')