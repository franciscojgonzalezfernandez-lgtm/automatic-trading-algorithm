# Copyright 2021 TradersOfTheUniverse S.A. All Rights Reserved.
#
# [Binance Order - This class orchestrate orders of Binance]
#
# Authors:
#   antoniojose.luqueocana@telefonica.com
#   joseluis.roblesurquiza@telefonica.com
#   franciscojavier.gonzalezfernandez1@telefonica.com
#
# Version: 0.1
#

import logging
import time
import pandas as pd
from datetime import datetime

from google.cloud import bigquery

from core.alerts.telegram import Telegram
from core.candles.binance_client import BinanceClient
from core.order.moby_order import MobyOrder, OrderPosition, OrderMode, OrderStatus


class BinanceOrder:
    """Binance Order - This class orchestrate orders of Binance"""

    def __init__(self, account: str, testnet=False):
        """Default constructor"""

        self.__binance_client = BinanceClient(account, testnet)
        self.__telegram = Telegram()

        #self.__bq_client = bigquery.Client(project="fender-310315")
        #self.__bq_table_moby_order = self.__bq_client.get_table(self.__bq_client.dataset("MobyDick").table("MobyOrderReal"))

        self.__all_symbols = self.__binance_client.get_all_available_symbols()

    def open_position(self, moby_order: MobyOrder):
        """Open a position over Binance Futures and protect if with Stoploss and Trailing Stop"""

        is_long = (moby_order.position == OrderPosition.Long)
        stoploss_price = moby_order.stop_loss

        if moby_order.trailing_stop_activation_price is not None and moby_order.trailing_stop_activation_percent is not None:
            raise Exception("Configuración trailing erronea")

        if moby_order.take_profit_price is not None and moby_order.take_profit_percent is not None:
            raise Exception("Configuración take profit erronea")

        if moby_order.trailing_stop is None and moby_order.trailing_stop_activation_percent is not None:
            raise Exception("Configuración trailing stop erronea, trailing_stop is None ")

        if self.__is_wrong_stoploss(moby_order.order_price, stoploss_price, is_long):
            raise Exception("Order Error: El precio de stoploss es inválido, saltaría de inmediado")

        if moby_order.take_profit_percent is not None and moby_order.take_profit_percent <= 0:
            raise Exception("Order Error: Take profit debe ser positivo")

        # Esta comprobación mejor la última, pues conlleva una llamada al API
        if self.__binance_client.is_position_open(moby_order.ticker):
            logging.warning("Ya hay una ordern abierta sobre " + moby_order.ticker)
            return

        # 1. Registro en BQ
        moby_order.status = OrderStatus.Created
        moby_order.order_mode = OrderMode.Real
        self.__send_order_to_bq(moby_order)

        # 2. Borramos las anteriores ordenes de cierre residuales
        self.__binance_client.delete_orders_of(moby_order.ticker)

        quantity_precision = self.__all_symbols[moby_order.ticker]["quantityPrecision"]
        price_precision = self.__all_symbols[moby_order.ticker]["pricePrecision"]

        quantity_to_buy = (moby_order.quantity / moby_order.order_price) * moby_order.leverage
        quantity_to_buy = round(quantity_to_buy, quantity_precision)

        # 3. Establecemos el apalancamiento
        self.__binance_client.set_leverage(moby_order.ticker, moby_order.leverage)

        # 4. Abrimos posicion
        try:
            entry = self.__binance_client.make_market_order(moby_order.ticker, quantity_to_buy, is_long)
        except Exception as e:
            self.__telegram.send_message_to_group_1(moby_order.ticker + " Error abriendo posicion: " + repr(e))
            raise e
        real_entry_price = float(entry["avgPrice"])

        # 5. Esperamos un poco antes de lanzar los stops
        time.sleep(1)

        # 6. Trailing Stop Price
        if moby_order.trailing_stop_activation_price is None and moby_order.trailing_stop_activation_percent is not None:
            moby_order.trailing_stop_activation_price = self.get_trailing_stop_activation_price(real_entry_price, moby_order.trailing_stop_activation_percent, is_long, price_precision)

        elif moby_order.trailing_stop_activation_price is not None:
            moby_order.trailing_stop_activation_price = round(moby_order.trailing_stop_activation_price, price_precision)

        # 7. Stoploss Price
        stoploss_price = round(stoploss_price, price_precision)

        # 9. Orders
        try:
            if self.__is_wrong_stoploss(real_entry_price, stoploss_price, is_long):
                raise Exception("[" + moby_order.ticker +
                                "] Order Error: El precio de stoploss es inválido, saltaría de inmediado")

            # 9.1 Stoploss

            self.__binance_client.make_stoploss_order(moby_order.ticker, stoploss_price, is_long)

            # 9.2 Take Profit
            if moby_order.take_profit_percent is not None:
                moby_order.take_profit_price = self.get_roe_take_profit_price(real_entry_price, moby_order, price_precision)
                self.__binance_client.make_take_profit_order(moby_order.ticker, moby_order.take_profit_price, is_long)

            elif moby_order.take_profit_price is not None:
                moby_order.take_profit_price = round(moby_order.take_profit_price, price_precision)
                self.__binance_client.make_take_profit_order(moby_order.ticker, moby_order.take_profit_price, is_long)

            # 9.3 Trailing Stop
            elif moby_order.trailing_stop is not None and moby_order.trailing_stop_activation_price is not None:
                self.__binance_client.make_trailing_stop_order(moby_order.ticker,
                                                               moby_order.trailing_stop,
                                                               moby_order.trailing_stop_activation_price,
                                                               quantity_to_buy,
                                                               is_long)
            else:
                raise Exception("Compra sin control, ni TP ni Trailing")

        except Exception as e:
            # Podria darse si los valores stoploss_price o trailing_stop_activation_price no fueran validos
            msg = moby_order.ticker + " Error abriendo ordenes de cierre, ativamos trailling stop inmediato: " + repr(e)
            logging.exception(msg)
            self.__binance_client.make_trailing_stop_order(moby_order.ticker, 0.2, None, quantity_to_buy, is_long)
            self.__telegram.send_message_to_group_1(msg)

        # Registro en BQ
        moby_order.stop_loss = stoploss_price
        moby_order.status = OrderStatus.Open
        moby_order.open_time = datetime.utcnow()
        moby_order.open_price = real_entry_price
        self.__send_order_to_bq(moby_order)

        # Notificar Telegram
        message = ("<strong>{0} ENTRADA REAL</strong>".format(moby_order.ticker) +
                   "\nPosition: " + str(moby_order.position) +
                   "\nOrder Price: " + str(round(moby_order.order_price, 4)) +
                   "\nOpen Price: " + str(round(moby_order.open_price, 4)) +
                   "\nStoploss: " + str(round(moby_order.stop_loss, 4)) +
                   "\nTrailing Act Price: " + str(round(moby_order.trailing_stop_activation_price, 4) if moby_order.trailing_stop_activation_price is not None else "N/A") +
                   "\nTake Profit: " + str(round(moby_order.take_profit_price, 4) if moby_order.take_profit_price is not None else "N/A") +
                   "\nTime: " + str(moby_order.open_time) +
                   "\nPot ROE: " + str(round(moby_order.get_ROE(), 4) if moby_order.get_ROE() is not None else "N/A") +
                   "\nLabel: " + moby_order.order_label
                   )
        self.__telegram.send_message_to_group_1(message)

        return moby_order

    def get_trailing_stop_activation_price(self, real_entry_price: float, trailing_stop_activation_percent: float, is_long:bool, price_precision: int):

        # None is valid: immediate activation
        if trailing_stop_activation_percent is None:
            return None

        if is_long:
            # Break Even + %
            trailing_stop_activation_price = real_entry_price * (100 + trailing_stop_activation_percent) / 100
        else:
            # Break Even - %
            trailing_stop_activation_price = real_entry_price * (100 - trailing_stop_activation_percent) / 100

        return round(trailing_stop_activation_price, price_precision)

    def get_avaible_balance(self):
        """ Obtiene el balance restante para operar con la cuenta """
        return float(self.__binance_client.get_account_info()["availableBalance"])

    def __is_wrong_stoploss(self, entry_price, stoploss_price, is_long):
        """Return True if given stoploss is invalid"""
        if stoploss_price is None:
            return True
        if is_long and stoploss_price < entry_price:
            return False
        if (not is_long) and stoploss_price > entry_price:
            return False
        return True

    def __send_order_to_bq(self, moby_order: MobyOrder):
        """Almacena la orden en BQ"""
        # moby_order.update_metrics()
        # bq_array = [moby_order.__dict__]
        # df = pd.DataFrame(bq_array)
        # errors = self.__bq_client.insert_rows_from_dataframe(self.__bq_table_moby_order, df)
        # if errors:
        #     logging.error(errors)

    def get_roe_take_profit_price(self, real_entry_price: float, moby_order: MobyOrder, price_precision: int):
        is_long = moby_order.position == OrderPosition.Long

        percentage_with_leverage = moby_order.take_profit_percent / moby_order.leverage

        if is_long:
            # Break Even + (% / leverage)
            take_profit_price = real_entry_price * (100 + percentage_with_leverage) / 100
        else:
            # Break Even - (% / leverage)
            take_profit_price = real_entry_price * (100 - percentage_with_leverage) / 100

        return round(take_profit_price, price_precision)


# Local Testing
if __name__ == "__main__":

    binance_order = BinanceOrder(testnet=True)

    mock_moby_order = MobyOrder(
        ticker="BTCUSDT",
        order_price=40000,
        quantity=200.0,
        stop_loss=20000,
        position=OrderPosition.Long,
        order_label="[UNITTEST]",
        leverage=15,
        trailing_stop=0.4,
        trailing_stop_activation_percent=0.1,
        take_profit_percent=5
    )

    start = datetime.utcnow()
    binance_order.open_position(mock_moby_order)
    end = datetime.utcnow()
    time_elapsed = (end - start).total_seconds()
    print("Finished in", time_elapsed, "seconds.")
