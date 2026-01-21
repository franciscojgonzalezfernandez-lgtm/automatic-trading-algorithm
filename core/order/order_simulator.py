# Copyright 2021 TradersOfTheUniverse S.A. All Rights Reserved.
#
# [Order Simulator - This class simulates the tracking of orders for BQ]
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
import random
import pandas as pd

from copy import deepcopy
from datetime import datetime, timedelta
from enum import Enum

from google.cloud import bigquery, tasks_v2
from google.protobuf.timestamp_pb2 import Timestamp

from core.candles.binance_client import BinanceClient
from core.alerts.telegram import Telegram
from core.order.moby_order import MobyOrder, OrderPosition, OrderStatus, OrderMode, PositionCloseReason
from core.utils.redisclient import RedisClient
from core.utils.utils import unix_time_to_datetime_utc, datetime_utc_to_madrid, datetime_utc_to_unix_time, percentage_to_str


class PositionType(str, Enum):
    Long = "Long"
    Short = "Short"


class CloseOrder(str, Enum):
    Stoploss = "Stoploss"
    TrailingStop = "TrailingStop"
    TakeProfit = "TakeProfit"


class OrderSimulator:
    """Order Simulator - This class simulates the tracking of orders for BQ"""

    def __init__(self):
        """Default constructor"""

        self.__default_trailing_stop_percentage = 0.4  # 0,4%
        self.__default_stoploss_percentage = 8  # 8%

        self.__refresh_seconds = 3  # Time to wait to check the status of the order
        self.__random_threshold = 1  # Max random increment/decrement of refresh_seconds, to spread requests

        self.__chat = Telegram()
        self.__binance_client = BinanceClient()
        self.__redis_client = RedisClient()
        self.__redis_prefix_opened_order = "SIMULATION_"
        self.__redis_prefix_profits = "PROFITS_"

        # Big Query
        self.__bq_client = bigquery.Client(project="fender-310315")
        self.__bq_table_order_simulator = self.__bq_client.get_table(self.__bq_client.dataset("MobyDick").table("OrderSimulator"))
        self.__bq_table_moby_order = self.__bq_client.get_table(self.__bq_client.dataset("MobyDick").table("MobyOrder"))

        # Cloud Tasks
        self.__cloud_tasks_client = tasks_v2.CloudTasksClient()
        self.__cloud_tasks_parent = self.__cloud_tasks_client.queue_path("fender-310315", "europe-west1",
                                                                         "order-simulator")
        self.__task_schema = {
            "app_engine_http_request": {
                "http_method": tasks_v2.HttpMethod.POST,
                "relative_uri": "/mobydick/signals/simulation/refresh",
                "headers": {"Content-type": "application/json"},
                "body": "".encode()  # Fill on build
            },
            "schedule_time": datetime.utcnow()  # Fill on build
        }

    def open_position_simulation(self, moby_order: MobyOrder):
        """Simulate a buy order by scheduling refreshes of close possibilities"""

        if self.__redis_client.get_value(self.__build_redis_key(moby_order)) is not None:
            logging.warning("Simulación ya abierta para: " + moby_order.ticker)
            return

        ticker = moby_order.ticker
        order_price = moby_order.order_price
        position_type = PositionType(moby_order.position.value)
        order_label = moby_order.order_label
        stoploss_price = moby_order.stop_loss
        take_profit_price = moby_order.take_profit_price
        trailing_stop_percentage = moby_order.trailing_stop
        trailing_stop_activation_percentage = moby_order.trailing_stop_activation_percent
        if trailing_stop_activation_percentage is None and moby_order.trailing_stop_activation_price is not None:
            trailing_stop_activation_percentage = 100 * abs(order_price - moby_order.trailing_stop_activation_price) / order_price

        if trailing_stop_percentage is None:
            trailing_stop_percentage = 999999999

        if trailing_stop_activation_percentage is None:
            trailing_stop_activation_percentage = 0

        trailling_stop_inc_price = order_price * trailing_stop_percentage / 100

        # Comprobación parametros
        if position_type == PositionType.Long:
            trailing_stop_price = 0
            if not trailing_stop_activation_percentage:
                # Activamos directamente el trailing stop si el percentage es None o cero
                trailing_stop_price = order_price - trailling_stop_inc_price

            if stoploss_price is None:
                stoploss_price = 0
            elif stoploss_price >= order_price:
                raise Exception("Simulator Error: El precio de stoploss es inválido, saltaría de inmediado StopLoss: "
                                + str(stoploss_price) + " Price: " + str(order_price))

            if take_profit_price is None:
                take_profit_price = 999999999
            elif take_profit_price <= order_price:
                raise Exception("Simulator Error: El precio de take profit es inválido, saltaría de inmediado TakeProft: "
                                + str(take_profit_price) + " Price: " + str(order_price))

        else:
            trailing_stop_price = 999999999
            if not trailing_stop_activation_percentage:
                # Activamos directamente el trailing stop si el percentage es None o cero
                trailing_stop_price = order_price + trailling_stop_inc_price

            if stoploss_price is None:
                stoploss_price = 999999999
            elif stoploss_price <= order_price:
                raise Exception("Simulator Error: El precio de stoploss es inválido, saltaría de inmediado"
                                + str(stoploss_price) + " Price: " + str(order_price))

            if take_profit_price is None:
                take_profit_price = 0
            elif take_profit_price >= order_price:
                raise Exception("Simulator Error: El precio de take profit es inválido, saltaría de inmediado TakeProft: "
                                + str(take_profit_price) + " Price: " + str(order_price))

        moby_order.open_price = order_price
        moby_order.open_time = datetime.utcnow()
        moby_order.status = OrderStatus.Open
        moby_order.order_mode = OrderMode.Simulated

        body = {
            "order_id": moby_order.id,
            "ticker": ticker,
            "position_type": position_type,
            "entry_indicator": order_label,
            "start_price": order_price,
            "start_time": datetime_utc_to_unix_time(moby_order.open_time),
            "stoploss_price": stoploss_price,
            "take_profit_price": take_profit_price,
            "trailing_stop_percentage": trailing_stop_percentage,
            "trailing_stop_activation_percentage": trailing_stop_activation_percentage,
            "trailing_stop_price": trailing_stop_price,
            "quantity": moby_order.quantity,
            "leverage": moby_order.leverage
        }

        self.__schedule_next_refresh(body)
        self.__send_order_to_bq(moby_order)
        self.__redis_client.save_value(self.__build_redis_key(moby_order), "SIMULATION")

    def refresh_order(self, request_body):
        """Check stoploss and trailing stop of a opened simulated order"""

        # Variables del request_body
        order_id = request_body["order_id"]
        ticker = request_body["ticker"]
        position_type = PositionType(request_body["position_type"])
        start_price = request_body["start_price"]
        start_time = unix_time_to_datetime_utc(request_body["start_time"])
        stoploss_price = request_body["stoploss_price"]
        take_profit_price = request_body["take_profit_price"]
        trailing_stop_price = request_body["trailing_stop_price"]
        trailing_stop_percentage = request_body["trailing_stop_percentage"]
        trailing_stop_activation_percentage = request_body["trailing_stop_activation_percentage"]
        entry_indicator = request_body["entry_indicator"]
        quantity = request_body["quantity"]
        leverage = request_body["leverage"]
        current_time = datetime_utc_to_madrid(datetime.utcnow())

        current_price = self.__binance_client.get_current_mark_price(ticker)

        trailling_stop_inc_price = start_price * trailing_stop_percentage / 100
        trailling_stop_activation_inc_price = start_price * trailing_stop_activation_percentage / 100

        if position_type == PositionType.Long:
            # Actualizar trailing stop price
            trailing_stop_activation_price = start_price + trailling_stop_activation_inc_price
            if current_price >= trailing_stop_activation_price:
                new_trailling_stop_price = current_price - trailling_stop_inc_price
                if new_trailling_stop_price > trailing_stop_price:
                    trailing_stop_price = new_trailling_stop_price
                    request_body["trailing_stop_price"] = new_trailling_stop_price

            # Comprobar si salta un cierre
            trailing_stop_executed = current_price <= trailing_stop_price
            stoploss_executed = current_price <= stoploss_price
            take_profit_executed = current_price >= take_profit_price

        else:  # Short
            # Actualizar trailing stop price
            trailing_stop_activation_price = start_price - trailling_stop_activation_inc_price
            if current_price <= trailing_stop_activation_price:
                new_trailling_stop_price = current_price + trailling_stop_inc_price
                if new_trailling_stop_price < trailing_stop_price:
                    trailing_stop_price = new_trailling_stop_price
                    request_body["trailing_stop_price"] = new_trailling_stop_price

            # Comprobar si salta un cierre
            trailing_stop_executed = current_price >= trailing_stop_price
            stoploss_executed = current_price >= stoploss_price
            take_profit_executed = current_price <= take_profit_price

        moby_order = MobyOrder(ticker=ticker,
                               order_price=start_price,
                               quantity=quantity,
                               stop_loss=stoploss_price,
                               position=OrderPosition(position_type.value),
                               order_label=entry_indicator,
                               leverage=leverage,
                               trailing_stop=trailing_stop_percentage,
                               trailing_stop_activation_percent=trailing_stop_activation_percentage,
                               take_profit_price=take_profit_price)
        moby_order.open_price = start_price
        moby_order.close_time = datetime.utcnow()
        moby_order.open_time = start_time
        moby_order.status = OrderStatus.Close
        moby_order.order_mode = OrderMode.Simulated
        moby_order.id = order_id

        # Cerrar por trailling stop
        if trailing_stop_executed:
            self.__send_to_bq(request_body, trailing_stop_price, CloseOrder.TrailingStop, current_time)
            moby_order.close_reason = PositionCloseReason.TrailingStop
            moby_order.close_price = trailing_stop_price
            self.__send_order_to_bq(moby_order)
            self.__send_alert(request_body, trailing_stop_price, CloseOrder.TrailingStop, current_time)
            self.__redis_client.clear_key(self.__build_redis_key(moby_order))
            self.__save_profit_in_redis(moby_order)

        # Cerrar por stoploss
        elif stoploss_executed:
            self.__send_to_bq(request_body, stoploss_price, CloseOrder.Stoploss, current_time)
            moby_order.close_reason = PositionCloseReason.Stoploss
            moby_order.close_price = stoploss_price
            self.__send_order_to_bq(moby_order)
            self.__send_alert(request_body, stoploss_price, CloseOrder.Stoploss, current_time)
            self.__redis_client.clear_key(self.__build_redis_key(moby_order))
            self.__save_profit_in_redis(moby_order)

        # Cerrar por take profit
        elif take_profit_executed:
            self.__send_to_bq(request_body, take_profit_price, CloseOrder.TakeProfit, current_time)
            moby_order.close_reason = PositionCloseReason.TakeProfit
            moby_order.close_price = take_profit_price
            self.__send_order_to_bq(moby_order)
            self.__send_alert(request_body, take_profit_price, CloseOrder.TakeProfit, current_time)
            self.__redis_client.clear_key(self.__build_redis_key(moby_order))
            self.__save_profit_in_redis(moby_order)

        # La orden sigue abierta. Programar siguiente refresco
        else:
            self.__schedule_next_refresh(request_body)

    def get_profits_from_redis(self, order_label:str, min: int, max: int):
        """"""
        return self.__redis_client.get_values_between_scores(self.__redis_prefix_profits + order_label, min, max)

    def __save_profit_in_redis(self, moby_order: MobyOrder):
        """Save profit in redis, deleting oldest profits"""

        redis_key = self.__redis_prefix_profits + moby_order.order_label

        one_day_seconds = 24 * 60 * 60
        current_time = moby_order.close_time
        self.__redis_client.save_scored_value(redis_key, moby_order.profit_percent, int(datetime_utc_to_unix_time(current_time)), one_day_seconds)

        # Barremos las claves antiguas
        self.__redis_client.clear_between_scores(redis_key, 0, int(datetime_utc_to_unix_time(current_time - timedelta(hours=24))))

    def __schedule_next_refresh(self, request_body):
        """Schedule next refresh in Google Cloud Tasks"""

        task = deepcopy(self.__task_schema)
        task["app_engine_http_request"]["body"] = json.dumps(request_body).encode()

        # Random threshold to avoid all tasks executing at the same time
        in_seconds = self.__refresh_seconds + random.uniform(-self.__random_threshold, self.__random_threshold)
        timestamp = Timestamp()
        timestamp.FromDatetime(datetime.utcnow() + timedelta(seconds=in_seconds))
        task["schedule_time"] = timestamp

        response = self.__cloud_tasks_client.create_task(parent=self.__cloud_tasks_parent, task=task)

        logging.warning('Created task {}'.format(response.name))

    def __send_alert(self, request_body, end_price, close_reason: str, end_time: datetime):
        """Send alert to telegram"""

        # Variables del request_body
        ticker = request_body["ticker"]
        position_type = PositionType(request_body["position_type"])
        entry_indicator = request_body["entry_indicator"]
        start_price = request_body["start_price"]
        start_time = datetime_utc_to_madrid(unix_time_to_datetime_utc(request_body["start_time"]))

        price_inc = end_price - start_price
        if position_type == PositionType.Short:
            price_inc *= -1
        price_inc_percent = price_inc / start_price

        logging.warning("_Notify from profit_ " + ticker)
        msg = "[SALIDA][{0}][{1}]" \
              "\nIndicador de entrada: {2}" \
              "\nOrden abierta a las: {3}" \
              "\nOrden cerrada a las: {4}" \
              "\nPrecio entrada: {5}" \
              "\nPrecio salida: {6}" \
              "\nCerrada por: {7}" \
              "\nBeneficio: {8}".format(
            ticker, position_type, entry_indicator, str(start_time), str(end_time), start_price, end_price,
            close_reason, percentage_to_str(price_inc_percent)
        )
        self.__chat.send_message_to_group_2(msg)

    def __send_to_bq(self, request_body, end_price, close_reason: str, end_time: datetime):
        """Send theorical profit to BQ"""

        # Variables del request_body
        ticker = request_body["ticker"]
        position_type = PositionType(request_body["position_type"])
        entry_indicator = request_body["entry_indicator"]
        start_price = request_body["start_price"]
        start_time = datetime_utc_to_madrid(unix_time_to_datetime_utc(request_body["start_time"]))

        msg = {
            "ticker": ticker,
            "positionType": position_type,
            "entryIndicator": entry_indicator,
            "startTime": str(start_time),
            "endTime": str(end_time),
            "startPrice": start_price,
            "endPrice": end_price,
            "closeReason": close_reason
        }

        rows_to_insert = [
            msg
        ]

        errors = self.__bq_client.insert_rows(self.__bq_table_order_simulator, rows_to_insert)
        if errors:
            logging.error(errors)

    def __send_order_to_bq(self, moby_order: MobyOrder):
        """Almacena la orden en BQ"""

        moby_order.update_metrics()
        bq_array = [moby_order.__dict__]
        df = pd.DataFrame(bq_array)
        errors = self.__bq_client.insert_rows_from_dataframe(self.__bq_table_moby_order, df)
        if errors:
            logging.error(errors)

    def __build_redis_key(self, moby_order: MobyOrder):
        return self.__redis_prefix_opened_order + moby_order.ticker + moby_order.order_label


# Local Testing
if __name__ == "__main__":
    raise Exception("¡¡Cuidado!! Borrar la cloud task manualmente despues de crearla aqui")

    order_simulator = OrderSimulator()

    mock_moby_order = MobyOrder(
        ticker="BTCUSDT",
        order_price=40000,
        quantity=200.0,
        stop_loss=20000,
        position=OrderPosition.Long,
        order_label="[UNITTEST]",
        leverage=5,
        trailing_stop=0.4)

    start = datetime.utcnow()
    order_simulator.open_position_simulation(mock_moby_order)
    end = datetime.utcnow()
    time_elapsed = (end - start).total_seconds()
    print("Finished in", time_elapsed, "seconds.")
