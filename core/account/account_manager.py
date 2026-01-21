# Copyright 2021 TradersOfTheUniverse S.A. All Rights Reserved.
#
# [Account Manager - This class manage Binance Account Api]
#
# Authors:
#   antoniojose.luqueocana@telefonica.com
#   joseluis.roblesurquiza@telefonica.com
#   franciscojavier.gonzalezfernandez1@telefonica.com
#
# Version: 0.1
#
import datetime
import logging
from copy import deepcopy

from dateutil import parser
from typing import List

from google.api_core.exceptions import BadRequest
from google.cloud import bigquery

from core.backtesting.backtest_result import BacktestResult
from core.candles.binance_client import BinanceClient
from core.order.moby_order import MobyOrder, OrderMode, OrderStatus, OrderPosition
from core.utils.utils import datetime_utc_to_madrid, unix_time_to_datetime_utc, percentage_to_str, \
    datetime_madrid_to_utc


class Account:
    """Util data of an account"""
    def __init__(self):
        """Default constructor"""
        self.wallet_balance: float = 0
        self.margin_balance: float = 0
        self.available_balance: float = 0
        self.opened_positions: List[MobyOrder] = list()
        self.closed_positions: List[MobyOrder] = list()


class AccountManager:
    """Account Manager - This class manage Binance Account Api"""

    def __init__(self, account: str):
        """Default constructor"""

        self.__account_name = account
        self.__binance_client = BinanceClient(account=account)

        self.__bq_client: bigquery.Client = None
        self.__bq_table_moby_order: bigquery.Table = None
        self.__bq_job_config: bigquery.LoadJobConfig = None

    def load(self):
        self.__bq_client = bigquery.Client(project="fender-310315")
        self.__bq_table_moby_order = self.__bq_client.get_table(self.__bq_client.dataset("MobyDick").table("MobyOrderReal"))
        # Config to upload and append tables via json
        self.__bq_job_config = bigquery.LoadJobConfig()
        self.__bq_job_config.source_format = bigquery.SourceFormat.NEWLINE_DELIMITED_JSON
        self.__bq_job_config.write_disposition = bigquery.WriteDisposition.WRITE_APPEND
        return self

    def build_account_summary(self, start_time_text: str = None, end_time_text: str = None, upload_to_bq=False) -> str:
        """Transforms an Account object to a readable string"""
        start_time = None
        end_time = None
        if start_time_text is not None:
            start_time = datetime_madrid_to_utc(parser.parse(start_time_text))
        if end_time_text is not None:
            end_time = datetime_madrid_to_utc(parser.parse(end_time_text))

        hide_opened_positions = start_time_text is not None or end_time_text is not None

        account = self.get_account_info(start_time, end_time, with_opened_positions_prices=(not hide_opened_positions))
        if upload_to_bq:
            uploaded_rows = self.__upload_closed_orders_to_bq(account)
            logging.info("Se han volcado a BQ {0} nuevas posiciones".format(uploaded_rows))

        msg = "BALANCE:\n"
        msg += "Billetera: \t" + str(account.wallet_balance) + " USDT\n"
        msg += "Margen:    \t" + str(account.margin_balance) + " USDT\n"
        msg += "Disponible:\t" + str(account.available_balance) + " USDT\n"
        msg += "\n\nPOSICIONES ABIERTAS:\n\n"

        if account.opened_positions:
            if hide_opened_positions:
                msg += "Hay {0} posiciones abiertas. Quita el filtro de fechas para verlas\n\n".format(len(account.opened_positions))
            else:
                msg += "Hay {0} posiciones abiertas\nHay {1} posiciones en beneficio\nProfit en curso: {2} (x apalancamiento)\n\n".format(
                    len(account.opened_positions),
                    sum([(1 if position.profit_percent > 0 else 0) for position in account.opened_positions]),
                    percentage_to_str(sum(position.profit_percent for position in account.opened_positions))
                )
                for position in account.opened_positions:
                    msg += "[{0}][{1}]" \
                            "\nOrden abierta a las: {2}" \
                            "\nPrecio entrada: {3}" \
                            "\nPrecio actual: {4}" \
                            "\nPrecio SL: {5}" \
                            "\nPrecio TP: {6}" \
                            "\nMargen inicial: {7} USDT" \
                            "\nApalancamiento: x{8}" \
                            "\nComisión apertura: -{9:.8f} {10}" \
                            "\nBeneficio: {11}" \
                            "\nBeneficio apalancado: {12}" \
                            "\nBeneficio: {13} USDT".format(
                        position.ticker,
                        position.position,
                        str(datetime_utc_to_madrid(position.open_time)),
                        position.open_price,
                        position.close_price,
                        position.stop_loss,
                        position.take_profit_price,
                        position.quantity,
                        position.leverage,
                        position.commission,
                        position.commission_asset,
                        percentage_to_str(position.profit_percent),
                        percentage_to_str(position.profit_percent * position.leverage),
                        position.profit_usdt
                    )
                    msg += "\n\n"

        msg += "\n\n\n\nPOSICIONES CERRADAS:\n\n"

        if not account.closed_positions:
            msg += "No hay órdenes cerradas en este período de tiempo"
        else:
            msg += "\nÚltima orden cerrada a las: " + str(max(pos.close_time for pos in account.closed_positions))
            msg += "\nPrimera orden cerrada a las: " + str(min(pos.close_time for pos in account.closed_positions))

        if not end_time:
            end_time = datetime.datetime.utcnow()

        hours_ago_24 = end_time - datetime.timedelta(hours=24)
        detailed_closed_positions_24_hours_ago = BacktestResult()
        detailed_closed_positions_24_hours_ago.orders = [pos for pos in account.closed_positions if pos.close_time > hours_ago_24]
        detailed_closed_positions_24_hours_ago.init_metrics()
        msg += "\n\nProf Últimas 24h: " + percentage_to_str(detailed_closed_positions_24_hours_ago.profit)

        detailed_closed_positions = BacktestResult()
        detailed_closed_positions.orders = account.closed_positions
        detailed_closed_positions.init_metrics()
        msg += "\n\nProf: {0:<8} (x apalancamiento)\nMedio: {1:<6}\nÓrdenes: {2:<3}\nAciert: {3:<7}\nDD({4}): {5:<7}\nDDRelativo({6}:{7}): {8}\nProf: {9} USDT\nComisiones: -{10:.8f} BNB  -{11} USDT\n\n\n".format(
            percentage_to_str(detailed_closed_positions.profit),
            percentage_to_str(detailed_closed_positions.average_profit),
            len(detailed_closed_positions.orders),
            percentage_to_str(detailed_closed_positions.success, False),
            detailed_closed_positions.drawdown.historic_dd_time.date() if detailed_closed_positions.drawdown.historic_dd_time is not None else "YYYY-MM-DD",
            percentage_to_str(detailed_closed_positions.drawdown.historic_dd, False),
            detailed_closed_positions.drawdown.relative_dd_start.date() if detailed_closed_positions.drawdown.relative_dd_start is not None else "YYYY-MM-DD",
            detailed_closed_positions.drawdown.relative_dd_end.date() if detailed_closed_positions.drawdown.relative_dd_end is not None else "YYYY-MM-DD",
            percentage_to_str(detailed_closed_positions.drawdown.relative_dd, False),
            sum(order.profit_usdt for order in detailed_closed_positions.orders),
            sum(order.commission for order in detailed_closed_positions.orders if order.commission_asset == "BNB"),
            sum(order.commission for order in detailed_closed_positions.orders if order.commission_asset == "USDT")
        )

        for position in account.closed_positions:
            msg += "[{0}][{1}]" \
                    "\nOrden abierta a las: {2}" \
                    "\nOrden cerrada a las: {3}" \
                    "\nPrecio entrada: {4}" \
                    "\nPrecio salida: {5}" \
                    "\nComisión: -{6:.8f} {7}" \
                    "\nBeneficio: {8} (x apalancamiento)" \
                    "\nBeneficio: {9} USDT".format(
                position.ticker,
                position.position,
                str(datetime_utc_to_madrid(position.open_time)),
                str(datetime_utc_to_madrid(position.close_time)),
                position.open_price,
                position.close_price,
                position.commission,
                position.commission_asset,
                percentage_to_str(position.profit_percent),
                position.profit_usdt
            )
            msg += "\n\n"

        return msg

    def get_account_info(self, start_time: datetime = None, end_time: datetime = None, with_opened_positions_prices=True) -> Account:
        """Calls Binance Account Api to build an Account"""
        account = Account()

        account_info = self.__binance_client.get_account_info()
        account_trades = self.__binance_client.get_account_trade_list(start_time, end_time)
        if with_opened_positions_prices:
            all_positions_info = self.__binance_client.get_all_positions_information()
            all_open_orders = self.__binance_client.get_open_orders()

        account.wallet_balance = float(account_info["totalWalletBalance"])
        account.margin_balance = float(account_info["totalMarginBalance"])
        account.available_balance = float(account_info["availableBalance"])

        for position in account_info["positions"]:
            if position["initialMargin"] != "0":
                opened_position = MobyOrder(position["symbol"])
                opened_position.order_price = float(position["entryPrice"])
                opened_position.quantity = float(position["initialMargin"])
                opened_position.profit_usdt = float(position["unrealizedProfit"])
                opened_position.leverage = int(position["leverage"])
                opened_position.order_mode = OrderMode.Real
                opened_position.status = OrderStatus.Open
                opened_position.account = self.__account_name
                if with_opened_positions_prices:
                    pos_info = [pos for pos in all_positions_info if pos["symbol"] == opened_position.ticker]
                    open_orders = [order for order in all_open_orders if order["symbol"] == opened_position.ticker]
                    stoploss_order = [order for order in open_orders if order["type"] == "STOP_MARKET"]
                    take_profit_order = [order for order in open_orders if order["type"] == "TAKE_PROFIT_MARKET"]

                    opened_position.close_price = float(pos_info[0]["markPrice"]) if pos_info else None
                    opened_position.stop_loss = float(stoploss_order[0]["stopPrice"]) if stoploss_order else None
                    opened_position.take_profit_price = float(take_profit_order[0]["stopPrice"]) if take_profit_order else None

                account.opened_positions.append(opened_position)

        for trade in reversed(account_trades):
            if trade["realizedPnl"] == "0":  # Open
                position = None
                symbol = trade["symbol"]
                for current_position in account.opened_positions + account.closed_positions:
                    if (current_position.ticker == symbol and current_position.open_time is None) or current_position.open_order_id == trade["orderId"]:
                        position = current_position
                        break

                if position is None:
                    logging.warning("Posicion abierta que no está ni cerrada ni en curso ¿?")
                else:
                    position.open_order_id = trade["orderId"]
                    position.open_time = unix_time_to_datetime_utc(trade["time"])
                    position.position = OrderPosition.Long if trade["side"] == "BUY" else OrderPosition.Short
                    position.open_price = float(trade["price"])
                    position.commission += float(trade["commission"])
                    position.commission_asset = trade["commissionAsset"]
                    position.update_metrics()

            else:  # Close
                closed_position = None

                # Find position closed with >1 orders
                for current_position in account.closed_positions:
                    if current_position.close_order_id == trade["orderId"]:
                        closed_position = current_position
                        break

                if closed_position is None:
                    closed_position = MobyOrder(trade["symbol"])
                    account.closed_positions.append(closed_position)

                    closed_position.close_order_id = trade["orderId"]
                    closed_position.close_price = float(trade["price"])
                    closed_position.close_time = unix_time_to_datetime_utc(trade["time"])
                    closed_position.order_mode = OrderMode.Real
                    closed_position.status = OrderStatus.Close
                    closed_position.account = self.__account_name
                    closed_position.commission_asset = trade["commissionAsset"]
                    closed_position.profit_usdt = float(trade["realizedPnl"])
                else:
                    closed_position.profit_usdt += float(trade["realizedPnl"])

                closed_position.commission += float(trade["commission"])

        account.closed_positions = [position for position in account.closed_positions if position.open_time is not None]

        return account

    def __upload_closed_orders_to_bq(self, account: Account):
        """Upload orders from account to BQ only if not already exist. Does not uses Streaming Insert"""

        query = """
            SELECT open_order_id
            FROM `fender-310315.MobyDick.MobyOrderReal`
            WHERE account = '{0}'
        """.format(self.__account_name)

        query_job = self.__bq_client.query(query)  # Make an API request.
        already_in_bq_open_order_ids = [row[0] for row in query_job]

        json_rows = [
            deepcopy(position).__dict__ for position in account.closed_positions
            if position.open_order_id not in already_in_bq_open_order_ids
        ]

        if not json_rows:
            return 0

        # Datetimes to str, and delete Nones
        for i in range(len(json_rows)):
            position_json = json_rows[i]
            position_json_copy = position_json.copy()
            for key, value in position_json.items():
                if type(value) == datetime.datetime:
                    position_json_copy[key] = str(value)
                elif value is None:
                    del(position_json_copy[key])
            json_rows[i] = position_json_copy

        job = self.__bq_client.load_table_from_json(
            json_rows=json_rows,
            destination=self.__bq_table_moby_order,
            job_config=self.__bq_job_config
        )

        try:
            job.result()  # Waits for table load to complete.
        except BadRequest as e:
            for error in job.errors:
                logging.error('ERROR: {}'.format(error['message']))
            raise e

        # Return number of uploaded rows
        return job.output_rows


# Local Testing
if __name__ == "__main__":
    print(AccountManager("binance2").load().build_account_summary(upload_to_bq=True))
    # print(AccountManager("binance").load().build_account_summary("2021-11-26T00:00:00", "2021-11-27T23:59:59", upload_to_bq=True))
