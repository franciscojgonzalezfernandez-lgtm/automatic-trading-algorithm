# Copyright 2021 TradersOfTheUniverse S.A. All Rights Reserved.
#
# [Profit Checker - This class check the changes of the price after a signal]
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

from core.market.analyzer import Analyzer
from core.candles.binance_client import BinanceClient
from core.candles.candlestick import Candlestick
from core.alerts.telegram import Telegram


class ProfitChecker:
    """Profit Checker - This class check the changes of the price after a signal"""

    def __init__(self, market_analyzer: Analyzer):
        """Default constructor"""

        self.__market_analyzer = market_analyzer
        self.__chat = Telegram()
        self.__binance_client = BinanceClient()

        # Big Query
        self.__bq_client = bigquery.Client(project="fender-310315")
        self.__bq_table = self.__bq_client.get_table(self.__bq_client.dataset("MobyDick").table("Profit"))

    def check_profit_from_all(self):
        """Analyze profit of previous alerted signals"""

        for coin in self.__market_analyzer.coins_to_analyze:
            try:
                logging.warning("Cheking profit " + coin)
                last_8_candlesticks = self.__binance_client.get_last_candlesticks(coin, 8)

                alerted = False
                if self.__market_analyzer.analyze_volume(coin, candlesarray=last_8_candlesticks[:3], send_alert=False):
                    alerted = True

                if alerted:
                    self.__check_five_minutes_profit(coin, last_6_candlesticks=last_8_candlesticks[1:7], send_alert=True)
                    self.__send_to_bq(last_6_candlesticks=last_8_candlesticks[1:7])

            except Exception as e:
                logging.exception("Error comprobando el beneficio de " + coin + ". " + repr(e))

    def __check_five_minutes_profit(self, coin, last_6_candlesticks=None, send_alert=True):
        """"Check the changes of the price in the last 5 minutes"""

        if last_6_candlesticks is None:
            last_6_candlesticks = self.__binance_client.get_last_candlesticks(coin, 6)
        first_candlestick: Candlestick = last_6_candlesticks[0]

        message = ""
        price_inc_percent_str = ""
        first = True
        for candlestick in last_6_candlesticks:

            if first:
                price_inc_percent_str = "SIGNAL"
                first = False
            else:
                price_inc = candlestick.close_price - first_candlestick.close_price
                price_inc_percent = price_inc / first_candlestick.close_price * 100

                price_inc_percent_str = str(round(price_inc_percent, 2)) + "%"
                if price_inc_percent >= 0:
                    price_inc_percent_str = "+" + price_inc_percent_str

            message += str(candlestick.close_price) \
                       + " (" + price_inc_percent_str + ")" \
                       + " at {:d}:{:02d}".format(candlestick.close_time.hour, candlestick.close_time.minute) \
                       + "\n"

        message = coin + ": " + price_inc_percent_str + "\n" + message

        if send_alert:
            logging.warning("_Notify from profit_ " + coin)
            self.__chat.send_message_to_group_2(message)

    def __send_to_bq(self, last_6_candlesticks: List[Candlestick]):
        """Send theorical profit to BQ"""
        msg = {
            "ticker": last_6_candlesticks[0].ticker,
            "signalTimestamp": str(last_6_candlesticks[0].close_time),
            "signalPrice": last_6_candlesticks[0].close_price,
            "minute1Price": last_6_candlesticks[1].close_price,
            "minute2Price": last_6_candlesticks[2].close_price,
            "minute3Price": last_6_candlesticks[3].close_price,
            "minute4Price": last_6_candlesticks[4].close_price,
            "minute5Price": last_6_candlesticks[5].close_price,
        }

        rows_to_insert = [
            msg
        ]

        errors = self.__bq_client.insert_rows(self.__bq_table, rows_to_insert)
        if errors:
            logging.error(errors)


# Local Testing
if __name__ == "__main__":
    import time
    profit_checker = ProfitChecker(Analyzer())
    while True:
        start = datetime.utcnow()
        profit_checker.check_profit_from_all()
        end = datetime.utcnow()
        time_elapsed = (end-start).total_seconds()
        print("Finished in", time_elapsed, "seconds. Sleep...")
        time.sleep(60 - time_elapsed)
