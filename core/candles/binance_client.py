# Copyright 2021 TradersOfTheUniverse All Rights Reserved.
#
# [Binance Client - Class created to manage the interaction with Binance API]
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
import time
import hmac
import hashlib

from urllib.parse import urlencode
from datetime import datetime
from typing import List
from requests import Session
from core.candles.candlestick import Candlestick
from google.cloud.secretmanager import SecretManagerServiceClient

from core.utils.utils import datetime_utc_to_unix_time


class BinanceClient:
    """Class created to manage the interaction with Binance API"""

    def __init__(self,  account: str = None, testnet=False):
        """Default constructor"""

        self.__session = Session()
        self.__spot_api_url = "https://api.binance.com/api/v3"
        self.__futures_api_url = "https://fapi.binance.com/fapi/v1"

        #Real Account
        if account is not None and len(account) > 2:

            __binance_secret = SecretManagerServiceClient().access_secret_version(
                    request={"name": "projects/fender-310315/secrets/" + account + "/versions/latest"}
                ).payload.data.decode("UTF-8")
            secret_json = json.loads(__binance_secret)
            self.__futures_api_key = secret_json.get("futures_api_key", None)
            self.__futures_api_secret = secret_json.get("futures_api_secret", None)

        elif testnet:
            self.__futures_api_url = "https://testnet.binancefuture.com/fapi/v1"
            self.__futures_api_key = "f8e9ee443940b6639422184338a77aff0f59af6489020ce6be35c163959eaea0"
            self.__futures_api_secret = "824d77af465b396e58ab08f0fe29115cf1b9629667b7ce51408d47ff0b59db5e"
            logging.warning("Operando sobre la Testnet")

        else:
            logging.warning("Operando solo en modo consulta")

    def get_last_candlesticks(self,
                              coin: str,
                              num_candlesticks: int = None,
                              interval: str = "1m",
                              futures_info: bool = False,
                              start_time_utc: datetime = None,
                              end_time_utc: datetime = None) -> List[Candlestick]:
        """https://github.com/binance/binance-spot-api-docs/blob/master/rest-api.md#klinecandlestick-data"""

        path = "/klines"
        params = {"symbol": coin, "interval": interval}
        if num_candlesticks is not None:
            params["limit"] = num_candlesticks
        if start_time_utc is not None:
            params["startTime"] = datetime_utc_to_unix_time(start_time_utc, True)
        if end_time_utc is not None:
            params["endTime"] = datetime_utc_to_unix_time(end_time_utc, True)

        if futures_info is True:
            response = self.__session.get(url=self.__futures_api_url + path, params=params)
        else:
            response = self.__session.get(url=self.__spot_api_url + path, params=params)

        if response.status_code != 200:
            reason = response.reason if response.reason else ""
            info = response.text if response.text else ""
            raise Exception("Binance Http Error " + str(response.status_code) + " " + reason + " " + info)

        response_json = response.json()

        return [Candlestick(coin, raw_candlestick) for raw_candlestick in response_json]

    def get_current_mark_price(self, coin):
        """Get current mark price from futures"""

        path = "/premiumIndex"
        params = {"symbol": coin}

        response = self.__session.get(url=self.__futures_api_url + path, params=params)

        if response.status_code != 200:
            reason = response.reason if response.reason else ""
            info = response.text if response.text else ""
            raise Exception("Binance Http Error " + str(response.status_code) + " " + reason + " " + info)

        return float(response.json()["markPrice"])

    def get_all_available_symbols(self):
        """Get all available symbols to operate"""

        path = "/exchangeInfo"

        response = self.__session.get(url=self.__futures_api_url + path)

        if response.status_code != 200:
            reason = response.reason if response.reason else ""
            info = response.text if response.text else ""
            raise Exception("Binance Http Error " + str(response.status_code) + " " + reason + " " + info)

        return {symbol_data["symbol"]: symbol_data for symbol_data in response.json()["symbols"]
                if symbol_data["symbol"].endswith("USDT")
                and "TRAILING_STOP_MARKET" in symbol_data["orderTypes"]}

    def get_account_info(self):
        """Get account info from api"""

        path = "/account"
        headers = {"X-MBX-APIKEY": self.__futures_api_key}
        params = {}
        self.__sign_query_string_params(params, self.__futures_api_secret)

        response = self.__session.get(
            url=self.__futures_api_url.replace("v1", "v2") + path,
            params=params,
            headers=headers
        )

        if response.status_code != 200:
            reason = response.reason if response.reason else ""
            info = response.text if response.text else ""
            raise Exception("Binance Http Error " + str(response.status_code) + " " + reason + " " + info)

        response_json = response.json()

        if __name__ == "__main__":
            print(json.dumps(response_json, indent=4))

        return response_json

    def get_account_trade_list(self, start_time_utc: datetime = None, end_time_utc: datetime = None):
        """Get account trade list from api"""

        path = "/userTrades"
        headers = {"X-MBX-APIKEY": self.__futures_api_key}
        params = {"limit": 1000}
        if start_time_utc is not None:
            params["startTime"] = datetime_utc_to_unix_time(start_time_utc, True)
        if end_time_utc is not None:
            params["endTime"] = datetime_utc_to_unix_time(end_time_utc, True)
        self.__sign_query_string_params(params, self.__futures_api_secret)

        response = self.__session.get(
            url=self.__futures_api_url + path,
            params=params,
            headers=headers
        )

        if response.status_code != 200:
            reason = response.reason if response.reason else ""
            info = response.text if response.text else ""
            raise Exception("Binance Http Error " + str(response.status_code) + " " + reason + " " + info)

        response_json = response.json()

        if __name__ == "__main__":
            print(json.dumps(response_json, indent=4))

        return response_json

    def set_leverage(self, ticker: str, leverage: int = 20):
        """Set initial leverage for a ticket"""

        logging.warning("Poniendo el apalancamiento de {0} a x{1}".format(ticker, leverage))

        path = "/leverage"
        headers = {"X-MBX-APIKEY": self.__futures_api_key}
        params = {
            "symbol": ticker,
            "leverage": leverage
        }
        self.__sign_query_string_params(params, self.__futures_api_secret)

        response = self.__session.post(
            url=self.__futures_api_url + path,
            params=params,
            headers=headers
        )

        if response.status_code != 200:
            reason = response.reason if response.reason else ""
            info = response.text if response.text else ""
            raise Exception("Binance Http Error " + str(response.status_code) + " " + reason + " " + info)

        if __name__ == "__main__":
            print(json.dumps(response.json(), indent=4))

    def make_market_order(self, ticker: str, quantity: float, is_long: bool):
        """Make a buy/sell market order"""

        logging.warning("Ejecutando orden Market sobre " + ticker)

        path = "/order"
        headers = {"X-MBX-APIKEY": self.__futures_api_key}
        params = {
            "symbol": ticker,
            "side": "BUY" if is_long else "SELL",
            # "positionSide": "LONG" if long else "SHORT",  # For Hedge Mode
            "type": "MARKET",
            "quantity": quantity,
            "newOrderRespType": "RESULT"
        }
        self.__sign_query_string_params(params, self.__futures_api_secret)

        response = self.__session.post(
            url=self.__futures_api_url + path,
            params=params,
            headers=headers
        )

        if response.status_code != 200:
            reason = response.reason if response.reason else ""
            info = response.text if response.text else ""
            raise Exception("Binance Http Error " + str(response.status_code) + " " + reason + " " + info)

        response_json = response.json()

        if __name__ == "__main__":
            print(json.dumps(response_json, indent=4))

        return response_json

    def make_stoploss_order(self, ticker: str, stop_price: float, is_long: bool):
        """Make a stop market order"""

        logging.warning("Ejecutando orden Stoploss sobre " + ticker)

        path = "/order"
        headers = {"X-MBX-APIKEY": self.__futures_api_key}
        params = {
            "symbol": ticker,
            "side": "SELL" if is_long else "BUY",
            # "positionSide": "LONG" if long else "SHORT",  # For Hedge Mode
            "type": "STOP_MARKET",
            "stopPrice": stop_price,
            "closePosition": True,
            "workingType": "CONTRACT_PRICE",  # CONTRACT_PRICE vs MARK_PRICE
            "priceProtect": True,
            "newOrderRespType": "ACK"
        }
        self.__sign_query_string_params(params, self.__futures_api_secret)

        response = self.__session.post(
            url=self.__futures_api_url + path,
            params=params,
            headers=headers
        )

        if response.status_code != 200:
            reason = response.reason if response.reason else ""
            info = response.text if response.text else ""
            raise Exception("Binance Http Error " + str(response.status_code) + " " + reason + " " + info)

        if __name__ == "__main__":
            print(json.dumps(response.json(), indent=4))

    def make_take_profit_order(self, ticker: str, stop_price: float, is_long: bool):
        """Make a take profit market order"""

        logging.warning("Ejecutando orden Take Profit sobre " + ticker)

        path = "/order"
        headers = {"X-MBX-APIKEY": self.__futures_api_key}
        params = {
            "symbol": ticker,
            "side": "SELL" if is_long else "BUY",
            # "positionSide": "LONG" if long else "SHORT",  # For Hedge Mode
            "type": "TAKE_PROFIT_MARKET",
            "stopPrice": stop_price,
            "closePosition": True,
            "workingType": "CONTRACT_PRICE",  # CONTRACT_PRICE vs MARK_PRICE
            "priceProtect": True,
            "newOrderRespType": "ACK"
        }
        self.__sign_query_string_params(params, self.__futures_api_secret)

        response = self.__session.post(
            url=self.__futures_api_url + path,
            params=params,
            headers=headers
        )

        if response.status_code != 200:
            reason = response.reason if response.reason else ""
            info = response.text if response.text else ""
            raise Exception("[" + ticker + "] Binance Http Error " + str(response.status_code) + " " + reason + " " + info)

        if __name__ == "__main__":
            print(json.dumps(response.json(), indent=4))

    def make_trailing_stop_order(self, ticker: str, callback_rate: float, activation_price: float, quantity: float, is_long: bool):
        """Make a trailing stop market order
        If activation_price is None, trailing stop will be activated immediately
        """

        logging.warning("Ejecutando orden Trailing Stop sobre " + ticker)

        path = "/order"
        headers = {"X-MBX-APIKEY": self.__futures_api_key}
        params = {
            "symbol": ticker,
            "side": "SELL" if is_long else "BUY",
            # "positionSide": "LONG" if long else "SHORT",  # For Hedge Mode
            "type": "TRAILING_STOP_MARKET",
            "quantity": quantity,
            "reduceOnly": True,  # For One-Way Mode
            "callbackRate": callback_rate,
            "workingType": "CONTRACT_PRICE",  # CONTRACT_PRICE vs MARK_PRICE
            "priceProtect": True,
            "newOrderRespType": "ACK"
        }
        if activation_price is not None:
            params["activationPrice"] = activation_price

        self.__sign_query_string_params(params, self.__futures_api_secret)

        response = self.__session.post(
            url=self.__futures_api_url + path,
            params=params,
            headers=headers
        )

        if response.status_code != 200:
            reason = response.reason if response.reason else ""
            info = response.text if response.text else ""
            raise Exception("Binance Http Error " + str(response.status_code) + " " + reason + " " + info)

        if __name__ == "__main__":
            print(json.dumps(response.json(), indent=4))

    def delete_orders_of(self, ticker):
        """Delete all open orders of a ticker"""

        logging.warning("Borrando ordenes pendientes sobre " + ticker)

        path = "/allOpenOrders"
        headers = {"X-MBX-APIKEY": self.__futures_api_key}
        params = {"symbol": ticker}
        self.__sign_query_string_params(params, self.__futures_api_secret)

        response = self.__session.delete(
            url=self.__futures_api_url + path,
            params=params,
            headers=headers
        )

        if response.status_code != 200:
            reason = response.reason if response.reason else ""
            info = response.text if response.text else ""
            raise Exception("Binance Http Error " + str(response.status_code) + " " + reason + " " + info)

        if __name__ == "__main__":
            print(json.dumps(response.json(), indent=4))

    def is_position_open(self, ticker):
        """Returns True if there is already an open position of the given ticker"""
        path = "/positionRisk"
        headers = {"X-MBX-APIKEY": self.__futures_api_key}
        params = {"symbol": ticker}
        self.__sign_query_string_params(params, self.__futures_api_secret)

        response = self.__session.get(
            url=self.__futures_api_url.replace("v1", "v2") + path,
            params=params,
            headers=headers
        )

        if response.status_code != 200:
            reason = response.reason if response.reason else ""
            info = response.text if response.text else ""
            raise Exception("Binance Http Error " + str(response.status_code) + " " + reason + " " + info)

        response_json = response.json()

        position_amount = float(response_json[0]["positionAmt"])

        position_open = position_amount != 0

        if __name__ == "__main__":
            print("La posicion {0} esta {1}.".format(ticker, "abierta" if position_open else "cerrada"))

        return position_open

    def get_all_positions_information(self):
        """Return positions info"""
        path = "/positionRisk"
        headers = {"X-MBX-APIKEY": self.__futures_api_key}
        params = {}
        self.__sign_query_string_params(params, self.__futures_api_secret)

        response = self.__session.get(
            url=self.__futures_api_url.replace("v1", "v2") + path,
            params=params,
            headers=headers
        )

        if response.status_code != 200:
            reason = response.reason if response.reason else ""
            info = response.text if response.text else ""
            raise Exception("Binance Http Error " + str(response.status_code) + " " + reason + " " + info)

        return response.json()

    def get_open_orders(self):
        """Returns open orders info"""
        path = "/openOrders"
        headers = {"X-MBX-APIKEY": self.__futures_api_key}
        params = {}
        self.__sign_query_string_params(params, self.__futures_api_secret)

        response = self.__session.get(
            url=self.__futures_api_url + path,
            params=params,
            headers=headers
        )

        if response.status_code != 200:
            reason = response.reason if response.reason else ""
            info = response.text if response.text else ""
            raise Exception("Binance Http Error " + str(response.status_code) + " " + reason + " " + info)

        return response.json()

    def __sign_query_string_params(self, params, api_secret):
        """Add params needed for USER_DATA endpoints"""

        params["timestamp"] = int(time.time() * 1000)

        query_string = urlencode(params)
        # replace single quote to double quote
        query_string = query_string.replace('%27', '%22')

        params["signature"] = self.__hashing(query_string, api_secret)

    def __hashing(self, query_string, api_secret):
        return hmac.new(api_secret.encode('utf-8'), query_string.encode('utf-8'), hashlib.sha256).hexdigest()


# Local Testing
if __name__ == "__main__":
    # TODO Stoploss y TrailingStop a la vez en batch (da error, al menos en la Testnet)

    binance_client = BinanceClient(testnet=True)
    coin = "BTCUSDT"
    quantity = 0.111111111111
    quantity = round(quantity, binance_client.get_all_available_symbols()[coin]["quantityPrecision"])
    binance_client.is_position_open(coin)
    binance_client.delete_orders_of(coin)
    binance_client.make_market_order(coin, quantity, is_long=True)
    binance_client.is_position_open(coin)
    binance_client.make_stoploss_order(coin, 10000, is_long=True)
    binance_client.make_take_profit_order(coin, 90000, is_long=True)
    binance_client.make_trailing_stop_order(coin, 5.0, None, quantity, is_long=True)
