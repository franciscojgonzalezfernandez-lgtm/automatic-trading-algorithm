# Local Testing
from datetime import datetime

from core.backtesting.backtesting import Backtesting
from core.candles.binance_client import BinanceClient

if __name__ == "__main__":
    backtesting = Backtesting()
    backtest_analyzers = list()
    backtest_start_time = datetime(2020, 12, 31)
    binance_client = BinanceClient()
    coins = [key for key in binance_client.get_all_available_symbols()]
    count = 0
    for coin in coins:
        count = count + 1
        print("Coin: " + str(coin) + " progreso: " + str(count) + "/" + str(len(coins)))
        backtesting.get_all_candles_from_start_time(coin, "1m", backtest_start_time)