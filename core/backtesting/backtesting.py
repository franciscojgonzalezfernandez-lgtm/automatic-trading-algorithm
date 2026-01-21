# Copyright 2021 TradersOfTheUniverse S.A. All Rights Reserved.
#
# [Market Analyzer - This class looks for profitability in the past of the market]
#
# Authors:
#   antoniojose.luqueocana@telefonica.com
#   joseluis.roblesurquiza@telefonica.com
#   franciscojavier.gonzalezfernandez1@telefonica.com
#
# Version: 0.1
#
import inspect
import os
import csv
import pickle
from multiprocessing import Pool, cpu_count
from typing import List, Dict
from datetime import datetime
import matplotlib.pyplot as plt
import matplotlib.ticker as mtick

from core.backtesting.backtest_result import BacktestResult
from core.backtesting.analyzer_modelo import AnalyzerModelo
from core.candles.binance_client import BinanceClient
from core.candles.candlestick import Candlestick
from core.order.binance_order import BinanceOrder
from core.order.moby_order import MobyOrder, OrderPosition, PositionCloseReason, OrderMode
from core.order.order_simulator import OrderSimulator
from core.utils.utils import percentage_to_str, datetime_utc_to_madrid


# Reutilizamos velas cargadas en ram entre instancias de la clase Backtesting() en los compare_backtests()
all_candles: List[Candlestick] = None

# Reutilizamos el indice cargado en ram entre monedas de la misma instancia y entre instancias de la clase Backtesting()
btc_index: List[Candlestick] = None


class Backtesting:
    """Market Analyzer - This class looks for profitability in the past of the market"""

    __months_names = {
        1: "Enero",
        2: "Febrero",
        3: "Marzo",
        4: "Abril",
        5: "Mayo",
        6: "Junio",
        7: "Julio",
        8: "Agosto",
        9: "Septiembre",
        10: "Octubre",
        11: "Noviembre",
        12: "Diciembre",
    }

    def __init__(self):
        """Default constructor"""
        self.__start_datetime = datetime.now()
        self.__result_per_coin: Dict[str, BacktestResult] = dict()

        self.__moby_order: MobyOrder = None
        self.__previous_moby_order: MobyOrder = None

    def __reset_opened_position_variables(self):
        """Reset variables for each order"""
        self.__previous_moby_order = self.__moby_order
        self.__moby_order: MobyOrder = None
        self.__trailing_stop_price: float = None

    def backtest(self, analyzer, start_time: datetime, processes: int = None):
        """Realiza un backtest sobre la hipotesis implementada en un analyzer"""

        coins_to_analyze = analyzer.coins_to_analyze.copy()

        # Error "code":-4003,"msg":"Quantity less than zero."
        # for remove_coin in ["BTCUSDT", "LUNAUSDT", "AXSUSDT", "SOLUSDT", "HNTUSDT", "KSMUSDT", "EGLDUSDT"]:
        #     coins_to_analyze.remove(remove_coin)

        if processes is None:
            processes = cpu_count()

        if processes == 1:
            for coin in coins_to_analyze:
                self.__backtest_single_coin(analyzer, start_time, coin)
        else:
            analyzer = Backtesting.__clean_analyzer_for_multiprocessing(analyzer)
            with Pool(processes) as p:
                results = p.starmap(Backtesting.thread_backtest_single_coin, [(analyzer, start_time, coin) for coin in coins_to_analyze])
            for result_per_coin in results:
                self.__result_per_coin.update(result_per_coin)

        self.__write_backtest_results(start_time, analyzer.interval, analyzer.order_label, full_report=True)

    @staticmethod
    def thread_backtest_single_coin(analyzer, start_time, coin) -> Dict[str, BacktestResult]:
        coin_backtest = Backtesting()
        coin_backtest.__backtest_single_coin(analyzer, start_time, coin)
        return coin_backtest.__result_per_coin

    def __backtest_single_coin(self, analyzer, start_time, coin, reuse_candles=False):
        """Realiza un backtest sobre la hipotesis implementada en un analyzer, para una sola moneda"""

        print("Backtest:", coin)

        global all_candles
        global btc_index

        analyze_args = inspect.getfullargspec(analyzer.analyze).args
        use_btc_index = "external_index_candles" in analyze_args
        use_previous_moby_order = "previous_moby_order" in analyze_args

        if all_candles and coin == all_candles[0].ticker:
            print("Reutilizando velas")
        else:
            all_candles = self.get_all_candles_from_start_time(coin, analyzer.interval, start_time)
            if hasattr(analyzer, "prepare_candles"):
                print("Preparando velas")
                analyzer.prepare_candles(all_candles)
                print("Velas preparadas")

        if not btc_index and use_btc_index:
            btc_index = self.get_all_candles_from_start_time("BTCUSDT", analyzer.interval, start_time)
            print("External Index BTC: " + str(len(btc_index)))

        print()

        self.__reset_opened_position_variables()
        self.__result_per_coin[coin] = BacktestResult()

        # Forma sucia pero rapida de tirar de fecha en lugar de candle_index_to_start_backtest
        # init_date = datetime(2021, 12, 2, 18, 30)
        # end_date = datetime(2021, 12, 4)

        for i in range(analyzer.candle_index_to_start_backtest, len(all_candles)):
            # if all_candles[i].open_time < init_date or all_candles[i].open_time > end_date:
            #     continue

            if self.__moby_order is None:
                current_candles = all_candles[i - analyzer.num_candles_to_iterate:i]

                # Llama a analyze() con los parametros que necesite
                if use_btc_index:
                    external_index_candles = btc_index[i - analyzer.num_candles_to_iterate:i]
                    if use_previous_moby_order:
                        moby_order = analyzer.analyze(current_candles, external_index_candles=external_index_candles, previous_moby_order=self.__previous_moby_order)
                    else:
                        moby_order = analyzer.analyze(current_candles, external_index_candles=external_index_candles)
                elif use_previous_moby_order:
                    moby_order = analyzer.analyze(current_candles, previous_moby_order=self.__previous_moby_order)
                else:
                    moby_order = analyzer.analyze(current_candles)

                if moby_order is not None:
                    self.__open_order(moby_order, current_candles[-1])
            else:
                self.__track_opened_position(all_candles[i - 1], all_candles[i - 2])

        if not reuse_candles:
            all_candles = None

        self.__write_backtest_results(start_time, analyzer.interval, analyzer.order_label, full_report=False)

    def __get_all_results(self, with_commissions=False) -> BacktestResult:
        """Obtiene el Result acumulado de todas las monedas, ordenando las ordenes por fecha"""
        all_results = BacktestResult()
        for result in self.__result_per_coin.values():
            all_results.orders += result.orders
        if with_commissions:
            all_results = all_results.copy_with_commissions()
        all_results.orders = sorted(all_results.orders, key=lambda order: order.close_time, reverse=False)
        all_results.init_metrics()
        return all_results

    def __open_order(self, moby_order: MobyOrder, current_candle: Candlestick):
        """ ¡¡¡WARNING!!! Modifica la moby_order por referencia
        Añade valores a la moby_order (Ej: si tenemos trailing_stop_activation_price, lo traduce a
        trailing_stop_activation_percent, tambien transforma nos None en float imposibles, etc)
        para facilitarle el trabajo al simulador.
        """

        self.__reset_opened_position_variables()

        moby_order.order_mode = OrderMode.Backtest
        moby_order.open_time = current_candle.close_time
        moby_order.open_price = current_candle.close_price

        if moby_order.trailing_stop_activation_percent is None and moby_order.trailing_stop_activation_price is not None:
            moby_order.trailing_stop_activation_percent = 100 * abs(moby_order.order_price - moby_order.trailing_stop_activation_price) / moby_order.order_price

        if moby_order.trailing_stop is None:
            moby_order.trailing_stop = 999999999

        if moby_order.trailing_stop_activation_percent is None:
            moby_order.trailing_stop_activation_percent = 0

        trailling_stop_inc_price = moby_order.order_price * moby_order.trailing_stop / 100

        # Comprobación parametros
        if moby_order.position == OrderPosition.Long:
            self.__trailing_stop_price = 0
            if not moby_order.trailing_stop_activation_percent:
                # Activamos directamente el trailing stop si el percentage es None o cero
                self.__trailing_stop_price = moby_order.order_price - trailling_stop_inc_price

            if moby_order.stop_loss is None:
                moby_order.stop_loss = 0
            elif moby_order.stop_loss >= moby_order.order_price:
                raise Exception("Simulator Error: El precio de stoploss es inválido, saltaría de inmediado StopLoss: "
                                + str(moby_order.stop_loss) + " Price: " + str(moby_order.order_price))

            if moby_order.take_profit_price is None:
                moby_order.take_profit_price = 999999999
            elif moby_order.take_profit_price <= moby_order.order_price:
                raise Exception("Simulator Error: El precio de take profit es inválido, saltaría de inmediado TakeProft: "
                                + str(moby_order.take_profit_price) + " Price: " + str(moby_order.order_price))

        else:
            self.__trailing_stop_price = 999999999
            if not moby_order.trailing_stop_activation_percent:
                # Activamos directamente el trailing stop si el percentage es None o cero
                self.__trailing_stop_price = moby_order.order_price + trailling_stop_inc_price

            if moby_order.stop_loss is None:
                moby_order.stop_loss = 999999999
            elif moby_order.stop_loss <= moby_order.order_price:
                raise Exception("Simulator Error: El precio de stoploss es inválido, saltaría de inmediado"
                                + str(moby_order.stop_loss) + " Price: " + str(moby_order.order_price))

            if moby_order.take_profit_price is None:
                moby_order.take_profit_price = 0
            elif moby_order.take_profit_price >= moby_order.order_price:
                raise Exception("Simulator Error: El precio de take profit es inválido, saltaría de inmediado TakeProft: "
                                + str(moby_order.take_profit_price) + " Price: " + str(moby_order.order_price))

        self.__moby_order = moby_order

    def __track_opened_position(self, current_candle: Candlestick, previous_candle: Candlestick):
        """Comprueba ha saltado una orden de cierre, y actualiza el trailing stop price"""

        trailling_stop_inc_price = self.__moby_order.order_price * self.__moby_order.trailing_stop / 100
        trailling_stop_activation_inc_price = self.__moby_order.order_price * self.__moby_order.trailing_stop_activation_percent / 100

        if self.__moby_order.position == OrderPosition.Long:
            # Actualizar trailing stop price
            trailing_stop_activation_price = self.__moby_order.order_price + trailling_stop_activation_inc_price
            if previous_candle.high_price >= trailing_stop_activation_price and previous_candle.close_time > self.__moby_order.open_time:
                new_trailling_stop_price = previous_candle.high_price - trailling_stop_inc_price
                if new_trailling_stop_price > self.__trailing_stop_price:
                    self.__trailing_stop_price = new_trailling_stop_price

            # Comprobar si salta un cierre
            trailing_stop_executed = current_candle.low_price <= self.__trailing_stop_price
            stoploss_executed = current_candle.low_price <= self.__moby_order.stop_loss
            take_profit_executed = current_candle.high_price >= self.__moby_order.take_profit_price

        else:  # Short
            # Actualizar trailing stop price
            trailing_stop_activation_price = self.__moby_order.order_price - trailling_stop_activation_inc_price
            if previous_candle.low_price <= trailing_stop_activation_price and previous_candle.close_time > self.__moby_order.open_time:
                new_trailling_stop_price = previous_candle.low_price + trailling_stop_inc_price
                if new_trailling_stop_price < self.__trailing_stop_price:
                    self.__trailing_stop_price = new_trailling_stop_price

            # Comprobar si salta un cierre
            trailing_stop_executed = current_candle.high_price >= self.__trailing_stop_price
            stoploss_executed = current_candle.high_price >= self.__moby_order.stop_loss
            take_profit_executed = current_candle.low_price <= self.__moby_order.take_profit_price

        if trailing_stop_executed:
            self.__moby_order.close_price = self.__trailing_stop_price
            self.__moby_order.close_reason = PositionCloseReason.TrailingStop
        elif stoploss_executed:
            self.__moby_order.close_price = self.__moby_order.stop_loss
            self.__moby_order.close_reason = PositionCloseReason.Stoploss
        elif take_profit_executed:
            self.__moby_order.close_price = self.__moby_order.take_profit_price
            self.__moby_order.close_reason = PositionCloseReason.TakeProfit

        if self.__moby_order.close_reason is not None:
            self.__moby_order.close_time = current_candle.close_time
            self.__close_order()

    def __close_order(self):
        """Resetea variables de orden, avisa de orden cerrada, y actualiza las metricas del backtest"""

        moby_order = self.__moby_order
        self.__reset_opened_position_variables()

        moby_order.update_metrics()

        msg = "[SALIDA][{0}][{1}]" \
              "\nOrden abierta a las: {2}" \
              "\nOrden cerrada a las: {3}" \
              "\nPrecio entrada: {4}" \
              "\nPrecio salida: {5}" \
              "\nCerrada por: {6}" \
              "\nBeneficio: {7}".format(
                moby_order.ticker,
                moby_order.position,
                str(datetime_utc_to_madrid(moby_order.open_time)),
                str(datetime_utc_to_madrid(moby_order.close_time)),
                moby_order.open_price,
                moby_order.close_price,
                moby_order.close_reason,
                percentage_to_str(moby_order.profit_percent))

        print(msg)
        print()

        coin_results = self.__result_per_coin[moby_order.ticker]
        coin_results.orders.append(moby_order)

    def __write_backtest_results(self, start_time: datetime, interval: str, order_label: str, full_report: bool = False):
        """Muestra y escribe en fichero los resultados del Backtest"""

        msg = ""
        msg += "---------------------------------------------\n"
        msg += "-------------BACKTEST FINALIZADO-------------\n"
        msg += "---------------------------------------------\n"
        msg += order_label + "\n"
        msg += " Analizadas velas de {0} desde el {1}\n".format(interval, start_time.date())
        msg += "\n"
        msg += "RESULTADO POR MONEDA:\n"

        for coin, result in self.__result_per_coin.items():
            result.init_metrics()
            msg += " {0:<12}\tProf: {1:<8}\tMedio: {2:<6}\tÓrdenes: {3:<3}\tAciert: {4:<7}\tDD({5}): {6:<7}\tDDRelativo({7}:{8}): {9}\tScore: {10} \tP75: {11} \tP95: {12} \n".format(
                coin,
                percentage_to_str(result.profit),
                percentage_to_str(result.average_profit),
                len(result.orders),
                percentage_to_str(result.success, False),
                result.drawdown.historic_dd_time.date() if result.drawdown.historic_dd_time is not None else "YYYY-MM-DD",
                percentage_to_str(result.drawdown.historic_dd, False),
                result.drawdown.relative_dd_start.date() if result.drawdown.relative_dd_start is not None else "YYYY-MM-DD",
                result.drawdown.relative_dd_end.date() if result.drawdown.relative_dd_end is not None else "YYYY-MM-DD",
                percentage_to_str(result.drawdown.relative_dd, False),
                round(result.target_score, 2),
                round(result.get_profit_percentile(75), 3),
                round(result.get_profit_percentile(95), 3)
            )
        msg += "\n"

        all_results = self.__get_all_results()

        msg += "RENDIMIENTO DIARIO:\n"
        daily_profit = Backtesting.__get_daily_results(all_results.orders)
        for day, result in daily_profit.items():
            msg += "{0:<16}\tProf: {1:<8}\tMedio: {2:<6}\tÓrdenes: {3:<3}\tAciert: {4:<7}\n".format(
                day + ":",
                percentage_to_str(result.profit),
                percentage_to_str(result.average_profit),
                len(result.orders),
                percentage_to_str(result.success, False)
            )
        msg += "\n"

        msg += "RENDIMIENTO MENSUAL:\n"
        monthly_profit = Backtesting.__get_monthly_results(all_results.orders)
        for month, result in monthly_profit.items():
            msg += "{0:<16}\tProf: {1:<8}\tMedio: {2:<6}\tÓrdenes: {3:<3}\tAciert: {4:<7}\n".format(
                month + ":",
                percentage_to_str(result.profit),
                percentage_to_str(result.average_profit),
                len(result.orders),
                percentage_to_str(result.success, False)
            )
        msg += "\n"

        msg += "RESULTADO TOTAL:\n"
        msg += " " + all_results.get_full_summary() + "\n"
        msg += "\n"
        msg += "\n"

        all_results_with_commissions = all_results.copy_with_commissions()
        msg += "RESULTADO TOTAL CON COMISIONES (-{0}%):\n".format(BacktestResult.binance_commission_percent)
        msg += " " + all_results_with_commissions.get_full_summary() + "\n"
        msg += "\n"
        msg += "\n"

        if full_report:
            for order in sorted(all_results.orders, key=lambda order: order.close_time, reverse=False):
                msg = "[{0}][{1}]" \
                       "\nOrden abierta a las: {2}" \
                       "\nOrden cerrada a las: {3}" \
                       "\nPrecio entrada: {4}" \
                       "\nPrecio salida: {5}" \
                       "\nBeneficio: {6}\n\n".format(
                    order.ticker,
                    order.position,
                    str(datetime_utc_to_madrid(order.open_time)),
                    str(datetime_utc_to_madrid(order.close_time)),
                    order.open_price,
                    order.close_price,
                    percentage_to_str(order.profit_percent)
                ) + msg

        print()
        print(msg)

        if full_report:
            filename = "results/{0} {1}".format(order_label, self.__start_datetime).replace(":", ".")
            os.makedirs(os.path.dirname(filename), exist_ok=True)
            with open(filename + ".txt", "wb") as text_file:
                text_file.write(msg.encode("UTF-8"))

            Backtesting.__generate_accumulated_profit_plot(all_results.orders, filename)
            Backtesting.__generate_accumulated_profit_plot(all_results_with_commissions.orders, filename + " WithCommissions")

            Backtesting.__write_csv_summary(all_results.orders, filename)
            Backtesting.__write_csv_summary(all_results_with_commissions.orders, filename + " WithCommissions")

    @staticmethod
    def __write_csv_summary(orders: List[MobyOrder], filename: str):
        """Generate a csv with all the positions"""

        sorted_orders = sorted(orders, key=lambda order: order.close_time, reverse=False)

        with open(filename + ".csv", 'w', newline='') as f:
            writer = csv.writer(f)
            writer.writerow([
                "order_ticker",
                "order_profit_amount",
                "order_profit_percent",
                "order_price",
                "order_open_price",
                "order_close_price",
                "order_open_time",
                "order_close_time",
                "order_close_reason",
                "order_quantity",
                "order_stop_loss",
                "order_position",
                "order_label",
                "order_leverage",
                "order_trailing_stop",
                "order_trailing_stop_activation_percent",
                "order_trailing_stop_activation_price",
                "order_take_profit_percent",
                "order_take_profit_price"
            ])
            for order in sorted_orders:
                writer.writerow([
                    order.ticker,
                    order.profit_amount,
                    order.profit_percent,
                    order.order_price,
                    order.open_price,
                    order.close_price,
                    datetime_utc_to_madrid(order.open_time),
                    datetime_utc_to_madrid(order.close_time),
                    order.close_reason,
                    order.quantity,
                    order.stop_loss,
                    order.position,
                    order.order_label,
                    order.leverage,
                    order.trailing_stop,
                    order.trailing_stop_activation_percent,
                    order.trailing_stop_activation_price,
                    order.take_profit_percent,
                    order.take_profit_price
                ])

    @staticmethod
    def __generate_accumulated_profit_plot(orders: List[MobyOrder], filename: str):
        """Generate a plot with the accumulated profit by time"""
        # https://matplotlib.org/stable/gallery/text_labels_and_annotations/date.html
        # https://matplotlib.org/stable/faq/howto_faq.html#generate-images-without-having-a-window-appear

        sorted_orders = sorted(orders, key=lambda order: order.close_time, reverse=False)

        x = [order.close_time for order in sorted_orders]
        total_profit = []
        short_profit = []
        long_profit = []

        accumulated_profit = 0
        short_accumulated_profit = 0
        long_accumulated_profit = 0
        for order in sorted_orders:
            accumulated_profit += order.profit_percent * 100
            total_profit.append(accumulated_profit)

            if order.position == OrderPosition.Long:
                long_accumulated_profit += order.profit_percent * 100
                long_profit.append(long_accumulated_profit)
            else:
                long_profit.append(0)

            if order.position == OrderPosition.Short:
                short_accumulated_profit += order.profit_percent * 100
                short_profit.append(short_accumulated_profit)
            else:
                short_profit.append(0)

        plt.plot(x, total_profit, label="Total", color="blue")
        plt.plot(x, long_profit, label="Long", color="green")
        plt.plot(x, short_profit, label="Short", color="red")
        plt.legend()
        plt.grid()
        plt.gca().yaxis.set_major_formatter(mtick.PercentFormatter())

        # size
        plt.gcf().set_dpi(100)
        plt.gcf().set_size_inches(20, 10)

        # beautify the x-labels
        plt.gcf().autofmt_xdate()

        plt.savefig(filename + ".png")

        # Clear figure
        plt.clf()

    @staticmethod
    def __write_podium(backtesters: Dict[str, 'Backtesting'], with_commissions: bool):
        """Compare finshed pairs label-backtesters into a podium of different parameters"""

        backtesters_results = {label: backtester.__get_all_results(with_commissions) for label, backtester in backtesters.items()}

        score_podium = dict(sorted(backtesters_results.items(), key=lambda item: item[1].target_score, reverse=True))
        profit_podium = dict(sorted(backtesters_results.items(), key=lambda item: item[1].profit, reverse=True))
        drawdown_podium = dict(sorted(backtesters_results.items(), key=lambda item: item[1].drawdown.relative_dd, reverse=True))

        msg = "PODIUMS"
        if with_commissions:
            msg += " WITH COMMISIONS"
        msg += "\n\n"
        msg += "SCORE PODIUM:\n" + "\n".join([result.get_small_summary() + "\t" + label for label, result in score_podium.items()])
        msg += "\n\n"
        msg += "PROFIT PODIUM:\n" + "\n".join([result.get_small_summary() + "\t" + label for label, result in profit_podium.items()])
        msg += "\n\n"
        msg += "DRAWDOWN PODIUM:\n" + "\n".join([result.get_small_summary() + "\t" + label for label, result in drawdown_podium.items()])

        print()
        print()
        print(msg)
        print()

        filename = "results/PODIUM {0}".format(datetime.now()).replace(":", ".")
        if with_commissions:
            filename += " WithCommissions"
        os.makedirs(os.path.dirname(filename), exist_ok=True)
        with open(filename + ".txt", "wb") as text_file:
            text_file.write(msg.encode("UTF-8"))

    @staticmethod
    def __get_monthly_results(orders: List[MobyOrder]) -> Dict[str, BacktestResult]:
        """Get the total result by month"""
        monthly_results = dict()

        sorted_orders = sorted(orders, key=lambda order: order.close_time, reverse=False)
        for order in sorted_orders:
            month = str(order.close_time.year) + " " + Backtesting.__months_names[order.close_time.month]
            result = monthly_results.get(month, BacktestResult())
            result.orders.append(order)
            monthly_results[month] = result

        for result in monthly_results.values():
            result.init_metrics()

        return monthly_results

    @staticmethod
    def __get_daily_results(orders: List[MobyOrder]) -> Dict[str, BacktestResult]:
        """Get the total result by day"""
        daily_results = dict()

        sorted_orders = sorted(orders, key=lambda order: order.close_time, reverse=False)
        for order in sorted_orders:
            day = str(order.close_time.date())
            result = daily_results.get(day, BacktestResult())
            result.orders.append(order)
            daily_results[day] = result

        for result in daily_results.values():
            result.init_metrics()

        return daily_results

    def get_all_candles_from_start_time(self, coin, interval, start_time) -> List[Candlestick]:
        """Download a lot of candles with pagination. Save and reload them from disk."""
        all_candles = list()
        filename = "cache/candles_{0}_{1}_from_{2}".format(coin, interval, start_time.date())

        if os.path.isfile(filename):
            print("Cargando velas de fichero local")
            with open(filename, "rb") as f:
                all_candles = pickle.load(f)

        else:
            print("Descarga de velas de Binance API")
            next_start_time = start_time
            iter_count = 0
            new_candles = None
            binance_client = BinanceClient()
            while new_candles is None or len(new_candles) == 1500:
                iter_count += 1
                print("Descarga de velas. Paginacion:", iter_count, "Fecha:", next_start_time)
                new_candles = binance_client.get_last_candlesticks(coin=coin, start_time_utc=next_start_time,
                                                                          num_candlesticks=1500, interval=interval,
                                                                          futures_info=True)
                if new_candles:
                    all_candles += new_candles
                    next_start_time = new_candles[-1].close_time

            # Save candles as a file
            os.makedirs(os.path.dirname(filename), exist_ok=True)
            with open(filename, "wb") as f:
                pickle.dump(all_candles, f)

        print("Velas cargadas")

        return all_candles

    @staticmethod
    def compare_backtests(analyzers: list, start_time: datetime, processes: int = None):
        """Realiza y compara backtests sobre varias hipotesis implementada en varios analyzer"""

        if len({analyzer.order_label for analyzer in analyzers}) != len(analyzers):
            raise Exception("Hay order_label repetidos")
        all_coins_to_analyze = [analyzer.coins_to_analyze for analyzer in analyzers]
        coins_to_analyze = all_coins_to_analyze[0]
        if not all(element == coins_to_analyze for element in all_coins_to_analyze):
            raise Exception("Hay analyzers con diferentes coins_to_analyze")
        if len({analyzer.interval for analyzer in analyzers}) != 1:
            raise Exception("Hay analyzers con diferentes interval")

        backtesters: Dict[str, Backtesting] = {analyzer.order_label: Backtesting() for analyzer in analyzers}

        if processes is None:
            processes = cpu_count()

        if processes == 1:
            global all_candles
            for coin in coins_to_analyze:
                for analyzer in analyzers:
                    backtesters[analyzer.order_label].__backtest_single_coin(analyzer, start_time, coin, True)
                all_candles = None  # Liberamos recursos
        else:
            analyzers = [Backtesting.__clean_analyzer_for_multiprocessing(analyzer) for analyzer in analyzers]
            with Pool(processes) as p:
                results = p.starmap(Backtesting.thread_backtest_single_coin_with_many_analyzers,
                                    [(analyzers, start_time, coin) for coin in coins_to_analyze])
            for result_per_analyzer in results:
                for order_label, result_per_coin in result_per_analyzer.items():
                    backtesters[order_label].__result_per_coin.update(result_per_coin)

        for order_label, backtester in backtesters.items():
            backtester.__write_backtest_results(start_time, analyzers[0].interval, order_label, full_report=True)

        Backtesting.__write_podium(backtesters, False)
        Backtesting.__write_podium(backtesters, True)

    @staticmethod
    def thread_backtest_single_coin_with_many_analyzers(analyzers, start_time, coin) -> Dict[str, Dict[str, BacktestResult]]:
        all_result_per_coin = dict()
        for analyzer in analyzers:
            coin_backtest = Backtesting()
            coin_backtest.__backtest_single_coin(analyzer, start_time, coin, reuse_candles=True)
            all_result_per_coin[analyzer.order_label] = coin_backtest.__result_per_coin

        global all_candles
        all_candles = None  # Liberamos recursos

        return all_result_per_coin

    @staticmethod
    def __clean_analyzer_for_multiprocessing(analyzer):
        """
        Quitamos OrderSimulator y BinanceOrder del analyzer,
        que no hacen falta en backtest e impiden el multiproceso
        """
        analyzer_dict = analyzer.__dict__
        for key, value in analyzer_dict.items():
            if type(value) == OrderSimulator or type(value) == BinanceOrder:
                analyzer_dict[key] = None
        return analyzer


# Local Run
if __name__ == "__main__":
    backtest_analyzer = AnalyzerModelo()
    backtest_start_time = datetime(2021, 12, 1)

    start = datetime.utcnow()
    Backtesting().backtest(backtest_analyzer, backtest_start_time, processes=1)
    end = datetime.utcnow()

    time_elapsed = (end - start).total_seconds()
    print("Finished in", time_elapsed, "seconds.")
