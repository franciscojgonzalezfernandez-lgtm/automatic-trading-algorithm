from copy import deepcopy
from datetime import datetime
from typing import List

import numpy as np

from core.order.moby_order import MobyOrder
from core.utils.utils import percentage_to_str


class BacktestResult:
    """Backtest result information"""

    binance_commission_percent = 0.08  # -0.08%

    class Drawdown:
        """Historic and relative drawdown information"""

        def __init__(self):
            self.historic_dd: float = 0
            self.historic_dd_time: datetime = None

            self.relative_dd: float = 0
            self.relative_dd_start: datetime = None
            self.relative_dd_end: datetime = None

    def __init__(self):
        self.orders: List[MobyOrder] = list()

        self.profit = 0
        self.average_profit = 0
        self.profit_np_array = None
        self.success = 0
        self.drawdown: BacktestResult.Drawdown = None

        # Custom score to compare results
        self.target_score = None

        self.with_commissions = False

    def init_metrics(self):
        self.profit = sum([order.profit_percent for order in self.orders]) if self.orders else 0
        self.average_profit = self.profit / len(self.orders) if self.orders else 0
        self.success = len([None for order in self.orders if order.profit_percent > 0]) / len(
            self.orders) if self.orders else 0
        self.drawdown = BacktestResult.get_drawdown(self.orders)
        self.profit_np_array = np.array([order.profit_percent for order in self.orders])

        self.calculate_target_score()

    def get_profit_percentile(self, percentile):
        if self.profit_np_array is not None and len(self.profit_np_array) > 0:
            return np.percentile(self.profit_np_array, percentile)
        return 0.0

    def calculate_target_score(self):
        if not self.orders or self.drawdown.relative_dd == 0 or self.profit <= 0:
            self.target_score = 0  # No nos vale un backtest sin ordenes, sin ordenes negativas, o sin profit
        else:
            first_date = min(order.open_time for order in self.orders).date()
            last_date = max(order.open_time for order in self.orders).date()
            total_days = (last_date - first_date).days + 1
            self.target_score = 100 * ((self.profit / (self.drawdown.relative_dd ** 2)) / total_days)

    def copy_with_commissions(self) -> 'BacktestResult':
        if self.with_commissions:
            raise Exception("Commissions already applied")
        result_with_commissions = BacktestResult()
        result_with_commissions.with_commissions = True
        result_with_commissions.orders = deepcopy(self.orders)
        for order in result_with_commissions.orders:
            order.profit_percent -= (BacktestResult.binance_commission_percent / 100)
        result_with_commissions.init_metrics()
        return result_with_commissions

    def get_full_summary(self) -> str:
        return (
        "Prof: {0:<8}\tMedio: {1:<6}\tÓrdenes: {2:<3}\tAciert: {3:<7}\tDD({4}): {5:<7}\tDDRelativo({6}:{7}): {8:<7}\tScore: {9} \tP75: {10} \tP90: {11}".format(
            percentage_to_str(self.profit),
            percentage_to_str(self.average_profit),
            len(self.orders),
            percentage_to_str(self.success, False),
            self.drawdown.historic_dd_time.date() if self.drawdown.historic_dd_time is not None else "YYYY-MM-DD",
            percentage_to_str(self.drawdown.historic_dd, False),
            self.drawdown.relative_dd_start.date() if self.drawdown.relative_dd_start is not None else "YYYY-MM-DD",
            self.drawdown.relative_dd_end.date() if self.drawdown.relative_dd_end is not None else "YYYY-MM-DD",
            percentage_to_str(self.drawdown.relative_dd, False),
            round(self.target_score, 2),
            round(self.get_profit_percentile(75), 3),
            round(self.get_profit_percentile(95), 3)
        )
        )

    def get_small_summary(self) -> str:
        return ("Score: {0:<6}\tProf: {1:<8}\tDDRelativo({2}:{3}): {4:<7}".format(
            round(self.target_score, 2),
            percentage_to_str(self.profit),
            self.drawdown.relative_dd_start.date() if self.drawdown.relative_dd_start is not None else "YYYY-MM-DD",
            self.drawdown.relative_dd_end.date() if self.drawdown.relative_dd_end is not None else "YYYY-MM-DD",
            percentage_to_str(self.drawdown.relative_dd, False)
        )
        )

    @staticmethod
    def get_drawdown(orders: List[MobyOrder]) -> Drawdown:
        """Get the maximum historic and relative drawdown for a sequence of orders"""
        drawdown = BacktestResult.Drawdown()

        current_profit = 0
        max_historic_profit = 0
        max_historic_profit_time = None
        min_relative_profit = 0

        sorted_orders = sorted(orders, key=lambda order: order.close_time, reverse=False)
        for order in sorted_orders:
            current_profit += order.profit_percent

            if current_profit <= drawdown.historic_dd:
                drawdown.historic_dd = current_profit
                drawdown.historic_dd_time = order.close_time

            if current_profit >= max_historic_profit:
                max_historic_profit = current_profit
                max_historic_profit_time = order.close_time
                min_relative_profit = current_profit

            elif current_profit <= min_relative_profit:
                min_relative_profit = current_profit
                current_relative_dd = current_profit - max_historic_profit
                if current_relative_dd < drawdown.relative_dd:
                    drawdown.relative_dd = current_relative_dd
                    drawdown.relative_dd_start = max_historic_profit_time
                    drawdown.relative_dd_end = order.close_time

        return drawdown
