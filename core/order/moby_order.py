import datetime
import uuid
from enum import Enum


class OrderPosition(str, Enum):
    Long = "Long"
    Short = "Short"


class OrderStatus(str, Enum):
    Created = "Created"
    Open = "Open"
    Close = "Close"


class OrderMode(str, Enum):
    Real = "Real"
    Simulated = "Simulated"
    Backtest = "Backtest"


class PositionCloseReason(str, Enum):
    Stoploss = "Stoploss"
    TrailingStop = "TrailingStop"
    TakeProfit = "TakeProfit"


class MobyOrder:
    """Representa la información de una orden"""

    def __init__(self, ticker: str,
                 order_price: float = None,
                 quantity: float = None,
                 stop_loss: float = None,
                 position: OrderPosition = None,
                 order_label: str = "",
                 order_description: str = "",
                 leverage: int = 1,
                 trailing_stop: float = None,
                 trailing_stop_activation_percent: float = None,
                 trailing_stop_activation_price: float = None,
                 take_profit_percent: float = None,
                 take_profit_price: float = None):

        # Internal Config
        self.version: str = "1.0.3"

        # Metas
        self.id: str = str(uuid.uuid4())
        self.order_label: str = order_label
        self.order_description: str = order_description
        self.status: OrderStatus = OrderStatus.Created
        self.order_mode: OrderMode = OrderMode.Simulated
        self.creation_time: datetime.datetime = datetime.datetime.utcnow()

        # Info Entrada
        self.ticker: str = ticker
        self.position: OrderPosition = position
        self.leverage: int = leverage
        self.stop_loss: float = stop_loss
        self.trailing_stop: float = trailing_stop
        self.trailing_stop_activation_percent: float = trailing_stop_activation_percent
        self.trailing_stop_activation_price: float = trailing_stop_activation_price
        self.quantity: float = quantity
        self.order_price: float = order_price
        self.take_profit_price: float = take_profit_price

        # ALERTA: Los simuladores no estan preparados para reproducir este valor
        # Estamos mezclando conceptos de ROE
        # Usar mejor el take_profit_price
        self.take_profit_percent: float = take_profit_percent

        # Info posición abierta
        self.open_time: datetime.datetime = None
        self.open_price: float = None

        # Info posición cerrada
        self.close_price: float = None
        self.close_reason: PositionCloseReason = None
        self.close_time: datetime.datetime = None

        # Metricas
        self.profit_amount: float = None
        self.profit_percent: float = None
        self.positive_order: bool = None
        self.profit_usdt: float = None

        # Info para AccountManager()
        self.commission: float = 0
        self.commission_asset: str = None  # USDT / BNB
        self.open_order_id: int = None
        self.close_order_id: int = None
        self.account: str = None

    def update_metrics(self):

        if self.close_price is not None and self.open_price is not None:
            if self.position == OrderPosition.Long:
                self.profit_amount = self.close_price - self.open_price

            elif self.position == OrderPosition.Short:
                self.profit_amount = self.open_price - self.close_price

            self.profit_percent = self.profit_amount / self.open_price
            self.positive_order = self.profit_amount > 0.0

            if self.quantity is not None:
                self.profit_usdt = self.quantity * self.profit_percent * self.leverage

    def get_ROE(self):

        if self.take_profit_price is not None:
            result = (self.take_profit_price - self.order_price) / self.order_price * 100 * self.leverage
            if self.position == self.position.Short:
                result = -result
            return result
        return None

    def get_LOSS(self):

        if self.stop_loss is not None:
            result = (self.stop_loss - self.order_price) / self.order_price * 100 * self.leverage
            if self.position == self.position.Long:
                result = -result
            return result
        return None