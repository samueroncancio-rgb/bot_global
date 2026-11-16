"""Modelos de datos inmutables y de alto rendimiento para eventos del mercado y ejecución."""

from dataclasses import dataclass
from enum import StrEnum
from typing import Any


class Side(StrEnum):
    """Lado de una operación u orden."""

    BUY = "BUY"
    SELL = "SELL"


class OrderType(StrEnum):
    """Tipo de orden admitido."""

    LIMIT = "LIMIT"
    MARKET = "MARKET"
    STOP_MARKET = "STOP_MARKET"
    TAKE_PROFIT_MARKET = "TAKE_PROFIT_MARKET"


class OrderStatus(StrEnum):
    """Estado del ciclo de vida de una orden."""

    PENDING_NEW = "PENDING_NEW"
    NEW = "NEW"
    PARTIALLY_FILLED = "PARTIALLY_FILLED"
    FILLED = "FILLED"
    CANCELLED = "CANCELLED"
    REJECTED = "REJECTED"
    EXPIRED = "EXPIRED"


class HealthStatus(StrEnum):
    """Estado de salud de un componente del sistema."""

    OK = "OK"
    DEGRADED = "DEGRADED"
    ERROR = "ERROR"
    DISCONNECTED = "DISCONNECTED"


@dataclass(slots=True, frozen=True)
class OrderBookLevel:
    """Nivel de precio y volumen en un libro de órdenes."""

    price: float
    size: float  # En moneda base


@dataclass(slots=True, frozen=True)
class EventBase:
    """Clase base de eventos con marcas de tiempo obligatorias."""

    ts_recv_ns: int  # Timestamp de recepción en nanosegundos (reloj local)


@dataclass(slots=True, frozen=True)
class MarketEvent(EventBase):
    """Clase base para eventos de mercado provenientes de exchanges."""

    exchange: str
    symbol: str  # Símbolo canónico global (ej. BTC-USDT-PERP)
    ts_exchange_ms: int  # Timestamp del motor de matching del exchange en milisegundos
    raw: str = ""  # Payload crudo serializado para persistencia en capa Bronze


@dataclass(slots=True, frozen=True)
class TradeEvent(MarketEvent):
    """Evento normalizado de trade ejecutado."""

    trade_id: str = ""
    price: float = 0.0
    size: float = 0.0  # Tamaño normalizado en moneda base
    notional_usd: float = 0.0  # Valor nocional en USD
    side: Side = Side.BUY  # Lado del agresor/taker


@dataclass(slots=True, frozen=True)
class BookSnapshotEvent(MarketEvent):
    """Snapshot completo de los mejores niveles del libro de órdenes."""

    bids: tuple[OrderBookLevel, ...] = ()
    asks: tuple[OrderBookLevel, ...] = ()
    last_update_id: int = 0


@dataclass(slots=True, frozen=True)
class BookDeltaEvent(MarketEvent):
    """Diferencias incrementales de profundidad de libro."""

    bids: tuple[OrderBookLevel, ...] = ()
    asks: tuple[OrderBookLevel, ...] = ()
    first_update_id: int = 0
    final_update_id: int = 0
    prev_final_update_id: int = 0


@dataclass(slots=True, frozen=True)
class LiquidationEvent(MarketEvent):
    """Evento de liquidación forzada en el exchange."""

    side: Side = Side.BUY  # Lado de la orden liquidada
    price: float = 0.0
    size: float = 0.0  # En moneda base
    notional_usd: float = 0.0


@dataclass(slots=True, frozen=True)
class FundingOIEvent(MarketEvent):
    """Tasa de financiación e Interés Abierto (Open Interest)."""

    funding_rate: float = 0.0
    open_interest: float = 0.0  # En moneda base
    open_interest_usd: float = 0.0  # Nocional en USD
    next_funding_time_ms: int = 0


@dataclass(slots=True, frozen=True)
class HealthEvent(EventBase):
    """Métrica de latencia y estado operativo de un componente."""

    component: str = ""
    status: HealthStatus = HealthStatus.OK
    latency_ms: float = 0.0
    details: str = ""


@dataclass(slots=True, frozen=True)
class OrderEvent(EventBase):
    """Evento de estado de orden en ejecución."""

    client_order_id: str = ""
    exchange_order_id: str = ""
    symbol: str = ""
    side: Side = Side.BUY
    order_type: OrderType = OrderType.LIMIT
    price: float = 0.0
    size: float = 0.0
    filled_size: float = 0.0
    status: OrderStatus = OrderStatus.PENDING_NEW
    is_algo: bool = False
    is_reduce_only: bool = False
    ts_exchange_ms: int = 0
    error_message: str = ""


def event_to_dict(event: EventBase) -> dict[str, Any]:
    """Convierte un evento en un diccionario ligero para serialización."""
    result: dict[str, Any] = {}
    for field_name in event.__dataclass_fields__:
        val = getattr(event, field_name)
        if isinstance(val, (tuple, list)):
            if val and isinstance(val[0], OrderBookLevel):
                result[field_name] = [[level.price, level.size] for level in val]
            else:
                result[field_name] = list(val)
        elif isinstance(val, (Side, OrderType, OrderStatus, HealthStatus)):
            result[field_name] = val.value
        else:
            result[field_name] = val
    return result
