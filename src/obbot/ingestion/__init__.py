"""Módulo de ingestión de datos en tiempo real multi-exchange."""

from obbot.ingestion.base import BaseExchangeAdapter
from obbot.ingestion.binance import (
    ALLOWED_MARKET_STREAMS,
    ALLOWED_PUBLIC_STREAMS,
    BinanceAdapter,
    BinanceCategoryViolationError,
    validate_binance_stream_category,
)
from obbot.ingestion.bybit import BybitAdapter
from obbot.ingestion.hyperliquid import HyperliquidAdapter
from obbot.ingestion.okx import OkxAdapter
from obbot.ingestion.orderbook.book import OrderBookL2
from obbot.ingestion.orderbook.gap_detector import (
    BinanceGapDetector,
    BufferedDelta,
    SyncState,
)
from obbot.ingestion.supervisor import ConnectionState, WsSupervisor

__all__ = [
    "ALLOWED_MARKET_STREAMS",
    "ALLOWED_PUBLIC_STREAMS",
    "BaseExchangeAdapter",
    "BinanceAdapter",
    "BinanceCategoryViolationError",
    "BinanceGapDetector",
    "BufferedDelta",
    "BybitAdapter",
    "ConnectionState",
    "HyperliquidAdapter",
    "OkxAdapter",
    "OrderBookL2",
    "SyncState",
    "WsSupervisor",
    "validate_binance_stream_category",
]
