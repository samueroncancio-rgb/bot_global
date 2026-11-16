"""Tests unitarios para el adaptador de Bybit Linear."""

import pytest

from obbot.bus.memory import InMemoryEventBus
from obbot.common.events import Side
from obbot.common.registry import InstrumentRegistry
from obbot.ingestion.bybit import BybitAdapter


def test_bybit_subscription_payload(sample_registry: InstrumentRegistry) -> None:
    bus = InMemoryEventBus()
    adapter = BybitAdapter(["BTC-USDT-PERP"], sample_registry, bus)
    payload = adapter.create_subscribe_payload()

    assert payload["op"] == "subscribe"
    assert "publicTrade.BTCUSDT" in payload["args"]
    assert "orderbook.50.BTCUSDT" in payload["args"]
    assert "tickers.BTCUSDT" in payload["args"]


def test_bybit_parse_trade(sample_registry: InstrumentRegistry) -> None:
    bus = InMemoryEventBus()
    adapter = BybitAdapter(["BTC-USDT-PERP"], sample_registry, bus)

    item = {
        "T": 1700000000000,
        "s": "BTCUSDT",
        "S": "Buy",
        "v": "0.25",
        "p": "50000.00",
        "i": "bybit-trade-1",
    }
    trade = adapter.parse_trade(item, "raw_bybit_trade")
    assert trade is not None
    assert trade.symbol == "BTC-USDT-PERP"
    assert trade.side == Side.BUY
    assert trade.price == 50000.0
    assert trade.size == 0.25
    assert trade.notional_usd == 12500.0


@pytest.mark.asyncio
async def test_bybit_orderbook_handling(sample_registry: InstrumentRegistry) -> None:
    bus = InMemoryEventBus()
    await bus.start()
    adapter = BybitAdapter(["BTC-USDT-PERP"], sample_registry, bus)

    snap_data = {
        "s": "BTCUSDT",
        "b": [["50000.0", "1.5"]],
        "a": [["50010.0", "2.0"]],
        "u": 100,
        "cts": 1700000000100,
    }
    await adapter._handle_orderbook("snapshot", snap_data, "raw_snap")
    book = adapter.books["BTC-USDT-PERP"]
    assert book.is_synchronized is True
    assert book.best_bid() == (50000.0, 1.5)
    assert book.best_ask() == (50010.0, 2.0)

    # Delta
    delta_data = {
        "s": "BTCUSDT",
        "b": [["50005.0", "0.5"]],
        "a": [["50010.0", "0.0"]],
        "u": 101,
        "cts": 1700000000200,
    }
    await adapter._handle_orderbook("delta", delta_data, "raw_delta")
    assert book.best_bid() == (50005.0, 0.5)
    assert book.best_ask() is None

    await bus.stop()
