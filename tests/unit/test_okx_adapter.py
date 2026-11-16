"""Tests unitarios para el adaptador de OKX."""

import pytest

from obbot.bus.memory import InMemoryEventBus
from obbot.common.events import Side
from obbot.common.registry import InstrumentRegistry
from obbot.ingestion.okx import OkxAdapter


def test_okx_subscription_payload(sample_registry: InstrumentRegistry) -> None:
    bus = InMemoryEventBus()
    adapter = OkxAdapter(["BTC-USDT-PERP"], sample_registry, bus)
    payload = adapter.create_subscribe_payload()

    assert payload["op"] == "subscribe"
    channels = {arg["channel"] for arg in payload["args"]}
    assert channels == {"trades", "books", "funding-rate", "open-interest"}
    inst_ids = {arg["instId"] for arg in payload["args"]}
    assert inst_ids == {"BTC-USDT-SWAP"}


def test_okx_trade_normalization_with_multiplier(sample_registry: InstrumentRegistry) -> None:
    bus = InMemoryEventBus()
    adapter = OkxAdapter(["BTC-USDT-PERP"], sample_registry, bus)

    # En OKX 1 contrato SWAP de BTC = 0.01 BTC
    # 250 contratos = 2.5 BTC
    raw_item = {
        "instId": "BTC-USDT-SWAP",
        "tradeId": "999888",
        "px": "50000.0",
        "sz": "250",
        "side": "buy",
        "ts": "1700000000000",
    }
    trade_evt = adapter.parse_trade(raw_item, "raw_okx_str")
    assert trade_evt is not None
    assert trade_evt.symbol == "BTC-USDT-PERP"
    assert trade_evt.side == Side.BUY
    assert trade_evt.price == 50000.0
    # Normalizado: 250 * 0.01 = 2.50 BTC
    assert trade_evt.size == 2.5
    # Notional: 2.5 * 50000 = 125000 USD
    assert trade_evt.notional_usd == 125000.0


@pytest.mark.asyncio
async def test_okx_book_snapshot_and_delta(sample_registry: InstrumentRegistry) -> None:
    bus = InMemoryEventBus()
    await bus.start()
    adapter = OkxAdapter(["BTC-USDT-PERP"], sample_registry, bus)

    arg = {"channel": "books", "instId": "BTC-USDT-SWAP"}
    data = [
        {
            "ts": "1700000000100",
            # [price, size_contracts, liquidated_orders, num_orders]
            "bids": [["50000.0", "100", "0", "1"]],  # 100 contratos = 1.0 BTC
            "asks": [["50010.0", "50", "0", "1"]],  # 50 contratos = 0.5 BTC
        }
    ]

    await adapter._handle_books(arg, "snapshot", data, "raw_snap")
    book = adapter.books["BTC-USDT-PERP"]
    assert book.is_synchronized is True
    assert book.best_bid() == (50000.0, 1.0)
    assert book.best_ask() == (50010.0, 0.5)

    # Aplicar delta
    delta_data = [
        {
            "ts": "1700000000200",
            "bids": [["50000.0", "200", "0", "2"]],  # actualiza a 2.0 BTC
            "asks": [["50010.0", "0", "0", "0"]],  # elimina ask
        }
    ]
    await adapter._handle_books(arg, "update", delta_data, "raw_delta")
    assert book.best_bid() == (50000.0, 2.0)
    assert book.best_ask() is None

    await bus.stop()
