"""Tests unitarios para el adaptador de Hyperliquid L1."""

import pytest

from obbot.bus.memory import InMemoryEventBus
from obbot.common.events import Side
from obbot.common.registry import InstrumentRegistry
from obbot.ingestion.hyperliquid import HyperliquidAdapter


def test_hyperliquid_subscriptions(sample_registry: InstrumentRegistry) -> None:
    bus = InMemoryEventBus()
    adapter = HyperliquidAdapter(["BTC-USDT-PERP"], sample_registry, bus)
    subs = adapter.create_subscriptions()

    assert len(subs) == 2
    types = {s["subscription"]["type"] for s in subs}
    coins = {s["subscription"]["coin"] for s in subs}
    assert types == {"l2Book", "trades"}
    assert coins == {"BTC"}


def test_hyperliquid_parse_trade(sample_registry: InstrumentRegistry) -> None:
    bus = InMemoryEventBus()
    adapter = HyperliquidAdapter(["BTC-USDT-PERP"], sample_registry, bus)

    # Buy trade
    item_buy = {
        "coin": "BTC",
        "side": "B",
        "px": "50000.0",
        "sz": "0.1",
        "time": 1700000000000,
        "tid": 112233,
    }
    trade_buy = adapter.parse_trade(item_buy, "raw_hl_buy")
    assert trade_buy is not None
    assert trade_buy.symbol == "BTC-USDT-PERP"
    assert trade_buy.side == Side.BUY
    assert trade_buy.price == 50000.0
    assert trade_buy.size == 0.1
    assert trade_buy.notional_usd == 5000.0

    # Sell trade (side 'A')
    item_sell = {
        "coin": "BTC",
        "side": "A",
        "px": "49990.0",
        "sz": "0.2",
        "time": 1700000000100,
        "tid": 112234,
    }
    trade_sell = adapter.parse_trade(item_sell, "raw_hl_sell")
    assert trade_sell is not None
    assert trade_sell.side == Side.SELL


@pytest.mark.asyncio
async def test_hyperliquid_l2_book_handling(sample_registry: InstrumentRegistry) -> None:
    bus = InMemoryEventBus()
    await bus.start()
    adapter = HyperliquidAdapter(["BTC-USDT-PERP"], sample_registry, bus)

    l2_data = {
        "coin": "BTC",
        "time": 1700000000200,
        "levels": [
            [{"px": "50000.0", "sz": "1.2", "n": 3}],  # bids
            [{"px": "50010.0", "sz": "0.8", "n": 2}],  # asks
        ],
    }
    await adapter._handle_l2_book(l2_data, "raw_hl_book")
    book = adapter.books["BTC-USDT-PERP"]
    assert book.is_synchronized is True
    assert book.best_bid() == (50000.0, 1.2)
    assert book.best_ask() == (50010.0, 0.8)

    await bus.stop()
