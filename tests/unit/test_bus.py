"""Tests unitarios para el bus de eventos en memoria."""

import pytest

from obbot.bus.memory import InMemoryEventBus
from obbot.common.events import EventBase, Side, TradeEvent


@pytest.mark.asyncio
async def test_in_memory_bus_publish_subscribe() -> None:
    bus = InMemoryEventBus()
    await bus.start()

    received_trades: list[TradeEvent] = []

    async def trade_handler(topic: str, event: EventBase) -> None:
        if isinstance(event, TradeEvent):
            received_trades.append(event)

    # Suscribirse con wildcard
    await bus.subscribe("trades.*", trade_handler)

    sample_trade = TradeEvent(
        exchange="binance",
        symbol="BTC-USDT-PERP",
        ts_exchange_ms=1700000000000,
        ts_recv_ns=1700000000000000000,
        trade_id="t-1",
        price=50000.0,
        size=1.0,
        notional_usd=50000.0,
        side=Side.BUY,
    )

    await bus.publish("trades.BTC-USDT-PERP", sample_trade)
    await bus.drain()

    assert len(received_trades) == 1
    assert received_trades[0].trade_id == "t-1"

    # Tópico no coincidente no entrega eventos
    await bus.publish("book.BTC-USDT-PERP", sample_trade)
    await bus.drain()
    assert len(received_trades) == 1

    # Desuscribir
    await bus.unsubscribe("trades.*", trade_handler)
    await bus.publish("trades.BTC-USDT-PERP", sample_trade)
    await bus.drain()
    assert len(received_trades) == 1

    await bus.stop()


@pytest.mark.asyncio
async def test_bus_handler_exception_isolation() -> None:
    bus = InMemoryEventBus()
    await bus.start()

    successful_events: list[str] = []

    async def faulty_handler(topic: str, event: EventBase) -> None:
        raise RuntimeError("Fallo simulado en handler")

    async def normal_handler(topic: str, event: EventBase) -> None:
        if isinstance(event, TradeEvent):
            successful_events.append(event.trade_id)

    await bus.subscribe("trades.v1", faulty_handler)
    await bus.subscribe("trades.v1", normal_handler)

    trade = TradeEvent(
        exchange="binance",
        symbol="BTC-USDT-PERP",
        ts_exchange_ms=1000,
        ts_recv_ns=1000000,
        trade_id="safe-trade",
    )

    await bus.publish("trades.v1", trade)
    await bus.drain()

    assert successful_events == ["safe-trade"]
    await bus.stop()
