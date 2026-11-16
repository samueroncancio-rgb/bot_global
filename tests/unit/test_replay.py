"""Tests unitarios para el motor de reproducción determinista (DeterministicReplayEngine)."""

import json

import pytest
import zstandard as zstd

from obbot.bus.memory import InMemoryEventBus
from obbot.common.clock import SimulationClock
from obbot.common.events import EventBase, TradeEvent
from obbot.lakehouse.replay import DeterministicReplayEngine
from obbot.lakehouse.s3 import LocalStorageBackend


@pytest.mark.asyncio
async def test_deterministic_replay_ordering_and_hash(tmp_path: pytest.TempPathFactory) -> None:
    storage = LocalStorageBackend(base_dir=str(tmp_path))
    bus = InMemoryEventBus()
    await bus.start()
    clock = SimulationClock(initial_time_ns=0)

    compressor = zstd.ZstdCompressor(level=3)

    # Crear 2 archivos bronze simulando feeds de exchanges distintos con timestamps entrelazados
    # Archivo 1 (Binance): t = 100ns, 300ns
    events_binance = [
        {
            "topic": "trades.BTC-USDT-PERP",
            "exchange": "binance",
            "symbol": "BTC-USDT-PERP",
            "event_type": "TradeEvent",
            "ts_recv_ns": 100,
            "ts_exchange_ms": 1,
            "data": {
                "trade_id": "b-1",
                "price": 50000.0,
                "size": 1.0,
                "notional_usd": 50000.0,
                "side": "BUY",
            },
        },
        {
            "topic": "trades.BTC-USDT-PERP",
            "exchange": "binance",
            "symbol": "BTC-USDT-PERP",
            "event_type": "TradeEvent",
            "ts_recv_ns": 300,
            "ts_exchange_ms": 3,
            "data": {
                "trade_id": "b-2",
                "price": 50020.0,
                "size": 0.5,
                "notional_usd": 25010.0,
                "side": "SELL",
            },
        },
    ]
    raw_b = "\n".join(json.dumps(r) for r in events_binance).encode("utf-8")
    await storage.put_object("bronze", "binance/part1.json.zst", compressor.compress(raw_b))

    # Archivo 2 (OKX): t = 200ns, 400ns
    events_okx = [
        {
            "topic": "trades.BTC-USDT-PERP",
            "exchange": "okx",
            "symbol": "BTC-USDT-PERP",
            "event_type": "TradeEvent",
            "ts_recv_ns": 200,
            "ts_exchange_ms": 2,
            "data": {
                "trade_id": "o-1",
                "price": 50010.0,
                "size": 0.8,
                "notional_usd": 40008.0,
                "side": "BUY",
            },
        },
        {
            "topic": "trades.BTC-USDT-PERP",
            "exchange": "okx",
            "symbol": "BTC-USDT-PERP",
            "event_type": "TradeEvent",
            "ts_recv_ns": 400,
            "ts_exchange_ms": 4,
            "data": {
                "trade_id": "o-2",
                "price": 50015.0,
                "size": 1.2,
                "notional_usd": 60018.0,
                "side": "SELL",
            },
        },
    ]
    raw_o = "\n".join(json.dumps(r) for r in events_okx).encode("utf-8")
    await storage.put_object("bronze", "okx/part1.json.zst", compressor.compress(raw_o))

    received_events: list[TradeEvent] = []

    async def trade_handler(topic: str, event: EventBase) -> None:
        if isinstance(event, TradeEvent):
            received_events.append(event)

    await bus.subscribe("trades.*", trade_handler)

    engine = DeterministicReplayEngine(storage, bus, clock, bronze_bucket="bronze")
    keys = ["binance/part1.json.zst", "okx/part1.json.zst"]

    # Ejecución 1
    total1, hash1 = await engine.run(keys)
    await bus.drain()

    assert total1 == 4
    # Comprobar ordenación cronológica estricta: 100 -> 200 -> 300 -> 400
    timestamps = [e.ts_recv_ns for e in received_events]
    assert timestamps == [100, 200, 300, 400]
    # Comprobar entrelazado de exchanges
    exchanges = [e.exchange for e in received_events]
    assert exchanges == ["binance", "okx", "binance", "okx"]
    # El reloj simulado avanzó hasta el último timestamp
    assert clock.time_ns() == 400

    # Ejecución 2 (Verificación de determinismo bit a bit)
    clock2 = SimulationClock(initial_time_ns=0)
    engine2 = DeterministicReplayEngine(storage, bus, clock2, bronze_bucket="bronze")
    total2, hash2 = await engine2.run(keys)

    assert total2 == total1
    assert hash1 == hash2  # Reproducibilidad exacta garantizada

    await bus.stop()
