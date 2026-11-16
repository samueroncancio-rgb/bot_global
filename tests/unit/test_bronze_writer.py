"""Tests unitarios para el escritor de la capa Bronze (NDJSON + Zstandard)."""

import json

import pytest
import zstandard as zstd

from obbot.bus.memory import InMemoryEventBus
from obbot.common.clock import SimulationClock
from obbot.common.events import Side, TradeEvent
from obbot.lakehouse.bronze_writer import BronzeWriter
from obbot.lakehouse.s3 import LocalStorageBackend


@pytest.mark.asyncio
async def test_bronze_writer_buffering_and_compression(tmp_path: pytest.TempPathFactory) -> None:
    storage = LocalStorageBackend(base_dir=str(tmp_path))
    bus = InMemoryEventBus()
    await bus.start()

    clock = SimulationClock(initial_time_ns=1700000000000000000)
    writer = BronzeWriter(
        storage=storage,
        bucket="lakehouse-bronze",
        bus=bus,
        clock=clock,
        max_buffer_records=5,  # Forzar flush tras 5 registros
        flush_interval_sec=100.0,
    )
    await writer.start()

    # Emitir 5 trades
    for i in range(5):
        trade = TradeEvent(
            exchange="binance",
            symbol="BTC-USDT-PERP",
            ts_exchange_ms=1700000000000 + i * 10,
            ts_recv_ns=1700000000000000000 + i * 10_000_000,
            raw=f'{{"tradeId": {i}}}',
            trade_id=str(i),
            price=50000.0 + i,
            size=0.1,
            notional_usd=5000.0,
            side=Side.BUY,
        )
        await bus.publish("trades.BTC-USDT-PERP", trade)

    await bus.drain()
    await writer.stop()
    await bus.stop()

    # Verificar que el archivo comprimido fue guardado en storage
    files = await storage.list_objects("lakehouse-bronze")
    assert len(files) == 1
    bronze_file = files[0]
    assert bronze_file.endswith(".json.zst")
    assert "binance/trades/" in bronze_file

    # Descomprimir y verificar contenido
    compressed_bytes = await storage.get_object("lakehouse-bronze", bronze_file)
    decompressor = zstd.ZstdDecompressor()
    decompressed_bytes = decompressor.decompress(compressed_bytes)

    lines = decompressed_bytes.decode("utf-8").strip().split("\n")
    assert len(lines) == 5

    parsed_first = json.loads(lines[0])
    assert parsed_first["exchange"] == "binance"
    assert parsed_first["event_type"] == "TradeEvent"
    assert parsed_first["data"]["trade_id"] == "0"
    assert parsed_first["raw"] == '{"tradeId": 0}'
