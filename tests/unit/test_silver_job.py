"""Tests unitarios para la transformación Bronze -> Silver (Apache Parquet)."""

import io
import json

import polars as pl
import pytest
import zstandard as zstd

from obbot.lakehouse.s3 import LocalStorageBackend
from obbot.lakehouse.silver_job import SilverJob


@pytest.mark.asyncio
async def test_silver_job_processing(tmp_path: pytest.TempPathFactory) -> None:
    storage = LocalStorageBackend(base_dir=str(tmp_path))
    silver_job = SilverJob(storage, bronze_bucket="bronze", silver_bucket="silver")

    # Preparar archivo bronze sintético
    records = [
        {
            "topic": "trades.v1",
            "exchange": "binance",
            "symbol": "BTC-USDT-PERP",
            "event_type": "TradeEvent",
            "ts_recv_ns": 1700000000100000000,
            "ts_exchange_ms": 1700000000100,
            "data": {
                "trade_id": "t-100",
                "price": 50000.0,
                "size": 0.5,
                "notional_usd": 25000.0,
                "side": "BUY",
            },
            "raw": "raw-1",
        },
        {
            "topic": "book.v1",
            "exchange": "binance",
            "symbol": "BTC-USDT-PERP",
            "event_type": "BookSnapshotEvent",
            "ts_recv_ns": 1700000000200000000,
            "ts_exchange_ms": 1700000000200,
            "data": {
                "last_update_id": 999,
                "bids": [[49995.0, 1.0]],
                "asks": [[50005.0, 2.0]],
            },
            "raw": "raw-2",
        },
    ]

    compressor = zstd.ZstdCompressor(level=3)
    ndjson = "\n".join(json.dumps(r) for r in records).encode("utf-8")
    compressed = compressor.compress(ndjson)

    bronze_key = "binance/mixed/2026-11-16/12/part_1.json.zst"
    await storage.put_object("bronze", bronze_key, compressed)

    # Ejecutar transformación Silver
    counts = await silver_job.process_bronze_partition(bronze_key)
    assert counts.get("trades") == 1
    assert counts.get("book_snapshots") == 1

    # Verificar tabla de trades en silver
    silver_files = await storage.list_objects("silver")
    assert any("trades/" in f for f in silver_files)
    assert any("book_snapshots/" in f for f in silver_files)

    trades_file = next(f for f in silver_files if f.startswith("trades/"))
    trades_bytes = await storage.get_object("silver", trades_file)
    df_trades = pl.read_parquet(io.BytesIO(trades_bytes))

    assert len(df_trades) == 1
    assert df_trades["price"][0] == 50000.0
    assert df_trades["side"][0] == "BUY"
    assert df_trades["notional_usd"][0] == 25000.0

    # Verificar tabla de book_snapshots en silver
    books_file = next(f for f in silver_files if f.startswith("book_snapshots/"))
    books_bytes = await storage.get_object("silver", books_file)
    df_books = pl.read_parquet(io.BytesIO(books_bytes))

    assert len(df_books) == 1
    assert df_books["best_bid_px"][0] == 49995.0
    assert df_books["best_ask_px"][0] == 50005.0
    assert df_books["spread"][0] == 10.0
    assert df_books["mid_price"][0] == 50000.0
