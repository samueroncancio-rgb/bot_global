"""Tests unitarios para la agregación Silver -> Gold (barras de 1s y métricas de order flow)."""

import io

import polars as pl
import pytest

from obbot.lakehouse.gold_job import GoldJob
from obbot.lakehouse.s3 import LocalStorageBackend


@pytest.mark.asyncio
async def test_gold_microstructure_aggregation(tmp_path: pytest.TempPathFactory) -> None:
    storage = LocalStorageBackend(base_dir=str(tmp_path))
    gold_job = GoldJob(storage, silver_bucket="silver", gold_bucket="gold")

    # Crear trades sintéticos en dos segundos distintos
    # Segundo 1: ts_recv_ns = 1700000001_000_000_000 (2 trades: 1 BUY, 1 SELL)
    # Segundo 2: ts_recv_ns = 1700000002_000_000_000 (1 trade: BUY)
    trades = [
        {
            "exchange": "binance",
            "symbol": "BTC-USDT-PERP",
            "trade_id": "1",
            "price": 50000.0,
            "size": 1.0,
            "notional_usd": 50000.0,
            "side": "BUY",
            "ts_exchange_ms": 1700000001000,
            "ts_recv_ns": 1700000001100000000,
        },
        {
            "exchange": "binance",
            "symbol": "BTC-USDT-PERP",
            "trade_id": "2",
            "price": 50050.0,
            "size": 0.5,
            "notional_usd": 25025.0,
            "side": "SELL",
            "ts_exchange_ms": 1700000001500,
            "ts_recv_ns": 1700000001500000000,
        },
        {
            "exchange": "binance",
            "symbol": "BTC-USDT-PERP",
            "trade_id": "3",
            "price": 50100.0,
            "size": 2.0,
            "notional_usd": 100200.0,
            "side": "BUY",
            "ts_exchange_ms": 1700000002100,
            "ts_recv_ns": 1700000002100000000,
        },
    ]

    df_trades = pl.DataFrame(trades)
    buf = io.BytesIO()
    df_trades.write_parquet(buf, compression="zstd")

    silver_key = "trades/binance_trades.parquet"
    await storage.put_object("silver", silver_key, buf.getvalue())

    # Ejecutar GoldJob
    gold_key = "microstructure_1s/BTC-USDT-PERP.parquet"
    bars_count = await gold_job.build_microstructure_1s(silver_key, gold_key)
    assert bars_count == 2

    gold_bytes = await storage.get_object("gold", gold_key)
    df_gold = pl.read_parquet(io.BytesIO(gold_bytes))

    # Barra 1:
    bar1 = df_gold.filter(pl.col("bucket_ts_sec") == 1700000001)
    assert bar1["open"][0] == 50000.0
    assert bar1["high"][0] == 50050.0
    assert bar1["low"][0] == 50000.0
    assert bar1["close"][0] == 50050.0
    assert bar1["volume_base"][0] == 1.5
    assert bar1["buy_volume"][0] == 1.0
    assert bar1["sell_volume"][0] == 0.5
    assert bar1["delta_volume"][0] == 0.5  # 1.0 - 0.5
    assert bar1["trades_count"][0] == 2

    # Barra 2:
    bar2 = df_gold.filter(pl.col("bucket_ts_sec") == 1700000002)
    assert bar2["open"][0] == 50100.0
    assert bar2["close"][0] == 50100.0
    assert bar2["volume_base"][0] == 2.0
    assert bar2["buy_volume"][0] == 2.0
    assert bar2["sell_volume"][0] == 0.0
    assert bar2["delta_volume"][0] == 2.0
