"""Proceso de transformación y tipado de Bronze a Silver en Apache Parquet comprimido con Zstandard."""

import io
import json
from typing import Any
import polars as pl
import zstandard as zstd
from obbot.common.logging import get_logger
from obbot.lakehouse.s3 import StorageBackend

logger = get_logger("lakehouse.silver_job")


class SilverJob:
    """Transformador batch de eventos raw NDJSON.ZST (Bronze) a tablas Parquet tipadas (Silver)."""

    def __init__(
        self,
        storage: StorageBackend,
        bronze_bucket: str = "lakehouse-bronze",
        silver_bucket: str = "lakehouse-silver",
    ) -> None:
        self.storage = storage
        self.bronze_bucket = bronze_bucket
        self.silver_bucket = silver_bucket
        self._decompressor = zstd.ZstdDecompressor()

    async def decompress_bronze_file(self, bronze_key: str) -> list[dict[str, Any]]:
        """Descarga y descomprime un archivo .json.zst de la capa Bronze."""
        compressed_bytes = await self.storage.get_object(self.bronze_bucket, bronze_key)
        decompressed_bytes = self._decompressor.decompress(compressed_bytes)
        lines = decompressed_bytes.decode("utf-8").strip().split("\n")
        records: list[dict[str, Any]] = []
        for line in lines:
            if line:
                records.append(json.loads(line))
        return records

    async def process_bronze_partition(self, bronze_key: str) -> dict[str, int]:
        """Procesa un archivo bronze y genera tablas Parquet en Silver."""
        records = await self.decompress_bronze_file(bronze_key)
        if not records:
            return {}

        trades_rows: list[dict[str, Any]] = []
        book_rows: list[dict[str, Any]] = []
        liq_rows: list[dict[str, Any]] = []
        funding_rows: list[dict[str, Any]] = []

        for r in records:
            event_type = r.get("event_type")
            data = r.get("data", {})
            exchange = r.get("exchange", "")
            symbol = r.get("symbol", "")
            ts_recv_ns = int(r.get("ts_recv_ns", 0))
            ts_exchange_ms = int(r.get("ts_exchange_ms", 0))

            if event_type == "TradeEvent":
                trades_rows.append(
                    {
                        "exchange": exchange,
                        "symbol": symbol,
                        "trade_id": str(data.get("trade_id", "")),
                        "price": float(data.get("price", 0.0)),
                        "size": float(data.get("size", 0.0)),
                        "notional_usd": float(data.get("notional_usd", 0.0)),
                        "side": str(data.get("side", "")),
                        "ts_exchange_ms": ts_exchange_ms,
                        "ts_recv_ns": ts_recv_ns,
                    }
                )

            elif event_type == "BookSnapshotEvent":
                bids = data.get("bids", [])
                asks = data.get("asks", [])
                best_bid_px = float(bids[0][0]) if bids else 0.0
                best_bid_sz = float(bids[0][1]) if bids else 0.0
                best_ask_px = float(asks[0][0]) if asks else 0.0
                best_ask_sz = float(asks[0][1]) if asks else 0.0
                mid = (best_bid_px + best_ask_px) / 2.0 if (best_bid_px and best_ask_px) else 0.0
                spread = best_ask_px - best_bid_px if (best_bid_px and best_ask_px) else 0.0

                book_rows.append(
                    {
                        "exchange": exchange,
                        "symbol": symbol,
                        "last_update_id": int(data.get("last_update_id", 0)),
                        "best_bid_px": best_bid_px,
                        "best_bid_sz": best_bid_sz,
                        "best_ask_px": best_ask_px,
                        "best_ask_sz": best_ask_sz,
                        "mid_price": mid,
                        "spread": spread,
                        "bids_json": json.dumps(bids),
                        "asks_json": json.dumps(asks),
                        "ts_exchange_ms": ts_exchange_ms,
                        "ts_recv_ns": ts_recv_ns,
                    }
                )

            elif event_type == "LiquidationEvent":
                liq_rows.append(
                    {
                        "exchange": exchange,
                        "symbol": symbol,
                        "side": str(data.get("side", "")),
                        "price": float(data.get("price", 0.0)),
                        "size": float(data.get("size", 0.0)),
                        "notional_usd": float(data.get("notional_usd", 0.0)),
                        "ts_exchange_ms": ts_exchange_ms,
                        "ts_recv_ns": ts_recv_ns,
                    }
                )

            elif event_type == "FundingOIEvent":
                funding_rows.append(
                    {
                        "exchange": exchange,
                        "symbol": symbol,
                        "funding_rate": float(data.get("funding_rate", 0.0)),
                        "open_interest": float(data.get("open_interest", 0.0)),
                        "open_interest_usd": float(data.get("open_interest_usd", 0.0)),
                        "next_funding_time_ms": int(data.get("next_funding_time_ms", 0)),
                        "ts_exchange_ms": ts_exchange_ms,
                        "ts_recv_ns": ts_recv_ns,
                    }
                )

        counts: dict[str, int] = {}
        # Guardar tablas en silver
        base_name = bronze_key.replace(".json.zst", "").replace("/", "_")

        if trades_rows:
            df_trades = pl.DataFrame(trades_rows)
            parquet_bytes = self._df_to_parquet(df_trades)
            key = f"trades/{base_name}.parquet"
            await self.storage.put_object(self.silver_bucket, key, parquet_bytes)
            counts["trades"] = len(trades_rows)

        if book_rows:
            df_books = pl.DataFrame(book_rows)
            parquet_bytes = self._df_to_parquet(df_books)
            key = f"book_snapshots/{base_name}.parquet"
            await self.storage.put_object(self.silver_bucket, key, parquet_bytes)
            counts["book_snapshots"] = len(book_rows)

        if liq_rows:
            df_liq = pl.DataFrame(liq_rows)
            parquet_bytes = self._df_to_parquet(df_liq)
            key = f"liquidations/{base_name}.parquet"
            await self.storage.put_object(self.silver_bucket, key, parquet_bytes)
            counts["liquidations"] = len(liq_rows)

        if funding_rows:
            df_funding = pl.DataFrame(funding_rows)
            parquet_bytes = self._df_to_parquet(df_funding)
            key = f"funding_oi/{base_name}.parquet"
            await self.storage.put_object(self.silver_bucket, key, parquet_bytes)
            counts["funding_oi"] = len(funding_rows)

        logger.info("Transformada partición %s a Silver: %s", bronze_key, counts)
        return counts

    def _df_to_parquet(self, df: pl.DataFrame) -> bytes:
        """Serializa un DataFrame de Polars a bytes en formato Parquet comprimido con Zstandard."""
        buf = io.BytesIO()
        df.write_parquet(buf, compression="zstd")
        return buf.getvalue()
