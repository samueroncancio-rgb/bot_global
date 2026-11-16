"""Proceso de agregación analítica de Silver a Gold para métricas de microestructura y barras de order flow."""

import io
import duckdb
import polars as pl
from obbot.common.logging import get_logger
from obbot.lakehouse.s3 import StorageBackend

logger = get_logger("lakehouse.gold_job")


class GoldJob:
    """Consolidador analítico de microestructura y barras volumétricas a la capa Gold."""

    def __init__(
        self,
        storage: StorageBackend,
        silver_bucket: str = "lakehouse-silver",
        gold_bucket: str = "lakehouse-gold",
    ) -> None:
        self.storage = storage
        self.silver_bucket = silver_bucket
        self.gold_bucket = gold_bucket

    async def build_microstructure_1s(self, trades_silver_key: str, output_key: str) -> int:
        """
        Agrega trades de Silver en barras de 1 segundo con métricas de order flow
        (OHLCV, buy/sell volume, delta, trades count).
        """
        parquet_bytes = await self.storage.get_object(self.silver_bucket, trades_silver_key)
        df_trades = pl.read_parquet(io.BytesIO(parquet_bytes))

        if df_trades.is_empty():
            return 0

        # Usar DuckDB para agregaciones analíticas de alta velocidad
        con = duckdb.connect()
        con.register("trades", df_trades.to_arrow())

        query = """
        SELECT
            symbol,
            CAST(ts_recv_ns // 1000000000 AS BIGINT) AS bucket_ts_sec,
            FIRST(price ORDER BY ts_recv_ns ASC) AS open,
            MAX(price) AS high,
            MIN(price) AS low,
            LAST(price ORDER BY ts_recv_ns ASC) AS close,
            SUM(size) AS volume_base,
            SUM(notional_usd) AS volume_usd,
            COUNT(*) AS trades_count,
            SUM(CASE WHEN side = 'BUY' THEN size ELSE 0.0 END) AS buy_volume,
            SUM(CASE WHEN side = 'SELL' THEN size ELSE 0.0 END) AS sell_volume,
            SUM(CASE WHEN side = 'BUY' THEN size ELSE -size END) AS delta_volume,
            SUM(CASE WHEN side = 'BUY' THEN notional_usd ELSE -notional_usd END) AS delta_notional_usd
        FROM trades
        GROUP BY symbol, bucket_ts_sec
        ORDER BY bucket_ts_sec ASC
        """

        arrow_result = con.sql(query).to_arrow_table()
        df_raw = pl.from_arrow(arrow_result)
        df_gold = df_raw if isinstance(df_raw, pl.DataFrame) else pl.DataFrame(df_raw)

        buf = io.BytesIO()
        df_gold.write_parquet(buf, compression="zstd")

        await self.storage.put_object(self.gold_bucket, output_key, buf.getvalue())
        logger.info("Generada tabla Gold %s con %d barras de 1s", output_key, len(df_gold))
        return len(df_gold)
