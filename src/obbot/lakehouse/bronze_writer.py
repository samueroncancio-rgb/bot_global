"""Escritor en streaming de la capa Bronze en formato NDJSON comprimido con Zstandard."""

import asyncio
import contextlib
import json
from datetime import UTC, datetime
from typing import Any

import zstandard as zstd

from obbot.bus.base import EventBus
from obbot.common.clock import Clock, SystemClock
from obbot.common.events import EventBase, event_to_dict
from obbot.common.logging import get_logger
from obbot.lakehouse.s3 import StorageBackend

logger = get_logger("lakehouse.bronze_writer")


class BronzeWriter:
    """Consumidor del bus que persiste eventos crudos y normalizados en la capa Bronze."""

    def __init__(
        self,
        storage: StorageBackend,
        bucket: str = "lakehouse-bronze",
        bus: EventBus | None = None,
        clock: Clock | None = None,
        max_buffer_records: int = 2000,
        flush_interval_sec: float = 15.0,
    ) -> None:
        self.storage = storage
        self.bucket = bucket
        self.bus = bus
        self.clock = clock or SystemClock()
        self.max_buffer_records = max_buffer_records
        self.flush_interval_sec = flush_interval_sec

        # Búferes indexados por partition_key: list[dict]
        self._buffers: dict[str, list[dict[str, Any]]] = {}
        self._compressor = zstd.ZstdCompressor(level=3)
        self._running = False
        self._flush_task: asyncio.Task[None] | None = None

    async def start(self) -> None:
        """Inicia suscripciones al bus y tarea de volcado periódico."""
        if self._running:
            return
        self._running = True
        if self.bus:
            await self.bus.subscribe("trades.*", self.handle_event)
            await self.bus.subscribe("book.*", self.handle_event)
            await self.bus.subscribe("liquidations.*", self.handle_event)
            await self.bus.subscribe("funding_oi.*", self.handle_event)

        self._flush_task = asyncio.create_task(self._periodic_flush_loop())
        logger.info("BronzeWriter iniciado con bucket '%s'", self.bucket)

    async def stop(self) -> None:
        """Detiene el escritor y vuelca todos los datos remanentes."""
        if not self._running:
            return
        self._running = False
        if self._flush_task:
            self._flush_task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await self._flush_task
            self._flush_task = None

        if self.bus:
            await self.bus.unsubscribe("trades.*", self.handle_event)
            await self.bus.unsubscribe("book.*", self.handle_event)
            await self.bus.unsubscribe("liquidations.*", self.handle_event)
            await self.bus.unsubscribe("funding_oi.*", self.handle_event)

        await self.flush()
        logger.info("BronzeWriter detenido limpiamente")

    def _get_partition_key(self, exchange: str, channel: str, ts_recv_ns: int) -> str:
        """Genera la ruta de partición temporal: exchange/channel/YYYY-MM-DD/HH."""
        dt = datetime.fromtimestamp(ts_recv_ns / 1e9, tz=UTC)
        date_str = dt.strftime("%Y-%m-%d")
        hour_str = dt.strftime("%H")
        return f"{exchange}/{channel}/{date_str}/{hour_str}"

    async def handle_event(self, topic: str, event: EventBase) -> None:
        """Manejador asíncrono para eventos del bus."""
        exchange = getattr(event, "exchange", "common")
        symbol = getattr(event, "symbol", "global")
        channel = topic.split(".")[0]

        record: dict[str, Any] = {
            "topic": topic,
            "exchange": exchange,
            "symbol": symbol,
            "event_type": type(event).__name__,
            "ts_recv_ns": event.ts_recv_ns,
            "ts_exchange_ms": getattr(event, "ts_exchange_ms", 0),
            "data": event_to_dict(event),
            "raw": getattr(event, "raw", ""),
        }

        part_key = self._get_partition_key(exchange, channel, event.ts_recv_ns)
        if part_key not in self._buffers:
            self._buffers[part_key] = []
        self._buffers[part_key].append(record)

        if len(self._buffers[part_key]) >= self.max_buffer_records:
            await self._flush_partition(part_key)

    async def flush(self) -> None:
        """Vuelca todas las particiones con eventos acumulados."""
        keys = list(self._buffers.keys())
        for key in keys:
            await self._flush_partition(key)

    async def _flush_partition(self, partition_key: str) -> None:
        """Serializa, comprime con ZSTD y almacena el contenido de una partición."""
        records = self._buffers.pop(partition_key, None)
        if not records:
            return

        # Generar NDJSON
        lines = [json.dumps(r, ensure_ascii=False) for r in records]
        ndjson_bytes = ("\n".join(lines) + "\n").encode("utf-8")

        # Comprimir con Zstandard
        compressed_bytes = self._compressor.compress(ndjson_bytes)

        # Nombre de archivo determinista: part_{timestamp_ns}_{record_count}.json.zst
        now_ns = self.clock.time_ns()
        key = f"{partition_key}/part_{now_ns}_{len(records)}.json.zst"

        await self.storage.put_object(self.bucket, key, compressed_bytes)
        logger.debug(
            "Guardada partición bronze: %s/%s (%d registros, %d bytes comprimidos)",
            self.bucket,
            key,
            len(records),
            len(compressed_bytes),
        )

    async def _periodic_flush_loop(self) -> None:
        """Bucle en segundo plano para volcado periódico de búferes."""
        while self._running:
            try:
                await self.clock.sleep(self.flush_interval_sec)
                await self.flush()
            except asyncio.CancelledError:
                break
            except Exception as exc:
                logger.exception("Error en volcado periódico de bronze: %s", exc)
