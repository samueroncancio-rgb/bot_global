"""Motor de reproducción determinista de eventos históricos para backtest y simulación."""

import hashlib
import heapq
import json
from typing import Any
import zstandard as zstd
from obbot.bus.base import EventBus
from obbot.common.clock import SimulationClock
from obbot.common.events import (
    BookDeltaEvent,
    BookSnapshotEvent,
    EventBase,
    FundingOIEvent,
    LiquidationEvent,
    OrderBookLevel,
    Side,
    TradeEvent,
)
from obbot.common.logging import get_logger
from obbot.lakehouse.s3 import StorageBackend

logger = get_logger("lakehouse.replay")


def deserialize_event(record: dict[str, Any]) -> tuple[str, EventBase] | None:
    """Reconstruye la dataclass inmutable del evento a partir de un registro de Bronze."""
    topic = str(record.get("topic", ""))
    event_type = record.get("event_type", "")
    data = record.get("data", {})
    raw = str(record.get("raw", ""))
    ts_recv_ns = int(record.get("ts_recv_ns", 0))
    ts_exchange_ms = int(record.get("ts_exchange_ms", 0))
    exchange = str(record.get("exchange", ""))
    symbol = str(record.get("symbol", ""))

    if event_type == "TradeEvent":
        side = Side.BUY if data.get("side") == "BUY" else Side.SELL
        evt = TradeEvent(
            exchange=exchange,
            symbol=symbol,
            ts_exchange_ms=ts_exchange_ms,
            ts_recv_ns=ts_recv_ns,
            raw=raw,
            trade_id=str(data.get("trade_id", "")),
            price=float(data.get("price", 0.0)),
            size=float(data.get("size", 0.0)),
            notional_usd=float(data.get("notional_usd", 0.0)),
            side=side,
        )
        return topic, evt

    if event_type == "BookSnapshotEvent":
        bids = tuple(OrderBookLevel(float(p), float(q)) for p, q in data.get("bids", []))
        asks = tuple(OrderBookLevel(float(p), float(q)) for p, q in data.get("asks", []))
        evt_snap = BookSnapshotEvent(
            exchange=exchange,
            symbol=symbol,
            ts_exchange_ms=ts_exchange_ms,
            ts_recv_ns=ts_recv_ns,
            raw=raw,
            bids=bids,
            asks=asks,
            last_update_id=int(data.get("last_update_id", 0)),
        )
        return topic, evt_snap

    if event_type == "BookDeltaEvent":
        bids = tuple(OrderBookLevel(float(p), float(q)) for p, q in data.get("bids", []))
        asks = tuple(OrderBookLevel(float(p), float(q)) for p, q in data.get("asks", []))
        evt_delta = BookDeltaEvent(
            exchange=exchange,
            symbol=symbol,
            ts_exchange_ms=ts_exchange_ms,
            ts_recv_ns=ts_recv_ns,
            raw=raw,
            bids=bids,
            asks=asks,
            first_update_id=int(data.get("first_update_id", 0)),
            final_update_id=int(data.get("final_update_id", 0)),
            prev_final_update_id=int(data.get("prev_final_update_id", 0)),
        )
        return topic, evt_delta

    if event_type == "LiquidationEvent":
        side = Side.BUY if data.get("side") == "BUY" else Side.SELL
        evt_liq = LiquidationEvent(
            exchange=exchange,
            symbol=symbol,
            ts_exchange_ms=ts_exchange_ms,
            ts_recv_ns=ts_recv_ns,
            raw=raw,
            side=side,
            price=float(data.get("price", 0.0)),
            size=float(data.get("size", 0.0)),
            notional_usd=float(data.get("notional_usd", 0.0)),
        )
        return topic, evt_liq

    if event_type == "FundingOIEvent":
        evt_funding = FundingOIEvent(
            exchange=exchange,
            symbol=symbol,
            ts_exchange_ms=ts_exchange_ms,
            ts_recv_ns=ts_recv_ns,
            raw=raw,
            funding_rate=float(data.get("funding_rate", 0.0)),
            open_interest=float(data.get("open_interest", 0.0)),
            open_interest_usd=float(data.get("open_interest_usd", 0.0)),
            next_funding_time_ms=int(data.get("next_funding_time_ms", 0)),
        )
        return topic, evt_funding

    return None


class DeterministicReplayEngine:
    """Motor que reproduce eventos de Bronze ordenados cronológicamente sin fuga temporal."""

    def __init__(
        self,
        storage: StorageBackend,
        bus: EventBus,
        clock: SimulationClock,
        bronze_bucket: str = "lakehouse-bronze",
    ) -> None:
        self.storage = storage
        self.bus = bus
        self.clock = clock
        self.bronze_bucket = bronze_bucket
        self._decompressor = zstd.ZstdDecompressor()

    async def run(self, bronze_keys: list[str]) -> tuple[int, str]:
        """
        Ejecuta la reproducción de una lista de claves de Bronze.
        Retorna (total_eventos_emitidos, sha256_hash_secuencia).
        """
        # Cola de prioridad (min-heap): (ts_recv_ns, seq, topic, event)
        heap: list[tuple[int, int, str, EventBase]] = []
        seq = 0

        # Cargar todos los eventos en memoria para ordenación estricta
        for key in bronze_keys:
            compressed = await self.storage.get_object(self.bronze_bucket, key)
            decompressed = self._decompressor.decompress(compressed)
            lines = decompressed.decode("utf-8").strip().split("\n")
            for line in lines:
                if not line:
                    continue
                record = json.loads(line)
                deserialized = deserialize_event(record)
                if deserialized:
                    topic, event = deserialized
                    heapq.heappush(heap, (event.ts_recv_ns, seq, topic, event))
                    seq += 1

        total_emitted = 0
        hasher = hashlib.sha256()

        while heap:
            ts_recv_ns, _, topic, event = heapq.heappop(heap)
            # Avanzar el reloj simulado estrictamente al tiempo de recepción del evento
            self.clock.set_time_ns(ts_recv_ns)

            # Actualizar hash determinista de la secuencia
            event_repr = f"{topic}:{event.ts_recv_ns}:{getattr(event, 'symbol', '')}:{getattr(event, 'price', 0.0)}"
            hasher.update(event_repr.encode("utf-8"))

            await self.bus.publish(topic, event)
            total_emitted += 1

        seq_hash = hasher.hexdigest()
        logger.info(
            "Replay determinista completado: %d eventos emitidos. Hash: %s",
            total_emitted,
            seq_hash,
        )
        return total_emitted, seq_hash
