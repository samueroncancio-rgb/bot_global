"""Detector de huecos y validador de secuencia de libros L2 para Binance USDⓈ-M Futures."""

from collections import deque
from dataclasses import dataclass
from enum import StrEnum

from obbot.common.logging import get_logger

logger = get_logger("orderbook.gap_detector")


class SyncState(StrEnum):
    """Estado de sincronización del libro de órdenes."""

    WAITING_FOR_SNAPSHOT = "WAITING_FOR_SNAPSHOT"
    SYNCHRONIZED = "SYNCHRONIZED"
    DESYNCHRONIZED = "DESYNCHRONIZED"


@dataclass(slots=True, frozen=True)
class BufferedDelta:
    """Actualización incremental en búfer temporal antes de alinear snapshot."""

    first_update_id: int  # U
    final_update_id: int  # u
    prev_final_update_id: int  # pu
    bids: list[tuple[float, float]]
    asks: list[tuple[float, float]]
    ts_exchange_ms: int


class BinanceGapDetector:
    """Validador estricto de secuencias Binance (U, u, pu) contra snapshots REST."""

    def __init__(self, symbol: str, max_buffer_size: int = 5000) -> None:
        self.symbol = symbol
        self.max_buffer_size = max_buffer_size
        self.state: SyncState = SyncState.WAITING_FOR_SNAPSHOT
        self.last_final_update_id: int = 0
        self._buffer: deque[BufferedDelta] = deque(maxlen=max_buffer_size)

    def reset(self) -> None:
        """Reinicia el detector para solicitar un nuevo snapshot."""
        self.state = SyncState.WAITING_FOR_SNAPSHOT
        self.last_final_update_id = 0
        self._buffer.clear()
        logger.info("Gap detector para %s reiniciado a WAITING_FOR_SNAPSHOT", self.symbol)

    def buffer_delta(self, delta: BufferedDelta) -> None:
        """Almacena en búfer diffs recibidos antes o durante la obtención del snapshot."""
        if self.state == SyncState.WAITING_FOR_SNAPSHOT:
            self._buffer.append(delta)

    def align_snapshot(
        self,
        snapshot_last_update_id: int,
    ) -> list[BufferedDelta] | None:
        """
        Alinea el snapshot de Binance contra los deltas acumulados en el búfer.

        Regla oficial de Binance Futures:
        1. Descartar deltas con u < snapshot_last_update_id.
        2. Primer evento válido: U <= snapshot_last_update_id <= u.
        3. Eventos sucesivos: pu == anterior.u.
        """
        if not self._buffer:
            logger.warning(
                "No hay deltas en búfer para %s al recibir snapshot id=%d",
                self.symbol,
                snapshot_last_update_id,
            )
            return None

        # 1. Descartar deltas obsoletos
        while self._buffer and self._buffer[0].final_update_id < snapshot_last_update_id:
            self._buffer.popleft()

        if not self._buffer:
            logger.warning(
                "Todos los deltas del búfer eran más antiguos que el snapshot %d",
                snapshot_last_update_id,
            )
            return None

        first = self._buffer[0]
        # 2. Validación del primer evento tras snapshot
        if not (first.first_update_id <= snapshot_last_update_id <= first.final_update_id):
            logger.error(
                "Primer delta [U=%d, u=%d] no solapa con snapshot %d para %s",
                first.first_update_id,
                first.final_update_id,
                snapshot_last_update_id,
                self.symbol,
            )
            self.state = SyncState.DESYNCHRONIZED
            return None

        valid_deltas: list[BufferedDelta] = []
        current_u = snapshot_last_update_id

        # Procesar la cola verificando continuidad
        for delta in self._buffer:
            if not valid_deltas:
                # Primer delta: ya validado que U <= snapshot_id <= u
                valid_deltas.append(delta)
                current_u = delta.final_update_id
            else:
                if delta.prev_final_update_id != current_u:
                    logger.error(
                        "Discrepancia en búfer: pu=%d != prev_u=%d para %s",
                        delta.prev_final_update_id,
                        current_u,
                        self.symbol,
                    )
                    self.state = SyncState.DESYNCHRONIZED
                    return None
                valid_deltas.append(delta)
                current_u = delta.final_update_id

        self.last_final_update_id = current_u
        self.state = SyncState.SYNCHRONIZED
        self._buffer.clear()
        logger.info(
            "Libro %s sincronizado exitosamente con %d deltas tras snapshot",
            self.symbol,
            len(valid_deltas),
        )
        return valid_deltas

    def validate_next_delta(self, first_id: int, final_id: int, prev_final_id: int) -> bool:
        """
        Valida un nuevo delta incremental en tiempo real.
        Debe cumplir: prev_final_id == last_final_update_id.
        """
        if self.state != SyncState.SYNCHRONIZED:
            return False

        if prev_final_id != self.last_final_update_id:
            logger.error(
                "Hueco detectado en %s: pu=%d != esperado=%d (U=%d, u=%d)",
                self.symbol,
                prev_final_id,
                self.last_final_update_id,
                first_id,
                final_id,
            )
            self.state = SyncState.DESYNCHRONIZED
            return False

        self.last_final_update_id = final_id
        return True
