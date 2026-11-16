"""Supervisor de conexiones WebSocket con backoff exponencial, jitter y watchdog."""

import random
from enum import StrEnum

from obbot.bus.base import EventBus
from obbot.common.clock import Clock, SystemClock
from obbot.common.events import HealthEvent, HealthStatus
from obbot.common.logging import get_logger

logger = get_logger("ingestion.supervisor")


class ConnectionState(StrEnum):
    """Estado de una conexión WebSocket supervisada."""

    DISCONNECTED = "DISCONNECTED"
    CONNECTING = "CONNECTING"
    CONNECTED = "CONNECTED"
    RECONNECTING = "RECONNECTING"
    ERROR = "ERROR"


class WsSupervisor:
    """Supervisor de resiliencia y salud para conexiones WebSocket de streaming."""

    def __init__(
        self,
        name: str,
        bus: EventBus,
        clock: Clock | None = None,
        base_delay_ms: float = 500.0,
        max_delay_ms: float = 10000.0,
        jitter_factor: float = 0.25,
        watchdog_timeout_sec: float = 15.0,
    ) -> None:
        self.name = name
        self.bus = bus
        self.clock = clock or SystemClock()
        self.base_delay_ms = base_delay_ms
        self.max_delay_ms = max_delay_ms
        self.jitter_factor = jitter_factor
        self.watchdog_timeout_sec = watchdog_timeout_sec

        self.state: ConnectionState = ConnectionState.DISCONNECTED
        self.reconnect_attempts = 0
        self.last_msg_timestamp_ns = 0
        self.is_running = False

    def notify_message_received(self) -> None:
        """Registra la recepción de un mensaje para el watchdog."""
        self.last_msg_timestamp_ns = self.clock.time_ns()
        if self.state != ConnectionState.CONNECTED:
            self.state = ConnectionState.CONNECTED
            self.reconnect_attempts = 0

    def compute_backoff_seconds(self) -> float:
        """Calcula el tiempo de espera para reconexión con backoff exponencial y jitter."""
        factor = min(self.max_delay_ms, self.base_delay_ms * (2**self.reconnect_attempts))
        jitter = factor * random.uniform(-self.jitter_factor, self.jitter_factor)
        delay_ms = max(self.base_delay_ms, factor + jitter)
        self.reconnect_attempts += 1
        return float(delay_ms / 1000.0)

    async def emit_health(self, status: HealthStatus, details: str = "") -> None:
        """Emite una métrica de estado operativo al bus."""
        event = HealthEvent(
            ts_recv_ns=self.clock.time_ns(),
            component=self.name,
            status=status,
            latency_ms=0.0,
            details=details,
        )
        if self.bus:
            try:
                await self.bus.publish("health.v1", event)
            except Exception as exc:
                logger.warning("No se pudo emitir evento de salud para %s: %s", self.name, exc)

    def is_stale(self) -> bool:
        """Indica si la conexión ha dejado de recibir datos según el umbral del watchdog."""
        if self.state != ConnectionState.CONNECTED:
            return True
        elapsed_sec = (self.clock.time_ns() - self.last_msg_timestamp_ns) / 1e9
        return elapsed_sec > self.watchdog_timeout_sec
