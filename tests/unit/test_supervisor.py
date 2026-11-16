"""Tests unitarios para el supervisor de conexiones WebSocket."""

import pytest

from obbot.bus.memory import InMemoryEventBus
from obbot.common.clock import SimulationClock
from obbot.common.events import EventBase, HealthEvent, HealthStatus
from obbot.ingestion.supervisor import ConnectionState, WsSupervisor


def test_supervisor_backoff_calculation() -> None:
    bus = InMemoryEventBus()
    supervisor = WsSupervisor(
        "test.supervisor",
        bus,
        base_delay_ms=100.0,
        max_delay_ms=1000.0,
        jitter_factor=0.20,
    )

    # Intento 0: base ~100ms
    d0 = supervisor.compute_backoff_seconds()
    assert 0.08 <= d0 <= 0.15
    assert supervisor.reconnect_attempts == 1

    # Intento 1: base ~200ms
    d1 = supervisor.compute_backoff_seconds()
    assert 0.16 <= d1 <= 0.28
    assert supervisor.reconnect_attempts == 2

    # Intentos altos: clamped a max_delay_ms ~1000ms
    for _ in range(10):
        supervisor.compute_backoff_seconds()
    d_max = supervisor.compute_backoff_seconds()
    assert 0.70 <= d_max <= 1.30


@pytest.mark.asyncio
async def test_supervisor_watchdog_and_health_emission() -> None:
    bus = InMemoryEventBus()
    await bus.start()

    clock = SimulationClock(initial_time_ns=1_000_000_000_000_000)
    supervisor = WsSupervisor("test.feed", bus, clock=clock, watchdog_timeout_sec=5.0)

    received_health: list[HealthEvent] = []

    async def health_handler(topic: str, event: EventBase) -> None:
        if isinstance(event, HealthEvent):
            received_health.append(event)

    await bus.subscribe("health.v1", health_handler)

    assert supervisor.is_stale() is True

    supervisor.notify_message_received()
    assert supervisor.state == ConnectionState.CONNECTED
    assert supervisor.is_stale() is False

    # Avanzar tiempo 3 segundos (menor a timeout de 5s)
    clock.advance_ns(3_000_000_000)
    assert supervisor.is_stale() is False

    # Avanzar tiempo 3 segundos más (total 6s > timeout de 5s)
    clock.advance_ns(3_000_000_000)
    assert supervisor.is_stale() is True

    # Emitir evento de salud
    await supervisor.emit_health(HealthStatus.ERROR, "Watchdog timeout")
    await bus.drain()

    assert len(received_health) == 1
    assert received_health[0].component == "test.feed"
    assert received_health[0].status == HealthStatus.ERROR
    assert received_health[0].details == "Watchdog timeout"

    await bus.stop()
