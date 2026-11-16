"""Tests unitarios para la abstracción de reloj."""

import pytest

from obbot.common.clock import SimulationClock, SystemClock


def test_system_clock_advances() -> None:
    clock = SystemClock()
    t1 = clock.time_ns()
    t2 = clock.time_ns()
    assert t2 >= t1
    assert clock.time_ms() == t2 // 1_000_000
    assert clock.time() > 0.0


@pytest.mark.asyncio
async def test_simulation_clock_manual_control() -> None:
    initial_ns = 1_000_000_000_000_000
    clock = SimulationClock(initial_time_ns=initial_ns)
    assert clock.time_ns() == initial_ns
    assert clock.time_ms() == 1_000_000_000

    # Avanzar tiempo
    clock.advance_ns(500_000_000)
    assert clock.time_ns() == initial_ns + 500_000_000

    # Establecer tiempo absoluto
    clock.set_time_ns(initial_ns + 1_000_000_000)
    assert clock.time_ns() == initial_ns + 1_000_000_000

    # Prohibir retroceder el tiempo
    with pytest.raises(ValueError, match="El tiempo simulado no puede retroceder"):
        clock.set_time_ns(initial_ns)

    # Test sleep simulado
    await clock.sleep(1.5)
    assert clock.time_ns() == initial_ns + 1_000_000_000 + 1_500_000_000
