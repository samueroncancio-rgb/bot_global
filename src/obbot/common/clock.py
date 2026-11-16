"""Módulo de reloj abstracto para ejecuciones en vivo y deterministas en backtest/replay."""

import asyncio
import time
from abc import ABC, abstractmethod


class Clock(ABC):
    """Interfaz abstracta de reloj."""

    @abstractmethod
    def time_ns(self) -> int:
        """Retorna el timestamp actual en nanosegundos."""
        ...

    def now_ns(self) -> int:
        """Retorna el timestamp actual en nanosegundos (alias de time_ns)."""
        return self.time_ns()

    def time_ms(self) -> int:
        """Retorna el timestamp actual en milisegundos."""
        return self.time_ns() // 1_000_000

    def time(self) -> float:
        """Retorna el timestamp actual en segundos con precisión flotante."""
        return self.time_ns() / 1_000_000_000.0

    @abstractmethod
    async def sleep(self, seconds: float) -> None:
        """Suspende la ejecución durante el intervalo especificado."""
        ...


class SystemClock(Clock):
    """Reloj del sistema para operaciones en tiempo real (paper, testnet, live)."""

    def time_ns(self) -> int:
        return time.time_ns()

    async def sleep(self, seconds: float) -> None:
        await asyncio.sleep(seconds)


class SimulationClock(Clock):
    """Reloj determinista para backtest y replay de eventos históricos."""

    def __init__(self, initial_time_ns: int = 0) -> None:
        self._current_time_ns: int = initial_time_ns

    def time_ns(self) -> int:
        return self._current_time_ns

    def set_time_ns(self, timestamp_ns: int) -> None:
        """Fija el tiempo simulado actual."""
        if timestamp_ns < self._current_time_ns:
            # En replay no permitimos retroceder el tiempo
            raise ValueError(
                f"El tiempo simulado no puede retroceder: {timestamp_ns} < {self._current_time_ns}"
            )
        self._current_time_ns = timestamp_ns

    def advance_ns(self, duration_ns: int) -> None:
        """Avanza el tiempo simulado en nanosegundos."""
        if duration_ns < 0:
            raise ValueError("La duración a avanzar debe ser positiva")
        self._current_time_ns += duration_ns

    async def sleep(self, seconds: float) -> None:
        """Avanza el reloj simulado de inmediato y cede el control de la corrutina."""
        self.advance_ns(int(seconds * 1_000_000_000))
        await asyncio.sleep(0)
