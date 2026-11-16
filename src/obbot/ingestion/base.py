"""Clase base abstracta para adaptadores de ingesta de datos por exchange."""

from abc import ABC, abstractmethod

from obbot.bus.base import EventBus
from obbot.common.clock import Clock, SystemClock
from obbot.common.registry import InstrumentRegistry


class BaseExchangeAdapter(ABC):
    """Adaptador de streaming de mercado y supervisor de suscripciones."""

    def __init__(
        self,
        exchange_name: str,
        symbols: list[str],
        registry: InstrumentRegistry,
        bus: EventBus,
        clock: Clock | None = None,
    ) -> None:
        self.exchange_name = exchange_name.lower()
        self.symbols = symbols
        self.registry = registry
        self.bus = bus
        self.clock = clock or SystemClock()
        self.is_running = False

    @abstractmethod
    async def start(self) -> None:
        """Inicia conexiones WebSocket y tareas en segundo plano."""
        ...

    @abstractmethod
    async def stop(self) -> None:
        """Cierra conexiones activas de forma limpia."""
        ...
