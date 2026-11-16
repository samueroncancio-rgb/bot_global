"""Interfaz abstracta para el bus de eventos del sistema."""

from abc import ABC, abstractmethod
from collections.abc import Awaitable, Callable

from obbot.common.events import EventBase

EventHandler = Callable[[str, EventBase], Awaitable[None]]


class EventBus(ABC):
    """Bus de eventos asíncrono para desacoplar ingestión, almacenamiento, features y ejecución."""

    @abstractmethod
    async def start(self) -> None:
        """Inicializa conexiones o recursos del bus."""
        ...

    @abstractmethod
    async def stop(self) -> None:
        """Cierra de forma limpia conexiones y recursos pendientes."""
        ...

    @abstractmethod
    async def publish(self, topic: str, event: EventBase) -> None:
        """Publica un evento en un tópico específico."""
        ...

    @abstractmethod
    async def subscribe(self, topic_pattern: str, handler: EventHandler) -> None:
        """Suscribe un manejador asíncrono a un patrón de tópicos (admite wildcards)."""
        ...

    @abstractmethod
    async def unsubscribe(self, topic_pattern: str, handler: EventHandler) -> None:
        """Cancela la suscripción de un manejador."""
        ...
