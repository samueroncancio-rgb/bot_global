"""Bus de eventos en memoria basado en asyncio.Queue para tests, simulación y baja latencia."""

import asyncio
import contextlib
import fnmatch

from obbot.bus.base import EventBus, EventHandler
from obbot.common.events import EventBase
from obbot.common.logging import get_logger

logger = get_logger("bus.memory")


class InMemoryEventBus(EventBus):
    """Implementación de EventBus en memoria, sin dependencias externas."""

    def __init__(self, maxsize: int = 100_000) -> None:
        self._maxsize = maxsize
        self._queue: asyncio.Queue[tuple[str, EventBase]] = asyncio.Queue(maxsize=maxsize)
        self._subscriptions: list[tuple[str, EventHandler]] = []
        self._worker_task: asyncio.Task[None] | None = None
        self._running = False

    async def start(self) -> None:
        """Inicia la tarea consumidora en segundo plano."""
        if self._running:
            return
        self._running = True
        self._worker_task = asyncio.create_task(self._process_queue())
        logger.info("InMemoryEventBus iniciado con capacidad máxima %d", self._maxsize)

    async def stop(self) -> None:
        """Detiene el bus y espera a que la cola se procese."""
        if not self._running:
            return
        self._running = False
        if self._worker_task:
            self._worker_task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await self._worker_task
            self._worker_task = None
        logger.info("InMemoryEventBus detenido")

    async def publish(self, topic: str, event: EventBase) -> None:
        """Encola el evento para su despacho a suscriptores."""
        if not self._running:
            raise RuntimeError("No se puede publicar en un InMemoryEventBus detenido")
        await self._queue.put((topic, event))

    async def subscribe(self, topic_pattern: str, handler: EventHandler) -> None:
        """Registra un nuevo manejador para un patrón de tópicos."""
        self._subscriptions.append((topic_pattern, handler))

    async def unsubscribe(self, topic_pattern: str, handler: EventHandler) -> None:
        """Elimina el manejador del registro de suscripciones."""
        self._subscriptions = [
            (p, h) for p, h in self._subscriptions if not (p == topic_pattern and h == handler)
        ]

    async def drain(self) -> None:
        """Bloquea hasta que todos los eventos actualmente en cola hayan sido despachados."""
        await self._queue.join()

    async def _process_queue(self) -> None:
        """Bucle principal de despacho a manejadores coincidentes."""
        while self._running:
            try:
                topic, event = await self._queue.get()
            except asyncio.CancelledError:
                break

            matched_handlers = [
                handler
                for pattern, handler in self._subscriptions
                if fnmatch.fnmatch(topic, pattern)
            ]

            for handler in matched_handlers:
                try:
                    await handler(topic, event)
                except Exception as exc:
                    logger.exception(
                        "Error no controlado en suscriptor de tópico '%s': %s", topic, exc
                    )

            self._queue.task_done()
