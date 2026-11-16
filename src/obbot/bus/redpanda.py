"""Bus de eventos conectado a Redpanda / Kafka mediante aiokafka."""

import asyncio
import json

from aiokafka import AIOKafkaConsumer, AIOKafkaProducer

from obbot.bus.base import EventBus, EventHandler
from obbot.common.events import EventBase, event_to_dict
from obbot.common.logging import get_logger

logger = get_logger("bus.redpanda")


class RedpandaEventBus(EventBus):
    """Implementación de EventBus distribuido sobre Redpanda (Kafka API)."""

    def __init__(
        self,
        bootstrap_servers: str = "localhost:19092",
        client_id: str = "obbot-producer",
        consumer_group: str = "obbot-group",
    ) -> None:
        self._bootstrap_servers = bootstrap_servers
        self._client_id = client_id
        self._consumer_group = consumer_group
        self._producer: AIOKafkaProducer | None = None
        self._consumers: list[AIOKafkaConsumer] = []
        self._consumer_tasks: list[asyncio.Task[None]] = []
        self._subscriptions: list[tuple[str, EventHandler]] = []
        self._running = False

    async def start(self) -> None:
        """Inicializa el productor Kafka."""
        if self._running:
            return
        self._producer = AIOKafkaProducer(
            bootstrap_servers=self._bootstrap_servers,
            client_id=self._client_id,
            linger_ms=5,
            compression_type="zstd",
        )
        await self._producer.start()
        self._running = True
        logger.info("RedpandaEventBus productor conectado a %s", self._bootstrap_servers)

    async def stop(self) -> None:
        """Detiene consumidores y productor de forma ordenada."""
        if not self._running:
            return
        self._running = False

        for task in self._consumer_tasks:
            task.cancel()
        if self._consumer_tasks:
            await asyncio.gather(*self._consumer_tasks, return_exceptions=True)
            self._consumer_tasks.clear()

        for consumer in self._consumers:
            await consumer.stop()
        self._consumers.clear()

        if self._producer:
            await self._producer.stop()
            self._producer = None

        logger.info("RedpandaEventBus detenido")

    async def publish(self, topic: str, event: EventBase) -> None:
        """Serializa y publica el evento en Redpanda."""
        if not self._producer or not self._running:
            raise RuntimeError("RedpandaEventBus no ha sido iniciado")

        payload = json.dumps(event_to_dict(event)).encode("utf-8")
        # Particionamiento por símbolo si el evento es de mercado
        key = getattr(event, "symbol", "").encode("utf-8") if hasattr(event, "symbol") else None
        await self._producer.send_and_wait(topic, value=payload, key=key)

    async def subscribe(self, topic_pattern: str, handler: EventHandler) -> None:
        """Suscribe un manejador. Si no es un patrón wildcard, levanta un consumidor específico."""
        self._subscriptions.append((topic_pattern, handler))
        if self._running and "*" not in topic_pattern:
            consumer = AIOKafkaConsumer(
                topic_pattern,
                bootstrap_servers=self._bootstrap_servers,
                group_id=f"{self._consumer_group}-{topic_pattern}",
                auto_offset_reset="latest",
            )
            await consumer.start()
            self._consumers.append(consumer)
            task = asyncio.create_task(self._consume_topic(consumer, topic_pattern, handler))
            self._consumer_tasks.append(task)

    async def unsubscribe(self, topic_pattern: str, handler: EventHandler) -> None:
        """Elimina el manejador del registro de suscripciones."""
        self._subscriptions = [
            (p, h) for p, h in self._subscriptions if not (p == topic_pattern and h == handler)
        ]

    async def _consume_topic(
        self, consumer: AIOKafkaConsumer, topic: str, handler: EventHandler
    ) -> None:
        """Tarea consumidora para un tópico de Redpanda."""
        try:
            async for msg in consumer:
                if not self._running:
                    break
                try:
                    # En una fase posterior se añade deserialización a dataclasses tipadas
                    _ = json.loads(msg.value.decode("utf-8"))
                    # Despacho preliminar
                except Exception as exc:
                    logger.exception("Error procesando mensaje en tópico %s: %s", topic, exc)
        except asyncio.CancelledError:
            pass
