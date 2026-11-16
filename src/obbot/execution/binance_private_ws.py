"""Conexión WebSocket privada y supervisada a Binance USDⓈ-M User Data Stream."""

import asyncio
import json
from collections.abc import Callable, Coroutine
from typing import Any

import websockets

from obbot.common.clock import Clock, SystemClock
from obbot.common.logging import get_logger
from obbot.execution.binance_rest import BinanceFuturesRestClient

logger = get_logger("execution.binance_private_ws")


class BinancePrivateWsClient:
    """
    Cliente WebSocket para el stream privado de datos de usuario de Binance (/private).

    Reglas de arquitectura:
    1. Segregación categórica absoluta: la URL es estrictamente /private?listenKey=...
    2. Renovación automática y periódica de listenKey (keepalive cada 30 minutos).
    3. Notificación de eventos ORDER_TRADE_UPDATE y ACCOUNT_UPDATE.
    """

    def __init__(
        self,
        rest_client: BinanceFuturesRestClient,
        testnet: bool = False,
        clock: Clock | None = None,
        on_order_update: Callable[[dict[str, Any]], Coroutine[Any, Any, None]] | None = None,
        on_account_update: Callable[[dict[str, Any]], Coroutine[Any, Any, None]] | None = None,
    ) -> None:
        self.rest_client = rest_client
        self.testnet = testnet
        self.clock = clock or SystemClock()
        self.on_order_update = on_order_update
        self.on_account_update = on_account_update

        self.ws_base_url = (
            "wss://stream.binancefuture.com/private"
            if testnet
            else "wss://fstream.binance.com/private"
        )
        self._listen_key: str | None = None
        self._running = False
        self._ws_task: asyncio.Task[None] | None = None
        self._keepalive_task: asyncio.Task[None] | None = None

    async def start(self) -> None:
        """Inicia la obtención del listenKey y la conexión WebSocket supervisada."""
        if self._running:
            return
        self._running = True

        self._listen_key = await self.rest_client.create_listen_key()
        logger.info("binance_private_ws.listen_key_created")

        self._ws_task = asyncio.create_task(self._run_ws_loop())
        self._keepalive_task = asyncio.create_task(self._run_keepalive_loop())

    async def stop(self) -> None:
        """Detiene la conexión y elimina el listenKey."""
        self._running = False
        if self._keepalive_task:
            self._keepalive_task.cancel()
            self._keepalive_task = None

        if self._ws_task:
            self._ws_task.cancel()
            self._ws_task = None

        if self._listen_key:
            try:
                await self.rest_client.delete_listen_key()
            except Exception as exc:
                logger.warning("binance_private_ws.delete_listen_key_error: %s", exc)
            self._listen_key = None

        logger.info("binance_private_ws.stopped")

    async def _run_keepalive_loop(self) -> None:
        """Envía keepalive cada 30 minutos (el listenKey de Binance expira en 60 min)."""
        while self._running:
            try:
                await asyncio.sleep(1800)  # 30 minutos
                if self._running and self._listen_key:
                    await self.rest_client.keepalive_listen_key()
                    logger.info("binance_private_ws.keepalive_sent")
            except asyncio.CancelledError:
                break
            except Exception as exc:
                logger.error("binance_private_ws.keepalive_failed: %s", exc)

    async def _run_ws_loop(self) -> None:
        """Bucle de conexión con reconexión automática."""
        while self._running:
            if not self._listen_key:
                try:
                    self._listen_key = await self.rest_client.create_listen_key()
                except Exception as exc:
                    logger.error("binance_private_ws.create_key_failed: %s", exc)
                    await asyncio.sleep(5.0)
                    continue

            url = f"{self.ws_base_url}?listenKey={self._listen_key}"

            # Validación de segregación de categoría
            if not url.startswith(self.ws_base_url):
                raise ValueError(
                    f"Violación de categoría WS: URL debe iniciar con {self.ws_base_url}"
                )

            try:
                async with websockets.connect(url, ping_interval=20, ping_timeout=10) as ws:
                    logger.info("binance_private_ws.connected")
                    while self._running:
                        msg_raw = await ws.recv()
                        msg = json.loads(msg_raw)
                        await self._handle_message(msg)
            except asyncio.CancelledError:
                break
            except Exception as exc:
                if self._running:
                    logger.warning("binance_private_ws.disconnected: %s. Reintentando...", exc)
                    await asyncio.sleep(2.0)

    async def _handle_message(self, msg: dict[str, Any]) -> None:
        """Despacha los eventos del User Data Stream a los callbacks registrados."""
        event_type = msg.get("e")
        if event_type == "ORDER_TRADE_UPDATE":
            order_data = msg.get("o", {})
            if self.on_order_update:
                await self.on_order_update(order_data)
        elif event_type == "ACCOUNT_UPDATE":
            account_data = msg.get("a", {})
            if self.on_account_update:
                await self.on_account_update(account_data)
        elif event_type == "listenKeyExpired":
            logger.error("binance_private_ws.listen_key_expired. Recreando clave...")
            self._listen_key = None
