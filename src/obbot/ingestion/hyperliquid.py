"""Adaptador de ingesta de datos para Hyperliquid L1 WebSocket."""

import asyncio
import contextlib
import json
from typing import Any

import websockets

from obbot.bus.base import EventBus
from obbot.common.clock import Clock
from obbot.common.events import (
    BookSnapshotEvent,
    Side,
    TradeEvent,
)
from obbot.common.logging import get_logger
from obbot.common.registry import InstrumentRegistry
from obbot.ingestion.base import BaseExchangeAdapter
from obbot.ingestion.orderbook.book import OrderBookL2
from obbot.ingestion.supervisor import ConnectionState, WsSupervisor

logger = get_logger("ingestion.hyperliquid")


class HyperliquidAdapter(BaseExchangeAdapter):
    """Adaptador de lectura de datos de mercado para Hyperliquid L1."""

    def __init__(
        self,
        symbols: list[str],
        registry: InstrumentRegistry,
        bus: EventBus,
        ws_url: str = "wss://api.hyperliquid.xyz/ws",
        clock: Clock | None = None,
    ) -> None:
        super().__init__("hyperliquid", symbols, registry, bus, clock)
        self.ws_url = ws_url
        self.supervisor = WsSupervisor("hyperliquid.public", bus, clock)
        self.books: dict[str, OrderBookL2] = {
            s: OrderBookL2("hyperliquid", s) for s in self.symbols
        }
        self._task: asyncio.Task[None] | None = None
        self._ping_task: asyncio.Task[None] | None = None

    async def start(self) -> None:
        if self.is_running:
            return
        self.is_running = True
        self._task = asyncio.create_task(self._run_ws())
        logger.info("HyperliquidAdapter iniciado para símbolos: %s", self.symbols)

    async def stop(self) -> None:
        if not self.is_running:
            return
        self.is_running = False
        if self._ping_task:
            self._ping_task.cancel()
            self._ping_task = None
        if self._task:
            self._task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await self._task
            self._task = None
        logger.info("HyperliquidAdapter detenido")

    def create_subscriptions(self) -> list[dict[str, Any]]:
        """Genera mensajes de suscripción para l2Book y trades."""
        subs: list[dict[str, Any]] = []
        for s in self.symbols:
            native = self.registry.to_exchange("hyperliquid", s)
            subs.append({"method": "subscribe", "subscription": {"type": "l2Book", "coin": native}})
            subs.append({"method": "subscribe", "subscription": {"type": "trades", "coin": native}})
        return subs

    def parse_trade(self, item: dict[str, Any], raw_str: str) -> TradeEvent | None:
        """Parsea un trade ejecutado en Hyperliquid."""
        coin = item.get("coin", "")
        try:
            canonical = self.registry.to_canonical("hyperliquid", coin)
        except KeyError:
            return None

        price = float(item["px"])
        qty = float(item["sz"])
        # 'B' = Buy, 'A' = Ask/Sell
        side = Side.BUY if item.get("side", "B") == "B" else Side.SELL
        ts_exchange_ms = int(item.get("time", self.clock.time_ms()))
        ts_recv = self.clock.time_ns()
        notional_usd = float(
            self.registry.calculate_notional_usd("hyperliquid", canonical, price, qty)
        )

        return TradeEvent(
            exchange="hyperliquid",
            symbol=canonical,
            ts_exchange_ms=ts_exchange_ms,
            ts_recv_ns=ts_recv,
            raw=raw_str,
            trade_id=str(item.get("tid", "")),
            price=price,
            size=qty,
            notional_usd=notional_usd,
            side=side,
        )

    async def _handle_l2_book(self, data: dict[str, Any], raw_str: str) -> None:
        """Parsea y publica el snapshot L2 de Hyperliquid."""
        coin = data.get("coin", "")
        try:
            canonical = self.registry.to_canonical("hyperliquid", coin)
        except KeyError:
            return

        book = self.books[canonical]
        ts_exchange_ms = int(data.get("time", self.clock.time_ms()))
        raw_levels = data.get("levels", [[], []])
        raw_bids = raw_levels[0] if len(raw_levels) > 0 else []
        raw_asks = raw_levels[1] if len(raw_levels) > 1 else []

        bids = [(float(x["px"]), float(x["sz"])) for x in raw_bids]
        asks = [(float(x["px"]), float(x["sz"])) for x in raw_asks]

        book.apply_snapshot(bids, asks, ts_exchange_ms)
        snapshot_evt = BookSnapshotEvent(
            exchange="hyperliquid",
            symbol=canonical,
            ts_exchange_ms=ts_exchange_ms,
            ts_recv_ns=self.clock.time_ns(),
            raw=raw_str,
            bids=book.get_bids(20),
            asks=book.get_asks(20),
            last_update_id=ts_exchange_ms,
        )
        await self.bus.publish("book.v1", snapshot_evt)

    async def _ping_loop(self, ws: Any) -> None:
        """Envía pings periódicos para mantener viva la conexión de Hyperliquid."""
        while self.is_running:
            try:
                await self.clock.sleep(45.0)
                if not self.is_running:
                    break
                await ws.send(json.dumps({"method": "ping"}))
            except asyncio.CancelledError:
                break
            except Exception:
                break

    async def _run_ws(self) -> None:
        """Bucle principal de conexión a Hyperliquid."""
        while self.is_running:
            try:
                self.supervisor.state = ConnectionState.CONNECTING
                async with websockets.connect(self.ws_url, ping_interval=20, ping_timeout=10) as ws:
                    self.supervisor.notify_message_received()
                    # Iniciar ping loop
                    self._ping_task = asyncio.create_task(self._ping_loop(ws))

                    # Enviar suscripciones
                    for sub in self.create_subscriptions():
                        await ws.send(json.dumps(sub))

                    logger.info("Conectado y suscrito a Hyperliquid WS: %s", self.ws_url)

                    async for msg in ws:
                        if not self.is_running:
                            break
                        self.supervisor.notify_message_received()
                        msg_str = msg if isinstance(msg, str) else msg.decode("utf-8")
                        payload = json.loads(msg_str)

                        channel = payload.get("channel", "")
                        data = payload.get("data", {})

                        if channel == "trades":
                            for item in data:
                                trade_evt = self.parse_trade(item, msg_str)
                                if trade_evt:
                                    await self.bus.publish("trades.v1", trade_evt)
                        elif channel == "l2Book":
                            await self._handle_l2_book(data, msg_str)

            except asyncio.CancelledError:
                break
            except Exception as exc:
                self.supervisor.state = ConnectionState.ERROR
                delay = self.supervisor.compute_backoff_seconds()
                logger.warning(
                    "Desconexión en Hyperliquid WS: %s. Reintentando en %.2fs", exc, delay
                )
                await self.clock.sleep(delay)
            finally:
                if self._ping_task:
                    self._ping_task.cancel()
                    self._ping_task = None
