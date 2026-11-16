"""Adaptador de ingesta de datos para Bybit v5 Linear WebSocket."""

import asyncio
import contextlib
import json
from typing import Any

import websockets

from obbot.bus.base import EventBus
from obbot.common.clock import Clock
from obbot.common.events import (
    BookDeltaEvent,
    BookSnapshotEvent,
    FundingOIEvent,
    OrderBookLevel,
    Side,
    TradeEvent,
)
from obbot.common.logging import get_logger
from obbot.common.registry import InstrumentRegistry
from obbot.ingestion.base import BaseExchangeAdapter
from obbot.ingestion.orderbook.book import OrderBookL2
from obbot.ingestion.supervisor import ConnectionState, WsSupervisor

logger = get_logger("ingestion.bybit")


class BybitAdapter(BaseExchangeAdapter):
    """Adaptador de lectura de datos de mercado para Bybit v5 Linear."""

    def __init__(
        self,
        symbols: list[str],
        registry: InstrumentRegistry,
        bus: EventBus,
        ws_url: str = "wss://stream.bybit.com/v5/public/linear",
        clock: Clock | None = None,
    ) -> None:
        super().__init__("bybit", symbols, registry, bus, clock)
        self.ws_url = ws_url
        self.supervisor = WsSupervisor("bybit.public", bus, clock)
        self.books: dict[str, OrderBookL2] = {s: OrderBookL2("bybit", s) for s in self.symbols}
        self._task: asyncio.Task[None] | None = None

    async def start(self) -> None:
        if self.is_running:
            return
        self.is_running = True
        self._task = asyncio.create_task(self._run_ws())
        logger.info("BybitAdapter iniciado para símbolos: %s", self.symbols)

    async def stop(self) -> None:
        if not self.is_running:
            return
        self.is_running = False
        if self._task:
            self._task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await self._task
            self._task = None
        logger.info("BybitAdapter detenido")

    def create_subscribe_payload(self) -> dict[str, Any]:
        """Genera el payload de suscripción para Bybit v5."""
        args: list[str] = []
        for s in self.symbols:
            native = self.registry.to_exchange("bybit", s)
            args.append(f"publicTrade.{native}")
            args.append(f"orderbook.50.{native}")
            args.append(f"tickers.{native}")
        return {"op": "subscribe", "args": args}

    def parse_trade(self, item: dict[str, Any], raw_str: str) -> TradeEvent | None:
        """Parsea trade de Bybit."""
        native_symbol = item.get("s", "")
        try:
            canonical = self.registry.to_canonical("bybit", native_symbol)
        except KeyError:
            return None

        price = float(item["p"])
        qty = float(item["v"])
        side = Side.BUY if item["S"].lower() == "buy" else Side.SELL
        ts_exchange_ms = int(item["T"])
        ts_recv = self.clock.time_ns()
        notional_usd = float(self.registry.calculate_notional_usd("bybit", canonical, price, qty))

        return TradeEvent(
            exchange="bybit",
            symbol=canonical,
            ts_exchange_ms=ts_exchange_ms,
            ts_recv_ns=ts_recv,
            raw=raw_str,
            trade_id=str(item.get("i", "")),
            price=price,
            size=qty,
            notional_usd=notional_usd,
            side=side,
        )

    def parse_ticker(self, item: dict[str, Any], raw_str: str) -> FundingOIEvent | None:
        """Parsea ticker de Bybit con funding rate y open interest."""
        native_symbol = item.get("symbol", "")
        try:
            canonical = self.registry.to_canonical("bybit", native_symbol)
        except KeyError:
            return None

        funding_rate = float(item.get("fundingRate", 0.0))
        next_funding = int(item.get("nextFundingTime", 0))
        oi = float(item.get("openInterest", 0.0))
        oi_usd = float(item.get("openInterestValue", 0.0))

        return FundingOIEvent(
            exchange="bybit",
            symbol=canonical,
            ts_exchange_ms=self.clock.time_ms(),
            ts_recv_ns=self.clock.time_ns(),
            raw=raw_str,
            funding_rate=funding_rate,
            open_interest=oi,
            open_interest_usd=oi_usd,
            next_funding_time_ms=next_funding,
        )

    async def _handle_orderbook(self, msg_type: str, data: dict[str, Any], raw_str: str) -> None:
        """Procesa snapshots y deltas del canal orderbook.50 de Bybit."""
        native_symbol = data.get("s", "")
        try:
            canonical = self.registry.to_canonical("bybit", native_symbol)
        except KeyError:
            return

        book = self.books[canonical]
        update_id = int(data.get("u", 0))
        ts_exchange_ms = int(data.get("cts", self.clock.time_ms()))

        bids = [(float(p), float(q)) for p, q in data.get("b", [])]
        asks = [(float(p), float(q)) for p, q in data.get("a", [])]

        if msg_type == "snapshot":
            book.apply_snapshot(bids, asks, update_id)
            snapshot_evt = BookSnapshotEvent(
                exchange="bybit",
                symbol=canonical,
                ts_exchange_ms=ts_exchange_ms,
                ts_recv_ns=self.clock.time_ns(),
                raw=raw_str,
                bids=book.get_bids(20),
                asks=book.get_asks(20),
                last_update_id=update_id,
            )
            await self.bus.publish("book.v1", snapshot_evt)
        elif msg_type == "delta":
            book.apply_delta(bids, asks, update_id)
            delta_evt = BookDeltaEvent(
                exchange="bybit",
                symbol=canonical,
                ts_exchange_ms=ts_exchange_ms,
                ts_recv_ns=self.clock.time_ns(),
                raw=raw_str,
                bids=tuple(OrderBookLevel(p, q) for p, q in bids),
                asks=tuple(OrderBookLevel(p, q) for p, q in asks),
                first_update_id=update_id,
                final_update_id=update_id,
            )
            await self.bus.publish("book.v1", delta_evt)

    async def _run_ws(self) -> None:
        """Bucle de conexión a Bybit v5."""
        sub_payload = json.dumps(self.create_subscribe_payload())

        while self.is_running:
            try:
                self.supervisor.state = ConnectionState.CONNECTING
                async with websockets.connect(self.ws_url, ping_interval=20, ping_timeout=10) as ws:
                    self.supervisor.notify_message_received()
                    await ws.send(sub_payload)
                    logger.info("Conectado y suscrito a Bybit Linear WS: %s", self.ws_url)

                    async for msg in ws:
                        if not self.is_running:
                            break
                        self.supervisor.notify_message_received()
                        msg_str = msg if isinstance(msg, str) else msg.decode("utf-8")
                        payload = json.loads(msg_str)

                        if payload.get("op") == "pong":
                            continue

                        topic = payload.get("topic", "")
                        msg_type = payload.get("type", "")
                        data = payload.get("data", {})

                        if topic.startswith("publicTrade."):
                            for item in data:
                                trade_evt = self.parse_trade(item, msg_str)
                                if trade_evt:
                                    await self.bus.publish("trades.v1", trade_evt)
                        elif topic.startswith("orderbook."):
                            await self._handle_orderbook(msg_type, data, msg_str)
                        elif topic.startswith("tickers."):
                            funding_evt = self.parse_ticker(data, msg_str)
                            if funding_evt:
                                await self.bus.publish("funding_oi.v1", funding_evt)

            except asyncio.CancelledError:
                break
            except Exception as exc:
                self.supervisor.state = ConnectionState.ERROR
                delay = self.supervisor.compute_backoff_seconds()
                logger.warning("Desconexión en Bybit WS: %s. Reintentando en %.2fs", exc, delay)
                await self.clock.sleep(delay)
