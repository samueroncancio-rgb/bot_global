"""Adaptador de ingesta de datos para OKX v5 Public WebSocket."""

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

logger = get_logger("ingestion.okx")


class OkxAdapter(BaseExchangeAdapter):
    """Adaptador de lectura de datos de mercado para OKX."""

    def __init__(
        self,
        symbols: list[str],
        registry: InstrumentRegistry,
        bus: EventBus,
        ws_url: str = "wss://ws.okx.com:8443/ws/v5/public",
        clock: Clock | None = None,
    ) -> None:
        super().__init__("okx", symbols, registry, bus, clock)
        self.ws_url = ws_url
        self.supervisor = WsSupervisor("okx.public", bus, clock)
        self.books: dict[str, OrderBookL2] = {s: OrderBookL2("okx", s) for s in self.symbols}
        self._task: asyncio.Task[None] | None = None

    async def start(self) -> None:
        if self.is_running:
            return
        self.is_running = True
        self._task = asyncio.create_task(self._run_ws())
        logger.info("OkxAdapter iniciado para símbolos: %s", self.symbols)

    async def stop(self) -> None:
        if not self.is_running:
            return
        self.is_running = False
        if self._task:
            self._task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await self._task
            self._task = None
        logger.info("OkxAdapter detenido")

    def create_subscribe_payload(self) -> dict[str, Any]:
        """Genera el payload de suscripción a canales de OKX v5."""
        args: list[dict[str, str]] = []
        for s in self.symbols:
            native = self.registry.to_exchange("okx", s)
            args.append({"channel": "trades", "instId": native})
            args.append({"channel": "books", "instId": native})
            args.append({"channel": "funding-rate", "instId": native})
            args.append({"channel": "open-interest", "instId": native})
        return {"op": "subscribe", "args": args}

    def parse_trade(self, item: dict[str, Any], raw_str: str) -> TradeEvent | None:
        """Parsea un trade ejecutado en OKX aplicando el multiplicador de contrato."""
        native_symbol = item.get("instId", "")
        try:
            canonical = self.registry.to_canonical("okx", native_symbol)
        except KeyError:
            return None

        price = float(item["px"])
        raw_sz = float(item["sz"])
        # Normalizar número de contratos a moneda base usando el registro
        base_size = float(self.registry.normalize_size("okx", canonical, raw_sz))
        notional_usd = float(self.registry.calculate_notional_usd("okx", canonical, price, raw_sz))
        side = Side.BUY if item["side"].lower() == "buy" else Side.SELL

        return TradeEvent(
            exchange="okx",
            symbol=canonical,
            ts_exchange_ms=int(item["ts"]),
            ts_recv_ns=self.clock.time_ns(),
            raw=raw_str,
            trade_id=str(item["tradeId"]),
            price=price,
            size=base_size,
            notional_usd=notional_usd,
            side=side,
        )

    def parse_funding_oi(
        self, item: dict[str, Any], channel: str, raw_str: str
    ) -> FundingOIEvent | None:
        """Parsea eventos de financiación o interés abierto de OKX."""
        native_symbol = item.get("instId", "")
        try:
            canonical = self.registry.to_canonical("okx", native_symbol)
        except KeyError:
            return None

        ts = int(item.get("ts", self.clock.time_ms()))
        funding_rate = float(item.get("fundingRate", 0.0))
        next_funding = int(item.get("nextFundingTime", 0))

        raw_oi = float(item.get("oi", 0.0))
        oi_base = (
            float(self.registry.normalize_size("okx", canonical, raw_oi)) if raw_oi > 0 else 0.0
        )
        oi_usd = float(item.get("oiCcy", 0.0))

        return FundingOIEvent(
            exchange="okx",
            symbol=canonical,
            ts_exchange_ms=ts,
            ts_recv_ns=self.clock.time_ns(),
            raw=raw_str,
            funding_rate=funding_rate,
            open_interest=oi_base,
            open_interest_usd=oi_usd,
            next_funding_time_ms=next_funding,
        )

    async def _handle_books(
        self, arg: dict[str, str], action: str, data: list[dict[str, Any]], raw_str: str
    ) -> None:
        """Procesa snapshots y deltas del canal books de OKX."""
        native_symbol = arg.get("instId", "")
        try:
            canonical = self.registry.to_canonical("okx", native_symbol)
        except KeyError:
            return

        book = self.books[canonical]
        for book_entry in data:
            ts = int(book_entry.get("ts", self.clock.time_ms()))
            # Niveles OKX: [price, size_contracts, liquidated_orders, num_orders]
            bids = [
                (float(b[0]), float(self.registry.normalize_size("okx", canonical, float(b[1]))))
                for b in book_entry.get("bids", [])
            ]
            asks = [
                (float(a[0]), float(self.registry.normalize_size("okx", canonical, float(a[1]))))
                for a in book_entry.get("asks", [])
            ]

            if action == "snapshot":
                book.apply_snapshot(bids, asks, ts)
                snapshot_evt = BookSnapshotEvent(
                    exchange="okx",
                    symbol=canonical,
                    ts_exchange_ms=ts,
                    ts_recv_ns=self.clock.time_ns(),
                    raw=raw_str,
                    bids=book.get_bids(20),
                    asks=book.get_asks(20),
                    last_update_id=ts,
                )
                await self.bus.publish("book.v1", snapshot_evt)
            elif action == "update":
                book.apply_delta(bids, asks, ts)
                delta_evt = BookDeltaEvent(
                    exchange="okx",
                    symbol=canonical,
                    ts_exchange_ms=ts,
                    ts_recv_ns=self.clock.time_ns(),
                    raw=raw_str,
                    bids=tuple(OrderBookLevel(p, q) for p, q in bids),
                    asks=tuple(OrderBookLevel(p, q) for p, q in asks),
                    first_update_id=ts,
                    final_update_id=ts,
                )
                await self.bus.publish("book.v1", delta_evt)

    async def _run_ws(self) -> None:
        """Bucle de conexión y suscripción a OKX."""
        sub_payload = json.dumps(self.create_subscribe_payload())

        while self.is_running:
            try:
                self.supervisor.state = ConnectionState.CONNECTING
                async with websockets.connect(self.ws_url, ping_interval=20, ping_timeout=10) as ws:
                    self.supervisor.notify_message_received()
                    await ws.send(sub_payload)
                    logger.info("Conectado y suscrito a OKX Public WS: %s", self.ws_url)

                    async for msg in ws:
                        if not self.is_running:
                            break
                        self.supervisor.notify_message_received()
                        msg_str = msg if isinstance(msg, str) else msg.decode("utf-8")
                        if msg_str in ("pong", '{"event":"pong"}'):
                            continue

                        payload = json.loads(msg_str)
                        arg = payload.get("arg", {})
                        channel = arg.get("channel", "")
                        action = payload.get("action", "")
                        data = payload.get("data", [])

                        if channel == "trades":
                            for item in data:
                                trade_evt = self.parse_trade(item, msg_str)
                                if trade_evt:
                                    await self.bus.publish("trades.v1", trade_evt)
                        elif channel == "books":
                            await self._handle_books(arg, action, data, msg_str)
                        elif channel in ("funding-rate", "open-interest"):
                            for item in data:
                                funding_evt = self.parse_funding_oi(item, channel, msg_str)
                                if funding_evt:
                                    await self.bus.publish("funding_oi.v1", funding_evt)

            except asyncio.CancelledError:
                break
            except Exception as exc:
                self.supervisor.state = ConnectionState.ERROR
                delay = self.supervisor.compute_backoff_seconds()
                logger.warning("Desconexión en OKX WS: %s. Reintentando en %.2fs", exc, delay)
                await self.clock.sleep(delay)
