"""Adaptador de ingesta para Binance USDⓈ-M Futures con segregación estricta de WebSockets."""

import asyncio
import json
from typing import Any

import aiohttp
import websockets

from obbot.bus.base import EventBus
from obbot.common.clock import Clock
from obbot.common.events import (
    BookDeltaEvent,
    BookSnapshotEvent,
    FundingOIEvent,
    LiquidationEvent,
    OrderBookLevel,
    Side,
    TradeEvent,
)
from obbot.common.logging import get_logger
from obbot.common.registry import InstrumentRegistry
from obbot.ingestion.base import BaseExchangeAdapter
from obbot.ingestion.orderbook.book import OrderBookL2
from obbot.ingestion.orderbook.gap_detector import (
    BinanceGapDetector,
    BufferedDelta,
    SyncState,
)
from obbot.ingestion.supervisor import ConnectionState, WsSupervisor

logger = get_logger("ingestion.binance")


class BinanceCategoryViolationError(Exception):
    """Excepción lanzada cuando se intenta mezclar categorías de WebSocket en Binance."""


ALLOWED_PUBLIC_STREAMS = {"depth", "depth@100ms", "depth@500ms", "bookticker"}
ALLOWED_MARKET_STREAMS = {"aggtrade", "markprice", "markprice@1s", "forceorder", "kline"}


def validate_binance_stream_category(base_url: str, streams: list[str]) -> None:
    """
    Valida que los canales solicitados pertenezcan estrictamente a la URL de WebSocket correcta.
    Binance retiró las URLs unificadas /ws y /stream el 2026-04-23.
    """
    url_lower = base_url.lower()

    if "fstream.binance.com/public" in url_lower:
        for stream in streams:
            stream_type = stream.split("@")[-1].lower()
            # Casos como btcusdt@depth@100ms
            if "@" in stream:
                parts = stream.lower().split("@")
                stream_type = parts[1]
            if stream_type not in ALLOWED_PUBLIC_STREAMS:
                raise BinanceCategoryViolationError(
                    f"Canal de mercado '{stream}' no permitido en /public. "
                    f"Debe conectarse a /market. Prohibida la mezcla de categorías."
                )

    elif "fstream.binance.com/market" in url_lower:
        for stream in streams:
            parts = stream.lower().split("@")
            stream_type = parts[1] if len(parts) > 1 else parts[0]
            if stream_type in ALLOWED_PUBLIC_STREAMS:
                raise BinanceCategoryViolationError(
                    f"Canal de profundidad '{stream}' no permitido en /market. "
                    f"Debe conectarse a /public. Prohibida la mezcla de categorías."
                )


class BinanceAdapter(BaseExchangeAdapter):
    """Adaptador institucional para Binance USDⓈ-M Futures (datos de mercado L2 y trades)."""

    def __init__(
        self,
        symbols: list[str],
        registry: InstrumentRegistry,
        bus: EventBus,
        rest_url: str = "https://fapi.binance.com",
        ws_public_url: str = "wss://fstream.binance.com/public",
        ws_market_url: str = "wss://fstream.binance.com/market",
        clock: Clock | None = None,
    ) -> None:
        super().__init__("binance", symbols, registry, bus, clock)
        self.rest_url = rest_url
        self.ws_public_url = ws_public_url
        self.ws_market_url = ws_market_url

        self.public_supervisor = WsSupervisor("binance.public", bus, clock)
        self.market_supervisor = WsSupervisor("binance.market", bus, clock)

        self.books: dict[str, OrderBookL2] = {s: OrderBookL2("binance", s) for s in self.symbols}
        self.gap_detectors: dict[str, BinanceGapDetector] = {
            s: BinanceGapDetector(s) for s in self.symbols
        }

        self._session: aiohttp.ClientSession | None = None
        self._tasks: list[asyncio.Task[None]] = []
        self._background_tasks: set[asyncio.Task[Any]] = set()

    def _spawn_task(self, coro: Any) -> asyncio.Task[Any]:
        """Crea una tarea en segundo plano conservando referencia para evitar GC prematuro."""
        task = asyncio.create_task(coro)
        self._background_tasks.add(task)
        task.add_done_callback(self._background_tasks.discard)
        return task

    async def start(self) -> None:
        """Inicia clientes WebSocket y supervisión."""
        if self.is_running:
            return
        self.is_running = True
        self._session = aiohttp.ClientSession()

        # Construir listas de streams
        public_streams = [
            f"{self.registry.to_exchange('binance', s).lower()}@depth@100ms" for s in self.symbols
        ]
        market_streams = (
            [f"{self.registry.to_exchange('binance', s).lower()}@aggTrade" for s in self.symbols]
            + [
                f"{self.registry.to_exchange('binance', s).lower()}@markPrice@1s"
                for s in self.symbols
            ]
            + [
                f"{self.registry.to_exchange('binance', s).lower()}@forceOrder"
                for s in self.symbols
            ]
        )

        # Validación categórica de URLs y streams antes de conectar
        validate_binance_stream_category(self.ws_public_url, public_streams)
        validate_binance_stream_category(self.ws_market_url, market_streams)

        self._tasks.append(asyncio.create_task(self._run_public_ws(public_streams)))
        self._tasks.append(asyncio.create_task(self._run_market_ws(market_streams)))
        logger.info("BinanceAdapter iniciado para símbolos: %s", self.symbols)

    async def stop(self) -> None:
        """Detiene todas las tareas y conexiones de forma ordenada."""
        if not self.is_running:
            return
        self.is_running = False

        for task in self._tasks:
            task.cancel()
        if self._tasks:
            await asyncio.gather(*self._tasks, return_exceptions=True)
            self._tasks.clear()

        # Cancelar y esperar tareas en segundo plano (snapshot downloads)
        for task in list(self._background_tasks):
            task.cancel()
        if self._background_tasks:
            await asyncio.gather(*self._background_tasks, return_exceptions=True)
            self._background_tasks.clear()

        if self._session:
            await self._session.close()
            self._session = None

        logger.info("BinanceAdapter detenido")

    def parse_agg_trade(self, data: dict[str, Any], raw_str: str) -> TradeEvent | None:
        """Parsea un evento aggTrade de Binance Futures."""
        native_symbol = data.get("s", "")
        try:
            canonical = self.registry.to_canonical("binance", native_symbol)
        except KeyError:
            return None

        price = float(data["p"])
        qty = float(data["q"])
        # isBuyerMaker = True -> Taker es vendedor -> SELL
        # isBuyerMaker = False -> Taker es comprador -> BUY
        is_buyer_maker = bool(data["m"])
        side = Side.SELL if is_buyer_maker else Side.BUY

        ts_recv = self.clock.time_ns()
        notional_usd = float(self.registry.calculate_notional_usd("binance", canonical, price, qty))

        return TradeEvent(
            exchange="binance",
            symbol=canonical,
            ts_exchange_ms=int(data["T"]),
            ts_recv_ns=ts_recv,
            raw=raw_str,
            trade_id=str(data["a"]),
            price=price,
            size=qty,
            notional_usd=notional_usd,
            side=side,
        )

    def parse_force_order(self, data: dict[str, Any], raw_str: str) -> LiquidationEvent | None:
        """
        Parsea un evento de liquidación forzada de Binance Futures.
        ATENCIÓN: Stream muestreado a 1/s; representa un indicador sesgado a la baja.
        """
        order_data = data.get("o", {})
        native_symbol = order_data.get("s", "")
        try:
            canonical = self.registry.to_canonical("binance", native_symbol)
        except KeyError:
            return None

        price = float(order_data.get("p", 0.0))
        qty = float(order_data.get("q", 0.0))
        raw_side = order_data.get("S", "BUY")
        side = Side.BUY if raw_side == "BUY" else Side.SELL

        ts_recv = self.clock.time_ns()
        notional_usd = float(self.registry.calculate_notional_usd("binance", canonical, price, qty))

        return LiquidationEvent(
            exchange="binance",
            symbol=canonical,
            ts_exchange_ms=int(order_data.get("T", data.get("E", 0))),
            ts_recv_ns=ts_recv,
            raw=raw_str,
            side=side,
            price=price,
            size=qty,
            notional_usd=notional_usd,
        )

    def parse_mark_price(self, data: dict[str, Any], raw_str: str) -> FundingOIEvent | None:
        """Parsea precio de marca y tasa de financiación estimada."""
        native_symbol = data.get("s", "")
        try:
            canonical = self.registry.to_canonical("binance", native_symbol)
        except KeyError:
            return None

        funding_rate = float(data.get("r", 0.0))
        next_funding = int(data.get("T", 0))

        return FundingOIEvent(
            exchange="binance",
            symbol=canonical,
            ts_exchange_ms=int(data.get("E", 0)),
            ts_recv_ns=self.clock.time_ns(),
            raw=raw_str,
            funding_rate=funding_rate,
            open_interest=0.0,
            open_interest_usd=0.0,
            next_funding_time_ms=next_funding,
        )

    async def fetch_depth_snapshot(self, canonical_symbol: str) -> int:
        """Obtiene snapshot REST de profundidad L2 en /fapi/v1/depth."""
        if not self._session:
            return 0
        native = self.registry.to_exchange("binance", canonical_symbol)
        url = f"{self.rest_url}/fapi/v1/depth?symbol={native}&limit=1000"

        try:
            timeout = aiohttp.ClientTimeout(total=5.0)
            async with self._session.get(url, timeout=timeout) as resp:
                if resp.status != 200:
                    logger.error(
                        "Error al obtener snapshot REST para %s: HTTP %d", native, resp.status
                    )
                    return 0
                data = await resp.json()

            last_update_id = int(data["lastUpdateId"])
            bids = [(float(p), float(q)) for p, q in data.get("bids", [])]
            asks = [(float(p), float(q)) for p, q in data.get("asks", [])]

            book = self.books[canonical_symbol]
            detector = self.gap_detectors[canonical_symbol]

            book.apply_snapshot(bids, asks, last_update_id)
            aligned_deltas = detector.align_snapshot(last_update_id)

            if aligned_deltas:
                for delta in aligned_deltas:
                    book.apply_delta(delta.bids, delta.asks, delta.final_update_id)

            # Publicar snapshot al bus
            snapshot_event = BookSnapshotEvent(
                exchange="binance",
                symbol=canonical_symbol,
                ts_exchange_ms=self.clock.time_ms(),
                ts_recv_ns=self.clock.time_ns(),
                bids=book.get_bids(20),
                asks=book.get_asks(20),
                last_update_id=book.last_update_id,
            )
            await self.bus.publish("book.v1", snapshot_event)
            return last_update_id

        except Exception as exc:
            logger.exception("Fallo al obtener o aplicar snapshot REST para %s: %s", native, exc)
            return 0

    async def _run_public_ws(self, streams: list[str]) -> None:
        """Bucle de conexión para streams de profundidad de Binance en /public."""
        query = "/".join(streams)
        url = f"{self.ws_public_url}/stream?streams={query}"

        while self.is_running:
            try:
                self.public_supervisor.state = ConnectionState.CONNECTING
                async with websockets.connect(url, ping_interval=20, ping_timeout=10) as ws:
                    self.public_supervisor.notify_message_received()
                    logger.info("Conectado a Binance Public WS: %s", self.ws_public_url)

                    # Lanzar fetch de snapshots iniciales
                    for s in self.symbols:
                        self.gap_detectors[s].reset()
                        self._spawn_task(self.fetch_depth_snapshot(s))

                    async for msg in ws:
                        if not self.is_running:
                            break
                        self.public_supervisor.notify_message_received()
                        msg_str = msg if isinstance(msg, str) else msg.decode("utf-8")
                        payload = json.loads(msg_str)
                        data = payload.get("data", payload)

                        event_type = data.get("e")
                        if event_type == "depthUpdate":
                            await self._handle_depth_delta(data, msg_str)

            except asyncio.CancelledError:
                break
            except Exception as exc:
                self.public_supervisor.state = ConnectionState.ERROR
                delay = self.public_supervisor.compute_backoff_seconds()
                logger.warning(
                    "Desconexión en Binance Public WS: %s. Reintentando en %.2fs", exc, delay
                )
                await self.clock.sleep(delay)

    async def _handle_depth_delta(self, data: dict[str, Any], raw_str: str) -> None:
        """Procesa una actualización incremental de profundidad de Binance."""
        native_symbol = data.get("s", "")
        try:
            canonical = self.registry.to_canonical("binance", native_symbol)
        except KeyError:
            return

        u = int(data["u"])
        first_u = int(data["U"])
        pu = int(data["pu"])
        ts_exchange_ms = int(data["E"])

        bids = [(float(p), float(q)) for p, q in data.get("b", [])]
        asks = [(float(p), float(q)) for p, q in data.get("a", [])]

        detector = self.gap_detectors[canonical]
        book = self.books[canonical]

        if detector.state == SyncState.WAITING_FOR_SNAPSHOT:
            detector.buffer_delta(
                BufferedDelta(
                    first_update_id=first_u,
                    final_update_id=u,
                    prev_final_update_id=pu,
                    bids=bids,
                    asks=asks,
                    ts_exchange_ms=ts_exchange_ms,
                )
            )
            return

        if detector.state == SyncState.SYNCHRONIZED:
            is_valid = detector.validate_next_delta(first_u, u, pu)
            if is_valid:
                book.apply_delta(bids, asks, u)
                delta_event = BookDeltaEvent(
                    exchange="binance",
                    symbol=canonical,
                    ts_exchange_ms=ts_exchange_ms,
                    ts_recv_ns=self.clock.time_ns(),
                    raw=raw_str,
                    bids=tuple(OrderBookLevel(p, q) for p, q in bids),
                    asks=tuple(OrderBookLevel(p, q) for p, q in asks),
                    first_update_id=first_u,
                    final_update_id=u,
                    prev_final_update_id=pu,
                )
                await self.bus.publish("book.v1", delta_event)
            else:
                # Desincronización detectada -> re-snapshot obligatorio
                logger.error("Desincronización en %s. Solicitando nuevo snapshot REST", canonical)
                detector.reset()
                book.reset()
                self._spawn_task(self.fetch_depth_snapshot(canonical))

    async def _run_market_ws(self, streams: list[str]) -> None:
        """Bucle de conexión para streams de trades y liquidaciones en /market."""
        query = "/".join(streams)
        url = f"{self.ws_market_url}/stream?streams={query}"

        while self.is_running:
            try:
                self.market_supervisor.state = ConnectionState.CONNECTING
                async with websockets.connect(url, ping_interval=20, ping_timeout=10) as ws:
                    self.market_supervisor.notify_message_received()
                    logger.info("Conectado a Binance Market WS: %s", self.ws_market_url)

                    async for msg in ws:
                        if not self.is_running:
                            break
                        self.market_supervisor.notify_message_received()
                        msg_str = msg if isinstance(msg, str) else msg.decode("utf-8")
                        payload = json.loads(msg_str)
                        data = payload.get("data", payload)

                        event_type = data.get("e")
                        if event_type == "aggTrade":
                            trade_evt = self.parse_agg_trade(data, msg_str)
                            if trade_evt:
                                await self.bus.publish("trades.v1", trade_evt)
                        elif event_type == "forceOrder":
                            liq_evt = self.parse_force_order(data, msg_str)
                            if liq_evt:
                                await self.bus.publish("liquidations.v1", liq_evt)
                        elif event_type == "markPriceUpdate":
                            funding_evt = self.parse_mark_price(data, msg_str)
                            if funding_evt:
                                await self.bus.publish("funding_oi.v1", funding_evt)

            except asyncio.CancelledError:
                break
            except Exception as exc:
                self.market_supervisor.state = ConnectionState.ERROR
                delay = self.market_supervisor.compute_backoff_seconds()
                logger.warning(
                    "Desconexión en Binance Market WS: %s. Reintentando en %.2fs", exc, delay
                )
                await self.clock.sleep(delay)
