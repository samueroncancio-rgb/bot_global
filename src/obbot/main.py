"""Punto de entrada principal (CLI) para arranque y supervisión del bot de order flow."""

import argparse
import asyncio
import signal
import sys
from typing import Any

from obbot.bus.base import EventBus
from obbot.bus.memory import InMemoryEventBus
from obbot.bus.redpanda import RedpandaEventBus
from obbot.common.clock import Clock, SystemClock
from obbot.common.config import AppConfig
from obbot.common.events import (
    BookDeltaEvent,
    BookSnapshotEvent,
    EventBase,
    FundingOIEvent,
    LiquidationEvent,
    TradeEvent,
)
from obbot.common.logging import get_logger, setup_logging
from obbot.common.registry import InstrumentRegistry
from obbot.execution.algo_orders import BinanceAlgoOrderClient
from obbot.execution.binance_private_ws import BinancePrivateWsClient
from obbot.execution.binance_rest import BinanceFuturesRestClient
from obbot.execution.order_manager import OrderManager
from obbot.execution.paper import (
    PaperAlgoClient,
    PaperExecutionEngine,
    PaperRestClient,
)
from obbot.execution.reconciler import StateReconciler
from obbot.features.engine import FeatureEngine
from obbot.ingestion.binance import BinanceAdapter
from obbot.ingestion.bybit import BybitAdapter
from obbot.ingestion.hyperliquid import HyperliquidAdapter
from obbot.ingestion.okx import OkxAdapter
from obbot.lakehouse.bronze_writer import BronzeWriter
from obbot.lakehouse.s3 import create_storage_backend
from obbot.obs.health import HealthServer
from obbot.obs.metrics import start_metrics_server
from obbot.risk.guard import RiskGuard
from obbot.risk.kill_switch import FiveLayerKillSwitch, KillSwitchAction
from obbot.risk.limits import PortfolioState, PositionState, RiskLimits
from obbot.strategy.baseline_example import BaselineOrderFlowStrategy

logger = get_logger("main")


class OrderFlowBotApp:
    """Aplicación principal que orquesta el ciclo de vida de los servicios del bot."""

    def __init__(
        self,
        mode: str,
        symbol: str,
        config: AppConfig,
        initial_balance: float | None = None,
    ) -> None:
        self.mode = mode
        self.symbol = symbol
        self.config = config
        self.initial_balance = initial_balance
        self.clock: Clock = SystemClock()
        self.bus: EventBus | None = None
        self.running = False
        self._tasks: list[asyncio.Task[Any]] = []
        self._last_event_ts_ns: int = 0
        self._last_rtt_ms: float = 50.0
        self.total_trades_received: int = 0
        self.total_books_received: int = 0
        self.total_orders_submitted: int = 0

    async def start(self) -> None:
        """Inicia todos los subsistemas de forma secuencial y supervisada."""
        logger.info("app.starting: mode=%s symbol=%s", self.mode, self.symbol)
        self.running = True

        # 1. Bus de Eventos
        if self.config.env.bus_type == "redpanda":
            self.bus = RedpandaEventBus(bootstrap_servers=self.config.env.redpanda_brokers)
        else:
            self.bus = InMemoryEventBus()
        await self.bus.start()

        # 2. Capa de Almacenamiento Lakehouse (Bronze)
        storage = create_storage_backend(self.config)
        self.bronze_writer = BronzeWriter(
            storage=storage,
            bucket=self.config.env.s3_bucket_bronze,
            bus=self.bus,
            clock=self.clock,
        )
        await self.bronze_writer.start()

        # 3. Motor de Features
        self.feature_engine = FeatureEngine(symbol=self.symbol)

        # 4. Gestión de Riesgo y Kill-Switch
        self.risk_limits = RiskLimits(
            max_order_notional_usd=float(
                self.config.risk.get("risk", {})
                .get("limits", {})
                .get("max_order_notional_usd", 2000.0)
            ),
            max_position_notional_usd=float(
                self.config.risk.get("risk", {})
                .get("limits", {})
                .get("max_position_notional_usd", 10000.0)
            ),
            max_daily_drawdown_pct=float(
                self.config.risk.get("risk", {})
                .get("limits", {})
                .get("max_daily_drawdown_pct", 0.03)
            ),
        )
        self.kill_switch = FiveLayerKillSwitch(self.risk_limits)
        self.risk_guard = RiskGuard(self.risk_limits, kill_switch=self.kill_switch)
        init_bal = self.initial_balance or float(
            self.config.risk.get("risk", {}).get("initial_balance_usd", 100.0)
        )
        self.portfolio = PortfolioState(
            initial_daily_balance=init_bal,
            current_wallet_balance=init_bal,
            positions={},
        )

        # 5. Ejecución (Paper o Testnet/Live)
        if self.mode == "paper":
            self.paper_engine = PaperExecutionEngine(
                initial_balance=init_bal,
                clock=self.clock,
                maker_fee_rate=0.0002,
                taker_fee_rate=0.0005,
                slippage_bps=1.0,
                on_fill_callback=self._handle_paper_fill,
            )
            rest_client = PaperRestClient(self.paper_engine)
            algo_client = PaperAlgoClient(self.paper_engine, rest_client)
        else:
            rest_client = BinanceFuturesRestClient(
                api_key=self.config.env.binance_api_key,
                api_secret=self.config.env.binance_api_secret,
                testnet=self.mode == "testnet",
            )
            algo_client = BinanceAlgoOrderClient(rest_client)
            await rest_client.sync_server_time()

            # Conexión WebSocket privada de Binance para User Data Stream
            self.private_ws = BinancePrivateWsClient(
                rest_client=rest_client,
                testnet=self.mode == "testnet",
                clock=self.clock,
                on_order_update=self._handle_private_order_update,
            )
            await self.private_ws.start()

        self.order_manager = OrderManager(
            rest_client=rest_client,
            algo_client=algo_client,
            risk_guard=self.risk_guard,
            clock=self.clock,
        )
        self.reconciler = StateReconciler(
            rest_client=rest_client,
            algo_client=algo_client,
            order_manager=self.order_manager,
            clock=self.clock,
        )

        # 6. Estrategia Baseline
        self.strategy = BaselineOrderFlowStrategy(
            symbol=self.symbol,
            use_dynamic_volatility=True,
        )

        # 7. Iniciar Observabilidad (Métricas Prometheus y Health Checks)
        start_metrics_server(port=self.config.env.metrics_port)

        def _check_ready() -> tuple[bool, str]:
            if self.kill_switch.is_triggered():
                action = self.kill_switch.get_highest_severity_action()
                if action == KillSwitchAction.CLOSE_POSITIONS_EMERGENCY:
                    return False, f"Kill-Switch en {action.name}"
            return True, "Ready"

        self.health_server = HealthServer(
            port=self.config.env.health_port,
            readiness_check=_check_ready,
        )
        await self.health_server.start()

        # 8. Registro de Instrumentos y Normalización Dimensional
        self.registry = InstrumentRegistry()
        self.registry.load_from_dict(self.config.symbols)

        # 9. Suscripción y Conexión de Ingesta Completa (trades, books, liqs, funding/OI)
        await self.bus.subscribe("trades.v1", self._on_bus_trade)
        await self.bus.subscribe("book.v1", self._on_bus_book)
        await self.bus.subscribe("liquidations.v1", self._on_bus_liquidation)
        await self.bus.subscribe("funding_oi.v1", self._on_bus_funding_oi)

        symbols = [self.symbol]
        self.adapters = [
            BinanceAdapter(symbols=symbols, bus=self.bus, registry=self.registry, clock=self.clock),
            OkxAdapter(symbols=symbols, bus=self.bus, registry=self.registry, clock=self.clock),
            BybitAdapter(symbols=symbols, bus=self.bus, registry=self.registry, clock=self.clock),
            HyperliquidAdapter(
                symbols=symbols, bus=self.bus, registry=self.registry, clock=self.clock
            ),
        ]

        for adapter in self.adapters:
            await adapter.start()

        # 10. Iniciar Tareas Supervisoras en Segundo Plano
        # A) Bucle de reconciliación periódica (cada 15s)
        self._tasks.append(
            asyncio.create_task(
                self.reconciler.run_periodic_loop(self.portfolio, interval_sec=15.0)
            )
        )
        # B) Bucle supervisor del Kill-Switch de 5 capas (cada 100ms)
        self._tasks.append(asyncio.create_task(self._run_risk_supervisor()))
        # C) Bucle de estrategia desacoplado (100ms)
        self._tasks.append(asyncio.create_task(self._run_strategy_loop()))

        logger.info("app.started_successfully: listening for order flow events")

    async def _handle_order_fill_event(
        self,
        client_order_id: str,
        filled_qty: float,
        avg_price: float,
        side: str = "BUY",
    ) -> None:
        """Procesa la confirmación de fill de una orden, adjunta stop-loss y actualiza posición."""
        await self.order_manager.on_order_fill(client_order_id, filled_qty, avg_price)

        # Actualizar positions en PortfolioState
        if self.portfolio.positions is None:
            self.portfolio.positions = {}

        direction = 1.0 if side.upper() == "BUY" else -1.0
        current_pos = self.portfolio.positions.get(self.symbol)
        if current_pos:
            current_pos.size += filled_qty * direction
            current_pos.mark_price = avg_price
        else:
            self.portfolio.positions[self.symbol] = PositionState(
                symbol=self.symbol,
                size=filled_qty * direction,
                entry_price=avg_price,
                mark_price=avg_price,
                unrealized_pnl=0.0,
                leverage=3,
                margin_type="ISOLATED",
            )

        # Notificar a la estrategia sobre el estado de la orden
        order = self.order_manager.get_order(client_order_id)
        if order:
            self.strategy.on_order_update(order)

    async def _handle_paper_fill(
        self,
        client_order_id: str,
        filled_qty: float,
        avg_price: float,
        side: str,
    ) -> None:
        """Callback invocado por PaperExecutionEngine ante un fill simulado."""
        await self._handle_order_fill_event(client_order_id, filled_qty, avg_price, side)

    async def _handle_private_order_update(self, order_data: dict[str, Any]) -> None:
        """Callback invocado por BinancePrivateWsClient ante ORDER_TRADE_UPDATE."""
        status = order_data.get("X")
        cid = order_data.get("c", "")
        cum_qty = float(order_data.get("z", 0.0))
        avg_px = float(order_data.get("ap", 0.0))
        side = order_data.get("S", "BUY")

        if status == "FILLED":
            await self._handle_order_fill_event(cid, cum_qty, avg_px, side)

    async def _on_bus_trade(self, topic: str, event: EventBase) -> None:
        """Procesa un trade del bus y actualiza el motor de features y mark price."""
        if isinstance(event, TradeEvent):
            self.total_trades_received += 1
            self._last_event_ts_ns = event.ts_recv_ns
            self.feature_engine.on_trade(event)
            if self.mode == "paper" and hasattr(self, "paper_engine"):
                self.paper_engine.set_mark_price(event.symbol, event.price)

    async def _on_bus_book(self, topic: str, event: EventBase) -> None:
        """Procesa actualizaciones snapshots y deltas del libro de órdenes L2."""
        if isinstance(event, BookSnapshotEvent):
            self.total_books_received += 1
            self._last_event_ts_ns = event.ts_recv_ns
            self.feature_engine.on_book_snapshot(event)
        elif isinstance(event, BookDeltaEvent):
            self.total_books_received += 1
            self._last_event_ts_ns = event.ts_recv_ns
            self.feature_engine.on_book_delta(event)

    async def _on_bus_liquidation(self, topic: str, event: EventBase) -> None:
        """Procesa eventos de liquidación forzada."""
        if isinstance(event, LiquidationEvent):
            self._last_event_ts_ns = event.ts_recv_ns
            self.feature_engine.on_liquidation(event)

    async def _on_bus_funding_oi(self, topic: str, event: EventBase) -> None:
        """Procesa actualizaciones de funding rate y Open Interest."""
        if isinstance(event, FundingOIEvent):
            self._last_event_ts_ns = event.ts_recv_ns
            self.feature_engine.on_funding_oi(event)

    async def _run_strategy_loop(self) -> None:
        """Bucle desacoplado a 10 Hz (100ms) para evaluación cuantitativa y ejecución."""
        interval_sec = (
            self.config.features.get("features", {}).get("sampling_interval_ms", 100) / 1000.0
        )
        while self.running:
            try:
                await asyncio.sleep(interval_sec)
                now_ns = self.clock.now_ns()
                fv = self.feature_engine.compute_feature_vector(now_ns)
                if fv and fv.mid_price > 0.0:
                    orders = self.strategy.on_feature_vector(fv, self.portfolio)
                    for req in orders:
                        self.total_orders_submitted += 1
                        await self.order_manager.submit_order(req, self.portfolio, fv.mid_price)
            except asyncio.CancelledError:
                break
            except Exception as exc:
                logger.error("strategy_loop.error: %s", exc)

    async def _run_risk_supervisor(self) -> None:
        """Bucle supervisor de salud del Kill-Switch de 5 capas (cada 100ms con warmup)."""
        # Periodo de calentamiento (warmup) para estabilización de WebSockets iniciales
        await asyncio.sleep(5.0)
        while self.running:
            try:
                now_ns = self.clock.now_ns()
                last_ev = self._last_event_ts_ns or now_ns

                # Capa 1: Integridad y frescura de datos
                self.kill_switch.check_data_integrity(
                    now_ns=now_ns,
                    last_event_ts_ns=last_ev,
                    book_synced=True,
                )
                # Capa 2: Conectividad y latencia RTT
                self.kill_switch.check_connectivity(
                    rtt_ms=self._last_rtt_ms,
                    now_ns=now_ns,
                )
                # Capa 3: Riesgo financiero y drawdown acumulado
                init_bal = self.portfolio.initial_daily_balance
                curr_bal = self.portfolio.current_wallet_balance
                daily_dd = max(0.0, (init_bal - curr_bal) / init_bal) if init_bal > 0 else 0.0

                unrealized_loss_pct = 0.0
                for pos in self.portfolio.positions.values():
                    if pos.notional_usd > 0:
                        loss_pct = max(0.0, -pos.unrealized_pnl / pos.notional_usd)
                        if loss_pct > unrealized_loss_pct:
                            unrealized_loss_pct = loss_pct

                self.kill_switch.check_financial(
                    daily_drawdown_pct=daily_dd,
                    current_unrealized_loss_pct=unrealized_loss_pct,
                    now_ns=now_ns,
                )
            except asyncio.CancelledError:
                break
            except Exception as exc:
                logger.error("risk_supervisor.error: %s", exc)
            await asyncio.sleep(0.1)

    async def stop(self) -> None:
        """Detiene todos los servicios ordenadamente."""
        if not self.running:
            return
        logger.info("app.stopping...")
        self.running = False

        # 1. Poner Kill-Switch en bloqueo de entradas
        self.kill_switch.trigger_manual(
            "Parada del sistema",
            now_ns=self.clock.now_ns(),
            action=KillSwitchAction.BLOCK_NEW_ENTRIES,
        )

        # 2. Cancelar tareas en segundo plano
        for t in self._tasks:
            t.cancel()
        if self._tasks:
            await asyncio.gather(*self._tasks, return_exceptions=True)
            self._tasks.clear()

        # 3. Detener Private WS si estaba activo
        if hasattr(self, "private_ws"):
            await self.private_ws.stop()

        # 4. Detener adaptadores de ingestión
        for adapter in self.adapters:
            await adapter.stop()

        # 5. Detener BronzeWriter y volcar buffers
        if hasattr(self, "bronze_writer"):
            await self.bronze_writer.stop()

        # 6. Detener Servidor de Salud
        if hasattr(self, "health_server"):
            await self.health_server.stop()

        # 7. Detener Bus
        if self.bus:
            await self.bus.stop()

        logger.info("app.stopped_cleanly")

    def print_session_summary(self) -> None:
        """Imprime un resumen de métricas y rendimiento al finalizar la sesión."""
        now_ns = self.clock.now_ns()
        fv = self.feature_engine.compute_feature_vector(now_ns)
        initial_bal = self.portfolio.initial_daily_balance
        final_bal = (
            self.paper_engine.wallet_balance
            if hasattr(self, "paper_engine")
            else self.portfolio.current_wallet_balance
        )
        pnl = final_bal - initial_bal
        pnl_pct = (pnl / initial_bal) * 100.0 if initial_bal > 0 else 0.0

        positions = (
            self.paper_engine._positions
            if hasattr(self, "paper_engine")
            else (self.portfolio.positions or {})
        )
        active_pos_str = ", ".join(
            f"{s}: size={p.get('positionAmt', getattr(p, 'size', 0))} entry={p.get('entryPrice', getattr(p, 'entry_price', 0))}"
            for s, p in positions.items()
            if abs(p.get("positionAmt", getattr(p, "size", 0))) > 1e-6
        ) or "Ninguna"

        print("\n" + "=" * 70)
        print("          RESUMEN DE SESIÓN OPERATIVA - ORDERFLOW BOT")
        print("=" * 70)
        print(f"  Modo de Ejecución:         {self.mode.upper()}")
        print(f"  Símbolo Operado:           {self.symbol}")
        print(f"  Trades Procesados:         {self.total_trades_received}")
        print(f"  Libros L2 Procesados:      {self.total_books_received}")
        print(f"  Órdenes Evaluadas:         {self.total_orders_submitted}")
        print("-" * 70)
        if fv:
            print(f"  Último Mark Price:         ${fv.mid_price:,.2f}")
            print(f"  Spread Actual:             {fv.spread_bps:.2f} bps")
            print(f"  Delta Global Z-Score:      {fv.global_delta_z:+.2f}")
            print(f"  OBI (10 niveles):          {fv.obi_10_levels:+.2f}")
            print(f"  Persistencia Liquidez:     rho={fv.liquidity_persistence:+.2f}")
            print(f"  Volatilidad Realizada:     {fv.realized_volatility * 100:.2f}%")
        print("-" * 70)
        print(f"  Balance Inicial:           ${initial_bal:,.2f}")
        print(f"  Balance Final:             ${final_bal:,.2f}")
        print(f"  PnL Neto:                  ${pnl:+,.2f} ({pnl_pct:+.2f}%)")
        print(f"  Posiciones Abiertas:       {active_pos_str}")
        ks_action = self.kill_switch.get_highest_severity_action()
        print(f"  Estado Kill-Switch:        {ks_action.name}")
        print("=" * 70 + "\n")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Bot institucional de microestructura y order flow multi-exchange"
    )
    parser.add_argument(
        "--mode",
        choices=["replay", "paper", "testnet", "live"],
        default="paper",
        help="Modo operativo (por defecto 'paper'). 'live' requiere confirmación.",
    )
    parser.add_argument(
        "--symbol",
        default="BTC-USDT-PERP",
        help="Símbolo canónico a operar (por defecto 'BTC-USDT-PERP').",
    )
    parser.add_argument(
        "--config-dir",
        default="configs",
        help="Directorio de configuraciones YAML.",
    )
    parser.add_argument(
        "--duration-sec",
        type=float,
        default=0.0,
        help="Duración en segundos de la sesión de ejecución (0 para infinito).",
    )
    parser.add_argument(
        "--initial-balance",
        type=float,
        default=0.0,
        help="Balance inicial en USD (por defecto desde risk.yaml o 100.0).",
    )
    return parser.parse_args()


async def async_main() -> None:
    args = parse_args()
    setup_logging("INFO")

    if args.mode == "live":
        logger.critical(
            "ACCESO DENEGADO: El modo 'live' con capital real está deshabilitado por seguridad."
        )
        sys.exit(1)

    config = AppConfig(configs_dir=args.config_dir)
    app = OrderFlowBotApp(
        mode=args.mode,
        symbol=args.symbol,
        config=config,
        initial_balance=args.initial_balance if args.initial_balance > 0 else None,
    )

    loop = asyncio.get_running_loop()
    stop_event = asyncio.Event()

    def _sig_handler() -> None:
        logger.info("Señal de terminación recibida. Iniciando parada limpia...")
        stop_event.set()

    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, _sig_handler)

    try:
        await app.start()
        if args.duration_sec > 0:
            try:
                await asyncio.wait_for(stop_event.wait(), timeout=args.duration_sec)
            except TimeoutError:
                logger.info(
                    "Duración configurada alcanzada (%.1fs). Finalizando sesión...",
                    args.duration_sec,
                )
        else:
            await stop_event.wait()
    finally:
        await app.stop()
        app.print_session_summary()


def main() -> None:
    asyncio.run(async_main())


if __name__ == "__main__":
    main()
