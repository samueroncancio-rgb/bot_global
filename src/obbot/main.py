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
from obbot.common.events import BookSnapshotEvent, EventBase, TradeEvent
from obbot.common.logging import get_logger, setup_logging
from obbot.common.registry import InstrumentRegistry
from obbot.execution.algo_orders import BinanceAlgoOrderClient
from obbot.execution.binance_rest import BinanceFuturesRestClient
from obbot.execution.order_manager import OrderManager
from obbot.execution.paper import PaperExecutionEngine
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
from obbot.risk.limits import PortfolioState, RiskLimits
from obbot.strategy.baseline_example import BaselineOrderFlowStrategy

logger = get_logger("main")


class OrderFlowBotApp:
    """Aplicación principal que orquesta el ciclo de vida de los servicios del bot."""

    def __init__(self, mode: str, symbol: str, config: AppConfig) -> None:
        self.mode = mode
        self.symbol = symbol
        self.config = config
        self.clock: Clock = SystemClock()
        self.bus: EventBus | None = None
        self.running = False
        self._tasks: list[asyncio.Task[Any]] = []

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
        self.portfolio = PortfolioState(
            initial_daily_balance=10000.0,
            current_wallet_balance=10000.0,
        )

        # 5. Ejecución (Paper o Testnet)
        if self.mode == "paper":
            self.paper_engine = PaperExecutionEngine(
                initial_balance=10000.0,
                clock=self.clock,
            )
            # Adaptador rest mock para paper
            rest_client = BinanceFuturesRestClient("paper_key", "paper_secret", testnet=True)
            algo_client = BinanceAlgoOrderClient(rest_client)
        else:
            rest_client = BinanceFuturesRestClient(
                api_key=self.config.env.binance_api_key,
                api_secret=self.config.env.binance_api_secret,
                testnet=self.mode == "testnet",
            )
            algo_client = BinanceAlgoOrderClient(rest_client)
            await rest_client.sync_server_time()

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
        self.strategy = BaselineOrderFlowStrategy(symbol=self.symbol)

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

        # 9. Suscripción y Conexión de Ingesta
        await self.bus.subscribe("trades.v1", self._on_bus_trade)
        await self.bus.subscribe("book.v1", self._on_bus_book)

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

        logger.info("app.started_successfully: listening for order flow events")

    async def _on_bus_trade(self, topic: str, event: EventBase) -> None:
        """Procesa un trade del bus, actualiza features y evalúa estrategia."""
        if isinstance(event, TradeEvent):
            self.feature_engine.on_trade(event)
            now_ns = self.clock.now_ns()
            fv = self.feature_engine.compute_feature_vector(now_ns)
            if fv and fv.mid_price > 0.0:
                orders = self.strategy.on_feature_vector(fv, self.portfolio)
                for req in orders:
                    await self.order_manager.submit_order(req, self.portfolio, fv.mid_price)

    async def _on_bus_book(self, topic: str, event: EventBase) -> None:
        """Procesa una actualización de libro de órdenes."""
        if isinstance(event, BookSnapshotEvent):
            self.feature_engine.on_book_snapshot(event)

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

        # 2. Detener adaptadores de ingestión
        for adapter in self.adapters:
            await adapter.stop()

        # 3. Detener BronzeWriter y volcar buffers
        if hasattr(self, "bronze_writer"):
            await self.bronze_writer.stop()

        # 4. Detener Servidor de Salud
        if hasattr(self, "health_server"):
            await self.health_server.stop()

        # 5. Detener Bus
        if self.bus:
            await self.bus.stop()

        logger.info("app.stopped_cleanly")


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
    app = OrderFlowBotApp(mode=args.mode, symbol=args.symbol, config=config)

    loop = asyncio.get_running_loop()
    stop_event = asyncio.Event()

    def _sig_handler() -> None:
        logger.info("Señal de terminación recibida. Iniciando parada limpia...")
        stop_event.set()

    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, _sig_handler)

    try:
        await app.start()
        await stop_event.wait()
    finally:
        await app.stop()


def main() -> None:
    asyncio.run(async_main())


if __name__ == "__main__":
    main()
