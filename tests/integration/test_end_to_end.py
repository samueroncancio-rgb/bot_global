"""Test de integración End-to-End: ingesta, features, ejecución paper y reconciliación."""

import asyncio
from typing import Any
from unittest.mock import AsyncMock

import pytest

from obbot.bus.memory import InMemoryEventBus
from obbot.common.clock import SystemClock
from obbot.common.events import BookSnapshotEvent, OrderBookLevel, Side, TradeEvent
from obbot.execution.algo_orders import BinanceAlgoOrderClient
from obbot.execution.binance_rest import BinanceFuturesRestClient
from obbot.execution.order_manager import OrderManager, OrderStatus
from obbot.execution.paper import PaperExecutionEngine
from obbot.execution.reconciler import StateReconciler
from obbot.features.engine import FeatureEngine
from obbot.risk.guard import RiskGuard
from obbot.risk.limits import PortfolioState, RiskLimits
from obbot.strategy.baseline_example import BaselineOrderFlowStrategy


@pytest.mark.asyncio
async def test_end_to_end_orderflow_pipeline() -> None:
    clock = SystemClock()
    bus = InMemoryEventBus()
    await bus.start()

    # 1. Componentes de Feature Engine
    feature_engine = FeatureEngine(symbol="BTC-USDT-PERP")

    # 2. Infraestructura de Ejecución (Paper Engine)
    paper = PaperExecutionEngine(initial_balance=10000.0, clock=clock)
    paper.set_mark_price("BTC-USDT-PERP", 50000.0)

    # Rest and Algo client mocks conectadas a la semántica de PaperExecutionEngine
    rest_mock = AsyncMock(spec=BinanceFuturesRestClient)
    algo_mock = AsyncMock(spec=BinanceAlgoOrderClient)

    # Enrutar llamadas a paper
    async def _mock_create_order(**kwargs: Any) -> dict[str, Any]:
        return await paper.create_order(**kwargs)

    async def _mock_create_stop_loss(**kwargs: Any) -> dict[str, Any]:
        return await paper.create_algo_order(
            symbol=kwargs["symbol"],
            side=kwargs["side"],
            algo_type="STOP_MARKET",
            trigger_price=kwargs["trigger_price"],
            close_position=kwargs.get("close_position", True),
            client_algo_id=kwargs.get("client_algo_id"),
        )

    async def _mock_get_positions() -> list[dict[str, Any]]:
        return await paper.get_positions()

    async def _mock_get_open_orders() -> list[dict[str, Any]]:
        return await paper.get_open_orders()

    async def _mock_get_open_algo() -> list[dict[str, Any]]:
        return await paper.get_open_algo_orders()

    rest_mock.create_order.side_effect = _mock_create_order
    algo_mock.create_stop_loss.side_effect = _mock_create_stop_loss
    rest_mock.get_positions.side_effect = _mock_get_positions
    rest_mock.get_open_orders.side_effect = _mock_get_open_orders
    algo_mock.get_open_algo_orders.side_effect = _mock_get_open_algo

    # 3. RiskGuard y OrderManager
    risk_guard = RiskGuard(RiskLimits(max_order_notional_usd=2000.0))
    order_manager = OrderManager(rest_mock, algo_mock, risk_guard, clock=clock)
    reconciler = StateReconciler(rest_mock, algo_mock, order_manager, clock=clock)

    # 4. Estrategia Baseline
    strategy = BaselineOrderFlowStrategy(
        symbol="BTC-USDT-PERP",
        z_threshold=1.5,
        obi_threshold=0.15,
        trade_size=0.01,
        stop_loss_pct=0.01,
        take_profit_pct=0.02,
    )

    portfolio = PortfolioState(initial_daily_balance=10000.0, current_wallet_balance=10000.0)

    # 5. Inyectar Instantáneas del libro de órdenes L2 para los 4 exchanges
    now_ns = clock.now_ns()
    exchanges = ["binance", "okx", "bybit", "hyperliquid"]
    for ex in exchanges:
        # Libro con fuerte presión compradora (bids > asks)
        bids = tuple(OrderBookLevel(price=49990.0 - i * 10, size=5.0 + i) for i in range(25))
        asks = tuple(OrderBookLevel(price=50010.0 + i * 10, size=1.0) for i in range(25))
        snap = BookSnapshotEvent(
            ts_recv_ns=now_ns,
            exchange=ex,
            symbol="BTC-USDT-PERP",
            ts_exchange_ms=now_ns // 1_000_000,
            bids=bids,
            asks=asks,
            last_update_id=1000,
        )
        feature_engine.on_book_snapshot(snap)

    # 6. Inyectar flujo continuo de compras (aggressor BUY trades) para inflar el Delta Global
    for i in range(20):
        t_ns = now_ns + i * 10_000_000
        trade = TradeEvent(
            ts_recv_ns=t_ns,
            exchange="binance" if i % 2 == 0 else "okx",
            symbol="BTC-USDT-PERP",
            ts_exchange_ms=t_ns // 1_000_000,
            trade_id=f"T-{i}",
            price=50000.0,
            size=1.0,
            side=Side.BUY,
            notional_usd=50000.0,
        )
        feature_engine.on_trade(trade)

    # 7. Calcular FeatureVector consolidado
    fv = feature_engine.compute_feature_vector(now_ns + 300_000_000)
    assert fv is not None
    assert fv.mid_price > 0.0

    # 8. Generar señal y órdenes desde la estrategia
    # Forzar temporalmente los umbrales para garantizar disparo en este test
    strategy.z_threshold = 0.0
    strategy.obi_threshold = 0.0
    orders = strategy.on_feature_vector(fv, portfolio)
    assert len(orders) == 1
    req = orders[0]
    assert req.side == "BUY"
    assert req.stop_loss_price is not None

    # 9. Enviar orden a través de OrderManager (pre-trade checks de RiskGuard incluidos)
    managed_order = await order_manager.submit_order(req, portfolio, fv.mid_price)
    assert managed_order.status == OrderStatus.NEW

    # 10. Simular fill en el paper exchange y adjunto automático de Algo Stop-Loss
    paper.fill_order(managed_order.client_order_id, fill_price=fv.mid_price)
    await order_manager.on_order_fill(
        managed_order.client_order_id,
        filled_qty=req.quantity,
        avg_price=fv.mid_price,
    )
    await asyncio.sleep(0.1)

    updated_order = order_manager.get_order(managed_order.client_order_id)
    assert updated_order is not None
    assert updated_order.status == OrderStatus.FILLED
    assert updated_order.algo_stop_id is not None  # Stop-Loss adjuntado con éxito

    # 11. Reconciliación de estado: validar coherencia entre exchange y portafolio local
    report = await reconciler.reconcile_orders()
    assert report["remote_open_algo"] == 1  # 1 orden Stop-Loss abierta en el exchange
    positions = await reconciler.reconcile_positions(portfolio)
    assert len(positions) == 1
    assert positions[0].size == req.quantity

    await bus.stop()
