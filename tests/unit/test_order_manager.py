"""Tests unitarios para OrderManager y Watchdog atómico de Stop-Loss."""

import asyncio
from unittest.mock import AsyncMock

import pytest

from obbot.execution.algo_orders import BinanceAlgoOrderClient
from obbot.execution.binance_rest import BinanceFuturesRestClient
from obbot.execution.order_manager import OrderManager, OrderStatus
from obbot.risk.guard import RiskGuard
from obbot.risk.kill_switch import FiveLayerKillSwitch, KillSwitchAction
from obbot.risk.limits import OrderRequest, PortfolioState, RiskLimits


@pytest.mark.asyncio
async def test_order_manager_successful_order_lifecycle() -> None:
    rest_mock = AsyncMock(spec=BinanceFuturesRestClient)
    algo_mock = AsyncMock(spec=BinanceAlgoOrderClient)
    rest_mock.create_order.return_value = {"orderId": 9999, "status": "NEW"}

    guard = RiskGuard(RiskLimits(max_order_notional_usd=2000.0))
    manager = OrderManager(rest_mock, algo_mock, guard)

    portfolio = PortfolioState(initial_daily_balance=10000.0, current_wallet_balance=10000.0)
    request = OrderRequest(
        symbol="BTCUSDT",
        side="BUY",
        order_type="LIMIT",
        quantity=0.01,
        price=50000.0,
        stop_loss_price=49000.0,
    )

    order = await manager.submit_order(request, portfolio, current_mark_price=50000.0)

    assert order.status == OrderStatus.NEW
    assert order.exchange_order_id == 9999
    assert order.client_order_id.startswith("OB_BTCUSDT_BUY_")
    rest_mock.create_order.assert_awaited_once()


@pytest.mark.asyncio
async def test_order_manager_rejection_by_risk_guard() -> None:
    rest_mock = AsyncMock(spec=BinanceFuturesRestClient)
    algo_mock = AsyncMock(spec=BinanceAlgoOrderClient)

    # Límite bajo para forzar rechazo
    guard = RiskGuard(RiskLimits(max_order_notional_usd=500.0))
    manager = OrderManager(rest_mock, algo_mock, guard)

    portfolio = PortfolioState(initial_daily_balance=10000.0, current_wallet_balance=10000.0)
    request = OrderRequest(
        symbol="BTCUSDT",
        side="BUY",
        order_type="LIMIT",
        quantity=0.02,
        price=50000.0,  # 1,000 USD > 500 USD
        stop_loss_price=49000.0,
    )

    order = await manager.submit_order(request, portfolio, current_mark_price=50000.0)

    assert order.status == OrderStatus.REJECTED
    assert "RiskGuard rechazó la orden" in order.reject_reason
    rest_mock.create_order.assert_not_awaited()


@pytest.mark.asyncio
async def test_order_manager_atomic_stop_loss_attachment_on_fill() -> None:
    rest_mock = AsyncMock(spec=BinanceFuturesRestClient)
    algo_mock = AsyncMock(spec=BinanceAlgoOrderClient)
    rest_mock.create_order.return_value = {"orderId": 8888, "status": "NEW"}
    algo_mock.create_stop_loss.return_value = {"algoId": 7777, "status": "NEW"}

    guard = RiskGuard()
    manager = OrderManager(rest_mock, algo_mock, guard)
    portfolio = PortfolioState(initial_daily_balance=10000.0, current_wallet_balance=10000.0)

    request = OrderRequest(
        symbol="BTCUSDT",
        side="BUY",
        order_type="LIMIT",
        quantity=0.01,
        price=50000.0,
        stop_loss_price=49000.0,
    )

    order = await manager.submit_order(request, portfolio, current_mark_price=50000.0)
    assert order.status == OrderStatus.NEW

    # Simular fill de la orden
    await manager.on_order_fill(order.client_order_id, filled_qty=0.01, avg_price=50000.0)

    # Esperar que la tarea de adjunto de stop se complete
    await asyncio.sleep(0.1)

    updated_order = manager.get_order(order.client_order_id)
    assert updated_order is not None
    assert updated_order.status == OrderStatus.FILLED
    assert updated_order.algo_stop_id == 7777
    algo_mock.create_stop_loss.assert_awaited_once_with(
        symbol="BTCUSDT",
        side="SELL",
        trigger_price=49000.0,
        close_position=True,
        client_algo_id=f"SL_{order.client_order_id}",
    )


@pytest.mark.asyncio
async def test_order_manager_stop_loss_watchdog_failure_triggers_emergency_market_close() -> None:
    rest_mock = AsyncMock(spec=BinanceFuturesRestClient)
    algo_mock = AsyncMock(spec=BinanceAlgoOrderClient)
    rest_mock.create_order.return_value = {"orderId": 8888, "status": "NEW"}
    # Simular fallo en Algo Order (error de red o rechazo de Binance)
    algo_mock.create_stop_loss.side_effect = RuntimeError("Fallo de red en algoOrder")

    ks = FiveLayerKillSwitch()
    guard = RiskGuard(kill_switch=ks)
    manager = OrderManager(
        rest_mock,
        algo_mock,
        guard,
        stop_attach_timeout_ms=50.0,  # 50 ms para test rápido
    )
    portfolio = PortfolioState(initial_daily_balance=10000.0, current_wallet_balance=10000.0)

    request = OrderRequest(
        symbol="BTCUSDT",
        side="BUY",
        order_type="LIMIT",
        quantity=0.01,
        price=50000.0,
        stop_loss_price=49000.0,
    )

    order = await manager.submit_order(request, portfolio, current_mark_price=50000.0)

    # Simular fill de la orden
    await manager.on_order_fill(order.client_order_id, filled_qty=0.01, avg_price=50000.0)

    # Esperar que el watchdog falle y dispare la salida de emergencia
    await asyncio.sleep(0.1)

    # 1. Capa 4 de Kill-Switch disparada con CLOSE_POSITIONS_EMERGENCY
    assert ks.is_triggered()
    assert ks.get_highest_severity_action() == KillSwitchAction.CLOSE_POSITIONS_EMERGENCY

    # 2. Orden de emergencia a mercado reduceOnly enviada a Binance
    # Debe haber 2 llamadas a create_order: la inicial y la de cierre de emergencia
    assert rest_mock.create_order.await_count == 2
    last_call = rest_mock.create_order.await_args_list[-1]
    assert last_call.kwargs["symbol"] == "BTCUSDT"
    assert last_call.kwargs["side"] == "SELL"  # Opuesto a BUY
    assert last_call.kwargs["order_type"] == "MARKET"
    assert last_call.kwargs["reduce_only"] is True
