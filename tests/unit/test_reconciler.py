"""Tests unitarios para el reconciliador de estado contra el exchange."""

from unittest.mock import AsyncMock

import pytest

from obbot.execution.algo_orders import BinanceAlgoOrderClient
from obbot.execution.binance_rest import BinanceFuturesRestClient
from obbot.execution.order_manager import ManagedOrder, OrderManager, OrderStatus
from obbot.execution.reconciler import StateReconciler
from obbot.risk.guard import RiskGuard
from obbot.risk.limits import PortfolioState


@pytest.mark.asyncio
async def test_reconciler_synchronizes_closed_orders_and_positions() -> None:
    rest_mock = AsyncMock(spec=BinanceFuturesRestClient)
    algo_mock = AsyncMock(spec=BinanceAlgoOrderClient)
    guard = RiskGuard()
    manager = OrderManager(rest_mock, algo_mock, guard)

    reconciler = StateReconciler(rest_mock, algo_mock, manager)

    # 1. Simular orden local NEW que ya no está en openOrders en Binance (se llenó)
    local_order = ManagedOrder(
        client_order_id="OB_BTCUSDT_BUY_100",
        symbol="BTCUSDT",
        side="BUY",
        order_type="LIMIT",
        quantity=0.01,
        price=50000.0,
        reduce_only=False,
        status=OrderStatus.NEW,
    )
    manager._orders["OB_BTCUSDT_BUY_100"] = local_order

    rest_mock.get_open_orders.return_value = []
    algo_mock.get_open_algo_orders.return_value = []
    rest_mock.query_order.return_value = {
        "status": "FILLED",
        "executedQty": "0.01",
        "avgPrice": "50000.0",
    }

    report = await reconciler.reconcile_orders()
    assert report["reconciled"] == 1
    assert local_order.status == OrderStatus.FILLED
    assert local_order.filled_quantity == 0.01

    # 2. Reconciliar posiciones
    rest_mock.get_positions.return_value = [
        {
            "symbol": "BTCUSDT",
            "positionAmt": "0.01",
            "entryPrice": "50000.0",
            "markPrice": "50500.0",
            "unRealizedProfit": "5.0",
            "leverage": "3",
            "marginType": "isolated",
        }
    ]

    portfolio = PortfolioState(initial_daily_balance=10000.0, current_wallet_balance=10000.0)
    positions = await reconciler.reconcile_positions(portfolio)

    assert len(positions) == 1
    assert positions[0].symbol == "BTCUSDT"
    assert positions[0].size == 0.01
    assert positions[0].unrealized_pnl == 5.0
    assert portfolio.positions is not None
    assert "BTCUSDT" in portfolio.positions
