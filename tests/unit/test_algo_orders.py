"""Tests unitarios para el cliente de Algo Orders de Binance Futures."""

from unittest.mock import AsyncMock

import pytest

from obbot.execution.algo_orders import BinanceAlgoOrderClient
from obbot.execution.binance_rest import BinanceFuturesRestClient


@pytest.mark.asyncio
async def test_algo_orders_stop_loss_creation() -> None:
    rest_mock = AsyncMock(spec=BinanceFuturesRestClient)
    rest_mock.request.return_value = {
        "algoId": 123456,
        "clientAlgoId": "SL_TEST_01",
        "code": "200",
        "msg": "success",
    }

    algo_client = BinanceAlgoOrderClient(rest_mock)

    res = await algo_client.create_stop_loss(
        symbol="BTCUSDT",
        side="SELL",
        trigger_price=49000.0,
        close_position=True,
        client_algo_id="SL_TEST_01",
    )

    assert res["algoId"] == 123456
    rest_mock.request.assert_awaited_once_with(
        "POST",
        "/fapi/v1/algoOrder",
        params={
            "symbol": "BTCUSDT",
            "side": "SELL",
            "algoType": "STOP_MARKET",
            "triggerPrice": 49000.0,
            "closePosition": "true",
            "clientAlgoId": "SL_TEST_01",
        },
        signed=True,
    )


@pytest.mark.asyncio
async def test_algo_orders_query_open_algo_orders() -> None:
    rest_mock = AsyncMock(spec=BinanceFuturesRestClient)
    rest_mock.request.return_value = [
        {"algoId": 111, "symbol": "BTCUSDT", "algoType": "STOP_MARKET"}
    ]

    algo_client = BinanceAlgoOrderClient(rest_mock)
    res = await algo_client.get_open_algo_orders("BTCUSDT")

    assert len(res) == 1
    assert res[0]["algoId"] == 111
    rest_mock.request.assert_awaited_once_with(
        "GET",
        "/fapi/v1/openAlgoOrders",
        params={"symbol": "BTCUSDT"},
        signed=True,
    )

