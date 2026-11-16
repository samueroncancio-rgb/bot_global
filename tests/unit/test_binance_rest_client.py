"""Tests unitarios para el cliente REST institucional de Binance Futures."""

import pytest

from obbot.execution.binance_rest import BinanceFuturesRestClient


def test_binance_rest_signature_generation() -> None:
    client = BinanceFuturesRestClient(
        api_key="test_api_key",
        api_secret="test_api_secret",
        testnet=True,
    )

    params = {
        "symbol": "BTCUSDT",
        "side": "BUY",
        "type": "LIMIT",
        "quantity": 0.01,
        "price": 50000.0,
        "timestamp": 1700000000000,
    }

    signed_query = client._sign_query(params)

    assert "signature=" in signed_query
    assert "symbol=BTCUSDT" in signed_query
    assert "timestamp=1700000000000" in signed_query


@pytest.mark.asyncio
async def test_binance_rest_blocks_forbidden_withdrawal_endpoints() -> None:
    client = BinanceFuturesRestClient(
        api_key="test_key",
        api_secret="test_secret",
    )

    # Bloqueo estricto de retiro
    with pytest.raises(PermissionError, match="viola la política de no retiro"):
        await client.request("POST", "/sapi/v1/capital/withdraw/apply", signed=True)

    # Bloqueo estricto de transferencias
    with pytest.raises(PermissionError, match="viola la política de no retiro"):
        await client.request("POST", "/sapi/v1/asset/transfer", signed=True)


@pytest.mark.asyncio
async def test_binance_rest_blocks_conditional_orders_in_regular_endpoint() -> None:
    client = BinanceFuturesRestClient(
        api_key="test_key",
        api_secret="test_secret",
    )

    # Intentar enviar un STOP_MARKET al endpoint regular debe fallar
    with pytest.raises(ValueError, match="debe enviarse a través del Algo Order client"):
        await client.create_order(
            symbol="BTCUSDT",
            side="SELL",
            order_type="STOP_MARKET",
            quantity=0.01,
        )

