"""Tests unitarios para el cliente WebSocket privado de Binance USDⓈ-M Futures."""

import asyncio
import contextlib
from unittest.mock import AsyncMock

import pytest

from obbot.execution.binance_private_ws import BinancePrivateWsClient
from obbot.execution.binance_rest import BinanceFuturesRestClient


@pytest.mark.asyncio
async def test_binance_private_ws_listen_key_expired_closes_socket() -> None:
    rest_mock = AsyncMock(spec=BinanceFuturesRestClient)
    rest_mock.create_listen_key.return_value = "mock_listen_key_123"
    rest_mock.delete_listen_key.return_value = None

    client = BinancePrivateWsClient(rest_client=rest_mock, testnet=True)
    client._listen_key = "mock_listen_key_123"

    ws_mock = AsyncMock()
    ws_mock.close = AsyncMock()

    # Simular recepción de listenKeyExpired
    msg = {"e": "listenKeyExpired", "E": 1700000000000}
    await client._handle_message(msg, ws=ws_mock)

    assert client._listen_key is None
    ws_mock.close.assert_awaited_once()


@pytest.mark.asyncio
async def test_binance_private_ws_dispatches_order_trade_update() -> None:
    rest_mock = AsyncMock(spec=BinanceFuturesRestClient)
    order_callback = AsyncMock()

    client = BinancePrivateWsClient(
        rest_client=rest_mock,
        testnet=True,
        on_order_update=order_callback,
    )

    msg = {
        "e": "ORDER_TRADE_UPDATE",
        "E": 1700000000000,
        "o": {
            "s": "ETHUSDT",
            "c": "OB_ETHUSDT_BUY_1",
            "S": "BUY",
            "X": "FILLED",
            "q": "0.01",
            "z": "0.01",
            "ap": "2600.0",
        },
    }

    await client._handle_message(msg)
    order_callback.assert_awaited_once_with(msg["o"])


@pytest.mark.asyncio
async def test_binance_private_ws_clean_stop_cancels_tasks() -> None:
    rest_mock = AsyncMock(spec=BinanceFuturesRestClient)
    rest_mock.create_listen_key.return_value = "mock_listen_key_456"
    rest_mock.delete_listen_key.return_value = None

    client = BinancePrivateWsClient(rest_client=rest_mock, testnet=True)
    client._running = True
    client._listen_key = "mock_listen_key_456"

    # Simular tareas en segundo plano
    async def _dummy_task() -> None:
        with contextlib.suppress(asyncio.CancelledError):
            await asyncio.sleep(100)

    client._ws_task = asyncio.create_task(_dummy_task())
    client._keepalive_task = asyncio.create_task(_dummy_task())

    await client.stop()

    assert client._running is False
    assert client._listen_key is None
    assert client._ws_task is None
    assert client._keepalive_task is None
    rest_mock.delete_listen_key.assert_awaited_once()
