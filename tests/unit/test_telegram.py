"""Tests unitarios para el servicio de control y notificaciones de Telegram."""

import asyncio
from unittest.mock import AsyncMock, MagicMock

import pytest

from obbot.obs.telegram import TelegramBotService
from obbot.risk.kill_switch import FiveLayerKillSwitch, KillSwitchAction, KillSwitchLayer
from obbot.risk.limits import PortfolioState, PositionState, RiskLimits


@pytest.fixture
def mock_app() -> MagicMock:
    app = MagicMock()
    app.mode = "paper"
    app.symbol = "ETH-USDT-PERP"
    app.total_trades_received = 1500
    app.total_books_received = 2000
    app.total_orders_submitted = 5
    app._stop_event = asyncio.Event()

    clock = MagicMock()
    clock.now_ns.return_value = 1_000_000_000_000
    app.clock = clock

    limits = RiskLimits(max_order_notional_usd=100.0, max_position_notional_usd=250.0)
    app.kill_switch = FiveLayerKillSwitch(limits)

    app.portfolio = PortfolioState(
        initial_daily_balance=100.0,
        current_wallet_balance=100.0,
        positions={
            "ETH-USDT-PERP": PositionState(
                symbol="ETH-USDT-PERP",
                size=0.01,
                entry_price=1750.0,
                mark_price=1760.0,
                unrealized_pnl=0.10,
                leverage=3,
            )
        },
    )

    fv = MagicMock()
    fv.mid_price = 1760.0
    fv.spread_bps = 0.05
    fv.global_delta_z = 1.25
    fv.obi_10_levels = 0.30
    fv.realized_volatility = 0.0025
    app.feature_engine.compute_feature_vector.return_value = fv

    app.order_manager.submit_order = AsyncMock()
    app.order_manager.algo_client.get_open_algo_orders = AsyncMock(return_value=[])
    app.order_manager.algo_client.cancel_algo_order = AsyncMock()
    del app.paper_engine

    return app


@pytest.mark.asyncio
async def test_telegram_security_rejects_unauthorized_user(mock_app: MagicMock) -> None:
    service = TelegramBotService(
        bot_token="TEST_TOKEN",
        chat_id="123456",
        allowed_user_ids="123456,999999",
        app=mock_app,
    )
    service.send_message = AsyncMock()  # type: ignore[method-assign]

    # Mensaje de un atacante / usuario no autorizado (id = 666666)
    unauthorized_msg = {
        "chat": {"id": 123456},
        "from": {"id": 666666, "username": "intruder"},
        "text": "/close",
    }

    await service._handle_incoming_message(unauthorized_msg)

    # Debe ser ignorado: no se ejecutan órdenes ni se responde
    mock_app.order_manager.submit_order.assert_not_called()
    service.send_message.assert_not_called()


@pytest.mark.asyncio
async def test_telegram_command_status(mock_app: MagicMock) -> None:
    service = TelegramBotService(
        bot_token="TEST_TOKEN",
        chat_id="123456",
        allowed_user_ids="123456",
        app=mock_app,
    )
    sent_messages: list[str] = []

    async def _mock_send(text: str, chat_id: str | None = None, parse_mode: str | None = "Markdown") -> None:
        sent_messages.append(text)

    service.send_message = _mock_send  # type: ignore[method-assign]

    msg = {
        "chat": {"id": 123456},
        "from": {"id": 123456, "username": "admin"},
        "text": "/status",
    }

    await service._handle_incoming_message(msg)

    assert len(sent_messages) == 1
    assert "TELEMETRÍA EN TIEMPO REAL" in sent_messages[0]
    assert "ETH-USDT-PERP" in sent_messages[0]
    assert "$100.00 USD" in sent_messages[0]
    assert "LONG 🟢" in sent_messages[0]


@pytest.mark.asyncio
async def test_telegram_command_close_position(mock_app: MagicMock) -> None:
    service = TelegramBotService(
        bot_token="TEST_TOKEN",
        chat_id="123456",
        allowed_user_ids="123456",
        app=mock_app,
    )
    service.send_message = AsyncMock()  # type: ignore[method-assign]

    managed_mock = MagicMock()
    managed_mock.status.name = "NEW"
    mock_app.order_manager.submit_order.return_value = managed_mock

    msg = {
        "chat": {"id": 123456},
        "from": {"id": 123456, "username": "admin"},
        "text": "/close",
    }

    await service._handle_incoming_message(msg)

    # Verifica que submit_order fue llamado con orden MARKET reduceOnly SELL
    mock_app.order_manager.submit_order.assert_called_once()
    order_req = mock_app.order_manager.submit_order.call_args[0][0]
    assert order_req.symbol == "ETH-USDT-PERP"
    assert order_req.side == "SELL"
    assert order_req.order_type == "MARKET"
    assert order_req.reduce_only is True
    assert order_req.quantity == 0.01

    service.send_message.assert_called_once()
    assert "POSICIÓN CERRADA MANUALMENTE" in service.send_message.call_args[0][0]


@pytest.mark.asyncio
async def test_telegram_command_stop_trips_kill_switch(mock_app: MagicMock) -> None:
    service = TelegramBotService(
        bot_token="TEST_TOKEN",
        chat_id="123456",
        allowed_user_ids="123456",
        app=mock_app,
    )
    service.send_message = AsyncMock()  # type: ignore[method-assign]

    msg = {
        "chat": {"id": 123456},
        "from": {"id": 123456, "username": "admin"},
        "text": "/stop",
    }

    await service._handle_incoming_message(msg)

    # Verifica que la Capa Manual se disparó a BLOCK_NEW_ENTRIES
    highest = mock_app.kill_switch.get_highest_severity_action()
    assert highest == KillSwitchAction.BLOCK_NEW_ENTRIES
    assert mock_app.kill_switch._layers[KillSwitchLayer.MANUAL].triggered is True

    service.send_message.assert_called_once()
    assert "KILL-SWITCH MANUAL ACTIVADO" in service.send_message.call_args[0][0]


@pytest.mark.asyncio
async def test_telegram_command_resume_resets_kill_switch(mock_app: MagicMock) -> None:
    service = TelegramBotService(
        bot_token="TEST_TOKEN",
        chat_id="123456",
        allowed_user_ids="123456",
        app=mock_app,
    )
    service.send_message = AsyncMock()  # type: ignore[method-assign]

    # Disparar kill switch primero
    mock_app.kill_switch.trigger_manual("Prueba", 1000, action=KillSwitchAction.BLOCK_NEW_ENTRIES)
    assert mock_app.kill_switch.get_highest_severity_action() == KillSwitchAction.BLOCK_NEW_ENTRIES

    msg = {
        "chat": {"id": 123456},
        "from": {"id": 123456, "username": "admin"},
        "text": "/resume",
    }

    await service._handle_incoming_message(msg)

    # Verifica que el Kill-Switch se restableció
    assert mock_app.kill_switch.get_highest_severity_action() == KillSwitchAction.NONE
    service.send_message.assert_called_once()
    assert "SISTEMA REANUDADO" in service.send_message.call_args[0][0]


@pytest.mark.asyncio
async def test_telegram_command_shutdown(mock_app: MagicMock) -> None:
    service = TelegramBotService(
        bot_token="TEST_TOKEN",
        chat_id="123456",
        allowed_user_ids="123456",
        app=mock_app,
    )
    service.send_message = AsyncMock()  # type: ignore[method-assign]

    assert mock_app._stop_event.is_set() is False

    msg = {
        "chat": {"id": 123456},
        "from": {"id": 123456, "username": "admin"},
        "text": "/shutdown",
    }

    await service._handle_incoming_message(msg)

    assert mock_app._stop_event.is_set() is True


def test_telegram_chunking_for_long_messages() -> None:
    service = TelegramBotService(
        bot_token="TEST_TOKEN",
        chat_id="123456",
        allowed_user_ids="123456",
        app=MagicMock(),
    )
    long_text = "\n".join([f"Línea de log #{i} con suficiente texto descriptivo" for i in range(200)])
    assert len(long_text) > 4000

    chunks = service._chunk_message(long_text, max_len=4000)
    assert len(chunks) >= 2
    for chunk in chunks:
        assert len(chunk) <= 4000
