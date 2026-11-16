"""Tests unitarios para la CLI y ciclo de vida de la aplicación principal (main.py)."""

from unittest.mock import patch

import pytest

from obbot.common.config import AppConfig
from obbot.main import OrderFlowBotApp, parse_args


def test_main_cli_arg_parser() -> None:
    test_args = ["main.py", "--mode", "paper", "--symbol", "BTC-USDT-PERP"]
    with patch("sys.argv", test_args):
        args = parse_args()
        assert args.mode == "paper"
        assert args.symbol == "BTC-USDT-PERP"


@pytest.mark.asyncio
async def test_order_flow_bot_app_start_and_stop() -> None:
    config = AppConfig()
    app = OrderFlowBotApp(mode="paper", symbol="BTC-USDT-PERP", config=config)

    # Iniciar y detener componentes
    with (
        patch("obbot.main.start_metrics_server"),
        patch("obbot.obs.health.HealthServer.start"),
        patch("obbot.obs.health.HealthServer.stop"),
        patch("obbot.ingestion.binance.BinanceAdapter.start"),
        patch("obbot.ingestion.binance.BinanceAdapter.stop"),
        patch("obbot.ingestion.okx.OkxAdapter.start"),
        patch("obbot.ingestion.okx.OkxAdapter.stop"),
        patch("obbot.ingestion.bybit.BybitAdapter.start"),
        patch("obbot.ingestion.bybit.BybitAdapter.stop"),
        patch("obbot.ingestion.hyperliquid.HyperliquidAdapter.start"),
        patch("obbot.ingestion.hyperliquid.HyperliquidAdapter.stop"),
    ):
        await app.start()
        assert app.running is True
        assert app.bus is not None
        assert app.risk_guard is not None

        await app.stop()
        assert app.running is False

