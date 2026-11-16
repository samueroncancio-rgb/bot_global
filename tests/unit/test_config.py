"""Tests unitarios para la carga y validación de archivos de configuración."""

from decimal import Decimal

from obbot.common.config import AppConfig, EnvSettings, load_yaml
from obbot.common.registry import InstrumentRegistry


def test_load_all_yaml_configs() -> None:
    config = AppConfig(configs_dir="configs")

    # Verificar exchanges
    assert "binance" in config.exchanges["exchanges"]
    assert "okx" in config.exchanges["exchanges"]
    assert "bybit" in config.exchanges["exchanges"]
    assert "hyperliquid" in config.exchanges["exchanges"]

    binance_cfg = config.exchanges["exchanges"]["binance"]
    assert binance_cfg["is_execution"] is True
    # Confirmar URLs de WebSocket segregadas (sin /ws legacy)
    assert binance_cfg["ws_public_url"] == "wss://fstream.binance.com/public"
    assert binance_cfg["ws_market_url"] == "wss://fstream.binance.com/market"
    assert binance_cfg["ws_private_url"] == "wss://fstream.binance.com/private"

    # Verificar riesgo
    assert config.risk["risk"]["execution_mode"] == "paper"
    assert config.risk["risk"]["margin_type"] == "ISOLATED"
    assert config.risk["risk"]["position_mode"] == "ONE_WAY"
    assert config.risk["risk"]["limits"]["max_order_notional_usd"] > 0

    # Verificar features
    assert config.features["features"]["sampling_interval_ms"] == 100
    assert config.features["features"]["obi"]["levels"] == [5, 10, 20]


def test_registry_integration_with_yaml_symbols() -> None:
    symbols_data = load_yaml("configs/symbols.yaml")
    registry = InstrumentRegistry()
    registry.load_from_dict(symbols_data)

    spec_btc = registry.get("BTC-USDT-PERP")
    assert spec_btc.base_asset == "BTC"
    assert spec_btc.tick_size == Decimal("0.10")
    assert spec_btc.step_size == Decimal("0.001")
    assert spec_btc.get_multiplier("okx") == Decimal("0.01")
    assert spec_btc.get_multiplier("binance") == Decimal("1.0")

    spec_eth = registry.get("ETH-USDT-PERP")
    assert spec_eth.base_asset == "ETH"
    assert spec_eth.tick_size == Decimal("0.01")
    assert spec_eth.step_size == Decimal("0.01")
    assert spec_eth.get_multiplier("okx") == Decimal("0.1")


def test_env_settings_defaults() -> None:
    env = EnvSettings()
    assert env.bot_mode == "paper"
    assert env.bus_type == "memory"
    assert env.storage_backend == "local"
    assert env.llm_provider == "gemini"
