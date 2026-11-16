"""Tests unitarios para el Instrument Registry y la normalización dimensional."""

from decimal import Decimal

import pytest

from obbot.common.registry import InstrumentRegistry


def test_symbol_resolution(sample_registry: InstrumentRegistry) -> None:
    # De nativo a canónico
    assert sample_registry.to_canonical("binance", "BTCUSDT") == "BTC-USDT-PERP"
    assert sample_registry.to_canonical("okx", "BTC-USDT-SWAP") == "BTC-USDT-PERP"
    assert sample_registry.to_canonical("bybit", "BTCUSDT") == "BTC-USDT-PERP"
    assert sample_registry.to_canonical("hyperliquid", "BTC") == "BTC-USDT-PERP"

    # De canónico a nativo
    assert sample_registry.to_exchange("binance", "BTC-USDT-PERP") == "BTCUSDT"
    assert sample_registry.to_exchange("okx", "BTC-USDT-PERP") == "BTC-USDT-SWAP"
    assert sample_registry.to_exchange("hyperliquid", "BTC-USDT-PERP") == "BTC"

    with pytest.raises(KeyError):
        sample_registry.to_canonical("binance", "SOLUSDT")


def test_multiplier_and_normalization(sample_registry: InstrumentRegistry) -> None:
    # En Binance 1 contrato = 1 BTC
    binance_norm = sample_registry.normalize_size("binance", "BTC-USDT-PERP", 2.5)
    assert binance_norm == Decimal("2.5")

    # En OKX 1 contrato = 0.01 BTC -> 100 contratos = 1 BTC
    okx_norm = sample_registry.normalize_size("okx", "BTC-USDT-PERP", 100)
    assert okx_norm == Decimal("1.0")

    # Cálculo de notional USD
    notional_okx = sample_registry.calculate_notional_usd(
        "okx", "BTC-USDT-PERP", price=50000.0, raw_qty=100
    )
    assert notional_okx == Decimal("50000.0")


def test_price_and_size_rounding(sample_registry: InstrumentRegistry) -> None:
    # tick_size = 0.10
    assert sample_registry.round_price("BTC-USDT-PERP", Decimal("50000.14")) == Decimal("50000.10")
    assert sample_registry.round_price("BTC-USDT-PERP", Decimal("50000.16")) == Decimal("50000.20")

    # step_size = 0.001 (truncamiento hacia abajo obligatorio por defecto)
    assert sample_registry.round_size("BTC-USDT-PERP", Decimal("0.1239")) == Decimal("0.123")
    assert sample_registry.round_size("BTC-USDT-PERP", Decimal("0.0005")) == Decimal("0.000")


def test_order_preconditions_validation(sample_registry: InstrumentRegistry) -> None:
    # Válida: 0.01 BTC @ 50000 USD = 500 USD nocional (> 5 USD)
    valid, err = sample_registry.validate_order(
        "BTC-USDT-PERP", price=Decimal("50000.0"), size=Decimal("0.01")
    )
    assert valid is True
    assert err == ""

    # Rechazo por min qty (< 0.001)
    valid, err = sample_registry.validate_order(
        "BTC-USDT-PERP", price=Decimal("50000.0"), size=Decimal("0.0005")
    )
    assert valid is False
    assert "inferior al mínimo permitido" in err

    # Rechazo por min notional (< 5 USD)
    valid, err = sample_registry.validate_order(
        "BTC-USDT-PERP", price=Decimal("1000.0"), size=Decimal("0.001")
    )
    assert valid is False
    assert "inferior al mínimo de 5.0 USD" in err

    # Rechazo por alineación de step_size
    valid, err = sample_registry.validate_order(
        "BTC-USDT-PERP", price=Decimal("50000.0"), size=Decimal("0.0015")
    )
    assert valid is False
    assert "no es múltiplo de stepSize" in err
