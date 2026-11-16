"""Configuración y fixtures compartidas de pytest."""

from decimal import Decimal

import pytest

from obbot.common.clock import SimulationClock, SystemClock
from obbot.common.registry import InstrumentRegistry, InstrumentSpec


@pytest.fixture
def system_clock() -> SystemClock:
    return SystemClock()


@pytest.fixture
def simulation_clock() -> SimulationClock:
    return SimulationClock(initial_time_ns=1_700_000_000_000_000_000)


@pytest.fixture
def sample_registry() -> InstrumentRegistry:
    registry = InstrumentRegistry()
    spec_btc = InstrumentSpec(
        canonical_symbol="BTC-USDT-PERP",
        base_asset="BTC",
        quote_asset="USDT",
        settlement_asset="USDT",
        contract_type="perpetual",
        tick_size=Decimal("0.10"),
        step_size=Decimal("0.001"),
        min_qty=Decimal("0.001"),
        min_notional_usd=Decimal("5.0"),
        exchange_mapping={
            "binance": "BTCUSDT",
            "okx": "BTC-USDT-SWAP",
            "bybit": "BTCUSDT",
            "hyperliquid": "BTC",
        },
        contract_multipliers={
            "binance": Decimal("1.0"),
            "okx": Decimal("0.01"),
            "bybit": Decimal("1.0"),
            "hyperliquid": Decimal("1.0"),
        },
        weights_prior={
            "binance": 0.48,
            "okx": 0.20,
            "hyperliquid": 0.18,
            "bybit": 0.14,
        },
    )
    registry.register(spec_btc)
    return registry
