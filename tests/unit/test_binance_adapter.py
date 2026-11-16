"""Tests unitarios para el adaptador de Binance USDⓈ-M y verificación categórica."""

import pytest

from obbot.bus.memory import InMemoryEventBus
from obbot.common.events import Side
from obbot.common.registry import InstrumentRegistry
from obbot.ingestion.binance import (
    BinanceAdapter,
    BinanceCategoryViolationError,
    validate_binance_stream_category,
)


def test_binance_category_segregation_validation() -> None:
    public_url = "wss://fstream.binance.com/public"
    market_url = "wss://fstream.binance.com/market"

    # 1. Casos válidos
    validate_binance_stream_category(public_url, ["btcusdt@depth@100ms", "ethusdt@bookTicker"])
    validate_binance_stream_category(
        market_url, ["btcusdt@aggTrade", "btcusdt@markPrice@1s", "btcusdt@forceOrder"]
    )

    # 2. Infracción: pedir trades en /public
    with pytest.raises(BinanceCategoryViolationError, match="no permitido en /public"):
        validate_binance_stream_category(public_url, ["btcusdt@aggTrade"])

    # 3. Infracción: pedir depth en /market
    with pytest.raises(BinanceCategoryViolationError, match="no permitido en /market"):
        validate_binance_stream_category(market_url, ["btcusdt@depth@100ms"])


def test_binance_parse_agg_trade(sample_registry: InstrumentRegistry) -> None:
    bus = InMemoryEventBus()
    adapter = BinanceAdapter(["BTC-USDT-PERP"], sample_registry, bus)

    # Caso 1: m=False -> El comprador es taker (agresor) -> BUY
    raw_payload_buy = {
        "e": "aggTrade",
        "E": 1700000000000,
        "s": "BTCUSDT",
        "a": 1234567,
        "p": "50000.50",
        "q": "0.150",
        "f": 100,
        "l": 105,
        "T": 1700000000000,
        "m": False,
    }
    trade_buy = adapter.parse_agg_trade(raw_payload_buy, '{"dummy": 1}')
    assert trade_buy is not None
    assert trade_buy.symbol == "BTC-USDT-PERP"
    assert trade_buy.side == Side.BUY
    assert trade_buy.price == 50000.50
    assert trade_buy.size == 0.150
    assert trade_buy.notional_usd == pytest.approx(50000.50 * 0.150)

    # Caso 2: m=True -> El vendedor es taker (agresor) -> SELL
    raw_payload_sell = {
        "e": "aggTrade",
        "E": 1700000000100,
        "s": "BTCUSDT",
        "a": 1234568,
        "p": "49999.00",
        "q": "0.500",
        "T": 1700000000100,
        "m": True,
    }
    trade_sell = adapter.parse_agg_trade(raw_payload_sell, '{"dummy": 2}')
    assert trade_sell is not None
    assert trade_sell.side == Side.SELL
    assert trade_sell.price == 49999.00
    assert trade_sell.size == 0.500


def test_binance_parse_force_order_liquidation(sample_registry: InstrumentRegistry) -> None:
    bus = InMemoryEventBus()
    adapter = BinanceAdapter(["BTC-USDT-PERP"], sample_registry, bus)

    raw_liq = {
        "e": "forceOrder",
        "E": 1700000000200,
        "o": {
            "s": "BTCUSDT",
            "S": "SELL",
            "o": "LIMIT",
            "f": "IOC",
            "q": "2.500",
            "p": "48500.00",
            "ap": "48450.00",
            "X": "FILLED",
            "l": "2.500",
            "z": "2.500",
            "T": 1700000000200,
        },
    }
    liq_evt = adapter.parse_force_order(raw_liq, "raw_liq_str")
    assert liq_evt is not None
    assert liq_evt.symbol == "BTC-USDT-PERP"
    assert liq_evt.side == Side.SELL
    assert liq_evt.price == 48500.00
    assert liq_evt.size == 2.500
    assert liq_evt.notional_usd == pytest.approx(48500.00 * 2.5)


def test_binance_parse_mark_price(sample_registry: InstrumentRegistry) -> None:
    bus = InMemoryEventBus()
    adapter = BinanceAdapter(["BTC-USDT-PERP"], sample_registry, bus)

    raw_mp = {
        "e": "markPriceUpdate",
        "E": 1700000000300,
        "s": "BTCUSDT",
        "p": "50050.25",
        "i": "50040.00",
        "r": "0.00010000",
        "T": 1700028800000,
    }
    funding_evt = adapter.parse_mark_price(raw_mp, "raw_mp_str")
    assert funding_evt is not None
    assert funding_evt.symbol == "BTC-USDT-PERP"
    assert funding_evt.funding_rate == 0.0001
    assert funding_evt.next_funding_time_ms == 1700028800000
