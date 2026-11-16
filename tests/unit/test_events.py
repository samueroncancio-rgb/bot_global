"""Tests unitarios para modelos de datos de eventos."""

from dataclasses import FrozenInstanceError

import pytest

from obbot.common.events import (
    BookSnapshotEvent,
    FundingOIEvent,
    HealthEvent,
    HealthStatus,
    LiquidationEvent,
    OrderBookLevel,
    OrderEvent,
    OrderStatus,
    OrderType,
    Side,
    TradeEvent,
    event_to_dict,
)


def test_trade_event_immutability() -> None:
    trade = TradeEvent(
        exchange="binance",
        symbol="BTC-USDT-PERP",
        ts_exchange_ms=1700000000000,
        ts_recv_ns=1700000000000000000,
        trade_id="123456",
        price=50000.0,
        size=0.1,
        notional_usd=5000.0,
        side=Side.BUY,
    )
    assert trade.exchange == "binance"
    assert trade.price == 50000.0
    assert trade.side == Side.BUY

    # Comprobar inmutabilidad estricta
    with pytest.raises(FrozenInstanceError):
        trade.price = 51000.0  # type: ignore[misc]


def test_book_snapshot_serialization() -> None:
    snapshot = BookSnapshotEvent(
        exchange="okx",
        symbol="BTC-USDT-PERP",
        ts_exchange_ms=1700000000100,
        ts_recv_ns=1700000000100000000,
        bids=(OrderBookLevel(price=49999.0, size=1.5), OrderBookLevel(price=49998.0, size=2.0)),
        asks=(OrderBookLevel(price=50001.0, size=0.8),),
        last_update_id=987654321,
    )
    d = event_to_dict(snapshot)
    assert d["exchange"] == "okx"
    assert d["bids"] == [[49999.0, 1.5], [49998.0, 2.0]]
    assert d["asks"] == [[50001.0, 0.8]]
    assert d["last_update_id"] == 987654321


def test_liquidation_and_funding_events() -> None:
    liq = LiquidationEvent(
        exchange="binance",
        symbol="BTC-USDT-PERP",
        ts_exchange_ms=1700000000200,
        ts_recv_ns=1700000000200000000,
        side=Side.SELL,
        price=48000.0,
        size=0.5,
        notional_usd=24000.0,
    )
    assert liq.side == Side.SELL
    assert liq.notional_usd == 24000.0

    funding = FundingOIEvent(
        exchange="bybit",
        symbol="BTC-USDT-PERP",
        ts_exchange_ms=1700000000300,
        ts_recv_ns=1700000000300000000,
        funding_rate=0.0001,
        open_interest=5000.0,
        open_interest_usd=250000000.0,
        next_funding_time_ms=1700028800000,
    )
    assert funding.funding_rate == 0.0001
    assert funding.open_interest_usd == 250000000.0


def test_order_and_health_events() -> None:
    health = HealthEvent(
        ts_recv_ns=1700000000400000000,
        component="binance_public_ws",
        status=HealthStatus.OK,
        latency_ms=12.5,
        details="Heartbeat OK",
    )
    assert health.status == HealthStatus.OK
    assert health.latency_ms == 12.5

    order = OrderEvent(
        ts_recv_ns=1700000000500000000,
        client_order_id="OBB_12345_01",
        exchange_order_id="BIN_9999",
        symbol="BTC-USDT-PERP",
        side=Side.BUY,
        order_type=OrderType.LIMIT,
        price=50000.0,
        size=0.01,
        filled_size=0.01,
        status=OrderStatus.FILLED,
        is_algo=False,
    )
    assert order.status == OrderStatus.FILLED
    assert order.client_order_id == "OBB_12345_01"
