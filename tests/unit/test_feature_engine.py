"""Tests unitarios para el motor unificado de features (FeatureEngine)."""

from obbot.common.clock import SimulationClock
from obbot.common.events import (
    BookSnapshotEvent,
    FundingOIEvent,
    LiquidationEvent,
    OrderBookLevel,
    Side,
    TradeEvent,
)
from obbot.features.engine import FeatureEngine


def test_feature_engine_integration() -> None:
    clock = SimulationClock(initial_time_ns=1_000_000_000_000)
    engine = FeatureEngine("BTC-USDT-PERP", clock=clock)

    # 1. Alimentar snapshots de libros L2
    snap_binance = BookSnapshotEvent(
        exchange="binance",
        symbol="BTC-USDT-PERP",
        ts_exchange_ms=1000,
        ts_recv_ns=1_000_000_000_000,
        bids=(OrderBookLevel(50000.0, 2.0), OrderBookLevel(49990.0, 5.0)),
        asks=(OrderBookLevel(50010.0, 1.0), OrderBookLevel(50020.0, 4.0)),
    )
    snap_okx = BookSnapshotEvent(
        exchange="okx",
        symbol="BTC-USDT-PERP",
        ts_exchange_ms=1000,
        ts_recv_ns=1_000_000_000_000,
        bids=(OrderBookLevel(50002.0, 1.5),),
        asks=(OrderBookLevel(50012.0, 1.5),),
    )
    engine.on_book_snapshot(snap_binance)
    engine.on_book_snapshot(snap_okx)

    # 2. Alimentar trades
    trade_b = TradeEvent(
        exchange="binance",
        symbol="BTC-USDT-PERP",
        trade_id="1",
        price=50005.0,
        size=1.0,
        notional_usd=50005.0,
        side=Side.BUY,
        ts_exchange_ms=1001,
        ts_recv_ns=1_000_000_100_000,
    )
    trade_o = TradeEvent(
        exchange="okx",
        symbol="BTC-USDT-PERP",
        trade_id="2",
        price=50006.0,
        size=0.5,
        notional_usd=25003.0,
        side=Side.SELL,
        ts_exchange_ms=1002,
        ts_recv_ns=1_000_000_200_000,
    )
    engine.on_trade(trade_b)
    engine.on_trade(trade_o)

    # 3. Alimentar liquidación y funding
    liq = LiquidationEvent(
        exchange="binance",
        symbol="BTC-USDT-PERP",
        side=Side.BUY,
        price=50000.0,
        size=0.1,
        notional_usd=5000.0,
        ts_exchange_ms=1003,
        ts_recv_ns=1_000_000_300_000,
    )
    funding = FundingOIEvent(
        exchange="binance",
        symbol="BTC-USDT-PERP",
        funding_rate=0.0001,
        open_interest=1000.0,
        open_interest_usd=50_000_000.0,
        next_funding_time_ms=1700028800000,
        ts_exchange_ms=1004,
        ts_recv_ns=1_000_000_400_000,
    )
    engine.on_liquidation(liq)
    engine.on_funding_oi(funding)

    # 4. Sintetizar FeatureVector
    fv = engine.compute_feature_vector(current_ts_ns=1_000_000_500_000)

    assert fv.symbol == "BTC-USDT-PERP"
    assert fv.timestamp_ns == 1_000_000_500_000
    assert fv.mid_price == 50005.0  # Mid de Binance: (50000 + 50010) / 2
    assert fv.spread_bps > 0.0
    assert fv.funding_rate == 0.0001
    assert fv.open_interest_usd == 50_000_000.0

    # Comprobar OBI calculado para Binance y OKX
    assert 5 in fv.obi_by_exchange["binance"]
    assert 5 in fv.obi_by_exchange["okx"]

    # Comprobar CVD
    assert fv.cvd_by_exchange["binance"] == 50005.0
    assert fv.cvd_by_exchange["okx"] == -25003.0
    assert fv.global_cvd == 50005.0 - 25003.0

    # Comprobar Basis: OKX mid es 50007.0 vs Binance mid 50005.0 (+2 USD)
    assert fv.basis_bps_by_exchange["okx"] > 0.0
