"""Tests unitarios para la estrategia cuantitativa de baseline."""

from obbot.features.engine import FeatureVector
from obbot.risk.limits import PortfolioState, PositionState
from obbot.strategy.baseline_example import BaselineOrderFlowStrategy


def _create_test_feature_vector(
    symbol: str,
    global_delta_z: float,
    obi: float,
    mid_price: float = 50000.0,
) -> FeatureVector:
    return FeatureVector(
        symbol=symbol,
        timestamp_ns=1_000_000_000,
        delta_by_exchange={"binance": 100.0},
        z_delta_by_exchange={"binance": global_delta_z},
        weights_by_exchange={"binance": 1.0},
        global_delta=global_delta_z,
        cvd_by_exchange={"binance": 500000.0},
        global_cvd=500000.0,
        obi_by_exchange={"binance": {5: obi, 10: obi, 20: obi}},
        liquidity_persistence=0.8,
        mid_price=mid_price,
        spread_bps=1.0,
        basis_bps_by_exchange={"binance": 0.0},
        realized_volatility=0.01,
        liquidation_intensity=0.0,
        funding_rate=0.0001,
        open_interest_usd=1000000.0,
    )


def test_baseline_strategy_emits_long_signal() -> None:
    strat = BaselineOrderFlowStrategy(
        symbol="BTCUSDT",
        z_threshold=1.8,
        obi_threshold=0.20,
        trade_size=0.01,
        stop_loss_pct=0.01,
        take_profit_pct=0.02,
    )
    portfolio = PortfolioState(initial_daily_balance=10000.0, current_wallet_balance=10000.0)

    # Vector con confluencia compradora (z = 2.1 >= 1.8, OBI = 0.35 >= 0.20)
    features = _create_test_feature_vector(
        symbol="BTCUSDT",
        global_delta_z=2.1,
        obi=0.35,
        mid_price=50000.0,
    )

    orders = strat.on_feature_vector(features, portfolio)

    assert len(orders) == 1
    order = orders[0]
    assert order.symbol == "BTCUSDT"
    assert order.side == "BUY"
    assert order.price == 50000.0
    assert order.stop_loss_price == 49500.0  # 50,000 * (1 - 0.01)
    assert order.take_profit_price == 51000.0  # 50,000 * (1 + 0.02)
    assert order.time_in_force == "GTX"


def test_baseline_strategy_emits_short_signal() -> None:
    strat = BaselineOrderFlowStrategy(
        symbol="BTCUSDT",
        z_threshold=1.8,
        obi_threshold=0.20,
        trade_size=0.01,
        stop_loss_pct=0.01,
        take_profit_pct=0.02,
    )
    portfolio = PortfolioState(initial_daily_balance=10000.0, current_wallet_balance=10000.0)

    # Vector con confluencia vendedora (z = -2.5 <= -1.8, OBI = -0.40 <= -0.20)
    features = _create_test_feature_vector(
        symbol="BTCUSDT",
        global_delta_z=-2.5,
        obi=-0.40,
        mid_price=50000.0,
    )

    orders = strat.on_feature_vector(features, portfolio)

    assert len(orders) == 1
    order = orders[0]
    assert order.side == "SELL"
    assert order.stop_loss_price == 50500.0  # 50,000 * (1 + 0.01)
    assert order.take_profit_price == 49000.0  # 50,000 * (1 - 0.02)


def test_baseline_strategy_no_signal_when_position_already_open() -> None:
    strat = BaselineOrderFlowStrategy(symbol="BTCUSDT")
    # Posición existente en BTCUSDT
    portfolio = PortfolioState(
        initial_daily_balance=10000.0,
        current_wallet_balance=10000.0,
        positions={
            "BTCUSDT": PositionState(
                symbol="BTCUSDT",
                size=0.01,
                entry_price=50000.0,
                mark_price=50000.0,
                unrealized_pnl=0.0,
                leverage=3,
            )
        },
    )

    # Señal fuerte de compra, pero ya tenemos posición abierta
    features = _create_test_feature_vector(
        symbol="BTCUSDT",
        global_delta_z=3.0,
        obi=0.50,
        mid_price=50000.0,
    )

    orders = strat.on_feature_vector(features, portfolio)
    assert len(orders) == 0


def test_baseline_strategy_rejects_low_persistence_spoofing() -> None:
    strat = BaselineOrderFlowStrategy(
        symbol="BTCUSDT",
        z_threshold=1.8,
        obi_threshold=0.20,
        enable_anti_spoofing=True,
        min_persistence=0.50,
    )
    portfolio = PortfolioState(initial_daily_balance=10000.0, current_wallet_balance=10000.0)

    # Señal fuerte pero persistencia baja (rho = 0.20 < 0.50 -> quote flickering / spoofing)
    features = FeatureVector(
        symbol="BTCUSDT",
        timestamp_ns=1_000_000_000,
        delta_by_exchange={"binance": 100.0},
        z_delta_by_exchange={"binance": 2.5},
        weights_by_exchange={"binance": 1.0},
        global_delta=2.5,
        cvd_by_exchange={"binance": 500000.0},
        global_cvd=500000.0,
        obi_by_exchange={"binance": {5: 0.35, 10: 0.35, 20: 0.35}},
        liquidity_persistence=0.20,  # Persistencia baja
        mid_price=50000.0,
        spread_bps=1.0,
        basis_bps_by_exchange={"binance": 0.0},
        realized_volatility=0.01,
        liquidation_intensity=0.0,
        funding_rate=0.0001,
        open_interest_usd=1000000.0,
    )

    orders = strat.on_feature_vector(features, portfolio)
    assert len(orders) == 0, "Debe rechazar la señal ante persistencia baja de liquidez"


def test_baseline_strategy_rejects_phantom_wall() -> None:
    strat = BaselineOrderFlowStrategy(
        symbol="BTCUSDT",
        z_threshold=1.8,
        obi_threshold=0.20,
        enable_anti_spoofing=True,
    )
    portfolio = PortfolioState(initial_daily_balance=10000.0, current_wallet_balance=10000.0)

    # Muro fantasma: OBI a 20 niveles es alcista (+0.40), pero a 5 niveles inmediatos es vendedor (-0.15)
    features = FeatureVector(
        symbol="BTCUSDT",
        timestamp_ns=1_000_000_000,
        delta_by_exchange={"binance": 100.0},
        z_delta_by_exchange={"binance": 2.5},
        weights_by_exchange={"binance": 1.0},
        global_delta=2.5,
        cvd_by_exchange={"binance": 500000.0},
        global_cvd=500000.0,
        obi_by_exchange={"binance": {5: -0.15, 10: 0.25, 20: 0.40}},
        liquidity_persistence=0.85,
        mid_price=50000.0,
        spread_bps=1.0,
        basis_bps_by_exchange={"binance": 0.0},
        realized_volatility=0.01,
        liquidation_intensity=0.0,
        funding_rate=0.0001,
        open_interest_usd=1000000.0,
    )

    orders = strat.on_feature_vector(features, portfolio)
    assert len(orders) == 0, "Debe rechazar la señal ante un muro fantasma en niveles lejanos"


def test_baseline_strategy_rejects_isolated_exchange_spoofing() -> None:
    strat = BaselineOrderFlowStrategy(
        symbol="BTCUSDT",
        z_threshold=1.8,
        obi_threshold=0.20,
        enable_anti_spoofing=True,
        min_confirming_venues=2,
    )
    portfolio = PortfolioState(initial_daily_balance=10000.0, current_wallet_balance=10000.0)

    # Solo Binance reporta compra (+0.45), mientras que Bybit y OKX reportan libro vendedor (-0.30, -0.20)
    features = FeatureVector(
        symbol="BTCUSDT",
        timestamp_ns=1_000_000_000,
        delta_by_exchange={"binance": 100.0},
        z_delta_by_exchange={"binance": 2.5},
        weights_by_exchange={"binance": 1.0},
        global_delta=2.5,
        cvd_by_exchange={"binance": 500000.0},
        global_cvd=500000.0,
        obi_by_exchange={
            "binance": {5: 0.45, 10: 0.45, 20: 0.45},
            "bybit": {5: -0.30, 10: -0.30, 20: -0.30},
            "okx": {5: -0.20, 10: -0.20, 20: -0.20},
        },
        liquidity_persistence=0.85,
        mid_price=50000.0,
        spread_bps=1.0,
        basis_bps_by_exchange={"binance": 0.0},
        realized_volatility=0.01,
        liquidation_intensity=0.0,
        funding_rate=0.0001,
        open_interest_usd=1000000.0,
    )

    orders = strat.on_feature_vector(features, portfolio)
    assert len(orders) == 0, "Debe rechazar spoofing monoplataforma sin confirmación multi-exchange"

