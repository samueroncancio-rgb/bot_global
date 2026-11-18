"""Tests unitarios para la estrategia cuantitativa de baseline optimizada."""

from obbot.execution.order_manager import ManagedOrder, OrderStatus
from obbot.features.engine import FeatureVector
from obbot.risk.limits import PortfolioState, PositionState
from obbot.strategy.baseline_example import BaselineOrderFlowStrategy


def _create_test_feature_vector(
    symbol: str,
    global_delta_z: float,
    obi: float,
    mid_price: float = 50000.0,
    persistence: float = 0.80,
    realized_vol: float = 0.0020,
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
        liquidity_persistence=persistence,
        mid_price=mid_price,
        spread_bps=1.0,
        basis_bps_by_exchange={"binance": 0.0},
        realized_volatility=realized_vol,
        liquidation_intensity=0.0,
        funding_rate=0.0001,
        open_interest_usd=1000000.0,
    )


def test_baseline_strategy_emits_market_long_signal() -> None:
    strat = BaselineOrderFlowStrategy(
        symbol="BTCUSDT",
        z_threshold=1.8,
        obi_threshold=0.20,
        trade_size=0.001,
        stop_loss_pct=0.002,
        take_profit_pct=0.004,
        use_dynamic_volatility=False,
    )
    portfolio = PortfolioState(initial_daily_balance=100.0, current_wallet_balance=100.0)

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
    assert order.order_type == "MARKET"
    assert order.price == 0.0
    assert order.quantity == 0.001
    assert order.stop_loss_price == 49900.0  # 50,000 * (1 - 0.002)
    assert order.take_profit_price == 50200.0  # 50,000 * (1 + 0.004)


def test_baseline_strategy_emits_market_short_signal() -> None:
    strat = BaselineOrderFlowStrategy(
        symbol="BTCUSDT",
        z_threshold=1.8,
        obi_threshold=0.20,
        trade_size=0.001,
        stop_loss_pct=0.002,
        take_profit_pct=0.004,
        use_dynamic_volatility=False,
    )
    portfolio = PortfolioState(initial_daily_balance=100.0, current_wallet_balance=100.0)

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
    assert order.order_type == "MARKET"
    assert order.price == 0.0
    assert order.stop_loss_price == 50100.0  # 50,000 * (1 + 0.002)
    assert order.take_profit_price == 49800.0  # 50,000 * (1 - 0.004)


def test_baseline_strategy_flow_invalidation_closes_long() -> None:
    strat = BaselineOrderFlowStrategy(
        symbol="BTCUSDT",
        enable_flow_invalidation=True,
        invalidation_z_threshold=1.0,
        invalidation_obi_threshold=0.25,
    )
    # Posición LONG abierta
    portfolio = PortfolioState(
        initial_daily_balance=100.0,
        current_wallet_balance=100.0,
        positions={
            "BTCUSDT": PositionState(
                symbol="BTCUSDT",
                size=0.001,
                entry_price=50000.0,
                mark_price=49950.0,
                unrealized_pnl=-0.05,
                leverage=3,
            )
        },
    )

    # El flujo agresor se invierte violentamente contra el Long (Z = -1.5, OBI = -0.30)
    features = _create_test_feature_vector(
        symbol="BTCUSDT",
        global_delta_z=-1.5,
        obi=-0.30,
        mid_price=49950.0,
    )

    orders = strat.on_feature_vector(features, portfolio)

    assert len(orders) == 1
    order = orders[0]
    assert order.side == "SELL"
    assert order.order_type == "MARKET"
    assert order.quantity == 0.001
    assert order.reduce_only is True


def test_baseline_strategy_flow_invalidation_closes_short() -> None:
    strat = BaselineOrderFlowStrategy(
        symbol="BTCUSDT",
        enable_flow_invalidation=True,
        invalidation_z_threshold=1.0,
        invalidation_obi_threshold=0.25,
    )
    # Posición SHORT abierta
    portfolio = PortfolioState(
        initial_daily_balance=100.0,
        current_wallet_balance=100.0,
        positions={
            "BTCUSDT": PositionState(
                symbol="BTCUSDT",
                size=-0.001,
                entry_price=50000.0,
                mark_price=50050.0,
                unrealized_pnl=-0.05,
                leverage=3,
            )
        },
    )

    # El flujo agresor se invierte violentamente contra el Short (Z = +1.6, OBI = +0.35)
    features = _create_test_feature_vector(
        symbol="BTCUSDT",
        global_delta_z=1.6,
        obi=0.35,
        mid_price=50050.0,
    )

    orders = strat.on_feature_vector(features, portfolio)

    assert len(orders) == 1
    order = orders[0]
    assert order.side == "BUY"
    assert order.order_type == "MARKET"
    assert order.quantity == 0.001
    assert order.reduce_only is True


def test_baseline_strategy_adaptive_sizing_for_eth_and_btc() -> None:
    strat_btc = BaselineOrderFlowStrategy(symbol="BTC-USDT-PERP")
    assert strat_btc.trade_size == 0.001
    assert strat_btc.tick_size == 0.1

    strat_eth = BaselineOrderFlowStrategy(symbol="ETH-USDT-PERP")
    assert strat_eth.trade_size == 0.01
    assert strat_eth.tick_size == 0.01


def test_baseline_strategy_rejects_uncalibrated_persistence_during_warmup() -> None:
    strat = BaselineOrderFlowStrategy(
        symbol="BTCUSDT",
        enable_anti_spoofing=True,
        require_persistence_warmup=True,
    )
    portfolio = PortfolioState(initial_daily_balance=100.0, current_wallet_balance=100.0)

    # Búferes aún sin calibrar: persistence == 0.0
    features = _create_test_feature_vector(
        symbol="BTCUSDT",
        global_delta_z=2.5,
        obi=0.40,
        persistence=0.0,
    )

    orders = strat.on_feature_vector(features, portfolio)
    assert len(orders) == 0, "Debe rechazar la señal si la persistencia es 0.0 (en calentamiento)"


def test_baseline_strategy_rejects_low_persistence_spoofing() -> None:
    strat = BaselineOrderFlowStrategy(
        symbol="BTCUSDT",
        z_threshold=1.8,
        obi_threshold=0.20,
        enable_anti_spoofing=True,
        min_persistence=0.50,
    )
    portfolio = PortfolioState(initial_daily_balance=100.0, current_wallet_balance=100.0)

    features = _create_test_feature_vector(
        symbol="BTCUSDT",
        global_delta_z=2.5,
        obi=0.35,
        persistence=0.20,  # Persistencia baja (quote flickering)
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
    portfolio = PortfolioState(initial_daily_balance=100.0, current_wallet_balance=100.0)

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
    portfolio = PortfolioState(initial_daily_balance=100.0, current_wallet_balance=100.0)

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


def test_baseline_strategy_cooldown_prevents_order_bursts() -> None:
    strat = BaselineOrderFlowStrategy(
        symbol="BTCUSDT",
        z_threshold=1.8,
        obi_threshold=0.20,
        cooldown_sec=3.0,
        use_dynamic_volatility=False,
    )
    portfolio = PortfolioState(initial_daily_balance=100.0, current_wallet_balance=100.0)

    # 1. Primera señal en t = 1,000,000,000 ns -> debe emitir orden
    features1 = _create_test_feature_vector(
        symbol="BTCUSDT",
        global_delta_z=2.5,
        obi=0.40,
        mid_price=50000.0,
    )
    features1 = FeatureVector(
        symbol="BTCUSDT",
        timestamp_ns=1_000_000_000,
        delta_by_exchange=features1.delta_by_exchange,
        z_delta_by_exchange=features1.z_delta_by_exchange,
        weights_by_exchange=features1.weights_by_exchange,
        global_delta=features1.global_delta,
        cvd_by_exchange=features1.cvd_by_exchange,
        global_cvd=features1.global_cvd,
        obi_by_exchange=features1.obi_by_exchange,
        liquidity_persistence=features1.liquidity_persistence,
        mid_price=features1.mid_price,
        spread_bps=features1.spread_bps,
        basis_bps_by_exchange=features1.basis_bps_by_exchange,
        realized_volatility=features1.realized_volatility,
        liquidation_intensity=features1.liquidation_intensity,
        funding_rate=features1.funding_rate,
        open_interest_usd=features1.open_interest_usd,
    )
    orders1 = strat.on_feature_vector(features1, portfolio)
    assert len(orders1) == 1

    # 2. Segunda señal solo 100ms después (t = 1,100,000,000 ns) -> debe bloquearse por Cooldown
    features2 = FeatureVector(
        symbol="BTCUSDT",
        timestamp_ns=1_100_000_000,
        delta_by_exchange=features1.delta_by_exchange,
        z_delta_by_exchange=features1.z_delta_by_exchange,
        weights_by_exchange=features1.weights_by_exchange,
        global_delta=features1.global_delta,
        cvd_by_exchange=features1.cvd_by_exchange,
        global_cvd=features1.global_cvd,
        obi_by_exchange=features1.obi_by_exchange,
        liquidity_persistence=features1.liquidity_persistence,
        mid_price=features1.mid_price,
        spread_bps=features1.spread_bps,
        basis_bps_by_exchange=features1.basis_bps_by_exchange,
        realized_volatility=features1.realized_volatility,
        liquidation_intensity=features1.liquidation_intensity,
        funding_rate=features1.funding_rate,
        open_interest_usd=features1.open_interest_usd,
    )
    orders2 = strat.on_feature_vector(features2, portfolio)
    assert len(orders2) == 0, "Debe ser bloqueada por Cooldown activo"

    # 3. Tercera señal tras 3.5 segundos (t = 4,500,000,000 ns) -> debe emitir orden
    features3 = FeatureVector(
        symbol="BTCUSDT",
        timestamp_ns=4_500_000_000,
        delta_by_exchange=features1.delta_by_exchange,
        z_delta_by_exchange=features1.z_delta_by_exchange,
        weights_by_exchange=features1.weights_by_exchange,
        global_delta=features1.global_delta,
        cvd_by_exchange=features1.cvd_by_exchange,
        global_cvd=features1.global_cvd,
        obi_by_exchange=features1.obi_by_exchange,
        liquidity_persistence=features1.liquidity_persistence,
        mid_price=features1.mid_price,
        spread_bps=features1.spread_bps,
        basis_bps_by_exchange=features1.basis_bps_by_exchange,
        realized_volatility=features1.realized_volatility,
        liquidation_intensity=features1.liquidation_intensity,
        funding_rate=features1.funding_rate,
        open_interest_usd=features1.open_interest_usd,
    )
    orders3 = strat.on_feature_vector(features3, portfolio)
    assert len(orders3) == 1, "Debe permitir la orden tras expirar el Cooldown"


def test_baseline_strategy_in_flight_guard_blocks_duplicate_entries() -> None:
    strat = BaselineOrderFlowStrategy(
        symbol="BTCUSDT",
        z_threshold=1.8,
        obi_threshold=0.20,
        cooldown_sec=0.0,
        use_dynamic_volatility=False,
    )
    portfolio = PortfolioState(initial_daily_balance=100.0, current_wallet_balance=100.0)

    # Registrar una orden previa en tránsito (SUBMITTED)
    pending_order = ManagedOrder(
        client_order_id="OB_BTCUSDT_BUY_1",
        symbol="BTCUSDT",
        side="BUY",
        order_type="MARKET",
        quantity=0.001,
        price=0.0,
        reduce_only=False,
        status=OrderStatus.SUBMITTED,
    )
    strat.on_order_update(pending_order)

    features = _create_test_feature_vector(
        symbol="BTCUSDT",
        global_delta_z=2.5,
        obi=0.40,
        mid_price=50000.0,
    )

    orders = strat.on_feature_vector(features, portfolio)
    assert len(orders) == 0, "No debe emitir orden si ya hay una orden no terminal en vuelo"

    # Cuando la orden previa pasa a FILLED o CANCELLED, se libera la guardia
    pending_order.status = OrderStatus.CANCELLED
    strat.on_order_update(pending_order)

    orders_after = strat.on_feature_vector(features, portfolio)
    assert len(orders_after) == 1, "Debe permitir orden una vez finalizada la orden anterior"
