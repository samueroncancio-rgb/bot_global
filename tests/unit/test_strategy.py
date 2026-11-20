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
    htf_ema_15m: float = 0.0,
    htf_trend_regime: str = "NEUTRAL",
    timestamp_ns: int = 14 * 3600 * 1_000_000_000,  # 14:00 UTC (Sesión Londres / NY)
) -> FeatureVector:
    return FeatureVector(
        symbol=symbol,
        timestamp_ns=timestamp_ns,
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
        htf_ema_15m=htf_ema_15m,
        htf_trend_regime=htf_trend_regime,
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
    assert strat_eth.trade_size == 0.02
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


def test_baseline_strategy_single_position_lock() -> None:
    strat = BaselineOrderFlowStrategy(
        symbol="ETH-USDT-PERP",
        z_threshold=1.8,
        obi_threshold=0.20,
        cooldown_sec=0.0,
        enable_flow_invalidation=False,
    )
    portfolio = PortfolioState(initial_daily_balance=100.0, current_wallet_balance=100.0)
    # Posición LONG abierta registrada con símbolo nativo "ETHUSDT"
    portfolio.positions["ETHUSDT"] = PositionState(
        symbol="ETHUSDT",
        size=0.02,
        entry_price=1780.0,
        mark_price=1780.0,
        unrealized_pnl=0.0,
        leverage=3,
    )

    features = _create_test_feature_vector(
        symbol="ETH-USDT-PERP",
        global_delta_z=2.5,
        obi=0.45,
        mid_price=1780.0,
    )

    # Con la posición abierta, NO debe emitir orden de entrada (Single-Position Lock)
    orders = strat.on_feature_vector(features, portfolio)
    assert len(orders) == 0, "Single-Position Lock debe bloquear cualquier nueva entrada mientras haya posición"


def test_baseline_strategy_eth_minimum_notional_guard() -> None:
    strat = BaselineOrderFlowStrategy(
        symbol="ETH-USDT-PERP",
        trade_size=None,  # dinámico
    )
    # A $1,770 USD, 0.02 ETH = $35.40 USD (supera mínimo $20)
    qty1 = strat.get_trade_size(1770.0)
    assert qty1 >= 0.02
    assert qty1 * 1770.0 >= 21.0

    # Si ETH cayera a $800 USD, debe ajustar el tamaño para superar el nocional mínimo de Binance
    qty2 = strat.get_trade_size(800.0)
    assert qty2 * 800.0 >= 21.0


def test_baseline_strategy_swing_parameters_default() -> None:
    strat = BaselineOrderFlowStrategy(symbol="ETH-USDT-PERP")
    assert strat.stop_loss_pct == 0.0050  # 0.50%
    assert strat.take_profit_pct == 0.0120  # 1.20%
    assert strat.enable_htf_filter is True
    assert strat.enable_flow_invalidation is False
    assert strat.post_close_cooldown_sec == 120.0


def test_baseline_strategy_htf_rejects_counter_trend_signals() -> None:
    strat = BaselineOrderFlowStrategy(
        symbol="ETH-USDT-PERP",
        z_threshold=1.8,
        obi_threshold=0.20,
        enable_htf_filter=True,
    )
    portfolio = PortfolioState(initial_daily_balance=100.0, current_wallet_balance=100.0)

    # 1. Señal BUY pero régimen HTF BEARISH -> Debe ser rechazada
    feat_long_bearish = _create_test_feature_vector(
        symbol="ETH-USDT-PERP",
        global_delta_z=2.5,
        obi=0.45,
        mid_price=1750.0,
        htf_ema_15m=1800.0,
        htf_trend_regime="BEARISH",
    )
    orders = strat.on_feature_vector(feat_long_bearish, portfolio)
    assert len(orders) == 0, "HTF filter debe descartar compras si el régimen HTF es BEARISH"

    # 2. Señal SELL pero régimen HTF BULLISH -> Debe ser rechazada
    feat_short_bullish = _create_test_feature_vector(
        symbol="ETH-USDT-PERP",
        global_delta_z=-2.5,
        obi=-0.45,
        mid_price=1850.0,
        htf_ema_15m=1800.0,
        htf_trend_regime="BULLISH",
    )
    orders = strat.on_feature_vector(feat_short_bullish, portfolio)
    assert len(orders) == 0, "HTF filter debe descartar ventas si el régimen HTF es BULLISH"


def test_baseline_strategy_htf_allows_trend_aligned_signals() -> None:
    strat = BaselineOrderFlowStrategy(
        symbol="ETH-USDT-PERP",
        z_threshold=1.8,
        obi_threshold=0.20,
        enable_htf_filter=True,
    )
    portfolio = PortfolioState(initial_daily_balance=100.0, current_wallet_balance=100.0)

    # Señal BUY alineada con BULLISH
    feat_long_bullish = _create_test_feature_vector(
        symbol="ETH-USDT-PERP",
        global_delta_z=2.5,
        obi=0.45,
        mid_price=1850.0,
        htf_ema_15m=1800.0,
        htf_trend_regime="BULLISH",
    )
    orders = strat.on_feature_vector(feat_long_bullish, portfolio)
    assert len(orders) == 1
    assert orders[0].side == "BUY"
    assert orders[0].stop_loss_price < 1850.0
    assert orders[0].take_profit_price > 1850.0


def test_baseline_strategy_asian_session_higher_confirmation() -> None:
    strat = BaselineOrderFlowStrategy(
        symbol="BTCUSDT",
        z_threshold=1.8,
        obi_threshold=0.20,
        asian_session_z_threshold=2.2,
        enable_session_filter=True,
    )
    portfolio = PortfolioState(initial_daily_balance=100.0, current_wallet_balance=100.0)

    # 03:00 UTC (Sesión Asiática)
    asian_ts_ns = 3 * 3600 * 1_000_000_000

    # 1. Señal con z=2.0 (supera 1.8, pero menor que 2.2 de Asia) -> Rechazada
    feat_low = _create_test_feature_vector(
        symbol="BTCUSDT",
        global_delta_z=2.0,
        obi=0.30,
        timestamp_ns=asian_ts_ns,
    )
    assert len(strat.on_feature_vector(feat_low, portfolio)) == 0

    # 2. Señal con z=2.3 (supera 2.2 de Asia) -> Aceptada
    feat_high = _create_test_feature_vector(
        symbol="BTCUSDT",
        global_delta_z=2.3,
        obi=0.30,
        timestamp_ns=asian_ts_ns,
    )
    assert len(strat.on_feature_vector(feat_high, portfolio)) == 1


def test_baseline_strategy_dynamic_volatility_preserves_swing_ratio() -> None:
    strat = BaselineOrderFlowStrategy(
        symbol="ETH-USDT-PERP",
        stop_loss_pct=0.0050,
        take_profit_pct=0.0120,
        use_dynamic_volatility=True,
        vol_multiplier=1.5,
    )
    portfolio = PortfolioState(initial_daily_balance=100.0, current_wallet_balance=100.0)

    # Volatilidad realizada alta (0.0060 = 60 bps)
    feat = _create_test_feature_vector(
        symbol="ETH-USDT-PERP",
        global_delta_z=2.5,
        obi=0.35,
        mid_price=2000.0,
        realized_vol=0.0060,
    )
    orders = strat.on_feature_vector(feat, portfolio)
    assert len(orders) == 1
    order = orders[0]
    sl_dist = 2000.0 - (order.stop_loss_price or 0.0)
    tp_dist = (order.take_profit_price or 0.0) - 2000.0

    # El ratio TP/SL debe ser al menos 2.40x nominal
    ratio = tp_dist / sl_dist
    assert ratio >= 2.39, f"El ratio R/B debe ser al menos 2.40x, se obtuvo {ratio:.2f}"


def test_strategy_time_stop_exit() -> None:
    """Verifica que la estrategia cierre por Time-Stop si tras 30 min no avanza y el delta se neutraliza."""
    strat = BaselineOrderFlowStrategy(
        symbol="ETH-USDT-PERP",
        enable_time_stop=True,
        time_stop_sec=1800.0,  # 30 minutos
        time_stop_z_threshold=0.50,
        enable_flow_invalidation=False,
    )
    portfolio = PortfolioState(initial_daily_balance=5000.0, current_wallet_balance=5000.0)
    pos = PositionState(
        symbol="ETH-USDT-PERP",
        size=0.02,  # Posición LONG abierta
        entry_price=2000.0,
        mark_price=2001.0,
        unrealized_pnl=0.02,
        leverage=3,
        margin_type="ISOLATED",
    )
    portfolio.positions["ETH-USDT-PERP"] = pos

    t_entry = 1000 * 1_000_000_000  # t = 1000s
    feat_entry = _create_test_feature_vector(
        symbol="ETH-USDT-PERP",
        global_delta_z=1.0,
        obi=0.10,
        mid_price=2001.0,
        timestamp_ns=t_entry,
    )
    # Primera evaluación registra t_entry
    orders1 = strat.on_feature_vector(feat_entry, portfolio)
    assert len(orders1) == 0  # No sale aún

    # Evaluar a los 10 minutos (t = 1600s, elapsed = 600s < 1800s)
    feat_mid = _create_test_feature_vector(
        symbol="ETH-USDT-PERP",
        global_delta_z=0.1,  # Delta neutralizado
        obi=0.05,
        mid_price=2001.0,
        timestamp_ns=t_entry + 600 * 1_000_000_000,
    )
    assert len(strat.on_feature_vector(feat_mid, portfolio)) == 0

    # Evaluar a los 31 minutos (t = 2860s, elapsed = 1860s >= 1800s) con delta neutralizado (z=0.1 <= 0.50)
    feat_expired = _create_test_feature_vector(
        symbol="ETH-USDT-PERP",
        global_delta_z=0.1,
        obi=0.02,
        mid_price=2001.0,
        timestamp_ns=t_entry + 1860 * 1_000_000_000,
    )
    orders_exit = strat.on_feature_vector(feat_expired, portfolio)
    assert len(orders_exit) == 1
    exit_order = orders_exit[0]
    assert exit_order.side == "SELL"
    assert exit_order.reduce_only is True
    assert exit_order.order_type == "MARKET"
    assert exit_order.quantity == 0.02

