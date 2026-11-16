"""Tests unitarios exhaustivos para RiskGuard y validación pre-trade."""

from obbot.risk.guard import RiskGuard
from obbot.risk.kill_switch import FiveLayerKillSwitch
from obbot.risk.limits import (
    OrderRequest,
    PortfolioState,
    PositionState,
    RiskLimits,
)


def _build_test_portfolio(symbol: str = "BTCUSDT", current_qty: float = 0.0) -> PortfolioState:
    portfolio = PortfolioState(
        initial_daily_balance=10000.0,
        current_wallet_balance=10000.0,
    )
    if current_qty != 0.0:
        portfolio.positions = {
            symbol: PositionState(
                symbol=symbol,
                size=current_qty,
                entry_price=50000.0,
                mark_price=50000.0,
                unrealized_pnl=0.0,
                leverage=3,
            )
        }
    return portfolio


def test_risk_guard_valid_order_approval_and_token_consumption() -> None:
    limits = RiskLimits(max_order_notional_usd=2000.0)
    guard = RiskGuard(limits)
    portfolio = _build_test_portfolio()
    now_ns = 1_000_000_000

    order = OrderRequest(
        symbol="BTCUSDT",
        side="BUY",
        order_type="LIMIT",
        quantity=0.02,
        price=50000.0,  # Notional = 1,000 USD <= 2,000 USD
        stop_loss_price=49000.0,
    )

    decision = guard.evaluate_order(
        order=order,
        portfolio=portfolio,
        current_mark_price=50000.0,
        current_ts_ns=now_ns,
    )

    assert decision.approved
    assert decision.token is not None
    assert decision.token.symbol == "BTCUSDT"

    # Consumir el token legítimamente
    consumed = guard.consume_token(decision.token.token_id, order, current_ts_ns=now_ns + 100)
    assert consumed

    # Reintento de doble uso del token debe fallar (un solo uso)
    assert not guard.consume_token(decision.token.token_id, order, current_ts_ns=now_ns + 200)


def test_risk_guard_rejects_exceeded_order_notional() -> None:
    limits = RiskLimits(max_order_notional_usd=2000.0)
    guard = RiskGuard(limits)
    portfolio = _build_test_portfolio()
    now_ns = 1_000_000_000

    # Orden de 0.1 BTC a 50,000 USD = 5,000 USD > 2,000 USD
    order = OrderRequest(
        symbol="BTCUSDT",
        side="BUY",
        order_type="LIMIT",
        quantity=0.1,
        price=50000.0,
        stop_loss_price=49000.0,
    )

    decision = guard.evaluate_order(
        order=order,
        portfolio=portfolio,
        current_mark_price=50000.0,
        current_ts_ns=now_ns,
    )

    assert not decision.approved
    assert "Nocional de orden excesivo" in decision.reason
    assert decision.token is None


def test_risk_guard_rejects_exceeded_position_notional() -> None:
    limits = RiskLimits(max_position_notional_usd=5000.0, max_order_notional_usd=2000.0)
    guard = RiskGuard(limits)
    # Posición existente de 0.08 BTC a 50,000 USD = 4,000 USD
    portfolio = _build_test_portfolio(current_qty=0.08)
    now_ns = 1_000_000_000

    # Nueva orden de 0.03 BTC a 50,000 USD = 1,500 USD (Total proyectado = 5,500 USD > 5,000 USD)
    order = OrderRequest(
        symbol="BTCUSDT",
        side="BUY",
        order_type="LIMIT",
        quantity=0.03,
        price=50000.0,
        stop_loss_price=49000.0,
    )

    decision = guard.evaluate_order(
        order=order,
        portfolio=portfolio,
        current_mark_price=50000.0,
        current_ts_ns=now_ns,
    )

    assert not decision.approved
    assert "Nocional proyectado de posición excesivo" in decision.reason


def test_risk_guard_rejects_fat_finger_price() -> None:
    limits = RiskLimits(fat_finger_price_deviation_pct=0.02)  # Máximo 2%
    guard = RiskGuard(limits)
    portfolio = _build_test_portfolio()
    now_ns = 1_000_000_000

    # Mark price = 50,000 USD, orden a 52,000 USD (4% de desviación)
    order = OrderRequest(
        symbol="BTCUSDT",
        side="BUY",
        order_type="LIMIT",
        quantity=0.01,
        price=52000.0,
        stop_loss_price=49000.0,
    )

    decision = guard.evaluate_order(
        order=order,
        portfolio=portfolio,
        current_mark_price=50000.0,
        current_ts_ns=now_ns,
    )

    assert not decision.approved
    assert "Desviación de precio Fat-Finger" in decision.reason


def test_risk_guard_rejects_invalid_stop_loss_direction() -> None:
    guard = RiskGuard()
    portfolio = _build_test_portfolio()
    now_ns = 1_000_000_000

    # BUY con Stop Loss por ENCIMA del precio de entrada (inválido)
    order_buy = OrderRequest(
        symbol="BTCUSDT",
        side="BUY",
        order_type="LIMIT",
        quantity=0.01,
        price=50000.0,
        stop_loss_price=51000.0,
    )
    decision_buy = guard.evaluate_order(order_buy, portfolio, 50000.0, now_ns)
    assert not decision_buy.approved
    assert "Stop-Loss para BUY debe ser menor" in decision_buy.reason

    # SELL con Stop Loss por DEBAJO del precio de entrada (inválido)
    order_sell = OrderRequest(
        symbol="BTCUSDT",
        side="SELL",
        order_type="LIMIT",
        quantity=0.01,
        price=50000.0,
        stop_loss_price=49000.0,
    )
    decision_sell = guard.evaluate_order(order_sell, portfolio, 50000.0, now_ns)
    assert not decision_sell.approved
    assert "Stop-Loss para SELL debe ser mayor" in decision_sell.reason


def test_risk_guard_blocks_on_kill_switch_active() -> None:
    ks = FiveLayerKillSwitch()
    guard = RiskGuard(kill_switch=ks)
    portfolio = _build_test_portfolio()
    now_ns = 1_000_000_000

    # Disparar Kill-Switch manual
    ks.trigger_manual("Test de parada", now_ns=now_ns)

    # Orden de apertura debe ser bloqueada
    entry_order = OrderRequest(
        symbol="BTCUSDT",
        side="BUY",
        order_type="LIMIT",
        quantity=0.01,
        price=50000.0,
        stop_loss_price=49000.0,
    )
    decision = guard.evaluate_order(entry_order, portfolio, 50000.0, now_ns)
    assert not decision.approved
    assert "Kill-Switch en CLOSE_POSITIONS_EMERGENCY" in decision.reason

    # Orden de salida reduce_only está permitida para desarmar riesgo
    exit_order = OrderRequest(
        symbol="BTCUSDT",
        side="SELL",
        order_type="MARKET",
        quantity=0.01,
        reduce_only=True,
    )
    decision_exit = guard.evaluate_order(exit_order, portfolio, 50000.0, now_ns)
    assert decision_exit.approved
