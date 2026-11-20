"""Tests unitarios para el simulador PaperExecutionEngine."""

import pytest

from obbot.execution.paper import PaperExecutionEngine


@pytest.mark.asyncio
async def test_paper_engine_order_fill_and_pnl() -> None:
    engine = PaperExecutionEngine(initial_balance=10000.0)
    engine.set_mark_price("BTCUSDT", 50000.0)

    # 1. Compra a mercado
    order_res = await engine.create_order(
        symbol="BTCUSDT",
        side="BUY",
        order_type="MARKET",
        quantity=0.1,  # 5,000 USD
    )
    assert order_res["status"] == "FILLED"

    positions = await engine.get_positions("BTCUSDT")
    assert len(positions) == 1
    assert positions[0]["positionAmt"] == 0.1
    assert positions[0]["entryPrice"] == 50000.0

    # 2. El precio sube a 52,000 USD
    engine.set_mark_price("BTCUSDT", 52000.0)
    positions = await engine.get_positions("BTCUSDT")
    assert positions[0]["unRealizedProfit"] == pytest.approx(200.0, abs=1e-6)

    # 3. Venta a mercado para cerrar la posición
    close_res = await engine.create_order(
        symbol="BTCUSDT",
        side="SELL",
        order_type="MARKET",
        quantity=0.1,
        reduce_only=True,
    )
    assert close_res["status"] == "FILLED"

    # Posición debe ser 0 y balance debe haber aumentado en 200 USD
    positions = await engine.get_positions("BTCUSDT")
    assert positions[0]["positionAmt"] == 0.0
    assert engine.wallet_balance == pytest.approx(10200.0, abs=1e-6)


@pytest.mark.asyncio
async def test_paper_engine_algo_stop_loss_trigger() -> None:
    engine = PaperExecutionEngine(initial_balance=10000.0)
    engine.set_mark_price("BTCUSDT", 50000.0)

    # Abrir LONG
    await engine.create_order(
        symbol="BTCUSDT",
        side="BUY",
        order_type="MARKET",
        quantity=0.1,
    )

    # Colocar Stop-Loss en 49,000 USD
    algo_res = await engine.create_algo_order(
        symbol="BTCUSDT",
        side="SELL",
        algo_type="STOP_MARKET",
        trigger_price=49000.0,
        close_position=True,
    )
    assert algo_res["status"] == "NEW"

    # Precio cae a 48,900 USD -> Debe disparar el Stop-Loss
    engine.set_mark_price("BTCUSDT", 48900.0)

    positions = await engine.get_positions("BTCUSDT")
    assert positions[0]["positionAmt"] == 0.0
    # Pérdida de 0.1 * (48900 - 50000) = -110 USD
    assert engine.wallet_balance == pytest.approx(9890.0, abs=1e-6)


@pytest.mark.asyncio
async def test_paper_engine_algo_take_profit_trigger_and_oco() -> None:
    engine = PaperExecutionEngine(initial_balance=10000.0)
    engine.set_mark_price("BTCUSDT", 50000.0)

    # Abrir LONG
    await engine.create_order(
        symbol="BTCUSDT",
        side="BUY",
        order_type="MARKET",
        quantity=0.1,
    )

    # Colocar Stop-Loss en 49,000 USD
    sl_res = await engine.create_algo_order(
        symbol="BTCUSDT",
        side="SELL",
        algo_type="STOP_MARKET",
        trigger_price=49000.0,
        close_position=True,
    )
    # Colocar Take-Profit en 52,000 USD
    tp_res = await engine.create_algo_order(
        symbol="BTCUSDT",
        side="SELL",
        algo_type="TAKE_PROFIT_MARKET",
        trigger_price=52000.0,
        close_position=True,
    )

    assert sl_res["status"] == "NEW"
    assert tp_res["status"] == "NEW"

    # Al subir a 51,000 USD (no llega a TP 52,000 y está arriba de SL 49,000), ninguna debe dispararse
    engine.set_mark_price("BTCUSDT", 51000.0)
    open_algos = await engine.get_open_algo_orders("BTCUSDT")
    assert len(open_algos) == 2

    # Precio sube a 52,100 USD -> Debe disparar el Take-Profit y cancelar el Stop-Loss (OCO)
    engine.set_mark_price("BTCUSDT", 52100.0)

    positions = await engine.get_positions("BTCUSDT")
    assert positions[0]["positionAmt"] == 0.0
    # Ganancia de 0.1 * (52100 - 50000) = +210 USD
    assert engine.wallet_balance == pytest.approx(10210.0, abs=1e-6)

    # Las órdenes abiertas deben ser 0 (el SL se canceló por OCO)
    open_after = await engine.get_open_algo_orders("BTCUSDT")
    assert len(open_after) == 0
