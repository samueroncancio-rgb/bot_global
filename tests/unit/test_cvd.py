"""Tests unitarios para el acumulador de CVD."""

from obbot.features.cvd import CVDTracker


def test_cvd_accumulation_and_reset() -> None:
    tracker = CVDTracker("BTC-USDT-PERP")

    # Trade Binance BUY +1000
    tracker.on_trade("binance", 1000.0)
    # Trade OKX SELL -500
    tracker.on_trade("okx", -500.0)
    # Trade Bybit BUY +200
    tracker.on_trade("bybit", 200.0)

    assert tracker.get_cvd("binance") == 1000.0
    assert tracker.get_cvd("okx") == -500.0
    assert tracker.get_cvd("bybit") == 200.0
    assert tracker.get_global_cvd() == 700.0  # 1000 - 500 + 200

    tracker.reset()
    assert tracker.get_global_cvd() == 0.0
    assert tracker.get_cvd("binance") == 0.0


def test_cvd_automated_utc_session_reset() -> None:
    tracker = CVDTracker("ETH-USDT-PERP")

    # Día 1: 12:00 UTC (12 * 3600 * 1e9 ns)
    t_day1 = 12 * 3600 * 1_000_000_000
    tracker.on_trade("binance", 5000.0, ts_recv_ns=t_day1)
    assert tracker.get_global_cvd() == 5000.0

    # Día 2: 00:05 UTC (86400 + 300) * 1e9 ns
    t_day2 = (86400 + 300) * 1_000_000_000
    tracker.on_trade("binance", 1000.0, ts_recv_ns=t_day2)

    # Debe haber reiniciado el acumulador y solo contar el nuevo trade de 1000.0
    assert tracker.get_global_cvd() == 1000.0


def test_cvd_delta_rolling_window() -> None:
    tracker = CVDTracker("ETH-USDT-PERP")
    t0 = 1_000_000_000_000

    # Trade 1 @ t0: +5000 USD
    tracker.on_trade("binance", 5000.0, ts_recv_ns=t0)
    # Trade 2 @ t0 + 100s: -2000 USD
    tracker.on_trade("binance", -2000.0, ts_recv_ns=t0 + 100_000_000_000)
    # Trade 3 @ t0 + 1000s: +4000 USD (fuera de la ventana de 900s respecto a t0)
    tracker.on_trade("binance", 4000.0, ts_recv_ns=t0 + 1000_000_000_000)

    # El CVD global total es 5000 - 2000 + 4000 = 7000 USD
    assert tracker.get_global_cvd() == 7000.0

    # La variación en los últimos 900s respecto a t0 + 1000s es respecto a la muestra de t0 + 100s (CVD = 3000)
    # delta = 7000 - 3000 = 4000 USD
    delta_15m = tracker.get_cvd_delta(900.0, t0 + 1000_000_000_000)
    assert delta_15m == 4000.0
