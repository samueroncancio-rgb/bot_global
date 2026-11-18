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
