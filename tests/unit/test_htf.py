"""Tests unitarios para HTFTrendTracker y régimen de tendencia superior."""

import pytest
from obbot.features.htf import HTFTrendTracker


def test_htf_tracker_initialization() -> None:
    tracker = HTFTrendTracker(tau_sec=900.0)
    assert tracker.get_ema() == 0.0
    assert tracker.compute_regime(mid_price=1800.0, global_cvd=100.0) == "NEUTRAL"


def test_htf_tracker_seed_from_klines() -> None:
    tracker = HTFTrendTracker(tau_sec=900.0)
    # Formato de klines de Binance: [time, open, high, low, close, volume, ...]
    klines = [
        [1000, "1700.0", "1710.0", "1695.0", "1705.0", "100.0"],
        [2000, "1705.0", "1720.0", "1700.0", "1715.0", "150.0"],
        [3000, "1715.0", "1730.0", "1710.0", "1725.0", "200.0"],
    ]
    tracker.seed_from_klines(klines)
    ema = tracker.get_ema()
    assert 1700.0 < ema < 1730.0

    # Test regime evaluation
    assert tracker.compute_regime(mid_price=1750.0, global_cvd=500.0) == "BULLISH"
    assert tracker.compute_regime(mid_price=1680.0, global_cvd=-500.0) == "BEARISH"
    # Divergence: price > EMA but CVD < 0
    assert tracker.compute_regime(mid_price=1750.0, global_cvd=-500.0) == "NEUTRAL"
    # Divergence: price < EMA but CVD > 0
    assert tracker.compute_regime(mid_price=1680.0, global_cvd=500.0) == "NEUTRAL"


def test_htf_tracker_continuous_decay() -> None:
    tracker = HTFTrendTracker(tau_sec=900.0)
    t0 = 1_000_000_000_000_000_000  # 1s en ns
    # Primera actualización
    ema1 = tracker.on_price_update(1800.0, t0)
    assert ema1 == 1800.0

    # Avance de tiempo 900s con nuevo precio 2000.0
    t1 = t0 + 900_000_000_000
    ema2 = tracker.on_price_update(2000.0, t1)
    # alpha = 1 - exp(-1) ~= 0.6321
    # expected = 0.6321 * 2000 + 0.3679 * 1800 ~= 1926.4
    assert 1920.0 < ema2 < 1930.0
