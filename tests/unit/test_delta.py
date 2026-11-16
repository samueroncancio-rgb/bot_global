"""Tests unitarios para el cálculo de Delta y estandarización z-score."""

import pytest

from obbot.features.delta import ExchangeDeltaTracker, RollingWindowDelta, RollingZScore


def test_rolling_window_delta_eviction() -> None:
    # Ventana de 1 segundo (1_000_000_000 ns)
    tracker = RollingWindowDelta(window_sec=1.0)

    # Trade 1 @ t = 100ms (+1000 USD)
    tracker.add_trade(100_000_000, 1000.0)
    # Trade 2 @ t = 500ms (-400 USD)
    tracker.add_trade(500_000_000, -400.0)

    assert tracker.get_delta(600_000_000) == 600.0

    # Trade 3 @ t = 1200ms (+500 USD) -> Desaloja Trade 1 (1200ms - 1000ms = 200ms cutoff)
    tracker.add_trade(1_200_000_000, 500.0)

    # Quedan Trade 2 (-400) y Trade 3 (+500) = 100.0
    assert tracker.get_delta(1_200_000_000) == 100.0


def test_rolling_z_score_calculation() -> None:
    scorer = RollingZScore(max_samples=50, min_samples=5)

    # Muestras insuficientes -> retorna 0.0
    for v in [10.0, 10.0, 10.0]:
        scorer.add_sample(v)
    assert scorer.compute_z_score(10.0) == 0.0

    # Añadir valores sintéticos con media = 0, varianza conocida
    # [-2, -1, 0, 1, 2] -> media = 0.0, var = 2.5, std = sqrt(2.5) ~ 1.581
    scorer = RollingZScore(max_samples=50, min_samples=5)
    for x in [-2.0, -1.0, 0.0, 1.0, 2.0]:
        scorer.add_sample(x)

    z = scorer.compute_z_score(1.58113883)
    assert z == pytest.approx(1.0, rel=1e-3)


def test_exchange_delta_tracker_multi_window() -> None:
    tracker = ExchangeDeltaTracker(
        "binance", "BTC-USDT-PERP", windows_sec=[1.0, 5.0], min_samples=3
    )

    # Añadir trades
    tracker.on_trade(100_000_000, 5000.0)
    tracker.on_trade(500_000_000, -2000.0)

    assert tracker.get_delta(1.0, 600_000_000) == 3000.0
    assert tracker.get_delta(5.0, 600_000_000) == 3000.0

    # Avanzar a t = 2000ms
    tracker.on_trade(2_000_000_000, 1000.0)

    # Ventana 1.0s solo contiene el trade de 2000ms
    assert tracker.get_delta(1.0, 2_000_000_000) == 1000.0
    # Ventana 5.0s contiene los 3 trades (5000 - 2000 + 1000 = 4000)
    assert tracker.get_delta(5.0, 2_000_000_000) == 4000.0
