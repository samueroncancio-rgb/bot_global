"""Tests unitarios para el cálculo de pesos dinámicos y agregación del Delta Global."""

import pytest

from obbot.features.weights import DynamicWeightCalculator


def test_dynamic_weights_clamping_and_normalization() -> None:
    calc = DynamicWeightCalculator(
        symbol="BTC-USDT-PERP",
        clamp_min=0.05,
        clamp_max=0.60,
        ema_alpha=1.0,  # Sin inercia para testear directamente los targets
    )

    # Simular volumen masivo solo en Binance (99% de volumen)
    calc.record_trade("binance", 1_000_000, 990_000.0)
    calc.record_trade("okx", 1_000_000, 10_000.0)
    calc.record_trade("bybit", 1_000_000, 0.0)
    calc.record_trade("hyperliquid", 1_000_000, 0.0)

    weights = calc.update_weights(1_000_000)

    # 1. Comprobar que ningún peso es menor a clamp_min ni excede clamp_max
    assert all(w >= 0.04 for w in weights.values())
    assert weights["binance"] <= 0.65  # Tras normalizar la suma a 1.0

    # 2. La suma total de pesos debe ser exactamente 1.0
    assert sum(weights.values()) == pytest.approx(1.0, abs=1e-6)


def test_dynamic_weights_global_delta() -> None:
    calc = DynamicWeightCalculator(symbol="BTC-USDT-PERP", correlation_rho=0.85)

    # z-scores por exchange
    z_scores = {
        "binance": 2.0,
        "okx": 1.0,
        "hyperliquid": -1.0,
        "bybit": 0.5,
    }

    # 1. Con estandarización formal por covarianza (Solución A)
    global_delta, weights = calc.compute_global_delta(z_scores, current_ts_ns=1_000_000)

    raw_expected = sum(weights[ex] * z_scores[ex] for ex in z_scores)
    sum_w = sum(weights.values())
    sum_w_sq = sum(w * w for w in weights.values())
    var_comp = (1.0 - 0.85) * sum_w_sq + 0.85 * (sum_w**2)
    expected_std = raw_expected / (var_comp**0.5)

    assert global_delta == pytest.approx(expected_std, abs=1e-6)
    assert sum(weights.values()) == pytest.approx(1.0, abs=1e-6)

    # 2. Modo sin estandarización para cálculo directo
    raw_delta, _ = calc.compute_global_delta(z_scores, current_ts_ns=1_000_000, standardize=False)
    assert raw_delta == pytest.approx(raw_expected, abs=1e-6)
