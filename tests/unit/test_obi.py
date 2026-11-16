"""Tests unitarios para el cálculo de OBI multiprofundidad y persistencia."""

import pytest

from obbot.common.events import OrderBookLevel
from obbot.features.obi import OBICalculator


def test_obi_balanced_and_extreme_scenarios() -> None:
    calc = OBICalculator(levels=[5, 10, 20], decay_lambda=0.1)

    # 1. Libro perfectamente balanceado
    bids_bal = [OrderBookLevel(50000.0 - i * 10, 1.0) for i in range(10)]
    asks_bal = [OrderBookLevel(50010.0 + i * 10, 1.0) for i in range(10)]

    obi_bal = calc.compute_weighted_obi(bids_bal, asks_bal, k_levels=5)
    assert obi_bal == pytest.approx(0.0, abs=1e-3)

    # 2. Libro dominado por compradores
    bids_heavy = [OrderBookLevel(50000.0 - i * 10, 100.0) for i in range(10)]
    asks_light = [OrderBookLevel(50010.0 + i * 10, 1.0) for i in range(10)]

    obi_buy = calc.compute_weighted_obi(bids_heavy, asks_light, k_levels=5)
    assert obi_buy > 0.90

    # 3. Libro dominado por vendedores
    obi_sell = calc.compute_weighted_obi(asks_light, bids_heavy, k_levels=5)
    assert obi_sell < -0.90


def test_obi_distance_weighting() -> None:
    calc = OBICalculator(decay_lambda=1.0)  # Fuerte decaimiento por distancia

    # Nivel 1 idéntico en ambos lados
    # Nivel 2 en el bid está muy lejos (50000 vs 40000)
    # Nivel 2 en el ask está muy cerca (50010 vs 50020)
    bids = [OrderBookLevel(50000.0, 1.0), OrderBookLevel(40000.0, 10.0)]
    asks = [OrderBookLevel(50010.0, 1.0), OrderBookLevel(50020.0, 10.0)]

    obi = calc.compute_weighted_obi(bids, asks, k_levels=2)
    # El volumen del ask tiene más peso por estar más cerca al mid
    assert obi < 0.0


def test_obi_persistence_autocorrelation() -> None:
    calc = OBICalculator(persistence_window=20)

    # Secuencia altamente persistente (tendencia positiva suave)
    for i in range(20):
        # Inyectar OBI constante o suavemente creciente
        bids = [OrderBookLevel(50000.0, 10.0 + i * 0.1)]
        asks = [OrderBookLevel(50010.0, 1.0)]
        calc.update_snapshot(bids, asks)

    rho = calc.compute_persistence()
    assert rho > 0.50  # Alta persistencia positiva
