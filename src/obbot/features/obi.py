"""Cálculo de Desbalance de Libro (OBI) multiprofundidad y persistencia de liquidez."""

import math
from collections import deque
from collections.abc import Sequence

from obbot.common.events import OrderBookLevel
from obbot.common.logging import get_logger

logger = get_logger("features.obi")


class OBICalculator:
    """Calcula OBI multiprofundidad ponderado por distancia exponencial y persistencia."""

    def __init__(
        self,
        levels: list[int] | None = None,
        decay_lambda: float = 0.10,
        persistence_window: int = 30,
    ) -> None:
        self.levels = levels or [5, 10, 20]
        self.decay_lambda = decay_lambda
        # Búfer circular para cálculo de autocorrelación (persistencia)
        self._obi_history: deque[float] = deque(maxlen=persistence_window)

    def compute_weighted_obi(
        self,
        bids: Sequence[OrderBookLevel],
        asks: Sequence[OrderBookLevel],
        k_levels: int,
    ) -> float:
        """
        Calcula el desbalance ponderado para K niveles:
        OBI_k = (W_bids - W_asks) / (W_bids + W_asks)
        con pesos W_i = v_i * exp(-lambda * distance_to_mid / mid).
        """
        if not bids or not asks or k_levels <= 0:
            return 0.0

        best_bid = bids[0].price
        best_ask = asks[0].price
        mid = (best_bid + best_ask) / 2.0
        if mid <= 0.0:
            return 0.0

        w_bids = 0.0
        for i, level in enumerate(bids[:k_levels]):
            dist_rel = max(0.0, (mid - level.price) / mid)
            weight = math.exp(-self.decay_lambda * (i + dist_rel * 10.0))
            w_bids += level.size * weight

        w_asks = 0.0
        for i, level in enumerate(asks[:k_levels]):
            dist_rel = max(0.0, (level.price - mid) / mid)
            weight = math.exp(-self.decay_lambda * (i + dist_rel * 10.0))
            w_asks += level.size * weight

        total_weight = w_bids + w_asks
        if total_weight <= 1e-9:
            return 0.0

        obi = (w_bids - w_asks) / total_weight
        return max(-1.0, min(1.0, obi))

    def update_snapshot(
        self,
        bids: Sequence[OrderBookLevel],
        asks: Sequence[OrderBookLevel],
    ) -> dict[int, float]:
        """Calcula OBI para todos los niveles configurados y registra en el historial."""
        results: dict[int, float] = {}
        for k in self.levels:
            results[k] = self.compute_weighted_obi(bids, asks, k)

        # Usar el OBI de nivel primario (e.g. 10 niveles o el primero disponible) para persistencia
        primary_k = self.levels[1] if len(self.levels) > 1 else self.levels[0]
        self._obi_history.append(results.get(primary_k, 0.0))

        return results

    def compute_persistence(self) -> float:
        """
        Calcula la persistencia como autocorrelación lag-1 del desbalance del libro.
        Retorna [-1.0, 1.0]. Valores cercanos a 1.0 indican soporte/resistencia persistente.
        """
        if len(self._obi_history) < 10:
            return 0.0

        n = len(self._obi_history)
        vals = list(self._obi_history)
        mean = sum(vals) / n

        denom = sum((x - mean) ** 2 for x in vals)
        if denom <= 1e-9:
            return 1.0 if abs(mean) > 1e-6 else 0.0

        num = sum((vals[t] - mean) * (vals[t - 1] - mean) for t in range(1, n))
        autocorr = num / denom
        return max(-1.0, min(1.0, autocorr))
