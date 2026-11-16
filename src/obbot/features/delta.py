"""Cálculo de Delta local por exchange y estandarización con z-score rodante."""

import math
from collections import deque

from obbot.common.logging import get_logger

logger = get_logger("features.delta")


class RollingWindowDelta:
    """Calcula el delta de volumen nocional (comprador - vendedor) en ventanas rodantes."""

    def __init__(self, window_sec: float = 60.0) -> None:
        self.window_ns = int(window_sec * 1_000_000_000)
        # Búfer de tuplas: (timestamp_ns, signed_notional_usd)
        self._trades: deque[tuple[int, float]] = deque()
        self._current_delta_usd: float = 0.0

    def add_trade(self, ts_recv_ns: int, signed_notional_usd: float) -> None:
        """Añade una operación y actualiza la ventana temporal."""
        self._trades.append((ts_recv_ns, signed_notional_usd))
        self._current_delta_usd += signed_notional_usd
        self._evict_old(ts_recv_ns)

    def _evict_old(self, current_ts_ns: int) -> None:
        """Descarta operaciones fuera de la ventana rodante."""
        cutoff_ns = current_ts_ns - self.window_ns
        while self._trades and self._trades[0][0] < cutoff_ns:
            _, old_signed_notional = self._trades.popleft()
            self._current_delta_usd -= old_signed_notional

    def get_delta(self, current_ts_ns: int) -> float:
        """Retorna el delta acumulado dentro de la ventana."""
        self._evict_old(current_ts_ns)
        return self._current_delta_usd


class RollingZScore:
    """Calcula la media y desviación estándar de forma causal para estandarización z-score."""

    def __init__(self, max_samples: int = 300, min_samples: int = 15) -> None:
        self.max_samples = max_samples
        self.min_samples = min_samples
        self._samples: deque[float] = deque(maxlen=max_samples)

    def add_sample(self, value: float) -> None:
        """Registra una muestra de delta en el búfer circular."""
        self._samples.append(value)

    def compute_z_score(self, current_value: float) -> float:
        """
        Calcula el z-score causal estandarizado: (x - mean) / std.
        Si la varianza es nula o hay muestras insuficientes, retorna 0.0.
        """
        if len(self._samples) < self.min_samples:
            return 0.0

        n = len(self._samples)
        mean = sum(self._samples) / n
        variance = sum((x - mean) ** 2 for x in self._samples) / (n - 1)

        if variance <= 1e-9:
            return 0.0

        std = math.sqrt(variance)
        return (current_value - mean) / std


class ExchangeDeltaTracker:
    """Gestor integral del delta y z-score por exchange."""

    def __init__(
        self,
        exchange: str,
        symbol: str,
        windows_sec: list[float] | None = None,
        min_samples: int = 15,
    ) -> None:
        self.exchange = exchange
        self.symbol = symbol
        windows = windows_sec or [60.0, 300.0, 900.0]
        self._trackers: dict[float, RollingWindowDelta] = {
            w: RollingWindowDelta(w) for w in windows
        }
        self._z_scorers: dict[float, RollingZScore] = {
            w: RollingZScore(max_samples=300, min_samples=min_samples) for w in windows
        }

    def on_trade(self, ts_recv_ns: int, signed_notional_usd: float) -> None:
        """Procesa una operación en todas las ventanas temporales activas."""
        for window, tracker in self._trackers.items():
            tracker.add_trade(ts_recv_ns, signed_notional_usd)
            delta = tracker.get_delta(ts_recv_ns)
            self._z_scorers[window].add_sample(delta)

    def get_delta(self, window_sec: float, current_ts_ns: int) -> float:
        """Obtiene el delta en USD para una ventana específica."""
        tracker = self._trackers.get(window_sec)
        if not tracker:
            return 0.0
        return tracker.get_delta(current_ts_ns)

    def get_z_score(self, window_sec: float, current_ts_ns: int) -> float:
        """Obtiene el z-score de delta para una ventana específica."""
        tracker = self._trackers.get(window_sec)
        scorer = self._z_scorers.get(window_sec)
        if not tracker or not scorer:
            return 0.0
        current_delta = tracker.get_delta(current_ts_ns)
        return scorer.compute_z_score(current_delta)
