"""Calculador de pesos dinámicos adaptativos y agregación de Delta Global."""

from collections import deque

from obbot.common.logging import get_logger

logger = get_logger("features.weights")


class DynamicWeightCalculator:
    """Calcula pesos dinámicos w_i(t) por cuota rodante de volumen y Open Interest."""

    def __init__(
        self,
        symbol: str,
        priors: dict[str, float] | None = None,
        volume_share_alpha: float = 0.70,
        oi_share_beta: float = 0.30,
        ema_alpha: float = 0.05,
        clamp_min: float = 0.05,
        clamp_max: float = 0.60,
        window_sec: float = 300.0,
        correlation_rho: float = 0.85,
    ) -> None:
        self.symbol = symbol
        self.priors: dict[str, float] = priors or {
            "binance": 0.48,
            "okx": 0.20,
            "hyperliquid": 0.18,
            "bybit": 0.14,
        }
        self.volume_share_alpha = volume_share_alpha
        self.oi_share_beta = oi_share_beta
        self.ema_alpha = ema_alpha
        self.clamp_min = clamp_min
        self.clamp_max = clamp_max
        self.window_ns = int(window_sec * 1_000_000_000)
        self.correlation_rho = correlation_rho

        # Volúmenes rodantes: exchange -> deque de (timestamp_ns, notional_usd)
        self._vol_history: dict[str, deque[tuple[int, float]]] = {ex: deque() for ex in self.priors}
        self._latest_oi_usd: dict[str, float] = dict.fromkeys(self.priors, 0.0)
        self._current_weights: dict[str, float] = dict(self.priors)

    def record_trade(self, exchange: str, ts_recv_ns: int, notional_usd: float) -> None:
        """Registra el volumen nocional de una operación para el cálculo de cuota."""
        ex = exchange.lower()
        if ex not in self._vol_history:
            self._vol_history[ex] = deque()
            self._latest_oi_usd[ex] = 0.0
            self._current_weights[ex] = 0.0
        self._vol_history[ex].append((ts_recv_ns, notional_usd))

    def record_oi(self, exchange: str, oi_usd: float) -> None:
        """Actualiza el último valor de Interés Abierto en USD."""
        ex = exchange.lower()
        if ex in self._latest_oi_usd:
            self._latest_oi_usd[ex] = oi_usd

    def _get_rolling_volumes(self, current_ts_ns: int) -> dict[str, float]:
        """Calcula el volumen rodante total de cada exchange en la ventana temporal."""
        cutoff_ns = current_ts_ns - self.window_ns
        vols: dict[str, float] = {}

        for ex, queue in self._vol_history.items():
            while queue and queue[0][0] < cutoff_ns:
                queue.popleft()
            vols[ex] = sum(q[1] for q in queue)

        return vols

    def _project_bounded_simplex(self, raw_weights: dict[str, float]) -> dict[str, float]:
        """Proyecta pesos al símplex unitario respetando [clamp_min, clamp_max]."""
        keys = list(raw_weights.keys())
        n = len(keys)
        if n == 0:
            return {}
        if self.clamp_min * n > 1.0 or self.clamp_max * n < 1.0:
            return dict.fromkeys(keys, 1.0 / n)

        low = min(raw_weights.values()) - self.clamp_max
        high = max(raw_weights.values()) - self.clamp_min

        for _ in range(40):
            mid = (low + high) / 2.0
            val = sum(min(max(raw_weights[k] - mid, self.clamp_min), self.clamp_max) for k in keys)
            if val > 1.0:
                low = mid
            else:
                high = mid

        res = {k: min(max(raw_weights[k] - low, self.clamp_min), self.clamp_max) for k in keys}
        s = sum(res.values())
        if s > 0.0:
            return {k: v / s for k, v in res.items()}
        return dict.fromkeys(keys, 1.0 / n)

    def update_weights(self, current_ts_ns: int) -> dict[str, float]:
        """
        Calcula los pesos w_i(t) aplicando:
        1. Cuota combinada: alpha * vol_share + (1 - alpha) * oi_share
        2. Proyección acotada al símplex con clamping [clamp_min, clamp_max]
        3. Suavizado exponencial EMA
        4. Proyección final al símplex unitario
        """
        vols = self._get_rolling_volumes(current_ts_ns)
        total_vol = sum(vols.values())
        total_oi = sum(self._latest_oi_usd.values())

        if total_vol <= 0.0 and total_oi <= 0.0:
            return dict(self._current_weights)

        raw_shares: dict[str, float] = {}
        for ex in self.priors:
            vol_share = (
                (vols.get(ex, 0.0) / total_vol) if total_vol > 0.0 else self.priors.get(ex, 0.25)
            )
            oi_val = self._latest_oi_usd.get(ex, 0.0)
            oi_share = (oi_val / total_oi) if total_oi > 0.0 else vol_share

            # Combinación convexa de volumen y Open Interest
            raw_shares[ex] = self.volume_share_alpha * vol_share + self.oi_share_beta * oi_share

        # Proyección acotada al símplex
        norm_shares = self._project_bounded_simplex(raw_shares)

        # Suavizado EMA
        smoothed: dict[str, float] = {}
        for ex, target in norm_shares.items():
            prev = self._current_weights.get(ex, target)
            smoothed[ex] = self.ema_alpha * target + (1.0 - self.ema_alpha) * prev

        # Proyección final para garantizar sum=1.0 y cotas estrictas
        self._current_weights = self._project_bounded_simplex(smoothed)

        return dict(self._current_weights)

    def compute_global_delta(
        self,
        z_scores: dict[str, float],
        current_ts_ns: int,
        standardize: bool = True,
    ) -> tuple[float, dict[str, float]]:
        """
        Calcula el Delta Global consolidado con estandarización formal por covarianza.

        Bajo la Solución A (Estandarización por Covarianza):
        S = sum(w_i * z_i)
        Var(S) = (1 - rho) * sum(w_i^2) + rho * (sum(w_i))^2
        global_delta = S / sqrt(Var(S))

        Garantiza que la señal consolidada conserve una varianza unitaria (Z-Score real).
        """
        weights = self.update_weights(current_ts_ns)
        raw_delta = 0.0
        active_weights: list[float] = []

        for ex, z in z_scores.items():
            w = weights.get(ex.lower(), 0.0)
            if w > 0.0:
                raw_delta += w * z
                active_weights.append(w)

        if not standardize or not active_weights:
            return raw_delta, weights

        sum_w = sum(active_weights)
        sum_w_sq = sum(w * w for w in active_weights)
        rho = self.correlation_rho
        variance_composite = (1.0 - rho) * sum_w_sq + rho * (sum_w**2)
        std_composite = variance_composite**0.5

        standardized_delta = raw_delta / std_composite if std_composite > 1e-9 else raw_delta

        return standardized_delta, weights
