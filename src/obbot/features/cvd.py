"""Seguimiento de Delta de Volumen Acumulado (CVD) por exchange y global."""

from collections import deque

from obbot.common.logging import get_logger

logger = get_logger("features.cvd")


class CVDTracker:
    """
    Acumulador continuo de Delta de Volumen Acumulado (CVD) en USD.
    Implementa reinicio automático en el cambio de sesión a las 00:00:00 UTC (86,400s)
    y seguimiento temporal rodante para evaluar variaciones y pendientes dinámicas (15m).
    """

    def __init__(self, symbol: str) -> None:
        self.symbol = symbol
        self._cvd_by_exchange: dict[str, float] = {}
        self._global_cvd: float = 0.0
        self._last_day: int = -1
        # Búfer rodante de snapshots temporales (ts_ns, global_cvd)
        self._history: deque[tuple[int, float]] = deque(maxlen=3600)
        self._last_history_ts_ns: int = 0

    def on_trade(
        self,
        exchange: str,
        signed_notional_usd: float,
        ts_recv_ns: int | None = None,
    ) -> None:
        """
        Actualiza el CVD acumulado del exchange y el total global.
        Si se pasa ts_recv_ns y ocurre el paso a un nuevo día UTC (00:00:00), reinicia la sesión.
        """
        if ts_recv_ns is not None and ts_recv_ns > 0:
            current_day = int(ts_recv_ns // (86_400 * 1_000_000_000))
            if self._last_day != -1 and current_day > self._last_day:
                logger.info(
                    "cvd.session_reset: nuevo día UTC (%d -> %d). Reiniciando CVD de sesión a $0.00",
                    self._last_day,
                    current_day,
                )
                self.reset()
            self._last_day = current_day

        ex = exchange.lower()
        if ex not in self._cvd_by_exchange:
            self._cvd_by_exchange[ex] = 0.0

        self._cvd_by_exchange[ex] += signed_notional_usd
        self._global_cvd += signed_notional_usd

        # Registrar muestra temporal al menos cada 500ms
        if ts_recv_ns is not None and ts_recv_ns > 0:
            if (ts_recv_ns - self._last_history_ts_ns) >= 500_000_000:
                self._history.append((ts_recv_ns, self._global_cvd))
                self._last_history_ts_ns = ts_recv_ns

    def get_cvd(self, exchange: str) -> float:
        """Retorna el CVD acumulado de un exchange individual."""
        return self._cvd_by_exchange.get(exchange.lower(), 0.0)

    def get_global_cvd(self) -> float:
        """Retorna la suma global de CVD de todos los exchanges conectados."""
        return self._global_cvd

    def get_cvd_delta(self, window_sec: float, current_ts_ns: int) -> float:
        """
        Retorna la variación del CVD global en la ventana especificada:
        Delta_CVD = CVD_actual - CVD(t - window_sec).
        Permite evaluar la pendiente / tendencia reciente sin distorsión
        del valor acumulado absoluto de la sesión.
        """
        if not self._history:
            return self._global_cvd
        cutoff_ns = current_ts_ns - int(window_sec * 1_000_000_000)
        baseline_cvd = self._history[0][1]
        for ts, val in self._history:
            if ts <= cutoff_ns:
                baseline_cvd = val
            else:
                break
        return self._global_cvd - baseline_cvd

    def get_all(self) -> dict[str, float]:
        """Retorna desglose completo de CVD por exchange y global."""
        result = dict(self._cvd_by_exchange)
        result["global"] = self._global_cvd
        return result

    def reset(self) -> None:
        """Reinicia los acumuladores (e.g. al inicio de una nueva sesión)."""
        self._cvd_by_exchange.clear()
        self._global_cvd = 0.0
        self._history.clear()
        self._last_history_ts_ns = 0
