"""Seguimiento de Delta de Volumen Acumulado (CVD) por exchange y global."""

from obbot.common.logging import get_logger

logger = get_logger("features.cvd")


class CVDTracker:
    """Acumulador continuo de Delta de Volumen Acumulado (CVD) en USD."""

    def __init__(self, symbol: str) -> None:
        self.symbol = symbol
        self._cvd_by_exchange: dict[str, float] = {}
        self._global_cvd: float = 0.0

    def on_trade(self, exchange: str, signed_notional_usd: float) -> None:
        """Actualiza el CVD acumulado del exchange y el total global."""
        ex = exchange.lower()
        if ex not in self._cvd_by_exchange:
            self._cvd_by_exchange[ex] = 0.0

        self._cvd_by_exchange[ex] += signed_notional_usd
        self._global_cvd += signed_notional_usd

    def get_cvd(self, exchange: str) -> float:
        """Retorna el CVD acumulado de un exchange individual."""
        return self._cvd_by_exchange.get(exchange.lower(), 0.0)

    def get_global_cvd(self) -> float:
        """Retorna la suma global de CVD de todos los exchanges conectados."""
        return self._global_cvd

    def get_all(self) -> dict[str, float]:
        """Retorna desglose completo de CVD por exchange y global."""
        result = dict(self._cvd_by_exchange)
        result["global"] = self._global_cvd
        return result

    def reset(self) -> None:
        """Reinicia los acumuladores (e.g. al inicio de una nueva sesión)."""
        self._cvd_by_exchange.clear()
        self._global_cvd = 0.0

