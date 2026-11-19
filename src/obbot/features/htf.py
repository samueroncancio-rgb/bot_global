"""Seguimiento de tendencia en marco temporal superior (HTF) en streaming continuo."""

import math
from typing import Any

from obbot.common.logging import get_logger

logger = get_logger("features.htf")


class HTFTrendTracker:
    """
    Seguimiento de tendencia en marco temporal superior (HTF) en streaming continuo.
    Calcula una EMA continua de 15 minutos (tau = 900s) y evalúa el régimen estructural
    cruzando la posición del precio respecto a la EMA y el CVD global de sesión.
    """

    def __init__(self, tau_sec: float = 900.0) -> None:
        self.tau_sec = tau_sec
        self._ema: float | None = None
        self._last_ts_ns: int = 0
        self._initialized = False

    def seed_from_klines(self, klines: list[list[Any]]) -> None:
        """Inicializa la EMA con velas históricas cerradas (e.g. 50 velas de 15m)."""
        if not klines:
            return
        close_prices = [float(k[4]) for k in klines if len(k) > 4]
        if not close_prices:
            return
        # Inicializar EMA sobre la secuencia de precios de cierre
        alpha = 2.0 / (min(len(close_prices), 20) + 1.0)
        ema = close_prices[0]
        for p in close_prices[1:]:
            ema = alpha * p + (1.0 - alpha) * ema
        self._ema = ema
        self._initialized = True
        logger.info("htf.seeded_from_klines: candles=%d initial_ema=%.2f", len(close_prices), ema)

    def on_price_update(self, price: float, ts_ns: int) -> float:
        """Actualiza la EMA continua ante cada nuevo precio de mercado."""
        if price <= 0.0:
            return self._ema or 0.0

        if self._ema is None:
            self._ema = price
            self._last_ts_ns = ts_ns
            self._initialized = True
            return self._ema

        if self._last_ts_ns > 0 and ts_ns > self._last_ts_ns:
            dt_sec = (ts_ns - self._last_ts_ns) / 1_000_000_000.0
            # Decaimiento continuo: alpha = 1 - exp(-dt / tau)
            alpha = 1.0 - math.exp(-min(dt_sec, self.tau_sec) / self.tau_sec)
            self._ema = alpha * price + (1.0 - alpha) * self._ema

        self._last_ts_ns = ts_ns
        return self._ema

    def get_ema(self) -> float:
        return self._ema or 0.0

    def compute_regime(self, mid_price: float, global_cvd: float) -> str:
        """
        Determina el régimen estructural:
        - BULLISH: mid_price >= ema_15m Y global_cvd >= 0
        - BEARISH: mid_price < ema_15m Y global_cvd <= 0
        - NEUTRAL: divergencia o consolidación
        """
        if not self._initialized or self._ema is None or self._ema <= 0.0 or mid_price <= 0.0:
            # Si aún no ha calibrado, permitir operar neutral
            return "NEUTRAL"

        is_above_ema = mid_price >= self._ema
        is_cvd_positive = global_cvd >= 0.0

        if is_above_ema and is_cvd_positive:
            return "BULLISH"
        elif not is_above_ema and not is_cvd_positive:
            return "BEARISH"
        else:
            return "NEUTRAL"
