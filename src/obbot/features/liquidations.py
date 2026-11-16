"""Rastreador de intensidad y desbalance de liquidaciones forzadas."""

from collections import deque
from dataclasses import dataclass

from obbot.common.events import Side
from obbot.common.logging import get_logger

logger = get_logger("features.liquidations")


@dataclass(slots=True, frozen=True)
class LiquidationMetrics:
    """Métricas consolidadas de liquidaciones en la ventana rodante."""

    total_events: int
    buy_liq_count: int
    sell_liq_count: int
    buy_liq_notional_usd: float
    sell_liq_notional_usd: float
    net_liq_notional_usd: float  # buy_notional - sell_notional
    events_per_minute: float
    is_downsampled_proxy: bool = True  # Alerta de que Binance forceOrder está muestreado


class LiquidationTracker:
    """Rastreador rodante de intensidad de liquidaciones con gestión de muestreo."""

    def __init__(self, symbol: str, window_sec: float = 60.0) -> None:
        self.symbol = symbol
        self.window_ns = int(window_sec * 1_000_000_000)
        self.window_sec = window_sec
        # Búfer: (timestamp_ns, side, notional_usd)
        self._history: deque[tuple[int, Side, float]] = deque()

    def record_liquidation(self, ts_recv_ns: int, side: Side, notional_usd: float) -> None:
        """Registra un evento de liquidación forzada."""
        self._history.append((ts_recv_ns, side, notional_usd))

    def _evict_old(self, current_ts_ns: int) -> None:
        """Descarta eventos fuera de la ventana."""
        cutoff_ns = current_ts_ns - self.window_ns
        while self._history and self._history[0][0] < cutoff_ns:
            self._history.popleft()

    def get_metrics(self, current_ts_ns: int) -> LiquidationMetrics:
        """Calcula métricas agregadas de liquidación en la ventana actual."""
        self._evict_old(current_ts_ns)

        buy_count = 0
        sell_count = 0
        buy_notional = 0.0
        sell_notional = 0.0

        for _, side, notional in self._history:
            if side == Side.BUY:
                buy_count += 1
                buy_notional += notional
            else:
                sell_count += 1
                sell_notional += notional

        total_count = buy_count + sell_count
        rate_per_min = (total_count / self.window_sec) * 60.0 if self.window_sec > 0 else 0.0

        return LiquidationMetrics(
            total_events=total_count,
            buy_liq_count=buy_count,
            sell_liq_count=sell_count,
            buy_liq_notional_usd=buy_notional,
            sell_liq_notional_usd=sell_notional,
            net_liq_notional_usd=buy_notional - sell_notional,
            events_per_minute=rate_per_min,
            is_downsampled_proxy=True,
        )
