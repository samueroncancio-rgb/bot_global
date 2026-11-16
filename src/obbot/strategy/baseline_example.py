"""Estrategia baseline de ejemplo basada en confluencia de Delta Global y OBI."""

from obbot.common.logging import get_logger
from obbot.execution.order_manager import ManagedOrder
from obbot.features.engine import FeatureVector
from obbot.risk.limits import OrderRequest, PortfolioState
from obbot.strategy.base import Strategy

logger = get_logger("strategy.baseline")


class BaselineOrderFlowStrategy(Strategy):
    """
    Estrategia de ejemplo basada en la hipótesis H1 y H2:
    - Señal LONG: Delta Global z-score > z_threshold (presión compradora multi-exchange)
      y desequilibrio del libro OBI (10 niveles) > obi_threshold (liquidez favorable).
    - Señal SHORT: Delta Global z-score < -z_threshold y OBI < -obi_threshold.
    - Adjunto estricto de Stop-Loss y Take-Profit.
    """

    def __init__(
        self,
        symbol: str = "BTCUSDT",
        z_threshold: float = 1.8,
        obi_threshold: float = 0.20,
        trade_size: float = 0.01,
        stop_loss_pct: float = 0.008,  # 0.8%
        take_profit_pct: float = 0.016,  # 1.6% (ratio riesgo/beneficio 1:2)
    ) -> None:
        super().__init__(name="BaselineOrderFlow", symbol=symbol)
        self.z_threshold = z_threshold
        self.obi_threshold = obi_threshold
        self.trade_size = trade_size
        self.stop_loss_pct = stop_loss_pct
        self.take_profit_pct = take_profit_pct
        self._active_orders: dict[str, ManagedOrder] = {}

    def on_feature_vector(
        self,
        features: FeatureVector,
        portfolio: PortfolioState,
    ) -> list[OrderRequest]:
        """Evalúa las condiciones de entrada y emite órdenes protegidas."""
        if features.symbol != self.symbol or features.mid_price <= 0.0:
            return []

        # Solo buscar nuevas entradas si no tenemos posición abierta en este símbolo
        current_pos = portfolio.positions.get(self.symbol) if portfolio.positions else None
        current_size = current_pos.size if current_pos else 0.0
        if abs(current_size) > 1e-6:
            return []

        mid_px = features.mid_price
        z = features.global_delta_z
        obi = features.obi_10_levels

        # Señal LONG
        if z >= self.z_threshold and obi >= self.obi_threshold:
            stop_px = round(mid_px * (1.0 - self.stop_loss_pct), 2)
            tp_px = round(mid_px * (1.0 + self.take_profit_pct), 2)
            logger.info(
                "strategy.signal_long: symbol=%s z=%.2f obi=%.2f px=%.2f sl=%.2f tp=%.2f",
                self.symbol,
                z,
                obi,
                mid_px,
                stop_px,
                tp_px,
            )
            return [
                OrderRequest(
                    symbol=self.symbol,
                    side="BUY",
                    order_type="LIMIT",
                    quantity=self.trade_size,
                    price=mid_px,
                    time_in_force="GTX",  # Maker post-only
                    stop_loss_price=stop_px,
                    take_profit_price=tp_px,
                )
            ]

        # Señal SHORT
        if z <= -self.z_threshold and obi <= -self.obi_threshold:
            stop_px = round(mid_px * (1.0 + self.stop_loss_pct), 2)
            tp_px = round(mid_px * (1.0 - self.take_profit_pct), 2)
            logger.info(
                "strategy.signal_short: symbol=%s z=%.2f obi=%.2f px=%.2f sl=%.2f tp=%.2f",
                self.symbol,
                z,
                obi,
                mid_px,
                stop_px,
                tp_px,
            )
            return [
                OrderRequest(
                    symbol=self.symbol,
                    side="SELL",
                    order_type="LIMIT",
                    quantity=self.trade_size,
                    price=mid_px,
                    time_in_force="GTX",
                    stop_loss_price=stop_px,
                    take_profit_price=tp_px,
                )
            ]

        return []

    def on_order_update(self, order: ManagedOrder) -> None:
        """Actualiza el seguimiento de órdenes abiertas."""
        self._active_orders[order.client_order_id] = order

