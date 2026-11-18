"""Estrategia baseline de ejemplo basada en confluencia de Delta Global y OBI."""

from obbot.common.logging import get_logger
from obbot.execution.order_manager import ManagedOrder
from obbot.features.engine import FeatureVector
from obbot.risk.limits import OrderRequest, PortfolioState
from obbot.strategy.base import Strategy

logger = get_logger("strategy.baseline")


def quantize(val: float, step: float) -> float:
    """Cuantiza un valor al múltiplo más cercano del tick/step size para cumplir filtros de exchange."""
    if step <= 0:
        return val
    step_str = f"{step:.8f}".rstrip("0")
    decimals = len(step_str.split(".")[1]) if "." in step_str else 0
    return round(round(val / step) * step, decimals)


class BaselineOrderFlowStrategy(Strategy):
    """
    Estrategia cuantitativa de microestructura basada en la confluencia H1 y H2:
    - Señal LONG: Delta Global z-score > z_threshold (presión compradora agresora multi-exchange)
      y desequilibrio del libro OBI (10 niveles) > obi_threshold (liquidez favorable).
    - Señal SHORT: Delta Global z-score < -z_threshold y OBI < -obi_threshold.
    - Adjunto estricto de Stop-Loss y Take-Profit atómico.
    - Soporte para volatilidad dinámica adaptativa y cuantización por tick_size.
    - Triple cinturón de defensa anti-spoofing y filtrado de liquidez fantasma HFT.
    """

    def __init__(
        self,
        symbol: str = "BTCUSDT",
        z_threshold: float = 1.8,
        obi_threshold: float = 0.20,
        trade_size: float = 0.01,
        stop_loss_pct: float = 0.008,  # 0.8%
        take_profit_pct: float = 0.016,  # 1.6% (ratio riesgo/beneficio 1:2)
        tick_size: float = 0.1,
        entry_order_type: str = "LIMIT",
        time_in_force: str = "GTX",
        use_dynamic_volatility: bool = False,
        vol_multiplier: float = 1.5,
        min_sl_pct: float = 0.004,
        max_sl_pct: float = 0.025,
        enable_anti_spoofing: bool = True,
        min_persistence: float = 0.50,
        min_confirming_venues: int = 2,
    ) -> None:
        super().__init__(name="BaselineOrderFlow", symbol=symbol)
        self.z_threshold = z_threshold
        self.obi_threshold = obi_threshold
        self.trade_size = trade_size
        self.stop_loss_pct = stop_loss_pct
        self.take_profit_pct = take_profit_pct
        self.tick_size = tick_size
        self.entry_order_type = entry_order_type.upper()
        self.time_in_force = time_in_force.upper()
        self.use_dynamic_volatility = use_dynamic_volatility
        self.vol_multiplier = vol_multiplier
        self.min_sl_pct = min_sl_pct
        self.max_sl_pct = max_sl_pct
        self.enable_anti_spoofing = enable_anti_spoofing
        self.min_persistence = min_persistence
        self.min_confirming_venues = min_confirming_venues
        self._active_orders: dict[str, ManagedOrder] = {}

    def _validate_anti_spoofing(self, side: str, features: FeatureVector) -> bool:
        """
        Valida que la liquidez del libro sea genuina y persistente frente a spoofing HFT.

        Aplica 3 barreras de filtrado matemático:
        1. Persistencia temporal (rho_1): Si hay suficientes muestras (rho != 0.0),
           exige que la autocorrelación lag-1 sea >= min_persistence (descarta órdenes relámpago).
        2. Consenso multi-exchange: Si múltiples exchanges reportan libros L2, exige confirmación
           cruzada en al menos min_confirming_venues para evitar manipulación monoplataforma.
        3. Detección de muros fantasma (Phantom Wall): Descarta la señal si el libro profundo
           (20 niveles) está muy cargado pero los 5 niveles inmediatos no tienen soporte/resistencia.
        """
        if not self.enable_anti_spoofing:
            return True

        # 1. Filtro de Persistencia Temporal (Autocorrelación lag-1)
        if (
            features.liquidity_persistence != 0.0
            and features.liquidity_persistence < self.min_persistence
        ):
            logger.info(
                "strategy.anti_spoofing_reject_persistence: rho=%.2f < min=%.2f (posible quote flickering)",
                features.liquidity_persistence,
                self.min_persistence,
            )
            return False

        # 2. Filtro de Consenso Multi-Exchange
        active_venues = [
            ex for ex, obis in features.obi_by_exchange.items() if 10 in obis
        ]
        if len(active_venues) > 1:
            req_confirming = min(self.min_confirming_venues, len(active_venues))
            if side == "BUY":
                confirming = sum(
                    1
                    for ex in active_venues
                    if features.obi_by_exchange[ex].get(10, 0.0) >= self.obi_threshold * 0.4
                )
            else:
                confirming = sum(
                    1
                    for ex in active_venues
                    if features.obi_by_exchange[ex].get(10, 0.0) <= -self.obi_threshold * 0.4
                )
            if confirming < req_confirming:
                logger.info(
                    "strategy.anti_spoofing_reject_isolated: solo %d de %d venues confirman OBI para %s",
                    confirming,
                    req_confirming,
                    side,
                )
                return False

        # 3. Detección de Muros Fantasma (Phantom Wall)
        primary_obis = features.obi_by_exchange.get("binance") or (
            next(iter(features.obi_by_exchange.values())) if features.obi_by_exchange else {}
        )
        obi_5 = primary_obis.get(5, 0.0)
        obi_20 = primary_obis.get(20, 0.0)

        if side == "BUY":
            # Libro profundo cargado de compras pero niveles inmediatos vacíos o vendedores
            if obi_20 >= self.obi_threshold and obi_5 < 0.0:
                logger.warning(
                    "strategy.anti_spoofing_reject_phantom_wall: BUY obi_20=%.2f pero obi_5=%.2f",
                    obi_20,
                    obi_5,
                )
                return False
        else:
            # Libro profundo cargado de ventas pero niveles inmediatos vacíos o compradores
            if obi_20 <= -self.obi_threshold and obi_5 > 0.0:
                logger.warning(
                    "strategy.anti_spoofing_reject_phantom_wall: SELL obi_20=%.2f pero obi_5=%.2f",
                    obi_20,
                    obi_5,
                )
                return False

        return True

    def on_feature_vector(
        self,
        features: FeatureVector,
        portfolio: PortfolioState,
    ) -> list[OrderRequest]:
        """Evalúa las condiciones de entrada y emite órdenes protegidas con validación anti-spoofing."""
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

        # Determinar porcentajes de SL y TP (estático o ajustado por volatilidad realizada)
        if self.use_dynamic_volatility and features.realized_volatility > 0.0:
            sl_pct = max(
                self.min_sl_pct,
                min(self.max_sl_pct, features.realized_volatility * self.vol_multiplier),
            )
            tp_pct = sl_pct * 2.0
        else:
            sl_pct = self.stop_loss_pct
            tp_pct = self.take_profit_pct

        # Señal LONG
        if z >= self.z_threshold and obi >= self.obi_threshold:
            # Validar defensas anti-spoofing antes de emitir la orden
            if not self._validate_anti_spoofing("BUY", features):
                return []

            stop_px = quantize(mid_px * (1.0 - sl_pct), self.tick_size)
            tp_px = quantize(mid_px * (1.0 + tp_pct), self.tick_size)
            order_px = quantize(mid_px, self.tick_size) if self.entry_order_type == "LIMIT" else 0.0
            tif = self.time_in_force if self.entry_order_type == "LIMIT" else None

            logger.info(
                "strategy.signal_long: symbol=%s z=%.2f obi=%.2f px=%.2f sl=%.2f tp=%.2f rho=%.2f",
                self.symbol,
                z,
                obi,
                mid_px,
                stop_px,
                tp_px,
                features.liquidity_persistence,
            )
            return [
                OrderRequest(
                    symbol=self.symbol,
                    side="BUY",
                    order_type=self.entry_order_type,
                    quantity=self.trade_size,
                    price=order_px,
                    time_in_force=tif,
                    stop_loss_price=stop_px,
                    take_profit_price=tp_px,
                )
            ]

        # Señal SHORT
        if z <= -self.z_threshold and obi <= -self.obi_threshold:
            # Validar defensas anti-spoofing antes de emitir la orden
            if not self._validate_anti_spoofing("SELL", features):
                return []

            stop_px = quantize(mid_px * (1.0 + sl_pct), self.tick_size)
            tp_px = quantize(mid_px * (1.0 - tp_pct), self.tick_size)
            order_px = quantize(mid_px, self.tick_size) if self.entry_order_type == "LIMIT" else 0.0
            tif = self.time_in_force if self.entry_order_type == "LIMIT" else None

            logger.info(
                "strategy.signal_short: symbol=%s z=%.2f obi=%.2f px=%.2f sl=%.2f tp=%.2f rho=%.2f",
                self.symbol,
                z,
                obi,
                mid_px,
                stop_px,
                tp_px,
                features.liquidity_persistence,
            )
            return [
                OrderRequest(
                    symbol=self.symbol,
                    side="SELL",
                    order_type=self.entry_order_type,
                    quantity=self.trade_size,
                    price=order_px,
                    time_in_force=tif,
                    stop_loss_price=stop_px,
                    take_profit_price=tp_px,
                )
            ]

        return []

    def on_order_update(self, order: ManagedOrder) -> None:
        """Actualiza el seguimiento de órdenes abiertas."""
        self._active_orders[order.client_order_id] = order
