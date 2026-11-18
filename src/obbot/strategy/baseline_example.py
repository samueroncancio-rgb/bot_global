"""Estrategia baseline de ejemplo basada en confluencia de Delta Global y OBI."""

from typing import Literal

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
    - Ejecución MARKET (Taker inmediata) por defecto para eliminar la selección adversa de órdenes GTX.
    - Adjunto estricto de Stop-Loss y Take-Profit atómico con suelo de fricción dinámico.
    - Soporte para volatilidad dinámica adaptativa y cuantización por tick_size discreto.
    - Salida temprana por invalidación de flujo (Early Exit) cuando la presión L2 se revierte.
    - Triple cinturón de defensa anti-spoofing con control de calentamiento y consenso multi-venue.
    - Dimensionamiento adaptativo según el activo (BTC vs ETH) y capital de cuenta ($100 USD).
    """

    def __init__(
        self,
        symbol: str = "BTCUSDT",
        z_threshold: float = 1.8,
        obi_threshold: float = 0.20,
        trade_size: float | None = None,
        stop_loss_pct: float = 0.0020,  # 20 bps (0.20%)
        take_profit_pct: float = 0.0040,  # 40 bps (0.40% ratio riesgo/beneficio 1:2)
        tick_size: float | None = None,
        entry_order_type: str = "MARKET",
        time_in_force: str = "GTC",
        use_dynamic_volatility: bool = True,
        vol_multiplier: float = 1.5,
        min_sl_pct: float = 0.0015,  # 15 bps
        max_sl_pct: float = 0.0060,  # 60 bps
        min_ticks_sl: int = 4,
        taker_fee_rate: float = 0.0005,  # 5 bps
        enable_flow_invalidation: bool = True,
        invalidation_z_threshold: float = 1.0,
        invalidation_obi_threshold: float = 0.25,
        enable_anti_spoofing: bool = True,
        min_persistence: float = 0.50,
        min_confirming_venues: int = 2,
        require_persistence_warmup: bool = False,
    ) -> None:
        super().__init__(name="BaselineOrderFlow", symbol=symbol)
        self.z_threshold = z_threshold
        self.obi_threshold = obi_threshold

        # Dimensionamiento adaptativo según símbolo (optimizado para cuenta pequeña de $100 USD)
        if trade_size is not None:
            self.trade_size = trade_size
        elif "ETH" in symbol.upper():
            self.trade_size = 0.01  # 0.01 ETH (~$26 USD nocional, margen a 3x ~$8.67 USD)
        else:
            self.trade_size = 0.001  # 0.001 BTC (~$64 USD nocional, margen a 3x ~$21.33 USD)

        # Tick size adaptativo según símbolo si no se especifica
        if tick_size is not None:
            self.tick_size = tick_size
        elif "ETH" in symbol.upper():
            self.tick_size = 0.01
        else:
            self.tick_size = 0.1

        self.stop_loss_pct = stop_loss_pct
        self.take_profit_pct = take_profit_pct
        self.entry_order_type = entry_order_type.upper()
        self.time_in_force = time_in_force.upper()
        self.use_dynamic_volatility = use_dynamic_volatility
        self.vol_multiplier = vol_multiplier
        self.min_sl_pct = min_sl_pct
        self.max_sl_pct = max_sl_pct
        self.min_ticks_sl = min_ticks_sl
        self.taker_fee_rate = taker_fee_rate
        self.enable_flow_invalidation = enable_flow_invalidation
        self.invalidation_z_threshold = invalidation_z_threshold
        self.invalidation_obi_threshold = invalidation_obi_threshold
        self.enable_anti_spoofing = enable_anti_spoofing
        self.min_persistence = min_persistence
        self.min_confirming_venues = min_confirming_venues
        self.require_persistence_warmup = require_persistence_warmup
        self._active_orders: dict[str, ManagedOrder] = {}

    def _validate_anti_spoofing(self, side: str, features: FeatureVector) -> bool:
        """
        Valida que la liquidez del libro sea genuina y persistente frente a spoofing HFT.

        Aplica 3 barreras de filtrado matemático:
        1. Persistencia temporal (rho_1): Si require_persistence_warmup=True, rechaza búferes fríos.
           Si rho != 0.0, exige que la autocorrelación lag-1 sea >= min_persistence.
        2. Consenso multi-exchange: Si múltiples exchanges reportan libros L2, exige confirmación
           cruzada en al menos min_confirming_venues para evitar manipulación monoplataforma.
        3. Detección de muros fantasma (Phantom Wall): Descarta la señal si en cualquier exchange
           el libro profundo (20 niveles) está muy cargado pero los 5 niveles inmediatos no tienen soporte.
        """
        if not self.enable_anti_spoofing:
            return True

        # 1. Filtro de Persistencia Temporal (Autocorrelación lag-1)
        if self.require_persistence_warmup and features.liquidity_persistence <= 0.0:
            logger.info(
                "strategy.anti_spoofing_warming_up: buffers de persistencia aun sin calibrar (rho=%.2f)",
                features.liquidity_persistence,
            )
            return False

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

        # 3. Detección de Muros Fantasma (Phantom Wall) en todos los exchanges activos
        for ex, obis in features.obi_by_exchange.items():
            if 5 in obis and 20 in obis:
                obi_5 = obis[5]
                obi_20 = obis[20]
                if side == "BUY" and obi_20 >= self.obi_threshold and obi_5 < 0.0:
                    logger.warning(
                        "strategy.anti_spoofing_reject_phantom_wall: %s BUY obi_20=%.2f pero obi_5=%.2f",
                        ex,
                        obi_20,
                        obi_5,
                    )
                    return False
                elif side == "SELL" and obi_20 <= -self.obi_threshold and obi_5 > 0.0:
                    logger.warning(
                        "strategy.anti_spoofing_reject_phantom_wall: %s SELL obi_20=%.2f pero obi_5=%.2f",
                        ex,
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
        """Evalúa las condiciones de entrada y salida con gestión de riesgo y anti-spoofing."""
        if features.symbol != self.symbol or features.mid_price <= 0.0:
            return []

        current_pos = portfolio.positions.get(self.symbol) if portfolio.positions else None
        current_size = current_pos.size if current_pos else 0.0

        # Si ya existe una posición abierta, evaluar invalidación temprana de flujo (Early Exit)
        if abs(current_size) > 1e-6:
            if self.enable_flow_invalidation:
                is_long = current_size > 0
                z = features.global_delta_z
                obi = features.obi_10_levels

                should_invalidate = False
                exit_side: Literal["BUY", "SELL"] = "SELL" if is_long else "BUY"

                if is_long and (
                    z <= -self.invalidation_z_threshold
                    or obi <= -self.invalidation_obi_threshold
                ):
                    should_invalidate = True
                elif not is_long and (
                    z >= self.invalidation_z_threshold
                    or obi >= self.invalidation_obi_threshold
                ):
                    should_invalidate = True

                if should_invalidate:
                    logger.warning(
                        "strategy.flow_invalidation_exit: cerrando posicion %s size=%.4f por reversion z=%.2f obi=%.2f",
                        self.symbol,
                        current_size,
                        z,
                        obi,
                    )
                    return [
                        OrderRequest(
                            symbol=self.symbol,
                            side=exit_side,
                            order_type="MARKET",
                            quantity=abs(current_size),
                            price=0.0,
                            reduce_only=True,
                            time_in_force="GTC",
                        )
                    ]
            return []

        mid_px = features.mid_price
        z = features.global_delta_z
        obi = features.obi_10_levels

        # Determinar distancias de SL y TP
        if self.use_dynamic_volatility and features.realized_volatility > 0.0:
            raw_sl = features.realized_volatility * self.vol_multiplier
            # Suelo de fricción: spread + comisiones taker ida y vuelta
            friction_pct = (features.spread_bps / 10000.0) + (2.0 * self.taker_fee_rate)
            friction_floor = friction_pct * 1.5

            effective_sl = max(self.min_sl_pct, friction_floor, min(self.max_sl_pct, raw_sl))
            effective_tp = effective_sl * 2.0
        else:
            effective_sl = self.stop_loss_pct
            effective_tp = self.take_profit_pct

        # Garantizar distancia mínima en múltiplos enteros de tick_size
        min_tick_dist = self.min_ticks_sl * self.tick_size
        sl_dist = max(mid_px * effective_sl, min_tick_dist)
        tp_dist = max(mid_px * effective_tp, min_tick_dist * 2.0)

        # Señal LONG
        if z >= self.z_threshold and obi >= self.obi_threshold:
            if not self._validate_anti_spoofing("BUY", features):
                return []

            stop_px = quantize(mid_px - sl_dist, self.tick_size)
            tp_px = quantize(mid_px + tp_dist, self.tick_size)
            order_px = (
                quantize(mid_px, self.tick_size) if self.entry_order_type == "LIMIT" else 0.0
            )
            tif = self.time_in_force if self.entry_order_type == "LIMIT" else "GTC"

            logger.info(
                "strategy.signal_long: symbol=%s z=%.2f obi=%.2f px=%.2f sl=%.2f tp=%.2f type=%s size=%.4f",
                self.symbol,
                z,
                obi,
                mid_px,
                stop_px,
                tp_px,
                self.entry_order_type,
                self.trade_size,
            )
            return [
                OrderRequest(
                    symbol=self.symbol,
                    side="BUY",
                    order_type=self.entry_order_type,  # type: ignore[arg-type]
                    quantity=self.trade_size,
                    price=order_px,
                    time_in_force=tif,  # type: ignore[arg-type]
                    stop_loss_price=stop_px,
                    take_profit_price=tp_px,
                )
            ]

        # Señal SHORT
        if z <= -self.z_threshold and obi <= -self.obi_threshold:
            if not self._validate_anti_spoofing("SELL", features):
                return []

            stop_px = quantize(mid_px + sl_dist, self.tick_size)
            tp_px = quantize(mid_px - tp_dist, self.tick_size)
            order_px = (
                quantize(mid_px, self.tick_size) if self.entry_order_type == "LIMIT" else 0.0
            )
            tif = self.time_in_force if self.entry_order_type == "LIMIT" else "GTC"

            logger.info(
                "strategy.signal_short: symbol=%s z=%.2f obi=%.2f px=%.2f sl=%.2f tp=%.2f type=%s size=%.4f",
                self.symbol,
                z,
                obi,
                mid_px,
                stop_px,
                tp_px,
                self.entry_order_type,
                self.trade_size,
            )
            return [
                OrderRequest(
                    symbol=self.symbol,
                    side="SELL",
                    order_type=self.entry_order_type,  # type: ignore[arg-type]
                    quantity=self.trade_size,
                    price=order_px,
                    time_in_force=tif,  # type: ignore[arg-type]
                    stop_loss_price=stop_px,
                    take_profit_price=tp_px,
                )
            ]

        return []

    def on_order_update(self, order: ManagedOrder) -> None:
        """Actualiza el seguimiento de órdenes abiertas."""
        self._active_orders[order.client_order_id] = order
