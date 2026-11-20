"""Estrategia baseline de ejemplo basada en confluencia de Delta Global y OBI."""

import math
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
        stop_loss_pct: float = 0.0050,  # 50 bps (0.50% horizonte swing intradía)
        take_profit_pct: float = 0.0120,  # 120 bps (1.20% ratio R/B nominal 1:2.4)
        tick_size: float | None = None,
        entry_order_type: str = "MARKET",
        time_in_force: str = "GTC",
        use_dynamic_volatility: bool = True,
        vol_multiplier: float = 1.5,
        min_sl_pct: float = 0.0035,  # 35 bps suelo de seguridad
        max_sl_pct: float = 0.0100,  # 100 bps
        min_ticks_sl: int = 4,
        taker_fee_rate: float = 0.0005,  # 5 bps
        enable_flow_invalidation: bool = False,
        invalidation_z_threshold: float = 1.0,
        invalidation_obi_threshold: float = 0.25,
        enable_anti_spoofing: bool = True,
        enable_htf_filter: bool = True,
        enable_session_filter: bool = True,
        asian_session_z_threshold: float = 2.2,
        min_persistence: float = 0.50,
        min_confirming_venues: int = 2,
        require_persistence_warmup: bool = False,
        cooldown_sec: float = 3.0,
        post_close_cooldown_sec: float = 120.0,
    ) -> None:
        super().__init__(name="BaselineOrderFlow", symbol=symbol)
        self.z_threshold = z_threshold
        self.obi_threshold = obi_threshold
        self.cooldown_sec = cooldown_sec
        self.post_close_cooldown_sec = post_close_cooldown_sec
        self.enable_htf_filter = enable_htf_filter
        self.enable_session_filter = enable_session_filter
        self.asian_session_z_threshold = asian_session_z_threshold
        self._last_order_ts_ns: int = 0
        self._last_position_closed_ts_ns: int = 0
        self._prev_position_size: float = 0.0

        # Dimensionamiento adaptativo según símbolo (optimizado para cuenta pequeña de $100 USD)
        if trade_size is not None:
            self.trade_size = trade_size
        elif "ETH" in symbol.upper():
            self.trade_size = 0.02  # 0.02 ETH (~$35.50 USD nocional, margen a 3x ~$11.83 USD, supera mínimo de $20 de Binance)
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

    def get_trade_size(self, mid_price: float) -> float:
        """
        Calcula el tamaño de la orden garantizando que supere el nocional mínimo de Binance ($20.00 USD)
        y se mantenga adecuado para una cuenta de $100 USD (apalancamiento 3x = margen ~$11-$12 USD).
        """
        if self.trade_size is not None:
            size = self.trade_size
        elif "ETH" in self.symbol.upper():
            size = 0.02
        else:
            size = 0.001

        # Salvaguarda Binance MIN_NOTIONAL: mínimo $22.00 USD nocional
        min_notional_usd = 22.0
        if mid_price > 0.0:
            notional = size * mid_price
            if notional < min_notional_usd:
                step = 0.001 if ("ETH" in self.symbol.upper() or "BTC" in self.symbol.upper()) else 0.01
                size = round(math.ceil((min_notional_usd / mid_price) / step) * step, 4)
        return size

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
        active_venues = [ex for ex, obis in features.obi_by_exchange.items() if 10 in obis]
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

        current_pos = portfolio.get_position(self.symbol) if portfolio else None
        current_size = current_pos.size if current_pos else 0.0

        # Detección de transición de cierre de posición para activar cooldown post-trade
        if abs(self._prev_position_size) > 1e-6 and abs(current_size) <= 1e-6:
            self._last_position_closed_ts_ns = features.timestamp_ns
            logger.info(
                "strategy.position_closed_cooldown_started: durmiendo %.0fs tras cierre de posicion",
                self.post_close_cooldown_sec,
            )
        self._prev_position_size = current_size

        # SINGLE-POSITION LOCK: Si ya existe una posición abierta, NO emitir nuevas entradas
        if abs(current_size) > 1e-6:
            if self.enable_flow_invalidation:
                is_long = current_size > 0
                z = features.global_delta_z
                obi = features.obi_10_levels

                should_invalidate = False
                exit_side: Literal["BUY", "SELL"] = "SELL" if is_long else "BUY"

                if (
                    is_long
                    and (
                        z <= -self.invalidation_z_threshold
                        or obi <= -self.invalidation_obi_threshold
                    )
                ) or (
                    not is_long
                    and (
                        z >= self.invalidation_z_threshold or obi >= self.invalidation_obi_threshold
                    )
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

        # Cooldown post-cierre de posición (evitar re-entradas inmediatas y sobre-operación / whipsaw)
        if self._last_position_closed_ts_ns > 0:
            post_close_ns = int(self.post_close_cooldown_sec * 1_000_000_000)
            if (features.timestamp_ns - self._last_position_closed_ts_ns) < post_close_ns:
                return []

        mid_px = features.mid_price
        z = features.global_delta_z
        obi = features.obi_10_levels

        # 1. Control de Cooldown: evitar ráfagas consecutivas de órdenes
        if self._last_order_ts_ns > 0:
            cooldown_ns = int(self.cooldown_sec * 1_000_000_000)
            if (features.timestamp_ns - self._last_order_ts_ns) < cooldown_ns:
                return []

        # 2. Bloqueo de orden en vuelo (in-flight): no emitir si hay orden activa pendiente
        has_in_flight = any(
            (o.symbol == self.symbol or o.symbol.replace("-PERP", "").replace("-", "") == self.symbol.replace("-PERP", "").replace("-", ""))
            and o.status.name
            in ("PENDING_RISK", "AUTHORIZED", "SUBMITTED", "NEW", "PARTIALLY_FILLED")
            for o in self._active_orders.values()
        )
        if has_in_flight:
            return []

        # Determinar distancias de SL y TP
        if self.use_dynamic_volatility and features.realized_volatility > 0.0:
            raw_sl = features.realized_volatility * self.vol_multiplier
            # Suelo de fricción: spread + comisiones taker ida y vuelta
            friction_pct = (features.spread_bps / 10000.0) + (2.0 * self.taker_fee_rate)
            friction_floor = friction_pct * 2.0  # Suelo reforzado a 2x fricción total

            effective_sl = max(self.min_sl_pct, friction_floor, min(self.max_sl_pct, raw_sl))
            # Garantizar que el TP conserve como mínimo el ratio Swing diseñado (2.4x nominal)
            min_rr_ratio = (
                self.take_profit_pct / self.stop_loss_pct if self.stop_loss_pct > 0 else 2.4
            )
            effective_tp = max(effective_sl * min_rr_ratio, friction_pct * 3.0)
        else:
            effective_sl = self.stop_loss_pct
            effective_tp = self.take_profit_pct

        # Garantizar distancia mínima en múltiplos enteros de tick_size
        min_tick_dist = self.min_ticks_sl * self.tick_size
        sl_dist = max(mid_px * effective_sl, min_tick_dist)
        tp_dist = max(mid_px * effective_tp, min_tick_dist * 2.0)
        trade_qty = self.get_trade_size(mid_px)

        # Filtro de horarios de sesión institucional (UTC):
        # Sesión asiática / baja liquidez: 22:00 a 07:00 UTC -> Exige mayor confirmación (Z-score 2.2)
        utc_hour = (features.timestamp_ns // 1_000_000_000 // 3600) % 24
        is_asian_session = (utc_hour >= 22 or utc_hour < 7) and self.enable_session_filter
        effective_z_threshold = self.asian_session_z_threshold if is_asian_session else self.z_threshold
        effective_obi_threshold = (
            max(self.obi_threshold, 0.25) if is_asian_session else self.obi_threshold
        )

        # Señal LONG
        if z >= effective_z_threshold and obi >= effective_obi_threshold:
            # Filtro de Tendencia Superior (HTF)
            if self.enable_htf_filter and features.htf_trend_regime == "BEARISH":
                logger.info(
                    "strategy.htf_reject_long: señal descartada por régimen bajista HTF (price=%.2f < EMA15m=%.2f o CVD=%.1f < 0)",
                    mid_px,
                    features.htf_ema_15m,
                    features.global_cvd,
                )
                return []

            if not self._validate_anti_spoofing("BUY", features):
                return []

            stop_px = quantize(mid_px - sl_dist, self.tick_size)
            tp_px = quantize(mid_px + tp_dist, self.tick_size)
            order_px = quantize(mid_px, self.tick_size) if self.entry_order_type == "LIMIT" else 0.0
            tif = self.time_in_force if self.entry_order_type == "LIMIT" else "GTC"

            self._last_order_ts_ns = features.timestamp_ns
            logger.info(
                "strategy.signal_long: symbol=%s z=%.2f obi=%.2f px=%.2f sl=%.2f tp=%.2f type=%s size=%.4f (notional=$%.2f)",
                self.symbol,
                z,
                obi,
                mid_px,
                stop_px,
                tp_px,
                self.entry_order_type,
                trade_qty,
                trade_qty * mid_px,
            )
            return [
                OrderRequest(
                    symbol=self.symbol,
                    side="BUY",
                    order_type=self.entry_order_type,  # type: ignore[arg-type]
                    quantity=trade_qty,
                    price=order_px,
                    time_in_force=tif,  # type: ignore[arg-type]
                    stop_loss_price=stop_px,
                    take_profit_price=tp_px,
                )
            ]

        # Señal SHORT
        if z <= -effective_z_threshold and obi <= -effective_obi_threshold:
            # Filtro de Tendencia Superior (HTF)
            if self.enable_htf_filter and features.htf_trend_regime == "BULLISH":
                logger.info(
                    "strategy.htf_reject_short: señal descartada por régimen alcista HTF (price=%.2f > EMA15m=%.2f o CVD=%.1f > 0)",
                    mid_px,
                    features.htf_ema_15m,
                    features.global_cvd,
                )
                return []

            if not self._validate_anti_spoofing("SELL", features):
                return []

            stop_px = quantize(mid_px + sl_dist, self.tick_size)
            tp_px = quantize(mid_px - tp_dist, self.tick_size)
            order_px = quantize(mid_px, self.tick_size) if self.entry_order_type == "LIMIT" else 0.0
            tif = self.time_in_force if self.entry_order_type == "LIMIT" else "GTC"

            self._last_order_ts_ns = features.timestamp_ns
            logger.info(
                "strategy.signal_short: symbol=%s z=%.2f obi=%.2f px=%.2f sl=%.2f tp=%.2f type=%s size=%.4f (notional=$%.2f)",
                self.symbol,
                z,
                obi,
                mid_px,
                stop_px,
                tp_px,
                self.entry_order_type,
                trade_qty,
                trade_qty * mid_px,
            )
            return [
                OrderRequest(
                    symbol=self.symbol,
                    side="SELL",
                    order_type=self.entry_order_type,  # type: ignore[arg-type]
                    quantity=trade_qty,
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
