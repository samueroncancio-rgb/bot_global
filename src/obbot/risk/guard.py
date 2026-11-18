"""Proxy de validación pre-trade bloqueante e ineludible (RiskGuard)."""

from collections import deque

from obbot.common.logging import get_logger
from obbot.risk.kill_switch import FiveLayerKillSwitch, KillSwitchAction
from obbot.risk.limits import (
    OrderRequest,
    PortfolioState,
    RiskAuthorizationToken,
    RiskDecision,
    RiskLimits,
)

logger = get_logger("risk.guard")


class RiskGuard:
    """
    Guardia de riesgo pre-trade institucional.

    Garantiza que ninguna orden llegue al motor de ejecución sin:
    1. Pasar todos los checks cuantitativos, normativos y de seguridad.
    2. Contar con un Kill-Switch inactivo.
    3. Obtener un token criptográfico de autorización de un solo uso.
    """

    def __init__(
        self,
        limits: RiskLimits | None = None,
        kill_switch: FiveLayerKillSwitch | None = None,
    ) -> None:
        self.limits = limits or RiskLimits()
        self.kill_switch = kill_switch or FiveLayerKillSwitch(self.limits)
        self._order_timestamps_ns: deque[int] = deque()
        self._active_tokens: dict[str, RiskAuthorizationToken] = {}

    def evaluate_order(
        self,
        order: OrderRequest,
        portfolio: PortfolioState,
        current_mark_price: float,
        current_ts_ns: int,
    ) -> RiskDecision:
        """
        Valida síncronamente una solicitud de orden antes de permitir su ejecución.
        """
        # 1. Comprobación del Kill-Switch
        ks_action = self.kill_switch.get_highest_severity_action()
        if ks_action == KillSwitchAction.CLOSE_POSITIONS_EMERGENCY and not order.reduce_only:
            return RiskDecision(
                approved=False,
                reason=(
                    "Kill-Switch en CLOSE_POSITIONS_EMERGENCY: "
                    f"{'; '.join(self.kill_switch.get_active_reasons())}"
                ),
            )
        if (
            ks_action in (KillSwitchAction.BLOCK_NEW_ENTRIES, KillSwitchAction.CANCEL_OPEN_ORDERS)
            and not order.reduce_only
        ):
            return RiskDecision(
                approved=False,
                reason=(
                    f"Kill-Switch en {ks_action.name}: "
                    f"{'; '.join(self.kill_switch.get_active_reasons())}"
                ),
            )

        # 2. Margen y modo de posición
        if self.limits.margin_type != "ISOLATED":
            return RiskDecision(
                approved=False,
                reason=(
                    f"Modo de margen no permitido: {self.limits.margin_type} (debe ser ISOLATED)"
                ),
            )
        if self.limits.position_mode != "ONE_WAY":
            return RiskDecision(
                approved=False,
                reason=(
                    f"Modo de posición no permitido: {self.limits.position_mode} (debe ser ONE_WAY)"
                ),
            )

        # 3. Precio efectivo para valoración
        is_limit_with_price = order.price > 0.0 and order.order_type == "LIMIT"
        effective_price = order.price if is_limit_with_price else current_mark_price
        if effective_price <= 0.0:
            return RiskDecision(
                approved=False,
                reason=f"Precio inválido para valoración nocional: {effective_price}",
            )

        order_notional = order.quantity * effective_price

        # 4. Límites de nocional por orden (solo para entradas)
        if not order.reduce_only and order_notional > self.limits.max_order_notional_usd:
            return RiskDecision(
                approved=False,
                reason=(
                    f"Nocional de orden excesivo: ${order_notional:.2f} "
                    f"(límite ${self.limits.max_order_notional_usd:.2f})"
                ),
            )

        # 5. Límite nocional de posición agregada
        if not order.reduce_only and portfolio.positions:
            pos = portfolio.positions.get(order.symbol)
            current_size = pos.size if pos else 0.0
            # Si la orden suma a la posición existente
            is_increasing = (current_size >= 0.0 and order.side == "BUY") or (
                current_size <= 0.0 and order.side == "SELL"
            )
            if is_increasing:
                projected_size = abs(current_size) + order.quantity
                projected_notional = projected_size * effective_price
                if projected_notional > self.limits.max_position_notional_usd:
                    return RiskDecision(
                        approved=False,
                        reason=(
                            f"Nocional proyectado de posición excesivo: ${projected_notional:.2f} "
                            f"(límite ${self.limits.max_position_notional_usd:.2f})"
                        ),
                    )

        # 6. Protección Fat-Finger (desviación de precio respecto a mark price)
        if order.order_type == "LIMIT" and order.price > 0.0:
            deviation = abs(order.price - current_mark_price) / current_mark_price
            if deviation > self.limits.fat_finger_price_deviation_pct:
                return RiskDecision(
                    approved=False,
                    reason=(
                        f"Desviación de precio Fat-Finger: {deviation * 100:.2f}% "
                        f"(límite {self.limits.fat_finger_price_deviation_pct * 100:.2f}%)"
                    ),
                )

        # 7. Rate Limiting de órdenes por minuto
        one_min_ago = current_ts_ns - 60_000_000_000
        while self._order_timestamps_ns and self._order_timestamps_ns[0] < one_min_ago:
            self._order_timestamps_ns.popleft()

        if len(self._order_timestamps_ns) >= self.limits.max_orders_per_minute:
            return RiskDecision(
                approved=False,
                reason=(
                    f"Límite de órdenes por minuto alcanzado: {len(self._order_timestamps_ns)} "
                    f"(máx {self.limits.max_orders_per_minute}/min)"
                ),
            )

        # 8. Verificación de Stop-Loss para órdenes de apertura
        if not order.reduce_only and order.stop_loss_price is not None:
            if order.side == "BUY" and order.stop_loss_price >= effective_price:
                return RiskDecision(
                    approved=False,
                    reason=(
                        f"Stop-Loss para BUY debe ser menor al precio de entrada "
                        f"(SL: {order.stop_loss_price}, Entrada: {effective_price})"
                    ),
                )
            if order.side == "SELL" and order.stop_loss_price <= effective_price:
                return RiskDecision(
                    approved=False,
                    reason=(
                        f"Stop-Loss para SELL debe ser mayor al precio de entrada "
                        f"(SL: {order.stop_loss_price}, Entrada: {effective_price})"
                    ),
                )
            loss_pct = abs(effective_price - order.stop_loss_price) / effective_price
            if loss_pct > (self.limits.max_trade_loss_pct + 1e-6):
                return RiskDecision(
                    approved=False,
                    reason=(
                        f"Pérdida potencial por Stop-Loss ({loss_pct * 100:.2f}%) excede "
                        f"el límite máximo permitido ({self.limits.max_trade_loss_pct * 100:.2f}%)"
                    ),
                )

        # Registro para rate-limiting
        self._order_timestamps_ns.append(current_ts_ns)

        # Emisión del token de autorización
        token = RiskAuthorizationToken.generate(
            symbol=order.symbol,
            side=order.side,
            quantity=order.quantity,
            price=effective_price,
            created_at_ns=current_ts_ns,
        )
        self._active_tokens[token.token_id] = token

        logger.info(
            "risk.order_authorized: token_id=%s symbol=%s side=%s qty=%s price=%s",
            token.token_id,
            order.symbol,
            order.side,
            order.quantity,
            effective_price,
            extra={
                "token_id": token.token_id,
                "symbol": order.symbol,
                "side": order.side,
                "quantity": order.quantity,
                "price": effective_price,
            },
        )

        return RiskDecision(
            approved=True,
            reason="APROBADO_PRE_TRADE",
            token=token,
        )

    def consume_token(
        self,
        token_id: str,
        order: OrderRequest,
        current_ts_ns: int,
    ) -> bool:
        """
        Verifica y consume un token de autorización previo al envío al exchange.
        El token solo puede ser utilizado una única vez y antes de su expiración.
        """
        token = self._active_tokens.pop(token_id, None)
        if not token:
            logger.error(
                "risk.token_not_found: token_id=%s",
                token_id,
                extra={"token_id": token_id},
            )
            return False

        if current_ts_ns > token.expires_at_ns:
            logger.error(
                "risk.token_expired: token_id=%s",
                token_id,
                extra={"token_id": token_id},
            )
            return False

        if token.symbol != order.symbol or token.side != order.side:
            logger.error(
                "risk.token_mismatch: token_id=%s expected=%s %s actual=%s %s",
                token_id,
                token.symbol,
                token.side,
                order.symbol,
                order.side,
                extra={
                    "token_id": token_id,
                    "expected": f"{token.symbol} {token.side}",
                    "actual": f"{order.symbol} {order.side}",
                },
            )
            return False

        return True
