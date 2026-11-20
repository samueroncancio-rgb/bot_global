"""Máquina de estados de órdenes con adjunto atómico de Stop-Loss y control de fallos."""

import asyncio
import time
from dataclasses import dataclass
from enum import Enum, auto

from obbot.common.clock import Clock, SystemClock
from obbot.common.logging import get_logger
from obbot.execution.algo_orders import BinanceAlgoOrderClient
from obbot.execution.binance_rest import BinanceFuturesRestClient, BinanceRestError
from obbot.risk.guard import RiskGuard
from obbot.risk.limits import OrderRequest, PortfolioState

logger = get_logger("execution.order_manager")


class OrderStatus(Enum):
    """Estados del ciclo de vida de una orden."""

    PENDING_RISK = auto()
    AUTHORIZED = auto()
    SUBMITTED = auto()
    NEW = auto()
    PARTIALLY_FILLED = auto()
    FILLED = auto()
    CANCELLED = auto()
    REJECTED = auto()
    EXPIRED = auto()


@dataclass
class ManagedOrder:
    """Representación y trazabilidad completa de una orden gestionada."""

    client_order_id: str
    symbol: str
    side: str
    order_type: str
    quantity: float
    price: float
    reduce_only: bool
    status: OrderStatus
    exchange_order_id: int | None = None
    filled_quantity: float = 0.0
    avg_fill_price: float = 0.0
    stop_loss_price: float | None = None
    take_profit_price: float | None = None
    algo_stop_id: int | None = None
    algo_tp_id: int | None = None
    created_at_ns: int = 0
    updated_at_ns: int = 0
    reject_reason: str = ""


class OrderManager:
    """
    Gestor institucional del ciclo de vida de órdenes.

    Garantiza:
    1. Zero Unchecked Execution: validación previa obligatoria por RiskGuard.
    2. newClientOrderId determinista.
    3. Manejo de timeouts ambiguos mediante consulta por clientOrderId sin reintentos ciegos.
    4. Stop-Loss atómico innegociable con watchdog de 1500ms y cierre forzoso por mercado.
    """

    def __init__(
        self,
        rest_client: BinanceFuturesRestClient,
        algo_client: BinanceAlgoOrderClient,
        risk_guard: RiskGuard,
        clock: Clock | None = None,
        stop_attach_timeout_ms: float = 1500.0,
    ) -> None:
        self.rest_client = rest_client
        self.algo_client = algo_client
        self.risk_guard = risk_guard
        self.clock = clock or SystemClock()
        self.stop_attach_timeout_ms = stop_attach_timeout_ms

        self._orders: dict[str, ManagedOrder] = {}  # client_order_id -> ManagedOrder
        self._sequence_counter: int = 0
        self._stop_watchdogs: dict[str, asyncio.Task[None]] = {}

    def _generate_client_order_id(self, symbol: str, side: str) -> str:
        """Genera un clientOrderId determinista y trazable (máximo 36 caracteres para Binance)."""
        self._sequence_counter += 1
        clean_symbol = symbol.replace("-PERP", "").replace("-", "")[:8]
        side_code = "B" if side.upper() == "BUY" else "S"
        ts_ms = int(time.time() * 1000)
        return f"O_{clean_symbol}_{side_code}_{ts_ms}_{self._sequence_counter:04d}"

    async def submit_order(
        self,
        request: OrderRequest,
        portfolio: PortfolioState,
        current_mark_price: float,
    ) -> ManagedOrder:
        """
        Ejecuta el flujo completo de validación pre-trade y envío de orden.
        """
        now_ns = self.clock.now_ns()
        client_order_id = request.client_order_id or self._generate_client_order_id(
            request.symbol, request.side
        )

        managed = ManagedOrder(
            client_order_id=client_order_id,
            symbol=request.symbol,
            side=request.side,
            order_type=request.order_type,
            quantity=request.quantity,
            price=request.price,
            reduce_only=request.reduce_only,
            status=OrderStatus.PENDING_RISK,
            stop_loss_price=request.stop_loss_price,
            take_profit_price=request.take_profit_price,
            created_at_ns=now_ns,
            updated_at_ns=now_ns,
        )
        self._orders[client_order_id] = managed

        # 1. Validación pre-trade ineludible por RiskGuard
        decision = self.risk_guard.evaluate_order(
            order=request,
            portfolio=portfolio,
            current_mark_price=current_mark_price,
            current_ts_ns=now_ns,
        )

        if not decision.approved:
            managed.status = OrderStatus.REJECTED
            managed.reject_reason = f"RiskGuard rechazó la orden: {decision.reason}"
            managed.updated_at_ns = self.clock.now_ns()
            logger.warning(
                "order_manager.order_rejected_by_risk: %s - %s",
                client_order_id,
                decision.reason,
            )
            return managed

        # 2. Consumir token emitido
        assert decision.token is not None
        if not self.risk_guard.consume_token(decision.token.token_id, request, now_ns):
            managed.status = OrderStatus.REJECTED
            managed.reject_reason = "Fallo al validar token de autorización de RiskGuard"
            return managed

        managed.status = OrderStatus.AUTHORIZED

        # 3. Envío al exchange con manejo seguro de fallos
        managed.status = OrderStatus.SUBMITTED
        try:
            res = await self.rest_client.create_order(
                symbol=request.symbol,
                side=request.side,
                order_type=request.order_type,
                quantity=request.quantity,
                price=request.price if request.order_type == "LIMIT" else None,
                time_in_force=request.time_in_force if request.order_type == "LIMIT" else None,
                reduce_only=request.reduce_only,
                client_order_id=client_order_id,
            )
            managed.exchange_order_id = res.get("orderId")
            managed.status = OrderStatus.NEW
            managed.updated_at_ns = self.clock.now_ns()

            logger.info(
                "order_manager.order_placed: %s (id=%s)",
                client_order_id,
                managed.exchange_order_id,
            )
            return managed

        except BinanceRestError as exc:
            managed.status = OrderStatus.REJECTED
            managed.reject_reason = f"Error Binance [{exc.code}]: {exc.message}"
            managed.updated_at_ns = self.clock.now_ns()
            logger.error(
                "order_manager.order_rejected_by_binance: %s - %s",
                client_order_id,
                managed.reject_reason,
            )
            return managed

        except Exception as exc:
            # Error de red ambiguo o timeout: NUNCA reintentar a ciegas
            logger.error(
                "order_manager.ambiguous_timeout: %s. Verificando por clientOrderId...",
                exc,
            )
            return await self._reconcile_ambiguous_order(managed)

    async def _reconcile_ambiguous_order(self, managed: ManagedOrder) -> ManagedOrder:
        """Consulta el estado de una orden tras un timeout de red ambiguo."""
        await asyncio.sleep(0.5)
        try:
            query_res = await self.rest_client.query_order(
                symbol=managed.symbol,
                orig_client_order_id=managed.client_order_id,
            )
            managed.exchange_order_id = query_res.get("orderId")
            status_str = query_res.get("status", "NEW")
            managed.status = self._map_binance_status(status_str)
            managed.updated_at_ns = self.clock.now_ns()
            return managed
        except Exception:
            managed.status = OrderStatus.REJECTED
            managed.reject_reason = "Timeout ambiguo no resuelto por consulta REST"
            return managed

    def _map_binance_status(self, status: str) -> OrderStatus:
        mapping = {
            "NEW": OrderStatus.NEW,
            "PARTIALLY_FILLED": OrderStatus.PARTIALLY_FILLED,
            "FILLED": OrderStatus.FILLED,
            "CANCELED": OrderStatus.CANCELLED,
            "REJECTED": OrderStatus.REJECTED,
            "EXPIRED": OrderStatus.EXPIRED,
        }
        return mapping.get(status, OrderStatus.NEW)

    async def on_order_fill(
        self,
        client_order_id: str,
        filled_qty: float,
        avg_price: float,
    ) -> None:
        """
        Procesa el fill de una orden.
        Si es una orden de entrada con stop_loss_price definido, inicia el watchdog de 1500ms
        para adjuntar la Algo Order de Stop-Loss de inmediato.
        """
        order = self._orders.get(client_order_id)
        if not order:
            return

        order.filled_quantity = filled_qty
        order.avg_fill_price = avg_price
        order.status = OrderStatus.FILLED
        order.updated_at_ns = self.clock.now_ns()

        # Si es una entrada protegida y no reduce_only, adjuntar Bracket (Stop-Loss y Take-Profit) inmediatamente
        if not order.reduce_only and (order.stop_loss_price is not None or order.take_profit_price is not None):
            task = asyncio.create_task(self._attach_atomic_bracket_with_watchdog(order))
            self._stop_watchdogs[client_order_id] = task

    async def _attach_atomic_stop_loss_with_watchdog(self, order: ManagedOrder) -> None:
        """Alias de compatibilidad: ejecuta el adjunto de bracket con watchdog."""
        await self._attach_atomic_bracket_with_watchdog(order)

    async def _attach_atomic_bracket_with_watchdog(self, order: ManagedOrder) -> None:
        """
        Envía Algo Orders de Stop-Loss y Take-Profit a Binance.
        Ejecuta salida forzosa si el Stop-Loss no puede adjuntarse en <= 1500ms.
        """
        exit_side = "SELL" if order.side == "BUY" else "BUY"
        qty = order.filled_quantity or order.quantity

        async def _try_send_bracket() -> bool:
            sl_ok = True
            tp_ok = True

            # 1. Adjuntar Stop-Loss (Crítico)
            if order.stop_loss_price is not None:
                client_algo_id_sl = f"SL_{order.client_order_id}"
                try:
                    res_sl = await self.algo_client.create_stop_loss(
                        symbol=order.symbol,
                        side=exit_side,
                        trigger_price=order.stop_loss_price,
                        quantity=qty,
                        close_position=False,
                        client_algo_id=client_algo_id_sl,
                    )
                    order.algo_stop_id = res_sl.get("algoId")
                    logger.info(
                        "order_manager.stop_loss_attached: %s (algoId=%s)",
                        order.client_order_id,
                        order.algo_stop_id,
                    )
                except Exception as exc:
                    logger.critical(
                        "order_manager.stop_loss_attach_failed: %s (%s)",
                        order.client_order_id,
                        exc,
                    )
                    sl_ok = False

            # 2. Adjuntar Take-Profit (TAKE_PROFIT_MARKET)
            if sl_ok and order.take_profit_price is not None:
                client_algo_id_tp = f"TP_{order.client_order_id}"
                try:
                    res_tp = await self.algo_client.create_take_profit(
                        symbol=order.symbol,
                        side=exit_side,
                        trigger_price=order.take_profit_price,
                        quantity=qty,
                        close_position=False,
                        client_algo_id=client_algo_id_tp,
                    )
                    order.algo_tp_id = res_tp.get("algoId")
                    logger.info(
                        "order_manager.take_profit_attached: %s (algoId=%s)",
                        order.client_order_id,
                        order.algo_tp_id,
                    )
                except Exception as exc:
                    logger.warning(
                        "order_manager.take_profit_attach_warning: %s (%s)",
                        order.client_order_id,
                        exc,
                    )
                    tp_ok = False

            return sl_ok

        try:
            # Ejecución con timeout estricto de 1500 ms para protección
            success = await asyncio.wait_for(
                _try_send_bracket(),
                timeout=self.stop_attach_timeout_ms / 1000.0,
            )
            if success:
                return
        except TimeoutError:
            logger.critical(
                "order_manager.stop_loss_watchdog_timeout: %s excedió %.0fms",
                order.client_order_id,
                self.stop_attach_timeout_ms,
            )

        # Si llegamos aquí, el Stop-Loss falló o expiró el watchdog:
        # 1. Disparar Capa 4 del Kill-Switch
        self.risk_guard.kill_switch.check_operational(
            consecutive_rejections=0,
            stop_loss_failed=True,
            now_ns=self.clock.now_ns(),
        )

        # 2. Cierre forzoso de emergencia con orden a mercado reduceOnly
        await self._emergency_market_close(order.symbol, exit_side, qty)

    async def cancel_algo_orders_for_symbol(self, symbol: str) -> None:
        """Cancela las Algo Orders (SL y TP) asociadas a un símbolo tras el cierre de posición."""
        clean_target = symbol.replace("-PERP", "").replace("-", "").upper()
        for order in self._orders.values():
            clean_ord = order.symbol.replace("-PERP", "").replace("-", "").upper()
            if clean_ord == clean_target:
                if order.algo_stop_id:
                    try:
                        await self.algo_client.cancel_algo_order(order.symbol, algo_id=order.algo_stop_id)
                        logger.info("order_manager.oco_cancelled_sl: %s", order.algo_stop_id)
                    except Exception as exc:
                        logger.debug("order_manager.cancel_sl_silent: %s", exc)
                    order.algo_stop_id = None
                if order.algo_tp_id:
                    try:
                        await self.algo_client.cancel_algo_order(order.symbol, algo_id=order.algo_tp_id)
                        logger.info("order_manager.oco_cancelled_tp: %s", order.algo_tp_id)
                    except Exception as exc:
                        logger.debug("order_manager.cancel_tp_silent: %s", exc)
                    order.algo_tp_id = None

    async def _emergency_market_close(self, symbol: str, side: str, quantity: float) -> None:
        """Cierra inmediatamente la posición a mercado ante fallo de protección."""
        logger.critical(
            "order_manager.executing_emergency_market_close: symbol=%s side=%s qty=%f",
            symbol,
            side,
            quantity,
        )
        try:
            await self.rest_client.create_order(
                symbol=symbol,
                side=side,
                order_type="MARKET",
                quantity=quantity,
                reduce_only=True,
                client_order_id=f"EMERGENCY_CLOSE_{int(time.time() * 1000)}",
            )
            logger.info("order_manager.emergency_market_close_successful: %s", symbol)
        except Exception as exc:
            logger.critical("order_manager.emergency_market_close_failed: %s (%s)", symbol, exc)

    def get_order(self, client_order_id: str) -> ManagedOrder | None:
        """Obtiene una orden gestionada por su clientOrderId."""
        return self._orders.get(client_order_id)

    def get_tracked_orders(self) -> dict[str, ManagedOrder]:
        """Retorna una copia superficial de las órdenes gestionadas."""
        return dict(self._orders)
