"""Motor de ejecución simulada para paper trading determinista de Binance USDⓈ-M."""

import asyncio
from collections.abc import Callable, Coroutine
from dataclasses import dataclass
from typing import Any

from obbot.common.clock import Clock, SystemClock
from obbot.common.logging import get_logger
from obbot.execution.algo_orders import BinanceAlgoOrderClient
from obbot.execution.binance_rest import BinanceFuturesRestClient

logger = get_logger("execution.paper")


@dataclass
class PaperOrder:
    order_id: int
    client_order_id: str
    symbol: str
    side: str
    order_type: str
    quantity: float
    price: float
    reduce_only: bool
    status: str = "NEW"
    filled_qty: float = 0.0
    avg_price: float = 0.0


@dataclass
class PaperAlgoOrder:
    algo_id: int
    client_algo_id: str
    symbol: str
    side: str
    algo_type: str
    trigger_price: float
    quantity: float | None
    close_position: bool
    status: str = "NEW"


class PaperExecutionEngine:
    """
    Simulador de ejecución de Binance USDⓈ-M Futures para paper trading.

    Permite probar estrategias, RiskGuard y OrderManager sin requerir claves API
    ni conectividad de red, reproduciendo la semántica exacta del exchange.
    """

    def __init__(
        self,
        initial_balance: float = 10000.0,
        clock: Clock | None = None,
        maker_fee_rate: float = 0.0,
        taker_fee_rate: float = 0.0,
        slippage_bps: float = 0.0,
        on_fill_callback: Callable[[str, float, float, str], Coroutine[Any, Any, None]]
        | None = None,
    ) -> None:
        self.wallet_balance = initial_balance
        self.initial_balance = initial_balance
        self.clock = clock or SystemClock()
        self.maker_fee_rate = maker_fee_rate
        self.taker_fee_rate = taker_fee_rate
        self.slippage_bps = slippage_bps
        self.on_fill_callback = on_fill_callback

        self._orders: dict[str, PaperOrder] = {}  # client_order_id -> PaperOrder
        self._algo_orders: dict[str, PaperAlgoOrder] = {}  # client_algo_id -> PaperAlgoOrder
        self._positions: dict[str, dict[str, Any]] = {}  # symbol -> position dict
        self._order_id_counter = 1000
        self._algo_id_counter = 5000
        self._latest_mark_prices: dict[str, float] = {}
        self._active_tasks: set[asyncio.Task[Any]] = set()

    def _notify_fill(self, client_order_id: str, quantity: float, price: float, side: str) -> None:
        """Notifica el fill a listeners asíncronos si están registrados."""
        if self.on_fill_callback:
            try:
                loop = asyncio.get_running_loop()
                coro = self.on_fill_callback(client_order_id, quantity, price, side)
                if asyncio.iscoroutine(coro):
                    task = loop.create_task(coro)
                    self._active_tasks.add(task)
                    task.add_done_callback(self._active_tasks.discard)
            except RuntimeError:
                pass

    def set_mark_price(self, symbol: str, price: float) -> None:
        """Actualiza el mark price y evalúa si se disparan órdenes condicionales o límites."""
        self._latest_mark_prices[symbol] = price
        if symbol in self._positions:
            pos = self._positions[symbol]
            pos["markPrice"] = price
            if pos["positionAmt"] != 0.0:
                direction = 1.0 if pos["positionAmt"] > 0 else -1.0
                pos["unRealizedProfit"] = (
                    abs(pos["positionAmt"]) * (price - pos["entryPrice"]) * direction
                )
            else:
                pos["unRealizedProfit"] = 0.0
        self._check_algo_triggers(symbol, price)
        self._check_limit_triggers(symbol, price)

    def _check_limit_triggers(self, symbol: str, current_price: float) -> None:
        """Evalúa si el precio actual ejecuta alguna orden límite abierta."""
        for client_id, order in list(self._orders.items()):
            if order.symbol != symbol or order.status not in ("NEW", "PARTIALLY_FILLED"):
                continue

            triggered = (order.side == "BUY" and current_price <= order.price) or (
                order.side == "SELL" and current_price >= order.price
            )

            if triggered:
                self.fill_order(client_id, fill_price=order.price)

    def _check_algo_triggers(self, symbol: str, current_price: float) -> None:
        """Evalúa si el precio actual activa alguna Algo Order (Stop-Loss o Take-Profit)."""
        for _client_id, algo in list(self._algo_orders.items()):
            if algo.symbol != symbol or algo.status != "NEW":
                continue

            is_tp = "TAKE_PROFIT" in algo.algo_type.upper()
            if is_tp:
                # Para TAKE_PROFIT:
                # - SELL (cierra LONG): se activa cuando el precio SUBE al trigger (price >= trigger)
                # - BUY (cierra SHORT): se activa cuando el precio CAE al trigger (price <= trigger)
                triggered = (algo.side == "SELL" and current_price >= algo.trigger_price) or (
                    algo.side == "BUY" and current_price <= algo.trigger_price
                )
            else:
                # Para STOP_MARKET:
                # - SELL (cierra LONG): se activa cuando el precio CAE al trigger (price <= trigger)
                # - BUY (cierra SHORT): se activa cuando el precio SUBE al trigger (price >= trigger)
                triggered = (algo.side == "SELL" and current_price <= algo.trigger_price) or (
                    algo.side == "BUY" and current_price >= algo.trigger_price
                )

            if triggered:
                algo.status = "TRIGGERED"
                pos = self._positions.get(symbol, {})
                qty = (
                    abs(pos.get("positionAmt", 0.0))
                    if algo.close_position
                    else (algo.quantity or 0.0)
                )

                # Ejecutar cierre inmediato de mercado
                self._execute_fill(
                    symbol=symbol,
                    side=algo.side,
                    quantity=qty,
                    fill_price=current_price,
                    reduce_only=True,
                    is_taker=True,
                )
                self._notify_fill(algo.client_algo_id, qty, current_price, algo.side)
                logger.info(
                    "paper.algo_triggered: %s %s %s trigger=%.2f fill=%.2f",
                    algo.algo_type,
                    algo.client_algo_id,
                    algo.side,
                    algo.trigger_price,
                    current_price,
                )

                # Cancelación cruzada (OCO): si la posición se cerró a 0, cancelar la orden hermana
                remaining_pos = self._positions.get(symbol, {})
                if abs(remaining_pos.get("positionAmt", 0.0)) <= 1e-6:
                    for sibling_id, sibling in list(self._algo_orders.items()):
                        if sibling.symbol == symbol and sibling.status == "NEW":
                            sibling.status = "CANCELLED"
                            logger.info("paper.oco_cancelled_sibling: %s", sibling_id)

    def _execute_fill(
        self,
        symbol: str,
        side: str,
        quantity: float,
        fill_price: float,
        reduce_only: bool,
        is_taker: bool = True,
    ) -> None:
        """Aplica la ejecución de un trade actualizando balance y posición con comisiones y slippage."""
        effective_price = fill_price
        if is_taker and self.slippage_bps > 0.0:
            slippage_factor = self.slippage_bps / 10000.0
            if side == "BUY":
                effective_price = fill_price * (1.0 + slippage_factor)
            else:
                effective_price = fill_price * (1.0 - slippage_factor)

        pos = self._positions.setdefault(
            symbol,
            {
                "symbol": symbol,
                "positionAmt": 0.0,
                "entryPrice": 0.0,
                "markPrice": effective_price,
                "unRealizedProfit": 0.0,
                "leverage": 3,
                "marginType": "ISOLATED",
            },
        )
        current_amt = pos["positionAmt"]
        current_entry = pos["entryPrice"]

        trade_amt = quantity if side == "BUY" else -quantity
        new_amt = current_amt + trade_amt

        if current_amt == 0.0:
            # Apertura limpia
            pos["positionAmt"] = new_amt
            pos["entryPrice"] = effective_price
        elif (current_amt > 0 and trade_amt > 0) or (current_amt < 0 and trade_amt < 0):
            # Aumento de posición: precio medio ponderado
            total_qty = abs(current_amt) + abs(trade_amt)
            pos["entryPrice"] = (
                abs(current_amt) * current_entry + abs(trade_amt) * effective_price
            ) / total_qty
            pos["positionAmt"] = new_amt
        else:
            # Cierre parcial o total: calcular PnL realizado
            closing_qty = min(abs(current_amt), abs(trade_amt))
            pnl_direction = 1.0 if current_amt > 0 else -1.0
            realized_pnl = closing_qty * (effective_price - current_entry) * pnl_direction
            self.wallet_balance += realized_pnl

            if abs(new_amt) < 1e-8:
                pos["positionAmt"] = 0.0
                pos["entryPrice"] = 0.0
            else:
                pos["positionAmt"] = new_amt
                if (current_amt > 0 and new_amt < 0) or (current_amt < 0 and new_amt > 0):
                    pos["entryPrice"] = effective_price

        # Deducción de comisiones si aplican
        fee_rate = self.taker_fee_rate if is_taker else self.maker_fee_rate
        if fee_rate > 0.0:
            fee_usd = quantity * effective_price * fee_rate
            self.wallet_balance -= fee_usd

        pos["markPrice"] = effective_price
        if pos["positionAmt"] != 0.0:
            direction = 1.0 if pos["positionAmt"] > 0 else -1.0
            pos["unRealizedProfit"] = (
                abs(pos["positionAmt"]) * (effective_price - pos["entryPrice"]) * direction
            )
        else:
            pos["unRealizedProfit"] = 0.0

    async def create_order(
        self,
        symbol: str,
        side: str,
        order_type: str,
        quantity: float,
        price: float | None = None,
        time_in_force: str | None = None,
        reduce_only: bool = False,
        client_order_id: str | None = None,
    ) -> dict[str, Any]:
        """Crea una orden simulada en el paper engine."""
        self._order_id_counter += 1
        cid = client_order_id or f"PAPER_{self._order_id_counter}"
        mark_px = self._latest_mark_prices.get(symbol, price or 100.0)

        paper_order = PaperOrder(
            order_id=self._order_id_counter,
            client_order_id=cid,
            symbol=symbol,
            side=side,
            order_type=order_type,
            quantity=quantity,
            price=price or mark_px,
            reduce_only=reduce_only,
        )
        self._orders[cid] = paper_order

        # Órdenes MARKET se ejecutan inmediatamente
        if order_type == "MARKET":
            paper_order.status = "FILLED"
            paper_order.filled_qty = quantity
            paper_order.avg_price = mark_px
            self._execute_fill(symbol, side, quantity, mark_px, reduce_only, is_taker=True)
            self._notify_fill(cid, quantity, mark_px, side)

        return {
            "orderId": paper_order.order_id,
            "clientOrderId": paper_order.client_order_id,
            "symbol": symbol,
            "status": paper_order.status,
            "executedQty": str(paper_order.filled_qty),
            "avgPrice": str(paper_order.avg_price),
        }

    def fill_order(self, client_order_id: str, fill_price: float | None = None) -> None:
        """Simula la ejecución completa de una orden en el paper engine."""
        order = self._orders.get(client_order_id)
        if order and order.status in ("NEW", "PARTIALLY_FILLED"):
            px = fill_price or order.price or self._latest_mark_prices.get(order.symbol, 100.0)
            order.status = "FILLED"
            order.filled_qty = order.quantity
            order.avg_price = px
            is_taker = order.order_type == "MARKET"
            self._execute_fill(
                order.symbol, order.side, order.quantity, px, order.reduce_only, is_taker=is_taker
            )
            self._notify_fill(order.client_order_id, order.quantity, px, order.side)

    async def cancel_order(
        self,
        symbol: str,
        order_id: int | None = None,
        orig_client_order_id: str | None = None,
    ) -> dict[str, Any]:
        """Cancela una orden regular simulada."""
        order = None
        if orig_client_order_id and orig_client_order_id in self._orders:
            order = self._orders[orig_client_order_id]
        elif order_id is not None:
            for o in self._orders.values():
                if o.order_id == order_id:
                    order = o
                    break

        if order and order.status in ("NEW", "PARTIALLY_FILLED"):
            order.status = "CANCELED"
            return {
                "orderId": order.order_id,
                "clientOrderId": order.client_order_id,
                "symbol": order.symbol,
                "status": "CANCELED",
            }
        return {"code": -2011, "msg": "Unknown order sent"}

    async def query_order(
        self,
        symbol: str,
        order_id: int | None = None,
        orig_client_order_id: str | None = None,
    ) -> dict[str, Any]:
        """Consulta el estado de una orden simulada."""
        order = None
        if orig_client_order_id and orig_client_order_id in self._orders:
            order = self._orders[orig_client_order_id]
        elif order_id is not None:
            for o in self._orders.values():
                if o.order_id == order_id:
                    order = o
                    break

        if not order:
            return {"status": "NOT_FOUND", "executedQty": "0.0", "avgPrice": "0.0"}

        return {
            "orderId": order.order_id,
            "clientOrderId": order.client_order_id,
            "symbol": order.symbol,
            "status": order.status,
            "executedQty": str(order.filled_qty),
            "avgPrice": str(order.avg_price),
        }

    async def create_algo_order(
        self,
        symbol: str,
        side: str,
        algo_type: str,
        trigger_price: float,
        quantity: float | None = None,
        close_position: bool = False,
        client_algo_id: str | None = None,
    ) -> dict[str, Any]:
        """Crea una orden condicional simulada (Stop-Loss)."""
        self._algo_id_counter += 1
        cid = client_algo_id or f"PAPER_ALGO_{self._algo_id_counter}"

        algo = PaperAlgoOrder(
            algo_id=self._algo_id_counter,
            client_algo_id=cid,
            symbol=symbol,
            side=side,
            algo_type=algo_type,
            trigger_price=trigger_price,
            quantity=quantity,
            close_position=close_position,
        )
        self._algo_orders[cid] = algo

        return {
            "algoId": algo.algo_id,
            "clientAlgoId": algo.client_algo_id,
            "symbol": symbol,
            "status": "NEW",
        }

    async def create_stop_loss(
        self,
        symbol: str,
        side: str,
        trigger_price: float,
        quantity: float | None = None,
        close_position: bool = True,
        client_algo_id: str | None = None,
    ) -> dict[str, Any]:
        """Crea una orden STOP_MARKET simulada."""
        return await self.create_algo_order(
            symbol=symbol,
            side=side,
            algo_type="STOP_MARKET",
            trigger_price=trigger_price,
            quantity=quantity,
            close_position=close_position,
            client_algo_id=client_algo_id,
        )

    async def create_take_profit(
        self,
        symbol: str,
        side: str,
        trigger_price: float,
        quantity: float | None = None,
        close_position: bool = True,
        client_algo_id: str | None = None,
    ) -> dict[str, Any]:
        """Crea una orden TAKE_PROFIT_MARKET simulada."""
        return await self.create_algo_order(
            symbol=symbol,
            side=side,
            algo_type="TAKE_PROFIT_MARKET",
            trigger_price=trigger_price,
            quantity=quantity,
            close_position=close_position,
            client_algo_id=client_algo_id,
        )

    async def cancel_algo_order(
        self,
        symbol: str,
        algo_id: int | None = None,
        client_algo_id: str | None = None,
    ) -> dict[str, Any]:
        """Cancela una Algo Order simulada."""
        algo = None
        if client_algo_id and client_algo_id in self._algo_orders:
            algo = self._algo_orders[client_algo_id]
        elif algo_id is not None:
            for a in self._algo_orders.values():
                if a.algo_id == algo_id:
                    algo = a
                    break

        if algo:
            algo.status = "CANCELLED"
            return {"code": 200, "msg": "SUCCESS"}
        return {"code": -2011, "msg": "Unknown algo order"}

    async def query_algo_order(
        self,
        symbol: str,
        algo_id: int | None = None,
        client_algo_id: str | None = None,
    ) -> dict[str, Any]:
        """Consulta el estado de una Algo Order simulada."""
        algo = None
        if client_algo_id and client_algo_id in self._algo_orders:
            algo = self._algo_orders[client_algo_id]
        elif algo_id is not None:
            for a in self._algo_orders.values():
                if a.algo_id == algo_id:
                    algo = a
                    break

        if not algo:
            return {"status": "NOT_FOUND"}

        return {
            "algoId": algo.algo_id,
            "clientAlgoId": algo.client_algo_id,
            "symbol": algo.symbol,
            "status": algo.status,
            "triggerPrice": str(algo.trigger_price),
        }

    async def get_positions(self, symbol: str | None = None) -> list[dict[str, Any]]:
        """Devuelve las posiciones simuladas."""
        if symbol:
            p = self._positions.get(symbol)
            return [p] if p else []
        return list(self._positions.values())

    async def get_open_orders(self, symbol: str | None = None) -> list[dict[str, Any]]:
        """Devuelve las órdenes regulares abiertas simuladas."""
        return [
            {
                "orderId": o.order_id,
                "clientOrderId": o.client_order_id,
                "symbol": o.symbol,
                "status": o.status,
            }
            for o in self._orders.values()
            if o.status in ("NEW", "PARTIALLY_FILLED") and (not symbol or o.symbol == symbol)
        ]

    async def get_open_algo_orders(self, symbol: str | None = None) -> list[dict[str, Any]]:
        """Devuelve las Algo Orders abiertas simuladas."""
        return [
            {
                "algoId": a.algo_id,
                "clientAlgoId": a.client_algo_id,
                "symbol": a.symbol,
                "status": a.status,
                "triggerPrice": str(a.trigger_price),
            }
            for a in self._algo_orders.values()
            if a.status == "NEW" and (not symbol or a.symbol == symbol)
        ]


class PaperRestClient(BinanceFuturesRestClient):
    """Adaptador de BinanceFuturesRestClient que enruta hacia PaperExecutionEngine."""

    def __init__(self, engine: PaperExecutionEngine) -> None:
        super().__init__(api_key="paper", api_secret="paper", testnet=True)
        self.engine = engine

    async def sync_server_time(self) -> int:
        return 0

    async def create_order(
        self,
        symbol: str,
        side: str,
        order_type: str,
        quantity: float,
        price: float | None = None,
        time_in_force: str | None = None,
        reduce_only: bool = False,
        client_order_id: str | None = None,
    ) -> dict[str, Any]:
        return await self.engine.create_order(
            symbol=symbol,
            side=side,
            order_type=order_type,
            quantity=quantity,
            price=price,
            time_in_force=time_in_force,
            reduce_only=reduce_only,
            client_order_id=client_order_id,
        )

    async def query_order(
        self,
        symbol: str,
        order_id: int | None = None,
        orig_client_order_id: str | None = None,
    ) -> dict[str, Any]:
        return await self.engine.query_order(
            symbol=symbol,
            orig_client_order_id=orig_client_order_id,
            order_id=order_id,
        )

    async def cancel_order(
        self,
        symbol: str,
        order_id: int | None = None,
        orig_client_order_id: str | None = None,
    ) -> dict[str, Any]:
        return await self.engine.cancel_order(
            symbol=symbol,
            order_id=order_id,
            orig_client_order_id=orig_client_order_id,
        )

    async def get_open_orders(self, symbol: str | None = None) -> list[dict[str, Any]]:
        return await self.engine.get_open_orders(symbol=symbol)

    async def get_positions(self, symbol: str | None = None) -> list[dict[str, Any]]:
        return await self.engine.get_positions(symbol=symbol)

    async def get_klines(
        self,
        symbol: str,
        interval: str = "15m",
        limit: int = 50,
    ) -> list[list[Any]]:
        px = self.engine._latest_mark_prices.get(symbol, 1800.0)
        now_ms = self.engine.clock.now_ns() // 1_000_000
        step_ms = 15 * 60 * 1000
        return [
            [
                now_ms - (limit - i) * step_ms,
                str(px),
                str(px * 1.001),
                str(px * 0.999),
                str(px),
                "100.0",
                now_ms - (limit - i - 1) * step_ms,
                "100000.0",
                100,
                "50.0",
                "50000.0",
                "0",
            ]
            for i in range(limit)
        ]

    async def close(self) -> None:
        pass


class PaperAlgoClient(BinanceAlgoOrderClient):
    """Adaptador de BinanceAlgoOrderClient que enruta hacia PaperExecutionEngine."""

    def __init__(self, engine: PaperExecutionEngine, rest_client: BinanceFuturesRestClient) -> None:
        super().__init__(rest_client=rest_client)
        self.engine = engine

    async def create_stop_loss(
        self,
        symbol: str,
        side: str,
        trigger_price: float,
        quantity: float | None = None,
        close_position: bool = True,
        client_algo_id: str | None = None,
    ) -> dict[str, Any]:
        return await self.engine.create_stop_loss(
            symbol=symbol,
            side=side,
            trigger_price=trigger_price,
            quantity=quantity,
            close_position=close_position,
            client_algo_id=client_algo_id,
        )

    async def create_take_profit(
        self,
        symbol: str,
        side: str,
        trigger_price: float,
        quantity: float | None = None,
        close_position: bool = True,
        client_algo_id: str | None = None,
    ) -> dict[str, Any]:
        return await self.engine.create_take_profit(
            symbol=symbol,
            side=side,
            trigger_price=trigger_price,
            quantity=quantity,
            close_position=close_position,
            client_algo_id=client_algo_id,
        )

    async def cancel_algo_order(
        self,
        symbol: str,
        algo_id: int | None = None,
        client_algo_id: str | None = None,
    ) -> dict[str, Any]:
        return await self.engine.cancel_algo_order(
            symbol=symbol,
            algo_id=algo_id,
            client_algo_id=client_algo_id,
        )

    async def query_algo_order(
        self,
        symbol: str,
        algo_id: int | None = None,
        client_algo_id: str | None = None,
    ) -> dict[str, Any]:
        return await self.engine.query_algo_order(
            symbol=symbol,
            algo_id=algo_id,
            client_algo_id=client_algo_id,
        )

    async def get_open_algo_orders(self, symbol: str | None = None) -> list[dict[str, Any]]:
        return await self.engine.get_open_algo_orders(symbol=symbol)
