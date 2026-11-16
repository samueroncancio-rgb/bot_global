"""Motor de ejecución simulada para paper trading determinista de Binance USDⓈ-M."""

from dataclasses import dataclass
from typing import Any

from obbot.common.clock import Clock, SystemClock
from obbot.common.logging import get_logger

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
    ) -> None:
        self.wallet_balance = initial_balance
        self.initial_balance = initial_balance
        self.clock = clock or SystemClock()

        self._orders: dict[str, PaperOrder] = {}  # client_order_id -> PaperOrder
        self._algo_orders: dict[str, PaperAlgoOrder] = {}  # client_algo_id -> PaperAlgoOrder
        self._positions: dict[str, dict[str, Any]] = {}  # symbol -> position dict
        self._order_id_counter = 1000
        self._algo_id_counter = 5000
        self._latest_mark_prices: dict[str, float] = {}

    def set_mark_price(self, symbol: str, price: float) -> None:
        """Actualiza el mark price y evalúa si se disparan órdenes condicionales."""
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

    def _check_algo_triggers(self, symbol: str, current_price: float) -> None:
        """Evalúa si el precio actual activa alguna Algo Order (Stop-Loss)."""
        for _client_id, algo in list(self._algo_orders.items()):
            if algo.symbol != symbol or algo.status != "NEW":
                continue

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
                )
                logger.info(
                    "paper.algo_triggered: %s %s trigger=%.2f fill=%.2f",
                    algo.client_algo_id,
                    algo.side,
                    algo.trigger_price,
                    current_price,
                )

    def _execute_fill(
        self,
        symbol: str,
        side: str,
        quantity: float,
        fill_price: float,
        reduce_only: bool,
    ) -> None:
        """Aplica la ejecución de un trade actualizando balance y posición."""
        pos = self._positions.setdefault(
            symbol,
            {
                "symbol": symbol,
                "positionAmt": 0.0,
                "entryPrice": 0.0,
                "markPrice": fill_price,
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
            pos["entryPrice"] = fill_price
        elif (current_amt > 0 and trade_amt > 0) or (current_amt < 0 and trade_amt < 0):
            # Aumento de posición: precio medio ponderado
            total_qty = abs(current_amt) + abs(trade_amt)
            pos["entryPrice"] = (
                abs(current_amt) * current_entry + abs(trade_amt) * fill_price
            ) / total_qty
            pos["positionAmt"] = new_amt
        else:
            # Cierre parcial o total: calcular PnL realizado
            closing_qty = min(abs(current_amt), abs(trade_amt))
            pnl_direction = 1.0 if current_amt > 0 else -1.0
            realized_pnl = closing_qty * (fill_price - current_entry) * pnl_direction
            self.wallet_balance += realized_pnl

            if abs(new_amt) < 1e-8:
                pos["positionAmt"] = 0.0
                pos["entryPrice"] = 0.0
            else:
                pos["positionAmt"] = new_amt
                if (current_amt > 0 and new_amt < 0) or (current_amt < 0 and new_amt > 0):
                    pos["entryPrice"] = fill_price

        pos["markPrice"] = fill_price
        if pos["positionAmt"] != 0.0:
            direction = 1.0 if pos["positionAmt"] > 0 else -1.0
            pos["unRealizedProfit"] = (
                abs(pos["positionAmt"]) * (fill_price - pos["entryPrice"]) * direction
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
            self._execute_fill(symbol, side, quantity, mark_px, reduce_only)

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
            self._execute_fill(order.symbol, order.side, order.quantity, px, order.reduce_only)

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
