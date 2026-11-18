"""Reconciliador periódico de órdenes y posiciones contra el exchange (gana el exchange)."""

import asyncio
from typing import Any

from obbot.common.clock import Clock, SystemClock
from obbot.common.logging import get_logger
from obbot.execution.algo_orders import BinanceAlgoOrderClient
from obbot.execution.binance_rest import BinanceFuturesRestClient
from obbot.execution.order_manager import OrderManager, OrderStatus
from obbot.risk.limits import PortfolioState, PositionState

logger = get_logger("execution.reconciler")


class StateReconciler:
    """
    Reconciliador de estado de órdenes y posiciones contra Binance Futures.

    Principio rector: "Gana el exchange".
    Cualquier discrepancia entre la memoria local y el estado reportado por los
    endpoints REST de Binance es corregida adoptando la verdad del exchange.
    """

    def __init__(
        self,
        rest_client: BinanceFuturesRestClient,
        algo_client: BinanceAlgoOrderClient,
        order_manager: OrderManager,
        clock: Clock | None = None,
    ) -> None:
        self.rest_client = rest_client
        self.algo_client = algo_client
        self.order_manager = order_manager
        self.clock = clock or SystemClock()

    async def reconcile_orders(self) -> dict[str, Any]:
        """
        Reconcilia órdenes regulares y condicionales abiertas.
        Devuelve un informe de discrepancias detectadas y corregidas.
        """
        remote_orders = await self.rest_client.get_open_orders()
        remote_algo_orders = await self.algo_client.get_open_algo_orders()

        remote_order_map: dict[str, dict[str, Any]] = {
            o["clientOrderId"]: o for o in remote_orders if "clientOrderId" in o
        }

        reconciled_count = 0
        discrepancies: list[str] = []

        tracked_orders = self.order_manager.get_tracked_orders()
        local_cids = set(tracked_orders.keys())

        for client_id, local_order in list(tracked_orders.items()):
            is_open = local_order.status in (OrderStatus.NEW, OrderStatus.PARTIALLY_FILLED)
            if is_open and client_id not in remote_order_map:
                # La orden ya no está abierta en Binance: consultar estado final
                try:
                    final_state = await self.rest_client.query_order(
                        symbol=local_order.symbol,
                        orig_client_order_id=client_id,
                    )
                    remote_status = final_state.get("status")
                    local_order.status = self.order_manager._map_binance_status(
                        remote_status or "FILLED"
                    )
                    local_order.filled_quantity = float(final_state.get("executedQty", 0.0))
                    local_order.avg_fill_price = float(final_state.get("avgPrice", 0.0))
                    local_order.updated_at_ns = self.clock.now_ns()
                    reconciled_count += 1
                    discrepancies.append(
                        f"Orden {client_id} actualizada a {local_order.status.name}"
                    )
                except Exception as exc:
                    discrepancies.append(f"No se pudo consultar orden {client_id}: {exc}")

        # Detección de órdenes huérfanas en Binance (no registradas localmente)
        orphan_count = 0
        for remote_order in remote_orders:
            cid = str(remote_order.get("clientOrderId", ""))
            if cid and cid not in local_cids:
                orphan_count += 1
                logger.warning(
                    "reconciler.orphan_order_detected: orderId=%s clientOrderId=%s symbol=%s qty=%s",
                    remote_order.get("orderId"),
                    cid,
                    remote_order.get("symbol"),
                    remote_order.get("origQty"),
                )
                discrepancies.append(f"Orden huérfana en exchange detectada: {cid}")

        logger.info(
            "reconciler.orders_checked: open_regular=%d open_algo=%d reconciled=%d",
            len(remote_orders),
            len(remote_algo_orders),
            reconciled_count,
        )

        return {
            "remote_open_regular": len(remote_orders),
            "remote_open_algo": len(remote_algo_orders),
            "reconciled": reconciled_count,
            "discrepancies": discrepancies,
        }

    async def reconcile_positions(
        self,
        portfolio: PortfolioState,
    ) -> list[PositionState]:
        """
        Reconcilia las posiciones abiertas en el exchange contra el PortfolioState local.
        Si existe una posición abierta sin un Algo Stop-Loss activo, genera una advertencia crítica.
        """
        remote_positions = await self.rest_client.get_positions()
        try:
            remote_algo_orders_raw = await self.algo_client.get_open_algo_orders()
            remote_algo_orders = (
                remote_algo_orders_raw if isinstance(remote_algo_orders_raw, list) else []
            )
        except Exception as exc:
            logger.warning("reconciler.fetch_algo_orders_failed: %s", exc)
            remote_algo_orders = []

        updated_positions: list[PositionState] = []

        for p in remote_positions:
            size = float(p.get("positionAmt", 0.0))
            if size == 0.0:
                continue

            symbol = p["symbol"]
            entry_price = float(p.get("entryPrice", 0.0))
            mark_price = float(p.get("markPrice", 0.0))
            unrealized_pnl = float(p.get("unRealizedProfit", 0.0))
            leverage = int(p.get("leverage", 1))
            margin_type = p.get("marginType", "isolated").upper()

            pos = PositionState(
                symbol=symbol,
                size=size,
                entry_price=entry_price,
                mark_price=mark_price,
                unrealized_pnl=unrealized_pnl,
                leverage=leverage,
                margin_type=margin_type,
            )
            updated_positions.append(pos)
            if portfolio.positions is not None:
                portfolio.positions[symbol] = pos

            # Auditoría: Comprobar que la posición cuente con Algo Stop-Loss activo
            expected_sl_side = "SELL" if size > 0 else "BUY"
            has_algo_sl = any(
                str(algo_o.get("symbol", "")).upper() == symbol.upper()
                and str(algo_o.get("side", "")).upper() == expected_sl_side
                and str(algo_o.get("algoType", algo_o.get("orderType", ""))).upper()
                in ("STOP_MARKET", "STOP")
                for algo_o in remote_algo_orders
            )
            if not has_algo_sl:
                logger.critical(
                    "reconciler.unprotected_position_detected: %s size=%.4f sin Algo Stop-Loss activo!",
                    symbol,
                    size,
                )

        # Actualizar posiciones en portfolio
        if portfolio.positions is not None:
            active_symbols = {p.symbol for p in updated_positions}
            # Limpiar posiciones que ya estén cerradas en el exchange
            closed_symbols = [s for s in portfolio.positions if s not in active_symbols]
            for s in closed_symbols:
                del portfolio.positions[s]

        logger.info(
            "reconciler.positions_synced: active_positions=%d",
            len(updated_positions),
        )
        return updated_positions

    async def run_periodic_loop(
        self,
        portfolio: PortfolioState,
        interval_sec: float = 15.0,
    ) -> None:
        """Bucle en segundo plano de reconciliación periódica."""
        while True:
            try:
                await asyncio.sleep(interval_sec)
                await self.reconcile_orders()
                await self.reconcile_positions(portfolio)
            except asyncio.CancelledError:
                break
            except Exception as exc:
                logger.error("reconciler.periodic_loop_error: %s", exc)
