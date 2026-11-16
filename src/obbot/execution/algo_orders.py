"""Cliente especializado para Algo Orders (Stop-Loss y Take-Profit) en Binance USDⓈ-M Futures."""

from typing import Any

from obbot.common.logging import get_logger
from obbot.execution.binance_rest import BinanceFuturesRestClient

logger = get_logger("execution.algo_orders")


class BinanceAlgoOrderClient:
    """
    Cliente para órdenes condicionales de Binance USDⓈ-M (/fapi/v1/algoOrder).

    Obligatorio para STOP_MARKET, TAKE_PROFIT_MARKET y TRAILING_STOP_MARKET.
    El endpoint tradicional /fapi/v1/order rechaza estas órdenes con error -4120.
    """

    def __init__(self, rest_client: BinanceFuturesRestClient) -> None:
        self.rest_client = rest_client

    async def create_algo_order(
        self,
        symbol: str,
        side: str,
        algo_type: str,
        trigger_price: float,
        quantity: float | None = None,
        close_position: bool = False,
        reduce_only: bool = True,
        client_algo_id: str | None = None,
    ) -> dict[str, Any]:
        """Crea una orden condicional Algo Order."""
        params: dict[str, Any] = {
            "symbol": symbol.upper(),
            "side": side.upper(),
            "algoType": algo_type.upper(),
            "triggerPrice": trigger_price,
        }

        if close_position:
            params["closePosition"] = "true"
        elif quantity is not None:
            params["quantity"] = quantity
            if reduce_only:
                params["reduceOnly"] = "true"
        else:
            raise ValueError("Se requiere quantity o close_position=True.")

        if client_algo_id:
            params["clientAlgoId"] = client_algo_id

        res = await self.rest_client.request(
            "POST", "/fapi/v1/algoOrder", params=params, signed=True
        )
        logger.info(
            "algo_orders.created: symbol=%s side=%s type=%s trigger=%s id=%s",
            symbol,
            side,
            algo_type,
            trigger_price,
            res.get("algoId") or client_algo_id,
            extra={
                "symbol": symbol,
                "side": side,
                "algo_type": algo_type,
                "trigger_price": trigger_price,
                "algo_id": res.get("algoId"),
            },
        )
        return res

    async def create_stop_loss(
        self,
        symbol: str,
        side: str,
        trigger_price: float,
        quantity: float | None = None,
        close_position: bool = True,
        client_algo_id: str | None = None,
    ) -> dict[str, Any]:
        """Crea una orden STOP_MARKET para protección de posición."""
        return await self.create_algo_order(
            symbol=symbol,
            side=side,
            algo_type="STOP_MARKET",
            trigger_price=trigger_price,
            quantity=quantity,
            close_position=close_position,
            reduce_only=True,
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
        """Crea una orden TAKE_PROFIT_MARKET."""
        return await self.create_algo_order(
            symbol=symbol,
            side=side,
            algo_type="TAKE_PROFIT_MARKET",
            trigger_price=trigger_price,
            quantity=quantity,
            close_position=close_position,
            reduce_only=True,
            client_algo_id=client_algo_id,
        )

    async def cancel_algo_order(
        self,
        symbol: str,
        algo_id: int | None = None,
        client_algo_id: str | None = None,
    ) -> dict[str, Any]:
        """Cancela una Algo Order abierta (/fapi/v1/algoOrder)."""
        params: dict[str, Any] = {"symbol": symbol.upper()}
        if algo_id is not None:
            params["algoId"] = algo_id
        elif client_algo_id is not None:
            params["clientAlgoId"] = client_algo_id
        else:
            raise ValueError("Se requiere algo_id o client_algo_id.")

        return await self.rest_client.request(
            "DELETE", "/fapi/v1/algoOrder", params=params, signed=True
        )

    async def query_algo_order(
        self,
        symbol: str,
        algo_id: int | None = None,
        client_algo_id: str | None = None,
    ) -> dict[str, Any]:
        """Consulta el estado de una orden condicional (/fapi/v1/algoOrder)."""
        params: dict[str, Any] = {"symbol": symbol.upper()}
        if algo_id is not None:
            params["algoId"] = algo_id
        elif client_algo_id is not None:
            params["clientAlgoId"] = client_algo_id
        else:
            raise ValueError("Se requiere algo_id o client_algo_id.")

        return await self.rest_client.request(
            "GET", "/fapi/v1/algoOrder", params=params, signed=True
        )

    async def get_open_algo_orders(self, symbol: str | None = None) -> list[dict[str, Any]]:
        """
        Obtiene la lista de Algo Orders abiertas (/fapi/v1/openAlgoOrders).
        NOTA: /fapi/v1/openOrders NO devuelve órdenes condicionales pendientes.
        """
        params: dict[str, Any] = {}
        if symbol:
            params["symbol"] = symbol.upper()
        res = await self.rest_client.request(
            "GET", "/fapi/v1/openAlgoOrders", params=params, signed=True
        )
        return res if isinstance(res, list) else []
