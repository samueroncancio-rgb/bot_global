"""Cliente REST institucional firmado para Binance USDⓈ-M Futures."""

import hashlib
import hmac
import time
import urllib.parse
from typing import Any

import aiohttp

from obbot.common.logging import get_logger

logger = get_logger("execution.binance_rest")


class BinanceRestError(Exception):
    """Excepción base para errores de la API REST de Binance."""

    def __init__(self, code: int, message: str, http_status: int = 400) -> None:
        self.code = code
        self.message = message
        self.http_status = http_status
        super().__init__(f"Binance API Error [{code}]: {message} (HTTP {http_status})")


class BinanceFuturesRestClient:
    """
    Cliente REST asíncrono para Binance USDⓈ-M Futures.

    Características de seguridad institucional:
    1. Firma criptográfica HMAC-SHA256 en parámetros de consulta y payload.
    2. Sincronización continua de deriva de reloj con el servidor (/fapi/v1/time).
    3. Bloqueo estricto de rutas de retiro y transferencia.
    4. Seguimiento de límites de peso de solicitudes (x-mbx-used-weight-1m).
    """

    FORBIDDEN_KEYWORDS = ("withdraw", "transfer", "sub-account", "asset")

    def __init__(
        self,
        api_key: str,
        api_secret: str,
        testnet: bool = False,
        session: aiohttp.ClientSession | None = None,
    ) -> None:
        self.api_key = api_key
        self.api_secret = api_secret.encode("utf-8")
        self.testnet = testnet
        self.base_url = (
            "https://testnet.binancefuture.com" if testnet else "https://fapi.binance.com"
        )
        self._session = session
        self._owns_session = session is None
        self._clock_offset_ms: int = 0
        self._last_used_weight_1m: int = 0

    async def _get_session(self) -> aiohttp.ClientSession:
        if self._session is None or self._session.closed:
            self._session = aiohttp.ClientSession()
            self._owns_session = True
        return self._session

    async def close(self) -> None:
        """Cierra la sesión HTTP si fue creada internamente."""
        if self._owns_session and self._session and not self._session.closed:
            await self._session.close()

    async def sync_server_time(self) -> int:
        """Sincroniza la deriva de reloj calculando: offset = server_time - client_time."""
        client_before = int(time.time() * 1000)
        data = await self.request("GET", "/fapi/v1/time", signed=False)
        client_after = int(time.time() * 1000)
        server_time = int(data["serverTime"])

        # Mediana del RTT para compensar el retardo de tránsito
        rtt_estimate = (client_after - client_before) // 2
        self._clock_offset_ms = server_time - (client_before + rtt_estimate)

        logger.info(
            "binance_rest.clock_synced: offset_ms=%d",
            self._clock_offset_ms,
            extra={"offset_ms": self._clock_offset_ms},
        )
        return self._clock_offset_ms

    def get_synced_timestamp_ms(self) -> int:
        """Retorna el timestamp actual sincronizado con el servidor de Binance."""
        return int(time.time() * 1000) + self._clock_offset_ms

    def _sign_query(self, params: dict[str, Any]) -> str:
        """Genera la firma HMAC-SHA256 para el query string."""
        query_string = urllib.parse.urlencode(params)
        signature = hmac.new(
            self.api_secret, query_string.encode("utf-8"), hashlib.sha256
        ).hexdigest()
        return f"{query_string}&signature={signature}"

    async def request(
        self,
        method: str,
        path: str,
        params: dict[str, Any] | None = None,
        signed: bool = False,
    ) -> dict[str, Any]:
        """Ejecuta una petición HTTP contra los endpoints de Binance Futures."""
        # 1. Verificación de seguridad ineludible
        path_lower = path.lower()
        if any(forbidden in path_lower for forbidden in self.FORBIDDEN_KEYWORDS):
            raise PermissionError(
                f"Acceso denegado: '{path}' viola la política de no retiro ni transferencias."
            )

        session = await self._get_session()
        headers: dict[str, str] = {}
        request_params = dict(params) if params else {}

        if self.api_key:
            headers["X-MBX-APIKEY"] = self.api_key

        if signed:
            request_params["timestamp"] = self.get_synced_timestamp_ms()
            request_params["recvWindow"] = 5000
            query_str = self._sign_query(request_params)
            url = f"{self.base_url}{path}?{query_str}"
            body_params = None
        else:
            url = f"{self.base_url}{path}"
            body_params = request_params if request_params else None

        async with session.request(
            method,
            url,
            params=body_params if not signed else None,
            headers=headers,
        ) as response:
            # Seguimiento de peso de la API
            if "x-mbx-used-weight-1m" in response.headers:
                try:
                    self._last_used_weight_1m = int(response.headers["x-mbx-used-weight-1m"])
                    if self._last_used_weight_1m > 2000:
                        logger.warning(
                            "binance_rest.high_weight_used: %d / 2400",
                            self._last_used_weight_1m,
                            extra={"used_weight": self._last_used_weight_1m},
                        )
                except (ValueError, TypeError):
                    pass

            response_json = await response.json()

            if response.status >= 400:
                code = response_json.get("code", -1)
                msg = response_json.get("msg", "Error desconocido")
                raise BinanceRestError(code=code, message=msg, http_status=response.status)

            return response_json  # type: ignore[no-any-return]

    # --- Endpoints de Órdenes Regulares (Maker / Taker) ---

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
        """
        Crea una orden regular en Binance Futures (/fapi/v1/order).
        NOTA: Órdenes condicionales (STOP*, TAKE_PROFIT*) deben usar BinanceAlgoOrderClient.
        """
        if order_type in ("STOP_MARKET", "TAKE_PROFIT_MARKET", "TRAILING_STOP_MARKET"):
            raise ValueError(
                f"La orden condicional {order_type} debe enviarse a través del Algo Order client."
            )

        params: dict[str, Any] = {
            "symbol": symbol.upper(),
            "side": side.upper(),
            "type": order_type.upper(),
            "quantity": quantity,
        }

        if price is not None and order_type != "MARKET":
            params["price"] = price

        if time_in_force is not None:
            params["timeInForce"] = time_in_force
        elif order_type == "LIMIT":
            params["timeInForce"] = "GTC"

        if reduce_only:
            params["reduceOnly"] = "true"

        if client_order_id:
            params["newClientOrderId"] = client_order_id

        return await self.request("POST", "/fapi/v1/order", params=params, signed=True)

    async def cancel_order(
        self,
        symbol: str,
        order_id: int | None = None,
        orig_client_order_id: str | None = None,
    ) -> dict[str, Any]:
        """Cancela una orden abierta (/fapi/v1/order)."""
        params: dict[str, Any] = {"symbol": symbol.upper()}
        if order_id is not None:
            params["orderId"] = order_id
        elif orig_client_order_id is not None:
            params["origClientOrderId"] = orig_client_order_id
        else:
            raise ValueError("Se requiere order_id u orig_client_order_id.")

        return await self.request("DELETE", "/fapi/v1/order", params=params, signed=True)

    async def query_order(
        self,
        symbol: str,
        order_id: int | None = None,
        orig_client_order_id: str | None = None,
    ) -> dict[str, Any]:
        """Consulta el estado de una orden regular (/fapi/v1/order)."""
        params: dict[str, Any] = {"symbol": symbol.upper()}
        if order_id is not None:
            params["orderId"] = order_id
        elif orig_client_order_id is not None:
            params["origClientOrderId"] = orig_client_order_id
        else:
            raise ValueError("Se requiere order_id u orig_client_order_id.")

        return await self.request("GET", "/fapi/v1/order", params=params, signed=True)

    async def get_open_orders(self, symbol: str | None = None) -> list[dict[str, Any]]:
        """Obtiene la lista de órdenes regulares abiertas (/fapi/v1/openOrders)."""
        params: dict[str, Any] = {}
        if symbol:
            params["symbol"] = symbol.upper()
        res = await self.request("GET", "/fapi/v1/openOrders", params=params, signed=True)
        return res if isinstance(res, list) else []

    async def get_positions(self, symbol: str | None = None) -> list[dict[str, Any]]:
        """Obtiene el riesgo de posiciones abiertas (/fapi/v1/positionRisk)."""
        params: dict[str, Any] = {}
        if symbol:
            params["symbol"] = symbol.upper()
        res = await self.request("GET", "/fapi/v1/positionRisk", params=params, signed=True)
        return res if isinstance(res, list) else []

    async def get_account(self) -> dict[str, Any]:
        """Obtiene la información de cuenta y balance (/fapi/v2/account)."""
        return await self.request("GET", "/fapi/v2/account", signed=True)

    # --- Gestión del User Data Stream ListenKey ---

    async def create_listen_key(self) -> str:
        """Genera un nuevo listenKey para el User Data Stream (/fapi/v1/listenKey)."""
        res = await self.request("POST", "/fapi/v1/listenKey", signed=False)
        return str(res["listenKey"])

    async def keepalive_listen_key(self) -> None:
        """Renueva la vigencia de un listenKey activo (/fapi/v1/listenKey)."""
        await self.request("PUT", "/fapi/v1/listenKey", signed=False)

    async def delete_listen_key(self) -> None:
        """Cierra el listenKey activo (/fapi/v1/listenKey)."""
        await self.request("DELETE", "/fapi/v1/listenKey", signed=False)
