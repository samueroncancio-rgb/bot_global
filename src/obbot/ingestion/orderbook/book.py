"""Mantenimiento en memoria del libro de órdenes L2 con ordenamiento logarítmico."""

from sortedcontainers import SortedDict

from obbot.common.events import OrderBookLevel


class OrderBookL2:
    """Libro de órdenes L2 optimizado para inserciones, eliminaciones y consultas de top-N."""

    def __init__(self, exchange: str, symbol: str) -> None:
        self.exchange = exchange
        self.symbol = symbol
        # Bids ordenados de mayor a menor (clave negativa para búsqueda directa en SortedDict)
        self._bids: SortedDict[float, float] = SortedDict()
        # Asks ordenados de menor a mayor
        self._asks: SortedDict[float, float] = SortedDict()
        self.last_update_id: int = 0
        self.is_synchronized: bool = False

    def reset(self) -> None:
        """Pensa y vacía el libro para forzar una resincronización limpia."""
        self._bids.clear()
        self._asks.clear()
        self.last_update_id = 0
        self.is_synchronized = False

    def apply_snapshot(
        self,
        bids: list[tuple[float, float]],
        asks: list[tuple[float, float]],
        last_update_id: int,
    ) -> None:
        """Inicializa o reconstruye el libro a partir de un snapshot completo."""
        self._bids.clear()
        self._asks.clear()

        for price, size in bids:
            if size > 0.0:
                self._bids[-price] = size

        for price, size in asks:
            if size > 0.0:
                self._asks[price] = size

        self.last_update_id = last_update_id
        self.is_synchronized = True

    def apply_delta(
        self,
        bids: list[tuple[float, float]],
        asks: list[tuple[float, float]],
        update_id: int = 0,
    ) -> None:
        """Aplica una actualización incremental (diff) al libro."""
        for price, size in bids:
            key = -price
            if size <= 0.0:
                self._bids.pop(key, None)
            else:
                self._bids[key] = size

        for price, size in asks:
            if size <= 0.0:
                self._asks.pop(price, None)
            else:
                self._asks[price] = size

        if update_id > 0:
            self.last_update_id = update_id

    def best_bid(self) -> tuple[float, float] | None:
        """Retorna el mejor precio y volumen comprador (mejor bid)."""
        if not self._bids:
            return None
        neg_price, size = next(iter(self._bids.items()))
        return -neg_price, size

    def best_ask(self) -> tuple[float, float] | None:
        """Retorna el mejor precio y volumen vendedor (mejor ask)."""
        if not self._asks:
            return None
        price, size = next(iter(self._asks.items()))
        return price, size

    def mid_price(self) -> float | None:
        """Calcula el precio medio entre el mejor bid y el mejor ask."""
        b = self.best_bid()
        a = self.best_ask()
        if b is None or a is None:
            return None
        return (b[0] + a[0]) / 2.0

    def spread(self) -> float | None:
        """Calcula el spread absoluto (best_ask - best_bid)."""
        b = self.best_bid()
        a = self.best_ask()
        if b is None or a is None:
            return None
        return a[0] - b[0]

    def is_crossed(self) -> bool:
        """Comprueba si el libro se encuentra cruzado (anomalía crítica)."""
        b = self.best_bid()
        a = self.best_ask()
        if b is None or a is None:
            return False
        return b[0] >= a[0]

    def get_bids(self, n: int = 20) -> tuple[OrderBookLevel, ...]:
        """Obtiene los N mejores niveles compradores."""
        levels: list[OrderBookLevel] = []
        for i, (neg_price, size) in enumerate(self._bids.items()):
            if i >= n:
                break
            levels.append(OrderBookLevel(price=-neg_price, size=size))
        return tuple(levels)

    def get_asks(self, n: int = 20) -> tuple[OrderBookLevel, ...]:
        """Obtiene los N mejores niveles vendedores."""
        levels: list[OrderBookLevel] = []
        for i, (price, size) in enumerate(self._asks.items()):
            if i >= n:
                break
            levels.append(OrderBookLevel(price=price, size=size))
        return tuple(levels)
