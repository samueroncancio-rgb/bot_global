"""Interfaz base abstracta para estrategias cuantitativas de order flow."""

from abc import ABC, abstractmethod

from obbot.execution.order_manager import ManagedOrder
from obbot.features.engine import FeatureVector
from obbot.risk.limits import OrderRequest, PortfolioState


class Strategy(ABC):
    """Clase base extensible para estrategias cuantitativas."""

    def __init__(self, name: str, symbol: str) -> None:
        self.name = name
        self.symbol = symbol

    @abstractmethod
    def on_feature_vector(
        self,
        features: FeatureVector,
        portfolio: PortfolioState,
    ) -> list[OrderRequest]:
        """
        Evalúa un nuevo vector de features de microestructura y emite solicitudes de órdenes.
        Las solicitudes de órdenes pasarán inmediatamente por RiskGuard antes de enviarse.
        """
        ...

    @abstractmethod
    def on_order_update(self, order: ManagedOrder) -> None:
        """Notificación de actualización en el estado de una orden gestionada."""
        ...
