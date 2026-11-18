"""Módulo de gestión de riesgo operativo y financiero, Kill-Switch y RiskGuard."""

from obbot.risk.guard import RiskGuard
from obbot.risk.kill_switch import (
    FiveLayerKillSwitch,
    KillSwitchAction,
    KillSwitchLayer,
    LayerStatus,
)
from obbot.risk.limits import (
    OrderRequest,
    PortfolioState,
    PositionState,
    RiskAuthorizationToken,
    RiskDecision,
    RiskLimits,
)

__all__ = [
    "FiveLayerKillSwitch",
    "KillSwitchAction",
    "KillSwitchLayer",
    "LayerStatus",
    "OrderRequest",
    "PortfolioState",
    "PositionState",
    "RiskAuthorizationToken",
    "RiskDecision",
    "RiskGuard",
    "RiskLimits",
]
