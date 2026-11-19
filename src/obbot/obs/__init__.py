"""Módulo de observabilidad, métricas Prometheus y sondas de salud."""

from obbot.obs.health import HealthServer
from obbot.obs.telegram import TelegramBotService
from obbot.obs.metrics import (
    CVD_USD,
    DAILY_DRAWDOWN_PCT,
    EVENTS_TOTAL,
    FEATURE_COMPUTE_LATENCY_SECONDS,
    GLOBAL_DELTA_Z,
    KILL_SWITCH_ACTIVE,
    KILL_SWITCH_TRIPS_TOTAL,
    LIQUIDATIONS_TOTAL,
    OBI_10_LEVELS,
    ORDER_EXECUTION_LATENCY_SECONDS,
    ORDERS_TOTAL,
    RTT_LATENCY_MS,
    WALLET_BALANCE_USD,
    start_metrics_server,
)

__all__ = [
    "CVD_USD",
    "DAILY_DRAWDOWN_PCT",
    "EVENTS_TOTAL",
    "FEATURE_COMPUTE_LATENCY_SECONDS",
    "GLOBAL_DELTA_Z",
    "KILL_SWITCH_ACTIVE",
    "KILL_SWITCH_TRIPS_TOTAL",
    "LIQUIDATIONS_TOTAL",
    "OBI_10_LEVELS",
    "ORDERS_TOTAL",
    "ORDER_EXECUTION_LATENCY_SECONDS",
    "RTT_LATENCY_MS",
    "WALLET_BALANCE_USD",
    "HealthServer",
    "TelegramBotService",
    "start_metrics_server",
]
