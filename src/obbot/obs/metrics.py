"""Módulo de métricas Prometheus para observabilidad de microestructura y ejecución."""

from prometheus_client import Counter, Gauge, Histogram, start_http_server

from obbot.common.logging import get_logger

logger = get_logger("obs.metrics")

# --- Contadores ---
EVENTS_TOTAL = Counter(
    "obbot_events_total",
    "Total de eventos de mercado ingeridos y procesados",
    ["exchange", "channel"],
)

ORDERS_TOTAL = Counter(
    "obbot_orders_total",
    "Total de órdenes emitidas por el sistema",
    ["symbol", "side", "status"],
)

KILL_SWITCH_TRIPS_TOTAL = Counter(
    "obbot_kill_switch_trips_total",
    "Disparos de capas del Kill-Switch",
    ["layer", "action"],
)

LIQUIDATIONS_TOTAL = Counter(
    "obbot_liquidations_total",
    "Total de eventos de liquidación detectados",
    ["exchange", "symbol", "side"],
)

# --- Gauges ---
RTT_LATENCY_MS = Gauge(
    "obbot_rtt_latency_ms",
    "Latencia RTT actual hacia el exchange en milisegundos",
    ["exchange"],
)

GLOBAL_DELTA_Z = Gauge(
    "obbot_global_delta_z",
    "Z-score actual del Delta Global multi-exchange",
    ["symbol"],
)

OBI_10_LEVELS = Gauge(
    "obbot_obi_10_levels",
    "Order Book Imbalance ponderado a 10 niveles",
    ["symbol"],
)

CVD_USD = Gauge(
    "obbot_cvd_usd",
    "Delta de volumen acumulado (CVD) en USD",
    ["exchange", "symbol"],
)

DAILY_DRAWDOWN_PCT = Gauge(
    "obbot_daily_drawdown_pct",
    "Drawdown porcentual acumulado en la sesión diaria",
)

WALLET_BALANCE_USD = Gauge(
    "obbot_wallet_balance_usd",
    "Balance total de la billetera en USD",
)

KILL_SWITCH_ACTIVE = Gauge(
    "obbot_kill_switch_active",
    "Estado binario del Kill-Switch (1 si alguna capa está disparada, 0 si normal)",
)

# --- Histogramas ---
ORDER_EXECUTION_LATENCY_SECONDS = Histogram(
    "obbot_order_execution_latency_seconds",
    "Latencia de validación pre-trade y envío de orden en segundos",
    buckets=[0.001, 0.005, 0.010, 0.025, 0.050, 0.100, 0.250, 0.500, 1.0, 2.0],
)

FEATURE_COMPUTE_LATENCY_SECONDS = Histogram(
    "obbot_feature_compute_latency_seconds",
    "Latencia de cálculo del vector de features en segundos",
    buckets=[0.0001, 0.0005, 0.001, 0.002, 0.005, 0.010],
)


def start_metrics_server(port: int = 8000) -> None:
    """Inicia el servidor HTTP de exportación de métricas Prometheus."""
    try:
        start_http_server(port)
        logger.info(
            "obs.metrics_server_started: port=%d",
            port,
            extra={"port": port},
        )
    except Exception as exc:
        logger.warning("obs.metrics_server_start_failed: %s", exc)

