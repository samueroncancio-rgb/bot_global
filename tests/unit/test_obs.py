"""Tests unitarios para el servidor de salud y métricas de observabilidad."""

import pytest
from aiohttp import web
from aiohttp.test_utils import AioHTTPTestCase

from obbot.obs.health import HealthServer
from obbot.obs.metrics import (
    DAILY_DRAWDOWN_PCT,
    EVENTS_TOTAL,
    GLOBAL_DELTA_Z,
    KILL_SWITCH_ACTIVE,
    start_metrics_server,
)


def test_prometheus_metrics_registration() -> None:
    # Comprobar que los contadores y gauges registran valores sin error
    EVENTS_TOTAL.labels(exchange="binance", channel="trades").inc()
    GLOBAL_DELTA_Z.labels(symbol="BTCUSDT").set(1.75)
    DAILY_DRAWDOWN_PCT.set(0.015)
    KILL_SWITCH_ACTIVE.set(0)

    # Iniciar el servidor en un puerto alto no conflictivo
    start_metrics_server(port=18001)


class TestHealthServer(AioHTTPTestCase):
    async def get_application(self) -> web.Application:
        self.is_ready = True
        self.reason = "Operativo"

        def _check_ready() -> tuple[bool, str]:
            return self.is_ready, self.reason

        self.health_service = HealthServer(port=18081, readiness_check=_check_ready)
        return self.health_service.app

    @pytest.mark.asyncio
    async def test_healthz_endpoint(self) -> None:
        resp = await self.client.request("GET", "/healthz")
        assert resp.status == 200
        data = await resp.json()
        assert data["status"] == "healthy"

    @pytest.mark.asyncio
    async def test_readyz_endpoint_ready_and_not_ready(self) -> None:
        # 1. Ready
        self.is_ready = True
        resp = await self.client.request("GET", "/readyz")
        assert resp.status == 200
        data = await resp.json()
        assert data["status"] == "ready"

        # 2. Not Ready (ej. Kill-Switch en CLOSE_POSITIONS_EMERGENCY)
        self.is_ready = False
        self.reason = "Kill-Switch activo"
        resp_bad = await self.client.request("GET", "/readyz")
        assert resp_bad.status == 503
        data_bad = await resp_bad.json()
        assert data_bad["status"] == "not_ready"
        assert "Kill-Switch activo" in data_bad["reason"]
