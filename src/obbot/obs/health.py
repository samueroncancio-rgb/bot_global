"""Servidor de sondas de salud (healthz) y preparación (readyz) para Kubernetes / Docker."""

from collections.abc import Callable

from aiohttp import web

from obbot.common.logging import get_logger

logger = get_logger("obs.health")


class HealthServer:
    """Servidor HTTP asíncrono para health checks y readiness probes."""

    def __init__(
        self,
        port: int = 8080,
        readiness_check: Callable[[], tuple[bool, str]] | None = None,
    ) -> None:
        self.port = port
        self.readiness_check = readiness_check
        self.app = web.Application()
        self.app.router.add_get("/healthz", self.handle_healthz)
        self.app.router.add_get("/readyz", self.handle_readyz)
        self._runner: web.AppRunner | None = None
        self._site: web.TCPSite | None = None

    async def handle_healthz(self, request: web.Request) -> web.Response:
        """Sonda Liveness: confirma que el proceso Python está vivo y respondiendo."""
        return web.json_response({"status": "healthy", "service": "orderflow-bot"})

    async def handle_readyz(self, request: web.Request) -> web.Response:
        """Sonda Readiness: verifica que el bot esté listo para operar o procesar eventos."""
        if self.readiness_check:
            is_ready, reason = self.readiness_check()
            if not is_ready:
                return web.json_response(
                    {"status": "not_ready", "reason": reason},
                    status=503,
                )
        return web.json_response({"status": "ready", "service": "orderflow-bot"})

    async def start(self) -> None:
        """Inicia el servidor HTTP de probes."""
        self._runner = web.AppRunner(self.app)
        await self._runner.setup()
        self._site = web.TCPSite(self._runner, "0.0.0.0", self.port)
        await self._site.start()
        logger.info(
            "obs.health_server_started: port=%d",
            self.port,
            extra={"port": self.port},
        )

    async def stop(self) -> None:
        """Detiene el servidor HTTP limpiamente."""
        if self._runner:
            await self._runner.cleanup()
            self._runner = None
            self._site = None
        logger.info("obs.health_server_stopped")
