"""Sistema de logs estructurados en JSON de alto rendimiento y seguridad."""

import json
import logging
import sys
from datetime import UTC, datetime
from typing import Any, ClassVar


class JSONFormatter(logging.Formatter):
    """Formateador de logs en formato JSON para trazabilidad y observabilidad."""

    STANDARD_ATTRS: ClassVar[set[str]] = {
        "name",
        "msg",
        "args",
        "levelname",
        "levelno",
        "pathname",
        "filename",
        "module",
        "exc_info",
        "exc_text",
        "stack_info",
        "lineno",
        "funcName",
        "created",
        "msecs",
        "relativeCreated",
        "thread",
        "threadName",
        "processName",
        "process",
        "message",
        "taskName",
    }

    def format(self, record: logging.LogRecord) -> str:
        log_entry: dict[str, Any] = {
            "timestamp": datetime.fromtimestamp(record.created, tz=UTC).isoformat(),
            "level": record.levelname,
            "logger": record.name,
            "message": record.getMessage(),
        }

        # Inyectar cualquier atributo adicional presente en record.__dict__
        for key, val in record.__dict__.items():
            if key not in self.STANDARD_ATTRS:
                log_entry[key] = val

        if record.exc_info:
            log_entry["exception"] = self.formatException(record.exc_info)

        return json.dumps(log_entry, ensure_ascii=False)


def setup_logging(level: str = "INFO") -> logging.Logger:
    """Configura el logger raíz con el formateador JSON estructurado."""
    root_logger = logging.getLogger("obbot")
    root_logger.setLevel(getattr(logging, level.upper(), logging.INFO))

    # Limpiar handlers existentes para evitar duplicación
    root_logger.handlers.clear()

    handler = logging.StreamHandler(sys.stdout)
    handler.setFormatter(JSONFormatter())
    root_logger.addHandler(handler)
    root_logger.propagate = False

    return root_logger


def get_logger(name: str) -> logging.Logger:
    """Retorna un logger hijo bajo la jerarquía 'obbot'."""
    return logging.getLogger(f"obbot.{name}")
