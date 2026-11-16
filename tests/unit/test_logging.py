"""Tests unitarios para el sistema de logs estructurados en JSON."""

import json
import logging

from obbot.common.logging import JSONFormatter, get_logger, setup_logging


def test_json_formatter_standard_fields() -> None:
    formatter = JSONFormatter()
    record = logging.LogRecord(
        name="obbot.test",
        level=logging.INFO,
        pathname="test.py",
        lineno=10,
        msg="Operación ejecutada con éxito",
        args=(),
        exc_info=None,
    )
    formatted = formatter.format(record)
    parsed = json.loads(formatted)

    assert parsed["level"] == "INFO"
    assert parsed["logger"] == "obbot.test"
    assert parsed["message"] == "Operación ejecutada con éxito"
    assert "timestamp" in parsed


def test_json_formatter_contextual_fields() -> None:
    formatter = JSONFormatter()
    record = logging.LogRecord(
        name="obbot.execution",
        level=logging.WARNING,
        pathname="exec.py",
        lineno=42,
        msg="Orden descartada por latencia excesiva",
        args=(),
        exc_info=None,
    )
    record.trace_id = "trace-987"
    record.client_order_id = "OBB_001"
    record.symbol = "BTC-USDT-PERP"
    record.exchange = "binance"
    record.latency_ms = 450.5

    formatted = formatter.format(record)
    parsed = json.loads(formatted)

    assert parsed["trace_id"] == "trace-987"
    assert parsed["client_order_id"] == "OBB_001"
    assert parsed["symbol"] == "BTC-USDT-PERP"
    assert parsed["exchange"] == "binance"
    assert parsed["latency_ms"] == 450.5


def test_setup_logging_and_get_logger() -> None:
    logger = setup_logging(level="DEBUG")
    assert logger.name == "obbot"
    assert logger.level == logging.DEBUG
    assert len(logger.handlers) == 1

    child = get_logger("features.delta")
    assert child.name == "obbot.features.delta"
