"""Tareas analíticas offline ejecutadas por el módulo LLM fuera de la ruta crítica."""

import json
from typing import Any

from obbot.common.logging import get_logger
from obbot.llm.client import LLMProvider

logger = get_logger("llm.tasks")

SYSTEM_PROMPT_ANALYST = (
    "Eres un Oficial Senior de Riesgos y Cuantitativo especializado en microestructura "
    "de cripto-derivados y order flow.\n"
    "Tu labor es auditar la sesión operativa de trading de forma objetiva, escéptica y concisa.\n"
    "Prioriza la detección de anomalías de riesgo, problemas de conectividad/latencia, "
    "estabilidad del feed y calibración de umbrales."
)


async def generate_end_of_session_report(
    provider: LLMProvider,
    session_metrics: dict[str, Any],
) -> str:
    """Genera un informe narrativo post-sesión analizando métricas de riesgo y microestructura."""
    metrics_json = json.dumps(session_metrics, indent=2, ensure_ascii=False)

    user_prompt = (
        f"Analiza las siguientes métricas registradas durante la sesión de trading del bot:\n\n"
        f"```json\n{metrics_json}\n```\n\n"
        "Estructura el informe en Markdown con las siguientes secciones:\n"
        "1. **Resumen Ejecutivo**: PnL neto, retorno porcentual y drawdown máximo alcanzado.\n"
        "2. **Evaluación de Riesgo y Kill-Switch**: Análisis de eventos de las 5 capas.\n"
        "3. **Comportamiento de Microestructura**: Dinámica de Delta Global, OBI y liquidaciones.\n"
        "4. **Recomendaciones Operativas**: Sugerencias de calibración para la próxima sesión.\n"
    )

    logger.info("llm.generating_end_of_session_report: model=%s", provider.model)
    return await provider.generate(
        prompt=user_prompt,
        system_instruction=SYSTEM_PROMPT_ANALYST,
    )
