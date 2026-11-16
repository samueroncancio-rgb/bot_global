"""Capa de abstracción de clientes LLM para análisis offline y post-sesión."""

import os
from abc import ABC, abstractmethod

from obbot.common.config import AppConfig
from obbot.common.logging import get_logger

logger = get_logger("llm.client")


class LLMProvider(ABC):
    """Interfaz abstracta para proveedores de modelos de lenguaje."""

    def __init__(self, model: str) -> None:
        self.model = model

    @abstractmethod
    async def generate(
        self,
        prompt: str,
        system_instruction: str | None = None,
    ) -> str:
        """Genera una respuesta textual a partir de un prompt y contexto."""
        ...


def create_llm_provider(config: AppConfig | None = None) -> LLMProvider | None:
    """
    Factory que instancia el proveedor LLM configurado (Gemini o OpenAI-compatible).
    Retorna None si no hay credenciales configuradas, manteniendo el módulo opcional.
    """
    cfg = config or AppConfig()
    provider_name = cfg.env.llm_provider.lower()
    model = cfg.env.llm_model

    if provider_name == "gemini":
        api_key = cfg.env.gemini_api_key or os.getenv("GEMINI_API_KEY", "")
        if not api_key:
            logger.warning("llm.gemini_disabled: no se encontró GEMINI_API_KEY")
            return None
        from obbot.llm.gemini_provider import GeminiProvider

        return GeminiProvider(api_key=api_key, model=model)

    if provider_name in ("openai", "litellm", "openai_compatible"):
        api_key = cfg.env.llm_api_key or os.getenv("LLM_API_KEY", "")
        base_url = cfg.env.llm_base_url or os.getenv("LLM_BASE_URL", "")
        if not api_key:
            logger.warning("llm.openai_disabled: no se encontró LLM_API_KEY")
            return None
        from obbot.llm.openai_compat_provider import OpenAICompatProvider

        return OpenAICompatProvider(api_key=api_key, base_url=base_url, model=model)

    logger.warning("llm.unknown_provider: %s", provider_name)
    return None
