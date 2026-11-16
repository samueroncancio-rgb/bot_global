"""Proveedor LLM para la API oficial de Gemini mediante el SDK google-genai."""

import asyncio
from typing import Any

from google import genai
from google.genai import types

from obbot.common.logging import get_logger
from obbot.llm.client import LLMProvider

logger = get_logger("llm.gemini")


class GeminiProvider(LLMProvider):
    """Integración con Gemini API usando el SDK oficial google-genai."""

    def __init__(self, api_key: str, model: str) -> None:
        super().__init__(model=model)
        self.client = genai.Client(api_key=api_key)

    async def generate(
        self,
        prompt: str,
        system_instruction: str | None = None,
    ) -> str:
        """Genera contenido textual asíncronamente mediante thread pool."""
        config: dict[str, Any] = {}
        if system_instruction:
            config["system_instruction"] = system_instruction

        def _call_gemini() -> str:
            response = self.client.models.generate_content(
                model=self.model,
                contents=prompt,
                config=types.GenerateContentConfig(**config) if config else None,
            )
            return response.text or ""

        try:
            return await asyncio.to_thread(_call_gemini)
        except Exception as exc:
            logger.error("llm.gemini_generate_failed: %s", exc)
            raise
