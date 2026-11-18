"""Proveedor agnóstico OpenAI-compatible / LiteLLM para modelos locales o en la nube."""

from typing import Any

from openai import AsyncOpenAI

from obbot.common.logging import get_logger
from obbot.llm.client import LLMProvider

logger = get_logger("llm.openai_compat")


class OpenAICompatProvider(LLMProvider):
    """Integración agnóstica con cualquier endpoint compatible con la API de OpenAI."""

    def __init__(
        self,
        api_key: str,
        model: str,
        base_url: str | None = None,
    ) -> None:
        super().__init__(model=model)
        self.client = AsyncOpenAI(
            api_key=api_key,
            base_url=base_url if base_url else None,
        )

    async def generate(
        self,
        prompt: str,
        system_instruction: str | None = None,
    ) -> str:
        """Genera una respuesta textual mediante el endpoint chat.completions."""
        messages: list[dict[str, Any]] = []
        if system_instruction:
            messages.append({"role": "system", "content": system_instruction})
        messages.append({"role": "user", "content": prompt})

        try:
            response = await self.client.chat.completions.create(
                model=self.model,
                messages=messages,  # type: ignore[arg-type]
            )
            choice = response.choices[0]
            return choice.message.content or ""
        except Exception as exc:
            logger.error("llm.openai_generate_failed: %s", exc)
            raise
