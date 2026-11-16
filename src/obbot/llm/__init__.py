"""Módulo LLM opcional y desacoplado para auditoría y reportes offline."""

from obbot.llm.client import LLMProvider, create_llm_provider
from obbot.llm.gemini_provider import GeminiProvider
from obbot.llm.openai_compat_provider import OpenAICompatProvider
from obbot.llm.tasks import generate_end_of_session_report

__all__ = [
    "GeminiProvider",
    "LLMProvider",
    "OpenAICompatProvider",
    "create_llm_provider",
    "generate_end_of_session_report",
]

