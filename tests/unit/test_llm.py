"""Tests unitarios para el módulo LLM offline de auditoría post-sesión."""

from unittest.mock import AsyncMock, patch

import pytest

from obbot.llm.client import LLMProvider, create_llm_provider
from obbot.llm.tasks import generate_end_of_session_report


class DummyProvider(LLMProvider):
    async def generate(self, prompt: str, system_instruction: str | None = None) -> str:
        return (
            "# Informe Post-Sesión\n\n"
            "- PnL: +2.5%\n"
            "- Kill-Switch: 0 disparos\n"
            "- Conclusión: Operativa estable."
        )


@pytest.mark.asyncio
async def test_generate_end_of_session_report() -> None:
    provider = DummyProvider(model="dummy-model")

    session_metrics = {
        "initial_balance": 10000.0,
        "final_balance": 10250.0,
        "net_pnl": 250.0,
        "max_drawdown_pct": 0.008,
        "total_trades": 12,
        "win_rate": 0.66,
        "kill_switch_trips": [],
    }

    report = await generate_end_of_session_report(provider, session_metrics)

    assert "Informe Post-Sesión" in report
    assert "Operativa estable" in report


def test_create_llm_provider_without_keys_returns_none() -> None:
    with patch.dict("os.environ", {}, clear=True):
        # Sin variables de entorno configuradas
        provider = create_llm_provider()
        # Debe ser None para asegurar desacoplamiento total y opcionalidad
        assert provider is None


@pytest.mark.asyncio
async def test_openai_compat_provider_mock() -> None:
    with patch("obbot.llm.openai_compat_provider.AsyncOpenAI") as mock_openai_cls:
        mock_instance = AsyncMock()
        mock_choice = AsyncMock()
        mock_choice.message.content = "Análisis completado satisfactoriamente."
        mock_res = AsyncMock()
        mock_res.choices = [mock_choice]
        mock_instance.chat.completions.create.return_value = mock_res
        mock_openai_cls.return_value = mock_instance

        from obbot.llm.openai_compat_provider import OpenAICompatProvider

        provider = OpenAICompatProvider(
            api_key="sk-test",
            model="gpt-4o-mini",
            base_url="https://api.openai.com/v1",
        )

        res = await provider.generate("Hola")
        assert "Análisis completado" in res
