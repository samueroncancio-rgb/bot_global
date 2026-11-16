"""Módulo de estrategias cuantitativas de trading de microestructura."""

from obbot.strategy.base import Strategy
from obbot.strategy.baseline_example import BaselineOrderFlowStrategy

__all__ = ["BaselineOrderFlowStrategy", "Strategy"]

