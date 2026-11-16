"""Sistema de Kill-Switch defensivo de 5 capas para trading cuantitativo."""

from dataclasses import dataclass
from enum import Enum, auto

from obbot.common.logging import get_logger
from obbot.risk.limits import RiskLimits

logger = get_logger("risk.kill_switch")


class KillSwitchLayer(Enum):
    """Las 5 capas defensivas de seguridad del sistema."""

    DATA_INTEGRITY = auto()  # Capa 1: Calidad y frescura de datos
    NETWORK_CONNECTIVITY = auto()  # Capa 2: Conectividad y latencia RTT
    FINANCIAL_DRAWDOWN = auto()  # Capa 3: Pérdidas y límites financieros
    OPERATIONAL_EXCHANGE = auto()  # Capa 4: Errores operativos y de exchange
    MANUAL = auto()  # Capa 5: Intervención manual del operador


class KillSwitchAction(Enum):
    """Acciones operativas requeridas según la severidad del riesgo detectado."""

    NONE = 0
    BLOCK_NEW_ENTRIES = 1
    CANCEL_OPEN_ORDERS = 2
    CLOSE_POSITIONS_EMERGENCY = 3

    def is_stricter_than(self, other: "KillSwitchAction") -> bool:
        """Determina si esta acción tiene mayor severidad que otra."""
        return self.value > other.value


@dataclass(frozen=True)
class LayerStatus:
    """Estado y diagnóstico actual de una capa de riesgo."""

    triggered: bool
    reason: str
    action: KillSwitchAction
    triggered_at_ns: int


class FiveLayerKillSwitch:
    """
    Gestor de seguridad de 5 capas con evaluación continua y jerarquía estricta.

    Prioridad de acciones:
    CLOSE_POSITIONS_EMERGENCY > CANCEL_OPEN_ORDERS > BLOCK_NEW_ENTRIES > NONE.
    """

    def __init__(self, limits: RiskLimits | None = None) -> None:
        self.limits = limits or RiskLimits()
        self._layers: dict[KillSwitchLayer, LayerStatus] = {
            layer: LayerStatus(
                triggered=False,
                reason="",
                action=KillSwitchAction.NONE,
                triggered_at_ns=0,
            )
            for layer in KillSwitchLayer
        }
        self._baseline_rtt_ms: float = 100.0  # RTT base estimado de arranque

    def calibrate_baseline_rtt(self, rtt_samples_ms: list[float]) -> None:
        """Calibra la latencia base RTT utilizando la mediana de muestras registradas."""
        if rtt_samples_ms:
            sorted_samples = sorted(rtt_samples_ms)
            mid = len(sorted_samples) // 2
            self._baseline_rtt_ms = sorted_samples[mid]
            logger.info(
                "kill_switch.rtt_calibrated baseline_rtt_ms=%.1f",
                self._baseline_rtt_ms,
                extra={"baseline_rtt_ms": self._baseline_rtt_ms},
            )

    def _trip_layer(
        self,
        layer: KillSwitchLayer,
        action: KillSwitchAction,
        reason: str,
        now_ns: int,
    ) -> None:
        """Dispara una capa específica registrando el motivo y la acción requerida."""
        current = self._layers[layer]
        # Solo actualizar si no estaba disparada o si la nueva acción es más estricta
        if not current.triggered or action.is_stricter_than(current.action):
            self._layers[layer] = LayerStatus(
                triggered=True,
                reason=reason,
                action=action,
                triggered_at_ns=now_ns,
            )
            logger.error(
                "kill_switch.layer_tripped: layer=%s action=%s reason=%s",
                layer.name,
                action.name,
                reason,
                extra={"layer": layer.name, "action": action.name, "reason": reason},
            )

    def check_data_integrity(
        self,
        now_ns: int,
        last_event_ts_ns: int,
        book_synced: bool,
        desync_duration_ms: float = 0.0,
    ) -> KillSwitchAction:
        """
        Capa 1: Evalúa frescura de datos y consistencia del Order Book.
        """
        staleness_ms = (now_ns - last_event_ts_ns) / 1_000_000.0

        if staleness_ms > self.limits.max_data_staleness_ms:
            msg = (
                f"Datos estancados: {staleness_ms:.1f}ms sin eventos "
                f"(límite {self.limits.max_data_staleness_ms:.1f}ms)"
            )
            action = (
                KillSwitchAction.CANCEL_OPEN_ORDERS
                if staleness_ms > 5000.0
                else KillSwitchAction.BLOCK_NEW_ENTRIES
            )
            self._trip_layer(KillSwitchLayer.DATA_INTEGRITY, action, msg, now_ns)
            return action

        if not book_synced and desync_duration_ms > self.limits.max_book_desync_ms:
            msg = (
                f"Order Book desincronizado por {desync_duration_ms:.1f}ms "
                f"(límite {self.limits.max_book_desync_ms:.1f}ms)"
            )
            self._trip_layer(
                KillSwitchLayer.DATA_INTEGRITY,
                KillSwitchAction.BLOCK_NEW_ENTRIES,
                msg,
                now_ns,
            )
            return KillSwitchAction.BLOCK_NEW_ENTRIES

        return KillSwitchAction.NONE

    def check_connectivity(
        self,
        rtt_ms: float,
        now_ns: int,
    ) -> KillSwitchAction:
        """
        Capa 2: Evalúa latencia RTT contra el umbral absoluto y el multiplicador de spike.
        """
        spike_limit_ms = self._baseline_rtt_ms * self.limits.rtt_spike_multiplier
        effective_limit_ms = min(self.limits.max_acceptable_rtt_ms, spike_limit_ms)

        if rtt_ms > effective_limit_ms:
            msg = (
                f"Latencia RTT excesiva: {rtt_ms:.1f}ms "
                f"(umbral dinámico {effective_limit_ms:.1f}ms, base {self._baseline_rtt_ms:.1f}ms)"
            )
            self._trip_layer(
                KillSwitchLayer.NETWORK_CONNECTIVITY,
                KillSwitchAction.BLOCK_NEW_ENTRIES,
                msg,
                now_ns,
            )
            return KillSwitchAction.BLOCK_NEW_ENTRIES

        return KillSwitchAction.NONE

    def check_financial(
        self,
        daily_drawdown_pct: float,
        current_unrealized_loss_pct: float,
        now_ns: int,
    ) -> KillSwitchAction:
        """
        Capa 3: Evalúa drawdown diario total y pérdida de la posición actual.
        """
        if daily_drawdown_pct >= self.limits.max_daily_drawdown_pct:
            msg = (
                f"Drawdown diario excedido: {daily_drawdown_pct * 100:.2f}% "
                f"(máximo {self.limits.max_daily_drawdown_pct * 100:.2f}%)"
            )
            self._trip_layer(
                KillSwitchLayer.FINANCIAL_DRAWDOWN,
                KillSwitchAction.CLOSE_POSITIONS_EMERGENCY,
                msg,
                now_ns,
            )
            return KillSwitchAction.CLOSE_POSITIONS_EMERGENCY

        if current_unrealized_loss_pct >= self.limits.max_trade_loss_pct:
            msg = (
                f"Pérdida por trade excedida: {current_unrealized_loss_pct * 100:.2f}% "
                f"(máximo {self.limits.max_trade_loss_pct * 100:.2f}%)"
            )
            self._trip_layer(
                KillSwitchLayer.FINANCIAL_DRAWDOWN,
                KillSwitchAction.BLOCK_NEW_ENTRIES,
                msg,
                now_ns,
            )
            return KillSwitchAction.BLOCK_NEW_ENTRIES

        return KillSwitchAction.NONE

    def check_operational(
        self,
        consecutive_rejections: int,
        stop_loss_failed: bool,
        now_ns: int,
    ) -> KillSwitchAction:
        """
        Capa 4: Evalúa rechazos de órdenes en Binance y fallos críticos en Algo Stop-Loss.
        """
        if stop_loss_failed:
            timeout_ms = self.limits.stop_loss_attach_timeout_ms
            msg = (
                f"Fallo crítico al adjuntar Stop-Loss tras {timeout_ms:.0f}ms. "
                "Cierre forzoso de emergencia requerido."
            )
            self._trip_layer(
                KillSwitchLayer.OPERATIONAL_EXCHANGE,
                KillSwitchAction.CLOSE_POSITIONS_EMERGENCY,
                msg,
                now_ns,
            )
            return KillSwitchAction.CLOSE_POSITIONS_EMERGENCY

        if consecutive_rejections >= self.limits.max_consecutive_order_rejections:
            msg = (
                f"Rechazos consecutivos de órdenes: {consecutive_rejections} "
                f"(límite {self.limits.max_consecutive_order_rejections})"
            )
            self._trip_layer(
                KillSwitchLayer.OPERATIONAL_EXCHANGE,
                KillSwitchAction.CANCEL_OPEN_ORDERS,
                msg,
                now_ns,
            )
            return KillSwitchAction.CANCEL_OPEN_ORDERS

        return KillSwitchAction.NONE

    def trigger_manual(
        self,
        reason: str,
        now_ns: int,
        action: KillSwitchAction = KillSwitchAction.CLOSE_POSITIONS_EMERGENCY,
    ) -> None:
        """Capa 5: Disparo manual inmediato por el operador o un comando de emergencia."""
        self._trip_layer(KillSwitchLayer.MANUAL, action, f"Intervención manual: {reason}", now_ns)

    def reset_layer(self, layer: KillSwitchLayer, operator_reason: str) -> None:
        """Restablece una capa del Kill-Switch tras certificar resolución del incidente."""
        self._layers[layer] = LayerStatus(
            triggered=False,
            reason="",
            action=KillSwitchAction.NONE,
            triggered_at_ns=0,
        )
        logger.info(
            "kill_switch.layer_reset: layer=%s reason=%s",
            layer.name,
            operator_reason,
            extra={"layer": layer.name, "reason": operator_reason},
        )

    def reset_all(self, operator_reason: str) -> None:
        """Restablece todas las capas del Kill-Switch."""
        for layer in KillSwitchLayer:
            self.reset_layer(layer, operator_reason)

    def get_highest_severity_action(self) -> KillSwitchAction:
        """Determina la acción más estricta activa entre todas las capas."""
        highest = KillSwitchAction.NONE
        for status in self._layers.values():
            if status.triggered and status.action.is_stricter_than(highest):
                highest = status.action
        return highest

    def is_triggered(self) -> bool:
        """Indica si al menos una capa defensiva se encuentra disparada."""
        return any(status.triggered for status in self._layers.values())

    def get_active_reasons(self) -> list[str]:
        """Devuelve la lista de motivos de las capas disparadas."""
        return [
            f"[{layer.name}] {status.reason}"
            for layer, status in self._layers.items()
            if status.triggered
        ]

    def get_status(self) -> dict[str, LayerStatus]:
        """Retorna una vista estructurada del estado de cada capa."""
        return {layer.name: status for layer, status in self._layers.items()}
