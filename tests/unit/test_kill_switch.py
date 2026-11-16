"""Tests unitarios exhaustivos para el Kill-Switch de 5 capas."""

from obbot.risk.kill_switch import (
    FiveLayerKillSwitch,
    KillSwitchAction,
    KillSwitchLayer,
)
from obbot.risk.limits import RiskLimits


def test_kill_switch_initial_state() -> None:
    ks = FiveLayerKillSwitch()
    assert not ks.is_triggered()
    assert ks.get_highest_severity_action() == KillSwitchAction.NONE
    assert len(ks.get_active_reasons()) == 0


def test_kill_switch_layer_1_data_integrity() -> None:
    limits = RiskLimits(max_data_staleness_ms=1000.0, max_book_desync_ms=300.0)
    ks = FiveLayerKillSwitch(limits)

    now_ns = 10_000_000_000

    # 1. Datos frescos (500 ms) -> Sin acción
    action = ks.check_data_integrity(
        now_ns=now_ns,
        last_event_ts_ns=now_ns - 500_000_000,
        book_synced=True,
    )
    assert action == KillSwitchAction.NONE
    assert not ks.is_triggered()

    # 2. Datos estancados leves (1500 ms) -> BLOCK_NEW_ENTRIES
    action = ks.check_data_integrity(
        now_ns=now_ns,
        last_event_ts_ns=now_ns - 1_500_000_000,
        book_synced=True,
    )
    assert action == KillSwitchAction.BLOCK_NEW_ENTRIES
    assert ks.is_triggered()
    assert ks.get_highest_severity_action() == KillSwitchAction.BLOCK_NEW_ENTRIES

    # 3. Datos estancados severos (6000 ms) -> CANCEL_OPEN_ORDERS
    action = ks.check_data_integrity(
        now_ns=now_ns,
        last_event_ts_ns=now_ns - 6_000_000_000,
        book_synced=True,
    )
    assert action == KillSwitchAction.CANCEL_OPEN_ORDERS
    assert ks.get_highest_severity_action() == KillSwitchAction.CANCEL_OPEN_ORDERS

    # Reseteo
    ks.reset_layer(KillSwitchLayer.DATA_INTEGRITY, "Datos reanudados")
    assert not ks.is_triggered()

    # 4. Desincronización de libro (> 300 ms) -> BLOCK_NEW_ENTRIES
    action = ks.check_data_integrity(
        now_ns=now_ns,
        last_event_ts_ns=now_ns - 100_000_000,
        book_synced=False,
        desync_duration_ms=450.0,
    )
    assert action == KillSwitchAction.BLOCK_NEW_ENTRIES
    assert ks.is_triggered()


def test_kill_switch_layer_2_network_connectivity() -> None:
    limits = RiskLimits(max_acceptable_rtt_ms=600.0, rtt_spike_multiplier=3.0)
    ks = FiveLayerKillSwitch(limits)
    ks.calibrate_baseline_rtt([80.0, 90.0, 100.0, 110.0])  # Mediana = 100.0 ms

    now_ns = 10_000_000_000

    # 1. RTT normal (120 ms) -> Sin acción
    action = ks.check_connectivity(rtt_ms=120.0, now_ns=now_ns)
    assert action == KillSwitchAction.NONE

    # 2. Spike RTT (350 ms > 3.0 * 100 ms) -> BLOCK_NEW_ENTRIES
    action = ks.check_connectivity(rtt_ms=350.0, now_ns=now_ns)
    assert action == KillSwitchAction.BLOCK_NEW_ENTRIES
    assert ks.is_triggered()

    # 3. RTT absoluto inaceptable (> 600 ms)
    ks.reset_all("Reset para test de límite absoluto")
    action = ks.check_connectivity(rtt_ms=750.0, now_ns=now_ns)
    assert action == KillSwitchAction.BLOCK_NEW_ENTRIES


def test_kill_switch_layer_3_financial_drawdown() -> None:
    limits = RiskLimits(max_daily_drawdown_pct=0.03, max_trade_loss_pct=0.01)
    ks = FiveLayerKillSwitch(limits)
    now_ns = 10_000_000_000

    # 1. Pérdidas dentro de tolerancia (drawdown 1.5%, trade loss 0.5%)
    action = ks.check_financial(
        daily_drawdown_pct=0.015,
        current_unrealized_loss_pct=0.005,
        now_ns=now_ns,
    )
    assert action == KillSwitchAction.NONE

    # 2. Pérdida por trade excesiva (1.2% > 1%) -> BLOCK_NEW_ENTRIES
    action = ks.check_financial(
        daily_drawdown_pct=0.015,
        current_unrealized_loss_pct=0.012,
        now_ns=now_ns,
    )
    assert action == KillSwitchAction.BLOCK_NEW_ENTRIES

    # 3. Drawdown diario crítico (3.2% >= 3%) -> CLOSE_POSITIONS_EMERGENCY
    action = ks.check_financial(
        daily_drawdown_pct=0.032,
        current_unrealized_loss_pct=0.005,
        now_ns=now_ns,
    )
    assert action == KillSwitchAction.CLOSE_POSITIONS_EMERGENCY
    assert ks.get_highest_severity_action() == KillSwitchAction.CLOSE_POSITIONS_EMERGENCY


def test_kill_switch_layer_4_operational_exchange() -> None:
    limits = RiskLimits(max_consecutive_order_rejections=3)
    ks = FiveLayerKillSwitch(limits)
    now_ns = 10_000_000_000

    # 1. 2 rechazos (por debajo del límite) -> Sin acción
    action = ks.check_operational(consecutive_rejections=2, stop_loss_failed=False, now_ns=now_ns)
    assert action == KillSwitchAction.NONE

    # 2. 3 rechazos consecutivos -> CANCEL_OPEN_ORDERS
    action = ks.check_operational(consecutive_rejections=3, stop_loss_failed=False, now_ns=now_ns)
    assert action == KillSwitchAction.CANCEL_OPEN_ORDERS

    # 3. Fallo en adjuntar Stop-Loss -> CLOSE_POSITIONS_EMERGENCY
    ks.reset_all("Reset")
    action = ks.check_operational(consecutive_rejections=0, stop_loss_failed=True, now_ns=now_ns)
    assert action == KillSwitchAction.CLOSE_POSITIONS_EMERGENCY


def test_kill_switch_layer_5_manual_trigger_and_hierarchy() -> None:
    ks = FiveLayerKillSwitch()
    now_ns = 10_000_000_000

    # Disparo manual
    ks.trigger_manual("Mantenimiento no programado", now_ns=now_ns)
    assert ks.is_triggered()
    assert ks.get_highest_severity_action() == KillSwitchAction.CLOSE_POSITIONS_EMERGENCY
    assert any("Intervención manual" in r for r in ks.get_active_reasons())

    # Reseteo total
    ks.reset_all("Fin de mantenimiento")
    assert not ks.is_triggered()
    assert ks.get_highest_severity_action() == KillSwitchAction.NONE

