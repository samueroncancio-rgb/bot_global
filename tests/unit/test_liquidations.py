"""Tests unitarios para el rastreador de liquidaciones."""

from obbot.common.events import Side
from obbot.features.liquidations import LiquidationTracker


def test_liquidation_metrics_and_eviction() -> None:
    # Ventana de 60 segundos
    tracker = LiquidationTracker("BTC-USDT-PERP", window_sec=60.0)

    # 1. Registrar 2 liquidaciones de compra y 1 de venta a t = 10s
    t10_ns = 10_000_000_000
    tracker.record_liquidation(t10_ns, Side.BUY, 50_000.0)
    tracker.record_liquidation(t10_ns + 1000, Side.BUY, 30_000.0)
    tracker.record_liquidation(t10_ns + 2000, Side.SELL, 20_000.0)

    metrics = tracker.get_metrics(current_ts_ns=t10_ns + 5_000_000_000)
    assert metrics.total_events == 3
    assert metrics.buy_liq_count == 2
    assert metrics.sell_liq_count == 1
    assert metrics.buy_liq_notional_usd == 80_000.0
    assert metrics.sell_liq_notional_usd == 20_000.0
    assert metrics.net_liq_notional_usd == 60_000.0
    assert metrics.events_per_minute == 3.0
    assert metrics.is_downsampled_proxy is True

    # 2. Avanzar a t = 80s (más de 60s después) -> Se deben desalojar
    metrics_empty = tracker.get_metrics(current_ts_ns=80_000_000_000)
    assert metrics_empty.total_events == 0
    assert metrics_empty.buy_liq_notional_usd == 0.0

