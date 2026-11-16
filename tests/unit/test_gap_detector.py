"""Tests unitarios para la validación de secuencias y detección de huecos en Binance."""

from obbot.ingestion.orderbook.gap_detector import (
    BinanceGapDetector,
    BufferedDelta,
    SyncState,
)


def test_gap_detector_happy_path() -> None:
    detector = BinanceGapDetector("BTC-USDT-PERP")
    assert detector.state == SyncState.WAITING_FOR_SNAPSHOT

    # 1. Acumular deltas en búfer
    # Delta 1: obsoleto (u=95 < snapshot=100)
    detector.buffer_delta(
        BufferedDelta(
            first_update_id=90,
            final_update_id=95,
            prev_final_update_id=89,
            bids=[],
            asks=[],
            ts_exchange_ms=1000,
        )
    )
    # Delta 2: solapa con snapshot (U=96 <= 100 <= u=105)
    detector.buffer_delta(
        BufferedDelta(
            first_update_id=96,
            final_update_id=105,
            prev_final_update_id=95,
            bids=[(50000.0, 1.0)],
            asks=[],
            ts_exchange_ms=1001,
        )
    )
    # Delta 3: continuo (pu=105 == prev_u)
    detector.buffer_delta(
        BufferedDelta(
            first_update_id=106,
            final_update_id=110,
            prev_final_update_id=105,
            bids=[],
            asks=[(50010.0, 0.5)],
            ts_exchange_ms=1002,
        )
    )

    # 2. Alinear con snapshot de last_update_id=100
    aligned = detector.align_snapshot(snapshot_last_update_id=100)
    assert aligned is not None
    assert len(aligned) == 2
    assert str(detector.state) == str(SyncState.SYNCHRONIZED)
    assert detector.last_final_update_id == 110

    # 3. Validar delta siguiente continuo en tiempo real (pu=110)
    valid = detector.validate_next_delta(first_id=111, final_id=115, prev_final_id=110)
    assert valid is True
    assert detector.last_final_update_id == 115


def test_gap_detector_gap_injection_failure() -> None:
    detector = BinanceGapDetector("BTC-USDT-PERP")

    # Alinear primero
    detector.buffer_delta(
        BufferedDelta(
            first_update_id=95,
            final_update_id=105,
            prev_final_update_id=94,
            bids=[],
            asks=[],
            ts_exchange_ms=1000,
        )
    )
    aligned = detector.align_snapshot(snapshot_last_update_id=100)
    assert aligned is not None
    assert str(detector.state) == str(SyncState.SYNCHRONIZED)
    assert detector.last_final_update_id == 105

    # Inyección de hueco: se espera pu=105, pero llega pu=108 (pérdida de paquetes)
    is_valid = detector.validate_next_delta(first_id=109, final_id=115, prev_final_id=108)
    assert is_valid is False
    assert str(detector.state) == str(SyncState.DESYNCHRONIZED)

    # Siguientes deltas son rechazados automáticamente hasta reset
    assert detector.validate_next_delta(first_id=116, final_id=120, prev_final_id=115) is False

    detector.reset()
    assert str(detector.state) == str(SyncState.WAITING_FOR_SNAPSHOT)


def test_gap_detector_snapshot_no_overlap() -> None:
    detector = BinanceGapDetector("BTC-USDT-PERP")

    # Búfer con deltas que quedan por detrás del snapshot (gap en snapshot)
    detector.buffer_delta(
        BufferedDelta(
            first_update_id=50,
            final_update_id=60,
            prev_final_update_id=49,
            bids=[],
            asks=[],
            ts_exchange_ms=1000,
        )
    )
    # El snapshot es 150 (muy adelante, no hay solapamiento)
    aligned = detector.align_snapshot(snapshot_last_update_id=150)
    assert aligned is None
    assert detector.state == SyncState.WAITING_FOR_SNAPSHOT
