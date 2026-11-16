"""Tests unitarios para la estructura de datos OrderBookL2."""

from obbot.ingestion.orderbook.book import OrderBookL2


def test_orderbook_snapshot_and_sorting() -> None:
    book = OrderBookL2(exchange="binance", symbol="BTC-USDT-PERP")
    assert book.is_synchronized is False
    assert book.best_bid() is None
    assert book.best_ask() is None

    bids = [(50000.0, 1.0), (49990.0, 2.0), (50010.0, 0.5)]
    asks = [(50020.0, 1.5), (50030.0, 3.0), (50015.0, 0.8)]

    book.apply_snapshot(bids, asks, last_update_id=100)
    assert book.is_synchronized is True
    assert book.last_update_id == 100

    # Best bid debe ser el mayor precio (50010.0)
    best_b = book.best_bid()
    assert best_b is not None
    assert best_b[0] == 50010.0
    assert best_b[1] == 0.5

    # Best ask debe ser el menor precio (50015.0)
    best_a = book.best_ask()
    assert best_a is not None
    assert best_a[0] == 50015.0
    assert best_a[1] == 0.8

    # Mid price y spread
    assert book.mid_price() == (50010.0 + 50015.0) / 2.0
    assert book.spread() == 50015.0 - 50010.0
    assert book.is_crossed() is False


def test_orderbook_delta_and_removal() -> None:
    book = OrderBookL2(exchange="binance", symbol="BTC-USDT-PERP")
    book.apply_snapshot([(50000.0, 1.0)], [(50010.0, 1.0)], last_update_id=10)

    # Actualizar nivel y añadir uno nuevo
    book.apply_delta(
        bids=[(50000.0, 2.5), (50005.0, 0.5)],
        asks=[(50010.0, 0.0), (50020.0, 1.0)],  # Eliminación de 50010.0
        update_id=11,
    )

    assert book.last_update_id == 11
    # 50005.0 es ahora el nuevo best bid
    best_b = book.best_bid()
    assert best_b is not None
    assert best_b[0] == 50005.0
    assert best_b[1] == 0.5

    # 50010.0 fue eliminado con size=0; nuevo best ask es 50020.0
    best_a = book.best_ask()
    assert best_a is not None
    assert best_a[0] == 50020.0
    assert best_a[1] == 1.0


def test_orderbook_crossed_detection_and_reset() -> None:
    book = OrderBookL2(exchange="binance", symbol="BTC-USDT-PERP")
    book.apply_snapshot([(50000.0, 1.0)], [(50010.0, 1.0)], last_update_id=1)
    assert book.is_crossed() is False

    # Insertar bid mayor que el ask (libro cruzado)
    book.apply_delta(bids=[(50015.0, 1.0)], asks=[])
    assert book.is_crossed() is True

    book.reset()
    assert book.is_synchronized is False
    assert book.best_bid() is None
    assert book.best_ask() is None


def test_orderbook_top_n_levels() -> None:
    book = OrderBookL2(exchange="okx", symbol="BTC-USDT-PERP")
    bids = [(float(p), 1.0) for p in range(50000, 50030)]
    asks = [(float(p), 1.0) for p in range(50040, 50070)]
    book.apply_snapshot(bids, asks, last_update_id=1)

    top5_bids = book.get_bids(n=5)
    assert len(top5_bids) == 5
    # Orden descendente
    assert [x.price for x in top5_bids] == [50029.0, 50028.0, 50027.0, 50026.0, 50025.0]

    top5_asks = book.get_asks(n=5)
    assert len(top5_asks) == 5
    # Orden ascendente
    assert [x.price for x in top5_asks] == [50040.0, 50041.0, 50042.0, 50043.0, 50044.0]
