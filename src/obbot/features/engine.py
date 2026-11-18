"""Motor unificado de características de microestructura compartido entre vivo y backtest."""

import math
from collections import deque
from dataclasses import dataclass

from obbot.bus.base import EventBus
from obbot.common.clock import Clock, SystemClock
from obbot.common.events import (
    BookDeltaEvent,
    BookSnapshotEvent,
    FundingOIEvent,
    LiquidationEvent,
    TradeEvent,
)
from obbot.common.logging import get_logger
from obbot.features.cvd import CVDTracker
from obbot.features.delta import ExchangeDeltaTracker
from obbot.features.liquidations import LiquidationTracker
from obbot.features.obi import OBICalculator
from obbot.features.weights import DynamicWeightCalculator
from obbot.ingestion.orderbook.book import OrderBookL2

logger = get_logger("features.engine")


@dataclass(slots=True, frozen=True)
class FeatureVector:
    """Vector de características analíticas de microestructura sin sesgo de futuro."""

    symbol: str
    timestamp_ns: int
    # Delta local y estandarizado
    delta_by_exchange: dict[str, float]
    z_delta_by_exchange: dict[str, float]
    weights_by_exchange: dict[str, float]
    global_delta: float
    # CVD
    cvd_by_exchange: dict[str, float]
    global_cvd: float
    # OBI
    obi_by_exchange: dict[str, dict[int, float]]
    liquidity_persistence: float
    # Microestructura de precios y spreads
    mid_price: float
    spread_bps: float
    basis_bps_by_exchange: dict[str, float]  # Desviación respecto al mid de Binance
    realized_volatility: float
    # Liquidaciones y derivados
    liquidation_intensity: float
    funding_rate: float
    open_interest_usd: float

    @property
    def global_delta_z(self) -> float:
        """Alias para global_delta (suma ponderada de z-scores de delta)."""
        return self.global_delta

    @property
    def obi_10_levels(self) -> float:
        """Obtiene el OBI medio a 10 niveles entre los exchanges disponibles."""
        obis = [
            depth_map.get(10, 0.0) for depth_map in self.obi_by_exchange.values() if 10 in depth_map
        ]
        return sum(obis) / len(obis) if obis else 0.0


class FeatureEngine:
    """Motor de features en memoria idéntico para ejecución en tiempo real y backtest."""

    def __init__(
        self,
        symbol: str,
        exchanges: list[str] | None = None,
        clock: Clock | None = None,
        primary_exchange: str = "binance",
        primary_delta_window_sec: float = 60.0,
    ) -> None:
        self.symbol = symbol
        default_exchanges = ["binance", "okx", "bybit", "hyperliquid"]
        self.exchanges = [e.lower() for e in (exchanges or default_exchanges)]
        self.primary_exchange = primary_exchange.lower()
        self.primary_delta_window_sec = primary_delta_window_sec
        self.clock = clock or SystemClock()

        # Componentes por subsistema
        self.delta_trackers: dict[str, ExchangeDeltaTracker] = {
            ex: ExchangeDeltaTracker(ex, symbol) for ex in self.exchanges
        }
        self.weight_calculator = DynamicWeightCalculator(symbol)
        self.cvd_tracker = CVDTracker(symbol)
        self.obi_calculators: dict[str, OBICalculator] = {
            ex: OBICalculator() for ex in self.exchanges
        }
        self.liq_tracker = LiquidationTracker(symbol)
        self.books: dict[str, OrderBookL2] = {ex: OrderBookL2(ex, symbol) for ex in self.exchanges}

        # Volatilidad realizada sobre retornos de trades recientes
        self._price_history: deque[tuple[int, float]] = deque(maxlen=200)

        # Últimos valores de funding y OI
        self._latest_funding_rate: float = 0.0
        self._latest_oi_usd: float = 0.0

    def register_bus_handlers(self, bus: EventBus) -> None:
        """Suscribe el motor a todos los tópicos relevantes del bus de eventos."""
        pass  # En fase posterior se acopla como tarea de fondo si se requiere streaming directo

    def on_trade(self, trade: TradeEvent) -> None:
        """Actualiza delta, CVD, precios recientes y pesos tras la llegada de un trade."""
        ex = trade.exchange.lower()
        signed_notional = trade.notional_usd if trade.side == "BUY" else -trade.notional_usd

        # 1. Delta tracker
        if ex in self.delta_trackers:
            self.delta_trackers[ex].on_trade(trade.ts_recv_ns, signed_notional)

        # 2. CVD
        self.cvd_tracker.on_trade(ex, signed_notional)

        # 3. Dynamic weights
        self.weight_calculator.record_trade(ex, trade.ts_recv_ns, trade.notional_usd)

        # 4. Historial de precios para volatilidad realizada
        self._price_history.append((trade.ts_recv_ns, trade.price))

    def on_book_snapshot(self, snapshot: BookSnapshotEvent) -> None:
        """Actualiza el libro L2 y el cálculo de OBI tras un snapshot."""
        ex = snapshot.exchange.lower()
        if ex in self.books:
            bids = [(b.price, b.size) for b in snapshot.bids]
            asks = [(a.price, a.size) for a in snapshot.asks]
            self.books[ex].apply_snapshot(bids, asks, snapshot.last_update_id)

        if ex in self.obi_calculators:
            self.obi_calculators[ex].update_snapshot(snapshot.bids, snapshot.asks)

    def on_book_delta(self, delta: BookDeltaEvent) -> None:
        """Aplica deltas al libro de órdenes y actualiza OBI."""
        ex = delta.exchange.lower()
        if ex in self.books:
            bids = [(b.price, b.size) for b in delta.bids]
            asks = [(a.price, a.size) for a in delta.asks]
            self.books[ex].apply_delta(bids, asks, delta.final_update_id)

            # Actualizar OBI con los niveles top consolidados
            bids_top = self.books[ex].get_bids(20)
            asks_top = self.books[ex].get_asks(20)
            if ex in self.obi_calculators and bids_top and asks_top:
                self.obi_calculators[ex].update_snapshot(bids_top, asks_top)

    def on_liquidation(self, liq: LiquidationEvent) -> None:
        """Registra un evento de liquidación."""
        self.liq_tracker.record_liquidation(liq.ts_recv_ns, liq.side, liq.notional_usd)

    def on_funding_oi(self, event: FundingOIEvent) -> None:
        """Registra actualización de funding rate e Interés Abierto."""
        if event.funding_rate != 0.0:
            self._latest_funding_rate = event.funding_rate
        if event.open_interest_usd != 0.0:
            self._latest_oi_usd = event.open_interest_usd
            self.weight_calculator.record_oi(event.exchange, event.open_interest_usd)

    def _compute_realized_volatility(self) -> float:
        """Calcula la volatilidad realizada escalada a una ventana de 60 segundos."""
        if len(self._price_history) < 10:
            return 0.0

        prices = [p for _, p in self._price_history]
        returns = [
            math.log(prices[i] / prices[i - 1])
            for i in range(1, len(prices))
            if prices[i - 1] > 0.0
        ]
        if not returns:
            return 0.0

        mean_ret = sum(returns) / len(returns)
        var = sum((r - mean_ret) ** 2 for r in returns) / len(returns)
        tick_vol = math.sqrt(var)

        # Escalar a horizonte de 60 segundos según intensidad temporal de trades
        first_ts = self._price_history[0][0]
        last_ts = self._price_history[-1][0]
        duration_sec = max(0.5, (last_ts - first_ts) / 1_000_000_000.0)
        trades_per_sec = len(returns) / duration_sec
        scale_factor = math.sqrt(trades_per_sec * 60.0)
        return tick_vol * scale_factor

    def compute_feature_vector(self, current_ts_ns: int | None = None) -> FeatureVector:
        """
        Sintetiza el estado actual en un FeatureVector inmutable sin fuga de futuro.
        """
        now_ns = current_ts_ns if current_ts_ns is not None else self.clock.time_ns()

        # 1. Delta y z-scores locales
        deltas: dict[str, float] = {}
        z_scores: dict[str, float] = {}
        for ex, tracker in self.delta_trackers.items():
            deltas[ex] = tracker.get_delta(self.primary_delta_window_sec, now_ns)
            z_scores[ex] = tracker.get_z_score(self.primary_delta_window_sec, now_ns)

        # 2. Delta Global dinámico
        global_delta, weights = self.weight_calculator.compute_global_delta(z_scores, now_ns)

        # 3. OBI por exchange y persistencia de liquidez
        obi_map: dict[str, dict[int, float]] = {}
        persistence = 0.0
        for ex, obi_calc in self.obi_calculators.items():
            bids = self.books[ex].get_bids(20)
            asks = self.books[ex].get_asks(20)
            obi_map[ex] = {k: obi_calc.compute_weighted_obi(bids, asks, k) for k in [5, 10, 20]}
            if ex == self.primary_exchange:
                persistence = obi_calc.compute_persistence()

        # 4. Precios, spreads y basis
        primary_book = self.books.get(self.primary_exchange)
        primary_mid = primary_book.mid_price() if primary_book else None
        primary_spread = primary_book.spread() if primary_book else None

        mid_val = primary_mid if primary_mid is not None else 0.0
        spread_bps = (
            (primary_spread / mid_val * 10_000.0) if (primary_spread and mid_val > 0) else 0.0
        )

        basis_map: dict[str, float] = {}
        for ex, b in self.books.items():
            ex_mid = b.mid_price()
            if ex_mid and mid_val > 0:
                basis_map[ex] = ((ex_mid - mid_val) / mid_val) * 10_000.0
            else:
                basis_map[ex] = 0.0

        # 5. Liquidaciones y volatilidad
        liq_metrics = self.liq_tracker.get_metrics(now_ns)
        realized_vol = self._compute_realized_volatility()

        return FeatureVector(
            symbol=self.symbol,
            timestamp_ns=now_ns,
            delta_by_exchange=deltas,
            z_delta_by_exchange=z_scores,
            weights_by_exchange=weights,
            global_delta=global_delta,
            cvd_by_exchange=self.cvd_tracker.get_all(),
            global_cvd=self.cvd_tracker.get_global_cvd(),
            obi_by_exchange=obi_map,
            liquidity_persistence=persistence,
            mid_price=mid_val,
            spread_bps=spread_bps,
            basis_bps_by_exchange=basis_map,
            realized_volatility=realized_vol,
            liquidation_intensity=liq_metrics.events_per_minute,
            funding_rate=self._latest_funding_rate,
            open_interest_usd=self._latest_oi_usd,
        )
