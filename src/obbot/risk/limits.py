"""Definición de modelos de límites de riesgo, estado de portafolio y tokens de autorización."""

import uuid
from dataclasses import dataclass
from typing import Literal


@dataclass(frozen=True)
class RiskLimits:
    """Configuración institucional de límites cuantitativos y operativos de riesgo."""

    margin_type: Literal["ISOLATED", "CROSSED"] = "ISOLATED"
    position_mode: Literal["ONE_WAY", "HEDGE"] = "ONE_WAY"
    default_leverage: int = 3
    max_leverage: int = 5

    max_order_notional_usd: float = 2000.0
    max_position_notional_usd: float = 10000.0
    max_daily_drawdown_pct: float = 0.05  # 5% del balance inicial del día (según risk.yaml)
    max_trade_loss_pct: float = 0.02  # 2% por operación (según risk.yaml)
    fat_finger_price_deviation_pct: float = 0.02  # 2% máximo respecto al mark price
    max_orders_per_minute: int = 60

    # Latencia y calidad de datos
    max_data_staleness_ms: float = 1000.0
    max_book_desync_ms: float = 300.0
    max_acceptable_rtt_ms: float = 600.0
    rtt_spike_multiplier: float = 3.0
    max_consecutive_order_rejections: int = 3
    stop_loss_attach_timeout_ms: float = 1500.0


@dataclass(frozen=True)
class PositionState:
    """Estado de posición para un símbolo en Binance USDⓈ-M Futures."""

    symbol: str
    size: float  # Positivo para LONG, negativo para SHORT, 0.0 para neutral
    entry_price: float
    mark_price: float
    unrealized_pnl: float
    leverage: int
    margin_type: str = "ISOLATED"

    @property
    def notional_usd(self) -> float:
        """Valor nocional absoluto actual de la posición en USD."""
        return abs(self.size * self.mark_price)


@dataclass
class PortfolioState:
    """Estado agregado del portafolio y balance de la cuenta."""

    initial_daily_balance: float
    current_wallet_balance: float
    realized_daily_pnl: float = 0.0
    positions: dict[str, PositionState] | None = None

    def __post_init__(self) -> None:
        if self.positions is None:
            self.positions = {}

    @property
    def total_unrealized_pnl(self) -> float:
        """Suma de PnL no realizado de todas las posiciones abiertas."""
        if not self.positions:
            return 0.0
        return sum(pos.unrealized_pnl for pos in self.positions.values())

    @property
    def total_equity(self) -> float:
        """Patrimonio neto total (wallet balance + unrealized PnL)."""
        return self.current_wallet_balance + self.total_unrealized_pnl

    @property
    def daily_drawdown_pct(self) -> float:
        """Pérdida porcentual acumulada en la sesión con respecto al balance inicial."""
        if self.initial_daily_balance <= 0.0:
            return 0.0
        drawdown = (self.initial_daily_balance - self.total_equity) / self.initial_daily_balance
        return max(0.0, drawdown)


@dataclass(frozen=True)
class OrderRequest:
    """Solicitud de orden generada por una estrategia antes de validación pre-trade."""

    symbol: str
    side: Literal["BUY", "SELL"]
    order_type: Literal["LIMIT", "MARKET"]
    quantity: float
    price: float = 0.0  # 0.0 para órdenes MARKET
    reduce_only: bool = False
    time_in_force: Literal["GTC", "IOC", "FOK", "GTX"] = "GTC"
    stop_loss_price: float | None = None
    take_profit_price: float | None = None
    client_order_id: str | None = None


@dataclass(frozen=True)
class RiskAuthorizationToken:
    """Token criptográfico de autorización previa emitido por RiskGuard."""

    token_id: str
    symbol: str
    side: str
    quantity: float
    price: float
    created_at_ns: int
    expires_at_ns: int

    @classmethod
    def generate(
        cls,
        symbol: str,
        side: str,
        quantity: float,
        price: float,
        created_at_ns: int,
        validity_duration_ns: int = 5_000_000_000,
    ) -> "RiskAuthorizationToken":
        """Genera un nuevo token de autorización con validez temporal acotada."""
        return cls(
            token_id=f"RISK-{uuid.uuid4().hex[:12].upper()}",
            symbol=symbol,
            side=side,
            quantity=quantity,
            price=price,
            created_at_ns=created_at_ns,
            expires_at_ns=created_at_ns + validity_duration_ns,
        )


@dataclass(frozen=True)
class RiskDecision:
    """Resultado de la evaluación pre-trade de RiskGuard."""

    approved: bool
    reason: str
    token: RiskAuthorizationToken | None = None
