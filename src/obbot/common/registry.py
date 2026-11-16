"""Registro de instrumentos y normalización dimensional multi-exchange."""

from dataclasses import dataclass, field
from decimal import ROUND_DOWN, ROUND_HALF_EVEN, Decimal
from typing import Any


@dataclass(slots=True, frozen=True)
class InstrumentSpec:
    """Especificación dimensional y de filtros para un activo canónico."""

    canonical_symbol: str
    base_asset: str
    quote_asset: str
    settlement_asset: str
    contract_type: str
    tick_size: Decimal
    step_size: Decimal
    min_qty: Decimal
    min_notional_usd: Decimal
    exchange_mapping: dict[str, str] = field(default_factory=dict)
    contract_multipliers: dict[str, Decimal] = field(default_factory=dict)
    weights_prior: dict[str, float] = field(default_factory=dict)

    def get_exchange_symbol(self, exchange: str) -> str:
        """Obtiene el símbolo nativo del exchange."""
        exchange_lower = exchange.lower()
        if exchange_lower not in self.exchange_mapping:
            raise KeyError(
                f"Símbolo no configurado para el exchange '{exchange}' en {self.canonical_symbol}"
            )
        return self.exchange_mapping[exchange_lower]

    def get_multiplier(self, exchange: str) -> Decimal:
        """Obtiene el multiplicador de contrato para convertir contratos a moneda base."""
        exchange_lower = exchange.lower()
        return self.contract_multipliers.get(exchange_lower, Decimal("1.0"))


class InstrumentRegistry:
    """Registro global de instrumentos que previene mezcla de unidades y normaliza cantidades."""

    def __init__(self) -> None:
        self._specs: dict[str, InstrumentSpec] = {}
        # Mapeo inverso: (exchange, native_symbol) -> canonical_symbol
        self._reverse_map: dict[tuple[str, str], str] = {}

    def register(self, spec: InstrumentSpec) -> None:
        """Registra una especificación de instrumento."""
        self._specs[spec.canonical_symbol] = spec
        for exchange, native_symbol in spec.exchange_mapping.items():
            self._reverse_map[(exchange.lower(), native_symbol.upper())] = spec.canonical_symbol

    def load_from_dict(self, data: dict[str, Any]) -> None:
        """Carga especificaciones desde el diccionario de configuración YAML."""
        symbols_dict = data.get("symbols", {})
        for canonical, spec_data in symbols_dict.items():
            multipliers = {
                k.lower(): Decimal(str(v))
                for k, v in spec_data.get("contract_multipliers", {}).items()
            }
            spec = InstrumentSpec(
                canonical_symbol=canonical,
                base_asset=str(spec_data["base_asset"]),
                quote_asset=str(spec_data["quote_asset"]),
                settlement_asset=str(spec_data.get("settlement_asset", spec_data["quote_asset"])),
                contract_type=str(spec_data.get("contract_type", "perpetual")),
                tick_size=Decimal(str(spec_data["tick_size"])),
                step_size=Decimal(str(spec_data["step_size"])),
                min_qty=Decimal(str(spec_data["min_qty"])),
                min_notional_usd=Decimal(str(spec_data["min_notional_usd"])),
                exchange_mapping={
                    k.lower(): str(v).upper()
                    for k, v in spec_data.get("exchange_mapping", {}).items()
                },
                contract_multipliers=multipliers,
                weights_prior={
                    k.lower(): float(v) for k, v in spec_data.get("weights_prior", {}).items()
                },
            )
            self.register(spec)

    def get(self, canonical_symbol: str) -> InstrumentSpec:
        """Retorna la especificación de un símbolo canónico."""
        if canonical_symbol not in self._specs:
            raise KeyError(f"Símbolo canónico no registrado: '{canonical_symbol}'")
        return self._specs[canonical_symbol]

    def to_canonical(self, exchange: str, native_symbol: str) -> str:
        """Convierte un símbolo nativo de exchange a su identificador canónico."""
        key = (exchange.lower(), native_symbol.upper())
        if key not in self._reverse_map:
            raise KeyError(
                f"Mapeo no encontrado para el exchange '{exchange}' y símbolo '{native_symbol}'"
            )
        return self._reverse_map[key]

    def to_exchange(self, exchange: str, canonical_symbol: str) -> str:
        """Convierte un símbolo canónico al formato nativo del exchange."""
        return self.get(canonical_symbol).get_exchange_symbol(exchange)

    def normalize_size(
        self, exchange: str, canonical_symbol: str, raw_qty: float | Decimal
    ) -> Decimal:
        """Convierte la cantidad cruda (contratos o lotes) a unidades de moneda base."""
        spec = self.get(canonical_symbol)
        multiplier = spec.get_multiplier(exchange)
        qty_dec = Decimal(str(raw_qty)) if not isinstance(raw_qty, Decimal) else raw_qty
        return qty_dec * multiplier

    def calculate_notional_usd(
        self,
        exchange: str,
        canonical_symbol: str,
        price: float | Decimal,
        raw_qty: float | Decimal,
    ) -> Decimal:
        """Calcula el valor nocional exacto en USD normalizado."""
        base_size = self.normalize_size(exchange, canonical_symbol, raw_qty)
        price_dec = Decimal(str(price)) if not isinstance(price, Decimal) else price
        return base_size * price_dec

    def round_price(
        self, canonical_symbol: str, price: float | Decimal, mode: str = "HALF_EVEN"
    ) -> Decimal:
        """Redondea un precio respetando estrictamente el tickSize."""
        spec = self.get(canonical_symbol)
        p = Decimal(str(price)) if not isinstance(price, Decimal) else price
        rounding = ROUND_DOWN if mode == "DOWN" else ROUND_HALF_EVEN
        return (p / spec.tick_size).quantize(Decimal("1"), rounding=rounding) * spec.tick_size

    def round_size(
        self, canonical_symbol: str, size: float | Decimal, mode: str = "DOWN"
    ) -> Decimal:
        """Redondea tamaño según el stepSize (por defecto truncamiento hacia abajo)."""
        spec = self.get(canonical_symbol)
        s = Decimal(str(size)) if not isinstance(size, Decimal) else size
        rounding = ROUND_DOWN if mode == "DOWN" else ROUND_HALF_EVEN
        return (s / spec.step_size).quantize(Decimal("1"), rounding=rounding) * spec.step_size

    def validate_order(
        self, canonical_symbol: str, price: Decimal, size: Decimal
    ) -> tuple[bool, str]:
        """Comprueba pre-condiciones de tick, step y notional mínimo."""
        spec = self.get(canonical_symbol)
        if size < spec.min_qty:
            return False, f"Cantidad {size} inferior al mínimo permitido {spec.min_qty}"

        notional = price * size
        if notional < spec.min_notional_usd:
            return (
                False,
                f"Nocional {notional} USD inferior al mínimo de {spec.min_notional_usd} USD",
            )

        # Verificar alineación con step size y tick size
        if (size % spec.step_size) != Decimal("0"):
            return False, f"Cantidad {size} no es múltiplo de stepSize {spec.step_size}"

        if (price % spec.tick_size) != Decimal("0"):
            return False, f"Precio {price} no es múltiplo de tickSize {spec.tick_size}"

        return True, ""
