# Diccionario de Datos y Esquemas del Lakehouse

## 1. Eventos Normalizados en el Bus de Mensajes

Todos los eventos transmitidos a través del bus (Redpanda o memoria interna) heredan de `EventBase` y garantizan trazabilidad con marcas de tiempo en nanosegundos (`ts_event_ns`, `ts_recv_ns`).

### `TradeEvent` (Tópico: `trades.v1`)
Representa una ejecución de mercado normalizada.
- `ts_event_ns`: `int` (Unix timestamp del evento generado por el exchange en ns).
- `ts_recv_ns`: `int` (Unix timestamp de recepción en el bot en ns).
- `exchange`: `str` (`"binance"`, `"okx"`, `"bybit"`, `"hyperliquid"`).
- `symbol`: `str` (Símbolo canónico, ej. `"BTC-USDT-PERP"`).
- `trade_id`: `str` (Identificador único asignado por el exchange).
- `price`: `float` (Precio de ejecución en USD).
- `size`: `float` (Cantidad ejecutada en activo base).
- `side`: `Literal["BUY", "SELL"]` (Lado del agresor/taker).
- `notional_usd`: `float` (Valor nocional = `price * size`).

### `BookSnapshotEvent` (Tópico: `book.v1`)
Instantánea completa o truncada del L2 Order Book sincronizado.
- `ts_event_ns`: `int` (Timestamp de emisión en ns).
- `ts_recv_ns`: `int` (Timestamp de recepción en ns).
- `exchange`: `str`.
- `symbol`: `str`.
- `bids`: `list[tuple[float, float]]` (Lista de `(precio, volumen)` ordenada descendentemente).
- `asks`: `list[tuple[float, float]]` (Lista de `(precio, volumen)` ordenada ascendentemente).
- `last_update_id`: `int` (ID de secuencia o versión del libro).

### `BookDeltaEvent` (Tópico: `book.v1`)
Actualización diferencial de niveles de precio del libro L2.
- `ts_event_ns`, `ts_recv_ns`, `exchange`, `symbol`.
- `bids`: `list[tuple[float, float]]` (Nivel de precio y nueva cantidad; cantidad 0.0 indica eliminación).
- `asks`: `list[tuple[float, float]]`.
- `first_update_id`: `int` (Binance `U`).
- `final_update_id`: `int` (Binance `u`).
- `prev_update_id`: `int` (Binance `pu`).

### `LiquidationEvent` (Tópico: `liquidations.v1`)
Liquidación forzada detectada en el exchange.
- `ts_event_ns`, `ts_recv_ns`, `exchange`, `symbol`.
- `side`: `Literal["BUY", "SELL"]` (BUY = liquidación de posición corta; SELL = liquidación de posición larga).
- `price`: `float` (Precio de liquidación o mark price).
- `size`: `float` (Cantidad liquidada en activo base).
- `notional_usd`: `float` (Valor nocional liquidado).
- *Nota metodológica*: En Binance, el stream `forceOrder` entrega un muestreo por segundo (sesgado a la baja).

### `FundingOIEvent` (Tópico: `funding_oi.v1`)
Tasa de financiación e Interés Abierto (Open Interest).
- `ts_event_ns`, `ts_recv_ns`, `exchange`, `symbol`.
- `funding_rate`: `float` (Tasa porcentual periódica).
- `funding_time_ns`: `int` (Timestamp de liquidación del funding).
- `open_interest`: `float` (Contratos o unidades base abiertas).
- `open_interest_usd`: `float` (Valor nocional del interés abierto).

---

## 2. Capas de Almacenamiento del Lakehouse

### Capa Bronze: `lakehouse-bronze/{exchange}/{channel}/{YYYY-MM-DD}/{HH}/*.json.zst`
- **Formato**: Streaming NDJSON comprimido en tiempo real con Zstandard (`zstd`, nivel 3).
- **Contenido**: Registro bit a bit de todos los eventos crudos normalizados sin descarte ni truncamiento.
- **Propósito**: Auditoría forense y fuente inmutable para el replay determinista.

### Capa Silver: `lakehouse-silver/{table_name}/{YYYY-MM-DD}/*.parquet`
- **Formato**: Apache Parquet con compresión Zstandard generado mediante Apache Arrow / Polars.
- **Tablas Tipadas**:
  - `trades`: Particionada por fecha y símbolo.
  - `book_snapshots`: Instantáneas periódicas para análisis de liquidez.
  - `liquidations`: Registro analítico de eventos forzados.
  - `funding_oi`: Series temporales de tasas y OI.

### Capa Gold: `lakehouse-gold/bars_1s/{YYYY-MM-DD}/*.parquet`
- **Formato**: Apache Parquet optimizado con DuckDB.
- **Esquema de Barras de Microestructura (1 segundo)**:
  - `symbol`: `Utf8`
  - `bucket_ts_sec`: `Int64` (Timestamp truncado al segundo entero)
  - `open`, `high`, `low`, `close`: `Float64`
  - `total_trades`: `Int64`
  - `total_volume`: `Float64` (Volumen total en activo base)
  - `total_notional_usd`: `Float64`
  - `buy_volume`: `Float64` (Volumen agresor comprador)
  - `sell_volume`: `Float64` (Volumen agresor vendedor)
  - `delta_volume`: `Float64` (`buy_volume - sell_volume`)
  - `delta_notional_usd`: `Float64`
  - `cvd`: `Float64` (Delta acumulado histórico en la sesión)
