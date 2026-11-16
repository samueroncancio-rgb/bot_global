# Arquitectura del Sistema de Order Flow Multi-Exchange

## 1. Visión General de la Arquitectura
El sistema es una plataforma institucional de trading cuantitativo de baja latencia construida en Python 3.12 (`asyncio` + `uvloop`), diseñada bajo principios escépticos de microestructura de mercado, seguridad ineludible y reproducibilidad matemática:

```mermaid
flowchart TD
    subgraph INGESTION ["Capa de Ingesta Multi-Exchange"]
        B_PUB["Binance WS /public\n(depth@100ms, bookTicker)"]
        B_MKT["Binance WS /market\n(aggTrade, markPrice, forceOrder)"]
        OKX_WS["OKX WS Public\n(books, trades, open-interest)"]
        BYB_WS["Bybit WS Linear\n(orderbook.50, publicTrade)"]
        HL_WS["Hyperliquid WS L1\n(l2Book, trades)"]
        GAP["Gap Detector & L2 Order Books\n(SortedDict, U/u/pu validation)"]
        B_PUB --> GAP
        OKX_WS --> GAP
        BYB_WS --> GAP
        HL_WS --> GAP
    end

    subgraph BUS ["Bus de Eventos Normalizados"]
        REGISTRY["Instrument Registry\n(BTC-USDT-PERP)"]
        EP_BUS["Event Bus\n(Redpanda / Memory)\ntrades.v1 · book.v1 · liquidations.v1"]
        GAP --> REGISTRY --> EP_BUS
        B_MKT --> REGISTRY
    end

    subgraph LAKEHOUSE ["Lakehouse & Replay"]
        BRONZE["Bronze Layer\n(NDJSON + Zstandard)"]
        SILVER["Silver Layer\n(Parquet ZSTD / Polars)"]
        GOLD["Gold Layer\n(1s OHLCV + Order Flow DuckDB)"]
        REPLAY["Deterministic Replay Engine\n(PriorityQueue ts_recv_ns + SimulationClock)"]
        EP_BUS --> BRONZE --> SILVER --> GOLD
        BRONZE -.-> REPLAY -.-> EP_BUS
    end

    subgraph FEATURES ["Motor de Features de Microestructura"]
        FEAT_ENG["Feature Engine\n(Sin fuga de futuro)"]
        DELTA["Delta por Exchange & Z-Score Rodante"]
        WEIGHTS["Pesos Dinámicos w_i(t)\n(Símplex Acotado [0.05, 0.60] + EMA)"]
        GLOBAL_DELTA["Delta Global Ponderado: Σ w_i · z(delta_i)"]
        OBI["OBI Multi-Profundidad K={5,10,20}\n(Ponderación e^-λd + Persistencia)"]
        CVD_MOD["CVD por Exchange & Global"]
        LIQ_MOD["Intensidad de Liquidaciones"]
        EP_BUS --> FEAT_ENG
        FEAT_ENG --> DELTA & WEIGHTS & OBI & CVD_MOD & LIQ_MOD
        DELTA & WEIGHTS --> GLOBAL_DELTA
    end

    subgraph DECISION ["Ruta de Decisión y Riesgo"]
        STRAT["Strategy Engine\n(BaselineOrderFlowStrategy)"]
        RISK["RiskGuard Pre-Trade Proxy\n(Límites, Fat-Finger, Rate Limiter)"]
        KS["Kill-Switch de 5 Capas\n(Datos, RTT, PnL, Operativo, Manual)"]
        TOKEN["RiskAuthorizationToken\n(Criptográfico, Validez 5s)"]
        FEAT_ENG --> STRAT
        STRAT --> RISK
        KS --> RISK
        RISK -->|Aprobado| TOKEN
    end

    subgraph EXECUTION ["Ejecución Binance USDⓈ-M"]
        OM["Order Manager\n(Máquina de Estados)"]
        REST["Binance REST Client\n(HMAC-SHA256, Clock Drift Sync)"]
        ALGO["Algo Order Client\n(STOP_MARKET, TAKE_PROFIT_MARKET)"]
        WATCHDOG["Stop-Loss Watchdog (1500ms)\n(Fallo → Cierre de Emergencia MARKET reduceOnly)"]
        PRIV_WS["Binance Private WS /private\n(ORDER_TRADE_UPDATE, listenKey keepalive)"]
        RECON["State Reconciler\n(Gana el exchange)"]
        TOKEN --> OM
        OM --> REST & ALGO
        OM --> WATCHDOG
        PRIV_WS --> OM
        RECON --> OM
    end

    subgraph OFFLINE ["Observabilidad y Auditoría LLM"]
        PROM["Prometheus Metrics (:8000)"]
        HEALTH["HealthServer /healthz & /readyz (:8080)"]
        LLM["Módulo LLM Offline\n(Gemini SDK / OpenAI-Compat LiteLLM)"]
    end
```

---

## 2. Decisiones Clave de Diseño y Reglas Ineludibles

### Segregación Categórica de WebSockets de Binance
Tras la retirada definitiva de las URLs legacy `/ws` y `/stream` el 23 de abril de 2026, el sistema segrega estrictamente sus conexiones:
1. `wss://fstream.binance.com/public`: Exclusivo para streams de profundidad de libro (`depth@100ms`) y mejores cotizaciones (`bookTicker`).
2. `wss://fstream.binance.com/market`: Exclusivo para trades agregados (`aggTrade`), mark price (`markPrice@1s`) y liquidaciones forzadas (`forceOrder`).
3. `wss://fstream.binance.com/private`: Exclusivo para el User Data Stream con `listenKey` autenticado.

Cualquier intento de suscribir canales de mercado en la conexión `/public` o en `/private` es interceptado y bloqueado con error categórico `BinanceCategoryViolationError`.

### Órdenes Condicionales y Protección Atómica
1. **Rutas Oficiales**: Todas las órdenes condicionales (`STOP_MARKET`, `TAKE_PROFIT_MARKET`) se dirigen exclusivamente a `POST /fapi/v1/algoOrder`. El endpoint `/fapi/v1/order` devuelve el error `-4120 (STOP_ORDER_SWITCH_ALGO)` si se intentan enviar allí.
2. **Watchdog de Stop-Loss Innegociable**: Al confirmarse un fill de entrada, el `OrderManager` inicia un temporizador de 1500 ms para adjuntar la orden condicional en Binance. Si transcurren más de 1500 ms sin confirmación o el endpoint falla, se dispara la Capa 4 del Kill-Switch y se emite inmediatamente una orden a mercado con `reduceOnly=True` para cerrar la posición de inmediato.

### Proyección Acotada al Símplex en Pesos Dinámicos
Para evitar que un exchange domine el cálculo del Delta Global o que otro quede anulado, los pesos $w_i(t)$ se calculan mediante una proyección euclidiana sobre el símplex unitario $\sum w_i = 1.0$ con cotas estrictas $w_i \in [0.05, 0.60]$ resuelta por bisección monótona en tiempo $O(N \log \frac{1}{\epsilon})$.
