# Orderflow Bot: Sistema Institucional de Trading Cuantitativo

Sistema de alta disponibilidad y baja latencia para trading cuantitativo y análisis de microestructura de mercado (order flow) en cripto-derivados, con ingesta multi-exchange en tiempo real (**Binance, OKX, Bybit, Hyperliquid**), Lakehouse local/S3, motor de features unificado, pre-trade `RiskGuard`, Kill-Switch de 5 capas y ejecución en **Binance USDⓈ-M Futures**.

---

## 1. Guía Rápida de Pruebas

### A. Ejecutar la Suite Completa de Tests Automatizados (92 tests)
El proyecto cuenta con 92 tests unitarios y de integración con cobertura del 74%+:

```bash
# Activar el entorno virtual
source .venv/bin/activate

# Ejecutar todos los tests con reporte de cobertura
pytest --cov=obbot -v
```

### B. Ejecutar el Test de Integración End-to-End
Valida el ciclo completo (ingesta sintética L2 → motor de features → señal de estrategia → pre-trade check de RiskGuard → ejecución en Paper Engine → adjunto atómico de Stop-Loss → reconciliación REST):

```bash
pytest tests/integration/test_end_to_end.py -v -s
```

### C. Probar Módulos Específicos
```bash
# 1. Probar el Kill-Switch de 5 capas y RiskGuard
pytest tests/unit/test_kill_switch.py tests/unit/test_risk_guard.py -v

# 2. Probar la máquina de estados de órdenes y el watchdog de Stop-Loss (1500 ms)
pytest tests/unit/test_order_manager.py tests/unit/test_algo_orders.py -v

# 3. Probar el Lakehouse (Bronze Zstandard, Silver/Gold DuckDB/Polars y Replay)
pytest tests/unit/test_bronze_writer.py tests/unit/test_silver_job.py tests/unit/test_gold_job.py tests/unit/test_replay.py -v

# 4. Probar el motor de features (Delta Global, OBI multiescala, CVD)
pytest tests/unit/test_delta.py tests/unit/test_weights.py tests/unit/test_obi.py tests/unit/test_cvd.py -v
```

### D. Verificación Estática (Tipado y Estilo)
```bash
# Verificación de formato y linter con Ruff
ruff format --check .
ruff check .

# Tipado estricto con Mypy (0 errores en 94 archivos fuente)
mypy src tests
```

---

## 2. Pruebas en Vivo (Modo Paper Trading)

El modo `paper` permite correr el bot contra los WebSockets en tiempo real de Binance, OKX, Bybit e Hyperliquid, calculando métricas reales y simulando fills de órdenes sin arriesgar capital:

### Paso 1: Iniciar Infraestructura Local (Opcional si usas memoria/filesystem)
```bash
docker compose up -d
```
Servicios disponibles:
- **MinIO**: `http://localhost:9001` (usuario: `minioadmin`, contraseña: `minioadmin`)
- **Prometheus**: `http://localhost:9090`
- **Grafana**: `http://localhost:3000` (usuario: `admin`, contraseña: `admin`)
- **Redpanda**: `localhost:19092`

### Paso 2: Iniciar el Bot en Modo Paper
```bash
python -m obbot.main --mode paper --symbol BTC-USDT-PERP
```

### Paso 3: Inspeccionar Salud y Métricas en Tiempo Real
Desde otra terminal:
```bash
# Sonda Liveness
curl -s http://localhost:8080/healthz

# Sonda Readiness
curl -s http://localhost:8080/readyz

# Métricas de microestructura de Prometheus
curl -s http://localhost:8000 | grep obbot_
```

---

## 3. Documentación Técnica del Sistema
- [docs/runbook.md](docs/runbook.md): Manual operativo paso a paso (arranque, parada limpia, matriz de respuesta ante alertas y reconciliación).
- [docs/risk_policy.md](docs/risk_policy.md): Política de riesgo, límites institucionales y especificación de las 5 capas del Kill-Switch.
- [docs/architecture.md](docs/architecture.md): Diagrama Mermaid de arquitectura, segregación de feeds WS y flujo de ejecución.
- [docs/data_dictionary.md](docs/data_dictionary.md): Esquemas de eventos del bus y catálogo de particiones Bronze, Silver y Gold.
- [docs/hypotheses.md](docs/hypotheses.md): Especificación formal de las hipótesis cuantitativas falsables H1, H2 y H3.
