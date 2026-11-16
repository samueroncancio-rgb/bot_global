# Manual Operativo del Sistema (Runbook)

## 1. Modos Operativos y Pre-Flight Checks
El bot soporta 4 modos de operación configurables vía variable de entorno `BOT_MODE`:
- `replay`: Replay determinista de datos históricos Bronze con reloj simulado.
- `paper`: Modo predeterminado. Conexión a feeds de datos de mercado en vivo, simulación local de órdenes.
- `testnet`: Conexión a Binance Futures Testnet (`https://testnet.binancefuture.com` y `wss://stream.binancefuture.com`).
- `live`: Conexión real con capital. **Desactivado por defecto**; requiere autorización expresa del usuario.

### Pre-Flight Checklist
Antes de iniciar el sistema, verificar:
1. Infraestructura local levantada:
   ```bash
   docker compose up -d
   docker compose ps
   ```
   Comprobar que Redpanda (`localhost:19092`), MinIO (`localhost:9000`), Prometheus (`localhost:9090`) y Grafana (`localhost:3000`) estén en estado `Up`.
2. Archivo `.env` configurado con claves válidas (sin permisos de retiro ni transferencia).
3. Calibración de latencia base a Binance:
   El sistema medirá automáticamente el RTT medio inicial a `/fapi/v1/time`.
4. Comprobar que los puertos de métricas (`8000`) y salud (`8080`) estén libres.

---

## 2. Procedimiento de Arranque
Para iniciar el sistema en modo `paper` con entorno virtual:
```bash
source .venv/bin/activate
python -m obbot.main --mode paper --symbol BTCUSDT
```

Logs esperados al arranque:
- `binance_rest.clock_synced`: Deriva calculada contra Binance.
- `binance_adapter.connected`: Conexión a `/public` (depth) y `/market` (aggTrade).
- `okx_adapter.connected`: Conexión y suscripción a feeds de OKX.
- `bybit_adapter.connected`: Conexión y suscripción a feeds de Bybit.
- `hyperliquid_adapter.connected`: Conexión y ping loop activo en Hyperliquid.
- `obs.metrics_server_started`: Puerto 8000 activo.
- `obs.health_server_started`: Puerto 8080 activo (`/healthz`, `/readyz`).

---

## 3. Procedimiento de Parada Limpia (Graceful Shutdown)
Ante una señal `SIGINT` (Ctrl+C) o `SIGTERM`:
1. `RiskGuard` pasa inmediatamente a `BLOCK_NEW_ENTRIES`.
2. El `OrderManager` cancela todas las órdenes pendientes tipo Maker (GTX/LIMIT) abiertas en Binance.
3. Se verifica el estado de las posiciones abiertas y sus Algo Stop-Loss asociados.
4. Se desconectan supervisadamente los WebSockets (Binance, OKX, Bybit, Hyperliquid).
5. Se elimina el `listenKey` en Binance (`DELETE /fapi/v1/listenKey`).
6. El escritor Bronze vacía sus buffers en memoria a disco/S3 con compresión Zstandard.
7. El bus de eventos se detiene limpiamente.

---

## 4. Matriz de Respuestas ante Disparos del Kill-Switch

### Alerta: Capa 1 (DATA_INTEGRITY)
- **Causa**: Más de 1000 ms sin recibir eventos en los feeds de datos, o desincronización del Order Book por más de 300 ms tras un hueco de secuencia.
- **Acción Automática**: `BLOCK_NEW_ENTRIES`. Si persiste más de 5000 ms, escala a `CANCEL_OPEN_ORDERS`.
- **Procedimiento del Operador**:
  1. Revisar estado de conectividad a los exchanges (`curl -I https://fapi.binance.com/fapi/v1/ping`).
  2. Verificar los logs del supervisor de WebSocket (`WsSupervisor`).
  3. Si el orderbook de Binance se desincronizó, el adaptador solicitará automáticamente un nuevo snapshot REST (`/fapi/v1/depth?limit=1000`).
  4. Una vez restablecido el flujo continuo, desarmar con `kill_switch.reset_layer(KillSwitchLayer.DATA_INTEGRITY, "Flujo normalizado")`.

### Alerta: Capa 2 (NETWORK_CONNECTIVITY)
- **Causa**: Latencia RTT superior a 600 ms o spike de latencia > 3× la latencia base calculada.
- **Acción Automática**: `BLOCK_NEW_ENTRIES`.
- **Procedimiento del Operador**:
  1. Verificar latencia de red hacia los servidores de Binance.
  2. Comprobar congestión en la interfaz de red local o saturación de ancho de banda.
  3. Desarmar tras certificar estabilización de RTT por debajo de 200 ms.

### Alerta: Capa 3 (FINANCIAL_DRAWDOWN)
- **Causa**: Drawdown diario acumulado $\ge$ 3.0% o pérdida en una operación abierta $\ge$ 1.0%.
- **Acción Automática**: `CLOSE_POSITIONS_EMERGENCY` para drawdown diario crítico; `BLOCK_NEW_ENTRIES` para pérdida por trade.
- **Procedimiento del Operador**:
  1. **DETENER LA OPERACIÓN**: No reactivar el bot hasta completar auditoría cuantitativa.
  2. Ejecutar el reconciliador REST para certificar balance e inventario plano.
  3. Correr el informe post-sesión del módulo LLM (`generate_end_of_session_report`).

### Alerta: Capa 4 (OPERATIONAL_EXCHANGE)
- **Causa**: 3 rechazos consecutivos de órdenes por Binance, o fallo/timeout (> 1500 ms) al adjuntar Algo Stop-Loss tras un fill de entrada.
- **Acción Automática**: `CANCEL_OPEN_ORDERS` ante rechazos; `CLOSE_POSITIONS_EMERGENCY` a mercado `reduceOnly` si falló el Stop-Loss.
- **Procedimiento del Operador**:
  1. Comprobar códigos de error de Binance en los logs estructurados (ej. error `-2010 Account has insufficient balance`, `-1013 Filter failure`, `-4120`).
  2. Verificar si se usó erróneamente `/fapi/v1/order` para órdenes condicionales en lugar de `/fapi/v1/algoOrder`.
  3. Verificar saldo de margen disponible en Binance USDⓈ-M.

### Alerta: Capa 5 (MANUAL)
- **Causa**: Activación manual por comando del operador.
- **Acción Automática**: `CLOSE_POSITIONS_EMERGENCY`.
- **Procedimiento**: El operador debe enviar orden de reset explícita una vez superada la contingencia.

---

## 5. Procedimiento de Reconciliación Manual
Si se sospecha desincronización entre memoria local y Binance:
```python
from obbot.execution.reconciler import StateReconciler

# Ejecuta verificación síncrona contra endpoints del exchange
report = await reconciler.reconcile_orders()
positions = await reconciler.reconcile_positions(portfolio)
```
El principio "Gana el exchange" asegurará que el inventario reportado por `/fapi/v1/positionRisk` sea la única fuente de verdad.
