# Política de Gestión de Riesgo Operativo y Financiero (Risk Policy)

## 1. Principios Fundamentales
Este documento define la política estricta de gestión de riesgo para el sistema institucional de trading cuantitativo de order flow. La preservación del capital y la integridad operativa prevalecen sobre cualquier oportunidad alfa o ganancia potencial:
1. **Zero Unchecked Execution**: Ninguna orden puede ser enviada a ningún exchange sin la validación previa, síncrona y bloqueante de `RiskGuard` y la emisión de un token de autorización criptográfico (`RiskAuthorizationToken`).
2. **Stop-Loss Innegociable y Atómico**: Toda posición abierta debe estar protegida en el exchange mediante un Algo Order (`STOP_MARKET`). Si tras una ejecución (fill) el stop no se confirma en un plazo máximo de **1500 ms**, el sistema liquida la posición inmediatamente con una orden a mercado `reduceOnly`.
3. **Margen Aislado y One-Way Mode Obligatorios**: Se prohíbe terminantemente el uso de margen cruzado (`CROSSED`) y el modo bidireccional (`HEDGE`). Toda posición se aísla por contrato con apalancamiento conservador (por defecto 3x, máximo 5x).
4. **Kill-Switch de 5 Capas**: Ante cualquier anomalía cuantitativa, de datos, de red o de ejecución, el kill-switch bloquea nuevas entradas y, si la gravedad lo exige, cancela órdenes abiertas o liquida inventario.

---

## 2. Las 5 Capas del Kill-Switch

```
+---------------------------------------------------------------------------------+
|                                 5-LAYER KILL-SWITCH                             |
+---------------------------------------------------------------------------------+
| Capa 1: Integridad y Frescura de Datos (Heartbeat, Sequence Gaps, Staleness)    |
| Capa 2: Conectividad y Latencia RTT (RTT > p99 * spike_factor, WS Timeouts)     |
| Capa 3: Riesgo Financiero y Drawdown (Max Drawdown, Max Loss/Trade, Notional)   |
| Capa 4: Riesgo Operativo y Exchange (Rechazos consecutivos, Fallo de Stop-Loss) |
| Capa 5: Intervención Manual / Externa (Disparo por operador o CLI)             |
+---------------------------------------------------------------------------------+
```

### Capa 1: Integridad y Frescura de Datos
- **Métricas Evaluadas**: Tiempo transcurrido desde el último evento en WS (`data_staleness_ms`), detección de huecos de secuencia en L2 book (`gap_detected`), tiempo de desincronización de libro (`book_desync_ms`).
- **Umbral**:
  - `max_data_staleness_ms`: 1000 ms (1 segundo sin eventos en feeds activos).
  - `max_book_desync_ms`: 300 ms sin sincronizar tras detectar hueco de secuencia.
- **Acción**: `BLOCK_NEW_ENTRIES`. Si persiste más de 5000 ms, escala a `CANCEL_OPEN_ORDERS`.

### Capa 2: Conectividad y Latencia RTT
- **Métricas Evaluadas**: Latencia de ida y vuelta (RTT) a los endpoints REST de Binance Futures y tiempos de respuesta de WebSockets.
- **Calibración**: Medición continua de RTT base durante el arranque y operación.
- **Umbral**:
  - `max_acceptable_rtt_ms`: 600 ms.
  - `rtt_spike_multiplier`: 3.0 × latencia base (ej. si base=80 ms, disparo a 240 ms).
- **Acción**: `BLOCK_NEW_ENTRIES`. No se envían órdenes de entrada en condiciones de degradación de red.

### Capa 3: Riesgo Financiero y Drawdown
- **Métricas Evaluadas**: PnL no realizado de posiciones abiertas, PnL realizado acumulado en la sesión de 24h, Drawdown porcentual respecto al balance inicial (`max_daily_drawdown_pct`), pérdida máxima por trade (`max_trade_loss_pct`).
- **Umbral**:
  - `max_daily_drawdown_pct`: 3.0% del balance de la cuenta.
  - `max_trade_loss_pct`: 1.0% del nocional de la posición.
  - `max_position_notional_usd`: 10,000 USD (por símbolo).
- **Acción**:
  - Al superar `max_trade_loss_pct`: Disparo de stop automático.
  - Al superar `max_daily_drawdown_pct`: `CLOSE_POSITIONS_EMERGENCY` y suspensión del bot por el resto de la sesión diaria.

### Capa 4: Riesgo Operativo y Exchange
- **Métricas Evaluadas**: Tasa de rechazos de órdenes devueltos por Binance REST (códigos de error HTTP 400, 429, o errores de negocio como `-2010`, `-1013`), fallos al colocar órdenes condicionales de Stop-Loss, desincronización entre el estado local de órdenes y el estado reportado por `/fapi/v1/openOrders`.
- **Umbral**:
  - `max_consecutive_order_rejections`: 3 rechazos consecutivos.
  - Fallo en confirmar Stop-Loss tras 1500 ms del fill de entrada.
- **Acción**:
  - 3 rechazos consecutivos: `CANCEL_OPEN_ORDERS` y `BLOCK_NEW_ENTRIES`.
  - Fallo de Stop-Loss: Cierre inmediato de la posición asociada con orden de mercado `reduceOnly`.

### Capa 5: Intervención Manual / Externa
- **Métricas Evaluadas**: Señal de operador humano vía CLI, señal de parada de emergencia (`SIGINT`, `SIGTERM`), o activación por watchdog externo.
- **Acción**: Inmediata y configurable según el nivel de urgencia (`CANCEL_OPEN_ORDERS` o `CLOSE_POSITIONS_EMERGENCY`).

---

## 3. Matriz de Estados y Acciones

| Acción | Envío de Nuevas Entradas | Cancelación de Órdenes Pendientes | Cierre de Posiciones Abiertas | Notificación / Log |
| :--- | :---: | :---: | :---: | :---: |
| `NONE` | Permitido | No | No | INFO |
| `BLOCK_NEW_ENTRIES` | **Bloqueado** | No | No | WARNING |
| `CANCEL_OPEN_ORDERS` | **Bloqueado** | **Sí (Inmediata)** | No | ERROR |
| `CLOSE_POSITIONS_EMERGENCY` | **Bloqueado** | **Sí (Inmediata)** | **Sí (Mercado reduceOnly)** | CRITICAL |

---

## 4. Validación Pre-Trade de `RiskGuard`

Antes de que cualquier orden llegue a la capa de ejecución, `RiskGuard` ejecuta síncronamente los siguientes checks atómicos:
1. **Kill-Switch Inactivo**: Verifica que ninguna de las 5 capas del Kill-Switch esté en estado disparado.
2. **Límite de Nocional por Orden**: `order_notional <= max_order_notional_usd` (por defecto 2,000 USD).
3. **Límite de Nocional de Posición**: `current_position + order_notional <= max_position_notional_usd` (por defecto 10,000 USD).
4. **Verificación de Margen y Modo**: Confirma `margin_type == ISOLATED` y `position_mode == ONE_WAY`.
5. **Apalancamiento Permitido**: `leverage <= max_leverage` (máximo 5x, defecto 3x).
6. **Protección Fat-Finger**: Si la orden es límite, comprueba que el precio no se desvíe más de `fat_finger_price_deviation_pct` (2%) del Mark Price actual.
7. **Rate Limiting Local**: Comprueba que la frecuencia de órdenes no exceda el límite operativo por minuto para evitar penalizaciones de peso en la API de Binance (`x-mbx-used-weight-1m`).
8. **Asignación de Token**: Si todos los checks pasan, se genera un `RiskAuthorizationToken(token_id, symbol, side, qty, price, expires_at_ns)` con una vigencia de 5000 ms. El ejecutor rechazará cualquier orden sin un token válido y no expirado.

---

## 5. Protocolo de Recuperación y Reseteo Post-Incidente
1. **Análisis de Causa Raíz**: Tras un disparo del Kill-Switch, el sistema no se reactiva automáticamente. Un operador debe inspeccionar los logs JSON estructurados.
2. **Reconciliación de Estado**: Ejecución manual o automática del reconciliador REST contra Binance (`/fapi/v1/positionRisk` y `/fapi/v1/openOrders`) para certificar inventario neutro.
3. **Comando de Desarme**: El kill-switch solo puede ser desarmado mediante una llamada explícita `reset(layer, operator_signature)`.
