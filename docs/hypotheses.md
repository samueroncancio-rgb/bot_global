# Hipótesis Cuantitativas de Microestructura (H1–H3)

Este documento define formalmente las tres hipótesis empíricas fundamentales del sistema. Como ingenieros cuantitativos escépticos, **no asumimos que estas hipótesis sean válidas a priori**. La infraestructura está diseñada para medir, auditar y falsar rigurosamente cada una antes de comprometer capital.

---

## Hipótesis 1 (H1): Anticipación del Delta Global Dinámico (Lead-Lag Effect)

### Enunciado Teórico
El flujo de órdenes agresor agregado entre múltiples exchanges ($\Delta_{\text{global}}$), ponderado por cuota de volumen y Open Interest, anticipa la dirección del precio a corto plazo ($\tau \in [100\text{ ms}, 2\text{ s}]$) en Binance Futures con mayor ratio señal-ruido que el delta local de Binance por separado.

$$\Delta_{\text{global}}(t) = \sum_{i} w_i(t) \cdot z(\Delta_i(t))$$

### Métrica de Falsabilidad
1. **Information Coefficient (IC):** Correlación de Spearman entre $\Delta_{\text{global}}(t)$ y el retorno forward $R(t, t+\tau) = \frac{P_{\text{mid}}(t+\tau) - P_{\text{mid}}(t)}{P_{\text{mid}}(t)}$.
2. **Criterio de Falsación:**
   * La hipótesis se considera **falsada** si:
     $$\text{IC}_{\text{global}} \le \text{IC}_{\text{binance\_local}}$$
     o si el $p$-valor del $t$-statistic asociado a la correlación excede $0.01$ tras ajustar por heterocedasticidad y autocorrelación (Newey-West con $L=5$ lags).
   * En caso de falsación, el modelo descarta la agregación multi-exchange y revierte a la lectura del libro local para evitar costes innecesarios de red.

---

## Hipótesis 2 (H2): Desbalance de Libro (OBI) y Persistencia de Liquidez

### Enunciado Teórico
Un desbalance de libro ponderado por distancia exponencial al mid ($\text{OBI}_K$) solo tiene poder predictivo sobre el micro-price drift cuando la persistencia de liquidez ($\rho_1 \ge 0.50$) confirma que no se trata de cotizaciones ficticias (*quote flickering* o *spoofing* transitorio).

$$\text{OBI}_K = \frac{\sum_{k=1}^K v_{b,k} e^{-\lambda d_{b,k}} - \sum_{k=1}^K v_{a,k} e^{-\lambda d_{a,k}}}{\sum_{k=1}^K v_{b,k} e^{-\lambda d_{b,k}} + \sum_{k=1}^K v_{a,k} e^{-\lambda d_{a,k}}}$$

### Métrica de Falsabilidad
1. **Regresión Condicionada:**
   $$\mathbb{E}[R(t, t+\tau) \mid \text{OBI}_K, \rho_1]$$
2. **Criterio de Falsación:**
   * La hipótesis se considera **falsada** si la interacción $\beta_{\text{OBI} \times \rho_1}$ en la regresión:
     $$R(t, t+\tau) = \alpha + \beta_1 \text{OBI}_K + \beta_2 \rho_1 + \beta_3 (\text{OBI}_K \times \rho_1) + \varepsilon$$
     no es estadísticamente significativa ($\beta_3 \le 0$ o $t < 2.5$).
   * Si la persistencia no mejora la predictibilidad, el cálculo de autocorrelación se desactiva para ahorrar ciclos de CPU.

---

## Hipótesis 3 (H3): Ráfagas de Liquidación y Overshoot de Microestructura

### Enunciado Teórico
Ráfagas concentradas de órdenes de liquidación forzada en Binance, OKX o Bybit consumen agresivamente la liquidez pasiva disponible en el libro, provocando una deformación temporal del precio (*overshoot*) seguida de una reversión rápida a la media cuando el flujo forzado cesa.

### Métrica de Falsabilidad
1. **Curva de Impacto de Evento (Event Study):**
   Medición del retorno medio condicional acumulado pre y post evento de liquidación masiva ($I_{\text{liq}} > P_{99}$):
   $$\text{CAR}(t - 5\text{ s}, t + 10\text{ s})$$
2. **Criterio de Falsación:**
   * La hipótesis se considera **falsada** si tras un choque de liquidaciones el precio continúa en la dirección de la liquidación sin reversión mínima del $30\%$ del impacto en los siguientes 5 segundos ($\text{Reversión} < 0.30 \cdot \text{Impacto}$).
   * Si no hay reversión, el flujo de liquidación se trata como información direccional persistente (momentum) en lugar de una anomalía de agotamiento de inventario.
