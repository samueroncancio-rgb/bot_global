# Reglas permanentes del proyecto orderflow-bot

## Idioma y estilo
- Código y nombres en español; comentarios, docs y commits en español.
- Python 3.12, tipado estricto en ingestion/, execution/ y risk/. ruff + mypy + pytest deben pasar antes de dar una tarea por terminada.

## Verificación obligatoria contra documentación oficial
- ANTES de implementar cualquier endpoint, stream o parámetro de un exchange, abre con el navegador su documentación oficial, confirma ruta, parámetros y límites vigentes, y deja la URL como comentario en el código.
- Si una fuente de terceros contradice la oficial, gana la oficial. Si no puedes verificar algo, escribe `# TODO-VERIFY:` y avísame; no inventes.

## Seguridad (innegociable)
- Nunca escribas, imprimas ni registres claves, secretos, firmas ni listen keys. Solo desde variables de entorno / `.env` (en .gitignore) con `.env.example` sin valores.
- Las claves de API de Binance serán SIN permiso de retiro. No implementes ninguna función de retiro/transferencia.
- Modo por defecto: `paper`. Modo `live` requiere: BOT_MODE=live + flag CLI `--i-understand-live-risk` + confirmación interactiva. Nunca lo actives tú.
- No ejecutes comandos destructivos (rm -rf fuera del repo, docker system prune, etc.) sin pedir confirmación.

## Reglas de arquitectura
- Toda cantidad pasa por el Instrument Registry (moneda base / notional USD). Prohibido sumar cantidades crudas de exchanges distintos.
- Cada evento lleva ts_exchange_ms y ts_recv_ns. Guarda el mensaje crudo en bronze antes de transformar.
- El mismo código de features se usa en vivo y en backtest (sin lógica duplicada).
- Ninguna orden sale sin pasar por RiskGuard. El LLM nunca está en la ruta de órdenes.
- Binance Futures WS: una conexión por categoría (/public, /market, /private); falla ruidosamente si se mezclan.
- Órdenes condicionales (stop/take-profit) de Binance USDⓈ-M: SOLO por Algo Order endpoints.
- No hay números mágicos: todo umbral va en configs/*.yaml con valor por defecto conservador.

## Calidad de evidencia
- No afirmes que una estrategia es rentable. Reporta resultados con comisiones, funding, slippage y latencia, y separa in-sample de out-of-sample.
- Cada fase termina con: tests pasando, README de la fase, y un walkthrough con evidencias (logs, capturas de métricas, resultados de tests).

## Flujo de trabajo
- Planifica primero (implementation plan), espera mi aprobación, luego implementa por tareas pequeñas con commits atómicos.
- Si una decisión de diseño tiene alternativas relevantes, preséntalas con pros/contras en lugar de elegir en silencio.