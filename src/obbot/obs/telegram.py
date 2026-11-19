"""Servicio de control y notificaciones asíncronas de Telegram para OrderFlow Bot."""

import asyncio
from typing import TYPE_CHECKING, Any, Literal, cast

import aiohttp

from obbot.common.logging import get_logger
from obbot.risk.kill_switch import KillSwitchAction, KillSwitchLayer
from obbot.risk.limits import OrderRequest

if TYPE_CHECKING:
    from obbot.main import OrderFlowBotApp

logger = get_logger("obs.telegram")


class TelegramBotService:
    """
    Servicio de observabilidad, notificaciones push y control remoto interactivo vía Telegram.

    Características:
    - 100% asíncrono sobre aiohttp sin bloquear la ruta crítica de trading.
    - Control de acceso estricto (RBAC) por user_id.
    - Cola de salida desacoplada con chunking automático de 4096 caracteres.
    - Comandos operativos: /status, /report, /close, /stop, /resume, /shutdown, /help.
    """

    def __init__(
        self,
        bot_token: str,
        chat_id: str,
        allowed_user_ids: str,
        app: "OrderFlowBotApp",
    ) -> None:
        self.bot_token = bot_token.strip()
        self.default_chat_id = chat_id.strip()
        self.app = app
        self._base_url = f"https://api.telegram.org/bot{self.bot_token}"

        # Parsear IDs de usuarios permitidos
        self.allowed_user_ids: set[int] = set()
        for raw_id in allowed_user_ids.split(","):
            cleaned = raw_id.strip()
            if cleaned.isdigit():
                self.allowed_user_ids.add(int(cleaned))

        # Si default_chat_id coincide con el bot token ID, reemplazarlo por el primer usuario autorizado
        bot_id_prefix = self.bot_token.split(":")[0] if ":" in self.bot_token else ""
        if (self.default_chat_id == bot_id_prefix or not self.default_chat_id) and self.allowed_user_ids:
            self.default_chat_id = str(next(iter(self.allowed_user_ids)))

        # Si default_chat_id es un usuario numérico y no se especificó allowed_user_ids, autorizarlo
        if not self.allowed_user_ids and self.default_chat_id.lstrip("-").isdigit():
            self.allowed_user_ids.add(int(self.default_chat_id))

        self._session: aiohttp.ClientSession | None = None
        self._outbound_queue: asyncio.Queue[tuple[str, str, str | None]] = asyncio.Queue()
        self._running = False
        self._tasks: list[asyncio.Task[Any]] = []
        self._last_update_id = 0

    async def start(self) -> None:
        """Inicia la sesión HTTP y las tareas de polling y despacho en segundo plano."""
        if not self.bot_token:
            logger.warning("telegram.disabled: TELEGRAM_BOT_TOKEN no configurado.")
            return

        self._running = True
        self._session = aiohttp.ClientSession()
        self._tasks.append(asyncio.create_task(self._dispatch_loop()))
        self._tasks.append(asyncio.create_task(self._poll_loop()))

        logger.info(
            "telegram.started: chat_id=%s usuarios_autorizados=%s",
            self.default_chat_id,
            self.allowed_user_ids,
        )

        # Enviar mensaje inicial de conexión
        await self.send_message(
            "🚀 *OrderFlow Bot Conectado*\n"
            f"• Modo: `{self.app.mode.upper()}`\n"
            f"• Símbolo: `{self.app.symbol}`\n"
            f"• Balance Inicial: `${self.app.portfolio.initial_daily_balance:,.2f} USD`\n\n"
            "Escribe /help para ver la lista de comandos disponibles."
        )

    async def stop(self) -> None:
        """Detiene ordenadamente el servicio de Telegram."""
        if not self._running:
            return

        self._running = False
        logger.info("telegram.stopping...")

        for task in self._tasks:
            task.cancel()
        if self._tasks:
            await asyncio.gather(*self._tasks, return_exceptions=True)
            self._tasks.clear()

        if self._session and not self._session.closed:
            await self._session.close()

        logger.info("telegram.stopped_cleanly")

    async def send_message(
        self,
        text: str,
        chat_id: str | None = None,
        parse_mode: str | None = "Markdown",
    ) -> None:
        """Encola un mensaje saliente de forma no bloqueante hacia Telegram."""
        if not self._running or not self.bot_token:
            return

        target_chat = chat_id or self.default_chat_id
        if not target_chat:
            return

        # Poner en la cola sin bloquear el hilo principal
        self._outbound_queue.put_nowait((target_chat, text, parse_mode))

    async def _send_api_request(
        self,
        method: str,
        payload: dict[str, Any],
        timeout_sec: float = 15.0,
    ) -> dict[str, Any] | None:
        """Realiza una petición HTTP POST a la API de Telegram."""
        if not self._session or self._session.closed:
            return None

        url = f"{self._base_url}/{method}"
        try:
            async with self._session.post(url, json=payload, timeout=aiohttp.ClientTimeout(total=timeout_sec)) as resp:
                if resp.status == 200:
                    data = await resp.json()
                    return cast(dict[str, Any], data) if isinstance(data, dict) else None
                elif resp.status == 429:
                    # Rate limiting de Telegram
                    retry_after = 2
                    try:
                        err_data = await resp.json()
                        retry_after = err_data.get("parameters", {}).get("retry_after", 2)
                    except Exception:
                        pass
                    logger.warning("telegram.rate_limited: durmiendo %ds", retry_after)
                    await asyncio.sleep(retry_after)
                else:
                    err_text = await resp.text()
                    logger.error("telegram.api_error: status=%d body=%s", resp.status, err_text)
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            logger.error("telegram.http_exception: %r", exc)

        return None

    async def _dispatch_loop(self) -> None:
        """Bucle consumidor de la cola de mensajes salientes."""
        while self._running:
            try:
                target_chat, text, parse_mode = await self._outbound_queue.get()

                # Telegram tiene un límite de 4096 caracteres por mensaje
                chunks = self._chunk_message(text, max_len=4000)
                for chunk in chunks:
                    payload: dict[str, Any] = {
                        "chat_id": target_chat,
                        "text": chunk,
                    }
                    if parse_mode:
                        payload["parse_mode"] = parse_mode

                    await self._send_api_request("sendMessage", payload)
                    await asyncio.sleep(0.05)  # Evitar rate limits

                self._outbound_queue.task_done()
            except asyncio.CancelledError:
                break
            except Exception as exc:
                logger.error("telegram.dispatch_loop_error: %s", exc)
                await asyncio.sleep(0.5)

    def _chunk_message(self, text: str, max_len: int = 4000) -> list[str]:
        """Divide mensajes largos en fragmentos que no excedan el límite de Telegram."""
        if len(text) <= max_len:
            return [text]

        chunks = []
        lines = text.split("\n")
        current_chunk = ""

        for line in lines:
            if len(current_chunk) + len(line) + 1 <= max_len:
                current_chunk += ("\n" if current_chunk else "") + line
            else:
                if current_chunk:
                    chunks.append(current_chunk)
                current_chunk = line

        if current_chunk:
            chunks.append(current_chunk)

        return chunks

    async def _poll_loop(self) -> None:
        """Bucle de recepción de comandos vía Long-Polling de Telegram."""
        await asyncio.sleep(1.0)  # Breve pausa para estabilización

        while self._running:
            try:
                payload: dict[str, Any] = {
                    "offset": self._last_update_id + 1,
                    "timeout": 20,
                    "allowed_updates": ["message"],
                }
                res = await self._send_api_request("getUpdates", payload, timeout_sec=35.0)
                if res and res.get("ok"):
                    for update in res.get("result", []):
                        self._last_update_id = max(self._last_update_id, update.get("update_id", 0))
                        message = update.get("message")
                        if message and "text" in message:
                            await self._handle_incoming_message(message)
                else:
                    await asyncio.sleep(1.0)
            except asyncio.CancelledError:
                break
            except Exception as exc:
                logger.error("telegram.poll_loop_error: %s", exc)
                await asyncio.sleep(2.0)

    async def _handle_incoming_message(self, message: dict[str, Any]) -> None:
        """Valida autorización y enruta los comandos interactivos."""
        chat = message.get("chat", {})
        chat_id = str(chat.get("id", ""))
        sender = message.get("from", {})
        user_id = sender.get("id")
        username = sender.get("username", "Anon")
        text = message.get("text", "").strip()

        # Validación estricta de seguridad (RBAC)
        if self.allowed_user_ids and user_id not in self.allowed_user_ids:
            logger.warning(
                "telegram.unauthorized_access: user_id=%s username=%s cmd='%s'",
                user_id,
                username,
                text,
            )
            # Descartar en silencio para no retroalimentar a atacantes
            return

        if not text.startswith("/"):
            return

        parts = text.split()
        command = parts[0].lower().split("@")[0]  # Limpiar sufijos tipo /status@mi_bot

        logger.info("telegram.command_received: cmd=%s user_id=%s", command, user_id)

        try:
            if command in ("/start", "/help"):
                await self._cmd_help(chat_id)
            elif command in ("/status", "/telemetria"):
                await self._cmd_status(chat_id)
            elif command in ("/report", "/auditoria"):
                await self._cmd_report(chat_id)
            elif command in ("/close", "/panic", "/close_all"):
                await self._cmd_close(chat_id)
            elif command in ("/stop", "/kill", "/block"):
                await self._cmd_stop(chat_id)
            elif command in ("/resume", "/unblock", "/activar"):
                await self._cmd_resume(chat_id)
            elif command in ("/shutdown", "/apagar"):
                await self._cmd_shutdown(chat_id)
            else:
                await self.send_message(
                    f"❓ Comando desconocido `{command}`. Usa /help para ver la lista.",
                    chat_id=chat_id,
                )
        except Exception as exc:
            logger.error("telegram.command_execution_error: cmd=%s err=%s", command, exc)
            await self.send_message(
                f"❌ Error al ejecutar `{command}`: {exc}",
                chat_id=chat_id,
            )

    # =========================================================================
    # Manejadores de Comandos
    # =========================================================================

    async def _cmd_help(self, chat_id: str) -> None:
        """Muestra el manual interactivo de comandos."""
        msg = (
            "🤖 *OrderFlow Bot - Terminal de Control*\n\n"
            "📊 *Comandos de Telemetría y Reportes:*\n"
            "• `/status` - Resumen en tiempo real (Balance, PnL, Posiciones, Order Flow)\n"
            "• `/report` - Auditoría narrativa y análisis cuantitativo por Agente LLM\n\n"
            "🚨 *Comandos de Control Operativo:*\n"
            "• `/close` (o `/panic`) - Cierre inmediato de posiciones a mercado (reduceOnly)\n"
            "• `/stop` (o `/kill`) - Bloquea nuevas entradas activando Kill-Switch Manual\n"
            "• `/resume` - Restablece el Kill-Switch para reanudar operaciones\n"
            "• `/shutdown` - Apaga el bot de forma limpia y ordenada\n"
            "• `/help` - Muestra esta ayuda"
        )
        await self.send_message(msg, chat_id=chat_id)

    async def _cmd_status(self, chat_id: str) -> None:
        """Genera y envía el dashboard de telemetría y microestructura en tiempo real."""
        now_ns = self.app.clock.now_ns()
        fv = self.app.feature_engine.compute_feature_vector(now_ns)
        initial_bal = float(self.app.portfolio.initial_daily_balance)
        curr_bal = (
            float(self.app.paper_engine.wallet_balance)
            if hasattr(self.app, "paper_engine")
            and hasattr(self.app.paper_engine, "wallet_balance")
            and isinstance(self.app.paper_engine.wallet_balance, (int, float))
            else float(self.app.portfolio.current_wallet_balance)
        )
        pnl = curr_bal - initial_bal
        pnl_pct = (pnl / initial_bal) * 100.0 if initial_bal > 0 else 0.0

        exchange_custody = float(getattr(self.app.portfolio, "exchange_custody_balance", 0.0))
        custody_line = (
            f"• Billetera Exchange: `${exchange_custody:,.2f} USDT`\n"
            if exchange_custody > 0
            else ""
        )

        # Posiciones deduplicadas
        positions = (
            self.app.paper_engine._positions
            if hasattr(self.app, "paper_engine")
            and hasattr(self.app.paper_engine, "_positions")
            and isinstance(self.app.paper_engine._positions, dict)
            else (self.app.portfolio.positions or {})
        )
        seen_clean = set()
        active_pos_list = []
        for s, p in positions.items():
            clean = s.replace("-PERP", "").replace("-", "").upper()
            if clean in seen_clean:
                continue
            if isinstance(p, dict):
                amt = float(p.get("positionAmt", 0.0))
                entry = float(p.get("entryPrice", 0.0))
                upnl = float(p.get("unRealizedProfit", 0.0))
            else:
                amt = float(getattr(p, "size", 0.0))
                entry = float(getattr(p, "entry_price", 0.0))
                upnl = float(getattr(p, "unrealized_pnl", 0.0))

            if abs(amt) > 1e-6:
                seen_clean.add(clean)
                side = "LONG 🟢" if amt > 0 else "SHORT 🔴"
                active_pos_list.append(
                    f"• *{s}* ({side}): size={amt:.4f} @ ${entry:,.2f} | uPnL: `${upnl:+,.2f}`"
                )

        pos_str = "\n".join(active_pos_list) if active_pos_list else "• _Ninguna posición abierta_"
        ks_action = self.app.kill_switch.get_highest_severity_action()
        ks_emoji = "🟢" if ks_action == KillSwitchAction.NONE else "🔴"

        mid_str = f"${fv.mid_price:,.2f}" if fv and fv.mid_price > 0 else "$0.00"
        spread_str = f"{fv.spread_bps:.2f} bps" if fv else "0.00 bps"
        z_str = f"{fv.global_delta_z:+.2f}" if fv else "+0.00"
        obi_str = f"{fv.obi_10_levels:+.2f}" if fv else "+0.00"
        vol_str = f"{fv.realized_volatility * 100:.2f}%" if fv else "0.00%"

        htf_ema_raw = getattr(fv, "htf_ema_15m", 0.0) if fv else 0.0
        htf_ema = float(htf_ema_raw) if isinstance(htf_ema_raw, (int, float)) else 0.0
        htf_regime_raw = getattr(fv, "htf_trend_regime", "NEUTRAL") if fv else "NEUTRAL"
        htf_regime = str(htf_regime_raw) if isinstance(htf_regime_raw, str) else "NEUTRAL"
        regime_emoji = (
            "🟢 ALCISTA"
            if htf_regime == "BULLISH"
            else ("🔴 BAJISTA" if htf_regime == "BEARISH" else "⚪ NEUTRAL")
        )
        htf_str = f"${htf_ema:,.2f}" if htf_ema > 0 else "Calibrando..."
        cvd_val = (
            float(fv.global_cvd) if fv and isinstance(getattr(fv, "global_cvd", None), (int, float)) else 0.0
        )
        cvd_str = f"${cvd_val:+,.1f} USD"

        msg = (
            f"📊 *TELEMETRÍA EN TIEMPO REAL*\n"
            f"• Modo: `{self.app.mode.upper()}` | Símbolo: `{self.app.symbol}`\n"
            f"• Kill-Switch: {ks_emoji} `{ks_action.name}`\n"
            f"• Trades: `{self.app.total_trades_received}` | L2 Books: `{self.app.total_books_received}`\n"
            "──────────────────────\n"
            f"💰 *Finanzas y Balance:*\n"
            f"• Capital Asignado (Base): `${initial_bal:,.2f} USD`\n"
            f"• Capital Actual Estrategia: `${curr_bal:,.2f} USD`\n"
            f"• PnL Sesión: `${pnl:+,.2f} USD` (`{pnl_pct:+.2f}%`)\n"
            f"{custody_line}"
            "──────────────────────\n"
            f"🧭 *Tendencia Estructural (HTF 15m):*\n"
            f"• Régimen: `{regime_emoji}`\n"
            f"• EMA 15m: `{htf_str}`\n"
            f"• CVD Sesión: `{cvd_str}`\n"
            "──────────────────────\n"
            f"📈 *Microestructura (Order Flow 400ms):*\n"
            f"• Mark Price: `{mid_str}`\n"
            f"• Spread: `{spread_str}`\n"
            f"• Delta Global Z: `{z_str}`\n"
            f"• OBI (10 niveles): `{obi_str}`\n"
            f"• Volatilidad (60s): `{vol_str}`\n"
            "──────────────────────\n"
            f"📍 *Posiciones Abiertas:*\n"
            f"{pos_str}"
        )
        await self.send_message(msg, chat_id=chat_id)

    async def _cmd_report(self, chat_id: str) -> None:
        """Invoca el Agente LLM para generar un reporte narrativo de auditoría."""
        from obbot.llm.client import create_llm_provider
        from obbot.llm.tasks import generate_end_of_session_report

        provider = create_llm_provider(self.app.config)
        if not provider:
            await self.send_message(
                "⚠️ *Módulo LLM no configurado.*\n"
                "Para activar reportes automáticos con IA, añade `GEMINI_API_KEY` "
                "(o `LLM_API_KEY`) en tu archivo `.env`.\n"
                "Puedes usar `/status` para consultar las métricas cuantitativas en directo.",
                chat_id=chat_id,
            )
            return

        await self.send_message(
            "🧠 *Generando auditoría de sesión con el Agente LLM...*\n"
            "Analizando métricas de riesgo, order flow y microestructura...",
            chat_id=chat_id,
        )

        now_ns = self.app.clock.now_ns()
        fv = self.app.feature_engine.compute_feature_vector(now_ns)
        initial_bal = self.app.portfolio.initial_daily_balance
        curr_bal = (
            self.app.paper_engine.wallet_balance
            if hasattr(self.app, "paper_engine")
            else self.app.portfolio.current_wallet_balance
        )
        pnl = curr_bal - initial_bal
        pnl_pct = (pnl / initial_bal) * 100.0 if initial_bal > 0 else 0.0

        session_metrics = {
            "mode": self.app.mode,
            "symbol": self.app.symbol,
            "total_trades_received": self.app.total_trades_received,
            "total_books_received": self.app.total_books_received,
            "total_orders_submitted": self.app.total_orders_submitted,
            "initial_balance_usd": initial_bal,
            "current_balance_usd": curr_bal,
            "net_pnl_usd": round(pnl, 2),
            "net_pnl_pct": round(pnl_pct, 2),
            "daily_drawdown_pct": round(self.app.portfolio.daily_drawdown_pct * 100.0, 2),
            "kill_switch_action": self.app.kill_switch.get_highest_severity_action().name,
            "last_mark_price": fv.mid_price if fv else 0.0,
            "last_spread_bps": fv.spread_bps if fv else 0.0,
            "global_delta_z": fv.global_delta_z if fv else 0.0,
            "obi_10_levels": fv.obi_10_levels if fv else 0.0,
            "realized_volatility_pct": round(fv.realized_volatility * 100.0, 2) if fv else 0.0,
        }

        try:
            report_text = await generate_end_of_session_report(provider, session_metrics)
            await self.send_message(report_text, chat_id=chat_id)
        except Exception as exc:
            logger.error("telegram.llm_report_error: %s", exc)
            await self.send_message(f"❌ Error al generar reporte con el LLM: {exc}", chat_id=chat_id)

    async def _cmd_close(self, chat_id: str) -> None:
        """Cierra inmediatamente cualquier posición abierta mediante orden MARKET reduceOnly."""
        now_ns = self.app.clock.now_ns()
        fv = self.app.feature_engine.compute_feature_vector(now_ns)
        mark_px = fv.mid_price if fv and fv.mid_price > 0 else 50000.0

        positions = (
            self.app.paper_engine._positions
            if hasattr(self.app, "paper_engine")
            and hasattr(self.app.paper_engine, "_positions")
            and isinstance(self.app.paper_engine._positions, dict)
            else (self.app.portfolio.positions or {})
        )

        closed_any = False
        for s, p in list(positions.items()):
            if isinstance(p, dict):
                amt = float(p.get("positionAmt", 0.0))
            else:
                amt = float(getattr(p, "size", 0.0))

            if abs(amt) > 1e-6:
                closed_any = True
                exit_side: Literal["BUY", "SELL"] = "SELL" if amt > 0 else "BUY"
                qty = abs(amt)

                # Cancelar Algo Orders abiertas
                try:
                    open_algos = await self.app.order_manager.algo_client.get_open_algo_orders(s)
                    for algo in open_algos:
                        aid = (
                            algo.get("algoId")
                            if isinstance(algo, dict)
                            else getattr(algo, "algo_id", getattr(algo, "algoId", None))
                        )
                        if aid:
                            await self.app.order_manager.algo_client.cancel_algo_order(s, algo_id=aid)
                except Exception as exc:
                    logger.warning("telegram.close.cancel_algos_error: %s", exc)

                # Enviar orden MARKET reduceOnly
                req = OrderRequest(
                    symbol=s,
                    side=exit_side,
                    order_type="MARKET",
                    quantity=qty,
                    price=0.0,
                    reduce_only=True,
                    time_in_force="GTC",
                )
                managed = await self.app.order_manager.submit_order(req, self.app.portfolio, mark_px)

                await self.send_message(
                    f"🚨 *POSICIÓN CERRADA MANUALMENTE*\n"
                    f"• Símbolo: `{s}`\n"
                    f"• Lado Salida: `{exit_side}`\n"
                    f"• Cantidad: `{qty:.4f}`\n"
                    f"• Estado Orden: `{managed.status.name}`\n"
                    f"• Órdenes condicionales canceladas.",
                    chat_id=chat_id,
                )

        if not closed_any:
            await self.send_message("💡 No hay posiciones abiertas para cerrar.", chat_id=chat_id)

    async def _cmd_stop(self, chat_id: str) -> None:
        """Activa manualmente la Capa 5 del Kill-Switch para bloquear nuevas operaciones."""
        now_ns = self.app.clock.now_ns()
        self.app.kill_switch.trigger_manual(
            reason="Comando /stop ejecutado desde Telegram",
            now_ns=now_ns,
            action=KillSwitchAction.BLOCK_NEW_ENTRIES,
        )
        await self.send_message(
            "🛑 *KILL-SWITCH MANUAL ACTIVADO*\n"
            "• Acción: `BLOCK_NEW_ENTRIES`\n"
            "• El bot continúa monitoreando el mercado y protegiendo posiciones, pero "
            "**no se abrirán nuevas entradas**.\n\n"
            "Usa `/resume` para reactivar o `/close` para salir de posiciones existentes.",
            chat_id=chat_id,
        )

    async def _cmd_resume(self, chat_id: str) -> None:
        """Restablece el Kill-Switch para reanudar la operativa."""
        self.app.kill_switch.reset_all(operator_reason="Reanudación manual desde Telegram")
        await self.send_message(
            "✅ *SISTEMA REANUDADO*\n"
            "• Todas las capas del Kill-Switch han sido restablecidas.\n"
            "• La estrategia está autorizada para evaluar y ejecutar nuevas entradas.",
            chat_id=chat_id,
        )

    async def _cmd_shutdown(self, chat_id: str) -> None:
        """Inicia el apagado limpio del bot."""
        await self.send_message("🔌 *Apagando OrderFlow Bot limpiamente...*", chat_id=chat_id)
        # Notificar a la app principal para detener el loop
        if hasattr(self.app, "_stop_event") and self.app._stop_event is not None:
            self.app._stop_event.set()
        else:
            task = asyncio.create_task(self.app.stop())
            self._tasks.append(task)

    # =========================================================================
    # Notificaciones Automáticas Push
    # =========================================================================

    async def notify_order_fill(
        self,
        symbol: str,
        side: str,
        quantity: float,
        price: float,
        reduce_only: bool,
        stop_loss: float | None = None,
        take_profit: float | None = None,
    ) -> None:
        """Notifica inmediatamente la ejecución de una orden (Fill)."""
        tipo = "CIERRE (ReduceOnly) ⚠️" if reduce_only else ("COMPRA 🟢" if side == "BUY" else "VENTA 🔴")
        sl_str = f"${stop_loss:,.2f}" if stop_loss else "N/A"
        tp_str = f"${take_profit:,.2f}" if take_profit else "N/A"

        msg = (
            f"⚡ *ORDEN EJECUTADA ({tipo})*\n"
            f"• Símbolo: `{symbol}`\n"
            f"• Lado: `{side}` | Cantidad: `{quantity:.4f}`\n"
            f"• Precio Fill: `${price:,.2f}`\n"
            f"• Nocional: `${quantity * price:,.2f} USD`\n"
            f"• Stop-Loss: `{sl_str}`\n"
            f"• Take-Profit: `{tp_str}`"
        )
        await self.send_message(msg)

    async def notify_early_invalidation(
        self,
        symbol: str,
        side: str,
        quantity: float,
        reason: str,
        z_score: float,
        obi: float,
    ) -> None:
        """Notifica el cierre anticipado de una posición por reversión violenta del flujo."""
        msg = (
            f"⚠️ *SALIDA ANTICIPADA (Early Invalidation)*\n"
            f"• Símbolo: `{symbol}`\n"
            f"• Razón: `{reason}`\n"
            f"• Delta Z-Score: `{z_score:+.2f}` | OBI: `{obi:+.2f}`\n"
            f"• Acción: Cierre preventivo de `{quantity:.4f}` para recortar pérdida."
        )
        await self.send_message(msg)

    async def notify_kill_switch_tripped(
        self,
        layer: KillSwitchLayer,
        action: KillSwitchAction,
        reason: str,
    ) -> None:
        """Alerta de emergencia cuando cualquier capa del Kill-Switch se dispara."""
        msg = (
            f"🚨 *ALERTA CRÍTICA: KILL-SWITCH DISPARADO*\n"
            f"• Capa: `{layer.name}`\n"
            f"• Acción Impuesta: `{action.name}`\n"
            f"• Motivo: `{reason}`"
        )
        await self.send_message(msg)
