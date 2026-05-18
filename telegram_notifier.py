"""
telegram_notifier.py — Notificaciones Telegram con señales manuales y test aciertos.
"""
from __future__ import annotations
import logging
import aiohttp
from strategy import Signal

log = logging.getLogger("telegram")
TELEGRAM_API = "https://api.telegram.org/bot{token}/sendMessage"


class TelegramNotifier:
    def __init__(self, token: str, chat_id: str):
        self.token   = token
        self.chat_id = chat_id
        self._url    = TELEGRAM_API.format(token=token)

    async def _send(self, text: str):
        if not self.token or not self.chat_id:
            log.warning("Telegram no configurado.")
            return
        try:
            async with aiohttp.ClientSession() as s:
                resp = await s.post(self._url, json={
                    "chat_id":    self.chat_id,
                    "text":       text,
                    "parse_mode": "Markdown",
                })
                if resp.status != 200:
                    body = await resp.text()
                    log.error("Telegram error %d: %s", resp.status, body[:200])
        except Exception as e:
            log.error("Error Telegram: %s", e)

    async def startup(self, dry_run: bool, symbols: int):
        mode = "🟡 DRY RUN (simulación)" if dry_run else "🟢 REAL ⚠️"
        await self._send(
            f"🤖 *EMA8 Scalper Bot v4 — Iniciado*\n\n"
            f"Modo: `{mode}`\n"
            f"Símbolos: `{symbols}`\n"
            f"_Las señales aparecerán aquí con todos los niveles._\n\n"
            f"📌 Puedes abrir trades manualmente usando los niveles indicados."
        )

    # ── SEÑAL PRINCIPAL — con todos los niveles para trade manual ──
    async def signal_alert(self, sig: Signal, qty: float, dry_run: bool):
        emoji  = "🟢" if sig.side == "LONG"  else "🔴"
        dir_es = "LARGO (COMPRA)" if sig.side == "LONG" else "CORTO (VENTA)"
        mode   = " _(simulado)_" if dry_run else ""
        rr     = abs(sig.tp1 - sig.price) / abs(sig.price - sig.sl) if abs(sig.price - sig.sl) else 0

        sl_pct  = abs(sig.price - sig.sl)  / sig.price * 100
        tp1_pct = abs(sig.tp1  - sig.price) / sig.price * 100
        tp2_pct = abs(sig.tp2  - sig.price) / sig.price * 100

        await self._send(
            f"{emoji} *{sig.side} — {sig.symbol}*{mode}\n"
            f"━━━━━━━━━━━━━━━━━━━━\n"
            f"📍 *Dirección:* `{dir_es}`\n"
            f"💲 *Entrada:*   `{sig.price:.6g}`\n"
            f"━━━━━━━━━━━━━━━━━━━━\n"
            f"🛑 *Stop Loss:* `{sig.sl:.6g}` _(-{sl_pct:.2f}%)_\n"
            f"🎯 *TP1:*       `{sig.tp1:.6g}` _(+{tp1_pct:.2f}%)_\n"
            f"🎯 *TP2:*       `{sig.tp2:.6g}` _(+{tp2_pct:.2f}%)_\n"
            f"━━━━━━━━━━━━━━━━━━━━\n"
            f"⚖️  *R/R:*       `1 : {rr:.2f}`\n"
            f"⭐ *Score:*     `{sig.score}/5`\n"
            f"📋 *Filtros:*   `{sig.reason}`\n"
            f"📦 *Qty bot:*   `{qty:.4f}` contratos\n"
            f"━━━━━━━━━━━━━━━━━━━━\n"
            f"👆 _Puedes abrir este trade manualmente en BingX_"
        )

    async def order_filled(self, symbol: str, side: str, price: float):
        emoji = "✅" if side == "LONG" else "⛔"
        await self._send(
            f"{emoji} *Orden Ejecutada*\n"
            f"`{symbol}` | `{side}` | Precio: `{price:.6g}`"
        )

    async def position_closed(self, symbol: str, side: str,
                               pnl: float, reason: str = "TP/SL"):
        emoji = "💰" if pnl >= 0 else "💸"
        await self._send(
            f"{emoji} *Posición Cerrada*\n"
            f"`{symbol}` | `{side}`\n"
            f"PnL: `{pnl:+.2f} USDT` | Razón: `{reason}`"
        )

    # ── TEST DE ACIERTOS (cada hora) ──────────────────────────
    async def hourly_accuracy(self, report: str):
        await self._send(report)

    async def daily_summary(self, summary_text: str):
        await self._send(f"📊 {summary_text}")

    async def heartbeat(self, balance: float, open_trades: int, wr: float = 0):
        await self._send(
            f"💓 *Heartbeat*\n"
            f"Balance: `{balance:.2f} USDT` | "
            f"Posiciones: `{open_trades}` | "
            f"Win Rate 24h: `{wr:.0f}%`"
        )

    async def error(self, msg: str):
        await self._send(f"⚠️ *Error Bot*\n`{msg[:300]}`")

    async def auth_ok(self, balance: float):
        await self._send(
            f"🔑 *API BingX Verificada*\n"
            f"Balance disponible: `{balance:.2f} USDT` ✅"
        )
