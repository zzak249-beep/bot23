"""
telegram_notifier.py — Envío de mensajes a Telegram con formato Markdown.
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
            log.warning("Telegram no configurado — mensaje no enviado.")
            return
        try:
            async with aiohttp.ClientSession() as s:
                await s.post(self._url, json={
                    "chat_id":    self.chat_id,
                    "text":       text,
                    "parse_mode": "Markdown",
                })
        except Exception as e:
            log.error("Error enviando Telegram: %s", e)

    # ── Arranque ──────────────────────────────────────────────
    async def startup(self, dry_run: bool, symbols: int):
        mode = "🟡 DRY RUN" if dry_run else "🟢 REAL"
        await self._send(
            f"🤖 *EMA8 Scalper Bot Iniciado*\n"
            f"Modo: `{mode}`\n"
            f"Símbolos monitoreados: `{symbols}`\n"
            f"_¡El bot está operando!_"
        )

    # ── Señal de entrada ──────────────────────────────────────
    async def signal_alert(self, sig: Signal, qty: float, dry_run: bool):
        emoji  = "🟢" if sig.side == "LONG" else "🔴"
        mode   = "_(SIMULADO)_" if dry_run else ""
        rr     = abs(sig.tp1 - sig.price) / abs(sig.price - sig.sl)
        await self._send(
            f"{emoji} *{sig.side} — {sig.symbol}* {mode}\n\n"
            f"💲 Precio entrada: `{sig.price:.6g}`\n"
            f"🛑 Stop Loss:      `{sig.sl:.6g}`\n"
            f"🎯 TP1:            `{sig.tp1:.6g}`\n"
            f"🎯 TP2:            `{sig.tp2:.6g}`\n"
            f"📦 Cantidad:       `{qty:.4f}`\n"
            f"⚖️  RR:             `1 : {rr:.2f}`\n"
            f"⭐ Score:          `{sig.score}/5`\n"
            f"📋 Filtros:        `{sig.reason}`"
        )

    # ── Orden ejecutada ───────────────────────────────────────
    async def order_filled(self, symbol: str, side: str, price: float):
        emoji = "✅" if side == "LONG" else "⛔"
        await self._send(
            f"{emoji} *Orden Ejecutada*\n"
            f"Par: `{symbol}` | Lado: `{side}` | Precio: `{price:.6g}`"
        )

    # ── Cierre de posición ────────────────────────────────────
    async def position_closed(self, symbol: str, side: str,
                               pnl: float, reason: str = "TP/SL"):
        emoji = "💰" if pnl >= 0 else "💸"
        await self._send(
            f"{emoji} *Posición Cerrada*\n"
            f"Par: `{symbol}` | Lado: `{side}`\n"
            f"PnL: `{pnl:+.2f} USDT`\n"
            f"Razón: `{reason}`"
        )

    # ── Resumen diario ────────────────────────────────────────
    async def daily_summary(self, summary_text: str):
        await self._send(f"📊 {summary_text}")

    # ── Error ─────────────────────────────────────────────────
    async def error(self, msg: str):
        await self._send(f"⚠️ *Error del Bot*\n`{msg}`")

    # ── Heartbeat (cada hora) ─────────────────────────────────
    async def heartbeat(self, balance: float, open_trades: int):
        await self._send(
            f"💓 *Heartbeat*\n"
            f"Balance: `{balance:.2f} USDT` | "
            f"Posiciones: `{open_trades}`"
        )
