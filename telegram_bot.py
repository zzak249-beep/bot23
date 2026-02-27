"""
Bot de Telegram — imports planos (sin subcarpetas)
"""

import os
import asyncio
import logging
from datetime import datetime
from telegram import Update, Bot
from telegram.ext import ApplicationBuilder, CommandHandler, ContextTypes, Application
from telegram.constants import ParseMode

from vwap_strategy import Signal, TradeSignal
from logger import setup_logger

logger = setup_logger(__name__)


def fmt_signal(symbol, strategy, sig):
    d = sig.signal.value
    arrow = "🟢 LONG ▲" if d == "LONG" else "🔴 SHORT ▼"
    conf  = {"HIGH": "🔥 ALTA", "MEDIUM": "⚡ MEDIA"}.get(sig.confidence, "⚪")
    strat = {"VWAP+SD": "📊 VWAP+SD", "BB+RSI": "📉 BB+RSI", "EMA Ribbon 5m": "⚡ EMA Scalp"}.get(strategy, strategy)
    ts    = datetime.utcnow().strftime("%H:%M:%S UTC")
    if sig.signal == Signal.LONG and sig.sl_price:
        rr = (sig.tp_price - sig.entry_price) / max(sig.entry_price - sig.sl_price, 0.0001)
    elif sig.sl_price:
        rr = (sig.entry_price - sig.tp_price) / max(sig.sl_price - sig.entry_price, 0.0001)
    else:
        rr = 0
    return (
        f"🔔 *SEÑAL DETECTADA*\n"
        f"━━━━━━━━━━━━━━━━━━━━\n"
        f"{arrow}  `{symbol}`\n"
        f"{strat} | Confianza: {conf}\n"
        f"━━━━━━━━━━━━━━━━━━━━\n"
        f"📍 Entrada: `{sig.entry_price}`\n"
        f"✅ TP:      `{sig.tp_price}`\n"
        f"🛑 SL:      `{sig.sl_price}`\n"
        f"⚖️ R:R:     `{rr:.2f}`\n"
        f"📝 _{sig.reason}_\n"
        f"⏰ `{ts}`"
    )


def fmt_open(t):
    arrow = "🟢 LONG ▲" if t["signal"] == "LONG" else "🔴 SHORT ▼"
    ts    = datetime.utcnow().strftime("%H:%M:%S UTC")
    rr_n  = abs(t.get("tp", 0) - t.get("entry", 0))
    rr_d  = abs(t.get("sl", 0) - t.get("entry", 0))
    rr    = rr_n / rr_d if rr_d else 0
    return (
        f"🚀 *TRADE ABIERTO — BINGX*\n"
        f"━━━━━━━━━━━━━━━━━━━━\n"
        f"{arrow}  `{t['symbol']}`\n"
        f"📊 `{t['strategy']}` | 🎯 `{t.get('confidence','?')}`\n"
        f"━━━━━━━━━━━━━━━━━━━━\n"
        f"📍 Entrada:  `{t['entry']}`\n"
        f"✅ TP:       `{t['tp']}`\n"
        f"🛑 SL:       `{t['sl']}`\n"
        f"⚖️ R:R:      `{rr:.2f}`\n"
        f"━━━━━━━━━━━━━━━━━━━━\n"
        f"💰 Margen:   `{t['margin']} USDT × {t['leverage']}x`\n"
        f"📦 Posición: `{t['pos_value']} USDT` | `{t['qty']} contratos`\n"
        f"🏦 Balance:  `${t.get('balance', 0):.2f} USDT`\n"
        f"🆔 `{t.get('order_id','N/A')}`\n"
        f"⏰ `{ts}`\n"
        f"📈 _Trailing stop activo_"
    )


def fmt_close(c):
    won   = c["won"]
    icon  = "✅" if won else "❌"
    arrow = "🟢 LONG ▲" if c["direction"] == "LONG" else "🔴 SHORT ▼"
    sign  = "+" if c["pnl_usdt"] >= 0 else ""
    ts    = datetime.utcnow().strftime("%H:%M:%S UTC")
    return (
        f"{icon} *TRADE CERRADO — BINGX*\n"
        f"━━━━━━━━━━━━━━━━━━━━\n"
        f"{arrow}  `{c['symbol']}` | `{c['strategy']}`\n"
        f"━━━━━━━━━━━━━━━━━━━━\n"
        f"📍 Entrada: `{c['entry']}`\n"
        f"🚪 Salida:  `{c['exit']}`\n"
        f"━━━━━━━━━━━━━━━━━━━━\n"
        f"💵 PnL:     `{sign}{c['pnl_usdt']:.4f} USDT ({sign}{c['pnl_pct']:.2f}%)`\n"
        f"🔄 Fase:    `{c['phase']}`\n"
        f"📝 _{c['reason']}_\n"
        f"━━━━━━━━━━━━━━━━━━━━\n"
        f"📊 P&L hoy: `{'+' if c['daily_pnl']>=0 else ''}{c['daily_pnl']:.4f} USDT`\n"
        f"🎯 Winrate: `{c['winrate']}%`\n"
        f"⏰ `{ts}`"
    )


class TelegramSignalBot:
    def __init__(self, trader):
        self.token   = os.environ["TELEGRAM_TOKEN"]
        self.chat_id = os.environ["TELEGRAM_CHAT_ID"]
        self.trader  = trader
        self.app = None
        self.bot = None

        self.trader.register_signal_callback(self.on_signal)
        self.trader.register_trade_callback(self.on_open)
        self.trader.register_close_callback(self.on_close)

    async def send(self, text: str):
        try:
            await self.bot.send_message(chat_id=self.chat_id, text=text, parse_mode=ParseMode.MARKDOWN)
        except Exception as e:
            logger.error(f"Telegram error: {e}")

    async def on_signal(self, symbol, strategy, sig):
        await self.send(fmt_signal(symbol, strategy, sig))

    async def on_open(self, trade_info):
        await self.send(fmt_open(trade_info))

    async def on_close(self, close_info):
        await self.send(fmt_close(close_info))

    async def cmd_start(self, update: Update, ctx: ContextTypes.DEFAULT_TYPE):
        await update.message.reply_text(
            "🤖 *BingX Trading Bot*\n"
            "━━━━━━━━━━━━━━━━━━━━\n"
            "💰 Entrada: `8 USDT × 7x = 56 USDT`\n"
            "📈 Si gana → trailing stop (deja correr)\n"
            "🛑 Si pierde → cierre rápido al SL\n\n"
            "📌 *Comandos:*\n"
            "/status /balance /trades /config",
            parse_mode=ParseMode.MARKDOWN
        )

    async def cmd_status(self, update: Update, ctx: ContextTypes.DEFAULT_TYPE):
        t = self.trader
        wr = round((t.winning_trades / max(t.total_trades, 1)) * 100, 1)
        modo = "⚪ DEMO" if t.testnet else "🔴 DINERO REAL"
        await update.message.reply_text(
            f"📊 *Estado* | {modo}\n"
            f"━━━━━━━━━━━━━━━━━━━━\n"
            f"📂 Posiciones abiertas: `{len(t.active_trades)}`\n"
            f"📈 Trades total: `{t.total_trades}` | WR: `{wr}%`\n"
            f"💵 P&L hoy: `{'+' if t.daily_pnl>=0 else ''}{t.daily_pnl:.4f} USDT`\n"
            f"💱 Pares: `{', '.join(t.pairs)}`",
            parse_mode=ParseMode.MARKDOWN
        )

    async def cmd_balance(self, update: Update, ctx: ContextTypes.DEFAULT_TYPE):
        await update.message.reply_text("⏳ Consultando...")
        try:
            bal = await self.trader.get_balance()
            pos_val = self.trader.usdt_per_trade * self.trader.leverage
            await update.message.reply_text(
                f"💰 *Balance BingX Futuros*\n"
                f"━━━━━━━━━━━━━━━━━━━━\n"
                f"💵 Disponible: `${bal:.4f} USDT`\n"
                f"📦 Por trade: `{self.trader.usdt_per_trade} × {self.trader.leverage}x = {pos_val} USDT`\n"
                f"🔢 Trades posibles: `~{int(bal/self.trader.usdt_per_trade)}`",
                parse_mode=ParseMode.MARKDOWN
            )
        except Exception as e:
            await update.message.reply_text(f"❌ Error: {e}")

    async def cmd_trades(self, update: Update, ctx: ContextTypes.DEFAULT_TYPE):
        t = self.trader
        if not t.active_trades:
            await update.message.reply_text("📭 Sin posiciones abiertas.")
            return
        msg = "📋 *Posiciones Abiertas*\n━━━━━━━━━━━━━━━━━━━━\n"
        for sym, trade in t.active_trades.items():
            try:
                price = await t.get_current_price(sym)
                trade.update_pnl(price)
            except:
                price = 0
            pnl_icon = "📈" if trade.pnl_usdt >= 0 else "📉"
            phase_name = {0: "🔒 SL Fijo", 1: "🔄 Breakeven", 2: "🎯 Trailing"}.get(trade.phase, "?")
            arr = "▲" if trade.direction == "LONG" else "▼"
            msg += (
                f"{arr} *{sym}* `{trade.direction}`\n"
                f"  Entrada: `{trade.entry}` | Precio: `{price:.4f}`\n"
                f"  {pnl_icon} PnL: `{trade.pnl_usdt:+.4f} USDT`\n"
                f"  Fase: {phase_name} | SL: `{trade.current_sl:.4f}`\n\n"
            )
        await update.message.reply_text(msg, parse_mode=ParseMode.MARKDOWN)

    async def cmd_config(self, update: Update, ctx: ContextTypes.DEFAULT_TYPE):
        from trader import BE_TRIGGER_PCT, TRAIL_START_PCT, TRAIL_DISTANCE_PCT, LOSS_CUT_PCT, MONITOR_INTERVAL
        t = self.trader
        await update.message.reply_text(
            f"⚙️ *Configuración*\n"
            f"━━━━━━━━━━━━━━━━━━━━\n"
            f"💰 Entrada: `{t.usdt_per_trade} USDT × {t.leverage}x = {t.usdt_per_trade*t.leverage} USDT`\n"
            f"🔄 Breakeven: `+{BE_TRIGGER_PCT}%`\n"
            f"🎯 Trailing inicia: `+{TRAIL_START_PCT}%`\n"
            f"📏 Distancia trailing: `{TRAIL_DISTANCE_PCT}%`\n"
            f"🛑 Corte pérdida: `-{LOSS_CUT_PCT}%`\n"
            f"⏱ Monitor: cada `{MONITOR_INTERVAL}s`",
            parse_mode=ParseMode.MARKDOWN
        )

    async def run(self):
        self.app = ApplicationBuilder().token(self.token).build()
        self.bot = self.app.bot

        self.app.add_handler(CommandHandler("start",   self.cmd_start))
        self.app.add_handler(CommandHandler("status",  self.cmd_status))
        self.app.add_handler(CommandHandler("balance", self.cmd_balance))
        self.app.add_handler(CommandHandler("trades",  self.cmd_trades))
        self.app.add_handler(CommandHandler("config",  self.cmd_config))

        modo = "⚪ DEMO" if self.trader.testnet else "🔴 DINERO REAL"
        pos_val = self.trader.usdt_per_trade * self.trader.leverage
        await self.send(
            f"🚀 *BingX Bot iniciado* — {modo}\n"
            f"💰 `{self.trader.usdt_per_trade} USDT × {self.trader.leverage}x = {pos_val} USDT/trade`\n"
            f"📊 Pares: `{', '.join(self.trader.pairs)}`\n"
            f"Usa /start para ver comandos."
        )

        await self.app.initialize()
        await self.app.start()
        await self.app.updater.start_polling(allowed_updates=Update.ALL_TYPES)

        while True:
            await asyncio.sleep(3600)
