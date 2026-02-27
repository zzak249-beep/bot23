"""
RSI BREAKOUT SCANNER - BingX All Markets 24/7
Escanea TODOS los pares USDT perpetuos de BingX
Long:  RSI 10-25  (sobreventa extrema)
Short: RSI 80-90  (sobrecompra extrema)
Entry: 8 USDT x 7x leverage
Exit:  Trailing stop - deja correr hasta agotar profit
"""

import os
import time
import logging
import requests
import ccxt
import pandas as pd
import numpy as np
from datetime import datetime, timezone

# ══════════════════════════════════════════════════════════
# LOGGING
# ══════════════════════════════════════════════════════════
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.StreamHandler()]
)
log = logging.getLogger("rsi_scanner")

# ══════════════════════════════════════════════════════════
# CONFIG - Variables de entorno Railway
# ══════════════════════════════════════════════════════════
API_KEY         = os.environ.get("BINGX_API_KEY",       "")
API_SECRET      = os.environ.get("BINGX_API_SECRET",    "")
TG_TOKEN        = os.environ.get("TELEGRAM_BOT_TOKEN",  "")
TG_CHAT_ID      = os.environ.get("TELEGRAM_CHAT_ID",    "")

TIMEFRAME       = os.environ.get("TIMEFRAME",           "15m")
POLL_SECS       = int(os.environ.get("POLL_SECONDS",    "120"))
USDT_PER_TRADE  = float(os.environ.get("USDT_PER_TRADE","8.0"))
LEVERAGE        = int(os.environ.get("LEVERAGE",        "7"))
RSI_LONG_MIN    = float(os.environ.get("RSI_LONG_MIN",  "10"))
RSI_LONG_MAX    = float(os.environ.get("RSI_LONG_MAX",  "25"))
RSI_SHORT_MIN   = float(os.environ.get("RSI_SHORT_MIN", "80"))
RSI_SHORT_MAX   = float(os.environ.get("RSI_SHORT_MAX", "90"))
MAX_OPEN_TRADES = int(os.environ.get("MAX_OPEN_TRADES", "10"))
TRAILING_PCT    = float(os.environ.get("TRAILING_PCT",  "3.0"))
SL_PCT          = float(os.environ.get("SL_PCT",        "5.0"))
RSI_LEN         = 14
CANDLE_LIMIT    = 200


# ══════════════════════════════════════════════════════════
# TELEGRAM
# ══════════════════════════════════════════════════════════
def tg(msg: str):
    if not TG_TOKEN or not TG_CHAT_ID:
        return
    try:
        requests.post(
            f"https://api.telegram.org/bot{TG_TOKEN}/sendMessage",
            data={"chat_id": TG_CHAT_ID, "text": msg, "parse_mode": "HTML"},
            timeout=10
        )
    except Exception as e:
        log.warning(f"TG error: {e}")


def now_str() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")


# ══════════════════════════════════════════════════════════
# INDICADORES
# ══════════════════════════════════════════════════════════
def calc_rsi(closes: list) -> float:
    if len(closes) < RSI_LEN + 1:
        return 50.0
    s     = pd.Series(closes, dtype=float)
    delta = s.diff()
    gain  = delta.clip(lower=0).ewm(span=RSI_LEN, adjust=False).mean()
    loss  = (-delta.clip(upper=0)).ewm(span=RSI_LEN, adjust=False).mean()
    rs    = gain / loss.replace(0, np.nan)
    rsi   = 100 - (100 / (1 + rs))
    val   = float(rsi.iloc[-1])
    return val if not np.isnan(val) else 50.0


# ══════════════════════════════════════════════════════════
# ESTADO GLOBAL
# ══════════════════════════════════════════════════════════
# open_trades[symbol] = {side, entry, peak, sl, size, rsi_entry, entry_time}
open_trades: dict = {}
stats = {"wins": 0, "losses": 0, "total_pnl": 0.0, "total_trades": 0}


# ══════════════════════════════════════════════════════════
# EXCHANGE HELPERS
# ══════════════════════════════════════════════════════════
def build_exchange():
    ex = ccxt.bingx({
        "apiKey":  API_KEY,
        "secret":  API_SECRET,
        "options": {"defaultType": "swap"},
    })
    ex.load_markets()
    return ex


def get_all_usdt_pairs(ex) -> list:
    pairs = []
    for sym, mkt in ex.markets.items():
        if (mkt.get("quote") == "USDT" and
                mkt.get("swap") and
                mkt.get("active") and
                ":USDT" in sym):
            pairs.append(sym)
    return sorted(pairs)


def fetch_closes(ex, symbol: str) -> list:
    try:
        raw = ex.fetch_ohlcv(symbol, timeframe=TIMEFRAME, limit=CANDLE_LIMIT)
        if not raw or len(raw) < 20:
            return []
        return [float(c[4]) for c in raw[:-1]]
    except Exception:
        return []


def get_price(ex, symbol: str) -> float:
    try:
        return float(ex.fetch_ticker(symbol)["last"])
    except Exception:
        return 0.0


def get_balance(ex) -> float:
    try:
        return float(ex.fetch_balance()["USDT"]["free"])
    except Exception:
        return 0.0


def set_leverage_safe(ex, symbol: str):
    try:
        ex.set_leverage(LEVERAGE, symbol)
    except Exception:
        pass


def get_live_positions(ex) -> dict:
    result = {}
    try:
        for p in ex.fetch_positions():
            sym = p.get("symbol")
            if sym and abs(float(p.get("contracts", 0) or 0)) > 0:
                result[sym] = p
    except Exception as e:
        log.warning(f"fetch_positions: {e}")
    return result


# ══════════════════════════════════════════════════════════
# TRADE EXECUTION
# ══════════════════════════════════════════════════════════
def open_trade(ex, symbol: str, side: str, rsi_val: float):
    try:
        price = get_price(ex, symbol)
        if price <= 0:
            return

        set_leverage_safe(ex, symbol)

        notional = USDT_PER_TRADE * LEVERAGE
        amount   = notional / price
        amount   = float(ex.amount_to_precision(symbol, amount))
        if amount <= 0:
            return

        order       = ex.create_order(symbol, "market", side, amount)
        entry_price = float(order.get("average") or price)

        # Stop Loss duro
        sl_mult    = (1 - SL_PCT / 100) if side == "buy" else (1 + SL_PCT / 100)
        sl_price   = float(ex.price_to_precision(symbol, entry_price * sl_mult))
        close_side = "sell" if side == "buy" else "buy"

        try:
            ex.create_order(symbol, "stop_market", close_side, amount, None,
                            {"stopPrice": sl_price, "reduceOnly": True})
        except Exception as e:
            log.warning(f"SL failed {symbol}: {e}")

        open_trades[symbol] = {
            "side":       side,
            "entry":      entry_price,
            "peak":       entry_price,
            "sl":         sl_price,
            "size":       amount,
            "rsi_entry":  rsi_val,
            "entry_time": now_str(),
        }

        label = "LONG" if side == "buy" else "SHORT"
        emoji = "🟢" if side == "buy" else "🔴"
        coin  = symbol.replace("/USDT:USDT", "")
        log.info(f"[OPEN] {label} {symbol} @ {entry_price:.6f} RSI={rsi_val:.1f}")
        tg(
            f"{emoji} <b>{label}</b> — <b>{coin}</b>\n"
            f"━━━━━━━━━━━━━━━━━━━\n"
            f"💵 Entrada: <code>{entry_price:.6f}</code>\n"
            f"📊 RSI: <b>{rsi_val:.1f}</b>\n"
            f"💰 {USDT_PER_TRADE}$ x{LEVERAGE} = {notional:.0f}$ nocional\n"
            f"🛑 SL: <code>{sl_price:.6f}</code> (-{SL_PCT}%)\n"
            f"📈 Trail: {TRAILING_PCT}% desde el pico\n"
            f"🔓 Trades abiertos: {len(open_trades)}/{MAX_OPEN_TRADES}\n"
            f"⏰ {now_str()}"
        )

    except Exception as e:
        log.error(f"open_trade {symbol}: {e}")


def close_trade(ex, symbol: str, reason: str, current_price: float = 0):
    trade = open_trades.get(symbol)
    if not trade:
        return

    try:
        ex.cancel_all_orders(symbol)
    except Exception:
        pass

    side       = trade["side"]
    size       = trade["size"]
    entry      = trade["entry"]
    close_side = "sell" if side == "buy" else "buy"

    if current_price <= 0:
        current_price = get_price(ex, symbol)

    try:
        ex.create_order(symbol, "market", close_side, size,
                        params={"reduceOnly": True})
    except Exception as e:
        log.error(f"close_trade {symbol}: {e}")

    # PnL calculation
    if side == "buy":
        pnl = (current_price - entry) / entry * USDT_PER_TRADE * LEVERAGE
    else:
        pnl = (entry - current_price) / entry * USDT_PER_TRADE * LEVERAGE

    stats["total_pnl"]    += pnl
    stats["total_trades"] += 1
    if pnl > 0:
        stats["wins"] += 1
    else:
        stats["losses"] += 1

    wr    = stats["wins"] / max(stats["total_trades"], 1) * 100
    emoji = "✅" if pnl > 0 else "❌"
    coin  = symbol.replace("/USDT:USDT", "")
    log.info(f"[CLOSE] {symbol} | {reason} | pnl={pnl:+.2f}$")
    tg(
        f"{emoji} <b>CERRADO</b> — <b>{coin}</b>\n"
        f"━━━━━━━━━━━━━━━━━━━\n"
        f"📌 Razon: {reason}\n"
        f"💵 Entrada: <code>{entry:.6f}</code>\n"
        f"💵 Salida:  <code>{current_price:.6f}</code>\n"
        f"{'💰' if pnl > 0 else '💸'} PnL: <b>${pnl:+.2f}</b>\n"
        f"━━━━━━━━━━━━━━━━━━━\n"
        f"✅ {stats['wins']}W  ❌ {stats['losses']}L  "
        f"WR:{wr:.0f}%  PnL total:${stats['total_pnl']:+.2f}\n"
        f"⏰ {now_str()}"
    )

    if symbol in open_trades:
        del open_trades[symbol]


# ══════════════════════════════════════════════════════════
# TRAILING STOP
# ══════════════════════════════════════════════════════════
def check_trailing(ex, symbol: str, price: float) -> bool:
    """Returns True if trailing stop was hit."""
    if symbol not in open_trades or price <= 0:
        return False

    trade = open_trades[symbol]
    side  = trade["side"]
    trail = TRAILING_PCT / 100

    if side == "buy":
        if price > trade["peak"]:
            open_trades[symbol]["peak"] = price
        stop = open_trades[symbol]["peak"] * (1 - trail)
        if price <= stop:
            peak_gain = (open_trades[symbol]["peak"] - trade["entry"]) / trade["entry"] * 100
            close_trade(ex, symbol, f"TRAILING STOP (pico +{peak_gain:.1f}%)", price)
            return True
    else:
        if price < trade["peak"]:
            open_trades[symbol]["peak"] = price
        stop = open_trades[symbol]["peak"] * (1 + trail)
        if price >= stop:
            peak_gain = (trade["entry"] - open_trades[symbol]["peak"]) / trade["entry"] * 100
            close_trade(ex, symbol, f"TRAILING STOP (pico +{peak_gain:.1f}%)", price)
            return True

    return False


def record_external_close(symbol: str, price: float):
    """Record a trade that was closed externally by the exchange (SL/TP)."""
    trade = open_trades.get(symbol)
    if not trade:
        return

    side  = trade["side"]
    entry = trade["entry"]

    if side == "buy":
        pnl = (price - entry) / entry * USDT_PER_TRADE * LEVERAGE
    else:
        pnl = (entry - price) / entry * USDT_PER_TRADE * LEVERAGE

    stats["total_pnl"]    += pnl
    stats["total_trades"] += 1
    if pnl > 0:
        stats["wins"] += 1
    else:
        stats["losses"] += 1

    wr    = stats["wins"] / max(stats["total_trades"], 1) * 100
    emoji = "✅" if pnl > 0 else "❌"
    coin  = symbol.replace("/USDT:USDT", "")
    tg(
        f"{emoji} <b>CERRADO (exchange)</b> — {coin}\n"
        f"PnL estimado: ${pnl:+.2f}\n"
        f"W:{stats['wins']} L:{stats['losses']} WR:{wr:.0f}%  "
        f"Total:${stats['total_pnl']:+.2f}\n"
        f"⏰ {now_str()}"
    )
    del open_trades[symbol]


# ══════════════════════════════════════════════════════════
# MAIN
# ══════════════════════════════════════════════════════════
def main():
    log.info("=" * 60)
    log.info("  RSI BREAKOUT SCANNER - BingX All Markets  STARTING")
    log.info("=" * 60)

    if not API_KEY or not API_SECRET:
        log.warning("DRY-RUN: Set BINGX_API_KEY and BINGX_API_SECRET.")
        while True:
            log.info("DRY-RUN active - no API keys set")
            time.sleep(POLL_SECS)

    ex      = build_exchange()
    pairs   = get_all_usdt_pairs(ex)
    balance = get_balance(ex)

    log.info(f"Connected | Pairs: {len(pairs)} | Balance: ${balance:.2f} USDT")
    log.info(f"Config: {USDT_PER_TRADE}$ x{LEVERAGE} | RSI L:{RSI_LONG_MIN}-{RSI_LONG_MAX} S:{RSI_SHORT_MIN}-{RSI_SHORT_MAX}")
    log.info(f"Trail: {TRAILING_PCT}% | SL: {SL_PCT}% | Max trades: {MAX_OPEN_TRADES}")

    tg(
        f"🚀 <b>RSI BREAKOUT SCANNER INICIADO</b>\n"
        f"━━━━━━━━━━━━━━━━━━━━━━━\n"
        f"🪙 <b>Pares:</b> {len(pairs)} (todos USDT perpetuos)\n"
        f"⏱ <b>Timeframe:</b> {TIMEFRAME}\n"
        f"💰 <b>Por trade:</b> {USDT_PER_TRADE}$ x {LEVERAGE}x\n"
        f"📊 <b>RSI Long:</b> {RSI_LONG_MIN} - {RSI_LONG_MAX}\n"
        f"📊 <b>RSI Short:</b> {RSI_SHORT_MIN} - {RSI_SHORT_MAX}\n"
        f"📈 <b>Trailing stop:</b> {TRAILING_PCT}%\n"
        f"🛑 <b>Stop Loss duro:</b> {SL_PCT}%\n"
        f"🔢 <b>Max trades abiertos:</b> {MAX_OPEN_TRADES}\n"
        f"💵 <b>Balance Futuros:</b> ${balance:.2f} USDT\n"
        f"⏰ {now_str()}"
    )

    scan_n = 0

    while True:
        try:
            scan_n += 1
            ts = datetime.now(timezone.utc).strftime("%H:%M:%S")
            log.info(f"{'─'*50}")
            log.info(f"SCAN #{scan_n} | {ts} | Open:{len(open_trades)}/{MAX_OPEN_TRADES}")

            # ── Refresh pair list every 100 scans ──
            if scan_n % 100 == 0:
                pairs   = get_all_usdt_pairs(ex)
                balance = get_balance(ex)
                log.info(f"Pairs refreshed: {len(pairs)} | Balance: ${balance:.2f}")

            # ── Check existing positions ──
            live_pos = get_live_positions(ex)

            for sym in list(open_trades.keys()):
                if sym not in live_pos:
                    # Closed by exchange (SL hit)
                    price = get_price(ex, sym)
                    record_external_close(sym, price)
                    continue

                # Update trailing stop
                price = get_price(ex, sym)
                check_trailing(ex, sym, price)

            # ── Scan for new entries ──
            if len(open_trades) >= MAX_OPEN_TRADES:
                log.info(f"Max trades open ({MAX_OPEN_TRADES}) — waiting")
                time.sleep(POLL_SECS)
                continue

            long_signals  = 0
            short_signals = 0

            for symbol in pairs:
                if symbol in open_trades:
                    continue
                if len(open_trades) >= MAX_OPEN_TRADES:
                    break

                closes = fetch_closes(ex, symbol)
                if not closes:
                    continue

                rsi_val = calc_rsi(closes)

                if RSI_LONG_MIN <= rsi_val <= RSI_LONG_MAX:
                    log.info(f"  LONG  {symbol:30s} RSI={rsi_val:.1f}")
                    open_trade(ex, symbol, "buy", rsi_val)
                    long_signals += 1
                    time.sleep(0.5)

                elif RSI_SHORT_MIN <= rsi_val <= RSI_SHORT_MAX:
                    log.info(f"  SHORT {symbol:30s} RSI={rsi_val:.1f}")
                    open_trade(ex, symbol, "sell", rsi_val)
                    short_signals += 1
                    time.sleep(0.5)

            wr = stats["wins"] / max(stats["total_trades"], 1) * 100
            log.info(
                f"Scan #{scan_n} done | "
                f"Senales: L={long_signals} S={short_signals} | "
                f"Open:{len(open_trades)} | "
                f"W:{stats['wins']} L:{stats['losses']} "
                f"WR:{wr:.0f}% PnL:${stats['total_pnl']:+.2f}"
            )

            # ── Summary Telegram cada 20 scans ──
            if scan_n % 20 == 0 and stats["total_trades"] > 0:
                open_list = "\n".join([
                    f"  {'L' if v['side'] == 'buy' else 'S'} "
                    f"{k.replace('/USDT:USDT', ''):<10} "
                    f"@ {v['entry']:.4f} RSI:{v['rsi_entry']:.0f}"
                    for k, v in list(open_trades.items())[:15]
                ]) or "  Ninguna"

                tg(
                    f"📡 <b>RESUMEN #{scan_n}</b>\n"
                    f"━━━━━━━━━━━━━━━━━━━━━━━\n"
                    f"🔓 <b>Abiertas:</b> {len(open_trades)}/{MAX_OPEN_TRADES}\n"
                    f"<code>{open_list}</code>\n"
                    f"━━━━━━━━━━━━━━━━━━━━━━━\n"
                    f"✅ {stats['wins']}W  ❌ {stats['losses']}L\n"
                    f"📊 WR: {wr:.0f}%\n"
                    f"💰 PnL total: <b>${stats['total_pnl']:+.2f}</b>\n"
                    f"⏰ {now_str()}"
                )

        except ccxt.NetworkError as e:
            log.warning(f"Network error: {e} — retrying")
            time.sleep(30)
            continue
        except ccxt.ExchangeError as e:
            log.error(f"Exchange error: {e}")
            tg(f"❌ <b>Exchange error:</b>\n<code>{e}</code>")
        except KeyboardInterrupt:
            log.info("Stopped by user.")
            tg("🛑 <b>Bot detenido manualmente.</b>")
            break
        except Exception as e:
            log.exception(f"Unexpected error: {e}")
            tg(f"🔥 <b>Error:</b>\n<code>{str(e)[:200]}</code>")

        time.sleep(POLL_SECS)


if __name__ == "__main__":
    main()
