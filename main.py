"""
╔══════════════════════════════════════════════════════════════════╗
║         SATY ELITE v11 — REAL MONEY EDITION                     ║
║         BingX Perpetual Futures · 12 Trades · 24/7             ║
╠══════════════════════════════════════════════════════════════════╣
║  CAMBIOS v11 vs v10:                                            ║
║                                                                  ║
║  · MAX_OPEN_TRADES = 12                                         ║
║  · 24/7 siempre activo (USE_SESSION = false hardcoded)          ║
║  · Una sola posición por moneda base:                           ║
║    Si hay LONG en BTC, no abre SHORT en BTC y viceversa        ║
║  · Volumen mínimo reducido a 100,000 USDT (incluye altcoins     ║
║    nuevos y de bajo volumen)                                    ║
║  · TOP_N_SYMBOLS = 300 (escanea el universo completo)          ║
║  · MAX_SPREAD_PCT = 1.0% (acepta pares menos líquidos)         ║
║  · Score mínimo reducido a 4 para pares nuevos/pequeños        ║
║  · Detección automática de pares nuevos listados en BingX      ║
║  · Prioridad a pares con mayor score (no por volumen)          ║
╚══════════════════════════════════════════════════════════════════╝

VARIABLES OBLIGATORIAS:
    BINGX_API_KEY
    BINGX_API_SECRET
    TELEGRAM_BOT_TOKEN
    TELEGRAM_CHAT_ID

VARIABLES OPCIONALES:
    MAX_OPEN_TRADES      (def: 12)
    MIN_SCORE            (def: 4)
    FIXED_USDT           USDT fijos por trade (def: 8)
    MAX_DRAWDOWN         % circuit breaker    (def: 15)
    DAILY_LOSS_LIMIT     % pérdida diaria max (def: 8)
    MIN_VOLUME_USDT      volumen mín 24h      (def: 100000)
    TOP_N_SYMBOLS        pares a escanear     (def: 300)
    POLL_SECONDS         segundos ciclo       (def: 60)
    TIMEFRAME            TF entrada           (def: 5m)
    HTF1                                      (def: 15m)
    HTF2                                      (def: 1h)
    BTC_FILTER           filtro macro BTC     (def: true)
    COOLDOWN_MIN         pausa tras cierre    (def: 20)
    MAX_SPREAD_PCT       spread máximo %      (def: 1.0)
    BLACKLIST            pares excluidos
"""

import os, time, logging, csv
from datetime import datetime, timezone
from dataclasses import dataclass, field
from typing import Optional, Dict, List, Tuple
from concurrent.futures import ThreadPoolExecutor, as_completed

import requests
import ccxt
import pandas as pd
import numpy as np

# ══════════════════════════════════════════════════════════
# LOGGING
# ══════════════════════════════════════════════════════════
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.StreamHandler()]
)
log = logging.getLogger("saty_v11")

# ══════════════════════════════════════════════════════════
# CONFIG — optimizado para dinero real + universo amplio
# ══════════════════════════════════════════════════════════
API_KEY    = os.environ.get("BINGX_API_KEY",    "")
API_SECRET = os.environ.get("BINGX_API_SECRET", "")
TF         = os.environ.get("TIMEFRAME",  "5m")
HTF1       = os.environ.get("HTF1",       "15m")
HTF2       = os.environ.get("HTF2",       "1h")
POLL_SECS  = int(os.environ.get("POLL_SECONDS", "60"))
TG_TOKEN   = os.environ.get("TELEGRAM_BOT_TOKEN", "")
TG_CHAT_ID = os.environ.get("TELEGRAM_CHAT_ID",   "")

_bl = os.environ.get("BLACKLIST", "")
BLACKLIST: List[str] = [s.strip() for s in _bl.split(",") if s.strip()]

# ── Parámetros clave v11 ──
FIXED_USDT       = float(os.environ.get("FIXED_USDT",       "8.0"))
MAX_OPEN_TRADES  = int(os.environ.get("MAX_OPEN_TRADES",    "12"))
MIN_SCORE        = int(os.environ.get("MIN_SCORE",          "4"))
CB_DD            = float(os.environ.get("MAX_DRAWDOWN",     "15.0"))
DAILY_LOSS_LIMIT = float(os.environ.get("DAILY_LOSS_LIMIT", "8.0"))
COOLDOWN_MIN     = int(os.environ.get("COOLDOWN_MIN",       "20"))
MAX_SPREAD_PCT   = float(os.environ.get("MAX_SPREAD_PCT",   "1.0"))

# Universo amplio: bajo volumen para incluir altcoins nuevos
MIN_VOLUME_USDT = float(os.environ.get("MIN_VOLUME_USDT", "100000"))
TOP_N_SYMBOLS   = int(os.environ.get("TOP_N_SYMBOLS",     "300"))

# 24/7 siempre activo — hardcoded, no configurable
USE_SESSION = False
BTC_FILTER  = os.environ.get("BTC_FILTER", "true").lower() == "true"

# ── Indicadores ──
FAST_LEN  = 8;   PIVOT_LEN = 21; BIAS_LEN  = 48; SLOW_LEN  = 200
ADX_LEN   = 14;  ADX_MIN   = 16; RSI_LEN   = 14; ATR_LEN   = 14
VOL_LEN   = 20;  OSC_LEN   = 3;  SWING_LB  = 10
MACD_FAST = 12;  MACD_SLOW = 26; MACD_SIG  = 9;  STOCH_LEN = 14

# ── Exits ──
TP1_MULT   = 1.2
TP2_MULT   = 3.0
SL_ATR     = 1.0

# ── RSI extremo ──
RSI_OB_LOW = 10;  RSI_OB_HIGH = 25   # sobreventa
RSI_OS_LOW = 78;  RSI_OS_HIGH = 90   # sobrecompra

# ── Risk ──
MAX_CONSEC_LOSS = 3
USE_CB          = True

# ── Hedge mode (auto-detectado) ──
HEDGE_MODE: bool = False

# ── CSV ──
CSV_PATH = "/tmp/saty_v11_trades.csv"


# ══════════════════════════════════════════════════════════
# CACHE OHLCV
# ══════════════════════════════════════════════════════════
_cache: Dict[str, Tuple[float, pd.DataFrame]] = {}
CACHE_TTL = 55

def fetch_df(ex: ccxt.Exchange, symbol: str, tf: str, limit: int = 400) -> pd.DataFrame:
    key = f"{symbol}|{tf}"
    now = time.time()
    if key in _cache:
        ts, df = _cache[key]
        if now - ts < CACHE_TTL:
            return df
    raw = ex.fetch_ohlcv(symbol, timeframe=tf, limit=limit)
    df  = pd.DataFrame(raw, columns=["timestamp","open","high","low","close","volume"])
    df["timestamp"] = pd.to_datetime(df["timestamp"], unit="ms")
    df.set_index("timestamp", inplace=True)
    df = df.astype(float)
    _cache[key] = (now, df)
    return df

def clear_cache():
    _cache.clear()


# ══════════════════════════════════════════════════════════
# ESTADO
# ══════════════════════════════════════════════════════════
@dataclass
class TradeState:
    symbol:         str   = ""
    side:           str   = ""       # "long" | "short"
    base:           str   = ""       # moneda base ej "BTC"
    entry_price:    float = 0.0
    tp1_price:      float = 0.0
    tp2_price:      float = 0.0
    sl_price:       float = 0.0
    sl_moved_be:    bool  = False
    tp1_hit:        bool  = False
    trail_high:     float = 0.0
    trail_low:      float = 0.0
    peak_price:     float = 0.0
    prev_price:     float = 0.0
    stall_count:    int   = 0
    trail_phase:    str   = "normal"
    max_profit_pct: float = 0.0
    entry_score:    int   = 0
    entry_time:     str   = ""
    contracts:      float = 0.0
    atr_entry:      float = 0.0


@dataclass
class BotState:
    wins:           int   = 0
    losses:         int   = 0
    gross_profit:   float = 0.0
    gross_loss:     float = 0.0
    consec_losses:  int   = 0
    peak_equity:    float = 0.0
    total_pnl:      float = 0.0
    daily_pnl:      float = 0.0
    daily_reset_ts: float = 0.0
    last_heartbeat: float = 0.0

    trades:    Dict[str, TradeState] = field(default_factory=dict)
    cooldowns: Dict[str, float]      = field(default_factory=dict)
    rsi_alerts:Dict[str, float]      = field(default_factory=dict)

    # BTC macro
    btc_bull: bool  = True
    btc_bear: bool  = False
    btc_rsi:  float = 50.0

    def open_count(self) -> int:
        return len(self.trades)

    def bases_open(self) -> Dict[str, str]:
        """Devuelve {base: side} para todas las posiciones abiertas."""
        return {t.base: t.side for t in self.trades.values()}

    def base_has_trade(self, base: str) -> bool:
        """True si ya hay un trade abierto (long o short) en esta moneda base."""
        return base in self.bases_open()

    def win_rate(self) -> float:
        t = self.wins + self.losses
        return (self.wins / t * 100) if t else 0.0

    def profit_factor(self) -> float:
        return (self.gross_profit / self.gross_loss) if self.gross_loss else 0.0

    def score_bar(self, score: int, mx: int = 12) -> str:
        return "█" * min(score, mx) + "░" * (mx - min(score, mx))

    def cb_active(self) -> bool:
        if not USE_CB or self.peak_equity <= 0: return False
        dd = (self.peak_equity - (self.peak_equity + self.total_pnl)) / self.peak_equity * 100
        return dd >= CB_DD

    def daily_limit_hit(self) -> bool:
        if self.peak_equity <= 0: return False
        return self.daily_pnl < 0 and abs(self.daily_pnl) / self.peak_equity * 100 >= DAILY_LOSS_LIMIT

    def risk_mult(self) -> float:
        return 0.5 if self.consec_losses >= MAX_CONSEC_LOSS else 1.0

    def in_cooldown(self, symbol: str) -> bool:
        return time.time() - self.cooldowns.get(symbol, 0) < COOLDOWN_MIN * 60

    def set_cooldown(self, symbol: str):
        self.cooldowns[symbol] = time.time()

    def reset_daily(self):
        now = time.time()
        if now - self.daily_reset_ts > 86400:
            self.daily_pnl     = 0.0
            self.daily_reset_ts = now
            log.info("Daily PnL reseteado")


state = BotState()


# ══════════════════════════════════════════════════════════
# CSV LOG
# ══════════════════════════════════════════════════════════
def log_csv(action: str, t: TradeState, price: float, pnl: float = 0.0):
    try:
        exists = os.path.exists(CSV_PATH)
        with open(CSV_PATH, "a", newline="") as f:
            w = csv.writer(f)
            if not exists:
                w.writerow(["ts","action","symbol","base","side","score",
                            "entry","exit","pnl","contracts"])
            w.writerow([utcnow(), action, t.symbol, t.base, t.side,
                        t.entry_score, t.entry_price, price,
                        round(pnl, 4), t.contracts])
    except Exception as e:
        log.warning(f"CSV: {e}")


# ══════════════════════════════════════════════════════════
# TELEGRAM
# ══════════════════════════════════════════════════════════
def tg(msg: str):
    if not TG_TOKEN or not TG_CHAT_ID: return
    try:
        requests.post(
            f"https://api.telegram.org/bot{TG_TOKEN}/sendMessage",
            data={"chat_id": TG_CHAT_ID, "text": msg, "parse_mode": "HTML"},
            timeout=10
        )
    except Exception as e:
        log.warning(f"TG: {e}")

def utcnow() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")

def tg_startup(balance: float, n: int):
    tg(
        f"<b>🚀 SATY ELITE v11 — REAL MONEY</b>\n"
        f"══════════════════════════════\n"
        f"🌍 Universo: {n} pares USDT (vol≥${MIN_VOLUME_USDT/1000:.0f}K)\n"
        f"⚙️ Modo: {'HEDGE' if HEDGE_MODE else 'ONE-WAY'} | 24/7\n"
        f"⏱ {TF} · {HTF1} · {HTF2}\n"
        f"🎯 Score min: {MIN_SCORE}/12 | Max trades: {MAX_OPEN_TRADES}\n"
        f"💰 Balance: ${balance:.2f} | ${FIXED_USDT:.0f} por trade\n"
        f"🛡 CB: -{CB_DD}% | Límite diario: -{DAILY_LOSS_LIMIT}%\n"
        f"⚠️ 1 posición por moneda base (no duplica)\n"
        f"₿ Filtro BTC: {'✅' if BTC_FILTER else '❌'}\n"
        f"⏰ {utcnow()}"
    )

def tg_signal(t: TradeState, row: pd.Series):
    e = "🟢" if t.side == "long" else "🔴"
    sl_d = abs(t.sl_price - t.entry_price)
    rr1  = abs(t.tp1_price - t.entry_price) / max(sl_d, 1e-9)
    rr2  = abs(t.tp2_price - t.entry_price) / max(sl_d, 1e-9)
    tg(
        f"{e} <b>{'LONG' if t.side=='long' else 'SHORT'}</b> — {t.symbol}\n"
        f"══════════════════════════════\n"
        f"🎯 Score: {t.entry_score}/12  {state.score_bar(t.entry_score)}\n"
        f"💵 Entrada: <code>{t.entry_price:.6g}</code>\n"
        f"🟡 TP1 50%: <code>{t.tp1_price:.6g}</code>  R:R 1:{rr1:.1f}\n"
        f"🟢 TP2 50%: <code>{t.tp2_price:.6g}</code>  R:R 1:{rr2:.1f}\n"
        f"🛑 SL: <code>{t.sl_price:.6g}</code> → BE tras TP1\n"
        f"══════════════════════════════\n"
        f"{rsi_zone_label(float(row['rsi']))} | ADX:{row['adx']:.1f}\n"
        f"MACD:{row['macd_hist']:.5f} | Vol:{row['volume']/row['vol_ma']:.2f}x\n"
        f"ATR:{t.atr_entry:.5f} | ${FIXED_USDT:.0f} fijos\n"
        f"₿{'🟢' if state.btc_bull else '🔴' if state.btc_bear else '⚪'} "
        f"RSI:{state.btc_rsi:.0f}\n"
        f"📊 {state.open_count()}/{MAX_OPEN_TRADES} trades\n"
        f"⏰ {utcnow()}"
    )

def tg_tp1_be(t: TradeState, price: float, pnl: float):
    tg(
        f"🟡 <b>TP1 + BREAK-EVEN</b> — {t.symbol}\n"
        f"💵 <code>{price:.6g}</code> | PnL parcial: ~${pnl:+.2f}\n"
        f"🛡 SL → entrada <code>{t.entry_price:.6g}</code>\n"
        f"⏰ {utcnow()}"
    )

def tg_trail_phase(t: TradeState, phase: str, price: float,
                   retrace: float, trail_m: float):
    icons = {"normal": "🏃", "tight": "⚡", "locked": "🔒"}
    tg(
        f"{icons.get(phase,'⚡')} <b>TRAILING {phase.upper()}</b> — {t.symbol}\n"
        f"Precio: <code>{price:.6g}</code> | Peak: <code>{t.peak_price:.6g}</code>\n"
        f"Retroceso: {retrace:.1f}% | Stop: {trail_m}×ATR\n"
        f"Ganancia max: {t.max_profit_pct:.2f}%\n"
        f"⏰ {utcnow()}"
    )

def tg_close(reason: str, t: TradeState, exit_p: float, pnl: float):
    e = "✅" if pnl > 0 else "❌"
    pct = (pnl / (t.entry_price * t.contracts) * 100) if t.contracts > 0 else 0
    tg(
        f"{e} <b>CERRADO</b> — {t.symbol}\n"
        f"📋 {t.side.upper()} · Score:{t.entry_score}/12 · {reason}\n"
        f"💵 <code>{t.entry_price:.6g}</code> → <code>{exit_p:.6g}</code> "
        f"({pct:+.2f}%)\n"
        f"{'💰' if pnl>0 else '💸'} PnL: ${pnl:+.2f}\n"
        f"📊 {state.wins}W/{state.losses}L · "
        f"WR:{state.win_rate():.1f}% · PF:{state.profit_factor():.2f}\n"
        f"💹 Hoy:${state.daily_pnl:+.2f} · Total:${state.total_pnl:+.2f}\n"
        f"⏰ {utcnow()}"
    )

def tg_rsi_alert(symbol: str, rsi: float, ls: int, ss: int, price: float):
    direction = "📉 LONG rebote" if rsi_extreme_long(rsi) else "📈 SHORT caída"
    tg(
        f"🔔 <b>RSI EXTREMO</b> — {symbol}\n"
        f"{rsi_zone_label(rsi)}\n"
        f"💵 <code>{price:.6g}</code> | {direction}\n"
        f"Score: L:{ls}/12 S:{ss}/12\n"
        f"⏰ {utcnow()}"
    )

def tg_summary(signals: List[dict], n_scanned: int):
    open_lines = "\n".join(
        f"  {'🟢' if ts.side=='long' else '🔴'} {sym} "
        f"({ts.base}) E:{ts.entry_price:.5g} "
        f"{'🛡' if ts.sl_moved_be else ''}"
        for sym, ts in state.trades.items()
    ) or "  (ninguna)"
    top = "\n".join(
        f"  {'🟢' if s['side']=='long' else '🔴'} {s['symbol']} "
        f"{s['score']}/12"
        for s in signals[:5]
    ) or "  (ninguna)"
    tg(
        f"📡 <b>RESUMEN</b> — {n_scanned} pares · {utcnow()}\n"
        f"Top señales:\n{top}\n"
        f"══════════════════════════════\n"
        f"Posiciones ({state.open_count()}/{MAX_OPEN_TRADES}):\n{open_lines}\n"
        f"══════════════════════════════\n"
        f"CB:{'⛔' if state.cb_active() else '✅'} | "
        f"Hoy:${state.daily_pnl:+.2f}\n"
        f"₿{'🟢' if state.btc_bull else '🔴' if state.btc_bear else '⚪'} "
        f"RSI:{state.btc_rsi:.0f} | "
        f"{state.wins}W/{state.losses}L · PF:{state.profit_factor():.2f}"
    )

def tg_heartbeat(balance: float):
    bases = state.bases_open()
    open_str = ", ".join(
        f"{b}({'L' if s=='long' else 'S'})" for b, s in bases.items()
    ) or "ninguna"
    tg(
        f"💓 <b>HEARTBEAT</b> — {utcnow()}\n"
        f"Balance: ${balance:.2f} | Hoy: ${state.daily_pnl:+.2f}\n"
        f"Trades: {state.open_count()}/{MAX_OPEN_TRADES}\n"
        f"Monedas: {open_str}\n"
        f"₿ {'BULL' if state.btc_bull else 'BEAR' if state.btc_bear else 'NEUTRAL'} "
        f"RSI:{state.btc_rsi:.0f}"
    )

def tg_error(msg: str):
    tg(f"🔥 <b>ERROR:</b> <code>{msg[:300]}</code>\n⏰ {utcnow()}")


# ══════════════════════════════════════════════════════════
# RSI ZONES
# ══════════════════════════════════════════════════════════
def rsi_extreme_long(rsi: float) -> bool:
    return RSI_OB_LOW <= rsi <= RSI_OB_HIGH

def rsi_extreme_short(rsi: float) -> bool:
    return RSI_OS_LOW <= rsi <= RSI_OS_HIGH

def rsi_zone_label(rsi: float) -> str:
    if rsi < RSI_OB_LOW:    return f"⚠️ RSI HIPERVENTA {rsi:.1f}"
    if rsi <= RSI_OB_HIGH:  return f"🔥 RSI SOBREVENTA {rsi:.1f}"
    if rsi < 42:             return f"🟢 RSI bajo {rsi:.1f}"
    if rsi <= 58:            return f"⚪ RSI neutral {rsi:.1f}"
    if rsi < RSI_OS_LOW:    return f"🟡 RSI alto {rsi:.1f}"
    if rsi <= RSI_OS_HIGH:  return f"🔥 RSI SOBRECOMPRA {rsi:.1f}"
    return                         f"⚠️ RSI HIPERCOMPRA {rsi:.1f}"


# ══════════════════════════════════════════════════════════
# INDICADORES
# ══════════════════════════════════════════════════════════
def ema(s: pd.Series, n: int) -> pd.Series:
    return s.ewm(span=n, adjust=False).mean()

def sma(s: pd.Series, n: int) -> pd.Series:
    return s.rolling(n).mean()

def calc_atr(df: pd.DataFrame, n: int) -> pd.Series:
    h, l, c = df["high"], df["low"], df["close"]
    tr = pd.concat([(h-l), (h-c.shift()).abs(), (l-c.shift()).abs()], axis=1).max(axis=1)
    return tr.ewm(span=n, adjust=False).mean()

def calc_rsi(s: pd.Series, n: int) -> pd.Series:
    d  = s.diff()
    g  = d.clip(lower=0).ewm(span=n, adjust=False).mean()
    lo = (-d.clip(upper=0)).ewm(span=n, adjust=False).mean()
    return 100 - (100 / (1 + g / lo.replace(0, np.nan)))

def calc_adx(df: pd.DataFrame, n: int) -> Tuple[pd.Series, pd.Series, pd.Series]:
    h, l   = df["high"], df["low"]
    up, dn = h.diff(), -l.diff()
    pdm    = up.where((up > dn) & (up > 0), 0.0)
    mdm    = dn.where((dn > up) & (dn > 0), 0.0)
    atr_s  = calc_atr(df, n)
    dip    = 100 * pdm.ewm(span=n, adjust=False).mean() / atr_s
    dim    = 100 * mdm.ewm(span=n, adjust=False).mean() / atr_s
    dx     = 100 * (dip - dim).abs() / (dip + dim).replace(0, np.nan)
    return dip, dim, dx.ewm(span=n, adjust=False).mean()

def calc_macd(s: pd.Series):
    m  = ema(s, MACD_FAST) - ema(s, MACD_SLOW)
    sg = ema(m, MACD_SIG)
    return m, sg, m - sg

def calc_stoch_rsi(s: pd.Series):
    rsi   = calc_rsi(s, RSI_LEN)
    lo    = rsi.rolling(STOCH_LEN).min()
    hi    = rsi.rolling(STOCH_LEN).max()
    stoch = 100 * (rsi - lo) / (hi - lo).replace(0, np.nan)
    k     = stoch.rolling(3).mean()
    d     = k.rolling(3).mean()
    return k, d


def compute(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    c, h, l, v, o = df["close"], df["high"], df["low"], df["volume"], df["open"]

    df["ema8"]   = ema(c, FAST_LEN)
    df["ema21"]  = ema(c, PIVOT_LEN)
    df["ema48"]  = ema(c, BIAS_LEN)
    df["ema200"] = ema(c, SLOW_LEN)
    df["atr"]    = calc_atr(df, ATR_LEN)
    df["rsi"]    = calc_rsi(c, RSI_LEN)

    dip, dim, adx = calc_adx(df, ADX_LEN)
    df["dip"] = dip; df["dim"] = dim; df["adx"] = adx

    macd, macd_sg, macd_h = calc_macd(c)
    df["macd_hist"]       = macd_h
    df["macd_bull"]       = (macd_h > 0) & (macd_h > macd_h.shift())
    df["macd_bear"]       = (macd_h < 0) & (macd_h < macd_h.shift())
    df["macd_cross_up"]   = (macd > macd_sg) & (macd.shift() <= macd_sg.shift())
    df["macd_cross_down"] = (macd < macd_sg) & (macd.shift() >= macd_sg.shift())

    sk, sd = calc_stoch_rsi(c)
    df["stoch_k"]    = sk; df["stoch_d"] = sd
    df["stoch_bull"] = (sk > sd) & (sk < 80) & (sk.shift() <= sd.shift())
    df["stoch_bear"] = (sk < sd) & (sk > 20) & (sk.shift() >= sd.shift())

    df["osc"]    = ema(((c - df["ema21"]) / (3.0 * df["atr"].replace(0,np.nan))) * 100, OSC_LEN)
    df["osc_up"] = (df["osc"] > 0) & (df["osc"].shift() <= 0)
    df["osc_dn"] = (df["osc"] < 0) & (df["osc"].shift() >= 0)

    bb_std        = c.rolling(PIVOT_LEN).std()
    bb_up         = df["ema21"] + 2.0 * bb_std
    bb_lo         = df["ema21"] - 2.0 * bb_std
    kc_up         = df["ema21"] + 2.0 * df["atr"]
    df["squeeze"] = bb_up < kc_up
    bb_w          = (bb_up - bb_lo) / df["ema21"].replace(0, np.nan)
    df["is_trending"] = (adx > ADX_MIN) & (bb_w > sma(bb_w, 20) * 0.8)

    rng            = (h - l).replace(0, np.nan)
    df["buy_vol"]  = v * (c - l) / rng
    df["sell_vol"] = v * (h - c) / rng
    df["vol_ma"]   = sma(v, VOL_LEN)
    df["vol_spike"]= v > df["vol_ma"] * 1.05
    df["vol_bull"] = df["buy_vol"] > df["sell_vol"]
    df["vol_bear"] = df["sell_vol"] > df["buy_vol"]

    body              = (c - o).abs()
    body_pct          = body / rng.replace(0, np.nan)
    df["bull_candle"] = (c > o) & (body_pct >= 0.30)
    df["bear_candle"] = (c < o) & (body_pct >= 0.30)

    prev_body = (o.shift() - c.shift()).abs()
    df["bull_engulf"] = (c > o) & (o <= c.shift()) & (c >= o.shift()) & (body > prev_body * 0.8)
    df["bear_engulf"] = (c < o) & (o >= c.shift()) & (c <= o.shift()) & (body > prev_body * 0.8)

    df["swing_low"]  = l.rolling(SWING_LB).min()
    df["swing_high"] = h.rolling(SWING_LB).max()

    rsi = df["rsi"]
    df["bull_div"] = (
        (l < l.shift(1)) & (l.shift(1) < l.shift(2)) &
        (rsi > rsi.shift(1)) & (rsi.shift(1) > rsi.shift(2)) & (rsi < 42)
    )
    df["bear_div"] = (
        (h > h.shift(1)) & (h.shift(1) > h.shift(2)) &
        (rsi < rsi.shift(1)) & (rsi.shift(1) < rsi.shift(2)) & (rsi > 58)
    )
    return df


def htf_bias(df: pd.DataFrame) -> Tuple[bool, bool]:
    df  = compute(df)
    row = df.iloc[-2]
    bull = bool(row["close"] > row["ema48"] and row["ema21"] > row["ema48"])
    bear = bool(row["close"] < row["ema48"] and row["ema21"] < row["ema48"])
    return bull, bear

def htf2_macro(df: pd.DataFrame) -> Tuple[bool, bool]:
    df  = compute(df)
    row = df.iloc[-2]
    bull = bool(row["close"] > row["ema48"] and row["ema48"] > row["ema200"])
    bear = bool(row["close"] < row["ema48"] and row["ema48"] < row["ema200"])
    return bull, bear


# ══════════════════════════════════════════════════════════
# SCORE 12 PUNTOS
# ══════════════════════════════════════════════════════════
def confluence_score(row: pd.Series,
                     htf1_bull: bool, htf1_bear: bool,
                     htf2_bull: bool, htf2_bear: bool) -> Tuple[int, int]:
    rsi = float(row["rsi"])

    l1  = bool(row["close"] > row["ema48"] and row["ema8"] > row["ema21"])
    l2  = bool(row["osc_up"])
    l3  = htf1_bull
    l4  = htf2_bull
    l5  = bool(row["adx"] > ADX_MIN and row["dip"] > row["dim"])
    l6  = bool(42 <= rsi <= 78)
    l7  = bool(row["vol_bull"] and row["vol_spike"] and not row["squeeze"])
    l8  = bool(row["bull_candle"] and row["close"] > row["ema21"])
    l9  = bool(row["macd_bull"] or row["macd_cross_up"])
    l10 = bool(row["stoch_bull"] or (row["stoch_k"] > row["stoch_d"] and row["stoch_k"] < 75))
    l11 = rsi_extreme_long(rsi)
    l12 = bool(row["bull_engulf"] or row["bull_div"])

    s1  = bool(row["close"] < row["ema48"] and row["ema8"] < row["ema21"])
    s2  = bool(row["osc_dn"])
    s3  = htf1_bear
    s4  = htf2_bear
    s5  = bool(row["adx"] > ADX_MIN and row["dim"] > row["dip"])
    s6  = bool(22 <= rsi <= 58)
    s7  = bool(row["vol_bear"] and row["vol_spike"] and not row["squeeze"])
    s8  = bool(row["bear_candle"] and row["close"] < row["ema21"])
    s9  = bool(row["macd_bear"] or row["macd_cross_down"])
    s10 = bool(row["stoch_bear"] or (row["stoch_k"] < row["stoch_d"] and row["stoch_k"] > 25))
    s11 = rsi_extreme_short(rsi)
    s12 = bool(row["bear_engulf"] or row["bear_div"])

    return (sum([l1,l2,l3,l4,l5,l6,l7,l8,l9,l10,l11,l12]),
            sum([s1,s2,s3,s4,s5,s6,s7,s8,s9,s10,s11,s12]))


# ══════════════════════════════════════════════════════════
# BTC BIAS
# ══════════════════════════════════════════════════════════
def update_btc_bias(ex: ccxt.Exchange):
    try:
        df  = fetch_df(ex, "BTC/USDT:USDT", "1h", limit=250)
        df  = compute(df)
        row = df.iloc[-2]
        state.btc_bull = bool(row["close"] > row["ema48"] and row["ema48"] > row["ema200"])
        state.btc_bear = bool(row["close"] < row["ema48"] and row["ema48"] < row["ema200"])
        state.btc_rsi  = float(row["rsi"])
        log.info(f"BTC: {'BULL' if state.btc_bull else 'BEAR' if state.btc_bear else 'NEUTRAL'} "
                 f"RSI:{state.btc_rsi:.1f}")
    except Exception as e:
        log.warning(f"BTC bias: {e}")


# ══════════════════════════════════════════════════════════
# EXCHANGE
# ══════════════════════════════════════════════════════════
def build_exchange() -> ccxt.Exchange:
    ex = ccxt.bingx({
        "apiKey": API_KEY, "secret": API_SECRET,
        "options": {"defaultType": "swap"},
        "enableRateLimit": True,
    })
    ex.load_markets()
    return ex

def detect_hedge_mode(ex: ccxt.Exchange) -> bool:
    try:
        for p in ex.fetch_positions()[:5]:
            if p.get("info", {}).get("positionSide", "") in ("LONG", "SHORT"):
                return True
    except Exception:
        pass
    return False

def get_balance(ex: ccxt.Exchange) -> float:
    return float(ex.fetch_balance()["USDT"]["free"])

def get_position(ex: ccxt.Exchange, symbol: str) -> Optional[dict]:
    try:
        for p in ex.fetch_positions([symbol]):
            if abs(float(p.get("contracts", 0) or 0)) > 0:
                return p
    except Exception:
        pass
    return None

def get_all_positions(ex: ccxt.Exchange) -> Dict[str, dict]:
    result: Dict[str, dict] = {}
    try:
        for p in ex.fetch_positions():
            if abs(float(p.get("contracts", 0) or 0)) > 0:
                result[p["symbol"]] = p
    except Exception as e:
        log.warning(f"fetch_positions: {e}")
    return result

def get_last_price(ex: ccxt.Exchange, symbol: str) -> float:
    return float(ex.fetch_ticker(symbol)["last"])

def get_spread_pct(ex: ccxt.Exchange, symbol: str) -> float:
    try:
        ob  = ex.fetch_order_book(symbol, limit=1)
        bid = ob["bids"][0][0] if ob["bids"] else 0
        ask = ob["asks"][0][0] if ob["asks"] else 0
        mid = (bid + ask) / 2
        return ((ask - bid) / mid * 100) if mid > 0 else 999.0
    except Exception:
        return 0.0

def get_min_amount(ex: ccxt.Exchange, symbol: str) -> float:
    try:
        mkt = ex.markets.get(symbol, {})
        return float(mkt.get("limits", {}).get("amount", {}).get("min", 0) or 0)
    except Exception:
        return 0.0

def entry_params(side: str) -> dict:
    if HEDGE_MODE:
        return {"positionSide": "LONG" if side == "buy" else "SHORT"}
    return {}

def exit_params(trade_side: str) -> dict:
    if HEDGE_MODE:
        return {"positionSide": "LONG" if trade_side == "long" else "SHORT",
                "reduceOnly": True}
    return {"reduceOnly": True}


# ══════════════════════════════════════════════════════════
# UNIVERSO — bajo volumen, incluye altcoins nuevos
# ══════════════════════════════════════════════════════════
def get_symbols(ex: ccxt.Exchange) -> List[str]:
    """
    Devuelve todos los pares USDT perpetuos activos con volumen >= MIN_VOLUME_USDT.
    - Sin límite de volumen superior: incluye pares grandes y pequeños.
    - Ordena por volumen DESC para priorizar primero los líquidos,
      pero incluye todos los que pasen el mínimo.
    - Detecta pares nuevos por fecha de listado si está disponible.
    """
    candidates = []
    for sym, mkt in ex.markets.items():
        if not (mkt.get("swap") and mkt.get("quote") == "USDT"
                and mkt.get("active", True)):
            continue
        if sym in BLACKLIST: continue
        candidates.append(sym)

    if not candidates:
        log.warning("Sin candidatos de mercado")
        return []

    log.info(f"Obteniendo tickers para {len(candidates)} pares...")
    try:
        tickers = ex.fetch_tickers(candidates)
    except Exception as e:
        log.warning(f"fetch_tickers: {e}")
        return candidates[:TOP_N_SYMBOLS]

    ranked = []
    for sym in candidates:
        tk  = tickers.get(sym, {})
        vol = float(tk.get("quoteVolume", 0) or 0)
        if vol >= MIN_VOLUME_USDT:
            # Detectar si es par nuevo (listado reciente)
            info    = ex.markets.get(sym, {}).get("info", {})
            created = info.get("onboardDate", 0) or info.get("deliveryDate", 0)
            is_new  = False
            if created:
                try:
                    age_days = (time.time() - float(created) / 1000) / 86400
                    is_new   = age_days < 30
                except Exception:
                    pass
            ranked.append((sym, vol, is_new))

    # Ordenar: primero pares nuevos (priorizarlos), luego por volumen
    ranked.sort(key=lambda x: (not x[2], -x[1]))

    result = [s for s, _, _ in ranked]
    if TOP_N_SYMBOLS > 0:
        result = result[:TOP_N_SYMBOLS]

    new_count = sum(1 for _, _, n in ranked[:len(result)] if n)
    log.info(f"Universo: {len(result)} pares "
             f"(vol≥${MIN_VOLUME_USDT/1000:.0f}K, {new_count} nuevos primero)")
    return result


# ══════════════════════════════════════════════════════════
# APERTURA DE POSICIÓN
# ══════════════════════════════════════════════════════════
def open_trade(ex: ccxt.Exchange, symbol: str, base: str,
               side: str, score: int, row: pd.Series) -> Optional[TradeState]:
    try:
        # Spread check
        spread = get_spread_pct(ex, symbol)
        if spread > MAX_SPREAD_PCT:
            log.warning(f"[{symbol}] spread {spread:.3f}% > {MAX_SPREAD_PCT}% — skip")
            return None

        price   = get_last_price(ex, symbol)
        atr     = float(row["atr"])
        usdt    = FIXED_USDT * state.risk_mult()
        raw_amt = usdt / price
        amount  = float(ex.amount_to_precision(symbol, raw_amt))

        min_amt = get_min_amount(ex, symbol)
        if amount <= 0 or amount < min_amt:
            log.warning(f"[{symbol}] amount {amount:.6f} < min {min_amt}")
            return None
        if amount * price < 3:
            log.warning(f"[{symbol}] notional ${amount*price:.2f} < $3")
            return None

        log.info(f"[OPEN] {symbol} {side.upper()} score={score}/12 "
                 f"${usdt:.1f} size={amount} @ {price:.6g} spread={spread:.2f}%")

        order       = ex.create_order(symbol, "market", side, amount,
                                      params=entry_params(side))
        entry_price = float(order.get("average") or price)
        trade_side  = "long" if side == "buy" else "short"

        if side == "buy":
            sl_p  = min(float(row["swing_low"])  - atr * 0.2,
                        entry_price - atr * SL_ATR)
            tp1_p = entry_price + atr * TP1_MULT
            tp2_p = entry_price + atr * TP2_MULT
        else:
            sl_p  = max(float(row["swing_high"]) + atr * 0.2,
                        entry_price + atr * SL_ATR)
            tp1_p = entry_price - atr * TP1_MULT
            tp2_p = entry_price - atr * TP2_MULT

        tp1_p = float(ex.price_to_precision(symbol, tp1_p))
        tp2_p = float(ex.price_to_precision(symbol, tp2_p))
        sl_p  = float(ex.price_to_precision(symbol, sl_p))

        close_side = "sell" if side == "buy" else "buy"
        half       = float(ex.amount_to_precision(symbol, amount * 0.5))
        ep         = exit_params(trade_side)

        for lbl, qty, px in [("TP1", half, tp1_p), ("TP2", half, tp2_p)]:
            try:
                ex.create_order(symbol, "limit", close_side, qty, px, ep)
                log.info(f"[{symbol}] {lbl} @ {px:.6g}")
            except Exception as e:
                log.warning(f"[{symbol}] {lbl}: {e}")

        try:
            sl_ep = {**ep, "stopPrice": sl_p}
            ex.create_order(symbol, "stop_market", close_side, amount, None, sl_ep)
            log.info(f"[{symbol}] SL @ {sl_p:.6g}")
        except Exception as e:
            log.warning(f"[{symbol}] SL: {e}")

        t = TradeState(
            symbol=symbol,       base=base,         side=trade_side,
            entry_price=entry_price, tp1_price=tp1_p,
            tp2_price=tp2_p,     sl_price=sl_p,
            entry_score=score,   entry_time=utcnow(),
            contracts=amount,    atr_entry=atr,
        )
        if side == "buy": t.trail_high = entry_price
        else:             t.trail_low  = entry_price

        log_csv("OPEN", t, entry_price)
        tg_signal(t, row)
        return t

    except Exception as e:
        log.error(f"[{symbol}] open_trade: {e}")
        tg_error(f"open_trade {symbol}: {e}")
        return None


def move_be(ex: ccxt.Exchange, symbol: str):
    if symbol not in state.trades: return
    t = state.trades[symbol]
    if t.sl_moved_be: return
    try:
        ex.cancel_all_orders(symbol)
    except Exception as e:
        log.warning(f"[{symbol}] cancel for BE: {e}")
    be    = float(ex.price_to_precision(symbol, t.entry_price))
    ep    = {**exit_params(t.side), "stopPrice": be}
    cside = "sell" if t.side == "long" else "buy"
    try:
        ex.create_order(symbol, "stop_market", cside, t.contracts, None, ep)
        t.sl_price    = be
        t.sl_moved_be = True
        log.info(f"[{symbol}] BE @ {be:.6g}")
    except Exception as e:
        log.warning(f"[{symbol}] BE failed: {e}")


def close_trade(ex: ccxt.Exchange, symbol: str, reason: str, price: float):
    if symbol not in state.trades: return
    t = state.trades[symbol]
    try: ex.cancel_all_orders(symbol)
    except Exception as e: log.warning(f"[{symbol}] cancel: {e}")

    pos = get_position(ex, symbol)
    pnl = 0.0
    if pos:
        contracts  = abs(float(pos.get("contracts", 0)))
        close_side = "sell" if t.side == "long" else "buy"
        try:
            ex.create_order(symbol, "market", close_side, contracts,
                            params=exit_params(t.side))
            pnl = ((price - t.entry_price) if t.side == "long"
                   else (t.entry_price - price)) * contracts
        except Exception as e:
            log.error(f"[{symbol}] close: {e}")
            tg_error(f"close {symbol}: {e}")
            return

    if pnl > 0:
        state.wins += 1; state.gross_profit += pnl; state.consec_losses = 0
    elif pnl < 0:
        state.losses += 1; state.gross_loss += abs(pnl); state.consec_losses += 1

    state.total_pnl   += pnl
    state.daily_pnl   += pnl
    state.peak_equity  = max(state.peak_equity, state.peak_equity + pnl)
    state.set_cooldown(symbol)

    log_csv("CLOSE", t, price, pnl)
    tg_close(reason, t, price, pnl)
    del state.trades[symbol]


# ══════════════════════════════════════════════════════════
# GESTIÓN DEL TRADE
# ══════════════════════════════════════════════════════════
def manage_trade(ex: ccxt.Exchange, symbol: str,
                 live_price: float, atr: float,
                 long_score: int, short_score: int,
                 live_pos: Optional[dict],
                 result: Optional[dict] = None):

    if symbol not in state.trades: return
    t = state.trades[symbol]

    # ── Cerrado externamente ──
    if live_pos is None:
        pnl = ((live_price - t.entry_price) if t.side == "long"
               else (t.entry_price - live_price)) * t.contracts
        reason = ("TP2 ALCANZADO"
                  if (t.side=="long" and live_price >= t.tp2_price) or
                     (t.side=="short" and live_price <= t.tp2_price)
                  else "SL ALCANZADO")
        if pnl > 0:
            state.wins += 1; state.gross_profit += pnl; state.consec_losses = 0
        else:
            state.losses += 1; state.gross_loss += abs(pnl); state.consec_losses += 1
        state.total_pnl += pnl; state.daily_pnl += pnl
        state.set_cooldown(symbol)
        log_csv("CLOSE_EXT", t, live_price, pnl)
        tg_close(reason, t, live_price, pnl)
        del state.trades[symbol]
        return

    # ── Cierre por pérdida dinámica (antes de TP1) ──
    if not t.tp1_hit:
        atr_now   = atr if atr > 0 else t.atr_entry
        loss_dist = (t.entry_price - live_price if t.side == "long"
                     else live_price - t.entry_price)
        if loss_dist >= atr_now * 0.8:
            tg(
                f"🛑 <b>CIERRE PÉRDIDA</b> — {symbol}\n"
                f"Contra: {loss_dist:.5f} > 0.8×ATR\n"
                f"<code>{t.entry_price:.6g}</code> → <code>{live_price:.6g}</code>\n"
                f"⏰ {utcnow()}"
            )
            close_trade(ex, symbol, "PÉRDIDA DINÁMICA", live_price)
            return

    # ── Cierre por agotamiento (solo en ganancia) ──
    if result is not None and symbol in state.trades:
        row = result["row"]
        try:
            in_profit = ((t.side == "long"  and live_price > t.entry_price) or
                         (t.side == "short" and live_price < t.entry_price))
            if in_profit:
                rsi_v     = float(row["rsi"])
                adx_v     = float(row["adx"])
                vol_ratio = float(row["volume"]) / max(float(row["vol_ma"]), 1)
                if t.side == "long":
                    e1 = bool(row["macd_bear"])
                    e2 = adx_v < 20
                    e3 = vol_ratio < 0.7
                    e4 = bool(row["bear_div"])
                    e5 = bool(row["osc_dn"])
                    e6 = rsi_v > 72
                else:
                    e1 = bool(row["macd_bull"])
                    e2 = adx_v < 20
                    e3 = vol_ratio < 0.7
                    e4 = bool(row["bull_div"])
                    e5 = bool(row["osc_up"])
                    e6 = rsi_v < 28
                exh = sum([e1, e2, e3, e4, e5, e6])
                if exh >= 3:
                    profit = ((live_price - t.entry_price) if t.side == "long"
                              else (t.entry_price - live_price)) * t.contracts
                    tg(
                        f"🏁 <b>AGOTAMIENTO</b> — {symbol}\n"
                        f"Señales: {exh}/6 | Ganancia: ${profit:+.2f}\n"
                        f"  {'✅' if e1 else '❌'} MACD  "
                        f"  {'✅' if e2 else '❌'} ADX<20\n"
                        f"  {'✅' if e3 else '❌'} Vol↓  "
                        f"  {'✅' if e4 else '❌'} Div RSI\n"
                        f"  {'✅' if e5 else '❌'} OSC   "
                        f"  {'✅' if e6 else '❌'} RSI ext\n"
                        f"⏰ {utcnow()}"
                    )
                    close_trade(ex, symbol, "AGOTAMIENTO", live_price)
                    return
        except Exception as e:
            log.debug(f"[{symbol}] agotamiento: {e}")

    # ── TP1 → Break-even ──
    if not t.tp1_hit:
        hit = ((t.side == "long"  and live_price >= t.tp1_price) or
               (t.side == "short" and live_price <= t.tp1_price))
        if hit:
            t.tp1_hit    = True
            t.peak_price = live_price
            t.prev_price = live_price
            contracts    = float(live_pos.get("contracts", 0))
            pnl_est      = abs(t.tp1_price - t.entry_price) * contracts * 0.5
            move_be(ex, symbol)
            tg_tp1_be(t, live_price, pnl_est)

    # ── Trailing dinámico (post TP1) ──
    if t.tp1_hit and symbol in state.trades:
        atr_t = atr if atr > 0 else t.atr_entry

        # Ganancia actual %
        if t.side == "long":
            cur_pct = (live_price - t.entry_price) / t.entry_price * 100
        else:
            cur_pct = (t.entry_price - live_price) / t.entry_price * 100
        t.max_profit_pct = max(t.max_profit_pct, cur_pct)

        # Peak
        new_peak = (live_price > t.peak_price if t.side == "long"
                    else live_price < t.peak_price)
        if new_peak:
            t.peak_price  = live_price
            t.stall_count = 0
        else:
            t.stall_count += 1

        # Retroceso desde peak
        denom = abs(t.peak_price - t.entry_price)
        if t.side == "long":
            retrace = (t.peak_price - live_price) / max(denom, 1e-9) * 100
        else:
            retrace = (live_price - t.peak_price) / max(denom, 1e-9) * 100

        # Fase
        prev_phase = t.trail_phase
        if retrace > 30:
            t.trail_phase = "locked"
        elif t.stall_count >= 3:
            t.trail_phase = "tight"
        else:
            t.trail_phase = "normal"

        trail_m = {"normal": 0.8, "tight": 0.4, "locked": 0.2}[t.trail_phase]

        if t.trail_phase != prev_phase:
            tg_trail_phase(t, t.trail_phase, live_price, retrace, trail_m)

        if t.side == "long":
            t.trail_high = max(t.trail_high, live_price)
            if live_price <= t.trail_high - atr_t * trail_m:
                close_trade(ex, symbol, f"TRAILING {t.trail_phase.upper()}", live_price)
                return
        else:
            t.trail_low = min(t.trail_low, live_price)
            if live_price >= t.trail_low + atr_t * trail_m:
                close_trade(ex, symbol, f"TRAILING {t.trail_phase.upper()}", live_price)
                return

        t.prev_price = live_price

    # ── Flip de señal (exige score fuerte) ──
    if symbol in state.trades:
        if t.side == "long"  and short_score >= MIN_SCORE + 2:
            close_trade(ex, symbol, "FLIP LONG→SHORT", live_price)
        elif t.side == "short" and long_score >= MIN_SCORE + 2:
            close_trade(ex, symbol, "FLIP SHORT→LONG", live_price)


# ══════════════════════════════════════════════════════════
# SCAN DE UN SÍMBOLO
# ══════════════════════════════════════════════════════════
def scan_symbol(ex: ccxt.Exchange, symbol: str) -> Optional[dict]:
    try:
        df  = fetch_df(ex, symbol, TF,   400)
        df1 = fetch_df(ex, symbol, HTF1, 200)
        df2 = fetch_df(ex, symbol, HTF2, 300)

        df  = compute(df)
        row = df.iloc[-2]

        if pd.isna(row["adx"]) or pd.isna(row["rsi"]) or pd.isna(row["macd_hist"]):
            return None

        htf1_bull, htf1_bear = htf_bias(df1)
        htf2_bull, htf2_bear = htf2_macro(df2)
        ls, ss = confluence_score(row, htf1_bull, htf1_bear, htf2_bull, htf2_bear)

        rsi_v = float(row["rsi"])

        # Alerta RSI extremo (anti-spam 30min)
        if rsi_extreme_long(rsi_v) or rsi_extreme_short(rsi_v):
            now  = time.time()
            last = state.rsi_alerts.get(symbol, 0)
            if now - last > 1800:
                state.rsi_alerts[symbol] = now
                tg_rsi_alert(symbol, rsi_v, ls, ss, float(row["close"]))

        return {
            "symbol":      symbol,
            "base":        symbol.split("/")[0],
            "long_score":  ls,
            "short_score": ss,
            "row":         row,
            "atr":         float(row["atr"]),
            "live_price":  float(row["close"]),
            "is_trending": bool(row["is_trending"]),
            "rsi":         rsi_v,
        }
    except Exception as e:
        log.debug(f"[{symbol}] scan: {e}")
        return None


# ══════════════════════════════════════════════════════════
# MAIN
# ══════════════════════════════════════════════════════════
def main():
    global HEDGE_MODE

    log.info("=" * 65)
    log.info("  SATY ELITE v11 — REAL MONEY · 12 TRADES · 24/7")
    log.info("=" * 65)

    if not (API_KEY and API_SECRET):
        log.warning("DRY-RUN: sin claves API")
        while True: log.info("DRY-RUN..."); time.sleep(POLL_SECS)

    # Conexión con reintentos
    ex = None
    for attempt in range(10):
        try:
            ex = build_exchange()
            log.info("Exchange conectado ✓")
            break
        except Exception as e:
            wait = min(2 ** attempt, 120)
            log.warning(f"Conexión {attempt+1}/10: {e} — retry {wait}s")
            time.sleep(wait)

    if ex is None:
        raise RuntimeError("No se pudo conectar al exchange")

    HEDGE_MODE = detect_hedge_mode(ex)
    log.info(f"Modo cuenta: {'HEDGE' if HEDGE_MODE else 'ONE-WAY'}")

    # Balance con reintentos
    balance = 0.0
    for i in range(10):
        try:
            balance = get_balance(ex)
            break
        except Exception as e:
            log.warning(f"get_balance {i+1}/10: {e}")
            time.sleep(5)

    state.peak_equity    = balance
    state.daily_reset_ts = time.time()
    log.info(f"Balance: ${balance:.2f} USDT")

    # Universo con reintentos
    symbols: List[str] = []
    while not symbols:
        try:
            ex.load_markets()
            symbols = get_symbols(ex)
        except Exception as e:
            log.error(f"get_symbols: {e} — reintento 60s")
            time.sleep(60)

    update_btc_bias(ex)
    tg_startup(balance, len(symbols))

    scan_count    = 0
    REFRESH_EVERY = max(1, 3600 // max(POLL_SECS, 1))
    BTC_REFRESH   = max(1, 900  // max(POLL_SECS, 1))
    HB_INTERVAL   = 3600

    while True:
        ts_start = time.time()
        try:
            scan_count += 1
            state.reset_daily()
            clear_cache()

            log.info(
                f"━━━ SCAN #{scan_count} "
                f"{datetime.now(timezone.utc):%H:%M:%S} "
                f"| {len(symbols)} pares "
                f"| {state.open_count()}/{MAX_OPEN_TRADES} trades "
                f"| bases: {list(state.bases_open().keys())} ━━━"
            )

            # Refrescos periódicos
            if scan_count % REFRESH_EVERY == 0:
                try:
                    ex.load_markets()
                    symbols = get_symbols(ex)
                except Exception as e:
                    log.warning(f"Refresh: {e}")

            if scan_count % BTC_REFRESH == 0:
                update_btc_bias(ex)

            if time.time() - state.last_heartbeat > HB_INTERVAL:
                try:
                    tg_heartbeat(get_balance(ex))
                    state.last_heartbeat = time.time()
                except Exception:
                    pass

            # Guardias
            if state.cb_active():
                log.warning(f"CIRCUIT BREAKER >= {CB_DD}%")
                time.sleep(POLL_SECS); continue

            if state.daily_limit_hit():
                log.warning(f"LÍMITE DIARIO >= {DAILY_LOSS_LIMIT}%")
                time.sleep(POLL_SECS); continue

            # Gestionar posiciones abiertas
            live_positions = get_all_positions(ex)

            for sym in list(state.trades.keys()):
                try:
                    lp    = live_positions.get(sym)
                    lp_   = float(lp["markPrice"]) if lp else get_last_price(ex, sym)
                    res   = scan_symbol(ex, sym)
                    ls    = res["long_score"]  if res else 0
                    ss    = res["short_score"] if res else 0
                    atr   = res["atr"]         if res else state.trades[sym].atr_entry
                    manage_trade(ex, sym, lp_, atr, ls, ss, lp, res)
                except Exception as e:
                    log.warning(f"[{sym}] manage: {e}")

            # Buscar nuevas señales
            new_signals: List[dict] = []

            if state.open_count() < MAX_OPEN_TRADES:
                # Excluir: ya en trades, en cooldown, o moneda base ya abierta
                bases_open = state.bases_open()
                to_scan    = [
                    s for s in symbols
                    if s not in state.trades
                    and not state.in_cooldown(s)
                    and s.split("/")[0] not in bases_open
                ]

                log.info(f"Escaneando {len(to_scan)} pares "
                         f"(excluidas bases: {list(bases_open.keys())})")

                with ThreadPoolExecutor(max_workers=8) as pool:
                    futures = {pool.submit(scan_symbol, ex, s): s for s in to_scan}
                    results = [f.result() for f in as_completed(futures)
                               if f.result() is not None]

                for res in results:
                    base       = res["base"]
                    best_side  = None
                    best_score = 0

                    can_long  = (res["long_score"]  >= MIN_SCORE and
                                 res["is_trending"])
                    can_short = (res["short_score"] >= MIN_SCORE and
                                 res["is_trending"])

                    # Filtro BTC macro
                    if BTC_FILTER:
                        if state.btc_bear: can_long  = False
                        if state.btc_bull: can_short = False

                    # ── REGLA CLAVE: no abrir si base ya tiene trade ──
                    if state.base_has_trade(base):
                        continue

                    if can_long and res["long_score"] > best_score:
                        best_score = res["long_score"];  best_side = "long"
                    if can_short and res["short_score"] > best_score:
                        best_score = res["short_score"]; best_side = "short"

                    if best_side:
                        new_signals.append({
                            "symbol": res["symbol"],
                            "base":   base,
                            "side":   best_side,
                            "score":  best_score,
                            "row":    res["row"],
                            "rsi":    res["rsi"],
                        })

                # Ordenar por score descendente
                new_signals.sort(key=lambda x: x["score"], reverse=True)

                for sig in new_signals:
                    if state.open_count() >= MAX_OPEN_TRADES: break
                    sym  = sig["symbol"]
                    base = sig["base"]
                    # Doble check antes de abrir
                    if sym in state.trades:         continue
                    if state.base_has_trade(base):  continue
                    if state.in_cooldown(sym):       continue

                    order_side = "buy" if sig["side"] == "long" else "sell"
                    t = open_trade(ex, sym, base, order_side, sig["score"], sig["row"])
                    if t:
                        state.trades[sym] = t

            else:
                log.info(f"Max trades alcanzado ({MAX_OPEN_TRADES})")

            # Log ciclo
            elapsed = time.time() - ts_start
            log.info(
                f"✓ {elapsed:.1f}s | señales:{len(new_signals)} | "
                f"{state.wins}W/{state.losses}L | "
                f"hoy:${state.daily_pnl:+.2f} | total:${state.total_pnl:+.2f}"
            )

            if scan_count % 20 == 0:
                tg_summary(new_signals, len(symbols))

        except ccxt.NetworkError as e:
            log.warning(f"Network: {e} — 10s")
            time.sleep(10)
        except ccxt.ExchangeError as e:
            log.error(f"Exchange: {e}")
            tg(f"❌ Exchange: <code>{str(e)[:200]}</code>")
        except KeyboardInterrupt:
            log.info("Detenido.")
            tg("🛑 <b>Bot detenido.</b>")
            break
        except Exception as e:
            log.exception(f"Error: {e}")
            tg_error(str(e))

        elapsed = time.time() - ts_start
        time.sleep(max(0, POLL_SECS - elapsed))


if __name__ == "__main__":
    while True:
        try:
            main()
        except KeyboardInterrupt:
            log.info("Detenido por usuario.")
            break
        except Exception as e:
            log.exception(f"CRASH: {e}")
            try: tg_error(f"CRASH — reinicio en 30s:\n{e}")
            except Exception: pass
            log.info("Reiniciando en 30s...")
            time.sleep(30)
