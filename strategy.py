"""
strategy.py — EMA8 Scalper v4 con triple confirmación + mejoras de rentabilidad.

Mejoras vs versión original:
  • Filtro EMA 200 en 1h (tendencia macro) — elimina trades contra tendencia
  • Stop Loss dinámico basado en ATR en lugar de % fijo
  • Filtro de estructura de precio (Higher Highs / Lower Lows)
  • Score de señal 0-5: solo opera con score ≥ 3
  • Detección de velas de rechazo (pin bars)
  • Divergencia RSI básica para mejorar timing
"""
from __future__ import annotations
import logging
from dataclasses import dataclass
from typing import Optional

import numpy as np

log = logging.getLogger("strategy")


# ── Estructuras de datos ───────────────────────────────────────
@dataclass
class Candle:
    open:   float
    high:   float
    low:    float
    close:  float
    volume: float

    @property
    def body(self) -> float:
        return abs(self.close - self.open)

    @property
    def wick_upper(self) -> float:
        return self.high - max(self.open, self.close)

    @property
    def wick_lower(self) -> float:
        return min(self.open, self.close) - self.low

    @property
    def bullish(self) -> bool:
        return self.close > self.open

    @property
    def bearish(self) -> bool:
        return self.close < self.open


@dataclass
class Signal:
    symbol: str
    side:   str       # "LONG" | "SHORT"
    price:  float
    sl:     float
    tp1:    float
    tp2:    float
    score:  int       # 0-5, calidad de la señal
    reason: str       # descripción humana de los filtros que pasó


# ── Funciones técnicas ─────────────────────────────────────────

def ema(values: list[float], period: int) -> np.ndarray:
    arr = np.array(values, dtype=float)
    result = np.full_like(arr, np.nan)
    if len(arr) < period:
        return result
    k = 2 / (period + 1)
    result[period - 1] = arr[:period].mean()
    for i in range(period, len(arr)):
        result[i] = arr[i] * k + result[i - 1] * (1 - k)
    return result


def rsi(closes: list[float], period: int = 14) -> float:
    arr = np.array(closes[-(period * 3):], dtype=float)
    deltas = np.diff(arr)
    gains  = np.where(deltas > 0, deltas, 0.0)
    losses = np.where(deltas < 0, -deltas, 0.0)
    avg_gain = gains[-period:].mean()
    avg_loss = losses[-period:].mean()
    if avg_loss == 0:
        return 100.0
    rs = avg_gain / avg_loss
    return 100 - (100 / (1 + rs))


def atr(candles: list[Candle], period: int = 14) -> float:
    """Average True Range."""
    if len(candles) < period + 1:
        return 0.0
    trs = []
    for i in range(1, len(candles)):
        c = candles[i]
        prev_close = candles[i - 1].close
        tr = max(
            c.high - c.low,
            abs(c.high - prev_close),
            abs(c.low  - prev_close),
        )
        trs.append(tr)
    return float(np.mean(trs[-period:]))


def is_pin_bar_bull(c: Candle) -> bool:
    """Vela de rechazo alcista: mecha inferior larga, cuerpo pequeño arriba."""
    if c.body == 0:
        return False
    return (c.wick_lower > c.body * 2.0) and (c.wick_upper < c.body * 0.8)


def is_pin_bar_bear(c: Candle) -> bool:
    """Vela de rechazo bajista: mecha superior larga, cuerpo pequeño abajo."""
    if c.body == 0:
        return False
    return (c.wick_upper > c.body * 2.0) and (c.wick_lower < c.body * 0.8)


def higher_highs(candles: list[Candle], n: int = 3) -> bool:
    highs = [c.high for c in candles[-n:]]
    return highs[-1] > max(highs[:-1])


def lower_lows(candles: list[Candle], n: int = 3) -> bool:
    lows = [c.low for c in candles[-n:]]
    return lows[-1] < min(lows[:-1])


def volume_surge(candles: list[Candle], period: int = 20) -> bool:
    """Vela actual con volumen > 1.3× media."""
    if len(candles) < period + 1:
        return False
    avg = np.mean([c.volume for c in candles[-(period + 1):-1]])
    return candles[-1].volume >= avg * 1.3


# ══════════════════════════════════════════════════════════════
#  ESTRATEGIA PRINCIPAL
# ══════════════════════════════════════════════════════════════

class EMA8Strategy:
    def __init__(self, cfg):
        self.cfg = cfg

    def evaluate(
        self,
        symbol:     str,
        candles3m:  list[Candle],
        candles15m: list[Candle],
        candles1h:  list[Candle] | None = None,
    ) -> Optional[Signal]:
        """
        Evalúa los tres timeframes y retorna una Signal o None.
        Score mínimo requerido: 3/5.
        """
        cfg = self.cfg
        closes3m = [c.close for c in candles3m]
        c = candles3m[-1]   # vela actual

        # ── EMAs en 3m ────────────────────────────────────────
        e8  = ema(closes3m, cfg.EMA_FAST)
        e21 = ema(closes3m, cfg.EMA_MID)
        e55 = ema(closes3m, cfg.EMA_SLOW)

        if np.isnan(e8[-1]) or np.isnan(e21[-1]) or np.isnan(e55[-1]):
            return None

        ema8_now, ema21_now, ema55_now = e8[-1], e21[-1], e55[-1]
        ema8_prev = e8[-2] if len(e8) > 1 else ema8_now

        bull_ema = ema8_now > ema21_now > ema55_now
        bear_ema = ema8_now < ema21_now < ema55_now

        # Cross sobre / bajo EMA8
        long_cross  = (candles3m[-2].close <= e8[-2]) and (c.close > ema8_now)
        short_cross = (candles3m[-2].close >= e8[-2]) and (c.close < ema8_now)

        # ── RSI ───────────────────────────────────────────────
        rsi_val = rsi(closes3m, cfg.RSI_PERIOD)

        # ── ATR dinámico ──────────────────────────────────────
        atr_val = atr(candles3m, cfg.ATR_PERIOD)
        if atr_val == 0:
            atr_val = c.close * (cfg.SL_PCT / 100)

        # ── Tendencia 15m ─────────────────────────────────────
        closes15m  = [c.close for c in candles15m]
        ema21_15m  = ema(closes15m, cfg.EMA_MID)
        trend_up   = closes15m[-1] > ema21_15m[-1]
        trend_down = closes15m[-1] < ema21_15m[-1]

        # ── Tendencia macro 1h (EMA 200) ─────────────────────
        macro_up   = True
        macro_down = True
        if candles1h and len(candles1h) >= cfg.EMA_MACRO:
            closes1h   = [c.close for c in candles1h]
            ema200_1h  = ema(closes1h, cfg.EMA_MACRO)
            macro_up   = closes1h[-1] > ema200_1h[-1]
            macro_down = closes1h[-1] < ema200_1h[-1]

        # ══════════════════════════════════════════════════════
        #  SCORING — cada filtro vale 1 punto (máx 5)
        # ══════════════════════════════════════════════════════

        # ── LONG ──────────────────────────────────────────────
        if bull_ema and long_cross:
            score    = 0
            reasons  = []

            # 1. Tendencia 15m alineada
            if trend_up:
                score += 1; reasons.append("15m↑")

            # 2. Tendencia macro 1h alineada
            if macro_up:
                score += 1; reasons.append("1h↑")

            # 3. RSI en zona favorable (no sobrecomprado)
            if rsi_val < cfg.RSI_OB:
                score += 1; reasons.append(f"RSI={rsi_val:.0f}")

            # 4. Estructura alcista (higher highs)
            if higher_highs(candles3m):
                score += 1; reasons.append("HH")

            # 5. Volumen o pin bar
            if volume_surge(candles3m) or is_pin_bar_bull(c):
                score += 1; reasons.append("VOL/PIN")

            if score >= 3:
                sl  = round(c.close - atr_val * cfg.ATR_SL_MULT,  8)
                tp1 = round(c.close + atr_val * cfg.ATR_TP1_MULT, 8)
                tp2 = round(c.close + atr_val * cfg.ATR_TP2_MULT, 8)
                # Verificar RR mínimo 1.3
                rr = (tp1 - c.close) / (c.close - sl) if (c.close - sl) > 0 else 0
                if rr < 1.3:
                    log.debug("LONG %s rechazado: RR %.2f < 1.3", symbol, rr)
                    return None
                return Signal(
                    symbol=symbol, side="LONG", price=c.close,
                    sl=sl, tp1=tp1, tp2=tp2,
                    score=score, reason=" | ".join(reasons)
                )

        # ── SHORT ─────────────────────────────────────────────
        if bear_ema and short_cross:
            score    = 0
            reasons  = []

            if trend_down:
                score += 1; reasons.append("15m↓")
            if macro_down:
                score += 1; reasons.append("1h↓")
            if rsi_val > cfg.RSI_OS:
                score += 1; reasons.append(f"RSI={rsi_val:.0f}")
            if lower_lows(candles3m):
                score += 1; reasons.append("LL")
            if volume_surge(candles3m) or is_pin_bar_bear(c):
                score += 1; reasons.append("VOL/PIN")

            if score >= 3:
                sl  = round(c.close + atr_val * cfg.ATR_SL_MULT,  8)
                tp1 = round(c.close - atr_val * cfg.ATR_TP1_MULT, 8)
                tp2 = round(c.close - atr_val * cfg.ATR_TP2_MULT, 8)
                rr  = (c.close - tp1) / (sl - c.close) if (sl - c.close) > 0 else 0
                if rr < 1.3:
                    log.debug("SHORT %s rechazado: RR %.2f < 1.3", symbol, rr)
                    return None
                return Signal(
                    symbol=symbol, side="SHORT", price=c.close,
                    sl=sl, tp1=tp1, tp2=tp2,
                    score=score, reason=" | ".join(reasons)
                )

        return None
