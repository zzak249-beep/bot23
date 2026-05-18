"""
signal_tracker.py — Registra señales emitidas y evalúa aciertos cada hora.

Para cada señal guardamos: símbolo, lado, precio_entrada, SL, TP1, TP2, timestamp.
Cada hora el bot verifica el precio actual y determina si la señal fue:
  ✅ WIN  — precio tocó TP1
  ❌ LOSS — precio tocó SL
  ⏳ OPEN — aún dentro del rango
"""
from __future__ import annotations
import logging
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Literal

log = logging.getLogger("tracker")

Status = Literal["OPEN", "WIN", "LOSS", "EXPIRED"]


@dataclass
class TrackedSignal:
    symbol:    str
    side:      str        # LONG | SHORT
    price:     float
    sl:        float
    tp1:       float
    tp2:       float
    score:     int
    timestamp: datetime = field(default_factory=lambda: datetime.now(timezone.utc))
    status:    Status   = "OPEN"
    exit_price: float   = 0.0

    def check(self, current_high: float, current_low: float) -> Status:
        """Actualiza el estado según el precio actual."""
        if self.status != "OPEN":
            return self.status

        if self.side == "LONG":
            if current_low <= self.sl:
                self.status = "LOSS"
                self.exit_price = self.sl
            elif current_high >= self.tp1:
                self.status = "WIN"
                self.exit_price = self.tp1
        else:  # SHORT
            if current_high >= self.sl:
                self.status = "LOSS"
                self.exit_price = self.sl
            elif current_low <= self.tp1:
                self.status = "WIN"
                self.exit_price = self.tp1

        # Expirar señales > 4 horas sin resolver
        age_h = (datetime.now(timezone.utc) - self.timestamp).seconds / 3600
        if self.status == "OPEN" and age_h > 4:
            self.status = "EXPIRED"

        return self.status

    @property
    def rr(self) -> float:
        d = abs(self.price - self.sl)
        return abs(self.tp1 - self.price) / d if d else 0

    @property
    def age_min(self) -> int:
        return int((datetime.now(timezone.utc) - self.timestamp).seconds / 60)


class SignalTracker:
    def __init__(self):
        self._signals: list[TrackedSignal] = []

    def add(self, symbol: str, side: str, price: float,
            sl: float, tp1: float, tp2: float, score: int):
        sig = TrackedSignal(symbol=symbol, side=side, price=price,
                            sl=sl, tp1=tp1, tp2=tp2, score=score)
        self._signals.append(sig)
        log.info("📌 Señal registrada: %s %s @ %.6g", side, symbol, price)

    async def update_all(self, get_ticker_fn) -> list[TrackedSignal]:
        """
        Actualiza el estado de todas las señales abiertas.
        get_ticker_fn: función async que recibe symbol y retorna dict con 'high','low'
        """
        open_sigs = [s for s in self._signals if s.status == "OPEN"]
        for sig in open_sigs:
            try:
                ticker = await get_ticker_fn(sig.symbol)
                high = float(ticker.get("highPrice", ticker.get("high", sig.price)))
                low  = float(ticker.get("lowPrice",  ticker.get("low",  sig.price)))
                sig.check(high, low)
            except Exception as e:
                log.debug("Error actualizando %s: %s", sig.symbol, e)
        return self._signals

    def hourly_report(self) -> str:
        """Genera el reporte de aciertos para Telegram."""
        # Solo señales de las últimas 24 horas
        now   = datetime.now(timezone.utc)
        recent = [s for s in self._signals
                  if (now - s.timestamp).seconds < 86400]

        wins     = [s for s in recent if s.status == "WIN"]
        losses   = [s for s in recent if s.status == "LOSS"]
        opens    = [s for s in recent if s.status == "OPEN"]
        expired  = [s for s in recent if s.status == "EXPIRED"]

        total    = len(wins) + len(losses)
        wr       = (len(wins) / total * 100) if total > 0 else 0
        avg_score = sum(s.score for s in recent) / len(recent) if recent else 0

        lines = [
            "📊 *Test de Aciertos — Últimas 24h*\n",
            f"✅ Wins:    `{len(wins)}`",
            f"❌ Losses:  `{len(losses)}`",
            f"⏳ Abiertas: `{len(opens)}`",
            f"⌛ Expiradas: `{len(expired)}`",
            f"🎯 Win Rate: `{wr:.1f}%`",
            f"⭐ Score medio: `{avg_score:.1f}/5`\n",
        ]

        # Detalle de señales recientes (últimas 6)
        if recent:
            lines.append("*Últimas señales:*")
            for s in sorted(recent, key=lambda x: x.timestamp, reverse=True)[:6]:
                icon = {"WIN":"✅","LOSS":"❌","OPEN":"⏳","EXPIRED":"⌛"}[s.status]
                lines.append(
                    f"{icon} `{s.side} {s.symbol}` "
                    f"@ `{s.price:.5g}` | "
                    f"RR `1:{s.rr:.1f}` | "
                    f"Score `{s.score}/5` | "
                    f"`{s.age_min}min`"
                )

        return "\n".join(lines)

    def stats(self) -> dict:
        total  = len(self._signals)
        wins   = sum(1 for s in self._signals if s.status == "WIN")
        losses = sum(1 for s in self._signals if s.status == "LOSS")
        return {"total": total, "wins": wins, "losses": losses,
                "wr": wins / (wins + losses) * 100 if (wins + losses) else 0}
