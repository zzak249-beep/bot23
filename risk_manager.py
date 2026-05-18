"""
risk_manager.py — Gestión de riesgo profesional.

Mejoras vs versión original:
  • Tamaño de posición basado en % de riesgo real (Kelly parcial)
  • Límite de drawdown diario (para el bot)
  • Verificación de margen disponible antes de operar
  • Reducción automática de tamaño cuando hay drawdown
"""
from __future__ import annotations
import logging
from datetime import datetime, timezone
from typing import Tuple

from strategy import Signal

log = logging.getLogger("risk")


class RiskManager:
    def __init__(self, cfg):
        self.cfg = cfg
        self._daily_pnl: dict[str, float] = {}   # date → pnl acumulado
        self._daily_trades: dict[str, int] = {}   # date → número de trades

    def _today(self) -> str:
        return datetime.now(timezone.utc).date().isoformat()

    # ── Registrar resultado de trade ──────────────────────────
    def record_trade_result(self, pnl_usdt: float):
        key = self._today()
        self._daily_pnl[key]    = self._daily_pnl.get(key, 0.0) + pnl_usdt
        self._daily_trades[key] = self._daily_trades.get(key, 0) + 1

    def daily_pnl(self) -> float:
        return self._daily_pnl.get(self._today(), 0.0)

    # ── Validar si se puede abrir el trade ───────────────────
    def validate(
        self,
        balance:    float,
        sig:        Signal,
        open_count: int,
    ) -> Tuple[bool, str]:
        cfg = self.cfg

        # 1. Balance mínimo
        if balance < 20:
            return False, f"Balance insuficiente: {balance:.2f} USDT"

        # 2. Máx posiciones abiertas
        if open_count >= cfg.MAX_OPEN_TRADES:
            return False, f"Máx posiciones abiertas ({cfg.MAX_OPEN_TRADES})"

        # 3. Drawdown diario (máx 5% del balance)
        max_daily_loss = balance * 0.05
        if self.daily_pnl() < -max_daily_loss:
            return False, f"Drawdown diario alcanzado ({self.daily_pnl():.2f} USDT)"

        # 4. Número de trades diarios
        trades_today = self._daily_trades.get(self._today(), 0)
        if trades_today >= cfg.MAX_SIGNALS_DAY:
            return False, f"Límite diario de trades ({cfg.MAX_SIGNALS_DAY})"

        # 5. SL coherente (no mayor al 5% del precio)
        sl_pct = abs(sig.price - sig.sl) / sig.price * 100
        if sl_pct > 5.0:
            return False, f"SL demasiado amplio: {sl_pct:.1f}%"

        # 6. Score mínimo
        if sig.score < 3:
            return False, f"Score insuficiente: {sig.score}/5"

        return True, "ok"

    # ── Tamaño de posición ────────────────────────────────────
    def position_size(self, balance: float, sig: Signal) -> float:
        """
        Calcula la cantidad a operar.
        Fórmula: qty = (balance × riesgo%) / (precio - SL) × precio
        Ajuste por drawdown: si hoy perdemos >2%, reducimos tamaño 50%.
        """
        cfg    = self.cfg
        riesgo = cfg.RISK_PER_TRADE / 100  # e.g. 0.01

        # Reducción por drawdown parcial
        loss_pct = abs(min(self.daily_pnl(), 0)) / max(balance, 1) * 100
        if loss_pct > 2.0:
            riesgo *= 0.5  # half size tras 2% drawdown diario
            log.info("Tamaño reducido 50%% por drawdown diario: %.2f%%", loss_pct)

        risk_usdt   = balance * riesgo
        sl_distance = abs(sig.price - sig.sl)

        if sl_distance == 0:
            return 0.0

        # Contratos = riesgo en USDT / (distancia SL × precio)
        qty = (risk_usdt * cfg.LEVERAGE) / sig.price
        # Limitar a lo que cubre el SL
        max_qty_by_sl = risk_usdt / sl_distance
        qty = min(qty, max_qty_by_sl)

        return round(qty, 4)

    # ── Resumen diario para Telegram ─────────────────────────
    def daily_summary(self, balance: float) -> str:
        pnl    = self.daily_pnl()
        trades = self._daily_trades.get(self._today(), 0)
        pct    = pnl / balance * 100 if balance else 0
        emoji  = "🟢" if pnl >= 0 else "🔴"
        return (
            f"{emoji} *Resumen del día*\n"
            f"PnL: `{pnl:+.2f} USDT` ({pct:+.2f}%)\n"
            f"Trades: `{trades}`\n"
            f"Balance: `{balance:.2f} USDT`"
        )
