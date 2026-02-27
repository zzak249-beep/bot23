"""
Gestión de riesgo:
- Máximo de posiciones abiertas simultáneas
- Límite de pérdida diaria
- Anti-martingala: no doblar posiciones en pérdida
"""

import logging
from datetime import datetime, date

logger = logging.getLogger(__name__)


class RiskManager:
    def __init__(self, max_risk_pct: float, max_open_positions: int, max_daily_loss_pct: float):
        self.max_risk_pct = max_risk_pct
        self.max_open_positions = max_open_positions
        self.max_daily_loss_pct = max_daily_loss_pct

        self._open_symbols = set()
        self._today = date.today()
        self._daily_loss = 0.0

    def _reset_daily_if_needed(self):
        today = date.today()
        if today != self._today:
            self._today = today
            self._daily_loss = 0.0
            logger.info("📅 Nuevo día: P&L diario reseteado.")

    def can_trade(self, symbol: str, active_trades: dict, daily_pnl: float) -> bool:
        self._reset_daily_if_needed()

        # No operar el mismo par dos veces
        if symbol in active_trades:
            logger.warning(f"⛔ {symbol} ya tiene posición abierta.")
            return False

        # Límite de posiciones simultáneas
        if len(active_trades) >= self.max_open_positions:
            logger.warning(f"⛔ Máximo de posiciones alcanzado ({self.max_open_positions}).")
            return False

        # Límite de pérdida diaria (diario_pnl es negativo si hay pérdidas)
        if daily_pnl < 0 and abs(daily_pnl) >= self.max_daily_loss_pct:
            logger.warning(f"⛔ Límite de pérdida diaria alcanzado: {daily_pnl:.2f}%")
            return False

        return True

    def register_trade(self, symbol: str):
        self._open_symbols.add(symbol)

    def close_trade(self, symbol: str, pnl_pct: float = 0.0):
        self._open_symbols.discard(symbol)
        self._daily_loss += min(pnl_pct, 0)  # Solo sumar pérdidas
