"""
config.py — Configuración central.
FIX: USE_SESSION_FILTER ahora es false por defecto para crypto 24/7.
"""
import os
from dataclasses import dataclass, field


@dataclass
class Config:
    # ── BingX API ──────────────────────────────────────────────
    BINGX_API_KEY:    str = os.getenv("BINGX_API_KEY", "")
    BINGX_API_SECRET: str = os.getenv("BINGX_API_SECRET", "")
    BINGX_BASE_URL:   str = "https://open-api.bingx.com"

    # ── Telegram ───────────────────────────────────────────────
    TELEGRAM_TOKEN:   str = os.getenv("TELEGRAM_TOKEN", "")
    TELEGRAM_CHAT_ID: str = os.getenv("TELEGRAM_CHAT_ID", "")

    # ── Modo operación ─────────────────────────────────────────
    DRY_RUN: bool = os.getenv("DRY_RUN", "true").lower() == "true"

    # ── Símbolos ───────────────────────────────────────────────
    SYMBOLS: list = field(default_factory=lambda:
        [s.strip() for s in os.getenv("SYMBOLS", "").split(",") if s.strip()])

    LIQUIDITY_MODE: str = os.getenv("LIQUIDITY_MODE", "high_only")

    # ── Timeframes ────────────────────────────────────────────
    TF_ENTRY: str = "3m"
    TF_TREND: str = "15m"
    TF_MACRO: str = "1h"

    # ── EMAs ──────────────────────────────────────────────────
    EMA_FAST:  int = 8
    EMA_MID:   int = 21
    EMA_SLOW:  int = 55
    EMA_MACRO: int = 200

    # ── RSI ───────────────────────────────────────────────────
    RSI_PERIOD: int   = 14
    RSI_OB:     float = 68.0
    RSI_OS:     float = 32.0

    # ── ATR ───────────────────────────────────────────────────
    ATR_PERIOD:   int   = 14
    ATR_SL_MULT:  float = 1.5
    ATR_TP1_MULT: float = 2.0
    ATR_TP2_MULT: float = 3.5

    # Fallback porcentual
    SL_PCT:  float = float(os.getenv("SL_PCT",  "0.8"))
    TP1_PCT: float = float(os.getenv("TP1_PCT", "1.6"))
    TP2_PCT: float = float(os.getenv("TP2_PCT", "3.2"))

    # ── Gestión de riesgo ─────────────────────────────────────
    RISK_PER_TRADE:  float = float(os.getenv("RISK_PER_TRADE", "1.0"))
    MAX_OPEN_TRADES: int   = int(os.getenv("MAX_OPEN_TRADES", "4"))
    LEVERAGE:        int   = int(os.getenv("LEVERAGE", "5"))
    MAX_SIGNALS_DAY: int   = int(os.getenv("MAX_SIGNALS_DAY", "20"))
    COOLDOWN_BARS:   int   = int(os.getenv("COOLDOWN_BARS", "3"))

    # ── Liquidez ──────────────────────────────────────────────
    MIN_BAR_VOL_USDT: float = float(os.getenv("MIN_BAR_VOL_USDT", "200000"))
    # Bajado de 500k a 200k para generar más señales inicialmente

    # ── Trailing Stop ─────────────────────────────────────────
    USE_TRAILING_STOP: bool  = os.getenv("USE_TRAILING_STOP", "true").lower() == "true"
    TRAIL_ACTIVATION:  float = 0.8
    TRAIL_DISTANCE:    float = 0.4

    # ── Sesiones ──────────────────────────────────────────────
    # FALSE por defecto: crypto opera 24/7, no tiene sentido filtrar sesiones
    USE_SESSION_FILTER: bool = os.getenv("USE_SESSION_FILTER", "false").lower() == "true"
    ACTIVE_SESSIONS: list = field(default_factory=lambda: [
        (0, 8), (7, 16), (13, 22)
    ])

    # ── Scanner ───────────────────────────────────────────────
    SCAN_INTERVAL_SECONDS: int = int(os.getenv("SCAN_INTERVAL_SECONDS", "180"))

    def validate(self):
        errors = []
        if not self.BINGX_API_KEY:
            errors.append("BINGX_API_KEY no configurada")
        if not self.BINGX_API_SECRET:
            errors.append("BINGX_API_SECRET no configurada")
        if not self.TELEGRAM_TOKEN:
            errors.append("TELEGRAM_TOKEN no configurada")
        if not self.TELEGRAM_CHAT_ID:
            errors.append("TELEGRAM_CHAT_ID no configurada")
        if errors:
            raise EnvironmentError("Variables faltantes:\n" + "\n".join(errors))
        return True


cfg = Config()
