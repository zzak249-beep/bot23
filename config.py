"""
config.py — Configuración central cargada desde variables de entorno.
Todas las claves sensibles van en Railway Environment Variables.
"""
import os
from dataclasses import dataclass, field
from typing import Optional


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
    # Vacío = escanear todo el mercado. Ej: "BTC-USDT,ETH-USDT"
    SYMBOLS: list = field(default_factory=lambda:
        [s.strip() for s in os.getenv("SYMBOLS", "").split(",") if s.strip()])

    LIQUIDITY_MODE: str = os.getenv("LIQUIDITY_MODE", "high_only")
    # high_only = solo top pares; all = todo el mercado

    # ── Timeframes ────────────────────────────────────────────
    TF_ENTRY:  str = "3m"    # velas de entrada
    TF_TREND:  str = "15m"   # confirmación de tendencia
    TF_MACRO:  str = "1h"    # tendencia macro (NUEVO)

    # ── EMAs ──────────────────────────────────────────────────
    EMA_FAST:  int = 8
    EMA_MID:   int = 21
    EMA_SLOW:  int = 55
    EMA_MACRO: int = 200     # EMA 200 en 1h (NUEVO — filtro tendencia)

    # ── RSI ───────────────────────────────────────────────────
    RSI_PERIOD:    int   = 14
    RSI_OB:        float = 68.0   # Sobrecompra (más estricto que 70)
    RSI_OS:        float = 32.0   # Sobreventa  (más estricto que 30)

    # ── ATR — Stop Loss dinámico (NUEVO) ─────────────────────
    ATR_PERIOD:    int   = 14
    ATR_SL_MULT:   float = 1.5    # SL = ATR × 1.5
    ATR_TP1_MULT:  float = 2.0    # TP1 = ATR × 2.0  (RR ≥ 1.3)
    ATR_TP2_MULT:  float = 3.5    # TP2 = ATR × 3.5

    # Fallback porcentual si ATR no disponible
    SL_PCT:        float = float(os.getenv("SL_PCT",  "0.8"))
    TP1_PCT:       float = float(os.getenv("TP1_PCT", "1.6"))
    TP2_PCT:       float = float(os.getenv("TP2_PCT", "3.2"))

    # ── Gestión de riesgo ─────────────────────────────────────
    RISK_PER_TRADE:    float = float(os.getenv("RISK_PER_TRADE", "1.0"))
    # % del balance arriesgado por operación (1% = conservador)
    MAX_OPEN_TRADES:   int   = int(os.getenv("MAX_OPEN_TRADES", "4"))
    LEVERAGE:          int   = int(os.getenv("LEVERAGE", "5"))
    MAX_SIGNALS_DAY:   int   = int(os.getenv("MAX_SIGNALS_DAY", "12"))
    COOLDOWN_BARS:     int   = int(os.getenv("COOLDOWN_BARS", "5"))

    # ── Liquidez mínima ───────────────────────────────────────
    MIN_BAR_VOL_USDT: float = float(os.getenv("MIN_BAR_VOL_USDT", "500000"))

    # ── Trailing Stop (NUEVO) ─────────────────────────────────
    USE_TRAILING_STOP: bool  = os.getenv("USE_TRAILING_STOP", "true").lower() == "true"
    TRAIL_ACTIVATION:  float = 0.8   # activa trail cuando ganancia >= 0.8 × TP1
    TRAIL_DISTANCE:    float = 0.4   # distancia del trailing en % del precio

    # ── Sesiones de mercado (NUEVO) ───────────────────────────
    # Operar solo en sesiones de alta liquidez (UTC)
    USE_SESSION_FILTER: bool = os.getenv("USE_SESSION_FILTER", "true").lower() == "true"
    # Sesión Asia: 00-08, Europa: 07-16, NY: 13-22
    ACTIVE_SESSIONS: list = field(default_factory=lambda: [
        (0, 8), (7, 16), (13, 22)
    ])

    # ── Intervalo del scanner ─────────────────────────────────
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
            raise EnvironmentError("Variables de entorno faltantes:\n" + "\n".join(errors))
        return True


cfg = Config()
