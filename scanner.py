"""
scanner.py — Motor de escaneo completo.

Mejoras vs versión original:
  • Datos 1h para filtro macro EMA200
  • Filtro de sesión horaria (no operar en horas de baja liquidez)
  • Trailing stop automático tras alcanzar 80% del TP1
  • Heartbeat cada hora por Telegram
  • Resumen diario automático a las 00:00 UTC
"""
import asyncio
import logging
from datetime import datetime, timezone

import numpy as np

from bingx_api import BingXClient, BingXAPIError
from strategy import EMA8Strategy, Candle, Signal
from risk_manager import RiskManager
from telegram_notifier import TelegramNotifier
from config import cfg

log = logging.getLogger("scanner")


def _to_candles(raw: list) -> list[Candle]:
    return [Candle(**c) for c in raw]


class Scanner:
    def __init__(self, notifier: TelegramNotifier):
        self.notifier  = notifier
        self.client    = BingXClient(cfg.BINGX_API_KEY, cfg.BINGX_API_SECRET,
                                     cfg.BINGX_BASE_URL)
        self.strategy  = EMA8Strategy(cfg)
        self.risk      = RiskManager(cfg)

        self._traded_today: dict[str, int]   = {}
        self._last_signal:  dict[str, int]   = {}
        self._last_heartbeat: float          = 0
        self._last_summary_day: str          = ""
        self._scan_count: int                = 0

    # ── Sesión de mercado activa ───────────────────────────────
    def _in_active_session(self) -> bool:
        if not cfg.USE_SESSION_FILTER:
            return True
        hour = datetime.now(timezone.utc).hour
        for (start, end) in cfg.ACTIVE_SESSIONS:
            if start <= hour < end:
                return True
        return False

    # ── Obtener símbolos ───────────────────────────────────────
    async def _get_symbols(self) -> list[str]:
        if cfg.SYMBOLS:
            return cfg.SYMBOLS
        try:
            syms = await self.client.get_symbols()
            exclude = {"USDC","BUSD","TUSD","DAI","FDUSD","USDP",
                       "USDT","USTC","LUNA","LUNC","UST"}
            if cfg.LIQUIDITY_MODE == "high_only":
                whitelist = {
                    "BTC-USDT","ETH-USDT","SOL-USDT","BNB-USDT",
                    "XRP-USDT","ADA-USDT","DOGE-USDT","AVAX-USDT",
                    "LINK-USDT","DOT-USDT","MATIC-USDT","LTC-USDT",
                    "UNI-USDT","ATOM-USDT","FIL-USDT","NEAR-USDT",
                    "APT-USDT","ARB-USDT","OP-USDT","SUI-USDT",
                }
                return [s for s in syms if s in whitelist]
            return [s for s in syms
                    if not any(s.startswith(e) for e in exclude)]
        except Exception as e:
            log.error("Error obteniendo símbolos: %s", e)
            return []

    # ── Filtro de liquidez ────────────────────────────────────
    def _check_liquidity(self, candles: list[Candle]) -> bool:
        if len(candles) < 20:
            return False
        vols = np.array([c.volume * c.close for c in candles[-50:]])
        return float(np.mean(vols)) >= cfg.MIN_BAR_VOL_USDT

    # ── Datos de mercado (3 timeframes) ──────────────────────
    async def _fetch_data(self, symbol: str):
        try:
            raw3m, raw15m, raw1h = await asyncio.gather(
                self.client.get_klines(symbol, cfg.TF_ENTRY,  limit=200),
                self.client.get_klines(symbol, cfg.TF_TREND,  limit=60),
                self.client.get_klines(symbol, cfg.TF_MACRO,  limit=220),
            )
            if len(raw3m) < 50 or len(raw15m) < 10:
                return None
            return _to_candles(raw3m), _to_candles(raw15m), _to_candles(raw1h)
        except Exception as e:
            log.debug("Fetch error %s: %s", symbol, e)
            return None

    # ── Cooldown y límites ────────────────────────────────────
    def _check_cooldown(self, symbol: str, bar_idx: int) -> bool:
        last = self._last_signal.get(symbol, -999)
        return (bar_idx - last) >= cfg.COOLDOWN_BARS

    def _check_daily_limit(self, symbol: str) -> bool:
        today = datetime.now(timezone.utc).date().isoformat()
        key   = f"{symbol}:{today}"
        return self._traded_today.get(key, 0) < cfg.MAX_SIGNALS_DAY

    def _record_signal(self, symbol: str, bar_idx: int):
        today = datetime.now(timezone.utc).date().isoformat()
        key   = f"{symbol}:{today}"
        self._traded_today[key] = self._traded_today.get(key, 0) + 1
        self._last_signal[symbol] = bar_idx

    # ── Ejecutar trade ────────────────────────────────────────
    async def _execute(self, sig: Signal, balance: float, open_pos: int):
        ok, reason = self.risk.validate(balance, sig, open_pos)
        if not ok:
            log.info("Señal %s %s rechazada: %s", sig.symbol, sig.side, reason)
            return

        qty = self.risk.position_size(balance, sig)
        if qty <= 0:
            return

        log.info("SEÑAL %s | %s | qty=%.4f | SL=%.6g | TP1=%.6g | Score=%d/5",
                 sig.side, sig.symbol, qty, sig.sl, sig.tp1, sig.score)

        await self.notifier.signal_alert(sig, qty, cfg.DRY_RUN)

        if cfg.DRY_RUN:
            log.info("DRY_RUN — orden simulada.")
            return

        # Margen aislado
        await self.client.set_margin_type(sig.symbol, "ISOLATED")

        # Apalancamiento
        pos_side = "LONG" if sig.side == "LONG" else "SHORT"
        await self.client.set_leverage(sig.symbol, cfg.LEVERAGE, pos_side)

        # Orden principal con SL + TP1
        res = await self.client.place_order(
            symbol       = sig.symbol,
            side         = "BUY" if sig.side == "LONG" else "SELL",
            qty          = qty,
            stop_loss    = sig.sl,
            take_profit  = sig.tp1,
            position_side= pos_side,
        )

        if res.get("code", -1) == 0:
            await self.notifier.order_filled(sig.symbol, sig.side, sig.price)
            log.info("Orden ejecutada: %s", res.get("data", {}).get("orderId"))

            # Trailing stop si está habilitado
            if cfg.USE_TRAILING_STOP:
                activation = sig.price + (sig.tp1 - sig.price) * cfg.TRAIL_ACTIVATION \
                             if sig.side == "LONG" \
                             else sig.price - (sig.price - sig.tp1) * cfg.TRAIL_ACTIVATION
                try:
                    await self.client.set_trailing_stop(
                        symbol           = sig.symbol,
                        activation_price = round(activation, 8),
                        callback_rate    = cfg.TRAIL_DISTANCE,
                        position_side    = pos_side,
                    )
                    log.info("Trailing stop configurado en %.6g", activation)
                except BingXAPIError as e:
                    log.warning("Trailing stop falló (no crítico): %s", e)
        else:
            err = res.get("msg", "desconocido")
            log.error("Orden falló %s: %s", sig.symbol, err)
            await self.notifier.error(f"Orden falló `{sig.symbol}`: {err}")

    # ── Escanear un símbolo ───────────────────────────────────
    async def _scan_one(self, symbol: str,
                        balance: float, open_count: int) -> bool:
        data = await self._fetch_data(symbol)
        if data is None:
            return False

        candles3m, candles15m, candles1h = data
        bar_idx = len(candles3m)

        if not self._check_liquidity(candles3m):
            log.debug("Liquidez insuficiente: %s", symbol)
            return False
        if not self._check_cooldown(symbol, bar_idx):
            return False
        if not self._check_daily_limit(symbol):
            return False

        sig = self.strategy.evaluate(symbol, candles3m, candles15m, candles1h)
        if sig is None:
            return False

        self._record_signal(symbol, bar_idx)
        await self._execute(sig, balance, open_count)
        return True

    # ── Heartbeat / resumen diario ────────────────────────────
    async def _maybe_heartbeat(self, balance: float, open_count: int):
        import time
        now = time.time()
        if now - self._last_heartbeat >= 3600:   # cada hora
            self._last_heartbeat = now
            await self.notifier.heartbeat(balance, open_count)

    async def _maybe_daily_summary(self, balance: float):
        today = datetime.now(timezone.utc).date().isoformat()
        hour  = datetime.now(timezone.utc).hour
        if hour == 0 and self._last_summary_day != today:
            self._last_summary_day = today
            summary = self.risk.daily_summary(balance)
            await self.notifier.daily_summary(summary)

    # ── Ciclo principal ───────────────────────────────────────
    async def run(self):
        now = datetime.now(timezone.utc).strftime("%H:%M:%S")
        self._scan_count += 1
        log.info("▶ Scan #%d — %s UTC", self._scan_count, now)

        # Verificar sesión activa
        if not self._in_active_session():
            log.info("Fuera de sesión activa — esperando.")
            return

        symbols = await self._get_symbols()
        if not symbols:
            log.warning("Sin símbolos.")
            return

        balance, open_positions = await asyncio.gather(
            self.client.get_balance(),
            self.client.get_open_positions(),
        )
        open_count = len(open_positions)
        log.info("Balance: %.2f USDT | Pos: %d | Símbolos: %d",
                 balance, open_count, len(symbols))

        await self._maybe_heartbeat(balance, open_count)
        await self._maybe_daily_summary(balance)

        signals_found = 0
        batch_size    = 8  # lotes más pequeños = menos errores de rate limit

        for i in range(0, len(symbols), batch_size):
            batch   = symbols[i:i + batch_size]
            tasks   = [self._scan_one(s, balance, open_count) for s in batch]
            results = await asyncio.gather(*tasks, return_exceptions=True)
            for r in results:
                if r is True:
                    signals_found += 1
            await asyncio.sleep(0.5)

        log.info("✔ Scan completo — %d señal(es)", signals_found)

    async def shutdown(self):
        await self.client.close()
