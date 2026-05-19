"""
scanner.py — Motor de escaneo.

FIX 1: Balance 0.00 → el scanner ahora loguea el motivo exacto del rechazo
        en cada símbolo para diagnóstico.
FIX 2: USE_SESSION_FILTER=false por defecto en el scanner para no bloquear.
FIX 3: Log detallado de cada filtro que falla (liquidez, cooldown, estrategia).
FIX 4: Si balance = 0, avisa por Telegram y sigue enviando señales igualmente.
"""
import asyncio
import logging
import time
from datetime import datetime, timezone

import numpy as np

from bingx_api import BingXClient, BingXAPIError
from strategy import EMA8Strategy, Candle, Signal
from risk_manager import RiskManager
from signal_tracker import SignalTracker
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
        self.tracker   = SignalTracker()

        self._traded_today:     dict[str, int] = {}
        self._last_signal:      dict[str, int] = {}
        self._last_heartbeat:   float = 0
        self._last_accuracy:    float = 0
        self._last_summary_day: str   = ""
        self._scan_count:       int   = 0
        self._balance_warned:   bool  = False

        # Contadores de diagnóstico
        self._diag: dict[str, int] = {
            "fetch_error": 0, "liquidity": 0, "cooldown": 0,
            "daily_limit": 0, "no_signal": 0, "score_low": 0,
            "rr_low": 0, "balance_zero": 0, "signals": 0,
        }

    def _in_active_session(self) -> bool:
        if not cfg.USE_SESSION_FILTER:
            return True
        hour = datetime.now(timezone.utc).hour
        for (start, end) in cfg.ACTIVE_SESSIONS:
            if start <= hour < end:
                return True
        return False

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
                    "UNI-USDT","ATOM-USDT","NEAR-USDT","APT-USDT",
                    "ARB-USDT","OP-USDT","SUI-USDT","INJ-USDT",
                }
                return [s for s in syms if s in whitelist]
            return [s for s in syms
                    if not any(s.startswith(e) for e in exclude)]
        except Exception as e:
            log.error("Error obteniendo símbolos: %s", e)
            return []

    def _check_liquidity(self, candles: list[Candle]) -> bool:
        if len(candles) < 20:
            return False
        vols = np.array([c.volume * c.close for c in candles[-50:]])
        avg  = float(np.mean(vols))
        ok   = avg >= cfg.MIN_BAR_VOL_USDT
        if not ok:
            log.debug("Liquidez %.0f < %.0f", avg, cfg.MIN_BAR_VOL_USDT)
        return ok

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
            log.debug("Fetch %s: %s", symbol, e)
            return None

    def _check_cooldown(self, symbol: str, bar_idx: int) -> bool:
        last = self._last_signal.get(symbol, -999)
        return (bar_idx - last) >= cfg.COOLDOWN_BARS

    def _check_daily_limit(self, symbol: str) -> bool:
        today = datetime.now(timezone.utc).date().isoformat()
        return self._traded_today.get(f"{symbol}:{today}", 0) < cfg.MAX_SIGNALS_DAY

    def _record_signal(self, symbol: str, bar_idx: int):
        today = datetime.now(timezone.utc).date().isoformat()
        key   = f"{symbol}:{today}"
        self._traded_today[key] = self._traded_today.get(key, 0) + 1
        self._last_signal[symbol] = bar_idx

    async def _execute(self, sig: Signal, balance: float, open_pos: int):
        # Registrar en tracker SIEMPRE (para test de aciertos)
        self.tracker.add(sig.symbol, sig.side, sig.price,
                         sig.sl, sig.tp1, sig.tp2, sig.score)
        self._diag["signals"] += 1

        # Calcular qty aunque balance sea 0 (para mostrar en Telegram)
        qty = self.risk.position_size(balance, sig) if balance >= 20 else 0

        # Enviar señal a Telegram SIEMPRE (para trade manual)
        await self.notifier.signal_alert(sig, qty, cfg.DRY_RUN)

        # Validar antes de ejecutar real
        ok, reason = self.risk.validate(balance, sig, open_pos)
        if not ok:
            log.info("No ejecutar (%s) — señal enviada a Telegram igualmente", reason)
            return
        if qty <= 0 or cfg.DRY_RUN:
            if cfg.DRY_RUN:
                log.info("DRY_RUN — señal simulada.")
            return

        # ── Ejecutar orden real ────────────────────────────────
        await self.client.set_margin_type(sig.symbol, "ISOLATED")
        pos_side = "LONG" if sig.side == "LONG" else "SHORT"
        await self.client.set_leverage(sig.symbol, cfg.LEVERAGE, pos_side)

        res = await self.client.place_order(
            symbol        = sig.symbol,
            side          = "BUY" if sig.side == "LONG" else "SELL",
            qty           = qty,
            stop_loss     = sig.sl,
            take_profit   = sig.tp1,
            position_side = pos_side,
        )

        if res.get("code", -1) == 0:
            await self.notifier.order_filled(sig.symbol, sig.side, sig.price)
            log.info("✅ Orden ejecutada: %s", res.get("data", {}).get("orderId",""))
            if cfg.USE_TRAILING_STOP:
                activation = (
                    sig.price + (sig.tp1 - sig.price) * cfg.TRAIL_ACTIVATION
                    if sig.side == "LONG"
                    else sig.price - (sig.price - sig.tp1) * cfg.TRAIL_ACTIVATION
                )
                try:
                    await self.client.set_trailing_stop(
                        sig.symbol, round(activation, 8),
                        cfg.TRAIL_DISTANCE, pos_side)
                except BingXAPIError as e:
                    log.warning("Trailing stop: %s", e)
        else:
            err = res.get("msg", "desconocido")
            log.error("❌ Orden falló %s: %s", sig.symbol, err)
            await self.notifier.error(f"Orden falló `{sig.symbol}`: {err}")

    async def _scan_one(self, symbol: str, balance: float, open_count: int) -> bool:
        data = await self._fetch_data(symbol)
        if data is None:
            self._diag["fetch_error"] += 1
            return False

        candles3m, candles15m, candles1h = data
        bar_idx = len(candles3m)

        if not self._check_liquidity(candles3m):
            self._diag["liquidity"] += 1
            return False
        if not self._check_cooldown(symbol, bar_idx):
            self._diag["cooldown"] += 1
            return False
        if not self._check_daily_limit(symbol):
            self._diag["daily_limit"] += 1
            return False

        sig = self.strategy.evaluate(symbol, candles3m, candles15m, candles1h)
        if sig is None:
            self._diag["no_signal"] += 1
            return False

        self._record_signal(symbol, bar_idx)
        await self._execute(sig, balance, open_count)
        return True

    # ── Log de diagnóstico cada scan ─────────────────────────
    def _log_diag(self, total: int):
        d = self._diag
        log.info(
            "📊 Diagnóstico: total=%d | fetch_err=%d | liquidez=%d | "
            "cooldown=%d | daily=%d | sin_señal=%d | señales=%d",
            total, d["fetch_error"], d["liquidity"],
            d["cooldown"], d["daily_limit"], d["no_signal"], d["signals"]
        )
        # Reset contadores de diagnóstico
        for k in d:
            d[k] = 0

    # ── Tareas periódicas ─────────────────────────────────────

    async def _maybe_accuracy_report(self):
        now = time.time()
        if now - self._last_accuracy >= 3600:
            self._last_accuracy = now
            await self.tracker.update_all(self.client.get_ticker)
            report = self.tracker.hourly_report()
            await self.notifier.hourly_accuracy(report)

    async def _maybe_heartbeat(self, balance: float, open_trades: int):
        now = time.time()
        if now - self._last_heartbeat >= 3600:
            self._last_heartbeat = now
            stats = self.tracker.stats()
            await self.notifier.heartbeat(balance, open_trades, stats["wr"])

    async def _maybe_daily_summary(self, balance: float):
        today = datetime.now(timezone.utc).date().isoformat()
        if datetime.now(timezone.utc).hour == 0 and self._last_summary_day != today:
            self._last_summary_day = today
            await self.notifier.daily_summary(self.risk.daily_summary(balance))

    # ── Ciclo principal ───────────────────────────────────────

    async def run(self):
        now = datetime.now(timezone.utc).strftime("%H:%M:%S")
        self._scan_count += 1
        log.info("▶ Scan #%d — %s UTC", self._scan_count, now)

        if not self._in_active_session():
            log.info("Fuera de sesión activa.")
            await self._maybe_accuracy_report()
            return

        symbols = await self._get_symbols()
        if not symbols:
            log.warning("Sin símbolos para escanear.")
            return

        # Obtener balance con log del valor raw para diagnóstico
        balance_raw = await self.client.debug_balance_raw()
        balance     = await self.client.get_balance()
        log.info("Balance: %.2f USDT (raw: %s...)", balance, balance_raw[:80])

        open_positions = await self.client.get_open_positions()
        open_count     = len(open_positions)
        log.info("Posiciones abiertas: %d | Símbolos a escanear: %d",
                 open_count, len(symbols))

        # Advertencia de balance cero
        if balance == 0.0 and not self._balance_warned:
            self._balance_warned = True
            await self.notifier.error(
                "⚠️ Balance = 0.00 USDT\n\n"
                "Posibles causas:\n"
                "1️⃣ La API key no tiene permiso de futuros\n"
                "2️⃣ No has transferido fondos a la cuenta de futuros\n"
                "3️⃣ El endpoint de balance retorna un formato distinto\n\n"
                "El bot seguirá enviando SEÑALES por Telegram para trade manual.\n"
                "Revisa los logs de Railway para ver el balance RAW."
            )

        await self._maybe_heartbeat(balance, open_count)
        await self._maybe_daily_summary(balance)
        await self._maybe_accuracy_report()

        signals_found = 0
        for i in range(0, len(symbols), 8):
            batch   = symbols[i:i + 8]
            results = await asyncio.gather(
                *[self._scan_one(s, balance, open_count) for s in batch],
                return_exceptions=True,
            )
            signals_found += sum(1 for r in results if r is True)
            await asyncio.sleep(0.5)

        self._log_diag(len(symbols))
        log.info("✔ Scan #%d — %d señal(es) encontradas", self._scan_count, signals_found)

    async def shutdown(self):
        await self.client.close()
