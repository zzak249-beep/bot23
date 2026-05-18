"""
main.py — Punto de entrada del bot EMA8 Scalper.

Ejecutar:
  python main.py

Railway ejecuta este archivo automáticamente.
"""
import asyncio
import logging
import signal
import sys

from config import cfg
from scanner import Scanner
from telegram_notifier import TelegramNotifier

# ── Logging ───────────────────────────────────────────────────
logging.basicConfig(
    level   = logging.INFO,
    format  = "%(asctime)s | %(levelname)-8s | %(name)s | %(message)s",
    datefmt = "%Y-%m-%d %H:%M:%S",
    handlers=[
        logging.StreamHandler(sys.stdout),
    ],
)
log = logging.getLogger("main")


async def main():
    # ── Validar configuración ──────────────────────────────────
    try:
        cfg.validate()
    except EnvironmentError as e:
        log.critical("Configuración inválida:\n%s", e)
        sys.exit(1)

    log.info("═══════════════════════════════════════")
    log.info("  EMA8 Scalper Bot — BingX Futures")
    log.info("  Modo: %s", "DRY RUN ⚠️" if cfg.DRY_RUN else "REAL 🟢")
    log.info("  Intervalo: %ds | Leverage: %dx", cfg.SCAN_INTERVAL_SECONDS, cfg.LEVERAGE)
    log.info("  Riesgo/trade: %.1f%% | Max trades: %d", cfg.RISK_PER_TRADE, cfg.MAX_OPEN_TRADES)
    log.info("═══════════════════════════════════════")

    notifier = TelegramNotifier(cfg.TELEGRAM_TOKEN, cfg.TELEGRAM_CHAT_ID)
    scanner  = Scanner(notifier)

    # Notificar arranque
    symbols_preview = await scanner._get_symbols()
    await notifier.startup(cfg.DRY_RUN, len(symbols_preview))

    # ── Graceful shutdown ──────────────────────────────────────
    stop_event = asyncio.Event()

    def _handle_signal(*_):
        log.info("Señal de parada recibida — cerrando...")
        stop_event.set()

    loop = asyncio.get_event_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, _handle_signal)

    # ── Loop principal ─────────────────────────────────────────
    log.info("Bot iniciado. Ctrl+C para detener.")
    while not stop_event.is_set():
        try:
            await scanner.run()
        except Exception as e:
            log.exception("Error inesperado en scan: %s", e)
            await notifier.error(f"Error crítico: {e}")

        # Esperar hasta el próximo ciclo (o parada)
        try:
            await asyncio.wait_for(
                stop_event.wait(),
                timeout=cfg.SCAN_INTERVAL_SECONDS,
            )
        except asyncio.TimeoutError:
            pass  # timeout normal — continuar

    await scanner.shutdown()
    log.info("Bot detenido correctamente.")


if __name__ == "__main__":
    asyncio.run(main())
