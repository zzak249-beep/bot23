"""
main.py — Punto de entrada. Verifica firma API antes de arrancar.
"""
import asyncio
import logging
import signal
import sys

from config import cfg
from bingx_api import BingXClient
from scanner import Scanner
from telegram_notifier import TelegramNotifier

logging.basicConfig(
    level   = logging.INFO,
    format  = "%(asctime)s | %(levelname)-8s | %(name)s | %(message)s",
    datefmt = "%Y-%m-%d %H:%M:%S",
    handlers=[logging.StreamHandler(sys.stdout)],
)
log = logging.getLogger("main")


async def main():
    try:
        cfg.validate()
    except EnvironmentError as e:
        log.critical("Config inválida: %s", e)
        sys.exit(1)

    log.info("═══════════════════════════════════════════")
    log.info("  EMA8 Scalper Bot v4 — BingX Futures")
    log.info("  Modo: %s", "DRY RUN ⚠️" if cfg.DRY_RUN else "REAL 🟢")
    log.info("  Intervalo: %ds | Leverage: %dx", cfg.SCAN_INTERVAL_SECONDS, cfg.LEVERAGE)
    log.info("  Riesgo/trade: %.1f%% | Max trades: %d",
             cfg.RISK_PER_TRADE, cfg.MAX_OPEN_TRADES)
    log.info("═══════════════════════════════════════════")

    notifier = TelegramNotifier(cfg.TELEGRAM_TOKEN, cfg.TELEGRAM_CHAT_ID)

    # ── Verificar firma API antes de arrancar ─────────────────
    client = BingXClient(cfg.BINGX_API_KEY, cfg.BINGX_API_SECRET, cfg.BINGX_BASE_URL)
    ok, msg = await client.test_auth()
    if ok:
        log.info("✅ API BingX OK: %s", msg)
        await notifier.auth_ok(float(msg.split(":")[1].split()[0]))
    else:
        log.error("❌ API BingX FALLÓ: %s", msg)
        await notifier.error(f"API BingX falló al arrancar:\n{msg}\n\nVerifica BINGX_API_KEY y BINGX_API_SECRET en Railway.")
        # No salimos — seguimos en modo señales Telegram (DRY_RUN forzado)
        log.warning("Continuando en modo señales Telegram únicamente.")
    await client.close()

    scanner = Scanner(notifier)
    symbols = await scanner._get_symbols()
    await notifier.startup(cfg.DRY_RUN, len(symbols))

    stop_event = asyncio.Event()
    loop = asyncio.get_event_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, stop_event.set)

    log.info("Bot en marcha. Señales llegarán a Telegram cada 3 minutos.")

    while not stop_event.is_set():
        try:
            await scanner.run()
        except Exception as e:
            log.exception("Error en scan: %s", e)
            await notifier.error(f"Error inesperado: {str(e)[:200]}")

        try:
            await asyncio.wait_for(stop_event.wait(),
                                   timeout=cfg.SCAN_INTERVAL_SECONDS)
        except asyncio.TimeoutError:
            pass

    await scanner.shutdown()
    log.info("Bot detenido.")


if __name__ == "__main__":
    asyncio.run(main())
