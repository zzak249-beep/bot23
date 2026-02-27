#!/usr/bin/env python3
"""
BingX Trading Bot - Estructura plana (todos los archivos en raíz)
"""

import asyncio
import logging
from logger import setup_logger
from telegram_bot import TelegramSignalBot
from trader import Trader

logger = setup_logger(__name__)


async def main():
    logger.info("🚀 Iniciando BingX Trading Bot...")
    trader = Trader()
    telegram_bot = TelegramSignalBot(trader)
    await asyncio.gather(
        trader.run_loop(),
        telegram_bot.run()
    )


if __name__ == "__main__":
    asyncio.run(main())
