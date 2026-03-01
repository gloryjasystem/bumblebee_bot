"""
bot.py — Точка входа Bumblebee Bot.
Запуск: python bot.py
"""
import asyncio
import logging
import os

from aiogram import Bot, Dispatcher
from aiogram.client.default import DefaultBotProperties
from aiogram.enums import ParseMode
from aiogram.fsm.storage.memory import MemoryStorage

from config import settings
from db.pool import create_pool, close_pool

# ── Хендлеры ─────────────────────────────────────────────────
from handlers.start import router as start_router
from handlers.channels import router as channels_router
from handlers.blacklist import router as blacklist_router
from handlers.join_requests import router as join_requests_router
from handlers.captcha import router as captcha_router
from handlers.mailing import router as mailing_router
from handlers.links import router as links_router
from handlers.payment_handler import router as payment_router
from handlers.channel_settings import router as channel_settings_router
from handlers.feedback import router as feedback_router

# ── Мидлвары ─────────────────────────────────────────────────
from middlewares.owner_check import OwnerMiddleware

# ── Scheduler ────────────────────────────────────────────────
from scheduler.jobs import setup_scheduler

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
)
logger = logging.getLogger(__name__)


async def on_startup(bot: Bot):
    logger.info("Starting Bumblebee Bot...")
    await create_pool()
    logger.info("DB pool created")

    # Применяем схему БД (CREATE TABLE IF NOT EXISTS — безопасно при каждом запуске)
    from db.pool import get_pool
    with open("db/init.sql", "r", encoding="utf-8") as f:
        sql = f.read()
    async with get_pool().acquire() as conn:
        await conn.execute(sql)
    logger.info("DB schema applied")

    # Запуск планировщика
    scheduler = setup_scheduler(bot)
    scheduler.start()
    logger.info("Scheduler started")

    if settings.bot_mode == "webhook":
        await bot.set_webhook(
            url=f"{settings.server_url}/bot/webhook",
            drop_pending_updates=True,
            allowed_updates=["message", "callback_query", "chat_join_request", "chat_member"],
        )
        logger.info(f"Webhook set: {settings.server_url}/bot/webhook")
    else:
        await bot.delete_webhook(drop_pending_updates=True)

    logger.info("Bot started successfully ✅")


async def on_shutdown(bot: Bot):
    logger.info("Shutting down...")
    await close_pool()
    await bot.session.close()


async def main():
    bot = Bot(
        token=settings.bot_token,
        default=DefaultBotProperties(parse_mode=ParseMode.HTML),
    )

    storage = MemoryStorage()
    dp = Dispatcher(storage=storage)

    # ── Мидлвары ──────────────────────────────────────────────
    dp.message.middleware(OwnerMiddleware())
    dp.callback_query.middleware(OwnerMiddleware())

    # ── Роутеры ───────────────────────────────────────────────
    dp.include_router(start_router)
    dp.include_router(channels_router)
    dp.include_router(blacklist_router)
    dp.include_router(join_requests_router)
    dp.include_router(captcha_router)
    dp.include_router(mailing_router)
    dp.include_router(links_router)
    dp.include_router(payment_router)
    dp.include_router(channel_settings_router)
    dp.include_router(feedback_router)

    # ── Webhook (Railway) или Polling (локально) ──────────────
    if settings.bot_mode == "webhook":
        # Railway передаёт $PORT через переменную окружения
        port = int(os.environ.get("PORT", 8000))
        from api.server import create_app
        import uvicorn
        app = create_app(bot, dp)
        logger.info(f"Starting webhook server on port {port}")

        # Инициализируем бота перед запуском сервера
        await on_startup(bot)

        config = uvicorn.Config(
            app,
            host="0.0.0.0",
            port=port,
            log_level="info",
        )
        server = uvicorn.Server(config)
        try:
            await server.serve()
        finally:
            await on_shutdown(bot)

    else:
        # Polling — регистрируем хуки через dp
        dp.startup.register(on_startup)
        dp.shutdown.register(on_shutdown)
        await dp.start_polling(
            bot,
            allowed_updates=["message", "callback_query", "chat_join_request", "chat_member"],
        )


if __name__ == "__main__":
    asyncio.run(main())
