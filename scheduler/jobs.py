"""
scheduler/jobs.py — APScheduler с PostgreSQL JobStore.
"""
import logging
from aiogram import Bot
from apscheduler.schedulers.asyncio import AsyncIOScheduler

logger = logging.getLogger(__name__)

_bot: Bot = None


def setup_scheduler(bot: Bot) -> AsyncIOScheduler:
    """
    Создаёт и настраивает APScheduler с MemoryJobStore.
    Memory store проще и не конфликтует при rolling deploy на Railway.
    """
    global _bot
    _bot = bot

    scheduler = AsyncIOScheduler()

    # ── Ежечасовой downgrade тарифов ──────────────────────────
    scheduler.add_job(
        expire_tariffs,
        "interval",
        hours=1,
        id="expire_tariffs",
        replace_existing=True,
        misfire_grace_time=600,  # 10 минут допуска при пропуске
    )

    return scheduler


async def expire_tariffs():
    """
    Снимает тариф у пользователей с истёкшей подпиской.
    Запускается каждый час через APScheduler.
    """
    import db.pool as db

    expired = await db.fetch(
        """
        UPDATE platform_users
        SET tariff = 'free', tariff_until = NULL
        WHERE tariff != 'free'
          AND tariff_until < now()
        RETURNING user_id, tariff
        """
    )

    if expired:
        logger.info(f"Expired tariffs: {len(expired)} users")
        for row in expired:
            try:
                await _notify_expired(row["user_id"], row["tariff"])
            except Exception as e:
                logger.debug(f"Failed to notify {row['user_id']}: {e}")


async def _notify_expired(user_id: int, old_tariff: str):
    """Уведомляет пользователя об истечении тарифа."""
    if not _bot:
        return
    tariff_names = {"start": "Старт", "pro": "Про", "business": "Бизнес"}
    name = tariff_names.get(old_tariff, old_tariff)
    try:
        from aiogram.types import InlineKeyboardMarkup, InlineKeyboardButton
        await _bot.send_message(
            user_id,
            f"⏰ <b>Тариф {name} истёк</b>\n\n"
            f"Ваша подписка закончилась. Вы переведены на Free.\n\n"
            f"Для продления нажмите /start → 💳 Тарифы.",
            reply_markup=InlineKeyboardMarkup(inline_keyboard=[[
                InlineKeyboardButton(text="💳 Продлить подписку", callback_data="menu:tariffs")
            ]]),
        )
    except Exception:
        pass  # Пользователь заблокировал бота
