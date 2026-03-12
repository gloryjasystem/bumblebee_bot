"""
scheduler/jobs.py — APScheduler: истечение тарифов + уведомления о скором окончании.
"""
import logging
from aiogram import Bot
from apscheduler.schedulers.asyncio import AsyncIOScheduler

logger = logging.getLogger(__name__)

_bot: Bot = None

TARIFF_NAMES = {"start": "🌱 Старт", "pro": "⭐ Про", "business": "💼 Бизнес"}


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
        misfire_grace_time=600,
    )

    # ── Ежедневные уведомления об истечении тарифов ───────────
    scheduler.add_job(
        warn_expiring_tariffs,
        "interval",
        hours=24,
        id="warn_expiring_tariffs",
        replace_existing=True,
        misfire_grace_time=3600,
    )

    # ── Каждые 15 мин: открепить/удалить истёкшие сообщения рассылки ──
    scheduler.add_job(
        cleanup_pinned_mailing_msgs,
        "interval",
        minutes=15,
        id="cleanup_pinned_mailing_msgs",
        replace_existing=True,
        misfire_grace_time=300,
    )

    return scheduler



# ── Снятие тарифа при истечении ───────────────────────────────
async def expire_tariffs():
    """
    Переводит на free пользователей с истёкшей подпиской.
    Запускается каждый час.
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
    name = TARIFF_NAMES.get(old_tariff, old_tariff)
    try:
        from aiogram.types import InlineKeyboardMarkup, InlineKeyboardButton
        await _bot.send_message(
            user_id,
            f"❌ <b>Тариф {name} истёк</b>\n\n"
            f"Ваша подписка закончилась. Вы переведены на Free.\n\n"
            f"Для продления нажмите кнопку ниже 👇",
            reply_markup=InlineKeyboardMarkup(inline_keyboard=[[
                InlineKeyboardButton(text="💳 Продлить подписку", callback_data="menu:tariffs")
            ]]),
        )
    except Exception:
        pass  # Пользователь заблокировал бота


# ── Предупреждения о скором истечении ─────────────────────────
async def warn_expiring_tariffs():
    """
    Отправляет предупреждения пользователям за 7, 3 и 1 день до истечения.
    Запускается раз в сутки.
    """
    import db.pool as db

    # За 7 дней
    rows_7 = await db.fetch(
        """
        SELECT user_id, tariff, tariff_until
        FROM platform_users
        WHERE tariff != 'free'
          AND tariff_until::date = (now() + interval '7 days')::date
        """
    )
    for row in rows_7:
        await _notify_expiring(row["user_id"], row["tariff"], row["tariff_until"], days=7)

    # За 3 дня
    rows_3 = await db.fetch(
        """
        SELECT user_id, tariff, tariff_until
        FROM platform_users
        WHERE tariff != 'free'
          AND tariff_until::date = (now() + interval '3 days')::date
        """
    )
    for row in rows_3:
        await _notify_expiring(row["user_id"], row["tariff"], row["tariff_until"], days=3)

    # За 1 день
    rows_1 = await db.fetch(
        """
        SELECT user_id, tariff, tariff_until
        FROM platform_users
        WHERE tariff != 'free'
          AND tariff_until::date = (now() + interval '1 day')::date
        """
    )
    for row in rows_1:
        await _notify_expiring(row["user_id"], row["tariff"], row["tariff_until"], days=1)


async def _notify_expiring(user_id: int, tariff: str, until, days: int):
    """Отправляет предупреждение о скором истечении тарифа."""
    if not _bot:
        return
    name = TARIFF_NAMES.get(tariff, tariff)
    until_str = until.strftime("%d.%m.%Y") if until else "—"

    if days == 7:
        icon = "⚠️"
        urgency = f"через {days} дней"
    elif days == 3:
        icon = "🔔"
        urgency = f"через {days} дня"
    else:
        icon = "🚨"
        urgency = "ЗАВТРА"

    try:
        from aiogram.types import InlineKeyboardMarkup, InlineKeyboardButton
        await _bot.send_message(
            user_id,
            f"{icon} <b>Тариф {name} заканчивается {urgency}</b>\n\n"
            f"📅 Дата окончания: <b>{until_str}</b>\n\n"
            f"Продлите подписку, чтобы сохранить доступ к боту и всем функциям.\n"
            f"При продлении до истечения — дни <b>добавляются</b> к текущим!",
            reply_markup=InlineKeyboardMarkup(inline_keyboard=[[
                InlineKeyboardButton(text="💳 Продлить сейчас", callback_data="menu:tariffs")
            ]]),
        )
    except Exception:
        pass


# ── Открепление / удаление сообщений рассылки через 24ч ──────

async def cleanup_pinned_mailing_msgs():
    """
    Открепляет и/или удаляет сообщения рассылки у получателей,
    у которых истёк 24-часовой срок закрепления.
    Запускается каждые 15 минут.
    """
    import db.pool as db
    from services.security import decrypt_token

    rows = await db.fetch(
        """
        SELECT id, child_bot_id, tg_user_id, tg_message_id, delete_after
        FROM mailing_sent_messages
        WHERE pin_until IS NOT NULL
          AND pin_until <= now()
          AND unpinned = false
        LIMIT 500
        """
    )

    if not rows:
        return

    logger.info(f"[cleanup_pinned] found {len(rows)} messages to unpin/delete")

    # Группируем по child_bot_id, чтобы создавать бот-инстанс по одному разу
    from collections import defaultdict
    by_bot: dict[int | None, list] = defaultdict(list)
    for row in rows:
        by_bot[row["child_bot_id"]].append(row)

    for child_bot_id, bot_rows in by_bot.items():
        send_bot = _bot  # fallback — основной бот
        child_bot_instance = None

        if child_bot_id:
            try:
                bot_row = await db.fetchrow(
                    "SELECT token_encrypted FROM child_bots WHERE id=$1",
                    child_bot_id,
                )
                if bot_row and bot_row.get("token_encrypted"):
                    from aiogram import Bot as AioBot
                    child_bot_instance = AioBot(token=decrypt_token(bot_row["token_encrypted"]))
                    send_bot = child_bot_instance
            except Exception as e:
                logger.warning(f"[cleanup_pinned] child bot {child_bot_id} init error: {e}")

        processed_ids = []
        for row in bot_rows:
            user_id  = row["tg_user_id"]
            msg_id   = row["tg_message_id"]
            do_delete = bool(row["delete_after"])
            try:
                # Откреплять всегда (если pin_until задан)
                try:
                    await send_bot.unpin_chat_message(
                        chat_id=user_id,
                        message_id=msg_id,
                    )
                except Exception:
                    pass  # боты не могут открепить в ЛС — игнорируем

                # Удалить если надо
                if do_delete:
                    try:
                        await send_bot.delete_message(
                            chat_id=user_id,
                            message_id=msg_id,
                        )
                    except Exception:
                        pass
            except Exception as e:
                logger.debug(f"[cleanup_pinned] row {row['id']} error: {e}")
            finally:
                processed_ids.append(row["id"])

        # Помечаем обработанные строки
        if processed_ids:
            await db.execute(
                "UPDATE mailing_sent_messages SET unpinned=true WHERE id = ANY($1::bigint[])",
                processed_ids,
            )

        if child_bot_instance:
            try:
                await child_bot_instance.session.close()
            except Exception:
                pass

    logger.info(f"[cleanup_pinned] done, processed {len(rows)} rows")
