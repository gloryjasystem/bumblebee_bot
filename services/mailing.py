"""
services/mailing.py — Рассылка сообщений с rate limiting и паузой.
"""
import asyncio
import logging
from aiogram import Bot
from aiogram.exceptions import TelegramForbiddenError, TelegramRetryAfter

import db.pool as db

logger = logging.getLogger(__name__)

# Глобальный реестр активных рассылок: {mailing_id: {"paused": bool, "cancelled": bool}}
_active_mailings: dict[int, dict] = {}

SPEEDS = {
    "low":    0.08,   # ~12 msg/s
    "medium": 0.04,   # ~25 msg/s
    "high":   0.02,   # ~50 msg/s (риск ограничений)
}


async def run_mailing(mailing_id: int, bot: Bot):
    """
    Запускает рассылку для mailing_id.
    Отправляет только bot_activated=true пользователям.
    Поддерживает паузу и отмену в реальном времени.
    """
    mailing = await db.fetchrow(
        "SELECT * FROM mailings WHERE id=$1", mailing_id
    )
    if not mailing or mailing["status"] != "pending":
        return

    owner_id = mailing["owner_id"]
    chat_id  = mailing["chat_id"]

    # Получаем получателей — только bot_activated
    recipients = await db.fetch(
        """
        SELECT user_id FROM bot_users
        WHERE owner_id=$1 AND chat_id=$2
          AND is_active=true AND bot_activated=true
        ORDER BY joined_at
        """,
        owner_id, chat_id,
    )

    total = len(recipients)
    await db.execute(
        "UPDATE mailings SET status='running', started_at=now(), total_count=$1 WHERE id=$2",
        total, mailing_id,
    )

    _active_mailings[mailing_id] = {"paused": False, "cancelled": False, "speed": "low"}
    sent = errors = 0

    for rec in recipients:
        control = _active_mailings.get(mailing_id, {})

        # Проверка отмены
        if control.get("cancelled"):
            break

        # Пауза
        while control.get("paused"):
            await asyncio.sleep(1)
            control = _active_mailings.get(mailing_id, {})
            if control.get("cancelled"):
                break

        # Отправка
        try:
            await _send_message(bot, rec["user_id"], mailing)
            sent += 1
            await db.execute(
                "UPDATE mailings SET sent_count=$1 WHERE id=$2",
                sent, mailing_id,
            )
        except TelegramForbiddenError:
            # Пользователь заблокировал бота
            errors += 1
            await db.execute(
                "UPDATE bot_users SET bot_activated=false "
                "WHERE owner_id=$1 AND chat_id=$2 AND user_id=$3",
                owner_id, chat_id, rec["user_id"],
            )
        except TelegramRetryAfter as e:
            logger.warning(f"Mailing {mailing_id}: rate limit, sleeping {e.retry_after}s")
            await asyncio.sleep(e.retry_after)
            # Повторяем этого пользователя
            try:
                await _send_message(bot, rec["user_id"], mailing)
                sent += 1
            except Exception:
                errors += 1
        except Exception as e:
            errors += 1
            logger.debug(f"Mailing {mailing_id}: failed to send to {rec['user_id']}: {e}")

        # Rate limiting по скорости
        speed = _active_mailings.get(mailing_id, {}).get("speed", "low")
        await asyncio.sleep(SPEEDS.get(speed, 0.08))

    # Завершаем рассылку
    cancelled = _active_mailings.get(mailing_id, {}).get("cancelled", False)
    final_status = "cancelled" if cancelled else "done"
    await db.execute(
        "UPDATE mailings SET status=$1, finished_at=now(), "
        "sent_count=$2, error_count=$3 WHERE id=$4",
        final_status, sent, errors, mailing_id,
    )
    _active_mailings.pop(mailing_id, None)
    logger.info(f"Mailing {mailing_id} done: {sent}/{total}, errors={errors}")


async def _send_message(bot: Bot, user_id: int, mailing: dict):
    """Отправляет одно сообщение рассылки с учётом медиа и кнопок."""
    from aiogram.types import InlineKeyboardMarkup, InlineKeyboardButton
    import json

    # Кнопки
    kb = None
    if mailing.get("inline_buttons"):
        buttons_data = mailing["inline_buttons"]
        if isinstance(buttons_data, str):
            buttons_data = json.loads(buttons_data)
        kb = InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text=b["text"], url=b["url"])]
            for b in buttons_data
        ])

    text = mailing.get("text") or ""
    media_id = mailing.get("media_file_id")
    media_type = mailing.get("media_type")

    if media_type == "photo":
        await bot.send_photo(user_id, media_id, caption=text, reply_markup=kb)
    elif media_type == "video":
        await bot.send_video(user_id, media_id, caption=text, reply_markup=kb)
    elif media_type == "document":
        await bot.send_document(user_id, media_id, caption=text, reply_markup=kb)
    else:
        await bot.send_message(user_id, text, reply_markup=kb)


# ── Управление рассылкой в реальном времени ──────────────────
def pause_mailing(mailing_id: int):
    if mailing_id in _active_mailings:
        _active_mailings[mailing_id]["paused"] = True


def resume_mailing(mailing_id: int):
    if mailing_id in _active_mailings:
        _active_mailings[mailing_id]["paused"] = False


def cancel_mailing(mailing_id: int):
    if mailing_id in _active_mailings:
        _active_mailings[mailing_id]["cancelled"] = True


def set_speed(mailing_id: int, speed: str):
    if mailing_id in _active_mailings and speed in SPEEDS:
        _active_mailings[mailing_id]["speed"] = speed


def get_status(mailing_id: int) -> dict | None:
    return _active_mailings.get(mailing_id)
