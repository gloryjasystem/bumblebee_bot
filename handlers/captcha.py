"""
handlers/captcha.py — Капча «Я не робот», таймер, авто-удаление сообщения.
"""
import asyncio
import logging
from aiogram import Router, F, Bot
from aiogram.types import CallbackQuery, ChatJoinRequest
from aiogram.types import InlineKeyboardMarkup, InlineKeyboardButton

import db.pool as db

logger = logging.getLogger(__name__)
router = Router()

# Хранилище pending-заявок: {(chat_id, user_id): join_request}
_pending: dict[tuple[int, int], ChatJoinRequest] = {}


async def send_captcha(bot: Bot, event: ChatJoinRequest, settings_row: dict):
    """
    Отправляет капчу пользователю в личку.
    Если пользователь не открыл бота — авто-одобряем (нельзя написать ему).
    """
    user = event.from_user
    key = (event.chat.id, user.id)
    _pending[key] = event

    text = (
        settings_row.get("captcha_text")
        or f"👋 Привет, <b>{user.first_name}</b>!\n\n"
           f"Прежде чем войти в <b>{event.chat.title}</b>,\n"
           f"докажи что ты не робот — нажми кнопку ниже ✅\n\n"
           f"⏱ У тебя {settings_row.get('captcha_timer', 60)} секунд."
    )
    kb = InlineKeyboardMarkup(inline_keyboard=[[
        InlineKeyboardButton(
            text="✅ Я не робот",
            callback_data=f"captcha:{event.chat.id}:{user.id}",
        )
    ]])

    try:
        msg = await bot.send_message(user.id, text, reply_markup=kb)
        # Таймер — если не прошёл, отклоняем заявку
        asyncio.create_task(
            _captcha_timeout(bot, event, settings_row, msg.message_id)
        )
    except Exception:
        # Пользователь не открыл диалог с ботом — авто-одобряем
        _pending.pop(key, None)
        await event.approve()
        from handlers.join_requests import _register_user, _send_welcome
        await _register_user(settings_row["owner_id"], event.chat.id, user)
        await _send_welcome(bot, event.chat.id, user, settings_row)


async def _captcha_timeout(
    bot: Bot, event: ChatJoinRequest, settings_row: dict, msg_id: int
):
    """Отклоняет заявку если капча не пройдена за timer секунд."""
    timer = settings_row.get("captcha_timer") or 60
    await asyncio.sleep(timer)

    key = (event.chat.id, event.from_user.id)
    if key in _pending:
        _pending.pop(key)
        try:
            await event.decline()
        except Exception:
            pass
        # Удаляем сообщение с капчей
        try:
            await bot.delete_message(event.from_user.id, msg_id)
        except Exception:
            pass
        # Уведомляем об истечении
        try:
            await bot.send_message(
                event.from_user.id,
                "⏱ Время вышло. Заявка отклонена.\n"
                "Вы можете подать заявку повторно.",
            )
        except Exception:
            pass


@router.callback_query(F.data.startswith("captcha:"))
async def on_captcha_passed(callback: CallbackQuery, bot: Bot):
    """Пользователь нажал «Я не робот»."""
    parts = callback.data.split(":")
    chat_id = int(parts[1])
    user_id = int(parts[2])

    # Проверяем что это именно тот пользователь
    if callback.from_user.id != user_id:
        await callback.answer("❌ Эта капча не для вас", show_alert=True)
        return

    key = (chat_id, user_id)
    event = _pending.pop(key, None)
    if not event:
        await callback.answer("Капча уже обработана или истекла", show_alert=True)
        return

    # Одобряем заявку
    try:
        await event.approve()
    except Exception as e:
        logger.warning(f"Approve failed: {e}")
        await callback.answer("❌ Не удалось одобрить заявку", show_alert=True)
        return

    # Регистрируем пользователя
    settings_row = await db.fetchrow(
        "SELECT * FROM bot_chats WHERE chat_id=$1", chat_id
    )
    if settings_row:
        from handlers.join_requests import _register_user, _send_welcome
        await _register_user(settings_row["owner_id"], chat_id, callback.from_user)
        await _send_welcome(bot, chat_id, callback.from_user, dict(settings_row))

        # Авто-удаление сообщения капчи (Про+)
        if settings_row.get("captcha_delete"):
            try:
                await callback.message.delete()
            except Exception:
                pass
        else:
            await callback.message.edit_text("✅ Капча пройдена! Добро пожаловать.")

    await callback.answer("✅ Отлично!")
