"""
handlers/feedback.py — Обратная связь: форвардинг сообщений от участников владельцу.
"""
import logging
from aiogram import Router, F, Bot
from aiogram.types import (
    Message, CallbackQuery, InlineKeyboardMarkup, InlineKeyboardButton
)

import db.pool as db

logger = logging.getLogger(__name__)
router = Router()


@router.callback_query(F.data.startswith("ch_feedback:"))
async def on_feedback_settings(callback: CallbackQuery, platform_user: dict | None):
    if not platform_user:
        return
    chat_id = int(callback.data.split(":")[1])
    await _show_feedback(callback, platform_user, chat_id)


async def _show_feedback(callback: CallbackQuery, platform_user: dict, chat_id: int):
    ch = await db.fetchrow(
        "SELECT * FROM bot_chats WHERE owner_id=$1 AND chat_id=$2::bigint",
        platform_user["user_id"], chat_id,
    )
    if not ch:
        return

    enabled = ch.get("feedback_enabled", False)
    target  = ch.get("feedback_target", "owner")
    toggle = "🔴 Отключить" if enabled else "🟢 Включить"
    target_text = "Всем администраторам" if target == "all" else "Только владельцу"

    await callback.message.edit_text(
        f"📩 <b>Обратная связь</b>\n\n"
        f"Статус: {'✅ Включена' if enabled else '❌ Выключена'}\n"
        f"Получатель: {target_text}\n\n"
        f"💡 Когда включена, сообщения участников\n"
        f"пересылаются вам анонимно.",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text=toggle,              callback_data=f"fb_toggle:{chat_id}")],
            [InlineKeyboardButton(text="👤 Кому пересылать",  callback_data=f"fb_target:{chat_id}")],
            [InlineKeyboardButton(text="◀️ Назад",              callback_data=f"channel_by_chat:{chat_id}")],
        ]),
    )
    await callback.answer()


@router.callback_query(F.data.startswith("fb_toggle:"))
async def on_feedback_toggle(callback: CallbackQuery, platform_user: dict | None):
    if not platform_user:
        return
    chat_id = int(callback.data.split(":")[1])
    ch = await db.fetchrow(
        "SELECT feedback_enabled FROM bot_chats WHERE owner_id=$1 AND chat_id=$2",
        platform_user["user_id"], chat_id,
    )
    new_val = not (ch["feedback_enabled"] if ch else False)
    await db.execute(
        "UPDATE bot_chats SET feedback_enabled=$1 WHERE owner_id=$2 AND chat_id=$3",
        new_val, platform_user["user_id"], chat_id,
    )
    await callback.answer("✅ Включена" if new_val else "❌ Выключена")
    await _show_feedback(callback, platform_user, chat_id)


@router.callback_query(F.data.startswith("fb_target:"))
async def on_feedback_target(callback: CallbackQuery, platform_user: dict | None):
    if not platform_user:
        return
    chat_id = int(callback.data.split(":")[1])
    ch = await db.fetchrow(
        "SELECT feedback_target FROM bot_chats WHERE owner_id=$1 AND chat_id=$2",
        platform_user["user_id"], chat_id,
    )
    current = ch["feedback_target"] if ch else "owner"
    new_target = "all" if current == "owner" else "owner"
    await db.execute(
        "UPDATE bot_chats SET feedback_target=$1 WHERE owner_id=$2 AND chat_id=$3",
        new_target, platform_user["user_id"], chat_id,
    )
    label = "Всем администраторам" if new_target == "all" else "Только владельцу"
    await callback.answer(f"Получатель: {label}")
    await _show_feedback(callback, platform_user, chat_id)


# ── Обработка входящих сообщений от участников (форвардинг) ──
async def handle_feedback_message(message: Message, bot: Bot, owner_id: int, chat_id: int):
    """
    Вызывается из join_requests.py или autoresponder.
    Пересылает (анонимно) сообщение владельцу через copy_message.
    """
    settings = await db.fetchrow(
        "SELECT * FROM bot_chats WHERE owner_id=$1 AND chat_id=$2",
        owner_id, chat_id,
    )
    if not settings or not settings.get("feedback_enabled"):
        return

    target = settings.get("feedback_target", "owner")
    from_user = message.from_user
    username = f"@{from_user.username}" if from_user.username else f"ID: {from_user.id}"

    caption = (
        f"📩 <b>Обратная связь</b>\n"
        f"Площадка: <b>{settings.get('chat_title','?')}</b>\n"
        f"От: {from_user.first_name} ({username})"
    )

    recipients = [owner_id]
    if target == "all":
        # Получаем всех администраторов площадки
        admins = await db.fetch(
            "SELECT user_id FROM team_members WHERE owner_id=$1 AND role IN ('admin','moderator') AND is_active=true",
            owner_id,
        )
        recipients += [a["user_id"] for a in admins]

    for recipient_id in set(recipients):
        try:
            # Отправляем заголовок
            await bot.send_message(recipient_id, caption)
            # Копируем сообщение анонимно (без имени отправителя)
            await message.copy_to(recipient_id)
        except Exception as e:
            logger.debug(f"Feedback forward failed to {recipient_id}: {e}")
