"""
handlers/feedback.py — Обратная связь: выбор режима, получателя, языка.
"""
import logging
from aiogram import Router, F, Bot
from aiogram.types import (
    Message, CallbackQuery, InlineKeyboardMarkup, InlineKeyboardButton
)

import db.pool as db

logger = logging.getLogger(__name__)
router = Router()

# ── Языки ────────────────────────────────────────────────────
LANGUAGES = [
    ("RU 🇷🇺", "ru"), ("UK 🇺🇦", "uk"), ("BY 🇧🇾", "by"),
    ("UZ 🇺🇿", "az", ), ("AZ 🇦🇿", "az"), ("KZ 🇰🇿", "kz"),
    ("EN 🇬🇧", "en"), ("ES 🇪🇸", "es"), ("DE 🇩🇪", "de"),
    ("ZH 🇨🇳", "zh"), ("HI 🇮🇳", "hi"), ("AR 🇸🇦", "ar"),
]

# сгруппируем по 3 в ряд
def _lang_rows(chat_id: int, current_lang: str) -> list:
    rows = []
    flat = [
        ("RU 🇷🇺", "ru"), ("UK 🇺🇦", "uk"), ("BY 🇧🇾", "by"),
        ("UZ 🇺🇿", "uz"), ("AZ 🇦🇿", "az"), ("KZ 🇰🇿", "kz"),
        ("EN 🇬🇧", "en"), ("ES 🇪🇸", "es"), ("DE 🇩🇪", "de"),
        ("ZH 🇨🇳", "zh"), ("HI 🇮🇳", "hi"), ("AR 🇸🇦", "ar"),
    ]
    for i in range(0, len(flat), 3):
        row = []
        for label, code in flat[i:i+3]:
            dot = "🔵" if code == current_lang else "⚪"
            row.append(InlineKeyboardButton(
                text=f"{dot} {label}",
                callback_data=f"fb_lang:{chat_id}:{code}",
            ))
        rows.append(row)
    return rows


# ── Главный экран Обратной связи ─────────────────────────────
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
        await callback.answer("Площадка не найдена", show_alert=True)
        return

    enabled = ch.get("feedback_enabled", False)
    target  = ch.get("feedback_target", "owner")
    lang    = ch.get("feedback_lang", "ru")

    toggle_text  = "✗ Обратная связь: выкл" if not enabled else "✓ Обратная связь: вкл"
    target_label = "создателю" if target == "owner" else "всем администраторам"

    # Строим клавиатуру
    keyboard = [
        [InlineKeyboardButton(text=toggle_text,          callback_data=f"fb_toggle:{chat_id}")],
        [InlineKeyboardButton(text=f"→ Отправлять: {target_label}", callback_data=f"fb_target:{chat_id}")],
        [InlineKeyboardButton(text="Выберите язык", callback_data="noop")],
    ]
    keyboard += _lang_rows(chat_id, lang)
    keyboard.append([InlineKeyboardButton(text="◀️ Назад", callback_data=f"channel_by_chat:{chat_id}")])

    await callback.message.edit_text(
        "📣 <b>Обратная связь</b>\n\n"
        "🛎 Сообщения от пользователей будут приходить в диалог с ботом.\n\n"
        "🤖 Язык технических сообщений \"обратной связи\" зависит от выбранных настроек.\n\n"
        "👥 Сообщения будут приходить только владельцу или всем админам, в зависимости от настроек.\n\n"
        "<b>Выберите действие 👇</b>",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=keyboard),
    )
    await callback.answer()


# ── Тоггл вкл/выкл ───────────────────────────────────────────
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


# ── Переключение получателя ───────────────────────────────────
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
    label = "Всем администраторам" if new_target == "all" else "Создателю"
    await callback.answer(f"→ Отправлять: {label}")
    await _show_feedback(callback, platform_user, chat_id)


# ── Выбор языка ───────────────────────────────────────────────
@router.callback_query(F.data.startswith("fb_lang:"))
async def on_feedback_lang(callback: CallbackQuery, platform_user: dict | None):
    if not platform_user:
        return
    parts   = callback.data.split(":")
    chat_id = int(parts[1])
    lang    = parts[2]
    await db.execute(
        "UPDATE bot_chats SET feedback_lang=$1 WHERE owner_id=$2 AND chat_id=$3",
        lang, platform_user["user_id"], chat_id,
    )
    await callback.answer(f"Язык: {lang.upper()}")
    await _show_feedback(callback, platform_user, chat_id)


# ── Форвардинг входящих сообщений ─────────────────────────────
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
        admins = await db.fetch(
            "SELECT user_id FROM team_members WHERE owner_id=$1 AND role IN ('admin','moderator') AND is_active=true",
            owner_id,
        )
        recipients += [a["user_id"] for a in admins]

    for recipient_id in set(recipients):
        try:
            await bot.send_message(recipient_id, caption)
            await message.copy_to(recipient_id)
        except Exception as e:
            logger.debug(f"Feedback forward failed to {recipient_id}: {e}")
