"""
handlers/feedback.py — Обратная связь: выбор режима, получателя, языка, ответ на сообщение.
"""
import logging
from aiogram import Router, F, Bot
from aiogram.types import (
    Message, CallbackQuery, InlineKeyboardMarkup, InlineKeyboardButton
)
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup

import db.pool as db

logger = logging.getLogger(__name__)
router = Router()


class FeedbackFSM(StatesGroup):
    waiting_for_reply = State()


# ── Языковая сетка ───────────────────────────────────────────
def _mk_lang_rows(prefix_key: int, prefix: str, current_lang: str) -> list:
    flat = [
        ("RU 🇷🇺", "ru"), ("UK 🇺🇦", "uk"), ("BY 🇧🇾", "by"),
        ("UZ 🇺🇿", "uz"), ("AZ 🇦🇿", "az"), ("KZ 🇰🇿", "kz"),
        ("EN 🇬🇧", "en"), ("ES 🇪🇸", "es"), ("DE 🇩🇪", "de"),
        ("ZH 🇨🇳", "zh"), ("HI 🇮🇳", "hi"), ("AR 🇸🇦", "ar"),
    ]
    rows = []
    for i in range(0, len(flat), 3):
        row = []
        for label, code in flat[i:i+3]:
            dot = "🔵" if code == current_lang else "⚪"
            row.append(InlineKeyboardButton(
                text=f"{dot} {label}",
                callback_data=f"{prefix}:{prefix_key}:{code}",
            ))
        rows.append(row)
    return rows


def _feedback_text() -> str:
    return (
        "📣 <b>Обратная связь</b>\n\n"
        "🛎 Сообщения от пользователей будут приходить в диалог с ботом.\n\n"
        "🤖 Язык технических сообщений \"обратной связи\" зависит от выбранных настроек.\n\n"
        "👥 Сообщения будут приходить только владельцу или всем админам, в зависимости от настроек.\n\n"
        "<b>Выберите действие 👇</b>"
    )


# ══════════════════════════════════════════════════════════════
# Бот-уровневый экран (вызывается из bs_feedback без picker-а)
# ══════════════════════════════════════════════════════════════
async def show_bot_feedback(callback: CallbackQuery, platform_user: dict, child_bot_id: int):
    # Ищем бот через реального owner (поддерживаем admin-доступ)
    bot_row = await db.fetchrow(
        """SELECT cb.* FROM child_bots cb
           WHERE cb.id=$1 AND (
               cb.owner_id=$2
               OR EXISTS (
                   SELECT 1 FROM team_members tm
                   WHERE tm.child_bot_id=cb.id AND tm.user_id=$2 AND tm.is_active=true
               )
           )""",
        child_bot_id, platform_user["user_id"],
    )
    if not bot_row:
        await callback.answer("Бот не найден", show_alert=True)
        return

    enabled = bot_row.get("feedback_enabled", False)
    target  = bot_row.get("feedback_target", "owner")
    lang    = bot_row.get("feedback_lang", "ru")

    toggle_text  = "✗ Обратная связь: выкл" if not enabled else "✓ Обратная связь: вкл"
    target_label = "создателю" if target == "owner" else "всем администраторам"

    keyboard = [
        [InlineKeyboardButton(text=toggle_text,                       callback_data=f"bsf_toggle:{child_bot_id}")],
        [InlineKeyboardButton(text=f"→ Отправлять: {target_label}",  callback_data=f"bsf_target:{child_bot_id}")],
        [InlineKeyboardButton(text="Выберите язык",                   callback_data="noop")],
    ]
    keyboard += _mk_lang_rows(child_bot_id, "bsf_lang", lang)
    keyboard.append([InlineKeyboardButton(text="◀️ Назад", callback_data=f"bot_settings:{child_bot_id}")])

    await callback.message.edit_text(
        _feedback_text(),
        reply_markup=InlineKeyboardMarkup(inline_keyboard=keyboard),
    )
    await callback.answer()


@router.callback_query(F.data.startswith("bsf_toggle:"))
async def on_bsf_toggle(callback: CallbackQuery, platform_user: dict | None):
    if not platform_user:
        return
    bot_id = int(callback.data.split(":")[1])
    # Ищем через реального owner (admin-доступ)
    row = await db.fetchrow(
        """SELECT cb.feedback_enabled, cb.owner_id FROM child_bots cb
           WHERE cb.id=$1 AND (
               cb.owner_id=$2
               OR EXISTS (SELECT 1 FROM team_members tm
                          WHERE tm.child_bot_id=cb.id AND tm.user_id=$2 AND tm.is_active=true)
           )""",
        bot_id, platform_user["user_id"],
    )
    new_val = not (row["feedback_enabled"] if row else False)
    real_owner = row["owner_id"] if row else platform_user["user_id"]
    await db.execute(
        "UPDATE child_bots SET feedback_enabled=$1 WHERE id=$2 AND owner_id=$3",
        new_val, bot_id, real_owner,
    )
    await callback.answer("✅ Включена" if new_val else "❌ Выключена")
    await show_bot_feedback(callback, platform_user, bot_id)


@router.callback_query(F.data.startswith("bsf_target:"))
async def on_bsf_target(callback: CallbackQuery, platform_user: dict | None):
    if not platform_user:
        return
    bot_id = int(callback.data.split(":")[1])
    row = await db.fetchrow(
        """SELECT cb.feedback_target, cb.owner_id FROM child_bots cb
           WHERE cb.id=$1 AND (
               cb.owner_id=$2
               OR EXISTS (SELECT 1 FROM team_members tm
                          WHERE tm.child_bot_id=cb.id AND tm.user_id=$2 AND tm.is_active=true)
           )""",
        bot_id, platform_user["user_id"],
    )
    current = row["feedback_target"] if row else "owner"
    new_target = "all" if current == "owner" else "owner"
    real_owner = row["owner_id"] if row else platform_user["user_id"]
    await db.execute(
        "UPDATE child_bots SET feedback_target=$1 WHERE id=$2 AND owner_id=$3",
        new_target, bot_id, real_owner,
    )
    await callback.answer("→ Всем администраторам" if new_target == "all" else "→ Создателю")
    await show_bot_feedback(callback, platform_user, bot_id)


@router.callback_query(F.data.startswith("bsf_lang:"))
async def on_bsf_lang(callback: CallbackQuery, platform_user: dict | None):
    if not platform_user:
        return
    parts  = callback.data.split(":")
    bot_id = int(parts[1])
    lang   = parts[2]
    # Находим реального owner (admin-доступ)
    row = await db.fetchrow(
        """SELECT cb.owner_id FROM child_bots cb
           WHERE cb.id=$1 AND (
               cb.owner_id=$2
               OR EXISTS (SELECT 1 FROM team_members tm
                          WHERE tm.child_bot_id=cb.id AND tm.user_id=$2 AND tm.is_active=true)
           )""",
        bot_id, platform_user["user_id"],
    )
    real_owner = row["owner_id"] if row else platform_user["user_id"]
    await db.execute(
        "UPDATE child_bots SET feedback_lang=$1 WHERE id=$2 AND owner_id=$3",
        lang, bot_id, real_owner,
    )
    await callback.answer(f"Язык: {lang.upper()}")
    await show_bot_feedback(callback, platform_user, bot_id)


# ── Языки (устаревший алиас для per-chat) ────────────────────
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
async def handle_feedback_message(
    message: Message, bot: Bot, owner_id: int, chat_id: int,
    child_bot_id: int | None = None,
    child_bot_instance: Bot | None = None,   # дочерний бот — основной канал уведомлений
):
    """
    Вызывается из child_bot_runner при входящем личном сообщении.
    Проверяет feedback_enabled на уровне площадки (bot_chats) ИЛИ бота (child_bots).
    Пересылает сообщение владельцу (или всем администраторам).

    Логика отправки:
      1) child_bot_instance (дочерний бот) — primary
      2) bot (основной Bumblebee) — fallback, если admin не запустил /start дочернему
    """
    settings = await db.fetchrow(
        "SELECT * FROM bot_chats WHERE owner_id=$1 AND chat_id=$2",
        owner_id, chat_id,
    )
    if not settings:
        return

    chat_level_enabled = settings.get("feedback_enabled") or False
    bot_level_enabled  = False
    bot_row = None

    if not chat_level_enabled and child_bot_id:
        bot_row = await db.fetchrow(
            "SELECT feedback_enabled, feedback_target, feedback_lang FROM child_bots WHERE id=$1",
            child_bot_id,
        )
        bot_level_enabled = (bot_row.get("feedback_enabled") or False) if bot_row else False

    if not chat_level_enabled and not bot_level_enabled:
        return

    if chat_level_enabled:
        target = settings.get("feedback_target", "owner")
    else:
        target = (bot_row.get("feedback_target") or "owner") if bot_row else "owner"

    from_user = message.from_user
    username  = f"@{from_user.username}" if from_user and from_user.username else f"ID: {from_user.id if from_user else '?'}"
    name      = (from_user.first_name or "?") if from_user else "?"
    user_id   = from_user.id if from_user else 0

    chat_title = settings.get("chat_title") or str(chat_id)
    caption = (
        f"📩 <b>Обратная связь</b>\n"
        f"Площадка: <b>{chat_title}</b>\n"
        f"От: {name} ({username})"
    )

    # Кнопка «Ответить» — callback будет обработан дочерним ботом
    reply_kb = None
    if child_bot_id and user_id:
        reply_kb = InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(
                text=f"✉️ Ответить {name}",
                callback_data=f"fb_reply:{child_bot_id}:{user_id}:{owner_id}",
            )]
        ])

    recipients = [owner_id]
    if target == "all":
        admins = await db.fetch(
            "SELECT user_id FROM team_members WHERE owner_id=$1 AND role IN ('admin','moderator') AND is_active=true",
            owner_id,
        )
        recipients += [a["user_id"] for a in admins]

    # Определяем «основного» отправителя: дочерний бот (предпочтительно) или main_bot (fallback)
    primary_bot = child_bot_instance if child_bot_instance else bot

    for recipient_id in set(recipients):
        sent = False
        # ── Попытка 1: через дочернего бота ──────────────────────
        if child_bot_instance:
            try:
                if message.text:
                    full_text = f"{caption}\n\n{message.text}"
                    await child_bot_instance.send_message(recipient_id, full_text, parse_mode="HTML", reply_markup=reply_kb)
                else:
                    extra = ("\n\n" + message.caption) if message.caption else ""
                    await message.copy_to(
                        recipient_id,
                        caption=caption + extra,
                        parse_mode="HTML",
                        reply_markup=reply_kb,
                    )
                sent = True
            except Exception as e:
                logger.debug(f"[FEEDBACK] child_bot send to {recipient_id} failed: {e} — trying sticker fallback")
                # Стикеры не поддерживают caption — шлём уведомление + медиа отдельно
                try:
                    await child_bot_instance.send_message(recipient_id, caption, parse_mode="HTML", reply_markup=reply_kb)
                    await message.copy_to(recipient_id)
                    sent = True
                except Exception as e2:
                    logger.debug(f"[FEEDBACK] child_bot sticker fallback also failed to {recipient_id}: {e2}")

        # ── Попытка 2 (fallback): через основной бот ─────────────
        if not sent:
            logger.info(f"[FEEDBACK] Falling back to main_bot for recipient {recipient_id}")
            try:
                if message.text:
                    full_text = f"{caption}\n\n{message.text}"
                    await bot.send_message(recipient_id, full_text, parse_mode="HTML", reply_markup=reply_kb)
                else:
                    extra = ("\n\n" + message.caption) if message.caption else ""
                    await message.copy_to(
                        recipient_id,
                        caption=caption + extra,
                        parse_mode="HTML",
                        reply_markup=reply_kb,
                    )
            except Exception as e:
                logger.debug(f"[FEEDBACK] main_bot fallback also failed to {recipient_id}: {e}")
                try:
                    await bot.send_message(recipient_id, caption, parse_mode="HTML", reply_markup=reply_kb)
                    await message.copy_to(recipient_id)
                except Exception as e2:
                    logger.debug(f"[FEEDBACK] main_bot sticker fallback failed to {recipient_id}: {e2}")


# ── Обработка кнопки «Ответить» ────────────────────────────────
@router.callback_query(F.data.startswith("fb_reply:"))
async def on_fb_reply(callback: CallbackQuery, state: FSMContext):
    """Владелец/админ нажал «Ответить». Callback приходит в главный бот т.к. уведомление отправлялось от него."""
    await callback.answer()
    try:
        parts          = callback.data.split(":")
        if len(parts) < 4:
            logger.warning(f"[FB_REPLY] Old callback format (no owner_id): {callback.data}")
            await callback.message.answer("⚠️ Устаревшая кнопка. Дождитесь нового сообщения.")
            return
        child_bot_id   = int(parts[1])
        target_user_id = int(parts[2])
        owner_id       = int(parts[3])
        clicker_id     = callback.from_user.id

        # Проверяем: пользователь должен быть владельцем ИЛИ членом команды
        if clicker_id != owner_id:
            is_allowed = await db.fetchval(
                "SELECT 1 FROM team_members WHERE user_id=$1 AND owner_id=$2 AND is_active=true",
                clicker_id, owner_id,
            )
            if not is_allowed:
                await callback.message.answer("❌ Нет доступа к этому действию.")
                return

        # Извлекаем имя и юзернейм из текста уведомления
        target_name = "пользователю"
        target_username = ""
        if callback.message and callback.message.text:
            for line in callback.message.text.splitlines():
                if line.startswith("От:"):
                    raw = line.replace("От:", "").strip()
                    if " (" in raw:
                        target_name     = raw.split(" (")[0].strip()
                        target_username = raw.split(" (")[1].rstrip(")")
                    else:
                        target_name = raw
                    break

        # Сохраняем исходный текст уведомления для восстановления при отмене
        original_text = callback.message.text or ""

        # Редактируем то же сообщение в режим ввода ответа (без создания нового)
        name_display = f"{target_name} ({target_username})" if target_username else target_name
        cancel_kb = InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="❌ Отмена", callback_data="fb_cancel_reply")],
        ])
        try:
            await callback.message.edit_text(
                f"✉️ <b>Напишите ответ для {name_display}:</b>\n\n"
                f"Следующее сообщение, которое вы напишете сюда, будет отправлено пользователю в личку через дочернего бота.\n"
                f"Для отмены — нажмите кнопку ниже.",
                parse_mode="HTML",
                reply_markup=cancel_kb,
            )
        except Exception as edit_err:
            logger.warning(f"[FB_REPLY] Could not edit notification message: {edit_err}")

        await state.set_state(FeedbackFSM.waiting_for_reply)
        await state.update_data(
            child_bot_id=child_bot_id,
            target_user_id=target_user_id,
            target_name=target_name,
            target_username=target_username,
            notification_msg_id=callback.message.message_id,
            original_text=original_text,
            owner_id=owner_id,
        )
        logger.info(f"[FB_REPLY] clicker={clicker_id} set reply state for user={target_user_id} via bot={child_bot_id}")
        await callback.answer(f"✉️ Напишите ответ для {target_name} 👇")

    except Exception as e:
        logger.error(f"[FB_REPLY] Unexpected error: {e}", exc_info=True)
        await callback.answer(f"⚠️ Ошибка: {e}", show_alert=True)


# ── Обработка кнопки «Отмена» ────────────────────────────────
@router.callback_query(F.data == "fb_cancel_reply")
async def on_fb_cancel_reply(callback: CallbackQuery, state: FSMContext):
    data = await state.get_data()
    await state.clear()

    original_text   = data.get("original_text", "")
    target_name     = data.get("target_name", "пользователю")
    target_username = data.get("target_username", "")
    child_bot_id    = data.get("child_bot_id")
    target_user_id  = data.get("target_user_id")
    owner_id        = data.get("owner_id", 0)

    try:
        if original_text and child_bot_id and target_user_id:
            # Восстанавливаем исходное уведомление с кнопкой «Ответить»
            restore_kb = InlineKeyboardMarkup(inline_keyboard=[[
                InlineKeyboardButton(
                    text=f"✉️ Ответить {target_name}",
                    callback_data=f"fb_reply:{child_bot_id}:{target_user_id}:{owner_id}",
                )
            ]])
            await callback.message.edit_text(original_text, parse_mode="HTML", reply_markup=restore_kb)
        else:
            await callback.message.edit_text("❌ Ответ отменён.")
    except Exception:
        pass
    await callback.answer()


# ── Обработка введённого ответа ────────────────────────────────
@router.message(FeedbackFSM.waiting_for_reply)
async def on_feedback_reply_text(message: Message, state: FSMContext):
    """Владелец ввёл текст ответа — отправляем через дочернего бота."""
    data             = await state.get_data()
    child_bot_id     = data.get("child_bot_id")
    target_user_id   = data.get("target_user_id")
    target_name      = data.get("target_name", "пользователю")
    target_username  = data.get("target_username", "")
    prompt_msg_id    = data.get("prompt_msg_id")     # ID промпт-сообщения
    notification_msg_id = data.get("notification_msg_id")  # ID уведомления наверху
    owner_id_fb      = data.get("owner_id", 0)
    await state.clear()

    if not child_bot_id or not target_user_id:
        await message.answer("⚠️ Ошибка: данные для ответа потеряны.")
        return

    row = await db.fetchrow(
        "SELECT token_encrypted FROM child_bots WHERE id=$1",
        child_bot_id,
    )
    if not row:
        await message.answer("⚠️ Дочерний бот не найден.")
        return

    from services.security import decrypt_token
    token = decrypt_token(row["token_encrypted"])
    child_bot = Bot(token=token)
    name_display = f"{target_name} ({target_username})" if target_username else target_name

    try:
        # Отправляем через дочернего бота с заголовком
        header = "💬 <b>Ответ от поддержки</b>\n\n"
        reply_hint = "\n\n──────────────────\n💌 <i>Отправьте своё сообщение</i> 👇"
        sent_msg = None
        if message.text:
            sent_msg = await child_bot.send_message(
                target_user_id,
                header + message.text + reply_hint,
                parse_mode="HTML",
            )
        elif message.photo:
            caption_text = (header + (message.caption or "")) + reply_hint
            sent_msg = await child_bot.send_photo(
                target_user_id, message.photo[-1].file_id,
                caption=caption_text,
                parse_mode="HTML",
            )
        elif message.video:
            caption_text = (header + (message.caption or "")) + reply_hint
            sent_msg = await child_bot.send_video(
                target_user_id, message.video.file_id,
                caption=caption_text,
                parse_mode="HTML",
            )
        elif message.document:
            caption_text = (header + (message.caption or "")) + reply_hint
            sent_msg = await child_bot.send_document(
                target_user_id, message.document.file_id,
                caption=caption_text,
                parse_mode="HTML",
            )
        elif message.voice:
            sent_msg = await child_bot.send_voice(target_user_id, message.voice.file_id)
        elif message.audio:
            caption_text = (header + (message.caption or "")) + reply_hint
            sent_msg = await child_bot.send_audio(
                target_user_id, message.audio.file_id,
                caption=caption_text,
                parse_mode="HTML",
            )
        elif message.sticker:
            await child_bot.send_message(
                target_user_id,
                header.strip() + reply_hint,
                parse_mode="HTML",
            )
            sent_msg = await child_bot.send_sticker(target_user_id, message.sticker.file_id)
        elif message.video_note:
            sent_msg = await child_bot.send_video_note(target_user_id, message.video_note.file_id)
        else:
            await message.answer("⚠️ Такой тип сообщения не поддерживается. Отправьте текст, фото или голосовое.")
            return

        logger.info(f"[FEEDBACK REPLY] Sent reply to user {target_user_id} via bot {child_bot_id}")

        # Удаляем prompt-сообщение и отправляем новое сообщение об успехе внизу (с кнопкой «Написать ещё»)
        work_msg_id   = data.get("work_msg_id")
        target_msg_id = notification_msg_id or work_msg_id
        more_kb = InlineKeyboardMarkup(inline_keyboard=[[
            InlineKeyboardButton(
                text="💬 Написать ещё",
                callback_data=f"fbr_more:{child_bot_id}:{target_user_id}:{owner_id_fb}",
            )
        ]])
        if target_msg_id:
            try:
                await message.bot.delete_message(
                    chat_id=message.chat.id,
                    message_id=target_msg_id,
                )
            except Exception:
                pass
        await message.answer(
            f"✅ <b>Ответ отправлен</b>\n\nПользователь <b>{name_display}</b> получил ваш ответ.",
            parse_mode="HTML",
            reply_markup=more_kb,
        )

    except Exception as e:
        await message.answer(
            f"⚠️ Не удалось отправить ответ: {e}\n\n"
            "Возможно, пользователь не писал дочернему боту (/start не нажал)."
        )
        logger.warning(f"[FEEDBACK REPLY] Failed: {e}")
    finally:
        await child_bot.session.close()


@router.callback_query(F.data.startswith("fbr_more:"))
async def on_fbr_more(callback: CallbackQuery, state: FSMContext):
    """Редактируем сообщение в режим ввода (main-bot путь)."""
    parts          = callback.data.split(":")
    child_bot_id   = int(parts[1])
    target_user_id = int(parts[2])
    owner_id_fb    = int(parts[3])
    work_msg_id    = callback.message.message_id  # редактируем то же сообщение

    row = await db.fetchrow(
        "SELECT first_name, username FROM bot_users WHERE user_id=$1 LIMIT 1",
        target_user_id,
    )
    if row:
        target_name     = row["first_name"] or "Пользователь"
        target_username = f"@{row['username']}" if row["username"] else ""
    else:
        target_name, target_username = "Пользователь", ""
    name_display = f"{target_name} ({target_username})" if target_username else target_name

    # Редактируем то же сообщение → «режим ввода»
    cancel_kb = InlineKeyboardMarkup(inline_keyboard=[[
        InlineKeyboardButton(
            text="❌ Отмена",
            callback_data=f"fbr_cancel:{child_bot_id}:{target_user_id}:{owner_id_fb}",
        )
    ]])
    await callback.message.edit_text(
        f"✉️ <b>Напишите ответ для {name_display}:</b>\n\n"
        "Следующее сообщение, которое вы напишете сюда, будет отправлено пользователю.\n"
        "Для отмены — нажмите кнопку ниже или /cancel",
        parse_mode="HTML",
        reply_markup=cancel_kb,
    )
    await state.set_state(FeedbackFSM.waiting_for_reply)
    await state.update_data(
        child_bot_id=child_bot_id,
        target_user_id=target_user_id,
        target_name=target_name,
        target_username=row["username"] if row and row["username"] else "",
        notification_msg_id=None,
        owner_id=owner_id_fb,
        work_msg_id=work_msg_id,
        prompt_msg_id=None,
    )
    await callback.answer("✍️ Напишите следующее 👇")


@router.callback_query(F.data.startswith("fbr_cancel:"))
async def on_fbr_cancel(callback: CallbackQuery, state: FSMContext):
    """Отмена «Написать ещё» — редактируем сообщение обратно в «успех»."""
    parts          = callback.data.split(":")
    child_bot_id   = int(parts[1])
    target_user_id = int(parts[2])
    owner_id_fb    = int(parts[3])

    data = await state.get_data()
    tname  = data.get("target_name", "Пользователь")
    tuname = data.get("target_username", "")
    ndisplay = f"{tname} ({tuname})" if tuname else tname
    await state.clear()

    more_kb = InlineKeyboardMarkup(inline_keyboard=[[
        InlineKeyboardButton(
            text="💬 Написать ещё",
            callback_data=f"fbr_more:{child_bot_id}:{target_user_id}:{owner_id_fb}",
        )
    ]])
    try:
        await callback.message.edit_text(
            f"✅ Ответ успешно отправлен пользователю <b>{ndisplay}</b>.",
            parse_mode="HTML",
            reply_markup=more_kb,
        )
    except Exception:
        pass
    await callback.answer("❌ Отменено")
