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
    bot_row = await db.fetchrow(
        "SELECT * FROM child_bots WHERE id=$1 AND owner_id=$2",
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
    row = await db.fetchrow(
        "SELECT feedback_enabled FROM child_bots WHERE id=$1 AND owner_id=$2",
        bot_id, platform_user["user_id"],
    )
    new_val = not (row["feedback_enabled"] if row else False)
    await db.execute(
        "UPDATE child_bots SET feedback_enabled=$1 WHERE id=$2 AND owner_id=$3",
        new_val, bot_id, platform_user["user_id"],
    )
    await callback.answer("✅ Включена" if new_val else "❌ Выключена")
    await show_bot_feedback(callback, platform_user, bot_id)


@router.callback_query(F.data.startswith("bsf_target:"))
async def on_bsf_target(callback: CallbackQuery, platform_user: dict | None):
    if not platform_user:
        return
    bot_id = int(callback.data.split(":")[1])
    row = await db.fetchrow(
        "SELECT feedback_target FROM child_bots WHERE id=$1 AND owner_id=$2",
        bot_id, platform_user["user_id"],
    )
    current = row["feedback_target"] if row else "owner"
    new_target = "all" if current == "owner" else "owner"
    await db.execute(
        "UPDATE child_bots SET feedback_target=$1 WHERE id=$2 AND owner_id=$3",
        new_target, bot_id, platform_user["user_id"],
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
    await db.execute(
        "UPDATE child_bots SET feedback_lang=$1 WHERE id=$2 AND owner_id=$3",
        lang, bot_id, platform_user["user_id"],
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
):
    """
    Вызывается из child_bot_runner при входящем личном сообщении.
    Проверяет feedback_enabled на уровне площадки (bot_chats) ИЛИ бота (child_bots).
    Пересылает сообщение владельцу (или всем администраторам) через copy_message.
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

    # Кнопка «Ответить» — передаём child_bot_id, user_id И owner_id (для аутентификации)
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

    for recipient_id in set(recipients):
        try:
            if message.text:
                # Текстовое сообщение: объединяем уведомление + текст в одно сообщение
                full_text = f"{caption}\n\n{message.text}"
                await bot.send_message(recipient_id, full_text, parse_mode="HTML", reply_markup=reply_kb)
            else:
                # Медиа: подставляем напись с кнопкой прямо в caption
                extra = ("\n\n" + message.caption) if message.caption else ""
                await message.copy_to(
                    recipient_id,
                    caption=caption + extra,
                    parse_mode="HTML",
                    reply_markup=reply_kb,
                )
        except Exception as e:
            logger.debug(f"Feedback forward failed to {recipient_id}: {e}")
            # У стикеров нет caption — шлём уведомление отдельно
            try:
                await bot.send_message(recipient_id, caption, parse_mode="HTML", reply_markup=reply_kb)
                await message.copy_to(recipient_id)
            except Exception as e2:
                logger.debug(f"Feedback fallback also failed to {recipient_id}: {e2}")


# ── Обработка кнопки «Ответить» ────────────────────────────────
@router.callback_query(F.data.startswith("fb_reply:"))
async def on_fb_reply(callback: CallbackQuery, state: FSMContext):
    """Владелец/админ нажал «Ответить». Не требует platform_user — проверяем права через БД."""
    # Отвечаем СРАЗУ, чтобы Telegram не показывал часы загрузки
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

        # Меняем кнопку на статус ожидания (убираем кнопку из уведомления)
        waiting_text = (
            f"✉️ <b>Ответ на обратную связь</b>\n"
            f"От: {target_name} ({target_username})\n\n"
            f"⏳ <i>Ожидаем ответ...</i>"
        )
        try:
            await callback.message.edit_text(waiting_text, parse_mode="HTML")
        except Exception as edit_err:
            logger.warning(f"[FB_REPLY] Could not edit notification message: {edit_err}")

        await state.set_state(FeedbackFSM.waiting_for_reply)
        await state.update_data(
            child_bot_id=child_bot_id,
            target_user_id=target_user_id,
            target_name=target_name,
            target_username=target_username,
        )
        logger.info(f"[FB_REPLY] clicker={clicker_id} set reply state for user={target_user_id} via bot={child_bot_id}")

        # Отправляем явное сообщение-подсказку куда писать ответ
        name_display = f"{target_name} ({target_username})" if target_username else target_name
        await callback.message.answer(
            f"✉️ <b>Напишите ответ для {name_display}:</b>\n\n"
            f"Следующее сообщение, которое вы напишете сюда, будет отправлено пользователю в личку через дочернего бота.",
            parse_mode="HTML",
            reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                [InlineKeyboardButton(text="❌ Отмена", callback_data="fb_cancel_reply")],
            ]),
        )
        # Видимый тост — подтверждение нажатия
        await callback.answer(f"✅ Пишите ответ для {target_name} 👇")

    except Exception as e:
        logger.error(f"[FB_REPLY] Unexpected error: {e}", exc_info=True)
        await callback.answer(f"⚠️ Ошибка: {e}", show_alert=True)


# ── Обработка кнопки «Ответить» ────────────────────────────────
@router.callback_query(F.data == "fb_cancel_reply")
async def on_fb_cancel_reply(callback: CallbackQuery, state: FSMContext):
    await state.clear()
    try:
        await callback.message.edit_text("❌ Ответ отменён.")
    except Exception:
        pass
    await callback.answer()


# ── Обработка введённого ответа ────────────────────────────────
@router.message(FeedbackFSM.waiting_for_reply)
async def on_feedback_reply_text(message: Message, state: FSMContext):
    """Владелец ввёл текст ответа — отправляем через дочернего бота."""
    data           = await state.get_data()
    child_bot_id   = data.get("child_bot_id")
    target_user_id = data.get("target_user_id")
    target_name    = data.get("target_name", "пользователю")
    target_username = data.get("target_username", "")
    await state.clear()

    if not child_bot_id or not target_user_id:
        await message.answer("⚠️ Ошибка: данные для ответа потеряны.")
        return

    row = await db.fetchrow(
        "SELECT encrypted_token FROM child_bots WHERE id=$1",
        child_bot_id,
    )
    if not row:
        await message.answer("⚠️ Дочерний бот не найден.")
        return

    from services.security import decrypt_token
    token = decrypt_token(row["encrypted_token"])
    child_bot = Bot(token=token)
    name_display = f"{target_name} ({target_username})" if target_username else target_name
    try:
        await message.copy_to(target_user_id, bot=child_bot)
        await message.answer(
            f"✅ Ответ отправлен <b>{name_display}</b>.",
            parse_mode="HTML",
        )
        logger.info(f"[FEEDBACK REPLY] Sent reply to user {target_user_id} via bot {child_bot_id}")
    except Exception as e:
        await message.answer(
            f"⚠️ Не удалось отправить ответ: {e}\n\n"
            "Возможно, пользователь не писал дочернему боту (/start не нажал)."
        )
        logger.warning(f"[FEEDBACK REPLY] Failed: {e}")
    finally:
        await child_bot.session.close()
