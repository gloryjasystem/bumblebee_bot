"""
handlers/messages.py — Раздел «💬 Сообщения»:
  - Экран 3: меню сообщений канала (Капча, Приветствие, Прощание,
             Автоответчик, Печать, Реакции, Удаление сообщений)
  - Экран 4: настройки капчи (простая/рандомная/выкл + все тогглеры)
  - Экран 5: рандомная капча — выбор вида эмодзи
"""
import logging
from aiogram import Router, F
from aiogram.types import (
    Message, CallbackQuery, InlineKeyboardMarkup, InlineKeyboardButton
)
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup

import db.pool as db
from services.security import sanitize

logger = logging.getLogger(__name__)
router = Router()


class MessagesFSM(StatesGroup):
    waiting_for_captcha_text    = State()
    waiting_for_captcha_buttons = State()
    waiting_for_autoreply_kw    = State()
    waiting_for_autoreply_text  = State()


# ── Константы ──────────────────────────────────────────────────

# Наборы эмодзи для рандомной капчи (правильный всегда первый — перемешиваем при показе)
_EMOJI_SETS = [
    "🍕🍔🌭🌮",
    "🐶🐱🐭🐹",
    "⚽🏀🏈🎾",
    "🚗🚕🚙🚌",
    "🌍🌎🌏🗺",
]

# Стили кнопок капчи
_BUTTON_STYLES = ["1x1", "1x2", "2x2"]

# Таймер капчи (цикл)
_TIMER_CYCLE = [1, 2, 5, 10, 30]

# Авто-удаление сообщений (цикл, в минутах; 0 = выкл)
_DELETE_CYCLE = [0, 5, 10, 15, 30, 60]

# Реакции (выбор)
_REACTION_OPTIONS = ["👍", "❤️", "🔥", "🎉", "👏", "😍", "🤩", "💯"]


# ══════════════════════════════════════════════════════════════
# Экран 3: меню сообщений канала
# ══════════════════════════════════════════════════════════════

async def _show_ch_messages(callback: CallbackQuery, chat_id: int, owner_id: int):
    """Рендерит экран 3 — меню сообщений для канала."""
    ch = await db.fetchrow(
        "SELECT * FROM bot_chats WHERE owner_id=$1 AND chat_id=$2::bigint",
        owner_id, chat_id,
    )
    if not ch:
        await callback.answer("Площадка не найдена", show_alert=True)
        return

    captcha_type    = ch.get("captcha_type") or "off"
    typing_on       = ch.get("typing_action", False)
    reaction        = ch.get("reaction_emoji") or "👍"
    delete_min      = ch.get("auto_delete_min") or 0

    captcha_label = {"off": "🔒 Капча: выкл", "simple": "🔒 Капча: простая",
                     "random": "🔒 Капча: рандомная"}.get(captcha_type, "🔒 Капча")
    typing_label  = f"🖨 Печать: {'вкл' if typing_on else 'выкл'}"
    reaction_label = f"❤️ Реакции: {reaction}"
    delete_label  = f"🗑 Удаление: {'выкл' if delete_min == 0 else f'{delete_min} мин'}"

    await callback.message.edit_text(
        "<blockquote>"
        "🗑 <b>Удаление сообщений</b> — позволяет удалять отправленные ботом сообщения.\n\n"
        "🖨 <b>Печать</b> — бот имитирует написание текста при отправке сообщений.\n\n"
        "❤️ <b>Реакции</b> — бот будет ставить реакции на сообщения пользователей."
        "</blockquote>\n\n"
        "Выберите действие ⬇️",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text=captcha_label,  callback_data=f"ch_captcha:{chat_id}")],
            [
                InlineKeyboardButton(text="👋 Приветствие", callback_data=f"welcome_set:{chat_id}"),
                InlineKeyboardButton(text="🤚 Прощание",    callback_data=f"farewell_set:{chat_id}"),
            ],
            [InlineKeyboardButton(text="💬 Автоответчик",   callback_data=f"ch_autoreply:{chat_id}")],
            [
                InlineKeyboardButton(text=typing_label,   callback_data=f"ch_toggle_typing:{chat_id}"),
                InlineKeyboardButton(text=reaction_label, callback_data=f"ch_reactions:{chat_id}"),
            ],
            [InlineKeyboardButton(text=delete_label, callback_data=f"ch_delete_toggle:{chat_id}")],
            [InlineKeyboardButton(text="◀️ Назад",    callback_data=f"ch_back_to_channel:{chat_id}")],
        ]),
    )
    await callback.answer()


@router.callback_query(F.data.startswith("ch_messages:"))
async def on_ch_messages(callback: CallbackQuery, platform_user: dict | None):
    if not platform_user:
        return
    chat_id = int(callback.data.split(":")[1])
    await _show_ch_messages(callback, chat_id, platform_user["user_id"])


# ── Toggle: Печать ─────────────────────────────────────────────

@router.callback_query(F.data.startswith("ch_toggle_typing:"))
async def on_ch_toggle_typing(callback: CallbackQuery, platform_user: dict | None):
    if not platform_user:
        return
    chat_id  = int(callback.data.split(":")[1])
    owner_id = platform_user["user_id"]
    ch = await db.fetchrow(
        "SELECT typing_action FROM bot_chats WHERE owner_id=$1 AND chat_id=$2::bigint",
        owner_id, chat_id,
    )
    new_val = not bool(ch["typing_action"] if ch else False)
    await db.execute(
        "UPDATE bot_chats SET typing_action=$1 WHERE owner_id=$2 AND chat_id=$3::bigint",
        new_val, owner_id, chat_id,
    )
    await callback.answer("🖨 Печать: " + ("вкл" if new_val else "выкл"))
    await _show_ch_messages(callback, chat_id, owner_id)


# ── Toggle: Реакции ────────────────────────────────────────────

@router.callback_query(F.data.startswith("ch_reactions:"))
async def on_ch_reactions(callback: CallbackQuery, platform_user: dict | None):
    if not platform_user:
        return
    chat_id  = int(callback.data.split(":")[1])
    owner_id = platform_user["user_id"]
    ch = await db.fetchrow(
        "SELECT reaction_emoji FROM bot_chats WHERE owner_id=$1 AND chat_id=$2::bigint",
        owner_id, chat_id,
    )
    current = (ch["reaction_emoji"] if ch else None) or "👍"
    buttons = [[InlineKeyboardButton(
        text=e + (" ✅" if e == current else ""),
        callback_data=f"ch_set_reaction:{chat_id}:{e}",
    )] for e in _REACTION_OPTIONS]
    buttons.append([InlineKeyboardButton(text="◀️ Назад", callback_data=f"ch_messages:{chat_id}")])
    await callback.message.edit_text(
        "❤️ <b>Реакции</b>\n\nВыберите реакцию, которую бот будет ставить на сообщения:",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=buttons),
    )
    await callback.answer()


@router.callback_query(F.data.startswith("ch_set_reaction:"))
async def on_ch_set_reaction(callback: CallbackQuery, platform_user: dict | None):
    if not platform_user:
        return
    parts    = callback.data.split(":")
    chat_id  = int(parts[1])
    emoji    = parts[2]
    owner_id = platform_user["user_id"]
    await db.execute(
        "UPDATE bot_chats SET reaction_emoji=$1 WHERE owner_id=$2 AND chat_id=$3::bigint",
        emoji, owner_id, chat_id,
    )
    await callback.answer(f"Реакция: {emoji}")
    callback.data = f"ch_reactions:{chat_id}"
    await on_ch_reactions(callback, platform_user)


# ── Toggle: Авто-удаление ──────────────────────────────────────

@router.callback_query(F.data.startswith("ch_delete_toggle:"))
async def on_ch_delete_toggle(callback: CallbackQuery, platform_user: dict | None):
    if not platform_user:
        return
    chat_id  = int(callback.data.split(":")[1])
    owner_id = platform_user["user_id"]
    ch = await db.fetchrow(
        "SELECT auto_delete_min FROM bot_chats WHERE owner_id=$1 AND chat_id=$2::bigint",
        owner_id, chat_id,
    )
    current = int(ch["auto_delete_min"] if ch and ch["auto_delete_min"] else 0)
    try:
        idx = _DELETE_CYCLE.index(current)
    except ValueError:
        idx = 0
    new_val = _DELETE_CYCLE[(idx + 1) % len(_DELETE_CYCLE)]
    await db.execute(
        "UPDATE bot_chats SET auto_delete_min=$1 WHERE owner_id=$2 AND chat_id=$3::bigint",
        new_val, owner_id, chat_id,
    )
    label = "выкл" if new_val == 0 else f"{new_val} мин"
    await callback.answer(f"🗑 Удаление: {label}")
    await _show_ch_messages(callback, chat_id, owner_id)


# ══════════════════════════════════════════════════════════════
# Экран 4: Настройки капчи
# ══════════════════════════════════════════════════════════════

async def _show_captcha(callback: CallbackQuery, chat_id: int, owner_id: int):
    ch = await db.fetchrow(
        "SELECT * FROM bot_chats WHERE owner_id=$1 AND chat_id=$2::bigint",
        owner_id, chat_id,
    )
    if not ch:
        await callback.answer("Площадка не найдена", show_alert=True)
        return

    ctype       = ch.get("captcha_type") or "off"
    timer       = int(ch["captcha_timer_min"] if ch.get("captcha_timer_min") else 1)
    lang_on     = bool(ch.get("captcha_lang", False))
    anim_on     = bool(ch.get("captcha_animation", False))
    btn_style   = ch.get("captcha_button_style") or "1x1"
    greet_on    = bool(ch.get("captcha_greet", False))
    accept_now  = bool(ch.get("captcha_accept_now", False))
    accept_all  = bool(ch.get("captcha_accept_all", False))
    emoji_set   = ch.get("captcha_emoji_set") or _EMOJI_SETS[0]

    type_labels = {"off": "🔒 Капча: выкл", "simple": "🔒 Капча: простая",
                   "random": "🔒 Капча: рандомная"}
    type_label = type_labels.get(ctype, "🔒 Капча")

    def yn(v): return "вкл" if v else "выкл"

    info = (
        "<blockquote>"
        "● <b>Простая:</b> пользователю достаточно нажать на любую кнопку.\n\n"
        "● <b>Рандомная:</b> пользователю необходимо нажать на верную кнопку.\n\n"
        "🤚 <b>Приветствовать:</b> позволяет отправлять пользователю приветствие "
        "сразу после решения капчи.\n\n"
        "⬇️ <b>Принимать сразу:</b> позволяет принимать заявки сразу после решения капчи.\n\n"
        "✅ <b>Принимать всех:</b> если данная опция включена, то даже если пользователь "
        "проигнорировал капчу, его заявка будет обработана."
        "</blockquote>\n\n"
        "ⓘ Капча отправляется перед приветственным сообщением."
    )

    # Базовые кнопки (всегда)
    buttons = [[InlineKeyboardButton(text=type_label, callback_data=f"ch_captcha_type:{chat_id}")]]

    if ctype != "off":
        # Рандомная: показываем выбор вида
        if ctype == "random":
            buttons.append([InlineKeyboardButton(
                text=f"👁 Вид: {emoji_set}",
                callback_data=f"ch_captcha_emoji:{chat_id}",
            )])

        buttons += [
            [
                InlineKeyboardButton(text="✏️ Текст капчи",  callback_data=f"ch_captcha_text:{chat_id}"),
                InlineKeyboardButton(text="✏️ Текст кнопок", callback_data=f"ch_captcha_btns:{chat_id}"),
            ],
            [
                InlineKeyboardButton(text=f"🌐 Язык: {yn(lang_on)}",  callback_data=f"ch_cap_toggle:{chat_id}:lang"),
                InlineKeyboardButton(text=f"🎬 Анимация: {yn(anim_on)}", callback_data=f"ch_cap_toggle:{chat_id}:anim"),
            ],
            [
                InlineKeyboardButton(text=f"🎲 Кнопки: {btn_style}", callback_data=f"ch_cap_toggle:{chat_id}:btn_style"),
                InlineKeyboardButton(text=f"⏱ Таймер: {timer} мин",  callback_data=f"ch_cap_toggle:{chat_id}:timer"),
            ],
            [InlineKeyboardButton(text=f"📦 Приветствовать: {yn(greet_on)}",  callback_data=f"ch_cap_toggle:{chat_id}:greet")],
            [InlineKeyboardButton(text=f"📲 Принимать сразу: {yn(accept_now)}", callback_data=f"ch_cap_toggle:{chat_id}:accept_now")],
            [InlineKeyboardButton(text=f"✅ Принимать всех: {yn(accept_all)}",  callback_data=f"ch_cap_toggle:{chat_id}:accept_all")],
        ]

    buttons.append([InlineKeyboardButton(text="◀️ Назад", callback_data=f"ch_messages:{chat_id}")])

    await callback.message.edit_text(
        info,
        parse_mode="HTML",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=buttons),
    )
    await callback.answer()


@router.callback_query(F.data.startswith("ch_captcha:"))
async def on_ch_captcha(callback: CallbackQuery, platform_user: dict | None):
    if not platform_user:
        return
    chat_id = int(callback.data.split(":")[1])
    await _show_captcha(callback, chat_id, platform_user["user_id"])


# ── Toggle: Тип капчи (off → simple → random → off → ...) ──────

@router.callback_query(F.data.startswith("ch_captcha_type:"))
async def on_ch_captcha_type(callback: CallbackQuery, platform_user: dict | None):
    if not platform_user:
        return
    chat_id  = int(callback.data.split(":")[1])
    owner_id = platform_user["user_id"]
    ch = await db.fetchrow(
        "SELECT captcha_type FROM bot_chats WHERE owner_id=$1 AND chat_id=$2::bigint",
        owner_id, chat_id,
    )
    cycle = ["off", "simple", "random"]
    ctype = (ch["captcha_type"] if ch and ch.get("captcha_type") else "off")
    try:
        idx = cycle.index(ctype)
    except ValueError:
        idx = 0
    new_type = cycle[(idx + 1) % len(cycle)]
    await db.execute(
        "UPDATE bot_chats SET captcha_type=$1 WHERE owner_id=$2 AND chat_id=$3::bigint",
        new_type, owner_id, chat_id,
    )
    labels = {"off": "Капча: выкл", "simple": "Капча: простая", "random": "Капча: рандомная"}
    await callback.answer(labels.get(new_type, new_type))
    await _show_captcha(callback, chat_id, owner_id)


# ── Toggle: всё остальное в капче ─────────────────────────────

@router.callback_query(F.data.startswith("ch_cap_toggle:"))
async def on_ch_cap_toggle(callback: CallbackQuery, platform_user: dict | None):
    if not platform_user:
        return
    parts    = callback.data.split(":")
    chat_id  = int(parts[1])
    key      = parts[2]
    owner_id = platform_user["user_id"]

    ch = await db.fetchrow(
        "SELECT * FROM bot_chats WHERE owner_id=$1 AND chat_id=$2::bigint",
        owner_id, chat_id,
    )
    if not ch:
        return

    if key == "lang":
        new_val = not bool(ch.get("captcha_lang", False))
        await db.execute("UPDATE bot_chats SET captcha_lang=$1 WHERE owner_id=$2 AND chat_id=$3::bigint",
                         new_val, owner_id, chat_id)
        await callback.answer("Язык: " + ("вкл" if new_val else "выкл"))

    elif key == "anim":
        new_val = not bool(ch.get("captcha_animation", False))
        await db.execute("UPDATE bot_chats SET captcha_animation=$1 WHERE owner_id=$2 AND chat_id=$3::bigint",
                         new_val, owner_id, chat_id)
        await callback.answer("Анимация: " + ("вкл" if new_val else "выкл"))

    elif key == "btn_style":
        cur = ch.get("captcha_button_style") or "1x1"
        try:
            idx = _BUTTON_STYLES.index(cur)
        except ValueError:
            idx = 0
        new_val = _BUTTON_STYLES[(idx + 1) % len(_BUTTON_STYLES)]
        await db.execute("UPDATE bot_chats SET captcha_button_style=$1 WHERE owner_id=$2 AND chat_id=$3::bigint",
                         new_val, owner_id, chat_id)
        await callback.answer(f"Кнопки: {new_val}")

    elif key == "timer":
        cur = int(ch.get("captcha_timer_min") or 1)
        try:
            idx = _TIMER_CYCLE.index(cur)
        except ValueError:
            idx = 0
        new_val = _TIMER_CYCLE[(idx + 1) % len(_TIMER_CYCLE)]
        await db.execute("UPDATE bot_chats SET captcha_timer_min=$1 WHERE owner_id=$2 AND chat_id=$3::bigint",
                         new_val, owner_id, chat_id)
        await callback.answer(f"Таймер: {new_val} мин")

    elif key == "greet":
        new_val = not bool(ch.get("captcha_greet", False))
        await db.execute("UPDATE bot_chats SET captcha_greet=$1 WHERE owner_id=$2 AND chat_id=$3::bigint",
                         new_val, owner_id, chat_id)
        await callback.answer("Приветствовать: " + ("вкл" if new_val else "выкл"))

    elif key == "accept_now":
        new_val = not bool(ch.get("captcha_accept_now", False))
        await db.execute("UPDATE bot_chats SET captcha_accept_now=$1 WHERE owner_id=$2 AND chat_id=$3::bigint",
                         new_val, owner_id, chat_id)
        await callback.answer("Принимать сразу: " + ("вкл" if new_val else "выкл"))

    elif key == "accept_all":
        new_val = not bool(ch.get("captcha_accept_all", False))
        await db.execute("UPDATE bot_chats SET captcha_accept_all=$1 WHERE owner_id=$2 AND chat_id=$3::bigint",
                         new_val, owner_id, chat_id)
        await callback.answer("Принимать всех: " + ("вкл" if new_val else "выкл"))

    await _show_captcha(callback, chat_id, owner_id)


# ── Выбор набора эмодзи (рандомная капча) ──────────────────────

@router.callback_query(F.data.startswith("ch_captcha_emoji:"))
async def on_ch_captcha_emoji(callback: CallbackQuery, platform_user: dict | None):
    if not platform_user:
        return
    chat_id  = int(callback.data.split(":")[1])
    owner_id = platform_user["user_id"]
    ch = await db.fetchrow(
        "SELECT captcha_emoji_set FROM bot_chats WHERE owner_id=$1 AND chat_id=$2::bigint",
        owner_id, chat_id,
    )
    current = (ch["captcha_emoji_set"] if ch else None) or _EMOJI_SETS[0]

    buttons = [[InlineKeyboardButton(
        text=es + (" ✅" if es == current else ""),
        callback_data=f"ch_cap_set_emoji:{chat_id}:{i}",
    )] for i, es in enumerate(_EMOJI_SETS)]
    buttons.append([InlineKeyboardButton(text="◀️ Назад", callback_data=f"ch_captcha:{chat_id}")])

    await callback.message.edit_text(
        "🎲 <b>Вид рандомной капчи</b>\n\nВыберите набор эмодзи:",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=buttons),
    )
    await callback.answer()


@router.callback_query(F.data.startswith("ch_cap_set_emoji:"))
async def on_ch_cap_set_emoji(callback: CallbackQuery, platform_user: dict | None):
    if not platform_user:
        return
    parts    = callback.data.split(":")
    chat_id  = int(parts[1])
    idx      = int(parts[2])
    owner_id = platform_user["user_id"]
    es       = _EMOJI_SETS[idx % len(_EMOJI_SETS)]
    await db.execute(
        "UPDATE bot_chats SET captcha_emoji_set=$1 WHERE owner_id=$2 AND chat_id=$3::bigint",
        es, owner_id, chat_id,
    )
    await callback.answer(f"Вид: {es}")
    callback.data = f"ch_captcha:{chat_id}"
    await on_ch_captcha(callback, platform_user)


# ── FSM: Текст капчи ───────────────────────────────────────────

@router.callback_query(F.data.startswith("ch_captcha_text:"))
async def on_ch_captcha_text(callback: CallbackQuery, state: FSMContext, platform_user: dict | None):
    if not platform_user:
        return
    chat_id = int(callback.data.split(":")[1])
    await state.set_state(MessagesFSM.waiting_for_captcha_text)
    await state.update_data(chat_id=chat_id, owner_id=platform_user["user_id"])
    await callback.message.edit_text(
        "✏️ <b>Текст капчи</b>\n\n"
        "Отправьте текст, который будет показан пользователю перед входом.\n\n"
        "Переменные: <code>{name}</code>, <code>{chat}</code>",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="◀️ Отмена", callback_data=f"ch_captcha:{chat_id}")],
        ]),
    )
    await callback.answer()


@router.message(MessagesFSM.waiting_for_captcha_text)
async def on_captcha_text_input(message: Message, state: FSMContext):
    data     = await state.get_data()
    chat_id  = data["chat_id"]
    owner_id = data["owner_id"]
    text     = sanitize(message.text or "", max_len=512)
    await db.execute(
        "UPDATE bot_chats SET captcha_text=$1 WHERE owner_id=$2 AND chat_id=$3::bigint",
        text, owner_id, chat_id,
    )
    await state.clear()
    await message.answer(
        "✅ Текст капчи сохранён.",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="◀️ К капче", callback_data=f"ch_captcha:{chat_id}")],
        ]),
    )


# ── FSM: Текст кнопок капчи ────────────────────────────────────

@router.callback_query(F.data.startswith("ch_captcha_btns:"))
async def on_ch_captcha_btns(callback: CallbackQuery, state: FSMContext, platform_user: dict | None):
    if not platform_user:
        return
    chat_id = int(callback.data.split(":")[1])
    await state.set_state(MessagesFSM.waiting_for_captcha_buttons)
    await state.update_data(chat_id=chat_id, owner_id=platform_user["user_id"])
    await callback.message.edit_text(
        "✏️ <b>Текст кнопок капчи</b>\n\n"
        "Отправьте текст кнопок через запятую.\n"
        "Пример: <code>Я не робот, Пропустить, Войти</code>",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="◀️ Отмена", callback_data=f"ch_captcha:{chat_id}")],
        ]),
    )
    await callback.answer()


@router.message(MessagesFSM.waiting_for_captcha_buttons)
async def on_captcha_btns_input(message: Message, state: FSMContext):
    data     = await state.get_data()
    chat_id  = data["chat_id"]
    owner_id = data["owner_id"]
    raw      = sanitize(message.text or "", max_len=256)
    await db.execute(
        "UPDATE bot_chats SET captcha_buttons_raw=$1 WHERE owner_id=$2 AND chat_id=$3::bigint",
        raw, owner_id, chat_id,
    )
    await state.clear()
    await message.answer(
        "✅ Текст кнопок сохранён.",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="◀️ К капче", callback_data=f"ch_captcha:{chat_id}")],
        ]),
    )


# ══════════════════════════════════════════════════════════════
# Автоответчик
# ══════════════════════════════════════════════════════════════

@router.callback_query(F.data.startswith("ch_autoreply:"))
async def on_ch_autoreply(callback: CallbackQuery, platform_user: dict | None):
    if not platform_user:
        return
    chat_id  = int(callback.data.split(":")[1])
    owner_id = platform_user["user_id"]

    rows = await db.fetch(
        "SELECT id, keyword, reply_text FROM autoreplies WHERE owner_id=$1 AND chat_id=$2::bigint LIMIT 20",
        owner_id, chat_id,
    )

    buttons = []
    for r in rows:
        buttons.append([InlineKeyboardButton(
            text=f"💬 {r['keyword'][:25]}",
            callback_data=f"ch_ar_del:{chat_id}:{r['id']}",
        )])
    buttons.append([InlineKeyboardButton(text="➕ Добавить ответ", callback_data=f"ch_ar_add:{chat_id}")])
    buttons.append([InlineKeyboardButton(text="◀️ Назад", callback_data=f"ch_messages:{chat_id}")])

    empty_note = "" if rows else "\n\n<i>Нет настроенных автоответов.</i>"
    await callback.message.edit_text(
        "💬 <b>Автоответчик</b>\n\n"
        "Бот автоматически отвечает на сообщения, содержащие заданные слова.\n"
        "Нажмите на ответ, чтобы удалить его." + empty_note,
        reply_markup=InlineKeyboardMarkup(inline_keyboard=buttons),
    )
    await callback.answer()


@router.callback_query(F.data.startswith("ch_ar_add:"))
async def on_ch_ar_add(callback: CallbackQuery, state: FSMContext, platform_user: dict | None):
    if not platform_user:
        return
    chat_id = int(callback.data.split(":")[1])
    await state.set_state(MessagesFSM.waiting_for_autoreply_kw)
    await state.update_data(chat_id=chat_id, owner_id=platform_user["user_id"])
    await callback.message.edit_text(
        "💬 <b>Автоответчик — ключевое слово</b>\n\n"
        "Отправьте слово или фразу, на которую бот будет реагировать:",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="◀️ Отмена", callback_data=f"ch_autoreply:{chat_id}")],
        ]),
    )
    await callback.answer()


@router.message(MessagesFSM.waiting_for_autoreply_kw)
async def on_ar_kw_input(message: Message, state: FSMContext):
    kw = sanitize(message.text or "", max_len=64)
    await state.update_data(keyword=kw)
    await state.set_state(MessagesFSM.waiting_for_autoreply_text)
    data = await state.get_data()
    await message.answer(
        f"✅ Ключевое слово: <code>{kw}</code>\n\nТеперь отправьте текст ответа:",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="◀️ Отмена", callback_data=f"ch_autoreply:{data['chat_id']}")],
        ]),
    )


@router.message(MessagesFSM.waiting_for_autoreply_text)
async def on_ar_text_input(message: Message, state: FSMContext):
    data     = await state.get_data()
    chat_id  = data["chat_id"]
    owner_id = data["owner_id"]
    keyword  = data.get("keyword", "")
    reply    = sanitize(message.text or "", max_len=1024)

    await db.execute(
        """INSERT INTO autoreplies (owner_id, chat_id, keyword, reply_text)
           VALUES ($1, $2, $3, $4)
           ON CONFLICT (owner_id, chat_id, keyword) DO UPDATE SET reply_text=EXCLUDED.reply_text""",
        owner_id, chat_id, keyword, reply,
    )
    await state.clear()
    await message.answer(
        f"✅ Автоответ <code>{keyword}</code> → сохранён.",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="◀️ К автоответчику", callback_data=f"ch_autoreply:{chat_id}")],
        ]),
    )


@router.callback_query(F.data.startswith("ch_ar_del:"))
async def on_ch_ar_del(callback: CallbackQuery, platform_user: dict | None):
    if not platform_user:
        return
    parts   = callback.data.split(":")
    chat_id = int(parts[1])
    ar_id   = int(parts[2])
    await db.execute(
        "DELETE FROM autoreplies WHERE id=$1 AND owner_id=$2",
        ar_id, platform_user["user_id"],
    )
    await callback.answer("🗑 Удалено")
    callback.data = f"ch_autoreply:{chat_id}"
    await on_ch_autoreply(callback, platform_user)


# ── Кнопка «Назад» из экрана ch_messages ──────────────────────

@router.callback_query(F.data.startswith("ch_back_to_channel:"))
async def on_ch_back_to_channel(callback: CallbackQuery, platform_user: dict | None):
    """Назад из меню Сообщений → главный экран бота (bot_settings)."""
    if not platform_user:
        return
    chat_id  = int(callback.data.split(":")[1])
    owner_id = platform_user["user_id"]

    #找к какому боту принадлежит площадка
    row = await db.fetchrow(
        "SELECT child_bot_id FROM bot_chats WHERE owner_id=$1 AND chat_id=$2::bigint",
        owner_id, chat_id,
    )
    child_bot_id = row["child_bot_id"] if row else None

    if child_bot_id:
        # Перенаправляем к главному экрану бота (статистика + кнопки)
        callback.data = f"bot_settings:{child_bot_id}"
    else:
        # Если площадка без привязки к боту — в список каналов
        callback.data = "menu:channels"

    from handlers.channels import on_bot_settings, on_channels_menu
    if child_bot_id:
        await on_bot_settings(callback, platform_user)
    else:
        await on_channels_menu(callback, platform_user)
