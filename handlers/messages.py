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
    waiting_for_captcha_text           = State()
    waiting_for_captcha_buttons        = State()
    waiting_for_autoreply_kw           = State()
    waiting_for_autoreply_text         = State()
    waiting_for_general_reply_text     = State()
    waiting_for_general_reply_buttons  = State()


# ── Константы ──────────────────────────────────────────────────

# Наборы эмодзи для рандомной капчи (правильный всегда первый — перемешиваем при показе)
_EMOJI_SETS = [
    "🍕🍔🌭🌮",
    "🐶🐱🐭🐹",
    "⚽🏀🏈🎾",
    "🚗🚕🚙🚌",
    "🌍🌎🌏🗺",
]

# Тип кнопок капчи (размещение)
_BUTTON_PLACEMENTS = ["inline", "reply"]

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

    captcha_type   = ch.get("captcha_type") or "off"
    typing_on      = ch.get("typing_action", False)
    reaction       = ch.get("reaction_emoji") or "👍"
    delete_min     = ch.get("auto_delete_min") or 0

    # Куда вернуться: главный экран бота
    child_bot_id = ch.get("child_bot_id")
    back_cb = f"bot_settings:{child_bot_id}" if child_bot_id else "menu:channels"

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
            [InlineKeyboardButton(text="◀️ Назад",    callback_data=back_cb)],
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

    ctype         = ch.get("captcha_type") or "off"
    timer         = int(ch["captcha_timer_min"] if ch.get("captcha_timer_min") else 1)
    anim_on       = bool(ch.get("captcha_animation", False))
    btn_placement = ch.get("captcha_button_style") or "inline"  # 'inline' / 'reply'
    greet_on      = bool(ch.get("captcha_greet", False))
    accept_now    = bool(ch.get("captcha_accept_now", False))
    accept_all    = bool(ch.get("captcha_accept_all", False))
    emoji_set     = ch.get("captcha_emoji_set") or _EMOJI_SETS[0]

    type_labels = {"off": "🔒 Капча: выкл", "simple": "🔒 Капча: простая",
                   "random": "🔒 Капча: рандомная"}
    type_label = type_labels.get(ctype, "🔒 Капча")

    # Иконки зависящие от состояния
    anim_icon     = "📸" if anim_on else "📷"
    greet_icon    = "📩" if greet_on else "✉️"
    accept_icon   = "🔋" if accept_now else "🪫"
    accept_a_icon = "✅" if accept_all else "❎"
    btn_label     = "⌨️ Reply" if btn_placement == "reply" else "📩 Inline"

    info = (
        "<blockquote>"
        "● <b>Простая:</b> пользователю достаточно нажать на любую кнопку.\n\n"
        "● <b>Рандомная:</b> пользователю необходимо нажать на верную кнопку.\n\n"
        "📩 <b>Приветствовать:</b> позволяет отправлять пользователю приветствие "
        "сразу после решения капчи.\n\n"
        "🔋 <b>Принимать сразу:</b> позволяет принимать заявки сразу после решения капчи.\n\n"
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
                InlineKeyboardButton(
                    text=f"🔄 Сброс капчи",
                    callback_data=f"ch_cap_reset:{chat_id}",
                ),
                InlineKeyboardButton(
                    text=f"{anim_icon} Анимация: {'вкл' if anim_on else 'выкл'}",
                    callback_data=f"ch_cap_toggle:{chat_id}:anim",
                ),
            ],
            [
                InlineKeyboardButton(
                    text=f"🎛 Кнопки: {btn_label}",
                    callback_data=f"ch_cap_toggle:{chat_id}:btn_type",
                ),
                InlineKeyboardButton(text=f"⏱ Таймер: {timer} мин", callback_data=f"ch_cap_toggle:{chat_id}:timer"),
            ],
            [InlineKeyboardButton(
                text=f"{greet_icon} Приветствовать: {'вкл' if greet_on else 'выкл'}",
                callback_data=f"ch_cap_toggle:{chat_id}:greet",
            )],
            [InlineKeyboardButton(
                text=f"{accept_icon} Принимать сразу: {'вкл' if accept_now else 'выкл'}",
                callback_data=f"ch_cap_toggle:{chat_id}:accept_now",
            )],
            [InlineKeyboardButton(
                text=f"{accept_a_icon} Принимать всех: {'вкл' if accept_all else 'выкл'}",
                callback_data=f"ch_cap_toggle:{chat_id}:accept_all",
            )],
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


# ── Toggle: anim / timer / greet / accept_now / accept_all ────

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

    if key == "anim":
        new_val = not bool(ch.get("captcha_animation", False))
        await db.execute("UPDATE bot_chats SET captcha_animation=$1 WHERE owner_id=$2 AND chat_id=$3::bigint",
                         new_val, owner_id, chat_id)
        await callback.answer("Анимация: " + ("вкл" if new_val else "выкл"))

    elif key == "btn_type":
        cur = ch.get("captcha_button_style") or "inline"
        new_val = "reply" if cur == "inline" else "inline"
        await db.execute("UPDATE bot_chats SET captcha_button_style=$1 WHERE owner_id=$2 AND chat_id=$3::bigint",
                         new_val, owner_id, chat_id)
        labels = {"inline": "📩 Inline", "reply": "⌨️ Reply"}
        await callback.answer(f"Кнопки: {labels[new_val]}")

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


# ── Сброс капчи: подтверждение ─────────────────────────────────

@router.callback_query(F.data.startswith("ch_cap_reset:"))
async def on_ch_cap_reset(callback: CallbackQuery, platform_user: dict | None):
    if not platform_user:
        return
    chat_id = int(callback.data.split(":")[1])
    await callback.message.edit_text(
        "🔄 <b>Сброс капчи</b>\n\n"
        "Это действие сбросит <u>текст капчи</u>, <u>текст кнопок</u> и <u>медиа</u> "
        "к стандартным значениям.\n\n"
        "Настройки (тип, таймер, переключатели) <b>не изменятся</b>.\n\n"
        "Подтвердить сброс?",
        parse_mode="HTML",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="✅ Подтвердить", callback_data=f"ch_cap_reset_ok:{chat_id}")],
            [InlineKeyboardButton(text="◀️ Отмена",     callback_data=f"ch_captcha:{chat_id}")],
        ]),
    )
    await callback.answer()


@router.callback_query(F.data.startswith("ch_cap_reset_ok:"))
async def on_ch_cap_reset_ok(callback: CallbackQuery, platform_user: dict | None):
    if not platform_user:
        return
    chat_id  = int(callback.data.split(":")[1])
    owner_id = platform_user["user_id"]
    await db.execute(
        """UPDATE bot_chats
           SET captcha_text=NULL, captcha_buttons_raw=NULL, captcha_media=NULL,
               captcha_emoji_set='🍕🍔🌭🌮'
           WHERE owner_id=$1 AND chat_id=$2::bigint""",
        owner_id, chat_id,
    )
    await callback.answer("✅ Капча сброшена")
    callback.data = f"ch_captcha:{chat_id}"
    await on_ch_captcha(callback, platform_user)


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
    chat_id  = int(callback.data.split(":")[1])
    ch = await db.fetchrow(
        "SELECT captcha_type FROM bot_chats WHERE owner_id=$1 AND chat_id=$2::bigint",
        platform_user["user_id"], chat_id,
    )
    ctype_label = {"simple": "простой", "random": "рандомной"}.get(
        (ch.get("captcha_type") or "simple") if ch else "simple", "простой"
    )
    await state.set_state(MessagesFSM.waiting_for_captcha_text)
    await state.update_data(chat_id=chat_id, owner_id=platform_user["user_id"])
    await callback.message.edit_text(
        f"Пришлите сообщение для <u>{ctype_label} капчи</u>.\n\n"
        "<b>Переменные:</b>\n"
        "├ Имя: <code>{name}</code>\n"
        "├ ФИО: <code>{allname}</code>\n"
        "├ Юзер: <code>{username}</code>\n"
        "├ Площадка: <code>{chat}</code>\n"
        "└ Текущая дата: <code>{day}</code>\n\n"
        "ℹ️ Можно прикрепить медиа.",
        parse_mode="HTML",
    )
    await callback.answer()


@router.message(MessagesFSM.waiting_for_captcha_text)
async def on_captcha_text_input(message: Message, state: FSMContext):
    data     = await state.get_data()
    chat_id  = data["chat_id"]
    owner_id = data["owner_id"]

    # Поддержка текста и/или фото
    if message.photo:
        text     = sanitize(message.caption or "", max_len=1024)
        media_id = message.photo[-1].file_id   # берём наибольшее разрешение
    else:
        text     = sanitize(message.text or "", max_len=1024)
        media_id = None

    await db.execute(
        "UPDATE bot_chats SET captcha_text=$1, captcha_media=$2 WHERE owner_id=$3 AND chat_id=$4::bigint",
        text, media_id, owner_id, chat_id,
    )
    await state.clear()
    await message.answer(
        "Сообщение установлено",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="↩ В меню капчи", callback_data=f"ch_captcha:{chat_id}")],
        ]),
    )


# ── FSM: Текст кнопок капчи ────────────────────────────────────

@router.callback_query(F.data.startswith("ch_captcha_btns:"))
async def on_ch_captcha_btns(callback: CallbackQuery, state: FSMContext, platform_user: dict | None):
    if not platform_user:
        return
    chat_id  = int(callback.data.split(":")[1])
    ch = await db.fetchrow(
        "SELECT captcha_type FROM bot_chats WHERE owner_id=$1 AND chat_id=$2::bigint",
        platform_user["user_id"], chat_id,
    )
    ctypes_map = {"simple": "Простая", "random": "Рандомная"}
    ctype_label = ctypes_map.get((ch.get("captcha_type") or "simple") if ch else "simple", "Простая")
    await state.set_state(MessagesFSM.waiting_for_captcha_buttons)
    await state.update_data(chat_id=chat_id, owner_id=platform_user["user_id"])
    await callback.message.edit_text(
        f"<u>{ctype_label} капча</u>:\n\n"
        "🔤 Текст кнопок\n\n"
        "<blockquote>Цвет:\n"
        "🟦 - Синий\n"
        "🟩 - Зелёный\n"
        "🟥 - Красный\n\n"
        "Пример: 🟩 Я не робот</blockquote>\n\n"
        "➡ Пришлите названия для <u>кнопок капчи</u>:",
        parse_mode="HTML",
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
        "Кнопки успешно установлены!",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="↩ Вернуться в капчу", callback_data=f"ch_captcha:{chat_id}")],
        ]),
    )


# ══════════════════════════════════════════════════════════════
# Автоответчик
# ══════════════════════════════════════════════════════════════

async def _show_autoreply(callback: CallbackQuery, chat_id: int, owner_id: int):
    """Рендерит главный экран автоответчика."""
    ch = await db.fetchrow(
        "SELECT general_reply_enabled, general_reply_text FROM bot_chats "
        "WHERE owner_id=$1 AND chat_id=$2::bigint",
        owner_id, chat_id,
    )
    general_on = bool(ch["general_reply_enabled"]) if ch else False

    buttons = []

    if general_on:
        # Общий ответ включён — только toggle
        buttons.append([InlineKeyboardButton(
            text="Общий ответ: вкл",
            callback_data=f"ch_ar_toggle_global:{chat_id}",
        )])
    else:
        # Список keyword-ответов
        rows = await db.fetch(
            "SELECT id, keyword FROM autoreplies "
            "WHERE owner_id=$1 AND chat_id=$2::bigint LIMIT 20",
            owner_id, chat_id,
        )
        for r in rows:
            buttons.append([InlineKeyboardButton(
                text=f"💬 {r['keyword'][:25]}",
                callback_data=f"ch_ar_del:{chat_id}:{r['id']}",
            )])
        buttons.append([InlineKeyboardButton(
            text="Общий ответ: выкл",
            callback_data=f"ch_ar_toggle_global:{chat_id}",
        )])
        buttons.append([InlineKeyboardButton(
            text="+ Добавить ответ",
            callback_data=f"ch_ar_add:{chat_id}",
        )])

    buttons.append([InlineKeyboardButton(text="◀️ Назад", callback_data=f"ch_messages:{chat_id}")])

    await callback.message.edit_text(
        "<blockquote>Вы можете установить <b>автоматические ответы</b> бота на "
        "любой текст или команду от пользователей.</blockquote>\n\n"
        "Выберите действие ⬇️",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=buttons),
    )
    await callback.answer()


@router.callback_query(F.data.startswith("ch_autoreply:"))
async def on_ch_autoreply(callback: CallbackQuery, platform_user: dict | None):
    if not platform_user:
        return
    chat_id = int(callback.data.split(":")[1])
    await _show_autoreply(callback, chat_id, platform_user["user_id"])


# ── Панель управления общим ответом ───────────────────────────

async def _show_global_mgmt(message, chat_id: int, owner_id: int):
    """Показывает панель управления общим ответом (2 сообщения: эхо + управление)."""
    ch = await db.fetchrow(
        "SELECT general_reply_text, general_reply_media, general_reply_media_type, "
        "general_reply_preview, general_reply_buttons FROM bot_chats "
        "WHERE owner_id=$1 AND chat_id=$2::bigint",
        owner_id, chat_id,
    )
    text       = (ch["general_reply_text"] or "") if ch else ""
    media_id   = (ch["general_reply_media"] or "") if ch else ""
    media_type = (ch["general_reply_media_type"] or "") if ch else ""
    preview_on = bool(ch["general_reply_preview"]) if ch else False

    # -- Эхо сохранённого сообщения (как пользователь его отправил)
    if media_id and media_type == "photo":
        await message.answer_photo(media_id, caption=text or None, parse_mode="HTML")
    elif media_id and media_type == "video":
        await message.answer_video(media_id, caption=text or None, parse_mode="HTML")
    elif media_id and media_type == "document":
        await message.answer_document(media_id, caption=text or None, parse_mode="HTML")
    else:
        await message.answer(text or "—", parse_mode="HTML",
                             disable_web_page_preview=not preview_on)

    # -- Панель управления
    media_icon = "⬆️" if not media_id else "✅"
    preview_label = "нет" if not preview_on else "есть"

    mgmt_kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="✏️ Редактировать",    callback_data=f"ch_ar_edit_global:{chat_id}")],
        [InlineKeyboardButton(text="🎛 Кнопки",           callback_data=f"ch_ar_btns_global:{chat_id}")],
        [InlineKeyboardButton(text=f"🎬 Медиа: {media_icon}", callback_data=f"ch_ar_media_global:{chat_id}")],
        [InlineKeyboardButton(text=f"👁 Превью: {preview_label}", callback_data=f"ch_ar_preview_global:{chat_id}")],
        [InlineKeyboardButton(text="🗑 Удалить",          callback_data=f"ch_ar_delete_global:{chat_id}")],
        [InlineKeyboardButton(text="◀️ Назад",            callback_data=f"ch_autoreply:{chat_id}")],
    ])
    await message.answer("💬 <b>Автоответчик</b>", parse_mode="HTML", reply_markup=mgmt_kb)


# ── Toggle: Общий ответ (вкл ↔ выкл) ─────────────────────────

@router.callback_query(F.data.startswith("ch_ar_toggle_global:"))
async def on_ch_ar_toggle_global(
    callback: CallbackQuery, state: FSMContext, platform_user: dict | None
):
    if not platform_user:
        return
    chat_id  = int(callback.data.split(":")[1])
    owner_id = platform_user["user_id"]

    ch = await db.fetchrow(
        "SELECT general_reply_enabled, general_reply_text FROM bot_chats "
        "WHERE owner_id=$1 AND chat_id=$2::bigint",
        owner_id, chat_id,
    )
    currently_on = bool(ch["general_reply_enabled"]) if ch else False
    has_text     = bool((ch["general_reply_text"] or "").strip()) if ch else False

    if currently_on:
        # Выключаем
        await db.execute(
            "UPDATE bot_chats SET general_reply_enabled=false WHERE owner_id=$1 AND chat_id=$2::bigint",
            owner_id, chat_id,
        )
        await callback.answer("Общий ответ: выкл")
        await _show_autoreply(callback, chat_id, owner_id)
    else:
        if has_text:
            # Текст уже есть — показываем панель управления
            await callback.answer("Общий ответ: вкл")
            await db.execute(
                "UPDATE bot_chats SET general_reply_enabled=true WHERE owner_id=$1 AND chat_id=$2::bigint",
                owner_id, chat_id,
            )
            await _show_global_mgmt(callback.message, chat_id, owner_id)
        else:
            # Текста нет — FSM-ввод
            await state.set_state(MessagesFSM.waiting_for_general_reply_text)
            await state.update_data(chat_id=chat_id, owner_id=owner_id)
            await callback.message.edit_text(
                "<blockquote>⟲ Пришлите сообщение, которое будет "
                "использоваться для автоматического ответа.</blockquote>\n\n"
                "<b>Переменные:</b>\n"
                "├ Имя: <code>{name}</code>\n"
                "├ ФИО: <code>{allname}</code>\n"
                "├ Юзер: <code>{username}</code>\n"
                "├ Площадка: <code>{chat}</code>\n"
                "└ Текущая дата: <code>{day}</code>\n\n"
                "ⓘ Можно прикрепить медиа.",
                reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                    [InlineKeyboardButton(text="◀️ Отмена", callback_data=f"ch_autoreply:{chat_id}")],
                ]),
            )
            await callback.answer()


@router.message(MessagesFSM.waiting_for_general_reply_text)
async def on_general_reply_text_input(message: Message, state: FSMContext):
    data     = await state.get_data()
    chat_id  = data["chat_id"]
    owner_id = data["owner_id"]

    # Поддержка медиа
    if message.photo:
        text     = sanitize(message.caption or "", max_len=1024)
        media_id  = message.photo[-1].file_id
        media_type = "photo"
    elif message.video:
        text     = sanitize(message.caption or "", max_len=1024)
        media_id  = message.video.file_id
        media_type = "video"
    elif message.document:
        text     = sanitize(message.caption or "", max_len=1024)
        media_id  = message.document.file_id
        media_type = "document"
    else:
        text     = sanitize(message.text or "", max_len=1024)
        media_id  = None
        media_type = None

    await db.execute(
        "UPDATE bot_chats "
        "SET general_reply_text=$1, general_reply_media=$2, general_reply_media_type=$3, "
        "    general_reply_enabled=true "
        "WHERE owner_id=$4 AND chat_id=$5::bigint",
        text, media_id, media_type, owner_id, chat_id,
    )
    await state.clear()
    # Показываем эхо + панель управления
    await _show_global_mgmt(message, chat_id, owner_id)


# ── Управление: Редактировать ──────────────────────────────────

@router.callback_query(F.data.startswith("ch_ar_edit_global:"))
async def on_ch_ar_edit_global(
    callback: CallbackQuery, state: FSMContext, platform_user: dict | None
):
    if not platform_user:
        return
    chat_id  = int(callback.data.split(":")[1])
    owner_id = platform_user["user_id"]
    await state.set_state(MessagesFSM.waiting_for_general_reply_text)
    await state.update_data(chat_id=chat_id, owner_id=owner_id)
    await callback.message.edit_text(
        "<blockquote>⟲ Пришлите сообщение, которое будет "
        "использоваться для автоматического ответа.</blockquote>\n\n"
        "<b>Переменные:</b>\n"
        "├ Имя: <code>{name}</code>\n"
        "├ ФИО: <code>{allname}</code>\n"
        "├ Юзер: <code>{username}</code>\n"
        "├ Площадка: <code>{chat}</code>\n"
        "└ Текущая дата: <code>{day}</code>\n\n"
        "ⓘ Можно прикрепить медиа.",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="◀️ Отмена", callback_data=f"ch_autoreply:{chat_id}")],
        ]),
    )
    await callback.answer()


# ── Управление: Медиа ─────────────────────────────────────────

@router.callback_query(F.data.startswith("ch_ar_media_global:"))
async def on_ch_ar_media_global(
    callback: CallbackQuery, state: FSMContext, platform_user: dict | None
):
    if not platform_user:
        return
    chat_id  = int(callback.data.split(":")[1])
    owner_id = platform_user["user_id"]
    # Если медиа уже есть — удаляем его (toggle)
    ch = await db.fetchrow(
        "SELECT general_reply_media FROM bot_chats WHERE owner_id=$1 AND chat_id=$2::bigint",
        owner_id, chat_id,
    )
    if ch and ch["general_reply_media"]:
        await db.execute(
            "UPDATE bot_chats SET general_reply_media=NULL, general_reply_media_type=NULL "
            "WHERE owner_id=$1 AND chat_id=$2::bigint",
            owner_id, chat_id,
        )
        await callback.answer("Медиа удалено")
        await _show_global_mgmt(callback.message, chat_id, owner_id)
    else:
        # Просим прислать медиа через тот же FSM (медиа + подпись)
        await state.set_state(MessagesFSM.waiting_for_general_reply_text)
        await state.update_data(chat_id=chat_id, owner_id=owner_id)
        await callback.message.edit_text(
            "<blockquote>⟲ Пришлите медиа (фото, видео, документ).\n"
            "Текст подписи сохранится как ответ.</blockquote>",
            reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                [InlineKeyboardButton(text="◀️ Отмена", callback_data=f"ch_autoreply:{chat_id}")],
            ]),
        )
        await callback.answer()


# ── Управление: Превью ────────────────────────────────────────

@router.callback_query(F.data.startswith("ch_ar_preview_global:"))
async def on_ch_ar_preview_global(callback: CallbackQuery, platform_user: dict | None):
    if not platform_user:
        return
    chat_id  = int(callback.data.split(":")[1])
    owner_id = platform_user["user_id"]
    ch = await db.fetchrow(
        "SELECT general_reply_preview FROM bot_chats WHERE owner_id=$1 AND chat_id=$2::bigint",
        owner_id, chat_id,
    )
    new_val = not bool(ch["general_reply_preview"] if ch else False)
    await db.execute(
        "UPDATE bot_chats SET general_reply_preview=$1 WHERE owner_id=$2 AND chat_id=$3::bigint",
        new_val, owner_id, chat_id,
    )
    await callback.answer("Превью: " + ("есть" if new_val else "нет"))
    await _show_global_mgmt(callback.message, chat_id, owner_id)


# ── Управление: Кнопки ────────────────────────────────────────

@router.callback_query(F.data.startswith("ch_ar_btns_global:"))
async def on_ch_ar_btns_global(
    callback: CallbackQuery, state: FSMContext, platform_user: dict | None
):
    if not platform_user:
        return
    chat_id  = int(callback.data.split(":")[1])
    owner_id = platform_user["user_id"]
    await state.set_state(MessagesFSM.waiting_for_general_reply_buttons)
    await state.update_data(chat_id=chat_id, owner_id=owner_id)
    await callback.message.edit_text(
        "⛓ <b>Кнопки общего ответа</b>\n\n"
        "Отправьте кнопки в формате:\n"
        "<code>Текст кнопки | https://example.com</code>\n"
        "По одной кнопке на строку.\n\n"
        "Для удаления всех кнопок — отправьте <code>-</code>",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="◀️ Отмена", callback_data=f"ch_autoreply:{chat_id}")],
        ]),
    )
    await callback.answer()


@router.message(MessagesFSM.waiting_for_general_reply_buttons)
async def on_general_reply_buttons_input(message: Message, state: FSMContext):
    data     = await state.get_data()
    chat_id  = data["chat_id"]
    owner_id = data["owner_id"]
    raw      = (message.text or "").strip()

    if raw == "-":
        buttons_json = None
    else:
        import json as _json
        parsed = []
        for line in raw.splitlines():
            if "|" in line:
                parts = line.split("|", 1)
                btn_text = parts[0].strip()
                btn_url  = parts[1].strip()
                if btn_text and btn_url:
                    parsed.append({"text": btn_text, "url": btn_url})
        buttons_json = _json.dumps(parsed, ensure_ascii=False) if parsed else None

    await db.execute(
        "UPDATE bot_chats SET general_reply_buttons=$1 WHERE owner_id=$2 AND chat_id=$3::bigint",
        buttons_json, owner_id, chat_id,
    )
    await state.clear()
    await _show_global_mgmt(message, chat_id, owner_id)


# ── Управление: Удалить ───────────────────────────────────────

@router.callback_query(F.data.startswith("ch_ar_delete_global:"))
async def on_ch_ar_delete_global(callback: CallbackQuery, platform_user: dict | None):
    if not platform_user:
        return
    chat_id  = int(callback.data.split(":")[1])
    owner_id = platform_user["user_id"]
    await db.execute(
        "UPDATE bot_chats "
        "SET general_reply_enabled=false, general_reply_text=NULL, "
        "    general_reply_media=NULL, general_reply_media_type=NULL, "
        "    general_reply_buttons=NULL, general_reply_preview=false "
        "WHERE owner_id=$1 AND chat_id=$2::bigint",
        owner_id, chat_id,
    )
    await callback.answer("🗑 Общий ответ удалён")
    await _show_autoreply(callback, chat_id, owner_id)


# ── Добавление keyword-ответа ──────────────────────────────────

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


# ── Удаление keyword-ответа ────────────────────────────────────

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


