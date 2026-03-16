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
from utils.nav import navigate

logger = logging.getLogger(__name__)
router = Router()


class MessagesFSM(StatesGroup):
    waiting_for_captcha_text           = State()
    waiting_captcha_anim_msg           = State()
    waiting_for_captcha_buttons        = State()
    waiting_for_autoreply_kw           = State()
    waiting_for_autoreply_text         = State()
    waiting_for_autoreply_buttons      = State()
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
    "🌲🌳🌴🌵",
    "🐌🦟🕷🐞",
    "🦀🦞🦐🦑",
    "🐠🐳🐬🦈",
]

# Тип кнопок капчи (размещение)
_BUTTON_PLACEMENTS = ["inline", "reply"]

# Таймер капчи (цикл)
_TIMER_CYCLE = [1, 2, 5, 10, 30]

# Авто-удаление сообщений (цикл, в минутах; 0 = выкл)
_DELETE_CYCLE = [0, 5, 10, 15, 30, 60]

# Реакции (выбор)
_REACTION_OPTIONS = ["👍", "❤️", "🔥", "🎉", "👏", "😍", "🤩", "💯"]


# Хранит message_id эхо-сообщения панели «Общий ответ»
# ключ: (owner_id, chat_id) → message_id
_gr_echo_ids: dict = {}


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
    reaction       = ch.get("reaction_emoji") or ""
    delete_min     = ch.get("auto_delete_min") or 0

    # Куда вернуться: главный экран бота
    child_bot_id = ch.get("child_bot_id")
    back_cb = f"bot_settings:{child_bot_id}" if child_bot_id else "menu:channels"

    captcha_label = {"off": "🔒 Капча: выкл", "simple": "🔒 Капча: простая",
                     "random": "🔒 Капча: рандомная"}.get(captcha_type, "🔒 Капча")
    typing_label  = f"🖨 Печать: {'вкл' if typing_on else 'выкл'}"
    reaction_label = f"❤️ Реакции: {reaction if reaction else 'выкл'}"
    delete_label  = f"🗑 Удаление: {'выкл' if delete_min == 0 else f'{delete_min} мин'}"

    await navigate(
        callback,
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
    current = (ch["reaction_emoji"] if ch else None) or ""
    # Кнопка «Нету» — самая первая
    no_reaction_check = " ✅" if current == "" else ""
    buttons = [[InlineKeyboardButton(
        text=f"Выкл{no_reaction_check}",
        callback_data=f"ch_set_reaction:{chat_id}:none",
    )]]
    buttons += [[InlineKeyboardButton(
        text=e + (" ✅" if e == current else ""),
        callback_data=f"ch_set_reaction:{chat_id}:{e}",
    )] for e in _REACTION_OPTIONS]
    buttons.append([InlineKeyboardButton(text="◀️ Назад", callback_data=f"ch_messages:{chat_id}")])
    await navigate(
        callback,
        "❤️ <b>Реакции</b>\n\nВыберите реакцию, которую бот будет ставить на сообщения:",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=buttons),
    )


@router.callback_query(F.data.startswith("ch_set_reaction:"))
async def on_ch_set_reaction(callback: CallbackQuery, platform_user: dict | None):
    if not platform_user:
        return
    parts    = callback.data.split(":")
    chat_id  = int(parts[1])
    emoji    = parts[2]
    owner_id = platform_user["user_id"]
    # «none» — отключить реакцию (сохраняем NULL)
    save_val = None if emoji == "none" else emoji
    await db.execute(
        "UPDATE bot_chats SET reaction_emoji=$1 WHERE owner_id=$2 AND chat_id=$3::bigint",
        save_val, owner_id, chat_id,
    )
    answer_text = "Реакции: выкл" if emoji == "none" else f"Реакция: {emoji}"
    await callback.answer(answer_text)
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
    anim_on       = bool(ch.get("captcha_anim_file_id")) or bool(ch.get("captcha_animation", False))
    btn_placement = ch.get("captcha_button_style") or "inline"  # 'inline' / 'reply'
    greet_on      = bool(ch.get("captcha_greet", False))
    accept_now    = bool(ch.get("captcha_accept_now", False))
    accept_all    = bool(ch.get("captcha_accept_all", False))
    emoji_set     = ch.get("captcha_emoji_set") or _EMOJI_SETS[0]

    type_labels = {"off": "🔒 Капча: выкл", "simple": "🔒 Капча: простая",
                   "random": "🔒 Капча: рандомная"}
    type_label = type_labels.get(ctype, "🔒 Капча")

    # Иконки зависящие от состояния
    anim_icon     = "🎞" if anim_on else "🎞" # Используем одну и ту же строгую иконку для обоих состояний
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

        if ctype == "simple":
            buttons.append([
                InlineKeyboardButton(text="✏️ Текст капчи",  callback_data=f"ch_captcha_text:{chat_id}"),
                InlineKeyboardButton(text="✏️ Текст кнопок", callback_data=f"ch_captcha_btns:{chat_id}"),
            ])
        else:
            # Для рандомной капчи скрываем кнопку настройки текста кнопок
            buttons.append([
                InlineKeyboardButton(text="✏️ Текст капчи", callback_data=f"ch_captcha_text:{chat_id}"),
            ])

        buttons.extend([
            [
                InlineKeyboardButton(
                    text=f"🔄 Сброс капчи",
                    callback_data=f"ch_cap_reset:{chat_id}",
                ),
                InlineKeyboardButton(
                    text=f"{anim_icon} Анимация капчи",
                    callback_data=f"ch_cap_anim_menu:{chat_id}",
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
        ])

    buttons.append([InlineKeyboardButton(text="◀️ Назад", callback_data=f"ch_messages:{chat_id}")])

    await navigate(
        callback,
        info,
        reply_markup=InlineKeyboardMarkup(inline_keyboard=buttons),
    )


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
    setting  = parts[2]
    owner_id = platform_user["user_id"]

    ch = await db.fetchrow(
        "SELECT * FROM bot_chats WHERE owner_id=$1 AND chat_id=$2::bigint",
        owner_id, chat_id,
    )
    if not ch:
        return

    if setting == "greet":
        new_val = not bool(ch.get("captcha_greet", False))
        await db.execute("UPDATE bot_chats SET captcha_greet=$1 WHERE owner_id=$2 AND chat_id=$3::bigint",
                         new_val, owner_id, chat_id)
        await callback.answer("Приветствовать: " + ("вкл" if new_val else "выкл"))

    elif setting == "btn_type":
        cur = ch.get("captcha_button_style") or "inline"
        new_val = "reply" if cur == "inline" else "inline"
        await db.execute("UPDATE bot_chats SET captcha_button_style=$1 WHERE owner_id=$2 AND chat_id=$3::bigint",
                         new_val, owner_id, chat_id)
        labels = {"inline": "📩 Inline", "reply": "⌨️ Reply"}
        await callback.answer(f"Кнопки: {labels[new_val]}")

    elif setting == "timer":
        cur = int(ch.get("captcha_timer_min") or 1)
        try:
            idx = _TIMER_CYCLE.index(cur)
        except ValueError:
            idx = 0
        new_val = _TIMER_CYCLE[(idx + 1) % len(_TIMER_CYCLE)]
        await db.execute("UPDATE bot_chats SET captcha_timer_min=$1 WHERE owner_id=$2 AND chat_id=$3::bigint",
                         new_val, owner_id, chat_id)
        await callback.answer(f"Таймер: {new_val} мин")

    elif setting == "accept_now":
        new_val = not bool(ch.get("captcha_accept_now", False))
        await db.execute("UPDATE bot_chats SET captcha_accept_now=$1 WHERE owner_id=$2 AND chat_id=$3::bigint",
                         new_val, owner_id, chat_id)
        await callback.answer("Принимать сразу: " + ("вкл" if new_val else "выкл"))

    elif setting == "accept_all":
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
    await navigate(
        callback,
        "🔄 <b>Сброс капчи</b>\n\n"
        "Это действие сбросит <u>текст капчи</u>, <u>текст кнопок</u> и <u>медиа</u> "
        "к стандартным значениям.\n\n"
        "Настройки (тип, таймер, переключатели) <b>не изменятся</b>.\n\n"
        "Подтвердить сброс?",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="✅ Подтвердить", callback_data=f"ch_cap_reset_ok:{chat_id}")],
            [InlineKeyboardButton(text="◀️ Отмена",     callback_data=f"ch_captcha:{chat_id}")],
        ]),
    )


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

    await navigate(
        callback,
        "🎲 <b>Вид рандомной капчи</b>\n\nВыберите набор эмоджи:",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=buttons),
    )


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


# ── Анимация капчи (загрузка) ───────────────────────────────────

@router.callback_query(F.data.startswith("ch_cap_anim_menu:"))
async def on_ch_cap_anim_menu(call: CallbackQuery, state: FSMContext, platform_user: dict | None):
    if not platform_user:
        return
    chat_id = int(call.data.split(":")[1])
    owner_id = platform_user["user_id"]
    
    # check permissions
    ch = await db.fetchrow(
        "SELECT captcha_anim_file_id, captcha_anim_type FROM bot_chats WHERE owner_id=$1 AND chat_id=$2::bigint",
        owner_id, chat_id,
    )
    if not ch: # or ch["owner_id"] != owner_id: (already filtered by owner_id in query)
        await call.answer("Нет прав", show_alert=True)
        return

    has_anim = bool(ch and ch.get("captcha_anim_file_id"))
    anim_file_id = ch.get("captcha_anim_file_id")
    anim_type = ch.get("captcha_anim_type")
    
    # Text menu content
    if has_anim:
        status_text = "🟢 Активна"
        text = (
            "🎬 <b>Анимация капчи</b>\n\n"
            f"<b>Статус:</b> {status_text}\n\n"
        )
    else:
        status_text = "🔴 Не задана"
        text = (
            "🎬 <b>Анимация капчи</b>\n\n"
            f"<b>Статус:</b> {status_text}\n\n"
            "ℹ️ Отправьте в этот чат GIF-файл или короткое видео.\n"
            "Загруженный медиа-файл будет автоматически отображаться над текстом капчи, привлекая внимание пользователей."
        )

    kb = []
    
    anim_msg = None
    menu_msg = None
    
    # Очищаем старую анимацию, если мы вернулись сюда по кнопке "Отмена" 
    data = await state.get_data()
    old_anim_id = data.get("anim_msg_id")
    if old_anim_id:
        try:
            await call.message.chat.delete_message(old_anim_id)
        except Exception:
            pass

    if has_anim:
        # We need to send a new message with the animation, so we delete the current menu
        try:
            await call.message.delete()
        except Exception:
            pass # Message might have been deleted already or not exist

        # Send the "echo" animation first
        try:
            if anim_type == "video":
                anim_msg = await call.message.answer_video(video=anim_file_id)
            else:
                anim_msg = await call.message.answer_animation(animation=anim_file_id)
        except Exception as e:
            logger.error(f"Failed to echo captcha animation for chat {chat_id}: {e}")
            
        # Add buttons for existing animation
        kb.append([InlineKeyboardButton(text="🔄 Изменить анимацию", callback_data=f"ch_cap_anim_change:{chat_id}")])
        kb.append([InlineKeyboardButton(text="🗑 Удалить анимацию", callback_data=f"ch_cap_anim_del:{chat_id}")])
        
        # Send text menu as a new message
        menu_msg = await call.message.answer(
            text,
            parse_mode="HTML",
            reply_markup=InlineKeyboardMarkup(inline_keyboard=kb + [[InlineKeyboardButton(text="◀️ Назад", callback_data=f"ch_cap_anim_back:{chat_id}")]]),
        )
    else:
        # No animation yet, just edit the existing text
        kb.append([InlineKeyboardButton(text="◀️ Назад", callback_data=f"ch_cap_anim_back:{chat_id}")])
        menu_msg = await call.message.edit_text(
            text,
            parse_mode="HTML",
            reply_markup=InlineKeyboardMarkup(inline_keyboard=kb),
        )

    await state.set_state(MessagesFSM.waiting_captcha_anim_msg)
    
    # Store message IDs so we can clean them up later
    await state.update_data(
        chat_id=chat_id, 
        owner_id=owner_id, 
        menu_msg_id=menu_msg.message_id,
        anim_msg_id=anim_msg.message_id if anim_msg else None
    )
    await call.answer()


@router.callback_query(F.data.startswith("ch_cap_anim_back:"))
async def on_ch_cap_anim_back(call: CallbackQuery, state: FSMContext, platform_user: dict | None):
    if not platform_user:
        return
    chat_id = int(call.data.split(":")[1])
    
    # Удаляем сообщение с анимацией, если оно есть
    data = await state.get_data()
    anim_msg_id = data.get("anim_msg_id")
    if anim_msg_id:
        try:
            await call.message.chat.delete_message(anim_msg_id)
        except Exception:
            pass
            
    await state.clear()
    await _show_captcha(call, chat_id, platform_user["user_id"])
    await call.answer()


@router.callback_query(F.data.startswith("ch_cap_anim_change:"))
async def on_ch_cap_anim_change(call: CallbackQuery, state: FSMContext, platform_user: dict | None):
    if not platform_user:
        return
    chat_id = int(call.data.split(":")[1])
    # data = await state.get_data() # Not strictly needed here, but good practice if more data was used
    
    text = (
        "🎬 <b>Анимация капчи</b>\n\n"
        "ℹ️ Отправьте в этот чат новый GIF-файл или короткое видео, чтобы заменить текущую анимацию."
    )
    
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="❌ Отмена", callback_data=f"ch_cap_anim_menu:{chat_id}")]
    ])
    
    await call.message.edit_text(text, parse_mode="HTML", reply_markup=kb)
    await state.set_state(MessagesFSM.waiting_captcha_anim_msg)
    await call.answer()


@router.callback_query(F.data.startswith("ch_cap_anim_del:"))
async def on_ch_cap_anim_del(call: CallbackQuery, state: FSMContext, platform_user: dict | None):
    if not platform_user:
        return
    chat_id = int(call.data.split(":")[1])
    owner_id = platform_user["user_id"]
    # check permissions
    ch = await db.fetchrow(
        "SELECT * FROM bot_chats WHERE owner_id=$1 AND chat_id=$2::bigint",
        owner_id, chat_id,
    )
    if not ch: # or ch["owner_id"] != owner_id: (already filtered by owner_id in query)
        await call.answer("Нет прав", show_alert=True)
        return

    await db.execute(
        "UPDATE bot_chats SET captcha_anim_file_id=NULL, captcha_anim_type=NULL WHERE owner_id=$1 AND chat_id=$2::bigint",
        owner_id, chat_id,
    )

    data = await state.get_data()
    anim_msg_id = data.get("anim_msg_id")
    if anim_msg_id:
        try:
            await call.message.chat.delete_message(anim_msg_id)
        except Exception:
            pass

    await call.answer("Анимация удалена!", show_alert=True)
    # Redirect back to the menu (which will now show "Не задана")
    await on_ch_cap_anim_menu(call, state, platform_user)


@router.message(MessagesFSM.waiting_captcha_anim_msg, F.content_type.in_({'animation', 'video'}))
async def on_captcha_anim_upload(message: Message, state: FSMContext):
    data = await state.get_data()
    chat_id = data.get("chat_id")
    owner_id = data.get("owner_id")
    menu_msg_id = data.get("menu_msg_id")
    anim_msg_id = data.get("anim_msg_id")

    if not chat_id or not owner_id:
        await state.clear()
        return

    # Delete the user's uploaded message to keep chat clean
    try:
        await message.delete()
    except Exception:
        pass

    if message.animation:
        file_id = message.animation.file_id
        atype = "animation"
    elif message.video:
        file_id = message.video.file_id
        atype = "video"
    else:
        return

    await db.execute(
        "UPDATE bot_chats SET captcha_anim_file_id=$1, captcha_anim_type=$2 WHERE owner_id=$3 AND chat_id=$4::bigint",
        file_id, atype, owner_id, chat_id,
    )

    # Clean up old messages
    if menu_msg_id:
        try:
            await message.chat.delete_message(menu_msg_id)
        except Exception:
            pass
    if anim_msg_id:
        try:
            await message.chat.delete_message(anim_msg_id)
        except Exception:
            pass

    await state.clear()
    await message.answer(
        "✅ Анимация успешно установлена!",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="◀️ Назад", callback_data=f"ch_cap_anim_menu:{chat_id}")],
        ]),
    )


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
    await state.update_data(chat_id=chat_id, owner_id=platform_user["user_id"],
                            editor_prompt_mid=callback.message.message_id)
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
    prompt_mid = data.get("editor_prompt_mid")

    if prompt_mid:
        try:
            await message.bot.delete_message(chat_id=message.chat.id, message_id=prompt_mid)
        except Exception:
            pass

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
    await state.update_data(chat_id=chat_id, owner_id=platform_user["user_id"],
                            editor_prompt_mid=callback.message.message_id)
    await callback.message.edit_text(
        f"{ctype_label} капча:\n\n"
        "⸬ <b>Текст кнопок</b>\n\n"
        "<blockquote><u>Цвет:</u>\n\n"
        "🟦 - Синий\n"
        "🟩 - Зелёный\n"
        "🟥 - Красный\n\n"
        "<u>Пример:</u> <code>🟩 Я не робот</code></blockquote>\n\n"
        "↪️ Пришлите названия для <u>кнопок капчи:</u>",
        parse_mode="HTML",
    )
    await callback.answer()


@router.message(MessagesFSM.waiting_for_captcha_buttons)
async def on_captcha_btns_input(message: Message, state: FSMContext):
    data     = await state.get_data()
    chat_id  = data["chat_id"]
    owner_id = data["owner_id"]
    prompt_mid = data.get("editor_prompt_mid")

    if prompt_mid:
        try:
            await message.bot.delete_message(chat_id=message.chat.id, message_id=prompt_mid)
        except Exception:
            pass

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
    has_text   = bool((ch["general_reply_text"] or "").strip()) if ch else False
    general_on = has_text  # Считаем «вкл» если сохранён текст

    buttons = []

    # Кнопка «Общий ответ»
    if general_on:
        buttons.append([InlineKeyboardButton(
            text="Общий ответ: вкл",
            callback_data=f"ch_ar_toggle_global:{chat_id}",
        )])
    else:
        buttons.append([InlineKeyboardButton(
            text="Общий ответ: выкл",
            callback_data=f"ch_ar_toggle_global:{chat_id}",
        )])

    # Keyword-ответы всегда показываем между «Общий ответ» и «+ Добавить ответ»
    rows = await db.fetch(
        "SELECT id, keyword FROM autoreplies "
        "WHERE owner_id=$1 AND chat_id=$2::bigint LIMIT 20",
        owner_id, chat_id,
    )
    for r in rows:
        buttons.append([InlineKeyboardButton(
            text=r['keyword'][:30],
            callback_data=f"ch_ar_view:{chat_id}:{r['id']}",
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
        parse_mode="HTML",
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
        "general_reply_preview, general_reply_buttons, general_reply_media_top FROM bot_chats "
        "WHERE owner_id=$1 AND chat_id=$2::bigint",
        owner_id, chat_id,
    )
    text       = (ch["general_reply_text"] or "") if ch else ""
    media_id   = (ch["general_reply_media"] or "") if ch else ""
    media_type = (ch["general_reply_media_type"] or "") if ch else ""
    preview_on = bool(ch["general_reply_preview"]) if ch else False
    # True = медиа сверху (по умолчанию), False = медиа снизу
    media_top  = ch["general_reply_media_top"] if ch and ch["general_reply_media_top"] is not None else True

    # Удалить старое эхо, если оно уже есть (обновление панели)
    key = (owner_id, chat_id)
    old_echo_id = _gr_echo_ids.pop(key, None)
    if old_echo_id:
        try:
            await message.bot.delete_message(message.chat.id, old_echo_id)
        except Exception:
            pass

    # -- Эхо сохранённого сообщения
    from utils.keyboard import build_inline_keyboard
    user_msg_kb = build_inline_keyboard(ch["general_reply_buttons"]) if ch else None

    if media_id:
        kwargs = {
            "caption": text or None,
            "parse_mode": "HTML",
            "show_caption_above_media": not media_top,
            "reply_markup": user_msg_kb,
        }
        if media_type == "photo":
            echo_msg = await message.answer_photo(media_id, **kwargs)
        elif media_type == "video":
            echo_msg = await message.answer_video(media_id, **kwargs)
        elif media_type == "document":
            echo_msg = await message.answer_document(media_id, **kwargs)
        else:
            echo_msg = await message.answer(text or "—", parse_mode="HTML",
                                            disable_web_page_preview=not preview_on,
                                            reply_markup=user_msg_kb)
    else:
        echo_msg = await message.answer(text or "—", parse_mode="HTML",
                                        disable_web_page_preview=not preview_on,
                                        reply_markup=user_msg_kb)

    # Сохраняем ID эхо-сообщения
    _gr_echo_ids[key] = echo_msg.message_id

    # -- Панель управления
    media_icon = "⬆️" if media_top else "⬇️"
    preview_label = "нет" if not preview_on else "есть"

    mgmt_kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="✏️ Редактировать",    callback_data=f"ch_ar_edit_global:{chat_id}")],
        [InlineKeyboardButton(text="🎛 Кнопки",           callback_data=f"ch_ar_btns_global:{chat_id}")],
        [InlineKeyboardButton(text=f"🎬 Медиа: {media_icon}", callback_data=f"ch_ar_media_global:{chat_id}")],
        [InlineKeyboardButton(text=f"👁 Превью: {preview_label}", callback_data=f"ch_ar_preview_global:{chat_id}")],
        [InlineKeyboardButton(text="🗑 Удалить",          callback_data=f"ch_ar_delete_global:{chat_id}")],
        [InlineKeyboardButton(text="◀️ Назад",            callback_data=f"ch_ar_back_global:{chat_id}")],
    ])
    await message.answer("💬 <b>Автоответчик</b>", parse_mode="HTML", reply_markup=mgmt_kb)


# ── «◀️ Назад» из панели управления общим ответом ───────────────

@router.callback_query(F.data.startswith("ch_ar_back_global:"))
async def on_ch_ar_back_global(callback: CallbackQuery, platform_user: dict | None):
    if not platform_user:
        return
    chat_id  = int(callback.data.split(":")[1])
    owner_id = platform_user["user_id"]

    # Удалить эхо-сообщение, если оно зарегистрировано
    key = (owner_id, chat_id)
    echo_id = _gr_echo_ids.pop(key, None)
    if echo_id:
        try:
            await callback.message.bot.delete_message(callback.message.chat.id, echo_id)
        except Exception:
            pass

    # Текст и флаг в БД НЕ трогаем — просто возвращаемся в меню
    await callback.answer()
    await _show_autoreply(callback, chat_id, owner_id)


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
    has_text = bool((ch["general_reply_text"] or "").strip()) if ch else False

    if has_text:
        # Текст уже есть — открываем панель управления (и включаем флаг)
        await db.execute(
            "UPDATE bot_chats SET general_reply_enabled=true WHERE owner_id=$1 AND chat_id=$2::bigint",
            owner_id, chat_id,
        )
        await callback.answer("Общий ответ: вкл")
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


# ── Управление: Медиа (позиция ⬆️/⬇️ или добавить) ──────────────

@router.callback_query(F.data.startswith("ch_ar_media_global:"))
async def on_ch_ar_media_global(
    callback: CallbackQuery, state: FSMContext, platform_user: dict | None
):
    if not platform_user:
        return
    chat_id  = int(callback.data.split(":")[1])
    owner_id = platform_user["user_id"]
    ch = await db.fetchrow(
        "SELECT general_reply_media, general_reply_media_top, general_reply_preview FROM bot_chats "
        "WHERE owner_id=$1 AND chat_id=$2::bigint",
        owner_id, chat_id,
    )
    if not ch:
        await callback.answer()
        return

    current_top = (ch["general_reply_media_top"] if ch["general_reply_media_top"] is not None else True)
    new_top = not current_top
    await db.execute(
        "UPDATE bot_chats SET general_reply_media_top=$1 WHERE owner_id=$2 AND chat_id=$3::bigint",
        new_top, owner_id, chat_id,
    )
    
    try:
        await callback.message.delete()
    except:
        pass
        
    await callback.answer("Медиа: " + ("сверху ⬆️" if new_top else "снизу ⬇️"))
    await _show_global_mgmt(callback.message, chat_id, owner_id)


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
        "📎 Отправьте <b>кнопки</b>, которые будут добавлены к сообщению.\n\n"
        "🔗 <b>URL-кнопки</b>\n\n"
        "<b>Одна кнопка в ряду:</b>\n"
        "<code>Кнопка 1 — ссылка</code>\n"
        "<code>Кнопка 2 — ссылка</code>\n\n"
        "<b>Несколько кнопок в ряду:</b>\n"
        "<code>Кнопка 1 — ссылка | Кнопка 2 — ссылка</code>\n\n"
        "🎨 <b>Цветные кнопки (добавь emoji перед названием):</b>\n"
        "<code>🟦 Кнопка — ссылка</code> — синяя\n"
        "<code>🟩 Кнопка — ссылка</code> — зелёная\n"
        "<code>🟥 Кнопка — ссылка</code> — красная\n\n"
        "*** <b>Другие виды кнопок</b>\n\n"
        "<b>WebApp кнопки:</b>\n"
        "<code>Кнопка 1 — ссылка (webapp)</code>\n\n"
        "ℹ️ Нажмите, чтобы скопировать.",
        parse_mode="HTML",
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
        rows = []
        for line in raw.splitlines():
            line = line.strip()
            if not line:
                continue
            row = []
            for btn_raw in line.split("|"):
                btn_raw = btn_raw.strip()
                sep = None
                if " — " in btn_raw:
                    sep = " — "
                elif " – " in btn_raw:
                    sep = " – "
                elif " - " in btn_raw:
                    sep = " - "
                if sep:
                    idx = btn_raw.index(sep)
                    btn_text = btn_raw[:idx].strip()
                    btn_url  = btn_raw[idx + len(sep):].strip()
                    if btn_text and btn_url:
                        row.append({"text": btn_text, "url": btn_url})
            if row:
                rows.append(row)
        buttons_json = _json.dumps(rows, ensure_ascii=False) if rows else None

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

    # Удалить эхо-сообщение из чата
    key = (owner_id, chat_id)
    echo_id = _gr_echo_ids.pop(key, None)
    if echo_id:
        try:
            await callback.message.bot.delete_message(callback.message.chat.id, echo_id)
        except Exception:
            pass

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
        "Отправьте триггер.\n\n"
        "<blockquote>① Триггер — это сообщение для вызова автоматического ответа.</blockquote>\n\n"
        "<b>Пример:</b>\n"
        "├ Бонус\n"
        "└ /bonus",
        parse_mode="HTML",
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
        "💬 <b>Автоответчик</b>\n\n"
        "<blockquote>⟲ Пришлите сообщение, которое будет "
        "использоваться для автоматического ответа.</blockquote>\n\n"
        "<b>Переменные:</b>\n"
        "├ Имя: <code>{name}</code>\n"
        "├ ФИО: <code>{allname}</code>\n"
        "├ Юзер: <code>{username}</code>\n"
        "├ Площадка: <code>{chat}</code>\n"
        "└ Текущая дата: <code>{day}</code>\n\n"
        "ⓘ Можно прикрепить медиа.",
        parse_mode="HTML",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="◀️ Назад", callback_data=f"ch_autoreply:{data['chat_id']}")],
        ]),
    )


@router.message(MessagesFSM.waiting_for_autoreply_text)
async def on_ar_text_input(message: Message, state: FSMContext):
    data     = await state.get_data()
    chat_id  = data["chat_id"]
    owner_id = data["owner_id"]
    keyword  = data.get("keyword", "")
    ar_id    = data.get("ar_id")  # есть при редактировании, нет при создании
    prompt_mid = data.get("prompt_mid")

    if prompt_mid:
         try:
             await message.bot.delete_message(message.chat.id, prompt_mid)
         except Exception:
             pass

    # Поддержка медиа
    if message.photo:
        reply      = sanitize(message.caption or "", max_len=1024)
        media_id   = message.photo[-1].file_id
        media_type = "photo"
    elif message.video:
        reply      = sanitize(message.caption or "", max_len=1024)
        media_id   = message.video.file_id
        media_type = "video"
    elif message.document:
        reply      = sanitize(message.caption or "", max_len=1024)
        media_id   = message.document.file_id
        media_type = "document"
    else:
        reply      = sanitize(message.text or "", max_len=1024)
        media_id   = None
        media_type = None

    if ar_id:
        # Режим редактирования — обновляем конкретную запись по ID
        await db.execute(
            """UPDATE autoreplies
               SET reply_text=$1, reply_media=$2, reply_media_type=$3
               WHERE id=$4 AND owner_id=$5""",
            reply, media_id, media_type, ar_id, owner_id,
        )
        await state.clear()
        # Возвращаем панель управления keyword-ответом
        await _show_keyword_mgmt(message, chat_id, owner_id, ar_id)
    else:
        # Режим создания — INSERT/ON CONFLICT, получаем id новой записи
        saved = await db.fetchrow(
            """INSERT INTO autoreplies (owner_id, chat_id, keyword, reply_text, reply_media, reply_media_type)
               VALUES ($1, $2, $3, $4, $5, $6)
               ON CONFLICT (owner_id, chat_id, keyword)
               DO UPDATE SET reply_text=EXCLUDED.reply_text,
                             reply_media=EXCLUDED.reply_media,
                             reply_media_type=EXCLUDED.reply_media_type
               RETURNING id""",
            owner_id, chat_id, keyword, reply, media_id, media_type,
        )
        await state.clear()
        new_ar_id = saved["id"] if saved else None

        if new_ar_id:
            # Открываем панель управления свежесозданным ответом
            await _show_keyword_mgmt(message, chat_id, owner_id, new_ar_id)


# ── Просмотр и управление keyword-ответом ──────────────────────

# Хранит message_id эхо-сообщения панели keyword-ответа: (owner_id, chat_id, ar_id) -> echo_msg_id
_kw_echo_ids: dict[tuple, int] = {}


async def _show_keyword_mgmt(message, chat_id: int, owner_id: int, ar_id: int):
    """Показывает панель управления keyword-ответом (2 сообщения: эхо + управление)."""
    row = await db.fetchrow(
        "SELECT keyword, reply_text, reply_media, reply_media_type, reply_preview, reply_buttons, reply_media_top "
        "FROM autoreplies WHERE id=$1 AND owner_id=$2",
        ar_id, owner_id,
    )
    if not row:
        return

    keyword    = row["keyword"] or ""
    text       = row["reply_text"] or ""
    media_id   = row["reply_media"] or ""
    media_type = row["reply_media_type"] or ""
    # True = медиа сверху (по умолчанию), False = медиа снизу
    media_top  = row["reply_media_top"] if row["reply_media_top"] is not None else True

    # Удалить старое эхо, если есть
    key = (owner_id, chat_id, ar_id)
    old_echo_id = _kw_echo_ids.pop(key, None)
    if old_echo_id:
        try:
            await message.bot.delete_message(message.chat.id, old_echo_id)
        except Exception:
            pass

    # -- Строим инлайн-клавиатуру из сохранённых кнопок
    import json as _json
    raw_btns = row["reply_buttons"] if row else None
    echo_kb = None
    if raw_btns:
        try:
            parsed = _json.loads(raw_btns)
            # Новый формат: [[{text, url}, ...], ...]
            # Старый формат (плоский): [{text, url}, ...]
            if parsed and isinstance(parsed[0], dict):
                parsed = [parsed]  # совместимость со старым форматом
            kb_rows = []
            for btn_row in parsed:
                kb_rows.append([
                    InlineKeyboardButton(text=b["text"], url=b["url"])
                    for b in btn_row if b.get("text") and b.get("url")
                ])
            if kb_rows:
                echo_kb = InlineKeyboardMarkup(inline_keyboard=kb_rows)
        except Exception:
            pass

    # -- Эхо сохранённого ответа
    if media_id:
        kwargs = {
            "caption": text or None,
            "parse_mode": "HTML",
            "reply_markup": echo_kb,
            "show_caption_above_media": not media_top,
        }
        if media_type == "photo":
            echo_msg = await message.answer_photo(media_id, **kwargs)
        elif media_type == "video":
            echo_msg = await message.answer_video(media_id, **kwargs)
        elif media_type == "document":
            echo_msg = await message.answer_document(media_id, **kwargs)
        else:
            echo_msg = await message.answer(text or "—", parse_mode="HTML", reply_markup=echo_kb)
    else:
        echo_msg = await message.answer(text or "—", parse_mode="HTML", reply_markup=echo_kb)

    _kw_echo_ids[key] = echo_msg.message_id

    # -- Панель управления
    if media_id:
        media_icon = "⬆️" if media_top else "⬇️"
    else:
        media_icon = "⬆️"
    preview_on   = bool(row["reply_preview"]) if row else False
    preview_label = "есть" if preview_on else "нет"
    mgmt_kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="✏️ Редактировать",       callback_data=f"ch_ar_kw_edit:{chat_id}:{ar_id}")],
        [InlineKeyboardButton(text="🎛 Кнопки",              callback_data=f"ch_ar_kw_btns:{chat_id}:{ar_id}")],
        [InlineKeyboardButton(text=f"🎬 Медиа: {media_icon}", callback_data=f"ch_ar_kw_media:{chat_id}:{ar_id}")],
        [InlineKeyboardButton(text=f"👁 Превью: {preview_label}", callback_data=f"ch_ar_kw_preview:{chat_id}:{ar_id}")],
        [InlineKeyboardButton(text="🗑 Удалить",              callback_data=f"ch_ar_del:{chat_id}:{ar_id}")],
        [InlineKeyboardButton(text="◀️ Назад",               callback_data=f"ch_ar_kw_back:{chat_id}:{ar_id}")],
    ])
    await message.answer(
        f"💬 <b>Автоответчик</b>\n\nТриггер: <code>{keyword}</code>",
        parse_mode="HTML",
        reply_markup=mgmt_kb,
    )


@router.callback_query(F.data.startswith("ch_ar_view:"))
async def on_ch_ar_view(callback: CallbackQuery, platform_user: dict | None):
    """Открывает панель управления keyword-ответом."""
    if not platform_user:
        return
    parts   = callback.data.split(":")
    chat_id = int(parts[1])
    ar_id   = int(parts[2])
    owner_id = platform_user["user_id"]
    await callback.message.delete()
    await _show_keyword_mgmt(callback.message, chat_id, owner_id, ar_id)
    await callback.answer()


@router.callback_query(F.data.startswith("ch_ar_kw_back:"))
async def on_ch_ar_kw_back(callback: CallbackQuery, platform_user: dict | None):
    """Назад из панели управления keyword-ответом — удаляем эхо и возвращаемся."""
    if not platform_user:
        return
    parts    = callback.data.split(":")
    chat_id  = int(parts[1])
    ar_id    = int(parts[2])
    owner_id = platform_user["user_id"]

    key = (owner_id, chat_id, ar_id)
    echo_id = _kw_echo_ids.pop(key, None)
    if echo_id:
        try:
            await callback.message.bot.delete_message(callback.message.chat.id, echo_id)
        except Exception:
            pass

    await callback.answer()
    await _show_autoreply(callback, chat_id, owner_id)


@router.callback_query(F.data.startswith("ch_ar_kw_edit:"))
async def on_ch_ar_kw_edit(
    callback: CallbackQuery, state: FSMContext, platform_user: dict | None
):
    """Редактирование текста keyword-ответа."""
    if not platform_user:
        return
    parts    = callback.data.split(":")
    chat_id  = int(parts[1])
    ar_id    = int(parts[2])
    owner_id = platform_user["user_id"]

    # Получаем текущий keyword для контекста
    row = await db.fetchrow(
        "SELECT keyword FROM autoreplies WHERE id=$1 AND owner_id=$2", ar_id, owner_id
    )
    keyword = (row["keyword"] if row else "") or ""

    # Удаляем эхо перед открытием формы редактирования
    key = (owner_id, chat_id, ar_id)
    echo_id = _kw_echo_ids.pop(key, None)
    if echo_id:
        try:
            await callback.bot.delete_message(chat_id=callback.message.chat.id, message_id=echo_id)
        except Exception:
            pass

    await state.set_state(MessagesFSM.waiting_for_autoreply_text)
    await state.update_data(chat_id=chat_id, owner_id=owner_id, keyword=keyword, ar_id=ar_id, prompt_mid=callback.message.message_id)
    await callback.message.edit_text(
        f"✏️ <b>Редактирование ответа</b>\n\nТриггер: <code>{keyword}</code>\n\n"
        "<blockquote>⟲ Пришлите новое сообщение для автоматического ответа.</blockquote>\n\n"
        "<b>Переменные:</b>\n"
        "├ Имя: <code>{name}</code>\n"
        "├ ФИО: <code>{allname}</code>\n"
        "├ Юзер: <code>{username}</code>\n"
        "├ Площадка: <code>{chat}</code>\n"
        "└ Текущая дата: <code>{day}</code>\n\n"
        "ⓘ Можно прикрепить медиа.",
        parse_mode="HTML",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="◀️ Отмена", callback_data=f"ch_ar_view:{chat_id}:{ar_id}")],
        ]),
    )
    await callback.answer()


@router.callback_query(F.data.startswith("ch_ar_kw_media:"))
async def on_ch_ar_kw_media(
    callback: CallbackQuery, state: FSMContext, platform_user: dict | None
):
    """Кнопка Медиа — всегда переключает стрелку ⬆️/⬇️ прямо на месте (без новых сообщений)."""
    if not platform_user:
        return
    parts    = callback.data.split(":")
    chat_id  = int(parts[1])
    ar_id    = int(parts[2])
    owner_id = platform_user["user_id"]

    row = await db.fetchrow(
        "SELECT reply_media, reply_media_top, reply_preview, reply_text, reply_buttons, reply_media_type FROM autoreplies WHERE id=$1 AND owner_id=$2",
        ar_id, owner_id,
    )
    if not row:
        await callback.answer()
        return

    current_top = row["reply_media_top"] if row["reply_media_top"] is not None else True
    new_top = not current_top
    await db.execute(
        "UPDATE autoreplies SET reply_media_top=$1 WHERE id=$2 AND owner_id=$3",
        new_top, ar_id, owner_id,
    )
    
    # Редактируем эхо-сообщение если есть медиа
    if row["reply_media"]:
        key = (owner_id, chat_id, ar_id)
        echo_id = _kw_echo_ids.get(key)
        if echo_id:
            try:
                # Получаем текст и кнопки для эха
                text = row.get("reply_text")
                
                import json as _json
                raw_btns = row.get("reply_buttons")
                echo_kb = None
                if raw_btns:
                    try:
                        parsed = _json.loads(raw_btns)
                        if parsed and isinstance(parsed[0], dict):
                            parsed = [parsed]
                        kb_rows = []
                        for dict_row in parsed:
                            kb_rows.append([
                                InlineKeyboardButton(text=b["text"], url=b["url"])
                                for b in dict_row if b.get("text") and b.get("url")
                            ])
                        if kb_rows:
                            echo_kb = InlineKeyboardMarkup(inline_keyboard=kb_rows)
                    except Exception:
                        pass
                
                await callback.bot.edit_message_caption(
                    chat_id=callback.message.chat.id,
                    message_id=echo_id,
                    caption=text or None,
                    parse_mode="HTML",
                    reply_markup=echo_kb,
                    show_caption_above_media=not new_top,
                )
            except Exception as e:
                logger.error(f"Error editing echo caption: {e}")
            
    # Обновляем инлайн-клавиатуру самого сообщения-меню
    media_icon    = "⬆️" if new_top else "⬇️"
    preview_on    = bool(row["reply_preview"]) if row else False
    preview_label = "есть" if preview_on else "нет"
    new_kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="✏️ Редактировать",       callback_data=f"ch_ar_kw_edit:{chat_id}:{ar_id}")],
        [InlineKeyboardButton(text="🎛 Кнопки",              callback_data=f"ch_ar_kw_btns:{chat_id}:{ar_id}")],
        [InlineKeyboardButton(text=f"🎬 Медиа: {media_icon}", callback_data=f"ch_ar_kw_media:{chat_id}:{ar_id}")],
        [InlineKeyboardButton(text=f"👁 Превью: {preview_label}", callback_data=f"ch_ar_kw_preview:{chat_id}:{ar_id}")],
        [InlineKeyboardButton(text="🗑 Удалить",              callback_data=f"ch_ar_del:{chat_id}:{ar_id}")],
        [InlineKeyboardButton(text="◀️ Назад",               callback_data=f"ch_ar_kw_back:{chat_id}:{ar_id}")],
    ])
    try:
        await callback.message.edit_reply_markup(reply_markup=new_kb)
    except Exception:
        pass
        
    await callback.answer("Медиа: " + ("сверху ⬆️" if new_top else "снизу ⬇️"))


# ── Управление: Кнопки keyword-ответа ────────────────────────

@router.callback_query(F.data.startswith("ch_ar_kw_btns:"))
async def on_ch_ar_kw_btns(
    callback: CallbackQuery, state: FSMContext, platform_user: dict | None
):
    """Кнопки для keyword-ответа."""
    if not platform_user:
        return
    parts    = callback.data.split(":")
    chat_id  = int(parts[1])
    ar_id    = int(parts[2])
    owner_id = platform_user["user_id"]

    # Удаляем эхо-сообщение
    key = (owner_id, chat_id, ar_id)
    echo_id = _kw_echo_ids.pop(key, None)
    if echo_id:
        try:
            await callback.message.bot.delete_message(callback.message.chat.id, echo_id)
        except Exception:
            pass

    await state.set_state(MessagesFSM.waiting_for_autoreply_buttons)
    await state.update_data(chat_id=chat_id, owner_id=owner_id, ar_id=ar_id, prompt_mid=callback.message.message_id)
    await callback.message.edit_text(
        " Отправьте <b>кнопки</b>, которые будут добавлены к сообщению.\n\n"
        "🔗 <b>URL-кнопки</b>\n\n"
        "<b>Одна кнопка в ряду:</b>\n"
        "<code>Кнопка 1 — ссылка</code>\n"
        "<code>Кнопка 2 — ссылка</code>\n\n"
        "<b>Несколько кнопок в ряду:</b>\n"
        "<code>Кнопка 1 — ссылка | Кнопка 2 — ссылка</code>\n\n"
        "🎨 <b>Цветные кнопки (добавь emoji перед названием):</b>\n"
        "<code>🟦 Кнопка — ссылка</code> — синяя\n"
        "<code>🟩 Кнопка — ссылка</code> — зелёная\n"
        "<code>🟥 Кнопка — ссылка</code> — красная\n\n"
        "*** <b>Другие виды кнопок</b>\n\n"
        "<b>WebApp кнопки:</b>\n"
        "<code>Кнопка 1 — ссылка (webapp)</code>\n\n"
        "ℹ️ Нажмите, чтобы скопировать.",
        parse_mode="HTML",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="◀️ Назад", callback_data=f"ch_ar_view:{chat_id}:{ar_id}")],
        ]),
    )
    await callback.answer()


@router.message(MessagesFSM.waiting_for_autoreply_buttons)
async def on_ar_buttons_input(message: Message, state: FSMContext):
    import json as _json
    data     = await state.get_data()
    chat_id  = data["chat_id"]
    owner_id = data["owner_id"]
    ar_id    = data["ar_id"]
    raw      = (message.text or "").strip()
    prompt_mid = data.get("prompt_mid")

    if prompt_mid:
         try:
             await message.bot.delete_message(message.chat.id, prompt_mid)
         except Exception:
             pass

    if raw == "-":
        buttons_json = None
    else:
        # Формат: каждая строка — ряд кнопок.
        # Кнопки в ряду разделены " | ".
        # Текст и ссылка разделены " – " или " - ".
        rows = []
        for line in raw.splitlines():
            line = line.strip()
            if not line:
                continue
            row = []
            for btn_raw in line.split("|"):
                btn_raw = btn_raw.strip()
                # Поддерживаем em-dash (—), en-dash (–) и обычный дефис (-)
                sep = None
                if " — " in btn_raw:
                    sep = " — "
                elif " – " in btn_raw:
                    sep = " – "
                elif " - " in btn_raw:
                    sep = " - "
                elif "—" in btn_raw:
                    sep = "—"
                elif "–" in btn_raw:
                    sep = "–"
                elif "-" in btn_raw:
                    sep = "-"
                if sep:
                    idx = btn_raw.index(sep)
                    btn_text = btn_raw[:idx].strip()
                    btn_url  = btn_raw[idx + len(sep):].strip()
                    if btn_text and btn_url:
                        row.append({"text": btn_text, "url": btn_url})
            if row:
                rows.append(row)
        buttons_json = _json.dumps(rows, ensure_ascii=False) if rows else None

    await db.execute(
        "UPDATE autoreplies SET reply_buttons=$1 WHERE id=$2 AND owner_id=$3",
        buttons_json, ar_id, owner_id,
    )
    await state.clear()
    await _show_keyword_mgmt(message, chat_id, owner_id, ar_id)



# ── Управление: Превью keyword-ответа ─────────────────────────

@router.callback_query(F.data.startswith("ch_ar_kw_preview:"))
async def on_ch_ar_kw_preview(callback: CallbackQuery, platform_user: dict | None):
    """Превью ссылок для keyword-ответа."""
    if not platform_user:
        return
    parts    = callback.data.split(":")
    chat_id  = int(parts[1])
    ar_id    = int(parts[2])
    owner_id = platform_user["user_id"]
    row = await db.fetchrow(
        "SELECT reply_preview FROM autoreplies WHERE id=$1 AND owner_id=$2", ar_id, owner_id
    )
    new_val = not bool(row["reply_preview"] if row else False)
    await db.execute(
        "UPDATE autoreplies SET reply_preview=$1 WHERE id=$2 AND owner_id=$3",
        new_val, ar_id, owner_id,
    )
    await callback.answer("Превью: " + ("есть" if new_val else "нет"))
    await callback.message.delete()
    await _show_keyword_mgmt(callback.message, chat_id, owner_id, ar_id)


# ── Удаление keyword-ответа ────────────────────────────────────

@router.callback_query(F.data.startswith("ch_ar_del:"))
async def on_ch_ar_del(callback: CallbackQuery, platform_user: dict | None):
    if not platform_user:
        return
    parts    = callback.data.split(":")
    chat_id  = int(parts[1])
    ar_id    = int(parts[2])
    owner_id = platform_user["user_id"]

    # Удаляем эхо-сообщение панели управления, если есть
    key = (owner_id, chat_id, ar_id)
    echo_id = _kw_echo_ids.pop(key, None)
    if echo_id:
        try:
            await callback.message.bot.delete_message(callback.message.chat.id, echo_id)
        except Exception:
            pass

    await db.execute(
        "DELETE FROM autoreplies WHERE id=$1 AND owner_id=$2",
        ar_id, owner_id,
    )
    await callback.answer("🗑 Удалено")
    await _show_autoreply(callback, chat_id, owner_id)


