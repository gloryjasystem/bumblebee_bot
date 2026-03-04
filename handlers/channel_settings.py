"""
handlers/channel_settings.py — Полные настройки площадки:
  - Обработка заявок (авто/отложенное/капча)
  - Приветственное сообщение
  - Прощание
  - Языковые фильтры
  - RTL / иероглифы / без фото
  - Реакции на сообщения
  - Обратная связь
"""
import logging
from aiogram import Router, F, Bot
from aiogram.types import (
    Message, CallbackQuery, InlineKeyboardMarkup, InlineKeyboardButton
)
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup

import db.pool as db
from services.security import sanitize

logger = logging.getLogger(__name__)
router = Router()


class SettingsFSM(StatesGroup):
    # Обработка заявок
    waiting_for_delay       = State()
    # Капча
    waiting_for_captcha_text  = State()
    waiting_for_captcha_timer = State()
    # Приветствие
    waiting_for_welcome_text  = State()
    # Прощание
    waiting_for_farewell_text = State()
    # ЧС: загрузка файлов (бот-уровень)
    bs_bl_waiting_add_file  = State()
    bs_bl_waiting_del_file  = State()
    # База пользователей: ручное добавление/удаление
    bs_base_waiting_add     = State()
    bs_base_waiting_del     = State()


# ══════════════════════════════════════════════════════════════
# Обработка заявок
# ══════════════════════════════════════════════════════════════

# Цикл интервалов отложенного принятия (по ТЗ)
_DELAY_CYCLE = [0, 1, 5, 15, 30, 60, 180, 360, 720, 1080, 1440]


def _delay_label(minutes: int) -> str:
    if minutes == 0:
        return "ВЫКЛ 🔴"
    if minutes < 60:
        return f"{minutes} мин 🟡"
    hours = minutes // 60
    return f"{hours} ч 🟡"


def _next_delay(current: int) -> int:
    """Следующий интервал в цикле ТЗ."""
    try:
        idx = _DELAY_CYCLE.index(current)
    except ValueError:
        idx = 0
    return _DELAY_CYCLE[(idx + 1) % len(_DELAY_CYCLE)]


async def _show_requests_menu(callback: CallbackQuery, platform_user: dict, chat_id: int):
    """Рендерит экран Обработка заявок по ТЗ."""
    owner_id = platform_user["user_id"]
    ch = await db.fetchrow(
        "SELECT * FROM bot_chats WHERE owner_id=$1 AND chat_id=$2::bigint",
        owner_id, chat_id,
    )
    if not ch:
        await callback.answer("Площадка не найдена", show_alert=True)
        return

    # Кол-во ожидающих заявок
    pending = await db.fetchval(
        "SELECT COUNT(*) FROM join_requests WHERE owner_id=$1 AND chat_id=$2::bigint AND status='pending'",
        owner_id, chat_id,
    ) or 0

    auto   = ch["autoaccept"]
    delay  = ch["autoaccept_delay"] or 0

    auto_label  = "✅ Автопринятие: ВКЛ 🟢"  if auto else "☑️ Автопринятие: ВЫКЛ 🔴"
    delay_label = f"⏰ Отложенное: {_delay_label(delay)}"

    text = (
        "✅ <b>Обработка заявок</b>\n\n"
        f"🔍 Заявок в очереди: <b>{pending}</b>\n\n"
        "💡 <i>Автопринятие</i> — заявки одобряются автоматически.\n"
        "   Бот проверяет каждого по чёрному списку.\n\n"
        "⏰ <i>Отложенное принятие</i> — заявка принимается\n"
        "   через заданное время. Используется для прогрева."
    )

    await callback.message.edit_text(
        text,
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text=auto_label,   callback_data=f"req_auto_toggle:{chat_id}")],
            [
                InlineKeyboardButton(text="✔️ Принять всё",   callback_data=f"req_accept_all:{chat_id}"),
                InlineKeyboardButton(text="✖️ Отклонить всё", callback_data=f"req_decline_all:{chat_id}"),
            ],
            [InlineKeyboardButton(text=delay_label,  callback_data=f"req_delay_cycle:{chat_id}")],
            [InlineKeyboardButton(text="◀️ Назад",    callback_data=f"channel_by_chat:{chat_id}")],
        ]),
    )
    await callback.answer()


@router.callback_query(F.data.startswith("ch_requests:"))
async def on_requests_menu(callback: CallbackQuery, platform_user: dict | None):
    if not platform_user:
        return
    chat_id = int(callback.data.split(":")[1])
    await _show_requests_menu(callback, platform_user, chat_id)


@router.callback_query(F.data.startswith("req_auto_toggle:"))
async def on_req_auto_toggle(callback: CallbackQuery, platform_user: dict | None):
    if not platform_user:
        return
    chat_id = int(callback.data.split(":")[1])
    ch = await db.fetchrow(
        "SELECT autoaccept FROM bot_chats WHERE owner_id=$1 AND chat_id=$2::bigint",
        platform_user["user_id"], chat_id,
    )
    if not ch:
        return
    new_auto = not ch["autoaccept"]
    await db.execute(
        "UPDATE bot_chats SET autoaccept=$1 WHERE owner_id=$2 AND chat_id=$3::bigint",
        new_auto, platform_user["user_id"], chat_id,
    )
    await callback.answer("✅ ВКЛ" if new_auto else "🔴 ВЫКЛ")
    await _show_requests_menu(callback, platform_user, chat_id)


@router.callback_query(F.data.startswith("req_delay_cycle:"))
async def on_req_delay_cycle(callback: CallbackQuery, platform_user: dict | None):
    if not platform_user:
        return
    chat_id = int(callback.data.split(":")[1])
    ch = await db.fetchrow(
        "SELECT autoaccept_delay FROM bot_chats WHERE owner_id=$1 AND chat_id=$2::bigint",
        platform_user["user_id"], chat_id,
    )
    current = ch["autoaccept_delay"] or 0 if ch else 0
    new_delay = _next_delay(current)
    await db.execute(
        "UPDATE bot_chats SET autoaccept_delay=$1 WHERE owner_id=$2 AND chat_id=$3::bigint",
        new_delay, platform_user["user_id"], chat_id,
    )
    await callback.answer(f"⏰ {_delay_label(new_delay)}")
    await _show_requests_menu(callback, platform_user, chat_id)


@router.callback_query(F.data.startswith("req_accept_all:"))
async def on_req_accept_all(callback: CallbackQuery, bot: Bot, platform_user: dict | None):
    if not platform_user:
        return
    chat_id = int(callback.data.split(":")[1])
    owner_id = platform_user["user_id"]

    # Получаем токен дочернего бота
    bot_row = await db.fetchrow(
        """SELECT cb.token_encrypted FROM child_bots cb
           JOIN bot_chats bc ON bc.child_bot_id = cb.id
           WHERE bc.owner_id=$1 AND bc.chat_id=$2::bigint AND bc.is_active=true""",
        owner_id, chat_id,
    )

    pending = await db.fetch(
        "SELECT user_id FROM join_requests WHERE owner_id=$1 AND chat_id=$2::bigint AND status='pending'",
        owner_id, chat_id,
    )
    if not pending:
        await callback.answer("Нет заявок в очереди", show_alert=True)
        return

    from aiogram import Bot as AioBot
    from services.security import decrypt_token
    child_bot = AioBot(token=decrypt_token(bot_row["token_encrypted"])) if bot_row else bot

    approved = 0
    for row in pending:
        try:
            await child_bot.approve_chat_join_request(chat_id, row["user_id"])
            approved += 1
        except Exception:
            pass
    if bot_row:
        await child_bot.session.close()

    await db.execute(
        "UPDATE join_requests SET status='approved', resolved_at=now() "
        "WHERE owner_id=$1 AND chat_id=$2::bigint AND status='pending'",
        owner_id, chat_id,
    )
    await callback.answer(f"✔️ Принято: {approved}", show_alert=True)
    await _show_requests_menu(callback, platform_user, chat_id)


@router.callback_query(F.data.startswith("req_decline_all:"))
async def on_req_decline_all(callback: CallbackQuery, bot: Bot, platform_user: dict | None):
    if not platform_user:
        return
    chat_id = int(callback.data.split(":")[1])
    owner_id = platform_user["user_id"]

    bot_row = await db.fetchrow(
        """SELECT cb.token_encrypted FROM child_bots cb
           JOIN bot_chats bc ON bc.child_bot_id = cb.id
           WHERE bc.owner_id=$1 AND bc.chat_id=$2::bigint AND bc.is_active=true""",
        owner_id, chat_id,
    )

    pending = await db.fetch(
        "SELECT user_id FROM join_requests WHERE owner_id=$1 AND chat_id=$2::bigint AND status='pending'",
        owner_id, chat_id,
    )
    if not pending:
        await callback.answer("Нет заявок в очереди", show_alert=True)
        return

    from aiogram import Bot as AioBot
    from services.security import decrypt_token
    child_bot = AioBot(token=decrypt_token(bot_row["token_encrypted"])) if bot_row else bot

    declined = 0
    for row in pending:
        try:
            await child_bot.decline_chat_join_request(chat_id, row["user_id"])
            declined += 1
        except Exception:
            pass
    if bot_row:
        await child_bot.session.close()

    await db.execute(
        "UPDATE join_requests SET status='declined', resolved_at=now() "
        "WHERE owner_id=$1 AND chat_id=$2::bigint AND status='pending'",
        owner_id, chat_id,
    )
    await callback.answer(f"✖️ Отклонено: {declined}", show_alert=True)
    await _show_requests_menu(callback, platform_user, chat_id)



# ══════════════════════════════════════════════════════════════
# Настройки капчи
# ══════════════════════════════════════════════════════════════
def kb_captcha_settings(ch: dict) -> InlineKeyboardMarkup:
    chat_id = ch["chat_id"]
    delete_label = "✅ Авто-удаление сообщ" if ch.get("captcha_delete") else "☐ Авто-удаление сообщ"
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text=f"⏱ Таймер: {ch.get('captcha_timer',60)} сек", callback_data=f"captcha_timer:{chat_id}")],
        [InlineKeyboardButton(text="✏️ Изменить текст капчи", callback_data=f"captcha_text:{chat_id}")],
        [InlineKeyboardButton(text=delete_label,              callback_data=f"captcha_delete:{chat_id}")],
        [InlineKeyboardButton(text="◀️ Назад",                callback_data=f"ch_requests:{chat_id}")],
    ])


@router.callback_query(F.data.startswith("captcha_settings:"))
async def on_captcha_settings(callback: CallbackQuery, platform_user: dict | None):
    if not platform_user:
        return
    chat_id = int(callback.data.split(":")[1])
    ch = await db.fetchrow(
        "SELECT * FROM bot_chats WHERE owner_id=$1 AND chat_id=$2::bigint",
        platform_user["user_id"], chat_id,
    )
    if not ch:
        return

    tariff = platform_user["tariff"]
    pro_note = "" if tariff in ("pro", "business") else "\n\n⚠️ Авто-удаление сообщений капчи — только Про+"

    await callback.message.edit_text(
        f"🔑 <b>Настройки капчи</b>{pro_note}",
        reply_markup=kb_captcha_settings(dict(ch)),
    )
    await callback.answer()


@router.callback_query(F.data.startswith("captcha_delete:"))
async def on_captcha_delete_toggle(callback: CallbackQuery, platform_user: dict | None):
    if not platform_user:
        return
    if platform_user["tariff"] not in ("pro", "business"):
        await callback.answer("Авто-удаление сообщений капчи — только Про+", show_alert=True)
        return
    chat_id = int(callback.data.split(":")[1])
    ch = await db.fetchrow(
        "SELECT captcha_delete FROM bot_chats WHERE owner_id=$1 AND chat_id=$2::bigint",
        platform_user["user_id"], chat_id,
    )
    new_val = not (ch["captcha_delete"] if ch else False)
    await db.execute(
        "UPDATE bot_chats SET captcha_delete=$1 WHERE owner_id=$2 AND chat_id=$3::bigint",
        new_val, platform_user["user_id"], chat_id,
    )
    await callback.answer("✅ Изменено")
    # Обновляем экран
    callback.data = f"captcha_settings:{chat_id}"
    await on_captcha_settings(callback, platform_user)


@router.callback_query(F.data.startswith("captcha_timer:"))
async def on_captcha_timer(callback: CallbackQuery, state: FSMContext, platform_user: dict | None):
    if not platform_user:
        return
    chat_id = callback.data.split(":")[1]
    await state.set_state(SettingsFSM.waiting_for_captcha_timer)
    await state.update_data(chat_id=int(chat_id), owner_id=platform_user["user_id"])
    await callback.message.edit_text(
        "⏱ <b>Таймер капчи</b>\n\n"
        "Сколько секунд есть у пользователя для прохождения?\n"
        "Минимум 30, максимум 600 сек.",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="30 сек",  callback_data="timer_preset:30")],
            [InlineKeyboardButton(text="60 сек",  callback_data="timer_preset:60")],
            [InlineKeyboardButton(text="120 сек", callback_data="timer_preset:120")],
            [InlineKeyboardButton(text="🚫 Отмена", callback_data=f"captcha_settings:{chat_id}")],
        ]),
    )
    await callback.answer()


@router.callback_query(F.data.startswith("timer_preset:"))
async def on_timer_preset(callback: CallbackQuery, state: FSMContext):
    data = await state.get_data()
    secs = int(callback.data.split(":")[1])
    await db.execute(
        "UPDATE bot_chats SET captcha_timer=$1 WHERE owner_id=$2 AND chat_id=$3::bigint",
        secs, data["owner_id"], data["chat_id"],
    )
    await state.clear()
    await callback.answer(f"✅ Таймер: {secs} сек")
    callback.data = f"captcha_settings:{data['chat_id']}"
    fake_pu = {"user_id": data["owner_id"], "tariff": "pro"}
    await on_captcha_settings(callback, fake_pu)


@router.callback_query(F.data.startswith("captcha_text:"))
async def on_captcha_text(callback: CallbackQuery, state: FSMContext, platform_user: dict | None):
    if not platform_user:
        return
    chat_id = callback.data.split(":")[1]
    await state.set_state(SettingsFSM.waiting_for_captcha_text)
    await state.update_data(chat_id=int(chat_id), owner_id=platform_user["user_id"])
    await callback.message.edit_text(
        "✏️ <b>Текст капчи</b>\n\n"
        "Отправьте новый текст. Можно использовать переменные:\n"
        "• <code>{name}</code> — имя пользователя\n"
        "• <code>{channel}</code> — название канала",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="🚫 Отмена", callback_data=f"captcha_settings:{chat_id}")]
        ]),
    )
    await callback.answer()


@router.message(SettingsFSM.waiting_for_captcha_text)
async def on_captcha_text_input(message: Message, state: FSMContext):
    data = await state.get_data()
    text = sanitize(message.text, max_len=512)
    await db.execute(
        "UPDATE bot_chats SET captcha_text=$1 WHERE owner_id=$2 AND chat_id=$3::bigint",
        text, data["owner_id"], data["chat_id"],
    )
    await state.clear()
    await message.answer("✅ Текст капчи сохранён")




# ── Приветствие ───────────────────────────────────────────────
@router.callback_query(F.data.startswith("welcome_set:"))
async def on_welcome_set(callback: CallbackQuery, state: FSMContext, platform_user: dict | None):
    if not platform_user:
        return
    chat_id = callback.data.split(":")[1]
    await state.set_state(SettingsFSM.waiting_for_welcome_text)
    await state.update_data(chat_id=int(chat_id), owner_id=platform_user["user_id"])

    ch = await db.fetchrow(
        "SELECT welcome_text FROM bot_chats WHERE owner_id=$1 AND chat_id=$2::bigint",
        platform_user["user_id"], int(chat_id),
    )
    current = f"\n\nТекущий текст:\n<i>{ch['welcome_text'][:200]}</i>" if ch and ch.get("welcome_text") else ""

    await callback.message.edit_text(
        f"👋 <b>Приветствие</b>{current}\n\n"
        "Отправьте новый текст приветствия.\n"
        "Переменные: <code>{name}</code>, <code>{channel}</code>",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="🗑 Удалить",  callback_data=f"welcome_del:{chat_id}")],
            [InlineKeyboardButton(text="🚫 Отмена",   callback_data=f"ch_messages:{chat_id}")],
        ]),
    )
    await callback.answer()


@router.message(SettingsFSM.waiting_for_welcome_text)
async def on_welcome_input(message: Message, state: FSMContext):
    data = await state.get_data()
    text = sanitize(message.text or message.caption or "", max_len=1024)
    if data.get("mode") == "bot" and data.get("child_bot_id"):
        await db.execute(
            "UPDATE bot_chats SET welcome_text=$1 WHERE child_bot_id=$2 AND owner_id=$3",
            text, data["child_bot_id"], data["owner_id"],
        )
    else:
        await db.execute(
            "UPDATE bot_chats SET welcome_text=$1 WHERE owner_id=$2 AND chat_id=$3::bigint",
            text, data["owner_id"], data["chat_id"],
        )
    await state.clear()
    await message.answer("✅ Приветствие сохранено")


@router.callback_query(F.data.startswith("welcome_del:"))
async def on_welcome_del(callback: CallbackQuery, platform_user: dict | None):
    if not platform_user:
        return
    chat_id = int(callback.data.split(":")[1])
    await db.execute(
        "UPDATE bot_chats SET welcome_text=NULL WHERE owner_id=$1 AND chat_id=$2::bigint",
        platform_user["user_id"], chat_id,
    )
    await callback.answer("✅ Приветствие удалено")
    callback.data = f"ch_messages:{chat_id}"
    await on_messages_menu(callback, platform_user)


# ── Прощание ─────────────────────────────────────────────────
@router.callback_query(F.data.startswith("farewell_set:"))
async def on_farewell_set(callback: CallbackQuery, state: FSMContext, platform_user: dict | None):
    if not platform_user:
        return
    chat_id = callback.data.split(":")[1]
    await state.set_state(SettingsFSM.waiting_for_farewell_text)
    await state.update_data(chat_id=int(chat_id), owner_id=platform_user["user_id"])
    await callback.message.edit_text(
        "👋 <b>Прощание</b>\n\n"
        "Сообщение при отписке (отправляется в личку).\n"
        "Переменные: <code>{name}</code>, <code>{channel}</code>",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="🗑 Удалить",  callback_data=f"farewell_del:{chat_id}")],
            [InlineKeyboardButton(text="🚫 Отмена",   callback_data=f"ch_messages:{chat_id}")],
        ]),
    )
    await callback.answer()


@router.message(SettingsFSM.waiting_for_farewell_text)
async def on_farewell_input(message: Message, state: FSMContext):
    data = await state.get_data()
    text = sanitize(message.text or "", max_len=512)
    if data.get("mode") == "bot" and data.get("child_bot_id"):
        await db.execute(
            "UPDATE bot_chats SET farewell_text=$1 WHERE child_bot_id=$2 AND owner_id=$3",
            text, data["child_bot_id"], data["owner_id"],
        )
    else:
        await db.execute(
            "UPDATE bot_chats SET farewell_text=$1 WHERE owner_id=$2 AND chat_id=$3::bigint",
            text, data["owner_id"], data["chat_id"],
        )
    await state.clear()
    await message.answer("✅ Прощание сохранено")


@router.callback_query(F.data.startswith("farewell_del:"))
async def on_farewell_del(callback: CallbackQuery, platform_user: dict | None):
    if not platform_user:
        return
    chat_id = int(callback.data.split(":")[1])
    await db.execute(
        "UPDATE bot_chats SET farewell_text=NULL WHERE owner_id=$1 AND chat_id=$2::bigint",
        platform_user["user_id"], chat_id,
    )
    await callback.answer("✅ Прощание удалено")
    callback.data = f"ch_messages:{chat_id}"
    await on_messages_menu(callback, platform_user)


# ══════════════════════════════════════════════════════════════
# Реакции
# ══════════════════════════════════════════════════════════════
REACTIONS_POOL = ["👍", "🔥", "❤️", "🎉", "😂", "🤩", "😎", "💯", "⚡", "🤝", "💎", "🦋"]


def kb_reactions(current: list, chat_id: int, tariff: str) -> InlineKeyboardMarkup:
    max_reactions = 3 if tariff in ("pro", "business") else 1
    buttons = []
    row = []
    for emoji in REACTIONS_POOL:
        mark = "✅" if emoji in current else ""
        row.append(InlineKeyboardButton(
            text=f"{mark}{emoji}", callback_data=f"reaction_toggle:{chat_id}:{emoji}",
        ))
        if len(row) == 4:
            buttons.append(row)
            row = []
    if row:
        buttons.append(row)
    note = f"(макс. {max_reactions}: {', '.join(current) if current else 'не выбрано'})"
    buttons.append([InlineKeyboardButton(text=note, callback_data="noop")])
    buttons.append([InlineKeyboardButton(text="◀️ Назад", callback_data=f"ch_messages:{chat_id}")])
    return InlineKeyboardMarkup(inline_keyboard=buttons)


@router.callback_query(F.data.startswith("reactions_set:"))
async def on_reactions_set(callback: CallbackQuery, platform_user: dict | None):
    if not platform_user:
        return
    chat_id = int(callback.data.split(":")[1])
    ch = await db.fetchrow(
        "SELECT reaction_emojis FROM bot_chats WHERE owner_id=$1 AND chat_id=$2::bigint",
        platform_user["user_id"], chat_id,
    )
    current = list(ch["reaction_emojis"] or []) if ch else []
    tariff = platform_user["tariff"]
    await callback.message.edit_text(
        "😄 <b>Реакции</b>\n\nВыберите эмодзи для авто-реакций на сообщения:",
        reply_markup=kb_reactions(current, chat_id, tariff),
    )
    await callback.answer()


@router.callback_query(F.data.startswith("reaction_toggle:"))
async def on_reaction_toggle(callback: CallbackQuery, platform_user: dict | None):
    if not platform_user:
        return
    parts = callback.data.split(":")
    chat_id, emoji = int(parts[1]), parts[2]
    max_r = 3 if platform_user["tariff"] in ("pro", "business") else 1

    ch = await db.fetchrow(
        "SELECT reaction_emojis FROM bot_chats WHERE owner_id=$1 AND chat_id=$2::bigint",
        platform_user["user_id"], chat_id,
    )
    current = list(ch["reaction_emojis"] or []) if ch else []

    if emoji in current:
        current.remove(emoji)
    else:
        if len(current) >= max_r:
            await callback.answer(f"Максимум {max_r} реакции для вашего тарифа", show_alert=True)
            return
        current.append(emoji)

    await db.execute(
        "UPDATE bot_chats SET reaction_emojis=$1::text[] WHERE owner_id=$2 AND chat_id=$3::bigint",
        current, platform_user["user_id"], chat_id,
    )
    await callback.answer()
    callback.data = f"reactions_set:{chat_id}"
    await on_reactions_set(callback, platform_user)


# ══════════════════════════════════════════════════════════════
# Языковые фильтры и фильтры имён
# ══════════════════════════════════════════════════════════════
LANGUAGE_OPTIONS = {
    "ar": "🇸🇦 Арабский",
    "zh": "🇨🇳 Китайский",
    "fa": "🇮🇷 Фарси",
    "tr": "🇹🇷 Турецкий",
    "hi": "🇮🇳 Хинди",
    "uk": "🇺🇦 Украинский",
    "en": "🇺🇸 Английский",
    "de": "🇩🇪 Немецкий",
}


def kb_protection(ch: dict, blocked_langs: list) -> InlineKeyboardMarkup:
    chat_id = ch["chat_id"]
    rtl      = "✅" if ch.get("filter_rtl") else "☐"
    hiero    = "✅" if ch.get("filter_hieroglyph") else "☐"
    no_photo = "✅" if ch.get("filter_no_photo") else "☐"
    buttons = [
        [InlineKeyboardButton(text=f"{rtl} Фильтр RTL-имён",         callback_data=f"filter_rtl:{chat_id}")],
        [InlineKeyboardButton(text=f"{hiero} Фильтр иероглифов",      callback_data=f"filter_hier:{chat_id}")],
        [InlineKeyboardButton(text=f"{no_photo} Фильтр без фото",     callback_data=f"filter_photo:{chat_id}")],
        [InlineKeyboardButton(text="🌍 Языковые фильтры",              callback_data=f"lang_filters:{chat_id}")],
        [InlineKeyboardButton(text="◀️ Назад",                         callback_data=f"channel_by_chat:{chat_id}")],
    ]
    return InlineKeyboardMarkup(inline_keyboard=buttons)


@router.callback_query(F.data.startswith("ch_protection:"))
async def on_protection_menu_main(callback: CallbackQuery, platform_user: dict | None):
    """Перенаправляем на blacklist если нажали кнопку ЧС, иначе — настройки фильтров."""
    # Уже был on_protection_menu в blacklist.py — этот для настроек, не ЧС.
    # Разграничение: blacklist.py обрабатывает ch_protection через свой хендлер.
    # Здесь оставляем пустой pass — blacklist.py первее в роутере.
    pass


@router.callback_query(F.data.startswith("filter_rtl:"))
async def on_filter_rtl(callback: CallbackQuery, platform_user: dict | None):
    if not platform_user:
        return
    chat_id = int(callback.data.split(":")[1])
    ch = await db.fetchrow(
        "SELECT filter_rtl FROM bot_chats WHERE owner_id=$1 AND chat_id=$2::bigint",
        platform_user["user_id"], chat_id,
    )
    new_val = not (ch["filter_rtl"] if ch else False)
    await db.execute(
        "UPDATE bot_chats SET filter_rtl=$1 WHERE owner_id=$2 AND chat_id=$3::bigint",
        new_val, platform_user["user_id"], chat_id,
    )
    await callback.answer("RTL-фильтр: " + ("✅ Вкл" if new_val else "☐ Выкл"))


@router.callback_query(F.data.startswith("filter_hier:"))
async def on_filter_hier(callback: CallbackQuery, platform_user: dict | None):
    if not platform_user:
        return
    chat_id = int(callback.data.split(":")[1])
    ch = await db.fetchrow(
        "SELECT filter_hieroglyph FROM bot_chats WHERE owner_id=$1 AND chat_id=$2::bigint",
        platform_user["user_id"], chat_id,
    )
    new_val = not (ch["filter_hieroglyph"] if ch else False)
    await db.execute(
        "UPDATE bot_chats SET filter_hieroglyph=$1 WHERE owner_id=$2 AND chat_id=$3::bigint",
        new_val, platform_user["user_id"], chat_id,
    )
    await callback.answer("Иероглифы: " + ("✅ Вкл" if new_val else "☐ Выкл"))


@router.callback_query(F.data.startswith("filter_photo:"))
async def on_filter_photo(callback: CallbackQuery, platform_user: dict | None):
    if not platform_user:
        return
    chat_id = int(callback.data.split(":")[1])
    ch = await db.fetchrow(
        "SELECT filter_no_photo FROM bot_chats WHERE owner_id=$1 AND chat_id=$2::bigint",
        platform_user["user_id"], chat_id,
    )
    new_val = not (ch["filter_no_photo"] if ch else False)
    await db.execute(
        "UPDATE bot_chats SET filter_no_photo=$1 WHERE owner_id=$2 AND chat_id=$3::bigint",
        new_val, platform_user["user_id"], chat_id,
    )
    await callback.answer("Без фото: " + ("✅ Вкл" if new_val else "☐ Выкл"))


# ── Языковые фильтры ─────────────────────────────────────────
@router.callback_query(F.data.startswith("lang_filters:"))
async def on_lang_filters(callback: CallbackQuery, platform_user: dict | None):
    if not platform_user:
        return
    chat_id = int(callback.data.split(":")[1])
    blocked = await db.fetch(
        "SELECT language_code FROM language_filters WHERE owner_id=$1 AND chat_id=$2::bigint",
        platform_user["user_id"], chat_id,
    )
    blocked_codes = {r["language_code"] for r in blocked}

    buttons = []
    for code, label in LANGUAGE_OPTIONS.items():
        mark = "🚫" if code in blocked_codes else "🌍"
        buttons.append([InlineKeyboardButton(
            text=f"{mark} {label}",
            callback_data=f"lang_toggle:{chat_id}:{code}",
        )])
    buttons.append([InlineKeyboardButton(text="◀️ Назад", callback_data=f"channel_by_chat:{chat_id}")])

    await callback.message.edit_text(
        "🌍 <b>Языковые фильтры</b>\n\n"
        "🚫 — заблокирован (бот отклоняет заявки с этим языком)\n"
        "🌍 — разрешён",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=buttons),
    )
    await callback.answer()


@router.callback_query(F.data.startswith("lang_toggle:"))
async def on_lang_toggle(callback: CallbackQuery, platform_user: dict | None):
    if not platform_user:
        return
    parts = callback.data.split(":")
    chat_id, code = int(parts[1]), parts[2]

    exists = await db.fetchrow(
        "SELECT 1 FROM language_filters WHERE owner_id=$1 AND chat_id=$2::bigint AND language_code=$3",
        platform_user["user_id"], chat_id, code,
    )
    if exists:
        await db.execute(
            "DELETE FROM language_filters WHERE owner_id=$1 AND chat_id=$2::bigint AND language_code=$3",
            platform_user["user_id"], chat_id, code,
        )
        await callback.answer(f"✅ {LANGUAGE_OPTIONS.get(code,'?')} разрешён")
    else:
        await db.execute(
            "INSERT INTO language_filters (owner_id, chat_id, language_code) VALUES ($1,$2,$3) "
            "ON CONFLICT DO NOTHING",
            platform_user["user_id"], chat_id, code,
        )
        await callback.answer(f"🚫 {LANGUAGE_OPTIONS.get(code,'?')} заблокирован")
    # Обновляем экран
    callback.data = f"lang_filters:{chat_id}"
    await on_lang_filters(callback, platform_user)


# ══════════════════════════════════════════════════════════════
# Статистика площадки
# ══════════════════════════════════════════════════════════════
@router.callback_query(F.data.startswith("ch_stats:"))
async def on_ch_stats(callback: CallbackQuery, platform_user: dict | None):
    if not platform_user:
        return
    chat_id = int(callback.data.split(":")[1])
    owner_id = platform_user["user_id"]

    total   = await db.fetchval(
        "SELECT COUNT(*) FROM bot_users WHERE owner_id=$1 AND chat_id=$2::bigint AND is_active=true",
        owner_id, chat_id,
    )
    active_bot = await db.fetchval(
        "SELECT COUNT(*) FROM bot_users WHERE owner_id=$1 AND chat_id=$2::bigint "
        "AND is_active=true AND bot_activated=true",
        owner_id, chat_id,
    )
    premium = await db.fetchval(
        "SELECT COUNT(*) FROM bot_users WHERE owner_id=$1 AND chat_id=$2::bigint "
        "AND is_active=true AND is_premium=true",
        owner_id, chat_id,
    )
    today = await db.fetchval(
        "SELECT COUNT(*) FROM bot_users WHERE owner_id=$1 AND chat_id=$2::bigint "
        "AND joined_at > now() - interval '24 hours'",
        owner_id, chat_id,
    )
    week = await db.fetchval(
        "SELECT COUNT(*) FROM bot_users WHERE owner_id=$1 AND chat_id=$2::bigint "
        "AND joined_at > now() - interval '7 days'",
        owner_id, chat_id,
    )
    bl_count = await db.fetchval(
        "SELECT COUNT(*) FROM blacklist WHERE owner_id=$1", owner_id,
    )

    await callback.message.edit_text(
        "📊 <b>Статистика площадки</b>\n\n"
        f"👥 <b>Участники</b>\n"
        f"├ Всего в базе: {total:,}\n"
        f"├ Доступны для рассылки: {active_bot:,}\n"
        f"├ Premium-аккаунты: {premium:,}\n"
        f"├ Вступило сегодня: {today:,}\n"
        f"└ За 7 дней: {week:,}\n\n"
        f"🛡 Чёрный список (всего): {bl_count:,}",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="◀️ Назад", callback_data=f"channel_by_chat:{chat_id}")]
        ]),
    )
    await callback.answer()


# ══════════════════════════════════════════════════════════════
# ⚙️ Управление площадкой (ch_settings:{ch_id}) — DB id
# ══════════════════════════════════════════════════════════════
TIMEZONES = [
    ("UTC+3 Москва",   "Europe/Moscow"),
    ("UTC+0 Лондон",   "UTC"),
    ("UTC+1 Берлин",   "Europe/Berlin"),
    ("UTC+2 Киев",     "Europe/Kiev"),
    ("UTC+5 Екатеринбург", "Asia/Yekaterinburg"),
    ("UTC+6 Омск",     "Asia/Omsk"),
    ("UTC+7 Красноярск", "Asia/Krasnoyarsk"),
    ("UTC+8 Иркутск",  "Asia/Irkutsk"),
    ("UTC+9 Якутск",   "Asia/Yakutsk"),
    ("UTC+10 Владивосток", "Asia/Vladivostok"),
]


@router.callback_query(F.data.startswith("ch_settings:"))
async def on_ch_settings(callback: CallbackQuery, platform_user: dict | None):
    """Управление конкретной площадкой (по DB id)."""
    if not platform_user:
        return
    ch_id = int(callback.data.split(":")[1])
    ch = await db.fetchrow(
        """SELECT bc.*, cb.bot_username
           FROM bot_chats bc
           LEFT JOIN child_bots cb ON bc.child_bot_id = cb.id
           WHERE bc.id=$1 AND bc.owner_id=$2""",
        ch_id, platform_user["user_id"],
    )
    if not ch:
        await callback.answer("Площадка не найдена", show_alert=True)
        return

    status_icon = "🟢 Активна" if ch["is_active"] else "🔴 Остановлена"
    tz = ch.get("timezone") or "UTC"
    added = ch["added_at"].strftime("%d.%m.%Y") if ch.get("added_at") else "—"
    chat_id = ch["chat_id"]

    toggle_text = "⏸ Остановить" if ch["is_active"] else "▶️ Запустить"

    await callback.message.edit_text(
        f"⚙️ <b>Управление площадкой</b>\n\n"
        f"📢 {ch['chat_title']}\n"
        f"🤖 Бот: @{ch['bot_username'] or '—'}\n"
        f"📅 Подключена: {added}\n"
        f"🕐 Часовой пояс: {tz}\n"
        f"📡 Статус: {status_icon}",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text=toggle_text, callback_data=f"ch_toggle:{ch_id}")],
            [InlineKeyboardButton(text="🕐 Часовой пояс", callback_data=f"ch_tz:{ch_id}")],
            [InlineKeyboardButton(text="📊 Статистика",   callback_data=f"ch_stats:{chat_id}")],
            [InlineKeyboardButton(text="◀️ Назад",         callback_data=f"channel:{ch_id}")],
        ]),
    )
    await callback.answer()


@router.callback_query(F.data.startswith("ch_tz:"))
async def on_ch_tz(callback: CallbackQuery, platform_user: dict | None):
    if not platform_user:
        return
    ch_id = int(callback.data.split(":")[1])
    buttons = [
        [InlineKeyboardButton(text=label, callback_data=f"ch_tz_set:{ch_id}:{tz}")]
        for label, tz in TIMEZONES
    ]
    buttons.append([InlineKeyboardButton(text="◀️ Назад", callback_data=f"ch_settings:{ch_id}")])
    await callback.message.edit_text(
        "🕐 <b>Выберите часовой пояс</b>",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=buttons),
    )
    await callback.answer()


@router.callback_query(F.data.startswith("ch_tz_set:"))
async def on_ch_tz_set(callback: CallbackQuery, platform_user: dict | None):
    if not platform_user:
        return
    parts = callback.data.split(":")
    ch_id = int(parts[1])
    tz = ":".join(parts[2:])
    await db.execute(
        "UPDATE bot_chats SET timezone=$1 WHERE id=$2 AND owner_id=$3",
        tz, ch_id, platform_user["user_id"],
    )
    await callback.answer(f"✅ Часовой пояс: {tz}")
    # Вернуться в управление
    fake_cb_data = f"ch_settings:{ch_id}"
    await callback.message.edit_text(
        "✅ Часовой пояс обновлён.",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="◀️ Назад", callback_data=f"ch_settings:{ch_id}")],
        ]),
    )


# ══════════════════════════════════════════════════════════════
# ██ БОТ-УРОВЕНЬ: bs_* handlers (применяют к ВСЕМ каналам бота)
# ══════════════════════════════════════════════════════════════

async def _get_bot_first_chat(owner_id: int, child_bot_id: int):
    """Возвращает первый активный bot_chats для чтения текущих настроек."""
    return await db.fetchrow(
        "SELECT * FROM bot_chats WHERE child_bot_id=$1 AND owner_id=$2 AND is_active=true LIMIT 1",
        child_bot_id, owner_id,
    )


# ── Обработка заявок ─────────────────────────────────────────
async def _show_bs_requests(callback: CallbackQuery, platform_user: dict, child_bot_id: int):
    owner_id = platform_user["user_id"]
    ch = await _get_bot_first_chat(owner_id, child_bot_id)
    if not ch:
        await callback.answer("Нет активных площадок у бота", show_alert=True)
        return
    pending = await db.fetchval(
        """SELECT COUNT(*) FROM join_requests jr
           JOIN bot_chats bc ON jr.chat_id=bc.chat_id AND jr.owner_id=bc.owner_id
           WHERE bc.child_bot_id=$1 AND bc.owner_id=$2 AND jr.status='pending'""",
        child_bot_id, owner_id,
    ) or 0
    auto  = ch["autoaccept"]
    delay = ch["autoaccept_delay"] or 0
    auto_label  = "✅ Автопринятие: ВКЛ 🟢" if auto else "☑️ Автопринятие: ВЫКЛ 🔴"
    delay_label = f"⏰ Отложенное: {_delay_label(delay)}"
    await callback.message.edit_text(
        "✅ <b>Обработка заявок</b> (все площадки)\n\n"
        f"🔍 Заявок в очереди: <b>{pending}</b>\n\n"
        "💡 <i>Настройки применяются ко всем каналам бота.</i>",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text=auto_label,           callback_data=f"bs_req_auto:{child_bot_id}")],
            [
                InlineKeyboardButton(text="✔️ Принять всё",  callback_data=f"bs_req_accept_all:{child_bot_id}"),
                InlineKeyboardButton(text="✖️ Отклонить всё",callback_data=f"bs_req_decline_all:{child_bot_id}"),
            ],
            [InlineKeyboardButton(text=delay_label,          callback_data=f"bs_req_delay:{child_bot_id}")],
            [InlineKeyboardButton(text="◀️ Назад",            callback_data=f"bot_settings:{child_bot_id}")],
        ]),
    )
    await callback.answer()


@router.callback_query(F.data.startswith("bs_requests:"))
async def on_bs_requests(callback: CallbackQuery, platform_user: dict | None):
    if not platform_user:
        return
    await _show_bs_requests(callback, platform_user, int(callback.data.split(":")[1]))


@router.callback_query(F.data.startswith("bs_req_auto:"))
async def on_bs_req_auto(callback: CallbackQuery, platform_user: dict | None):
    if not platform_user:
        return
    child_bot_id = int(callback.data.split(":")[1])
    owner_id = platform_user["user_id"]
    ch = await _get_bot_first_chat(owner_id, child_bot_id)
    if not ch:
        return
    new_val = not ch["autoaccept"]
    await db.execute(
        "UPDATE bot_chats SET autoaccept=$1 WHERE child_bot_id=$2 AND owner_id=$3",
        new_val, child_bot_id, owner_id,
    )
    await callback.answer("✅ ВКЛ" if new_val else "🔴 ВЫКЛ")
    await _show_bs_requests(callback, platform_user, child_bot_id)


@router.callback_query(F.data.startswith("bs_req_delay:"))
async def on_bs_req_delay(callback: CallbackQuery, platform_user: dict | None):
    if not platform_user:
        return
    child_bot_id = int(callback.data.split(":")[1])
    owner_id = platform_user["user_id"]
    ch = await _get_bot_first_chat(owner_id, child_bot_id)
    current = ch["autoaccept_delay"] or 0 if ch else 0
    new_delay = _next_delay(current)
    await db.execute(
        "UPDATE bot_chats SET autoaccept_delay=$1 WHERE child_bot_id=$2 AND owner_id=$3",
        new_delay, child_bot_id, owner_id,
    )
    await callback.answer(f"⏰ {_delay_label(new_delay)}")
    await _show_bs_requests(callback, platform_user, child_bot_id)


@router.callback_query(F.data.startswith("bs_req_accept_all:"))
async def on_bs_req_accept_all(callback: CallbackQuery, bot: Bot, platform_user: dict | None):
    if not platform_user:
        return
    child_bot_id = int(callback.data.split(":")[1])
    owner_id = platform_user["user_id"]
    from aiogram import Bot as AioBot
    from services.security import decrypt_token
    bot_row = await db.fetchrow(
        "SELECT token_encrypted FROM child_bots WHERE id=$1 AND owner_id=$2", child_bot_id, owner_id)
    chats = await db.fetch(
        "SELECT chat_id FROM bot_chats WHERE child_bot_id=$1 AND owner_id=$2 AND is_active=true",
        child_bot_id, owner_id)
    child_bot_instance = AioBot(token=decrypt_token(bot_row["token_encrypted"])) if bot_row else bot
    approved = 0
    for chat_row in chats:
        pending = await db.fetch(
            "SELECT user_id FROM join_requests WHERE owner_id=$1 AND chat_id=$2::bigint AND status='pending'",
            owner_id, chat_row["chat_id"])
        for row in pending:
            try:
                await child_bot_instance.approve_chat_join_request(chat_row["chat_id"], row["user_id"])
                approved += 1
            except Exception:
                pass
        if pending:
            await db.execute(
                "UPDATE join_requests SET status='approved', resolved_at=now() "
                "WHERE owner_id=$1 AND chat_id=$2::bigint AND status='pending'",
                owner_id, chat_row["chat_id"])
    if bot_row:
        await child_bot_instance.session.close()
    await callback.answer(f"✔️ Принято: {approved}", show_alert=True)
    await _show_bs_requests(callback, platform_user, child_bot_id)


@router.callback_query(F.data.startswith("bs_req_decline_all:"))
async def on_bs_req_decline_all(callback: CallbackQuery, bot: Bot, platform_user: dict | None):
    if not platform_user:
        return
    child_bot_id = int(callback.data.split(":")[1])
    owner_id = platform_user["user_id"]
    from aiogram import Bot as AioBot
    from services.security import decrypt_token
    bot_row = await db.fetchrow(
        "SELECT token_encrypted FROM child_bots WHERE id=$1 AND owner_id=$2", child_bot_id, owner_id)
    chats = await db.fetch(
        "SELECT chat_id FROM bot_chats WHERE child_bot_id=$1 AND owner_id=$2 AND is_active=true",
        child_bot_id, owner_id)
    child_bot_instance = AioBot(token=decrypt_token(bot_row["token_encrypted"])) if bot_row else bot
    declined = 0
    for chat_row in chats:
        pending = await db.fetch(
            "SELECT user_id FROM join_requests WHERE owner_id=$1 AND chat_id=$2::bigint AND status='pending'",
            owner_id, chat_row["chat_id"])
        for row in pending:
            try:
                await child_bot_instance.decline_chat_join_request(chat_row["chat_id"], row["user_id"])
                declined += 1
            except Exception:
                pass
        if pending:
            await db.execute(
                "UPDATE join_requests SET status='declined', resolved_at=now() "
                "WHERE owner_id=$1 AND chat_id=$2::bigint AND status='pending'",
                owner_id, chat_row["chat_id"])
    if bot_row:
        await child_bot_instance.session.close()
    await callback.answer(f"✖️ Отклонено: {declined}", show_alert=True)
    await _show_bs_requests(callback, platform_user, child_bot_id)


# ── Сообщения ────────────────────────────────────────────────
@router.callback_query(F.data.startswith("bs_messages:"))
async def on_bs_messages(callback: CallbackQuery, platform_user: dict | None):
    if not platform_user:
        return
    child_bot_id = int(callback.data.split(":")[1])
    owner_id = platform_user["user_id"]
    chats = await db.fetch(
        "SELECT chat_id, chat_title FROM bot_chats WHERE child_bot_id=$1 AND owner_id=$2 AND is_active=true",
        child_bot_id, owner_id,
    )
    if not chats:
        await callback.answer("Нет активных площадок у бота", show_alert=True)
        return
    buttons = [[InlineKeyboardButton(
        text=f"📍 {ch['chat_title'] or ch['chat_id']}",
        callback_data=f"ch_messages:{ch['chat_id']}",
    )] for ch in chats]
    buttons.append([InlineKeyboardButton(text="◀️ Назад", callback_data=f"bot_settings:{child_bot_id}")])
    await callback.message.edit_text(
        "💬 <b>Сообщения</b>\n\nВыберите площадку:",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=buttons),
    )
    await callback.answer()


@router.callback_query(F.data.startswith("bs_welcome:"))
async def on_bs_welcome(callback: CallbackQuery, state: FSMContext, platform_user: dict | None):
    if not platform_user:
        return
    child_bot_id = int(callback.data.split(":")[1])
    owner_id = platform_user["user_id"]
    ch = await _get_bot_first_chat(owner_id, child_bot_id)
    current = f"\n\nТекущий текст:\n<i>{ch['welcome_text'][:200]}</i>" if ch and ch.get("welcome_text") else ""
    await state.set_state(SettingsFSM.waiting_for_welcome_text)
    await state.update_data(child_bot_id=child_bot_id, owner_id=owner_id, mode="bot")
    await callback.message.edit_text(
        f"👋 <b>Приветствие</b>{current}\n\n"
        "Отправьте новый текст — он применится ко всем площадкам бота.\n"
        "Переменные: <code>{name}</code>, <code>{channel}</code>",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="🗑 Удалить", callback_data=f"bs_welcome_del:{child_bot_id}")],
            [InlineKeyboardButton(text="🚫 Отмена",  callback_data=f"bs_messages:{child_bot_id}")],
        ]),
    )
    await callback.answer()


@router.callback_query(F.data.startswith("bs_welcome_del:"))
async def on_bs_welcome_del(callback: CallbackQuery, platform_user: dict | None):
    if not platform_user:
        return
    child_bot_id = int(callback.data.split(":")[1])
    await db.execute(
        "UPDATE bot_chats SET welcome_text=NULL WHERE child_bot_id=$1 AND owner_id=$2",
        child_bot_id, platform_user["user_id"])
    await callback.answer("✅ Приветствие удалено")
    callback.data = f"bs_messages:{child_bot_id}"
    await on_bs_messages(callback, platform_user)


@router.callback_query(F.data.startswith("bs_farewell:"))
async def on_bs_farewell(callback: CallbackQuery, state: FSMContext, platform_user: dict | None):
    if not platform_user:
        return
    child_bot_id = int(callback.data.split(":")[1])
    owner_id = platform_user["user_id"]
    await state.set_state(SettingsFSM.waiting_for_farewell_text)
    await state.update_data(child_bot_id=child_bot_id, owner_id=owner_id, mode="bot")
    await callback.message.edit_text(
        "👋 <b>Прощание</b>\n\nПрименяется ко всем площадкам бота.\n"
        "Переменные: <code>{name}</code>, <code>{channel}</code>",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="🗑 Удалить", callback_data=f"bs_farewell_del:{child_bot_id}")],
            [InlineKeyboardButton(text="🚫 Отмена",  callback_data=f"bs_messages:{child_bot_id}")],
        ]),
    )
    await callback.answer()


@router.callback_query(F.data.startswith("bs_farewell_del:"))
async def on_bs_farewell_del(callback: CallbackQuery, platform_user: dict | None):
    if not platform_user:
        return
    child_bot_id = int(callback.data.split(":")[1])
    await db.execute(
        "UPDATE bot_chats SET farewell_text=NULL WHERE child_bot_id=$1 AND owner_id=$2",
        child_bot_id, platform_user["user_id"])
    await callback.answer("✅ Прощание удалено")
    callback.data = f"bs_messages:{child_bot_id}"
    await on_bs_messages(callback, platform_user)


# ── Защита ────────────────────────────────────────────────────
async def _show_bs_protection(callback: CallbackQuery, platform_user: dict, child_bot_id: int):
    owner_id = platform_user["user_id"]
    ch = await _get_bot_first_chat(owner_id, child_bot_id)
    if not ch:
        await callback.answer("Нет активных площадок", show_alert=True)
        return
    rtl      = "✅" if ch.get("filter_rtl")       else "☐"
    hiero    = "✅" if ch.get("filter_hieroglyph") else "☐"
    no_photo = "✅" if ch.get("filter_no_photo")   else "☐"
    await callback.message.edit_text(
        "🛡 <b>Защита</b> (все площадки)\n\nФильтры применяются ко всем каналам бота.",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text=f"{rtl} Фильтр RTL-имён",       callback_data=f"bs_filter_rtl:{child_bot_id}")],
            [InlineKeyboardButton(text=f"{hiero} Фильтр иероглифов",    callback_data=f"bs_filter_hier:{child_bot_id}")],
            [InlineKeyboardButton(text=f"{no_photo} Фильтр без фото",   callback_data=f"bs_filter_photo:{child_bot_id}")],
            [InlineKeyboardButton(text="🌍 Языковые фильтры",            callback_data=f"bs_lang_filters:{child_bot_id}")],
            [InlineKeyboardButton(text="◀️ Назад",                       callback_data=f"bot_settings:{child_bot_id}")],
        ]),
    )
    await callback.answer()


@router.callback_query(F.data.startswith("bs_protection:"))
async def on_bs_protection(callback: CallbackQuery, platform_user: dict | None):
    if not platform_user:
        return
    await _show_bs_protection(callback, platform_user, int(callback.data.split(":")[1]))


@router.callback_query(F.data.startswith("bs_filter_rtl:"))
async def on_bs_filter_rtl(callback: CallbackQuery, platform_user: dict | None):
    if not platform_user:
        return
    child_bot_id = int(callback.data.split(":")[1])
    owner_id = platform_user["user_id"]
    ch = await _get_bot_first_chat(owner_id, child_bot_id)
    new_val = not (ch["filter_rtl"] if ch else False)
    await db.execute("UPDATE bot_chats SET filter_rtl=$1 WHERE child_bot_id=$2 AND owner_id=$3",
                     new_val, child_bot_id, owner_id)
    await callback.answer("RTL: " + ("✅ Вкл" if new_val else "☐ Выкл"))
    await _show_bs_protection(callback, platform_user, child_bot_id)


@router.callback_query(F.data.startswith("bs_filter_hier:"))
async def on_bs_filter_hier(callback: CallbackQuery, platform_user: dict | None):
    if not platform_user:
        return
    child_bot_id = int(callback.data.split(":")[1])
    owner_id = platform_user["user_id"]
    ch = await _get_bot_first_chat(owner_id, child_bot_id)
    new_val = not (ch["filter_hieroglyph"] if ch else False)
    await db.execute("UPDATE bot_chats SET filter_hieroglyph=$1 WHERE child_bot_id=$2 AND owner_id=$3",
                     new_val, child_bot_id, owner_id)
    await callback.answer("Иероглифы: " + ("✅ Вкл" if new_val else "☐ Выкл"))
    await _show_bs_protection(callback, platform_user, child_bot_id)


@router.callback_query(F.data.startswith("bs_filter_photo:"))
async def on_bs_filter_photo(callback: CallbackQuery, platform_user: dict | None):
    if not platform_user:
        return
    child_bot_id = int(callback.data.split(":")[1])
    owner_id = platform_user["user_id"]
    ch = await _get_bot_first_chat(owner_id, child_bot_id)
    new_val = not (ch["filter_no_photo"] if ch else False)
    await db.execute("UPDATE bot_chats SET filter_no_photo=$1 WHERE child_bot_id=$2 AND owner_id=$3",
                     new_val, child_bot_id, owner_id)
    await callback.answer("Без фото: " + ("✅ Вкл" if new_val else "☐ Выкл"))
    await _show_bs_protection(callback, platform_user, child_bot_id)


@router.callback_query(F.data.startswith("bs_lang_filters:"))
async def on_bs_lang_filters(callback: CallbackQuery, platform_user: dict | None):
    if not platform_user:
        return
    child_bot_id = int(callback.data.split(":")[1])
    owner_id = platform_user["user_id"]
    ch = await _get_bot_first_chat(owner_id, child_bot_id)
    if not ch:
        await callback.answer("Нет площадок", show_alert=True)
        return
    chat_id = ch["chat_id"]
    blocked = await db.fetch(
        "SELECT language_code FROM language_filters WHERE owner_id=$1 AND chat_id=$2::bigint",
        owner_id, chat_id)
    blocked_codes = {r["language_code"] for r in blocked}
    buttons = []
    for code, label in LANGUAGE_OPTIONS.items():
        mark = "🚫" if code in blocked_codes else "🌍"
        buttons.append([InlineKeyboardButton(text=f"{mark} {label}",
                                             callback_data=f"bs_lang_toggle:{child_bot_id}:{code}")])
    buttons.append([InlineKeyboardButton(text="◀️ Назад", callback_data=f"bs_protection:{child_bot_id}")])
    await callback.message.edit_text(
        "🌍 <b>Языковые фильтры</b> (все площадки)\n\n🚫 — заблокирован | 🌍 — разрешён",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=buttons))
    await callback.answer()


@router.callback_query(F.data.startswith("bs_lang_toggle:"))
async def on_bs_lang_toggle(callback: CallbackQuery, platform_user: dict | None):
    if not platform_user:
        return
    parts = callback.data.split(":")
    child_bot_id, code = int(parts[1]), parts[2]
    owner_id = platform_user["user_id"]
    chats = await db.fetch(
        "SELECT chat_id FROM bot_chats WHERE child_bot_id=$1 AND owner_id=$2", child_bot_id, owner_id)
    if not chats:
        return
    first = chats[0]["chat_id"]
    exists = await db.fetchrow(
        "SELECT 1 FROM language_filters WHERE owner_id=$1 AND chat_id=$2::bigint AND language_code=$3",
        owner_id, first, code)
    for chat_row in chats:
        cid = chat_row["chat_id"]
        if exists:
            await db.execute(
                "DELETE FROM language_filters WHERE owner_id=$1 AND chat_id=$2::bigint AND language_code=$3",
                owner_id, cid, code)
        else:
            await db.execute(
                "INSERT INTO language_filters (owner_id, chat_id, language_code) VALUES ($1,$2,$3) ON CONFLICT DO NOTHING",
                owner_id, cid, code)
    msg = f"✅ {LANGUAGE_OPTIONS.get(code,'?')} разрешён" if exists else f"🚫 {LANGUAGE_OPTIONS.get(code,'?')} заблокирован"
    await callback.answer(msg)
    callback.data = f"bs_lang_filters:{child_bot_id}"
    await on_bs_lang_filters(callback, platform_user)


# ── Управление ботом ─────────────────────────────────────────
@router.callback_query(F.data.startswith("bs_settings:"))
async def on_bs_settings(callback: CallbackQuery, platform_user: dict | None):
    if not platform_user:
        return
    child_bot_id = int(callback.data.split(":")[1])
    await callback.message.edit_text(
        "⚙️ <b>Управление</b>\n\nВыберите действие ⬇️",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="🗄 База",          callback_data=f"bs_base:{child_bot_id}")],
            [InlineKeyboardButton(text="👥 Команда",       callback_data=f"bs_team:{child_bot_id}")],
            [InlineKeyboardButton(text="🕐 Часовой пояс",  callback_data=f"bs_timezone:{child_bot_id}")],
            [InlineKeyboardButton(text="◀️ Назад",          callback_data=f"bot_settings:{child_bot_id}")],
        ]),
    )
    await callback.answer()


# ── База пользователей ────────────────────────────────────────
@router.callback_query(F.data.startswith("bs_base:"))
async def on_bs_base(callback: CallbackQuery, platform_user: dict | None):
    if not platform_user:
        return
    child_bot_id = int(callback.data.split(":")[1])
    owner_id = platform_user["user_id"]

    bot_row = await db.fetchrow(
        "SELECT bot_username FROM child_bots WHERE id=$1 AND owner_id=$2",
        child_bot_id, owner_id,
    )
    bot_username = bot_row["bot_username"] if bot_row else "—"

    total = await db.fetchval(
        """SELECT COUNT(*) FROM bot_users bu
           JOIN bot_chats bc ON bu.chat_id=bc.chat_id AND bu.owner_id=bc.owner_id
           WHERE bc.child_bot_id=$1 AND bc.owner_id=$2""",
        child_bot_id, owner_id,
    ) or 0

    blocked = await db.fetchval(
        "SELECT COUNT(*) FROM blacklist WHERE owner_id=$1",
        owner_id,
    ) or 0

    await callback.message.edit_text(
        "≡ <b>База</b>\n\n"
        f"🤖 Бот: @{bot_username}\n"
        f"👥 Пользователей в базе: {total:,}\n"
        f"⛔️ Заблокированных: {blocked:,}",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="✏️ Изменить базу",    callback_data=f"bs_base_edit:{child_bot_id}")],
            [InlineKeyboardButton(text="📤 Экспорт базы",     callback_data=f"bs_base_export_menu:{child_bot_id}")],
            [InlineKeyboardButton(text="⛔️ ЧС пользователей", callback_data=f"bs_blacklist:{child_bot_id}")],
            [InlineKeyboardButton(text="◀️ Назад",            callback_data=f"bs_settings:{child_bot_id}")],
        ]),
    )
    await callback.answer()


# ── Меню редактирования базы ─────────────────────────────────
@router.callback_query(F.data.startswith("bs_base_edit:"))
async def on_bs_base_edit(callback: CallbackQuery, state: FSMContext,
                          platform_user: dict | None):
    if not platform_user:
        return
    await state.clear()
    child_bot_id = int(callback.data.split(":")[1])
    owner_id = platform_user["user_id"]

    total = await db.fetchval(
        """SELECT COUNT(*) FROM bot_users bu
           JOIN bot_chats bc ON bu.chat_id=bc.chat_id AND bu.owner_id=bc.owner_id
           WHERE bc.child_bot_id=$1 AND bc.owner_id=$2""",
        child_bot_id, owner_id,
    ) or 0

    await callback.message.edit_text(
        "✏️ <b>Управление базой пользователей</b>\n\n"
        f"<blockquote>В базе сейчас: <b>{total:,}</b> пользователей.\n\n"
        "Вы можете вручную добавить пользователей по Telegram ID или @username, "
        "или удалить их из базы. Поддерживается как ввод текстом, так и загрузка файла.</blockquote>\n\n"
        "<b>Выберите действие:</b>",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="➕ Добавить пользователей",   callback_data=f"bs_base_add:{child_bot_id}")],
            [InlineKeyboardButton(text="🗑 Удалить пользователей",    callback_data=f"bs_base_del:{child_bot_id}")],
            [InlineKeyboardButton(text="◀️ Назад",                    callback_data=f"bs_base:{child_bot_id}")],
        ]),
    )
    await callback.answer()


# ── Добавить пользователей ────────────────────────────────────
@router.callback_query(F.data.startswith("bs_base_add:"))
async def on_bs_base_add(callback: CallbackQuery, state: FSMContext,
                         platform_user: dict | None):
    if not platform_user:
        return
    child_bot_id = int(callback.data.split(":")[1])
    await state.update_data(child_bot_id=child_bot_id)
    await state.set_state(SettingsFSM.bs_base_waiting_add)
    await callback.message.edit_text(
        "➕ <b>Добавление пользователей</b>\n\n"
        "Отправьте <b>@username</b> или <b>Telegram ID</b> — можно несколько через пробел или с новой строки.\n"
        "Или загрузите <b>TXT/CSV файл</b> (один пользователь на строку).\n\n"
        "<b>Пример:</b>\n"
        "<code>@ivan @maria\n123456789\n987654321</code>",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="🚫 Отменить", callback_data=f"bs_base_edit:{child_bot_id}")],
        ]),
    )
    await callback.answer()


# ── Удалить пользователей ────────────────────────────────────
@router.callback_query(F.data.startswith("bs_base_del:"))
async def on_bs_base_del(callback: CallbackQuery, state: FSMContext,
                         platform_user: dict | None):
    if not platform_user:
        return
    child_bot_id = int(callback.data.split(":")[1])
    await state.update_data(child_bot_id=child_bot_id)
    await state.set_state(SettingsFSM.bs_base_waiting_del)
    await callback.message.edit_text(
        "🗑 <b>Удаление пользователей</b>\n\n"
        "Отправьте <b>@username</b> или <b>Telegram ID</b> тех, кого хотите удалить.\n"
        "Можно несколько через пробел или с новой строки.\n"
        "Или загрузите <b>TXT/CSV файл</b>.\n\n"
        "<b>Пример:</b>\n"
        "<code>@spammer1\n111222333</code>",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="🚫 Отменить", callback_data=f"bs_base_edit:{child_bot_id}")],
        ]),
    )
    await callback.answer()


def _parse_user_lines(text: str) -> list[str]:
    """Возвращает список @username или числовых ID из свободного текста."""
    import re
    tokens = re.split(r'[\s,;]+', text.strip())
    result = []
    for t in tokens:
        t = t.strip()
        if not t:
            continue
        if t.startswith('@') and len(t) > 1:
            result.append(t.lower())
        elif t.lstrip('-').isdigit():
            result.append(t)
    return result


async def _process_base_add(owner_id: int, child_bot_id: int,
                             tokens: list[str]) -> dict:
    """Добавляет пользователей в bot_users возможных площадок бота."""
    # Получаем первый активный chat_id площадки для bot_id
    chat_row = await db.fetchrow(
        "SELECT chat_id FROM bot_chats WHERE child_bot_id=$1 AND owner_id=$2 "
        "AND is_active=true ORDER BY id LIMIT 1",
        child_bot_id, owner_id,
    )
    if not chat_row:
        return {"added": 0, "already": 0, "invalid": 0, "details": []}
    chat_id = chat_row["chat_id"]

    added = already = invalid = 0
    details = []
    for token in tokens:
        if token.startswith('@'):
            username = token.lstrip('@')
            exists = await db.fetchval(
                "SELECT 1 FROM bot_users WHERE owner_id=$1 AND chat_id=$2 "
                "AND lower(username)=$3",
                owner_id, chat_id, username.lower(),
            )
            if exists:
                already += 1
                details.append(f"• {token} — уже в базе")
                continue
            await db.execute(
                "INSERT INTO bot_users (owner_id, chat_id, username, first_name, "
                "joined_at, is_active, bot_activated) "
                "VALUES ($1,$2,$3,'',NOW(),true,false) ON CONFLICT DO NOTHING",
                owner_id, chat_id, username,
            )
            added += 1
            details.append(f"• {token} ✅")
        else:
            try:
                uid = int(token)
            except ValueError:
                invalid += 1
                details.append(f"• {token} — неверный формат")
                continue
            exists = await db.fetchval(
                "SELECT 1 FROM bot_users WHERE owner_id=$1 AND chat_id=$2 AND user_id=$3",
                owner_id, chat_id, uid,
            )
            if exists:
                already += 1
                details.append(f"• `{uid}` — уже в базе")
                continue
            await db.execute(
                "INSERT INTO bot_users (owner_id, chat_id, user_id, first_name, "
                "joined_at, is_active, bot_activated) "
                "VALUES ($1,$2,$3,'',NOW(),true,false) ON CONFLICT DO NOTHING",
                owner_id, chat_id, uid,
            )
            added += 1
            details.append(f"• `{uid}` ✅")
    return {"added": added, "already": already, "invalid": invalid, "details": details}


async def _process_base_del(owner_id: int, child_bot_id: int,
                             tokens: list[str]) -> dict:
    """Удаляет пользователей из bot_users по площадкам бота."""
    removed = not_found = invalid = 0
    details = []
    for token in tokens:
        if token.startswith('@'):
            username = token.lstrip('@')
            res = await db.execute(
                "DELETE FROM bot_users bu USING bot_chats bc "
                "WHERE bu.chat_id=bc.chat_id AND bu.owner_id=bc.owner_id "
                "AND bc.child_bot_id=$1 AND bc.owner_id=$2 "
                "AND lower(bu.username)=$3",
                child_bot_id, owner_id, username.lower(),
            )
            count = int(res.split()[-1]) if res else 0
            if count:
                removed += 1
                details.append(f"• {token} 🗑 удалён")
            else:
                not_found += 1
                details.append(f"• {token} — не найден")
        else:
            try:
                uid = int(token)
            except ValueError:
                invalid += 1
                details.append(f"• {token} — неверный формат")
                continue
            res = await db.execute(
                "DELETE FROM bot_users bu USING bot_chats bc "
                "WHERE bu.chat_id=bc.chat_id AND bu.owner_id=bc.owner_id "
                "AND bc.child_bot_id=$1 AND bc.owner_id=$2 AND bu.user_id=$3",
                child_bot_id, owner_id, uid,
            )
            count = int(res.split()[-1]) if res else 0
            if count:
                removed += 1
                details.append(f"• `{uid}` 🗑 удалён")
            else:
                not_found += 1
                details.append(f"• `{uid}` — не найден")
    return {"removed": removed, "not_found": not_found, "invalid": invalid, "details": details}


# ── Обработка текстового ввода для добавления/удаления ────────
@router.message(SettingsFSM.bs_base_waiting_add)
async def on_bs_base_add_input(message: Message, state: FSMContext,
                                platform_user: dict | None):
    if not platform_user:
        return
    data = await state.get_data()
    child_bot_id = data.get("child_bot_id")
    owner_id = platform_user["user_id"]

    # Файл
    if message.document:
        doc = message.document
        if not doc.file_name.lower().endswith((".txt", ".csv")):
            await message.answer("❌ Поддерживаются только TXT и CSV файлы.")
            return
        from aiogram import Bot as AioBot
        # используем бот платформы (message.bot)
        file_obj = await message.bot.get_file(doc.file_id)
        content_io = await message.bot.download_file(file_obj.file_path)
        text = content_io.read().decode("utf-8", errors="replace")
    elif message.text:
        text = message.text
    else:
        await message.answer("❌ Отправьте текст или файл.")
        return

    tokens = _parse_user_lines(text)
    if not tokens:
        await message.answer(
            "⚠️ Не найдено ни одного @username или Telegram ID.\n"
            "Убедитесь в правильности формата."
        )
        return

    res = await _process_base_add(owner_id, child_bot_id, tokens)
    total_now = await db.fetchval(
        """SELECT COUNT(*) FROM bot_users bu
           JOIN bot_chats bc ON bu.chat_id=bc.chat_id AND bu.owner_id=bc.owner_id
           WHERE bc.child_bot_id=$1 AND bc.owner_id=$2""",
        child_bot_id, owner_id,
    ) or 0

    detail_text = "\n".join(res["details"][:20])
    if len(res["details"]) > 20:
        detail_text += f"\n... и ещё {len(res['details']) - 20}"

    await state.clear()
    await message.answer(
        f"✅ <b>Готово!</b>\n\n"
        f"➕ Добавлено: <b>{res['added']}</b>\n"
        f"⏩ Уже были в базе: <b>{res['already']}</b>\n"
        f"❌ Неверный формат: <b>{res['invalid']}</b>\n\n"
        f"{detail_text}\n\n"
        f"👥 Итого в базе: <b>{total_now:,}</b>",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="◀️ Назад к управлению",
                                  callback_data=f"bs_base_edit:{child_bot_id}")],
        ]),
    )


@router.message(SettingsFSM.bs_base_waiting_del)
async def on_bs_base_del_input(message: Message, state: FSMContext,
                                platform_user: dict | None):
    if not platform_user:
        return
    data = await state.get_data()
    child_bot_id = data.get("child_bot_id")
    owner_id = platform_user["user_id"]

    if message.document:
        doc = message.document
        if not doc.file_name.lower().endswith((".txt", ".csv")):
            await message.answer("❌ Поддерживаются только TXT и CSV файлы.")
            return
        file_obj = await message.bot.get_file(doc.file_id)
        content_io = await message.bot.download_file(file_obj.file_path)
        text = content_io.read().decode("utf-8", errors="replace")
    elif message.text:
        text = message.text
    else:
        await message.answer("❌ Отправьте текст или файл.")
        return

    tokens = _parse_user_lines(text)
    if not tokens:
        await message.answer(
            "⚠️ Не найдено ни одного @username или Telegram ID."
        )
        return

    res = await _process_base_del(owner_id, child_bot_id, tokens)
    total_now = await db.fetchval(
        """SELECT COUNT(*) FROM bot_users bu
           JOIN bot_chats bc ON bu.chat_id=bc.chat_id AND bu.owner_id=bc.owner_id
           WHERE bc.child_bot_id=$1 AND bc.owner_id=$2""",
        child_bot_id, owner_id,
    ) or 0

    detail_text = "\n".join(res["details"][:20])
    if len(res["details"]) > 20:
        detail_text += f"\n... и ещё {len(res['details']) - 20}"

    await state.clear()
    await message.answer(
        f"🗑 <b>Удаление завершено!</b>\n\n"
        f"✅ Удалено: <b>{res['removed']}</b>\n"
        f"🔍 Не найдено: <b>{res['not_found']}</b>\n"
        f"❌ Неверный формат: <b>{res['invalid']}</b>\n\n"
        f"{detail_text}\n\n"
        f"👥 Осталось в базе: <b>{total_now:,}</b>",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="◀️ Назад к управлению",
                                  callback_data=f"bs_base_edit:{child_bot_id}")],
        ]),
    )


@router.callback_query(F.data.startswith("bs_blacklist:"))
async def on_bs_blacklist(callback: CallbackQuery, platform_user: dict | None):
    """Главное меню ЧС на уровне бота."""
    if not platform_user:
        return
    child_bot_id = int(callback.data.split(":")[1])
    owner_id = platform_user["user_id"]

    count = await db.fetchval(
        "SELECT COUNT(*) FROM blacklist WHERE owner_id=$1", owner_id,
    ) or 0

    await callback.message.edit_text(
        "⛔️ <b>ЧС пользователей</b>\n\n"
        f"🔢 Записей в базе ЧС: {count:,}",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="📂 Загрузить базу ЧС",   callback_data=f"bs_bl_upload:{child_bot_id}")],
            [InlineKeyboardButton(text="🔍 Найти нарушителей",    callback_data=f"bs_bl_sweep:{child_bot_id}")],
            [InlineKeyboardButton(text="⚙️ Управлять базой ЧС",  callback_data=f"bs_bl_manage:{child_bot_id}")],
            [InlineKeyboardButton(text="◀️ Назад",               callback_data=f"bs_base:{child_bot_id}")],
        ]),
    )
    await callback.answer()


# ── Загрузить базу ЧС ─────────────────────────────────────────
@router.callback_query(F.data.startswith("bs_bl_upload:"))
async def on_bs_bl_upload(callback: CallbackQuery, state: FSMContext,
                          platform_user: dict | None):
    if not platform_user:
        return
    child_bot_id = callback.data.split(":")[1]
    await state.update_data(child_bot_id=child_bot_id, bs_bl_mode="add")
    await state.set_state(SettingsFSM.bs_bl_waiting_add_file)
    await callback.message.edit_text(
        "📂 <b>Загрузить базу ЧС</b>\n\n"
        "Отправьте файл <b>TXT</b> или <b>CSV</b> с @username или Telegram ID.\n\n"
        "📋 Формат строки:\n"
        "<code>@spammer1\n123456789\n@baduser</code>\n\n"
        "Максимум: 20 MB, до 100 000 записей.",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="🚫 Отмена", callback_data=f"bs_blacklist:{child_bot_id}")],
        ]),
    )
    await callback.answer()


# ── Найти нарушителей ─────────────────────────────────────────
@router.callback_query(F.data.startswith("bs_bl_sweep:"))
async def on_bs_bl_sweep(callback: CallbackQuery, platform_user: dict | None):
    if not platform_user:
        return
    child_bot_id = int(callback.data.split(":")[1])
    owner_id = platform_user["user_id"]

    chats = await db.fetch(
        "SELECT chat_id, chat_title FROM bot_chats "
        "WHERE child_bot_id=$1 AND owner_id=$2 AND is_active=true",
        child_bot_id, owner_id,
    )
    if not chats:
        await callback.answer("Нет активных площадок у бота", show_alert=True)
        return

    if len(chats) == 1:
        await _show_bs_sweep_screen(
            callback, platform_user, child_bot_id,
            chats[0]["chat_id"], chats[0]["chat_title"],
        )
    else:
        buttons = [
            [InlineKeyboardButton(
                text=f"📍 {ch['chat_title'] or ch['chat_id']}",
                callback_data=f"bs_bl_sweep_chat:{child_bot_id}:{ch['chat_id']}",
            )]
            for ch in chats
        ]
        buttons.append([InlineKeyboardButton(
            text="◀️ Назад", callback_data=f"bs_blacklist:{child_bot_id}",
        )])
        await callback.message.edit_text(
            "🔍 <b>Найти нарушителей</b>\n\nВыберите площадку для проверки:",
            reply_markup=InlineKeyboardMarkup(inline_keyboard=buttons),
        )
        await callback.answer()


async def _show_bs_sweep_screen(
    callback: CallbackQuery,
    platform_user: dict,
    child_bot_id: int,
    chat_id: int,
    chat_title: str,
):
    owner_id = platform_user["user_id"]
    user_count = await db.fetchval(
        "SELECT COUNT(*) FROM bot_users "
        "WHERE owner_id=$1 AND chat_id=$2::bigint AND is_active=true",
        owner_id, chat_id,
    ) or 0
    await callback.message.edit_text(
        "🔍 <b>Найти нарушителей</b>\n\n"
        f"Площадка: <b>{chat_title or chat_id}</b>\n"
        f"Пользователей в базе бота: {user_count:,}\n\n"
        "ℹ️ Будут проверены только те, кто вступил\n"
        "   после подключения бота.",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(
                text="▶️ Запустить проверку",
                callback_data=f"bs_bl_sweep_run:{child_bot_id}:{chat_id}",
            )],
            [InlineKeyboardButton(text="◀️ Назад", callback_data=f"bs_blacklist:{child_bot_id}")],
        ]),
    )
    await callback.answer()


@router.callback_query(F.data.startswith("bs_bl_sweep_chat:"))
async def on_bs_bl_sweep_chat(callback: CallbackQuery, platform_user: dict | None):
    if not platform_user:
        return
    parts = callback.data.split(":")
    child_bot_id, chat_id = int(parts[1]), int(parts[2])
    ch = await db.fetchrow(
        "SELECT chat_title FROM bot_chats "
        "WHERE child_bot_id=$1 AND chat_id=$2::bigint AND owner_id=$3",
        child_bot_id, chat_id, platform_user["user_id"],
    )
    title = ch["chat_title"] if ch else str(chat_id)
    await _show_bs_sweep_screen(callback, platform_user, child_bot_id, chat_id, title)


# ── Запустить проверку ────────────────────────────────────────
@router.callback_query(F.data.startswith("bs_bl_sweep_run:"))
async def on_bs_bl_sweep_run(callback: CallbackQuery, platform_user: dict | None):
    if not platform_user:
        return
    parts = callback.data.split(":")
    child_bot_id, chat_id = int(parts[1]), int(parts[2])
    owner_id = platform_user["user_id"]

    await callback.answer("⏳ Проверяю...")

    total = await db.fetchval(
        "SELECT COUNT(*) FROM bot_users "
        "WHERE owner_id=$1 AND chat_id=$2::bigint AND is_active=true",
        owner_id, chat_id,
    ) or 0

    violators = await db.fetch(
        """SELECT bu.user_id, bu.username FROM bot_users bu
           INNER JOIN blacklist bl ON bl.owner_id=bu.owner_id
             AND ((bl.user_id IS NOT NULL AND bl.user_id=bu.user_id)
                  OR  (bl.username IS NOT NULL AND lower(bl.username)=lower(bu.username)))
           WHERE bu.owner_id=$1 AND bu.chat_id=$2::bigint AND bu.is_active=true""",
        owner_id, chat_id,
    )
    n = len(violators)

    if n == 0:
        await callback.message.edit_text(
            "✅ <b>Проверка завершена</b>\n\n"
            f"📊 Проверено: {total:,}\n"
            "🎉 Нарушителей не найдено.",
            reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                [InlineKeyboardButton(text="◀️ Назад", callback_data=f"bs_bl_sweep:{child_bot_id}")],
            ]),
        )
        return

    preview_lines = []
    for v in violators[:5]:
        if v["username"]:
            line = f"• @{v['username']}"
            if v["user_id"]:
                line += f" (ID: {v['user_id']})"
        else:
            line = f"• ID: {v['user_id']}"
        preview_lines.append(line)
    preview = "\n".join(preview_lines)
    if n > 5:
        preview += f"\n• ... и ещё {n - 5}"

    await callback.message.edit_text(
        "✅ <b>Проверка завершена</b>\n\n"
        f"📊 Проверено: {total:,}\n"
        f"🚫 Совпадений с ЧС: {n}\n\n"
        f"{preview}",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(
                text=f"🚫 Забанить всех ({n})",
                callback_data=f"bs_bl_ban_all:{child_bot_id}:{chat_id}",
            )],
            [InlineKeyboardButton(
                text="👁 Показать полный список",
                callback_data=f"bs_bl_full_list:{child_bot_id}:{chat_id}",
            )],
            [InlineKeyboardButton(text="◀️ Назад", callback_data=f"bs_bl_sweep:{child_bot_id}")],
        ]),
    )


# ── Забанить всех нарушителей (бот-уровень) ──────────────────
@router.callback_query(F.data.startswith("bs_bl_ban_all:"))
async def on_bs_bl_ban_all(callback: CallbackQuery, bot: Bot,
                           platform_user: dict | None):
    if not platform_user:
        return
    parts = callback.data.split(":")
    child_bot_id, chat_id = int(parts[1]), int(parts[2])
    owner_id = platform_user["user_id"]

    from aiogram import Bot as AioBot
    from services.security import decrypt_token

    bot_row = await db.fetchrow(
        "SELECT token_encrypted FROM child_bots WHERE id=$1 AND owner_id=$2",
        child_bot_id, owner_id,
    )
    if not bot_row:
        await callback.answer("Бот не найден", show_alert=True)
        return

    await callback.answer("⏳ Баню...")

    child_bot_instance = AioBot(token=decrypt_token(bot_row["token_encrypted"]))
    violators = await db.fetch(
        """SELECT bu.user_id FROM bot_users bu
           INNER JOIN blacklist bl ON bl.owner_id=bu.owner_id
             AND ((bl.user_id IS NOT NULL AND bl.user_id=bu.user_id)
                  OR  (bl.username IS NOT NULL AND lower(bl.username)=lower(bu.username)))
           WHERE bu.owner_id=$1 AND bu.chat_id=$2::bigint AND bu.is_active=true""",
        owner_id, chat_id,
    )

    banned = 0
    for v in violators:
        try:
            await child_bot_instance.ban_chat_member(chat_id, v["user_id"])
            banned += 1
        except Exception:
            pass

    await child_bot_instance.session.close()

    if violators:
        user_ids = [v["user_id"] for v in violators]
        await db.execute(
            "UPDATE bot_users SET is_active=false, left_at=now() "
            "WHERE owner_id=$1 AND chat_id=$2::bigint AND user_id=ANY($3::bigint[])",
            owner_id, chat_id, user_ids,
        )

    await callback.message.edit_text(
        "✅ <b>Готово!</b>\n\n"
        f"🚫 Забанено: <b>{banned}</b> из {len(violators)}",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="◀️ Назад", callback_data=f"bs_blacklist:{child_bot_id}")],
        ]),
    )


# ── Полный список нарушителей ─────────────────────────────────
@router.callback_query(F.data.startswith("bs_bl_full_list:"))
async def on_bs_bl_full_list(callback: CallbackQuery, bot: Bot,
                             platform_user: dict | None):
    if not platform_user:
        return
    parts = callback.data.split(":")
    child_bot_id, chat_id = int(parts[1]), int(parts[2])
    owner_id = platform_user["user_id"]

    violators = await db.fetch(
        """SELECT bu.user_id, bu.username FROM bot_users bu
           INNER JOIN blacklist bl ON bl.owner_id=bu.owner_id
             AND ((bl.user_id IS NOT NULL AND bl.user_id=bu.user_id)
                  OR  (bl.username IS NOT NULL AND lower(bl.username)=lower(bu.username)))
           WHERE bu.owner_id=$1 AND bu.chat_id=$2::bigint AND bu.is_active=true""",
        owner_id, chat_id,
    )
    if not violators:
        await callback.answer("Список пуст", show_alert=True)
        return

    lines = []
    for i, v in enumerate(violators, 1):
        if v["username"]:
            entry = f"{i}. @{v['username']}"
            if v["user_id"]:
                entry += f" (ID: {v['user_id']})"
        else:
            entry = f"{i}. ID: {v['user_id']}"
        lines.append(entry)

    from aiogram.types import BufferedInputFile
    content = "\n".join(lines).encode("utf-8")
    file = BufferedInputFile(content, filename="violators_list.txt")
    await bot.send_document(
        chat_id=owner_id,
        document=file,
        caption=f"👁 Полный список нарушителей: {len(violators):,} чел.",
    )
    await callback.answer("✅ Список отправлен в чат")


# ── Управление базой ЧС ───────────────────────────────────────
async def _show_bs_bl_manage(callback: CallbackQuery, platform_user: dict,
                             child_bot_id: int):
    owner_id = platform_user["user_id"]
    try:
        bot_row = await db.fetchrow(
            "SELECT blacklist_enabled FROM child_bots WHERE id=$1 AND owner_id=$2",
            child_bot_id, owner_id,
        )
        enabled = bot_row["blacklist_enabled"] if bot_row else True
    except Exception:
        enabled = True

    count = await db.fetchval(
        "SELECT COUNT(*) FROM blacklist WHERE owner_id=$1", owner_id,
    ) or 0

    toggle_text = "✅ ЧС: Включён 🟢" if enabled else "☑️ ЧС: Выключен 🔴"
    await callback.message.edit_text(
        "⚙️ <b>Управление базой ЧС</b>\n\n"
        f"📊 Записей в базе: {count:,}\n\n"
        "Включите ЧС, чтобы бот автоматически проверял\n"
        "вступающих пользователей по списку.",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text=toggle_text,
                                  callback_data=f"bs_bl_toggle:{child_bot_id}")],
            [InlineKeyboardButton(text="➕ Добавить пользователей (TXT/CSV)",
                                  callback_data=f"bs_bl_add_file:{child_bot_id}")],
            [InlineKeyboardButton(text="➖ Удалить пользователей (TXT/CSV)",
                                  callback_data=f"bs_bl_del_file:{child_bot_id}")],
            [InlineKeyboardButton(text="🗑 Очистить базу",
                                  callback_data=f"bs_bl_clear:{child_bot_id}")],
            [InlineKeyboardButton(text="◀️ Назад",
                                  callback_data=f"bs_blacklist:{child_bot_id}")],
        ]),
    )
    await callback.answer()


@router.callback_query(F.data.startswith("bs_bl_manage:"))
async def on_bs_bl_manage(callback: CallbackQuery, platform_user: dict | None):
    if not platform_user:
        return
    await _show_bs_bl_manage(callback, platform_user, int(callback.data.split(":")[1]))


# ── Тумблер ЧС ───────────────────────────────────────────────
@router.callback_query(F.data.startswith("bs_bl_toggle:"))
async def on_bs_bl_toggle(callback: CallbackQuery, platform_user: dict | None):
    if not platform_user:
        return
    child_bot_id = int(callback.data.split(":")[1])
    owner_id = platform_user["user_id"]
    try:
        row = await db.fetchrow(
            "SELECT blacklist_enabled FROM child_bots WHERE id=$1 AND owner_id=$2",
            child_bot_id, owner_id,
        )
        current = row["blacklist_enabled"] if row else True
        new_val = not current
        await db.execute(
            "UPDATE child_bots SET blacklist_enabled=$1 WHERE id=$2 AND owner_id=$3",
            new_val, child_bot_id, owner_id,
        )
        await callback.answer("✅ ВКЛ" if new_val else "🔴 ВЫКЛ")
    except Exception:
        await callback.answer("⚠️ Ошибка обновления", show_alert=True)
        return
    await _show_bs_bl_manage(callback, platform_user, child_bot_id)


# ── Добавить пользователей файлом ─────────────────────────────
@router.callback_query(F.data.startswith("bs_bl_add_file:"))
async def on_bs_bl_add_file(callback: CallbackQuery, state: FSMContext,
                            platform_user: dict | None):
    if not platform_user:
        return
    child_bot_id = callback.data.split(":")[1]
    await state.update_data(child_bot_id=child_bot_id, bs_bl_mode="add")
    await state.set_state(SettingsFSM.bs_bl_waiting_add_file)
    await callback.message.edit_text(
        "➕ <b>Добавить в ЧС</b>\n\n"
        "Отправьте файл <b>TXT</b> или <b>CSV</b>.\n"
        "Каждая строка — один пользователь: @username или Telegram ID.\n\n"
        "<code>@spammer1\n123456789\n@baduser</code>",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="🚫 Отмена", callback_data=f"bs_bl_manage:{child_bot_id}")],
        ]),
    )
    await callback.answer()


# ── Удалить пользователей файлом ─────────────────────────────
@router.callback_query(F.data.startswith("bs_bl_del_file:"))
async def on_bs_bl_del_file(callback: CallbackQuery, state: FSMContext,
                            platform_user: dict | None):
    if not platform_user:
        return
    child_bot_id = callback.data.split(":")[1]
    await state.update_data(child_bot_id=child_bot_id, bs_bl_mode="del")
    await state.set_state(SettingsFSM.bs_bl_waiting_del_file)
    await callback.message.edit_text(
        "➖ <b>Удалить из ЧС</b>\n\n"
        "Отправьте файл <b>TXT</b> или <b>CSV</b> с пользователями,\n"
        "которых нужно <b>убрать</b> из чёрного списка.\n\n"
        "<code>@gooduser\n987654321</code>",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="🚫 Отмена", callback_data=f"bs_bl_manage:{child_bot_id}")],
        ]),
    )
    await callback.answer()


# ── Очистить базу ЧС ─────────────────────────────────────────
@router.callback_query(F.data.startswith("bs_bl_clear:"))
async def on_bs_bl_clear(callback: CallbackQuery, platform_user: dict | None):
    if not platform_user:
        return
    child_bot_id = callback.data.split(":")[1]
    count = await db.fetchval(
        "SELECT COUNT(*) FROM blacklist WHERE owner_id=$1",
        platform_user["user_id"],
    ) or 0
    await callback.message.edit_text(
        f"⚠️ <b>Очистить базу ЧС?</b>\n\n"
        f"Будет удалено <b>{count:,}</b> записей. Действие необратимо.",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="✅ Да, очистить",
                                  callback_data=f"bs_bl_clear_do:{child_bot_id}")],
            [InlineKeyboardButton(text="🚫 Отмена",
                                  callback_data=f"bs_bl_manage:{child_bot_id}")],
        ]),
    )
    await callback.answer()


@router.callback_query(F.data.startswith("bs_bl_clear_do:"))
async def on_bs_bl_clear_do(callback: CallbackQuery, platform_user: dict | None):
    if not platform_user:
        return
    child_bot_id = callback.data.split(":")[1]
    owner_id = platform_user["user_id"]
    deleted = await db.fetchval(
        "WITH d AS (DELETE FROM blacklist WHERE owner_id=$1 RETURNING 1) "
        "SELECT COUNT(*) FROM d",
        owner_id,
    ) or 0
    await callback.message.edit_text(
        f"✅ База ЧС очищена. Удалено <b>{deleted:,}</b> записей.",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="◀️ Назад",
                                  callback_data=f"bs_blacklist:{child_bot_id}")],
        ]),
    )
    await callback.answer()


# ── Экран выбора типа экспорта ───────────────────────────────
@router.callback_query(F.data.startswith("bs_base_export_menu:"))
async def on_bs_base_export_menu(callback: CallbackQuery, platform_user: dict | None):
    if not platform_user:
        return
    child_bot_id = int(callback.data.split(":")[1])
    owner_id = platform_user["user_id"]

    total = await db.fetchval(
        """SELECT COUNT(*) FROM bot_users bu
           JOIN bot_chats bc ON bu.chat_id=bc.chat_id AND bu.owner_id=bc.owner_id
           WHERE bc.child_bot_id=$1 AND bc.owner_id=$2""",
        child_bot_id, owner_id,
    ) or 0
    active = await db.fetchval(
        """SELECT COUNT(*) FROM bot_users bu
           JOIN bot_chats bc ON bu.chat_id=bc.chat_id AND bu.owner_id=bc.owner_id
           WHERE bc.child_bot_id=$1 AND bc.owner_id=$2 AND bu.is_active=true AND bu.bot_activated=true""",
        child_bot_id, owner_id,
    ) or 0
    premium = await db.fetchval(
        """SELECT COUNT(*) FROM bot_users bu
           JOIN bot_chats bc ON bu.chat_id=bc.chat_id AND bu.owner_id=bc.owner_id
           WHERE bc.child_bot_id=$1 AND bc.owner_id=$2 AND bu.is_premium=true""",
        child_bot_id, owner_id,
    ) or 0
    inactive = total - active

    await callback.message.edit_text(
        "🗄 <b>Экспорт базы</b>\n\n"
        "<blockquote>Здесь хранятся все пользователи, которые "
        "взаимодействовали с каналами и группами вашего бота.\n\n"
        "Вы можете выгрузить базу в формате CSV — это удобно для "
        "аналитики, рассылок через сторонние сервисы или резервной копии.</blockquote>\n\n"
        f"👥 Всего в базе: {total:,}\n"
        f"🟢 Активных (в боте): {active:,}\n"
        f"🔴 Неактивных: {inactive:,}\n"
        f"⭐ Premium: {premium:,}",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="📥 Выгрузить всю базу (CSV)",  callback_data=f"bs_base_export:{child_bot_id}:all")],
            [InlineKeyboardButton(text="🟢 Выгрузить активных",         callback_data=f"bs_base_export:{child_bot_id}:active")],
            [InlineKeyboardButton(text="⭐ Выгрузить Premium",           callback_data=f"bs_base_export:{child_bot_id}:premium")],
            [InlineKeyboardButton(text="◀️ Назад",                       callback_data=f"bs_base:{child_bot_id}")],
        ]),
    )
    await callback.answer()


@router.callback_query(F.data.startswith("bs_base_export:"))
async def on_bs_base_export(callback: CallbackQuery, bot: Bot, platform_user: dict | None):
    if not platform_user:
        return
    parts = callback.data.split(":")
    child_bot_id = int(parts[1])
    mode = parts[2] if len(parts) > 2 else "all"
    owner_id = platform_user["user_id"]

    await callback.answer("⏳ Формирую файл...", show_alert=False)

    # Формируем WHERE в зависимости от режима
    extra_filter = ""
    label = "все"
    if mode == "active":
        extra_filter = "AND bu.is_active=true AND bu.bot_activated=true"
        label = "активные"
    elif mode == "premium":
        extra_filter = "AND bu.is_premium=true"
        label = "premium"

    rows = await db.fetch(
        f"""SELECT bu.user_id, bu.username, bu.first_name, bu.language_code,
                   bu.is_premium, bu.is_active, bu.bot_activated,
                   bu.joined_at, bc.chat_title
            FROM bot_users bu
            JOIN bot_chats bc ON bu.chat_id=bc.chat_id AND bu.owner_id=bc.owner_id
            WHERE bc.child_bot_id=$1 AND bc.owner_id=$2 {extra_filter}
            ORDER BY bu.joined_at DESC""",
        child_bot_id, owner_id,
    )

    if not rows:
        try:
            await callback.message.edit_text(
                "🗄 <b>База пользователей</b>\n\n"
                "⚠️ Нет пользователей для выгрузки по выбранному фильтру.",
                reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                    [InlineKeyboardButton(text="◀️ Назад", callback_data=f"bs_base:{child_bot_id}")],
                ]),
            )
        except Exception:
            pass
        return

    # Генерируем CSV в памяти
    import csv
    import io
    from datetime import datetime

    output = io.StringIO()
    writer = csv.writer(output, delimiter=";", quoting=csv.QUOTE_MINIMAL)
    writer.writerow(["user_id", "username", "first_name", "language",
                     "premium", "active", "bot_activated", "joined_at", "channel"])
    for r in rows:
        writer.writerow([
            r["user_id"],
            r["username"] or "",
            r["first_name"] or "",
            r["language_code"] or "",
            "да" if r["is_premium"] else "нет",
            "да" if r["is_active"] else "нет",
            "да" if r["bot_activated"] else "нет",
            r["joined_at"].strftime("%d.%m.%Y %H:%M") if r["joined_at"] else "",
            r["chat_title"] or "",
        ])

    csv_bytes = output.getvalue().encode("utf-8-sig")  # BOM для Excel
    filename = f"users_{label}_{datetime.now().strftime('%Y%m%d_%H%M')}.csv"

    from aiogram.types import BufferedInputFile
    file = BufferedInputFile(csv_bytes, filename=filename)

    bot_row = await db.fetchrow(
        "SELECT bot_username FROM child_bots WHERE id=$1", child_bot_id)
    caption = (
        f"📊 <b>База пользователей @{bot_row['bot_username'] if bot_row else ''}</b>\n\n"
        f"🔖 Фильтр: <b>{label}</b>\n"
        f"👥 Записей: <b>{len(rows):,}</b>\n\n"
        "Формат: CSV (разделитель ;)\n"
        "Кодировка: UTF-8 с BOM (совместимо с Excel)"
    )

    try:
        await bot.send_document(
            chat_id=owner_id,
            document=file,
            caption=caption,
            parse_mode="HTML",
        )
        # Обновляем сообщение-меню
        await callback.message.edit_text(
            "🗄 <b>База пользователей</b>\n\n"
            f"✅ Файл <code>{filename}</code> отправлен.\n"
            f"Записей: <b>{len(rows):,}</b>",
            reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                [InlineKeyboardButton(text="◀️ Назад", callback_data=f"bs_base:{child_bot_id}")],
            ]),
        )
    except Exception as e:
        logger.error(f"CSV export error: {e}")
        await callback.message.edit_text(
            "❌ Ошибка при отправке файла. Попробуйте позже.",
            reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                [InlineKeyboardButton(text="◀️ Назад", callback_data=f"bs_base:{child_bot_id}")],
            ]),
        )


# ── Часовой пояс ─────────────────────────────────────────────
_BS_TIMEZONES = [
    ("UTC+3 Москва",          "Europe/Moscow"),
    ("UTC+0 Лондон",          "UTC"),
    ("UTC+1 Берлин",          "Europe/Berlin"),
    ("UTC+2 Киев",            "Europe/Kiev"),
    ("UTC+5 Екатеринбург",    "Asia/Yekaterinburg"),
    ("UTC+6 Омск",            "Asia/Omsk"),
    ("UTC+7 Красноярск",      "Asia/Krasnoyarsk"),
    ("UTC+8 Иркутск",         "Asia/Irkutsk"),
    ("UTC+9 Якутск",          "Asia/Yakutsk"),
    ("UTC+10 Владивосток",    "Asia/Vladivostok"),
]


@router.callback_query(F.data.startswith("bs_timezone:"))
async def on_bs_timezone(callback: CallbackQuery, platform_user: dict | None):
    if not platform_user:
        return
    child_bot_id = int(callback.data.split(":")[1])
    owner_id = platform_user["user_id"]

    ch = await _get_bot_first_chat(owner_id, child_bot_id)
    current_tz = ch["timezone"] if ch and ch.get("timezone") else "UTC"

    buttons = []
    for label, tz in _BS_TIMEZONES:
        mark = "✅ " if tz == current_tz else ""
        buttons.append([InlineKeyboardButton(
            text=f"{mark}{label}",
            callback_data=f"bs_tz_set:{child_bot_id}:{tz}",
        )])
    buttons.append([InlineKeyboardButton(text="◀️ Назад", callback_data=f"bs_settings:{child_bot_id}")])

    await callback.message.edit_text(
        f"🕐 <b>Часовой пояс</b>\n\n"
        f"Текущий: <code>{current_tz}</code>\n\n"
        "Применяется ко всем площадкам бота (время рассылок, отчётов):",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=buttons),
    )
    await callback.answer()


@router.callback_query(F.data.startswith("bs_tz_set:"))
async def on_bs_tz_set(callback: CallbackQuery, platform_user: dict | None):
    if not platform_user:
        return
    parts = callback.data.split(":")
    child_bot_id = int(parts[1])
    tz = ":".join(parts[2:])
    owner_id = platform_user["user_id"]

    await db.execute(
        "UPDATE bot_chats SET timezone=$1 WHERE child_bot_id=$2 AND owner_id=$3",
        tz, child_bot_id, owner_id,
    )
    await callback.answer(f"✅ Часовой пояс: {tz}")
    # Перерисовываем экран
    callback.data = f"bs_timezone:{child_bot_id}"
    await on_bs_timezone(callback, platform_user)



# ── Рассылка / Ссылки / Обратная связь — выбор канала ────────
async def _bs_channel_picker(callback: CallbackQuery, platform_user: dict,
                             child_bot_id: int, section: str, title: str):
    owner_id = platform_user["user_id"]
    chats = await db.fetch(
        "SELECT chat_id, chat_title FROM bot_chats WHERE child_bot_id=$1 AND owner_id=$2 AND is_active=true",
        child_bot_id, owner_id)
    if not chats:
        await callback.answer("Нет активных площадок у бота", show_alert=True)
        return
    buttons = [[InlineKeyboardButton(
        text=f"📍 {ch['chat_title'] or ch['chat_id']}",
        callback_data=f"{section}:{ch['chat_id']}",
    )] for ch in chats]
    buttons.append([InlineKeyboardButton(text="◀️ Назад", callback_data=f"bot_settings:{child_bot_id}")])
    await callback.message.edit_text(
        f"{title}\n\nВыберите площадку:",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=buttons))
    await callback.answer()


@router.callback_query(F.data.startswith("bs_mailing:"))
async def on_bs_mailing(callback: CallbackQuery, platform_user: dict | None):
    """Рассылка: показываем экран выбора действия сразу, без выбора площадки."""
    if not platform_user:
        return
    child_bot_id = int(callback.data.split(":")[1])
    tariff = platform_user["tariff"]
    if tariff == "free":
        await callback.answer("Рассылка доступна с тарифа Старт.", show_alert=True)
        return

    await callback.message.edit_text(
        "📨 <b>Рассылка</b>\n\nВыберите действие ⬇️",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="➕ Создать рассылку",
                                  callback_data=f"mailing_bot_start:{child_bot_id}")],
            [InlineKeyboardButton(text="📅 Запланированные",
                                  callback_data=f"mailing_bot_scheduled:{child_bot_id}")],
            [InlineKeyboardButton(text="◀️ Назад",
                                  callback_data=f"bot_settings:{child_bot_id}")],
        ]),
    )
    await callback.answer()


@router.callback_query(F.data.startswith("bs_links:"))
async def on_bs_links(callback: CallbackQuery, platform_user: dict | None):
    if not platform_user:
        return
    await _bs_channel_picker(callback, platform_user, int(callback.data.split(":")[1]),
                             "ch_links", "🔗 <b>Ссылки</b>")


@router.callback_query(F.data.startswith("bs_feedback:"))
async def on_bs_feedback(callback: CallbackQuery, platform_user: dict | None):
    if not platform_user:
        return
    child_bot_id = int(callback.data.split(":")[1])
    from handlers.feedback import show_bot_feedback
    await show_bot_feedback(callback, platform_user, child_bot_id)



# ══════════════════════════════════════════════════════════════
# Команда — совместное управление ботом
# ══════════════════════════════════════════════════════════════

import secrets


async def _get_or_create_team_invites(owner_id: int, child_bot_id: int) -> dict:
    """Возвращает или создаёт invite-токены для admin и owner ролей."""
    rows = await db.fetch(
        "SELECT role, token FROM team_invites "
        "WHERE owner_id=$1 AND child_bot_id=$2",
        owner_id, child_bot_id,
    )
    tokens = {r["role"]: r["token"] for r in rows}
    for role in ("admin", "owner"):
        if role not in tokens:
            tok = secrets.token_hex(16)
            await db.execute(
                "INSERT INTO team_invites (owner_id, child_bot_id, token, role) "
                "VALUES ($1, $2, $3, $4) "
                "ON CONFLICT (token) DO NOTHING",
                owner_id, child_bot_id, tok, role,
            )
            tokens[role] = tok
    return tokens


async def _show_bs_team(callback: CallbackQuery, bot: Bot,
                        platform_user: dict, child_bot_id: int):
    owner_id = platform_user["user_id"]
    me = await bot.get_me()
    platform_username = me.username

    tokens = await _get_or_create_team_invites(owner_id, child_bot_id)
    admin_link = f"https://t.me/{platform_username}?start=team-{tokens['admin']}"
    owner_link  = f"https://t.me/{platform_username}?start=team-{tokens['owner']}"

    members = await db.fetch(
        "SELECT user_id, username, role FROM team_members "
        "WHERE owner_id=$1 AND child_bot_id=$2 AND is_active=true "
        "ORDER BY added_at",
        owner_id, child_bot_id,
    )
    member_lines = ""
    if members:
        lines = []
        for m in members:
            uname = f"@{m['username']}" if m["username"] else f"ID:{m['user_id']}"
            role_label = "👑 Владелец" if m["role"] == "owner" else "🛡 Админ"
            lines.append(f"  • {uname} — {role_label}")
        member_lines = "\n\n👥 <b>Участники команды:</b>\n" + "\n".join(lines)

    await callback.message.edit_text(
        "<blockquote>"
        "👥 Вы можете добавить админов для совместного управления ботом.\n\n"
        "🔄 Ссылки необходимо обновлять после использования."
        "</blockquote>\n\n"
        "👤 Чтобы добавить администратора,\n"
        f"   отправьте ему ссылку →\n<code>{admin_link}</code>\n\n"
        "👑 Чтобы сменить владельца бота,\n"
        f"   отправьте ему ссылку →\n<code>{owner_link}</code>"
        f"{member_lines}\n\n"
        "Выберите действие ⬇️",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="→ Добавить администратора",
                                  url=admin_link)],
            [InlineKeyboardButton(text="→ Сменить владельца",
                                  url=owner_link)],
            [InlineKeyboardButton(text="🔄 Обновить ссылки",
                                  callback_data=f"bs_team_refresh:{child_bot_id}")],
            *([
                [InlineKeyboardButton(text="👤 Участники команды",
                                      callback_data=f"bs_team_members:{child_bot_id}")]
            ] if members else []),
            [InlineKeyboardButton(text="◀️ Назад",
                                  callback_data=f"bs_settings:{child_bot_id}")],
        ]),
    )
    await callback.answer()


@router.callback_query(F.data.startswith("bs_team:"))
async def on_bs_team(callback: CallbackQuery, bot: Bot,
                     platform_user: dict | None):
    if not platform_user:
        return
    child_bot_id = int(callback.data.split(":")[1])
    owner_id = platform_user["user_id"]
    # Только владелец видит экран команды (проверяем owner_id в child_bots)
    row = await db.fetchrow(
        "SELECT id FROM child_bots WHERE id=$1 AND owner_id=$2",
        child_bot_id, owner_id,
    )
    if not row:
        await callback.answer("Только владелец бота может управлять командой", show_alert=True)
        return
    await _show_bs_team(callback, bot, platform_user, child_bot_id)


@router.callback_query(F.data.startswith("bs_team_refresh:"))
async def on_bs_team_refresh(callback: CallbackQuery, bot: Bot,
                             platform_user: dict | None):
    if not platform_user:
        return
    child_bot_id = int(callback.data.split(":")[1])
    owner_id = platform_user["user_id"]
    # Удаляем старые токены — они будут пересозданы
    await db.execute(
        "DELETE FROM team_invites WHERE owner_id=$1 AND child_bot_id=$2",
        owner_id, child_bot_id,
    )
    await _show_bs_team(callback, bot, platform_user, child_bot_id)


@router.callback_query(F.data.startswith("bs_team_members:"))
async def on_bs_team_members(callback: CallbackQuery, platform_user: dict | None):
    if not platform_user:
        return
    child_bot_id = int(callback.data.split(":")[1])
    owner_id = platform_user["user_id"]

    members = await db.fetch(
        "SELECT id, user_id, username, role, added_at FROM team_members "
        "WHERE owner_id=$1 AND child_bot_id=$2 AND is_active=true "
        "ORDER BY added_at",
        owner_id, child_bot_id,
    )
    if not members:
        await callback.answer("Команда пуста", show_alert=True)
        return

    buttons = []
    for m in members:
        uname = f"@{m['username']}" if m["username"] else f"ID:{m['user_id']}"
        role_label = "👑" if m["role"] == "owner" else "🛡"
        buttons.append([InlineKeyboardButton(
            text=f"{role_label} {uname} — ❌ Удалить",
            callback_data=f"bs_team_remove:{child_bot_id}:{m['id']}",
        )])
    buttons.append([InlineKeyboardButton(
        text="◀️ Назад", callback_data=f"bs_team:{child_bot_id}",
    )])

    await callback.message.edit_text(
        "👥 <b>Участники команды</b>\n\nНажмите на участника чтобы удалить его:",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=buttons),
    )
    await callback.answer()


@router.callback_query(F.data.startswith("bs_team_remove:"))
async def on_bs_team_remove(callback: CallbackQuery, platform_user: dict | None):
    if not platform_user:
        return
    parts = callback.data.split(":")
    child_bot_id, member_db_id = int(parts[1]), int(parts[2])
    owner_id = platform_user["user_id"]
    await db.execute(
        "UPDATE team_members SET is_active=false WHERE id=$1 AND owner_id=$2",
        member_db_id, owner_id,
    )
    await callback.answer("✅ Участник удалён из команды")
    # Перерендер списка
    callback.data = f"bs_team_members:{child_bot_id}"
    await on_bs_team_members(callback, platform_user)
