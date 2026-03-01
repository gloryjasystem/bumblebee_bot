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


# ══════════════════════════════════════════════════════════════
# Настройки сообщений (из ch_messages:)
# ══════════════════════════════════════════════════════════════
def kb_messages(ch: dict) -> InlineKeyboardMarkup:
    chat_id = ch["chat_id"]
    has_welcome  = "✏️ Изменить" if ch.get("welcome_text") else "➕ Настроить"
    has_farewell = "✏️ Изменить" if ch.get("farewell_text") else "➕ Настроить"
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text=f"👋 Приветствие — {has_welcome}", callback_data=f"welcome_set:{chat_id}")],
        [InlineKeyboardButton(text=f"👋 Прощание — {has_farewell}",  callback_data=f"farewell_set:{chat_id}")],
        [InlineKeyboardButton(text="😄 Реакции на сообщения",         callback_data=f"reactions_set:{chat_id}")],
        [InlineKeyboardButton(text="◀️ Назад",                        callback_data=f"channel_by_chat:{chat_id}")],
    ])


@router.callback_query(F.data.startswith("ch_messages:"))
async def on_messages_menu(callback: CallbackQuery, platform_user: dict | None):
    if not platform_user:
        return
    chat_id = int(callback.data.split(":")[1])
    ch = await db.fetchrow(
        "SELECT * FROM bot_chats WHERE owner_id=$1 AND chat_id=$2::bigint",
        platform_user["user_id"], chat_id,
    )
    if not ch:
        return
    await callback.message.edit_text(
        "💬 <b>Сообщения</b>\n\nНастройте приветствие, прощание и реакции:",
        reply_markup=kb_messages(dict(ch)),
    )
    await callback.answer()


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
