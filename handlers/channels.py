"""
handlers/channels.py — Подключение и управление площадками (каналами/группами).
"""
from aiogram import Router, F, Bot
from aiogram.types import Message, CallbackQuery, InlineKeyboardMarkup, InlineKeyboardButton
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup

import db.pool as db
from config import settings

router = Router()


# ── FSM ──────────────────────────────────────────────────────
class AddChannelFSM(StatesGroup):
    waiting_for_channel = State()


# ── Клавиатуры ───────────────────────────────────────────────
def kb_channels_list(channels: list) -> InlineKeyboardMarkup:
    buttons = []
    for ch in channels:
        status = "🟢" if ch["is_active"] else "⏸"
        buttons.append([
            InlineKeyboardButton(
                text=f"{status} {ch['chat_title'][:30]}",
                callback_data=f"channel:{ch['chat_id']}",
            )
        ])
    buttons.append([InlineKeyboardButton(text="➕ Подключить новую площадку", callback_data="channel:add")])
    buttons.append([InlineKeyboardButton(text="◀️ Назад", callback_data="menu:main")])
    return InlineKeyboardMarkup(inline_keyboard=buttons)


def kb_channel_detail(chat_id: int, is_active: bool) -> InlineKeyboardMarkup:
    status_text = "🟢 Активна" if is_active else "⏸ Приостановлена"
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="✅ Обработка заявок", callback_data=f"ch_requests:{chat_id}")],
        [
            InlineKeyboardButton(text="💬 Сообщения", callback_data=f"ch_messages:{chat_id}"),
            InlineKeyboardButton(text="🛡 Защита",    callback_data=f"ch_protection:{chat_id}"),
        ],
        [
            InlineKeyboardButton(text="🔗 Ссылки",    callback_data=f"ch_links:{chat_id}"),
            InlineKeyboardButton(text="📊 Статистика", callback_data=f"ch_stats:{chat_id}"),
        ],
        [InlineKeyboardButton(text=status_text, callback_data=f"ch_toggle:{chat_id}")],
        [InlineKeyboardButton(text="🗑 Удалить площадку", callback_data=f"ch_delete:{chat_id}")],
        [InlineKeyboardButton(text="◀️ Назад", callback_data="menu:channels")],
    ])


# ── Список площадок ───────────────────────────────────────────
@router.callback_query(F.data == "menu:channels")
async def on_channels_list(callback: CallbackQuery, platform_user: dict | None):
    if not platform_user:
        await callback.answer("Сначала зарегистрируйтесь через /start", show_alert=True)
        return

    channels = await db.fetch(
        "SELECT * FROM bot_chats WHERE owner_id=$1 ORDER BY added_at DESC",
        platform_user["user_id"],
    )

    if not channels:
        text = "📡 <b>Мои площадки</b>\n\nУ вас ещё нет подключённых площадок."
    else:
        text = f"📡 <b>Мои площадки</b>\n\nПодключено: {len(channels)}"

    await callback.message.edit_text(text, reply_markup=kb_channels_list(list(channels)))
    await callback.answer()


# ── Добавить площадку ────────────────────────────────────────
@router.callback_query(F.data == "channel:add")
async def on_channel_add(callback: CallbackQuery, state: FSMContext, platform_user: dict | None):
    if not platform_user:
        await callback.answer()
        return

    # Проверка лимита тарифа
    tariff = platform_user["tariff"]
    limit = settings.channel_limits.get(tariff, 1)
    count = await db.fetchval(
        "SELECT COUNT(*) FROM bot_chats WHERE owner_id=$1 AND is_active=true",
        platform_user["user_id"],
    )
    if count >= limit:
        await callback.answer(
            f"Достигнут лимит площадок для тарифа {tariff.title()} ({limit} шт.).\n"
            f"Обновите тариф для добавления большего числа каналов.",
            show_alert=True,
        )
        return

    await state.set_state(AddChannelFSM.waiting_for_channel)
    await callback.message.edit_text(
        "➕ <b>Подключение площадки</b>\n\n"
        "👉 Сделайте бота администратором вашего канала/группы,\n"
        "затем отправьте <b>ссылку на канал</b> или его <b>ID</b>:\n\n"
        "Примеры:\n• @mychannel\n• https://t.me/mychannel\n• -100123456789",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="🚫 Отменить", callback_data="menu:channels")]
        ]),
    )
    await callback.answer()


@router.message(AddChannelFSM.waiting_for_channel)
async def on_channel_input(message: Message, state: FSMContext, bot: Bot, platform_user: dict | None):
    if not platform_user:
        return

    raw = message.text.strip()

    # Преобразуем ссылку в username или ID
    if raw.startswith("https://t.me/"):
        raw = "@" + raw.split("/")[-1]

    try:
        chat = await bot.get_chat(raw)
    except Exception:
        await message.answer(
            "❌ Не удалось найти канал/группу.\n"
            "Убедитесь что:\n"
            "• Бот добавлен как администратор\n"
            "• Ссылка или ID указаны верно"
        )
        return

    # Проверка прав бота
    try:
        bot_member = await bot.get_chat_member(chat.id, (await bot.get_me()).id)
        if bot_member.status not in ("administrator", "creator"):
            await message.answer(
                "❌ Бот не является администратором этого канала/группы.\n"
                "Добавьте бота как администратора и попробуйте снова."
            )
            return
    except Exception:
        await message.answer("❌ Нет доступа к этому каналу. Проверьте права бота.")
        return

    # Compat: тип площадки
    chat_type = chat.type  # channel | supergroup | group

    try:
        await db.execute(
            """
            INSERT INTO bot_chats (owner_id, chat_id, chat_title, chat_type)
            VALUES ($1, $2, $3, $4)
            ON CONFLICT (owner_id, chat_id) DO UPDATE
              SET chat_title=$3, is_active=true
            """,
            platform_user["user_id"], chat.id, chat.title, chat_type,
        )
    except Exception as e:
        await message.answer(f"❌ Ошибка сохранения: {e}")
        return

    await state.clear()
    await message.answer(
        f"✅ Площадка подключена!\n\n"
        f"📢 <b>{chat.title}</b>\n"
        f"Тип: {'Канал' if chat_type == 'channel' else 'Группа'}\n\n"
        f"Теперь настройте её параметры в списке площадок."
    )


# ── Детали площадки ───────────────────────────────────────────
@router.callback_query(F.data.startswith("channel:") & ~F.data.endswith("add"))
async def on_channel_detail(callback: CallbackQuery, platform_user: dict | None):
    if not platform_user:
        return

    chat_id = int(callback.data.split(":")[1])
    ch = await db.fetchrow(
        "SELECT * FROM bot_chats WHERE owner_id=$1 AND chat_id=$2",
        platform_user["user_id"], chat_id,
    )
    if not ch:
        await callback.answer("Площадка не найдена", show_alert=True)
        return

    # Счётчики
    total = await db.fetchval(
        "SELECT COUNT(*) FROM bot_users WHERE owner_id=$1 AND chat_id=$2 AND is_active=true",
        platform_user["user_id"], chat_id,
    )

    await callback.message.edit_text(
        f"📡 <b>{ch['chat_title']}</b>\n\n"
        f"👥 Участников в базе: {total}\n"
        f"Тип: {'Закрытый канал 🔒' if ch['chat_type'] == 'channel' else 'Группа 👥'}\n"
        f"Статус: {'🟢 Активна' if ch['is_active'] else '⏸ Приостановлена'}",
        reply_markup=kb_channel_detail(chat_id, ch["is_active"]),
    )
    await callback.answer()


# ── Вкл/выкл площадки ────────────────────────────────────────
@router.callback_query(F.data.startswith("ch_toggle:"))
async def on_channel_toggle(callback: CallbackQuery, platform_user: dict | None):
    if not platform_user:
        return

    chat_id = int(callback.data.split(":")[1])
    ch = await db.fetchrow(
        "SELECT is_active FROM bot_chats WHERE owner_id=$1 AND chat_id=$2",
        platform_user["user_id"], chat_id,
    )
    if not ch:
        return

    new_status = not ch["is_active"]
    await db.execute(
        "UPDATE bot_chats SET is_active=$1 WHERE owner_id=$2 AND chat_id=$3",
        new_status, platform_user["user_id"], chat_id,
    )
    status = "🟢 Активна" if new_status else "⏸ Приостановлена"
    await callback.answer(f"Площадка: {status}")
    # Обновляем клавиатуру
    await on_channel_detail(callback, platform_user)


# ── Удаление площадки ─────────────────────────────────────────
@router.callback_query(F.data.startswith("ch_delete:"))
async def on_channel_delete(callback: CallbackQuery, platform_user: dict | None):
    if not platform_user:
        return

    chat_id = int(callback.data.split(":")[1])
    await db.execute(
        "DELETE FROM bot_chats WHERE owner_id=$1 AND chat_id=$2",
        platform_user["user_id"], chat_id,
    )
    await callback.answer("✅ Площадка удалена")
    # Возврат к списку
    await on_channels_list(callback, platform_user)
