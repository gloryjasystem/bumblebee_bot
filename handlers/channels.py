"""
handlers/channels.py — Подключение площадок через дочерний бот.
Флоу: Виды ботов → Ввод токена → Валидация → Добавить в канал/группу → Проверка → Подключено!
"""
import logging
from aiogram import Router, F
from aiogram.types import (
    Message, CallbackQuery,
    InlineKeyboardMarkup, InlineKeyboardButton,
)
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup

import db.pool as db
from services.child_bot_service import validate_and_save_child_bot, verify_bot_is_admin
from config import settings

logger = logging.getLogger(__name__)
router = Router()


class ChannelFSM(StatesGroup):
    waiting_for_token        = State()   # Ввод токена
    waiting_for_chat_verify  = State()   # Ввод @username или ID канала для проверки


# ══════════════════════════════════════════════════════════════
# 1. Список площадок
# ══════════════════════════════════════════════════════════════
@router.callback_query(F.data == "menu:channels")
async def on_channels_menu(callback: CallbackQuery, platform_user: dict | None):
    if not platform_user:
        await callback.answer("Выполните /start")
        return
    owner_id = platform_user["user_id"]

    channels = await db.fetch(
        """
        SELECT bc.id, bc.chat_title, bc.chat_type, bc.is_active,
               cb.bot_username
        FROM bot_chats bc
        JOIN child_bots cb ON bc.child_bot_id = cb.id
        WHERE bc.owner_id = $1
        ORDER BY bc.added_at DESC
        """,
        owner_id,
    )

    tariff = platform_user["tariff"]
    limit  = settings.channel_limits.get(tariff, 1)
    count  = len(channels)

    buttons = []
    for ch in channels:
        icon = "🟢" if ch["is_active"] else "🔴"
        type_icon = "📢" if ch["chat_type"] == "channel" else "👥"
        buttons.append([InlineKeyboardButton(
            text=f"{icon} {type_icon} {ch['chat_title'] or 'Без названия'} · @{ch['bot_username']}",
            callback_data=f"channel:{ch['id']}",
        )])

    if count < limit:
        buttons.append([InlineKeyboardButton(
            text="➕ Подключить новую площадку",
            callback_data="channel:new",
        )])
    else:
        buttons.append([InlineKeyboardButton(
            text=f"🔒 Лимит площадок ({count}/{limit}) — улучшите тариф",
            callback_data="menu:tariffs",
        )])

    buttons.append([InlineKeyboardButton(text="◀️ Назад", callback_data="menu:main")])

    await callback.message.edit_text(
        f"📡 <b>Мои площадки</b>\n\n"
        f"Подключено: {count}/{limit} (тариф {tariff.capitalize()})",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=buttons),
    )
    await callback.answer()


# ══════════════════════════════════════════════════════════════
# 2. Выбор типа бота (Виды ботов)
# ══════════════════════════════════════════════════════════════
@router.callback_query(F.data == "channel:new")
async def on_channel_new(callback: CallbackQuery, state: FSMContext, platform_user: dict | None):
    if not platform_user:
        return
    await state.clear()
    await callback.message.edit_text(
        "🐝 <b>Виды ботов</b>\n\n"
        "👋 <b>Бот приветствий</b> — автоматически обрабатывает заявки, "
        "отправляет приветственные сообщения и собирает базу пользователей для рассылок.\n\n"
        "<b>Выберите действие:</b>",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="👋 Бот приветствий", callback_data="bot_type:welcome")],
            [InlineKeyboardButton(text="🚫 Отменить",         callback_data="menu:channels")],
        ]),
    )
    await callback.answer()


# ══════════════════════════════════════════════════════════════
# 3. Запрос токена
# ══════════════════════════════════════════════════════════════
@router.callback_query(F.data == "bot_type:welcome")
async def on_bot_type_welcome(callback: CallbackQuery, state: FSMContext, platform_user: dict | None):
    if not platform_user:
        return
    await state.set_state(ChannelFSM.waiting_for_token)
    await state.update_data(owner_id=platform_user["user_id"])

    await callback.message.edit_text(
        "👋 Чтобы создать <b>«Бота приветствий»</b>, мне нужен токен:\n\n"
        "① Перейдите в @BotFather\n\n"
        "② Отправьте @BotFather команду: <code>/newbot</code>\n\n"
        "③ Придумайте название и юзернейм для вашего бота,\n"
        '   например: "Новости" → <code>@newsbot</code>\n\n'
        "④ @BotFather выдаст вам токен бота. Пример токена:\n"
        "   <code>5827254996:AAEBu9108achvHoWvPmvr6kueDgmFpJMjHo</code>\n\n"
        "<b>Отправьте токен бота</b> 👇",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="🚫 Отменить", callback_data="menu:channels")],
        ]),
    )
    await callback.answer()


@router.message(ChannelFSM.waiting_for_token)
async def on_token_received(message: Message, state: FSMContext, platform_user: dict | None):
    if not platform_user:
        return

    token = message.text.strip() if message.text else ""
    msg = await message.answer("⏳ Проверяю токен...")

    try:
        bot_info = await validate_and_save_child_bot(platform_user["user_id"], token)
    except ValueError as e:
        await msg.edit_text(
            f"❌ {e}",
            reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                [InlineKeyboardButton(text="🔄 Попробовать снова", callback_data="bot_type:welcome")],
                [InlineKeyboardButton(text="🚫 Отменить",          callback_data="menu:channels")],
            ]),
        )
        await state.clear()
        return

    # Запускаем polling для нового дочернего бота (слушает my_chat_member и join requests)
    try:
        from scheduler.child_bot_runner import start_child_bot
        bot_row = await db.fetchrow(
            "SELECT token_encrypted FROM child_bots WHERE id=$1",
            bot_info["id"],
        )
        if bot_row:
            await start_child_bot(
                bot_info["id"],
                platform_user["user_id"],
                bot_info["bot_username"],
                bot_row["token_encrypted"],
            )
    except Exception as e:
        logger.warning(f"Could not start child bot runner: {e}")

    # Сохраняем child_bot_id в state для следующих шагов
    await state.update_data(
        child_bot_id=bot_info["id"],
        bot_username=bot_info["bot_username"],
    )
    await state.set_state(ChannelFSM.waiting_for_chat_verify)

    username = bot_info["bot_username"]

    deep_channel = f"https://t.me/{username}?startchannel=true&admin=post_messages+delete_messages+invite_users+restrict_members+pin_messages"
    deep_group   = f"https://t.me/{username}?startgroup=true&admin=post_messages+delete_messages+invite_users+restrict_members+pin_messages"

    await msg.edit_text(
        f"✅ Бот: @{username} создан\n\n"
        f"➕ Добавьте бота в <b>канал или группу</b> в качестве администратора "
        f"с правами на «Добавление участников» (ios) → «Пригласительные ссылки» (android).\n\n"
        f"👥 Он будет обрабатывать заявки, приветствовать пользователей "
        f"и собирать их в базу для рассылок.\n\n"
        f"<b>Выберите действие</b> 👇",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="→ Добавить в канал", url=deep_channel)],
            [InlineKeyboardButton(text="→ Добавить в группу", url=deep_group)],
            [InlineKeyboardButton(text="✅ Проверить подключение", callback_data=f"verify_bot:{bot_info['id']}")],
            [InlineKeyboardButton(text="⊃ В меню", callback_data="menu:channels")],
        ]),
    )


# ══════════════════════════════════════════════════════════════
# 4. Проверка подключения
# ══════════════════════════════════════════════════════════════
@router.callback_query(F.data.startswith("verify_bot:"))
async def on_verify_bot(callback: CallbackQuery, state: FSMContext, platform_user: dict | None):
    if not platform_user:
        return
    child_bot_id = int(callback.data.split(":")[1])
    await state.set_state(ChannelFSM.waiting_for_chat_verify)
    await state.update_data(child_bot_id=child_bot_id, owner_id=platform_user["user_id"])

    await callback.message.edit_text(
        "📡 <b>Проверить подключение</b>\n\n"
        "Введите <b>@username</b> или <b>ID</b> канала/группы, "
        "куда вы добавили бота:\n\n"
        "Например: <code>@mychannel</code> или <code>-1001234567890</code>",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="🚫 Отменить", callback_data="menu:channels")],
        ]),
    )
    await callback.answer()


@router.message(ChannelFSM.waiting_for_chat_verify)
async def on_chat_verify_input(message: Message, state: FSMContext, platform_user: dict | None):
    if not platform_user:
        return

    data = await state.get_data()
    child_bot_id = data.get("child_bot_id")
    owner_id     = platform_user["user_id"]

    if not child_bot_id:
        await state.clear()
        return

    chat_input = message.text.strip() if message.text else ""
    msg = await message.answer("⏳ Проверяю подключение...")

    try:
        chat_info = await verify_bot_is_admin(owner_id, child_bot_id, chat_input)
    except ValueError as e:
        await msg.edit_text(
            f"❌ {e}",
            reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                [InlineKeyboardButton(text="🔄 Попробовать снова", callback_data=f"verify_bot:{child_bot_id}")],
                [InlineKeyboardButton(text="⊃ В меню",             callback_data="menu:channels")],
            ]),
        )
        return

    # Проверяем лимит
    tariff = platform_user["tariff"]
    limit  = settings.channel_limits.get(tariff, 1)
    count  = await db.fetchval(
        "SELECT COUNT(*) FROM bot_chats WHERE owner_id=$1", owner_id,
    )
    if count >= limit:
        await msg.edit_text(
            f"🔒 Достигнут лимит площадок ({limit}) для тарифа {tariff.capitalize()}.",
            reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                [InlineKeyboardButton(text="💳 Улучшить тариф", callback_data="menu:tariffs")],
                [InlineKeyboardButton(text="◀️ Назад",           callback_data="menu:channels")],
            ]),
        )
        await state.clear()
        return

    # Сохраняем площадку в БД
    await db.execute(
        """
        INSERT INTO bot_chats (owner_id, child_bot_id, chat_id, chat_title, chat_type)
        VALUES ($1, $2, $3, $4, $5)
        ON CONFLICT (owner_id, chat_id)
        DO UPDATE SET chat_title=EXCLUDED.chat_title,
                      child_bot_id=EXCLUDED.child_bot_id,
                      is_active=true
        """,
        owner_id, child_bot_id,
        chat_info["chat_id"], chat_info["chat_title"], chat_info["chat_type"],
    )
    await state.clear()

    type_icon = "📢" if chat_info["chat_type"] == "channel" else "👥"
    await msg.edit_text(
        f"🎉 {type_icon} <b>{chat_info['chat_title']}</b> подключён!\n\n"
        f"Бот активен и готов к работе. Перейдите в настройки площадки.",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="⚙️ Настройки площадки", callback_data=f"channel_by_chat:{chat_info['chat_id']}")],
            [InlineKeyboardButton(text="📡 Мои площадки",        callback_data="menu:channels")],
        ]),
    )


# ══════════════════════════════════════════════════════════════
# 5. Детали площадки
# ══════════════════════════════════════════════════════════════
@router.callback_query(F.data.startswith("channel:"))
async def on_channel_detail(callback: CallbackQuery, platform_user: dict | None):
    if not platform_user:
        return
    ch_id_str = callback.data.split(":")[1]
    if ch_id_str == "new":
        return

    ch_id = int(ch_id_str)
    ch = await db.fetchrow(
        """
        SELECT bc.*, cb.bot_username, cb.bot_name
        FROM bot_chats bc
        JOIN child_bots cb ON bc.child_bot_id = cb.id
        WHERE bc.id=$1 AND bc.owner_id=$2
        """,
        ch_id, platform_user["user_id"],
    )
    if not ch:
        await callback.answer("Площадка не найдена", show_alert=True)
        return

    chat_id  = ch["chat_id"]
    owner_id = platform_user["user_id"]

    # ── Статистика пользователей ──────────────────────────────
    from datetime import date, timedelta
    today     = date.today()
    yesterday = today - timedelta(days=1)

    total_users = await db.fetchval(
        "SELECT COUNT(*) FROM bot_users WHERE owner_id=$1 AND chat_id=$2", owner_id, chat_id
    ) or 0
    today_users = await db.fetchval(
        "SELECT COUNT(*) FROM bot_users WHERE owner_id=$1 AND chat_id=$2 AND joined_at::date=$3",
        owner_id, chat_id, today,
    ) or 0
    yesterday_users = await db.fetchval(
        "SELECT COUNT(*) FROM bot_users WHERE owner_id=$1 AND chat_id=$2 AND joined_at::date=$3",
        owner_id, chat_id, yesterday,
    ) or 0
    pending_requests = await db.fetchval(
        "SELECT COUNT(*) FROM bot_users WHERE owner_id=$1 AND chat_id=$2 AND is_active=false AND left_at IS NULL",
        owner_id, chat_id,
    ) or 0
    active_users = await db.fetchval(
        "SELECT COUNT(*) FROM bot_users WHERE owner_id=$1 AND chat_id=$2 AND is_active=true AND bot_activated=true",
        owner_id, chat_id,
    ) or 0
    dead_users = total_users - active_users

    # ── Формируем текст ───────────────────────────────────────
    text = (
        f"🤖 Бот: @{ch['bot_username']}\n\n"
        f"<u>👥 Пользователей</u>\n"
        f"├ Сегодня ≈ {today_users}\n"
        f"├ Вчера ≈ {yesterday_users}\n"
        f"├ Всего ≈ {total_users}\n"
        f"└ Заявок в очереди ≈ {pending_requests}\n\n"
        f"<u>💬 Сообщений</u>\n"
        f"├ Сегодня ≈ 0\n"
        f"├ Вчера ≈ 0\n"
        f"└ Всего ≈ 0\n\n"
        f"🟢 Живые ≈ {active_users}    🔴 Мёртвые ≈ {max(dead_users, 0)}"
    )

    toggle_text = "🔴 Выключить" if ch["is_active"] else "🟢 Включить"

    await callback.message.edit_text(
        text,
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="✅ Обработка заявок",    callback_data=f"ch_requests:{chat_id}")],
            [
                InlineKeyboardButton(text="💬 Сообщения",        callback_data=f"ch_messages:{chat_id}"),
                InlineKeyboardButton(text="📨 Рассылка",         callback_data=f"ch_mailing:{chat_id}"),
            ],
            [
                InlineKeyboardButton(text="🔗 Ссылки",           callback_data=f"ch_links:{chat_id}"),
                InlineKeyboardButton(text="📡 Площадки",         callback_data="menu:channels"),
            ],
            [
                InlineKeyboardButton(text="🛡 Защита",           callback_data=f"ch_protection:{chat_id}"),
                InlineKeyboardButton(text="⚙️ Управление",       callback_data=f"ch_settings:{ch_id}"),
            ],
            [InlineKeyboardButton(text="📣 Обратная связь",      callback_data=f"ch_feedback:{chat_id}")],
            [InlineKeyboardButton(text=f"🗑 Удалить бот",        callback_data=f"ch_delete:{ch_id}")],
            [InlineKeyboardButton(text="◀️ Назад",               callback_data="menu:channels")],
        ]),
    )
    await callback.answer()



@router.callback_query(F.data.startswith("channel_by_chat:"))
async def on_channel_by_chat(callback: CallbackQuery, platform_user: dict | None):
    """Переход в настройки площадки по chat_id."""
    if not platform_user:
        return
    chat_id = int(callback.data.split(":")[1])
    ch = await db.fetchrow(
        "SELECT id FROM bot_chats WHERE owner_id=$1 AND chat_id=$2",
        platform_user["user_id"], chat_id,
    )
    if ch:
        callback.data = f"channel:{ch['id']}"
        await on_channel_detail(callback, platform_user)
    else:
        await callback.answer("Площадка не найдена")


@router.callback_query(F.data.startswith("ch_toggle:"))
async def on_ch_toggle(callback: CallbackQuery, platform_user: dict | None):
    if not platform_user:
        return
    ch_id = int(callback.data.split(":")[1])
    ch = await db.fetchrow(
        "SELECT is_active FROM bot_chats WHERE id=$1 AND owner_id=$2",
        ch_id, platform_user["user_id"],
    )
    if not ch:
        return
    new_val = not ch["is_active"]
    await db.execute(
        "UPDATE bot_chats SET is_active=$1 WHERE id=$2 AND owner_id=$3",
        new_val, ch_id, platform_user["user_id"],
    )
    await callback.answer("🟢 Включена" if new_val else "🔴 Выключена")
    callback.data = f"channel:{ch_id}"
    await on_channel_detail(callback, platform_user)


@router.callback_query(F.data.startswith("ch_delete:"))
async def on_ch_delete(callback: CallbackQuery, platform_user: dict | None):
    if not platform_user:
        return
    ch_id = int(callback.data.split(":")[1])

    # Подтверждение
    await callback.message.edit_text(
        "⚠️ <b>Удалить площадку?</b>\n\nВся история, настройки и ЧС для этой площадки будут удалены.",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="✅ Да, удалить", callback_data=f"ch_delete_confirm:{ch_id}")],
            [InlineKeyboardButton(text="🚫 Отмена",      callback_data=f"channel:{ch_id}")],
        ]),
    )
    await callback.answer()


@router.callback_query(F.data.startswith("ch_delete_confirm:"))
async def on_ch_delete_confirm(callback: CallbackQuery, platform_user: dict | None):
    if not platform_user:
        return
    ch_id = int(callback.data.split(":")[1])
    await db.execute(
        "DELETE FROM bot_chats WHERE id=$1 AND owner_id=$2",
        ch_id, platform_user["user_id"],
    )
    await callback.answer("✅ Площадка удалена")
    callback.data = "menu:channels"
    await on_channels_menu(callback, platform_user)
