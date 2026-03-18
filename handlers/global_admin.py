import logging
from aiogram import Router, F, Bot
from aiogram.types import Message, CallbackQuery, InlineKeyboardMarkup, InlineKeyboardButton
from aiogram.filters import Command
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup

from db.pool import get_pool

logger = logging.getLogger(__name__)
router = Router()


class BotNetworkFSM(StatesGroup):
    waiting_search = State()


BOTS_PER_PAGE = 7


async def get_admin_context(user_id: int, username: str = None):
    """
    Returns (role, target_owner_id).
    role: 'owner', 'admin', or None.
    """
    from config import settings
    un = (username or "").lower().lstrip("@")
    is_project_owner = (
        user_id == settings.owner_telegram_id
        or un == settings.owner_username.lower().lstrip("@")
    )
    if is_project_owner:
        return 'owner', user_id

    async with get_pool().acquire() as conn:
        # Проверяем, является ли он наёмным глобальным админом
        owner_id = await conn.fetchval("SELECT owner_id FROM global_admins WHERE admin_id=$1 LIMIT 1", user_id)
        if owner_id:
            return 'admin', owner_id
            
    return None, None

@router.message(Command("admin"))
async def cmd_admin(message: Message, state: FSMContext):
    role, owner_id = await get_admin_context(message.from_user.id, message.from_user.username)
    if not role:
        return await message.answer("❌ Эта панель доступна только администраторам сети ботов.")
        
    await state.clear()
    await _show_admin_panel(message, role, owner_id)

async def _show_admin_panel(message_or_cb, role: str, owner_id: int):
    async with get_pool().acquire() as conn:
        total_bots = await conn.fetchval("SELECT COUNT(*) FROM child_bots WHERE owner_id=$1", owner_id) or 0
        net_bots = await conn.fetchval("SELECT COUNT(*) FROM child_bots WHERE owner_id=$1 AND in_global_network=true", owner_id) or 0
        total_users = await conn.fetchval("""
            SELECT COUNT(DISTINCT bu.user_id) FROM bot_users bu
            JOIN bot_chats bc ON bu.chat_id = bc.chat_id
            JOIN child_bots cb ON cb.id = bc.child_bot_id
            WHERE bc.owner_id=$1 AND bc.is_active=true AND bu.user_id IS NOT NULL
            AND cb.in_global_network=true
        """, owner_id) or 0
        bl_count = await conn.fetchval("SELECT COUNT(*) FROM blacklist WHERE owner_id=$1", owner_id) or 0
        admin_count = await conn.fetchval("SELECT COUNT(*) FROM global_admins WHERE owner_id=$1", owner_id) or 0

    if role == 'owner':
        header = (
            "🌐 <b>BotCloud — Глобальная Панель</b> • 👑 Owner\n"
            "─────────────────────────────\n"
            f"⚡️ Активных ботов: <b>{net_bots} из {total_bots}</b>\n"
            f"👥 Аудитория (активных): <b>{total_users:,}</b>\n"
            f"🚫 Записей в ЧС: <b>{bl_count}</b>   │   👷 Сотрудников: <b>{admin_count}</b>"
        )
    else:
        header = (
            "🌐 <b>BotCloud — Глобальная Панель</b> • 👷 Admin\n"
            "─────────────────────────────\n"
            f"👥 Активная аудитория сети: <b>{total_users:,}</b>\n"
            f"🚫 Записей в ЧС: <b>{bl_count}</b>"
        )

    kb = []
    if role == 'owner':
        kb.append([
            InlineKeyboardButton(text="⚙️ Сотрудники", callback_data=f"ga_team:{owner_id}"),
            InlineKeyboardButton(text="📊 Аналитика", callback_data=f"ga_stats:{owner_id}")
        ])
    kb.append([InlineKeyboardButton(text="⚡️ Активация ботов сети", callback_data=f"ga_bots:{owner_id}:0")])
    kb.append([InlineKeyboardButton(text="🚫 Глобальный ЧС — 🛡️ Защита сети", callback_data=f"ga_bl:{owner_id}")])
    kb.append([InlineKeyboardButton(text="👥 База аудитории — Выгрузка CSV", callback_data=f"ga_users:{owner_id}")])
    kb.append([InlineKeyboardButton(text="📢 Рассылки и Личные сообщения", callback_data=f"ga_broadcast:{owner_id}")])
    kb.append([InlineKeyboardButton(text="📜 Журнал действий (Audit Log)", callback_data=f"ga_audit:{owner_id}")])

    markup = InlineKeyboardMarkup(inline_keyboard=kb)
    if isinstance(message_or_cb, Message):
        await message_or_cb.answer(header, reply_markup=markup, parse_mode="HTML")
    else:
        await message_or_cb.message.edit_text(header, reply_markup=markup, parse_mode="HTML")

@router.callback_query(F.data.startswith("ga_main:"))
async def on_ga_main(callback: CallbackQuery):
    owner_id = int(callback.data.split(":")[1])
    role, _ = await get_admin_context(callback.from_user.id, callback.from_user.username)
    if not role:
        return await callback.answer("❌ Нет прав", show_alert=True)
    await _show_admin_panel(callback, role, owner_id)
    await callback.answer()


@router.callback_query(F.data.startswith("ga_team:"))
async def on_ga_team(callback: CallbackQuery):
    owner_id = int(callback.data.split(":")[1])
    role, _ = await get_admin_context(callback.from_user.id, callback.from_user.username)
    if role != 'owner':
        return await callback.answer("❌ Только для Владельца", show_alert=True)

    async with get_pool().acquire() as conn:
        rows = await conn.fetch(
            "SELECT admin_id, admin_username, added_at FROM global_admins WHERE owner_id=$1 ORDER BY added_at DESC",
            owner_id
        )

    lines = []
    for idx, r in enumerate(rows, 1):
        name = f"@{r['admin_username']}" if r['admin_username'] else f"<code>{r['admin_id']}</code>"
        dt = r['added_at'].strftime('%d.%m.%Y')
        lines.append(f"{idx}. {name}  —  ️ {dt}")

    staff_block = "\n".join(lines) if lines else "❎ Список пуст. Добавьте первого сотрудника."

    text = (
        "⚙️ <b>Управление Сотрудниками</b>\n"
        "─────────────────────────────\n"
        f"👷 Всего сотрудников: <b>{len(rows)}</b>\n\n"
        f"{staff_block}\n\n"
        "ℹ️ Сотрудник видит ЧС, базу аудитории и запускает рассылки, но не имеет доступа к финансам."
    )
    kb = [
        [InlineKeyboardButton(text="➕ Добавить админа", callback_data=f"ga_team_howto:{owner_id}")],
        [InlineKeyboardButton(text="◀️ Назад", callback_data=f"ga_main:{owner_id}")]
    ]
    await callback.message.edit_text(text, parse_mode="HTML", reply_markup=InlineKeyboardMarkup(inline_keyboard=kb))
    await callback.answer()


@router.callback_query(F.data.startswith("ga_team_howto:"))
async def on_ga_team_howto(callback: CallbackQuery):
    owner_id = int(callback.data.split(":")[1])
    text = (
        "ℹ️ <b>Как добавить администратора:</b>\n\n"
        "Отправьте команду: <code>/addadmin 123456789</code>\n"
        "где <code>123456789</code> — Telegram ID страницы / сотрудника.\n\n"
        "Узнать свой ID можно через <a href='https://t.me/userinfobot'>@userinfobot</a>\n\n"
        "Удалить админа: <code>/removeadmin 123456789</code>"
    )
    kb = [[InlineKeyboardButton(text="◀️ Назад", callback_data=f"ga_team:{owner_id}")]]
    await callback.message.edit_text(text, parse_mode="HTML", reply_markup=InlineKeyboardMarkup(inline_keyboard=kb),
                                     disable_web_page_preview=True)
    await callback.answer()


@router.callback_query(F.data.startswith("ga_audit:"))
async def on_ga_audit(callback: CallbackQuery):
    owner_id = int(callback.data.split(":")[1])
    role, _ = await get_admin_context(callback.from_user.id, callback.from_user.username)
    if not role:
        return await callback.answer("❌ Нет прав", show_alert=True)

    async with get_pool().acquire() as conn:
        rows = await conn.fetch("""
            SELECT action, entity_type, entity_id, details, created_at, user_id
            FROM audit_log
            WHERE owner_id = $1
            ORDER BY created_at DESC
            LIMIT 20
        """, owner_id)

    if not rows:
        text = "📜 <b>Журнал Модерации</b>\n\nДействий пока не зафиксировано."
    else:
        lines = ["📜 <b>Журнал Модерации</b> (последние 20):\n"]
        for r in rows:
            dt = r['created_at'].strftime("%d.%m %H:%M")
            uid = r.get('user_id', '?')
            action = r.get('action', '')
            details = r.get('details', '') or ''
            lines.append(f"• [{dt}] <code>{uid}</code> → {action} {details[:40]}")
        text = "\n".join(lines)

    kb = [[InlineKeyboardButton(text="◀️ Назад", callback_data=f"ga_main:{owner_id}")]]
    await callback.message.edit_text(text, parse_mode="HTML", reply_markup=InlineKeyboardMarkup(inline_keyboard=kb))
    await callback.answer()


@router.message(Command("admin_help"))
async def cmd_admin_help(message: Message):
    role, owner_id = await get_admin_context(message.from_user.id, message.from_user.username)
    if not role:
        return await message.answer("❌ У вас нет доступа к командам глобального управления.")
        
    text = "🌐 <b>Справка по Глобальному Управлению</b>\n\n"
    if role == 'owner':
        text += (
            "<b>Только для Владельца:</b>\n"
            "<code>/addadmin &lt;id&gt;</code> — Выдать права администратора\n"
            "<code>/removeadmin &lt;id&gt;</code> — Снять права администратора\n"
            "<code>/admins</code> — Список всех администраторов\n"
            "<code>/stats</code> — Общая сетевая статистика\n"
            "<code>/revenue [today|week|month|all]</code> — Отчет по доходам\n\n"
        )
        
    text += (
        "<b>Общие команды (Owner + Admin):</b>\n"
        "<code>/block &lt;id/@username&gt;</code> — Занести в Глобальный ЧС\n"
        "<code>/unblock &lt;id/@username&gt;</code> — Разблокировать глобально\n"
        "<code>/users [all|active|blocked|admins]</code> — Выгрузка списков (CSV)\n"
        "<code>/broadcast &lt;текст&gt;</code> — Глобальная рассылка\n"
        "<code>/notify &lt;id&gt; &lt;текст&gt;</code> — Личное сообщение юзеру сети\n"
        "<code>/admin</code> — Открыть <b>Панель Управления</b> (Кнопки)\n"
    )
    
    await message.answer(text, parse_mode="HTML")


@router.message(Command("addadmin"))
async def cmd_addadmin(message: Message):
    role, owner_id = await get_admin_context(message.from_user.id, message.from_user.username)
    if not role:
        return await message.answer("❌ Нет доступа. Роль не найдена.")
    if role != 'owner':
        return await message.answer("❌ Эта команда доступна только Владельцу.")
        
    args = message.text.split(maxsplit=1)
    if len(args) < 2:
        return await message.answer("❌ Используйте: <code>/addadmin &lt;user_id&gt;</code>\nУкажите числовой ID пользователя Telegram.")
        
    try:
        target_id = int(args[1])
    except ValueError:
        return await message.answer("❌ Неверный формат ID. Пожалуйста, укажите числовой ID пользователя Telegram.")
        
    import asyncpg
    async with get_pool().acquire() as conn:
        try:
            await conn.execute(
                "INSERT INTO global_admins (owner_id, admin_id) VALUES ($1, $2)",
                owner_id, target_id
            )
            await message.answer(f"✅ Пользователь <code>{target_id}</code> успешно назначен глобальным администратором.\nУправление: /admins")
        except asyncpg.UniqueViolationError:
            await message.answer(f"⚠️ Этот пользователь уже является администратором.")


@router.message(Command("removeadmin"))
async def cmd_removeadmin(message: Message):
    role, owner_id = await get_admin_context(message.from_user.id, message.from_user.username)
    if not role:
        return await message.answer("❌ Нет доступа. Роль не найдена.")
    if role != 'owner':
        return await message.answer("❌ Эта команда доступна только Владельцу.")
        
    args = message.text.split(maxsplit=1)
    if len(args) < 2:
        return await message.answer("❌ Используйте: <code>/removeadmin &lt;user_id&gt;</code>")
        
    try:
        target_id = int(args[1])
    except ValueError:
        return await message.answer("❌ Неверный формат ID.")
        
    async with get_pool().acquire() as conn:
        res = await conn.execute("DELETE FROM global_admins WHERE owner_id=$1 AND admin_id=$2", owner_id, target_id)
        if res == "DELETE 0":
            await message.answer(f"⚠️ Пользователь <code>{target_id}</code> не был найден в списке глобальных администраторов.")
        else:
            await message.answer(f"✅ Права пользователя <code>{target_id}</code> успешно отозваны.")


@router.message(Command("admins"))
async def cmd_admins(message: Message):
    role, owner_id = await get_admin_context(message.from_user.id, message.from_user.username)
    if not role:
        return await message.answer("❌ Нет доступа. Роль не найдена.")
    if role != 'owner':
        return await message.answer("❌ Эта команда доступна только Владельцу.")
        
    async with get_pool().acquire() as conn:
        rows = await conn.fetch("SELECT admin_id, admin_username, added_at FROM global_admins WHERE owner_id=$1", owner_id)
        
    if not rows:
        return await message.answer("📋 <b>Глобальные Администраторы:</b>\n\nСписок пуст. Вы можете добавить админа командой: <code>/addadmin &lt;user_id&gt;</code>")
        
    lines = ["📋 <b>Глобальные Администраторы:</b>\n"]
    for idx, r in enumerate(rows, start=1):
        username_str = f" (@{r['admin_username']})" if r['admin_username'] else ""
        lines.append(f"{idx}. <code>{r['admin_id']}</code>{username_str} — {r['added_at'].strftime('%d.%m.%Y')}")
        
    lines.append("\nУдалить: <code>/removeadmin &lt;user_id&gt;</code>")
    await message.answer("\n".join(lines), parse_mode="HTML")


async def _build_global_stats(owner_id: int, conn) -> str:
    # 1. Считаем глобальную аудиторию (уникальные user_id во всех ботах владельца)
    total_users = await conn.fetchval("""
        SELECT COUNT(DISTINCT bu.user_id) FROM bot_users bu
        JOIN bot_chats bc ON bu.chat_id = bc.chat_id
        WHERE bc.owner_id = $1 AND bc.is_active=true AND bu.user_id IS NOT NULL
    """, owner_id) or 0
    
    alive_users = await conn.fetchval("""
        SELECT COUNT(DISTINCT bu.user_id) FROM bot_users bu
        JOIN bot_chats bc ON bu.chat_id = bc.chat_id
        WHERE bc.owner_id = $1 AND bc.is_active=true AND bu.is_active=true AND bu.user_id IS NOT NULL
    """, owner_id) or 0
    
    dead_users = total_users - alive_users
    
    # 2. Боты и площадки
    total_bots = await conn.fetchval("SELECT COUNT(*) FROM child_bots WHERE owner_id=$1", owner_id) or 0
    total_chats = await conn.fetchval("SELECT COUNT(*) FROM bot_chats WHERE owner_id=$1 AND is_active=true", owner_id) or 0
    
    # 3. База ЧС
    blacklist_count = await conn.fetchval("SELECT COUNT(*) FROM blacklist WHERE owner_id=$1", owner_id) or 0

    return (
        "📊 <b>Глобальная Сетевая Статистика</b>\n\n"
        f"🤖 Подключено ботов: <b>{total_bots}</b>\n"
        f"📍 Активных каналов/групп: <b>{total_chats}</b>\n\n"
        f"👥 Уникальная аудитория: <b>{total_users:,}</b>\n"
        f" ├ 🟢 Живые: {alive_users:,}\n"
        f" └ 🔴 Мёртвые: {dead_users:,}\n\n"
        f"🚫 В глобальном ЧС: <b>{blacklist_count}</b>"
    )


@router.message(Command("stats"))
async def cmd_stats(message: Message):
    role, owner_id = await get_admin_context(message.from_user.id, message.from_user.username)
    if not role:
        return await message.answer("❌ Нет доступа.")
    if role != 'owner':
        return await message.answer("❌ Нет прав. Эта команда доступна только Владельцу.")
        
    async with get_pool().acquire() as conn:
        text = await _build_global_stats(owner_id, conn)
        
    await message.answer(text, parse_mode="HTML")


@router.callback_query(F.data.startswith("ga_stats:"))
async def on_ga_stats(callback: CallbackQuery):
    role, owner_id = await get_admin_context(callback.from_user.id, callback.from_user.username)
    if role != 'owner':
        return await callback.answer("❌ Нет прав", show_alert=True)
        
    async with get_pool().acquire() as conn:
        text = await _build_global_stats(owner_id, conn)
        
    kb = [
        [
            InlineKeyboardButton(text="💸 Платежи", callback_data=f"ga_rev:{owner_id}:all"),
            InlineKeyboardButton(text="🏆 Топ ботов", callback_data=f"ga_top_bots:{owner_id}")
        ],
        [InlineKeyboardButton(text="◀️ Назад", callback_data=f"ga_main:{owner_id}")],
    ]
    
    await callback.message.edit_text(text, parse_mode="HTML", reply_markup=InlineKeyboardMarkup(inline_keyboard=kb))
    await callback.answer()


@router.message(Command("revenue"))
@router.callback_query(F.data.startswith("ga_rev:"))
async def on_ga_revenue(event: Message | CallbackQuery):
    role, owner_id = await get_admin_context(event.from_user.id, event.from_user.username)
    if not role:
        if isinstance(event, Message):
            return await event.answer("❌ Нет прав.")
        return await event.answer("❌ Нет прав", show_alert=True)
    if role != 'owner':
        if isinstance(event, Message):
            return await event.answer("❌ Нет прав. Для владельца.")
        return await event.answer("❌ Нет прав", show_alert=True)
        
    async with get_pool().acquire() as conn:
        rows = await conn.fetch("SELECT tariff, amount_usd, status, created_at FROM payments WHERE user_id=$1 ORDER BY created_at DESC LIMIT 5", owner_id)
        
    text = "💸 <b>Ваши последние платежи по тарифам:</b>\n\n"
    if not rows:
        text += "Вы еще не совершали оплат."
    else:
        for r in rows:
            icon = "✅" if r['status'] == 'paid' else "⏳"
            text += f"{icon} <b>{r['tariff'].upper()}</b> — ${r['amount_usd']} (<i>{r['created_at'].strftime('%d.%m.%Y')}</i>)\n"
            
    if isinstance(event, Message):
        await event.answer(text, parse_mode="HTML")
    else:
        kb = [[InlineKeyboardButton(text="◀️ Назад в Статистику", callback_data=f"ga_stats:{owner_id}")]]
        await event.message.edit_text(text, parse_mode="HTML", reply_markup=InlineKeyboardMarkup(inline_keyboard=kb))
        await event.answer()


@router.callback_query(F.data.startswith("ga_top_bots:"))
async def on_ga_top_bots(callback: CallbackQuery):
    role, owner_id = await get_admin_context(callback.from_user.id, callback.from_user.username)
    if role != 'owner':
        return await callback.answer("❌ Нет прав", show_alert=True)
        
    async with get_pool().acquire() as conn:
        rows = await conn.fetch("""
            SELECT bc.chat_title, bc.chat_type, COUNT(bu.user_id) as users_count
            FROM bot_chats bc
            LEFT JOIN bot_users bu ON bu.chat_id = bc.chat_id AND bu.is_active = true
            WHERE bc.owner_id = $1 AND bc.is_active = true
            GROUP BY bc.id, bc.chat_title, bc.chat_type
            ORDER BY users_count DESC
            LIMIT 15
        """, owner_id)
        
    text = "🏆 <b>Топ площадок (По активной аудитории)</b>\n\n"
    if not rows:
        text += "Нет активных площадок."
    else:
        for idx, r in enumerate(rows, 1):
            icon = "📢" if r['chat_type'] == 'channel' else "👥"
            text += f"{idx}. {icon} <b>{r['chat_title']}</b> — {r['users_count']} чел.\n"
            
    kb = [[InlineKeyboardButton(text="◀️ Назад в Статистику", callback_data=f"ga_stats:{owner_id}")]]
    await callback.message.edit_text(text, parse_mode="HTML", reply_markup=InlineKeyboardMarkup(inline_keyboard=kb))
    await callback.answer()


async def _kick_from_all_chats(owner_id: int, target_user_id: int):
    """Кикает пользователя мгновенно изо всех активных площадок владельца, если включен глобальный ЧС."""
    async with get_pool().acquire() as conn:
        bots_chats = await conn.fetch("""
            SELECT cb.bot_token, bc.chat_id
            FROM bot_chats bc
            JOIN child_bots cb ON cb.id = bc.child_bot_id
            WHERE bc.owner_id = $1 AND bc.is_active = true AND cb.use_global_blacklist = true
        """, owner_id)
        
    for row in bots_chats:
        temp_bot = Bot(token=row['bot_token'])
        try:
            await temp_bot.ban_chat_member(chat_id=row['chat_id'], user_id=target_user_id)
        except Exception as e:
            logger.debug(f"Global Ban warning for chat {row['chat_id']}: {e}")
        finally:
            await temp_bot.session.close()


@router.callback_query(F.data.startswith("ga_bl:"))
async def on_ga_bl(callback: CallbackQuery):
    role, owner_id = await get_admin_context(callback.from_user.id, callback.from_user.username)
    if not role:
        return await callback.answer("❌ Нет прав", show_alert=True)

    async with get_pool().acquire() as conn:
        bl_count = await conn.fetchval("SELECT COUNT(*) FROM blacklist WHERE owner_id=$1", owner_id) or 0
        bots = await conn.fetch("""
            SELECT id, bot_username, bot_name, use_global_blacklist
            FROM child_bots WHERE owner_id=$1 ORDER BY created_at ASC
        """, owner_id)

    # Строим текст
    text = (
        "🚫 <b>Глобальный Чёрный Список</b> — 🛡️ Защита сети\n"
        "─────────────────────────────\n"
        "Люди из этого списка автоматически блокируются во всех ботах, где включён этот режим.\n\n"
        f"📂 Записей в базе: <b>{bl_count}</b>\n\n"
        "🤖 <b>Подключенные боты:</b> (нажмите на бота — переключить)"
    )

    kb = [
        [
            InlineKeyboardButton(text="➕ Добавить в ЧС", callback_data=f"ga_bl_add:{owner_id}"),
            InlineKeyboardButton(text="➖ Удалить из ЧС", callback_data=f"ga_bl_del:{owner_id}")
        ]
    ]

    # Тумблеры для каждого бота
    for bot_row in bots:
        status = "✅ ВКЛ" if bot_row['use_global_blacklist'] else "⛔ ВЫКЛ"
        btn_text = f"{status} @{bot_row['bot_username']}"
        kb.append([InlineKeyboardButton(
            text=btn_text,
            callback_data=f"ga_bl_bot:{owner_id}:{bot_row['id']}"
        )])

    kb.append([InlineKeyboardButton(text="📥 Скачать ЧС (CSV)", callback_data=f"ga_bl_export_csv:{owner_id}")])
    kb.append([InlineKeyboardButton(text="◀️ Назад", callback_data=f"ga_main:{owner_id}")])

    await callback.message.edit_text(text, parse_mode="HTML", reply_markup=InlineKeyboardMarkup(inline_keyboard=kb))
    await callback.answer()


@router.callback_query(F.data.startswith("ga_bl_bot:"))
async def on_ga_bl_bot_toggle(callback: CallbackQuery):
    """Toggle use_global_blacklist for a specific bot."""
    parts = callback.data.split(":")
    owner_id = int(parts[1])
    bot_id = int(parts[2])

    role, _ = await get_admin_context(callback.from_user.id, callback.from_user.username)
    if role not in ('owner', 'admin'):
        return await callback.answer("❌ Нет прав", show_alert=True)

    async with get_pool().acquire() as conn:
        current = await conn.fetchval(
            "SELECT use_global_blacklist FROM child_bots WHERE id=$1 AND owner_id=$2",
            bot_id, owner_id
        )
        if current is None:
            return await callback.answer("⚠️ Бот не найден", show_alert=True)

        new_val = not current
        await conn.execute(
            "UPDATE child_bots SET use_global_blacklist=$1 WHERE id=$2",
            new_val, bot_id
        )

    status_txt = "✅ включён" if new_val else "⛔ выключен"
    await callback.answer(f"Глобальный ЧС {status_txt} для этого бота", show_alert=False)

    # Обновляем экран
    fake_cb = callback
    fake_cb.data = f"ga_bl:{owner_id}"
    await on_ga_bl(fake_cb)


@router.callback_query(F.data.startswith("ga_bl_export_csv:"))
async def on_ga_bl_export_csv(callback: CallbackQuery):
    owner_id = int(callback.data.split(":")[1])
    role, _ = await get_admin_context(callback.from_user.id, callback.from_user.username)
    if not role:
        return await callback.answer("❌ Нет прав", show_alert=True)

    msg = await callback.message.edit_text("⏳ Генерирую CSV черного списка...")
    import asyncio
    asyncio.create_task(_export_users_csv(callback.bot, callback.message.chat.id, owner_id, "blocked", msg))
    await callback.answer()


@router.callback_query(F.data.startswith("ga_bl_add:"))
async def on_ga_bl_add(callback: CallbackQuery):
    owner_id = int(callback.data.split(":")[1])
    text = (
        "➕ <b>Добавить в ЧС</b>\n\n"
        "Для блокировки пользователя используйте команду:\n"
        "<code>/block 123456789</code>\n\n"
        "При блокировке пользователь будет автоматически кикнут изо всех ботов, где включён режим ЧС."
    )
    kb = [[InlineKeyboardButton(text="◀️ Назад", callback_data=f"ga_bl:{owner_id}")]]
    await callback.message.edit_text(text, parse_mode="HTML", reply_markup=InlineKeyboardMarkup(inline_keyboard=kb))
    await callback.answer()


@router.callback_query(F.data.startswith("ga_bl_del:"))
async def on_ga_bl_del(callback: CallbackQuery):
    owner_id = int(callback.data.split(":")[1])
    text = (
        "➖ <b>Удалить из ЧС</b>\n\n"
        "Для разблокировки пользователя используйте команду:\n"
        "<code>/unblock 123456789</code>"
    )
    kb = [[InlineKeyboardButton(text="◀️ Назад", callback_data=f"ga_bl:{owner_id}")]]
    await callback.message.edit_text(text, parse_mode="HTML", reply_markup=InlineKeyboardMarkup(inline_keyboard=kb))
    await callback.answer()


@router.message(Command("block"))
async def cmd_block(message: Message):
    role, owner_id = await get_admin_context(message.from_user.id, message.from_user.username)
    if not role:
        return await message.answer("❌ Нет прав.")
        
    args = message.text.split(maxsplit=1)
    if len(args) < 2:
        return await message.answer("❌ Формат: <code>/block &lt;user_id&gt;</code>")
        
    try:
        target_id = int(args[1])
    except ValueError:
        return await message.answer("❌ Укажите числовой ID пользователя.")

    import asyncpg
    async with get_pool().acquire() as conn:
        try:
            await conn.execute("""
                INSERT INTO blacklist (owner_id, user_id, added_by, reason) 
                VALUES ($1, $2, $3, 'Global Admin Block')
            """, owner_id, target_id, message.from_user.id)
            await message.answer(f"✅ Пользователь <code>{target_id}</code> успешно занесен в Глобальный ЧС.\nНачинаю синхронную блокировку во всех ваших каналах...")
            
            # Кикаем отовсюду асинхронно
            import asyncio
            asyncio.create_task(_kick_from_all_chats(owner_id, target_id))
            
        except asyncpg.UniqueViolationError:
            await message.answer("⚠️ Пользователь уже находится в Глобальном ЧС.")


@router.message(Command("unblock"))
async def cmd_unblock(message: Message):
    role, owner_id = await get_admin_context(message.from_user.id, message.from_user.username)
    if not role:
        return await message.answer("❌ Нет прав.")
        
    args = message.text.split(maxsplit=1)
    if len(args) < 2:
        return await message.answer("❌ Формат: <code>/unblock &lt;user_id&gt;</code>")

    try:
        target_id = int(args[1])
    except ValueError:
        return await message.answer("❌ Укажите числовой ID пользователя.")

    async with get_pool().acquire() as conn:
        res = await conn.execute("DELETE FROM blacklist WHERE owner_id=$1 AND user_id=$2", owner_id, target_id)
        if res == "DELETE 0":
            await message.answer(f"⚠️ Пользователя <code>{target_id}</code> нет в Глобальном ЧС.")
        else:
            await message.answer(f"✅ Пользователь <code>{target_id}</code> удален из Глобального ЧС.")

import tempfile
import csv
import os
from aiogram.types import FSInputFile

async def _show_ga_users(message_or_cb, owner_id: int):
    async with get_pool().acquire() as conn:
        total_users = await conn.fetchval("""
            SELECT COUNT(DISTINCT bu.user_id) FROM bot_users bu
            JOIN bot_chats bc ON bu.chat_id = bc.chat_id
            WHERE bc.owner_id = $1 AND bc.is_active=true AND bu.user_id IS NOT NULL
        """, owner_id) or 0
        
        alive_users = await conn.fetchval("""
            SELECT COUNT(DISTINCT bu.user_id) FROM bot_users bu
            JOIN bot_chats bc ON bu.chat_id = bc.chat_id
            WHERE bc.owner_id = $1 AND bc.is_active=true AND bu.is_active=true AND bu.user_id IS NOT NULL
        """, owner_id) or 0
        
        dead_users = total_users - alive_users

    text = (
        "👥 <b>Сводная База Аудитории</b>\n\n"
        "Здесь собраны уникальные пользователи со всех ваших активных площадок.\n\n"
        f"Всего уникальных: <b>{total_users:,}</b>\n"
        f" ├ 🟢 Живые: {alive_users:,}\n"
        f" └ 🔴 Мёртвые: {dead_users:,}"
    )
    
    kb = [
        [InlineKeyboardButton(text="📥 Выгрузить всю базу (CSV)", callback_data=f"ga_export_users:all:{owner_id}")],
        [InlineKeyboardButton(text="🟢 Выгрузить только живых", callback_data=f"ga_export_users:alive:{owner_id}")],
        [InlineKeyboardButton(text="◀️ Назад", callback_data=f"ga_main:{owner_id}")]
    ]
    
    markup = InlineKeyboardMarkup(inline_keyboard=kb)
    if isinstance(message_or_cb, Message):
        await message_or_cb.answer(text, reply_markup=markup, parse_mode="HTML")
    else:
        await message_or_cb.message.edit_text(text, reply_markup=markup, parse_mode="HTML")


@router.callback_query(F.data.startswith("ga_users:"))
async def on_ga_users(callback: CallbackQuery):
    role, owner_id = await get_admin_context(callback.from_user.id, callback.from_user.username)
    if not role:
        return await callback.answer("❌ Нет прав", show_alert=True)
    await _show_ga_users(callback, owner_id)
    await callback.answer()


async def _export_users_csv(bot: Bot, chat_id: int, owner_id: int, export_type: str, msg_to_delete: Message = None):
    query = """
        SELECT DISTINCT ON (bu.user_id)
               bu.user_id, bu.first_name, bu.username, bu.is_active, bu.joined_at
        FROM bot_users bu
        JOIN bot_chats bc ON bu.chat_id = bc.chat_id
        WHERE bc.owner_id = $1 AND bc.is_active=true AND bu.user_id IS NOT NULL
    """
    if export_type == "alive":
        query += " AND bu.is_active = true"
    elif export_type == "blocked":
        query = "SELECT user_id, reason as first_name, username, false as is_active, added_at as joined_at FROM blacklist WHERE owner_id = $1"
    elif export_type == "admins":
        query = "SELECT admin_id as user_id, admin_username as first_name, NULL as username, true as is_active, added_at as joined_at FROM global_admins WHERE owner_id = $1"
        
    async with get_pool().acquire() as conn:
        rows = await conn.fetch(query, owner_id)
        
    if msg_to_delete:
        try:
            await msg_to_delete.delete()
        except Exception:
            pass

    if not rows:
        kb = [[InlineKeyboardButton(text="◀️ Назад в Меню", callback_data=f"ga_dl_bck:{owner_id}")]]
        return await bot.send_message(chat_id, "⚠️ База пуста по заданным критериям.", reply_markup=InlineKeyboardMarkup(inline_keyboard=kb))
        
    fd, path = tempfile.mkstemp(suffix=".csv")
    with open(path, "w", encoding="utf-8-sig", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(["ID", "Имя", "Юзернейм", "Активен", "Дата входа"])
        for r in rows:
            writer.writerow([
                r['user_id'],
                r['first_name'] or "",
                r['username'] or "",
                "Да" if r['is_active'] else "Нет",
                r['joined_at'].strftime("%Y-%m-%d %H:%M:%S") if r['joined_at'] else ""
            ])
            
    doc = FSInputFile(path, filename=f"global_audience_{export_type}.csv")
    
    kb = [[InlineKeyboardButton(text="◀️ Назад в Базу Аудитории", callback_data=f"ga_dl_bck:{owner_id}")]]
    await bot.send_document(chat_id, document=doc, caption="✅ Ваш отчет скомпилирован и готов.", reply_markup=InlineKeyboardMarkup(inline_keyboard=kb))
    
    try:
        os.close(fd)
        os.remove(path)
    except Exception:
        pass


@router.callback_query(F.data.startswith("ga_export_users:"))
async def on_ga_export_users(callback: CallbackQuery):
    parts = callback.data.split(":")
    export_type = parts[1]
    owner_id = int(parts[2])
    
    role, context_owner_id = await get_admin_context(callback.from_user.id, callback.from_user.username)
    if not role or context_owner_id != owner_id:
        return await callback.answer("❌ Нет прав", show_alert=True)
        
    msg = await callback.message.edit_text("⏳ Идёт сбор глобальной базы и генерация CSV. Пожалуйста, подождите...")
    import asyncio
    asyncio.create_task(_export_users_csv(callback.bot, callback.message.chat.id, owner_id, export_type, msg))
    await callback.answer()


@router.callback_query(F.data.startswith("ga_dl_bck:"))
async def on_ga_dl_bck(callback: CallbackQuery):
    owner_id = int(callback.data.split(":")[1])
    try:
        await callback.message.delete()
    except Exception:
        pass
        
    role, _ = await get_admin_context(callback.from_user.id, callback.from_user.username)
    if role:
        msg = await callback.message.answer("♻️ Открываю меню...")
        await _show_ga_users(msg, owner_id)
    await callback.answer()


@router.message(Command("users"))
async def cmd_users(message: Message):
    role, owner_id = await get_admin_context(message.from_user.id, message.from_user.username)
    if not role:
        return await message.answer("❌ Нет прав.")
        
    args = message.text.split()
    export_type = args[1].lower() if len(args) > 1 else 'all'
    if export_type not in ('all', 'active', 'blocked', 'admins'):
        return await message.answer("❌ Используйте: <code>/users [all|active|blocked|admins]</code>")
        
    import asyncio
    msg = await message.answer(f"⏳ Начинаю выгрузку базы (Критерий: {export_type})...")
    internal_type = "alive" if export_type == "active" else export_type
    asyncio.create_task(_export_users_csv(message.bot, message.chat.id, owner_id, internal_type, msg))


@router.callback_query(F.data.startswith("ga_broadcast:"))
async def on_ga_broadcast(callback: CallbackQuery):
    role, owner_id = await get_admin_context(callback.from_user.id, callback.from_user.username)
    if not role:
        return await callback.answer("❌ Нет прав", show_alert=True)

    async with get_pool().acquire() as conn:
        active_users = await conn.fetchval("""
            SELECT COUNT(DISTINCT bu.user_id) FROM bot_users bu
            JOIN bot_chats bc ON bu.chat_id = bc.chat_id
            WHERE bc.owner_id=$1 AND bc.is_active=true AND bu.user_id IS NOT NULL AND bu.is_active=true
        """, owner_id) or 0

    text = (
        "📢 <b>Глобальные Рассылки</b>\n"
        "─────────────────────────────\n"
        f"🟢 Активных получателей: <b>{active_users:,}</b>\n\n"
        "<b>📨 Массовая рассылка:</b>\n"
        "<code>/broadcast Текст сообщения</code>\n"
        "Разошлет <b>всем</b> активным юзерам сети. Сообщение отправляется от имени нужного бота.\n\n"
        "<b>✉️ Личное сообщение:</b>\n"
        "<code>/notify 123456789 Текст</code>\n"
        "Отправить лично конкретному юзеру сети."
    )
    kb = [[InlineKeyboardButton(text="◀️ Назад", callback_data=f"ga_main:{owner_id}")]]
    await callback.message.edit_text(text, parse_mode="HTML", reply_markup=InlineKeyboardMarkup(inline_keyboard=kb))
    await callback.answer()


@router.message(Command("notify"))
async def cmd_notify(message: Message):
    role, owner_id = await get_admin_context(message.from_user.id, message.from_user.username)
    if not role: 
        return await message.answer("❌ Нет прав.")
    
    args = message.text.split(maxsplit=2)
    if len(args) < 3:
        return await message.answer("❌ Формат: <code>/notify &lt;user_id&gt; &lt;Текст сообщения&gt;</code>")
        
    try:
        target_id = int(args[1])
    except ValueError:
        return await message.answer("❌ ID пользователя должен быть числом.")
        
    text_to_send = "🔔 <b>Уведомление от Администрации</b>\n\n" + args[2]
    
    async with get_pool().acquire() as conn:
        # Найдем первого рабочего бота, где юзер активен
        row = await conn.fetchrow("""
            SELECT cb.bot_token, bc.chat_id
            FROM bot_users bu
            JOIN bot_chats bc ON bu.chat_id = bc.chat_id
            JOIN child_bots cb ON cb.id = bc.child_bot_id
            WHERE bc.owner_id = $1 AND bu.user_id = $2 AND bu.is_active = true
            LIMIT 1
        """, owner_id, target_id)
        
    if not row:
        return await message.answer(f"⚠️ Пользователь <code>{target_id}</code> не найден среди активных пользователей ваших площадок.")
        
    from aiogram.client.default import DefaultBotProperties
    temp_bot = Bot(token=row['bot_token'], default=DefaultBotProperties(parse_mode="HTML"))
    try:
        await temp_bot.send_message(chat_id=target_id, text=text_to_send)
        await message.answer(f"✅ Сообщение успешно доставлено пользователю <code>{target_id}</code>.")
    except Exception as e:
        await message.answer(f"❌ Ошибка отправки: пользователь заблокировал бота или недоступен ({e})")
    finally:
        await temp_bot.session.close()


@router.message(Command("broadcast"))
async def cmd_broadcast(message: Message):
    role, owner_id = await get_admin_context(message.from_user.id, message.from_user.username)
    if not role: 
        return await message.answer("❌ Нет прав.")
    
    args = message.text.split(maxsplit=1)
    if len(args) < 2:
        return await message.answer("❌ Формат: <code>/broadcast &lt;Текст рассылки&gt;</code>")
        
    text_to_send = args[1]
    msg = await message.answer("⏳ Начинаю глобальную рассылку. Это может занять несколько минут...")
    
    async with get_pool().acquire() as conn:
        # Выбираем уникальных пользователей и токен первого попавшегося бота
        rows = await conn.fetch("""
            SELECT DISTINCT ON (bu.user_id) bu.user_id, cb.bot_token
            FROM bot_users bu
            JOIN bot_chats bc ON bu.chat_id = bc.chat_id
            JOIN child_bots cb ON cb.id = bc.child_bot_id
            WHERE bc.owner_id = $1 AND bu.user_id IS NOT NULL AND bu.is_active = true
            ORDER BY bu.user_id, bu.created_at ASC
        """, owner_id)
        
    if not rows:
        return await msg.edit_text("⚠️ Активная аудитория не найдена.")
        
    async def run_broadcast():
        success = 0
        import asyncio
        from aiogram.client.default import DefaultBotProperties
        
        valid_bots = {}
        for r in rows:
            u_id = r['user_id']
            token = r['bot_token']
            if token not in valid_bots:
                valid_bots[token] = Bot(token=token, default=DefaultBotProperties(parse_mode="HTML"))
            
            tb = valid_bots[token]
            try:
                await tb.send_message(chat_id=u_id, text=text_to_send)
                success += 1
            except Exception:
                pass
            await asyncio.sleep(0.04) # Telegram limits ~30 msg/sec
            
        for b in valid_bots.values():
            await b.session.close()
            
        await msg.edit_text(f"✅ <b>Глобальная рассылка завершена!</b>\n\nУспешно доставлено: <b>{success}</b> пользователей.", parse_mode="HTML")

    import asyncio
    asyncio.create_task(run_broadcast())


# ══════════════════════════════════════════════════════════════
# ⚡️ АКТИВАЦИЯ БОТОВ СЕТИ
# ══════════════════════════════════════════════════════════════

async def _show_bots_network_page(callback: CallbackQuery, owner_id: int, page: int):
    """Отрисовывает страницу со списком ботов и тумблерами активации."""
    async with get_pool().acquire() as conn:
        all_bots = await conn.fetch("""
            SELECT id, bot_username, bot_name, in_global_network, created_at
            FROM child_bots WHERE owner_id=$1
            ORDER BY in_global_network DESC, created_at ASC
        """, owner_id)

    total = len(all_bots)
    total_pages = max(1, (total + BOTS_PER_PAGE - 1) // BOTS_PER_PAGE)
    page = max(0, min(page, total_pages - 1))
    active_count = sum(1 for b in all_bots if b['in_global_network'])

    page_bots = all_bots[page * BOTS_PER_PAGE : (page + 1) * BOTS_PER_PAGE]

    text = (
        "⚡️ <b>Активация ботов сети</b>\n"
        "─────────────────────────────\n"
        f"🤖 Всего ботов: <b>{total}</b>   │   ✅ Активных: <b>{active_count}</b>\n\n"
        "Включённые боты участвуют в общей аудитории, рассылках и Глобальном ЧС.\n"
        "<i>Нажмите на бота — переключить ВКЛ / ВЫКЛ</i>"
    )

    kb = [
        [InlineKeyboardButton(text="🔍 Найти и активировать бота по названию", callback_data=f"ga_bots_search:{owner_id}")]
    ]

    for bot_row in page_bots:
        icon = "✅" if bot_row['in_global_network'] else "⛔"
        kb.append([InlineKeyboardButton(
            text=f"{icon} @{bot_row['bot_username']}",
            callback_data=f"ga_bot_net_toggle:{owner_id}:{bot_row['id']}:{page}"
        )])

    # Пагинация
    if total_pages > 1:
        nav = []
        if page > 0:
            nav.append(InlineKeyboardButton(text="◀️", callback_data=f"ga_bots:{owner_id}:{page - 1}"))
        nav.append(InlineKeyboardButton(text=f"{page + 1} / {total_pages}", callback_data="ga_bots_noop"))
        if page < total_pages - 1:
            nav.append(InlineKeyboardButton(text="▶️", callback_data=f"ga_bots:{owner_id}:{page + 1}"))
        kb.append(nav)

    kb.append([InlineKeyboardButton(text="◀️ Назад в Панель", callback_data=f"ga_main:{owner_id}")])

    await callback.message.edit_text(text, parse_mode="HTML", reply_markup=InlineKeyboardMarkup(inline_keyboard=kb))


@router.callback_query(F.data.startswith("ga_bots:"))
async def on_ga_bots(callback: CallbackQuery):
    parts = callback.data.split(":")
    owner_id = int(parts[1])
    page = int(parts[2]) if len(parts) > 2 else 0

    role, _ = await get_admin_context(callback.from_user.id, callback.from_user.username)
    if not role:
        return await callback.answer("❌ Нет прав", show_alert=True)

    await _show_bots_network_page(callback, owner_id, page)
    await callback.answer()


@router.callback_query(F.data.startswith("ga_bot_net_toggle:"))
async def on_ga_bot_net_toggle(callback: CallbackQuery):
    parts = callback.data.split(":")
    owner_id = int(parts[1])
    bot_id = int(parts[2])
    page = int(parts[3]) if len(parts) > 3 else 0

    role, _ = await get_admin_context(callback.from_user.id, callback.from_user.username)
    if not role:
        return await callback.answer("❌ Нет прав", show_alert=True)

    async with get_pool().acquire() as conn:
        current = await conn.fetchval(
            "SELECT in_global_network FROM child_bots WHERE id=$1 AND owner_id=$2",
            bot_id, owner_id
        )
        if current is None:
            return await callback.answer("⚠️ Бот не найден", show_alert=True)

        new_val = not current
        await conn.execute(
            "UPDATE child_bots SET in_global_network=$1 WHERE id=$2",
            new_val, bot_id
        )

    status = "✅ Добавлен в сеть" if new_val else "⛔ Удалён из сети"
    await callback.answer(status)
    # Всегда возвращаемся на страницу 0, чтобы включённый бот появился первым
    await _show_bots_network_page(callback, owner_id, 0)


@router.callback_query(F.data == "ga_bots_noop")
async def on_ga_bots_noop(callback: CallbackQuery):
    await callback.answer()


@router.callback_query(F.data.startswith("ga_bots_search:"))
async def on_ga_bots_search(callback: CallbackQuery, state: FSMContext):
    owner_id = int(callback.data.split(":")[1])
    await state.set_state(BotNetworkFSM.waiting_search)
    await state.update_data(owner_id=owner_id)

    text = (
        "🔍 <b>Поиск бота</b>\n"
        "─────────────────────────────\n"
        "Введите <b>@username</b> бота, который нужно найти и включить в сеть.\n\n"
        "<i>Пример: @mybotname или просто mybotname</i>"
    )
    kb = [[InlineKeyboardButton(text="❌ Отмена", callback_data=f"ga_bots_search_cancel:{owner_id}")]]
    await callback.message.edit_text(text, parse_mode="HTML", reply_markup=InlineKeyboardMarkup(inline_keyboard=kb))
    await callback.answer()


@router.callback_query(F.data.startswith("ga_bots_search_cancel:"))
async def on_ga_bots_search_cancel(callback: CallbackQuery, state: FSMContext):
    await state.clear()
    owner_id = int(callback.data.split(":")[1])
    await _show_bots_network_page(callback, owner_id, 0)
    await callback.answer()


@router.message(BotNetworkFSM.waiting_search)
async def on_bots_search_input(message: Message, state: FSMContext):
    data = await state.get_data()
    owner_id = data.get("owner_id")
    await state.clear()

    query = message.text.strip().lstrip("@").lower()

    async with get_pool().acquire() as conn:
        bot_row = await conn.fetchrow(
            "SELECT id, bot_username, in_global_network FROM child_bots WHERE owner_id=$1 AND LOWER(bot_username)=$2",
            owner_id, query
        )

    if not bot_row:
        kb = [[InlineKeyboardButton(text="◀️ Назад к списку", callback_data=f"ga_bots:{owner_id}:0")]]
        await message.answer(
            f"⚠️ Бот <code>@{query}</code> не найден в вашем списке.\n"
            "Убедитесь, что бот добавлен в систему.",
            parse_mode="HTML",
            reply_markup=InlineKeyboardMarkup(inline_keyboard=kb)
        )
        return

    if not bot_row['in_global_network']:
        async with get_pool().acquire() as conn:
            await conn.execute(
                "UPDATE child_bots SET in_global_network=true WHERE id=$1",
                bot_row['id']
            )
        result_text = f"✅ Бот <b>@{bot_row['bot_username']}</b> включён в сеть и теперь участвует в статистике и ЧС."
    else:
        result_text = f"ℹ️ Бот <b>@{bot_row['bot_username']}</b> уже активен в сети."

    kb = [[InlineKeyboardButton(text="✅ К списку ботов", callback_data=f"ga_bots:{owner_id}:0")]]
    await message.answer(result_text, parse_mode="HTML", reply_markup=InlineKeyboardMarkup(inline_keyboard=kb))
