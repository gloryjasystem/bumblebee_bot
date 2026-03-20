import json
import logging
from aiogram import Router, F, Bot
from aiogram.types import Message, CallbackQuery, InlineKeyboardMarkup, InlineKeyboardButton
from aiogram.filters import Command
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup

from db.pool import get_pool

logger = logging.getLogger(__name__)
router = Router()


# ══════════════════════════════════════════════════════════
# ВРЕМЕННЫЙ ДИАГНОСТИЧЕСКИЙ ХЕНДЛЕР — удалить после проверки
# Команда: /dbcheck <child_bot_id>
# ══════════════════════════════════════════════════════════
@router.message(Command("dbcheck"))
async def cmd_dbcheck(message: Message):
    from config import settings
    if message.from_user.id != settings.owner_telegram_id:
        return
    args = (message.text or "").split()
    child_bot_id = int(args[1]) if len(args) > 1 else None

    async with get_pool().acquire() as conn:
        # Все боты владельца
        bots = await conn.fetch(
            "SELECT id, bot_username, owner_id FROM child_bots ORDER BY id"
        )
        bots_text = "\n".join(f"  id={b['id']} @{b['bot_username']} owner={b['owner_id']}" for b in bots)

        if child_bot_id is None and bots:
            child_bot_id = bots[0]['id']

        # Чаты этого бота
        chats = await conn.fetch(
            "SELECT chat_id, chat_title FROM bot_chats WHERE child_bot_id=$1 AND is_active=true",
            child_bot_id
        )

        users_text = ""
        for chat in chats:
            users = await conn.fetch(
                """SELECT user_id, username, first_name, is_active
                   FROM bot_users WHERE chat_id=$1
                   ORDER BY joined_at DESC LIMIT 10""",
                chat['chat_id']
            )
            users_text += f"\n📋 chat={chat['chat_id']} ({chat['chat_title']}):\n"
            for u in users:
                users_text += f"  uid={u['user_id']} uname={repr(u['username'])} name={repr(u['first_name'])} active={u['is_active']}\n"

        # Последние записи ЧС
        bl = await conn.fetch(
            "SELECT user_id, username FROM blacklist WHERE child_bot_id=$1 ORDER BY added_at DESC LIMIT 5",
            child_bot_id
        )
        bl_text = "\n".join(f"  uid={r['user_id']} uname={repr(r['username'])}" for r in bl)

    await message.answer(
        f"🤖 <b>child_bots:</b>\n<code>{bots_text}</code>\n\n"
        f"🎯 <b>Диагностика child_bot_id={child_bot_id}</b>\n\n"
        f"👥 <b>bot_users (последние 10 на чат):</b>\n<code>{users_text.strip()}</code>\n\n"
        f"🚫 <b>blacklist (последние 5):</b>\n<code>{bl_text or 'пусто'}</code>",
        parse_mode="HTML"
    )
# ══════════════════════════════════════════════════════════


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
    await _show_admin_panel(message, role, owner_id, admin_id=message.from_user.id)

async def _show_admin_panel(message_or_cb, role: str, owner_id: int, admin_id: int = None):
    if admin_id is None:
        admin_id = owner_id

    async with get_pool().acquire() as conn:
        total_bots = await conn.fetchval("SELECT COUNT(*) FROM child_bots WHERE owner_id=$1", owner_id) or 0
        net_bots   = await conn.fetchval("SELECT COUNT(*) FROM child_bots WHERE owner_id=$1 AND in_global_network=true", owner_id) or 0

        total_users = await conn.fetchval("""
            SELECT COUNT(DISTINCT bu.user_id) FROM bot_users bu
            JOIN bot_chats bc ON bu.chat_id = bc.chat_id
            JOIN child_bots cb ON cb.id = bc.child_bot_id
            WHERE bc.owner_id=$1 AND bc.is_active=true AND bu.user_id IS NOT NULL
              AND cb.in_global_network=true
        """, owner_id) or 0

        # Заблокировано — уникальные ЧС-записи из ВЫБРАННЫХ ботов (ga_selected_bots)
        selected_bot_ids_rows = await conn.fetch(
            "SELECT child_bot_id FROM ga_selected_bots WHERE admin_id=$1", admin_id
        )
        selected_bot_ids = [r['child_bot_id'] for r in selected_bot_ids_rows]
        if selected_bot_ids:
            bl_count = await conn.fetchval("""
                SELECT COUNT(*) FROM (
                    SELECT DISTINCT ON (COALESCE(user_id::text, lower(username))) user_id
                    FROM blacklist
                    WHERE child_bot_id = ANY($1::int[]) OR (owner_id = $2 AND child_bot_id IS NULL)
                ) t
            """, selected_bot_ids, owner_id) or 0
        else:
            bl_count = await conn.fetchval(
                "SELECT COUNT(*) FROM blacklist WHERE owner_id=$1 AND child_bot_id IS NULL", owner_id
            ) or 0

        admin_count = await conn.fetchval("SELECT COUNT(*) FROM global_admins WHERE owner_id=$1", owner_id) or 0

    if role == 'owner':
        header = (
            "🌐 <b>BotCloud — Глобальная Панель</b>  •  👑 <b>Owner</b>\n"
            "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n\n"
            f"🗄️  Активных ботов:  <b>{net_bots}</b> из <b>{total_bots}</b>\n"
            f"👥  Аудитория (активных):  <b>{total_users:,}</b>\n\n"
            f"🚫  Заблокировано в выбранных:  <b>{bl_count:,}</b>\n"
            f"👥  Команда:  <b>{admin_count}</b>"
        )
    else:
        header = (
            "🌐 <b>BotCloud — Глобальная Панель</b>  •  👷 <b>Admin</b>\n"
            "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n\n"
            f"👥  Активная аудитория сети:  <b>{total_users:,}</b>\n"
            f"🚫  Заблокировано в выбранных:  <b>{bl_count:,}</b>"
        )

    kb = []
    if role == 'owner':
        kb.append([
            InlineKeyboardButton(text="👥 Моя команда",   callback_data=f"ga_team:{owner_id}"),
            InlineKeyboardButton(text="📊 Аналитика",     callback_data=f"ga_stats:{owner_id}"),
        ])
        
    kb.extend([
        [
            InlineKeyboardButton(text="🚫 Глобальный ЧС",     callback_data=f"ga_bl:{owner_id}"),
            InlineKeyboardButton(text="👥 База пользователей", callback_data=f"ga_users:{owner_id}")
        ],
        [
            InlineKeyboardButton(text="📢 Рассылка",           callback_data=f"ga_broadcast:{owner_id}"),
            InlineKeyboardButton(text="🏷 Скидки",             callback_data=f"ga_discounts:{owner_id}")
        ],
        [InlineKeyboardButton(text="🗄️ Управление общей базой", callback_data=f"ga_bots:{owner_id}:0")],
        [InlineKeyboardButton(text="⚙️ Управление пользователями", callback_data=f"ga_manage_users:{owner_id}")],
        [InlineKeyboardButton(text="❌ Закрыть панель", callback_data=f"ga_close:{owner_id}")]
    ])

    markup = InlineKeyboardMarkup(inline_keyboard=kb)
    if isinstance(message_or_cb, Message):
        await message_or_cb.answer(header, reply_markup=markup, parse_mode="HTML")
    else:
        try:
            await message_or_cb.message.edit_text(header, reply_markup=markup, parse_mode="HTML")
        except Exception:
            await message_or_cb.message.answer(header, reply_markup=markup, parse_mode="HTML")


@router.callback_query(F.data.startswith("ga_main:"))
async def on_ga_main(callback: CallbackQuery):
    owner_id = int(callback.data.split(":")[1])
    role, _ = await get_admin_context(callback.from_user.id, callback.from_user.username)
    if not role:
        return await callback.answer("❌ Нет прав", show_alert=True)
    await _show_admin_panel(callback, role, owner_id, admin_id=callback.from_user.id)
    await callback.answer()


@router.callback_query(F.data.startswith("ga_close:"))
async def on_ga_close(callback: CallbackQuery):
    try:
        await callback.message.delete()
    except Exception:
        pass
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
            SELECT action, details, created_at, user_id
            FROM audit_log
            WHERE owner_id = $1
            ORDER BY created_at DESC
            LIMIT 25
        """, owner_id)

    if not rows:
        text = (
            "📜 <b>Журнал действий</b>\n"
            "─────────────────────────────\n"
            "❎ Действий пока не зафиксировано.\n"
            "\n<i>Журнал заполняется при блокировках, добавлении админов и других действиях.</i>"
        )
    else:
        ACTION_ICONS = {
            "block": "🚫",
            "unblock": "✅",
            "add_admin": "👷⬆️",
            "remove_admin": "👷⬇️",
            "broadcast": "📢",
            "bot_toggle": "⚡️",
        }
        lines = [
            "📜 <b>Журнал действий</b> (25 последних)\n"
            "─────────────────────────────"
        ]
        for r in rows:
            dt = r['created_at'].strftime("%d.%m %H:%M")
            icon = ACTION_ICONS.get(r['action'], "🔵")
            uid = f"<code>{r['user_id']}</code>" if r['user_id'] else "system"
            detail = (r['details'] or "")[:60]
            lines.append(f"{icon} [{dt}] {uid}\n    ↳ {detail}")
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
            await conn.execute("""
                INSERT INTO audit_log (owner_id, user_id, action, details)
                VALUES ($1, $2, 'add_admin', $3)
            """, owner_id, message.from_user.id, json.dumps({"info": f"Granted admin to {target_id}"}))
            await message.answer(f"✅ Пользователь <code>{target_id}</code> назначен администратором.\nСписок: /admins", parse_mode="HTML")
        except asyncpg.UniqueViolationError:
            await message.answer("⚠️ Этот пользователь уже является администратором.")


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
            await message.answer(f"⚠️ Пользователь <code>{target_id}</code> не найден в списке администраторов.", parse_mode="HTML")
        else:
            await conn.execute("""
                INSERT INTO audit_log (owner_id, user_id, action, details)
                VALUES ($1, $2, 'remove_admin', $3)
            """, owner_id, message.from_user.id, json.dumps({"info": f"Revoked admin from {target_id}"}))
            await message.answer(f"✅ Права пользователя <code>{target_id}</code> успешно отозваны.", parse_mode="HTML")



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



# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# 📊 АНАЛИТИКА — вспомогательные функции
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

_FRAME_TOP    = "┌────────────────────────────\n"
_FRAME_BOTTOM = "└────────────────────────────"
_FRAME_LINE   = "│ "

_PERIOD_META = {
    "today": ("Сегодня",    "date_trunc('day', {col} AT TIME ZONE 'UTC') = current_date"),
    "week":  ("За неделю",  "{col} >= now() - interval '7 days'"),
    "month": ("За месяц",   "{col} >= now() - interval '30 days'"),
    "all":   ("За всё время", None),
}
_PERIOD_TABS = [
    ("📅 Сегодня",   "today"),
    ("📆 Неделя",    "week"),
    ("🗓 Месяц",     "month"),
    ("🕰 Всё время", "all"),
]


def _period_where(period: str, col: str = "created_at") -> str | None:
    """Returns a SQL WHERE condition string for the given period, or None for 'all'."""
    tmpl = _PERIOD_META.get(period, _PERIOD_META["all"])[1]
    if tmpl is None:
        return None
    return tmpl.format(col=col)


def _period_label(period: str) -> str:
    return _PERIOD_META.get(period, _PERIOD_META["all"])[0]


def _period_tabs_kb(owner_id: int, section: str, current_period: str) -> InlineKeyboardMarkup:
    """Reusable period tab keyboard. Active tab is omitted. section = 'audience'|'segment'|'finance'|'rev'."""
    tabs = [
        InlineKeyboardButton(text=label, callback_data=f"ga_{section}:{owner_id}:{p}")
        for label, p in _PERIOD_TABS if p != current_period
    ]
    return InlineKeyboardMarkup(inline_keyboard=[
        tabs,
        [InlineKeyboardButton(text="🔄 Обновить", callback_data=f"ga_{section}:{owner_id}:{current_period}")],
        [InlineKeyboardButton(text="◀️ Назад в Аналитику", callback_data=f"ga_stats:{owner_id}")],
    ])


# ── Main screen builder (СЕТЬ БОТОВ only) ──────────────────────────────────

async def _build_network_text(owner_id: int, conn) -> str:
    total_bots   = await conn.fetchval("SELECT COUNT(*) FROM child_bots WHERE owner_id=$1", owner_id) or 0
    total_chats  = await conn.fetchval("SELECT COUNT(*) FROM bot_chats WHERE owner_id=$1 AND is_active=true", owner_id) or 0
    total_owners = await conn.fetchval("SELECT COUNT(*) FROM platform_users") or 0

    lines = (
        f"{_FRAME_LINE}🗄️  Подключено ботов:  <b>{total_bots}</b>\n"
        f"{_FRAME_LINE}📡  Активных каналов/групп:  <b>{total_chats}</b>\n"
        f"{_FRAME_LINE}👤  Владельцев ботов:  <b>{total_owners}</b>\n"
    )
    return (
        "📊 <b>Аналитика BotCloud</b>\n"
        "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n\n"
        "🤖 <b>СЕТЬ БОТОВ</b>\n"
        f"{_FRAME_TOP}{lines}{_FRAME_BOTTOM}"
    )


# ── Аудитория ───────────────────────────────────────────────────────────────

async def _build_audience_text(period: str, conn) -> str:
    label = _period_label(period)
    where = _period_where(period, "created_at")
    cond  = f"WHERE {where}" if where else ""

    total_all  = await conn.fetchval("SELECT COUNT(*) FROM platform_users") or 0
    new_period = await conn.fetchval(f"SELECT COUNT(*) FROM platform_users {cond}") or 0
    paid_period = await conn.fetchval(
        f"SELECT COUNT(*) FROM platform_users {cond} {'AND' if cond else 'WHERE'} tariff != 'free'"
        if cond else
        "SELECT COUNT(*) FROM platform_users WHERE tariff != 'free'"
    ) or 0
    free_period = new_period - paid_period

    lines = (
        f"{_FRAME_LINE}📊  Всего пользователей (всего):  <b>{total_all:,}</b>\n"
        f"{_FRAME_LINE}🆕  Новых {label.lower()}:  <b>{new_period:,}</b>\n"
        f"{_FRAME_LINE}✅  Платных (из новых):  <b>{paid_period:,}</b>\n"
        f"{_FRAME_LINE}💡  Бесплатных (из новых):  <b>{free_period:,}</b>\n"
    )
    return (
        f"🐝 <b>АУДИТОРИЯ BUMBLEBEE BOT</b>  —  <b>{label}</b>\n"
        "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n\n"
        f"{_FRAME_TOP}{lines}{_FRAME_BOTTOM}"
    )


# ── Сегментация ─────────────────────────────────────────────────────────────

async def _build_segment_text(period: str, owner_id: int, conn) -> str:
    label = _period_label(period)
    where = _period_where(period, "created_at")
    base  = f"WHERE {where}" if where else ""
    and_  = "AND" if base else "WHERE"

    seg_rows = await conn.fetch(
        f"SELECT COALESCE(tariff,'free') as t, COUNT(*) as cnt FROM platform_users {base} GROUP BY tariff"
    )
    seg = {r['t']: r['cnt'] for r in seg_rows}

    leads   = seg.get('free', 0)
    start_c = seg.get('start', 0)
    pro_c   = seg.get('pro', 0)
    biz_c   = seg.get('business', 0)
    clients = start_c + pro_c + biz_c
    quals   = pro_c + biz_c

    banned = await conn.fetchval(
        f"SELECT COUNT(*) FROM blacklist WHERE owner_id=$1 AND child_bot_id IS NULL"
        + (f" AND {_period_where(period, 'added_at')}" if where else ""),
        owner_id
    ) or 0

    period_note = f"  <i>(новых {label.lower()})</i>" if period != "all" else ""
    lines = (
        f"{_FRAME_LINE}💡  Лиды (free):  <b>{leads:,}</b>{period_note}\n"
        f"{_FRAME_LINE}✅  Клиенты (start):  <b>{start_c:,}</b>{period_note}\n"
        f"{_FRAME_LINE}🏆  Квалы (pro+business):  <b>{quals:,}</b>{period_note}\n"
        f"{_FRAME_LINE}💼  Всего платных:  <b>{clients:,}</b>{period_note}\n"
        f"{_FRAME_LINE}🚫  Новых в ЧС:  <b>{banned}</b>{period_note}\n"
    )
    return (
        f"📦 <b>СЕГМЕНТАЦИЯ</b>  —  <b>{label}</b>\n"
        "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n\n"
        f"{_FRAME_TOP}{lines}{_FRAME_BOTTOM}"
    )


# ── Финансы ─────────────────────────────────────────────────────────────────

async def _build_finance_text(period: str, conn) -> str:
    label   = _period_label(period)
    # Paid: use paid_at; Pending/Failed/Expired: use created_at
    w_paid    = _period_where(period, "paid_at")
    w_created = _period_where(period, "created_at")

    paid_cond    = f"AND {w_paid}"    if w_paid    else ""
    created_cond = f"AND {w_created}" if w_created else ""

    paid_row = await conn.fetchrow(
        f"SELECT COUNT(*) as cnt, COALESCE(SUM(amount_usd),0) as total FROM payments WHERE status='paid' {paid_cond}"
    )
    pending_row = await conn.fetchrow(
        f"SELECT COUNT(*) as cnt, COALESCE(SUM(amount_usd),0) as total FROM payments WHERE status='pending' {created_cond}"
    )
    failed_cnt = await conn.fetchval(
        f"SELECT COUNT(*) FROM payments WHERE status='failed' {created_cond}"
    ) or 0
    expired_cnt = await conn.fetchval(
        f"SELECT COUNT(*) FROM payments WHERE status='expired' {created_cond}"
    ) or 0

    paid_cnt    = paid_row['cnt']
    paid_sum    = float(paid_row['total'])
    pending_cnt = pending_row['cnt']
    pending_sum = float(pending_row['total'])
    cancelled   = failed_cnt + expired_cnt
    attempts    = paid_cnt + cancelled + pending_cnt
    conversion  = round(paid_cnt / attempts * 100, 1) if attempts > 0 else 0.0

    lines = (
        f"{_FRAME_LINE}⏳  В ожидании:  <b>{pending_cnt}</b>  •  <b>${pending_sum:,.2f}</b>\n"
        f"{_FRAME_LINE}✅  Оплачено:  <b>{paid_cnt}</b>  •  <b>${paid_sum:,.2f}</b>\n"
        f"{_FRAME_LINE}❌  Отмены/Просрочено:  <b>{cancelled}</b>\n"
        f"{_FRAME_LINE}📈  Конверсия:  <b>{conversion}%</b>\n"
        f"{_FRAME_LINE}💰  Доход {label.lower()}:  <b>${paid_sum:,.2f}</b>\n"
    )
    return (
        f"💳 <b>ФИНАНСЫ (NOWPayments)</b>  —  <b>{label}</b>\n"
        "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n\n"
        f"{_FRAME_TOP}{lines}{_FRAME_BOTTOM}"
    )


# ── Revenue (Доходы) builder stays the same but updated kb ──────────────────

async def _build_revenue_text(owner_id: int, period: str, conn) -> str:
    label    = _period_label(period)
    w_paid   = _period_where(period, "paid_at")
    paid_cond = f"AND {w_paid}" if w_paid else ""

    rows = await conn.fetch(f"""
        SELECT tariff, period, COUNT(*) as cnt, SUM(amount_usd) as total
        FROM payments
        WHERE status='paid' {paid_cond}
        GROUP BY tariff, period
        ORDER BY SUM(amount_usd) DESC
    """)

    total_sum = sum(float(r['total']) for r in rows)
    total_cnt = sum(r['cnt'] for r in rows)
    avg_check = round(total_sum / total_cnt, 2) if total_cnt > 0 else 0.0

    tariff_icons = {"start": "🌱 Start", "pro": "⭐ Pro", "business": "💼 Business"}
    period_icons = {"month": "/ месяц", "year": "/ год"}

    breakdown = ""
    for r in rows:
        t_label = tariff_icons.get(r['tariff'], r['tariff'].capitalize())
        p_label = period_icons.get(r['period'], r['period'])
        breakdown += f"{_FRAME_LINE}{t_label} {p_label}  —  {r['cnt']} шт.  ${float(r['total']):,.2f}\n"

    top_row = rows[0] if rows else None
    top_label = ""
    if top_row:
        t = tariff_icons.get(top_row['tariff'], top_row['tariff'])
        p = period_icons.get(top_row['period'], top_row['period'])
        top_label = f"{t} {p}"

    lines = (
        f"{_FRAME_LINE}💵  Итого:  <b>${total_sum:,.2f}</b>  ({total_cnt} платежей)\n"
        f"{_FRAME_LINE}📊  Средний чек:  <b>${avg_check:,.2f}</b>\n"
        f"{_FRAME_LINE}🏆  Топ-тариф:  <b>{top_label or '—'}</b>\n"
        "│\n"
        f"{_FRAME_LINE}<b>По тарифам:</b>\n"
        f"{breakdown or _FRAME_LINE + 'Нет оплат за период\n'}"
    )
    return (
        f"💰 <b>ДОХОДЫ</b>  —  <b>{label}</b>\n"
        "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n\n"
        f"{_FRAME_TOP}{lines}{_FRAME_BOTTOM}"
    )


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# 📊 ОБРАБОТЧИКИ
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

@router.message(Command("stats"))
async def cmd_stats(message: Message):
    role, owner_id = await get_admin_context(message.from_user.id, message.from_user.username)
    if not role:
        return await message.answer("❌ Нет доступа.")
    if role != 'owner':
        return await message.answer("❌ Только для Владельца.")
    async with get_pool().acquire() as conn:
        text = await _build_network_text(owner_id, conn)
    await message.answer(text, parse_mode="HTML")


@router.callback_query(F.data.startswith("ga_stats:"))
async def on_ga_stats(callback: CallbackQuery):
    role, owner_id = await get_admin_context(callback.from_user.id, callback.from_user.username)
    if role != 'owner':
        return await callback.answer("❌ Нет прав", show_alert=True)

    async with get_pool().acquire() as conn:
        text = await _build_network_text(owner_id, conn)

    kb = InlineKeyboardMarkup(inline_keyboard=[
        [
            InlineKeyboardButton(text="🐝 Аудитория",   callback_data=f"ga_audience:{owner_id}:all"),
            InlineKeyboardButton(text="📦 Сегментация", callback_data=f"ga_segment:{owner_id}:all"),
        ],
        [
            InlineKeyboardButton(text="💳 Финансы",     callback_data=f"ga_finance:{owner_id}:all"),
            InlineKeyboardButton(text="💰 Доходы",      callback_data=f"ga_rev:{owner_id}:all"),
        ],
        [InlineKeyboardButton(text="🔄 Обновить", callback_data=f"ga_stats:{owner_id}")],
        [InlineKeyboardButton(text="◀️ Назад",    callback_data=f"ga_main:{owner_id}")],
    ])
    try:
        await callback.message.edit_text(text, parse_mode="HTML", reply_markup=kb)
    except Exception:
        await callback.message.answer(text, parse_mode="HTML", reply_markup=kb)
    await callback.answer()


@router.callback_query(F.data.startswith("ga_audience:"))
async def on_ga_audience(callback: CallbackQuery):
    parts    = callback.data.split(":")
    owner_id = int(parts[1])
    period   = parts[2] if len(parts) > 2 else "all"
    role, _  = await get_admin_context(callback.from_user.id, callback.from_user.username)
    if role != 'owner':
        return await callback.answer("❌ Нет прав", show_alert=True)
    async with get_pool().acquire() as conn:
        text = await _build_audience_text(period, conn)
    kb = _period_tabs_kb(owner_id, "audience", period)
    try:
        await callback.message.edit_text(text, parse_mode="HTML", reply_markup=kb)
    except Exception:
        await callback.message.answer(text, parse_mode="HTML", reply_markup=kb)
    await callback.answer()


@router.callback_query(F.data.startswith("ga_segment:"))
async def on_ga_segment(callback: CallbackQuery):
    parts    = callback.data.split(":")
    owner_id = int(parts[1])
    period   = parts[2] if len(parts) > 2 else "all"
    role, _  = await get_admin_context(callback.from_user.id, callback.from_user.username)
    if role != 'owner':
        return await callback.answer("❌ Нет прав", show_alert=True)
    async with get_pool().acquire() as conn:
        text = await _build_segment_text(period, owner_id, conn)
    kb = _period_tabs_kb(owner_id, "segment", period)
    try:
        await callback.message.edit_text(text, parse_mode="HTML", reply_markup=kb)
    except Exception:
        await callback.message.answer(text, parse_mode="HTML", reply_markup=kb)
    await callback.answer()


@router.callback_query(F.data.startswith("ga_finance:"))
async def on_ga_finance(callback: CallbackQuery):
    parts    = callback.data.split(":")
    owner_id = int(parts[1])
    period   = parts[2] if len(parts) > 2 else "all"
    role, _  = await get_admin_context(callback.from_user.id, callback.from_user.username)
    if role != 'owner':
        return await callback.answer("❌ Нет прав", show_alert=True)
    async with get_pool().acquire() as conn:
        text = await _build_finance_text(period, conn)
    kb = _period_tabs_kb(owner_id, "finance", period)
    try:
        await callback.message.edit_text(text, parse_mode="HTML", reply_markup=kb)
    except Exception:
        await callback.message.answer(text, parse_mode="HTML", reply_markup=kb)
    await callback.answer()


@router.callback_query(F.data.startswith("ga_rev:"))
async def on_ga_revenue(callback: CallbackQuery):
    parts    = callback.data.split(":")
    owner_id = int(parts[1])
    period   = parts[2] if len(parts) > 2 else "all"
    role, _  = await get_admin_context(callback.from_user.id, callback.from_user.username)
    if role != 'owner':
        return await callback.answer("❌ Нет прав", show_alert=True)
    async with get_pool().acquire() as conn:
        text = await _build_revenue_text(owner_id, period, conn)
    kb = _period_tabs_kb(owner_id, "rev", period)
    try:
        await callback.message.edit_text(text, parse_mode="HTML", reply_markup=kb)
    except Exception:
        await callback.message.answer(text, parse_mode="HTML", reply_markup=kb)
    await callback.answer()



async def _kick_from_all_chats(admin_id: int, target_user_id: int):
    """Кикает пользователя изо всех площадок ботов, отмеченных админом в своей выборке (ga_selected_bots)."""
    from services.security import decrypt_token
    async with get_pool().acquire() as conn:
        bots_chats = await conn.fetch("""
            SELECT cb.token_encrypted, bc.chat_id
            FROM ga_selected_bots gsb
            JOIN child_bots cb ON cb.id = gsb.child_bot_id
            JOIN bot_chats bc ON bc.child_bot_id = cb.id
            WHERE gsb.admin_id = $1 AND bc.is_active = true
        """, admin_id)

    import asyncio
    kicked = 0
    for row in bots_chats:
        token = decrypt_token(row['token_encrypted'])
        if not token:
            continue
        temp_bot = Bot(token=token)
        try:
            await temp_bot.ban_chat_member(chat_id=row['chat_id'], user_id=target_user_id)
            kicked += 1
        except Exception as e:
            logger.debug(f"Global Ban warning for chat {row['chat_id']}: {e}")
        finally:
            await temp_bot.session.close()
        await asyncio.sleep(0.05)

    if kicked > 0:
        logger.info(f"[GA KICK] user={target_user_id} kicked from {kicked} chats by admin={admin_id}")



@router.callback_query(F.data.startswith("ga_bl:"))
async def on_ga_bl(callback: CallbackQuery):
    role, owner_id = await get_admin_context(callback.from_user.id, callback.from_user.username)
    if not role:
        return await callback.answer("❌ Нет прав", show_alert=True)

    admin_id = callback.from_user.id

    async with get_pool().acquire() as conn:
        # 1. Получаем список выбранных ботов (с их id и статистикой блокировок)
        selected_bots = await conn.fetch("""
            SELECT cb.id, cb.bot_username, cb.blocked_count
            FROM ga_selected_bots gsb
            JOIN child_bots cb ON cb.id = gsb.child_bot_id
            WHERE gsb.admin_id = $1
            ORDER BY gsb.selected_at ASC
        """, admin_id)
        selected_bot_ids = [r['id'] for r in selected_bots]

        # 2. Вытаскиваем глобальный статус активности ЧС платформы
        pu_row = await conn.fetchrow(
            "SELECT blacklist_active FROM platform_users WHERE user_id=$1", owner_id
        )
        bl_active = pu_row['blacklist_active'] if pu_row and pu_row['blacklist_active'] is not None else True

        # 3. Подсчитываем статистику строго для выбранных ботов + глобальный ЧС платформы
        if selected_bot_ids:
            # Уникальные записи базы (как при экспорте CSV)
            bl_count = await conn.fetchval("""
                SELECT COUNT(*) FROM (
                    SELECT DISTINCT ON (COALESCE(user_id::text, lower(username))) user_id
                    FROM blacklist
                    WHERE child_bot_id = ANY($1::int[]) OR (owner_id = $2 AND child_bot_id IS NULL)
                ) t
            """, selected_bot_ids, owner_id) or 0
            
            # Суммируем количество заблокированных именно этими ботами
            total_blocked = sum((r['blocked_count'] or 0) for r in selected_bots)
        else:
            # Если боты не выбраны — показываем только глобальный размер базы платформы (child_bot_id IS NULL)
            bl_count = await conn.fetchval(
                "SELECT COUNT(*) FROM blacklist WHERE owner_id=$1 AND child_bot_id IS NULL", owner_id
            ) or 0
            total_blocked = 0

    bots_list = ("\n".join(f"• @{r['bot_username']}" for r in selected_bots)
                 if selected_bots else
                 "❎ Выборка пуста. Перейдите в '🗄️ Управление общей базой'")

    if bl_active:
        shield = "🛡️ <b>Защита АКТИВНА</b> — записи ЧС блокируют вход"
        toggle_text = "✅ ЧС: Включён 🟢  —  нажать чтобы выключить"
    else:
        shield = "⚠️ <b>Защита ВЫКЛЮЧЕНА</b> — пользователи из ЧС могут входить"
        toggle_text = "⛔ ЧС: Выключен 🔴  —  нажать чтобы включить"

    text = (
        "🚫 <b>Глобальный Чёрный Список</b>\n"
        "─────────────────────────────\n"
        f"{shield}\n\n"
        f"📂 Записей в базе: <b>{bl_count}</b>\n"
        f"🚫 Заблокировано всего: <b>{total_blocked:,}</b>\n\n"
        f"🤖 <b>Распространяется на ботов:</b>\n{bots_list}\n\n"
        "<i>Управлять ботами — '🗄️ Управление общей базой'</i>"
    )

    kb = [
        [InlineKeyboardButton(text=toggle_text, callback_data=f"ga_bl_master:{owner_id}")],
        [
            InlineKeyboardButton(text="➕ Добавить в ЧС", callback_data=f"ga_bl_add:{owner_id}"),
            InlineKeyboardButton(text="➖ Удалить из ЧС", callback_data=f"ga_bl_del:{owner_id}")
        ],
        [InlineKeyboardButton(text="📥 Скачать ЧС (CSV)", callback_data=f"ga_bl_export_csv:{owner_id}")],
        [InlineKeyboardButton(text="◀️ Назад", callback_data=f"ga_main:{owner_id}")]
    ]

    if callback.message.text:
        try:
            await callback.message.edit_text(text, parse_mode="HTML", reply_markup=InlineKeyboardMarkup(inline_keyboard=kb))
        except Exception:
            pass
    else:
        try:
            await callback.message.delete()
        except:
            pass
        await callback.message.answer(text, parse_mode="HTML", reply_markup=InlineKeyboardMarkup(inline_keyboard=kb))

    await callback.answer()


@router.callback_query(F.data.startswith("ga_bl_master:"))
async def on_ga_bl_master_toggle(callback: CallbackQuery):
    """Master toggle: enable/disable global blacklist enforcement."""
    owner_id = int(callback.data.split(":")[1])
    role, _ = await get_admin_context(callback.from_user.id, callback.from_user.username)
    if not role:
        return await callback.answer("❌ Нет прав", show_alert=True)

    async with get_pool().acquire() as conn:
        current = await conn.fetchval(
            "SELECT blacklist_active FROM platform_users WHERE user_id=$1", owner_id
        )
        new_val = not (current if current is not None else True)
        await conn.execute(
            "UPDATE platform_users SET blacklist_active=$1 WHERE user_id=$2",
            new_val, owner_id
        )
        await conn.execute("""
            INSERT INTO audit_log (owner_id, user_id, action, details)
            VALUES ($1, $2, 'bl_toggle', $3)
        """, owner_id, callback.from_user.id,
            json.dumps({"info": "Blacklist ENABLED" if new_val else "Blacklist DISABLED"}))

    alert = ("✅ ЧС включён — защита активна, записи блокируют вход" if new_val
            else "⛔ ЧС выключен — пользователи из ЧС могут войти")
    await callback.answer(alert, show_alert=True)
    callback.data = f"ga_bl:{owner_id}"
    await on_ga_bl(callback)


@router.callback_query(F.data.startswith("ga_bl_export_csv:"))
async def on_ga_bl_export_csv(callback: CallbackQuery):
    owner_id = int(callback.data.split(":")[1])
    admin_id = callback.from_user.id
    role, _ = await get_admin_context(callback.from_user.id, callback.from_user.username)
    if not role:
        return await callback.answer("❌ Нет прав", show_alert=True)

    msg = await callback.message.edit_text("⏳ Генерирую CSV чёрного списка из выбранных ботов...")
    import asyncio
    asyncio.create_task(_export_bl_csv(callback.bot, callback.message.chat.id, admin_id, owner_id, msg))
    await callback.answer()


async def _export_bl_csv(bot, chat_id: int, admin_id: int, owner_id: int, msg_to_edit=None):
    """
    Собирает ЧС из ВСЕХ ботов, выбранных администратором через ga_selected_bots.
    Дедуплицирует записи по user_id / username.
    Только SELECT-запросы — ни одна запись не изменяется / не удаляется.
    """
    import tempfile, os, csv
    from aiogram.types import FSInputFile, InlineKeyboardMarkup, InlineKeyboardButton

    async with get_pool().acquire() as conn:
        # 1. Получаем child_bot_id выбранных ботов
        sel_bots = await conn.fetch(
            "SELECT child_bot_id FROM ga_selected_bots WHERE admin_id=$1",
            admin_id
        )
        selected_bot_ids = [r['child_bot_id'] for r in sel_bots]

        if not selected_bot_ids:
            kb = [[InlineKeyboardButton(text="◀️ Назад", callback_data=f"ga_bl:{owner_id}")]]
            if msg_to_edit:
                try:
                    await msg_to_edit.edit_text(
                        "⚠️ Выборка ботов пуста. Отметьте боты в '🗄️ Управление общей базой'",
                        reply_markup=InlineKeyboardMarkup(inline_keyboard=kb)
                    )
                except Exception:
                    pass
            return

        # 2. Читаем ЧС из выбранных ботов + глобальные записи платформы (child_bot_id IS NULL)
        rows = await conn.fetch("""
            SELECT DISTINCT ON (COALESCE(user_id::text, lower(username)))
                user_id, username, reason, added_at, child_bot_id
            FROM blacklist
            WHERE
                child_bot_id = ANY($1::int[])
                OR (owner_id = $2 AND child_bot_id IS NULL)
            ORDER BY COALESCE(user_id::text, lower(username)), added_at DESC
        """, selected_bot_ids, owner_id)

    kb = [[InlineKeyboardButton(text="◀️ Назад в ЧС", callback_data=f"ga_bl:{owner_id}")]]

    if not rows:
        if msg_to_edit:
            try:
                await msg_to_edit.edit_text(
                    "⚠️ Чёрный список пуст по выбранным ботам.",
                    reply_markup=InlineKeyboardMarkup(inline_keyboard=kb)
                )
            except Exception:
                pass
        return

    # 3. Генерация CSV
    fd, path = tempfile.mkstemp(suffix=".csv")
    try:
        with open(path, "w", encoding="utf-8-sig", newline="") as f:
            writer = csv.writer(f)
            writer.writerow(["ID", "Username", "Причина", "Дата добавления", "Bot ID"])
            for r in rows:
                writer.writerow([
                    r['user_id'] or "",
                    f"@{r['username']}" if r['username'] else "",
                    r['reason'] or "",
                    r['added_at'].strftime("%Y-%m-%d %H:%M:%S") if r['added_at'] else "",
                    r['child_bot_id'] if r['child_bot_id'] else "global",
                ])

        doc = FSInputFile(path, filename="global_blacklist_export.csv")

        if msg_to_edit:
            try:
                await msg_to_edit.delete()
            except Exception:
                pass

        await bot.send_document(
            chat_id,
            document=doc,
            caption=(
                f"✅ <b>Чёрный список готов!</b>\n"
                f"📄 Записей: <b>{len(rows):,}</b>\n"
                f"🤖 Из ботов: <b>{len(selected_bot_ids)}</b> выбранных"
            ),
            parse_mode="HTML",
            reply_markup=InlineKeyboardMarkup(inline_keyboard=kb)
        )
    finally:
        try:
            os.close(fd)
            os.remove(path)
        except Exception:
            pass


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
            # Запись в журнал
            await conn.execute("""
                INSERT INTO audit_log (owner_id, user_id, action, details)
                VALUES ($1, $2, 'block', $3)
            """, owner_id, message.from_user.id,
                json.dumps({"info": f"Blocked user {target_id} via /block"}))
            await message.answer(f"✅ Пользователь <code>{target_id}</code> занесён в Глобальный ЧС.\nНачинаю блокировку во всех активных ботах...", parse_mode="HTML")
            import asyncio
            asyncio.create_task(_kick_from_all_chats(message.from_user.id, target_id))
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
            await conn.execute("""
                INSERT INTO audit_log (owner_id, user_id, action, details)
                VALUES ($1, $2, 'unblock', $3)
            """, owner_id, message.from_user.id, json.dumps({"info": f"Unblocked user {target_id} via /unblock"}))
            await message.answer(f"✅ Пользователь <code>{target_id}</code> удалён из Глобального ЧС.", parse_mode="HTML")


import tempfile
import csv
import os
from aiogram.types import FSInputFile

async def _show_ga_users(message_or_cb, admin_id: int, owner_id: int):
    async with get_pool().acquire() as conn:
        total_users = await conn.fetchval("""
            SELECT COUNT(DISTINCT bu.user_id) FROM bot_users bu
            JOIN bot_chats bc ON bu.chat_id = bc.chat_id
            JOIN child_bots cb ON cb.id = bc.child_bot_id
            JOIN ga_selected_bots gsb ON gsb.child_bot_id = cb.id AND gsb.admin_id = $1
            WHERE bc.is_active=true AND bu.user_id IS NOT NULL
        """, admin_id) or 0

        alive_users = await conn.fetchval("""
            SELECT COUNT(DISTINCT bu.user_id) FROM bot_users bu
            JOIN bot_chats bc ON bu.chat_id = bc.chat_id
            JOIN child_bots cb ON cb.id = bc.child_bot_id
            JOIN ga_selected_bots gsb ON gsb.child_bot_id = cb.id AND gsb.admin_id = $1
            WHERE bc.is_active=true AND bu.is_active=true AND bu.user_id IS NOT NULL
        """, admin_id) or 0

        net_bots = await conn.fetchval(
            "SELECT COUNT(*) FROM ga_selected_bots WHERE admin_id=$1", admin_id
        ) or 0

        dead_users = total_users - alive_users

    text = (
        "👥 <b>Сводная База Аудитории</b>\n"
        "─────────────────────────────\n"
        f"🗂️ Ботов в выборке: <b>{net_bots}</b>\n"
        "Показываются пользователи только из ботов, отмеченных в 'Управление общей базой'.\n\n"
        f"👥 Уникальных пользователей: <b>{total_users:,}</b>\n"
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
    await _show_ga_users(callback, callback.from_user.id, owner_id)
    await callback.answer()


async def _export_users_csv(bot: Bot, chat_id: int, admin_id: int, export_type: str, msg_to_delete: Message = None):
    """Export audience from bots selected by admin_id in ga_selected_bots."""
    if export_type == "blocked":
        query = """
            SELECT user_id, reason AS first_name, username, false AS is_active, added_at AS joined_at
            FROM blacklist WHERE owner_id = $1
        """
        # For blocked list we use owner_id. admin_id IS the owner here.
        async with get_pool().acquire() as conn:
            rows = await conn.fetch(query, admin_id)
    elif export_type == "admins":
        query = "SELECT admin_id AS user_id, admin_username AS first_name, NULL AS username, true AS is_active, added_at AS joined_at FROM global_admins WHERE owner_id = $1"
        async with get_pool().acquire() as conn:
            rows = await conn.fetch(query, admin_id)
    else:
        # Cross-user audience export: only from bots in ga_selected_bots for this admin
        base_query = """
            SELECT DISTINCT ON (bu.user_id)
                   bu.user_id, bu.first_name, bu.username, bu.is_active, bu.joined_at
            FROM bot_users bu
            JOIN bot_chats bc ON bu.chat_id = bc.chat_id
            JOIN child_bots cb ON cb.id = bc.child_bot_id
            JOIN ga_selected_bots gsb ON gsb.child_bot_id = cb.id AND gsb.admin_id = $1
            WHERE bc.is_active = true AND bu.user_id IS NOT NULL
        """
        if export_type == "alive":
            base_query += " AND bu.is_active = true"
        async with get_pool().acquire() as conn:
            rows = await conn.fetch(base_query, admin_id)
        
    if msg_to_delete:
        try:
            await msg_to_delete.delete()
        except Exception:
            pass

    if not rows:
        kb = [[InlineKeyboardButton(text="◀️ Назад в Меню", callback_data=f"ga_dl_bck:{admin_id}")]]
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
    
    kb = [[InlineKeyboardButton(text="◀️ Назад в Базу Аудитории", callback_data=f"ga_dl_bck:{admin_id}")]]
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
    asyncio.create_task(_export_users_csv(callback.bot, callback.message.chat.id, callback.from_user.id, export_type, msg))
    await callback.answer()


@router.callback_query(F.data.startswith("ga_dl_bck:"))
async def on_ga_dl_bck(callback: CallbackQuery):
    owner_id = int(callback.data.split(":")[1])
    try:
        await callback.message.delete()
    except Exception:
        pass
        
    role, owner_id2 = await get_admin_context(callback.from_user.id, callback.from_user.username)
    if role:
        msg = await callback.message.answer("♻️ Открываю меню...")
        await _show_ga_users(msg, callback.from_user.id, owner_id2)
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
# 🗂️ Управление общей базой
# ══════════════════════════════════════════════════════════════

async def _show_bots_network_page(callback: CallbackQuery, admin_id: int, page: int):
    """Отрисовывает страницу со списком ВСЕХ ботов платформы и чекбоксами выбора."""
    async with get_pool().acquire() as conn:
        all_bots = await conn.fetch("""
            SELECT cb.id, cb.bot_username, cb.bot_name, cb.created_at,
                   pu.username AS owner_username,
                   EXISTS(
                       SELECT 1 FROM ga_selected_bots gsb
                       WHERE gsb.admin_id=$1 AND gsb.child_bot_id=cb.id
                   ) AS selected
            FROM child_bots cb
            JOIN platform_users pu ON pu.user_id = cb.owner_id
            ORDER BY selected DESC, cb.created_at DESC
        """, admin_id)

    total = len(all_bots)
    total_pages = max(1, (total + BOTS_PER_PAGE - 1) // BOTS_PER_PAGE)
    page = max(0, min(page, total_pages - 1))
    selected_count = sum(1 for b in all_bots if b['selected'])

    page_bots = all_bots[page * BOTS_PER_PAGE : (page + 1) * BOTS_PER_PAGE]

    text = (
        "🗄️ <b>Управление общей базой</b>\n"
        "─────────────────────────────\n"
        f"🤖 Всего ботов: <b>{total}</b>   │   ✅ В выборке: <b>{selected_count}</b>\n\n"
        "Отметьте нужные боты — они будут использоваться в Глобальном ЧС и экспорте аудитории.\n"
        "<i>Нажмите на бота — поставить/снять галочку</i>"
    )

    kb = [
        [InlineKeyboardButton(text="🔍 Найти бота по названию", callback_data=f"ga_bots_search:{admin_id}")]
    ]

    for bot_row in page_bots:
        icon = "✅" if bot_row['selected'] else "⬜"
        owner_tag = f" (@{bot_row['owner_username']})" if bot_row['owner_username'] else ""
        kb.append([InlineKeyboardButton(
            text=f"{icon} @{bot_row['bot_username']}{owner_tag}",
            callback_data=f"ga_bot_sel:{admin_id}:{bot_row['id']}:{page}"
        )])

    # Пагинация
    if total_pages > 1:
        nav = []
        if page > 0:
            nav.append(InlineKeyboardButton(text="◀️", callback_data=f"ga_bots:{admin_id}:{page - 1}"))
        nav.append(InlineKeyboardButton(text=f"{page + 1} / {total_pages}", callback_data="ga_bots_noop"))
        if page < total_pages - 1:
            nav.append(InlineKeyboardButton(text="▶️", callback_data=f"ga_bots:{admin_id}:{page + 1}"))
        kb.append(nav)

    kb.append([InlineKeyboardButton(text="◀️ Назад в Панель", callback_data=f"ga_main:{admin_id}")])

    await callback.message.edit_text(text, parse_mode="HTML", reply_markup=InlineKeyboardMarkup(inline_keyboard=kb))


@router.callback_query(F.data.startswith("ga_bots:"))
async def on_ga_bots(callback: CallbackQuery):
    parts = callback.data.split(":")
    admin_id = int(parts[1])
    page = int(parts[2]) if len(parts) > 2 else 0

    role, _ = await get_admin_context(callback.from_user.id, callback.from_user.username)
    if not role:
        return await callback.answer("❌ Нет прав", show_alert=True)

    await _show_bots_network_page(callback, callback.from_user.id, page)
    await callback.answer()


@router.callback_query(F.data.startswith("ga_bot_sel:"))
async def on_ga_bot_select_toggle(callback: CallbackQuery):
    """PUT/DELETE row in ga_selected_bots for this admin + chosen child_bot."""
    parts = callback.data.split(":")
    # admin_id in callback data is the platform owner_id — ignored, we trust the caller
    child_bot_id = int(parts[2])
    page = int(parts[3]) if len(parts) > 3 else 0
    admin_id = callback.from_user.id  # always tied to the actual admin pressing the button

    role, _ = await get_admin_context(callback.from_user.id, callback.from_user.username)
    if not role:
        return await callback.answer("❌ Нет прав", show_alert=True)

    async with get_pool().acquire() as conn:
        exists = await conn.fetchval(
            "SELECT 1 FROM ga_selected_bots WHERE admin_id=$1 AND child_bot_id=$2",
            admin_id, child_bot_id
        )
        if exists:
            await conn.execute(
                "DELETE FROM ga_selected_bots WHERE admin_id=$1 AND child_bot_id=$2",
                admin_id, child_bot_id
            )
            status = "⬜ Убран из выборки"
        else:
            await conn.execute(
                "INSERT INTO ga_selected_bots(admin_id, child_bot_id) VALUES($1,$2) ON CONFLICT DO NOTHING",
                admin_id, child_bot_id
            )
            status = "✅ Добавлен в выборку"

    await callback.answer(status)
    await _show_bots_network_page(callback, admin_id, page)


@router.callback_query(F.data == "ga_bots_noop")
async def on_ga_bots_noop(callback: CallbackQuery):
    await callback.answer()


@router.callback_query(F.data.startswith("ga_bots_search:"))
async def on_ga_bots_search(callback: CallbackQuery, state: FSMContext):
    admin_id = callback.from_user.id
    await state.set_state(BotNetworkFSM.waiting_search)
    await state.update_data(admin_id=admin_id)

    text = (
        "🔍 <b>Поиск бота</b>\n"
        "─────────────────────────────\n"
        "Введите <b>@username</b> бота, который нужно найти и добавить в выборку.\n\n"
        "<i>Пример: @mybotname или просто mybotname</i>"
    )
    kb = [[InlineKeyboardButton(text="❌ Отмена", callback_data=f"ga_bots_search_cancel:{admin_id}")]]
    await callback.message.edit_text(text, parse_mode="HTML", reply_markup=InlineKeyboardMarkup(inline_keyboard=kb))
    await callback.answer()


@router.callback_query(F.data.startswith("ga_bots_search_cancel:"))
async def on_ga_bots_search_cancel(callback: CallbackQuery, state: FSMContext):
    await state.clear()
    admin_id = callback.from_user.id
    await _show_bots_network_page(callback, admin_id, 0)
    await callback.answer()


@router.message(BotNetworkFSM.waiting_search)
async def on_bots_search_input(message: Message, state: FSMContext):
    data = await state.get_data()
    admin_id = data.get("admin_id") or message.from_user.id
    await state.clear()

    query = message.text.strip().lstrip("@").lower()

    async with get_pool().acquire() as conn:
        # Search across ALL bots on the platform, not just the admin's own
        bot_row = await conn.fetchrow(
            "SELECT id, bot_username FROM child_bots WHERE LOWER(bot_username)=$1",
            query
        )

    if not bot_row:
        kb = [[InlineKeyboardButton(text="◀️ Назад к списку", callback_data=f"ga_bots:{admin_id}:0")]]
        await message.answer(
            f"⚠️ Бот <code>@{query}</code> не найден на платформе.\n"
            "Убедитесь, что бот добавлен в систему.",
            parse_mode="HTML",
            reply_markup=InlineKeyboardMarkup(inline_keyboard=kb)
        )
        return

    async with get_pool().acquire() as conn:
        already = await conn.fetchval(
            "SELECT 1 FROM ga_selected_bots WHERE admin_id=$1 AND child_bot_id=$2",
            admin_id, bot_row['id']
        )
        if not already:
            await conn.execute(
                "INSERT INTO ga_selected_bots(admin_id, child_bot_id) VALUES($1,$2) ON CONFLICT DO NOTHING",
                admin_id, bot_row['id']
            )
            result_text = f"✅ Бот <b>@{bot_row['bot_username']}</b> добавлен в вашу выборку."
        else:
            result_text = f"ℹ️ Бот <b>@{bot_row['bot_username']}</b> уже в вашей выборке."

    kb = [[InlineKeyboardButton(text="✅ К списку ботов", callback_data=f"ga_bots:{admin_id}:0")]]
    await message.answer(result_text, parse_mode="HTML", reply_markup=InlineKeyboardMarkup(inline_keyboard=kb))
