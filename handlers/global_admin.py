import json
import logging
from aiogram import Router, F, Bot
from aiogram.types import Message, CallbackQuery, InlineKeyboardMarkup, InlineKeyboardButton
from aiogram.filters import Command
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup

from db.pool import get_pool
from services.discount import get_active_discount, set_discount
from services.security import decrypt_token

logger = logging.getLogger(__name__)
router = Router()

class BroadcastFSM(StatesGroup):
    waiting_message = State()

class StaffFSM(StatesGroup):
    waiting_remove_input = State()
    waiting_add_input    = State()
    waiting_user_search  = State()
    waiting_set_expire   = State()
    waiting_pm_input     = State()
    waiting_note_input   = State()


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
        or (settings.owner_username and un == settings.owner_username.lower().lstrip("@"))
        or (settings.co_owner_telegram_id and user_id == settings.co_owner_telegram_id)
        or (settings.co_owner_username and un == settings.co_owner_username.lower().lstrip("@"))
    )
    if is_project_owner:
        # Мы ВСЕГДА возвращаем ID главного владельца для запросов к БД, 
        # чтобы Совладелец работал с той же самой базой ботов!
        return 'owner', settings.owner_telegram_id

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
        net_bots = await conn.fetchval(
            "SELECT COUNT(*) FROM ga_selected_bots WHERE owner_id=$1", owner_id
        ) or 0

        pu_total = await conn.fetchval("SELECT COUNT(*) FROM platform_users") or 0
        owners_count = await conn.fetchval("SELECT COUNT(DISTINCT owner_id) FROM child_bots") or 0
        clients_count = await conn.fetchval("SELECT COUNT(*) FROM platform_users WHERE tariff != 'free'") or 0
        active_creators = await conn.fetchval("SELECT COUNT(*) FROM (SELECT owner_id FROM child_bots GROUP BY owner_id HAVING COUNT(*) >= 2) t") or 0

        # Заблокировано — уникальные ЧС-записи из ВЫБРАННЫХ ботов (ga_selected_bots)
        selected_bot_ids_rows = await conn.fetch(
            "SELECT child_bot_id FROM ga_selected_bots WHERE owner_id=$1", owner_id
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
            
            total_kicks = await conn.fetchval("""
                SELECT SUM(cb.global_blocked_count)
                FROM ga_selected_bots gsb
                JOIN child_bots cb ON cb.id = gsb.child_bot_id
                WHERE gsb.owner_id=$1
            """, owner_id) or 0
        else:
            bl_count = await conn.fetchval(
                "SELECT COUNT(*) FROM blacklist WHERE owner_id=$1 AND child_bot_id IS NULL", owner_id
            ) or 0
            total_kicks = 0

        admin_count = await conn.fetchval("SELECT COUNT(*) FROM global_admins WHERE owner_id=$1", owner_id) or 0

    status = "👑 ВЛАДЕЛЕЦ" if role == 'owner' else "👮‍♂️ АДМИН"

    blocks = [
        "<b>🐝 BUMBLEBEE BOT</b>",
        
        "<b>🎛 ПАНЕЛЬ УПРАВЛЕНИЯ</b>\n"
        f"├ 👤 <b>Статус:</b> {status}\n"
        f"├ 🤖 <b>Активных в выборке:</b> {net_bots} шт.\n"
        f"└ 🧑‍💼 <b>Моя команда:</b> {admin_count}",
        
        "<b>📊 АУДИТОРИЯ ПЛАТФОРМЫ</b>\n"
        f"├ 👥 <b>Всего пользователей:</b> {pu_total:,}\n"
        f"├ 👨‍💻 <b>Владельцев ботов:</b> {owners_count:,}\n"
        f"├ 💎 <b>Клиентов платформы:</b> {clients_count:,}\n"
        f"└ 🔥 <b>Создателей (2+ ботов):</b> {active_creators:,}"
    ]

    if role == 'owner':
        async with get_pool().acquire() as conn:
            pu_new7 = await conn.fetchval(
                "SELECT COUNT(*) FROM platform_users WHERE created_at >= NOW() - INTERVAL '7 days'"
            ) or 0
            tariff_rows = await conn.fetch(
                "SELECT tariff, COUNT(*) AS cnt FROM platform_users GROUP BY tariff ORDER BY cnt DESC LIMIT 5"
            )
        tariff_str = " • ".join(
            f"{r['tariff'].title()} <b>{r['cnt']}</b>" for r in tariff_rows
        ) if tariff_rows else "—"
        
        blocks.append(
            "<b>🛡 БЕЗОПАСНОСТЬ</b>\n"
            f"├ 📂 <b>Записи глобального ЧС:</b> {bl_count:,}\n"
            f"└ 🛑 <b>Отражено Глобальным ЧС:</b> {total_kicks:,}"
        )
        blocks.append(
            "<b>💳 СТАТИСТИКА И ТАРИФЫ</b>\n"
            f"├ ✨ <b>Прирост (за 7 дней):</b> +{pu_new7}\n"
            f"└ 📎 <b>Распределение:</b> {tariff_str}"
        )
    else:
        blocks.append(
            "<b>🛡 БЕЗОПАСНОСТЬ</b>\n"
            f"├ 📂 <b>Записи глобального ЧС:</b> {bl_count:,}\n"
            f"└ 🛑 <b>Отражено Глобальным ЧС:</b> {total_kicks:,}"
        )

    header = "\n\n".join(blocks)

    kb = []
    if role == 'owner':
        kb.append([
            InlineKeyboardButton(text="👥 Моя команда",   callback_data=f"ga_team:{owner_id}"),
            InlineKeyboardButton(text="📊 Аналитика",     callback_data=f"ga_stats:{owner_id}"),
        ])
        
    kb.extend([
        [
            InlineKeyboardButton(text="📢 Рассылка",           callback_data=f"ga_broadcast:{owner_id}"),
            InlineKeyboardButton(text="🏷 Скидки",             callback_data=f"ga_discounts:{owner_id}")
        ],
        [
            InlineKeyboardButton(text="🚫 Глобальный ЧС",     callback_data=f"ga_bl:{owner_id}"),
            InlineKeyboardButton(text="👥 База пользователей", callback_data=f"ga_users:{owner_id}")
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


@router.callback_query(F.data.startswith("ga_manage_users:"))
async def on_ga_manage_users(callback: CallbackQuery, state: FSMContext):
    owner_id = int(callback.data.split(":")[1])
    role, _ = await get_admin_context(callback.from_user.id, callback.from_user.username)
    if not role:
        return await callback.answer("❌ Нет прав", show_alert=True)
    await state.set_state(StaffFSM.waiting_user_search)
    await state.update_data(owner_id=owner_id)
    await callback.message.edit_text(
        "⚙️ <b>Управление пользователями</b>\n"
        "──────────────────────────────\n\n"
        "Введите <b>@username</b> или <b>Telegram ID</b> клиента платформы,\n"
        "чтобы открыть его карточку:\n\n"
        "<code>@ivan_user</code>  или  <code>123456789</code>",
        parse_mode="HTML",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="🚫 Отмена", callback_data=f"ga_main:{owner_id}")]
        ])
    )
    await callback.answer()


@router.message(StaffFSM.waiting_user_search)
async def on_ga_user_search_input(message: Message, state: FSMContext):
    role, _ = await get_admin_context(message.from_user.id, message.from_user.username)
    if not role:
        return
    data = await state.get_data()
    owner_id = data.get("owner_id")
    await state.clear()
    raw = (message.text or "").strip()
    try:
        await message.delete()
    except Exception:
        pass
    async with get_pool().acquire() as conn:
        if raw.lstrip("-").isdigit():
            row = await conn.fetchrow("SELECT * FROM platform_users WHERE user_id=$1", int(raw))
        elif raw.startswith("@"):
            row = await conn.fetchrow("SELECT * FROM platform_users WHERE lower(username)=lower($1)", raw.lstrip("@"))
        else:
            row = None
    if not row:
        return await message.answer(
            f"⚠️ Пользователь <b>{raw}</b> не найден в базе платформы.",
            parse_mode="HTML",
            reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                [InlineKeyboardButton(text="◀️ Назад", callback_data=f"ga_manage_users:{owner_id}")]
            ])
        )
    await _show_platform_user_card(message, owner_id, row)


async def _show_platform_user_card(message_or_cb, admin_owner_id: int, row):
    uid = row["user_id"]
    uname = f"@{row['username']}" if row.get("username") else "—"
    name = row.get("first_name") or "Аноним"
    tariff = (row.get("tariff") or "free").title()
    until = row["tariff_until"].strftime("%d.%m.%Y") if row.get("tariff_until") else "Бессрочно"
    reg = row["created_at"].strftime("%d.%m.%Y") if row.get("created_at") else "—"

    async with get_pool().acquire() as conn:
        bots_count = await conn.fetchval("SELECT COUNT(*) FROM child_bots WHERE owner_id=$1", uid) or 0
        chats_count = await conn.fetchval(
            "SELECT COUNT(*) FROM bot_chats bc"
            " JOIN child_bots cb ON cb.id=bc.child_bot_id"
            " WHERE cb.owner_id=$1 AND bc.is_active=true", uid
        ) or 0
        users_count = await conn.fetchval(
            "SELECT COUNT(DISTINCT bu.user_id) FROM bot_users bu"
            " JOIN bot_chats bc ON bu.chat_id=bc.chat_id AND bu.owner_id=bc.owner_id"
            " WHERE bu.owner_id=$1", uid
        ) or 0
        bl_count = await conn.fetchval(
            "SELECT COUNT(*) FROM blacklist WHERE owner_id=$1", uid
        ) or 0
        is_blocked = await conn.fetchval(
            "SELECT 1 FROM blacklist WHERE owner_id=$1 AND user_id=$2 AND child_bot_id IS NULL",
            admin_owner_id, uid
        )
        note_row = None
        try:
            note_row = await conn.fetchrow(
                "SELECT note FROM platform_user_notes WHERE target_user_id=$1 AND owner_id=$2",
                uid, admin_owner_id
            )
        except Exception:
            pass

    note_text = f"\n\n📝  <i>{note_row['note']}</i>" if note_row and note_row.get("note") else ""
    blocked_mark = "\n⛔️ <b>Заблокирован на платформе!</b>" if is_blocked else ""

    text = (
        "👤 <b>Карточка пользователя платформы</b>\n"
        "──────────────────────────────\n\n"
        f"🧾  <b>ID:</b> <code>{uid}</code>\n"
        f"🔗  <b>Username:</b> {uname}\n"
        f"👤  <b>Имя:</b> {name}\n"
        f"📅  <b>Регистрация:</b> {reg}\n\n"
        "────── 📊 Статистика ──────\n"
        f"📎  <b>Тариф:</b> {tariff}  (до {until})\n"
        f"🤖  <b>Ботов:</b> {bots_count}    📡  <b>Каналов:</b> {chats_count}\n"
        f"👥  <b>Пользователей:</b> {users_count:,}    🚫  <b>ЧС:</b> {bl_count:,}"
        f"{blocked_mark}{note_text}"
    )
    kb = [
        [InlineKeyboardButton(text="🤖 Боты и каналы",        callback_data=f"ga_pu_bots:{uid}:{admin_owner_id}")],
        [InlineKeyboardButton(text="📢 Написать сообщение",   callback_data=f"ga_pu_pm:{uid}:{admin_owner_id}")],
        [InlineKeyboardButton(text="💎 Управление тарифом",   callback_data=f"ga_pu_tariff:{uid}:{admin_owner_id}")],
        [InlineKeyboardButton(text="📈 История покупок",      callback_data=f"ga_pu_history:{uid}:{admin_owner_id}")],
        [InlineKeyboardButton(
            text="✅ Разблокировать" if is_blocked else "🚫 Заблокировать",
            callback_data=f"ga_pu_block:{uid}:{admin_owner_id}"
        )],
        [InlineKeyboardButton(text="📝 Добавить заметку",     callback_data=f"ga_pu_note:{uid}:{admin_owner_id}")],
        [InlineKeyboardButton(text="◀️ Назад к поиску",       callback_data=f"ga_manage_users:{admin_owner_id}")],
    ]
    markup = InlineKeyboardMarkup(inline_keyboard=kb)
    if isinstance(message_or_cb, Message):
        await message_or_cb.answer(text, parse_mode="HTML", reply_markup=markup)
    else:
        await message_or_cb.message.edit_text(text, parse_mode="HTML", reply_markup=markup)


@router.callback_query(F.data.startswith("ga_pu_card:"))
async def on_ga_pu_card(callback: CallbackQuery):
    parts = callback.data.split(":")
    target_uid, admin_owner_id = int(parts[1]), int(parts[2])
    async with get_pool().acquire() as conn:
        row = await conn.fetchrow("SELECT * FROM platform_users WHERE user_id=$1", target_uid)
    if not row:
        return await callback.answer("❌ Пользователь не найден", show_alert=True)
    await _show_platform_user_card(callback, admin_owner_id, row)
    await callback.answer()


@router.callback_query(F.data.startswith("ga_pu_block:"))
async def on_ga_pu_block(callback: CallbackQuery):
    parts = callback.data.split(":")
    target_uid, admin_owner_id = int(parts[1]), int(parts[2])
    async with get_pool().acquire() as conn:
        existing = await conn.fetchval(
            "SELECT 1 FROM blacklist WHERE owner_id=$1 AND user_id=$2 AND child_bot_id IS NULL",
            admin_owner_id, target_uid
        )
        if existing:
            await conn.execute(
                "DELETE FROM blacklist WHERE owner_id=$1 AND user_id=$2 AND child_bot_id IS NULL",
                admin_owner_id, target_uid
            )
            await callback.answer("✅ Пользователь разблокирован")
        else:
            await conn.execute(
                "INSERT INTO blacklist (owner_id, user_id, reason) VALUES ($1,$2,'Admin block') ON CONFLICT DO NOTHING",
                admin_owner_id, target_uid
            )
            await callback.answer("🚫 Пользователь заблокирован")
        row = await conn.fetchrow("SELECT * FROM platform_users WHERE user_id=$1", target_uid)
    await _show_platform_user_card(callback, admin_owner_id, row)


@router.callback_query(F.data.startswith("ga_pu_delete:"))
async def on_ga_pu_delete(callback: CallbackQuery):
    parts = callback.data.split(":")
    target_uid, admin_owner_id = int(parts[1]), int(parts[2])
    await callback.message.edit_text(
        f"⚠️ <b>Подтверждение удаления</b>\n\n"
        f"Вы собираетесь полностью удалить аккаунт <code>{target_uid}</code>.\n"
        "Действие необратимо!",
        parse_mode="HTML",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="⚠️ ДА, УДАЛИТЬ", callback_data=f"ga_pu_delete_confirm:{target_uid}:{admin_owner_id}")],
            [InlineKeyboardButton(text="❌ Нет, отмена",  callback_data=f"ga_pu_card:{target_uid}:{admin_owner_id}")],
        ])
    )
    await callback.answer()


@router.callback_query(F.data.startswith("ga_pu_delete_confirm:"))
async def on_ga_pu_delete_confirm(callback: CallbackQuery):
    parts = callback.data.split(":")
    target_uid, admin_owner_id = int(parts[1]), int(parts[2])
    async with get_pool().acquire() as conn:
        await conn.execute("DELETE FROM platform_users WHERE user_id=$1", target_uid)
    await callback.message.edit_text(
        f"✅ Аккаунт <code>{target_uid}</code> удалён.",
        parse_mode="HTML",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="◀️ Назад", callback_data=f"ga_manage_users:{admin_owner_id}")]
        ])
    )
    await callback.answer()


@router.callback_query(F.data.startswith("ga_pu_pm:"))
async def on_ga_pu_pm(callback: CallbackQuery, state: FSMContext):
    parts = callback.data.split(":")
    target_uid, admin_owner_id = int(parts[1]), int(parts[2])
    await state.set_state(StaffFSM.waiting_pm_input)
    await state.update_data(pm_target_uid=target_uid, owner_id=admin_owner_id)
    await callback.message.edit_text(
        "📢 <b>Отправка сообщения</b>\n\n"
        "Напишите текст, который будет отправлен пользователю\n"
        "в личные сообщения через основного бота.",
        parse_mode="HTML",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="🚫 Отмена", callback_data=f"ga_pu_card:{target_uid}:{admin_owner_id}")]
        ])
    )
    await callback.answer()


@router.message(StaffFSM.waiting_pm_input)
async def on_ga_pu_pm_input(message: Message, state: FSMContext):
    data = await state.get_data()
    target_uid = data.get("pm_target_uid")
    owner_id = data.get("owner_id")
    await state.clear()
    try:
        await message.delete()
    except Exception:
        pass
    try:
        await message.bot.send_message(target_uid, message.text or "", parse_mode="HTML")
        await message.answer(
            f"✅ <b>Сообщение отправлено</b> пользователю <code>{target_uid}</code>.",
            parse_mode="HTML",
            reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                [InlineKeyboardButton(text="◀️ Назад", callback_data=f"ga_pu_card:{target_uid}:{owner_id}")]
            ])
        )
    except Exception:
        await message.answer(
            "❌ Не удалось отправить. Возможно, пользователь не запускал бота.",
            reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                [InlineKeyboardButton(text="◀️ Назад", callback_data=f"ga_pu_card:{target_uid}:{owner_id}")]
            ])
        )


@router.callback_query(F.data.startswith("ga_pu_note:"))
async def on_ga_pu_note(callback: CallbackQuery, state: FSMContext):
    parts = callback.data.split(":")
    target_uid, admin_owner_id = int(parts[1]), int(parts[2])
    await state.set_state(StaffFSM.waiting_note_input)
    await state.update_data(note_target_uid=target_uid, owner_id=admin_owner_id)
    await callback.message.edit_text(
        "📝 <b>Добавить заметку</b>\n\n"
        "Напишите приватную заметку об этом пользователе\n"
        "(видите только вы, до 500 символов):",
        parse_mode="HTML",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="🚫 Отмена", callback_data=f"ga_pu_card:{target_uid}:{admin_owner_id}")]
        ])
    )
    await callback.answer()


@router.message(StaffFSM.waiting_note_input)
async def on_ga_pu_note_input(message: Message, state: FSMContext):
    data = await state.get_data()
    target_uid = data.get("note_target_uid")
    owner_id = data.get("owner_id")
    await state.clear()
    note = (message.text or "").strip()[:500]
    try:
        await message.delete()
    except Exception:
        pass
    async with get_pool().acquire() as conn:
        try:
            await conn.execute(
                """INSERT INTO platform_user_notes (owner_id, target_user_id, note)
                   VALUES ($1,$2,$3)
                   ON CONFLICT (owner_id, target_user_id) DO UPDATE SET note=EXCLUDED.note""",
                owner_id, target_uid, note
            )
        except Exception:
            pass
    await message.answer(
        "✅ <b>Заметка сохранена.</b>",
        parse_mode="HTML",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="◀️ Назад", callback_data=f"ga_pu_card:{target_uid}:{owner_id}")]
        ])
    )


# ═══ Боты и каналы ═══════════════════════════════════════════════════════════

@router.callback_query(F.data.startswith("ga_pu_bots:"))
async def on_ga_pu_bots(callback: CallbackQuery):
    parts = callback.data.split(":")
    target_uid, admin_owner_id = int(parts[1]), int(parts[2])
    async with get_pool().acquire() as conn:
        bots = await conn.fetch(
            "SELECT id, bot_username, bot_name FROM child_bots WHERE owner_id=$1 ORDER BY created_at",
            target_uid
        )
        pu = await conn.fetchrow("SELECT username, first_name FROM platform_users WHERE user_id=$1", target_uid)
    uname = f"@{pu['username']}" if pu and pu.get('username') else str(target_uid)
    if not bots:
        return await callback.message.edit_text(
            f"🤖 <b>Боты {uname}</b>\n\nУ пользователя нет подключённых ботов.",
            parse_mode="HTML",
            reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                [InlineKeyboardButton(text="◀️ Назад к карточке", callback_data=f"ga_pu_card:{target_uid}:{admin_owner_id}")]
            ])
        )
    async with get_pool().acquire() as conn:
        chat_counts = {}
        for b in bots:
            cnt = await conn.fetchval("SELECT COUNT(*) FROM bot_chats WHERE child_bot_id=$1", b['id']) or 0
            chat_counts[b['id']] = cnt
    lines = []
    kb = []
    for b in bots:
        cnt = chat_counts[b['id']]
        label = f"@{b['bot_username']}  ·  {cnt} пл."
        lines.append(f"• {label}")
        kb.append([InlineKeyboardButton(text=f"🤖 @{b['bot_username']} ({cnt} пл.)",
                                        callback_data=f"ga_pu_bot_detail:{b['id']}:{target_uid}:{admin_owner_id}")])
    kb.append([InlineKeyboardButton(text="◀️ Назад к карточке", callback_data=f"ga_pu_card:{target_uid}:{admin_owner_id}")])
    await callback.message.edit_text(
        f"🤖 <b>Боты {uname}</b>\n──────────────────────────",
        parse_mode="HTML",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=kb)
    )
    await callback.answer()


@router.callback_query(F.data.startswith("ga_pu_bot_detail:"))
async def on_ga_pu_bot_detail(callback: CallbackQuery):
    parts = callback.data.split(":")
    bot_id, target_uid, admin_owner_id = int(parts[1]), int(parts[2]), int(parts[3])
    async with get_pool().acquire() as conn:
        bot_row = await conn.fetchrow("SELECT bot_username FROM child_bots WHERE id=$1", bot_id)
        chats = await conn.fetch(
            "SELECT bc.id, bc.chat_title, bc.chat_type, bc.is_active, "
            "COUNT(bu.user_id) FILTER (WHERE bu.is_active=true) AS subs "
            "FROM bot_chats bc "
            "LEFT JOIN bot_users bu ON bu.chat_id=bc.chat_id AND bu.owner_id=bc.owner_id "
            "WHERE bc.child_bot_id=$1 GROUP BY bc.id ORDER BY bc.added_at",
            bot_id
        )
    bname = f"@{bot_row['bot_username']}" if bot_row else str(bot_id)
    kb = []
    for ch in chats:
        status = "✅" if ch['is_active'] else "❌"
        ctype = "📡" if ch['chat_type'] == 'channel' else "👥"
        subs = ch['subs'] or 0
        title = (ch['chat_title'] or 'Без названия')[:28]
        action_text = "⏸ Выкл" if ch['is_active'] else "▶️ Вкл"
        kb.append([
            InlineKeyboardButton(
                text=f"{status} {ctype} {title}  ·  👥 {subs:,}",
                callback_data=f"ga_pu_noop"
            )
        ])
        kb.append([
            InlineKeyboardButton(text=action_text, callback_data=f"ga_pu_chat_toggle:{ch['id']}:{bot_id}:{target_uid}:{admin_owner_id}"),
            InlineKeyboardButton(text="🗑 Удалить",  callback_data=f"ga_pu_chat_del:{ch['id']}:{bot_id}:{target_uid}:{admin_owner_id}"),
        ])
    kb.append([InlineKeyboardButton(text="🗑 Удалить бот целиком", callback_data=f"ga_pu_bot_del:{bot_id}:{target_uid}:{admin_owner_id}")])
    kb.append([InlineKeyboardButton(text="◀️ Назад к ботам",       callback_data=f"ga_pu_bots:{target_uid}:{admin_owner_id}")])
    total_subs = sum(ch['subs'] or 0 for ch in chats)
    await callback.message.edit_text(
        f"📡 <b>Площадки {bname}</b>\n"
        f"──────────────────────────\n"
        f"Всего площадок: {len(chats)}  ·  Подписчиков: {total_subs:,}",
        parse_mode="HTML",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=kb)
    )
    await callback.answer()


@router.callback_query(F.data.startswith("ga_pu_noop"))
async def on_ga_pu_noop(callback: CallbackQuery):
    await callback.answer()


@router.callback_query(F.data.startswith("ga_pu_chat_toggle:"))
async def on_ga_pu_chat_toggle(callback: CallbackQuery):
    parts = callback.data.split(":")
    chat_row_id, bot_id, target_uid, admin_owner_id = int(parts[1]), int(parts[2]), int(parts[3]), int(parts[4])
    async with get_pool().acquire() as conn:
        current = await conn.fetchval("SELECT is_active FROM bot_chats WHERE id=$1", chat_row_id)
        await conn.execute("UPDATE bot_chats SET is_active=$1 WHERE id=$2", not current, chat_row_id)
    await callback.answer("✅ Площадка выключена" if current else "✅ Площадка включена")
    # refresh the page
    fake_cb = callback
    fake_cb.data = f"ga_pu_bot_detail:{bot_id}:{target_uid}:{admin_owner_id}"
    await on_ga_pu_bot_detail(fake_cb)


@router.callback_query(F.data.startswith("ga_pu_chat_del:"))
async def on_ga_pu_chat_del(callback: CallbackQuery):
    parts = callback.data.split(":")
    chat_row_id, bot_id, target_uid, admin_owner_id = int(parts[1]), int(parts[2]), int(parts[3]), int(parts[4])
    async with get_pool().acquire() as conn:
        await conn.execute("DELETE FROM bot_chats WHERE id=$1", chat_row_id)
    await callback.answer("🗑 Площадка удалена")
    fake_cb = callback
    fake_cb.data = f"ga_pu_bot_detail:{bot_id}:{target_uid}:{admin_owner_id}"
    await on_ga_pu_bot_detail(fake_cb)


@router.callback_query(F.data.startswith("ga_pu_bot_del:"))
async def on_ga_pu_bot_del(callback: CallbackQuery):
    parts = callback.data.split(":")
    bot_id, target_uid, admin_owner_id = int(parts[1]), int(parts[2]), int(parts[3])
    async with get_pool().acquire() as conn:
        await conn.execute("DELETE FROM child_bots WHERE id=$1", bot_id)
    await callback.answer("🗑 Бот удалён")
    fake_cb = callback
    fake_cb.data = f"ga_pu_bots:{target_uid}:{admin_owner_id}"
    await on_ga_pu_bots(fake_cb)


# ═══ Управление тарифом ═══════════════════════════════════════════════════════

TARIFFS = ["free", "start", "pro", "business"]
TARIFF_LABELS = {"free": "Free", "start": "Start", "pro": "Pro", "business": "Business"}
DURATIONS = [("1m", "1 мес.", 1), ("1y", "1 год", 12), ("forever", "♾ Навсегда", 0)]


@router.callback_query(F.data.startswith("ga_pu_tariff:"))
async def on_ga_pu_tariff(callback: CallbackQuery):
    parts = callback.data.split(":")
    target_uid, admin_owner_id = int(parts[1]), int(parts[2])
    async with get_pool().acquire() as conn:
        row = await conn.fetchrow("SELECT tariff, tariff_until FROM platform_users WHERE user_id=$1", target_uid)
        pu = await conn.fetchrow("SELECT username FROM platform_users WHERE user_id=$1", target_uid)
    cur_tariff = (row["tariff"] or "free").lower() if row else "free"
    cur_until = row["tariff_until"].strftime("%d.%m.%Y") if row and row.get("tariff_until") else None
    uname = f"@{pu['username']}" if pu and pu.get('username') else str(target_uid)
    tariff_line = f"📎 <b>Текущий тариф:</b> {TARIFF_LABELS.get(cur_tariff, cur_tariff.title())}"
    if cur_tariff != "free" and cur_until:
        tariff_line += f"\n📅 <b>Действует до:</b> {cur_until}"
    kb = []
    row_btns = []
    for t in TARIFFS:
        if t == cur_tariff:
            continue
        row_btns.append(InlineKeyboardButton(text=TARIFF_LABELS[t], callback_data=f"ga_pu_tariff_pick:{t}:{target_uid}:{admin_owner_id}"))
    kb.append(row_btns)  # all tariff buttons in one row
    kb.append([InlineKeyboardButton(text="◀️ Назад к карточке", callback_data=f"ga_pu_card:{target_uid}:{admin_owner_id}")])
    await callback.message.edit_text(
        f"💎 <b>Управление тарифом</b> {uname}\n\n{tariff_line}\n\nСмените тариф:",
        parse_mode="HTML",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=kb)
    )
    await callback.answer()


@router.callback_query(F.data.startswith("ga_pu_tariff_pick:"))
async def on_ga_pu_tariff_pick(callback: CallbackQuery):
    parts = callback.data.split(":")
    new_tariff, target_uid, admin_owner_id = parts[1], int(parts[2]), int(parts[3])
    label = TARIFF_LABELS.get(new_tariff, new_tariff.title())
    kb = []
    dur_row = [
        InlineKeyboardButton(
            text=dur_label,
            callback_data=f"ga_pu_tariff_dur:{new_tariff}:{dur_key}:{target_uid}:{admin_owner_id}"
        )
        for dur_key, dur_label, _ in DURATIONS
    ]
    kb.append(dur_row)  # all durations in one row
    kb.append([InlineKeyboardButton(text="◀️ Назад", callback_data=f"ga_pu_tariff:{target_uid}:{admin_owner_id}")])
    await callback.message.edit_text(
        f"💎 <b>Тариф: {label}</b>\n\nВыберите срок:",
        parse_mode="HTML",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=kb)
    )
    await callback.answer()


@router.callback_query(F.data.startswith("ga_pu_tariff_dur:"))
async def on_ga_pu_tariff_dur(callback: CallbackQuery):
    from datetime import datetime, timezone, timedelta
    parts = callback.data.split(":")
    new_tariff, dur_key, target_uid, admin_owner_id = parts[1], parts[2], int(parts[3]), int(parts[4])
    label = TARIFF_LABELS.get(new_tariff, new_tariff.title())
    dur_label = next((d[1] for d in DURATIONS if d[0] == dur_key), dur_key)
    if dur_key == "forever":
        until_str = "Бессрочно"
    elif dur_key == "1m":
        until = datetime.now(timezone.utc) + timedelta(days=30)
        until_str = until.strftime("%d.%m.%Y")
    else:  # 1y
        until = datetime.now(timezone.utc) + timedelta(days=365)
        until_str = until.strftime("%d.%m.%Y")
    async with get_pool().acquire() as conn:
        pu = await conn.fetchrow("SELECT username FROM platform_users WHERE user_id=$1", target_uid)
    uname = f"@{pu['username']}" if pu and pu.get('username') else str(target_uid)
    await callback.message.edit_text(
        f"✅ <b>Подтвердите смену тарифа</b>\n\n"
        f"👤 {uname}\n"
        f"💎 {label}  •  {dur_label}\n"
        f"📅 Будет действовать до: <b>{until_str}</b>",
        parse_mode="HTML",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="✅ Да, применить", callback_data=f"ga_pu_tariff_apply:{new_tariff}:{dur_key}:{target_uid}:{admin_owner_id}")],
            [InlineKeyboardButton(text="❌ Отмена",        callback_data=f"ga_pu_tariff:{target_uid}:{admin_owner_id}")],
        ])
    )
    await callback.answer()


@router.callback_query(F.data.startswith("ga_pu_tariff_apply:"))
async def on_ga_pu_tariff_apply(callback: CallbackQuery):
    from datetime import datetime, timezone, timedelta
    parts = callback.data.split(":")
    new_tariff, dur_key, target_uid, admin_owner_id = parts[1], parts[2], int(parts[3]), int(parts[4])
    if dur_key == "forever":
        new_until = None
    elif dur_key == "1m":
        new_until = datetime.now(timezone.utc) + timedelta(days=30)
    else:
        new_until = datetime.now(timezone.utc) + timedelta(days=365)
    async with get_pool().acquire() as conn:
        await conn.execute(
            "UPDATE platform_users SET tariff=$1, tariff_until=$2 WHERE user_id=$3",
            new_tariff, new_until, target_uid
        )
    await callback.answer("✅ Тариф обновлён", show_alert=True)
    # Go back to tariff screen refreshed
    fake_cb = callback
    fake_cb.data = f"ga_pu_tariff:{target_uid}:{admin_owner_id}"
    await on_ga_pu_tariff(fake_cb)


# ═══ История покупок ══════════════════════════════════════════════════════════

@router.callback_query(F.data.startswith("ga_pu_history:"))
async def on_ga_pu_history(callback: CallbackQuery):
    from datetime import timedelta
    parts = callback.data.split(":")
    target_uid, admin_owner_id = int(parts[1]), int(parts[2])
    async with get_pool().acquire() as conn:
        pu = await conn.fetchrow(
            "SELECT username, tariff, tariff_until FROM platform_users WHERE user_id=$1", target_uid
        )
        try:
            payments = await conn.fetch(
                "SELECT tariff, period, paid_at FROM payments "
                "WHERE user_id=$1 AND status='paid' ORDER BY paid_at DESC LIMIT 20",
                target_uid
            )
        except Exception:
            payments = []

    uname = f"@{pu['username']}" if pu and pu.get('username') else str(target_uid)
    cur_tariff = (pu['tariff'] or 'free').lower() if pu else 'free'
    cur_until = pu['tariff_until'] if pu else None

    # Текущий тариф блок
    if cur_tariff == 'free':
        cur_block = "🆓  <b>Free</b> (бесплатный)"
    else:
        tname = TARIFF_LABELS.get(cur_tariff, cur_tariff.title())
        until_str = cur_until.strftime('%d.%m.%Y') if cur_until else 'Бессрочно'
        cur_block = (
            f"💎  <b>{tname}</b>\n"
            f"⏰  Окончание:  <b>{until_str}</b>"
        )

    sep = "─" * 8
    lines = [
        f"📈 <b>История тарифов {uname}</b>\n",
        f"{sep} Текущий тариф {sep}",
        cur_block,
    ]

    if not payments:
        lines.append(f"\n{sep} Оплаченные {sep}\nИстория покупок пуста.")
    else:
        lines.append(f"\n{sep} Оплаченные {sep}")
        for idx, p in enumerate(payments, 1):
            tname = TARIFF_LABELS.get((p['tariff'] or 'free').lower(), p.get('tariff', '?'))
            period = p.get('period') or 'month'
            paid = p['paid_at']
            if paid:
                start_str = paid.strftime('%d.%m.%Y')
                delta = timedelta(days=365) if period == 'year' else timedelta(days=30)
                end_str = (paid + delta).strftime('%d.%m.%Y')
            else:
                start_str = end_str = '?'
            lines.append(
                f"#{idx}  💎 <b>{tname}</b>  •  {period}\n"
                f"    📅 Оплачен:  {start_str}\n"
                f"    ⏰ До:  {end_str}"
            )
        lines.append(f"\n<b>Всего оплат: {len(payments)}</b>")

    await callback.message.edit_text(
        "\n".join(lines), parse_mode="HTML",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="◀️ Назад к карточке", callback_data=f"ga_pu_card:{target_uid}:{admin_owner_id}")]
        ])
    )
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
        [InlineKeyboardButton(text="➖ Удалить админа", callback_data=f"ga_team_remove:{owner_id}")],
        [InlineKeyboardButton(text="◀️ Назад", callback_data=f"ga_main:{owner_id}")]
    ]
    await callback.message.edit_text(text, parse_mode="HTML", reply_markup=InlineKeyboardMarkup(inline_keyboard=kb))
    await callback.answer()


@router.callback_query(F.data.startswith("ga_team_howto:"))
async def on_ga_team_howto(callback: CallbackQuery, state: FSMContext):
    owner_id = int(callback.data.split(":")[1])
    role, _ = await get_admin_context(callback.from_user.id, callback.from_user.username)
    if role != 'owner':
        return await callback.answer("❌ Только для Владельца", show_alert=True)
    await state.set_state(StaffFSM.waiting_add_input)
    await state.update_data(owner_id=owner_id)
    text = (
        "➕ <b>Добавить администратора</b>\n\n"
        "Введите <b>@username</b> или <b>Telegram ID</b> сотрудника:\n\n"
        "<code>@username</code>  или  <code>123456789</code>\n\n"
        "<i>ID можно узнать через </i><a href='https://t.me/userinfobot'>@userinfobot</a>"
    )
    kb = [[InlineKeyboardButton(text="🚫 Отмена", callback_data=f"ga_team:{owner_id}")]]
    await callback.message.edit_text(text, parse_mode="HTML",
                                     reply_markup=InlineKeyboardMarkup(inline_keyboard=kb),
                                     disable_web_page_preview=True)
    await callback.answer()


@router.message(StaffFSM.waiting_add_input)
async def on_ga_team_add_input(message: Message, state: FSMContext):
    role, _ = await get_admin_context(message.from_user.id, message.from_user.username)
    if role != 'owner':
        return
    data = await state.get_data()
    owner_id = data.get("owner_id")
    await state.clear()

    raw = (message.text or "").strip()
    try:
        await message.delete()
    except Exception:
        pass

    target_id = None
    target_username = None

    if raw.lstrip("-").isdigit():
        target_id = int(raw)
    elif raw.startswith("@"):
        target_username = raw.lstrip("@")
    else:
        return await message.answer(
            "❌ Неверный формат. Введите @username или числовой ID.",
            reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                [InlineKeyboardButton(text="◀️ Назад", callback_data=f"ga_team:{owner_id}")]
            ])
        )

    import asyncpg
    async with get_pool().acquire() as conn:
        # Если ввели username — ищем ID в platform_users
        if not target_id and target_username:
            row = await conn.fetchrow(
                "SELECT user_id FROM platform_users WHERE lower(username)=lower($1) LIMIT 1",
                target_username
            )
            if row:
                target_id = row["user_id"]

        display_name = f"@{target_username}" if target_username else f"<code>{target_id}</code>"

        if not target_id:
            return await message.answer(
                f"⚠️ Пользователь {display_name} не найден в базе. Убедитесь, что он хоть раз запускал вашего бота, или используйте Telegram ID.",
                parse_mode="HTML",
                reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                    [InlineKeyboardButton(text="◀️ Назад", callback_data=f"ga_team:{owner_id}")]
                ])
            )

        try:
            await conn.execute(
                "INSERT INTO global_admins (owner_id, admin_id, admin_username) VALUES ($1, $2, $3)",
                owner_id, target_id, target_username
            )
            await conn.execute("""
                INSERT INTO audit_log (owner_id, user_id, action, details)
                VALUES ($1, $2, 'add_admin', $3)
            """, owner_id, message.from_user.id,
                json.dumps({"added_admin_id": target_id, "username": target_username}))

            tag = f"@{target_username}" if target_username else ""
            id_str = f"<code>{target_id}</code>"
            label = f"{tag} ({id_str})" if tag else id_str
            await message.answer(
                f"✅ <b>Сотрудник добавлен!</b>\n\n"
                f"👤 {label}\n"
                f"🔑 <b>Роль:</b> Администратор\n"
                f"✅ Теперь он видит ЧС, базу аудитории и запускает рассылки.",
                parse_mode="HTML",
                reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                    [InlineKeyboardButton(text="◀️ К сотрудникам", callback_data=f"ga_team:{owner_id}")]
                ])
            )
        except asyncpg.UniqueViolationError:
            await message.answer(
                f"⚠️ {display_name} уже является администратором.",
                parse_mode="HTML",
                reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                    [InlineKeyboardButton(text="◀️ Назад", callback_data=f"ga_team:{owner_id}")]
                ])
            )


@router.callback_query(F.data.startswith("ga_team_remove:"))
async def on_ga_team_remove(callback: CallbackQuery, state: FSMContext):
    owner_id = int(callback.data.split(":")[1])
    role, _ = await get_admin_context(callback.from_user.id, callback.from_user.username)
    if role != 'owner':
        return await callback.answer("❌ Только для Владельца", show_alert=True)
    await state.set_state(StaffFSM.waiting_remove_input)
    await state.update_data(owner_id=owner_id)
    text = (
        "➖ <b>Удалить администратора</b>\n\n"
        "Введите <b>@username</b> или <b>Telegram ID</b> сотрудника, которого хотите убрать:\n\n"
        "<code>@username</code> или <code>123456789</code>"
    )
    kb = [[InlineKeyboardButton(text="🚫 Отмена", callback_data=f"ga_team:{owner_id}")]]
    await callback.message.edit_text(text, parse_mode="HTML", reply_markup=InlineKeyboardMarkup(inline_keyboard=kb))
    await callback.answer()


@router.message(StaffFSM.waiting_remove_input)
async def on_ga_team_remove_input(message: Message, state: FSMContext):
    role, _ = await get_admin_context(message.from_user.id, message.from_user.username)
    if role != 'owner':
        return
    data = await state.get_data()
    owner_id = data.get("owner_id")
    await state.clear()

    raw = (message.text or "").strip()
    try:
        await message.delete()
    except Exception:
        pass

    target_id = None
    target_username = None

    if raw.lstrip("-").isdigit():
        target_id = int(raw)
    elif raw.startswith("@"):
        target_username = raw.lstrip("@").lower()
    else:
        return await message.answer("❌ Неверный формат. Введите @username или числовой ID.",
                                    reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                                        [InlineKeyboardButton(text="◀️ Назад", callback_data=f"ga_team:{owner_id}")]
                                    ]))

    async with get_pool().acquire() as conn:
        # Если ввели @username, сначала попытаемся получить его ID из платформы, 
        # потому что старые админы могут не иметь сохраненного юзернейма в global_admins
        if not target_id and target_username:
            user_row = await conn.fetchrow(
                "SELECT user_id FROM platform_users WHERE lower(username)=$1 LIMIT 1",
                target_username
            )
            if user_row:
                target_id = user_row["user_id"]

        import asyncpg
        
        row = None
        if target_id:
            row = await conn.fetchrow(
                "SELECT admin_id, admin_username FROM global_admins WHERE owner_id=$1 AND admin_id=$2",
                owner_id, target_id
            )
        
        # Резервный поиск по имени, если в базе он есть, но ID не был найден
        if not row and target_username:
            row = await conn.fetchrow(
                "SELECT admin_id, admin_username FROM global_admins WHERE owner_id=$1 AND lower(admin_username)=$2",
                owner_id, target_username
            )

        if not row:
            ident = raw
            return await message.answer(
                f"⚠️ Администратор <b>{ident}</b> не найден в вашем списке сотрудников.",
                parse_mode="HTML",
                reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                    [InlineKeyboardButton(text="◀️ Назад", callback_data=f"ga_team:{owner_id}")]
                ])
            )

        removed_id = row["admin_id"]
        removed_name = f"@{row['admin_username']}" if row['admin_username'] else f"<code>{removed_id}</code>"
        await conn.execute(
            "DELETE FROM global_admins WHERE owner_id=$1 AND admin_id=$2",
            owner_id, removed_id
        )
        await conn.execute("""
            INSERT INTO audit_log (owner_id, user_id, action, details)
            VALUES ($1, $2, 'remove_admin', $3)
        """, owner_id, message.from_user.id,
            json.dumps({"removed_admin_id": removed_id, "removed_by": message.from_user.id}))

    await message.answer(
        f"✅ Администратор {removed_name} успешно удалён из списка сотрудников.",
        parse_mode="HTML",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="◀️ К сотрудникам", callback_data=f"ga_team:{owner_id}")]
        ])
    )


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
    from datetime import datetime
    
    # ── 1. БЕЗОПАСНОСТЬ И ЗАЩИТА ──
    total_bl = await conn.fetchval("SELECT COUNT(*) FROM blacklist") or 0
    total_prevented = await conn.fetchval("SELECT SUM(blocked_count) FROM child_bots") or 0

    # ── 2. ТРАФИК И АУДИТОРИЯ ──
    total_audience = await conn.fetchval("SELECT COUNT(*) FROM bot_users") or 0
    new_users_24h = await conn.fetchval("SELECT COUNT(*) FROM bot_users WHERE joined_at >= NOW() - INTERVAL '24 hours'") or 0
    active_bots = await conn.fetchval("SELECT COUNT(*) FROM child_bots") or 0
    
    # ── 3. ПЛАТФОРМА ──
    active_chats = await conn.fetchval("SELECT COUNT(*) FROM bot_chats WHERE is_active=true") or 0
    paid_owners = await conn.fetchval("SELECT COUNT(*) FROM platform_users WHERE tariff != 'free'") or 0
    
    report_time = datetime.now().strftime("%d.%m.%Y в %H:%M")

    # Форматирование тысячных разделителей через пробел
    fmt = lambda x: f"{x:,}".replace(",", " ")

    return (
        "📊 <b>Аналитика Bumblebee Bot</b>\n"
        "━━━━━━━━━━━━━━━━━━━━━━━\n\n"
        "🛡 <b>БЕЗОПАСНОСТЬ И ЗАЩИТА</b>\n"
        f"├ В глобальном ЧС: <b>{fmt(total_bl)}</b>\n"
        f"└ Предотвращено вторжений: <b>{fmt(total_prevented)}</b>\n\n"
        "⚡ <b>ТРАФИК И АУДИТОРИЯ</b>\n"
        f"├ Суммарный охват сети: <b>{fmt(total_audience)}</b> чел.\n"
        f"├ Прирост (за 24 часа): <b>+{fmt(new_users_24h)}</b>\n"
        f"└ Активных ботов в работе: <b>{fmt(active_bots)}</b>\n\n"
        "💎 <b>ПЛАТФОРМА</b>\n"
        f"├ Управляется каналов/групп: <b>{fmt(active_chats)}</b>\n"
        f"└ Клиентов с подпиской (PRO+): <b>{fmt(paid_owners)}</b>\n"
        "━━━━━━━━━━━━━━━━━━━━━━━\n"
        f"🕒 <i>Актуально на: {report_time}</i>"
    )


# ── Аудитория ───────────────────────────────────────────────────────────────

async def _build_audience_text(period: str, conn, owner_id: int = 0) -> str:
    label = _period_label(period)
    where = _period_where(period, "created_at")
    cond  = f"WHERE {where}" if where else ""

    # Общая статистика
    total_all  = await conn.fetchval("SELECT COUNT(*) FROM platform_users") or 0
    new_period = await conn.fetchval(f"SELECT COUNT(*) FROM platform_users {cond}") or 0
    paid_total = await conn.fetchval("SELECT COUNT(*) FROM platform_users WHERE tariff != 'free'") or 0
    free_total = total_all - paid_total

    # Сегментация по тарифам (всегда по всем данным)
    seg_rows = await conn.fetch(
        "SELECT COALESCE(tariff,'free') as t, COUNT(*) as cnt FROM platform_users GROUP BY tariff"
    )
    seg = {r['t']: r['cnt'] for r in seg_rows}
    leads   = seg.get('free', 0)
    start_c = seg.get('start', 0)
    pro_c   = seg.get('pro', 0)
    biz_c   = seg.get('business', 0)
    clients = start_c + pro_c + biz_c

    banned = 0
    if owner_id:
        banned = await conn.fetchval(
            "SELECT COUNT(*) FROM blacklist WHERE owner_id=$1 AND child_bot_id IS NULL", owner_id
        ) or 0

    sep = "─" * 8
    return (
        f"👥 <b>АУДИТОРИЯ</b>  —  <b>{label}</b>\n"
        "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n\n"
        f"{sep} Общая статистика {sep}\n"
        f"👤  Всего:  <b>{total_all:,}</b>    🆕  Новых {label.lower()}:  <b>{new_period:,}</b>\n"
        f"✅  Платных:  <b>{paid_total:,}</b>    💡  Бесплатных:  <b>{free_total:,}</b>\n\n"
        f"{sep} Сегменты по тарифам {sep}\n"
        f"💡  Free:  <b>{leads:,}</b>\n"
        f"🌱  Start:  <b>{start_c:,}</b>\n"
        f"⭐  Pro:  <b>{pro_c:,}</b>\n"
        f"💼  Business:  <b>{biz_c:,}</b>\n\n"
        f"💰  Всего платных:  <b>{clients:,}</b>" +
        (f"\n🚫  В ЧС (платформа):  <b>{banned}</b>" if owner_id else "")
    )


async def _build_segment_text(period: str, owner_id: int, conn) -> str:
    """Backward compat: redirects to merged audience function."""
    return await _build_audience_text(period, conn, owner_id)


async def _build_finance_text(period: str, conn, owner_id: int = 0) -> str:
    label   = _period_label(period)
    w_paid    = _period_where(period, "paid_at")
    w_created = _period_where(period, "created_at")
    paid_cond    = f"AND {w_paid}"    if w_paid    else ""
    created_cond = f"AND {w_created}" if w_created else ""

    # ——— Платежи ———
    paid_row = await conn.fetchrow(
        f"SELECT COUNT(*) as cnt, COALESCE(SUM(amount_usd),0) as total FROM payments WHERE status='paid' {paid_cond}"
    )
    pending_row = await conn.fetchrow(
        f"SELECT COUNT(*) as cnt, COALESCE(SUM(amount_usd),0) as total FROM payments WHERE status='pending' AND created_at >= NOW() - INTERVAL '1 hour' {created_cond}"
    )
    expired_pending_cnt = await conn.fetchval(
        f"SELECT COUNT(*) FROM payments WHERE status='pending' AND created_at < NOW() - INTERVAL '1 hour' {created_cond}"
    ) or 0
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
    cancelled   = failed_cnt + expired_cnt + expired_pending_cnt
    attempts    = paid_cnt + cancelled + pending_cnt
    conversion  = round(paid_cnt / attempts * 100, 1) if attempts > 0 else 0.0

    # ——— Доходы по тарифам ———
    rows = await conn.fetch(f"""
        SELECT tariff, period, COUNT(*) as cnt, SUM(amount_usd) as total,
               SUM(CASE WHEN applied_discount > 0 THEN 1 ELSE 0 END) as discount_cnt,
               SUM(CASE WHEN applied_discount > 0 THEN amount_usd ELSE 0 END) as discount_total
        FROM payments
        WHERE status='paid' {paid_cond}
        GROUP BY tariff, period
        ORDER BY SUM(amount_usd) DESC
    """)

    total_sum      = sum(float(r['total']) for r in rows)
    total_cnt_r    = sum(r['cnt'] for r in rows)
    total_disc_sum = sum(float(r['discount_total']) for r in rows)
    total_disc_cnt = sum(r['discount_cnt'] for r in rows)
    avg_check = round(total_sum / total_cnt_r, 2) if total_cnt_r > 0 else 0.0

    tariff_icons = {"start": "🌱 Start", "pro": "⭐ Pro", "business": "💼 Business"}
    period_icons = {"month": "/ мес.", "year": "/ год"}
    top_row   = rows[0] if rows else None
    top_label = f"{tariff_icons.get(top_row['tariff'], top_row['tariff'])} {period_icons.get(top_row['period'], top_row['period'])}" if top_row else '—'

    breakdown = ""
    for r in rows:
        t_l = tariff_icons.get(r['tariff'], r['tariff'].capitalize())
        p_l = period_icons.get(r['period'], r['period'])
        breakdown += f"🔹 {t_l} {p_l}  —  {r['cnt']} шт.  ${float(r['total']):,.2f}\n"
        if r['discount_cnt'] > 0:
            breakdown += f"   └ 🏷 По акциям: {r['discount_cnt']} шт. (${float(r['discount_total']):,.2f})\n"

    sep = "─" * 8
    disc_line = f"🏷  Скидок:  <b>${total_disc_sum:,.2f}</b>  ({total_disc_cnt} шт.)\n" if total_disc_cnt > 0 else ""

    return (
        f"💰 <b>ФИНАНСЫ</b>  —  <b>{label}</b>\n"
        "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n\n"
        f"{sep} Платежи {sep}\n"
        f"⏳  В ожидании:  <b>{pending_cnt}</b>  •  <b>${pending_sum:,.2f}</b>\n"
        f"✅  Оплачено:  <b>{paid_cnt}</b>  •  <b>${paid_sum:,.2f}</b>\n"
        f"❌  Отмены/Просрочено:  <b>{cancelled}</b>\n"
        f"📈  Конверсия:  <b>{conversion}%</b>\n\n"
        f"{sep} Доходы {sep}\n"
        f"💵  Итого:  <b>${total_sum:,.2f}</b>  ({total_cnt_r} платежей)\n"
        f"📊  Средний чек:  <b>${avg_check:,.2f}</b>\n"
        f"🏆  Топ-тариф:  <b>{top_label}</b>\n"
        f"{disc_line}\n"
        f"{sep} По тарифам {sep}\n"
        f"{breakdown or 'Нет оплат за период\n'}"
    )


async def _build_revenue_text(owner_id: int, period: str, conn) -> str:
    """Backward compat: calls merged finance."""
    return await _build_finance_text(period, conn, owner_id)

    paid_row = await conn.fetchrow(
        f"SELECT COUNT(*) as cnt, COALESCE(SUM(amount_usd),0) as total FROM payments WHERE status='paid' {paid_cond}"
    )
    pending_row = await conn.fetchrow(
        f"SELECT COUNT(*) as cnt, COALESCE(SUM(amount_usd),0) as total FROM payments WHERE status='pending' AND created_at >= NOW() - INTERVAL '1 hour' {created_cond}"
    )
    expired_pending_cnt = await conn.fetchval(
        f"SELECT COUNT(*) FROM payments WHERE status='pending' AND created_at < NOW() - INTERVAL '1 hour' {created_cond}"
    ) or 0
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
    cancelled   = failed_cnt + expired_cnt + expired_pending_cnt
    attempts    = paid_cnt + cancelled + pending_cnt
    conversion  = round(paid_cnt / attempts * 100, 1) if attempts > 0 else 0.0




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
        [InlineKeyboardButton(text="👥 Аудитория", callback_data=f"ga_audience:{owner_id}:all")],
        [InlineKeyboardButton(text="💰 Финансы",   callback_data=f"ga_finance:{owner_id}:all")],
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
        text = await _build_audience_text(period, conn, owner_id)
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
        text = await _build_finance_text(period, conn, owner_id)
    kb = _period_tabs_kb(owner_id, "finance", period)
    try:
        await callback.message.edit_text(text, parse_mode="HTML", reply_markup=kb)
    except Exception:
        await callback.message.answer(text, parse_mode="HTML", reply_markup=kb)
    await callback.answer()


# ga_segment and ga_rev redirect to merged handlers
@router.callback_query(F.data.startswith("ga_segment:"))
async def on_ga_segment(callback: CallbackQuery):
    callback.data = callback.data.replace("ga_segment:", "ga_audience:")
    await on_ga_audience(callback)


@router.callback_query(F.data.startswith("ga_rev:"))
async def on_ga_revenue(callback: CallbackQuery):
    callback.data = callback.data.replace("ga_rev:", "ga_finance:")
    await on_ga_finance(callback)



async def _kick_from_all_chats(owner_id: int, target_user_id: int):
    """Кикает пользователя изо всех площадок ботов, отмеченных в общей выборке (ga_selected_bots)."""
    from services.security import decrypt_token
    async with get_pool().acquire() as conn:
        bots_chats = await conn.fetch("""
            SELECT cb.token_encrypted, bc.chat_id
            FROM ga_selected_bots gsb
            JOIN child_bots cb ON cb.id = gsb.child_bot_id
            JOIN bot_chats bc ON bc.child_bot_id = cb.id
            WHERE gsb.owner_id = $1 AND bc.is_active = true
        """, owner_id)

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

async def _mass_kick_unban_list(owner_id: int, target_user_ids: list, turn_on: bool):
    """Снимает/ставит бан со всех активных площадок ботов в выборке для КОНКРЕТНОГО списка пользователей."""
    if not target_user_ids:
        return
        
    from services.security import decrypt_token
    import asyncio
    
    async with get_pool().acquire() as conn:
        bots_chats = await conn.fetch("""
            SELECT cb.id as child_bot_id, cb.token_encrypted, bc.chat_id
            FROM ga_selected_bots gsb
            JOIN child_bots cb ON cb.id = gsb.child_bot_id
            JOIN bot_chats bc ON bc.child_bot_id = cb.id
            WHERE gsb.owner_id = $1 AND bc.is_active = true
        """, owner_id)

    if not bots_chats:
        return

    # Словари для счетчиков
    total_kicked_platform = 0
    kicked_per_bot = {}

    for chat_row in bots_chats:
        token = decrypt_token(chat_row['token_encrypted'])
        if not token: continue
        temp_bot = Bot(token=token)
        cb_id = chat_row['child_bot_id']
        chat_id = chat_row['chat_id']
        try:
            for target_id in target_user_ids:
                try:
                    if turn_on:
                        await temp_bot.ban_chat_member(chat_id=chat_id, user_id=target_id)
                        total_kicked_platform += 1
                        kicked_per_bot[cb_id] = kicked_per_bot.get(cb_id, 0) + 1
                        
                        # Обновляем bot_users (помечаем как неактивного)
                        async with get_pool().acquire() as conn:
                            await conn.execute("""
                                UPDATE bot_users SET is_active=false, left_at=now()
                                WHERE owner_id=$1 AND chat_id=$2 AND user_id=$3
                            """, owner_id, chat_id, target_id)
                    else:
                        await temp_bot.unban_chat_member(chat_id=chat_id, user_id=target_id)
                        # Синхронизация: если юзера разбанили глобально, можно считать его снова `is_active=true`
                        # Но пока не будем трогать, т.к. он сам должен зайти.
                except Exception:
                    pass
                await asyncio.sleep(0.05)
        finally:
            await temp_bot.session.close()

    if total_kicked_platform > 0:
        async with get_pool().acquire() as conn:
            await conn.execute(
                "UPDATE platform_users SET blocked_count = blocked_count + $1 WHERE user_id = $2",
                total_kicked_platform, owner_id
            )
            for bot_id, cnt in kicked_per_bot.items():
                await conn.execute(
                    "UPDATE child_bots SET blocked_count = blocked_count + $1 WHERE id = $2",
                    cnt, bot_id
                )


class GlobalBlacklistFSM(StatesGroup):
    add_user = State()
    del_user = State()

async def _sweep_single_bot(owner_id: int, child_bot_id: int, turn_on: bool, admin_id: int, bot: Bot):
    """
    Целевой рейд при добавлении/удалении одного бота из глобальной выборки.
    Работает полностью в фоне — никаких сообщений администратору.
    """
    from services.security import decrypt_token
    import asyncio

    try:
        async with get_pool().acquire() as conn:
            chats = await conn.fetch("""
                SELECT bc.chat_id, cb.token_encrypted
                FROM bot_chats bc
                JOIN child_bots cb ON cb.id = bc.child_bot_id
                WHERE bc.child_bot_id = $1 AND bc.owner_id = $2 AND bc.is_active = true
            """, child_bot_id, owner_id)

            users = await conn.fetch(
                "SELECT user_id FROM blacklist WHERE owner_id=$1 AND child_bot_id IS NULL AND user_id IS NOT NULL",
                owner_id
            )

        if not chats or not users:
            return

        for user_row in users:
            uid = user_row["user_id"]
            for chat_row in chats:
                token = decrypt_token(chat_row["token_encrypted"])
                if not token:
                    continue
                temp_bot = Bot(token=token)
                try:
                    if turn_on:
                        await temp_bot.ban_chat_member(chat_id=chat_row["chat_id"], user_id=uid)
                    else:
                        await temp_bot.unban_chat_member(chat_id=chat_row["chat_id"], user_id=uid)
                except Exception:
                    pass
                finally:
                    await temp_bot.session.close()
                await asyncio.sleep(0.05)
    except Exception:
        pass


async def mass_sync_blacklist(owner_id: int, turn_on: bool, admin_id: int, bot: Bot):
    """Фоновая задача для массовой синхронизации (бан/разбан) всех записей глобального ЧС."""
    from services.security import decrypt_token
    import asyncio

    action_text = "блокировка" if turn_on else "разблокировка"
    try:
        msg = await bot.send_message(
            chat_id=admin_id,
            text=f"⏳ <b>Глобальная синхронизация</b>\n\nНачат процесс массовой операции ({action_text}) по базе ЧС во всех выбранных проектах.\n<i>Пожалуйста, подождите...</i>",
            parse_mode="HTML"
        )
    except Exception:
        return

    async with get_pool().acquire() as conn:
        bots_chats = await conn.fetch("""
            SELECT cb.token_encrypted, bc.chat_id
            FROM ga_selected_bots gsb
            JOIN child_bots cb ON cb.id = gsb.child_bot_id
            JOIN bot_chats bc ON bc.child_bot_id = cb.id
            WHERE gsb.owner_id = $1 AND bc.is_active = true
        """, owner_id)
        
        selected_bot_ids = [r['child_bot_id'] for r in await conn.fetch("SELECT child_bot_id FROM ga_selected_bots WHERE owner_id=$1", owner_id)]
        
        if not selected_bot_ids or not bots_chats:
            await msg.edit_text("ℹ️ Нет активных каналов выборки для синхронизации.")
            return

        users = await conn.fetch("""
            SELECT DISTINCT ON (COALESCE(user_id::text, lower(username))) user_id
            FROM blacklist
            WHERE child_bot_id = ANY($1::int[]) OR (owner_id = $2 AND child_bot_id IS NULL)
        """, selected_bot_ids, owner_id)

    success_count = 0
    total_actions = len(users) * len(bots_chats)
    
    if total_actions == 0:
        await msg.edit_text("ℹ️ Чёрный список пуст, синхронизация не требуется.")
        return

    for user_row in users:
        target_id = user_row['user_id']
        if not target_id: continue 
        
        for chat_row in bots_chats:
            token = decrypt_token(chat_row['token_encrypted'])
            if not token: continue
            
            temp_bot = Bot(token=token)
            try:
                if turn_on:
                    await temp_bot.ban_chat_member(chat_id=chat_row['chat_id'], user_id=target_id)
                else:
                    await temp_bot.unban_chat_member(chat_id=chat_row['chat_id'], user_id=target_id)
                success_count += 1
            except Exception:
                pass
            finally:
                await temp_bot.session.close()
            await asyncio.sleep(0.05) 

    finish_text = "заблокированы" if turn_on else "разблокированы"
    await msg.edit_text(f"✅ <b>Синхронизация завершена!</b>\n\nПользователи успешно {finish_text} во всех {len(bots_chats)} активных разделах выборки.", parse_mode="HTML")

@router.callback_query(F.data.startswith("ga_bl:"))
async def on_ga_bl(callback: CallbackQuery, state: FSMContext = None):
    if state:
        await state.clear()
    role, owner_id = await get_admin_context(callback.from_user.id, callback.from_user.username)
    if not role:
        return await callback.answer("❌ Нет прав", show_alert=True)

    async with get_pool().acquire() as conn:
        # 1. Получаем список выбранных ботов (с их id и обоими счетчиками блокировок)
        selected_bots = await conn.fetch("""
            SELECT cb.id, cb.bot_username, cb.blocked_count, cb.global_blocked_count
            FROM ga_selected_bots gsb
            JOIN child_bots cb ON cb.id = gsb.child_bot_id
            WHERE gsb.owner_id = $1
            ORDER BY gsb.selected_at ASC
        """, owner_id)
        selected_bot_ids = [r['id'] for r in selected_bots]

        # 2. Вытаскиваем глобальный статус активности ЧС платформы
        pu_row = await conn.fetchrow(
            "SELECT blacklist_active FROM platform_users WHERE user_id=$1", owner_id
        )
        bl_active = pu_row['blacklist_active'] if pu_row and pu_row['blacklist_active'] is not None else True

        # 3. Независимые счетчики записей: Глобальный ЧС (наши) vs Локальные ЧС (юзеров ботов)
        # Глобальный ЧС — записи загруженные администратором (child_bot_id IS NULL)
        global_record_count = await conn.fetchval(
            "SELECT COUNT(*) FROM blacklist WHERE owner_id=$1 AND child_bot_id IS NULL", owner_id
        ) or 0

        if selected_bot_ids:
            # Локальные ЧС — уникальные записи в базах выбранных ботов (только child_bot_id)
            local_record_count = await conn.fetchval("""
                SELECT COUNT(DISTINCT COALESCE(user_id::text, lower(username)))
                FROM blacklist
                WHERE child_bot_id = ANY($1::int[])
            """, selected_bot_ids) or 0

            # Суммарные блокировки: global_blocked — только наш ЧС, total — все боты
            total_global_blocked = sum((r['global_blocked_count'] or 0) for r in selected_bots)
            total_all_blocked = sum((r['blocked_count'] or 0) for r in selected_bots)
            # Локальные блокировки = все минус влияние глобального ЧС
            total_local_blocked = max(0, total_all_blocked - total_global_blocked)
        else:
            local_record_count = 0
            total_global_blocked = 0
            total_all_blocked = 0
            total_local_blocked = 0

        bl_count = global_record_count + local_record_count
        total_blocked = total_all_blocked

    bots_list = ("\n".join(f"• @{r['bot_username']}" for r in selected_bots)
                 if selected_bots else
                 "❎ Выборка пуста. Перейдите в '🗄️ Управление общей базой'")

    if bl_active:
        shield = "🛡️ <b>Защита АКТИВНА</b> — записи ЧС блокируют вход"
        toggle_text = "✅ ЧС: Включён 🟢"
    else:
        shield = "⚠️ <b>Защита ВЫКЛЮЧЕНА</b> — пользователи из ЧС могут входить"
        toggle_text = "☑️ ЧС: Выключен 🔴"

    text = (
        "🚫 <b>Глобальный Чёрный Список</b>\n\n"
        f"{shield}\n\n"
        f"🗄 <b>База ЧС: {bl_count} записей</b>\n"
        f"├ Глобальный (наш): <b>{global_record_count}</b>\n"
        f"└ ЧС пользователей: <b>{local_record_count}</b>\n\n"
        f"🛡 <b>Нейтрализовано угроз: {total_blocked:,}</b>\n"
        f"├ Нашим глоб. ЧС: <b>{total_global_blocked:,}</b>\n"
        f"└ ЧС пользов. ботов: <b>{total_local_blocked:,}</b>\n\n"
        f"🤖 <b>Распространяется на ботов:</b>\n{bots_list}\n\n"
        "<i>Управлять ботами — '🗄️ Управление общей базой'</i>"
    )

    kb = [
        [InlineKeyboardButton(text=toggle_text, callback_data=f"ga_bl_master:{owner_id}")],
        [
            InlineKeyboardButton(text="➕ Добавить в ЧС", callback_data=f"ga_bl_add:{owner_id}"),
            InlineKeyboardButton(text="➖ Удалить из ЧС", callback_data=f"ga_bl_del:{owner_id}")
        ],
        [InlineKeyboardButton(text="🗑 Очистить ЧС", callback_data=f"ga_bl_clear_confirm:{owner_id}")],
        [InlineKeyboardButton(text="📥 Скачать ЧС (CSV)", callback_data=f"ga_bl_export_csv:{owner_id}")],
        [InlineKeyboardButton(text="◀️ Назад", callback_data=f"ga_main:{owner_id}")]
    ]

    # Если текущее сообщение — документ (например, после загрузки CSV), edit_text недоступен.
    # Проверяем тип и либо редактируем, либо шлём новое сообщение.
    try:
        if callback.message.text or callback.message.caption:
            await callback.message.edit_text(text, parse_mode="HTML", reply_markup=InlineKeyboardMarkup(inline_keyboard=kb))
        else:
            # Сообщение без текста (документ/фото) — удаляем и отправляем новое
            try:
                await callback.message.delete()
            except Exception:
                pass
            await callback.message.answer(text, parse_mode="HTML", reply_markup=InlineKeyboardMarkup(inline_keyboard=kb))
    except Exception:
        # Если edit не удался (message too old, etc.) — просто шлём новое
        try:
            await callback.message.answer(text, parse_mode="HTML", reply_markup=InlineKeyboardMarkup(inline_keyboard=kb))
        except Exception:
            pass

    try:
        await callback.answer()
    except Exception:
        pass  # already answered by toggle handler


@router.callback_query(F.data.startswith("ga_bl_clear_confirm:"))
async def on_ga_bl_clear_confirm(callback: CallbackQuery):
    owner_id = int(callback.data.split(":")[1])
    role, context_owner_id = await get_admin_context(callback.from_user.id, callback.from_user.username)
    if not role or context_owner_id != owner_id:
        return await callback.answer("❌ Нет прав", show_alert=True)
    
    async with get_pool().acquire() as conn:
        bl_count_global = await conn.fetchval(
            "SELECT COUNT(*) FROM blacklist WHERE owner_id=$1 AND child_bot_id IS NULL", owner_id
        ) or 0
            
    if bl_count_global == 0:
        return await callback.answer("🗑 Ваш Глобальный Чёрный Список уже абсолютно пуст!", show_alert=True)

    text = (
        "⚠️ <b>Очистка Глобального ЧС</b>\n"
        "─────────────────────────────\n"
        "Вы уверены, что хотите безвозвратно удалить <b>ВЕСЬ</b> ваш глобальный черный список бота?\n\n"
        "✅ <i>Не переживайте: локальные базы данных пользователей (черные списки в их дочерних ботах) "
        "затронуты <b>НЕ будут</b>. Удаляются только глобальные блокировки!</i>\n\n"
        f"<i>Будет удалено ваших глобальных записей: {bl_count_global}</i>"
    )

    kb = [
        [
            InlineKeyboardButton(text="Да, очистить", callback_data=f"ga_bl_clear_yes:{owner_id}"),
            InlineKeyboardButton(text="Нет, отмена", callback_data=f"ga_bl:{owner_id}")
        ]
    ]
    await callback.message.edit_text(text, parse_mode="HTML", reply_markup=InlineKeyboardMarkup(inline_keyboard=kb))
    await callback.answer()

@router.callback_query(F.data.startswith("ga_bl_clear_yes:"))
async def on_ga_bl_clear_yes(callback: CallbackQuery):
    owner_id = int(callback.data.split(":")[1])
    role, context_owner_id = await get_admin_context(callback.from_user.id, callback.from_user.username)
    if not role or context_owner_id != owner_id:
        return await callback.answer("❌ Нет прав", show_alert=True)

    async with get_pool().acquire() as conn:
        # Удаляем ТОЛЬКО глобальные записи администратора платформы (child_bot_id IS NULL).
        await conn.execute(
            "DELETE FROM blacklist WHERE owner_id=$1 AND child_bot_id IS NULL", owner_id
        )
            
    await callback.answer("✅ ВАШ Глобальный Чёрный список полностью очищен!", show_alert=True)
    await on_ga_bl(callback)


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

    try:
        await callback.answer()   # тихо, без попапа
    except Exception:
        pass
    
    import asyncio
    asyncio.create_task(mass_sync_blacklist(owner_id, turn_on=new_val, admin_id=callback.from_user.id, bot=callback.bot))
    
    # Сразу обновляем экран, передав управление в on_ga_bl
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
            "SELECT child_bot_id FROM ga_selected_bots WHERE owner_id=$1",
            owner_id
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
            ORDER BY COALESCE(user_id::text, lower(username)), (child_bot_id IS NULL) DESC, added_at DESC
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
            writer.writerow(["ID", "Username", "Причина", "Дата добавления", "Источник (Система)"])
            for r in rows:
                source_label = "Глобальный ЧС (Платформа)" if r['child_bot_id'] is None else f"Локальный ЧС (Bot ID: {r['child_bot_id']})"
                writer.writerow([
                    r['user_id'] or "",
                    f"@{r['username']}" if r['username'] else "",
                    r['reason'] or "",
                    r['added_at'].strftime("%Y-%m-%d %H:%M:%S") if r['added_at'] else "",
                    source_label,
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
async def on_ga_bl_add(callback: CallbackQuery, state: FSMContext):
    owner_id = int(callback.data.split(":")[1])
    await state.set_state(GlobalBlacklistFSM.add_user)
    await state.update_data(owner_id=owner_id)
    text = (
        "➕ <b>Добавить в Глобальный ЧС</b>\n\n"
        "Отправьте мне <b>ID</b>, <b>@юзернейм</b> ИЛИ загрузите <b>документ (.txt / .csv)</b> со списком.\n\n"
        "<i>Пользователи будут немедленно исключены из всех каналов/групп в вашей активной выборке ботов.</i>"
    )
    kb = [[InlineKeyboardButton(text="Отмена", callback_data=f"ga_bl:{owner_id}")]]
    await callback.message.edit_text(text, parse_mode="HTML", reply_markup=InlineKeyboardMarkup(inline_keyboard=kb))
    await callback.answer()

@router.message(GlobalBlacklistFSM.add_user)
async def process_ga_bl_add(message: Message, state: FSMContext, bot: Bot):
    data = await state.get_data()
    owner_id = data.get("owner_id")
    
    import csv, io as _io

    parsed_lines = []
    file_io = None
    if message.text:
        parsed_lines = [message.text.strip()]
    elif message.document:
        if not message.document.file_name.lower().endswith(('.txt', '.csv')):
            return await message.answer("❌ Поддерживаются только форматы .txt и .csv")
        status_msg = await message.answer("⏳ Читаю файл...")
        file_io = _io.BytesIO()
        await bot.download(message.document, destination=file_io)
        parsed_lines = None  # будет разобран ниже через csv.reader
        await status_msg.delete()
    else:
        return await message.answer("❌ Формат неверен. Отправьте текст или документ.")

    targets = []
    if parsed_lines is not None:
        # Ручной ввод: одна строка = один пользователь
        for line in parsed_lines:
            clean = line.strip()
            if not clean or clean.startswith('#'):
                continue
            if clean.lstrip('-').isdigit():
                uid = int(clean)
                if uid > 0:
                    targets.append({'id': uid, 'username': None})
            elif clean.startswith('@'):
                targets.append({'id': None, 'username': clean[1:]})
            elif len(clean) >= 3 and all(c.isalnum() or c == '_' for c in clean):
                targets.append({'id': None, 'username': clean})
    else:
        # CSV/TXT документ: используем csv.reader для правильного парсинга
        text_content = file_io.getvalue().decode('utf-8-sig') if hasattr(file_io, 'getvalue') else content
        reader = csv.reader(_io.StringIO(text_content))
        for row in reader:
            if not row:
                continue
            # Берем первый непустой столбец (ID или @username)
            cell = row[0].strip().strip('"')
            if not cell or cell.startswith('#'):
                continue
            if cell.lstrip('-').isdigit():
                uid = int(cell)
                if uid > 0:
                    targets.append({'id': uid, 'username': None})
            elif cell.startswith('@'):
                targets.append({'id': None, 'username': cell[1:]})
            elif len(cell) >= 3 and all(c.isalnum() or c == '_' for c in cell):
                targets.append({'id': None, 'username': cell})

    if not targets:
        return await message.answer("❌ В сообщении или файле не найдено ни одного правильного ID или юзернейма.")

    await state.clear()
    status_msg = await message.answer(f"⏳ Анализ базы и добавление ({len(targets):,} записей)...")

    target_ids_to_kick = []
    inserted_count = 0
    added_by = message.from_user.id

    import asyncpg
    async with get_pool().acquire() as conn:
        for t in targets:
            if t['username'] and not t['id']:
                row = await conn.fetchrow("""
                    SELECT user_id FROM bot_users WHERE LOWER(username) = $1 AND user_id IS NOT NULL
                    UNION
                    SELECT user_id FROM platform_users WHERE LOWER(username) = $1 AND user_id IS NOT NULL
                    LIMIT 1
                """, t['username'].lower())
                if row and row['user_id']:
                    t['id'] = row['user_id']

            if t['id']:
                target_ids_to_kick.append(t['id'])

            try:
                if t['id'] and t['username']:
                    # Лечим уже существующую запись без ID
                    await conn.execute("""
                        UPDATE blacklist SET user_id = $1
                        WHERE owner_id = $2 AND child_bot_id IS NULL
                          AND lower(username) = $3 AND user_id IS NULL
                    """, t['id'], owner_id, t['username'].lower())
                    res = await conn.execute("""
                        INSERT INTO blacklist (owner_id, user_id, username, added_by, reason)
                        VALUES ($1, $2, $3, $4, 'Global Admin Block')
                        ON CONFLICT DO NOTHING
                    """, owner_id, t['id'], t['username'], added_by)
                elif t['id']:
                    res = await conn.execute("""
                        INSERT INTO blacklist (owner_id, user_id, added_by, reason)
                        VALUES ($1, $2, $3, 'Global Admin Block')
                        ON CONFLICT DO NOTHING
                    """, owner_id, t['id'], added_by)
                else:
                    res = await conn.execute("""
                        INSERT INTO blacklist (owner_id, username, added_by, reason)
                        VALUES ($1, $2, $3, 'Global Admin Block')
                        ON CONFLICT DO NOTHING
                    """, owner_id, t['username'], added_by)
                if res == "INSERT 0 1":
                    inserted_count += 1

                ident = t['id'] if t['id'] else f"@{t['username']}"
                await conn.execute("""
                    INSERT INTO audit_log (owner_id, user_id, action, details)
                    VALUES ($1, $2, 'block', $3)
                    ON CONFLICT DO NOTHING
                """, owner_id, added_by, json.dumps({"info": f"Blocked user {ident} via FSM"}))
            except Exception:
                pass

    kb = [[InlineKeyboardButton(text="◀️ Вернуться в Глобальный ЧС", callback_data=f"ga_bl:{owner_id}")]]
    text_res = (
        f"✅ <b>Операция завершена!</b>\n\n"
        f"📥 Обработано строк: <b>{len(targets):,}</b>\n"
        f"✅ Добавлено новых: <b>{inserted_count:,}</b>\n"
        f"⚠️ Уже было в базе: <b>{len(targets) - inserted_count:,}</b>"
    )
    if target_ids_to_kick:
        text_res += f"\n\n⏳ <i>Запущен фоновый рейд по всем выбранным проектам для {len(target_ids_to_kick):,} пользователей...</i>"
        import asyncio
        asyncio.create_task(_mass_kick_unban_list(owner_id, target_ids_to_kick, turn_on=True))

    await status_msg.edit_text(text_res, parse_mode="HTML", reply_markup=InlineKeyboardMarkup(inline_keyboard=kb))

@router.callback_query(F.data.startswith("ga_bl_del:"))
async def on_ga_bl_del(callback: CallbackQuery, state: FSMContext):
    owner_id = int(callback.data.split(":")[1])
    await state.set_state(GlobalBlacklistFSM.del_user)
    await state.update_data(owner_id=owner_id)
    text = (
        "➖ <b>Удалить из Глобального ЧС</b>\n\n"
        "Отправьте мне <b>ID</b>, <b>@юзернейм</b> ИЛИ загрузите <b>документ (.txt / .csv)</b> со списком.\n\n"
        "<i>Пользователи будут немедленно разблокированы во всех каналах/группах выборки.</i>"
    )
    kb = [[InlineKeyboardButton(text="Отмена", callback_data=f"ga_bl:{owner_id}")]]
    await callback.message.edit_text(text, parse_mode="HTML", reply_markup=InlineKeyboardMarkup(inline_keyboard=kb))
    await callback.answer()

@router.message(GlobalBlacklistFSM.del_user)
async def process_ga_bl_del(message: Message, state: FSMContext, bot: Bot):
    data = await state.get_data()
    owner_id = data.get("owner_id")
    
    targets = []
    if message.text:
        lines = [message.text.strip()]
    elif message.document:
        if not message.document.file_name.lower().endswith(('.txt', '.csv')):
            return await message.answer("❌ Поддерживаются только форматы .txt и .csv")
        
        status_msg = await message.answer("⏳ Читаю файл...")
        import io
        file_io = io.BytesIO()
        await bot.download(message.document, destination=file_io)
        try:
            content = file_io.getvalue().decode('utf-8-sig')
        except:
            return await status_msg.edit_text("❌ Ошибка кодировки файла. Пожалуйста, используйте UTF-8.")
        lines = content.splitlines()
        await status_msg.delete()
    else:
        return await message.answer("❌ Формат неверен. Отправьте текст или документ.")
        
    for line in lines:
        clean = line.replace('"', '').replace(',', ' ').strip().split()
        if not clean: continue
        clean = clean[0]
        
        if clean.isdigit():
            targets.append({'id': int(clean), 'username': None})
        elif clean.startswith('@'):
            targets.append({'id': None, 'username': clean[1:]})
        elif len(clean) >= 3 and clean.isalnum():
            targets.append({'id': None, 'username': clean})

    if not targets:
        return await message.answer("❌ Не найдено ни одного правильного ID или юзернейма.")

    await state.clear()
    status_msg = await message.answer("⏳ Анализ базы и удаление...")
    
    target_ids_to_unban = []
    deleted_count = 0
    
    async with get_pool().acquire() as conn:
        for t in targets:
            if t['username'] and not t['id']:
                row = await conn.fetchrow("""
                    SELECT user_id FROM bot_users WHERE LOWER(username) = $1
                    UNION
                    SELECT user_id FROM platform_users WHERE LOWER(username) = $1
                    LIMIT 1
                """, t['username'].lower())
                if row and row['user_id']:
                    t['id'] = row['user_id']
            
            if t['id']: target_ids_to_unban.append(t['id'])
            
            if t['id']:
                res = await conn.execute("DELETE FROM blacklist WHERE owner_id=$1 AND user_id=$2 AND child_bot_id IS NULL", owner_id, t['id'])
            else:
                res = await conn.execute("DELETE FROM blacklist WHERE owner_id=$1 AND username=$2 AND child_bot_id IS NULL", owner_id, t['username'])
                
            if res != "DELETE 0":
                deleted_count += 1
                ident = t['id'] if t['id'] else f"@{t['username']}"
                await conn.execute("""
                    INSERT INTO audit_log (owner_id, user_id, action, details)
                    VALUES ($1, $2, 'unblock', $3)
                """, owner_id, message.from_user.id, json.dumps({"info": f"Unblocked user {ident} via FSM bulk"}))

    kb = [[InlineKeyboardButton(text="◀️ Вернуться", callback_data=f"ga_bl:{owner_id}")]]
    text_res = f"✅ <b>Операция завершена!</b>\n\nОчищено записей из ЧС: <b>{deleted_count}</b> / {len(targets)}"
    if target_ids_to_unban:
        text_res += f"\n⏳ <i>Запущен процесс разблокировки {len(target_ids_to_unban)} пользователей по всем выбранным проектам...</i>"
        import asyncio
        asyncio.create_task(_mass_kick_unban_list(owner_id, target_ids_to_unban, turn_on=False))
        
    await status_msg.edit_text(text_res, parse_mode="HTML", reply_markup=InlineKeyboardMarkup(inline_keyboard=kb))


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
            JOIN ga_selected_bots gsb ON gsb.child_bot_id = cb.id AND gsb.owner_id = $1
            WHERE bc.is_active=true AND bu.user_id IS NOT NULL
        """, owner_id) or 0

        alive_users = await conn.fetchval("""
            SELECT COUNT(DISTINCT bu.user_id) FROM bot_users bu
            JOIN bot_chats bc ON bu.chat_id = bc.chat_id
            JOIN child_bots cb ON cb.id = bc.child_bot_id
            JOIN ga_selected_bots gsb ON gsb.child_bot_id = cb.id AND gsb.owner_id = $1
            WHERE bc.is_active=true AND bu.is_active=true AND bu.user_id IS NOT NULL
        """, owner_id) or 0

        net_bots = await conn.fetchval(
            "SELECT COUNT(*) FROM ga_selected_bots WHERE owner_id=$1", owner_id
        ) or 0

        selected_bots = await conn.fetch("""
            SELECT cb.bot_username
            FROM ga_selected_bots gsb
            JOIN child_bots cb ON cb.id = gsb.child_bot_id
            WHERE gsb.owner_id = $1
            ORDER BY gsb.selected_at ASC
        """, owner_id)
        bots_line = ("\n".join(f"• @{r['bot_username']}" for r in selected_bots)
                     if selected_bots else
                     "❎ Выборка пуста")

        dead_users = total_users - alive_users

    text = (
        "👥 <b>Сводная База Аудитории</b>\n"
        "─────────────────────────────\n"
        f"🗂️ Ботов в выборке: <b>{net_bots}</b>\n"
        "Показываются пользователи только из ботов, отмеченных в '🗄️ Управление общей базой'.\n\n"
        f"👥 Уникальных пользователей: <b>{total_users:,}</b>\n"
        f" ├ 🟢 Живые: {alive_users:,}\n"
        f" └ 🔴 Мёртвые: {dead_users:,}\n\n"
        f"🤖 <b>Распространяется на ботов:</b>\n{bots_line}"
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


async def _export_users_csv(bot: Bot, chat_id: int, owner_id: int, export_type: str, msg_to_delete: Message = None):
    """Export audience from bots selected by owner_id in ga_selected_bots."""
    if export_type == "blocked":
        query = """
            SELECT user_id, reason AS first_name, username, false AS is_active, added_at AS joined_at
            FROM blacklist WHERE owner_id = $1
        """
        async with get_pool().acquire() as conn:
            rows = await conn.fetch(query, owner_id)
    elif export_type == "admins":
        query = "SELECT admin_id AS user_id, admin_username AS first_name, NULL AS username, true AS is_active, added_at AS joined_at FROM global_admins WHERE owner_id = $1"
        async with get_pool().acquire() as conn:
            rows = await conn.fetch(query, owner_id)
    else:
        # Cross-user audience export: only from bots in ga_selected_bots for this owner
        base_query = """
            SELECT DISTINCT ON (bu.user_id)
                   bu.user_id, bu.first_name, bu.username, bu.is_active, bu.joined_at
            FROM bot_users bu
            JOIN bot_chats bc ON bu.chat_id = bc.chat_id
            JOIN child_bots cb ON cb.id = bc.child_bot_id
            JOIN ga_selected_bots gsb ON gsb.child_bot_id = cb.id AND gsb.owner_id = $1
            WHERE bc.is_active = true AND bu.user_id IS NOT NULL
        """
        if export_type == "alive":
            base_query += " AND bu.is_active = true"
        async with get_pool().acquire() as conn:
            rows = await conn.fetch(base_query, owner_id)
        
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
async def on_ga_broadcast(callback: CallbackQuery, state: FSMContext):
    await state.clear()  # <-- Очищаем состояние при входе в меню рассылок или нажатии "Отмена"
    
    role, owner_id = await get_admin_context(callback.from_user.id, callback.from_user.username)
    if not role:
        return await callback.answer("❌ Нет прав", show_alert=True)

    text = (
        "📣 <b>Глобальная Рассылка</b>\n"
        "─────────────────────────────\n"
        "Выберите аудиторию для отправки сообщения.\n\n"
        "<i>После выбора бот попросит прислать текст/фото/видео.\n"
        "Рассылка идет от имени подключенных ботов.</i>"
    )
    kb = [
        [
            InlineKeyboardButton(text="👥 Активные", callback_data=f"ga_bc_seg:{owner_id}:active"),
            InlineKeyboardButton(text="🌍 Вся база", callback_data=f"ga_bc_seg:{owner_id}:all")
        ],
        [
            InlineKeyboardButton(text="🆓 Лиды", callback_data=f"ga_bc_seg:{owner_id}:lead"),
            InlineKeyboardButton(text="🔥 Квалы", callback_data=f"ga_bc_seg:{owner_id}:qual"),
            InlineKeyboardButton(text="💎 Клиенты", callback_data=f"ga_bc_seg:{owner_id}:client")
        ],
        [InlineKeyboardButton(text="⬅️ Назад", callback_data=f"ga_main:{owner_id}")]
    ]
    await callback.message.edit_text(text, parse_mode="HTML", reply_markup=InlineKeyboardMarkup(inline_keyboard=kb))
    await callback.answer()


@router.callback_query(F.data.startswith("ga_bc_seg:"))
async def on_ga_broadcast_segment(callback: CallbackQuery, state: FSMContext):
    parts = callback.data.split(":")
    owner_id = int(parts[1])
    segment = parts[2]

    role, _ = await get_admin_context(callback.from_user.id, callback.from_user.username)
    if not role:
        return await callback.answer("❌ Нет прав", show_alert=True)

    await state.set_state(BroadcastFSM.waiting_message)
    await state.update_data(broadcast_owner=owner_id, broadcast_segment=segment)

    seg_names = {
        "active": "👥 Активных",
        "all": "🌍 Вся база",
        "lead": "🆓 Лиды (Free)",
        "qual": "🔥 Квалы (Pro/Biz)",
        "client": "💎 Клиенты (Платники)"
    }
    s_name = seg_names.get(segment, segment)

    text = (
        f"Выбран сегмент: <b>{s_name}</b>\n"
        "─────────────────────────────\n"
        "Отправьте мне сообщение, которое нужно разослать.\n"
        "<i>(Поддерживается текст, фото, видео)</i>\n\n"
        "Для отмены нажмите кнопку ниже."
    )
    kb = [[InlineKeyboardButton(text="❌ Отмена", callback_data=f"ga_broadcast:{owner_id}")]]
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



@router.message(BroadcastFSM.waiting_message)
async def on_broadcast_message_received(message: Message, state: FSMContext):
    data = await state.get_data()
    owner_id = data.get("broadcast_owner")
    segment = data.get("broadcast_segment")

    if not owner_id or not segment:
        await state.clear()
        return

    await state.clear()
    msg = await message.answer("⏳ Собираю аудиторию...")

    async with get_pool().acquire() as conn:
        if segment == "all":
            rows = await conn.fetch("""
                SELECT DISTINCT ON (bu.user_id) bu.user_id, cb.token_encrypted
                FROM bot_users bu
                JOIN bot_chats bc ON bu.chat_id = bc.chat_id
                JOIN child_bots cb ON cb.id = bc.child_bot_id
                WHERE bc.owner_id = $1 AND bu.user_id IS NOT NULL
                ORDER BY bu.user_id, bu.created_at ASC
            """, owner_id)
        elif segment == "active":
            rows = await conn.fetch("""
                SELECT DISTINCT ON (bu.user_id) bu.user_id, cb.token_encrypted
                FROM bot_users bu
                JOIN bot_chats bc ON bu.chat_id = bc.chat_id
                JOIN child_bots cb ON cb.id = bc.child_bot_id
                WHERE bc.owner_id = $1 AND bu.user_id IS NOT NULL AND bu.is_active = true
                ORDER BY bu.user_id, bu.created_at ASC
            """, owner_id)
        else:
            tariff_filter = ""
            if segment == "lead":
                tariff_filter = "COALESCE(pu.tariff, 'free') = 'free'"
            elif segment == "qual":
                tariff_filter = "pu.tariff IN ('pro', 'business')"
            elif segment == "client":
                tariff_filter = "pu.tariff IN ('start', 'pro', 'business')"
                
            rows = await conn.fetch(f"""
                SELECT DISTINCT ON (bu.user_id) bu.user_id, cb.token_encrypted
                FROM bot_users bu
                JOIN bot_chats bc ON bu.chat_id = bc.chat_id
                JOIN child_bots cb ON cb.id = bc.child_bot_id
                JOIN platform_users pu ON bu.user_id = pu.user_id
                WHERE bc.owner_id = $1 AND bu.user_id IS NOT NULL AND {tariff_filter}
                ORDER BY bu.user_id, bu.created_at ASC
            """, owner_id)

    if not rows:
        return await msg.edit_text("⚠️ В этом сегменте нет ни одного получателя.")

    async def run_broadcast():
        success = 0
        import asyncio
        from aiogram.client.default import DefaultBotProperties
        
        valid_bots = {}
        for r in rows:
            u_id = r['user_id']
            enc_token = r['token_encrypted']
            try:
                token = decrypt_token(enc_token)
            except Exception:
                continue
                
            if token not in valid_bots:
                valid_bots[token] = Bot(token=token, default=DefaultBotProperties(parse_mode="HTML"))
            
            tb = valid_bots[token]
            try:
                await message.copy_to(chat_id=u_id, bot=tb)
                success += 1
            except Exception:
                pass
            await asyncio.sleep(0.04)
            
        for b in valid_bots.values():
            await b.session.close()
            
        await msg.edit_text(f"✅ <b>Рассылка завершена!</b>\n\nДоставлено: <b>{success}</b> пользователей.", parse_mode="HTML")

    import asyncio
    asyncio.create_task(run_broadcast())


# ══════════════════════════════════════════════════════════════
# 🗂️ Управление общей базой
# ══════════════════════════════════════════════════════════════

async def _show_bots_network_page(callback: CallbackQuery, owner_id: int, page: int):
    """Отрисовывает страницу со списком ВСЕХ ботов платформы и чекбоксами выбора."""
    async with get_pool().acquire() as conn:
        all_bots = await conn.fetch("""
            SELECT cb.id, cb.bot_username, cb.bot_name, cb.created_at,
                   pu.username AS owner_username,
                   EXISTS(
                       SELECT 1 FROM ga_selected_bots gsb
                       WHERE gsb.owner_id=$1 AND gsb.child_bot_id=cb.id
                   ) AS selected
            FROM child_bots cb
            JOIN platform_users pu ON pu.user_id = cb.owner_id
            ORDER BY selected DESC, cb.created_at DESC
        """, owner_id)

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
        [
            InlineKeyboardButton(text="🔍 По названию",  callback_data=f"ga_bots_search:{owner_id}"),
            InlineKeyboardButton(text="🔍 По владельцу", callback_data=f"ga_bots_owner_search:{owner_id}"),
        ]
    ]

    for bot_row in page_bots:
        icon = "✅" if bot_row['selected'] else "☑️"
        owner_tag = f" (@{bot_row['owner_username']})" if bot_row['owner_username'] else ""
        kb.append([InlineKeyboardButton(
            text=f"{icon} @{bot_row['bot_username']}{owner_tag}",
            callback_data=f"ga_bot_sel:{owner_id}:{bot_row['id']}:{page}"
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
    admin_id = int(parts[1])
    page = int(parts[2]) if len(parts) > 2 else 0

    role, owner_id = await get_admin_context(callback.from_user.id, callback.from_user.username)
    if not role:
        return await callback.answer("❌ Нет прав", show_alert=True)

    await _show_bots_network_page(callback, owner_id, page)
    await callback.answer()


@router.callback_query(F.data.startswith("ga_bot_sel:"))
async def on_ga_bot_select_toggle(callback: CallbackQuery):
    """PUT/DELETE row in ga_selected_bots — shared selection keyed by owner_id."""
    parts = callback.data.split(":")
    child_bot_id = int(parts[2])
    page = int(parts[3]) if len(parts) > 3 else 0

    # Always use the shared owner_id so ALL admins of the same owner see the same selection
    role, owner_id = await get_admin_context(callback.from_user.id, callback.from_user.username)
    if not role:
        return await callback.answer("❌ Нет прав", show_alert=True)

    newly_added = False
    async with get_pool().acquire() as conn:
        exists = await conn.fetchval(
            "SELECT 1 FROM ga_selected_bots WHERE owner_id=$1 AND child_bot_id=$2",
            owner_id, child_bot_id
        )
        if exists:
            await conn.execute(
                "DELETE FROM ga_selected_bots WHERE owner_id=$1 AND child_bot_id=$2",
                owner_id, child_bot_id
            )
            status = "⬜ Убран из выборки"
        else:
            await conn.execute(
                "INSERT INTO ga_selected_bots(owner_id, child_bot_id) VALUES($1,$2) ON CONFLICT DO NOTHING",
                owner_id, child_bot_id
            )
            status = "✅ Добавлен в выборку"
            newly_added = True

        # Проверяем, активен ли глобальный ЧС прямо сейчас
        bl_active = await conn.fetchval(
            "SELECT COALESCE(blacklist_active, true) FROM platform_users WHERE user_id=$1", owner_id
        )

    await callback.answer(status)

    # Авто-рейд: при добавлении бота — банить всех из глобального ЧС в его каналах
    # При удалении — разбанить (чтобы глобальный ЧС не давил на чужие проекты)
    if bl_active:
        import asyncio
        asyncio.create_task(_sweep_single_bot(
            owner_id=owner_id,
            child_bot_id=child_bot_id,
            turn_on=newly_added,
            admin_id=callback.from_user.id,
            bot=callback.bot,
        ))

    await _show_bots_network_page(callback, owner_id, page)


@router.callback_query(F.data == "ga_bots_noop")
async def on_ga_bots_noop(callback: CallbackQuery):
    await callback.answer()


@router.callback_query(F.data.startswith("ga_bots_search:"))
async def on_ga_bots_search(callback: CallbackQuery, state: FSMContext):
    _, owner_id = await get_admin_context(callback.from_user.id, callback.from_user.username)
    await state.set_state(BotNetworkFSM.waiting_search)
    await state.update_data(owner_id=owner_id, search_mode="name")
    kb = [[InlineKeyboardButton(text="❌ Отмена", callback_data=f"ga_bots_search_cancel:{owner_id}")]]
    await callback.message.edit_text(
        "🔍 <b>Поиск по названию</b>\n"
        "─────────────────────────────\n"
        "Введите <b>часть или полное название</b> бота.\n"
        "<i>Пример: market, mybot, rec</i>",
        parse_mode="HTML",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=kb)
    )
    await callback.answer()


@router.callback_query(F.data.startswith("ga_bots_owner_search:"))
async def on_ga_bots_owner_search(callback: CallbackQuery, state: FSMContext):
    _, owner_id = await get_admin_context(callback.from_user.id, callback.from_user.username)
    await state.set_state(BotNetworkFSM.waiting_search)
    await state.update_data(owner_id=owner_id, search_mode="owner")
    kb = [[InlineKeyboardButton(text="❌ Отмена", callback_data=f"ga_bots_search_cancel:{owner_id}")]]
    await callback.message.edit_text(
        "🔍 <b>Поиск по владельцу</b>\n"
        "─────────────────────────────\n"
        "Введите <b>@username</b> или <b>Telegram ID</b> владельца бота.\n"
        "<i>Пример: @ivan_user или 123456789</i>",
        parse_mode="HTML",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=kb)
    )
    await callback.answer()


@router.callback_query(F.data.startswith("ga_bots_search_cancel:"))
async def on_ga_bots_search_cancel(callback: CallbackQuery, state: FSMContext):
    await state.clear()
    _, owner_id = await get_admin_context(callback.from_user.id, callback.from_user.username)
    await _show_bots_network_page(callback, owner_id, 0)
    await callback.answer()


@router.message(BotNetworkFSM.waiting_search)
async def on_bots_search_input(message: Message, state: FSMContext):
    data = await state.get_data()
    owner_id = data.get("owner_id") or (await get_admin_context(message.from_user.id, message.from_user.username))[1]
    search_mode = data.get("search_mode", "name")
    await state.clear()
    try:
        await message.delete()
    except Exception:
        pass
    raw = (message.text or "").strip()

    if search_mode == "owner":
        # Search by owner username or ID
        async with get_pool().acquire() as conn:
            if raw.lstrip("@").lstrip("-").isdigit():
                owner = await conn.fetchrow(
                    "SELECT user_id, username FROM platform_users WHERE user_id=$1",
                    int(raw.lstrip("@"))
                )
            else:
                owner = await conn.fetchrow(
                    "SELECT user_id, username FROM platform_users WHERE lower(username)=lower($1)",
                    raw.lstrip("@")
                )
        if not owner:
            return await message.answer(
                f"⚠️ Владелец <b>{raw}</b> не найден.",
                parse_mode="HTML",
                reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                    [InlineKeyboardButton(text="◀️ Назад к списку", callback_data=f"ga_bots:{admin_id}:0")]
                ])
            )
        async with get_pool().acquire() as conn:
            bots = await conn.fetch(
                """
                SELECT cb.id, cb.bot_username,
                       pu.username AS owner_username,
                       EXISTS(
                           SELECT 1 FROM ga_selected_bots gsb
                           WHERE gsb.owner_id=$1 AND gsb.child_bot_id=cb.id
                       ) AS selected
                FROM child_bots cb
                JOIN platform_users pu ON pu.user_id=cb.owner_id
                WHERE cb.owner_id=$2
                ORDER BY selected DESC, cb.created_at DESC
                """,
                owner_id, owner['user_id']
            )
        if not bots:
            return await message.answer(
                f"ℹ️ У владельца <b>@{owner.get('username') or owner['user_id']}</b> нет ботов.",
                parse_mode="HTML",
                reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                    [InlineKeyboardButton(text="◀️ Назад к списку", callback_data=f"ga_bots:{admin_id}:0")]
                ])
            )
        uname = f"@{owner['username']}" if owner.get('username') else str(owner['user_id'])
        selected_count = sum(1 for b in bots if b['selected'])
        kb = []
        for b in bots:
            icon = "✅" if b['selected'] else "☑️"
            kb.append([InlineKeyboardButton(
                text=f"{icon} @{b['bot_username']}",
                callback_data=f"ga_bot_sel:{admin_id}:{b['id']}:0"
            )])
        kb.append([InlineKeyboardButton(text="◀️ Назад к списку", callback_data=f"ga_bots:{admin_id}:0")])
        await message.answer(
            f"🔍 <b>Боты владельца {uname}</b>\n"
            f"─────────────────────────────\n"
            f"Найдено: <b>{len(bots)}</b>  │  В выборке: <b>{selected_count}</b>",
            parse_mode="HTML",
            reply_markup=InlineKeyboardMarkup(inline_keyboard=kb)
        )
    else:
        # Search by name — partial ILIKE match
        query = raw.lstrip("@").lower()
        async with get_pool().acquire() as conn:
            bots = await conn.fetch(
                """
                SELECT cb.id, cb.bot_username,
                       pu.username AS owner_username,
                       EXISTS(
                           SELECT 1 FROM ga_selected_bots gsb
                           WHERE gsb.owner_id=$1 AND gsb.child_bot_id=cb.id
                       ) AS selected
                FROM child_bots cb
                JOIN platform_users pu ON pu.user_id=cb.owner_id
                WHERE LOWER(cb.bot_username) LIKE $2
                   OR LOWER(cb.bot_name) LIKE $2
                ORDER BY selected DESC, cb.created_at DESC
                LIMIT 30
                """,
                owner_id, f"%{query}%"
            )
        if not bots:
            return await message.answer(
                f"⚠️ Боты с названием <b>{raw}</b> не найдены.",
                parse_mode="HTML",
                reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                    [InlineKeyboardButton(text="◀️ Назад к списку", callback_data=f"ga_bots:{admin_id}:0")]
                ])
            )
        selected_count = sum(1 for b in bots if b['selected'])
        kb = []
        for b in bots:
            icon = "✅" if b['selected'] else "☑️"
            owner_tag = f" (@{b['owner_username']})" if b['owner_username'] else ""
            kb.append([InlineKeyboardButton(
                text=f"{icon} @{b['bot_username']}{owner_tag}",
                callback_data=f"ga_bot_sel:{admin_id}:{b['id']}:0"
            )])
        kb.append([InlineKeyboardButton(text="◀️ Назад к списку", callback_data=f"ga_bots:{admin_id}:0")])
        await message.answer(
            f"🔍 <b>Результаты поиска: «{raw}»</b>\n"
            f"─────────────────────────────\n"
            f"Найдено: <b>{len(bots)}</b>  │  В выборке: <b>{selected_count}</b>",
            parse_mode="HTML",
            reply_markup=InlineKeyboardMarkup(inline_keyboard=kb)
        )


# ──────────────────────────────────────────────────────────────
# СКИДКИ
# ──────────────────────────────────────────────────────────────
@router.callback_query(F.data.startswith("ga_discounts:"))
async def on_ga_discounts(callback: CallbackQuery):
    owner_id = int(callback.data.split(":")[1])
    role, _ = await get_admin_context(callback.from_user.id, callback.from_user.username)
    if role != 'owner':
        return await callback.answer("❌ Только для Владельца", show_alert=True)

    percent, until = await get_active_discount()
    
    if percent > 0 and until:
        status_text = f"Текущее значение: {percent}%\n(до {until.strftime('%d.%m.%Y %H:%M')})"
    else:
        status_text = "Текущее значение: 0%"

    text = (
        "🏷 <b>Скидка на подписку</b>\n\n"
        f"{status_text}\n\n"
        "Выбери готовое значение кнопкой."
    )
    kb = [
        [
            InlineKeyboardButton(text="0%", callback_data=f"ga_discount:{owner_id}:0"),
            InlineKeyboardButton(text="10%", callback_data=f"ga_discount:{owner_id}:10"),
            InlineKeyboardButton(text="20%", callback_data=f"ga_discount:{owner_id}:20"),
            InlineKeyboardButton(text="30%", callback_data=f"ga_discount:{owner_id}:30")
        ],
        [InlineKeyboardButton(text="⬅️ Назад", callback_data=f"ga_main:{owner_id}")]
    ]
    await callback.message.edit_text(text, parse_mode="HTML", reply_markup=InlineKeyboardMarkup(inline_keyboard=kb))
    await callback.answer()


@router.callback_query(F.data.startswith("ga_discount:"))
async def on_ga_discount_select(callback: CallbackQuery):
    parts = callback.data.split(":")
    owner_id = int(parts[1])
    percent = int(parts[2])

    role, _ = await get_admin_context(callback.from_user.id, callback.from_user.username)
    if role != 'owner':
        return await callback.answer("❌ Только для Владельца", show_alert=True)

    if percent == 0:
        await set_discount(0)
        await callback.answer("Скидка отключена", show_alert=True)
        await callback.message.delete()
        
        status_text = "Текущее значение: 0%"
        text = (
            "🏷 <b>Скидка на подписку</b>\n\n"
            f"{status_text}\n\n"
            "Выбери готовое значение кнопкой."
        )
        kb = [
            [
                InlineKeyboardButton(text="0%", callback_data=f"ga_discount:{owner_id}:0"),
                InlineKeyboardButton(text="10%", callback_data=f"ga_discount:{owner_id}:10"),
                InlineKeyboardButton(text="20%", callback_data=f"ga_discount:{owner_id}:20"),
                InlineKeyboardButton(text="30%", callback_data=f"ga_discount:{owner_id}:30")
            ],
            [InlineKeyboardButton(text="⬅️ Назад", callback_data=f"ga_main:{owner_id}")]
        ]
        await callback.message.answer(text, parse_mode="HTML", reply_markup=InlineKeyboardMarkup(inline_keyboard=kb))
        return

    text = f"На сколько времени включить скидку <b>{percent}%</b>?"
    kb = [
        [
            InlineKeyboardButton(text="3 дня", callback_data=f"ga_dsave:{owner_id}:{percent}:3"),
            InlineKeyboardButton(text="1 неделя", callback_data=f"ga_dsave:{owner_id}:{percent}:7"),
            InlineKeyboardButton(text="2 недели", callback_data=f"ga_dsave:{owner_id}:{percent}:14"),
        ],
        [
            InlineKeyboardButton(text="1 месяц", callback_data=f"ga_dsave:{owner_id}:{percent}:30"),
            InlineKeyboardButton(text="2 месяца", callback_data=f"ga_dsave:{owner_id}:{percent}:60"),
            InlineKeyboardButton(text="3 месяца", callback_data=f"ga_dsave:{owner_id}:{percent}:90"),
        ],
        [InlineKeyboardButton(text="⬅️ Назад", callback_data=f"ga_discounts:{owner_id}")]
    ]
    await callback.message.edit_text(text, parse_mode="HTML", reply_markup=InlineKeyboardMarkup(inline_keyboard=kb))
    await callback.answer()


@router.callback_query(F.data.startswith("ga_dsave:"))
async def on_ga_discount_save(callback: CallbackQuery):
    parts = callback.data.split(":")
    owner_id = int(parts[1])
    percent = int(parts[2])
    days = int(parts[3])

    role, _ = await get_admin_context(callback.from_user.id, callback.from_user.username)
    if role != 'owner':
        return await callback.answer("❌ Только для Владельца", show_alert=True)

    await set_discount(percent, days)
    await callback.answer(f"✅ Скидка {percent}% включена на {days} дней!", show_alert=True)
    
    await callback.message.delete()
    
    percent_now, until = await get_active_discount()
    if percent_now > 0 and until:
        status_text = f"Текущее значение: {percent_now}%\n(до {until.strftime('%d.%m.%Y %H:%M')})"
    else:
        status_text = "Текущее значение: 0%"

    text = (
        "🏷 <b>Скидка на подписку</b>\n\n"
        f"{status_text}\n\n"
        "Выбери готовое значение кнопкой."
    )
    kb = [
        [
            InlineKeyboardButton(text="0%", callback_data=f"ga_discount:{owner_id}:0"),
            InlineKeyboardButton(text="10%", callback_data=f"ga_discount:{owner_id}:10"),
            InlineKeyboardButton(text="20%", callback_data=f"ga_discount:{owner_id}:20"),
            InlineKeyboardButton(text="30%", callback_data=f"ga_discount:{owner_id}:30")
        ],
        [InlineKeyboardButton(text="⬅️ Назад", callback_data=f"ga_main:{owner_id}")]
    ]
    await callback.message.answer(text, parse_mode="HTML", reply_markup=InlineKeyboardMarkup(inline_keyboard=kb))
