import logging
import tempfile
import csv
import os
from datetime import datetime

from aiogram import Router, F
from aiogram.types import (
    CallbackQuery, InlineKeyboardMarkup, InlineKeyboardButton, Message, FSInputFile
)
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup

from db.pool import get_pool
from handlers.global_admin import get_admin_context
from utils.nav import navigate

logger = logging.getLogger(__name__)
router = Router()


class AudienceAnalyzerFSM(StatesGroup):
    waiting_search_name = State()
    waiting_search_owner = State()

PAGE_SIZE = 5

def _fmt_num(n: int) -> str:
    """Форматирование числа с пробелом как разделителем тысяч (русский стандарт)."""
    return f"{n:,}".replace(",", " ")


async def _show_analyzer_panel(message_or_cb, owner_id: int, state: FSMContext, page: int = 0):
    data = await state.get_data()
    selected_channels = data.get("selected_channels", [])
    search_query = data.get("search_query", "")
    search_mode = data.get("search_mode", "name")

    safe_query = search_query.lower().lstrip("@")

    where_clauses = ["bc.is_active = true", "bc.chat_title IS NOT NULL"]
    args = []

    if search_query:
        if search_mode == "owner":
            if safe_query.lstrip("-").isdigit():
                args.append(int(safe_query.lstrip("-")))
                where_clauses.append(f"pu.user_id = ${len(args)}")
            else:
                args.append(f"%{safe_query}%")
                where_clauses.append(f"LOWER(pu.username) LIKE ${len(args)}")
        else:
            args.append(f"%{safe_query}%")
            where_clauses.append(f"LOWER(bc.chat_title) LIKE ${len(args)}")

    where_sql = " AND ".join(where_clauses)

    async with get_pool().acquire() as conn:
        total_channels = await conn.fetchval(f"""
            SELECT COUNT(DISTINCT bc.chat_id)
            FROM bot_chats bc
            JOIN child_bots cb ON cb.id = bc.child_bot_id
            JOIN platform_users pu ON pu.user_id = cb.owner_id
            WHERE {where_sql}
        """, *args) or 0

        args.append(PAGE_SIZE)
        limit_param = f"${len(args)}"

        total_pages = max(1, (total_channels + PAGE_SIZE - 1) // PAGE_SIZE)
        page = max(0, min(page, total_pages - 1))

        args.append(page * PAGE_SIZE)
        offset_param = f"${len(args)}"

        page_channels = await conn.fetch(f"""
            SELECT bc.chat_id, bc.chat_title, pu.username AS owner_username, pu.user_id AS owner_id,
                   COALESCE((SELECT COUNT(*) FROM bot_users bu WHERE bu.chat_id = bc.chat_id AND bu.is_active = true), 0) as subscribers
            FROM bot_chats bc
            JOIN child_bots cb ON cb.id = bc.child_bot_id
            JOIN platform_users pu ON pu.user_id = cb.owner_id
            WHERE {where_sql}
            ORDER BY subscribers DESC
            LIMIT {limit_param} OFFSET {offset_param}
        """, *args)

    # ── Header ────────────────────────────────────────────────────────────────
    sel_count = len(selected_channels)
    text = (
        "📊 <b>Анализ пересечения аудиторий</b>\n\n"
        "<i>Выберите площадки, чтобы найти общих пользователей в их базах.</i>\n\n"
        f"📁 Площадок в системе: <b>{total_channels}</b>  |  ✅ Выбрано: <b>{sel_count}</b>"
    )

    if search_query:
        mode_str = "По владельцу" if search_mode == "owner" else "По названию"
        text += f"\n🔍 <i>Поиск ({mode_str}):</i> <code>{search_query}</code>"


    # ── Клавиатура ────────────────────────────────────────────────────────────
    kb = []
    kb.append([
        InlineKeyboardButton(text="🔍 По названию", callback_data=f"aa_search:{owner_id}:name"),
        InlineKeyboardButton(text="🔍 По владельцу", callback_data=f"aa_search:{owner_id}:owner"),
    ])

    if search_query:
        kb.append([InlineKeyboardButton(text="✖️ Сбросить поиск", callback_data=f"aa_reset_search:{owner_id}")])

    for c in page_channels:
        mark = "✅" if c['chat_id'] in selected_channels else "☑️"
        subs_text = _fmt_num(c['subscribers'])

        chat_raw = c['chat_title'] or "Без назв."
        owner_raw = c['owner_username'] or str(c['owner_id'])

        stats = f" 👥 {subs_text}"
        
        limit = 35
        fixed_len = 2 + len(stats) # mark + space + stats
        avail = limit - fixed_len

        if len(chat_raw) + len(owner_raw) + 4 > avail: # +4 for " (@)"
            if len(owner_raw) > 8:
                o_tag = owner_raw[:6] + ".."
            else:
                o_tag = owner_raw
                
            avail_for_name = avail - len(o_tag) - 4
            if len(chat_raw) > avail_for_name:
                b_name = chat_raw[:max(1, avail_for_name - 2)] + ".."
            else:
                b_name = chat_raw
        else:
            b_name = chat_raw
            o_tag = owner_raw
            
        kb.append([InlineKeyboardButton(
            text=f"{mark} {b_name} (@{o_tag}){stats}",
            callback_data=f"aa_toggle:{owner_id}:{c['chat_id']}:{page}"
        )])

    nav_row = []
    if page > 0:
        nav_row.append(InlineKeyboardButton(text="◀️", callback_data=f"aa_page:{owner_id}:{page - 1}"))
    nav_row.append(InlineKeyboardButton(text=f"{page + 1} / {total_pages}", callback_data="ignore"))
    if page < total_pages - 1:
        nav_row.append(InlineKeyboardButton(text="▶️", callback_data=f"aa_page:{owner_id}:{page + 1}"))
    if len(nav_row) > 1:
        kb.append(nav_row)

    kb.append([InlineKeyboardButton(text=f"📊 Найти совпадения ({sel_count})", callback_data=f"aa_analyze:{owner_id}")])
    kb.append([
        InlineKeyboardButton(text="◀️ Назад", callback_data=f"ga_users:{owner_id}"),
        InlineKeyboardButton(text="♻️ Сбросить", callback_data=f"aa_reset:{owner_id}")
    ])

    markup = InlineKeyboardMarkup(inline_keyboard=kb)

    if isinstance(message_or_cb, Message):
        prompt_msg = await message_or_cb.answer(text, reply_markup=markup, parse_mode="HTML")
        await state.update_data(prompt_msg_id=prompt_msg.message_id)
    else:
        prompt_msg = await navigate(message_or_cb, text, reply_markup=markup, parse_mode="HTML")
        if prompt_msg and hasattr(prompt_msg, 'message_id'):
            await state.update_data(prompt_msg_id=prompt_msg.message_id)


@router.callback_query(F.data.startswith("aa_start:"))
async def on_aa_start(callback: CallbackQuery, state: FSMContext):
    role, owner_id = await get_admin_context(callback.from_user.id, callback.from_user.username)
    if not role:
        return await callback.answer("❌ Нет прав", show_alert=True)

    await state.update_data(selected_channels=[])
    await _show_analyzer_panel(callback, owner_id, state, page=0)
    await callback.answer()


@router.callback_query(F.data.startswith("aa_page:"))
async def on_aa_page(callback: CallbackQuery, state: FSMContext):
    parts = callback.data.split(":")
    owner_id = int(parts[1])
    page = int(parts[2])
    await _show_analyzer_panel(callback, owner_id, state, page=page)
    await callback.answer()


@router.callback_query(F.data.startswith("aa_toggle:"))
async def on_aa_toggle(callback: CallbackQuery, state: FSMContext):
    parts = callback.data.split(":")
    owner_id = int(parts[1])
    chat_id = int(parts[2])
    page = int(parts[3])

    data = await state.get_data()
    selected_channels = data.get("selected_channels", [])

    if chat_id in selected_channels:
        selected_channels.remove(chat_id)
    else:
        selected_channels.append(chat_id)

    await state.update_data(selected_channels=selected_channels)
    await _show_analyzer_panel(callback, owner_id, state, page=page)
    await callback.answer()


@router.callback_query(F.data.startswith("aa_reset:"))
async def on_aa_reset(callback: CallbackQuery, state: FSMContext):
    parts = callback.data.split(":")
    owner_id = int(parts[1])
    await state.update_data(selected_channels=[], search_query="", search_mode="name")
    await _show_analyzer_panel(callback, owner_id, state, page=0)
    await callback.answer()


@router.callback_query(F.data.startswith("aa_search:"))
async def on_aa_search(callback: CallbackQuery, state: FSMContext):
    parts = callback.data.split(":")
    owner_id = int(parts[1])
    search_mode = parts[2] if len(parts) > 2 else "name"

    await state.update_data(search_mode=search_mode)

    if search_mode == "owner":
        prompt_text = "🔍 <b>Поиск по владельцу</b>\n\n<blockquote>Введите <b>@username</b> или <b>ID</b> владельца канала.</blockquote>\n\nПример: <code>ivan</code>, <code>news</code>, <code>123456789</code>"
        await state.set_state(AudienceAnalyzerFSM.waiting_search_owner)
    else:
        prompt_text = "🔍 <b>Поиск по названию</b>\n\n<blockquote>Введите часть названия канала для поиска.</blockquote>\n\nПример: <code>shop</code>, <code>crypto</code>, <code>news</code>"
        await state.set_state(AudienceAnalyzerFSM.waiting_search_name)

    prompt_msg = await navigate(
        callback,
        prompt_text,
        parse_mode="HTML",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="✖️ Отмена", callback_data=f"aa_reset_search:{owner_id}")]
        ])
    )
    if prompt_msg and hasattr(prompt_msg, 'message_id'):
        await state.update_data(prompt_msg_id=prompt_msg.message_id)
    await callback.answer()


@router.callback_query(F.data.startswith("aa_reset_search:"))
async def on_aa_reset_search(callback: CallbackQuery, state: FSMContext):
    parts = callback.data.split(":")
    owner_id = int(parts[1])
    await state.update_data(search_query="")
    await state.set_state(None)
    await _show_analyzer_panel(callback, owner_id, state, page=0)
    await callback.answer()


@router.message(AudienceAnalyzerFSM.waiting_search_name)
@router.message(AudienceAnalyzerFSM.waiting_search_owner)
async def on_aa_search_input(message: Message, state: FSMContext):
    data = await state.get_data()
    role, owner_id = await get_admin_context(message.from_user.id, message.from_user.username)
    if not role:
        return

    search_query = message.text.strip()
    await state.update_data(search_query=search_query)

    prompt_msg_id = data.get("prompt_msg_id")
    try:
        await message.delete()
        if prompt_msg_id:
            await message.bot.delete_message(chat_id=message.chat.id, message_id=prompt_msg_id)
    except Exception:
        pass

    await state.set_state(None)
    await _show_analyzer_panel(message, owner_id, state, page=0)


@router.callback_query(F.data.startswith("aa_analyze:"))
async def on_aa_analyze(callback: CallbackQuery, state: FSMContext):
    parts = callback.data.split(":")
    owner_id = int(parts[1])

    data = await state.get_data()
    selected_channels = data.get("selected_channels", [])

    if not selected_channels:
        return await callback.answer("❌ Выберите хотя бы 1 канал", show_alert=True)

    await callback.answer("⏳ Анализирую данные...", show_alert=False)

    async with get_pool().acquire() as conn:
        users = await conn.fetch(f"""
            SELECT user_id
            FROM bot_users
            WHERE chat_id = ANY($1::bigint[]) AND is_active = true
            GROUP BY user_id
            HAVING COUNT(DISTINCT chat_id) = {len(selected_channels)}
        """, selected_channels)

        user_ids = [r['user_id'] for r in users]

        # Получаем названия выбранных каналов
        channels_info = await conn.fetch("""
            SELECT bc.chat_title
            FROM bot_chats bc
            WHERE bc.chat_id = ANY($1::bigint[])
        """, selected_channels)

    def _truncate(text: str, max_len: int = 34) -> str:
        if not text: return "Без названия"
        return text[:max_len-3] + "..." if len(text) > max_len else text

    # Строим список каналов
    channels_list = "\n".join([
        f"  • {_truncate(r['chat_title'])}"
        for r in channels_info
    ])

    text = (
        "📈 <b>Результаты анализа</b>\n\n"
        f"📑 Выбранные площадки ({len(selected_channels)}):\n"
        f"{channels_list}\n\n"
        f"🎯 <b>Найдено общих пользователей:</b> {_fmt_num(len(user_ids))} чел.\n"
    )
    if len(selected_channels) > 1:
        text += f"<i>(Они присутствуют в базах всех {len(selected_channels)} выбранных площадок одновременно)</i>"

    # Сохраняем результат для экспорта
    await state.update_data(analyzed_user_ids=user_ids)

    kb = [
        [InlineKeyboardButton(text="📥 Выгрузить список (.csv)", callback_data=f"aa_export:csv:{owner_id}")],
        [InlineKeyboardButton(text="📥 Выгрузить список (.txt)", callback_data=f"aa_export:txt:{owner_id}")],
        [InlineKeyboardButton(text="🔙 Вернуться к выбору", callback_data=f"aa_start:{owner_id}")]
    ]

    await navigate(callback, text, reply_markup=InlineKeyboardMarkup(inline_keyboard=kb), parse_mode="HTML")


@router.callback_query(F.data.startswith("aa_export:"))
async def on_aa_export(callback: CallbackQuery, state: FSMContext):
    parts = callback.data.split(":")
    ext = parts[1]
    owner_id = int(parts[2])

    data = await state.get_data()
    user_ids = data.get("analyzed_user_ids", [])

    if not user_ids:
        return await callback.answer("❌ Данные для выгрузки не найдены. Выполните анализ снова.", show_alert=True)

    await callback.answer("⏳ Подготовка файла...")

    fd, path = tempfile.mkstemp(suffix=f".{ext}")
    os.close(fd)

    try:
        async with get_pool().acquire() as conn:
            detailed_users = await conn.fetch("""
                SELECT user_id, MAX(username) as username, MAX(first_name) as first_name
                FROM bot_users
                WHERE user_id = ANY($1::bigint[])
                GROUP BY user_id
            """, user_ids)

        with open(path, "w", encoding="utf-8-sig", newline="") as f:
            if ext == "csv":
                writer = csv.writer(f)
                writer.writerow(["user_id", "username", "first_name"])
                for r in detailed_users:
                    writer.writerow([r['user_id'], r['username'] or "", r['first_name'] or ""])
            else:
                for r in detailed_users:
                    username_str = f" @{r['username']}" if r['username'] else ""
                    f.write(f"{r['user_id']}{username_str} ({r['first_name'] or 'Аноним'})\n")

        # Имя файла с датой и временем
        timestamp = datetime.now().strftime("%Y%m%d_%H%M")
        filename = f"audience_{timestamp}.{ext}"

        doc = FSInputFile(path, filename=filename)

        kb = [
            [InlineKeyboardButton(text="◀️ Назад к Результатам анализа", callback_data=f"aa_analyze:{owner_id}")]
        ]
        markup = InlineKeyboardMarkup(inline_keyboard=kb)

        try:
            await callback.message.delete()
        except Exception:
            pass

        await callback.message.answer_document(
            doc,
            caption="📁 Результат анализа пересечения аудиторий.",
            reply_markup=markup
        )

    finally:
        if os.path.exists(path):
            os.remove(path)
