import logging
from aiogram import Router, F
from aiogram.types import CallbackQuery, InlineKeyboardMarkup, InlineKeyboardButton, Message
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup

from db.pool import get_pool
from handlers.global_admin import get_admin_context
from utils.nav import navigate

logger = logging.getLogger(__name__)
router = Router()

class AudienceAnalyzerFSM(StatesGroup):
    waiting_search = State()

PAGE_SIZE = 5

async def _get_admin_channels(owner_id: int):
    async with get_pool().acquire() as conn:
        # Get all channels for all active bots of this owner
        channels = await conn.fetch("""
            SELECT bc.chat_id, bc.chat_title, COUNT(bu.user_id) as subscribers
            FROM bot_chats bc
            JOIN child_bots cb ON cb.id = bc.child_bot_id
            LEFT JOIN bot_users bu ON bu.chat_id = bc.chat_id AND bu.is_active = true
            WHERE cb.owner_id = $1 AND bc.is_active = true
            GROUP BY bc.chat_id, bc.chat_title
            ORDER BY subscribers DESC
        """, owner_id)
        return [{"chat_id": dict(c)["chat_id"], "chat_title": dict(c)["chat_title"], "subscribers": dict(c)["subscribers"]} for c in channels]

async def _show_analyzer_panel(message_or_cb, owner_id: int, state: FSMContext, page: int = 0, search_query: str = ""):
    data = await state.get_data()
    selected_channels = data.get("selected_channels", [])

    channels = await _get_admin_channels(owner_id)

    if search_query:
        channels = [c for c in channels if search_query.lower() in c['chat_title'].lower()]

    total_channels = len(channels)
    total_pages = (total_channels + PAGE_SIZE - 1) // PAGE_SIZE if total_channels > 0 else 1
    page = max(0, min(page, total_pages - 1))

    start_idx = page * PAGE_SIZE
    end_idx = start_idx + PAGE_SIZE
    page_channels = channels[start_idx:end_idx]

    text = (
        "📊 <b>Анализ баз пользователей</b>\n\n"
        "Выберите площадки для поиска общих подписчиков. Вы можете найти тех, кто подписан одновременно на все выбранные каналы.\n\n"
        f"📍 <i>Всего площадок для анализа: {total_channels}</i>\n"
        f"✅ <i>Выбрано для анализа: {len(selected_channels)}</i>"
    )
    if search_query:
        text += f"\n\n🔍 <i>Поиск:</i> <code>{search_query}</code>"

    kb = []
    kb.append([InlineKeyboardButton(text="🔍 Поиск по названию", callback_data=f"aa_search:{owner_id}")])
    
    for c in page_channels:
        mark = "✅" if c['chat_id'] in selected_channels else "❌"
        kb.append([InlineKeyboardButton(
            text=f"[{mark}] {c['chat_title']} — {c['subscribers']} пдп",
            callback_data=f"aa_toggle:{owner_id}:{c['chat_id']}:{page}"
        )])

    # Pagination
    nav_row = []
    if page > 0:
        nav_row.append(InlineKeyboardButton(text="⬅️ Назад", callback_data=f"aa_page:{owner_id}:{page-1}"))
    nav_row.append(InlineKeyboardButton(text=f"Стр. {page+1} из {total_pages}", callback_data="ignore"))
    if page < total_pages - 1:
        nav_row.append(InlineKeyboardButton(text="Вперед ➡️", callback_data=f"aa_page:{owner_id}:{page+1}"))
    kb.append(nav_row)

    kb.append([InlineKeyboardButton(text="♻️ Сбросить выбор", callback_data=f"aa_reset:{owner_id}")])
    kb.append([InlineKeyboardButton(text=f"📊 Найти совпадения ({len(selected_channels)})", callback_data=f"aa_analyze:{owner_id}")])
    kb.append([InlineKeyboardButton(text="🔙 Назад", callback_data=f"ga_users:{owner_id}")])

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
    data = await state.get_data()
    search_query = data.get("search_query", "")
    await _show_analyzer_panel(callback, owner_id, state, page=page, search_query=search_query)
    await callback.answer()

@router.callback_query(F.data.startswith("aa_toggle:"))
async def on_aa_toggle(callback: CallbackQuery, state: FSMContext):
    parts = callback.data.split(":")
    owner_id = int(parts[1])
    chat_id = int(parts[2])
    page = int(parts[3])

    data = await state.get_data()
    selected_channels = data.get("selected_channels", [])
    search_query = data.get("search_query", "")

    if chat_id in selected_channels:
        selected_channels.remove(chat_id)
    else:
        selected_channels.append(chat_id)
    
    await state.update_data(selected_channels=selected_channels)
    await _show_analyzer_panel(callback, owner_id, state, page=page, search_query=search_query)
    await callback.answer()

@router.callback_query(F.data.startswith("aa_reset:"))
async def on_aa_reset(callback: CallbackQuery, state: FSMContext):
    parts = callback.data.split(":")
    owner_id = int(parts[1])
    await state.update_data(selected_channels=[], search_query="")
    await _show_analyzer_panel(callback, owner_id, state, page=0)
    await callback.answer()

@router.callback_query(F.data.startswith("aa_search:"))
async def on_aa_search(callback: CallbackQuery, state: FSMContext):
    parts = callback.data.split(":")
    owner_id = int(parts[1])
    
    await state.set_state(AudienceAnalyzerFSM.waiting_search)
    prompt_msg = await navigate(
        callback,
        "🔍 <b>Поиск канала</b>\n\n"
        "Введите часть названия канала для поиска:",
        parse_mode="HTML",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="🔙 Отмена", callback_data=f"ga_users:{owner_id}")]
        ])
    )
    if prompt_msg and hasattr(prompt_msg, 'message_id'):
        await state.update_data(prompt_msg_id=prompt_msg.message_id)

@router.message(AudienceAnalyzerFSM.waiting_search)
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
    await _show_analyzer_panel(message, owner_id, state, page=0, search_query=search_query)

@router.callback_query(F.data.startswith("aa_analyze:"))
async def on_aa_analyze(callback: CallbackQuery, state: FSMContext):
    parts = callback.data.split(":")
    owner_id = int(parts[1])
    
    data = await state.get_data()
    selected_channels = data.get("selected_channels", [])
    
    if not selected_channels:
        return await callback.answer("❌ Выберите хотя бы 1 канал", show_alert=True)
    
    # Send a temporary loading message or answer callback
    await callback.answer("⏳ Анализирую данные...", show_alert=False)
    
    async with get_pool().acquire() as conn:
        # We need to get the overlapping users
        # For a user to be in ALL selected channels, they must appear exactly `len(selected_channels)` times
        # when we filter by those channels.
        
        selected_channels_arr = selected_channels
        
        # We find user_ids
        users = await conn.fetch(f"""
            SELECT user_id
            FROM bot_users
            WHERE chat_id = ANY($1::bigint[]) AND is_active = true
            GROUP BY user_id
            HAVING COUNT(DISTINCT chat_id) = {len(selected_channels_arr)}
        """, selected_channels_arr)
        
        user_ids = [r['user_id'] for r in users]
        
        # Get channel names for display
        channels_info = await conn.fetch("""
            SELECT chat_title FROM bot_chats WHERE chat_id = ANY($1::bigint[])
        """, selected_channels_arr)
        channel_names = [r['chat_title'] for r in channels_info]

    channels_list = "\n".join([f"- {name}" for name in channel_names])
    
    text = (
        "📈 <b>Результаты анализа</b>\n\n"
        f"📑 Выбранные каналы ({len(selected_channels)}):\n"
        f"{channels_list}\n\n"
        f"🎯 <b>Найдено общих подписчиков:</b> {len(user_ids)} чел.\n"
    )
    if len(selected_channels) > 1:
        text += f"<i>(Они состоят во всех {len(selected_channels)} выбранных каналах одновременно)</i>"
        
    # Store result for export
    await state.update_data(analyzed_user_ids=user_ids)
    
    kb = [
        [InlineKeyboardButton(text="📥 Выгрузить список (.csv)", callback_data=f"aa_export:csv:{owner_id}")],
        [InlineKeyboardButton(text="📥 Выгрузить список (.txt)", callback_data=f"aa_export:txt:{owner_id}")],
        [InlineKeyboardButton(text="🔙 Вернуться к выбору", callback_data=f"aa_start:{owner_id}")]
    ]
    
    await navigate(callback, text, reply_markup=InlineKeyboardMarkup(inline_keyboard=kb), parse_mode="HTML")

import tempfile
import csv
import os
from aiogram.types import FSInputFile

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
            # We may want to fetch additional info like usernames
            # Since bot_users might have multiple records for the same user (in different chats), we pick one.
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
                    
        doc = FSInputFile(path, filename=f"audience_intersection.{ext}")
        msg = await callback.message.answer_document(doc, caption="📁 Результат анализа пересечения аудиторий.")
        
        # Delete the document message after a timeout? Or just leave it.
    finally:
        if os.path.exists(path):
            os.remove(path)
