"""
handlers/blacklist.py — UI управления чёрным списком.
"""
from aiogram import Router, F, Bot
from aiogram.types import CallbackQuery, Message, InlineKeyboardMarkup, InlineKeyboardButton
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
import asyncio

import db.pool as db
from services.blacklist import (
    add_to_blacklist, import_file, sweep_after_import,
    get_blacklist_count, check_blacklist,
)
from services.security import validate_bl_file
from config import settings

router = Router()


class BlacklistFSM(StatesGroup):
    waiting_for_manual_input = State()
    waiting_for_search_input = State()


def kb_blacklist_main(chat_id: int) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="➕ Загрузить базу (TXT/CSV)", callback_data=f"bl_upload:{chat_id}")],
        [InlineKeyboardButton(text="✏️ Добавить вручную",          callback_data=f"bl_manual:{chat_id}")],
        [InlineKeyboardButton(text="🔍 Найти нарушителей",          callback_data=f"bl_sweep:{chat_id}")],
        [InlineKeyboardButton(text="🔎 Найти в базе",               callback_data=f"bl_search:{chat_id}")],
        [InlineKeyboardButton(text="📤 Экспорт базы",               callback_data=f"bl_export:{chat_id}")],
        [InlineKeyboardButton(text="🗑 Очистить базу",              callback_data=f"bl_clear_confirm:{chat_id}")],
        [InlineKeyboardButton(text="◀️ Назад",                      callback_data=f"channel:{chat_id}")],
    ])


# ── Главный экран ЧС ─────────────────────────────────────────
@router.callback_query(F.data.startswith("ch_protection:"))
async def on_protection_menu(callback: CallbackQuery, platform_user: dict | None):
    if not platform_user:
        return
    chat_id = int(callback.data.split(":")[1])
    count = await get_blacklist_count(platform_user["user_id"])
    tariff = platform_user["tariff"]
    limit = settings.blacklist_limits.get(tariff, 0)

    await callback.message.edit_text(
        f"🛡 <b>Чёрный список</b>\n\n"
        f"📊 Записей: {count:,} / {limit:,} (тариф {tariff.title()})\n\n"
        f"Чёрный список автоматически защищает все ваши площадки.",
        reply_markup=kb_blacklist_main(chat_id),
    )
    await callback.answer()


# ── Ручное добавление ─────────────────────────────────────────
@router.callback_query(F.data.startswith("bl_manual:"))
async def on_bl_manual(callback: CallbackQuery, state: FSMContext, platform_user: dict | None):
    if not platform_user:
        return
    chat_id = callback.data.split(":")[1]
    await state.update_data(chat_id=chat_id)
    await state.set_state(BlacklistFSM.waiting_for_manual_input)
    await callback.message.edit_text(
        "✏️ <b>Добавить в базу</b>\n\n"
        "Отправьте @username или Telegram ID.\n"
        "Можно несколько через пробел или с новой строки.\n\n"
        "Пример:\n<code>@baduser1 @baduser2\n123456789</code>",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="🚫 Отменить", callback_data=f"ch_protection:{chat_id}")]
        ]),
    )
    await callback.answer()


@router.message(BlacklistFSM.waiting_for_manual_input)
async def on_bl_manual_input(message: Message, state: FSMContext, platform_user: dict | None):
    if not platform_user:
        return
    data = await state.get_data()
    chat_id = data.get("chat_id")
    owner_id = platform_user["user_id"]

    from services.security import parse_blacklist_line
    lines = message.text.replace(",", "\n").split()
    added = errors = 0
    results = []

    for token in lines[:100]:  # Макс 100 за раз вручную
        parsed = parse_blacklist_line(token)
        if parsed:
            ok = await add_to_blacklist(owner_id, parsed["user_id"], parsed["username"])
            if ok:
                added += 1
                results.append(f"• {token} ✅")
            else:
                results.append(f"• {token} (уже был)")
        else:
            errors += 1

    total = await get_blacklist_count(owner_id)
    await state.clear()

    result_text = "\n".join(results[:10])
    if len(results) > 10:
        result_text += f"\n... и ещё {len(results)-10}"

    await message.answer(
        f"✅ <b>Добавлено: {added}</b> | ❌ Ошибок: {errors}\n\n"
        f"{result_text}\n\n"
        f"Итого в базе: {total:,}"
    )


# ── Загрузка файла ────────────────────────────────────────────
@router.callback_query(F.data.startswith("bl_upload:"))
async def on_bl_upload(callback: CallbackQuery, platform_user: dict | None):
    if not platform_user:
        return
    tariff = platform_user["tariff"]
    if tariff == "free":
        await callback.answer("Загрузка файлов доступна с тарифа Старт.", show_alert=True)
        return
    chat_id = callback.data.split(":")[1]
    await callback.message.edit_text(
        "📂 <b>Загрузка файла ЧС</b>\n\n"
        "Отправьте файл <b>TXT</b> или <b>CSV</b> с @username или ID.\n"
        "Максимум: 20 MB, до 100,000 записей.\n\n"
        "Формат:\n<code>@spammer1\n123456789\n@baduser</code>",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="🚫 Отменить", callback_data=f"ch_protection:{chat_id}")]
        ]),
    )
    await callback.answer()


@router.message(F.document)
async def on_bl_file_upload(message: Message, bot: Bot, platform_user: dict | None):
    if not platform_user:
        return

    doc = message.document
    if not doc.file_name.lower().endswith((".txt", ".csv")):
        return  # Не файл ЧС

    owner_id = platform_user["user_id"]
    file = await bot.get_file(doc.file_id)
    content = await bot.download_file(file.file_path)
    content_bytes = content.read()

    ok, error = validate_bl_file(content_bytes, doc.file_name)
    if not ok:
        await message.answer(f"❌ {error}")
        return

    wait_msg = await message.answer("⏳ Импортирую файл...")
    stats = await import_file(owner_id, content_bytes, doc.file_name)
    await wait_msg.delete()

    await message.answer(
        f"✅ <b>Файл обработан</b>\n\n"
        f"✅ Добавлено: {stats['added']:,}\n"
        f"⚠️ Неверный формат: {stats['invalid']:,}\n"
        f"📊 Итого в базе: {stats['total']:,}\n\n"
        f"⚙️ Запускаю авто-зачистку в фоне..."
    )
    # Фоновая зачистка с rate limiting
    asyncio.create_task(sweep_after_import(owner_id, bot))


# ── Поиск нарушителей ────────────────────────────────────────
@router.callback_query(F.data.startswith("bl_sweep:"))
async def on_bl_sweep(callback: CallbackQuery, platform_user: dict | None):
    if not platform_user:
        return
    chat_id = int(callback.data.split(":")[1])
    owner_id = platform_user["user_id"]

    count = await db.fetchval(
        "SELECT COUNT(*) FROM bot_users WHERE owner_id=$1 AND chat_id=$2 AND is_active=true",
        owner_id, chat_id,
    )
    violators = await db.fetch(
        """
        SELECT bu.user_id, bu.username FROM bot_users bu
        INNER JOIN blacklist bl ON bl.owner_id=bu.owner_id
          AND ((bl.user_id IS NOT NULL AND bl.user_id=bu.user_id)
               OR (bl.username IS NOT NULL AND bl.username=bu.username))
        WHERE bu.owner_id=$1 AND bu.chat_id=$2 AND bu.is_active=true
        """,
        owner_id, chat_id,
    )

    n = len(violators)
    if n == 0:
        await callback.answer(f"✅ Проверено {count} — нарушителей нет", show_alert=True)
        return

    preview = "\n".join(
        f"• @{v['username'] or v['user_id']}" for v in violators[:5]
    )
    if n > 5:
        preview += f"\n... и ещё {n-5}"

    await callback.message.edit_text(
        f"🔍 <b>Найдено нарушителей: {n}</b>\n"
        f"Проверено в базе: {count}\n\n{preview}",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text=f"🚫 Забанить всех ({n})", callback_data=f"bl_ban_all:{chat_id}")],
            [InlineKeyboardButton(text="◀️ Назад", callback_data=f"ch_protection:{chat_id}")],
        ]),
    )
    await callback.answer()
