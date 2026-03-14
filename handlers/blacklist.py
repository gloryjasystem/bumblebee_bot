"""
handlers/blacklist.py — UI управления чёрным списком.
"""
from aiogram import Router, F, Bot
from aiogram.types import CallbackQuery, Message, InlineKeyboardMarkup, InlineKeyboardButton
from aiogram.filters import StateFilter
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
        [InlineKeyboardButton(text="◀️ Назад",                      callback_data=f"channel_by_chat:{chat_id}")],
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


@router.message(F.document, StateFilter(None))
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
        "SELECT COUNT(*) FROM bot_users WHERE owner_id=$1 AND chat_id=$2::bigint AND is_active=true",
        owner_id, chat_id,
    )
    violators = await db.fetch(
        """
        SELECT bu.user_id, bu.username FROM bot_users bu
        INNER JOIN blacklist bl ON bl.owner_id=bu.owner_id
          AND ((bl.user_id IS NOT NULL AND bl.user_id=bu.user_id)
               OR (bl.username IS NOT NULL AND bl.username=bu.username))
        WHERE bu.owner_id=$1 AND bu.chat_id=$2::bigint AND bu.is_active=true
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


# ── Найти в базе ─────────────────────────────────────────────
@router.callback_query(F.data.startswith("bl_search:"))
async def on_bl_search(callback: CallbackQuery, state: FSMContext, platform_user: dict | None):
    if not platform_user:
        return
    chat_id = callback.data.split(":")[1]
    await state.set_state(BlacklistFSM.waiting_for_search_input)
    await state.update_data(chat_id=chat_id, owner_id=platform_user["user_id"])
    await callback.message.edit_text(
        "🔎 <b>Поиск в чёрном списке</b>\n\n"
        "Введите @username или Telegram ID для поиска:",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="🚫 Отменить", callback_data=f"ch_protection:{chat_id}")]
        ]),
    )
    await callback.answer()


@router.message(BlacklistFSM.waiting_for_search_input)
async def on_bl_search_input(message: Message, state: FSMContext, platform_user: dict | None):
    if not platform_user:
        return
    query = (message.text or "").strip().lstrip("@")
    owner_id = platform_user["user_id"]

    # Поиск по username или user_id
    try:
        uid = int(query)
        row = await db.fetchrow(
            "SELECT user_id, username, added_at FROM blacklist WHERE owner_id=$1 AND user_id=$2",
            owner_id, uid,
        )
    except ValueError:
        row = await db.fetchrow(
            "SELECT user_id, username, added_at FROM blacklist WHERE owner_id=$1 AND username ILIKE $2",
            owner_id, query,
        )

    await state.clear()
    if row:
        added = row["added_at"].strftime("%d.%m.%Y %H:%M") if row["added_at"] else "—"
        await message.answer(
            f"✅ <b>Найден в базе</b>\n\n"
            f"👤 @{row['username'] or '—'}\n"
            f"🆔 ID: <code>{row['user_id'] or '—'}</code>\n"
            f"📅 Добавлен: {added}",
            reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                [InlineKeyboardButton(
                    text="🗑 Удалить из ЧС",
                    callback_data=f"bl_remove:{row['user_id'] or row['username']}",
                )],
            ]),
        )
    else:
        await message.answer(f"❌ <code>@{query}</code> не найден в чёрном списке.")


# ── Экспорт базы ─────────────────────────────────────────────
@router.callback_query(F.data.startswith("bl_export:"))
async def on_bl_export(callback: CallbackQuery, platform_user: dict | None):
    if not platform_user:
        return
    owner_id = platform_user["user_id"]
    rows = await db.fetch(
        "SELECT user_id, username FROM blacklist WHERE owner_id=$1 ORDER BY added_at DESC LIMIT 10000",
        owner_id,
    )
    if not rows:
        await callback.answer("База чёрного списка пуста.", show_alert=True)
        return

    lines = []
    for r in rows:
        if r["username"]:
            lines.append(f"@{r['username']}")
        elif r["user_id"]:
            lines.append(str(r["user_id"]))

    content = "\n".join(lines).encode("utf-8")
    from aiogram.types import BufferedInputFile
    file = BufferedInputFile(content, filename="blacklist_export.txt")

    await callback.message.answer_document(
        file,
        caption=f"📤 Экспорт чёрного списка: {len(lines):,} записей",
    )
    await callback.answer()


# ── Очистить базу ─────────────────────────────────────────────
@router.callback_query(F.data.startswith("bl_clear_confirm:"))
async def on_bl_clear_confirm(callback: CallbackQuery, platform_user: dict | None):
    if not platform_user:
        return
    chat_id = callback.data.split(":")[1]
    count = await get_blacklist_count(platform_user["user_id"])
    await callback.message.edit_text(
        f"⚠️ <b>Очистить чёрный список?</b>\n\n"
        f"Будет удалено {count:,} записей. Действие необратимо.",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="✅ Да, очистить", callback_data=f"bl_clear_do:{chat_id}")],
            [InlineKeyboardButton(text="🚫 Отмена",       callback_data=f"ch_protection:{chat_id}")],
        ]),
    )
    await callback.answer()


@router.callback_query(F.data.startswith("bl_clear_do:"))
async def on_bl_clear_do(callback: CallbackQuery, platform_user: dict | None):
    if not platform_user:
        return
    chat_id = callback.data.split(":")[1]
    owner_id = platform_user["user_id"]
    deleted = await db.fetchval(
        "WITH d AS (DELETE FROM blacklist WHERE owner_id=$1 RETURNING 1) SELECT COUNT(*) FROM d",
        owner_id,
    )
    await callback.message.edit_text(
        f"✅ Чёрный список очищен. Удалено {deleted:,} записей.",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="◀️ Назад", callback_data=f"ch_protection:{chat_id}")],
        ]),
    )
    await callback.answer()


# ── Забанить всех нарушителей ────────────────────────────────
@router.callback_query(F.data.startswith("bl_ban_all:"))
async def on_bl_ban_all(callback: CallbackQuery, platform_user: dict | None):
    if not platform_user:
        return
    chat_id_str = callback.data.split(":")[1]
    chat_id = int(chat_id_str)
    owner_id = platform_user["user_id"]

    from handlers.channels import _show_channel_detail
    # Получаем токен дочернего бота
    bot_row = await db.fetchrow(
        """SELECT cb.token_encrypted, bc.id as bc_id
           FROM child_bots cb
           JOIN bot_chats bc ON bc.child_bot_id = cb.id
           WHERE bc.owner_id=$1 AND bc.chat_id=$2::bigint AND bc.is_active=true""",
        owner_id, chat_id,
    )
    if not bot_row:
        await callback.answer("Бот не найден.", show_alert=True)
        return

    from services.security import decrypt_token
    from aiogram import Bot as AioBot

    token = decrypt_token(bot_row["token_encrypted"])
    child_bot = AioBot(token=token)

    # Получаем нарушителей
    violators = await db.fetch(
        """SELECT bu.user_id FROM bot_users bu
           INNER JOIN blacklist bl ON bl.owner_id=bu.owner_id
             AND ((bl.user_id IS NOT NULL AND bl.user_id=bu.user_id)
                  OR (bl.username IS NOT NULL AND bl.username=bu.username))
           WHERE bu.owner_id=$1 AND bu.chat_id=$2::bigint AND bu.is_active=true""",
        owner_id, chat_id,
    )

    banned = 0
    for v in violators:
        try:
            await child_bot.ban_chat_member(chat_id, v["user_id"])
            banned += 1
        except Exception:
            pass

    await child_bot.session.close()
    await callback.message.edit_text(
        f"✅ Забанено: {banned} из {len(violators)} нарушителей.",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="◀️ Назад", callback_data=f"ch_protection:{chat_id_str}")],
        ]),
    )
    await callback.answer()



# ══════════════════════════════════════════════════════════════
# ЧС бот-уровень: обработка текста (ручное добавление)
# ══════════════════════════════════════════════════════════════

@router.message(F.text, StateFilter(
    "SettingsFSM:bs_bl_waiting_add_file",
    "SettingsFSM:bs_bl_waiting_del_file",
))
async def on_bs_bl_text(message: Message, state: FSMContext, platform_user: dict | None):
    """Обрабатывает текстовый ввод при добавлении/удалении в ЧС на бот-уровне."""
    if not platform_user:
        return

    from handlers.channel_settings import SettingsFSM
    current_state = await state.get_state()
    if current_state not in (
        SettingsFSM.bs_bl_waiting_add_file,
        SettingsFSM.bs_bl_waiting_del_file,
    ):
        return

    data = await state.get_data()
    child_bot_id = data.get("child_bot_id")
    mode = data.get("bs_bl_mode", "add")
    owner_id = platform_user["user_id"]

    from services.security import parse_blacklist_line
    lines = message.text.replace(",", "\n").split()
    added = removed = invalid = exists = 0
    results = []

    for token in lines[:100]:
        parsed = parse_blacklist_line(token)
        if not parsed:
            invalid += 1
            continue

        if mode == "add":
            ok = await add_to_blacklist(owner_id, parsed["user_id"], parsed["username"])
            if ok:
                added += 1
                results.append(f"• {token} ✅")
            else:
                exists += 1
                results.append(f"• {token} (уже в базе)")
        else: # del
            uid = parsed.get("user_id")
            uname = parsed.get("username")
            if uid:
                res = await db.execute(
                    "DELETE FROM blacklist WHERE owner_id=$1 AND user_id=$2",
                    owner_id, uid,
                )
            else:
                res = await db.execute(
                    "DELETE FROM blacklist WHERE owner_id=$1 AND lower(username)=lower($2)",
                    owner_id, uname,
                )
            if res and res.endswith("1"):
                removed += 1
                results.append(f"• {token} 🗑")
            else:
                invalid += 1
                results.append(f"• {token} (нет в базе)")

    total = await db.fetchval("SELECT COUNT(*) FROM blacklist WHERE owner_id=$1", owner_id) or 0
    await state.clear()

    result_text = "\n".join(results[:10])
    if len(results) > 10:
        result_text += f"\n... и ещё {len(results)-10}"

    if mode == "add":
        await message.answer(
            f"✅ <b>Добавлено: {added}</b> | Ошибок: {invalid}\n\n"
            f"{result_text}\n\n"
            f"Итого в ЧС: {total:,}",
            reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                [InlineKeyboardButton(text="◀️ Назад к ЧС", callback_data=f"bs_blacklist:{child_bot_id}")]
            ])
        )
    else:
        await message.answer(
            f"✅ <b>Удалено: {removed}</b> | Ошибок: {invalid}\n\n"
            f"{result_text}\n\n"
            f"Итого в ЧС: {total:,}",
            reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                [InlineKeyboardButton(text="◀️ Назад к ЧС", callback_data=f"bs_blacklist:{child_bot_id}")]
            ])
        )


# ══════════════════════════════════════════════════════════════
# ЧС бот-уровень: обработка загружаемых файлов (add / del)
# ══════════════════════════════════════════════════════════════

@router.message(F.document, StateFilter(
    "SettingsFSM:bs_bl_waiting_add_file",
    "SettingsFSM:bs_bl_waiting_del_file",
))
async def on_bs_bl_file(message: Message, bot: Bot, state: FSMContext,
                        platform_user: dict | None):
    """Обрабатывает TXT/CSV файлы в состояниях bs_bl_waiting_add_file / bs_bl_waiting_del_file."""
    if not platform_user:
        return

    current_state = await state.get_state()
    from handlers.channel_settings import SettingsFSM
    if current_state not in (
        SettingsFSM.bs_bl_waiting_add_file,
        SettingsFSM.bs_bl_waiting_del_file,
    ):
        return  # не наше состояние → другой обработчик

    doc = message.document
    if not doc.file_name.lower().endswith((".txt", ".csv")):
        await message.answer("❌ Поддерживаются только файлы TXT и CSV.")
        return

    data = await state.get_data()
    child_bot_id = data.get("child_bot_id")
    mode = data.get("bs_bl_mode", "add")  # "add" | "del"
    owner_id = platform_user["user_id"]

    file_obj = await bot.get_file(doc.file_id)
    content_io = await bot.download_file(file_obj.file_path)
    content_bytes = content_io.read()

    ok, error = validate_bl_file(content_bytes, doc.file_name)
    if not ok:
        await message.answer(f"❌ {error}")
        return

    wait_msg = await message.answer("⏳ Обрабатываю файл...")

    if mode == "add":
        stats = await import_file(owner_id, content_bytes, doc.file_name)
        await wait_msg.delete()
        await state.clear()
        await message.answer(
            "✅ <b>Файл обработан</b>\n\n"
            f"➕ Добавлено: {stats['added']:,}\n"
            f"⚠️ Неверный формат: {stats['invalid']:,}\n"
            f"📊 Итого в базе: {stats['total']:,}",
            parse_mode="HTML",
            reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                [InlineKeyboardButton(
                    text="◀️ Назад к ЧС",
                    callback_data=f"bs_blacklist:{child_bot_id}",
                )],
            ]),
        )
    else:
        # Режим удаления из ЧС
        from services.security import parse_blacklist_line
        text = content_bytes.decode("utf-8", errors="replace")
        lines = [ln.strip() for ln in text.splitlines() if ln.strip()]
        removed = invalid = 0
        for line in lines:
            parsed = parse_blacklist_line(line)
            if not parsed:
                invalid += 1
                continue
            uid = parsed.get("user_id")
            uname = parsed.get("username")
            if uid:
                res = await db.execute(
                    "DELETE FROM blacklist WHERE owner_id=$1 AND user_id=$2",
                    owner_id, uid,
                )
            else:
                res = await db.execute(
                    "DELETE FROM blacklist WHERE owner_id=$1 AND lower(username)=lower($2)",
                    owner_id, uname,
                )
            # asyncpg returns "DELETE N"
            if res and res.endswith("1"):
                removed += 1

        total = await db.fetchval(
            "SELECT COUNT(*) FROM blacklist WHERE owner_id=$1", owner_id,
        ) or 0

        await wait_msg.delete()
        await state.clear()
        await message.answer(
            "✅ <b>Файл обработан</b>\n\n"
            f"➖ Удалено из ЧС: {removed:,}\n"
            f"⚠️ Неверный формат: {invalid:,}\n"
            f"📊 Итого в базе: {total:,}",
            parse_mode="HTML",
            reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                [InlineKeyboardButton(
                    text="◀️ Назад к ЧС",
                    callback_data=f"bs_blacklist:{child_bot_id}",
                )],
            ]),
        )
