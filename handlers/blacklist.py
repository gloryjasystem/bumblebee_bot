"""
handlers/blacklist.py — UI управления чёрным списком.
"""
from aiogram import Router, F, Bot
from aiogram.types import CallbackQuery, Message, InlineKeyboardMarkup, InlineKeyboardButton
from aiogram.filters import StateFilter
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
import asyncio
import logging

logger = logging.getLogger(__name__)

import db.pool as db
from services.blacklist import (
    add_to_blacklist, import_file, sweep_after_import,
    get_blacklist_count, check_blacklist, kick_single_user,
)
from services.security import validate_bl_file
from config import settings
from utils.nav import navigate

router = Router()


class BlacklistFSM(StatesGroup):
    waiting_for_manual_input = State()
    waiting_for_search_input = State()


def kb_blacklist_main(chat_id: int) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="➕ Загрузить базу (TXT/CSV)", callback_data=f"bl_upload:{chat_id}")],
        [InlineKeyboardButton(text="✏️ Добавить вручную",          callback_data=f"bl_manual:{chat_id}")],
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

    await navigate(
        callback,
        f"🛡 <b>Чёрный список</b>\n\n"
        f"📊 Записей: {count:,} / {limit:,} (тариф {tariff.title()})\n\n"
        f"Чёрный список автоматически защищает все ваши площадки.",
        reply_markup=kb_blacklist_main(chat_id),
    )


# ── Ручное добавление ─────────────────────────────────────────
@router.callback_query(F.data.startswith("bl_manual:"))
async def on_bl_manual(callback: CallbackQuery, state: FSMContext, platform_user: dict | None):
    if not platform_user:
        return
    chat_id = callback.data.split(":")[1]
    await state.update_data(chat_id=chat_id)
    await state.set_state(BlacklistFSM.waiting_for_manual_input)
    await navigate(
        callback,
        "✏️ <b>Добавить в базу</b>\n\n"
        "Отправьте @username или Telegram ID.\n"
        "Можно несколько через пробел или с новой строки.\n\n"
        "Пример:\n<code>@baduser1 @baduser2\n123456789</code>",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="🚫 Отменить", callback_data=f"ch_protection:{chat_id}")]
        ]),
    )


@router.message(BlacklistFSM.waiting_for_manual_input)
async def on_bl_manual_input(message: Message, state: FSMContext, platform_user: dict | None):
    if not platform_user:
        return
    data = await state.get_data()
    chat_id = data.get("chat_id")
    owner_id = platform_user["user_id"]

    from services.security import parse_blacklist_line
    from services.blacklist import resolve_username_to_id
    lines = message.text.replace(",", "\n").split()
    added = errors = 0
    results = []
    newly_added = []  # (user_id, username) для кика

    for token in lines[:100]:  # Макс 100 за раз вручную
        parsed = parse_blacklist_line(token)
        if parsed:
            uid = parsed["user_id"]
            uname = parsed["username"]
            # Если только username — пробуем резолвить в user_id через API
            if not uid and uname:
                resolved = await resolve_username_to_id(uname)
                if resolved:
                    uid = resolved
            ok = await add_to_blacklist(owner_id, uid, uname, child_bot_id=child_bot_id)
            if ok:
                added += 1
                suffix = f" (ID: <code>{uid}</code>)" if uid and not parsed["user_id"] else ""
                results.append(f"• {token} ✅{suffix}")
                newly_added.append((uid, uname))
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
        f"Итого в базе: {total:,}\n"
        f"⚡ Запускаю зачистку из каналов...",
        parse_mode="HTML",
    )

    # Фоновый кик из всех каналов
    # Примечание: здесь child_bot_id неизвестен (маршрут через ch_protection), поэтому child_bot_id=None
    async def _kick_all():
        kicked = 0
        for uid, uname in newly_added:
            kicked += await kick_single_user(owner_id, uid, uname)
        if kicked > 0:
            try:
                await message.answer(
                    f"🚫 <b>Выкинуто из каналов: {kicked}</b>\n"
                    f"Пользователи находились в ваших площадках и были удалены.",
                    parse_mode="HTML",
                )
            except Exception:
                pass
    asyncio.create_task(_kick_all())



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
    await navigate(
        callback,
        "📂 <b>Загрузка файла ЧС</b>\n\n"
        "Отправьте файл <b>TXT</b> или <b>CSV</b> с @username или ID.\n"
        "Максимум: 20 MB, до 100,000 записей.\n\n"
        "Формат:\n<code>@spammer1\n123456789\n@baduser</code>",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="🚫 Отменить", callback_data=f"ch_protection:{chat_id}")]
        ]),
    )


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
    # Фоновая зачистка — передаём список ТОЛЬКО новых записей, чтобы не задваивать счётчик
    if stats.get("newly_added"):
        asyncio.create_task(sweep_after_import(owner_id, newly_added=stats["newly_added"]))



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
    child_bot_id = int(data.get("child_bot_id")) if data.get("child_bot_id") else None
    mode = data.get("bs_bl_mode", "add")
    owner_id = platform_user["user_id"]

    from services.security import parse_blacklist_line
    from services.blacklist import resolve_username_to_id
    lines = message.text.replace(",", "\n").split()
    added = removed = invalid = exists = 0
    results = []
    newly_added = []  # (user_id, username) для кика
    newly_removed = []

    for token in lines[:100]:
        parsed = parse_blacklist_line(token)
        if not parsed:
            invalid += 1
            continue

        if mode == "add":
            uid = parsed["user_id"]
            uname = parsed["username"]
            if not uid and uname:
                # Уровень 1: Telegram API (публичные аккаунты)
                resolved = await resolve_username_to_id(uname)
                if resolved:
                    uid = resolved

                # Уровень 2: ищем в bot_users по конкретным каналам этого child_bot
                # (работает даже для приватных аккаунтов, если они уже в канале)
                if not uid and child_bot_id:
                    row = await db.fetchrow(
                        """
                        SELECT bu.user_id FROM bot_users bu
                        JOIN bot_chats bc ON bu.chat_id = bc.chat_id AND bu.owner_id = bc.owner_id
                        WHERE bc.child_bot_id = $1
                          AND lower(bu.username) = lower($2)
                          AND bu.user_id IS NOT NULL
                        LIMIT 1
                        """,
                        child_bot_id, uname.lstrip("@"),
                    )
                    if row:
                        uid = row["user_id"]
                        logger.info(f"[BL ADD] Resolved @{uname} → {uid} via bot_users (child_bot_id={child_bot_id})")
                    else:
                        # Диагностика: показываем что реально хранится в bot_users для этого бота
                        diag = await db.fetch(
                            """
                            SELECT bu.user_id, bu.username, bu.first_name FROM bot_users bu
                            JOIN bot_chats bc ON bu.chat_id = bc.chat_id AND bu.owner_id = bc.owner_id
                            WHERE bc.child_bot_id = $1 AND bu.user_id IS NOT NULL
                            ORDER BY bu.joined_at DESC LIMIT 5
                            """,
                            child_bot_id,
                        )
                        logger.warning(
                            f"[BL ADD] @{uname} not found in bot_users for child_bot_id={child_bot_id}. "
                            f"Last 5 users: {[(r['user_id'], r['username'], r['first_name']) for r in diag]}"
                        )

            ok = await add_to_blacklist(owner_id, uid, uname, child_bot_id=child_bot_id)
            if ok:
                added += 1
                suffix = f" (ID: <code>{uid}</code>)" if uid and not parsed["user_id"] else ""
                results.append(f"• {token} ✅{suffix}")
                newly_added.append((uid, uname))
            else:
                exists += 1
                results.append(f"• {token} (уже в базе)")
        else: # del
            uid = parsed.get("user_id")
            uname = parsed.get("username")
            if uid:
                row = await db.fetchrow(
                    "DELETE FROM blacklist WHERE owner_id=$1 AND user_id=$2 AND child_bot_id=$3 RETURNING user_id, username",
                    owner_id, uid, child_bot_id,
                )
            else:
                row = await db.fetchrow(
                    "DELETE FROM blacklist WHERE owner_id=$1 AND lower(username)=lower($2) AND child_bot_id=$3 RETURNING user_id, username",
                    owner_id, uname, child_bot_id,
                )
            
            if row:
                removed += 1
                results.append(f"• {token} 🗑")
                newly_removed.append({"user_id": row["user_id"], "username": row["username"]})
            else:
                invalid += 1
                results.append(f"• {token} (нет в базе)")

    total = await db.fetchval(
        "SELECT COUNT(*) FROM blacklist WHERE owner_id=$1 AND child_bot_id=$2",
        owner_id, child_bot_id,
    ) or 0
    await state.clear()

    result_text = "\n".join(results[:10])
    if len(results) > 10:
        result_text += f"\n... и ещё {len(results)-10}"

    if mode == "add":
        await message.answer(
            f"✅ <b>Добавлено: {added}</b> | Ошибок: {invalid}\n\n"
            f"{result_text}\n\n"
            f"Итого в ЧС: {total:,}\n"
            f"⚡ Запускаю зачистку из каналов...",
            parse_mode="HTML",
            reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                [InlineKeyboardButton(text="◀️ Назад к ЧС", callback_data=f"bs_blacklist:{child_bot_id}")]
            ])
        )
        # Фоновый кик — передаём child_bot_id для per-bot счётчика
        async def _kick_bs():
            kicked = 0
            for uid, uname in newly_added:
                kicked += await kick_single_user(owner_id, uid, uname, child_bot_id=child_bot_id)
            if kicked > 0:
                try:
                    await message.answer(
                        f"🚫 <b>Выкинуто из каналов: {kicked}</b>",
                        parse_mode="HTML",
                    )
                except Exception:
                    pass
        asyncio.create_task(_kick_bs())
    else:
        await message.answer(
            f"✅ <b>Удалено: {removed}</b> | Ошибок: {invalid}\n\n"
            f"{result_text}\n\n"
            f"Итого в ЧС: {total:,}",
            reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                [InlineKeyboardButton(text="◀️ Назад к ЧС", callback_data=f"bs_blacklist:{child_bot_id}")]
            ])
        )
        if newly_removed:
            from services.blacklist import sweep_unban_records
            asyncio.create_task(sweep_unban_records(owner_id, newly_removed, child_bot_id=child_bot_id))


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
    child_bot_id = int(data.get("child_bot_id")) if data.get("child_bot_id") else None
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
        stats = await import_file(owner_id, content_bytes, doc.file_name, child_bot_id=child_bot_id)
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
        # Фоновая зачистка — передаём список ТОЛЬКО новых записей, чтобы не задваивать счётчик
        if stats.get("newly_added"):
            asyncio.create_task(sweep_after_import(owner_id, child_bot_id=child_bot_id, newly_added=stats["newly_added"]))
    else:
        # Режим удаления из ЧС
        from services.security import parse_blacklist_line
        text = content_bytes.decode("utf-8", errors="replace")
        lines = [ln.strip() for ln in text.splitlines() if ln.strip()]
        removed = invalid = 0
        newly_removed = []
        for line in lines:
            parsed = parse_blacklist_line(line)
            if not parsed:
                invalid += 1
                continue
            uid = parsed.get("user_id")
            uname = parsed.get("username")
            if uid:
                row = await db.fetchrow(
                    "DELETE FROM blacklist WHERE owner_id=$1 AND user_id=$2 AND child_bot_id=$3 RETURNING user_id, username",
                    owner_id, uid, child_bot_id,
                )
            else:
                row = await db.fetchrow(
                    "DELETE FROM blacklist WHERE owner_id=$1 AND lower(username)=lower($2) AND child_bot_id=$3 RETURNING user_id, username",
                    owner_id, uname, child_bot_id,
                )
            
            if row:
                removed += 1
                newly_removed.append({"user_id": row["user_id"], "username": row["username"]})

        total = await db.fetchval(
            "SELECT COUNT(*) FROM blacklist WHERE owner_id=$1 AND child_bot_id=$2", owner_id, child_bot_id,
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
        if newly_removed:
            from services.blacklist import sweep_unban_records
            asyncio.create_task(sweep_unban_records(owner_id, newly_removed, child_bot_id=child_bot_id))
