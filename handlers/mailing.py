"""
handlers/mailing.py — Рассылка: создание, настройки черновика, URL-кнопки, запуск.
"""
import asyncio
import logging
from datetime import datetime, timezone
from aiogram import Router, F, Bot
from aiogram.types import (
    Message, CallbackQuery, InlineKeyboardMarkup, InlineKeyboardButton
)
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup

import db.pool as db
from services import mailing as mailing_svc
from services.security import sanitize

logger = logging.getLogger(__name__)
router = Router()


class MailingFSM(StatesGroup):
    waiting_for_text     = State()
    waiting_for_schedule = State()
    waiting_for_buttons  = State()


class MassMailingFSM(StatesGroup):
    selecting_bots   = State()   # выбор ботов
    waiting_for_text = State()   # ввод текста рассылки


# ══════════════════════════════════════════════════════════════
# Массовая рассылка (menu:mailing)
# ══════════════════════════════════════════════════════════════

async def _show_mass_mailing(callback: CallbackQuery, state: FSMContext,
                             platform_user: dict):
    """Показывает экран выбора ботов для массовой рассылки."""
    owner_id = platform_user["user_id"]
    data = await state.get_data()
    selected: list[int] = data.get("mass_selected", [])

    # Все боты пользователя (включая admin-боты из команды)
    bots = await db.fetch(
        """
        SELECT cb.id, cb.bot_username FROM child_bots cb
        WHERE cb.owner_id = $1
        UNION
        SELECT cb.id, cb.bot_username FROM child_bots cb
        JOIN team_members tm ON tm.child_bot_id = cb.id AND tm.user_id = $1 AND tm.is_active = true
        WHERE cb.owner_id != $1
        ORDER BY id
        """,
        owner_id,
    )

    # Считаем пользователей по выбранным ботам
    total_users = 0
    if selected:
        total_users = await db.fetchval(
            """SELECT COUNT(DISTINCT bu.user_id) FROM bot_users bu
               JOIN bot_chats bc ON bu.chat_id=bc.chat_id AND bu.owner_id=bc.owner_id
               WHERE bc.child_bot_id = ANY($1::int[])
               AND bu.is_active=true AND bu.bot_activated=true""",
            selected,
        ) or 0

    sel_count = len(selected)
    buttons = []
    for b in bots:
        is_sel = b["id"] in selected
        icon = "🔵" if is_sel else "⚪"
        buttons.append([InlineKeyboardButton(
            text=f"{icon} @{b['bot_username']}",
            callback_data=f"ml_mass_toggle:{b['id']}",
        )])

    buttons.append([InlineKeyboardButton(
        text="🚀 Начать рассылку",
        callback_data="ml_mass_start",
    )])
    buttons.append([InlineKeyboardButton(
        text="📅 Запланированные",
        callback_data="ml_mass_scheduled",
    )])
    buttons.append([InlineKeyboardButton(
        text="◀️ Назад",
        callback_data="menu:main",
    )])

    await callback.message.edit_text(
        "<blockquote>"
        "📣 Здесь вы можете запустить или запланировать рассылку "
        "одновременно на несколько ботов."
        "</blockquote>\n\n"
        f"🔒 Выбрано ботов: {sel_count}\n"
        f"👥 Пользователей: {total_users:,}\n\n"
        "Выберите действие ⬇️",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=buttons),
    )
    await callback.answer()


@router.callback_query(F.data == "menu:mailing")
async def on_menu_mailing(callback: CallbackQuery, state: FSMContext,
                          platform_user: dict | None):
    if not platform_user:
        await callback.answer("Выполните /start", show_alert=True)
        return
    tariff = platform_user["tariff"]
    if tariff == "free":
        await callback.answer("Рассылка доступна с тарифа Старт.", show_alert=True)
        return
    # Сохраняем пустой выбор при первом входе (не сбрасываем если уже были)
    data = await state.get_data()
    if "mass_selected" not in data:
        await state.update_data(mass_selected=[])
    await state.set_state(MassMailingFSM.selecting_bots)
    await _show_mass_mailing(callback, state, platform_user)


@router.callback_query(F.data.startswith("ml_mass_toggle:"))
async def on_ml_mass_toggle(callback: CallbackQuery, state: FSMContext,
                            platform_user: dict | None):
    if not platform_user:
        return
    bot_id = int(callback.data.split(":")[1])
    data = await state.get_data()
    selected: list[int] = data.get("mass_selected", [])

    if bot_id in selected:
        selected.remove(bot_id)
    else:
        selected.append(bot_id)

    await state.update_data(mass_selected=selected)
    await _show_mass_mailing(callback, state, platform_user)


@router.callback_query(F.data == "ml_mass_start")
async def on_ml_mass_start(callback: CallbackQuery, state: FSMContext,
                           platform_user: dict | None):
    if not platform_user:
        return
    data = await state.get_data()
    selected: list[int] = data.get("mass_selected", [])

    if len(selected) < 2:
        await callback.answer("⊕ Выберите минимум двух ботов", show_alert=True)
        return

    # Считаем суммарных получателей
    total_users = await db.fetchval(
        """SELECT COUNT(DISTINCT bu.user_id) FROM bot_users bu
           JOIN bot_chats bc ON bu.chat_id=bc.chat_id AND bu.owner_id=bc.owner_id
           WHERE bc.child_bot_id = ANY($1::int[])
           AND bu.is_active=true AND bu.bot_activated=true""",
        selected,
    ) or 0

    await state.set_state(MassMailingFSM.waiting_for_text)
    await callback.message.edit_text(
        "📨 <b>Массовая рассылка</b>\n\n"
        "Отправьте сообщение для рассылки.\n\n"
        "<b>Переменные:</b>\n"
        "├ Имя: <code>{name}</code>\n"
        "├ ФИО: <code>{allname}</code>\n"
        "├ Юзер: <code>{username}</code>\n"
        "└ Дата: <code>{day}</code>\n\n"
        "ⓘ Можно прикрепить медиа.\n\n"
        f"🔒 Выбрано ботов: {len(selected)}\n"
        f"👥 Получателей: <b>{total_users:,}</b>",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="◀️ Назад", callback_data="menu:mailing")],
        ]),
    )
    await callback.answer()


@router.message(MassMailingFSM.waiting_for_text)
async def on_mass_mailing_text(message: Message, state: FSMContext):
    from services.security import sanitize
    data = await state.get_data()
    selected: list[int] = data.get("mass_selected", [])
    owner_id = data.get("owner_id") or message.from_user.id

    text = ""
    media_file_id = None
    media_type = None

    if message.text:
        text = sanitize(message.text, max_len=4096)
    elif message.caption:
        text = sanitize(message.caption, max_len=1024)

    if message.photo:
        media_file_id = message.photo[-1].file_id
        media_type = "photo"
    elif message.video:
        media_file_id = message.video.file_id
        media_type = "video"
    elif message.document:
        media_file_id = message.document.file_id
        media_type = "document"

    if not text and not media_file_id:
        await message.answer("⚠️ Отправьте текст или медиа.")
        return

    await state.clear()

    # Создаём черновики для каждого выбранного бота
    mailing_ids = []
    for bot_id in selected:
        mid = await db.fetchval(
            """INSERT INTO mailings
               (owner_id, child_bot_id, chat_id, text, media_file_id, media_type,
                notify_users, protect_content, pin_message, delete_after_send,
                disable_preview, url_buttons_raw, button_color, media_below)
               VALUES ($1,$2,NULL,$3,$4,$5, true,false,false,false, false,NULL,'blue',true)
               RETURNING id""",
            owner_id, bot_id, text, media_file_id, media_type,
        )
        mailing_ids.append(mid)

    if not mailing_ids:
        await message.answer("❌ Не удалось создать рассылку.")
        return

    # Показываем настройки первого черновика (остальные создаются идентично)
    m = await db.fetchrow("SELECT * FROM mailings WHERE id=$1", mailing_ids[0])
    bots_note = f"\n📣 Рассылка будет отправлена по <b>{len(selected)} ботам</b>"

    # ── Эхо: предпросмотр сообщения
    if media_file_id:
        send_fn = {
            "photo": message.answer_photo,
            "video": message.answer_video,
            "document": message.answer_document,
        }.get(media_type, message.answer_photo)
        await send_fn(media_file_id, caption=text[:1000] or None, parse_mode="HTML")
    elif text:
        await message.answer(text[:1200], parse_mode="HTML")

    # ── Меню управления
    await message.answer(
        _draft_settings_text(dict(m)) + bots_note,
        parse_mode="HTML",
        reply_markup=_kb_draft(dict(m)),
    )


@router.callback_query(F.data == "ml_mass_scheduled")
async def on_ml_mass_scheduled(callback: CallbackQuery, platform_user: dict | None):
    if not platform_user:
        return
    owner_id = platform_user["user_id"]
    rows = await db.fetch(
        """SELECT m.id, m.text, m.scheduled_at, cb.bot_username
           FROM mailings m
           JOIN child_bots cb ON cb.id = m.child_bot_id
           WHERE m.owner_id=$1 AND m.child_bot_id IS NOT NULL
             AND m.status IN ('pending','scheduled')
           ORDER BY m.scheduled_at ASC LIMIT 15""",
        owner_id,
    )
    if not rows:
        await callback.message.edit_text(
            "📅 <b>Запланированные рассылки</b>\n\nНет запланированных рассылок.",
            reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                [InlineKeyboardButton(text="◀️ Назад", callback_data="menu:mailing")],
            ]),
        )
        await callback.answer()
        return

    buttons = []
    for r in rows:
        dt = r["scheduled_at"].strftime("%d.%m %H:%M") if r.get("scheduled_at") else "—"
        bot_name = f"@{r['bot_username']}"
        preview = (r["text"] or "")[:20]
        buttons.append([InlineKeyboardButton(
            text=f"📅 {dt} [{bot_name}] {preview}…",
            callback_data=f"mailing_view:{r['id']}",
        )])
    buttons.append([InlineKeyboardButton(text="◀️ Назад", callback_data="menu:mailing")])

    await callback.message.edit_text(
        "📅 <b>Запланированные рассылки</b>",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=buttons),
    )
    await callback.answer()


# ══════════════════════════════════════════════════════════════
# Вспомогательные функции
# ══════════════════════════════════════════════════════════════

def _yn(value: bool) -> str:
    return "да" if value else "нет"

def _resolve_vars(text: str, user: dict | None = None, chat_title: str = "") -> str:
    """Подставляет переменные в текст рассылки."""
    if not text:
        return text
    now = datetime.now(timezone.utc)
    fname = (user or {}).get("first_name", "")
    lname = (user or {}).get("last_name", "")
    username = (user or {}).get("username", "")
    text = text.replace("{name}", fname)
    text = text.replace("{allname}", f"{fname} {lname}".strip())
    text = text.replace("{username}", f"@{username}" if username else fname)
    text = text.replace("{chat}", chat_title)
    text = text.replace("{day}", now.strftime("%d.%m.%Y"))
    return text


def _kb_draft(m: dict) -> InlineKeyboardMarkup:
    """Клавиатура настроек черновика (Экран 4)."""
    mid = m["id"]
    _below       = bool(m.get("media_below", False))
    media_icon   = f"📎 Медиа: {'⬇' if _below else '⬆'}"
    preview_icon = "👁 Превью: да" if not m.get("disable_preview") else "👁 Превью: нет"
    notify_icon  = f"🔔 Уведомить: {_yn(m.get('notify_users', True))}"
    protect_icon = f"🔒 Защитить: {_yn(m.get('protect_content', False))}"
    pin_icon     = f"📌 Закрепить: {_yn(m.get('pin_message', False))}"
    delete_icon  = f"🗑 Удалить: {_yn(m.get('delete_after_send', False))}"

    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="✏️ URL-кнопки", callback_data=f"ml_url_buttons:{mid}")],
        [
            InlineKeyboardButton(text=media_icon,   callback_data=f"ml_toggle:{mid}:media"),
            InlineKeyboardButton(text=preview_icon, callback_data=f"ml_toggle:{mid}:preview"),
        ],
        [
            InlineKeyboardButton(text=notify_icon,  callback_data=f"ml_toggle:{mid}:notify"),
            InlineKeyboardButton(text=protect_icon, callback_data=f"ml_toggle:{mid}:protect"),
        ],
        [
            InlineKeyboardButton(text=pin_icon,    callback_data=f"ml_toggle:{mid}:pin"),
            InlineKeyboardButton(text=delete_icon, callback_data=f"ml_toggle:{mid}:delete"),
        ],
        [
            InlineKeyboardButton(text="🗓 Запланировать", callback_data=f"mailing_schedule:{m['chat_id']}:{mid}"),
            InlineKeyboardButton(text="➡ Запустить",      callback_data=f"mailing_run:{m['chat_id']}:{mid}"),
        ],
        # Кнопка «Назад»: если chat_id=None — рассылка на уровне бота → возвращаем на bs_mailing
        [InlineKeyboardButton(
            text="◀️ Назад",
            callback_data=(f"bs_mailing:{m['child_bot_id']}" if not m.get('chat_id') else f"ch_mailing:{m['chat_id']}"),
        )],
    ])


def _draft_settings_text(m: dict) -> str:
    """Текст блока настроек под превью."""
    scheduled = m.get("scheduled_at")
    dt_str = scheduled.strftime("%d.%m.%Y %H:%M") if scheduled else datetime.now(timezone.utc).strftime("%d.%m.%Y %H:%M")
    return (
        f"\n\n📅 <b>Дата рассылки:</b> {dt_str}\n"
        f"🗑 <b>Удалить после:</b> {_yn(m.get('delete_after_send', False))}\n"
        f"📌 <b>Закрепить:</b> {_yn(m.get('pin_message', False))}"
    )


# Кэш echo-сообщений черновика: mailing_id -> (echo_msg_id, tg_chat_id)
_draft_echo_ids: dict[int, tuple[int, int]] = {}


async def _show_draft(callback: CallbackQuery, m: dict):
    """Показывает Экран 4: эхо сообщения сверху + меню управления снизу."""
    mid         = m["id"]
    text        = m.get("text") or ""
    media       = m.get("media_file_id")
    media_type  = m.get("media_type")
    media_below = bool(m.get("media_below", False))
    tg_chat_id  = callback.message.chat.id

    # Удаляем предыдущее эхо-сообщение (если есть) и текущее меню
    prev_echo = _draft_echo_ids.pop(mid, None)
    if prev_echo:
        try:
            await callback.bot.delete_message(prev_echo[1], prev_echo[0])
        except Exception:
            pass
    try:
        await callback.message.delete()
    except Exception:
        pass

    # ── Эхо: точный предпросмотр того, что получит пользователь ──────
    _send_fn_map = {
        "photo":    callback.message.answer_photo,
        "video":    callback.message.answer_video,
        "document": callback.message.answer_document,
    }
    sent_echo = None

    if media and not media_below:
        # ⬆ — текст сверху, фото/видео снизу
        send_fn = _send_fn_map.get(media_type, callback.message.answer_photo)
        sent_echo = await send_fn(
            media,
            caption=text or None,
            parse_mode="HTML",
            show_caption_above_media=True,
        )
    elif media:
        # ⬇ — стандарт: фото/видео сверху, текст капшоном снизу
        send_fn = _send_fn_map.get(media_type, callback.message.answer_photo)
        sent_echo = await send_fn(media, caption=text or None, parse_mode="HTML")
    else:
        if text:
            sent_echo = await callback.message.answer(text, parse_mode="HTML")

    # Сохраняем echo message_id для будущего удаления
    if sent_echo:
        _draft_echo_ids[mid] = (sent_echo.message_id, tg_chat_id)

    # ── Меню управления (настройки + кнопки) ───────────
    await callback.message.answer(
        _draft_settings_text(m),
        parse_mode="HTML",
        reply_markup=_kb_draft(m),
    )
    await callback.answer()



# ══════════════════════════════════════════════════════════════
# Экран 2: меню рассылки для канала (ch_mailing:{chat_id})
# ══════════════════════════════════════════════════════════════

@router.callback_query(F.data.startswith("ch_mailing:"))
async def on_ch_mailing(callback: CallbackQuery, platform_user: dict | None):
    if not platform_user:
        return
    tariff = platform_user["tariff"]
    if tariff == "free":
        await callback.answer("Рассылка доступна с тарифа Старт.", show_alert=True)
        return
    chat_id = int(callback.data.split(":")[1])
    ch = await db.fetchrow(
        "SELECT chat_title FROM bot_chats WHERE owner_id=$1 AND chat_id=$2::bigint",
        platform_user["user_id"], chat_id,
    )
    title = ch["chat_title"] if ch else "Площадка"

    await callback.message.edit_text(
        f"📨 <b>Рассылка</b>\n\nВыберите действие ⬇️",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="➕ Создать рассылку", callback_data=f"mailing_start:{chat_id}")],
            [InlineKeyboardButton(text="📅 Запланированные",  callback_data=f"mailing_scheduled:{chat_id}")],
            [InlineKeyboardButton(text="◀️ Назад",             callback_data=f"channel_by_chat:{chat_id}")],
        ]),
    )
    await callback.answer()


# ══════════════════════════════════════════════════════════════
# Bot-level mailing: из главного экрана (без выбора площадки)
# ══════════════════════════════════════════════════════════════

@router.callback_query(F.data.startswith("mailing_bot_start:"))
async def on_mailing_bot_start(callback: CallbackQuery, state: FSMContext,
                                platform_user: dict | None):
    """Создать рассылку по всем пользователям бота."""
    if not platform_user:
        return
    child_bot_id = int(callback.data.split(":")[1])
    owner_id = platform_user["user_id"]

    # Количество уникальных получателей по всем площадкам бота
    # is_active убран: считаем всех у кого есть user_id (IS NOT NULL)
    count = await db.fetchval(
        "SELECT COUNT(DISTINCT bu.user_id) FROM bot_users bu "
        "JOIN bot_chats bc ON bu.chat_id=bc.chat_id AND bu.owner_id=bc.owner_id "
        "WHERE bc.child_bot_id=$1 AND bc.owner_id=$2 AND bu.user_id IS NOT NULL",
        child_bot_id, owner_id,
    ) or 0

    await state.update_data(child_bot_id=child_bot_id, chat_id=None, owner_id=owner_id)
    await state.set_state(MailingFSM.waiting_for_text)

    await callback.message.edit_text(
        f"📨 Отправьте сообщение для рассылки.\n\n"
        f"<b>Переменные:</b>\n"
        f"├ Имя: <code>{{name}}</code>\n"
        f"├ ФИО: <code>{{allname}}</code>\n"
        f"├ Юзер: <code>{{username}}</code>\n"
        f"├ Площадка: <code>{{chat}}</code>\n"
        f"└ Текущая дата: <code>{{day}}</code>\n\n"
        f"ⓘ Можно прикрепить медиа.\n\n"
        f"👥 Получателей (все площадки): <b>{count:,}</b>",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="◀️ Отмена",
                                   callback_data=f"bs_mailing:{child_bot_id}")],
        ]),
    )
    await callback.answer()


@router.callback_query(F.data.startswith("mailing_bot_scheduled:"))
async def on_mailing_bot_scheduled(callback: CallbackQuery, platform_user: dict | None):
    """Запланированные рассылки на уровне бота."""
    if not platform_user:
        return
    child_bot_id = int(callback.data.split(":")[1])
    owner_id = platform_user["user_id"]

    rows = await db.fetch(
        """SELECT m.id, m.text, m.scheduled_at, bc.chat_title
           FROM mailings m
           JOIN bot_chats bc ON m.chat_id=bc.chat_id AND m.owner_id=bc.owner_id
           WHERE bc.child_bot_id=$1 AND m.owner_id=$2
             AND m.status IN ('pending','scheduled')
           ORDER BY m.scheduled_at ASC LIMIT 10""",
        child_bot_id, owner_id,
    )

    if not rows:
        await callback.message.edit_text(
            "📅 <b>Запланированные рассылки</b>\n\nНет запланированных рассылок.",
            reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                [InlineKeyboardButton(text="◀️ Назад",
                                       callback_data=f"bs_mailing:{child_bot_id}")],
            ]),
        )
        await callback.answer()
        return

    buttons = []
    for r in rows:
        dt = r["scheduled_at"].strftime("%d.%m %H:%M") if r.get("scheduled_at") else "—"
        title = (r.get("chat_title") or "")[:10]
        preview = (r["text"] or "")[:20]
        buttons.append([InlineKeyboardButton(
            text=f"📅 {dt} [{title}] {preview}…",
            callback_data=f"mailing_view:{r['id']}",
        )])
    buttons.append([InlineKeyboardButton(text="◀️ Назад",
                                          callback_data=f"bs_mailing:{child_bot_id}")])

    await callback.message.edit_text(
        "📅 <b>Запланированные рассылки</b>",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=buttons),
    )
    await callback.answer()



# ══════════════════════════════════════════════════════════════
# Запланированные рассылки
# ══════════════════════════════════════════════════════════════

@router.callback_query(F.data.startswith("mailing_scheduled:"))
async def on_mailing_scheduled(callback: CallbackQuery, platform_user: dict | None):
    if not platform_user:
        return
    chat_id = int(callback.data.split(":")[1])
    owner_id = platform_user["user_id"]

    rows = await db.fetch(
        """SELECT id, text, scheduled_at FROM mailings
           WHERE owner_id=$1 AND chat_id=$2::bigint AND status IN ('pending','scheduled')
           ORDER BY scheduled_at ASC LIMIT 10""",
        owner_id, chat_id,
    )
    if not rows:
        await callback.message.edit_text(
            "📅 <b>Запланированные рассылки</b>\n\nНет запланированных рассылок.",
            reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                [InlineKeyboardButton(text="◀️ Назад", callback_data=f"ch_mailing:{chat_id}")],
            ]),
        )
        await callback.answer()
        return

    text = "📅 <b>Запланированные рассылки</b>\n\n"
    buttons = []
    for r in rows:
        dt = r["scheduled_at"].strftime("%d.%m %H:%M") if r["scheduled_at"] else "Сейчас"
        preview = (r["text"] or "")[:35]
        text += f"• [{dt}] {preview}…\n"
        buttons.append([InlineKeyboardButton(
            text=f"🗓 {dt} — {preview[:20]}…",
            callback_data=f"ml_view_draft:{r['id']}:{chat_id}",
        )])
    buttons.append([InlineKeyboardButton(text="◀️ Назад", callback_data=f"ch_mailing:{chat_id}")])

    await callback.message.edit_text(
        text, parse_mode="HTML",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=buttons),
    )
    await callback.answer()


# ══════════════════════════════════════════════════════════════
# Экран 3: создание рассылки (ввод текста + переменные)
# ══════════════════════════════════════════════════════════════

@router.callback_query(F.data.startswith("mailing_start:"))
async def on_mailing_start(callback: CallbackQuery, state: FSMContext, platform_user: dict | None):
    if not platform_user:
        return
    chat_id = int(callback.data.split(":")[1])
    ch = await db.fetchrow(
        "SELECT * FROM bot_chats WHERE owner_id=$1 AND chat_id=$2",
        platform_user["user_id"], chat_id,
    )
    if not ch:
        return

    await state.update_data(chat_id=chat_id, owner_id=platform_user["user_id"])
    await state.set_state(MailingFSM.waiting_for_text)

    count = await db.fetchval(
        "SELECT COUNT(*) FROM bot_users WHERE owner_id=$1 AND chat_id=$2 "
        "AND user_id IS NOT NULL",
        platform_user["user_id"], chat_id,
    ) or 0

    await callback.message.edit_text(
        f"📨 Отправьте сообщение для рассылки.\n\n"
        f"<b>Переменные:</b>\n"
        f"├ Имя: <code>{{name}}</code>\n"
        f"├ ФИО: <code>{{allname}}</code>\n"
        f"├ Юзер: <code>{{username}}</code>\n"
        f"├ Площадка: <code>{{chat}}</code>\n"
        f"└ Текущая дата: <code>{{day}}</code>\n\n"
        f"ⓘ Можно прикрепить медиа.\n\n"
        f"👥 Получателей: <b>{count:,}</b>",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="◀️ Назад", callback_data=f"ch_mailing:{chat_id}")],
        ]),
    )
    await callback.answer()


# ══════════════════════════════════════════════════════════════
# Получение сообщения → создание черновика → Экран 4
# ══════════════════════════════════════════════════════════════

@router.message(MailingFSM.waiting_for_text)
async def on_mailing_text(message: Message, state: FSMContext):
    data = await state.get_data()
    chat_id      = data.get("chat_id")
    owner_id     = data.get("owner_id")
    child_bot_id = data.get("child_bot_id")  # set for bot-level mailing

    text = ""
    media_file_id = None
    media_type = None

    if message.text:
        text = sanitize(message.text, max_len=4096)
    elif message.caption:
        text = sanitize(message.caption, max_len=1024)

    if message.photo:
        media_file_id = message.photo[-1].file_id
        media_type = "photo"
    elif message.video:
        media_file_id = message.video.file_id
        media_type = "video"
    elif message.document:
        media_file_id = message.document.file_id
        media_type = "document"

    if not text and not media_file_id:
        await message.answer("⚠️ Пожалуйста, отправьте текст или медиа.")
        return

    # Создаём черновик
    if child_bot_id and not chat_id:
        # Bot-level: рассылка по всем пользователям бота (chat_id=NULL)
        mailing_id = await db.fetchval(
            """INSERT INTO mailings
               (owner_id, child_bot_id, chat_id, text, media_file_id, media_type,
                notify_users, protect_content, pin_message, delete_after_send,
                disable_preview, url_buttons_raw, button_color, media_below)
               VALUES ($1,$2,NULL,$3,$4,$5, true,false,false,false, false,NULL,'blue',true)
               RETURNING id""",
            owner_id, child_bot_id, text, media_file_id, media_type,
        )
    else:
        # Channel-level: рассылка по пользователям одного канала
        mailing_id = await db.fetchval(
            """INSERT INTO mailings
               (owner_id, chat_id, text, media_file_id, media_type,
                notify_users, protect_content, pin_message, delete_after_send,
                disable_preview, url_buttons_raw, button_color, media_below)
               VALUES ($1,$2,$3,$4,$5, true,false,false,false, false,NULL,'blue',true)
               RETURNING id""",
            owner_id, chat_id, text, media_file_id, media_type,
        )
    await state.clear()

    # Получаем только что созданный черновик
    m = await db.fetchrow("SELECT * FROM mailings WHERE id=$1", mailing_id)

    # ── Эхо: предпросмотр сообщения (без кнопок)
    if media_file_id:
        send_fn = {
            "photo": message.answer_photo,
            "video": message.answer_video,
            "document": message.answer_document,
        }.get(media_type, message.answer_photo)
        await send_fn(media_file_id, caption=text[:1000] or None, parse_mode="HTML")
    elif text:
        await message.answer(text[:1200], parse_mode="HTML")

    # ── Меню управления
    await message.answer(
        _draft_settings_text(dict(m)),
        parse_mode="HTML",
        reply_markup=_kb_draft(dict(m)),
    )



# ══════════════════════════════════════════════════════════════
# Открыть существующий черновик
# ══════════════════════════════════════════════════════════════

@router.callback_query(F.data.startswith("ml_view_draft:"))
async def on_ml_view_draft(callback: CallbackQuery, platform_user: dict | None):
    if not platform_user:
        return
    parts = callback.data.split(":")
    mailing_id = int(parts[1])
    m = await db.fetchrow("SELECT * FROM mailings WHERE id=$1 AND owner_id=$2",
                          mailing_id, platform_user["user_id"])
    if not m:
        await callback.answer("Черновик не найден", show_alert=True)
        return
    await _show_draft(callback, dict(m))


# ══════════════════════════════════════════════════════════════
# Тогглеры настроек (Экран 4)
# ══════════════════════════════════════════════════════════════

_TOGGLE_MAP = {
    "notify":  ("notify_users",       True),
    "protect": ("protect_content",    False),
    "pin":     ("pin_message",        False),
    "delete":  ("delete_after_send",  False),
    "preview": ("disable_preview",    False),
}


@router.callback_query(F.data.startswith("ml_toggle:"))
async def on_ml_toggle(callback: CallbackQuery, platform_user: dict | None):
    if not platform_user:
        return
    parts   = callback.data.split(":")
    mid     = int(parts[1])
    setting = parts[2]

    if setting not in _TOGGLE_MAP and setting != "media":
        return

    owner_id = platform_user["user_id"]
    m = await db.fetchrow("SELECT * FROM mailings WHERE id=$1 AND owner_id=$2", mid, owner_id)
    if not m:
        await callback.answer("Черновик не найден", show_alert=True)
        return

    if setting == "media":
        new_val = not bool(m.get("media_below", False))
        await db.execute(
            "UPDATE mailings SET media_below=$1 WHERE id=$2 AND owner_id=$3",
            new_val, mid, owner_id,
        )
        m = await db.fetchrow("SELECT * FROM mailings WHERE id=$1", mid)
        await callback.answer()
        await _show_draft(callback, dict(m))
        return

    col, default = _TOGGLE_MAP[setting]
    new_val = not (m[col] if m[col] is not None else default)
    await db.execute(
        f"UPDATE mailings SET {col}=$1 WHERE id=$2 AND owner_id=$3",
        new_val, mid, owner_id,
    )
    m = await db.fetchrow("SELECT * FROM mailings WHERE id=$1", mid)
    await callback.answer()
    await _show_draft(callback, dict(m))


# ══════════════════════════════════════════════════════════════
# Экран 5: URL-кнопки
# ══════════════════════════════════════════════════════════════

_URL_BUTTONS_HELP = (
    "🔗 <b>URL-кнопки</b>\n\n"
    "<b>Одна кнопка в ряду:</b>\n"
    "<blockquote>Кнопка 1 — ссылка\n"
    "Кнопка 2 — ссылка</blockquote>\n\n"
    "<b>Несколько кнопок в ряду:</b>\n"
    "<blockquote>Кнопка 1 — ссылка | Кнопка 2 — ссылка\n"
    "Кнопка 3 — ссылка | Кнопка 4 — ссылка</blockquote>\n\n"
    "<b>*** Другие виды кнопок</b>\n\n"
    "<b>WebApp кнопки:</b>\n"
    "<blockquote>Кнопка 1 — ссылка (webapp)</blockquote>\n\n"
    "<b>Цвет кнопок:</b> выберите ниже 👇\n\n"
    "ⓘ Нажмите на пример, чтобы скопировать."
)

_EXAMPLES = [
    ("Кнопка 1 — https://example.com",                         "one_btn"),
    ("Кнопка 1 — https://t.me | Кнопка 2 — https://t.me",     "two_btn"),
    ("Кнопка 1 — https://example.com (webapp)",                "webapp_btn"),
]


@router.callback_query(F.data.startswith("ml_url_buttons:"))
async def on_ml_url_buttons(callback: CallbackQuery, state: FSMContext, platform_user: dict | None):
    if not platform_user:
        return
    mid = int(callback.data.split(":")[1])
    m = await db.fetchrow("SELECT * FROM mailings WHERE id=$1 AND owner_id=$2",
                          mid, platform_user["user_id"])
    if not m:
        await callback.answer("Черновик не найден", show_alert=True)
        return

    color = m.get("button_color") or "blue"
    color_buttons = [
        InlineKeyboardButton(text="🟦" + (" ✅" if color == "blue"  else ""), callback_data=f"ml_color:{mid}:blue"),
        InlineKeyboardButton(text="🟩" + (" ✅" if color == "green" else ""), callback_data=f"ml_color:{mid}:green"),
        InlineKeyboardButton(text="🟥" + (" ✅" if color == "red"   else ""), callback_data=f"ml_color:{mid}:red"),
    ]

    existing = m.get("url_buttons_raw") or ""
    now_text = f"\n\n✅ <b>Текущие кнопки:</b>\n<code>{existing}</code>" if existing else ""

    buttons = [
        [InlineKeyboardButton(text="📋 Одна: Кнопка — ссылка",              callback_data="ml_example:0")],
        [InlineKeyboardButton(text="📋 Две в ряду: Кнопка | Кнопка",        callback_data="ml_example:1")],
        [InlineKeyboardButton(text="📋 WebApp: Кнопка — ссылка (webapp)",   callback_data="ml_example:2")],
        color_buttons,
        [InlineKeyboardButton(text="✏️ Ввести кнопки", callback_data=f"ml_input_buttons:{mid}")],
        [InlineKeyboardButton(text="🗑 Очистить кнопки", callback_data=f"ml_clear_buttons:{mid}")],
        [InlineKeyboardButton(text="◀️ Назад", callback_data=f"ml_view_draft:{mid}:")],
    ]

    await callback.message.edit_text(
        _URL_BUTTONS_HELP + now_text,
        parse_mode="HTML",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=buttons),
    )
    await callback.answer()


@router.callback_query(F.data.startswith("ml_example:"))
async def on_ml_example(callback: CallbackQuery):
    idx = int(callback.data.split(":")[1])
    text, _ = _EXAMPLES[idx]
    await callback.answer(text, show_alert=True)


@router.callback_query(F.data.startswith("ml_color:"))
async def on_ml_color(callback: CallbackQuery, platform_user: dict | None):
    if not platform_user:
        return
    parts = callback.data.split(":")
    mid, color = int(parts[1]), parts[2]
    await db.execute(
        "UPDATE mailings SET button_color=$1 WHERE id=$2 AND owner_id=$3",
        color, mid, platform_user["user_id"],
    )
    await callback.answer(f"Цвет: {color}")
    # Перерендер
    callback.data = f"ml_url_buttons:{mid}"
    await on_ml_url_buttons(callback, None, platform_user)  # state=None — не нужен здесь


@router.callback_query(F.data.startswith("ml_input_buttons:"))
async def on_ml_input_buttons(callback: CallbackQuery, state: FSMContext, platform_user: dict | None):
    if not platform_user:
        return
    mid = int(callback.data.split(":")[1])
    await state.set_state(MailingFSM.waiting_for_buttons)
    await state.update_data(mailing_id=mid, owner_id=platform_user["user_id"])
    await callback.message.edit_text(
        "🔗 Отправьте кнопки, которые будут добавлены к сообщению.\n\n"
        "<b>Формат одной кнопки:</b> <code>Текст — ссылка</code>\n"
        "<b>Несколько в ряду:</b> <code>Текст — ссылка | Текст 2 — ссылка</code>\n"
        "<b>WebApp:</b> <code>Текст — ссылка (webapp)</code>\n\n"
        "Каждая строка — отдельный ряд кнопок.",
        parse_mode="HTML",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="◀️ Назад", callback_data=f"ml_url_buttons:{mid}")],
        ]),
    )
    await callback.answer()


@router.message(MailingFSM.waiting_for_buttons)
async def on_mailing_buttons_input(message: Message, state: FSMContext):
    data = await state.get_data()
    mid = data.get("mailing_id")
    owner_id = data.get("owner_id")
    raw = sanitize(message.text or "", max_len=2000)
    await db.execute(
        "UPDATE mailings SET url_buttons_raw=$1 WHERE id=$2 AND owner_id=$3",
        raw, mid, owner_id,
    )
    await state.clear()
    await message.answer(
        "✅ Кнопки сохранены.",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="◀️ К настройкам", callback_data=f"ml_url_buttons:{mid}")],
        ]),
    )


@router.callback_query(F.data.startswith("ml_clear_buttons:"))
async def on_ml_clear_buttons(callback: CallbackQuery, platform_user: dict | None):
    if not platform_user:
        return
    mid = int(callback.data.split(":")[1])
    await db.execute(
        "UPDATE mailings SET url_buttons_raw=NULL WHERE id=$1 AND owner_id=$2",
        mid, platform_user["user_id"],
    )
    await callback.answer("🗑 Кнопки очищены")
    callback.data = f"ml_url_buttons:{mid}"
    await on_ml_url_buttons(callback, None, platform_user)


# ══════════════════════════════════════════════════════════════
# Планирование
# ══════════════════════════════════════════════════════════════

@router.callback_query(F.data.startswith("mailing_schedule:"))
async def on_mailing_schedule(callback: CallbackQuery, state: FSMContext, platform_user: dict | None):
    if not platform_user:
        return
    parts   = callback.data.split(":")
    chat_id = parts[1]
    mid     = parts[2] if len(parts) > 2 else None

    await state.set_state(MailingFSM.waiting_for_schedule)
    await state.update_data(chat_id=chat_id, mailing_id=mid)

    back_cb = f"ml_view_draft:{mid}:" if mid else f"ch_mailing:{chat_id}"
    await callback.message.edit_text(
        "⏱ <b>Планирование рассылки</b>\n\n"
        "Укажите дату и время в формате:\n"
        "<code>28.02 18:00</code>  или  <code>28.02.2026 18:00</code>\n\n"
        "⚠️ Время по UTC+0. Добавьте часы вашего часового пояса.",
        parse_mode="HTML",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="◀️ Отменить", callback_data=back_cb)],
        ]),
    )
    await callback.answer()


@router.message(MailingFSM.waiting_for_schedule)
async def on_schedule_input(message: Message, state: FSMContext):
    data = await state.get_data()
    chat_id    = data.get("chat_id")
    mailing_id = data.get("mailing_id")
    raw = (message.text or "").strip()

    dt = None
    for fmt in ("%d.%m %H:%M", "%d.%m.%Y %H:%M"):
        try:
            dt = datetime.strptime(raw, fmt)
            if fmt == "%d.%m %H:%M":
                dt = dt.replace(year=datetime.now().year)
            break
        except ValueError:
            continue

    if not dt:
        await message.answer(
            "❌ Неверный формат. Используйте: <code>28.02 18:00</code>",
            parse_mode="HTML",
        )
        return

    await state.clear()

    if mailing_id:
        await db.execute(
            "UPDATE mailings SET scheduled_at=$1, status='scheduled' WHERE id=$2",
            dt, int(mailing_id),
        )
        await message.answer(
            f"✅ Рассылка запланирована на <b>{dt.strftime('%d.%m.%Y %H:%M')}</b>",
            parse_mode="HTML",
            reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                [InlineKeyboardButton(text="◀️ К рассылке", callback_data=f"ml_view_draft:{mailing_id}:")],
            ]),
        )
    else:
        await message.answer(f"✅ Запланировано: {dt.strftime('%d.%m.%Y %H:%M')}")


# ══════════════════════════════════════════════════════════════
# Запуск рассылки
# ══════════════════════════════════════════════════════════════

def kb_mailing_control(mailing_id: int, paused: bool, speed: str) -> InlineKeyboardMarkup:
    pause_text = "▶ Возобновить" if paused else "⏸ Пауза"
    speed_map  = {"low": "🟢 Низкая (~10/сек)", "medium": "🟡 Средняя (~25/сек)", "high": "🔴 Высокая (~50/сек)"}
    next_speed = {"low": "medium", "medium": "high", "high": "low"}
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text=pause_text,    callback_data=f"ml_pause:{mailing_id}")],
        [InlineKeyboardButton(text="⏹ Остановить", callback_data=f"ml_cancel:{mailing_id}")],
        [InlineKeyboardButton(
            text=f"⚡ Скорость: {speed_map.get(speed, '🟢 Низкая (~10/сек)')}",
            callback_data=f"ml_speed:{mailing_id}:{next_speed.get(speed, 'medium')}",
        )],
    ])


def _mailing_progress_text(mailing_id: int, sent: int, total: int,
                            errors: int, status: str, bot_username: str = "",
                            started_at=None) -> str:
    """Строит текст экрана прогресса рассылки (как на скриншоте 2)."""
    # Прогресс-бар 10 символов
    pct = (sent / total * 100) if total > 0 else 0.0
    filled = int(pct / 10)
    bar = "▓" * filled + "░" * (10 - filled)

    status_map = {
        "running":   "🟢 В процессе",
        "done":      "✅ Завершено",
        "cancelled": "⏹ Остановлено",
    }
    status_str = status_map.get(status, "🟢 В процессе")

    received = sent - errors
    bot_line = f"📣 Рассылка: @{bot_username}\n" if bot_username else "📣 Рассылка\n"

    dt_str = ""
    if started_at:
        try:
            dt_str = f"\n📅 Дата запуска: {started_at.strftime('%d.%m.%Y %H:%M')}"
        except Exception:
            pass

    return (
        f"{bot_line}"
        f"<code>{bar}</code> {pct:.1f}%\n\n"
        f"{'ϙ' if status == 'running' else '📊'} Статус: {status_str}\n"
        f"↗️ Отправлено: <b>{sent}</b> из <b>{total}</b>\n"
        f"✅ Получили: <b>{received}</b>\n"
        f"🚫 Блокировали: <b>{errors}</b>\n\n"
        f"⚡ Скорость: Минимальная (~10/сек)"
        f"{dt_str}"
    )


@router.callback_query(F.data.startswith("mailing_run:"))
async def on_mailing_run(callback: CallbackQuery, bot: Bot, platform_user: dict | None):
    if not platform_user:
        return
    parts = callback.data.split(":")
    # Безопасный парсинг: chat_id может быть "None" для bot-level рассылки
    chat_id_raw = parts[1] if len(parts) > 1 else "None"
    chat_id = int(chat_id_raw) if chat_id_raw not in ("None", "", "0") else None
    mid     = int(parts[2]) if len(parts) > 2 and parts[2] else None

    owner_id = platform_user["user_id"]

    if mid:
        mailing = await db.fetchrow(
            "SELECT m.*, cb.bot_username FROM mailings m "
            "LEFT JOIN child_bots cb ON cb.id = m.child_bot_id "
            "WHERE m.id=$1 AND m.owner_id=$2 AND m.status='draft'",
            mid, owner_id,
        )
    elif chat_id:
        mailing = await db.fetchrow(
            "SELECT m.*, cb.bot_username FROM mailings m "
            "LEFT JOIN child_bots cb ON cb.id = m.child_bot_id "
            "WHERE m.owner_id=$1 AND m.chat_id=$2 AND m.status='draft' "
            "ORDER BY m.created_at DESC LIMIT 1",
            owner_id, chat_id,
        )
    else:
        await callback.answer("Черновик не найден", show_alert=True)
        return

    if not mailing:
        await callback.answer("Черновик не найден", show_alert=True)
        return

    mailing_id   = mailing["id"]
    bot_username = mailing.get("bot_username") or ""
    await db.execute("UPDATE mailings SET status='pending' WHERE id=$1", mailing_id)

    # Начальный экран «идём!»
    progress_text = _mailing_progress_text(mailing_id, 0, 0, 0, "running", bot_username)
    try:
        progress_msg = await callback.message.edit_text(
            progress_text,
            parse_mode="HTML",
            reply_markup=kb_mailing_control(mailing_id, False, "low"),
        )
    except Exception:
        progress_msg = None

    await callback.answer("▶ Рассылка запущена")

    # Захватываем chat_id / message_id для обновлений
    upd_chat_id = callback.message.chat.id
    upd_msg_id  = callback.message.message_id

    async def progress_callback(ml_id: int, sent: int, total: int,
                                errors: int, status: str):
        """Обновляем сообщение с прогрессом."""
        m_row = await db.fetchrow("SELECT started_at FROM mailings WHERE id=$1", ml_id)
        started_at = m_row["started_at"] if m_row else None
        text = _mailing_progress_text(ml_id, sent, total, errors, status, bot_username, started_at)
        kb = kb_mailing_control(ml_id, False, "low") if status == "running" else None
        try:
            await bot.edit_message_text(
                text,
                chat_id=upd_chat_id,
                message_id=upd_msg_id,
                parse_mode="HTML",
                reply_markup=kb,
            )
        except Exception as e:
            logger.debug(f"Progress update failed: {e}")

    asyncio.create_task(mailing_svc.run_mailing(mailing_id, bot, progress_callback))




# ══════════════════════════════════════════════════════════════
# Управление активной рассылкой
# ══════════════════════════════════════════════════════════════

@router.callback_query(F.data.startswith("ml_pause:"))
async def on_ml_pause(callback: CallbackQuery):
    mailing_id = int(callback.data.split(":")[1])
    status = mailing_svc.get_status(mailing_id)
    if not status:
        await callback.answer("Рассылка завершена")
        return
    if status.get("paused"):
        mailing_svc.resume_mailing(mailing_id)
        await callback.answer("▶ Возобновлена")
    else:
        mailing_svc.pause_mailing(mailing_id)
        await callback.answer("⏸ Приостановлена")


@router.callback_query(F.data.startswith("ml_cancel:"))
async def on_ml_cancel(callback: CallbackQuery):
    mailing_id = int(callback.data.split(":")[1])
    mailing_svc.cancel_mailing(mailing_id)
    await callback.message.edit_text("⏹ Рассылка остановлена.")
    await callback.answer("Рассылка остановлена")


@router.callback_query(F.data.startswith("ml_speed:"))
async def on_ml_speed(callback: CallbackQuery):
    parts      = callback.data.split(":")
    mailing_id = int(parts[1])
    speed      = parts[2]
    mailing_svc.set_speed(mailing_id, speed)
    await callback.answer(f"Скорость: {speed}")
