"""
handlers/mailing.py — Рассылка: создание, настройки черновика, URL-кнопки, запуск.
"""
import asyncio
import logging
import re
from datetime import datetime, timezone, timedelta
from aiogram import Router, F, Bot
from aiogram.types import (
    Message, CallbackQuery, InlineKeyboardMarkup, InlineKeyboardButton,
    LinkPreviewOptions
)
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup

import db.pool as db
from services import mailing as mailing_svc
from services.security import sanitize
from utils.nav import navigate

logger = logging.getLogger(__name__)
router = Router()


class MailingFSM(StatesGroup):
    waiting_for_text      = State()
    waiting_for_schedule  = State()
    waiting_for_buttons   = State()
    waiting_for_edit_text = State()  # редактирование запланированной рассылки


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

    tariff = platform_user.get("tariff", "free")
    from config import TARIFFS
    t_info = TARIFFS.get(tariff, TARIFFS["free"])
    max_bots = t_info["max_bots"]

    # Активные боты владельца (в рамках лимита) + admin-боты из команды
    bots = await db.fetch(
        """
        WITH RankedBots AS (
            SELECT cb.id, cb.bot_username,
                   ROW_NUMBER() OVER(PARTITION BY cb.owner_id ORDER BY cb.id ASC) as rn
            FROM child_bots cb
            WHERE cb.owner_id = $1
        )
        SELECT id, bot_username FROM RankedBots
        WHERE rn <= $2
        UNION
        SELECT cb.id, cb.bot_username FROM child_bots cb
        JOIN team_members tm ON tm.child_bot_id = cb.id AND tm.user_id = $1 AND tm.is_active = true
        WHERE cb.owner_id != $1
        ORDER BY id
        """,
        owner_id, max_bots,
    )

    # Убираем из выбранных те боты, которые были заморожены (если они там были)
    valid_bot_ids = [b["id"] for b in bots]
    selected = [sid for sid in selected if sid in valid_bot_ids]
    if len(selected) != len(data.get("mass_selected", [])):
        await state.update_data(mass_selected=selected)

    # Считаем пользователей по выбранным ботам
    total_users = 0
    if selected:
        total_users = await db.fetchval(
            """SELECT COUNT(DISTINCT bu.user_id) FROM bot_users bu
               LEFT JOIN bot_chats bc ON bu.chat_id=bc.chat_id AND bu.owner_id=bc.owner_id
               WHERE (bc.child_bot_id = ANY($1::int[]) OR bu.chat_id=bu.user_id)
               AND bu.owner_id=$2 AND bu.user_id IS NOT NULL""",
            selected, owner_id
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

    await navigate(
        callback,
        "<blockquote>"
        "📣 Здесь вы можете запустить или запланировать рассылку "
        "одновременно на несколько ботов."
        "</blockquote>\n\n"
        f"🔒 Выбрано ботов: {sel_count}\n"
        f"👥 Пользователей: {total_users:,}\n\n"
        "Выберите действие ⬇️",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=buttons),
    )


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

    # Считаем суммарных получателей (PM + выбранные площадки)
    total_users = await db.fetchval(
        """SELECT COUNT(DISTINCT bu.user_id) FROM bot_users bu
           LEFT JOIN bot_chats bc ON bu.chat_id=bc.chat_id AND bu.owner_id=bc.owner_id
           WHERE (bc.child_bot_id = ANY($1::int[]) OR bu.chat_id=bu.user_id)
           AND bu.owner_id=$2 AND bu.user_id IS NOT NULL""",
        selected, platform_user["user_id"]
    ) or 0

    await state.set_state(MassMailingFSM.waiting_for_text)
    prompt_msg = await navigate(
        callback,
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
    if prompt_msg and hasattr(prompt_msg, 'message_id'):
        await state.update_data(prompt_msg_id=prompt_msg.message_id)


@router.message(MassMailingFSM.waiting_for_text)
async def on_mass_mailing_text(message: Message, state: FSMContext, bot: Bot):
    from services.security import sanitize
    import uuid
    data = await state.get_data()
    selected: list[int] = data.get("mass_selected", [])
    owner_id = data.get("owner_id") or message.from_user.id
    prompt_msg_id = data.get("prompt_msg_id")

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

    # ── Clean UI: удаляем ввод юзера и сообщение-инструкцию бота ──
    try:
        await message.delete()
    except Exception:
        pass
    if prompt_msg_id:
        try:
            await bot.delete_message(message.chat.id, prompt_msg_id)
        except Exception:
            pass

    await state.clear()

    # Создаём ЕДИНЫЙ черновик для выбранных ботов (кампания)
    campaign_id = "bots:" + ",".join(str(x) for x in selected)
    
    mid = await db.fetchval(
        """INSERT INTO mailings
           (owner_id, child_bot_id, chat_id, text, media_file_id, media_type,
            notify_users, protect_content, pin_message, delete_after_send,
            disable_preview, url_buttons_raw, button_color, media_below, campaign_id)
           VALUES ($1, NULL, NULL, $2, $3, $4, true, false, false, false, false, NULL, 'blue', true, $5)
           RETURNING id""",
        owner_id, text, media_file_id, media_type, campaign_id
    )

    if not mid:
        await message.answer("❌ Не удалось создать рассылку.")
        return

    # Показываем настройки единого черновика
    m = await db.fetchrow("SELECT * FROM mailings WHERE id=$1", mid)
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
    _tz = await _get_bot_tz(dict(m).get("child_bot_id"))
    await message.answer(
        _draft_settings_text(dict(m), _tz) + bots_note,
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
    notify_icon  = f"{'🔔' if m.get('notify_users', True) else '🔕'} Уведомить: {_yn(m.get('notify_users', True))}"
    protect_icon = f"🔒 Защитить: {_yn(m.get('protect_content', False))}"
    pin_icon     = f"📌 Закрепить: {'24ч' if m.get('pin_message', False) else 'нет'}"
    delete_icon  = f"🗑 Удалить: {'24ч' if m.get('delete_after_send', False) else 'нет'}"

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
            # Если дата уже запланирована → «Сохранить», иначе «Запустить»
            InlineKeyboardButton(
                text="💾 Сохранить" if m.get("scheduled_at") else "➡ Запустить",
                callback_data=(
                    f"mailing_save:{m['chat_id']}:{mid}"
                    if m.get("scheduled_at")
                    else f"mailing_run:{m['chat_id']}:{mid}"
                ),
            ),
        ],
        # Кнопка «Назад»: если chat_id=None — рассылка на уровне бота → возвращаем на bs_mailing
        [InlineKeyboardButton(
            text="◀️ Назад",
            callback_data=(f"bs_mailing:{m['child_bot_id']}" if not m.get('chat_id') else f"ch_mailing:{m['chat_id']}"),
        )],
    ])


async def _get_bot_tz(child_bot_id: int | None) -> str:
    """Возвращает IANA-имя часового пояса бота. По умолчанию UTC."""
    if not child_bot_id:
        return "UTC"
    try:
        row = await db.fetchrow(
            "SELECT timezone FROM bot_chats WHERE child_bot_id=$1 LIMIT 1",
            child_bot_id,
        )
        return (row["timezone"] or "UTC") if row else "UTC"
    except Exception:
        return "UTC"


def _draft_settings_text(m: dict, tz_name: str = "UTC") -> str:
    """Текст блока настроек под превью.
    tz_name — IANA-зона бота, в которой отображается время.
    """
    import zoneinfo as _zi
    try:
        zi = _zi.ZoneInfo(tz_name)
    except Exception:
        zi = _zi.ZoneInfo("UTC")
    scheduled = m.get("scheduled_at")
    if scheduled:
        # scheduled_at хранится в UTC; переводим в часовой пояс бота
        dt_local = scheduled.astimezone(zi) if scheduled.tzinfo else scheduled.replace(tzinfo=timezone.utc).astimezone(zi)
        dt_str = dt_local.strftime("%d.%m.%Y %H:%M")
    else:
        # Черновик без даты — показываем текущий момент в TZ бота
        dt_str = datetime.now(timezone.utc).astimezone(zi).strftime("%d.%m.%Y %H:%M")
    return (
        f"\n\n📅 <b>Дата рассылки:</b> {dt_str}\n"
        f"🗑 <b>Удалить:</b> {'через 24 часа' if m.get('delete_after_send', False) else 'нет'}\n"
        f"📌 <b>Закрепить:</b> {'на 24 часа' if m.get('pin_message', False) else 'нет'}"
    )


# Кэш echo-сообщений черновика: mailing_id -> (echo_msg_id, tg_chat_id)
_draft_echo_ids: dict[int, tuple[int, int]] = {}


async def _delete_draft_echo(bot, mid: int) -> None:
    """Удаляет эхо-сообщение черновика из чата и очищает кэш."""
    cached = _draft_echo_ids.pop(mid, None)
    if cached:
        try:
            await bot.delete_message(cached[1], cached[0])
        except Exception:
            pass


async def _show_draft(callback: CallbackQuery, m: dict):
    """Показывает Экран 4: эхо сообщения сверху + меню управления снизу.

    При первом вызове — отправляет эхо и меню как новые сообщения.
    При повторном вызове (тогглер) — редактирует эхо и меню на месте.
    """
    from services.mailing import _parse_buttons
    mid          = m["id"]
    text         = m.get("text") or ""
    media        = m.get("media_file_id")
    media_type   = m.get("media_type")
    media_below  = bool(m.get("media_below", False))
    disable_prev = bool(m.get("disable_preview", False))
    tg_chat_id   = callback.message.chat.id
    # show_caption_above_media поддерживается только для фото/видео
    supports_above = media_type in ("photo", "video")
    # link_preview_options для текстовых сообщений
    lpo = LinkPreviewOptions(is_disabled=disable_prev)
    # Inline-кнопки из url_buttons_raw
    kb = _parse_buttons(m.get("url_buttons_raw") or "", m.get("button_color") or "blue")
    # Часовой пояс бота для отображения даты рассылки
    tz_name = await _get_bot_tz(m.get("child_bot_id"))

    prev_echo = _draft_echo_ids.get(mid)

    if prev_echo:
        # ── Редактируем существующее эхо на месте ─────────────────────
        echo_msg_id, echo_chat_id = prev_echo
        try:
            if media:
                # Медиа-сообщение — редактируем caption
                edit_kwargs: dict = dict(
                    chat_id=echo_chat_id,
                    message_id=echo_msg_id,
                    caption=text or None,
                    parse_mode="HTML",
                )
                if supports_above:
                    edit_kwargs["show_caption_above_media"] = (not media_below)
                await callback.bot.edit_message_caption(**edit_kwargs)
            else:
                # Текстовое сообщение — редактируем текст
                await callback.bot.edit_message_text(
                    chat_id=echo_chat_id,
                    message_id=echo_msg_id,
                    text=text or "(без текста)",
                    parse_mode="HTML",
                    link_preview_options=lpo,
                )
        except Exception:
            pass
        # Обновляем inline-кнопки на эхо
        try:
            await callback.bot.edit_message_reply_markup(
                chat_id=echo_chat_id,
                message_id=echo_msg_id,
                reply_markup=kb,
            )
        except Exception:
            pass

        # ── Редактируем меню на месте ───────────────────────────────
        try:
            await callback.message.edit_text(
                _draft_settings_text(m, tz_name),
                parse_mode="HTML",
                reply_markup=_kb_draft(m),
            )
        except Exception:
            pass
        await callback.answer()
        return

    # ── Первый показ: отправляем эхо и меню как новые сообщения ───────
    _send_fn_map = {
        "photo":    callback.message.answer_photo,
        "video":    callback.message.answer_video,
        "document": callback.message.answer_document,
    }
    sent_echo = None

    if media and not media_below and supports_above:
        # ⬆ — текст сверху, фото/видео снизу (только для photo/video)
        send_fn = _send_fn_map.get(media_type, callback.message.answer_photo)
        sent_echo = await send_fn(
            media,
            caption=text or None,
            parse_mode="HTML",
            show_caption_above_media=True,
            reply_markup=kb,
        )
    elif media:
        # ⬇ — стандарт: медиа сверху, текст капшоном снизу
        send_fn = _send_fn_map.get(media_type, callback.message.answer_photo)
        sent_echo = await send_fn(media, caption=text or None, parse_mode="HTML", reply_markup=kb)
    else:
        if text:
            sent_echo = await callback.message.answer(
                text, parse_mode="HTML", reply_markup=kb,
                link_preview_options=lpo,
            )

    # Сохраняем echo message_id для будущего редактирования/удаления
    if sent_echo:
        _draft_echo_ids[mid] = (sent_echo.message_id, tg_chat_id)

    # ── Меню управления (настройки + кнопки) ───────────
    await callback.message.answer(
        _draft_settings_text(m, tz_name),
        parse_mode="HTML",
        reply_markup=_kb_draft(m),
    )
    await callback.answer()



# ══════════════════════════════════════════════════════════════
# Экран 2: меню рассылки для канала (ch_mailing:{chat_id})
# ══════════════════════════════════════════════════════════════

def _extract_mailing_id_from_keyboard(callback: CallbackQuery) -> int | None:
    """Извлекает mailing_id из inline-клавиатуры текущего сообщения.
    Ищет кнопку с callback_data вида 'ml_toggle:{mid}:...'
    """
    try:
        markup = callback.message.reply_markup
        if not markup:
            return None
        for row in markup.inline_keyboard:
            for btn in row:
                if btn.callback_data and btn.callback_data.startswith("ml_toggle:"):
                    return int(btn.callback_data.split(":")[1])
    except Exception:
        pass
    return None


@router.callback_query(F.data.startswith("ch_mailing:"))
async def on_ch_mailing(callback: CallbackQuery, platform_user: dict | None):
    if not platform_user:
        return
    tariff = platform_user["tariff"]
    if tariff == "free":
        await callback.answer("Рассылка доступна с тарифа Старт.", show_alert=True)
        return
    chat_id = int(callback.data.split(":")[1])

    # Удаляем эхо-сообщение, если возвращаемся из меню черновика
    mid = _extract_mailing_id_from_keyboard(callback)
    if mid is not None:
        await _delete_draft_echo(callback.bot, mid)

    ch = await db.fetchrow(
        "SELECT chat_title FROM bot_chats WHERE owner_id=$1 AND chat_id=$2::bigint",
        platform_user["user_id"], chat_id,
    )
    title = ch["chat_title"] if ch else "Площадка"

    await navigate(
        callback,
        f"📨 <b>Рассылка</b>\n\nВыберите действие ⬇️",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="➕ Создать рассылку", callback_data=f"mailing_start:{chat_id}")],
            [InlineKeyboardButton(text="📅 Запланированные",  callback_data=f"mailing_scheduled:{chat_id}")],
            [InlineKeyboardButton(text="◀️ Назад",             callback_data=f"channel_by_chat:{chat_id}")],
        ]),
    )


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

    prompt_msg = await navigate(
        callback,
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
    if prompt_msg and hasattr(prompt_msg, 'message_id'):
        await state.update_data(prompt_msg_id=prompt_msg.message_id)


@router.callback_query(F.data.startswith("mailing_bot_scheduled:"))
async def on_mailing_bot_scheduled(callback: CallbackQuery, platform_user: dict | None):
    """Запланированные рассылки на уровне бота (bot-level + channel-level)."""
    if not platform_user:
        return
    child_bot_id = int(callback.data.split(":")[1])
    owner_id = platform_user["user_id"]

    rows = await db.fetch(
        """SELECT m.id, m.text, m.scheduled_at
           FROM mailings m
           WHERE m.owner_id=$1
             AND m.child_bot_id=$2
             AND m.status IN ('pending','scheduled')
           ORDER BY m.scheduled_at ASC LIMIT 15""",
        owner_id, child_bot_id,
    )

    if not rows:
        await callback.message.edit_text(
            "📅 <b>Запланированные</b>\n\nСписок задач ⬇️\n\nНет запланированных рассылок.",
            parse_mode="HTML",
            reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                [InlineKeyboardButton(text="◀️ Назад",
                                      callback_data=f"bs_mailing:{child_bot_id}")],
            ]),
        )
        await callback.answer()
        return

    buttons = []
    for r in rows:
        dt = r["scheduled_at"].strftime("%d.%m.%Y %H:%M") if r.get("scheduled_at") else "—"
        preview = (r["text"] or "")[:20]
        buttons.append([InlineKeyboardButton(
            text=f"{dt} | {preview}",
            callback_data=f"ml_scheduled_view:{r['id']}:{child_bot_id}",
        )])
    buttons.append([InlineKeyboardButton(text="◀️ Назад",
                                          callback_data=f"bs_mailing:{child_bot_id}")])

    await callback.message.edit_text(
        "📅 <b>Запланированные</b>\n\nСписок задач ⬇️",
        parse_mode="HTML",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=buttons),
    )
    await callback.answer()


def _kb_scheduled(m: dict, child_bot_id: int) -> InlineKeyboardMarkup:
    """Клавиатура просмотра запланированной рассылки (скриншот 3)."""
    mid = m["id"]
    _below       = bool(m.get("media_below", False))
    media_icon   = f"📎 Медиа: {'\u2b07' if _below else '\u2b06'}"
    preview_icon = "👁 Превью: нет" if m.get("disable_preview") else "👁 Превью: да"
    notify_icon  = f"{'\U0001f514' if m.get('notify_users', True) else '\U0001f515'} Уведомить: {'\u0434\u0430' if m.get('notify_users', True) else '\u043d\u0435\u0442'}"
    protect_icon = f"🔒 Защитить: {'\u0434\u0430' if m.get('protect_content') else '\u043d\u0435\u0442'}"
    pin_icon     = f"📌 Закрепить: {'24\u0447' if m.get('pin_message') else '\u043d\u0435\u0442'}"
    delete_icon  = f"🗑 Удалить: {'24\u0447' if m.get('delete_after_send') else '\u043d\u0435\u0442'}"

    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="✏️ Редактировать",       callback_data=f"ml_sched_edit:{mid}:{child_bot_id}")],
        [InlineKeyboardButton(text="🔗 URL-кнопки",              callback_data=f"ml_url_buttons:{mid}")],
        [
            InlineKeyboardButton(text=media_icon,   callback_data=f"ml_stoggle:{mid}:{child_bot_id}:media"),
            InlineKeyboardButton(text=preview_icon, callback_data=f"ml_stoggle:{mid}:{child_bot_id}:preview"),
        ],
        [
            InlineKeyboardButton(text=notify_icon,  callback_data=f"ml_stoggle:{mid}:{child_bot_id}:notify"),
            InlineKeyboardButton(text=protect_icon, callback_data=f"ml_stoggle:{mid}:{child_bot_id}:protect"),
        ],
        [
            InlineKeyboardButton(text=pin_icon,    callback_data=f"ml_stoggle:{mid}:{child_bot_id}:pin"),
            InlineKeyboardButton(text=delete_icon, callback_data=f"ml_stoggle:{mid}:{child_bot_id}:delete"),
        ],
        [InlineKeyboardButton(text="⏰ Изменить время",   callback_data=f"ml_sched_reschedule:{mid}:{child_bot_id}")],
        [InlineKeyboardButton(text="🚫 Отменить рассылку", callback_data=f"ml_sched_cancel:{mid}:{child_bot_id}")],
        [InlineKeyboardButton(text="◀️ Назад",               callback_data=f"mailing_bot_scheduled:{child_bot_id}")],
    ])


def _scheduled_settings_text(m: dict, tz_name: str = "UTC") -> str:
    """Текст блока настроек запланированной рассылки.
    tz_name — IANA-зона бота, в которой отображается время.
    """
    import zoneinfo as _zi
    try:
        zi = _zi.ZoneInfo(tz_name)
    except Exception:
        zi = _zi.ZoneInfo("UTC")
    scheduled = m.get("scheduled_at")
    if scheduled:
        dt_local = scheduled.astimezone(zi) if scheduled.tzinfo else scheduled.replace(tzinfo=timezone.utc).astimezone(zi)
        dt_str = dt_local.strftime("%d.%m.%Y %H:%M")
    else:
        dt_str = "—"
    return (
        f"\n\n📅 <b>Дата рассылки:</b> {dt_str}\n"
        f"🗑 <b>Удалить:</b> {'через 24 часа' if m.get('delete_after_send') else 'нет'}\n"
        f"📌 <b>Закрепить:</b> {'на 24 часа' if m.get('pin_message') else 'нет'}"
    )


@router.callback_query(F.data.startswith("ml_scheduled_view:"))
async def on_ml_scheduled_view(callback: CallbackQuery, platform_user: dict | None):
    """Открывает запланированную рассылку: эхо-сообщение + меню управления."""
    if not platform_user:
        return
    parts = callback.data.split(":")
    mid = int(parts[1])
    child_bot_id = int(parts[2])
    owner_id = platform_user["user_id"]

    m = await db.fetchrow(
        "SELECT * FROM mailings WHERE id=$1 AND owner_id=$2", mid, owner_id
    )
    if not m:
        await callback.answer("Рассылка не найдена", show_alert=True)
        return
    md = dict(m)

    # Удаляем сообщение-список
    try:
        await callback.message.delete()
    except Exception:
        pass

    # Отправляем эхо-сообщение
    from services.mailing import _parse_buttons as _pb
    text = md.get("text") or ""
    media = md.get("media_file_id")
    media_type = md.get("media_type")
    kb_echo = _pb(md.get("url_buttons_raw") or "", md.get("button_color") or "blue")
    lpo = LinkPreviewOptions(is_disabled=bool(md.get("disable_preview", False)))

    sent_echo = None
    if media:
        send_fn = {
            "photo": callback.message.answer_photo,
            "video": callback.message.answer_video,
            "document": callback.message.answer_document,
        }.get(media_type, callback.message.answer_photo)
        sent_echo = await send_fn(media, caption=text[:1000] or None,
                                   parse_mode="HTML", reply_markup=kb_echo)
    elif text:
        sent_echo = await callback.message.answer(
            text[:1200], parse_mode="HTML",
            link_preview_options=lpo, reply_markup=kb_echo,
        )

    if sent_echo:
        _draft_echo_ids[mid] = (sent_echo.message_id, callback.message.chat.id)

    # Меню управления
    _tz = await _get_bot_tz(child_bot_id)
    await callback.message.answer(
        _scheduled_settings_text(md, _tz),
        parse_mode="HTML",
        reply_markup=_kb_scheduled(md, child_bot_id),
    )
    await callback.answer()


@router.callback_query(F.data.startswith("ml_stoggle:"))
async def on_ml_stoggle(callback: CallbackQuery, platform_user: dict | None):
    """Тогглеры настроек для запланированной рассылки."""
    if not platform_user:
        return
    parts = callback.data.split(":")
    mid = int(parts[1])
    child_bot_id = int(parts[2])
    setting = parts[3]
    owner_id = platform_user["user_id"]

    m = await db.fetchrow("SELECT * FROM mailings WHERE id=$1 AND owner_id=$2", mid, owner_id)
    if not m:
        await callback.answer("Черновик не найден", show_alert=True)
        return
    md = dict(m)

    if setting == "media":
        new_val = not bool(md.get("media_below", False))
        if md.get("campaign_id"):
            await db.execute("UPDATE mailings SET media_below=$1 WHERE campaign_id=$2", new_val, md["campaign_id"])
        else:
            await db.execute("UPDATE mailings SET media_below=$1 WHERE id=$2", new_val, mid)
    elif setting in _TOGGLE_MAP:
        col, default = _TOGGLE_MAP[setting]
        new_val = not (md[col] if md[col] is not None else default)
        if md.get("campaign_id"):
            await db.execute(f"UPDATE mailings SET {col}=$1 WHERE campaign_id=$2", new_val, md["campaign_id"])
        else:
            await db.execute(f"UPDATE mailings SET {col}=$1 WHERE id=$2", new_val, mid)

    m = await db.fetchrow("SELECT * FROM mailings WHERE id=$1", mid)
    md = dict(m)

    # Обновляем эхо
    prev_echo = _draft_echo_ids.get(mid)
    if prev_echo:
        from services.mailing import _parse_buttons as _pb
        text = md.get("text") or ""
        media = md.get("media_file_id")
        media_type = md.get("media_type")
        kb_echo = _pb(md.get("url_buttons_raw") or "", md.get("button_color") or "blue")
        lpo = LinkPreviewOptions(is_disabled=bool(md.get("disable_preview", False)))
        echo_msg_id, echo_chat_id = prev_echo
        try:
            if media:
                await callback.bot.edit_message_caption(
                    chat_id=echo_chat_id, message_id=echo_msg_id,
                    caption=text or None, parse_mode="HTML",
                )
            else:
                await callback.bot.edit_message_text(
                    chat_id=echo_chat_id, message_id=echo_msg_id,
                    text=text or "(без текста)", parse_mode="HTML",
                    link_preview_options=lpo,
                )
            await callback.bot.edit_message_reply_markup(
                chat_id=echo_chat_id, message_id=echo_msg_id, reply_markup=kb_echo,
            )
        except Exception:
            pass

    _tz = await _get_bot_tz(child_bot_id)
    try:
        await callback.message.edit_text(
            _scheduled_settings_text(md, _tz),
            parse_mode="HTML",
            reply_markup=_kb_scheduled(md, child_bot_id),
        )
    except Exception:
        pass
    await callback.answer()


@router.callback_query(F.data.startswith("ml_sched_cancel:"))
async def on_ml_sched_cancel(callback: CallbackQuery, platform_user: dict | None):
    """Отменяет запланированную рассылку."""
    if not platform_user:
        return
    parts = callback.data.split(":")
    mid = int(parts[1])
    child_bot_id = int(parts[2])
    owner_id = platform_user["user_id"]

    m_old = await db.fetchrow("SELECT campaign_id FROM mailings WHERE id=$1 AND owner_id=$2", mid, owner_id)
    if m_old and m_old["campaign_id"]:
        await db.execute("UPDATE mailings SET status='cancelled', scheduled_at=NULL WHERE campaign_id=$1 AND owner_id=$2", m_old["campaign_id"], owner_id)
    else:
        await db.execute("UPDATE mailings SET status='cancelled', scheduled_at=NULL WHERE id=$1 AND owner_id=$2", mid, owner_id)
    await _delete_draft_echo(callback.bot, mid)

    await callback.message.edit_text(
        "✅ <b>Запланированная рассылка отменена.</b>",
        parse_mode="HTML",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="◀️ К списку",
                                  callback_data=f"mailing_bot_scheduled:{child_bot_id}")],
        ]),
    )
    await callback.answer("Рассылка отменена")


@router.callback_query(F.data.startswith("ml_sched_reschedule:"))
async def on_ml_sched_reschedule(callback: CallbackQuery, state: FSMContext,
                                  platform_user: dict | None):
    """Изменить время запланированной рассылки — переиспользуем FSM планирования."""
    if not platform_user:
        return
    parts = callback.data.split(":")
    mid = int(parts[1])
    child_bot_id = int(parts[2])

    await state.set_state(MailingFSM.waiting_for_schedule)
    await state.update_data(mailing_id=str(mid), child_bot_id=child_bot_id)

    now_utc = datetime.now(timezone.utc)
    now_str = now_utc.strftime("%H:%M")
    back_cb = f"ml_scheduled_view:{mid}:{child_bot_id}"

    await callback.message.edit_text(
        "<u>Отправьте новую дату в формате</u>\n"
        "├ <code>01.01.25 12:00</code>\n"
        "├ <code>01.01.23, 11:24 (+3)</code>\n"
        "├ <code>01-01-2023, 11:24 (+3)</code>\n"
        f"└ <code>{now_str} (+3)</code> [Текущая дата]\n\n"
        "<blockquote>⏱ Если необходимо, укажите в скобках ваш <a href='https://time.is/UTC'>часовой пояс</a>.</blockquote>\n\n"
        "<blockquote>⏱ Отправить сейчас: <code>now</code></blockquote>\n\n"
        "Выберите действие ⬇️",
        parse_mode="HTML",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="🚫 Отменить", callback_data=back_cb)],
        ]),
        link_preview_options=LinkPreviewOptions(is_disabled=True),
    )
    await callback.answer()


@router.callback_query(F.data.startswith("ml_sched_edit:"))
async def on_ml_sched_edit(callback: CallbackQuery, state: FSMContext,
                            platform_user: dict | None):
    """Переходим в FSM-режим редактирования текста рассылки."""
    if not platform_user:
        return
    parts = callback.data.split(":")
    mid = int(parts[1])
    child_bot_id = int(parts[2])

    await state.set_state(MailingFSM.waiting_for_edit_text)
    await state.update_data(mailing_id=str(mid), child_bot_id=child_bot_id,
                             owner_id=platform_user["user_id"])

    await _delete_draft_echo(callback.bot, mid)
    try:
        await callback.message.delete()
    except Exception:
        pass

    await callback.message.answer(
        "✏️ <b>Редактирование рассылки</b>\n\n"
        "Отправьте новое сообщение для рассылки.\n"
        "Можно прикрепить медиа.",
        parse_mode="HTML",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="🚫 Отмена",
                                  callback_data=f"ml_scheduled_view:{mid}:{child_bot_id}")],
        ]),
    )
    await callback.answer()


@router.message(MailingFSM.waiting_for_edit_text)
async def on_scheduled_edit_input(message: Message, state: FSMContext):
    """Получаем новый текст/медиа для запланированной рассылки."""
    data = await state.get_data()
    mailing_id = int(data.get("mailing_id", 0))
    child_bot_id = int(data.get("child_bot_id", 0))
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

    # Обновляем черновик
    m_old = await db.fetchrow("SELECT campaign_id FROM mailings WHERE id=$1", mailing_id)
    if media_file_id:
        if m_old and m_old["campaign_id"]:
            await db.execute("UPDATE mailings SET text=$1, media_file_id=$2, media_type=$3 WHERE campaign_id=$4 AND owner_id=$5", text, media_file_id, media_type, m_old["campaign_id"], owner_id)
        else:
            await db.execute("UPDATE mailings SET text=$1, media_file_id=$2, media_type=$3 WHERE id=$4 AND owner_id=$5", text, media_file_id, media_type, mailing_id, owner_id)
    else:
        if m_old and m_old["campaign_id"]:
            await db.execute("UPDATE mailings SET text=$1, media_file_id=NULL, media_type=NULL WHERE campaign_id=$2 AND owner_id=$3", text, m_old["campaign_id"], owner_id)
        else:
            await db.execute("UPDATE mailings SET text=$1, media_file_id=NULL, media_type=NULL WHERE id=$2 AND owner_id=$3", text, mailing_id, owner_id)

    await state.clear()
    m = await db.fetchrow("SELECT * FROM mailings WHERE id=$1", mailing_id)
    md = dict(m)

    # Отправляем обновлённое эхо
    from services.mailing import _parse_buttons as _pb
    kb_echo = _pb(md.get("url_buttons_raw") or "", md.get("button_color") or "blue")
    lpo = LinkPreviewOptions(is_disabled=bool(md.get("disable_preview", False)))
    sent_echo = None
    if media_file_id:
        send_fn = {
            "photo": message.answer_photo,
            "video": message.answer_video,
            "document": message.answer_document,
        }.get(media_type, message.answer_photo)
        sent_echo = await send_fn(media_file_id, caption=text[:1000] or None,
                                   parse_mode="HTML", reply_markup=kb_echo)
    elif text:
        sent_echo = await message.answer(text[:1200], parse_mode="HTML",
                                          link_preview_options=lpo, reply_markup=kb_echo)
    if sent_echo:
        _draft_echo_ids[mailing_id] = (sent_echo.message_id, message.chat.id)

    # Меню управления
    _tz = await _get_bot_tz(child_bot_id)
    await message.answer(
        _scheduled_settings_text(md, _tz),
        parse_mode="HTML",
        reply_markup=_kb_scheduled(md, child_bot_id),
    )



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
    await state.update_data(prompt_msg_id=callback.message.message_id)
    await callback.answer()


# ══════════════════════════════════════════════════════════════
# Получение сообщения → создание черновика → Экран 4
# ══════════════════════════════════════════════════════════════

@router.message(MailingFSM.waiting_for_text)
async def on_mailing_text(message: Message, state: FSMContext, bot: Bot):
    data = await state.get_data()
    chat_id      = data.get("chat_id")
    owner_id     = data.get("owner_id")
    child_bot_id = data.get("child_bot_id")  # set for bot-level mailing
    prompt_msg_id = data.get("prompt_msg_id")

    logger.info(
        f"[mailing_text] from={message.from_user.id} "
        f"chat_id={chat_id} child_bot_id={child_bot_id} owner_id={owner_id} "
        f"has_text={bool(message.text)} has_caption={bool(message.caption)} "
        f"has_photo={bool(message.photo)} has_video={bool(message.video)} "
        f"has_document={bool(message.document)}"
    )

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

    logger.info(f"[mailing_text] media_type={media_type} text_len={len(text)} media_file_id={'set' if media_file_id else 'None'}")

    if not text and not media_file_id:
        await message.answer("⚠️ Пожалуйста, отправьте текст или медиа.")
        return

    # ── Clean UI: удаляем ввод юзера и сообщение-инструкцию бота ──
    try:
        await message.delete()
    except Exception:
        pass
    if prompt_msg_id:
        try:
            await bot.delete_message(message.chat.id, prompt_msg_id)
        except Exception:
            pass

    # Создаём черновик
    try:
        if child_bot_id and not chat_id:
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
            mailing_id = await db.fetchval(
                """INSERT INTO mailings
                   (owner_id, chat_id, text, media_file_id, media_type,
                    notify_users, protect_content, pin_message, delete_after_send,
                    disable_preview, url_buttons_raw, button_color, media_below)
                   VALUES ($1,$2,$3,$4,$5, true,false,false,false, false,NULL,'blue',true)
                   RETURNING id""",
                owner_id, chat_id, text, media_file_id, media_type,
            )
    except Exception as e:
        logger.error(f"[mailing_text] DB insert failed: {e}")
        await message.answer("❌ Ошибка при создании черновика.")
        return

    logger.info(f"[mailing_text] mailing_id={mailing_id}")
    await state.clear()

    m = await db.fetchrow("SELECT * FROM mailings WHERE id=$1", mailing_id)

    # ── Эхо: предпросмотр сообщения (без кнопок)
    sent_echo = None
    _echo_lpo = LinkPreviewOptions(is_disabled=bool(dict(m).get("disable_preview", False)))
    try:
        if media_file_id:
            send_fn = {
                "photo": message.answer_photo,
                "video": message.answer_video,
                "document": message.answer_document,
            }.get(media_type, message.answer_photo)
            sent_echo = await send_fn(media_file_id, caption=text[:1000] or None, parse_mode="HTML")
        elif text:
            sent_echo = await message.answer(
                text[:1200], parse_mode="HTML",
                link_preview_options=_echo_lpo,
            )
        logger.info(f"[mailing_text] echo sent: msg_id={sent_echo.message_id if sent_echo else None}")
    except Exception as e:
        logger.error(f"[mailing_text] echo send failed: {e}")

    # Сохраняем echo message_id для последующего редактирования/удаления
    if sent_echo:
        _draft_echo_ids[mailing_id] = (sent_echo.message_id, message.chat.id)

    # ── Меню управления
    try:
        _tz = await _get_bot_tz(dict(m).get("child_bot_id"))
        await message.answer(
            _draft_settings_text(dict(m), _tz),
            parse_mode="HTML",
            reply_markup=_kb_draft(dict(m)),
        )
        logger.info(f"[mailing_text] menu sent OK")
    except Exception as e:
        logger.error(f"[mailing_text] menu send failed: {e}")




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
    # Удаляем текущее сообщение (экран URL-кнопок или список) перед восстановлением эхо
    try:
        await callback.message.delete()
    except Exception:
        pass
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
        if m.get("campaign_id"):
            await db.execute("UPDATE mailings SET media_below=$1 WHERE campaign_id=$2 AND owner_id=$3", new_val, m["campaign_id"], owner_id)
        else:
            await db.execute("UPDATE mailings SET media_below=$1 WHERE id=$2 AND owner_id=$3", new_val, mid, owner_id)
            
        m = await db.fetchrow("SELECT * FROM mailings WHERE id=$1", mid)
        await callback.answer()
        await _show_draft(callback, dict(m))
        return

    col, default = _TOGGLE_MAP[setting]
    new_val = not (m[col] if m[col] is not None else default)
    if m.get("campaign_id"):
        await db.execute(f"UPDATE mailings SET {col}=$1 WHERE campaign_id=$2 AND owner_id=$3", new_val, m["campaign_id"], owner_id)
    else:
        await db.execute(f"UPDATE mailings SET {col}=$1 WHERE id=$2 AND owner_id=$3", new_val, mid, owner_id)
        
    m = await db.fetchrow("SELECT * FROM mailings WHERE id=$1", mid)
    await callback.answer()
    await _show_draft(callback, dict(m))


# ══════════════════════════════════════════════════════════════
# Экран 5: URL-кнопки
# ══════════════════════════════════════════════════════════════

_URL_BUTTONS_HELP = (
    "🔗 Отправьте кнопки, которые будут добавлены к сообщению.\n\n"
    "🔗 <b>URL-кнопки</b>\n\n"
    "<b>Одна кнопка в ряду:</b>\n"
    "<code>Кнопка 1 — ссылка</code>\n"
    "<code>Кнопка 2 — ссылка</code>\n\n"
    "<b>Несколько кнопок в ряду:</b>\n"
    "<code>Кнопка 1 — ссылка | Кнопка 2 — ссылка</code>\n\n"
    "<b>🎨 Цветные кнопки (добавь emoji перед названием):</b>\n"
    "<code>🟦 Кнопка — ссылка</code>  — синяя\n"
    "<code>🟩 Кнопка — ссылка</code>  — зелёная\n"
    "<code>🟥 Кнопка — ссылка</code>  — красная\n\n"
    "<b>*** Другие виды кнопок</b>\n\n"
    "<b>WebApp кнопки:</b>\n"
    "<code>Кнопка 1 — ссылка (webapp)</code>\n\n"
    "ⓘ Нажмите, чтобы скопировать."
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

    existing = m.get("url_buttons_raw") or ""
    now_text = f"\n\n✅ <b>Текущие кнопки:</b>\n<code>{existing}</code>" if existing else ""

    # Удаляем эхо + текущее меню черновика
    await _delete_draft_echo(callback.bot, mid)
    try:
        await callback.message.delete()
    except Exception:
        pass

    # Ставим FSM-состояние — пользователь может сразу ввести кнопки
    await state.update_data(mailing_id=mid, owner_id=platform_user["user_id"])
    await state.set_state(MailingFSM.waiting_for_buttons)

    # Отправляем экран URL-кнопок как новое сообщение (+ только кнопка Назад)
    await callback.message.answer(
        _URL_BUTTONS_HELP + now_text,
        parse_mode="HTML",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="◀️ Назад", callback_data=f"ml_view_draft:{mid}:")],
        ]),
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
    owner_id = platform_user["user_id"]
    m_old = await db.fetchrow("SELECT campaign_id FROM mailings WHERE id=$1", mid)
    if m_old and m_old["campaign_id"]:
        await db.execute("UPDATE mailings SET button_color=$1 WHERE campaign_id=$2 AND owner_id=$3", color, m_old["campaign_id"], owner_id)
    else:
        await db.execute("UPDATE mailings SET button_color=$1 WHERE id=$2 AND owner_id=$3", color, mid, owner_id)
    await callback.answer(f"Цвет: {color}")
    # Перерендер
    fake_cb = callback.model_copy(update={"data": f"ml_url_buttons:{mid}"})
    await on_ml_url_buttons(fake_cb, None, platform_user)  # state=None — не нужен здесь


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
    from services.mailing import _parse_buttons
    data = await state.get_data()
    mid = data.get("mailing_id")
    owner_id = data.get("owner_id")
    raw = sanitize(message.text or "", max_len=2000)
    m_old = await db.fetchrow("SELECT campaign_id FROM mailings WHERE id=$1", mid)
    if m_old and m_old["campaign_id"]:
        await db.execute("UPDATE mailings SET url_buttons_raw=$1 WHERE campaign_id=$2 AND owner_id=$3", raw, m_old["campaign_id"], owner_id)
    else:
        await db.execute("UPDATE mailings SET url_buttons_raw=$1 WHERE id=$2 AND owner_id=$3", raw, mid, owner_id)
    await state.clear()

    # Загружаем обновлённый черновик
    m = await db.fetchrow("SELECT * FROM mailings WHERE id=$1", mid)
    if not m:
        await message.answer("❌ Черновик не найден.")
        return
    m = dict(m)

    # Парсим inline-кнопки
    kb = _parse_buttons(m.get("url_buttons_raw") or "", m.get("button_color") or "blue")

    text       = m.get("text") or ""
    media      = m.get("media_file_id")
    media_type = m.get("media_type")
    media_below = bool(m.get("media_below", False))
    supports_above = media_type in ("photo", "video")
    tg_chat_id = message.chat.id

    # ── Отправляем эхо с inline-кнопками ───────────────────────
    sent_echo = None
    _send_fn_map = {
        "photo":    message.answer_photo,
        "video":    message.answer_video,
        "document": message.answer_document,
    }
    try:
        if media and not media_below and supports_above:
            send_fn = _send_fn_map.get(media_type, message.answer_photo)
            sent_echo = await send_fn(
                media, caption=text or None, parse_mode="HTML",
                show_caption_above_media=True, reply_markup=kb,
            )
        elif media:
            send_fn = _send_fn_map.get(media_type, message.answer_photo)
            sent_echo = await send_fn(media, caption=text or None, parse_mode="HTML", reply_markup=kb)
        elif text:
            sent_echo = await message.answer(text, parse_mode="HTML", reply_markup=kb)
    except Exception:
        pass

    if sent_echo:
        _draft_echo_ids[mid] = (sent_echo.message_id, tg_chat_id)

    # ── Меню настроек черновика ─────────────────────────────────
    _tz = await _get_bot_tz(m.get("child_bot_id"))
    await message.answer(
        _draft_settings_text(m, _tz),
        parse_mode="HTML",
        reply_markup=_kb_draft(m),
    )


@router.callback_query(F.data.startswith("ml_clear_buttons:"))
async def on_ml_clear_buttons(callback: CallbackQuery, platform_user: dict | None):
    if not platform_user:
        return
    mid = int(callback.data.split(":")[1])
    owner_id = platform_user["user_id"]
    m_old = await db.fetchrow("SELECT campaign_id FROM mailings WHERE id=$1", mid)
    if m_old and m_old["campaign_id"]:
        await db.execute("UPDATE mailings SET url_buttons_raw=NULL WHERE campaign_id=$1 AND owner_id=$2", m_old["campaign_id"], owner_id)
    else:
        await db.execute("UPDATE mailings SET url_buttons_raw=NULL WHERE id=$1 AND owner_id=$2", mid, owner_id)
    await callback.answer("🗑 Кнопки очищены")
    fake_cb = callback.model_copy(update={"data": f"ml_url_buttons:{mid}"})
    await on_ml_url_buttons(fake_cb, None, platform_user)


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

    # Текущая дата для подсказки
    now_utc = datetime.now(timezone.utc)
    now_str = now_utc.strftime("%H:%M")

    await callback.message.edit_text(
        "<u>Отправьте дату в формате</u>\n"
        "├ <code>01.01.25 12:00</code>\n"
        "├ <code>01.01.23, 11:24 (+3)</code>\n"
        "├ <code>01-01-2023, 11:24 (+3)</code>\n"
        f"└ <code>{now_str} (+3)</code> [Текущая дата]\n\n"
        "<blockquote>⏱ Если необходимо, то укажите в скобках ваш <a href='https://time.is/UTC'>часовой пояс</a>. </blockquote>\n\n"
        "<blockquote>⏱ Если рассылку необходимо отправить сейчас, пришлите: <code>now</code></blockquote>\n\n"
        "Выберите действие ⬇️",
        parse_mode="HTML",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="🚫 Отменить", callback_data=back_cb)],
        ]),
        link_preview_options=LinkPreviewOptions(is_disabled=True),
    )
    await callback.answer()


@router.message(MailingFSM.waiting_for_schedule)
async def on_schedule_input(message: Message, state: FSMContext):
    data = await state.get_data()
    mailing_id = data.get("mailing_id")
    raw = (message.text or "").strip()
    now_utc = datetime.now(timezone.utc)

    # ── Поддержка "now" — НЕМЕДЛЕННЫЙ запуск рассылки ────────────
    if raw.lower() == "now":
        await state.clear()
        if not mailing_id:
            await message.answer("❌ Черновик не найден.")
            return

        mailing = await db.fetchrow(
            "SELECT m.*, cb.bot_username FROM mailings m "
            "LEFT JOIN child_bots cb ON cb.id = m.child_bot_id "
            "WHERE m.id=$1",
            int(mailing_id),
        )
        if not mailing:
            await message.answer("❌ Черновик не найден.")
            return

        mailing_id_int = int(mailing_id)
        bot_username   = mailing.get("bot_username") or ""
        child_bot_id   = mailing.get("child_bot_id")

        await db.execute("UPDATE mailings SET status='pending', scheduled_at=NULL WHERE id=$1", mailing_id_int)
        await _delete_draft_echo(message.bot, mailing_id_int)

        # Удаляем входящее сообщение «now»
        try:
            await message.delete()
        except Exception:
            pass

        # Начальный прогресс-экран (как в on_mailing_run)
        progress_text = _mailing_progress_text(mailing_id_int, 0, 0, 0, "running", bot_username)
        progress_msg = await message.answer(
            progress_text,
            parse_mode="HTML",
            reply_markup=kb_mailing_control(mailing_id_int, False, "low"),
        )

        upd_chat_id = message.chat.id
        upd_msg_id  = progress_msg.message_id
        back_cb     = f"bs_mailing:{child_bot_id}" if child_bot_id else "menu:mailing"

        async def _progress_cb_now(ml_id: int, sent: int, total: int,
                                    errors: int, status: str):
            m_row = await db.fetchrow("SELECT started_at FROM mailings WHERE id=$1", ml_id)
            started_at = m_row["started_at"] if m_row else None
            text = _mailing_progress_text(ml_id, sent, total, errors, status, bot_username, started_at)
            kb = kb_mailing_control(ml_id, False, "low") if status == "running" else None
            try:
                await message.bot.edit_message_text(
                    text,
                    chat_id=upd_chat_id,
                    message_id=upd_msg_id,
                    parse_mode="HTML",
                    reply_markup=kb,
                )
            except Exception:
                pass
            # После завершения — меню рассылки (Скриншот 3)
            if status in ("done", "cancelled"):
                try:
                    await message.bot.send_message(
                        chat_id=upd_chat_id,
                        text="📨 <b>Рассылка</b>\n\nВыберите действие ⬇️",
                        parse_mode="HTML",
                        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                            [InlineKeyboardButton(text="➕ Создать рассылку",
                                                  callback_data=f"mailing_bot_start:{child_bot_id}")],
                            [InlineKeyboardButton(text="📅 Запланированные",
                                                  callback_data=f"mailing_bot_scheduled:{child_bot_id}")],
                            [InlineKeyboardButton(text="◀️ Назад",
                                                  callback_data=back_cb)],
                        ]),
                    )
                except Exception:
                    pass

        asyncio.create_task(mailing_svc.run_mailing(mailing_id_int, message.bot, _progress_cb_now))
        return

    # ── Обычный флоу: парсим дату ────────────────────────────
    # Определяем часовой пояс бота (добавляется к оффсету, если пользователь не указал (+N))
    _bot_tz_offset_hours: float = 0.0
    if mailing_id:
        try:
            import zoneinfo as _zi
            _cbi = await db.fetchval("SELECT child_bot_id FROM mailings WHERE id=$1", int(mailing_id))
            if _cbi:
                _tz_name = await _get_bot_tz(_cbi)
                _zi_obj = _zi.ZoneInfo(_tz_name)
                _now_local = now_utc.astimezone(_zi_obj)
                _bot_tz_offset_hours = _now_local.utcoffset().total_seconds() / 3600
        except Exception:
            pass

    tz_offset_given = bool(re.search(r'\([+-]?\d{1,2}\)', raw))
    tz_offset = 0
    tz_match = re.search(r'\(([+-]?\d{1,2})\)', raw)
    if tz_match:
        tz_offset = int(tz_match.group(1))
        raw = re.sub(r'\s*\([+-]?\d{1,2}\)', '', raw).strip()

    # Нормализация: убираем запятые, дефисы → точки
    raw = raw.replace(',', '').strip()
    raw = re.sub(r'(\d{1,2})-(\d{1,2})-(\d{2,4})', r'\1.\2.\3', raw)

    dt = None
    for fmt in ("%d.%m.%y %H:%M", "%d.%m.%Y %H:%M", "%d.%m %H:%M"):
        try:
            dt = datetime.strptime(raw, fmt)
            if fmt == "%d.%m %H:%M":
                dt = dt.replace(year=now_utc.year)
            break
        except ValueError:
            continue

    if dt is None:
        err = await message.answer(
            "❌ <b>Неверный формат даты.</b>\n\n"
            "Используйте например:\n"
            "<code>01.03.25 12:00</code>\n"
            "<code>01.03.2026 14:00 (+3)</code>\n"
            "<code>now</code> — для немедленной отправки",
            parse_mode="HTML",
        )
        # Авто-удаление ошибки через 5 сек
        await asyncio.sleep(5)
        try:
            await err.delete()
        except Exception:
            pass
        return

    # Переводим в UTC:
    # • если пользователь дал явный (+N) — используем его
    # • если нет — считаем введённое время в TZ бота
    effective_offset = tz_offset if tz_offset_given else _bot_tz_offset_hours
    dt = (dt - timedelta(hours=effective_offset)).replace(tzinfo=timezone.utc)

    # ── Проверка: дата должна быть в будущем ────────────────────
    if dt <= now_utc:
        # Показываем подсказку в TZ бота (не в UTC)
        import zoneinfo as _zi_hint
        _hint_cbi = await db.fetchval("SELECT child_bot_id FROM mailings WHERE id=$1", int(mailing_id)) if mailing_id else None
        hint_time = (now_utc + timedelta(hours=1)).astimezone(
            _zi_hint.ZoneInfo(await _get_bot_tz(_hint_cbi))
        ).strftime('%d.%m.%Y %H:%M')
        err = await message.answer(
            "⏰ <b>Дата уже прошла!</b>\n\n"
            "Укажите дату <b>в будущем</b>.\n"
            "Например, через час: "
            f"<code>{hint_time}</code>",
            parse_mode="HTML",
        )
        # Авто-удаление ошибки через 5 сек
        await asyncio.sleep(5)
        try:
            await err.delete()
        except Exception:
            pass
        return

    # ── Проверка rate-limit: не чаще 1 рассылки в час ────────────
    if mailing_id:
        owner_id_check = data.get("owner_id") or message.from_user.id
        child_bot_id_check = await db.fetchval(
            "SELECT child_bot_id FROM mailings WHERE id=$1", int(mailing_id)
        )
        conflict = await db.fetchval(
            """SELECT 1 FROM mailings
               WHERE owner_id=$1
                 AND ($2::int IS NULL OR child_bot_id=$2::int)
                 AND status IN ('scheduled','pending','running')
                 AND id != $3
                 AND ABS(EXTRACT(EPOCH FROM (scheduled_at - $4::timestamptz))) < 3600
               LIMIT 1""",
            owner_id_check, child_bot_id_check, int(mailing_id), dt,
        )
        if conflict:
            err = await message.answer(
                "🚫 <b>Слишком часто!</b>\n\n"
                "Запланированные рассылки можно создавать "
                "<b>не чаще одного раза в час</b>.\n"
                "Пожалуйста, выберите другое время.",
                parse_mode="HTML",
            )
            await asyncio.sleep(5)
            try:
                await err.delete()
            except Exception:
                pass
            return

    await state.clear()

    if mailing_id:
        m_old = await db.fetchrow("SELECT campaign_id FROM mailings WHERE id=$1", int(mailing_id))
        if m_old and m_old["campaign_id"]:
            await db.execute("UPDATE mailings SET scheduled_at=$1, status='draft' WHERE campaign_id=$2", dt, m_old["campaign_id"])
        else:
            await db.execute("UPDATE mailings SET scheduled_at=$1, status='draft' WHERE id=$2", dt, int(mailing_id))
        # Загружаем обновлённый черновик и перерисовываем меню
        m = await db.fetchrow("SELECT * FROM mailings WHERE id=$1", int(mailing_id))
        if m:
            try:
                await callback_answer_noop(message)
            except Exception:
                pass
            # Удаляем старое эхо и перерисовываем меню черновика
            await _delete_draft_echo(message.bot, int(mailing_id))
            # Отправляем обновлённое эхо + меню с кнопкой «Сохранить»
            local_dt = dt + timedelta(hours=tz_offset)
            tz_str = f" UTC{'+' if tz_offset >= 0 else ''}{tz_offset}" if tz_offset != 0 else " UTC"

            # Отправляем эхо
            from services.mailing import _parse_buttons as _pb
            md = dict(m)
            text = md.get("text") or ""
            media = md.get("media_file_id")
            media_type = md.get("media_type")
            kb_echo = _pb(md.get("url_buttons_raw") or "", md.get("button_color") or "blue")
            lpo = LinkPreviewOptions(is_disabled=bool(md.get("disable_preview", False)))
            sent_echo = None
            if media:
                send_fn = {
                    "photo": message.answer_photo,
                    "video": message.answer_video,
                    "document": message.answer_document,
                }.get(media_type, message.answer_photo)
                sent_echo = await send_fn(media, caption=text[:1000] or None,
                                          parse_mode="HTML", reply_markup=kb_echo)
            elif text:
                sent_echo = await message.answer(text[:1200], parse_mode="HTML",
                                                  link_preview_options=lpo)
            if sent_echo:
                _draft_echo_ids[int(mailing_id)] = (sent_echo.message_id, message.chat.id)

            # Меню черновика с кнопкой «Сохранить»
            _tz = await _get_bot_tz(md.get("child_bot_id"))
            await message.answer(
                _draft_settings_text(md, _tz),
                parse_mode="HTML",
                reply_markup=_kb_draft(md),
            )
    else:
        await state.clear()
        await message.answer(f"✅ Запланировано: {dt.strftime('%d.%m.%Y %H:%M')}")


async def callback_answer_noop(message: Message) -> None:
    """Заглушка — нам не нужно отвечать на callback когда мы обрабатываем Message."""
    pass


# ══════════════════════════════════════════════════════════════
# Сохранение запланированной рассылки
# ══════════════════════════════════════════════════════════════

@router.callback_query(F.data.startswith("mailing_save:"))
async def on_mailing_save(callback: CallbackQuery, platform_user: dict | None):
    """Сохраняет запланированную рассылку (status='scheduled') и показывает экран успеха."""
    if not platform_user:
        return
    parts = callback.data.split(":")
    mid = int(parts[2]) if len(parts) > 2 and parts[2] else None
    if not mid:
        await callback.answer("Черновик не найден", show_alert=True)
        return

    owner_id = platform_user["user_id"]
    m = await db.fetchrow(
        "SELECT * FROM mailings WHERE id=$1 AND owner_id=$2", mid, owner_id
    )
    if not m:
        await callback.answer("Черновик не найден", show_alert=True)
        return

    scheduled_at = m.get("scheduled_at")
    if not scheduled_at:
        await callback.answer("Сначала укажите дату рассылки", show_alert=True)
        return

    # Сохраняем как 'scheduled'
    if m.get("campaign_id"):
        await db.execute("UPDATE mailings SET status='scheduled' WHERE campaign_id=$1 AND owner_id=$2", m["campaign_id"], owner_id)
    else:
        await db.execute("UPDATE mailings SET status='scheduled' WHERE id=$1 AND owner_id=$2", mid, owner_id)

    # Удаляем эхо-сообщение черновика
    await _delete_draft_echo(callback.bot, mid)

    # Определяем child_bot_id для ссылки «В меню рассылки»
    child_bot_id = m.get("child_bot_id")
    chat_id_val  = m.get("chat_id")
    if not child_bot_id and chat_id_val:
        row = await db.fetchrow(
            "SELECT child_bot_id FROM bot_chats WHERE owner_id=$1 AND chat_id=$2::bigint",
            owner_id, chat_id_val,
        )
        if row:
            child_bot_id = row["child_bot_id"]

    back_cb = f"bs_mailing:{child_bot_id}" if child_bot_id else "menu:mailing"
    dt_str = scheduled_at.strftime("%d.%m.%Y %H:%M") if scheduled_at else "—"

    await callback.message.edit_text(
        f"✅ <b>Рассылка успешно сохранена.</b>\n\n"
        f"📅 Дата отправки: <b>{dt_str}</b> (UTC)",
        parse_mode="HTML",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="➡ В меню рассылки", callback_data=back_cb)],
        ]),
    )
    await callback.answer("✅ Рассылка сохранена")


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
    # Прогресс считаем по попыткам (успешно + ошибки), а не только успешным.
    # Это гарантирует 100% по завершении даже при блокировках.
    attempted = sent + errors
    if status in ("done", "cancelled"):
        # По завершении всегда 100%
        pct = 100.0
    else:
        pct = (attempted / total * 100) if total > 0 else 0.0
    filled = int(pct / 10)
    bar = "▓" * filled + "░" * (10 - filled)

    status_map = {
        "running":   "🟢 В процессе",
        "done":      "✅ Завершено",
        "cancelled": "⏹ Остановлено",
    }
    status_str = status_map.get(status, "🟢 В процессе")

    # Отправлено = попытки (тем, кому пытались отправить)
    # Получили   = только успешно доставленные
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
        f"↗️ Отправлено: <b>{attempted}</b> из <b>{total}</b>\n"
        f"✅ Получили: <b>{sent}</b>\n"
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
    campaign_id  = mailing.get("campaign_id")

    if campaign_id:
        mailings_to_run = await db.fetch(
            "SELECT m.id, cb.bot_username FROM mailings m "
            "LEFT JOIN child_bots cb ON cb.id = m.child_bot_id "
            "WHERE m.campaign_id=$1 AND m.status='draft'",
            campaign_id
        )
    else:
        mailings_to_run = [{"id": mailing["id"], "bot_username": mailing.get("bot_username", "")}]

    # Удаляем эхо-сообщение (превью) и сообщение с настройками
    await _delete_draft_echo(callback.bot, mailing["id"])
    try:
        await callback.message.delete()
    except Exception:
        pass

    await callback.answer("▶ Рассылка запущена")

    def make_progress_callback(target_m_id: int, initial_chat_id: int, initial_msg_id: int | None, target_bot_username: str):
        async def progress_callback(ml_id: int, sent: int, total: int, errors: int, status: str):
            m_row = await db.fetchrow("SELECT started_at, child_bot_id, chat_id FROM mailings WHERE id=$1", ml_id)
            started_at = m_row["started_at"] if m_row else None
            text = _mailing_progress_text(ml_id, sent, total, errors, status, target_bot_username, started_at)
            kb = kb_mailing_control(ml_id, False, "low") if status == "running" else None
            try:
                await bot.edit_message_text(
                    text,
                    chat_id=initial_chat_id,
                    message_id=initial_msg_id,
                    parse_mode="HTML",
                    reply_markup=kb,
                )
            except Exception as e:
                logger.debug(f"Progress update failed: {e}")

            # После завершения — отправляем меню рассылки (только если это одиночный бот или мы можем это сделать)
            if status in ("done", "cancelled", "failed") and m_row:
                try:
                    chat_id_from_m = m_row.get("chat_id")
                    child_bot_from_m = m_row.get("child_bot_id")
                    if chat_id_from_m:
                        final_kb = InlineKeyboardMarkup(inline_keyboard=[
                            [InlineKeyboardButton(text="➕ Создать рассылку", callback_data=f"mailing_start:{chat_id_from_m}")],
                            [InlineKeyboardButton(text="📅 Запланированные",  callback_data=f"mailing_scheduled:{chat_id_from_m}")],
                            [InlineKeyboardButton(text="◀️ Назад",             callback_data=f"channel_by_chat:{chat_id_from_m}")],
                        ])
                    elif child_bot_from_m:
                        final_kb = InlineKeyboardMarkup(inline_keyboard=[
                            [InlineKeyboardButton(text="➕ Создать рассылку", callback_data=f"mailing_bot_start:{child_bot_from_m}")],
                            [InlineKeyboardButton(text="📅 Запланированные",  callback_data=f"mailing_bot_scheduled:{child_bot_from_m}")],
                            [InlineKeyboardButton(text="◀️ Назад",             callback_data=f"bs_mailing:{child_bot_from_m}")],
                        ])
                    else:
                        final_kb = InlineKeyboardMarkup(inline_keyboard=[
                            [InlineKeyboardButton(text="➕ Создать рассылку", callback_data="menu:mailing")],
                            [InlineKeyboardButton(text="◀️ Назад",             callback_data="menu:mailing")],
                        ])

                    header = f"📨 <b>Рассылка @{target_bot_username} завершена</b>" if target_bot_username else "📨 <b>Массовая рассылка завершена</b>"
                    await bot.send_message(
                        chat_id=initial_chat_id,
                        text=f"{header}\n\nВыберите действие ⬇️",
                        parse_mode="HTML",
                        reply_markup=final_kb,
                    )
                except Exception:
                    pass
        return progress_callback

    for m_item in mailings_to_run:
        m_id = m_item["id"]
        m_bot = m_item["bot_username"] or ""
        await db.execute("UPDATE mailings SET status='pending' WHERE id=$1", m_id)

        progress_text = _mailing_progress_text(m_id, 0, 0, 0, "running", m_bot)
        try:
            progress_msg = await callback.message.answer(
                progress_text,
                parse_mode="HTML",
                reply_markup=kb_mailing_control(m_id, False, "low"),
            )
            u_chat = progress_msg.chat.id
            u_msg  = progress_msg.message_id
        except Exception:
            u_chat = callback.message.chat.id
            u_msg  = callback.message.message_id

        cb = make_progress_callback(m_id, u_chat, u_msg, m_bot)
        asyncio.create_task(mailing_svc.run_mailing(m_id, callback.bot, cb))




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
