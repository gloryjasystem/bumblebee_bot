"""
handlers/channel_settings.py — Полные настройки площадки:
  - Обработка заявок (авто/отложенное/капча)
  - Приветственное сообщение
  - Прощание
  - Языковые фильтры
  - RTL / иероглифы / без фото
  - Реакции на сообщения
  - Обратная связь
"""
import logging
import zoneinfo
from datetime import datetime, timezone as dt_timezone
from aiogram import Router, F, Bot
from aiogram.types import (
    Message, CallbackQuery, InlineKeyboardMarkup, InlineKeyboardButton
)
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup

import db.pool as db
from services.security import sanitize, decrypt_token
from utils.nav import navigate

logger = logging.getLogger(__name__)
router = Router()


class SettingsFSM(StatesGroup):
    # Обработка заявок
    waiting_for_delay       = State()
    # Капча
    waiting_for_captcha_text  = State()
    waiting_for_captcha_timer = State()
    # Приветствие / Прощание (базовый ввод текста и медиа)
    waiting_for_welcome_text  = State()
    waiting_for_farewell_text = State()
    # Редактор сообщений: кнопки и медиа
    waiting_for_msg_buttons   = State()
    waiting_for_msg_media     = State()
    # ЧС: загрузка файлов (бот-уровень)
    bs_bl_waiting_add_file  = State()
    bs_bl_waiting_del_file  = State()
    # База пользователей: ручное добавление/удаление
    bs_base_waiting_add     = State()
    bs_base_waiting_del     = State() # also used for User Search

    # Карточка пользователя
    bs_um_waiting_pm        = State()
    
    # Обработка заявок (проценты)
    waiting_for_req_percent = State()


# ══════════════════════════════════════════════════════════════
# Обработка заявок
# ══════════════════════════════════════════════════════════════

# Цикл интервалов отложенного принятия (по ТЗ)
_DELAY_CYCLE = [0, 1, 5, 15, 30, 60, 180, 360, 720, 1080, 1440]


def _delay_label(minutes: int) -> str:
    if minutes == 0:
        return "ВЫКЛ 🔴"
    if minutes < 60:
        return f"{minutes} мин 🟡"
    hours = minutes // 60
    return f"{hours} ч 🟡"


def _next_delay(current: int) -> int:
    """Следующий интервал в цикле ТЗ."""
    try:
        idx = _DELAY_CYCLE.index(current)
    except ValueError:
        idx = 0
    return _DELAY_CYCLE[(idx + 1) % len(_DELAY_CYCLE)]


async def _show_requests_menu(callback: CallbackQuery, platform_user: dict, chat_id: int):
    """Рендерит экран Обработка заявок по ТЗ."""
    owner_id = platform_user["user_id"]
    ch = await db.fetchrow(
        "SELECT * FROM bot_chats WHERE owner_id=$1 AND chat_id=$2::bigint",
        owner_id, chat_id,
    )
    if not ch:
        await callback.answer("Площадка не найдена", show_alert=True)
        return

    # ── Синхронизация: очищаем устаревшие pending-заявки перед подсчётом ──
    # 1. Старше 30 дней — Telegram их авто-отклоняет
    await db.execute(
        "UPDATE join_requests SET status='expired', resolved_at=now() "
        "WHERE owner_id=$1 AND chat_id=$2::bigint AND status IN ('pending','captcha_verified') "
        "AND requested_at < now() - interval '30 days'",
        owner_id, chat_id,
    )
    # 2. Если автопринятие с задержкой включено — чистим зависшие задачи
    #    (возникают при перезапуске бота: asyncio.create_task теряется)
    auto   = ch["autoaccept"]
    delay  = ch["autoaccept_delay"] or 0
    if auto and delay > 0:
        from datetime import timedelta
        await db.execute(
            "UPDATE join_requests SET status='approved', resolved_at=now() "
            "WHERE owner_id=$1 AND chat_id=$2::bigint AND status='pending' "
            "AND requested_at < now() - $3::interval",
            owner_id, chat_id, timedelta(minutes=delay),
        )

    # Актуальное кол-во ожидающих заявок (свежий запрос после очистки)
    pending = await db.fetchval(
        "SELECT COUNT(*) FROM join_requests WHERE owner_id=$1 AND chat_id=$2::bigint AND status IN ('pending','captcha_verified')",
        owner_id, chat_id,
    ) or 0

    auto_label  = "✅ Автопринятие: ВКЛ 🟢"  if auto else "☑️ Автопринятие: ВЫКЛ 🔴"
    delay_label = f"⏰ Отложенное: {_delay_label(delay)}"

    text = (
        "✅ <b>Обработка заявок</b>\n\n"
        f"🔍 Заявок в очереди: <b>{pending}</b>\n\n"
        "💡 <i>Автопринятие</i> — заявки одобряются автоматически.\n"
        "   Бот проверяет каждого по чёрному списку.\n\n"
        "⏰ <i>Отложенное принятие</i> — заявка принимается\n"
        "   через заданное время. Используется для прогрева."
    )

    await navigate(
        callback,
        text,
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text=auto_label,   callback_data=f"req_auto_toggle:{chat_id}")],
            [
                InlineKeyboardButton(text="✔️ Принять",   callback_data=f"req_decide:accept:ch:{chat_id}"),
                InlineKeyboardButton(text="✖️ Отклонить", callback_data=f"req_decide:decline:ch:{chat_id}"),
            ],
            [InlineKeyboardButton(text=delay_label,  callback_data=f"req_delay_cycle:{chat_id}")],
            [InlineKeyboardButton(text="◀️ Назад",    callback_data=f"channel_by_chat:{chat_id}")],
        ]),
    )


@router.callback_query(F.data.startswith("ch_requests:"))
async def on_requests_menu(callback: CallbackQuery, platform_user: dict | None):
    if not platform_user:
        return
    chat_id = int(callback.data.split(":")[1])
    await _show_requests_menu(callback, platform_user, chat_id)


@router.callback_query(F.data.startswith("req_auto_toggle:"))
async def on_req_auto_toggle(callback: CallbackQuery, platform_user: dict | None):
    if not platform_user:
        return
    chat_id = int(callback.data.split(":")[1])
    ch = await db.fetchrow(
        "SELECT autoaccept FROM bot_chats WHERE owner_id=$1 AND chat_id=$2::bigint",
        platform_user["user_id"], chat_id,
    )
    if not ch:
        return
    new_auto = not ch["autoaccept"]
    await db.execute(
        "UPDATE bot_chats SET autoaccept=$1 WHERE owner_id=$2 AND chat_id=$3::bigint",
        new_auto, platform_user["user_id"], chat_id,
    )
    await callback.answer("✅ ВКЛ" if new_auto else "🔴 ВЫКЛ")
    await _show_requests_menu(callback, platform_user, chat_id)


@router.callback_query(F.data.startswith("req_delay_cycle:"))
async def on_req_delay_cycle(callback: CallbackQuery, platform_user: dict | None):
    if not platform_user:
        return
    chat_id = int(callback.data.split(":")[1])
    ch = await db.fetchrow(
        "SELECT autoaccept_delay FROM bot_chats WHERE owner_id=$1 AND chat_id=$2::bigint",
        platform_user["user_id"], chat_id,
    )
    current = ch["autoaccept_delay"] or 0 if ch else 0
    new_delay = _next_delay(current)
    await db.execute(
        "UPDATE bot_chats SET autoaccept_delay=$1 WHERE owner_id=$2 AND chat_id=$3::bigint",
        new_delay, platform_user["user_id"], chat_id,
    )
    await callback.answer(f"⏰ {_delay_label(new_delay)}")
    await _show_requests_menu(callback, platform_user, chat_id)


@router.callback_query(F.data.startswith("req_decide:"))
async def on_req_decide(callback: CallbackQuery, state: FSMContext, platform_user: dict | None):
    if not platform_user:
        return
    # req_decide:accept:ch:1234  or req_decide:decline:bs:56
    _, action, scope, target_id_str = callback.data.split(":")
    target_id = int(target_id_str)
    owner_id = platform_user["user_id"]
    
    if scope == "ch":
        pending = await db.fetchval(
            "SELECT COUNT(*) FROM join_requests WHERE owner_id=$1 AND chat_id=$2::bigint AND status IN ('pending','captcha_verified')",
            owner_id, target_id,
        ) or 0
    else:
        pending = await db.fetchval(
            "SELECT COUNT(*) FROM join_requests jr JOIN bot_chats bc ON jr.chat_id=bc.chat_id AND jr.owner_id=bc.owner_id WHERE bc.child_bot_id=$1 AND bc.owner_id=$2 AND jr.status IN ('pending','captcha_verified')",
            target_id, owner_id,
        ) or 0

    if pending == 0:
        await callback.answer("Нет заявок в очереди", show_alert=True)
        return

    await state.set_state(SettingsFSM.waiting_for_req_percent)
    await state.update_data(
        req_action=action,
        req_scope=scope,
        req_target_id=target_id,
        req_max_count=pending,
        req_sel_count=0,
        req_sel_pct=0
    )
    await _show_req_percent_menu(callback, pending, 0, action, scope, target_id, sel_pct=0)


async def _show_req_percent_menu(callback: CallbackQuery, max_count: int, sel_count: int, action: str, scope: str, target_id: int, sel_pct: int = 0):
    title = "заявок для принятия" if action == "accept" else "заявок для отклонения"
    action_verb = "Принять" if action == "accept" else "Отклонить"
    action_icon = "✅" if action == "accept" else "❌"
    
    text = (
        f"🔍 <b>{title.capitalize()}</b>\n\n"
        f"👤 Доступно: {max_count}\n\n"
        f"🫂 Выбрано: {sel_count}\n\n"
        f"Выберите необходимый процент заявок или отправьте своё значение и нажмите кнопку \"{action_icon} {action_verb}\""
    )
    
    back_cb = f"ch_requests:{target_id}" if scope == "ch" else f"bs_requests:{target_id}"

    def _btn_text(pct: int, label: str) -> str:
        return f"🔵 {label}" if sel_pct == pct else label

    kb = InlineKeyboardMarkup(inline_keyboard=[
        [
            InlineKeyboardButton(text=_btn_text(10, "10%"), callback_data="req_pct:10"),
            InlineKeyboardButton(text=_btn_text(25, "25%"), callback_data="req_pct:25"),
        ],
        [
            InlineKeyboardButton(text=_btn_text(50, "50%"), callback_data="req_pct:50"),
            InlineKeyboardButton(text=_btn_text(75, "75%"), callback_data="req_pct:75"),
        ],
        [InlineKeyboardButton(text=_btn_text(100, "Все: 100%"), callback_data="req_pct:100")],
        [InlineKeyboardButton(text=f"{action_icon} {action_verb}", callback_data="req_confirm")],
        [InlineKeyboardButton(text="◀️ Назад", callback_data=back_cb)],
    ])
    
    await navigate(callback, text, reply_markup=kb)


@router.callback_query(F.data.startswith("req_pct:"))
async def on_req_pct(callback: CallbackQuery, state: FSMContext):
    data = await state.get_data()
    if not data or "req_action" not in data:
        await callback.answer("Сессия истекла", show_alert=True)
        return
    
    pct = int(callback.data.split(":")[1])
    max_c = data["req_max_count"]
    sel_c = max(1, int(max_c * (pct / 100.0))) if pct < 100 else max_c
    
    await state.update_data(req_sel_count=sel_c, req_sel_pct=pct)
    await _show_req_percent_menu(callback, max_c, sel_c, data["req_action"], data["req_scope"], data["req_target_id"], sel_pct=pct)


@router.message(SettingsFSM.waiting_for_req_percent)
async def on_req_custom_count(message: Message, state: FSMContext):
    data = await state.get_data()
    if not data or "req_action" not in data:
        return
    
    try:
        val = int(message.text.strip())
    except ValueError:
        await message.answer("Пожалуйста, отправьте число.")
        return
        
    max_c = data["req_max_count"]
    sel_c = max(0, min(val, max_c))
    
    # Custom input clears the percentage selection
    await state.update_data(req_sel_count=sel_c, req_sel_pct=0)
    
    class FakeCallback:
        message = message
        bot = message.bot
        async def answer(self, *args, **kwargs): pass
        
    await _show_req_percent_menu(FakeCallback(), max_c, sel_c, data["req_action"], data["req_scope"], data["req_target_id"], sel_pct=0)


@router.callback_query(F.data == "req_confirm")
async def on_req_confirm(callback: CallbackQuery, bot: Bot, state: FSMContext, platform_user: dict | None):
    if not platform_user:
        return
    data = await state.get_data()
    if not data or "req_action" not in data:
        await callback.answer("Сессия истекла", show_alert=True)
        return
        
    sel_c = data.get("req_sel_count", 0)
    if sel_c <= 0:
        await callback.answer("Выберите количество заявок (процент или число)", show_alert=True)
        return
        
    action = data["req_action"]
    scope = data["req_scope"]
    target_id = data["req_target_id"]
    owner_id = platform_user["user_id"]
    
    await state.clear()
    
    await callback.message.edit_text(f"⏳ Обрабатывается {sel_c} заявок...")
    
    if scope == "ch":
        chat_ids = [target_id]
        bot_row = await db.fetchrow(
            "SELECT cb.token_encrypted FROM child_bots cb JOIN bot_chats bc ON bc.child_bot_id = cb.id WHERE bc.owner_id=$1 AND bc.chat_id=$2::bigint",
            owner_id, target_id,
        )
    else:
        chat_rows = await db.fetch("SELECT chat_id FROM bot_chats WHERE owner_id=$1 AND child_bot_id=$2", owner_id, target_id)
        chat_ids = [r["chat_id"] for r in chat_rows]
        bot_row = await db.fetchrow("SELECT token_encrypted FROM child_bots WHERE id=$1", target_id)

    from aiogram import Bot as AioBot
    from services.security import decrypt_token
    child_bot = AioBot(token=decrypt_token(bot_row["token_encrypted"])) if bot_row else bot

    total_processed = 0
    
    for c_id in chat_ids:
        if total_processed >= sel_c:
            break
            
        rem = sel_c - total_processed
        
        pending_full = await db.fetch(
            "SELECT jr.user_id, jr.first_name, COALESCE(bu.language_code,'') AS language_code, COALESCE(bu.is_premium, false) AS is_premium "
            "FROM join_requests jr LEFT JOIN bot_users bu ON bu.user_id=jr.user_id AND bu.chat_id=$2::bigint "
            "WHERE jr.owner_id=$1 AND jr.chat_id=$2::bigint AND jr.status IN ('pending','captcha_verified') ORDER BY jr.status DESC, jr.requested_at ASC LIMIT $3",
            owner_id, c_id, rem
        )
        
        link_rows = await db.fetch("SELECT link FROM invite_links WHERE chat_id=$1::bigint AND link_type='request' AND is_active=true", c_id)
        invite_link_url = link_rows[0]["link"] if len(link_rows) == 1 else None

        # Fetch settings once per chat for the welcome message
        settings_row = await db.fetchrow("SELECT * FROM bot_chats WHERE owner_id=$1 AND chat_id=$2::bigint", owner_id, c_id)

        for row in pending_full:
            try:
                if action == "accept":
                    await child_bot.approve_chat_join_request(c_id, row["user_id"])
                    await db.execute("UPDATE join_requests SET status='approved', resolved_at=now() WHERE owner_id=$1 AND chat_id=$2::bigint AND user_id=$3", owner_id, c_id, row["user_id"])
                    
                    if invite_link_url:
                        from scheduler.child_bot_runner import _track_invite_link
                        class _FakeUser:
                            id = row["user_id"]
                            first_name = row["first_name"] or ""
                            last_name = None
                            language_code = row["language_code"] or None
                            is_premium = row["is_premium"]
                            username = None
                        try:
                            await _track_invite_link(invite_link_url, _FakeUser())
                        except Exception: pass
                    
                    if settings_row:
                        from handlers.join_requests import _register_user, _send_welcome
                        class _FakeUser2:
                            id = row["user_id"]
                            first_name = row["first_name"] or ""
                            last_name = None
                            language_code = row["language_code"] or None
                            is_premium = row["is_premium"]
                            username = None
                        await _register_user(owner_id, c_id, _FakeUser2())
                        await _send_welcome(child_bot, c_id, _FakeUser2(), dict(settings_row))
                else:
                    await child_bot.decline_chat_join_request(c_id, row["user_id"])
                    await db.execute("UPDATE join_requests SET status='declined', resolved_at=now() WHERE owner_id=$1 AND chat_id=$2::bigint AND user_id=$3", owner_id, c_id, row["user_id"])
                
                total_processed += 1
            except Exception:
                await db.execute("UPDATE join_requests SET status='expired', resolved_at=now() WHERE owner_id=$1 AND chat_id=$2::bigint AND user_id=$3", owner_id, c_id, row["user_id"])
                pass

    if bot_row:
        await child_bot.session.close()

    verb = "Принято" if action == "accept" else "Отклонено"
    icon = "✔️" if action == "accept" else "✖️"
    await callback.answer(f"{icon} {verb}: {total_processed}", show_alert=True)
    
    if scope == "ch":
        callback.data = f"ch_requests:{target_id}"
        await on_requests_menu(callback, platform_user)
    else:
        callback.data = f"bs_requests:{target_id}"
        from handlers.channel_settings import on_bs_requests
        await on_bs_requests(callback, platform_user)



# ══════════════════════════════════════════════════════════════
# Настройки капчи
# ══════════════════════════════════════════════════════════════
def kb_captcha_settings(ch: dict) -> InlineKeyboardMarkup:
    chat_id = ch["chat_id"]
    delete_label = "✅ Авто-удаление сообщ" if ch.get("captcha_delete") else "☐ Авто-удаление сообщ"
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text=f"⏱ Таймер: {ch.get('captcha_timer',60)} сек", callback_data=f"captcha_timer:{chat_id}")],
        [InlineKeyboardButton(text="✏️ Изменить текст капчи", callback_data=f"captcha_text:{chat_id}")],
        [InlineKeyboardButton(text=delete_label,              callback_data=f"captcha_delete:{chat_id}")],
        [InlineKeyboardButton(text="◀️ Назад",                callback_data=f"ch_requests:{chat_id}")],
    ])


@router.callback_query(F.data.startswith("captcha_settings:"))
async def on_captcha_settings(callback: CallbackQuery, platform_user: dict | None):
    if not platform_user:
        return
    chat_id = int(callback.data.split(":")[1])
    ch = await db.fetchrow(
        "SELECT * FROM bot_chats WHERE owner_id=$1 AND chat_id=$2::bigint",
        platform_user["user_id"], chat_id,
    )
    if not ch:
        return

    tariff = platform_user["tariff"]
    pro_note = "" if tariff in ("pro", "business") else "\n\n⚠️ Авто-удаление сообщений капчи — только Про+"

    await navigate(
        callback,
        f"🔑 <b>Настройки капчи</b>{pro_note}",
        reply_markup=kb_captcha_settings(dict(ch)),
    )


@router.callback_query(F.data.startswith("captcha_delete:"))
async def on_captcha_delete_toggle(callback: CallbackQuery, platform_user: dict | None):
    if not platform_user:
        return
    if platform_user["tariff"] not in ("pro", "business"):
        await callback.answer("Авто-удаление сообщений капчи — только Про+", show_alert=True)
        return
    chat_id = int(callback.data.split(":")[1])
    ch = await db.fetchrow(
        "SELECT captcha_delete FROM bot_chats WHERE owner_id=$1 AND chat_id=$2::bigint",
        platform_user["user_id"], chat_id,
    )
    new_val = not (ch["captcha_delete"] if ch else False)
    await db.execute(
        "UPDATE bot_chats SET captcha_delete=$1 WHERE owner_id=$2 AND chat_id=$3::bigint",
        new_val, platform_user["user_id"], chat_id,
    )
    await callback.answer("✅ Изменено")
    # Обновляем экран
    callback.data = f"captcha_settings:{chat_id}"
    await on_captcha_settings(callback, platform_user)


@router.callback_query(F.data.startswith("captcha_timer:"))
async def on_captcha_timer(callback: CallbackQuery, state: FSMContext, platform_user: dict | None):
    if not platform_user:
        return
    chat_id = callback.data.split(":")[1]
    await state.set_state(SettingsFSM.waiting_for_captcha_timer)
    await state.update_data(chat_id=int(chat_id), owner_id=platform_user["user_id"])
    await navigate(
        callback,
        "⏱ <b>Таймер капчи</b>\n\n"
        "Сколько секунд есть у пользователя для прохождения?\n"
        "Минимум 30, максимум 600 сек.",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="30 сек",  callback_data="timer_preset:30")],
            [InlineKeyboardButton(text="60 сек",  callback_data="timer_preset:60")],
            [InlineKeyboardButton(text="120 сек", callback_data="timer_preset:120")],
            [InlineKeyboardButton(text="🚫 Отмена", callback_data=f"captcha_settings:{chat_id}")],
        ]),
    )


@router.callback_query(F.data.startswith("timer_preset:"))
async def on_timer_preset(callback: CallbackQuery, state: FSMContext):
    data = await state.get_data()
    secs = int(callback.data.split(":")[1])
    await db.execute(
        "UPDATE bot_chats SET captcha_timer=$1 WHERE owner_id=$2 AND chat_id=$3::bigint",
        secs, data["owner_id"], data["chat_id"],
    )
    await state.clear()
    await callback.answer(f"✅ Таймер: {secs} сек")
    callback.data = f"captcha_settings:{data['chat_id']}"
    fake_pu = {"user_id": data["owner_id"], "tariff": "pro"}
    await on_captcha_settings(callback, fake_pu)


@router.callback_query(F.data.startswith("captcha_text:"))
async def on_captcha_text(callback: CallbackQuery, state: FSMContext, platform_user: dict | None):
    if not platform_user:
        return
    chat_id = callback.data.split(":")[1]
    await state.set_state(SettingsFSM.waiting_for_captcha_text)
    await state.update_data(chat_id=int(chat_id), owner_id=platform_user["user_id"])
    await navigate(
        callback,
        "✏️ <b>Текст капчи</b>\n\n"
        "Отправьте новый текст. Можно использовать переменные:\n"
        "• <code>{name}</code> — имя пользователя\n"
        "• <code>{channel}</code> — название канала",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="🚫 Отмена", callback_data=f"captcha_settings:{chat_id}")]
        ]),
    )


@router.message(SettingsFSM.waiting_for_captcha_text)
async def on_captcha_text_input(message: Message, state: FSMContext):
    data = await state.get_data()
    text = sanitize(message.text, max_len=512)
    await db.execute(
        "UPDATE bot_chats SET captcha_text=$1 WHERE owner_id=$2 AND chat_id=$3::bigint",
        text, data["owner_id"], data["chat_id"],
    )
    await state.clear()
    await message.answer("✅ Текст капчи сохранён")





# ═══════════════════════════════════════════════════════════════
# ПРОДВИНУТЫЙ РЕДАКТОР: Приветствие и Прощание
# ═══════════════════════════════════════════════════════════════

_MSG_FIELDS = {
    "welcome": {
        "label": "👋 Приветствие",
        "text_col": "welcome_text",
        "media_col": "welcome_media",
        "media_type_col": "welcome_media_type",
        "buttons_col": "welcome_buttons",
        "preview_col": "welcome_preview",
        "timer_col": "welcome_timer",
        "media_below_col": "welcome_media_below",
    },
    "farewell": {
        "label": "🤚 Прощание",
        "text_col": "farewell_text",
        "media_col": "farewell_media",
        "media_type_col": "farewell_media_type",
        "buttons_col": "farewell_buttons",
        "preview_col": "farewell_preview",
        "timer_col": "farewell_timer",
        "media_below_col": "farewell_media_below",
    },
}

_MSG_TIMER_CYCLE = [0, 5, 15, 30, 60, 120, 300]  # секунды

# Кэш message_id эхо-сообщения: (owner_id, chat_id_str, msg_type) -> message_id
# Используется чтобы надёжно удалить эхо при нажатии «◀️ Назад»
_echo_msg_ids: dict = {}

import json as _json


def _timer_label(v: int) -> str:
    if not v:
        return "0 сек"
    if v < 60:
        return f"{v} сек"
    return f"{v // 60} мин"


async def _get_chat_by_id(owner_id: int, chat_id: int):
    return await db.fetchrow(
        "SELECT * FROM bot_chats WHERE owner_id=$1 AND chat_id=$2::bigint",
        owner_id, chat_id,
    )


async def _get_chat_by_bot(owner_id: int, child_bot_id: int):
    return await db.fetchrow(
        "SELECT * FROM bot_chats WHERE owner_id=$1 AND child_bot_id=$2 LIMIT 1",
        owner_id, child_bot_id,
    )


def _build_editor_kb(chat_id_str: str, msg_type: str, ch: dict, scope: str = "ch") -> InlineKeyboardMarkup:
    """Строим клавиатуру редактора (6 кнопок + Назад)."""
    f = _MSG_FIELDS[msg_type]
    buttons_raw = ch.get(f["buttons_col"])
    media_fid = ch.get(f["media_col"])
    preview_on = bool(ch.get(f["preview_col"], False))
    timer_val = int(ch.get(f["timer_col"]) or 0)
    media_below = bool(ch.get(f["media_below_col"], False))

    if media_fid:
        media_icon = "⬇️" if media_below else "⬆️"
        media_label = f"🎬 Медиа: {media_icon}"
    else:
        media_label = "🎬 Медиа: нет"
    preview_label = f"👁 Превью: {'да' if preview_on else 'нет'}"
    timer_label_txt = f"⏱ Таймер: {_timer_label(timer_val)}"

    pfx = f"{scope}_msg"  # ch_msg or bs_msg
    # Используем ch_msg_back чтобы удалить эхо-сообщение при нажатии «Назад»
    back_cb = f"ch_msg_back:{chat_id_str}:{msg_type}" if scope == "ch" else f"bs_messages:{chat_id_str}"

    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="✏️ Редактировать",  callback_data=f"{pfx}_edit:{chat_id_str}:{msg_type}")],
        [InlineKeyboardButton(text="🎛 Кнопки",          callback_data=f"{pfx}_btns:{chat_id_str}:{msg_type}")],
        [InlineKeyboardButton(text=media_label,           callback_data=f"{pfx}_media:{chat_id_str}:{msg_type}")],
        [InlineKeyboardButton(text=preview_label,         callback_data=f"{pfx}_preview:{chat_id_str}:{msg_type}")],
        [InlineKeyboardButton(text=timer_label_txt,       callback_data=f"{pfx}_timer:{chat_id_str}:{msg_type}")],
        [InlineKeyboardButton(text="🗑 Удалить",          callback_data=f"{pfx}_del:{chat_id_str}:{msg_type}")],
        [InlineKeyboardButton(text="◀️ Назад",            callback_data=back_cb)],
    ])


async def _show_msg_editor(callback: CallbackQuery, chat_id_str: str, msg_type: str,
                            ch: dict, scope: str = "ch",
                            state: FSMContext | None = None):
    """Экран редактора: сначала эхо текущего сообщения, затем меню."""
    f = _MSG_FIELDS[msg_type]
    label = f["label"]
    text = ch.get(f["text_col"]) or ""
    media_fid = ch.get(f["media_col"])
    media_type = ch.get(f["media_type_col"])
    buttons_raw = ch.get(f["buttons_col"])

    # Строим reply_markup из сохранённых кнопок
    inline_rows = []
    if buttons_raw:
        btns = buttons_raw if isinstance(buttons_raw, list) else _json.loads(buttons_raw)
        for btn in btns:
            inline_rows.append([InlineKeyboardButton(text=btn["text"], url=btn.get("url", ""))])

    editor_kb = _build_editor_kb(chat_id_str, msg_type, ch, scope)

    if not text and not media_fid:
        # Сообщение ещё не задано — показываем приглашение ввести
        await _show_msg_prompt(callback, chat_id_str, msg_type, scope)
        return

    # Отправляем эхо сообщения пользователю, затем меню
    user_msg_kb = InlineKeyboardMarkup(inline_keyboard=inline_rows) if inline_rows else None

    try:
        await callback.message.delete()
    except Exception:
        pass

    # Отправить эхо сообщения и запомнить его message_id для последующего удаления
    sent_echo = None
    if media_fid:
        media_below = bool(ch.get(f["media_below_col"], False))
        kwargs = {
            "caption": text or None,
            "reply_markup": user_msg_kb,
            "parse_mode": "HTML",
            "show_caption_above_media": media_below,
        }
        if media_type == "photo":
            sent_echo = await callback.message.answer_photo(media_fid, **kwargs)
        elif media_type == "video":
            sent_echo = await callback.message.answer_video(media_fid, **kwargs)
        elif media_type == "animation":
            sent_echo = await callback.message.answer_animation(media_fid, **kwargs)
        else:
            sent_echo = await callback.message.answer_document(media_fid, **kwargs)
    else:
        sent_echo = await callback.message.answer(text, reply_markup=user_msg_kb,
                                      parse_mode="HTML",
                                      disable_web_page_preview=not bool(ch.get(f["preview_col"])))

    # Сохраняем message_id эхо в FSM state (надёжно — переживает рестарты сервера)
    if sent_echo and state:
        try:
            await state.update_data(
                editor_echo_mid=sent_echo.message_id,
                editor_echo_chat_id=callback.message.chat.id,
            )
        except Exception:
            pass
    # Также обновляем in-memory cache как запасной вариант
    if sent_echo:
        _echo_msg_ids[(chat_id_str, msg_type)] = (sent_echo.message_id, callback.message.chat.id)

    # Отправить меню редактора под сообщением
    await callback.message.answer(
        f"<b>{label}</b>",
        reply_markup=editor_kb,
        parse_mode="HTML",
    )
    await callback.answer()


async def _show_msg_prompt(callback: CallbackQuery, chat_id_str: str, msg_type: str, scope: str = "ch", cancel_cb: str | None = None):
    """Экран-приглашение ввести текст (скрин 3)."""
    emoji = "👋" if msg_type == "welcome" else "🤚"
    subject = "новые подписчики при подаче заявки" if msg_type == "welcome" else "отписавшиеся участники"
    # cancel_cb задаётся снаружи: если редактируем — возврат на скрин 2, если сообщения нет — на скрин 1
    if cancel_cb is None:
        if scope == "ch":
            cancel_cb = f"welcome_set:{chat_id_str}" if msg_type == "welcome" else f"farewell_set:{chat_id_str}"
        else:
            cancel_cb = f"bs_messages:{chat_id_str}"

    await navigate(
        callback,
        f"<b>{emoji} Пришлите сообщение, которое будут получать {subject}.</b>\n\n"
        "<b>Переменные:</b>\n"
        "├ Имя: <code>{name}</code>\n"
        "├ ФИО: <code>{allname}</code>\n"
        "├ Юзер: <code>{username}</code>\n"
        "├ Площадка: <code>{chat}</code>\n"
        "└ Текущая дата: <code>{day}</code>\n\n"
        "ⓘ Можно прикрепить медиа.",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="◀️ Отмена", callback_data=cancel_cb)],
        ]),
    )


# ─────────────────────── Кнопка "Приветствие" ──────────────────────────


@router.callback_query(F.data.startswith("welcome_set:"))
async def on_welcome_set(callback: CallbackQuery, state: FSMContext, platform_user: dict | None):
    if not platform_user:
        return
    chat_id_str = callback.data.split(":")[1]
    ch = await _get_chat_by_id(platform_user["user_id"], int(chat_id_str))
    if ch and (ch.get("welcome_text") or ch.get("welcome_media")):
        await _show_msg_editor(callback, chat_id_str, "welcome", dict(ch), scope="ch", state=state)
    else:
        await state.set_state(SettingsFSM.waiting_for_welcome_text)
        await state.update_data(chat_id=int(chat_id_str), owner_id=platform_user["user_id"],
                                 msg_type="welcome", scope="ch")
        # Сообщения нет — «Отмена» возвращает на экран Сообщений (скрин 1), а не на редактор
        await _show_msg_prompt(callback, chat_id_str, "welcome", scope="ch",
                               cancel_cb=f"ch_messages:{chat_id_str}")


@router.callback_query(F.data.startswith("farewell_set:"))
async def on_farewell_set(callback: CallbackQuery, state: FSMContext, platform_user: dict | None):
    if not platform_user:
        return
    chat_id_str = callback.data.split(":")[1]
    ch = await _get_chat_by_id(platform_user["user_id"], int(chat_id_str))
    if ch and (ch.get("farewell_text") or ch.get("farewell_media")):
        await _show_msg_editor(callback, chat_id_str, "farewell", dict(ch), scope="ch", state=state)
    else:
        await state.set_state(SettingsFSM.waiting_for_farewell_text)
        await state.update_data(chat_id=int(chat_id_str), owner_id=platform_user["user_id"],
                                 msg_type="farewell", scope="ch")
        # Сообщения нет — «Отмена» возвращает на экран Сообщений (скрин 1), а не на редактор
        await _show_msg_prompt(callback, chat_id_str, "farewell", scope="ch",
                               cancel_cb=f"ch_messages:{chat_id_str}")


# ─────────────────────── FSM: ввод текста ──────────────────────────


async def _handle_msg_input(message: Message, state: FSMContext):
    """Общий обработчик ввода текста/медиа для приветствия и прощания."""
    data = await state.get_data()
    owner_id = data["owner_id"]
    msg_type = data.get("msg_type", "welcome")
    scope = data.get("scope", "ch")
    f = _MSG_FIELDS[msg_type]

    text = sanitize(message.text or message.caption or "", max_len=1024)

    # Определяем media
    media_fid = None
    media_type = None
    if message.photo:
        media_fid = message.photo[-1].file_id
        media_type = "photo"
    elif message.video:
        media_fid = message.video.file_id
        media_type = "video"
    elif message.animation:
        media_fid = message.animation.file_id
        media_type = "animation"
    elif message.document:
        media_fid = message.document.file_id
        media_type = "document"

    if scope == "ch":
        chat_id = data["chat_id"]
        await db.execute(
            f"UPDATE bot_chats SET {f['text_col']}=$1, {f['media_col']}=$2, {f['media_type_col']}=$3 "
            "WHERE owner_id=$4 AND chat_id=$5::bigint",
            text or None, media_fid, media_type, owner_id, chat_id,
        )
        chat_id_str = str(chat_id)
    else:
        child_bot_id = data["child_bot_id"]
        await db.execute(
            f"UPDATE bot_chats SET {f['text_col']}=$1, {f['media_col']}=$2, {f['media_type_col']}=$3 "
            "WHERE child_bot_id=$4 AND owner_id=$5",
            text or None, media_fid, media_type, child_bot_id, owner_id,
        )
        chat_id_str = str(child_bot_id)

    await state.clear()

    ch = await (
        _get_chat_by_id(owner_id, int(chat_id_str))
        if scope == "ch"
        else _get_chat_by_bot(owner_id, int(chat_id_str))
    )

    label = _MSG_FIELDS[msg_type]["label"]

    # Эхо отправлено, запоминаем message_id в FSM state
    sent_echo = None
    if media_fid:
        if media_type == "photo":
            sent_echo = await message.answer_photo(media_fid, caption=text or None, parse_mode="HTML")
        elif media_type == "video":
            sent_echo = await message.answer_video(media_fid, caption=text or None, parse_mode="HTML")
        elif media_type == "animation":
            sent_echo = await message.answer_animation(media_fid, caption=text or None, parse_mode="HTML")
        else:
            sent_echo = await message.answer_document(media_fid, caption=text or None, parse_mode="HTML")
    else:
        sent_echo = await message.answer(text, parse_mode="HTML")

    # Сохраняем echo_mid в FSM state (читаем потом в on_ch_msg_back)
    if sent_echo:
        await state.update_data(
            editor_echo_mid=sent_echo.message_id,
            editor_echo_chat_id=message.chat.id,
        )
        _echo_msg_ids[(chat_id_str, msg_type)] = (sent_echo.message_id, message.chat.id)

    # Меню редактора
    await message.answer(
        f"<b>{label}</b>",
        reply_markup=_build_editor_kb(chat_id_str, msg_type, dict(ch) if ch else {}, scope),
        parse_mode="HTML",
    )


@router.message(SettingsFSM.waiting_for_welcome_text)
async def on_welcome_input(message: Message, state: FSMContext):
    await _handle_msg_input(message, state)


@router.message(SettingsFSM.waiting_for_farewell_text)
async def on_farewell_input(message: Message, state: FSMContext):
    await _handle_msg_input(message, state)


# ─────────────────────── ch_msg_* actions ──────────────────────────


@router.callback_query(F.data.startswith("ch_msg_edit:"))
async def on_ch_msg_edit(callback: CallbackQuery, state: FSMContext, platform_user: dict | None):
    if not platform_user:
        return
    _, chat_id_str, msg_type = callback.data.split(":")

    # Удаляем эхо текущего сообщения (отображается над меню скрин 2) перед показом формы ввода
    fsm_data = await state.get_data()
    echo_mid = fsm_data.get("editor_echo_mid")
    echo_chat_id = fsm_data.get("editor_echo_chat_id") or callback.message.chat.id
    if not echo_mid:
        cached = _echo_msg_ids.pop((chat_id_str, msg_type), None)
        if cached:
            echo_mid, echo_chat_id = cached
    if echo_mid:
        try:
            await callback.bot.delete_message(chat_id=echo_chat_id, message_id=echo_mid)
        except Exception:
            pass

    await state.set_state(
        SettingsFSM.waiting_for_welcome_text if msg_type == "welcome"
        else SettingsFSM.waiting_for_farewell_text
    )
    await state.update_data(chat_id=int(chat_id_str), owner_id=platform_user["user_id"],
                             msg_type=msg_type, scope="ch")
    # Редактируем существующее сообщение — «Отмена» возвращает на скрин 2 (редактор)
    back_to_editor = f"welcome_set:{chat_id_str}" if msg_type == "welcome" else f"farewell_set:{chat_id_str}"
    await _show_msg_prompt(callback, chat_id_str, msg_type, scope="ch", cancel_cb=back_to_editor)


@router.callback_query(F.data.startswith("ch_msg_btns:"))
async def on_ch_msg_btns(callback: CallbackQuery, state: FSMContext, platform_user: dict | None):
    if not platform_user:
        return
    _, chat_id_str, msg_type = callback.data.split(":")

    # Удаляем эхо-сообщение перед показом экрана ввода кнопок
    fsm_data = await state.get_data()
    echo_mid = fsm_data.get("editor_echo_mid")
    echo_chat_id = fsm_data.get("editor_echo_chat_id") or callback.message.chat.id
    if not echo_mid:
        cached = _echo_msg_ids.pop((chat_id_str, msg_type), None)
        if cached:
            echo_mid, echo_chat_id = cached
    if echo_mid:
        try:
            await callback.bot.delete_message(chat_id=echo_chat_id, message_id=echo_mid)
        except Exception:
            pass

    await state.set_state(SettingsFSM.waiting_for_msg_buttons)
    await state.update_data(chat_id=int(chat_id_str), owner_id=platform_user["user_id"],
                             msg_type=msg_type, scope="ch")
    await callback.message.edit_text(
        "📎 Отправьте <b>кнопки</b>, которые будут добавлены к сообщению.\n\n"
        "🔗 <b>URL-кнопки</b>\n\n"
        "<b>Одна кнопка в ряду:</b>\n"
        "<code>Кнопка 1 — ссылка</code>\n"
        "<code>Кнопка 2 — ссылка</code>\n\n"
        "<b>Несколько кнопок в ряду:</b>\n"
        "<code>Кнопка 1 — ссылка | Кнопка 2 — ссылка</code>\n\n"
        "🎨 <b>Цветные кнопки (добавь emoji перед названием):</b>\n"
        "<code>🟦 Кнопка — ссылка</code> — синяя\n"
        "<code>🟩 Кнопка — ссылка</code> — зелёная\n"
        "<code>🟥 Кнопка — ссылка</code> — красная\n\n"
        "*** <b>Другие виды кнопок</b>\n\n"
        "<b>WebApp кнопки:</b>\n"
        "<code>Кнопка 1 — ссылка (webapp)</code>\n\n"
        "ℹ️ Нажмите, чтобы скопировать.",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="◀️ Отмена", callback_data=f"ch_msg_back:{chat_id_str}:{msg_type}")],
        ]),
        parse_mode="HTML",
    )
    await callback.answer()


@router.callback_query(F.data.startswith("ch_msg_media:"))
async def on_ch_msg_media(callback: CallbackQuery, state: FSMContext, platform_user: dict | None):
    if not platform_user:
        return
    _, chat_id_str, msg_type = callback.data.split(":")
    f = _MSG_FIELDS[msg_type]
    ch = await _get_chat_by_id(platform_user["user_id"], int(chat_id_str))
    has_media = ch and ch.get(f["media_col"])

    if has_media:
        # Медиа есть — переключаем позицию ⬆️/⬇️ и пересоздаем превью
        current_below = bool(ch.get(f["media_below_col"], False))
        new_below = not current_below
        await db.execute(
            f"UPDATE bot_chats SET {f['media_below_col']}=$1 WHERE owner_id=$2 AND chat_id=$3::bigint",
            new_below, platform_user["user_id"], int(chat_id_str),
        )
        ch = await _get_chat_by_id(platform_user["user_id"], int(chat_id_str))
        
        # In-place edit of the echo message
        try:
            # Resolve message id from state or cache
            echo_msg_id = None
            if state:
                state_data = await state.get_data()
                echo_msg_id = state_data.get("editor_echo_mid")
            
            if not echo_msg_id:
                cache_key = (chat_id_str, msg_type)
                cached = _echo_msg_ids.get(cache_key)
                if cached:
                    echo_msg_id = cached[0]
            
            if echo_msg_id:
                # Rebuild keyboard if necessary
                buttons_raw = ch.get(f["buttons_col"])
                inline_rows = []
                if buttons_raw:
                    import json as _json
                    btns = buttons_raw if isinstance(buttons_raw, list) else _json.loads(buttons_raw)
                    for btn in btns:
                        inline_rows.append([InlineKeyboardButton(text=btn["text"], url=btn.get("url", ""))])
                user_msg_kb = InlineKeyboardMarkup(inline_keyboard=inline_rows) if inline_rows else None
                
                await callback.bot.edit_message_caption(
                    chat_id=callback.message.chat.id,
                    message_id=echo_msg_id,
                    caption=(ch.get(f["text_col"]) or None),
                    parse_mode="HTML",
                    reply_markup=user_msg_kb,
                    show_caption_above_media=new_below
                )
        except Exception as e:
            pass
            
        icon = "⬇️" if new_below else "⬆️"
        await callback.answer(f"🎬 Медиа: {icon}")
        
        # update keyboard for the editor message itself
        await callback.message.edit_reply_markup(
            reply_markup=_build_editor_kb(chat_id_str, msg_type, dict(ch), scope="ch")
        )
    else:
        # Медиа нет
        await callback.answer("Медиа не прикреплено", show_alert=True)


@router.callback_query(F.data.startswith("ch_msg_preview:"))
async def on_ch_msg_preview(callback: CallbackQuery, platform_user: dict | None):
    if not platform_user:
        return
    _, chat_id_str, msg_type = callback.data.split(":")
    f = _MSG_FIELDS[msg_type]
    ch = await _get_chat_by_id(platform_user["user_id"], int(chat_id_str))
    new_val = not bool(ch.get(f["preview_col"], False) if ch else False)
    await db.execute(
        f"UPDATE bot_chats SET {f['preview_col']}=$1 WHERE owner_id=$2 AND chat_id=$3::bigint",
        new_val, platform_user["user_id"], int(chat_id_str),
    )
    await callback.answer(f"👁 Превью: {'да' if new_val else 'нет'}")
    ch = await _get_chat_by_id(platform_user["user_id"], int(chat_id_str))
    # Только обновляем кнопки меню на месте—без новых сообщений
    await callback.message.edit_reply_markup(
        reply_markup=_build_editor_kb(chat_id_str, msg_type, dict(ch), scope="ch")
    )


@router.callback_query(F.data.startswith("ch_msg_timer:"))
async def on_ch_msg_timer(callback: CallbackQuery, platform_user: dict | None):
    if not platform_user:
        return
    _, chat_id_str, msg_type = callback.data.split(":")
    f = _MSG_FIELDS[msg_type]
    ch = await _get_chat_by_id(platform_user["user_id"], int(chat_id_str))
    cur = int(ch.get(f["timer_col"]) or 0 if ch else 0)
    try:
        idx = _MSG_TIMER_CYCLE.index(cur)
    except ValueError:
        idx = 0
    new_val = _MSG_TIMER_CYCLE[(idx + 1) % len(_MSG_TIMER_CYCLE)]
    await db.execute(
        f"UPDATE bot_chats SET {f['timer_col']}=$1 WHERE owner_id=$2 AND chat_id=$3::bigint",
        new_val, platform_user["user_id"], int(chat_id_str),
    )
    await callback.answer(f"⏱ Таймер: {_timer_label(new_val)}")
    ch = await _get_chat_by_id(platform_user["user_id"], int(chat_id_str))
    # Только обновляем кнопки меню на месте—без новых сообщений
    await callback.message.edit_reply_markup(
        reply_markup=_build_editor_kb(chat_id_str, msg_type, dict(ch), scope="ch")
    )


@router.callback_query(F.data.startswith("ch_msg_del:"))
async def on_ch_msg_del(callback: CallbackQuery, platform_user: dict | None):
    if not platform_user:
        return
    _, chat_id_str, msg_type = callback.data.split(":")
    f = _MSG_FIELDS[msg_type]
    await db.execute(
        f"UPDATE bot_chats SET {f['text_col']}=NULL, {f['media_col']}=NULL, "
        f"{f['media_type_col']}=NULL, {f['buttons_col']}=NULL "
        "WHERE owner_id=$1 AND chat_id=$2::bigint",
        platform_user["user_id"], int(chat_id_str),
    )
    await callback.answer("🗑 Удалено")
    callback.data = f"ch_messages:{chat_id_str}"
    from handlers.messages import _show_ch_messages
    await _show_ch_messages(callback, int(chat_id_str), platform_user["user_id"])


@router.callback_query(F.data.startswith("ch_msg_back:"))
async def on_ch_msg_back(callback: CallbackQuery, state: FSMContext, platform_user: dict | None):
    if not platform_user:
        return
    _, chat_id_str, msg_type = callback.data.split(":")
    chat_id = int(chat_id_str)
    owner_id = platform_user["user_id"]

    # Приоритет: FSM state (переживает рестарты)
    fsm_data = await state.get_data()
    echo_mid = fsm_data.get("editor_echo_mid")
    echo_chat_id = fsm_data.get("editor_echo_chat_id") or callback.message.chat.id

    # Запасной вариант: in-memory cache
    if not echo_mid:
        cached = _echo_msg_ids.pop((chat_id_str, msg_type), None)
        if cached:
            echo_mid, echo_chat_id = cached

    await state.clear()

    # Удаляем эхо если нашли его ID
    if echo_mid:
        try:
            await callback.bot.delete_message(chat_id=echo_chat_id, message_id=echo_mid)
        except Exception:
            pass  # уже удалено или недоступно

    # Меню редактируем на месте в экран «Сообщения»
    from handlers.messages import _show_ch_messages
    await _show_ch_messages(callback, chat_id, owner_id)


# ─────────────────────── FSM: кнопки и медиа ──────────────────────────


@router.message(SettingsFSM.waiting_for_msg_buttons)
async def on_msg_buttons_input(message: Message, state: FSMContext):
    data = await state.get_data()
    owner_id = data["owner_id"]
    msg_type = data.get("msg_type", "welcome")
    scope = data.get("scope", "ch")
    f = _MSG_FIELDS[msg_type]

    raw = sanitize(message.text or "", max_len=2048)
    buttons = []
    for line in raw.splitlines():
        line = line.strip()
        if "—" in line:
            parts = line.split("—", 1)
        elif "-" in line:
            parts = line.split("-", 1)
        else:
            continue
        btn_text = parts[0].strip()
        btn_url = parts[1].strip()
        if btn_text and btn_url.startswith("http"):
            buttons.append({"text": btn_text, "url": btn_url})

    if not buttons:
        await message.answer(
            "⚠️ Не удалось распознать кнопки. Формат: <code>Текст — https://ссылка</code>",
            parse_mode="HTML",
        )
        return

    buttons_json = _json.dumps(buttons, ensure_ascii=False)
    if scope == "ch":
        chat_id = data["chat_id"]
        await db.execute(
            f"UPDATE bot_chats SET {f['buttons_col']}=$1::jsonb WHERE owner_id=$2 AND chat_id=$3::bigint",
            buttons_json, owner_id, chat_id,
        )
        chat_id_str = str(chat_id)
        ch = await _get_chat_by_id(owner_id, chat_id)
    else:
        child_bot_id = data["child_bot_id"]
        await db.execute(
            f"UPDATE bot_chats SET {f['buttons_col']}=$1::jsonb WHERE child_bot_id=$2 AND owner_id=$3",
            buttons_json, child_bot_id, owner_id,
        )
        chat_id_str = str(child_bot_id)
        ch = await _get_chat_by_bot(owner_id, child_bot_id)

    await state.clear()
    # Отправляем только одно меню-сообщение с обновлёнными кнопками (без дублирования эхо)
    await message.answer(
        f"✅ Добавлено {len(buttons)} кнопок. <b>{_MSG_FIELDS[msg_type]['label']}</b>",
        reply_markup=_build_editor_kb(chat_id_str, msg_type, dict(ch) if ch else {}, scope),
        parse_mode="HTML",
    )


@router.message(SettingsFSM.waiting_for_msg_media)
async def on_msg_media_input(message: Message, state: FSMContext):
    data = await state.get_data()
    owner_id = data["owner_id"]
    msg_type = data.get("msg_type", "welcome")
    scope = data.get("scope", "ch")
    f = _MSG_FIELDS[msg_type]

    media_fid = None
    media_type = None
    if message.photo:
        media_fid = message.photo[-1].file_id
        media_type = "photo"
    elif message.video:
        media_fid = message.video.file_id
        media_type = "video"
    elif message.animation:
        media_fid = message.animation.file_id
        media_type = "animation"
    elif message.document:
        media_fid = message.document.file_id
        media_type = "document"
    else:
        await message.answer("⚠️ Поддерживаются: фото, видео, GIF, документ.")
        return

    if scope == "ch":
        chat_id = data["chat_id"]
        await db.execute(
            f"UPDATE bot_chats SET {f['media_col']}=$1, {f['media_type_col']}=$2 "
            "WHERE owner_id=$3 AND chat_id=$4::bigint",
            media_fid, media_type, owner_id, chat_id,
        )
        chat_id_str = str(chat_id)
        ch = await _get_chat_by_id(owner_id, chat_id)
    else:
        child_bot_id = data["child_bot_id"]
        await db.execute(
            f"UPDATE bot_chats SET {f['media_col']}=$1, {f['media_type_col']}=$2 "
            "WHERE child_bot_id=$3 AND owner_id=$4",
            media_fid, media_type, child_bot_id, owner_id,
        )
        chat_id_str = str(child_bot_id)
        ch = await _get_chat_by_bot(owner_id, child_bot_id)

    await state.clear()
    # Отправляем только одно меню (без дублей эхо)
    await message.answer(
        f"🎬 Медиа сохранено. <b>{_MSG_FIELDS[msg_type]['label']}</b>",
        reply_markup=_build_editor_kb(chat_id_str, msg_type, dict(ch) if ch else {}, scope),
        parse_mode="HTML",
    )


# Legacy-обработчики для удаления (сохранены для обратной совместимости колбэков)
@router.callback_query(F.data.startswith("welcome_del:"))
async def on_welcome_del(callback: CallbackQuery, platform_user: dict | None):
    if not platform_user:
        return
    chat_id_str = callback.data.split(":")[1]
    callback.data = f"ch_msg_del:{chat_id_str}:welcome"
    await on_ch_msg_del(callback, platform_user)


@router.callback_query(F.data.startswith("farewell_del:"))
async def on_farewell_del(callback: CallbackQuery, platform_user: dict | None):
    if not platform_user:
        return
    chat_id_str = callback.data.split(":")[1]
    callback.data = f"ch_msg_del:{chat_id_str}:farewell"
    await on_ch_msg_del(callback, platform_user)


# ══════════════════════════════════════════════════════════════
# Реакции
# ══════════════════════════════════════════════════════════════
REACTIONS_POOL = ["👍", "🔥", "❤️", "🎉", "😂", "🤩", "😎", "💯", "⚡", "🤝", "💎", "🦋"]


def kb_reactions(current: list, chat_id: int, tariff: str) -> InlineKeyboardMarkup:
    max_reactions = 3 if tariff in ("pro", "business") else 1
    buttons = []
    row = []
    for emoji in REACTIONS_POOL:
        mark = "✅" if emoji in current else ""
        row.append(InlineKeyboardButton(
            text=f"{mark}{emoji}", callback_data=f"reaction_toggle:{chat_id}:{emoji}",
        ))
        if len(row) == 4:
            buttons.append(row)
            row = []
    if row:
        buttons.append(row)
    note = f"(макс. {max_reactions}: {', '.join(current) if current else 'не выбрано'})"
    buttons.append([InlineKeyboardButton(text=note, callback_data="noop")])
    buttons.append([InlineKeyboardButton(text="◀️ Назад", callback_data=f"ch_messages:{chat_id}")])
    return InlineKeyboardMarkup(inline_keyboard=buttons)


@router.callback_query(F.data.startswith("reactions_set:"))
async def on_reactions_set(callback: CallbackQuery, platform_user: dict | None):
    if not platform_user:
        return
    chat_id = int(callback.data.split(":")[1])
    ch = await db.fetchrow(
        "SELECT reaction_emojis FROM bot_chats WHERE owner_id=$1 AND chat_id=$2::bigint",
        platform_user["user_id"], chat_id,
    )
    current = list(ch["reaction_emojis"] or []) if ch else []
    tariff = platform_user["tariff"]
    await callback.message.edit_text(
        "😄 <b>Реакции</b>\n\nВыберите эмодзи для авто-реакций на сообщения:",
        reply_markup=kb_reactions(current, chat_id, tariff),
    )
    await callback.answer()


@router.callback_query(F.data.startswith("reaction_toggle:"))
async def on_reaction_toggle(callback: CallbackQuery, platform_user: dict | None):
    if not platform_user:
        return
    parts = callback.data.split(":")
    chat_id, emoji = int(parts[1]), parts[2]
    max_r = 3 if platform_user["tariff"] in ("pro", "business") else 1

    ch = await db.fetchrow(
        "SELECT reaction_emojis FROM bot_chats WHERE owner_id=$1 AND chat_id=$2::bigint",
        platform_user["user_id"], chat_id,
    )
    current = list(ch["reaction_emojis"] or []) if ch else []

    if emoji in current:
        current.remove(emoji)
    else:
        if len(current) >= max_r:
            await callback.answer(f"Максимум {max_r} реакции для вашего тарифа", show_alert=True)
            return
        current.append(emoji)

    await db.execute(
        "UPDATE bot_chats SET reaction_emojis=$1::text[] WHERE owner_id=$2 AND chat_id=$3::bigint",
        current, platform_user["user_id"], chat_id,
    )
    await callback.answer()
    callback.data = f"reactions_set:{chat_id}"
    await on_reactions_set(callback, platform_user)


# ══════════════════════════════════════════════════════════════
# Языковые фильтры и фильтры имён
# ══════════════════════════════════════════════════════════════
LANGUAGE_OPTIONS = {
    "ru": "RU 🇷🇺",
    "uk": "UK 🇺🇦",
    "by": "BY 🇧🇾",
    "uz": "UZ 🇺🇿",
    "kz": "KZ 🇰🇿",
    "az": "AZ 🇦🇿",
    "en": "EN 🇬🇧",
    "es": "ES 🇪🇸",
    "de": "DE 🇩🇪",
    "zh": "ZH 🇨🇳",
    "hi": "HI 🇮🇳",
    "ar": "AR 🇸🇦",
}


def kb_protection(ch: dict, blocked_langs: list) -> InlineKeyboardMarkup:
    chat_id  = ch["chat_id"]
    rtl      = "🔵" if ch.get("filter_rtl")        else "⚫"
    hiero    = "🔵" if ch.get("filter_hieroglyph")  else "⚫"
    no_photo = "🔵" if ch.get("filter_no_photo")    else "⚫"
    limit_on = "🔴" if ch.get("join_limit_enabled") else "🔴"
    buttons = [
        [InlineKeyboardButton(text=f"{limit_on} Лимиты",                  callback_data=f"filter_limits:{chat_id}")],
        [InlineKeyboardButton(text="⬛ Фильтр по языкам",                  callback_data=f"lang_filters:{chat_id}")],
        [InlineKeyboardButton(text=f"{rtl} RTL-символы в имени",           callback_data=f"filter_rtl:{chat_id}")],
        [InlineKeyboardButton(text=f"{hiero} Иероглифы в имени",           callback_data=f"filter_hier:{chat_id}")],
        [InlineKeyboardButton(text=f"{no_photo} Аккаунты без фото",        callback_data=f"filter_photo:{chat_id}")],
        [InlineKeyboardButton(text="◀️ Назад",                             callback_data=f"channel_by_chat:{chat_id}")],
    ]
    return InlineKeyboardMarkup(inline_keyboard=buttons)


@router.callback_query(F.data.startswith("ch_protection:"))
async def on_protection_menu_main(callback: CallbackQuery, platform_user: dict | None):
    """Перенаправляем на blacklist если нажали кнопку ЧС, иначе — настройки фильтров."""
    # Уже был on_protection_menu в blacklist.py — этот для настроек, не ЧС.
    # Разграничение: blacklist.py обрабатывает ch_protection через свой хендлер.
    # Здесь оставляем пустой pass — blacklist.py первее в роутере.
    pass


@router.callback_query(F.data.startswith("filter_rtl:"))
async def on_filter_rtl(callback: CallbackQuery, platform_user: dict | None):
    if not platform_user:
        return
    chat_id = int(callback.data.split(":")[1])
    ch = await db.fetchrow(
        "SELECT filter_rtl FROM bot_chats WHERE owner_id=$1 AND chat_id=$2::bigint",
        platform_user["user_id"], chat_id,
    )
    new_val = not (ch["filter_rtl"] if ch else False)
    await db.execute(
        "UPDATE bot_chats SET filter_rtl=$1 WHERE owner_id=$2 AND chat_id=$3::bigint",
        new_val, platform_user["user_id"], chat_id,
    )
    await callback.answer("RTL-фильтр: " + ("✅ Вкл" if new_val else "☐ Выкл"))


@router.callback_query(F.data.startswith("filter_hier:"))
async def on_filter_hier(callback: CallbackQuery, platform_user: dict | None):
    if not platform_user:
        return
    chat_id = int(callback.data.split(":")[1])
    ch = await db.fetchrow(
        "SELECT filter_hieroglyph FROM bot_chats WHERE owner_id=$1 AND chat_id=$2::bigint",
        platform_user["user_id"], chat_id,
    )
    new_val = not (ch["filter_hieroglyph"] if ch else False)
    await db.execute(
        "UPDATE bot_chats SET filter_hieroglyph=$1 WHERE owner_id=$2 AND chat_id=$3::bigint",
        new_val, platform_user["user_id"], chat_id,
    )
    await callback.answer("Иероглифы: " + ("✅ Вкл" if new_val else "☐ Выкл"))


@router.callback_query(F.data.startswith("filter_photo:"))
async def on_filter_photo(callback: CallbackQuery, platform_user: dict | None):
    if not platform_user:
        return
    chat_id = int(callback.data.split(":")[1])
    ch = await db.fetchrow(
        "SELECT filter_no_photo FROM bot_chats WHERE owner_id=$1 AND chat_id=$2::bigint",
        platform_user["user_id"], chat_id,
    )
    new_val = not (ch["filter_no_photo"] if ch else False)
    await db.execute(
        "UPDATE bot_chats SET filter_no_photo=$1 WHERE owner_id=$2 AND chat_id=$3::bigint",
        new_val, platform_user["user_id"], chat_id,
    )
    await callback.answer("Без фото: " + ("🔵 Вкл" if new_val else "⚫ Выкл"))
    # Обновляем экран
    ch2 = await db.fetchrow("SELECT * FROM bot_chats WHERE owner_id=$1 AND chat_id=$2::bigint",
                            platform_user["user_id"], chat_id)
    if ch2:
        await callback.message.edit_reply_markup(reply_markup=kb_protection(dict(ch2), []))


# ── Лимиты ────────────────────────────────────────────────────

def kb_limits(ch: dict, back_callback: str | None = None) -> InlineKeyboardMarkup:
    """Клавиатура экрана Лимитов (скрин 2)."""
    chat_id    = ch["chat_id"]
    enabled    = ch.get("join_limit_enabled") or False
    punishment = ch.get("join_limit_punishment") or "kick"
    period     = int(ch.get("join_limit_period_min") or 1)
    limit      = int(ch.get("join_limit_count") or 50)

    probe_label = "🔍 Проверка: вкл"  if enabled else "🔍 Проверка: выкл"
    pun_label   = {"kick": "⚖️ Наказание: Кик", "ban": "⚖️ Наказание: Бан"}.get(punishment, "⚖️ Наказание: Кик")
    time_label  = f"⏱ Время: за {period} мин." if period != 1 else "⏱ Время: за 1 минуту"
    limit_label = f"🚫 Лимит на вступление: ≥ {limit}"

    # Каллбаки кнопок-переключателей: если контекст bs (бот), используем префикс bs_lim_
    is_bs = back_callback and back_callback.startswith("bs_protection:")
    prefix = "bs_lim" if is_bs else "lim"

    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text=probe_label,  callback_data=f"{prefix}_probe:{chat_id}")],
        [InlineKeyboardButton(text=pun_label,    callback_data=f"{prefix}_pun:{chat_id}")],
        [InlineKeyboardButton(text=time_label,   callback_data=f"{prefix}_time:{chat_id}")],
        [InlineKeyboardButton(text=limit_label,  callback_data=f"{prefix}_count:{chat_id}")],
        [InlineKeyboardButton(text="◀️ Назад",   callback_data=back_callback or f"filter_limits_back:{chat_id}")],
    ])


async def _show_limits(callback: CallbackQuery, chat_id: int, owner_id: int):
    from aiogram.exceptions import TelegramBadRequest
    ch = await db.fetchrow(
        "SELECT * FROM bot_chats WHERE owner_id=$1 AND chat_id=$2::bigint",
        owner_id, chat_id,
    )
    if not ch:
        await callback.answer("Площадка не найдена", show_alert=True)
        return
    lk = dict(ch)
    try:
        await callback.message.edit_text(
            "🔄 <b>Лимит</b>\n\n"
            "<i>Установите лимит на количество вступлений в течении определённого времени.</i>\n\n"
            "ⓘ При <u>превышении лимита</u>, бот пришлёт уведомление.",
            parse_mode="HTML",
            reply_markup=kb_limits(lk),
        )
    except TelegramBadRequest:
        pass
    await callback.answer()


@router.callback_query(F.data.startswith("filter_limits:"))
async def on_filter_limits(callback: CallbackQuery, platform_user: dict | None):
    if not platform_user:
        return
    chat_id = int(callback.data.split(":")[1])
    await _show_limits(callback, chat_id, platform_user["user_id"])


@router.callback_query(F.data.startswith("filter_limits_back:"))
async def on_filter_limits_back(callback: CallbackQuery, platform_user: dict | None):
    """Назад → экран Защиты (ch-контекст, при нажатии из отдельной площадки)."""
    if not platform_user:
        return
    chat_id = int(callback.data.split(":")[1])
    owner_id = platform_user["user_id"]
    ch = await db.fetchrow(
        "SELECT * FROM bot_chats WHERE owner_id=$1 AND chat_id=$2::bigint",
        owner_id, chat_id,
    )
    if not ch:
        return
    lk = dict(ch)
    # Правильный edit_text (не edit_reply_markup!) — иначе заголовок "Лимит" оставался бы видием
    rtl      = "🔵" if lk.get("filter_rtl")       else "⚪"
    hiero    = "🔵" if lk.get("filter_hieroglyph") else "⚪"
    no_photo = "🔵" if lk.get("filter_no_photo")   else "⚪"
    limits_label  = "🚫 Лимиты"
    lang_label    = "🏁 Фильтр по языкам"
    await callback.message.edit_text(
        "🛡 <b>Защита</b>\n\nФильтры площадки.",
        parse_mode="HTML",
        reply_markup=kb_protection(lk, []),
    )
    await callback.answer()


@router.callback_query(F.data.startswith("lim_probe:"))
async def on_lim_probe(callback: CallbackQuery, platform_user: dict | None):
    """Переключить проверку лимита вкл/выкл."""
    if not platform_user:
        return
    await callback.answer()  # сразу закрываем, чтобы Telegram не ретраил
    chat_id = int(callback.data.split(":")[1])
    ch = await db.fetchrow(
        "SELECT join_limit_enabled FROM bot_chats WHERE owner_id=$1 AND chat_id=$2::bigint",
        platform_user["user_id"], chat_id,
    )
    new_val = not (ch["join_limit_enabled"] if ch else False)
    await db.execute(
        "UPDATE bot_chats SET join_limit_enabled=$1 WHERE owner_id=$2 AND chat_id=$3::bigint",
        new_val, platform_user["user_id"], chat_id,
    )
    await _show_limits(callback, chat_id, platform_user["user_id"])


@router.callback_query(F.data.startswith("lim_pun:"))
async def on_lim_pun(callback: CallbackQuery, platform_user: dict | None):
    """Переключить наказание: кик → бан → кик."""
    if not platform_user:
        return
    chat_id = int(callback.data.split(":")[1])
    ch = await db.fetchrow(
        "SELECT join_limit_punishment FROM bot_chats WHERE owner_id=$1 AND chat_id=$2::bigint",
        platform_user["user_id"], chat_id,
    )
    cycle   = {"kick": "ban", "ban": "kick"}
    cur     = (ch["join_limit_punishment"] if ch else None) or "kick"
    new_val = cycle.get(cur, "kick")
    await db.execute(
        "UPDATE bot_chats SET join_limit_punishment=$1 WHERE owner_id=$2 AND chat_id=$3::bigint",
        new_val, platform_user["user_id"], chat_id,
    )
    await _show_limits(callback, chat_id, platform_user["user_id"])


@router.callback_query(F.data.startswith("lim_time:"))
async def on_lim_time(callback: CallbackQuery, platform_user: dict | None):
    """Переключить период: 1 → 5 → 10 → 30 → 1 мин."""
    if not platform_user:
        return
    chat_id = int(callback.data.split(":")[1])
    ch = await db.fetchrow(
        "SELECT join_limit_period_min FROM bot_chats WHERE owner_id=$1 AND chat_id=$2::bigint",
        platform_user["user_id"], chat_id,
    )
    cycle   = {1: 5, 5: 10, 10: 30, 30: 1}
    cur     = int(ch["join_limit_period_min"] if ch and ch.get("join_limit_period_min") else 1)
    new_val = cycle.get(cur, 1)
    await db.execute(
        "UPDATE bot_chats SET join_limit_period_min=$1 WHERE owner_id=$2 AND chat_id=$3::bigint",
        new_val, platform_user["user_id"], chat_id,
    )
    await _show_limits(callback, chat_id, platform_user["user_id"])


@router.callback_query(F.data.startswith("lim_count:"))
async def on_lim_count(callback: CallbackQuery, platform_user: dict | None):
    """Переключить лимит: 2 → 5 → 10 → 20 → 50 → 100 → 200 → 2."""
    if not platform_user:
        return
    chat_id = int(callback.data.split(":")[1])
    ch = await db.fetchrow(
        "SELECT join_limit_count FROM bot_chats WHERE owner_id=$1 AND chat_id=$2::bigint",
        platform_user["user_id"], chat_id,
    )
    cycle   = {2: 5, 5: 10, 10: 20, 20: 50, 50: 100, 100: 200, 200: 2}
    cur     = int(ch["join_limit_count"] if ch and ch.get("join_limit_count") else 2)
    new_val = cycle.get(cur, 2)
    await db.execute(
        "UPDATE bot_chats SET join_limit_count=$1 WHERE owner_id=$2 AND chat_id=$3::bigint",
        new_val, platform_user["user_id"], chat_id,
    )
    await _show_limits(callback, chat_id, platform_user["user_id"])


# ── Языковые фильтры ─────────────────────────────────────────
@router.callback_query(F.data.startswith("lang_filters:"))
async def on_lang_filters(callback: CallbackQuery, platform_user: dict | None):
    if not platform_user:
        return
    chat_id = int(callback.data.split(":")[1])
    blocked = await db.fetch(
        "SELECT language_code FROM language_filters WHERE owner_id=$1 AND chat_id=$2::bigint",
        platform_user["user_id"], chat_id,
    )
    blocked_codes = {r["language_code"] for r in blocked}

    buttons = []
    for code, label in LANGUAGE_OPTIONS.items():
        mark = "🚫" if code in blocked_codes else "🌍"
        buttons.append([InlineKeyboardButton(
            text=f"{mark} {label}",
            callback_data=f"lang_toggle:{chat_id}:{code}",
        )])
    buttons.append([InlineKeyboardButton(text="◀️ Назад", callback_data=f"channel_by_chat:{chat_id}")])

    await callback.message.edit_text(
        "🌍 <b>Языковые фильтры</b>\n\n"
        "🚫 — заблокирован (бот отклоняет заявки с этим языком)\n"
        "🌍 — разрешён",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=buttons),
    )
    await callback.answer()


@router.callback_query(F.data.startswith("lang_toggle:"))
async def on_lang_toggle(callback: CallbackQuery, platform_user: dict | None):
    if not platform_user:
        return
    parts = callback.data.split(":")
    chat_id, code = int(parts[1]), parts[2]

    exists = await db.fetchrow(
        "SELECT 1 FROM language_filters WHERE owner_id=$1 AND chat_id=$2::bigint AND language_code=$3",
        platform_user["user_id"], chat_id, code,
    )
    if exists:
        await db.execute(
            "DELETE FROM language_filters WHERE owner_id=$1 AND chat_id=$2::bigint AND language_code=$3",
            platform_user["user_id"], chat_id, code,
        )
        await callback.answer(f"✅ {LANGUAGE_OPTIONS.get(code,'?')} разрешён")
    else:
        await db.execute(
            "INSERT INTO language_filters (owner_id, chat_id, language_code) VALUES ($1,$2,$3) "
            "ON CONFLICT DO NOTHING",
            platform_user["user_id"], chat_id, code,
        )
        await callback.answer(f"🚫 {LANGUAGE_OPTIONS.get(code,'?')} заблокирован")
    # Обновляем экран
    callback.data = f"lang_filters:{chat_id}"
    await on_lang_filters(callback, platform_user)


# ══════════════════════════════════════════════════════════════
# Статистика площадки
# ══════════════════════════════════════════════════════════════
@router.callback_query(F.data.startswith("ch_stats:"))
async def on_ch_stats(callback: CallbackQuery, platform_user: dict | None):
    if not platform_user:
        return
    chat_id = int(callback.data.split(":")[1])
    owner_id = platform_user["user_id"]

    total   = await db.fetchval(
        "SELECT COUNT(*) FROM bot_users WHERE owner_id=$1 AND chat_id=$2::bigint",
        owner_id, chat_id,
    )
    active_bot = await db.fetchval(
        "SELECT COUNT(*) FROM bot_users WHERE owner_id=$1 AND chat_id=$2::bigint "
        "AND bot_activated=true",
        owner_id, chat_id,
    )
    premium = await db.fetchval(
        "SELECT COUNT(*) FROM bot_users WHERE owner_id=$1 AND chat_id=$2::bigint "
        "AND is_active=true AND is_premium=true",
        owner_id, chat_id,
    )
    today = await db.fetchval(
        "SELECT COUNT(*) FROM bot_users WHERE owner_id=$1 AND chat_id=$2::bigint "
        "AND joined_at > now() - interval '24 hours'",
        owner_id, chat_id,
    )
    week = await db.fetchval(
        "SELECT COUNT(*) FROM bot_users WHERE owner_id=$1 AND chat_id=$2::bigint "
        "AND joined_at > now() - interval '7 days'",
        owner_id, chat_id,
    )
    bl_count = await db.fetchval(
        "SELECT COUNT(*) FROM blacklist WHERE owner_id=$1", owner_id,
    )

    await callback.message.edit_text(
        "📊 <b>Статистика площадки</b>\n\n"
        f"👥 <b>Участники</b>\n"
        f"├ Всего в базе: {total:,}\n"
        f"├ Доступны для рассылки: {active_bot:,}\n"
        f"├ Premium-аккаунты: {premium:,}\n"
        f"├ Вступило сегодня: {today:,}\n"
        f"└ За 7 дней: {week:,}\n\n"
        f"🛡 Чёрный список (всего): {bl_count:,}",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="◀️ Назад", callback_data=f"channel_by_chat:{chat_id}")]
        ]),
    )
    await callback.answer()


# ══════════════════════════════════════════════════════════════
# ⚙️ Управление площадкой (ch_settings:{ch_id}) — DB id
# ══════════════════════════════════════════════════════════════
TIMEZONES = [
    ("UTC+3 Москва",   "Europe/Moscow"),
    ("UTC+0 Лондон",   "UTC"),
    ("UTC+1 Берлин",   "Europe/Berlin"),
    ("UTC+2 Киев",     "Europe/Kiev"),
    ("UTC+5 Екатеринбург", "Asia/Yekaterinburg"),
    ("UTC+6 Омск",     "Asia/Omsk"),
    ("UTC+7 Красноярск", "Asia/Krasnoyarsk"),
    ("UTC+8 Иркутск",  "Asia/Irkutsk"),
    ("UTC+9 Якутск",   "Asia/Yakutsk"),
    ("UTC+10 Владивосток", "Asia/Vladivostok"),
]


@router.callback_query(F.data.startswith("ch_settings:"))
async def on_ch_settings(callback: CallbackQuery, platform_user: dict | None):
    """Управление конкретной площадкой (по DB id)."""
    if not platform_user:
        return
    ch_id = int(callback.data.split(":")[1])
    ch = await db.fetchrow(
        """SELECT bc.*, cb.bot_username
           FROM bot_chats bc
           LEFT JOIN child_bots cb ON bc.child_bot_id = cb.id
           WHERE bc.id=$1 AND bc.owner_id=$2""",
        ch_id, platform_user["user_id"],
    )
    if not ch:
        await callback.answer("Площадка не найдена", show_alert=True)
        return

    tz = ch.get("timezone") or "UTC"
    added = ch["added_at"].strftime("%d.%m.%Y") if ch.get("added_at") else "—"
    chat_id = ch["chat_id"]

    toggle_text = "🟢 Активна" if ch["is_active"] else "🔴 Выключена"

    await callback.message.edit_text(
        f"⚙️ <b>Управление площадкой</b>\n\n"
        f"📢 {ch['chat_title']}\n"
        f"🤖 Бот: @{ch['bot_username'] or '—'}\n"
        f"📅 Подключена: {added}\n"
        f"🕐 Часовой пояс: {tz}",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text=toggle_text, callback_data=f"ch_toggle:{ch_id}")],
            [InlineKeyboardButton(text="🕐 Часовой пояс", callback_data=f"ch_tz:{ch_id}")],
            [InlineKeyboardButton(text="📊 Статистика",   callback_data=f"ch_stats:{chat_id}")],
            [InlineKeyboardButton(text="◀️ Назад",         callback_data=f"channel:{ch_id}")],
        ]),
    )
    await callback.answer()


@router.callback_query(F.data.startswith("ch_tz:"))
async def on_ch_tz(callback: CallbackQuery, platform_user: dict | None):
    if not platform_user:
        return
    ch_id = int(callback.data.split(":")[1])
    buttons = [
        [InlineKeyboardButton(text=label, callback_data=f"ch_tz_set:{ch_id}:{tz}")]
        for label, tz in TIMEZONES
    ]
    buttons.append([InlineKeyboardButton(text="◀️ Назад", callback_data=f"ch_settings:{ch_id}")])
    await callback.message.edit_text(
        "🕐 <b>Выберите часовой пояс</b>",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=buttons),
    )
    await callback.answer()


@router.callback_query(F.data.startswith("ch_tz_set:"))
async def on_ch_tz_set(callback: CallbackQuery, platform_user: dict | None):
    if not platform_user:
        return
    parts = callback.data.split(":")
    ch_id = int(parts[1])
    tz = ":".join(parts[2:])
    await db.execute(
        "UPDATE bot_chats SET timezone=$1 WHERE id=$2 AND owner_id=$3",
        tz, ch_id, platform_user["user_id"],
    )
    await callback.answer(f"✅ Часовой пояс: {tz}")
    # Вернуться в управление
    fake_cb_data = f"ch_settings:{ch_id}"
    await callback.message.edit_text(
        "✅ Часовой пояс обновлён.",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="◀️ Назад", callback_data=f"ch_settings:{ch_id}")],
        ]),
    )


# ══════════════════════════════════════════════════════════════
# ██ БОТ-УРОВЕНЬ: bs_* handlers (применяют к ВСЕМ каналам бота)
# ══════════════════════════════════════════════════════════════

async def resolve_owner_id(user_id: int, child_bot_id: int) -> int | None:
    """Возвращает owner_id бота для user_id (владелец или admin через team_members)."""
    row = await db.fetchrow(
        """SELECT cb.owner_id FROM child_bots cb
           WHERE cb.id=$1 AND (
               cb.owner_id=$2
               OR EXISTS (
                   SELECT 1 FROM team_members tm
                   WHERE tm.child_bot_id=cb.id AND tm.user_id=$2 AND tm.is_active=true
               )
           )""",
        child_bot_id, user_id,
    )
    return row["owner_id"] if row else None


async def _get_bot_first_chat(owner_id: int, child_bot_id: int):
    """Возвращает первую площадку (активную или нет) для чтения текущих настроек."""
    return await db.fetchrow(
        "SELECT * FROM bot_chats WHERE child_bot_id=$1 AND owner_id=$2 LIMIT 1",
        child_bot_id, owner_id,
    )


# ── Обработка заявок ─────────────────────────────────────────
async def _show_bs_requests(callback: CallbackQuery, platform_user: dict, child_bot_id: int):
    owner_id = await resolve_owner_id(platform_user["user_id"], child_bot_id)
    if owner_id is None:
        await callback.answer("Нет доступа", show_alert=True)
        return
    ch = await _get_bot_first_chat(owner_id, child_bot_id)
    if not ch:
        await callback.answer("У бота нет подключенных площадок", show_alert=True)
        return

    # ── Синхронизация: очищаем устаревшие pending-заявки по всем чатам бота ──
    # 1. Старше 30 дней — Telegram их авто-отклоняет
    await db.execute(
        """UPDATE join_requests SET status='expired', resolved_at=now()
           WHERE status='pending'
           AND requested_at < now() - interval '30 days'
           AND owner_id=$1
           AND chat_id IN (
               SELECT chat_id FROM bot_chats WHERE child_bot_id=$2 AND owner_id=$1
           )""",
        owner_id, child_bot_id,
    )
    # 2. Зависшие отложенные задачи (потерянные при перезапуске бота)
    auto  = ch["autoaccept"]
    delay = ch["autoaccept_delay"] or 0
    if auto and delay > 0:
        from datetime import timedelta
        await db.execute(
            """UPDATE join_requests SET status='approved', resolved_at=now()
               WHERE status='pending'
               AND requested_at < now() - $3::interval
               AND owner_id=$1
               AND chat_id IN (
                   SELECT chat_id FROM bot_chats WHERE child_bot_id=$2 AND owner_id=$1
               )""",
            owner_id, child_bot_id, timedelta(minutes=delay),
        )

    # Актуальное кол-во ожидающих заявок (свежий запрос после очистки)
    pending = await db.fetchval(
        """SELECT COUNT(*) FROM join_requests jr
           JOIN bot_chats bc ON jr.chat_id=bc.chat_id AND jr.owner_id=bc.owner_id
           WHERE bc.child_bot_id=$1 AND bc.owner_id=$2 AND jr.status IN ('pending','captcha_verified')""",
        child_bot_id, owner_id,
    ) or 0
    auto_label  = "✅ Автопринятие: ВКЛ 🟢" if auto else "☑️ Автопринятие: ВЫКЛ 🔴"
    delay_label = f"⏰ Отложенное: {_delay_label(delay)}"
    await callback.message.edit_text(
        "✅ <b>Обработка заявок</b> (все площадки)\n\n"
        f"🔍 Заявок в очереди: <b>{pending}</b>\n\n"
        "💡 <i>Настройки применяются ко всем каналам бота.</i>",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text=auto_label,           callback_data=f"bs_req_auto:{child_bot_id}")],
            [
                InlineKeyboardButton(text="✔️ Принять",  callback_data=f"req_decide:accept:bs:{child_bot_id}"),
                InlineKeyboardButton(text="✖️ Отклонить",callback_data=f"req_decide:decline:bs:{child_bot_id}"),
            ],
            [InlineKeyboardButton(text=delay_label,          callback_data=f"bs_req_delay:{child_bot_id}")],
            [InlineKeyboardButton(text="◀️ Назад",            callback_data=f"bot_settings:{child_bot_id}")],
        ]),
    )
    await callback.answer()


@router.callback_query(F.data.startswith("bs_requests:"))
async def on_bs_requests(callback: CallbackQuery, platform_user: dict | None):
    if not platform_user:
        return
    await _show_bs_requests(callback, platform_user, int(callback.data.split(":")[1]))


@router.callback_query(F.data.startswith("bs_req_auto:"))
async def on_bs_req_auto(callback: CallbackQuery, platform_user: dict | None):
    if not platform_user:
        return
    child_bot_id = int(callback.data.split(":")[1])
    owner_id = await resolve_owner_id(platform_user["user_id"], child_bot_id)
    if owner_id is None:
        await callback.answer("Нет доступа", show_alert=True)
        return
    ch = await _get_bot_first_chat(owner_id, child_bot_id)
    if not ch:
        return
    new_val = not ch["autoaccept"]
    await db.execute(
        "UPDATE bot_chats SET autoaccept=$1 WHERE child_bot_id=$2 AND owner_id=$3",
        new_val, child_bot_id, owner_id,
    )
    await callback.answer("✅ ВКЛ" if new_val else "🔴 ВЫКЛ")
    await _show_bs_requests(callback, platform_user, child_bot_id)


@router.callback_query(F.data.startswith("bs_req_delay:"))
async def on_bs_req_delay(callback: CallbackQuery, platform_user: dict | None):
    if not platform_user:
        return
    child_bot_id = int(callback.data.split(":")[1])
    owner_id = await resolve_owner_id(platform_user["user_id"], child_bot_id)
    if owner_id is None:
        await callback.answer("Нет доступа", show_alert=True)
        return
    ch = await _get_bot_first_chat(owner_id, child_bot_id)
    current = ch["autoaccept_delay"] or 0 if ch else 0
    new_delay = _next_delay(current)
    await db.execute(
        "UPDATE bot_chats SET autoaccept_delay=$1 WHERE child_bot_id=$2 AND owner_id=$3",
        new_delay, child_bot_id, owner_id,
    )
    await callback.answer(f"⏰ {_delay_label(new_delay)}")
    await _show_bs_requests(callback, platform_user, child_bot_id)


@router.callback_query(F.data.startswith("bs_req_accept_all:"))
async def on_bs_req_accept_all(callback: CallbackQuery, bot: Bot, platform_user: dict | None):
    if not platform_user:
        return
    child_bot_id = int(callback.data.split(":")[1])
    owner_id = await resolve_owner_id(platform_user["user_id"], child_bot_id)
    if owner_id is None:
        await callback.answer("Нет доступа", show_alert=True)
        return
    from aiogram import Bot as AioBot
    from services.security import decrypt_token
    bot_row = await db.fetchrow(
        "SELECT token_encrypted FROM child_bots WHERE id=$1 AND owner_id=$2", child_bot_id, owner_id)
    chats = await db.fetch(
        "SELECT chat_id FROM bot_chats WHERE child_bot_id=$1 AND owner_id=$2",
        child_bot_id, owner_id)
    child_bot_instance = AioBot(token=decrypt_token(bot_row["token_encrypted"])) if bot_row else bot
    approved = 0
    for chat_row in chats:
        chat_id = chat_row["chat_id"]
        # Находим единственную активную request-ссылку для трекинга
        link_rows = await db.fetch(
            "SELECT link FROM invite_links WHERE chat_id=$1::bigint AND link_type='request' AND is_active=true",
            chat_id,
        )
        invite_link_url = link_rows[0]["link"] if len(link_rows) == 1 else None
        
        # Настройки для приветственного сообщения
        settings_row = await db.fetchrow("SELECT * FROM bot_chats WHERE owner_id=$1 AND chat_id=$2::bigint", owner_id, chat_id)

        pending = await db.fetch(
            "SELECT jr.user_id, jr.first_name, COALESCE(bu.language_code,'') AS language_code, "
            "       COALESCE(bu.is_premium, false) AS is_premium "
            "FROM join_requests jr "
            "LEFT JOIN bot_users bu ON bu.user_id=jr.user_id AND bu.chat_id=$2::bigint "
            "WHERE jr.owner_id=$1 AND jr.chat_id=$2::bigint AND jr.status IN ('pending','captcha_verified')",
            owner_id, chat_id)
        for row in pending:
            try:
                await child_bot_instance.approve_chat_join_request(chat_id, row["user_id"])
                approved += 1
                await db.execute(
                    "UPDATE join_requests SET status='approved', resolved_at=now() "
                    "WHERE owner_id=$1 AND chat_id=$2::bigint AND user_id=$3",
                    owner_id, chat_id, row["user_id"])
                if invite_link_url:
                    try:
                        from scheduler.child_bot_runner import _track_invite_link

                        class _FakeUser:
                            id = row["user_id"]
                            first_name = row["first_name"] or ""
                            last_name = None
                            language_code = row["language_code"] or None
                            is_premium = row["is_premium"]
                            username = None

                        await _track_invite_link(invite_link_url, _FakeUser())
                    except Exception:
                        pass
                
                if settings_row:
                    try:
                        from handlers.join_requests import _register_user, _send_welcome
                        class _FakeUser2:
                            id = row["user_id"]
                            first_name = row["first_name"] or ""
                            last_name = None
                            language_code = row["language_code"] or None
                            is_premium = row["is_premium"]
                            username = None
                        await _register_user(owner_id, chat_id, _FakeUser2())
                        await _send_welcome(child_bot_instance, chat_id, _FakeUser2(), dict(settings_row))
                    except Exception as _e:
                        import logging
                        logging.getLogger(__name__).warning(f"[MANUAL ACCEPT] Failed to send welcome: {_e}")
            except Exception:
                await db.execute(
                    "UPDATE join_requests SET status='expired', resolved_at=now() "
                    "WHERE owner_id=$1 AND chat_id=$2::bigint AND user_id=$3",
                    owner_id, chat_id, row["user_id"])
                pass
    if bot_row:
        await child_bot_instance.session.close()
    await callback.answer(f"✔️ Принято: {approved}", show_alert=True)
    try:
        await _show_bs_requests(callback, platform_user, child_bot_id)
    except Exception:
        pass  # MessageNotModified — экран уже актуален


@router.callback_query(F.data.startswith("bs_req_decline_all:"))
async def on_bs_req_decline_all(callback: CallbackQuery, bot: Bot, platform_user: dict | None):
    if not platform_user:
        return
    child_bot_id = int(callback.data.split(":")[1])
    owner_id = await resolve_owner_id(platform_user["user_id"], child_bot_id)
    if owner_id is None:
        await callback.answer("Нет доступа", show_alert=True)
        return
    from aiogram import Bot as AioBot
    from services.security import decrypt_token
    bot_row = await db.fetchrow(
        "SELECT token_encrypted FROM child_bots WHERE id=$1 AND owner_id=$2", child_bot_id, owner_id)
    chats = await db.fetch(
        "SELECT chat_id FROM bot_chats WHERE child_bot_id=$1 AND owner_id=$2",
        child_bot_id, owner_id)
    child_bot_instance = AioBot(token=decrypt_token(bot_row["token_encrypted"])) if bot_row else bot
    declined = 0
    for chat_row in chats:
        pending = await db.fetch(
            "SELECT user_id FROM join_requests WHERE owner_id=$1 AND chat_id=$2::bigint AND status IN ('pending','captcha_verified')",
            owner_id, chat_row["chat_id"])
        for row in pending:
            try:
                await child_bot_instance.decline_chat_join_request(chat_row["chat_id"], row["user_id"])
                declined += 1
                await db.execute(
                    "UPDATE join_requests SET status='declined', resolved_at=now() "
                    "WHERE owner_id=$1 AND chat_id=$2::bigint AND user_id=$3",
                    owner_id, chat_row["chat_id"], row["user_id"])
            except Exception:
                await db.execute(
                    "UPDATE join_requests SET status='expired', resolved_at=now() "
                    "WHERE owner_id=$1 AND chat_id=$2::bigint AND user_id=$3",
                    owner_id, chat_row["chat_id"], row["user_id"])
                pass
    if bot_row:
        await child_bot_instance.session.close()
    await callback.answer(f"✖️ Отклонено: {declined}", show_alert=True)
    await _show_bs_requests(callback, platform_user, child_bot_id)


# ── Сообщения ────────────────────────────────────────────────
@router.callback_query(F.data.startswith("bs_messages:"))
async def on_bs_messages(callback: CallbackQuery, platform_user: dict | None):
    if not platform_user:
        return
    child_bot_id = int(callback.data.split(":")[1])
    owner_id = await resolve_owner_id(platform_user["user_id"], child_bot_id)
    if owner_id is None:
        await callback.answer("Нет доступа", show_alert=True)
        return
    chats = await db.fetch(
        "SELECT chat_id, chat_title FROM bot_chats WHERE child_bot_id=$1 AND owner_id=$2",
        child_bot_id, owner_id,
    )
    if not chats:
        await callback.answer("У бота нет подключенных площадок", show_alert=True)
        return
    buttons = [[InlineKeyboardButton(
        text=f"📍 {ch['chat_title'] or ch['chat_id']}",
        callback_data=f"ch_messages:{ch['chat_id']}",
    )] for ch in chats]
    buttons.append([InlineKeyboardButton(text="◀️ Назад", callback_data=f"bot_settings:{child_bot_id}")])
    await callback.message.edit_text(
        "💬 <b>Сообщения</b>\n\nВыберите площадку:",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=buttons),
    )
    await callback.answer()


@router.callback_query(F.data.startswith("bs_welcome:"))
async def on_bs_welcome(callback: CallbackQuery, state: FSMContext, platform_user: dict | None):
    if not platform_user:
        return
    child_bot_id = int(callback.data.split(":")[1])
    owner_id = await resolve_owner_id(platform_user["user_id"], child_bot_id)
    if owner_id is None:
        await callback.answer("Нет доступа", show_alert=True)
        return
    ch = await _get_bot_first_chat(owner_id, child_bot_id)
    current = f"\n\nТекущий текст:\n<i>{ch['welcome_text'][:200]}</i>" if ch and ch.get("welcome_text") else ""
    await state.set_state(SettingsFSM.waiting_for_welcome_text)
    await state.update_data(child_bot_id=child_bot_id, owner_id=owner_id, mode="bot")
    await callback.message.edit_text(
        f"👋 <b>Приветствие</b>{current}\n\n"
        "Отправьте новый текст — он применится ко всем площадкам бота.\n"
        "Переменные: <code>{name}</code>, <code>{channel}</code>",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="🗑 Удалить", callback_data=f"bs_welcome_del:{child_bot_id}")],
            [InlineKeyboardButton(text="🚫 Отмена",  callback_data=f"bs_messages:{child_bot_id}")],
        ]),
    )
    await callback.answer()


@router.callback_query(F.data.startswith("bs_welcome_del:"))
async def on_bs_welcome_del(callback: CallbackQuery, platform_user: dict | None):
    if not platform_user:
        return
    child_bot_id = int(callback.data.split(":")[1])
    owner_id = await resolve_owner_id(platform_user["user_id"], child_bot_id)
    if owner_id is None:
        await callback.answer("Нет доступа", show_alert=True)
        return
    await db.execute(
        "UPDATE bot_chats SET welcome_text=NULL WHERE child_bot_id=$1 AND owner_id=$2",
        child_bot_id, owner_id)
    await callback.answer("✅ Приветствие удалено")
    callback.data = f"bs_messages:{child_bot_id}"
    await on_bs_messages(callback, platform_user)


@router.callback_query(F.data.startswith("bs_farewell:"))
async def on_bs_farewell(callback: CallbackQuery, state: FSMContext, platform_user: dict | None):
    if not platform_user:
        return
    child_bot_id = int(callback.data.split(":")[1])
    owner_id = await resolve_owner_id(platform_user["user_id"], child_bot_id)
    if owner_id is None:
        await callback.answer("Нет доступа", show_alert=True)
        return
    await state.set_state(SettingsFSM.waiting_for_farewell_text)
    await state.update_data(child_bot_id=child_bot_id, owner_id=owner_id, mode="bot")
    await callback.message.edit_text(
        "👋 <b>Прощание</b>\n\nПрименяется ко всем площадкам бота.\n"
        "Переменные: <code>{name}</code>, <code>{channel}</code>",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="🗑 Удалить", callback_data=f"bs_farewell_del:{child_bot_id}")],
            [InlineKeyboardButton(text="🚫 Отмена",  callback_data=f"bs_messages:{child_bot_id}")],
        ]),
    )
    await callback.answer()


@router.callback_query(F.data.startswith("bs_farewell_del:"))
async def on_bs_farewell_del(callback: CallbackQuery, platform_user: dict | None):
    if not platform_user:
        return
    child_bot_id = int(callback.data.split(":")[1])
    owner_id = await resolve_owner_id(platform_user["user_id"], child_bot_id)
    if owner_id is None:
        await callback.answer("Нет доступа", show_alert=True)
        return
    await db.execute(
        "UPDATE bot_chats SET farewell_text=NULL WHERE child_bot_id=$1 AND owner_id=$2",
        child_bot_id, owner_id)
    await callback.answer("✅ Прощание удалено")
    callback.data = f"bs_messages:{child_bot_id}"
    await on_bs_messages(callback, platform_user)


# ── Защита ────────────────────────────────────────────────────
async def _show_bs_protection(callback: CallbackQuery, platform_user: dict, child_bot_id: int):
    owner_id = await resolve_owner_id(platform_user["user_id"], child_bot_id)
    if owner_id is None:
        await callback.answer("Нет доступа", show_alert=True)
        return
    ch = await _get_bot_first_chat(owner_id, child_bot_id)
    if not ch:
        await callback.answer("Нет активных площадок", show_alert=True)
        return
    rtl      = "🔵" if ch.get("filter_rtl")        else "⚪"
    hiero    = "🔵" if ch.get("filter_hieroglyph")  else "⚪"
    no_photo = "🔵" if ch.get("filter_no_photo")    else "⚪"
    await callback.message.edit_text(
        "🛡 <b>Защита</b> (все площадки)\n\nФильтры применяются ко всем каналам бота.",
        parse_mode="HTML",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="🚫 Лимиты",                    callback_data=f"bs_limits:{child_bot_id}")],
            [InlineKeyboardButton(text="🏁 Фильтр по языкам",           callback_data=f"bs_lang_filters:{child_bot_id}")],
            [InlineKeyboardButton(text=f"{rtl} RTL-символы в имени",   callback_data=f"bs_filter_rtl:{child_bot_id}")],
            [InlineKeyboardButton(text=f"{hiero} Иероглифы в имени",   callback_data=f"bs_filter_hier:{child_bot_id}")],
            [InlineKeyboardButton(text=f"{no_photo} Аккаунты без фото",callback_data=f"bs_filter_photo:{child_bot_id}")],
            [InlineKeyboardButton(text="◀️ Назад",                     callback_data=f"bot_settings:{child_bot_id}")],
        ]),
    )
    await callback.answer()


@router.callback_query(F.data.startswith("bs_protection:"))
async def on_bs_protection(callback: CallbackQuery, platform_user: dict | None):
    if not platform_user:
        return
    await _show_bs_protection(callback, platform_user, int(callback.data.split(":")[1]))




@router.callback_query(F.data.startswith("bs_limits:"))
async def on_bs_limits(callback: CallbackQuery, platform_user: dict | None):
    """Экран Лимитов для child-bot."""
    if not platform_user:
        return
    child_bot_id = int(callback.data.split(":")[1])
    owner_id = await resolve_owner_id(platform_user["user_id"], child_bot_id)
    if owner_id is None:
        await callback.answer("Нет доступа", show_alert=True)
        return
    await _show_bs_limits(callback, child_bot_id, owner_id)


async def _show_bs_limits(callback: CallbackQuery, child_bot_id: int, owner_id: int):
    """Единая точка рендера экрана Лимитов в bs-контексте (назад → bs_protection)."""
    ch = await _get_bot_first_chat(owner_id, child_bot_id)
    if not ch:
        await callback.answer("Нет активных площадок", show_alert=True)
        return
    lk = dict(ch)
    await callback.message.edit_text(
        "🔄 <b>Лимит</b>\n\n"
        "<i>Установите лимит на количество вступлений в течении определённого времени.</i>\n\n"
        "ⓘ При <u>превышении лимита</u>, бот пришлёт уведомление.",
        parse_mode="HTML",
        reply_markup=kb_limits(lk, back_callback=f"bs_protection:{child_bot_id}"),
    )
    await callback.answer()


# bs_lim_* — переключатели для bs-контекста (получают chat_id, но знают child_bot_id через DB)
async def _get_child_bot_id_by_chat(chat_id: int, owner_id: int) -> int | None:
    row = await db.fetchrow(
        "SELECT child_bot_id FROM bot_chats WHERE owner_id=$1 AND chat_id=$2::bigint",
        owner_id, chat_id,
    )
    return row["child_bot_id"] if row else None


@router.callback_query(F.data.startswith("bs_lim_probe:"))
async def on_bs_lim_probe(callback: CallbackQuery, platform_user: dict | None):
    if not platform_user:
        return
    await callback.answer()  # сразу закрываем, чтобы Telegram не ретраил
    chat_id = int(callback.data.split(":")[1])
    owner_id = platform_user["user_id"]
    ch = await db.fetchrow(
        "SELECT join_limit_enabled FROM bot_chats WHERE owner_id=$1 AND chat_id=$2::bigint",
        owner_id, chat_id,
    )
    new_val = not (ch["join_limit_enabled"] if ch else False)
    await db.execute(
        "UPDATE bot_chats SET join_limit_enabled=$1 WHERE owner_id=$2 AND chat_id=$3::bigint",
        new_val, owner_id, chat_id,
    )
    child_bot_id = await _get_child_bot_id_by_chat(chat_id, owner_id)
    if child_bot_id:
        await _show_bs_limits(callback, child_bot_id, owner_id)


@router.callback_query(F.data.startswith("bs_lim_pun:"))
async def on_bs_lim_pun(callback: CallbackQuery, platform_user: dict | None):
    if not platform_user:
        return
    chat_id = int(callback.data.split(":")[1])
    owner_id = platform_user["user_id"]
    ch = await db.fetchrow(
        "SELECT join_limit_punishment FROM bot_chats WHERE owner_id=$1 AND chat_id=$2::bigint",
        owner_id, chat_id,
    )
    cycle = {"kick": "ban", "ban": "kick"}
    cur   = (ch["join_limit_punishment"] if ch else None) or "kick"
    new_val = cycle.get(cur, "kick")
    await db.execute(
        "UPDATE bot_chats SET join_limit_punishment=$1 WHERE owner_id=$2 AND chat_id=$3::bigint",
        new_val, owner_id, chat_id,
    )
    await callback.answer("Наказание: " + new_val.capitalize())
    child_bot_id = await _get_child_bot_id_by_chat(chat_id, owner_id)
    if child_bot_id:
        await _show_bs_limits(callback, child_bot_id, owner_id)


@router.callback_query(F.data.startswith("bs_lim_time:"))
async def on_bs_lim_time(callback: CallbackQuery, platform_user: dict | None):
    if not platform_user:
        return
    chat_id = int(callback.data.split(":")[1])
    owner_id = platform_user["user_id"]
    ch = await db.fetchrow(
        "SELECT join_limit_period_min FROM bot_chats WHERE owner_id=$1 AND chat_id=$2::bigint",
        owner_id, chat_id,
    )
    cycle = {1: 5, 5: 10, 10: 30, 30: 1}
    cur   = int((ch["join_limit_period_min"] if ch and ch.get("join_limit_period_min") else 1))
    new_val = cycle.get(cur, 1)
    await db.execute(
        "UPDATE bot_chats SET join_limit_period_min=$1 WHERE owner_id=$2 AND chat_id=$3::bigint",
        new_val, owner_id, chat_id,
    )
    await callback.answer(f"⏱ {new_val} мин.")
    child_bot_id = await _get_child_bot_id_by_chat(chat_id, owner_id)
    if child_bot_id:
        await _show_bs_limits(callback, child_bot_id, owner_id)


@router.callback_query(F.data.startswith("bs_lim_count:"))
async def on_bs_lim_count(callback: CallbackQuery, platform_user: dict | None):
    if not platform_user:
        return
    chat_id = int(callback.data.split(":")[1])
    owner_id = platform_user["user_id"]
    ch = await db.fetchrow(
        "SELECT join_limit_count FROM bot_chats WHERE owner_id=$1 AND chat_id=$2::bigint",
        owner_id, chat_id,
    )
    cycle = {2: 5, 5: 10, 10: 20, 20: 50, 50: 100, 100: 200, 200: 2}
    cur   = int((ch["join_limit_count"] if ch and ch.get("join_limit_count") else 2))
    new_val = cycle.get(cur, 2)
    await db.execute(
        "UPDATE bot_chats SET join_limit_count=$1 WHERE owner_id=$2 AND chat_id=$3::bigint",
        new_val, owner_id, chat_id,
    )
    await callback.answer(f"🚫 Лимит: {new_val}")
    child_bot_id = await _get_child_bot_id_by_chat(chat_id, owner_id)
    if child_bot_id:
        await _show_bs_limits(callback, child_bot_id, owner_id)



@router.callback_query(F.data.startswith("bs_filter_rtl:"))
async def on_bs_filter_rtl(callback: CallbackQuery, platform_user: dict | None):
    if not platform_user:
        return
    child_bot_id = int(callback.data.split(":")[1])
    owner_id = platform_user["user_id"]
    ch = await _get_bot_first_chat(owner_id, child_bot_id)
    new_val = not (ch["filter_rtl"] if ch else False)
    await db.execute("UPDATE bot_chats SET filter_rtl=$1 WHERE child_bot_id=$2 AND owner_id=$3",
                     new_val, child_bot_id, owner_id)
    await callback.answer("RTL: " + ("✅ Вкл" if new_val else "☐ Выкл"))
    await _show_bs_protection(callback, platform_user, child_bot_id)


@router.callback_query(F.data.startswith("bs_filter_hier:"))
async def on_bs_filter_hier(callback: CallbackQuery, platform_user: dict | None):
    if not platform_user:
        return
    child_bot_id = int(callback.data.split(":")[1])
    owner_id = platform_user["user_id"]
    ch = await _get_bot_first_chat(owner_id, child_bot_id)
    new_val = not (ch["filter_hieroglyph"] if ch else False)
    await db.execute("UPDATE bot_chats SET filter_hieroglyph=$1 WHERE child_bot_id=$2 AND owner_id=$3",
                     new_val, child_bot_id, owner_id)
    await callback.answer("Иероглифы: " + ("✅ Вкл" if new_val else "☐ Выкл"))
    await _show_bs_protection(callback, platform_user, child_bot_id)


@router.callback_query(F.data.startswith("bs_filter_photo:"))
async def on_bs_filter_photo(callback: CallbackQuery, platform_user: dict | None):
    if not platform_user:
        return
    child_bot_id = int(callback.data.split(":")[1])
    owner_id = platform_user["user_id"]
    ch = await _get_bot_first_chat(owner_id, child_bot_id)
    new_val = not (ch["filter_no_photo"] if ch else False)
    await db.execute("UPDATE bot_chats SET filter_no_photo=$1 WHERE child_bot_id=$2 AND owner_id=$3",
                     new_val, child_bot_id, owner_id)
    await callback.answer("Без фото: " + ("✅ Вкл" if new_val else "☐ Выкл"))
    await _show_bs_protection(callback, platform_user, child_bot_id)


def _build_bs_lang_kb(blocked_codes: set, child_bot_id: int) -> InlineKeyboardMarkup:
    """Строим клавиатуру фильтра языков (без edit_text — только кнопки)."""
    buttons = [
        [InlineKeyboardButton(text="🏳️ Все языки", callback_data=f"bs_lang_all:{child_bot_id}")]
    ]
    langs = list(LANGUAGE_OPTIONS.items())
    for i in range(0, len(langs), 3):
        row = []
        for code, label in langs[i:i+3]:
            mark = "🔵" if code in blocked_codes else "⚪️"
            row.append(InlineKeyboardButton(
                text=f"{mark} {label}",
                callback_data=f"bs_lang_toggle:{child_bot_id}:{code}"
            ))
        buttons.append(row)
    buttons.append([InlineKeyboardButton(text="◀️ Назад", callback_data=f"bs_protection:{child_bot_id}")])
    return InlineKeyboardMarkup(inline_keyboard=buttons)


@router.callback_query(F.data.startswith("bs_lang_filters:"))
async def on_bs_lang_filters(callback: CallbackQuery, platform_user: dict | None):
    if not platform_user:
        return
    child_bot_id = int(callback.data.split(":")[1])
    owner_id = platform_user["user_id"]
    ch = await _get_bot_first_chat(owner_id, child_bot_id)
    if not ch:
        await callback.answer("Нет площадок", show_alert=True)
        return
    chat_id = ch["chat_id"]
    blocked = await db.fetch(
        "SELECT language_code FROM language_filters WHERE owner_id=$1 AND chat_id=$2::bigint",
        owner_id, chat_id)
    blocked_codes = {r["language_code"] for r in blocked}
    await callback.message.edit_text(
        "🏁Фильтр по языкам\n\n"
        "🔵— Отклонять заявки\n"
        "⚪️— Принимать заявки\n\n"
        "Выберите действие⬇️",
        reply_markup=_build_bs_lang_kb(blocked_codes, child_bot_id),
    )
    await callback.answer()


@router.callback_query(F.data.startswith("bs_lang_toggle:"))
async def on_bs_lang_toggle(callback: CallbackQuery, platform_user: dict | None):
    if not platform_user:
        return
    parts = callback.data.split(":")
    child_bot_id, code = int(parts[1]), parts[2]
    owner_id = platform_user["user_id"]
    chats = await db.fetch(
        "SELECT chat_id FROM bot_chats WHERE child_bot_id=$1 AND owner_id=$2", child_bot_id, owner_id)
    if not chats:
        return
    first = chats[0]["chat_id"]
    exists = await db.fetchrow(
        "SELECT 1 FROM language_filters WHERE owner_id=$1 AND chat_id=$2::bigint AND language_code=$3",
        owner_id, first, code)
    for chat_row in chats:
        cid = chat_row["chat_id"]
        if exists:
            await db.execute(
                "DELETE FROM language_filters WHERE owner_id=$1 AND chat_id=$2::bigint AND language_code=$3",
                owner_id, cid, code)
        else:
            await db.execute(
                "INSERT INTO language_filters (owner_id, chat_id, language_code) VALUES ($1,$2,$3) ON CONFLICT DO NOTHING",
                owner_id, cid, code)
    # После изменения — подгружаем актуальный список и обновляем ТОЛЬКО клавиатуру (мгновенно)
    blocked = await db.fetch(
        "SELECT language_code FROM language_filters WHERE owner_id=$1 AND chat_id=$2::bigint",
        owner_id, first)
    blocked_codes = {r["language_code"] for r in blocked}
    msg = f"⚪️ {LANGUAGE_OPTIONS.get(code,'?')} — принимать" if exists else f"🔵 {LANGUAGE_OPTIONS.get(code,'?')} — отклонять"
    await callback.answer(msg)
    await callback.message.edit_reply_markup(
        reply_markup=_build_bs_lang_kb(blocked_codes, child_bot_id)
    )


@router.callback_query(F.data.startswith("bs_lang_all:"))
async def on_bs_lang_all(callback: CallbackQuery, platform_user: dict | None):
    """Снять все языковые блокировки — разрешить все языки."""
    if not platform_user:
        return
    child_bot_id = int(callback.data.split(":")[1])
    owner_id = platform_user["user_id"]
    chats = await db.fetch(
        "SELECT chat_id FROM bot_chats WHERE child_bot_id=$1 AND owner_id=$2", child_bot_id, owner_id)
    for chat_row in chats:
        await db.execute(
            "DELETE FROM language_filters WHERE owner_id=$1 AND chat_id=$2::bigint",
            owner_id, chat_row["chat_id"])
    await callback.answer("⚪️ Все языки — принимать")
    # Обновляем только клавиатуру — все кружки сразу становятся ⚪️
    await callback.message.edit_reply_markup(
        reply_markup=_build_bs_lang_kb(set(), child_bot_id)
    )


# ── Управление ботом ─────────────────────────────────────────
@router.callback_query(F.data.startswith("bs_settings:"))
async def on_bs_settings(callback: CallbackQuery, platform_user: dict | None):
    if not platform_user:
        return
    child_bot_id = int(callback.data.split(":")[1])
    await callback.message.edit_text(
        "⚙️ <b>Управление</b>\n\nВыберите действие ⬇️",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="🗄 База",          callback_data=f"bs_base:{child_bot_id}")],
            [InlineKeyboardButton(text="👥 Команда",       callback_data=f"bs_team:{child_bot_id}")],
            [InlineKeyboardButton(text="🕐 Часовой пояс",  callback_data=f"bs_timezone:{child_bot_id}")],
            [InlineKeyboardButton(text="◀️ Назад",          callback_data=f"bot_settings:{child_bot_id}")],
        ]),
    )
    await callback.answer()


# ── База пользователей ────────────────────────────────────────
@router.callback_query(F.data.startswith("bs_base:"))
async def on_bs_base(callback: CallbackQuery, platform_user: dict | None):
    if not platform_user:
        return
    child_bot_id = int(callback.data.split(":")[1])
    owner_id = platform_user["user_id"]

    bot_row = await db.fetchrow(
        "SELECT bot_username, blacklist_enabled FROM child_bots WHERE id=$1 AND owner_id=$2",
        child_bot_id, owner_id,
    )
    bot_username = bot_row["bot_username"] if bot_row else "—"
    bl_enabled = bot_row["blacklist_enabled"] if bot_row else True

    total = await db.fetchval(
        """SELECT COUNT(DISTINCT bu.user_id) FROM bot_users bu
           JOIN bot_chats bc ON bu.chat_id=bc.chat_id AND bu.owner_id=bc.owner_id
           WHERE bc.child_bot_id=$1 AND bc.owner_id=$2
             AND bu.user_id IS NOT NULL AND bu.user_id != $2""",
        child_bot_id, owner_id,
    ) or 0

    blocked = await db.fetchval(
        "SELECT COUNT(*) FROM blacklist WHERE owner_id=$1",
        owner_id,
    ) or 0

    bl_status = "Включён 🟢" if bl_enabled else "Выключен 🔴"

    await callback.message.edit_text(
        "≡ <b>База</b>\n\n"
        f"🤖 Бот: @{bot_username}\n"
        f"👥 Пользователей в базе: {total:,}\n"
        f"⛔️ Заблокированных: {blocked:,}\n"
        f"🛡 ЧС: {bl_status}",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="⚙️ Управление подписчиками", callback_data=f"bs_base_edit:{child_bot_id}")],
            [InlineKeyboardButton(text="⛔️ ЧС пользователей", callback_data=f"bs_blacklist:{child_bot_id}")],
            [InlineKeyboardButton(text="📥 Экспорт базы", callback_data=f"bs_base_export_menu:{child_bot_id}")],
            [InlineKeyboardButton(text="◀️ Назад",                    callback_data=f"bs_settings:{child_bot_id}")],
        ]),
    )
    await callback.answer()


# ── Меню редактирования базы ─────────────────────────────────
@router.callback_query(F.data.startswith("bs_base_edit:"))
async def on_bs_base_edit(callback: CallbackQuery, state: FSMContext,
                          platform_user: dict | None):
    if not platform_user:
        return
    await state.clear()
    child_bot_id = int(callback.data.split(":")[1])
    owner_id = platform_user["user_id"]

    total = await db.fetchval(
        """SELECT COUNT(DISTINCT bu.user_id) FROM bot_users bu
           JOIN bot_chats bc ON bu.chat_id=bc.chat_id AND bu.owner_id=bc.owner_id
           WHERE bc.child_bot_id=$1 AND bc.owner_id=$2 AND bu.user_id != $2""",
        child_bot_id, owner_id,
    ) or 0

    await callback.message.edit_text(
        "⚙️ <b>Управление подписчиками</b>\n\n"
        f"<blockquote>В базе сейчас: <b>{total:,}</b> пользователей.</blockquote>\n\n"
        "Здесь вы можете найти пользователя вашей группы/канала для просмотра его карточки, откуда можно:\n\n"
        "• Писать личные сообщения от лица бота\n"
        "• Выдавать или снимать Мут (Read-only)\n"
        "• Назначать права Администратора\n"
        "• Кикать или заносить в Черный список бота\n\n"
        "<b>Выберите действие:</b>",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="🔎 Найти / Вызвать карточку",    callback_data=f"bs_base_del:{child_bot_id}")],
            [InlineKeyboardButton(text="◀️ Назад",                    callback_data=f"bs_base:{child_bot_id}")],
        ]),
    )
    await callback.answer()


# ── Удалить пользователей ────────────────────────────────────
@router.callback_query(F.data.startswith("bs_base_del:"))
async def on_bs_base_del(callback: CallbackQuery, state: FSMContext,
                         platform_user: dict | None):
    if not platform_user:
        return
    child_bot_id = int(callback.data.split(":")[1])
    await state.update_data(child_bot_id=child_bot_id)
    await state.set_state(SettingsFSM.bs_base_waiting_del)
    await callback.message.edit_text(
        "🔎 <b>Поиск подписчика</b>\n\n"
        "Отправьте <b>@username</b>, <b>Telegram ID</b> или просто перешлите любое сообщение пользователя, чью карточку вы хотите открыть.\n\n"
        "<b>Пример:</b>\n"
        "<code>@spammer1\n111222333</code>",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="🚫 Отменить", callback_data=f"bs_base_edit:{child_bot_id}")],
        ]),
    )
    await callback.answer()


# ── Управление подписчиками (Карточка) ───────────────────────

def _extract_user_id_from_message(message: Message) -> int | None:
    """Извлекает user_id из пересланного сообщения, контакта или текста."""
    if message.forward_origin:
        if getattr(message.forward_origin, "type", None) == 'user':
            return message.forward_origin.sender_user.id
    if message.contact:
        return message.contact.user_id
    if message.text:
        text = message.text.strip()
        if text.isdigit():
            return int(text)
    return None

def _extract_username_from_message(message: Message) -> str | None:
    if message.text:
        text = message.text.strip()
        if text.startswith('@') and len(text) > 1:
            return text.lstrip('@').lower()
    return None

def _parse_user_lines(text: str) -> list[str]:
    """Возвращает список @username или числовых ID из свободного текста."""
    import re
    tokens = re.split(r'[\s,;]+', text.strip())
    result = []
    for t in tokens:
        t = t.strip()
        if not t:
            continue
        if t.startswith('@') and len(t) > 1:
            result.append(t.lower())
        elif t.lstrip('-').isdigit():
            result.append(t)
    return result


async def _process_base_add(owner_id: int, child_bot_id: int,
                             tokens: list[str]) -> dict:
    """Добавляет пользователей в bot_users возможных площадок бота."""
    # Получаем первый активный chat_id площадки для bot_id
    chat_row = await db.fetchrow(
        "SELECT chat_id FROM bot_chats WHERE child_bot_id=$1 AND owner_id=$2 "
        "AND is_active=true ORDER BY id LIMIT 1",
        child_bot_id, owner_id,
    )
    if not chat_row:
        return {"added": 0, "already": 0, "invalid": 0, "details": []}
    chat_id = chat_row["chat_id"]

    added = already = invalid = 0
    details = []
    for token in tokens:
        if token.startswith('@'):
            username = token.lstrip('@')
            exists = await db.fetchval(
                "SELECT 1 FROM bot_users WHERE owner_id=$1 AND chat_id=$2 "
                "AND lower(username)=$3",
                owner_id, chat_id, username.lower(),
            )
            if exists:
                already += 1
                details.append(f"• {token} — уже в базе")
                continue
            await db.execute(
                "INSERT INTO bot_users (owner_id, chat_id, username, first_name, "
                "joined_at, is_active, bot_activated) "
                "VALUES ($1,$2,$3,'',NOW(),true,false) ON CONFLICT DO NOTHING",
                owner_id, chat_id, username,
            )
            added += 1
            details.append(f"• {token} ✅")
        else:
            try:
                uid = int(token)
            except ValueError:
                invalid += 1
                details.append(f"• {token} — неверный формат")
                continue
            exists = await db.fetchval(
                "SELECT 1 FROM bot_users WHERE owner_id=$1 AND chat_id=$2 AND user_id=$3",
                owner_id, chat_id, uid,
            )
            if exists:
                already += 1
                details.append(f"• `{uid}` — уже в базе")
                continue
            await db.execute(
                "INSERT INTO bot_users (owner_id, chat_id, user_id, first_name, "
                "joined_at, is_active, bot_activated) "
                "VALUES ($1,$2,$3,'',NOW(),true,false) ON CONFLICT DO NOTHING",
                owner_id, chat_id, uid,
            )
async def _process_base_del(owner_id: int, child_bot_id: int,
                             tokens: list[str]) -> dict:
    """Удаляет пользователей из bot_users по площадкам бота."""
    removed = not_found = invalid = 0
    details = []
    for token in tokens:
        if token.startswith('@'):
            username = token.lstrip('@')
            res = await db.execute(
                "DELETE FROM bot_users bu USING bot_chats bc "
                "WHERE bu.chat_id=bc.chat_id AND bu.owner_id=bc.owner_id "
                "AND bc.child_bot_id=$1 AND bc.owner_id=$2 "
                "AND lower(bu.username)=$3",
                child_bot_id, owner_id, username.lower(),
            )
            count = int(res.split()[-1]) if res else 0
            if count:
                removed += 1
                details.append(f"• {token} 🗑 удалён")
            else:
                not_found += 1
                details.append(f"• {token} — не найден")
        else:
            try:
                uid = int(token)
            except ValueError:
                invalid += 1
                details.append(f"• {token} — неверный формат")
                continue
            res = await db.execute(
                "DELETE FROM bot_users bu USING bot_chats bc "
                "WHERE bu.chat_id=bc.chat_id AND bu.owner_id=bc.owner_id "
                "AND bc.child_bot_id=$1 AND bc.owner_id=$2 AND bu.user_id=$3",
                child_bot_id, owner_id, uid,
            )
            count = int(res.split()[-1]) if res else 0
            if count:
                removed += 1
                details.append(f"• `{uid}` 🗑 удалён")
            else:
                not_found += 1
                details.append(f"• `{uid}` — не найден")
    return {"removed": removed, "not_found": not_found, "invalid": invalid, "details": details}


# ── Обработка текстового ввода для добавления/удаления ────────


@router.message(SettingsFSM.bs_base_waiting_del)
async def on_bs_base_user_search(message: Message, state: FSMContext,
                                 platform_user: dict | None):
    """Ждет получения пользователя для управления (или файла для массового удаления)."""
    if not platform_user:
        return
    data = await state.get_data()
    child_bot_id = data.get("child_bot_id")
    owner_id = platform_user["user_id"]

    # 1. Массовое удаление через файл (оставляем старую логику для файлов)
    if message.document:
        doc = message.document
        if not doc.file_name.lower().endswith((".txt", ".csv")):
            await message.answer("❌ Поддерживаются только TXT и CSV файлы.")
            return
        file_obj = await message.bot.get_file(doc.file_id)
        content_io = await message.bot.download_file(file_obj.file_path)
        text = content_io.read().decode("utf-8", errors="replace")
        
        tokens = _parse_user_lines(text)
        if not tokens:
            await message.answer("⚠️ Не найдено ни одного @username или Telegram ID.")
            return

        res = await _process_base_del(owner_id, child_bot_id, tokens)
        total_now = await db.fetchval(
            """SELECT COUNT(*) FROM bot_users bu
               JOIN bot_chats bc ON bu.chat_id=bc.chat_id AND bu.owner_id=bc.owner_id
               WHERE bc.child_bot_id=$1 AND bc.owner_id=$2""",
            child_bot_id, owner_id,
        ) or 0
        detail_text = "\n".join(res["details"][:20])
        if len(res["details"]) > 20:
            detail_text += f"\n... и ещё {len(res['details']) - 20}"
        await state.clear()
        await message.answer(
            f"🗑 <b>Удаление завершено!</b>\n\n"
            f"✅ Удалено: <b>{res['removed']}</b>\n"
            f"🔍 Не найдено: <b>{res['not_found']}</b>\n"
            f"❌ Неверный формат: <b>{res['invalid']}</b>\n\n"
            f"{detail_text}\n\n"
            f"👥 Осталось в базе: <b>{total_now:,}</b>",
            reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                [InlineKeyboardButton(text="◀️ Назад к управлению",
                                      callback_data=f"bs_base_edit:{child_bot_id}")],
            ]),
        )
        return

    # 2. Поиск конкретного пользователя (Карточка)
    target_id = _extract_user_id_from_message(message)
    target_username = _extract_username_from_message(message)

    if not target_id and not target_username:
        await message.answer("❌ Не удалось распознать пользователя. Пришлите ID, @username или перешлите его сообщение.")
        return

    # Ищем пользователя в базе (объединяем площадки)
    query = """
        SELECT bu.user_id, MAX(bu.username) as username, MAX(bu.first_name) as first_name, 
               MIN(bu.joined_at) as joined_at, bool_or(bu.is_active) as is_active, 
               MAX(bc.chat_id) as chat_id, string_agg(DISTINCT bc.chat_title, ', ') as chat_titles
        FROM bot_users bu
        JOIN bot_chats bc ON bu.chat_id = bc.chat_id AND bu.owner_id = bc.owner_id
        WHERE bc.child_bot_id = $1 AND bc.owner_id = $2
    """
    args = [child_bot_id, owner_id]
    
    if target_id:
        query += " AND bu.user_id = $3 GROUP BY bu.user_id"
        args.append(target_id)
    else:
        query += " AND lower(bu.username) = $3 GROUP BY bu.user_id"
        args.append(target_username.lower())

    user_row = await db.fetchrow(query, *args)

    if not user_row:
        ident = target_id or f"@{target_username}"
        await message.answer(
            f"❌ Пользователь <b>{ident}</b> не найден в базе этого бота.\n"
            f"Убедитесь, что он писал сообщения в вашей группе.",
            reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                [InlineKeyboardButton(text="◀️ Назад к управлению", callback_data=f"bs_base_edit:{child_bot_id}")],
            ])
        )
        return

    # Рендерим карточку
    await _show_user_card(message, state, child_bot_id, user_row)


async def _show_user_card(message: Message | CallbackQuery, state: FSMContext, child_bot_id: int, user_row: dict):
    uid = user_row["user_id"]
    username = f"@{user_row['username']}" if user_row['username'] else "Нет"
    name = user_row['first_name'] or "Аноним"
    joined = user_row['joined_at'].strftime("%d.%m.%Y %H:%M") if user_row['joined_at'] else "Неизвестно"
    chat_titles = user_row['chat_titles'] or "Личные сообщения"
    is_active = "✅ Активен" if user_row['is_active'] else "❌ Вышел / Заблокировал"

    # Идем в Telegram узнавать реальный статус пользователя
    bot_row = await db.fetchrow("SELECT token_encrypted, owner_id FROM child_bots WHERE id=$1", child_bot_id)
    if not bot_row:
        return
        
    child_bot = Bot(token=decrypt_token(bot_row["token_encrypted"]))
    is_muted = False
    is_admin = False
    try:
        # Проверяем статус по всем чатам бота где числится юзер
        chat_rows = await db.fetch(
            "SELECT bc.chat_id, bc.chat_title FROM bot_users bu JOIN bot_chats bc ON bu.chat_id=bc.chat_id WHERE bc.child_bot_id=$1 AND bu.user_id=$2 AND bc.is_active=true",
            child_bot_id, uid
        )

        active_chat_titles = []

        for cr in chat_rows:
            try:
                member = await child_bot.get_chat_member(chat_id=cr["chat_id"], user_id=uid)
                if member.status in ('member', 'administrator', 'creator', 'restricted'):
                    active_chat_titles.append(cr["chat_title"] or f"Чат {cr['chat_id']}")
                if member.status in ('administrator', 'creator'):
                    is_admin = True
                if member.status == 'restricted':
                    if not getattr(member, 'can_send_messages', True):
                        is_muted = True
            except Exception:
                pass # Пропускаем если тут его нет
    finally:
        await child_bot.session.close()

    chat_titles = ", ".join(active_chat_titles) if active_chat_titles else "нету"

    text = (
        f"⚙️ <b>Карточка подписчика</b>\n\n"
        f"👤 <b>Имя:</b> <a href='tg://user?id={uid}'>{name}</a>\n"
        f"🪪 <b>ID:</b> <code>{uid}</code>\n"
        f"🔗 <b>Юзернейм:</b> {username}\n"
        f"📅 <b>Первое появление:</b> {joined}\n"
        f"📍 <b>Площадки:</b> {chat_titles}\n"
        f"📊 <b>Статус в базе:</b> {is_active}\n"
    )

    is_banned = await db.fetchval("SELECT 1 FROM blacklist WHERE owner_id=$1 AND user_id=$2", bot_row["owner_id"], uid)
    if is_banned:
        text += f"\n⛔️ <b>Находится в Черном списке!</b>"

    # Динамические кнопки
    mute_text = "🔊 Снять Мут" if is_muted else "🔇 Выдать Мут (Read-only)"
    admin_text = "🛡 Выдать Права Админа"
    ban_text = "⛔️ Удалить из черного списка" if is_banned else "⛔️ В Черный список"

    buttons = [
        [InlineKeyboardButton(text="📝 Написать от бота", callback_data=f"bs_um_pm:{child_bot_id}:{uid}")],
        [InlineKeyboardButton(text=mute_text, callback_data=f"bs_um_mute:{child_bot_id}:{uid}")],
        [InlineKeyboardButton(text=admin_text, callback_data=f"bs_um_promote:{child_bot_id}:{uid}")],
        [InlineKeyboardButton(text="🚷 Кикнуть (Удалить из группы)", callback_data=f"bs_um_kick:{child_bot_id}:{uid}")],
        [InlineKeyboardButton(text=ban_text, callback_data=f"bs_um_ban:{child_bot_id}:{uid}")],
        [InlineKeyboardButton(text="◀️ Назад к управлению", callback_data=f"bs_base_edit:{child_bot_id}")],
    ]

    await state.clear()
    if isinstance(message, Message):
        await message.answer(text, reply_markup=InlineKeyboardMarkup(inline_keyboard=buttons), parse_mode="HTML")
    else:
        await message.message.edit_text(text, reply_markup=InlineKeyboardMarkup(inline_keyboard=buttons), parse_mode="HTML")


# ── Действия из Карточки Подписчика ──────────────────────────

@router.callback_query(F.data.startswith("bs_um_pm:"))
async def on_bs_um_pm(callback: CallbackQuery, state: FSMContext, platform_user: dict | None):
    if not platform_user:
        return
    _, child_bot_id_str, uid_str = callback.data.split(":")
    await state.update_data(
        pm_child_bot_id=int(child_bot_id_str), 
        pm_uid=int(uid_str)
    )
    await state.set_state(SettingsFSM.bs_um_waiting_pm)
    sent_msg = await callback.message.edit_text(
        "📝 <b>Отправка сообщения</b>\n\n"
        "Напишите текст, который бот отправит этому пользователю в личные сообщения.\n"
        "<i>Поддерживается форматирование и эмодзи.</i>",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="◀️ Отмена", callback_data=f"bs_um_card:{child_bot_id_str}:{uid_str}")]
        ])
    )
    await state.update_data(pm_prompt_msg_id=sent_msg.message_id)
    await callback.answer()

@router.message(SettingsFSM.bs_um_waiting_pm)
async def on_bs_um_pm_input(message: Message, state: FSMContext, platform_user: dict | None):
    if not platform_user:
        return
    data = await state.get_data()
    child_bot_id = data.get("pm_child_bot_id")
    target_uid = data.get("pm_uid")

    if not message.text:
        await message.answer("❌ Отправьте текстовое сообщение.")
        return

    bot_row = await db.fetchrow("SELECT token_encrypted FROM child_bots WHERE id=$1", child_bot_id)
    if not bot_row:
        await message.answer("❌ Ошибка: бот не найден.")
        return

    child_bot = Bot(token=decrypt_token(bot_row["token_encrypted"]))
    try:
        await child_bot.send_message(chat_id=target_uid, text=message.text, parse_mode="HTML")
        await message.answer("✅ <b>Сообщение успешно отправлено!</b>")

        # Удаляем предыдущее сообщение-заглушку с кнопкой Отмена
        prompt_msg_id = data.get("pm_prompt_msg_id")
        if prompt_msg_id:
            try:
                await message.bot.delete_message(chat_id=message.chat.id, message_id=prompt_msg_id)
            except Exception:
                pass

        # Отправляем новое сообщение-заглушку под сообщением пользователя
        sent_prompt = await message.answer(
            "📝 <b>Отправка сообщения</b>\n\n"
            "Напишите текст, который бот отправит этому пользователю в личные сообщения.\n"
            "<i>Поддерживается форматирование и эмодзи.</i>",
            reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                [InlineKeyboardButton(text="◀️ Отмена", callback_data=f"bs_um_card:{child_bot_id}:{target_uid}")]
            ])
        )
        await state.update_data(pm_prompt_msg_id=sent_prompt.message_id)

    except Exception as e:
        await message.answer(f"❌ <b>Ошибка отправки:</b>\n<code>{e}</code>\n<i>Возможно, пользователь заблокировал бота.</i>")
        await state.clear()
    finally:
        await child_bot.session.close()


@router.callback_query(F.data.startswith("bs_um_card:"))
async def on_bs_um_card(callback: CallbackQuery, state: FSMContext, platform_user: dict | None):
    if not platform_user:
        return
    _, child_bot_id_str, uid_str = callback.data.split(":")
    await state.clear()
    await _refresh_user_card(callback, int(child_bot_id_str), int(uid_str))


@router.callback_query(F.data.startswith("bs_um_mute:"))
async def on_bs_um_mute(callback: CallbackQuery, platform_user: dict | None):
    if not platform_user:
        return
    _, child_bot_id_str, uid_str = callback.data.split(":")
    child_bot_id, uid = int(child_bot_id_str), int(uid_str)

    from aiogram.types import ChatPermissions

    bot_row = await db.fetchrow("SELECT token_encrypted FROM child_bots WHERE id=$1", child_bot_id)
    if not bot_row:
        return await callback.answer("❌ Бот не найден", show_alert=True)
    
    chat_rows = await db.fetch(
        "SELECT bc.chat_id FROM bot_users bu JOIN bot_chats bc ON bu.chat_id=bc.chat_id WHERE bc.child_bot_id=$1 AND bu.user_id=$2",
        child_bot_id, uid
    )
    if not chat_rows:
        return await callback.answer("❌ Нет доступных чатов для этого пользователя", show_alert=True)
        
    child_bot = Bot(token=decrypt_token(bot_row["token_encrypted"]))
    success_count = 0
    
    try:
        for cr in chat_rows:
            chat_id = cr["chat_id"]
            try:
                member = await child_bot.get_chat_member(chat_id=chat_id, user_id=uid)
                if member.status in ('left', 'kicked'):
                    continue
                    
                is_muted = False
                if member.status == 'restricted':
                    is_muted = not getattr(member, 'can_send_messages', True)
                
                if is_muted:
                    # Размут
                    await child_bot.restrict_chat_member(
                        chat_id=chat_id, user_id=uid,
                        permissions=ChatPermissions(
                            can_send_messages=True, can_send_audios=True, can_send_documents=True,
                            can_send_photos=True, can_send_videos=True, can_send_video_notes=True,
                            can_send_voice_notes=True, can_send_polls=True, can_send_other_messages=True,
                            can_add_web_page_previews=True, can_change_info=False, can_invite_users=True,
                            can_pin_messages=False
                        )
                    )
                else:
                    # Мут
                    await child_bot.restrict_chat_member(
                        chat_id=chat_id, user_id=uid,
                        permissions=ChatPermissions(can_send_messages=False)
                    )
                success_count += 1
            except Exception as e:
                import logging
                logging.getLogger(__name__).warning(f"Mute error for {uid} in {chat_id}: {e}")
                
        if success_count > 0:
            await callback.answer("⏳ Статус Мута переключён в активных чатах!", show_alert=True)
        else:
            await callback.answer("❌ Не удалось изменить статус: возможно, юзер вышел из чатов.", show_alert=True)
            
    finally:
        await child_bot.session.close()
        
    # Обновляем карточку
    await _refresh_user_card(callback, child_bot_id, uid)


@router.callback_query(F.data.startswith("bs_um_promote:"))
async def on_bs_um_promote(callback: CallbackQuery, state: FSMContext, platform_user: dict | None):
    if not platform_user:
        return
    _, child_bot_id_str, uid_str = callback.data.split(":")
    child_bot_id, uid = int(child_bot_id_str), int(uid_str)

    bot_row = await db.fetchrow("SELECT token_encrypted FROM child_bots WHERE id=$1", child_bot_id)
    if not bot_row:
        return await callback.answer("❌ Бот не найден", show_alert=True)
        
    chat_rows = await db.fetch(
        "SELECT bc.chat_id, bc.chat_title FROM bot_users bu JOIN bot_chats bc ON bu.chat_id=bc.chat_id WHERE bc.child_bot_id=$1 AND bu.user_id=$2",
        child_bot_id, uid
    )
    if not chat_rows:
        return await callback.answer("❌ Нет доступных чатов.", show_alert=True)
    
    child_bot = Bot(token=decrypt_token(bot_row["token_encrypted"]))
    try:
        active_chats = []
        for cr in chat_rows:
            try:
                member = await child_bot.get_chat_member(chat_id=cr["chat_id"], user_id=uid)
                if member.status not in ('left', 'kicked'):
                    active_chats.append((cr["chat_id"], cr.get("chat_title") or f"Чат {cr['chat_id']}"))
            except Exception:
                pass
                
        if not active_chats:
            return await callback.answer("❌ Пользователь покинул все группы! Нельзя выдать права.", show_alert=True)

        buttons = []
        for chat_id, chat_title in active_chats:
            buttons.append([InlineKeyboardButton(text=chat_title, callback_data=f"bs_prm_c:{child_bot_id}:{uid}:{chat_id}")])
            
        buttons.append([InlineKeyboardButton(text="◀️ Назад", callback_data=f"bs_um_card:{child_bot_id}:{uid}")])
        
        await callback.message.edit_text(
            "🛡 <b>Выбор площадки</b>\n\n"
            "Выберите площадку, на которой нужно выдать права администратора:",
            reply_markup=InlineKeyboardMarkup(inline_keyboard=buttons)
        )

    except Exception as e:
        await callback.answer(f"❌ Ошибка Telegram: {e}", show_alert=True)
    finally:
        await child_bot.session.close()


@router.callback_query(F.data.startswith("bs_prm_c:"))
async def on_bs_um_prom_chat(callback: CallbackQuery, state: FSMContext, platform_user: dict | None):
    if not platform_user:
        return
    _, child_bot_id_str, uid_str, chat_id_str = callback.data.split(":")
    child_bot_id, uid, chat_id = int(child_bot_id_str), int(uid_str), int(chat_id_str)

    bot_row = await db.fetchrow("SELECT token_encrypted FROM child_bots WHERE id=$1", child_bot_id)
    if not bot_row:
        return await callback.answer("❌ Бот не найден", show_alert=True)
        
    chat_title = await db.fetchval("SELECT chat_title FROM bot_chats WHERE chat_id=$1", chat_id)
    chat_title = chat_title or f"Чат {chat_id}"

    child_bot = Bot(token=decrypt_token(bot_row["token_encrypted"]))
    try:
        member = await child_bot.get_chat_member(chat_id=chat_id, user_id=uid)
        
        perms = {
            "delete": False,
            "restrict": False,
            "pin": False,
            "invite": False,
            "promote": False
        }
        
        if member.status in ('administrator', 'creator'):
            perms["delete"] = getattr(member, 'can_delete_messages', False)
            perms["restrict"] = getattr(member, 'can_restrict_members', False)
            perms["pin"] = getattr(member, 'can_pin_messages', False)
            perms["invite"] = getattr(member, 'can_invite_users', False)
            perms["promote"] = getattr(member, 'can_promote_members', False)
        
        await state.update_data(
            admin_perms=perms,
            admin_child_bot_id=child_bot_id,
            admin_uid=uid,
            admin_chat_id=chat_id,
            admin_chat_title=chat_title
        )
        await _render_admin_menu(callback, perms, child_bot_id, uid, chat_title)
    except Exception as e:
        await callback.answer(f"❌ Ошибка Telegram: {e}", show_alert=True)
    finally:
        await child_bot.session.close()

async def _render_admin_menu(callback: CallbackQuery, perms: dict, child_bot_id: int, uid: int, chat_title: str):
    buttons = [
        [InlineKeyboardButton(text=f"{'✅' if perms['delete'] else '❌'} Удаление сообщений", callback_data="bs_adm_tgl:delete")],
        [InlineKeyboardButton(text=f"{'✅' if perms['restrict'] else '❌'} Блокировка/Мут", callback_data="bs_adm_tgl:restrict")],
        [InlineKeyboardButton(text=f"{'✅' if perms['pin'] else '❌'} Закреп сообщений", callback_data="bs_adm_tgl:pin")],
        [InlineKeyboardButton(text=f"{'✅' if perms['invite'] else '❌'} Приглашение по ссылкам", callback_data="bs_adm_tgl:invite")],
        [InlineKeyboardButton(text=f"{'✅' if perms['promote'] else '❌'} Добавление админов", callback_data="bs_adm_tgl:promote")],
        [InlineKeyboardButton(text="Применить и Выдать права 🛡", callback_data="bs_adm_apply")],
        [InlineKeyboardButton(text="◀️ Назад", callback_data=f"bs_um_card:{child_bot_id}:{uid}")]
    ]
    await callback.message.edit_text(
        "🛡 <b>Выдача прав администратора</b>\n"
        f"📍 <i>{chat_title}</i>\n\n"
        "Отметьте нужные права для этого пользователя:",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=buttons)
    )

@router.callback_query(F.data.startswith("bs_adm_tgl:"))
async def on_bs_admin_toggle(callback: CallbackQuery, state: FSMContext):
    key = callback.data.split(":")[1]
    data = await state.get_data()
    perms = data.get("admin_perms", {})
    if key in perms:
        perms[key] = not perms[key]
    await state.update_data(admin_perms=perms)
    child_bot_id = data.get("admin_child_bot_id")
    uid = data.get("admin_uid")
    chat_title = data.get("admin_chat_title", "Чат")
    await _render_admin_menu(callback, perms, child_bot_id, uid, chat_title)

@router.callback_query(F.data == "bs_adm_apply")
async def on_bs_admin_apply(callback: CallbackQuery, state: FSMContext):
    data = await state.get_data()
    perms = data.get("admin_perms")
    child_bot_id = data.get("admin_child_bot_id")
    uid = data.get("admin_uid")
    chat_id = data.get("admin_chat_id")

    if not perms or not child_bot_id or not chat_id:
        return await callback.answer("❌ Истекло время сессии. Начните сначала.", show_alert=True)

    bot_row = await db.fetchrow("SELECT token_encrypted FROM child_bots WHERE id=$1", child_bot_id)
    child_bot = Bot(token=decrypt_token(bot_row["token_encrypted"]))
    try:
        await child_bot.promote_chat_member(
            chat_id=chat_id, user_id=uid,
            is_anonymous=False, can_manage_chat=True,
            can_delete_messages=perms.get('delete', False),
            can_restrict_members=perms.get('restrict', False),
            can_pin_messages=perms.get('pin', False),
            can_invite_users=perms.get('invite', False),
            can_promote_members=perms.get('promote', False),
            can_change_info=False, can_manage_video_chats=False, can_manage_topics=False
        )
        await callback.answer("🛡 Права успешно выданы!", show_alert=True)
    except Exception as e:
        await callback.answer(f"❌ Не удалось выдать права: {e}", show_alert=True)
    finally:
        await child_bot.session.close()

    await state.clear()
    await _refresh_user_card(callback, child_bot_id, uid)


@router.callback_query(F.data.startswith("bs_um_kick:"))
async def on_bs_um_kick(callback: CallbackQuery, platform_user: dict | None):
    if not platform_user:
        return
    _, child_bot_id_str, uid_str = callback.data.split(":")
    child_bot_id, uid = int(child_bot_id_str), int(uid_str)

    bot_row = await db.fetchrow("SELECT token_encrypted FROM child_bots WHERE id=$1", child_bot_id)
    if not bot_row:
        return await callback.answer("❌ Бот не найден", show_alert=True)
        
    chat_rows = await db.fetch(
        "SELECT bc.chat_id FROM bot_users bu JOIN bot_chats bc ON bu.chat_id=bc.chat_id WHERE bc.child_bot_id=$1 AND bu.user_id=$2",
        child_bot_id, uid
    )
    if not chat_rows:
        return await callback.answer("❌ Нет доступных чатов.", show_alert=True)
    
    child_bot = Bot(token=decrypt_token(bot_row["token_encrypted"]))
    some_success = False
    try:
        for cr in chat_rows:
            try:
                await child_bot.unban_chat_member(chat_id=cr["chat_id"], user_id=uid)
                some_success = True
            except Exception as e:
                pass
    finally:
        await child_bot.session.close()

    if some_success:
        await callback.answer("🚷 Пользователь кикнут из всех ваших групп.", show_alert=True)
    else:
        await callback.answer(" Пользователь кикнут (или его там уже не было).", show_alert=True)
        
    for cr in chat_rows:
        await db.execute(
            "update bot_users set is_active=false, left_at=NOW() where chat_id=$1 and user_id=$2",
            cr["chat_id"], uid
        )
        
    await _refresh_user_card(callback, child_bot_id, uid)


@router.callback_query(F.data.startswith("bs_um_ban:"))
async def on_bs_um_ban(callback: CallbackQuery, platform_user: dict | None):
    if not platform_user:
        return
    _, child_bot_id_str, uid_str = callback.data.split(":")
    child_bot_id, uid = int(child_bot_id_str), int(uid_str)
    owner_id = platform_user["user_id"]

    bot_row = await db.fetchrow("SELECT token_encrypted FROM child_bots WHERE id=$1", child_bot_id)
    if not bot_row:
        return await callback.answer("❌ Бот не найден", show_alert=True)
        
    chat_rows = await db.fetch(
        "SELECT bc.chat_id FROM bot_users bu JOIN bot_chats bc ON bu.chat_id=bc.chat_id WHERE bc.child_bot_id=$1 AND bu.user_id=$2",
        child_bot_id, uid
    )
    
    child_bot = Bot(token=decrypt_token(bot_row["token_encrypted"]))
    
    try:
        is_banned = await db.fetchval("SELECT 1 FROM blacklist WHERE owner_id=$1 AND user_id=$2", owner_id, uid)
        
        if is_banned:
            if chat_rows:
                for cr in chat_rows:
                    try:
                        await child_bot.unban_chat_member(chat_id=cr["chat_id"], user_id=uid)
                    except Exception:
                        pass
            
            await db.execute("DELETE FROM blacklist WHERE owner_id=$1 AND user_id=$2", owner_id, uid)
            action_text = "⛔️ Пользователь удален из черного списка"
        else:
            if chat_rows:
                for cr in chat_rows:
                    try:
                        await child_bot.ban_chat_member(chat_id=cr["chat_id"], user_id=uid)
                    except Exception:
                        pass # Игнорируем ошибку бана, если не в группе или уже забанен
            
            # В любом случае заносим в ЧС бота
            username = await db.fetchval("SELECT username FROM bot_users WHERE user_id=$1 LIMIT 1", uid)
            await db.execute(
                """INSERT INTO blacklist (owner_id, username, user_id, added_at, reason)
                   VALUES ($1, $2, $3, NOW(), 'Забанен через карточку пользователя')
                   ON CONFLICT (owner_id, user_id) WHERE user_id IS NOT NULL DO NOTHING""",
                owner_id, username or '', uid
            )
            
            if chat_rows:
                for cr in chat_rows:
                    await db.execute(
                        "update bot_users set is_active=false, left_at=NOW() where chat_id=$1 and user_id=$2",
                        cr["chat_id"], uid
                    )
            action_text = "⛔️ Пользователь забанен и занесен в ЧС бота"
    finally:
        await child_bot.session.close()

    await callback.answer(action_text, show_alert=True)
    await _refresh_user_card(callback, child_bot_id, uid)


async def _refresh_user_card(callback: CallbackQuery, child_bot_id: int, uid: int):
    """Обновляет карточку после действия, чтобы кнопки поменяли статус."""
    query = """
        SELECT bu.user_id, MAX(bu.username) as username, MAX(bu.first_name) as first_name, 
               MIN(bu.joined_at) as joined_at, bool_or(bu.is_active) as is_active, 
               MAX(bc.chat_id) as chat_id, string_agg(DISTINCT bc.chat_title, ', ') as chat_titles
        FROM bot_users bu
        JOIN bot_chats bc ON bu.chat_id = bc.chat_id AND bu.owner_id = bc.owner_id
        WHERE bc.child_bot_id = $1 AND bu.user_id = $2
        GROUP BY bu.user_id
    """
    user_row = await db.fetchrow(query, child_bot_id, uid)
    if user_row:
        # FSMContext не нужен для перерисовки callback'a, передаем пустой
        from aiogram.fsm.context import FSMContext
        from aiogram.fsm.storage.memory import MemoryStorage
        from aiogram.exceptions import TelegramBadRequest
        
        try:
            # Просто используем message_edit
            await _show_user_card(callback, FSMContext(storage=MemoryStorage(), key=None), child_bot_id, user_row)
        except TelegramBadRequest as e:
            if "message is not modified" in str(e):
                pass # Игнорируем ошибку, если статус (кнопки) визуально не изменился
            else:
                import logging
                logging.getLogger(__name__).error(f"Error refreshing user card: {e}")


@router.callback_query(F.data.startswith("bs_blacklist:"))
async def on_bs_blacklist(callback: CallbackQuery, platform_user: dict | None):
    """Главное меню ЧС на уровне бота."""
    if not platform_user:
        return
    child_bot_id = int(callback.data.split(":")[1])
    owner_id = platform_user["user_id"]

    count = await db.fetchval(
        "SELECT COUNT(*) FROM blacklist WHERE owner_id=$1", owner_id,
    ) or 0

    await callback.message.edit_text(
        "⛔️ <b>ЧС пользователей</b>\n\n"
        f"🔢 Записей в базе ЧС: {count:,}",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="⚙️ Управлять базой ЧС",  callback_data=f"bs_bl_manage:{child_bot_id}")],
            [InlineKeyboardButton(text="📤 Экспортировать базу ЧС", callback_data=f"bs_bl_export:{child_bot_id}")],
            [InlineKeyboardButton(text="◀️ Назад",               callback_data=f"bs_base:{child_bot_id}")],
        ]),
    )
    await callback.answer()


# ── Синхронизация участников ──────────────────────────────────
@router.callback_query(F.data.startswith("bs_sync:"))
async def on_bs_sync(callback: CallbackQuery, bot: Bot,
                     platform_user: dict | None):
    """
    Синхронизирует участников всех площадок дочернего бота:
    1) getChatAdministrators — добавляем всех админов в bot_users
    2) getChatMember для каждого уже известного user_id — обновляем is_active
    """
    if not platform_user:
        return
    child_bot_id = int(callback.data.split(":")[1])
    owner_id = platform_user["user_id"]

    await callback.answer("⏳ Синхронизация...")
    await callback.message.edit_text(
        "🔄 <b>Синхронизация участников...</b>\n\n"
        "⏳ Загружаю данные из Telegram, подождите.",
        parse_mode="HTML",
    )

    # Получаем токен дочернего бота
    bot_row = await db.fetchrow(
        "SELECT token_encrypted FROM child_bots WHERE id=$1 AND owner_id=$2",
        child_bot_id, owner_id,
    )
    if not bot_row:
        await callback.message.edit_text(
            "❌ Бот не найден.",
            reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                [InlineKeyboardButton(text="◀️ Назад", callback_data=f"bs_base:{child_bot_id}")]
            ]),
        )
        return

    import asyncio
    from aiogram import Bot as AioBot
    from services.security import decrypt_token
    child_bot_instance = AioBot(token=decrypt_token(bot_row["token_encrypted"]))

    chats = await db.fetch(
        "SELECT chat_id, chat_title FROM bot_chats "
        "WHERE child_bot_id=$1 AND owner_id=$2",
        child_bot_id, owner_id,
    )

    added_total   = 0
    updated_total = 0
    left_total    = 0
    errors_total  = 0
    chat_results  = []

    try:
        for chat_row in chats:
            chat_id    = chat_row["chat_id"]
            chat_title = chat_row["chat_title"] or str(chat_id)
            added_chat = updated_chat = left_chat = 0

            # ── 1. getChatAdministrators → добавляем в bot_users ──
            try:
                admins = await child_bot_instance.get_chat_administrators(chat_id)
                for member in admins:
                    u = member.user
                    if u.is_bot:
                        continue
                    existing = await db.fetchrow(
                        "SELECT id, is_active FROM bot_users "
                        "WHERE owner_id=$1 AND chat_id=$2 AND user_id=$3",
                        owner_id, chat_id, u.id,
                    )
                    if existing:
                        if not existing["is_active"]:
                            await db.execute(
                                "UPDATE bot_users SET is_active=true, left_at=NULL, "
                                "username=$4, first_name=$5 "
                                "WHERE owner_id=$1 AND chat_id=$2 AND user_id=$3",
                                owner_id, chat_id, u.id,
                                u.username or "", u.first_name or "",
                            )
                            updated_chat += 1
                    else:
                        await db.execute(
                            """INSERT INTO bot_users
                               (owner_id, chat_id, user_id, username, first_name,
                                is_active, bot_activated, joined_at)
                               VALUES ($1,$2,$3,$4,$5,true,false,now())
                               ON CONFLICT (owner_id, chat_id, user_id)
                               DO UPDATE SET is_active=true, left_at=NULL,
                                   username=EXCLUDED.username,
                                   first_name=EXCLUDED.first_name""",
                            owner_id, chat_id, u.id,
                            u.username or "", u.first_name or "",
                        )
                        added_chat += 1
            except Exception as e:
                logger.warning(f"[SYNC] getChatAdministrators failed for {chat_id}: {e}")
                errors_total += 1

            # ── 2. Проверяем каждого известного user_id через getChatMember ──
            known_users = await db.fetch(
                "SELECT user_id, username, first_name FROM bot_users "
                "WHERE owner_id=$1 AND chat_id=$2 AND is_active=true AND user_id IS NOT NULL",
                owner_id, chat_id,
            )
            for ku in known_users:
                try:
                    member = await child_bot_instance.get_chat_member(chat_id, ku["user_id"])
                    status = member.status
                    if status in ("left", "kicked", "banned"):
                        await db.execute(
                            "UPDATE bot_users SET is_active=false, left_at=now() "
                            "WHERE owner_id=$1 AND chat_id=$2 AND user_id=$3",
                            owner_id, chat_id, ku["user_id"],
                        )
                        left_chat += 1
                    else:
                        # Обновляем имя/username если изменились
                        new_username  = member.user.username or ""
                        new_firstname = member.user.first_name or ""
                        if new_username != (ku["username"] or "") or new_firstname != (ku["first_name"] or ""):
                            await db.execute(
                                "UPDATE bot_users SET username=$4, first_name=$5 "
                                "WHERE owner_id=$1 AND chat_id=$2 AND user_id=$3",
                                owner_id, chat_id, ku["user_id"],
                                new_username, new_firstname,
                            )
                            updated_chat += 1
                except Exception:
                    # Пользователь недоступен или ошибка — пропускаем
                    pass
                await asyncio.sleep(0.05)  # ~20 req/s, не превысить лимиты

            added_total   += added_chat
            updated_total += updated_chat
            left_total    += left_chat
            chat_results.append(
                f"• <b>{chat_title[:30]}</b>: +{added_chat} добавлено, "
                f"{updated_chat} обновлено, {left_chat} покинули"
            )

    finally:
        await child_bot_instance.session.close()

    # Итоговый счётчик
    total_now = await db.fetchval(
        """SELECT COUNT(*) FROM bot_users bu
           JOIN bot_chats bc ON bu.chat_id=bc.chat_id AND bu.owner_id=bc.owner_id
           WHERE bc.child_bot_id=$1 AND bc.owner_id=$2 AND bu.is_active=true""",
        child_bot_id, owner_id,
    ) or 0

    detail = "\n".join(chat_results) if chat_results else "Нет площадок"
    await callback.message.edit_text(
        "✅ <b>Синхронизация завершена!</b>\n\n"
        f"{detail}\n\n"
        f"📊 Итого: +{added_total} добавлено, "
        f"{updated_total} обновлено, {left_total} помечено как покинувшие\n"
        f"👥 Активных в базе: <b>{total_now:,}</b>",
        parse_mode="HTML",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="◀️ Назад к базе",
                                  callback_data=f"bs_base:{child_bot_id}")],
        ]),
    )


# ── Экспорт базы ЧС ───────────────────────────────────────────
@router.callback_query(F.data.startswith("bs_bl_export:"))
async def on_bs_bl_export(callback: CallbackQuery, platform_user: dict | None):
    if not platform_user:
        await callback.answer()
        return
    child_bot_id = int(callback.data.split(":")[1])
    owner_id = platform_user["user_id"]

    try:
        total = await db.fetchval(
            "SELECT COUNT(*) FROM blacklist WHERE owner_id=$1", owner_id,
        ) or 0

        if total == 0:
            await callback.message.edit_text(
                "📤 <b>Экспорт базы ЧС</b>\n\n"
                "⚠️ База ЧС пуста. Нечего экспортировать.",
                parse_mode="HTML",
                reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                    [InlineKeyboardButton(text="◀️ Назад", callback_data=f"bs_blacklist:{child_bot_id}")],
                ]),
            )
        else:
            await callback.message.edit_text(
                "📤 <b>Экспорт базы ЧС</b>\n\n"
                "<blockquote>Выгрузите базу для резервной копии или переноса "
                "в другой бот/сервис.\n\n"
                "CSV — полная база со всеми данными (user_id, username, причина, дата).\n"
                "TXT — только ID и @username, по одному на строку — готово для импорта "
                "в любой другой бот.</blockquote>\n\n"
                f"📊 Всего записей в базе ЧС: <b>{total:,}</b>",
                parse_mode="HTML",
                reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                    [InlineKeyboardButton(text="📥 Скачать CSV (полная база)",       callback_data=f"bs_bl_export_csv:{child_bot_id}")],
                    [InlineKeyboardButton(text="📋 Скачать TXT (для импорта в бот)", callback_data=f"bs_bl_export_txt:{child_bot_id}")],
                    [InlineKeyboardButton(text="◀️ Назад",                           callback_data=f"bs_blacklist:{child_bot_id}")],
                ]),
            )
    except Exception as e:
        logger.error(f"bs_bl_export error: {e}")
        await callback.message.edit_text(
            "❌ <b>Ошибка при загрузке данных.</b>\n\nПопробуйте позже.",
            parse_mode="HTML",
            reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                [InlineKeyboardButton(text="◀️ Назад", callback_data=f"bs_blacklist:{child_bot_id}")],
            ]),
        )
    finally:
        await callback.answer()


@router.callback_query(F.data.startswith("bs_bl_export_csv:"))
async def on_bs_bl_export_csv(callback: CallbackQuery, platform_user: dict | None):
    if not platform_user:
        return
    import io, csv
    from datetime import datetime
    from aiogram.types import BufferedInputFile

    owner_id = platform_user["user_id"]
    child_bot_id = int(callback.data.split(":")[1])
    await callback.answer("⏳ Формирую файл...", show_alert=False)

    rows = await db.fetch(
        "SELECT user_id, username, reason, created_at "
        "FROM blacklist WHERE owner_id=$1 ORDER BY created_at DESC",
        owner_id,
    )

    buf = io.StringIO()
    w = csv.writer(buf, delimiter=";")
    w.writerow(["user_id", "username", "reason", "added_at"])
    for r in rows:
        w.writerow([
            r["user_id"] or "",
            f"@{r['username']}" if r["username"] else "",
            r["reason"] or "",
            r["created_at"].strftime("%d.%m.%Y %H:%M") if r["created_at"] else "",
        ])

    date_str = datetime.now().strftime("%Y%m%d")
    data = buf.getvalue().encode("utf-8-sig")
    await callback.message.answer_document(
        BufferedInputFile(data, filename=f"blacklist_{date_str}.csv"),
        caption=(
            f"📥 <b>Экспорт базы ЧС (CSV)</b>\n\n"
            f"📊 Записей: <b>{len(rows):,}</b>\n"
            "Формат: CSV, разделитель ; | Кодировка UTF-8 с BOM (совместимо с Excel)"
        ),
        parse_mode="HTML",
    )


@router.callback_query(F.data.startswith("bs_bl_export_txt:"))
async def on_bs_bl_export_txt(callback: CallbackQuery, platform_user: dict | None):
    if not platform_user:
        return
    from datetime import datetime
    from aiogram.types import BufferedInputFile

    owner_id = platform_user["user_id"]
    child_bot_id = int(callback.data.split(":")[1])
    await callback.answer("⏳ Формирую файл...", show_alert=False)

    rows = await db.fetch(
        "SELECT user_id, username FROM blacklist WHERE owner_id=$1 ORDER BY created_at DESC",
        owner_id,
    )

    lines = []
    for r in rows:
        if r["user_id"]:
            lines.append(str(r["user_id"]))
        elif r["username"]:
            lines.append(f"@{r['username']}")

    date_str = datetime.now().strftime("%Y%m%d")
    data = "\n".join(lines).encode("utf-8")
    await callback.message.answer_document(
        BufferedInputFile(data, filename=f"blacklist_ids_{date_str}.txt"),
        caption=(
            f"📋 <b>Экспорт базы ЧС (TXT)</b>\n\n"
            f"📊 Записей: <b>{len(lines):,}</b>\n"
            "Формат: один ID или @username на строку — готово для импорта в другой бот"
        ),
        parse_mode="HTML",
    )


# ── Загрузить базу ЧС ─────────────────────────────────────────
@router.callback_query(F.data.startswith("bs_bl_upload:"))
async def on_bs_bl_upload(callback: CallbackQuery, state: FSMContext,
                          platform_user: dict | None):
    if not platform_user:
        return
    child_bot_id = callback.data.split(":")[1]
    await state.update_data(child_bot_id=child_bot_id, bs_bl_mode="add")
    await state.set_state(SettingsFSM.bs_bl_waiting_add_file)
    await callback.message.edit_text(
        "📂 <b>Загрузить базу ЧС</b>\n\n"
        "Отправьте файл <b>TXT</b> или <b>CSV</b> с @username или Telegram ID.\n\n"
        "📋 Формат строки:\n"
        "<code>@spammer1\n123456789\n@baduser</code>\n\n"
        "Максимум: 20 MB, до 100 000 записей.",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="🚫 Отмена", callback_data=f"bs_blacklist:{child_bot_id}")],
        ]),
    )
    await callback.answer()


# ── Забанить всех нарушителей (бот-уровень) ──────────────────
@router.callback_query(F.data.startswith("bs_bl_ban_all:"))
async def on_bs_bl_ban_all(callback: CallbackQuery, bot: Bot,
                           platform_user: dict | None):
    if not platform_user:
        return
    parts = callback.data.split(":")
    child_bot_id, chat_id = int(parts[1]), int(parts[2])
    owner_id = platform_user["user_id"]

    from aiogram import Bot as AioBot
    from services.security import decrypt_token

    bot_row = await db.fetchrow(
        "SELECT token_encrypted FROM child_bots WHERE id=$1 AND owner_id=$2",
        child_bot_id, owner_id,
    )
    if not bot_row:
        await callback.answer("Бот не найден", show_alert=True)
        return

    await callback.answer("⏳ Баню...")

    child_bot_instance = AioBot(token=decrypt_token(bot_row["token_encrypted"]))
    violators = await db.fetch(
        """SELECT bu.user_id FROM bot_users bu
           INNER JOIN blacklist bl ON bl.owner_id=bu.owner_id
             AND ((bl.user_id IS NOT NULL AND bl.user_id=bu.user_id)
                  OR  (bl.username IS NOT NULL AND lower(bl.username)=lower(bu.username)))
           WHERE bu.owner_id=$1 AND bu.chat_id=$2::bigint AND bu.is_active=true""",
        owner_id, chat_id,
    )

    banned = 0
    for v in violators:
        try:
            await child_bot_instance.ban_chat_member(chat_id, v["user_id"])
            banned += 1
        except Exception:
            pass

    await child_bot_instance.session.close()

    if violators:
        user_ids = [v["user_id"] for v in violators]
        await db.execute(
            "UPDATE bot_users SET is_active=false, left_at=now() "
            "WHERE owner_id=$1 AND chat_id=$2::bigint AND user_id=ANY($3::bigint[])",
            owner_id, chat_id, user_ids,
        )

    await callback.message.edit_text(
        "✅ <b>Готово!</b>\n\n"
        f"🚫 Забанено: <b>{banned}</b> из {len(violators)}",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="◀️ Назад", callback_data=f"bs_blacklist:{child_bot_id}")],
        ]),
    )


# ── Полный список нарушителей ─────────────────────────────────
@router.callback_query(F.data.startswith("bs_bl_full_list:"))
async def on_bs_bl_full_list(callback: CallbackQuery, bot: Bot,
                             platform_user: dict | None):
    if not platform_user:
        return
    parts = callback.data.split(":")
    child_bot_id, chat_id = int(parts[1]), int(parts[2])
    owner_id = platform_user["user_id"]

    violators = await db.fetch(
        """SELECT bu.user_id, bu.username FROM bot_users bu
           INNER JOIN blacklist bl ON bl.owner_id=bu.owner_id
             AND ((bl.user_id IS NOT NULL AND bl.user_id=bu.user_id)
                  OR  (bl.username IS NOT NULL AND lower(bl.username)=lower(bu.username)))
           WHERE bu.owner_id=$1 AND bu.chat_id=$2::bigint AND bu.is_active=true""",
        owner_id, chat_id,
    )
    if not violators:
        await callback.answer("Список пуст", show_alert=True)
        return

    lines = []
    for i, v in enumerate(violators, 1):
        if v["username"]:
            entry = f"{i}. @{v['username']}"
            if v["user_id"]:
                entry += f" (ID: {v['user_id']})"
        else:
            entry = f"{i}. ID: {v['user_id']}"
        lines.append(entry)

    from aiogram.types import BufferedInputFile
    content = "\n".join(lines).encode("utf-8")
    file = BufferedInputFile(content, filename="violators_list.txt")
    await bot.send_document(
        chat_id=owner_id,
        document=file,
        caption=f"👁 Полный список нарушителей: {len(violators):,} чел.",
    )
    await callback.answer("✅ Список отправлен в чат")


# ── Управление базой ЧС ───────────────────────────────────────
async def _show_bs_bl_manage(callback: CallbackQuery, platform_user: dict,
                             child_bot_id: int):
    owner_id = platform_user["user_id"]
    try:
        bot_row = await db.fetchrow(
            "SELECT blacklist_enabled FROM child_bots WHERE id=$1 AND owner_id=$2",
            child_bot_id, owner_id,
        )
        enabled = bot_row["blacklist_enabled"] if bot_row else True
    except Exception:
        enabled = True

    count = await db.fetchval(
        "SELECT COUNT(*) FROM blacklist WHERE owner_id=$1", owner_id,
    ) or 0

    toggle_text = "✅ ЧС: Включён 🟢" if enabled else "☑️ ЧС: Выключен 🔴"
    await callback.message.edit_text(
        "⚙️ <b>Управление базой ЧС</b>\n\n"
        f"📊 Записей в базе: {count:,}\n\n"
        "Включите ЧС, чтобы бот автоматически проверял\n"
        "вступающих пользователей по списку.",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text=toggle_text,
                                  callback_data=f"bs_bl_toggle:{child_bot_id}")],
            [InlineKeyboardButton(text="➕ Добавить пользователей (TXT/CSV)",
                                  callback_data=f"bs_bl_add_file:{child_bot_id}")],
            [InlineKeyboardButton(text="➖ Удалить пользователей (TXT/CSV)",
                                  callback_data=f"bs_bl_del_file:{child_bot_id}")],
            [InlineKeyboardButton(text="🗑 Очистить базу",
                                  callback_data=f"bs_bl_clear:{child_bot_id}")],
            [InlineKeyboardButton(text="◀️ Назад",
                                  callback_data=f"bs_blacklist:{child_bot_id}")],
        ]),
    )
    await callback.answer()


@router.callback_query(F.data.startswith("bs_bl_manage:"))
async def on_bs_bl_manage(callback: CallbackQuery, platform_user: dict | None):
    if not platform_user:
        return
    await _show_bs_bl_manage(callback, platform_user, int(callback.data.split(":")[1]))


# ── Тумблер ЧС ───────────────────────────────────────────────
@router.callback_query(F.data.startswith("bs_bl_toggle:"))
async def on_bs_bl_toggle(callback: CallbackQuery, platform_user: dict | None):
    if not platform_user:
        return
    child_bot_id = int(callback.data.split(":")[1])
    owner_id = platform_user["user_id"]
    try:
        row = await db.fetchrow(
            "SELECT blacklist_enabled FROM child_bots WHERE id=$1 AND owner_id=$2",
            child_bot_id, owner_id,
        )
        current = row["blacklist_enabled"] if row else True
        new_val = not current
        await db.execute(
            "UPDATE child_bots SET blacklist_enabled=$1 WHERE id=$2 AND owner_id=$3",
            new_val, child_bot_id, owner_id,
        )
        await callback.answer("✅ ВКЛ" if new_val else "🔴 ВЫКЛ")
        
        # Trigger background sweeps
        from services.blacklist import sweep_after_import, sweep_unban_after_disable
        import asyncio
        if new_val:
            asyncio.create_task(sweep_after_import(owner_id))
        else:
            asyncio.create_task(sweep_unban_after_disable(owner_id))
            
    except Exception:
        await callback.answer("⚠️ Ошибка обновления", show_alert=True)
        return
    await _show_bs_bl_manage(callback, platform_user, child_bot_id)


# ── Добавить пользователей файлом ─────────────────────────────
@router.callback_query(F.data.startswith("bs_bl_add_file:"))
async def on_bs_bl_add_file(callback: CallbackQuery, state: FSMContext,
                            platform_user: dict | None):
    if not platform_user:
        return
    child_bot_id = callback.data.split(":")[1]
    await state.update_data(child_bot_id=child_bot_id, bs_bl_mode="add")
    await state.set_state(SettingsFSM.bs_bl_waiting_add_file)
    await callback.message.edit_text(
        "➕ <b>Добавить в ЧС</b>\n\n"
        "Отправьте файл <b>TXT</b> или <b>CSV</b>.\n"
        "Каждая строка — один пользователь: @username или Telegram ID.\n\n"
        "<code>@spammer1\n123456789\n@baduser</code>",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="🚫 Отмена", callback_data=f"bs_bl_manage:{child_bot_id}")],
        ]),
    )
    await callback.answer()


# ── Удалить пользователей файлом ─────────────────────────────
@router.callback_query(F.data.startswith("bs_bl_del_file:"))
async def on_bs_bl_del_file(callback: CallbackQuery, state: FSMContext,
                            platform_user: dict | None):
    if not platform_user:
        return
    child_bot_id = callback.data.split(":")[1]
    await state.update_data(child_bot_id=child_bot_id, bs_bl_mode="del")
    await state.set_state(SettingsFSM.bs_bl_waiting_del_file)
    await callback.message.edit_text(
        "➖ <b>Удалить из ЧС</b>\n\n"
        "Отправьте файл <b>TXT</b> или <b>CSV</b> с пользователями,\n"
        "которых нужно <b>убрать</b> из чёрного списка.\n\n"
        "<code>@gooduser\n987654321</code>",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="🚫 Отмена", callback_data=f"bs_bl_manage:{child_bot_id}")],
        ]),
    )
    await callback.answer()


# ── Очистить базу ЧС ─────────────────────────────────────────
@router.callback_query(F.data.startswith("bs_bl_clear:"))
async def on_bs_bl_clear(callback: CallbackQuery, platform_user: dict | None):
    if not platform_user:
        return
    child_bot_id = callback.data.split(":")[1]
    count = await db.fetchval(
        "SELECT COUNT(*) FROM blacklist WHERE owner_id=$1",
        platform_user["user_id"],
    ) or 0
    await callback.message.edit_text(
        f"⚠️ <b>Действительно очистить базу ЧС?</b>\n\n"
        f"Вы собираетесь удалить <b>{count:,}</b> записей из чёрного списка.\n\n"
        f"<blockquote>ℹ️ При подтверждении все пользователи из этой базы будут "
        f"удалены безвозвратно, а также <b>автоматически разблокированы</b> во всех "
        f"подключенных группах и каналах вашего бота, куда им ранее был закрыт доступ.</blockquote>\n\n"
        f"Это действие необратимо. Вы уверены?",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="✅ Да, очистить и разблокировать",
                                  callback_data=f"bs_bl_clear_do:{child_bot_id}")],
            [InlineKeyboardButton(text="🚫 Нет, отменить",
                                  callback_data=f"bs_bl_manage:{child_bot_id}")],
        ]),
    )
    await callback.answer()


@router.callback_query(F.data.startswith("bs_bl_clear_do:"))
async def on_bs_bl_clear_do(callback: CallbackQuery, platform_user: dict | None):
    if not platform_user:
        return
    child_bot_id = callback.data.split(":")[1]
    owner_id = platform_user["user_id"]
    
    # 1. Запоминаем кого разбанивать перед удалением из базы
    records = await db.fetch("SELECT user_id, username FROM blacklist WHERE owner_id=$1", owner_id)
    
    # 2. Очищаем базу
    deleted = await db.fetchval(
        "WITH d AS (DELETE FROM blacklist WHERE owner_id=$1 RETURNING 1) "
        "SELECT COUNT(*) FROM d",
        owner_id,
    ) or 0
    
    # 3. Запускаем фоновую задачу разбана
    import asyncio
    from services.blacklist import sweep_unban_records
    if records:
        asyncio.create_task(sweep_unban_records(owner_id, records))

    await callback.message.edit_text(
        f"⏳ <b>Процесс запущен!</b>\n\n"
        f"Процесс очистки черного списка запущен. База ЧС будет в скором времени полностью очищена.\n"
        f"Это может занять некоторое время в зависимости от размера базы.",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="◀️ Назад к базе",
                                  callback_data=f"bs_blacklist:{child_bot_id}")],
        ]),
    )
    await callback.answer()


# ── Экран выбора типа экспорта ───────────────────────────────
@router.callback_query(F.data.startswith("bs_base_export_menu:"))
async def on_bs_base_export_menu(callback: CallbackQuery, platform_user: dict | None):
    if not platform_user:
        return
    child_bot_id = int(callback.data.split(":")[1])
    owner_id = platform_user["user_id"]

    total = await db.fetchval(
        """SELECT COUNT(DISTINCT bu.user_id) FROM bot_users bu
           JOIN bot_chats bc ON bu.chat_id=bc.chat_id AND bu.owner_id=bc.owner_id
           WHERE bc.child_bot_id=$1 AND bc.owner_id=$2
             AND bu.user_id IS NOT NULL""",
        child_bot_id, owner_id,
    ) or 0
    active = await db.fetchval(
        """SELECT COUNT(DISTINCT bu.user_id) FROM bot_users bu
           JOIN bot_chats bc ON bu.chat_id=bc.chat_id AND bu.owner_id=bc.owner_id
           WHERE bc.child_bot_id=$1 AND bc.owner_id=$2
             AND bu.is_active=true AND bu.bot_activated=true
             AND bu.user_id IS NOT NULL""",
        child_bot_id, owner_id,
    ) or 0
    premium = await db.fetchval(
        """SELECT COUNT(DISTINCT bu.user_id) FROM bot_users bu
           JOIN bot_chats bc ON bu.chat_id=bc.chat_id AND bu.owner_id=bc.owner_id
           WHERE bc.child_bot_id=$1 AND bc.owner_id=$2
             AND bu.is_premium=true AND bu.user_id IS NOT NULL""",
        child_bot_id, owner_id,
    ) or 0
    inactive = total - active

    await callback.message.edit_text(
        "🗄 <b>Экспорт базы</b>\n\n"
        "<blockquote>Здесь хранятся все пользователи, которые "
        "взаимодействовали с каналами и группами вашего бота.\n\n"
        "Вы можете выгрузить базу в формате CSV — это удобно для "
        "аналитики, рассылок через сторонние сервисы или резервной копии.</blockquote>\n\n"
        f"👥 Всего в базе: {total:,}\n"
        f"🟢 Активных (в боте): {active:,}\n"
        f"🔴 Неактивных: {inactive:,}\n"
        f"⭐ Premium: {premium:,}",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="📥 Выгрузить всю базу (CSV)",  callback_data=f"bs_base_export:{child_bot_id}:all")],
            [InlineKeyboardButton(text="🟢 Выгрузить активных",         callback_data=f"bs_base_export:{child_bot_id}:active")],
            [InlineKeyboardButton(text="⭐ Выгрузить Premium",           callback_data=f"bs_base_export:{child_bot_id}:premium")],
            [InlineKeyboardButton(text="◀️ Назад",                       callback_data=f"bs_base:{child_bot_id}")],
        ]),
    )
    await callback.answer()


@router.callback_query(F.data.startswith("bs_base_export:"))
async def on_bs_base_export(callback: CallbackQuery, bot: Bot, platform_user: dict | None):
    if not platform_user:
        return
    parts = callback.data.split(":")
    child_bot_id = int(parts[1])
    mode = parts[2] if len(parts) > 2 else "all"
    owner_id = platform_user["user_id"]

    await callback.answer("⏳ Формирую файл...", show_alert=False)

    # Формируем WHERE в зависимости от режима
    extra_filter = ""
    label = "все"
    if mode == "active":
        extra_filter = "AND bu.is_active=true AND bu.bot_activated=true"
        label = "активные"
    elif mode == "premium":
        extra_filter = "AND bu.is_premium=true"
        label = "premium"

    rows = await db.fetch(
        f"""SELECT bu.user_id, bu.username, bu.first_name, bu.language_code,
                   bu.is_premium, bu.is_active, bu.bot_activated,
                   bu.joined_at, bc.chat_title
            FROM bot_users bu
            JOIN bot_chats bc ON bu.chat_id=bc.chat_id AND bu.owner_id=bc.owner_id
            WHERE bc.child_bot_id=$1 AND bc.owner_id=$2 {extra_filter}
            ORDER BY bu.joined_at DESC""",
        child_bot_id, owner_id,
    )

    if not rows:
        try:
            await callback.message.edit_text(
                "🗄 <b>База пользователей</b>\n\n"
                "⚠️ Нет пользователей для выгрузки по выбранному фильтру.",
                reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                    [InlineKeyboardButton(text="◀️ Назад", callback_data=f"bs_base:{child_bot_id}")],
                ]),
            )
        except Exception:
            pass
        return

    # Генерируем CSV в памяти
    import csv
    import io
    from datetime import datetime

    output = io.StringIO()
    writer = csv.writer(output, delimiter=";", quoting=csv.QUOTE_MINIMAL)
    writer.writerow(["user_id", "username", "first_name", "language",
                     "premium", "active", "bot_activated", "joined_at", "channel"])
    for r in rows:
        writer.writerow([
            r["user_id"],
            r["username"] or "",
            r["first_name"] or "",
            r["language_code"] or "",
            "да" if r["is_premium"] else "нет",
            "да" if r["is_active"] else "нет",
            "да" if r["bot_activated"] else "нет",
            r["joined_at"].strftime("%d.%m.%Y %H:%M") if r["joined_at"] else "",
            r["chat_title"] or "",
        ])

    csv_bytes = output.getvalue().encode("utf-8-sig")  # BOM для Excel
    filename = f"users_{label}_{datetime.now().strftime('%Y%m%d_%H%M')}.csv"

    from aiogram.types import BufferedInputFile
    file = BufferedInputFile(csv_bytes, filename=filename)

    bot_row = await db.fetchrow(
        "SELECT bot_username FROM child_bots WHERE id=$1", child_bot_id)
    caption = (
        f"📊 <b>База пользователей @{bot_row['bot_username'] if bot_row else ''}</b>\n\n"
        f"🔖 Фильтр: <b>{label}</b>\n"
        f"👥 Записей: <b>{len(rows):,}</b>\n\n"
        "Формат: CSV (разделитель ;)\n"
        "Кодировка: UTF-8 с BOM (совместимо с Excel)"
    )

    try:
        await bot.send_document(
            chat_id=owner_id,
            document=file,
            caption=caption,
            parse_mode="HTML",
        )
        # Обновляем сообщение-меню
        await callback.message.edit_text(
            "🗄 <b>База пользователей</b>\n\n"
            f"✅ Файл <code>{filename}</code> отправлен.\n"
            f"Записей: <b>{len(rows):,}</b>",
            reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                [InlineKeyboardButton(text="◀️ Назад", callback_data=f"bs_base:{child_bot_id}")],
            ]),
        )
    except Exception as e:
        logger.error(f"CSV export error: {e}")
        await callback.message.edit_text(
            "❌ Ошибка при отправке файла. Попробуйте позже.",
            reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                [InlineKeyboardButton(text="◀️ Назад", callback_data=f"bs_base:{child_bot_id}")],
            ]),
        )


# ── Часовой пояс —————————————————————————————————————————————
# Репрезентативные зоны для каждого UTC-смещения (24 пояса)
_TZ_GRID = [
    "Pacific/Pago_Pago",      # UTC−11
    "Pacific/Honolulu",       # UTC−10
    "America/Anchorage",      # UTC−9
    "America/Los_Angeles",    # UTC−8
    "America/Denver",         # UTC−7
    "America/Chicago",        # UTC−6
    "America/New_York",       # UTC−5
    "America/Halifax",        # UTC−4
    "America/Sao_Paulo",      # UTC−3
    "Atlantic/South_Georgia", # UTC−2
    "Atlantic/Azores",        # UTC−1
    "UTC",                    # UTC+0
    "Europe/Berlin",          # UTC+1
    "Europe/Kyiv",            # UTC+2
    "Europe/Moscow",          # UTC+3
    "Asia/Dubai",             # UTC+4
    "Asia/Karachi",           # UTC+5
    "Asia/Dhaka",             # UTC+6
    "Asia/Bangkok",           # UTC+7
    "Asia/Shanghai",          # UTC+8
    "Asia/Tokyo",             # UTC+9
    "Australia/Sydney",       # UTC+10/+11
    "Pacific/Auckland",       # UTC+12
    "Pacific/Apia",           # UTC+13
]


def _tz_grid_buttons(child_bot_id: int) -> list[list[InlineKeyboardButton]]:
    """Строит сетку кнопок с текущим временем в каждой зоне (4 колонки).
    Время кодируется в callback_data через тире (HH-MM), чтобы не конфликтовать
    с разделителем ':' при split.
    """
    now_utc = datetime.now(dt_timezone.utc)
    rows: list[list[InlineKeyboardButton]] = []
    row: list[InlineKeyboardButton] = []
    for tz_name in _TZ_GRID:
        try:
            zi = zoneinfo.ZoneInfo(tz_name)
            label = now_utc.astimezone(zi).strftime("%H:%M")
        except Exception:
            label = now_utc.strftime("%H:%M")
        # Время кодируем через тире, чтобы ':' не ломал split при парсинге
        # Формат: bs_tz_pick:{child_bot_id}:{HH-MM}:{tz_name}
        time_encoded = label.replace(":", "-")  # «18:29» → «18-29»
        row.append(InlineKeyboardButton(
            text=label,
            callback_data=f"bs_tz_pick:{child_bot_id}:{time_encoded}:{tz_name}",
        ))
        if len(row) == 4:
            rows.append(row)
            row = []
    if row:
        rows.append(row)
    return rows


@router.callback_query(F.data.startswith("bs_timezone:"))
async def on_bs_timezone(callback: CallbackQuery, platform_user: dict | None,
                         _selected_time: str | None = None,
                         _child_bot_id: int | None = None):
    """Экран 2: текущий часовой пояс + время в blockquote.
    _selected_time — время с нажатой кнопки (если пришли из bs_tz_pick).
    _child_bot_id — если callback.data не bs_timezone:X (например, вызов из bs_tz_pick).
    """
    if not platform_user:
        return
    # При прямом вызове из bs_tz_pick callback.data нельзя мутировать (модель заморожена),
    # поэтому child_bot_id передаётся явным параметром.
    child_bot_id = _child_bot_id if _child_bot_id is not None else int(callback.data.split(":")[1])
    owner_id = platform_user["user_id"]

    ch = await _get_bot_first_chat(owner_id, child_bot_id)
    current_tz = ch["timezone"] if ch and ch.get("timezone") else "Europe/Moscow"

    if _selected_time:
        time_str = _selected_time
    else:
        try:
            zi = zoneinfo.ZoneInfo(current_tz)
            time_str = datetime.now(dt_timezone.utc).astimezone(zi).strftime("%H:%M")
        except Exception:
            time_str = "--:--"

    await callback.message.edit_text(
        "🌙 <b>Часовой пояс</b>\n\n"
        f"<blockquote>ℹ Сейчас установлен часовой пояс: {current_tz}.</blockquote>\n"
        f"<blockquote>🕐 Текущее время: {time_str}</blockquote>\n\n"
        "Выберите действие ⬇️",
        parse_mode="HTML",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(
                text="✏️ Изменить часовой пояс",
                callback_data=f"bs_tz_change:{child_bot_id}",
            )],
            [InlineKeyboardButton(
                text="◀️ Назад",
                callback_data=f"bs_settings:{child_bot_id}",
            )],
        ]),
    )
    await callback.answer()


@router.callback_query(F.data.startswith("bs_tz_change:"))
async def on_bs_tz_change(callback: CallbackQuery, platform_user: dict | None):
    """Экран 3: сетка времён — выбери своё текущее время."""
    if not platform_user:
        return
    child_bot_id = int(callback.data.split(":")[1])
    owner_id = platform_user["user_id"]

    ch = await _get_bot_first_chat(owner_id, child_bot_id)
    current_tz = ch["timezone"] if ch and ch.get("timezone") else "Europe/Moscow"

    grid = _tz_grid_buttons(child_bot_id)
    grid.append([InlineKeyboardButton(
        text="⏳ Обновить время",
        callback_data=f"bs_tz_change:{child_bot_id}",
    )])
    grid.append([InlineKeyboardButton(
        text="◀️ Назад",
        callback_data=f"bs_timezone:{child_bot_id}",
    )])

    await callback.message.edit_text(
        f"🌙 <b>Часовой пояс:</b> {current_tz}\n\n"
        "<blockquote>ℹ Для смены часового пояса выберите текущее время "
        "для вашего региона.</blockquote>\n\n"
        "Выберите действие ⬇️",
        parse_mode="HTML",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=grid),
    )
    await callback.answer()


@router.callback_query(F.data.startswith("bs_tz_pick:"))
async def on_bs_tz_pick(callback: CallbackQuery, platform_user: dict | None):
    """Пользователь нажал на время — сохраняем часовой пояс и возвращаемся на экран 2.
    Формат callback_data: bs_tz_pick:{child_bot_id}:{HH-MM}:{tz_name}
    Время закодировано через тире, tz_name — всё после времени.
    """
    if not platform_user:
        return
    parts = callback.data.split(":")
    # parts[0]='bs_tz_pick', parts[1]=child_bot_id, parts[2]=HH-MM (время),
    # parts[3:] = tz_name (например ['Europe', 'Kyiv'] → 'Europe/Kyiv')
    child_bot_id = int(parts[1])
    time_encoded = parts[2]                    # «18-29»
    selected_time = time_encoded.replace("-", ":")  # «18:29»
    tz = ":".join(parts[3:])                   # «Europe/Kyiv» или «UTC»
    owner_id = platform_user["user_id"]

    await db.execute(
        "UPDATE bot_chats SET timezone=$1 WHERE child_bot_id=$2 AND owner_id=$3",
        tz, child_bot_id, owner_id,
    )
    await callback.answer(f"✅ Часовой пояс: {tz}")
    # Не мутируем callback.data (модель заморожена), передаём child_bot_id напрямую
    await on_bs_timezone(callback, platform_user, _selected_time=selected_time, _child_bot_id=child_bot_id)



# ── Рассылка / Ссылки / Обратная связь — выбор канала ────────
async def _bs_channel_picker(callback: CallbackQuery, platform_user: dict,
                             child_bot_id: int, section: str, title: str):
    owner_id = await resolve_owner_id(platform_user["user_id"], child_bot_id)
    if owner_id is None:
        await callback.answer("Нет доступа", show_alert=True)
        return
    chats = await db.fetch(
        "SELECT chat_id, chat_title FROM bot_chats WHERE child_bot_id=$1 AND owner_id=$2",
        child_bot_id, owner_id)
    if not chats:
        await callback.answer("У бота нет подключенных площадок", show_alert=True)
        return
    buttons = [[InlineKeyboardButton(
        text=f"📍 {ch['chat_title'] or ch['chat_id']}",
        # Передаём child_bot_id в callback чтобы Back-кнопка не терялась
        callback_data=f"{section}:{ch['chat_id']}:{child_bot_id}",
    )] for ch in chats]
    buttons.append([InlineKeyboardButton(text="◀️ Назад", callback_data=f"bot_settings:{child_bot_id}")])
    await callback.message.edit_text(
        f"{title}\n\nВыберите площадку:",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=buttons))
    await callback.answer()


@router.callback_query(F.data.startswith("bs_mailing:"))
async def on_bs_mailing(callback: CallbackQuery, platform_user: dict | None):
    """Рассылка: показываем экран выбора действия сразу, без выбора площадки."""
    if not platform_user:
        return
    child_bot_id = int(callback.data.split(":")[1])

    # Проверяем тариф реального владельца бота (не admin'а)
    owner_id = await resolve_owner_id(platform_user["user_id"], child_bot_id)
    if owner_id is None:
        await callback.answer("Нет доступа", show_alert=True)
        return
    owner_row = await db.fetchrow(
        "SELECT tariff FROM platform_users WHERE user_id=$1", owner_id
    )
    tariff = owner_row["tariff"] if owner_row else "free"
    if tariff == "free":
        await callback.answer("Рассылка доступна с тарифа Старт.", show_alert=True)
        return

    # Удаляем эхо-сообщение, если возвращаемся из меню черновика
    from handlers.mailing import _extract_mailing_id_from_keyboard, _delete_draft_echo
    mid = _extract_mailing_id_from_keyboard(callback)
    if mid is not None:
        await _delete_draft_echo(callback.bot, mid)

    await callback.message.edit_text(
        "📨 <b>Рассылка</b>\n\nВыберите действие ⬇️",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="➕ Создать рассылку",
                                  callback_data=f"mailing_bot_start:{child_bot_id}")],
            [InlineKeyboardButton(text="📅 Запланированные",
                                  callback_data=f"mailing_bot_scheduled:{child_bot_id}")],
            [InlineKeyboardButton(text="◀️ Назад",
                                  callback_data=f"bot_settings:{child_bot_id}")],
        ]),
    )
    await callback.answer()


@router.callback_query(F.data.startswith("bs_links:"))
async def on_bs_links(callback: CallbackQuery, platform_user: dict | None):
    if not platform_user:
        return
    await _bs_channel_picker(callback, platform_user, int(callback.data.split(":")[1]),
                             "ch_links", "🔗 <b>Ссылки</b>")


@router.callback_query(F.data.startswith("bs_feedback:"))
async def on_bs_feedback(callback: CallbackQuery, platform_user: dict | None):
    if not platform_user:
        return
    child_bot_id = int(callback.data.split(":")[1])
    from handlers.feedback import show_bot_feedback
    await show_bot_feedback(callback, platform_user, child_bot_id)



# ══════════════════════════════════════════════════════════════
# Команда — совместное управление ботом
# ══════════════════════════════════════════════════════════════

import secrets


async def _get_or_create_team_invites(owner_id: int, child_bot_id: int) -> dict:
    """Возвращает или создаёт invite-токены для admin и owner ролей."""
    rows = await db.fetch(
        "SELECT role, token FROM team_invites "
        "WHERE owner_id=$1 AND child_bot_id=$2",
        owner_id, child_bot_id,
    )
    tokens = {r["role"]: r["token"] for r in rows}
    for role in ("admin", "owner"):
        if role not in tokens:
            tok = secrets.token_hex(16)
            await db.execute(
                "INSERT INTO team_invites (owner_id, child_bot_id, token, role) "
                "VALUES ($1, $2, $3, $4) "
                "ON CONFLICT (token) DO NOTHING",
                owner_id, child_bot_id, tok, role,
            )
            tokens[role] = tok
    return tokens


async def _show_bs_team(callback: CallbackQuery, bot: Bot,
                        platform_user: dict, child_bot_id: int):
    owner_id = platform_user["user_id"]
    me = await bot.get_me()
    platform_username = me.username

    tokens = await _get_or_create_team_invites(owner_id, child_bot_id)
    admin_link = f"https://t.me/{platform_username}?start=team-{tokens['admin']}"
    owner_link  = f"https://t.me/{platform_username}?start=team-{tokens['owner']}"

    members = await db.fetch(
        "SELECT user_id, username, role FROM team_members "
        "WHERE owner_id=$1 AND child_bot_id=$2 "
        "ORDER BY added_at",
        owner_id, child_bot_id,
    )
    member_lines = ""
    if members:
        lines = []
        for m in members:
            uname = f"@{m['username']}" if m["username"] else f"ID:{m['user_id']}"
            role_label = "👑 Владелец" if m["role"] == "owner" else "🛡 Админ"
            lines.append(f"  • {uname} — {role_label}")
        member_lines = "\n\n👥 <b>Участники команды:</b>\n" + "\n".join(lines)

    await callback.message.edit_text(
        "<blockquote>"
        "👥 Вы можете добавить админов для совместного управления ботом.\n\n"
        "🔄 Ссылки необходимо обновлять после использования."
        "</blockquote>\n\n"
        "👤 Чтобы добавить администратора,\n"
        f"   отправьте ему ссылку →\n<code>{admin_link}</code>\n\n"
        "👑 Чтобы сменить владельца бота,\n"
        f"   отправьте ему ссылку →\n<code>{owner_link}</code>"
        f"{member_lines}\n\n"
        "Выберите действие ⬇️",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="→ Добавить администратора",
                                  url=f"https://t.me/share/url?url={admin_link}")],
            [InlineKeyboardButton(text="→ Сменить владельца",
                                  url=f"https://t.me/share/url?url={owner_link}")],
            [InlineKeyboardButton(text="🔄 Обновить ссылки",
                                  callback_data=f"bs_team_refresh:{child_bot_id}")],
            *([
                [InlineKeyboardButton(text="👤 Участники команды",
                                      callback_data=f"bs_team_members:{child_bot_id}")]
            ] if members else []),
            [InlineKeyboardButton(text="◀️ Назад",
                                  callback_data=f"bs_settings:{child_bot_id}")],
        ]),
    )
    await callback.answer()


@router.callback_query(F.data.startswith("bs_team:"))
async def on_bs_team(callback: CallbackQuery, bot: Bot,
                     platform_user: dict | None):
    if not platform_user:
        return
    child_bot_id = int(callback.data.split(":")[1])
    owner_id = platform_user["user_id"]
    # Только владелец видит экран команды (проверяем owner_id в child_bots)
    row = await db.fetchrow(
        "SELECT id FROM child_bots WHERE id=$1 AND owner_id=$2",
        child_bot_id, owner_id,
    )
    if not row:
        await callback.answer("Только владелец бота может управлять командой", show_alert=True)
        return
    await _show_bs_team(callback, bot, platform_user, child_bot_id)


@router.callback_query(F.data.startswith("bs_team_refresh:"))
async def on_bs_team_refresh(callback: CallbackQuery, bot: Bot,
                             platform_user: dict | None):
    if not platform_user:
        return
    child_bot_id = int(callback.data.split(":")[1])
    owner_id = platform_user["user_id"]
    # Удаляем старые токены — они будут пересозданы
    await db.execute(
        "DELETE FROM team_invites WHERE owner_id=$1 AND child_bot_id=$2",
        owner_id, child_bot_id,
    )
    await _show_bs_team(callback, bot, platform_user, child_bot_id)


@router.callback_query(F.data.startswith("bs_team_members:"))
async def on_bs_team_members(callback: CallbackQuery, platform_user: dict | None):
    if not platform_user:
        return
    child_bot_id = int(callback.data.split(":")[1])
    owner_id = platform_user["user_id"]

    members = await db.fetch(
        "SELECT id, user_id, username, role, added_at FROM team_members "
        "WHERE owner_id=$1 AND child_bot_id=$2 "
        "ORDER BY added_at",
        owner_id, child_bot_id,
    )
    if not members:
        await callback.answer("Команда пуста", show_alert=True)
        return

    buttons = []
    for m in members:
        uname = f"@{m['username']}" if m["username"] else f"ID:{m['user_id']}"
        role_label = "👑" if m["role"] == "owner" else "🛡"
        buttons.append([InlineKeyboardButton(
            text=f"{role_label} {uname} — ❌ Удалить",
            callback_data=f"bs_team_remove:{child_bot_id}:{m['id']}",
        )])
    buttons.append([InlineKeyboardButton(
        text="◀️ Назад", callback_data=f"bs_team:{child_bot_id}",
    )])

    await callback.message.edit_text(
        "👥 <b>Участники команды</b>\n\nНажмите на участника чтобы удалить его:",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=buttons),
    )
    await callback.answer()


@router.callback_query(F.data.startswith("bs_team_remove:"))
async def on_bs_team_remove(callback: CallbackQuery, platform_user: dict | None):
    if not platform_user:
        return
    parts = callback.data.split(":")
    child_bot_id, member_db_id = int(parts[1]), int(parts[2])
    owner_id = platform_user["user_id"]
    await db.execute(
        "UPDATE team_members SET is_active=false WHERE id=$1 AND owner_id=$2",
        member_db_id, owner_id,
    )
    await callback.answer("✅ Участник удалён из команды")
    # Перерендер списка
    callback.data = f"bs_team_members:{child_bot_id}"
    await on_bs_team_members(callback, platform_user)
