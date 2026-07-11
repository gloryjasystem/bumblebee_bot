"""
handlers/welcome_seq.py — Цепочка сообщений (приветствие №1 + доп. шаги с интервалами).

Модель (Подход 1): базовое приветствие (welcome_*) — это шаг №1 (сразу), он же
редактируется прямо из менеджера цепочки кнопкой «✏️ №1 Приветствие» (тот же
редактор приветствия). Здесь настраиваются ДОПОЛНИТЕЛЬНЫЕ шаги, каждый со своей
задержкой от вступления:
  • шаг-«сообщение» — текст/медиа/кнопки + опц. авто-удаление;
  • шаг-«удаление»  — очищает переписку цепочки (базовое + все шаги).

Редактор шага повторяет редактор приветствия: сверху эхо-превью сообщения,
снизу контрол с кнопками. Эхо чистится при выходе, его id помнится в FSM.

Пример из ТЗ:
  №1 сразу        — «подпишись»          (базовое приветствие)
  №2 через 15 сек — «подписался?»        (шаг-сообщение, delay 15с)
  №3 через 1 мин  — очистка переписки     (шаг-удаление, delay 60с)
"""
import json as _json
import logging

from aiogram import Router, F
from aiogram.types import (
    Message, CallbackQuery, InlineKeyboardMarkup, InlineKeyboardButton,
)
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup

import db.pool as db
from db.channels import resolve_chat_owner
from services.security import sanitize
from utils.nav import navigate, set_active_msg
from utils.timing import format_delay_short, parse_delay_input

logger = logging.getLogger(__name__)
router = Router()

_SELF_DELETE_CYCLE = [0, 5, 15, 30, 60, 120, 300]  # секунды авто-удаления шага
_MAX_STEPS = 10  # максимум доп. шагов в цепочке
# Быстрые пресеты задержки шага (сек, подпись)
_DELAY_CHIPS = [(0, "сразу"), (15, "15с"), (30, "30с"), (60, "1 мин"), (300, "5 мин"), (900, "15 мин")]


class WSeqFSM(StatesGroup):
    adding_content = State()   # ждём текст/медиа нового шага-сообщения
    step_content   = State()   # ждём новый текст существующего шага
    step_media     = State()   # ждём медиа шага
    step_buttons   = State()   # ждём кнопки шага
    step_delay     = State()   # ждём свою задержку шага


# ── Вспомогательные ──────────────────────────────────────────────

def _self_del_label(sec: int) -> str:
    if not sec:
        return "нет"
    return f"{sec} сек" if sec < 60 else f"{sec // 60} мин"


def _delay_offset(sec) -> str:
    """Компактная подпись задержки шага: «сразу» / «+15с» / «+1м»."""
    sec = int(sec or 0)
    return "сразу" if sec <= 0 else f"+{format_delay_short(sec)}"


def _apply_vars(text: str, user, chat_title: str) -> str:
    """Подстановка переменных для предпросмотра (как в реальной отправке)."""
    if not text:
        return text
    import datetime
    return (text
        .replace("{name}", getattr(user, "first_name", None) or "Пользователь")
        .replace("{allname}", f"{getattr(user, 'first_name', '') or ''} {getattr(user, 'last_name', '') or ''}".strip())
        .replace("{username}", f"@{user.username}" if getattr(user, "username", None) else "")
        .replace("{chat}", chat_title or "")
        .replace("{day}", datetime.date.today().strftime("%d.%m.%Y"))
    )


def _parse_buttons(raw: str) -> list:
    """Парсит текст кнопок в формат [[{text,url}], ...] (как в редакторе сообщений)."""
    buttons = []
    for line in (raw or "").splitlines():
        line = line.strip()
        if not line:
            continue
        row = []
        for btn_raw in line.split("|"):
            btn_raw = btn_raw.strip()
            sep = None
            for s in (" — ", " – ", " - ", "—", "–", "-"):
                if s in btn_raw:
                    sep = s
                    break
            if sep:
                idx = btn_raw.index(sep)
                btn_text = btn_raw[:idx].strip()
                btn_url = btn_raw[idx + len(sep):].strip()
                if btn_text and btn_url.startswith("http"):
                    row.append({"text": btn_text, "url": btn_url})
        if row:
            buttons.append(row)
    return buttons


def _extract_media(message: Message):
    if message.photo:
        return message.photo[-1].file_id, "photo"
    if message.video:
        return message.video.file_id, "video"
    if message.animation:
        return message.animation.file_id, "animation"
    if message.document:
        return message.document.file_id, "document"
    return None, None


async def _own_chat(owner_id: int, chat_id: int) -> bool:
    row = await db.fetchval(
        "SELECT 1 FROM bot_chats WHERE owner_id=$1 AND chat_id=$2::bigint",
        owner_id, chat_id,
    )
    return bool(row)


async def _get_step(owner_id: int, step_id: int):
    return await db.fetchrow(
        "SELECT * FROM welcome_steps WHERE id=$1 AND owner_id=$2",
        step_id, owner_id,
    )


async def _step_owner(user_id: int, step_id: int) -> int:
    """owner_id, от имени которого user_id может править шаг step_id.

    Владелец шага → сам user_id; активный член команды бота, держащего чат шага →
    реальный owner_id; иначе → user_id (fallback: запросы WHERE owner_id=user_id
    ничего не найдут, как и раньше для постороннего). Нужно для командного доступа.
    """
    row = await db.fetchrow(
        "SELECT owner_id, chat_id FROM welcome_steps WHERE id=$1", step_id
    )
    if not row:
        return user_id
    resolved = await resolve_chat_owner(user_id, row["chat_id"])
    return resolved if resolved == row["owner_id"] else user_id


# ── Эхо редактора шага (память в FSM) ────────────────────────────

async def _drop_echo(bot, state: FSMContext | None, default_chat_id: int | None = None):
    """Удаляет текущее эхо-сообщение шага (если есть) и забывает его id.

    id эхо помнится в FSM (wstep_echo_mid) — как editor_echo_mid у приветствия.
    Все удаления best-effort: если сообщение уже удалено, ничего не падает.
    """
    if not state:
        return
    data = await state.get_data()
    mid = data.get("wstep_echo_mid")
    chat_id = data.get("wstep_echo_chat_id") or default_chat_id
    if mid and chat_id:
        try:
            await bot.delete_message(chat_id=chat_id, message_id=mid)
        except Exception:
            pass
    await state.update_data(wstep_echo_mid=None, wstep_echo_chat_id=None)


# ── Экран: менеджер цепочки ──────────────────────────────────────

async def _show_manager(event, chat_id: int, owner_id: int):
    steps = await db.fetch(
        "SELECT * FROM welcome_steps WHERE owner_id=$1 AND chat_id=$2::bigint "
        "ORDER BY step_order ASC, delay_sec ASC, id ASC",
        owner_id, chat_id,
    )
    base = await db.fetchrow(
        "SELECT welcome_text, welcome_media FROM bot_chats WHERE owner_id=$1 AND chat_id=$2::bigint",
        owner_id, chat_id,
    )
    has_base = bool(base and (base["welcome_text"] or base["welcome_media"]))

    # ── Тело: таймлайн ──
    lines = ["⛓ <b>Цепочка сообщений</b>\n"]
    if has_base:
        lines.append("<b>№1</b> · <i>сразу</i>\n   👋 Базовое приветствие\n")
    else:
        lines.append("<b>№1</b> · <i>сразу</i>\n   ⚠️ приветствие не задано\n")

    max_delay = 0
    for i, st in enumerate(steps, start=2):
        delay = int(st["delay_sec"] or 0)
        max_delay = max(max_delay, delay)
        if (st["action"] or "message") == "delete":
            icon, prev = "🧹", "очистить переписку"
        else:
            txt = (st["text"] or "").strip().replace("\n", " ")
            if st["media"] and not txt:
                icon, prev = "🖼", "медиа"
            elif st["media"]:
                icon, prev = "🖼", txt[:28]
            else:
                icon, prev = "✉️", (txt[:32] or "—")
        lines.append(f"<b>№{i}</b> · <i>{_delay_offset(delay)}</i>\n   {icon} {prev}\n")

    if not steps:
        lines.append("\nПока только приветствие (№1). Добавьте шаги ниже — до 10.")
    else:
        total = (1 if has_base else 0) + len(steps)
        fin = "сразу" if max_delay == 0 else f"+{format_delay_short(max_delay)}"
        lines.append(f"\n<i>Итог: {total} сообщ. · финал {fin} · до {_MAX_STEPS} доп. шагов.</i>")
    if not has_base:
        lines.append(
            "\n⚠️ <b>Пока приветствие (№1) не задано — цепочка не отправится.</b> "
            "Задайте его кнопкой ниже."
        )

    # ── Клавиатура ──
    kb_rows = [[InlineKeyboardButton(
        text=("✏️ №1 Приветствие" if has_base else "✏️ Задать приветствие №1"),
        callback_data=f"welcome_set:{chat_id}",
    )]]
    for i, st in enumerate(steps, start=2):
        delay = int(st["delay_sec"] or 0)
        if (st["action"] or "message") == "delete":
            prev = "очистить переписку"
        else:
            txt = (st["text"] or "").strip().replace("\n", " ")
            prev = "медиа" if (st["media"] and not txt) else (txt[:20] or "—")
        kb_rows.append([InlineKeyboardButton(
            text=f"✏️ №{i} · {_delay_offset(delay)} · {prev}",
            callback_data=f"wstep:{st['id']}",
        )])

    if len(steps) < _MAX_STEPS:
        kb_rows.append([
            InlineKeyboardButton(text="➕ Сообщение", callback_data=f"wseq_add:{chat_id}:msg"),
            InlineKeyboardButton(text="🧹 Очистить переписку", callback_data=f"wseq_add:{chat_id}:del"),
        ])
    if steps:
        kb_rows.append([InlineKeyboardButton(text="👁 Проверить цепочку", callback_data=f"wseq_test:{chat_id}")])
        kb_rows.append([InlineKeyboardButton(text="🗑 Удалить все шаги", callback_data=f"wseq_clear:{chat_id}")])
    kb_rows.append([InlineKeyboardButton(text="◀️ Назад", callback_data=f"ch_messages:{chat_id}")])

    kb = InlineKeyboardMarkup(inline_keyboard=kb_rows)
    text = "\n".join(lines)
    if isinstance(event, CallbackQuery):
        await navigate(event, text, reply_markup=kb)
    else:
        await event.answer(text, reply_markup=kb, parse_mode="HTML")


@router.callback_query(F.data.startswith("wseq:"))
async def on_wseq(callback: CallbackQuery, state: FSMContext, platform_user: dict | None):
    if not platform_user:
        return
    chat_id = int(callback.data.split(":")[1])
    owner_id = await resolve_chat_owner(platform_user["user_id"], chat_id)
    if not await _own_chat(owner_id, chat_id):
        await callback.answer("Площадка не найдена", show_alert=True)
        return
    await _drop_echo(callback.bot, state, callback.message.chat.id)
    await state.clear()
    await _show_manager(callback, chat_id, owner_id)


# ── Добавление шага ──────────────────────────────────────────────

@router.callback_query(F.data.startswith("wseq_add:"))
async def on_wseq_add(callback: CallbackQuery, state: FSMContext, platform_user: dict | None):
    if not platform_user:
        return
    _, chat_id_s, kind = callback.data.split(":")
    chat_id = int(chat_id_s)
    owner_id = await resolve_chat_owner(platform_user["user_id"], chat_id)
    if not await _own_chat(owner_id, chat_id):
        await callback.answer("Площадка не найдена", show_alert=True)
        return

    count = await db.fetchval(
        "SELECT COUNT(*) FROM welcome_steps WHERE owner_id=$1 AND chat_id=$2::bigint",
        owner_id, chat_id,
    ) or 0
    if count >= _MAX_STEPS:
        await callback.answer(f"Достигнут лимит шагов ({_MAX_STEPS})", show_alert=True)
        return

    next_order = (await db.fetchval(
        "SELECT COALESCE(MAX(step_order), 1) FROM welcome_steps WHERE owner_id=$1 AND chat_id=$2::bigint",
        owner_id, chat_id,
    ) or 1) + 1

    if kind == "del":
        # Шаг-очистка создаём сразу, дефолтная задержка 60с
        step_id = await db.fetchval(
            "INSERT INTO welcome_steps (owner_id, chat_id, step_order, delay_sec, action) "
            "VALUES ($1, $2, $3, 60, 'delete') RETURNING id",
            owner_id, chat_id, next_order,
        )
        await callback.answer("🧹 Шаг очистки добавлен")
        await _show_step_editor(callback, step_id, owner_id, state)
        return

    # Шаг-сообщение — просим прислать контент
    await state.set_state(WSeqFSM.adding_content)
    await state.update_data(wseq_chat_id=chat_id, wseq_order=next_order)
    prompt = await navigate(
        callback,
        "➕ <b>Новое сообщение цепочки</b>\n\n"
        "Пришлите текст (и, при желании, медиа) сообщения.\n\n"
        "<b>Переменные:</b> <code>{name}</code>, <code>{allname}</code>, "
        "<code>{username}</code>, <code>{chat}</code>, <code>{day}</code>.",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="◀️ Отмена", callback_data=f"wseq:{chat_id}")],
        ]),
    )
    if prompt:
        await state.update_data(wstep_prompt_mid=prompt.message_id)


@router.message(WSeqFSM.adding_content)
async def on_adding_content(message: Message, state: FSMContext, platform_user: dict | None):
    if not platform_user:
        return
    data = await state.get_data()
    chat_id = data.get("wseq_chat_id")
    order = data.get("wseq_order", 2)
    if chat_id is None:
        await state.clear()
        return
    owner_id = await resolve_chat_owner(platform_user["user_id"], chat_id)

    text = sanitize(message.text or message.caption or "", max_len=1024)
    media_fid, media_type = _extract_media(message)
    if not text and not media_fid:
        await message.answer("Пришлите текст или медиа для сообщения.")
        return

    step_id = await db.fetchval(
        "INSERT INTO welcome_steps (owner_id, chat_id, step_order, delay_sec, action, text, media, media_type) "
        "VALUES ($1, $2, $3, 15, 'message', $4, $5, $6) RETURNING id",
        owner_id, chat_id, order, text or None, media_fid, media_type,
    )
    await _show_step_editor(message, step_id, owner_id, state)


# ── Экран: редактор шага (эхо + контрол) ─────────────────────────

async def _show_step_editor(event, step_id: int, owner_id: int, state: FSMContext | None = None):
    """Рисует редактор шага: сверху эхо-превью сообщения, снизу контрол с кнопками.

    Повторяет механизм редактора приветствия (_show_msg_editor): старое эхо
    удаляется, новое отправляется и его id помнится в FSM. Контрол на callback
    рисуем через navigate (сохраняет God Mode и SPA-указатель), на ввод текста —
    обычной отправкой с чисткой сообщения-приглашения и ввода пользователя.
    """
    st = await _get_step(owner_id, step_id)
    if not st:
        if isinstance(event, CallbackQuery):
            await event.answer("Шаг не найден", show_alert=True)
        return
    chat_id = st["chat_id"]
    is_del = (st["action"] or "message") == "delete"
    is_cb = isinstance(event, CallbackQuery)
    msg = event.message if is_cb else event
    bot = msg.bot
    tg_chat_id = msg.chat.id
    uid = event.from_user.id

    data = (await state.get_data()) if state else {}

    # 1) убрать старое эхо и сообщение-приглашение (если пришли из ввода)
    old_echo = data.get("wstep_echo_mid")
    if old_echo:
        try:
            await bot.delete_message(data.get("wstep_echo_chat_id") or tg_chat_id, old_echo)
        except Exception:
            pass
    prompt_mid = data.get("wstep_prompt_mid")
    if prompt_mid:
        try:
            await bot.delete_message(tg_chat_id, prompt_mid)
        except Exception:
            pass

    # 2) эхо-превью (только для шага-сообщения)
    new_echo_mid = None
    if not is_del:
        from utils.keyboard import build_inline_keyboard
        step_kb = build_inline_keyboard(st["buttons"])
        txt = st["text"] or ""
        media_fid = st["media"]
        media_type = st["media_type"]
        try:
            if media_fid:
                if media_type == "photo":
                    sent = await bot.send_photo(tg_chat_id, media_fid, caption=txt or None, reply_markup=step_kb, parse_mode="HTML")
                elif media_type == "video":
                    sent = await bot.send_video(tg_chat_id, media_fid, caption=txt or None, reply_markup=step_kb, parse_mode="HTML")
                elif media_type == "animation":
                    sent = await bot.send_animation(tg_chat_id, media_fid, caption=txt or None, reply_markup=step_kb, parse_mode="HTML")
                else:
                    sent = await bot.send_document(tg_chat_id, media_fid, caption=txt or None, reply_markup=step_kb, parse_mode="HTML")
            else:
                sent = await bot.send_message(tg_chat_id, txt or "—", reply_markup=step_kb, parse_mode="HTML")
            new_echo_mid = sent.message_id
        except Exception as e:
            logger.debug(f"[WSEQ] echo send failed step={step_id}: {e}")
            try:
                sent = await bot.send_message(tg_chat_id, txt or "🖼 медиа", parse_mode="HTML")
                new_echo_mid = sent.message_id
            except Exception:
                new_echo_mid = None

    # 3) контрол
    if is_del:
        body = (
            "🧹 <b>Шаг: очистить переписку</b>\n\n"
            f"⏱ Задержка: <b>{_delay_offset(st['delay_sec'])}</b> от заявки\n\n"
            "Через это время бот удалит <b>всю переписку</b> цепочки "
            "(базовое приветствие и все шаги)."
        )
        kb_rows = [
            [InlineKeyboardButton(text=f"⏱ Задержка: {_delay_offset(st['delay_sec'])}", callback_data=f"wstep_delay:{step_id}")],
            [InlineKeyboardButton(text="🗑 Удалить шаг", callback_data=f"wstep_rm:{step_id}")],
            [InlineKeyboardButton(text="◀️ Назад", callback_data=f"wseq:{chat_id}")],
        ]
    else:
        media_state = "есть" if st["media"] else "нет"
        body = "⚙️ <b>Настройки шага</b>"
        kb_rows = [
            [InlineKeyboardButton(text="✏️ Текст", callback_data=f"wstep_edit:{step_id}")],
            [InlineKeyboardButton(text="🎛 Кнопки", callback_data=f"wstep_btns:{step_id}")],
            [InlineKeyboardButton(text=f"🎬 Медиа: {media_state}", callback_data=f"wstep_media:{step_id}")],
            [InlineKeyboardButton(text=f"⏱ Задержка: {_delay_offset(st['delay_sec'])}", callback_data=f"wstep_delay:{step_id}")],
            [InlineKeyboardButton(text=f"⏳ Авто-удаление: {_self_del_label(st['self_delete_sec'] or 0)}", callback_data=f"wstep_selfdel:{step_id}")],
            [InlineKeyboardButton(text="🗑 Удалить шаг", callback_data=f"wstep_rm:{step_id}")],
            [InlineKeyboardButton(text="◀️ Назад", callback_data=f"wseq:{chat_id}")],
        ]
    kb = InlineKeyboardMarkup(inline_keyboard=kb_rows)

    if is_cb:
        # navigate удалит старый контрол (callback.message) и пришлёт новый под эхо
        await navigate(event, body, reply_markup=kb)
    else:
        try:
            await msg.delete()  # сообщение пользователя (ввод)
        except Exception:
            pass
        control = await bot.send_message(tg_chat_id, body, reply_markup=kb, parse_mode="HTML")
        await set_active_msg(uid, control.message_id)

    # 4) запомнить эхо в памяти (FSM), сбросить служебные ключи и состояние ввода
    if state:
        await state.set_state(None)
        await state.update_data(
            wstep_echo_mid=new_echo_mid,
            wstep_echo_chat_id=tg_chat_id,
            wstep_prompt_mid=None,
            wstep_id=step_id,
        )
    if is_cb:
        try:
            await event.answer()
        except Exception:
            pass


@router.callback_query(F.data.startswith("wstep:"))
async def on_wstep(callback: CallbackQuery, state: FSMContext, platform_user: dict | None):
    if not platform_user:
        return
    step_id = int(callback.data.split(":")[1])
    owner_id = await _step_owner(platform_user["user_id"], step_id)
    await _show_step_editor(callback, step_id, owner_id, state)


# ── Задержка шага: быстрые чипы + своё ───────────────────────────

@router.callback_query(F.data.startswith("wstep_delay:"))
async def on_wstep_delay(callback: CallbackQuery, state: FSMContext, platform_user: dict | None):
    if not platform_user:
        return
    step_id = int(callback.data.split(":")[1])
    owner_id = await _step_owner(platform_user["user_id"], step_id)
    st = await _get_step(owner_id, step_id)
    if not st:
        await callback.answer("Шаг не найден", show_alert=True)
        return
    await _drop_echo(callback.bot, state, callback.message.chat.id)
    await state.set_state(None)

    cur = int(st["delay_sec"] or 0)
    chip_rows, row = [], []
    for sec, label in _DELAY_CHIPS:
        mark = " ✅" if sec == cur else ""
        row.append(InlineKeyboardButton(text=f"{label}{mark}", callback_data=f"wstep_delayset:{step_id}:{sec}"))
        if len(row) == 3:
            chip_rows.append(row); row = []
    if row:
        chip_rows.append(row)
    chip_rows.append([InlineKeyboardButton(text="✏️ Своё значение", callback_data=f"wstep_delaycustom:{step_id}")])
    chip_rows.append([InlineKeyboardButton(text="◀️ Назад", callback_data=f"wstep:{step_id}")])

    await navigate(
        callback,
        "⏱ <b>Задержка шага</b>\n\n"
        "Через сколько после заявки показать этот шаг.\n"
        f"Сейчас: <b>{_delay_offset(cur)}</b>",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=chip_rows),
    )


@router.callback_query(F.data.startswith("wstep_delayset:"))
async def on_wstep_delayset(callback: CallbackQuery, state: FSMContext, platform_user: dict | None):
    if not platform_user:
        return
    _, step_id_s, sec_s = callback.data.split(":")
    step_id = int(step_id_s)
    owner_id = await _step_owner(platform_user["user_id"], step_id)
    sec = max(0, int(sec_s))
    await db.execute(
        "UPDATE welcome_steps SET delay_sec=$1 WHERE id=$2 AND owner_id=$3",
        sec, step_id, owner_id,
    )
    await callback.answer(f"⏱ {_delay_offset(sec)}")
    await _show_step_editor(callback, step_id, owner_id, state)


@router.callback_query(F.data.startswith("wstep_delaycustom:"))
async def on_wstep_delaycustom(callback: CallbackQuery, state: FSMContext, platform_user: dict | None):
    if not platform_user:
        return
    step_id = int(callback.data.split(":")[1])
    owner_id = await _step_owner(platform_user["user_id"], step_id)
    st = await _get_step(owner_id, step_id)
    if not st:
        await callback.answer("Шаг не найден", show_alert=True)
        return
    await state.set_state(WSeqFSM.step_delay)
    await state.update_data(wstep_id=step_id)
    prompt = await navigate(
        callback,
        "✏️ <b>Своё значение задержки</b>\n\n"
        "Пришлите число. По умолчанию — <b>секунды</b>.\n"
        "Примеры: <code>15</code>, <code>90с</code>, <code>5м</code>, <code>1ч</code>.",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="◀️ Отмена", callback_data=f"wstep_delay:{step_id}")],
        ]),
    )
    if prompt:
        await state.update_data(wstep_prompt_mid=prompt.message_id)


@router.message(WSeqFSM.step_delay)
async def on_wstep_delay_input(message: Message, state: FSMContext, platform_user: dict | None):
    if not platform_user:
        return
    data = await state.get_data()
    step_id = data.get("wstep_id")
    if not step_id:
        await state.clear()
        return
    owner_id = await _step_owner(platform_user["user_id"], step_id)
    sec = parse_delay_input(message.text or "")
    if sec is None:
        await message.answer("Не понял значение. Пришлите число, напр. <code>15</code>, <code>90с</code>, <code>5м</code>.", parse_mode="HTML")
        return
    await db.execute(
        "UPDATE welcome_steps SET delay_sec=$1 WHERE id=$2 AND owner_id=$3",
        sec, step_id, owner_id,
    )
    await _show_step_editor(message, step_id, owner_id, state)


# ── Текст шага ───────────────────────────────────────────────────

@router.callback_query(F.data.startswith("wstep_edit:"))
async def on_wstep_edit(callback: CallbackQuery, state: FSMContext, platform_user: dict | None):
    if not platform_user:
        return
    step_id = int(callback.data.split(":")[1])
    owner_id = await _step_owner(platform_user["user_id"], step_id)
    st = await _get_step(owner_id, step_id)
    if not st:
        await callback.answer("Шаг не найден", show_alert=True)
        return
    await _drop_echo(callback.bot, state, callback.message.chat.id)
    await state.set_state(WSeqFSM.step_content)
    await state.update_data(wstep_id=step_id)
    prompt = await navigate(
        callback,
        "✏️ <b>Текст шага</b>\n\n"
        "Пришлите новый текст сообщения.\n"
        "Переменные: <code>{name}</code>, <code>{allname}</code>, <code>{username}</code>, <code>{chat}</code>, <code>{day}</code>.",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="◀️ Отмена", callback_data=f"wstep:{step_id}")],
        ]),
    )
    if prompt:
        await state.update_data(wstep_prompt_mid=prompt.message_id)


@router.message(WSeqFSM.step_content)
async def on_wstep_content_input(message: Message, state: FSMContext, platform_user: dict | None):
    if not platform_user:
        return
    data = await state.get_data()
    step_id = data.get("wstep_id")
    if not step_id:
        await state.clear()
        return
    owner_id = await _step_owner(platform_user["user_id"], step_id)
    text = sanitize(message.text or message.caption or "", max_len=1024)
    if not text:
        await message.answer("Пришлите текст сообщения (для медиа есть кнопка «🎬 Медиа»).")
        return
    await db.execute(
        "UPDATE welcome_steps SET text=$1 WHERE id=$2 AND owner_id=$3",
        text, step_id, owner_id,
    )
    await _show_step_editor(message, step_id, owner_id, state)


# ── Медиа шага ───────────────────────────────────────────────────

@router.callback_query(F.data.startswith("wstep_media:"))
async def on_wstep_media(callback: CallbackQuery, state: FSMContext, platform_user: dict | None):
    if not platform_user:
        return
    step_id = int(callback.data.split(":")[1])
    owner_id = await _step_owner(platform_user["user_id"], step_id)
    st = await _get_step(owner_id, step_id)
    if not st:
        await callback.answer("Шаг не найден", show_alert=True)
        return
    await _drop_echo(callback.bot, state, callback.message.chat.id)
    await state.set_state(WSeqFSM.step_media)
    await state.update_data(wstep_id=step_id)
    has_media = bool(st["media"])
    hint = "Сейчас медиа прикреплено. Пришлите новое, чтобы заменить, или <code>-</code>, чтобы убрать." if has_media \
        else "Пришлите фото, видео, GIF или документ для этого шага."
    prompt = await navigate(
        callback,
        f"🎬 <b>Медиа шага</b>\n\n{hint}",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="◀️ Отмена", callback_data=f"wstep:{step_id}")],
        ]),
    )
    if prompt:
        await state.update_data(wstep_prompt_mid=prompt.message_id)


@router.message(WSeqFSM.step_media)
async def on_wstep_media_input(message: Message, state: FSMContext, platform_user: dict | None):
    if not platform_user:
        return
    data = await state.get_data()
    step_id = data.get("wstep_id")
    if not step_id:
        await state.clear()
        return
    owner_id = await _step_owner(platform_user["user_id"], step_id)
    if (message.text or "").strip() == "-":
        await db.execute(
            "UPDATE welcome_steps SET media=NULL, media_type=NULL WHERE id=$1 AND owner_id=$2",
            step_id, owner_id,
        )
        await _show_step_editor(message, step_id, owner_id, state)
        return
    media_fid, media_type = _extract_media(message)
    if not media_fid:
        await message.answer("⚠️ Поддерживаются: фото, видео, GIF, документ. Или <code>-</code>, чтобы убрать.", parse_mode="HTML")
        return
    await db.execute(
        "UPDATE welcome_steps SET media=$1, media_type=$2 WHERE id=$3 AND owner_id=$4",
        media_fid, media_type, step_id, owner_id,
    )
    await _show_step_editor(message, step_id, owner_id, state)


# ── Кнопки шага ──────────────────────────────────────────────────

@router.callback_query(F.data.startswith("wstep_btns:"))
async def on_wstep_btns(callback: CallbackQuery, state: FSMContext, platform_user: dict | None):
    if not platform_user:
        return
    step_id = int(callback.data.split(":")[1])
    owner_id = await _step_owner(platform_user["user_id"], step_id)
    st = await _get_step(owner_id, step_id)
    if not st:
        await callback.answer("Шаг не найден", show_alert=True)
        return
    await _drop_echo(callback.bot, state, callback.message.chat.id)
    await state.set_state(WSeqFSM.step_buttons)
    await state.update_data(wstep_id=step_id)
    prompt = await navigate(
        callback,
        "🎛 <b>Кнопки шага</b>\n\n"
        "Пришлите кнопки в формате:\n"
        "<blockquote><code>Текст — https://ссылка</code></blockquote>\n"
        "Несколько в ряд — через <code>|</code>. Отправьте <code>-</code>, чтобы убрать кнопки.",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="◀️ Отмена", callback_data=f"wstep:{step_id}")],
        ]),
    )
    if prompt:
        await state.update_data(wstep_prompt_mid=prompt.message_id)


@router.message(WSeqFSM.step_buttons)
async def on_wstep_btns_input(message: Message, state: FSMContext, platform_user: dict | None):
    if not platform_user:
        return
    data = await state.get_data()
    step_id = data.get("wstep_id")
    if not step_id:
        await state.clear()
        return
    owner_id = await _step_owner(platform_user["user_id"], step_id)
    raw = sanitize(message.text or "", max_len=2048)
    if raw == "-":
        buttons = []
    else:
        buttons = _parse_buttons(raw)
        if not buttons:
            await message.answer(
                "⚠️ Не распознал кнопки. Формат: <code>Текст — https://ссылка</code>. "
                "Или <code>-</code>, чтобы убрать.",
                parse_mode="HTML",
            )
            return
    await db.execute(
        "UPDATE welcome_steps SET buttons=$1::jsonb WHERE id=$2 AND owner_id=$3",
        _json.dumps(buttons, ensure_ascii=False), step_id, owner_id,
    )
    await _show_step_editor(message, step_id, owner_id, state)


# ── Авто-удаление шага (цикл) ────────────────────────────────────

@router.callback_query(F.data.startswith("wstep_selfdel:"))
async def on_wstep_selfdel(callback: CallbackQuery, state: FSMContext, platform_user: dict | None):
    if not platform_user:
        return
    step_id = int(callback.data.split(":")[1])
    owner_id = await _step_owner(platform_user["user_id"], step_id)
    st = await _get_step(owner_id, step_id)
    if not st:
        await callback.answer("Шаг не найден", show_alert=True)
        return
    cur = int(st["self_delete_sec"] or 0)
    try:
        idx = _SELF_DELETE_CYCLE.index(cur)
    except ValueError:
        idx = 0
    new_val = _SELF_DELETE_CYCLE[(idx + 1) % len(_SELF_DELETE_CYCLE)]
    await db.execute(
        "UPDATE welcome_steps SET self_delete_sec=$1 WHERE id=$2 AND owner_id=$3",
        new_val, step_id, owner_id,
    )
    await callback.answer(f"⏳ Авто-удаление: {_self_del_label(new_val)}")
    await _show_step_editor(callback, step_id, owner_id, state)


# ── Удаление шага / очистка / предпросмотр ───────────────────────

@router.callback_query(F.data.startswith("wstep_rm:"))
async def on_wstep_rm(callback: CallbackQuery, state: FSMContext, platform_user: dict | None):
    if not platform_user:
        return
    step_id = int(callback.data.split(":")[1])
    owner_id = await _step_owner(platform_user["user_id"], step_id)
    st = await _get_step(owner_id, step_id)
    if not st:
        await callback.answer("Шаг не найден", show_alert=True)
        return
    chat_id = st["chat_id"]
    await db.execute(
        "DELETE FROM welcome_steps WHERE id=$1 AND owner_id=$2",
        step_id, owner_id,
    )
    await callback.answer("🗑 Шаг удалён")
    await _drop_echo(callback.bot, state, callback.message.chat.id)
    await state.clear()
    await _show_manager(callback, chat_id, owner_id)


@router.callback_query(F.data.startswith("wseq_clear:"))
async def on_wseq_clear(callback: CallbackQuery, state: FSMContext, platform_user: dict | None):
    if not platform_user:
        return
    chat_id = int(callback.data.split(":")[1])
    owner_id = await resolve_chat_owner(platform_user["user_id"], chat_id)
    await db.execute(
        "DELETE FROM welcome_steps WHERE owner_id=$1 AND chat_id=$2::bigint",
        owner_id, chat_id,
    )
    await callback.answer("🗑 Все доп. шаги удалены")
    await state.clear()
    await _show_manager(callback, chat_id, owner_id)


@router.callback_query(F.data.startswith("wseq_test:"))
async def on_wseq_test(callback: CallbackQuery, platform_user: dict | None):
    """Предпросмотр: присылает всю цепочку владельцу для проверки (без задержек)."""
    if not platform_user:
        return
    chat_id = int(callback.data.split(":")[1])
    owner_id = await resolve_chat_owner(platform_user["user_id"], chat_id)
    if not await _own_chat(owner_id, chat_id):
        await callback.answer("Площадка не найдена", show_alert=True)
        return

    row = await db.fetchrow(
        "SELECT welcome_text, welcome_media, welcome_media_type, welcome_buttons, chat_title "
        "FROM bot_chats WHERE owner_id=$1 AND chat_id=$2::bigint",
        owner_id, chat_id,
    )
    steps = await db.fetch(
        "SELECT * FROM welcome_steps WHERE owner_id=$1 AND chat_id=$2::bigint "
        "ORDER BY step_order ASC, delay_sec ASC, id ASC",
        owner_id, chat_id,
    )
    has_base = bool(row and (row["welcome_text"] or row["welcome_media"]))
    if not has_base:
        await callback.answer("Сначала задайте приветствие №1 — иначе цепочка не отправится.", show_alert=True)
        return

    await callback.answer("👁 Прислал предпросмотр ниже")
    bot = callback.bot
    tg = callback.message.chat.id
    user = callback.from_user
    chat_title = (row["chat_title"] if row else "") or ""
    from utils.keyboard import build_inline_keyboard

    await bot.send_message(tg, "👁 <b>Предпросмотр цепочки</b>\nТак она придёт пользователю (без задержек):", parse_mode="HTML")

    # №1 базовое приветствие
    try:
        base_txt = _apply_vars(row["welcome_text"] or "", user, chat_title)
        base_kb = build_inline_keyboard(row["welcome_buttons"])
        if row["welcome_media"]:
            mt = row["welcome_media_type"]
            fid = row["welcome_media"]
            if mt == "photo":
                await bot.send_photo(tg, fid, caption=base_txt or None, reply_markup=base_kb, parse_mode="HTML")
            elif mt == "video":
                await bot.send_video(tg, fid, caption=base_txt or None, reply_markup=base_kb, parse_mode="HTML")
            elif mt == "animation":
                await bot.send_animation(tg, fid, caption=base_txt or None, reply_markup=base_kb, parse_mode="HTML")
            else:
                await bot.send_document(tg, fid, caption=base_txt or None, reply_markup=base_kb, parse_mode="HTML")
        else:
            await bot.send_message(tg, base_txt or "—", reply_markup=base_kb, parse_mode="HTML")
    except Exception as e:
        logger.debug(f"[WSEQ TEST] base failed: {e}")

    # доп. шаги
    for i, st in enumerate(steps, start=2):
        offset = _delay_offset(st["delay_sec"])
        if (st["action"] or "message") == "delete":
            await bot.send_message(tg, f"🧹 <i>№{i} ({offset}): здесь бот очистил бы всю переписку цепочки.</i>", parse_mode="HTML")
            continue
        try:
            txt = _apply_vars(st["text"] or "", user, chat_title)
            kb = build_inline_keyboard(st["buttons"])
            head = f"<i>№{i} · {offset}</i>\n"
            if st["media"]:
                mt = st["media_type"]; fid = st["media"]
                cap = (head + (txt or "")).strip()
                if mt == "photo":
                    await bot.send_photo(tg, fid, caption=cap or None, reply_markup=kb, parse_mode="HTML")
                elif mt == "video":
                    await bot.send_video(tg, fid, caption=cap or None, reply_markup=kb, parse_mode="HTML")
                elif mt == "animation":
                    await bot.send_animation(tg, fid, caption=cap or None, reply_markup=kb, parse_mode="HTML")
                else:
                    await bot.send_document(tg, fid, caption=cap or None, reply_markup=kb, parse_mode="HTML")
            else:
                await bot.send_message(tg, head + (txt or "—"), reply_markup=kb, parse_mode="HTML")
        except Exception as e:
            logger.debug(f"[WSEQ TEST] step {i} failed: {e}")
