"""
handlers/welcome_seq.py — Цепочка сообщений (приветствие №1 + доп. шаги с интервалами).

Модель (Подход 1): базовое приветствие (welcome_*) — это шаг №1 (сразу), он же
редактируется прямо из менеджера цепочки кнопкой «✎ №1 Приветствие» (тот же
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
import asyncio
import json as _json
import logging
import re

from aiogram import Router, F
from aiogram.types import (
    Message, CallbackQuery, InlineKeyboardMarkup, InlineKeyboardButton,
)
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup

import db.pool as db
from db.channels import resolve_chat_owner
from services.security import sanitize
from utils.nav import navigate, set_active_msg, get_active_msg
from utils.timing import format_delay_short, parse_delay_input

logger = logging.getLogger(__name__)
router = Router()

_MAX_STEPS = 10  # максимум доп. шагов в цепочке
# Быстрые пресеты задержки шага (сек, подпись)
_DELAY_CHIPS = [(0, "сразу"), (15, "15с"), (30, "30с"), (60, "1 мин"), (300, "5 мин"), (900, "15 мин")]
# Быстрые пресеты авто-удаления шага (сек, подпись); своё значение — без потолка
_SELFDEL_CHIPS = [(0, "нет"), (15, "15с"), (60, "1 мин"), (300, "5 мин"), (900, "15 мин"), (1800, "30 мин")]
# Пресеты авто-очистки переписки (свойство цепочки; 0 = выкл)
_AUTOCLEAR_CHIPS = [(0, "выкл"), (60, "1 мин"), (300, "5 мин"), (900, "15 мин"), (1800, "30 мин")]


class WSeqFSM(StatesGroup):
    adding_content = State()   # ждём текст/медиа нового шага-сообщения
    step_content   = State()   # ждём новый текст+медиа существующего шага (комбо, как в приветствии)
    step_buttons   = State()   # ждём кнопки шага
    step_delay     = State()   # ждём свою задержку шага
    step_selfdel   = State()   # ждём своё время авто-удаления шага
    autoclear_delay = State()  # ждём своё время авто-очистки переписки


# ── Вспомогательные ──────────────────────────────────────────────

def _self_del_label(sec) -> str:
    sec = int(sec or 0)
    return "нет" if sec <= 0 else format_delay_short(sec)


def _delay_offset(sec) -> str:
    """Компактная подпись задержки шага: «сразу» / «+15с» / «+1м»."""
    sec = int(sec or 0)
    return "сразу" if sec <= 0 else f"+{format_delay_short(sec)}"


_URL_RE = re.compile(r"https?://([^/\s]+)")


def _domain(text: str) -> str:
    """Домен из ссылки: https://kick.com/x → kick.com (без www)."""
    m = _URL_RE.search(text or "")
    if not m:
        return (text or "").strip()
    host = m.group(1).lower()
    return host[4:] if host.startswith("www.") else host


def _shorten(text: str, n: int) -> str:
    text = (text or "").strip().replace("\n", " ")
    return text if len(text) <= n else text[: max(1, n - 1)].rstrip() + "…"


def _step_preview(st, n: int = 22):
    """Иконка типа + короткий превью шага (URL → домен, длинный текст → …)."""
    if (st["action"] or "message") == "delete":
        return "🧹", "очистить переписку"
    txt = (st["text"] or "").strip().replace("\n", " ")
    if txt.startswith("http://") or txt.startswith("https://"):
        return "🔗", _shorten(_domain(txt), n)
    if st["media"] and not txt:
        return "🖼", "медиа"
    if st["media"]:
        return "🖼", _shorten(txt, n)
    return "✉️", (_shorten(txt, n) or "—")


# Предпросмотр «Проверить цепочку»: message_id-ы сообщений предпросмотра на
# пару (chat_id, user_id) — чтобы «🧹 Убрать» и авто-уборка их снесли.
_preview_msgs: dict[tuple[int, int], list[int]] = {}


async def _auto_clear_preview(bot, tg_chat_id: int, key: tuple[int, int], delay: int):
    await asyncio.sleep(delay)
    mids = _preview_msgs.pop(key, None)
    if not mids:
        return
    for m in mids:
        try:
            await bot.delete_message(tg_chat_id, m)
        except Exception:
            pass


async def _preview_one(bot, tg_chat_id: int, label: str, txt: str, media_fid, media_type, kb):
    """Одно сообщение предпросмотра с меткой сверху.

    Устойчиво: сначала пробуем HTML, при сбое разметки — без неё (сообщение
    ВСЕГДА видно, а не молча пропадает). Ошибки логируем на warning.
    """
    txt = txt or ""
    for pm, content in (("HTML", f"<i>{label}</i>\n{txt}".strip()), (None, f"{label}\n{txt}".strip())):
        try:
            if media_fid:
                cap = content or None
                if media_type == "photo":
                    return await bot.send_photo(tg_chat_id, media_fid, caption=cap, parse_mode=pm, reply_markup=kb)
                if media_type == "video":
                    return await bot.send_video(tg_chat_id, media_fid, caption=cap, parse_mode=pm, reply_markup=kb)
                if media_type == "animation":
                    return await bot.send_animation(tg_chat_id, media_fid, caption=cap, parse_mode=pm, reply_markup=kb)
                return await bot.send_document(tg_chat_id, media_fid, caption=cap, parse_mode=pm, reply_markup=kb)
            return await bot.send_message(tg_chat_id, content or "—", parse_mode=pm, reply_markup=kb)
        except Exception as e:
            logger.warning(f"[WSEQ TEST] preview send failed (pm={pm}, label={label}): {e}")
    return None


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
        "SELECT welcome_text, welcome_media, welcome_enabled, welcome_delay_sec FROM bot_chats WHERE owner_id=$1 AND chat_id=$2::bigint",
        owner_id, chat_id,
    )
    has_base = bool(base and (base["welcome_text"] or base["welcome_media"]))
    # база «в игре», только если задана И тумблер не выключен (NULL/None = включено)
    base_on = has_base and (base["welcome_enabled"] is not False)
    base_delay = int(base["welcome_delay_sec"] or 0) if base else 0

    # Шаги-сообщения нумеруются; авто-очистка (action='delete') — свойство цепочки
    msg_steps = [s for s in steps if (s["action"] or "message") != "delete"]
    del_steps = [s for s in steps if (s["action"] or "message") == "delete"]
    autoclear_sec = min((int(s["delay_sec"] or 0) for s in del_steps), default=0)

    # ── Тело: компактный таймлайн (одна строка на шаг) ──
    lines = ["⛓ <b>Цепочка сообщений</b>", ""]
    if not has_base:
        lines.append("<b>№1</b> · <i>сразу</i> · ⚠️ не задано")
    elif base_on:
        lines.append(f"<b>№1</b> · <i>{_delay_offset(base_delay)}</i> · 👋 приветствие")
    else:
        lines.append("<b>№1</b> · <s>👋 приветствие</s> · выключено")

    max_delay = 0
    for i, st in enumerate(msg_steps, start=2):
        delay = int(st["delay_sec"] or 0)
        max_delay = max(max_delay, delay)
        icon, prev = _step_preview(st, 30)
        lines.append(f"<b>№{i}</b> · <i>{_delay_offset(delay)}</i> · {icon} {prev}")

    if autoclear_sec > 0:
        lines += ["", f"🧹 <b>через +{format_delay_short(autoclear_sec)}</b> — вся переписка очистится"]

    # Что реально уйдёт: база (если включена) + сообщения-шаги
    sending_total = (1 if base_on else 0) + len(msg_steps)
    if sending_total == 0:
        reason = "приветствие не задано, других сообщений нет" if not has_base \
                 else "приветствие выключено, других сообщений нет"
        lines += ["", f"⚠️ <b>Ничего не отправится</b> — {reason}."]
    else:
        # Первым уйдёт сообщение с наименьшей задержкой (№1 тоже может быть отложено)
        candidates = ([("№1", base_delay)] if base_on else [])
        candidates += [(f"№{i}", int(st["delay_sec"] or 0)) for i, st in enumerate(msg_steps, start=2)]
        _fl, _fd = min(candidates, key=lambda c: c[1])
        first = f"{_fl} · {_delay_offset(_fd)}"
        summary = f"Первым уйдёт {first} · всего {sending_total}"
        if autoclear_sec > 0:
            summary += f" · очистка +{format_delay_short(autoclear_sec)}"
        lines += ["", f"<i>{summary}.</i>"]
        if not msg_steps:
            lines += ["", "Пока только приветствие (№1). Добавьте сообщения ниже — до 10."]

    # ── Клавиатура ──
    kb_rows = [[InlineKeyboardButton(
        text=("✎ №1 Приветствие" if has_base else "✎ Задать приветствие №1"),
        callback_data=f"welcome_set_chain:{chat_id}",
    )]]
    for i, st in enumerate(msg_steps, start=2):
        delay = int(st["delay_sec"] or 0)
        icon, _prev = _step_preview(st, 18)
        kb_rows.append([InlineKeyboardButton(
            text=f"✎ №{i} · {_delay_offset(delay)} · {icon}",
            callback_data=f"wstep:{st['id']}",
        )])

    # ряд: «Сообщение» слева + короткая «Очистка» справа
    ac_label = f"🧹 Очистка: +{format_delay_short(autoclear_sec)}" if autoclear_sec > 0 else "🧹 Очистка: выкл"
    add_row = []
    if len(msg_steps) < _MAX_STEPS:
        add_row.append(InlineKeyboardButton(text="➕ Сообщение", callback_data=f"wseq_add:{chat_id}:msg"))
    add_row.append(InlineKeyboardButton(text=ac_label, callback_data=f"wseq_autoclear:{chat_id}"))
    kb_rows.append(add_row)

    if msg_steps or autoclear_sec > 0:
        kb_rows.append([InlineKeyboardButton(text="🗑 Удалить всё", callback_data=f"wseq_clear:{chat_id}")])
    if has_base or msg_steps:
        kb_rows.append([InlineKeyboardButton(text="👁 Проверить цепочку", callback_data=f"wseq_test:{chat_id}")])
    kb_rows.append([InlineKeyboardButton(text="◄ Назад", callback_data=f"ch_messages:{chat_id}")])

    kb = InlineKeyboardMarkup(inline_keyboard=kb_rows)
    text = "\n".join(lines)
    if isinstance(event, CallbackQuery):
        await navigate(event, text, reply_markup=kb, disable_web_page_preview=True)
    else:
        await event.answer(text, reply_markup=kb, parse_mode="HTML", disable_web_page_preview=True)


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
        "SELECT COUNT(*) FROM welcome_steps WHERE owner_id=$1 AND chat_id=$2::bigint "
        "AND COALESCE(action,'message') <> 'delete'",
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
            [InlineKeyboardButton(text="◄ Отмена", callback_data=f"wseq:{chat_id}")],
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
    # Порядковый номер шага (№2, №3…) — для ориентира в заголовке
    order_rows = await db.fetch(
        "SELECT id FROM welcome_steps WHERE owner_id=$1 AND chat_id=$2::bigint "
        "AND COALESCE(action,'message') <> 'delete' "
        "ORDER BY step_order ASC, delay_sec ASC, id ASC",
        owner_id, chat_id,
    )
    num = next((i for i, r in enumerate(order_rows, start=2) if r["id"] == step_id), None)
    num_label = f" №{num}" if num else ""
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
        # Эхо честно отражает настройки (как у приветствия): позиция подписи и превью ссылок
        media_below = bool(st["media_below"])
        no_preview = not bool(st["preview"])
        try:
            if media_fid:
                if media_type == "photo":
                    sent = await bot.send_photo(tg_chat_id, media_fid, caption=txt or None, reply_markup=step_kb, parse_mode="HTML", show_caption_above_media=media_below)
                elif media_type == "video":
                    sent = await bot.send_video(tg_chat_id, media_fid, caption=txt or None, reply_markup=step_kb, parse_mode="HTML", show_caption_above_media=media_below)
                elif media_type == "animation":
                    sent = await bot.send_animation(tg_chat_id, media_fid, caption=txt or None, reply_markup=step_kb, parse_mode="HTML", show_caption_above_media=media_below)
                else:
                    sent = await bot.send_document(tg_chat_id, media_fid, caption=txt or None, reply_markup=step_kb, parse_mode="HTML")
            else:
                sent = await bot.send_message(tg_chat_id, txt or "—", reply_markup=step_kb, parse_mode="HTML", disable_web_page_preview=no_preview)
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
            f"🧹 <b>Шаг{num_label} · очистить переписку</b>\n\n"
            f"⏱ Задержка: <b>{_delay_offset(st['delay_sec'])}</b> от заявки\n\n"
            "Через это время бот удалит <b>всю переписку</b> цепочки "
            "(базовое приветствие и все шаги)."
        )
        kb_rows = [
            [InlineKeyboardButton(text=f"⏱ Задержка: {_delay_offset(st['delay_sec'])}", callback_data=f"wstep_delay:{step_id}")],
            [InlineKeyboardButton(text="🗑 Удалить шаг", callback_data=f"wstep_rm:{step_id}")],
            [InlineKeyboardButton(text="◄ Назад", callback_data=f"wseq:{chat_id}")],
        ]
    else:
        # 🎬 Медиа — как в приветствии: показывает позицию ⬆️/⬇️ (само медиа задаётся в «Редактировать»)
        if st["media"]:
            media_label = f"🎬 Медиа: {'⬇️' if st['media_below'] else '⬆️'}"
        else:
            media_label = "🎬 Медиа: нет"
        preview_label = f"👁 Превью: {'да' if st['preview'] else 'нет'}"
        body = f"⚙️ <b>Шаг{num_label}</b>"
        kb_rows = [
            [InlineKeyboardButton(text="✎ Редактировать", callback_data=f"wstep_edit:{step_id}")],
            [InlineKeyboardButton(text="🎛 Кнопки", callback_data=f"wstep_btns:{step_id}")],
            [InlineKeyboardButton(text=media_label, callback_data=f"wstep_media:{step_id}")],
            [InlineKeyboardButton(text=preview_label, callback_data=f"wstep_preview:{step_id}")],
            [InlineKeyboardButton(text=f"⏱ Задержка: {_delay_offset(st['delay_sec'])}", callback_data=f"wstep_delay:{step_id}")],
            [InlineKeyboardButton(text=f"⏳ Авто-удаление: {_self_del_label(st['self_delete_sec'] or 0)}", callback_data=f"wstep_selfdel:{step_id}")],
            [InlineKeyboardButton(text="🗑 Удалить шаг", callback_data=f"wstep_rm:{step_id}")],
            [InlineKeyboardButton(text="◄ Назад", callback_data=f"wseq:{chat_id}")],
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
    chip_rows.append([InlineKeyboardButton(text="✎ Своё значение", callback_data=f"wstep_delaycustom:{step_id}")])
    chip_rows.append([InlineKeyboardButton(text="◄ Назад", callback_data=f"wstep:{step_id}")])

    await navigate(
        callback,
        "⏱ <b>Задержка шага</b>\n\n"
        "Через сколько после заявки показать этот шаг.\n"
        f"Сейчас: <b>{_delay_offset(cur)}</b>\n\n"
        "ⓘ Сообщение с выставленной задержкой придёт только тем, кто уже "
        "написал боту — прошёл капчу или отправил /start.",
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
        "✎ <b>Своё значение задержки</b>\n\n"
        "Пришлите число. По умолчанию — <b>секунды</b>.\n"
        "Примеры: <code>15</code>, <code>90с</code>, <code>5м</code>, <code>1ч</code>.",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="◄ Отмена", callback_data=f"wstep_delay:{step_id}")],
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
        "✎ <b>Редактировать шаг</b>\n\n"
        "Пришлите сообщение целиком — текст и, при необходимости, медиа. "
        "Оно полностью заменит предыдущее.\n\n"
        "Переменные: <code>{name}</code>, <code>{allname}</code>, <code>{username}</code>, <code>{chat}</code>, <code>{day}</code>.\n"
        "ⓘ Можно прикрепить фото, видео, GIF или документ.",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="◄ Отмена", callback_data=f"wstep:{step_id}")],
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
    # Комбинированный ввод как в приветствии (_handle_msg_input): текст И медиа из
    # одного сообщения ПОЛНОСТЬЮ заменяют оба поля (нет медиа во вводе → media стирается).
    text = sanitize(message.text or message.caption or "", max_len=1024)
    media_fid, media_type = _extract_media(message)
    await db.execute(
        "UPDATE welcome_steps SET text=$1, media=$2, media_type=$3 WHERE id=$4 AND owner_id=$5",
        text or None, media_fid, media_type, step_id, owner_id,
    )
    await _show_step_editor(message, step_id, owner_id, state)


# ── Медиа шага: позиция ⬆️/⬇️ (как в приветствии) ────────────────
# Само медиа добавляется/меняется через «✎ Редактировать» (комбинированный ввод).
# Эта кнопка, как у приветствия (on_ch_msg_media), только двигает подпись над/под медиа.

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
    if not st["media"]:
        await callback.answer("Медиа не прикреплено", show_alert=True)
        return
    new_below = not bool(st["media_below"])
    await db.execute(
        "UPDATE welcome_steps SET media_below=$1 WHERE id=$2 AND owner_id=$3",
        new_below, step_id, owner_id,
    )
    await callback.answer(f"🎬 Медиа: {'⬇️' if new_below else '⬆️'}")
    await _show_step_editor(callback, step_id, owner_id, state)


# ── Превью ссылок шага (как в приветствии) ───────────────────────

@router.callback_query(F.data.startswith("wstep_preview:"))
async def on_wstep_preview(callback: CallbackQuery, state: FSMContext, platform_user: dict | None):
    if not platform_user:
        return
    step_id = int(callback.data.split(":")[1])
    owner_id = await _step_owner(platform_user["user_id"], step_id)
    st = await _get_step(owner_id, step_id)
    if not st:
        await callback.answer("Шаг не найден", show_alert=True)
        return
    new_val = not bool(st["preview"])
    await db.execute(
        "UPDATE welcome_steps SET preview=$1 WHERE id=$2 AND owner_id=$3",
        new_val, step_id, owner_id,
    )
    await callback.answer(f"👁 Превью: {'да' if new_val else 'нет'}")
    await _show_step_editor(callback, step_id, owner_id, state)


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
        "📎 Отправьте <b>кнопки</b>, которые будут добавлены к сообщению.\n\n"
        "🔗 <u><b>URL-кнопки</b></u>\n\n"
        "<b>Одна кнопка в ряду:</b>\n"
        "<blockquote><code>Кнопка 1 — ссылка</code>\n"
        "<code>Кнопка 2 — ссылка</code></blockquote>\n\n"
        "<b>Несколько кнопок в ряду:</b>\n"
        "<blockquote><code>Кнопка 1 — ссылка | Кнопка 2 — ссылка</code></blockquote>\n\n"
        "🎨 <u><b>Цветные кнопки</b></u> (добавь emoji перед названием):\n"
        "<blockquote><code>🟦 Кнопка — ссылка</code> — синяя\n"
        "<code>🟩 Кнопка — ссылка</code> — зелёная\n"
        "<code>🟥 Кнопка — ссылка</code> — красная</blockquote>\n\n"
        "<b>WebApp кнопки:</b>\n"
        "<blockquote><code>Кнопка 1 — ссылка (webapp)</code></blockquote>\n\n"
        "ℹ️ Нажмите, чтобы скопировать. Отправьте <code>-</code>, чтобы убрать кнопки.",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="◄ Отмена", callback_data=f"wstep:{step_id}")],
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
    prompt_mid = data.get("wstep_prompt_mid")
    raw = sanitize(message.text or "", max_len=2048)
    if raw == "-":
        buttons = []
    else:
        buttons = _parse_buttons(raw)
        if not buttons:
            # Ошибка — как в приветствии: удаляем ввод, переписываем промпт in-place с «Отмена».
            try:
                await message.delete()
            except Exception:
                pass
            err_text = (
                "⚠️ <b>Не удалось распознать кнопки.</b>\n\n"
                "Формат: <code>Текст — https://ссылка</code>\n"
                "Несколько в ряд — через <code>|</code>\n\n"
                "✎ Введите кнопки в поле ниже ещё раз."
            )
            err_kb = InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="◄ Отмена", callback_data=f"wstep:{step_id}")]])
            if prompt_mid:
                try:
                    await message.bot.edit_message_text(
                        chat_id=message.chat.id, message_id=prompt_mid,
                        text=err_text, reply_markup=err_kb, parse_mode="HTML",
                    )
                    return
                except Exception as e:
                    if "not modified" in str(e).lower():
                        return
                    # промпт недоступен — пошлём новое предупреждение и запомним его
            m = await message.answer(err_text, reply_markup=err_kb, parse_mode="HTML")
            await state.update_data(wstep_prompt_mid=m.message_id)
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
    await _drop_echo(callback.bot, state, callback.message.chat.id)
    await state.set_state(None)

    cur = int(st["self_delete_sec"] or 0)
    chip_rows, row = [], []
    for sec, label in _SELFDEL_CHIPS:
        mark = " ✅" if sec == cur else ""
        row.append(InlineKeyboardButton(text=f"{label}{mark}", callback_data=f"wstep_selfdelset:{step_id}:{sec}"))
        if len(row) == 3:
            chip_rows.append(row); row = []
    if row:
        chip_rows.append(row)
    chip_rows.append([InlineKeyboardButton(text="✎ Своё значение", callback_data=f"wstep_selfdelcustom:{step_id}")])
    chip_rows.append([InlineKeyboardButton(text="◄ Назад", callback_data=f"wstep:{step_id}")])

    await navigate(
        callback,
        "⏳ <b>Авто-удаление сообщения</b>\n\n"
        "Через сколько удалить это сообщение после отправки.\n"
        f"Сейчас: <b>{_self_del_label(cur)}</b>",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=chip_rows),
    )


@router.callback_query(F.data.startswith("wstep_selfdelset:"))
async def on_wstep_selfdelset(callback: CallbackQuery, state: FSMContext, platform_user: dict | None):
    if not platform_user:
        return
    _, step_id_s, sec_s = callback.data.split(":")
    step_id = int(step_id_s)
    owner_id = await _step_owner(platform_user["user_id"], step_id)
    sec = max(0, int(sec_s))
    await db.execute(
        "UPDATE welcome_steps SET self_delete_sec=$1 WHERE id=$2 AND owner_id=$3",
        sec, step_id, owner_id,
    )
    await callback.answer(f"⏳ Авто-удаление: {_self_del_label(sec)}")
    await _show_step_editor(callback, step_id, owner_id, state)


@router.callback_query(F.data.startswith("wstep_selfdelcustom:"))
async def on_wstep_selfdelcustom(callback: CallbackQuery, state: FSMContext, platform_user: dict | None):
    if not platform_user:
        return
    step_id = int(callback.data.split(":")[1])
    owner_id = await _step_owner(platform_user["user_id"], step_id)
    st = await _get_step(owner_id, step_id)
    if not st:
        await callback.answer("Шаг не найден", show_alert=True)
        return
    await state.set_state(WSeqFSM.step_selfdel)
    await state.update_data(wstep_id=step_id)
    prompt = await navigate(
        callback,
        "✎ <b>Своё время авто-удаления</b>\n\n"
        "Пришлите число. По умолчанию — <b>секунды</b>.\n"
        "Примеры: <code>45</code>, <code>10м</code>, <code>1ч</code>. "
        "<code>0</code> или <code>нет</code> — выключить.",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="◄ Отмена", callback_data=f"wstep_selfdel:{step_id}")],
        ]),
    )
    if prompt:
        await state.update_data(wstep_prompt_mid=prompt.message_id)


@router.message(WSeqFSM.step_selfdel)
async def on_wstep_selfdel_input(message: Message, state: FSMContext, platform_user: dict | None):
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
        await message.answer("Не понял значение. Пришлите число, напр. <code>45</code>, <code>10м</code>, <code>1ч</code>, или <code>0</code>.", parse_mode="HTML")
        return
    await db.execute(
        "UPDATE welcome_steps SET self_delete_sec=$1 WHERE id=$2 AND owner_id=$3",
        sec, step_id, owner_id,
    )
    await _show_step_editor(message, step_id, owner_id, state)


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
    """Шаг 1 — подтверждение. Само удаление в on_wseq_clear_do."""
    if not platform_user:
        return
    chat_id = int(callback.data.split(":")[1])
    text = (
        "⚠️ <b>Удалить всю цепочку?</b>\n\n"
        "Все дополнительные сообщения (№2 и далее) и очистка переписки будут удалены "
        "без возможности восстановления.\n\n"
        "Приветствие №1 останется. Его можно убрать только внутри самого приветствия."
    )
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="🗑 Да, удалить всё", callback_data=f"wseq_clear_do:{chat_id}")],
        [InlineKeyboardButton(text="◄ Отмена", callback_data=f"wseq:{chat_id}")],
    ])
    await navigate(callback, text, reply_markup=kb, disable_web_page_preview=True)


@router.callback_query(F.data.startswith("wseq_clear_do:"))
async def on_wseq_clear_do(callback: CallbackQuery, state: FSMContext, platform_user: dict | None):
    """Шаг 2 — реальное удаление цепочки. Приветствие №1 (bot_chats) не трогаем."""
    if not platform_user:
        return
    chat_id = int(callback.data.split(":")[1])
    owner_id = await resolve_chat_owner(platform_user["user_id"], chat_id)
    await db.execute(
        "DELETE FROM welcome_steps WHERE owner_id=$1 AND chat_id=$2::bigint",
        owner_id, chat_id,
    )
    await callback.answer("🗑 Цепочка удалена")
    await state.clear()
    await _show_manager(callback, chat_id, owner_id)


# ── Авто-очистка переписки (свойство цепочки) ────────────────────

def _autoclear_label(sec: int) -> str:
    return "выкл" if sec <= 0 else f"+{format_delay_short(sec)}"


async def _set_autoclear(owner_id: int, chat_id: int, sec: int):
    """Схлопывает все шаги-очистки в один (или удаляет при sec<=0)."""
    await db.execute(
        "DELETE FROM welcome_steps WHERE owner_id=$1 AND chat_id=$2::bigint AND action='delete'",
        owner_id, chat_id,
    )
    if sec > 0:
        next_order = (await db.fetchval(
            "SELECT COALESCE(MAX(step_order), 1) FROM welcome_steps WHERE owner_id=$1 AND chat_id=$2::bigint",
            owner_id, chat_id,
        ) or 1) + 1
        await db.execute(
            "INSERT INTO welcome_steps (owner_id, chat_id, step_order, delay_sec, action) "
            "VALUES ($1, $2, $3, $4, 'delete')",
            owner_id, chat_id, next_order, sec,
        )


@router.callback_query(F.data.startswith("wseq_autoclear:"))
async def on_wseq_autoclear(callback: CallbackQuery, state: FSMContext, platform_user: dict | None):
    if not platform_user:
        return
    chat_id = int(callback.data.split(":")[1])
    owner_id = await resolve_chat_owner(platform_user["user_id"], chat_id)
    if not await _own_chat(owner_id, chat_id):
        await callback.answer("Площадка не найдена", show_alert=True)
        return
    cur = int(await db.fetchval(
        "SELECT MIN(delay_sec) FROM welcome_steps WHERE owner_id=$1 AND chat_id=$2::bigint AND action='delete'",
        owner_id, chat_id,
    ) or 0)
    await state.set_state(None)

    chip_rows, row = [], []
    for sec, label in _AUTOCLEAR_CHIPS:
        mark = " ✅" if sec == cur else ""
        row.append(InlineKeyboardButton(text=f"{label}{mark}", callback_data=f"wseq_autoclearset:{chat_id}:{sec}"))
        if len(row) == 3:
            chip_rows.append(row); row = []
    if row:
        chip_rows.append(row)
    chip_rows.append([InlineKeyboardButton(text="✎ Своё значение", callback_data=f"wseq_autoclearcustom:{chat_id}")])
    chip_rows.append([InlineKeyboardButton(text="◄ Назад", callback_data=f"wseq:{chat_id}")])

    await navigate(
        callback,
        "🧹 <b>Авто-очистка переписки</b>\n\n"
        "Через сколько после заявки удалить всю переписку цепочки (приветствие и все шаги).\n"
        f"Сейчас: <b>{_autoclear_label(cur)}</b>",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=chip_rows),
    )


@router.callback_query(F.data.startswith("wseq_autoclearset:"))
async def on_wseq_autoclearset(callback: CallbackQuery, state: FSMContext, platform_user: dict | None):
    if not platform_user:
        return
    _, chat_id_s, sec_s = callback.data.split(":")
    chat_id = int(chat_id_s)
    owner_id = await resolve_chat_owner(platform_user["user_id"], chat_id)
    if not await _own_chat(owner_id, chat_id):
        await callback.answer("Площадка не найдена", show_alert=True)
        return
    sec = max(0, int(sec_s))
    await _set_autoclear(owner_id, chat_id, sec)
    await callback.answer(f"🧹 Очистка: {_autoclear_label(sec)}")
    await state.clear()
    await _show_manager(callback, chat_id, owner_id)


@router.callback_query(F.data.startswith("wseq_autoclearcustom:"))
async def on_wseq_autoclearcustom(callback: CallbackQuery, state: FSMContext, platform_user: dict | None):
    if not platform_user:
        return
    chat_id = int(callback.data.split(":")[1])
    owner_id = await resolve_chat_owner(platform_user["user_id"], chat_id)
    if not await _own_chat(owner_id, chat_id):
        await callback.answer("Площадка не найдена", show_alert=True)
        return
    await state.set_state(WSeqFSM.autoclear_delay)
    await state.update_data(wseq_ac_chat_id=chat_id)
    await navigate(
        callback,
        "✎ <b>Своё время авто-очистки</b>\n\n"
        "Пришлите число. По умолчанию — <b>секунды</b>.\n"
        "Примеры: <code>90</code>, <code>5м</code>, <code>1ч</code>. <code>0</code> или <code>выкл</code> — выключить.",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="◄ Отмена", callback_data=f"wseq_autoclear:{chat_id}")],
        ]),
    )


@router.message(WSeqFSM.autoclear_delay)
async def on_wseq_autoclear_input(message: Message, state: FSMContext, platform_user: dict | None):
    if not platform_user:
        return
    data = await state.get_data()
    chat_id = data.get("wseq_ac_chat_id")
    if chat_id is None:
        await state.clear()
        return
    owner_id = await resolve_chat_owner(platform_user["user_id"], chat_id)
    sec = parse_delay_input(message.text or "")
    if sec is None:
        await message.answer("Не понял значение. Пришлите число, напр. <code>90</code>, <code>5м</code>, или <code>0</code>.", parse_mode="HTML")
        return
    await _set_autoclear(owner_id, chat_id, sec)
    await state.clear()
    # чистим экран-пикер и ввод пользователя, затем показываем менеджер
    prev = await get_active_msg(message.from_user.id)
    try:
        await message.delete()
    except Exception:
        pass
    if prev:
        try:
            await message.bot.delete_message(message.chat.id, prev)
        except Exception:
            pass
    await _show_manager(message, chat_id, owner_id)


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
        "SELECT welcome_text, welcome_media, welcome_media_type, welcome_buttons, welcome_enabled, chat_title "
        "FROM bot_chats WHERE owner_id=$1 AND chat_id=$2::bigint",
        owner_id, chat_id,
    )
    steps = await db.fetch(
        "SELECT * FROM welcome_steps WHERE owner_id=$1 AND chat_id=$2::bigint "
        "ORDER BY step_order ASC, delay_sec ASC, id ASC",
        owner_id, chat_id,
    )
    has_base = bool(row and (row["welcome_text"] or row["welcome_media"]))
    base_on = has_base and (row["welcome_enabled"] is not False)
    _msg_steps_cnt = sum(1 for s in steps if (s["action"] or "message") != "delete")
    if not base_on and _msg_steps_cnt == 0:
        if not has_base:
            await callback.answer("Сначала задайте приветствие №1 — цепочке нечего отправить.", show_alert=True)
        else:
            await callback.answer("Приветствие выключено, других сообщений нет — отправлять нечего.", show_alert=True)
        return

    await callback.answer("👁 Предпросмотр ниже")
    bot = callback.bot
    tg = callback.message.chat.id
    user = callback.from_user
    chat_title = (row["chat_title"] if row else "") or ""
    from utils.keyboard import build_inline_keyboard

    msg_steps = [s for s in steps if (s["action"] or "message") != "delete"]
    del_steps = [s for s in steps if (s["action"] or "message") == "delete"]
    autoclear_sec = min((int(s["delay_sec"] or 0) for s in del_steps), default=0)

    key = (chat_id, user.id)
    # снести прошлый предпросмотр, если он ещё висит
    prev = _preview_msgs.pop(key, None)
    if prev:
        for m in prev:
            try:
                await bot.delete_message(tg, m)
            except Exception:
                pass
    mids: list[int] = []

    def _track(sent):
        if sent:
            mids.append(sent.message_id)

    # №1 — приветствие (только если включено и задано; иначе сдержанная пометка)
    if base_on:
        base_txt = _apply_vars(row["welcome_text"] or "", user, chat_title)
        base_kb = build_inline_keyboard(row["welcome_buttons"])
        _track(await _preview_one(bot, tg, "№1 · приветствие", base_txt, row["welcome_media"], row["welcome_media_type"], base_kb))
    elif has_base:
        try:
            _track(await bot.send_message(tg, "<s>№1 · приветствие</s> — выключено, не отправится", parse_mode="HTML"))
        except Exception:
            pass

    # №2…№N — сообщения
    for i, st in enumerate(msg_steps, start=2):
        txt = _apply_vars(st["text"] or "", user, chat_title)
        kb = build_inline_keyboard(st["buttons"])
        _track(await _preview_one(bot, tg, f"№{i} · {_delay_offset(st['delay_sec'])}", txt, st["media"], st["media_type"], kb))

    # очистка переписки (свойство цепочки)
    if autoclear_sec > 0:
        try:
            _track(await bot.send_message(tg, f"🧹 <i>через +{format_delay_short(autoclear_sec)} — вся переписка очистится</i>", parse_mode="HTML"))
        except Exception:
            pass

    # один сдержанный контрол внизу; авто-уборка через 60с
    _track(await bot.send_message(
        tg,
        "👁 <i>Предпросмотр цепочки</i>",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="✕ Убрать предпросмотр", callback_data=f"wseq_testclear:{chat_id}")],
        ]),
        parse_mode="HTML",
    ))
    _preview_msgs[key] = mids
    asyncio.create_task(_auto_clear_preview(bot, tg, key, 60))


@router.callback_query(F.data.startswith("wseq_testclear:"))
async def on_wseq_testclear(callback: CallbackQuery, platform_user: dict | None):
    if not platform_user:
        return
    chat_id = int(callback.data.split(":")[1])
    key = (chat_id, callback.from_user.id)
    mids = _preview_msgs.pop(key, None) or []
    for m in mids:
        try:
            await callback.bot.delete_message(callback.message.chat.id, m)
        except Exception:
            pass
    try:
        await callback.answer("🧹 Предпросмотр убран")
    except Exception:
        pass
