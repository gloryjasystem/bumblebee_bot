"""
handlers/welcome_seq.py — Цепочка приветствий (доп. сообщения с интервалами).

Базовое приветствие (welcome_*) — это шаг №1 (сразу). Здесь настраиваются
ДОПОЛНИТЕЛЬНЫЕ шаги, каждый со своей задержкой от вступления:
  • шаг-«сообщение» — текст/медиа/кнопки + опц. авто-удаление;
  • шаг-«удаление»  — удаляет ранее отправленные ботом сообщения («Удаляю ссылку»).

Пример из ТЗ:
  №1 сразу        — «подпишись»          (базовое приветствие)
  №2 через 15 сек — «подписался?»        (шаг-сообщение, delay 15с)
  №3 через 1 мин  — удаление ссылки       (шаг-удаление, delay 60с)
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
from services.security import sanitize
from utils.nav import navigate
from utils.timing import format_delay, parse_delay_input

logger = logging.getLogger(__name__)
router = Router()

_SELF_DELETE_CYCLE = [0, 5, 15, 30, 60, 120, 300]  # секунды авто-удаления шага
_MAX_STEPS = 10  # максимум доп. шагов в цепочке


class WSeqFSM(StatesGroup):
    adding_content = State()   # ждём текст/медиа нового шага-сообщения
    step_content   = State()   # ждём новый текст/медиа существующего шага
    step_buttons   = State()   # ждём кнопки шага
    step_delay     = State()   # ждём задержку шага


# ── Вспомогательные ──────────────────────────────────────────────

def _self_del_label(sec: int) -> str:
    if not sec:
        return "нет"
    return f"{sec} сек" if sec < 60 else f"{sec // 60} мин"


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

    lines = [
        "⛓ <b>Цепочка сообщений</b>\n",
        "№1 <i>сразу</i> — базовое приветствие (кнопка «👋 Приветствие»).\n",
    ]
    if not has_base:
        lines.append(
            "⚠️ <b>Базовое приветствие не задано.</b> Сначала задайте «👋 Приветствие» "
            "(это шаг №1) — иначе цепочка не отправится.\n"
        )
    kb_rows = []
    for i, st in enumerate(steps, start=2):
        if (st["action"] or "message") == "delete":
            preview = "🗑 удаление сообщений"
        else:
            txt = (st["text"] or "").strip().replace("\n", " ")
            if st["media"] and not txt:
                preview = "🖼 медиа"
            elif st["media"]:
                preview = f"🖼 {txt[:22]}"
            else:
                preview = (txt[:26] or "—")
        kb_rows.append([InlineKeyboardButton(
            text=f"№{i} · {format_delay(st['delay_sec'])} · {preview}",
            callback_data=f"wstep:{st['id']}",
        )])

    if steps:
        lines.append(f"\nДоп. шагов: <b>{len(steps)}</b> (макс. {_MAX_STEPS}).")
    else:
        lines.append("\nДоп. шагов пока нет. Добавьте сообщение или шаг-удаление.")

    can_add = len(steps) < _MAX_STEPS
    add_row = []
    if can_add:
        add_row.append(InlineKeyboardButton(text="➕ Сообщение", callback_data=f"wseq_add:{chat_id}:msg"))
        add_row.append(InlineKeyboardButton(text="🗑 Шаг удаления", callback_data=f"wseq_add:{chat_id}:del"))
    if add_row:
        kb_rows.append(add_row)
    if steps:
        kb_rows.append([InlineKeyboardButton(text="🧹 Очистить цепочку", callback_data=f"wseq_clear:{chat_id}")])
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
    if not await _own_chat(platform_user["user_id"], chat_id):
        await callback.answer("Площадка не найдена", show_alert=True)
        return
    await state.clear()
    await _show_manager(callback, chat_id, platform_user["user_id"])


# ── Добавление шага ──────────────────────────────────────────────

@router.callback_query(F.data.startswith("wseq_add:"))
async def on_wseq_add(callback: CallbackQuery, state: FSMContext, platform_user: dict | None):
    if not platform_user:
        return
    _, chat_id_s, kind = callback.data.split(":")
    chat_id = int(chat_id_s)
    owner_id = platform_user["user_id"]
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
        # Шаг-удаление создаём сразу, дефолтная задержка 60с
        step_id = await db.fetchval(
            "INSERT INTO welcome_steps (owner_id, chat_id, step_order, delay_sec, action) "
            "VALUES ($1, $2, $3, 60, 'delete') RETURNING id",
            owner_id, chat_id, next_order,
        )
        await callback.answer("🗑 Шаг удаления добавлен")
        await _show_step_editor(callback, step_id, owner_id)
        return

    # Шаг-сообщение — просим прислать контент
    await state.set_state(WSeqFSM.adding_content)
    await state.update_data(wseq_chat_id=chat_id, wseq_order=next_order)
    await navigate(
        callback,
        "➕ <b>Новое сообщение цепочки</b>\n\n"
        "Пришлите текст (и, при желании, медиа) сообщения.\n\n"
        "<b>Переменные:</b> <code>{name}</code>, <code>{allname}</code>, "
        "<code>{username}</code>, <code>{chat}</code>, <code>{day}</code>.",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="◀️ Отмена", callback_data=f"wseq:{chat_id}")],
        ]),
    )


@router.message(WSeqFSM.adding_content)
async def on_adding_content(message: Message, state: FSMContext, platform_user: dict | None):
    if not platform_user:
        return
    data = await state.get_data()
    chat_id = data.get("wseq_chat_id")
    order = data.get("wseq_order", 2)
    owner_id = platform_user["user_id"]
    if chat_id is None:
        await state.clear()
        return

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
    await state.clear()
    await message.answer("✅ Шаг добавлен (задержка по умолчанию — 15 сек, можно изменить).")
    await _show_step_editor(message, step_id, owner_id)


# ── Экран: редактор шага ─────────────────────────────────────────

async def _show_step_editor(event, step_id: int, owner_id: int):
    st = await _get_step(owner_id, step_id)
    if not st:
        if isinstance(event, CallbackQuery):
            await event.answer("Шаг не найден", show_alert=True)
        return
    chat_id = st["chat_id"]
    action = st["action"] or "message"
    is_del = action == "delete"

    if is_del:
        body = (
            "🗑 <b>Шаг: удаление сообщений</b>\n\n"
            f"⏱ Задержка: <b>{format_delay(st['delay_sec'])}</b>\n\n"
            "Через указанное время после вступления бот удалит все сообщения,\n"
            "которые он отправил пользователю в рамках этой цепочки (включая базовое)."
        )
        kb_rows = [
            [InlineKeyboardButton(text=f"⏱ Задержка: {format_delay(st['delay_sec'])}", callback_data=f"wstep_delay:{step_id}")],
            [InlineKeyboardButton(text="🗑 Удалить шаг", callback_data=f"wstep_rm:{step_id}")],
            [InlineKeyboardButton(text="◀️ Назад", callback_data=f"wseq:{chat_id}")],
        ]
    else:
        txt = (st["text"] or "").strip()
        preview = txt[:120] if txt else ("🖼 медиа" if st["media"] else "—")
        media_note = "🖼 медиа прикреплено" if st["media"] else "без медиа"
        body = (
            "✉️ <b>Шаг: сообщение</b>\n\n"
            f"⏱ Задержка: <b>{format_delay(st['delay_sec'])}</b>\n"
            f"⏳ Авто-удаление: <b>{_self_del_label(st['self_delete_sec'] or 0)}</b>\n"
            f"🎬 {media_note}\n\n"
            f"<blockquote>{preview}</blockquote>"
        )
        kb_rows = [
            [InlineKeyboardButton(text=f"⏱ Задержка: {format_delay(st['delay_sec'])}", callback_data=f"wstep_delay:{step_id}")],
            [InlineKeyboardButton(text="✏️ Текст / медиа", callback_data=f"wstep_edit:{step_id}")],
            [InlineKeyboardButton(text="🎛 Кнопки", callback_data=f"wstep_btns:{step_id}")],
            [InlineKeyboardButton(text=f"⏳ Авто-удаление: {_self_del_label(st['self_delete_sec'] or 0)}", callback_data=f"wstep_selfdel:{step_id}")],
            [InlineKeyboardButton(text="🗑 Удалить шаг", callback_data=f"wstep_rm:{step_id}")],
            [InlineKeyboardButton(text="◀️ Назад", callback_data=f"wseq:{chat_id}")],
        ]

    kb = InlineKeyboardMarkup(inline_keyboard=kb_rows)
    if isinstance(event, CallbackQuery):
        await navigate(event, body, reply_markup=kb)
    else:
        await event.answer(body, reply_markup=kb, parse_mode="HTML")


@router.callback_query(F.data.startswith("wstep:"))
async def on_wstep(callback: CallbackQuery, state: FSMContext, platform_user: dict | None):
    if not platform_user:
        return
    step_id = int(callback.data.split(":")[1])
    await state.clear()
    await _show_step_editor(callback, step_id, platform_user["user_id"])


@router.callback_query(F.data.startswith("wstep_delay:"))
async def on_wstep_delay(callback: CallbackQuery, state: FSMContext, platform_user: dict | None):
    if not platform_user:
        return
    step_id = int(callback.data.split(":")[1])
    st = await _get_step(platform_user["user_id"], step_id)
    if not st:
        await callback.answer("Шаг не найден", show_alert=True)
        return
    await state.set_state(WSeqFSM.step_delay)
    await state.update_data(wstep_id=step_id)
    await navigate(
        callback,
        "⏱ <b>Задержка шага</b>\n\n"
        "Через сколько времени после вступления показать этот шаг?\n"
        "По умолчанию — <b>секунды</b>. Примеры: <code>15</code>, <code>90с</code>, <code>5м</code>, <code>1ч</code>.",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="◀️ Отмена", callback_data=f"wstep:{step_id}")],
        ]),
    )


@router.message(WSeqFSM.step_delay)
async def on_wstep_delay_input(message: Message, state: FSMContext, platform_user: dict | None):
    if not platform_user:
        return
    data = await state.get_data()
    step_id = data.get("wstep_id")
    if not step_id:
        await state.clear()
        return
    sec = parse_delay_input(message.text or "")
    if sec is None:
        await message.answer("Не понял значение. Пришлите число, напр. <code>15</code>, <code>90с</code>, <code>5м</code>.", parse_mode="HTML")
        return
    await db.execute(
        "UPDATE welcome_steps SET delay_sec=$1 WHERE id=$2 AND owner_id=$3",
        sec, step_id, platform_user["user_id"],
    )
    await state.clear()
    await message.answer(f"✅ Задержка шага: <b>{format_delay(sec)}</b>", parse_mode="HTML")
    await _show_step_editor(message, step_id, platform_user["user_id"])


@router.callback_query(F.data.startswith("wstep_edit:"))
async def on_wstep_edit(callback: CallbackQuery, state: FSMContext, platform_user: dict | None):
    if not platform_user:
        return
    step_id = int(callback.data.split(":")[1])
    st = await _get_step(platform_user["user_id"], step_id)
    if not st:
        await callback.answer("Шаг не найден", show_alert=True)
        return
    await state.set_state(WSeqFSM.step_content)
    await state.update_data(wstep_id=step_id)
    await navigate(
        callback,
        "✏️ <b>Текст / медиа шага</b>\n\n"
        "Пришлите новый текст (и, при желании, медиа).\n"
        "Переменные: <code>{name}</code>, <code>{allname}</code>, <code>{username}</code>, <code>{chat}</code>, <code>{day}</code>.",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="◀️ Отмена", callback_data=f"wstep:{step_id}")],
        ]),
    )


@router.message(WSeqFSM.step_content)
async def on_wstep_content_input(message: Message, state: FSMContext, platform_user: dict | None):
    if not platform_user:
        return
    data = await state.get_data()
    step_id = data.get("wstep_id")
    if not step_id:
        await state.clear()
        return
    text = sanitize(message.text or message.caption or "", max_len=1024)
    media_fid, media_type = _extract_media(message)
    if not text and not media_fid:
        await message.answer("Пришлите текст или медиа.")
        return
    if media_fid:
        await db.execute(
            "UPDATE welcome_steps SET text=$1, media=$2, media_type=$3 WHERE id=$4 AND owner_id=$5",
            text or None, media_fid, media_type, step_id, platform_user["user_id"],
        )
    else:
        # Только текст — сохраняем текст, медиа не трогаем
        await db.execute(
            "UPDATE welcome_steps SET text=$1 WHERE id=$2 AND owner_id=$3",
            text or None, step_id, platform_user["user_id"],
        )
    await state.clear()
    await message.answer("✅ Обновлено.")
    await _show_step_editor(message, step_id, platform_user["user_id"])


@router.callback_query(F.data.startswith("wstep_btns:"))
async def on_wstep_btns(callback: CallbackQuery, state: FSMContext, platform_user: dict | None):
    if not platform_user:
        return
    step_id = int(callback.data.split(":")[1])
    st = await _get_step(platform_user["user_id"], step_id)
    if not st:
        await callback.answer("Шаг не найден", show_alert=True)
        return
    await state.set_state(WSeqFSM.step_buttons)
    await state.update_data(wstep_id=step_id)
    await navigate(
        callback,
        "🎛 <b>Кнопки шага</b>\n\n"
        "Пришлите кнопки в формате:\n"
        "<blockquote><code>Текст — https://ссылка</code></blockquote>\n"
        "Несколько в ряд — через <code>|</code>. Отправьте <code>-</code>, чтобы убрать кнопки.",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="◀️ Отмена", callback_data=f"wstep:{step_id}")],
        ]),
    )


@router.message(WSeqFSM.step_buttons)
async def on_wstep_btns_input(message: Message, state: FSMContext, platform_user: dict | None):
    if not platform_user:
        return
    data = await state.get_data()
    step_id = data.get("wstep_id")
    if not step_id:
        await state.clear()
        return
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
        _json.dumps(buttons, ensure_ascii=False), step_id, platform_user["user_id"],
    )
    await state.clear()
    await message.answer("✅ Кнопки обновлены." if buttons else "✅ Кнопки убраны.")
    await _show_step_editor(message, step_id, platform_user["user_id"])


@router.callback_query(F.data.startswith("wstep_selfdel:"))
async def on_wstep_selfdel(callback: CallbackQuery, platform_user: dict | None):
    if not platform_user:
        return
    step_id = int(callback.data.split(":")[1])
    st = await _get_step(platform_user["user_id"], step_id)
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
        new_val, step_id, platform_user["user_id"],
    )
    await callback.answer(f"⏳ Авто-удаление: {_self_del_label(new_val)}")
    await _show_step_editor(callback, step_id, platform_user["user_id"])


@router.callback_query(F.data.startswith("wstep_rm:"))
async def on_wstep_rm(callback: CallbackQuery, platform_user: dict | None):
    if not platform_user:
        return
    step_id = int(callback.data.split(":")[1])
    st = await _get_step(platform_user["user_id"], step_id)
    if not st:
        await callback.answer("Шаг не найден", show_alert=True)
        return
    chat_id = st["chat_id"]
    await db.execute(
        "DELETE FROM welcome_steps WHERE id=$1 AND owner_id=$2",
        step_id, platform_user["user_id"],
    )
    await callback.answer("🗑 Шаг удалён")
    await _show_manager(callback, chat_id, platform_user["user_id"])


@router.callback_query(F.data.startswith("wseq_clear:"))
async def on_wseq_clear(callback: CallbackQuery, platform_user: dict | None):
    if not platform_user:
        return
    chat_id = int(callback.data.split(":")[1])
    await db.execute(
        "DELETE FROM welcome_steps WHERE owner_id=$1 AND chat_id=$2::bigint",
        platform_user["user_id"], chat_id,
    )
    await callback.answer("🧹 Цепочка очищена")
    await _show_manager(callback, chat_id, platform_user["user_id"])
