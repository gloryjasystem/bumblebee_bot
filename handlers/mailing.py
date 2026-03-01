"""
handlers/mailing.py — UI рассылки: создание, предпросмотр, запуск, управление.
"""
import asyncio
import logging
from datetime import datetime
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


def kb_mailing_main(channels: list) -> InlineKeyboardMarkup:
    buttons = [[
        InlineKeyboardButton(
            text=f"📢 {ch['chat_title'][:28]}",
            callback_data=f"mailing_start:{ch['chat_id']}",
        )
    ] for ch in channels]
    buttons.append([InlineKeyboardButton(text="📅 Запланированные", callback_data="mailing_scheduled")])
    buttons.append([InlineKeyboardButton(text="◀️ Назад", callback_data="menu:main")])
    return InlineKeyboardMarkup(inline_keyboard=buttons)


def kb_mailing_compose() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [
            InlineKeyboardButton(text="👁 Предпросмотр", callback_data="mailing_preview"),
            InlineKeyboardButton(text="🚫 Отмена",       callback_data="menu:mailing"),
        ],
    ])


def kb_mailing_confirm(chat_id: int) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="▶ Запустить сейчас",  callback_data=f"mailing_run:{chat_id}")],
        [InlineKeyboardButton(text="⏱ Запланировать",     callback_data=f"mailing_schedule:{chat_id}")],
        [InlineKeyboardButton(text="✏ Редактировать",     callback_data=f"mailing_edit:{chat_id}")],
        [InlineKeyboardButton(text="🗑 Удалить черновик", callback_data="menu:mailing")],
    ])


def kb_mailing_control(mailing_id: int, paused: bool, speed: str) -> InlineKeyboardMarkup:
    pause_text = "▶ Возобновить" if paused else "⏸ Пауза"
    speed_map = {"low": "🟢 Низкая", "medium": "🟡 Средняя", "high": "🔴 Высокая"}
    next_speed = {"low": "medium", "medium": "high", "high": "low"}
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text=pause_text,            callback_data=f"ml_pause:{mailing_id}")],
        [InlineKeyboardButton(text="⏹ Остановить",       callback_data=f"ml_cancel:{mailing_id}")],
        [InlineKeyboardButton(text=f"⚡ Скорость: {speed_map.get(speed,'🟢 Низкая')}",
                              callback_data=f"ml_speed:{mailing_id}:{next_speed.get(speed,'medium')}")],
    ])


# ── Рассылка для конкретной площадки (из меню площадки) ────────
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
        f"📨 <b>Рассылка</b>\n\n"
        f"Выберите действие ≫",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="➕ Создать рассылку",  callback_data=f"mailing_start:{chat_id}")],
            [InlineKeyboardButton(text="📅 Запланированные",  callback_data=f"mailing_scheduled:{chat_id}")],
            [InlineKeyboardButton(text="◀️ Назад",             callback_data=f"channel_by_chat:{chat_id}")],
        ]),
    )
    await callback.answer()


# ── Запланированные для конкретного канала ──────────────────────
@router.callback_query(F.data.startswith("mailing_scheduled:"))
async def on_mailing_scheduled_channel(callback: CallbackQuery, platform_user: dict | None):
    if not platform_user:
        return
    chat_id = int(callback.data.split(":")[1])
    owner_id = platform_user["user_id"]
    rows = await db.fetch(
        """SELECT id, text, scheduled_at, status FROM mailings
           WHERE owner_id=$1 AND chat_id=$2::bigint AND status IN ('pending','scheduled')
           ORDER BY scheduled_at ASC LIMIT 10""",
        owner_id, chat_id,
    )
    if not rows:
        await callback.answer("Нет запланированных рассылок.", show_alert=True)
        return
    text = "📅 <b>Запланированные рассылки</b>\n\n"
    for r in rows:
        dt = r["scheduled_at"].strftime("%d.%m %H:%M") if r["scheduled_at"] else "Сейчас"
        preview = (r["text"] or "")[:40]
        text += f"• [{dt}] {preview}…\n"
    await callback.message.edit_text(
        text,
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="◀️ Назад", callback_data=f"ch_mailing:{chat_id}")],
        ]),
    )
    await callback.answer()


# ── Главный экран рассылки ─────────────────────────────────────
@router.callback_query(F.data == "menu:mailing")
async def on_mailing_menu(callback: CallbackQuery, platform_user: dict | None):
    if not platform_user:
        return
    tariff = platform_user["tariff"]
    if tariff == "free":
        await callback.answer("Рассылка доступна с тарифа Старт.", show_alert=True)
        return

    channels = await db.fetch(
        "SELECT * FROM bot_chats WHERE owner_id=$1 AND is_active=true",
        platform_user["user_id"],
    )
    await callback.message.edit_text(
        "📨 <b>Массовая рассылка</b>\n\n"
        "⚠️ Рассылка отправляется только пользователям, "
        "которые открыли диалог с ботом (bot_activated).\n\n"
        "Выберите площадку:",
        reply_markup=kb_mailing_main(list(channels)),
    )
    await callback.answer()


# ── Начало создания рассылки ─────────────────────────────────
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

    # Считаем bot_activated получателей
    count = await db.fetchval(
        "SELECT COUNT(*) FROM bot_users WHERE owner_id=$1 AND chat_id=$2 "
        "AND is_active=true AND bot_activated=true",
        platform_user["user_id"], chat_id,
    )
    await state.update_data(chat_id=chat_id, owner_id=platform_user["user_id"])
    await state.set_state(MailingFSM.waiting_for_text)
    await callback.message.edit_text(
        f"📨 <b>Рассылка — {ch['chat_title']}</b>\n\n"
        f"👥 Получателей (bot_activated): <b>{count}</b>\n\n"
        f"📝 Отправьте текст рассылки:\n"
        f"(или прикрепите фото/видео с подписью)",
        reply_markup=kb_mailing_compose(),
    )
    await callback.answer()


# ── Получение текста рассылки ─────────────────────────────────
@router.message(MailingFSM.waiting_for_text)
async def on_mailing_text(message: Message, state: FSMContext):
    data = await state.get_data()
    chat_id = data.get("chat_id")
    owner_id = data.get("owner_id")

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

    # Сохраняем черновик
    mailing_id = await db.fetchval(
        """
        INSERT INTO mailings (owner_id, chat_id, text, media_file_id, media_type)
        VALUES ($1, $2, $3, $4, $5) RETURNING id
        """,
        owner_id, chat_id, text, media_file_id, media_type,
    )
    await state.update_data(mailing_id=mailing_id)
    await state.clear()

    preview = text[:200] + ("..." if len(text) > 200 else "")
    await message.answer(
        f"👁 <b>Предпросмотр:</b>\n\n{preview}\n\n"
        f"{'📎 Медиа: ' + media_type if media_type else ''}",
        reply_markup=kb_mailing_confirm(chat_id),
    )


# ── Запуск рассылки ───────────────────────────────────────────
@router.callback_query(F.data.startswith("mailing_run:"))
async def on_mailing_run(callback: CallbackQuery, bot: Bot, state: FSMContext, platform_user: dict | None):
    if not platform_user:
        return
    chat_id = int(callback.data.split(":")[1])

    # Берём последний черновик
    mailing = await db.fetchrow(
        "SELECT id FROM mailings WHERE owner_id=$1 AND chat_id=$2 AND status='draft' "
        "ORDER BY created_at DESC LIMIT 1",
        platform_user["user_id"], chat_id,
    )
    if not mailing:
        await callback.answer("Черновик не найден", show_alert=True)
        return

    mailing_id = mailing["id"]
    await db.execute("UPDATE mailings SET status='pending' WHERE id=$1", mailing_id)

    control_msg = await callback.message.edit_text(
        f"📨 <b>Рассылка запущена</b>\n\n"
        f"⏳ Отправлено: 0\n"
        f"⚡ Скорость: 🟢 Низкая",
        reply_markup=kb_mailing_control(mailing_id, False, "low"),
    )

    # Запускаем в фоне
    asyncio.create_task(mailing_svc.run_mailing(mailing_id, bot))
    await callback.answer("▶ Рассылка запущена")


# ── Планирование рассылки ─────────────────────────────────────
@router.callback_query(F.data.startswith("mailing_schedule:"))
async def on_mailing_schedule(callback: CallbackQuery, state: FSMContext):
    chat_id = callback.data.split(":")[1]
    await state.set_state(MailingFSM.waiting_for_schedule)
    await state.update_data(chat_id=chat_id)
    await callback.message.edit_text(
        "⏱ <b>Планирование рассылки</b>\n\n"
        "Укажите дату и время в формате:\n"
        "<code>28.02 18:00</code>\n\n"
        "⚠️ Максимум 3 дня от сейчас",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="🚫 Отменить", callback_data=f"mailing_start:{chat_id}")]
        ]),
    )
    await callback.answer()


# ── Управление рассылкой (пауза/скорость/отмена) ──────────────
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
    parts = callback.data.split(":")
    mailing_id = int(parts[1])
    speed = parts[2]
    mailing_svc.set_speed(mailing_id, speed)
    await callback.answer(f"Скорость изменена: {speed}")
