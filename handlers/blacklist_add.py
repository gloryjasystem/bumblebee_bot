"""
handlers/blacklist_add.py — Универсальный хэндлер добавления в ЧС через RapidAPI-пайплайн.

Точки входа:
  - Глобальный Администратор: callback_data="ga_bl_rapidapi_add"  (кнопка "➕ Добавить в ЧС")
  - Локальный владелец:       callback_data="bl_rapidapi_add:{chat_id}"

Поддерживает три режима ввода:
  - Текст: @username, цифровой ID, ссылки t.me, списки через запятую/новую строку
  - Файл: .txt / .csv со списком
  - Forward: пересланное сообщение от целевого пользователя (без API, квота не тратится)

Дополнительно:
  - cancel_pipeline — Graceful Shutdown активного пайплайна.
"""
import asyncio
import logging
from typing import Optional

from aiogram import Bot, F, Router
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.types import (
    CallbackQuery,
    InlineKeyboardButton,
    InlineKeyboardMarkup,
    Message,
    MessageOriginUser,
)

from services.ban_pipeline import active_pipelines, start_ban_pipeline
from utils.username_parser import parse_file_content, parse_usernames_and_ids

logger = logging.getLogger(__name__)

router = Router()
# Держим ссылки на задачи, чтобы GC не отменил их до завершения
_pipeline_tasks: set[asyncio.Task] = set()


# ── FSM-состояния пайплайна ───────────────────────────────────────────────────

class RapidApiFSM(StatesGroup):
    """Состояния ожидания данных для RapidAPI-пайплайна."""
    waiting_for_input = State()   # Ждём текст или файл от пользователя
    processing        = State()   # Фоновая обработка запущена


# ── Клавиатуры ────────────────────────────────────────────────────────────────

def _kb_cancel_input(back_cb: str) -> InlineKeyboardMarkup:
    """Клавиатура с кнопкой отмены ввода (до старта пайплайна)."""
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="🚫 Отменить", callback_data=back_cb)],
    ])


# ── Глобальная точка входа (Главный Администратор) ────────────────────────────

@router.callback_query(F.data.startswith("ga_bl_rapidapi_add:"))
async def on_ga_rapidapi_add(call: CallbackQuery, state: FSMContext, platform_user: dict | None):
    """
    Вход с уровня Глобального Администратора.
    Запрашивает @username / ID / файл для глобального ЧС (child_bot_id=None).
    """
    if not platform_user:
        return

    owner_id = int(call.data.split(":")[1])
    back_cb  = f"ga_bl:{owner_id}"

    await state.update_data(child_bot_id=None, back_cb=back_cb, pipeline_owner_id=owner_id)
    await state.set_state(RapidApiFSM.waiting_for_input)

    prompt_msg = await call.message.edit_text(
        "🌐 <b>Добавить в ЧС — Универсальный ввод</b>\n\n"
        "Отправьте любое из вариантов:\n"
        "• <b>@username</b>, <b>цифровой ID</b> или ссылку <b>t.me/user</b>\n"
        "• Файл <b>.txt/.csv</b> со списком\n"
        "• <b>Пересланное сообщение</b> от нужного пользователя — мгновенно забаним\n\n"
        "<i>По юзернеймам бот автоматически получает цифровой ID через RapidAPI.\n"
        "Цифровые ID банятся мгновенно во всех подключённых каналах.</i>",
        parse_mode="HTML",
        reply_markup=_kb_cancel_input(back_cb),
    )
    await state.update_data(prompt_msg_id=prompt_msg.message_id)


# ── Локальная точка входа (владелец дочернего бота) ───────────────────────────

@router.callback_query(F.data.startswith("bl_rapidapi_add:"))
async def on_bl_rapidapi_add(call: CallbackQuery, state: FSMContext, platform_user: dict | None):
    """
    Вход с уровня локального владельца.
    Банит только в чатах указанного child_bot_id.
    """
    if not platform_user:
        return

    child_bot_id = int(call.data.split(":")[1])
    back_cb = f"bs_blacklist:{child_bot_id}"

    pipeline_owner_id = platform_user["user_id"]
    await state.update_data(child_bot_id=child_bot_id, back_cb=back_cb, pipeline_owner_id=pipeline_owner_id)
    await state.set_state(RapidApiFSM.waiting_for_input)

    prompt_msg = await call.message.edit_text(
        "🔑 <b>Добавить в ЧС — Универсальный ввод</b>\n\n"
        "Отправьте любое из вариантов:\n"
        "• <b>@username</b>, <b>цифровой ID</b> или ссылку <b>t.me/user</b>\n"
        "• Файл <b>.txt/.csv</b> со списком\n"
        "• <b>Пересланное сообщение</b> от нужного пользователя (мгновенно баним по ID)\n\n"
        "<i>По юзернеймам бот автоматически получает цифровой ID через RapidAPI.\n"
        "Цифровые ID банятся мгновенно во всех подключённых каналах.</i>",
        parse_mode="HTML",
        reply_markup=_kb_cancel_input(back_cb),
    )
    await state.update_data(prompt_msg_id=prompt_msg.message_id)


# ── Приём пересланного сообщения (Forward) — ЗАРЕГИСТРИРОВАН ПЕРВЫМ ────────
# Это критически важно: aiogram проверяет хэндлеры в порядке регистрации.
# Форвардное сообщение содержит и forward_origin, и text. Без этого порядка
# F.text перехватывал бы форварды раньше, что вызывало ошибку "не найдено".

@router.message(RapidApiFSM.waiting_for_input, F.forward_origin | F.forward_from)
async def on_rapidapi_forward_input(
    msg: Message, state: FSMContext, bot: Bot, platform_user: dict | None
):
    """
    Пересланное сообщение от целевого пользователя.
    Зарегистрирован ДО text-хэндлера — иначе F.text перехватывает форвард первым.

    Два типа forward_origin в aiogram 3.x:
      - MessageOriginUser     — ID доступен (открытый профиль).
      - MessageOriginHiddenUser — ID скрыт настройками приватности.
    Также обрабатывает устаревший forward_from для совместимости.
    """
    if not platform_user:
        return

    data     = await state.get_data()
    back_cb  = data.get("back_cb", "ga_bl")
    back_btn = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="◀️ Назад", callback_data=back_cb)]
    ])

    origin  = msg.forward_origin
    user_id: int | None = None

    if isinstance(origin, MessageOriginUser):
        user_id = origin.sender_user.id
        logger.info("[BL ADD] Forward from user=%d, username=%s",
                    user_id, origin.sender_user.username)
    elif msg.forward_from:
        user_id = msg.forward_from.id
        logger.info("[BL ADD] Forward (legacy) from user=%d", user_id)

    if user_id:
        await _kick_off_pipeline(
            msg, state, bot, platform_user,
            usernames=[], numeric_ids=[user_id],
        )
    else:
        # Пользователь скрыл свой ID в настройках приватности Telegram
        await msg.answer(
            "⚠️ <b>Невозможно определить ID пользователя.</b>\n\n"
            "Этот аккаунт запретил пересылку своего идентификатора (настройки приватности).\n"
            "Пришлите его <b>@username</b> или ссылку <b>t.me/никнейм</b>.",
            parse_mode="HTML",
            reply_markup=back_btn,
        )


# ── Приём текста — зарегистрирован ПОСЛЕ forward-хэндлера ─────────────────
# ~F.forward_origin и ~F.forward_from исключают форвардные сообщения явно.

@router.message(RapidApiFSM.waiting_for_input, F.text, ~F.forward_origin, ~F.forward_from)
async def on_rapidapi_text_input(
    msg: Message, state: FSMContext, bot: Bot, platform_user: dict | None
):
    """Принимает текстовый ввод (@username, ID, ссылки). Не срабатывает на форварды."""
    if not platform_user:
        return

    usernames, numeric_ids = parse_usernames_and_ids(msg.text or "")
    await _kick_off_pipeline(msg, state, bot, platform_user, usernames, numeric_ids)




# ── Приём файла ───────────────────────────────────────────────────────────────

@router.message(RapidApiFSM.waiting_for_input, F.document)
async def on_rapidapi_file_input(
    msg: Message, state: FSMContext, bot: Bot, platform_user: dict | None
):
    """Принимает .txt или .csv файл с @username / ID."""
    if not platform_user:
        return

    doc = msg.document
    if not doc or not doc.file_name:
        return await msg.answer("❌ Не удалось прочитать файл.")

    if not doc.file_name.lower().endswith((".txt", ".csv")):
        return await msg.answer(
            "❌ Поддерживаются только файлы <b>.txt</b> и <b>.csv</b>.",
            parse_mode="HTML",
        )

    if doc.file_size and doc.file_size > 20 * 1024 * 1024:
        return await msg.answer("❌ Файл слишком большой (максимум 20 МБ).")

    file_obj = await bot.get_file(doc.file_id)
    content_io = await bot.download_file(file_obj.file_path)
    content_bytes = content_io.read()

    usernames, numeric_ids = parse_file_content(content_bytes, doc.file_name)
    await _kick_off_pipeline(msg, state, bot, platform_user, usernames, numeric_ids)


# ── Запуск пайплайна ──────────────────────────────────────────────────────────

async def _kick_off_pipeline(
    msg:          Message,
    state:        FSMContext,
    bot:          Bot,
    platform_user: dict,
    usernames:    list[str],
    numeric_ids:  list[int],
) -> None:
    """
    Валидирует данные, отправляет статус-сообщение и запускает пайплайн
    как фоновую задачу через asyncio.create_task().
    """
    total = len(usernames) + len(numeric_ids)
    if total == 0:
        data     = await state.get_data()
        back_cb  = data.get("back_cb", "ga_bl")
        back_btn = InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="◀️ Назад", callback_data=back_cb)]
        ])
        await msg.answer(
            "⚠️ <b>Не найдено ни одного валидного значения.</b>\n\n"
            "Проверьте формат ввода:\n"
            "• <b>@username</b> — начинается с буквы, 5–32 символа\n"
            "• <b>Цифровой ID</b> — только цифры, например 123456789\n"
            "• <b>Переслать сообщение</b> — самый надёжный способ\n"
            "• <b>Ссылка</b> — t.me/никнейм",
            parse_mode="HTML",
            reply_markup=back_btn,
        )
        return

    data          = await state.get_data()
    child_bot_id  = data.get("child_bot_id")
    prompt_msg_id = data.get("prompt_msg_id")
    owner_id      = data.get("pipeline_owner_id", platform_user["user_id"])

    await state.set_state(RapidApiFSM.processing)

    # Удаляем сообщение с просьбой ввести данные (если есть)
    if prompt_msg_id:
        try:
            await msg.bot.delete_message(chat_id=msg.chat.id, message_id=prompt_msg_id)
        except Exception:
            pass

    status_msg = await msg.answer(
        f"⏳ <b>Запущена обработка {total} записей</b>\n"
        f"(@username: {len(usernames)} | числовых ID: {len(numeric_ids)})\n\n"
        "Статус будет обновляться автоматически...",
        parse_mode="HTML",
    )

    _task = asyncio.create_task(
        start_ban_pipeline(
            bot=bot,
            owner_id=owner_id,
            usernames=usernames,
            numeric_ids=numeric_ids,
            notify_chat_id=msg.chat.id,
            status_msg_id=status_msg.message_id,
            child_bot_id=child_bot_id,
        )
    )
    _pipeline_tasks.add(_task)
    _task.add_done_callback(_pipeline_tasks.discard)

    logger.info(
        "[BL ADD] Pipeline started: owner=%d total=%d (u=%d id=%d) child_bot=%s",
        owner_id, total, len(usernames), len(numeric_ids), child_bot_id,
    )

    # Сбрасываем состояние — пайплайн работает независимо от FSM
    await state.clear()


# ── Graceful Shutdown — кнопка «Остановить» ───────────────────────────────────

@router.callback_query(F.data.startswith("cancel_pipeline:"))
async def on_cancel_pipeline(call: CallbackQuery):
    """
    Обрабатывает нажатие кнопки «🛑 Остановить процесс».

    Алгоритм:
      1. Ищем stop_event по status_msg_id в active_pipelines.
      2. Устанавливаем event.set() — воркеры проверят is_set() и выйдут.
      3. Сливаем очередь через get_nowait() + task_done() — защита от deadlock.
      4. Удаляем ключ из active_pipelines — защита от memory leak.
    """
    try:
        status_msg_id = int(call.data.split(":")[1])
    except (IndexError, ValueError):
        return await call.answer("⚠️ Некорректный запрос.", show_alert=True)

    if status_msg_id not in active_pipelines:
        # Пайплайн уже завершился (нормально или был остановлен ранее)
        return await call.answer("ℹ️ Процесс уже завершён.", show_alert=True)

    stop_event = active_pipelines[status_msg_id]

    # 1. Взводим сигнал остановки
    stop_event.set()
    logger.info("[PIPELINE] Stop signal sent for msg_id=%d by user=%d",
                status_msg_id, call.from_user.id)

    # 2. del — защита от memory leak (пайплайн сам тоже удалит в finally,
    #    но лучше удалить здесь, чтобы повторное нажатие не вызывало ошибку)
    del active_pipelines[status_msg_id]

    await call.answer("🛑 Сигнал остановки отправлен.", show_alert=False)
