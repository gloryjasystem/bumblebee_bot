"""
handlers/blacklist_add.py — Обработчики RapidAPI-пайплайна для добавления в ЧС.

Точки входа:
  - Глобальный Администратор: callback_data="ga_bl_rapidapi_add"
  - Локальный владелец:       callback_data="bl_rapidapi_add:{chat_id}"

Дополнительно:
  - cancel_pipeline:      — Graceful Shutdown активного пайплайна.
"""
import asyncio
import logging

from aiogram import Bot, F, Router
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.types import (
    CallbackQuery,
    InlineKeyboardButton,
    InlineKeyboardMarkup,
    Message,
)

from services.ban_pipeline import active_pipelines, start_ban_pipeline
from utils.username_parser import parse_file_content, parse_usernames_and_ids

logger = logging.getLogger(__name__)

router = Router()


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

@router.callback_query(F.data == "ga_bl_rapidapi_add")
async def on_ga_rapidapi_add(call: CallbackQuery, state: FSMContext, platform_user: dict | None):
    """
    Вход с уровня Глобального Администратора.
    Запрашивает @username / ID / файл для глобального ЧС (child_bot_id=None).
    """
    if not platform_user:
        return

    owner_id = platform_user["user_id"]
    back_cb  = f"ga_bl:{owner_id}"

    await state.update_data(child_bot_id=None, back_cb=back_cb)
    await state.set_state(RapidApiFSM.waiting_for_input)

    prompt_msg = await call.message.edit_text(
        "🌐 <b>Глобальный ЧС — добавить через RapidAPI</b>\n\n"
        "Отправьте <b>@username</b>, <b>числовой ID</b>, ссылку <b>t.me/user</b> "
        "или загрузите файл <b>.txt/.csv</b>.\n"
        "Несколько записей — каждая с новой строки или через запятую.\n\n"
        "<i>Пользователи будут конвертированы через RapidAPI и забанены "
        "во всех подключённых каналах.</i>",
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

    await state.update_data(child_bot_id=child_bot_id, back_cb=back_cb)
    await state.set_state(RapidApiFSM.waiting_for_input)

    prompt_msg = await call.message.edit_text(
        "🔑 <b>Добавить в ЧС через RapidAPI</b>\n\n"
        "Отправьте <b>@username</b>, <b>числовой ID</b>, ссылку <b>t.me/user</b> "
        "или загрузите файл <b>.txt/.csv</b>.\n"
        "Несколько записей — каждая с новой строки или через запятую.\n\n"
        "<i>Пользователи будут конвертированы через RapidAPI и забанены "
        "в ваших каналах.</i>",
        parse_mode="HTML",
        reply_markup=_kb_cancel_input(back_cb),
    )
    await state.update_data(prompt_msg_id=prompt_msg.message_id)


# ── Приём текста ──────────────────────────────────────────────────────────────

@router.message(RapidApiFSM.waiting_for_input, F.text)
async def on_rapidapi_text_input(
    msg: Message, state: FSMContext, bot: Bot, platform_user: dict | None
):
    """Принимает текстовый ввод (@username, ID, ссылки — через пробел/запятую)."""
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
        await msg.answer(
            "⚠️ Не найдено ни одного валидного @username или числового ID.\n\n"
            "<i>Убедитесь, что юзернейм содержит от 5 до 32 символов и начинается с буквы.</i>",
            parse_mode="HTML",
        )
        return

    data          = await state.get_data()
    child_bot_id  = data.get("child_bot_id")
    prompt_msg_id = data.get("prompt_msg_id")
    owner_id      = platform_user["user_id"]

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

    asyncio.create_task(
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
