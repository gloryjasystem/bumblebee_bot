"""
utils/nav.py — Утилита навигации.
Вместо редактирования сообщения на месте (edit_text),
удаляет его и отправляет новое в самый низ чата.
"""
from aiogram.types import CallbackQuery, InlineKeyboardMarkup, InlineKeyboardButton, Message


async def set_active_msg(user_id: int, message_id: int | None) -> None:
    """Запоминает id текущего «живого» экрана (SPA-указатель для команд вроде /help)."""
    import db.pool as db
    try:
        await db.execute(
            "UPDATE platform_users SET active_msg_id=$1 WHERE user_id=$2",
            message_id, user_id,
        )
    except Exception:
        pass  # best-effort: запись id не должна ломать навигацию


async def get_active_msg(user_id: int) -> int | None:
    """Возвращает id текущего «живого» экрана или None."""
    import db.pool as db
    try:
        return await db.fetchval(
            "SELECT active_msg_id FROM platform_users WHERE user_id=$1", user_id
        )
    except Exception:
        return None


async def consume_command(message) -> None:
    """SPA-приём для текстовых команд (/start, /help): удаляет сам командный
    месседж и предыдущий «живой» экран (меню/кабинет с кнопками), чтобы не
    оставалось старое окно. Вызывать ДО отправки нового экрана.
    Все удаления best-effort — ничего не падает, если сообщение уже удалено/устарело."""
    prev_id = await get_active_msg(message.from_user.id)
    try:
        await message.delete()
    except Exception:
        pass
    if prev_id:
        try:
            await message.bot.delete_message(chat_id=message.chat.id, message_id=prev_id)
        except Exception:
            pass


async def navigate(
    callback: CallbackQuery,
    text: str,
    reply_markup: InlineKeyboardMarkup | None = None,
    parse_mode: str = "HTML",
) -> Message | None:
    """
    Удаляет текущее сообщение и отправляет новое снизу.
    Используется вместо callback.message.edit_text при навигации назад/вперёд.

    Автоматически инжектирует UI «Режима управления» (God Mode), если администратор
    в данный момент управляет чужим аккаунтом. Это гарантирует, что плашка и кнопка
    выхода отображаются на ЛЮБОМ экране, а не только на исходном.
    """
    # ── Глобальный перехват God Mode ─────────────────────────────────────────
    from utils.god_mode import get_target as _god_get
    import db.pool as db

    _god_target = _god_get(callback.from_user.id)
    if _god_target:
        _pu_row = await db.fetchrow(
            "SELECT username, first_name FROM platform_users WHERE user_id=$1",
            _god_target,
        )
        if _pu_row:
            _uname = (
                f"@{_pu_row['username']}" if _pu_row.get("username")
                else (_pu_row["first_name"] if _pu_row.get("first_name") else str(_god_target))
            )
            # Добавляем плашку, только если её ещё нет (защита от дублирования)
            indicator = f"🔴 <b>Режим управления:</b> <code>{_uname}</code>\n\n"
            if not text.startswith("🔴 <b>Режим управления"):
                text = indicator + text

            # Добавляем кнопку выхода, создавая КОПИЮ клавиатуры
            # (не мутируем оригинальный объект, чтобы не было side-эффектов)
            if reply_markup is not None:
                exit_btn = InlineKeyboardButton(
                    text="🚪 Завершить управление",
                    callback_data=f"ga_exit:{_god_target}:{callback.from_user.id}",
                )
                new_keyboard = list(reply_markup.inline_keyboard) + [[exit_btn]]
                reply_markup = InlineKeyboardMarkup(inline_keyboard=new_keyboard)
    # ─────────────────────────────────────────────────────────────────────────

    try:
        await callback.message.delete()
    except Exception:
        pass  # Сообщение уже удалено или слишком старое — игнорируем

    msg = await callback.message.answer(
        text,
        reply_markup=reply_markup,
        parse_mode=parse_mode,
    )
    # Запоминаем новый экран как «живой» (для SPA-поведения /help и т.п.)
    await set_active_msg(callback.from_user.id, msg.message_id)
    try:
        await callback.answer()
    except Exception:
        pass

    return msg
