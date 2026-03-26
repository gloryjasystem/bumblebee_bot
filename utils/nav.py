"""
utils/nav.py — Утилита навигации.
Вместо редактирования сообщения на месте (edit_text),
удаляет его и отправляет новое в самый низ чата.
"""
from aiogram.types import CallbackQuery, InlineKeyboardMarkup, Message


async def navigate(
    callback: CallbackQuery,
    text: str,
    reply_markup: InlineKeyboardMarkup | None = None,
    parse_mode: str = "HTML",
) -> Message | None:
    """
    Удаляет текущее сообщение и отправляет новое снизу.
    Используется вместо callback.message.edit_text при навигации назад/вперёд.
    """
    try:
        await callback.message.delete()
    except Exception:
        pass  # Сообщение уже удалено или слишком старое — игнорируем

    msg = await callback.message.answer(
        text,
        reply_markup=reply_markup,
        parse_mode=parse_mode,
    )
    try:
        await callback.answer()
    except Exception:
        pass

    # ── Tracking for auto-closing the "Channels List" menu ──
    if msg and callback.data:
        from db.database import db
        import asyncio
        async def _update_menu_state():
            try:
                if callback.data.startswith("bot_chats_list:"):
                    await db.execute(
                        "UPDATE platform_users SET last_channels_menu_id=$1 WHERE user_id=$2",
                        msg.message_id, callback.from_user.id
                    )
                else:
                    await db.execute(
                        "UPDATE platform_users SET last_channels_menu_id=NULL WHERE user_id=$1",
                        callback.from_user.id
                    )
            except Exception:
                pass
        asyncio.create_task(_update_menu_state())

    return msg
