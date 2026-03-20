"""
utils/nav.py — Утилита навигации.
Вместо редактирования сообщения на месте (edit_text),
удаляет его и отправляет новое в самый низ чата.
"""
import logging
from aiogram.exceptions import TelegramBadRequest
from aiogram.types import CallbackQuery, InlineKeyboardMarkup, Message

logger = logging.getLogger(__name__)


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

    return msg


async def safe_edit_text(
    callback: CallbackQuery,
    text: str,
    reply_markup: InlineKeyboardMarkup | None = None,
    parse_mode: str = "HTML",
    **kwargs,
) -> Message | None:
    """
    Safely edit a callback message's text, handling the case where the message
    has no text content (e.g. media-only messages) or has been deleted.

    If the message cannot be edited (TelegramBadRequest), falls back to
    notifying the user via callback.answer() and sending a fresh message.
    """
    if not callback.message:
        await callback.answer("⚠️ Сообщение недоступно.", show_alert=True)
        return None

    if not callback.message.text:
        # Message has no text (media-only) — delete and send a new one
        try:
            await callback.message.delete()
        except Exception:
            pass
        try:
            return await callback.message.answer(
                text, reply_markup=reply_markup, parse_mode=parse_mode, **kwargs
            )
        except Exception as e:
            logger.warning(f"safe_edit_text: failed to send fallback message: {e}")
            await callback.answer("⚠️ Не удалось обновить сообщение.", show_alert=True)
            return None

    try:
        return await callback.message.edit_text(
            text, reply_markup=reply_markup, parse_mode=parse_mode, **kwargs
        )
    except TelegramBadRequest as e:
        logger.warning(f"safe_edit_text: TelegramBadRequest — {e}")
        await callback.answer("⚠️ Не удалось обновить сообщение.", show_alert=True)
        # Attempt to send a fresh message as fallback
        try:
            return await callback.message.answer(
                text, reply_markup=reply_markup, parse_mode=parse_mode, **kwargs
            )
        except Exception:
            pass
        return None
    except Exception as e:
        logger.warning(f"safe_edit_text: unexpected error — {e}")
        return None
