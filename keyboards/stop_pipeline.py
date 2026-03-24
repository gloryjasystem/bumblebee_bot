"""
keyboards/stop_pipeline.py — Inline-клавиатура с кнопкой аварийной остановки пайплайна.
"""
from aiogram.types import InlineKeyboardMarkup
from aiogram.utils.keyboard import InlineKeyboardBuilder


def stop_keyboard(status_msg_id: int) -> InlineKeyboardMarkup:
    """
    Возвращает Inline-клавиатуру с единственной кнопкой «Остановить процесс».

    callback_data содержит ID сообщения с прогресс-баром, чтобы handler
    мог найти нужный asyncio.Event через active_pipelines[status_msg_id].

    Args:
        status_msg_id: ID сообщения с прогресс-баром.
    """
    builder = InlineKeyboardBuilder()
    builder.button(
        text="🛑 Остановить процесс",
        callback_data=f"cancel_pipeline:{status_msg_id}",
    )
    return builder.as_markup()
