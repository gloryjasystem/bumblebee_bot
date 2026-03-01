"""
middlewares/owner_check.py — Проверка: кто обращается к боту.
Регистрирует новых пользователей и проверяет роль (владелец/модератор/незнакомец).
"""
from typing import Any, Callable, Dict, Awaitable
from aiogram import BaseMiddleware
from aiogram.types import TelegramObject, Message, CallbackQuery

import db.pool as db


class OwnerMiddleware(BaseMiddleware):
    """
    Добавляет в data['platform_user'] данные о пользователе из platform_users.
    Если пользователь новый — просто передаём None (регистрация в /start).
    """

    async def __call__(
        self,
        handler: Callable[[TelegramObject, Dict[str, Any]], Awaitable[Any]],
        event: TelegramObject,
        data: Dict[str, Any],
    ) -> Any:
        # Получаем from_user из Message или CallbackQuery
        user = None
        if isinstance(event, (Message, CallbackQuery)):
            user = event.from_user

        if user:
            row = await db.fetchrow(
                "SELECT * FROM platform_users WHERE user_id=$1", user.id
            )
            data["platform_user"] = dict(row) if row else None

        return await handler(event, data)
