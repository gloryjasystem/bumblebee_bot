"""
middlewares/owner_check.py — Проверка: кто обращается к боту.
Регистрирует новых пользователей и проверяет роль (владелец/модератор/незнакомец).
"""
from typing import Any, Callable, Dict, Awaitable
from aiogram import BaseMiddleware
from aiogram.types import TelegramObject, Message, CallbackQuery

import db.pool as db
from config import settings


class OwnerMiddleware(BaseMiddleware):
    """
    Добавляет в data['platform_user'] данные о пользователе из platform_users.
    Если пользователь новый — просто передаём None (регистрация в /start).

    Владелец проекта (@alextgads / settings.owner_username):
    - всегда получает тариф 'business' без ограничения по времени (tariff_until = NULL).
    - Это гарантируется как на уровне middleware (переопределение в памяти),
      так и в БД при каждом обращении.
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
            platform_user = dict(row) if row else None

            # ── Владелец проекта и Совладелец: принудительно business навсегда ──────────────
            username = (user.username or "").lower().lstrip("@")
            is_project_owner = (
                user.id == settings.owner_telegram_id
                or (settings.owner_username and username == settings.owner_username.lower().lstrip("@"))
                or (settings.co_owner_telegram_id and user.id == settings.co_owner_telegram_id)
                or (settings.co_owner_username and username == settings.co_owner_username.lower().lstrip("@"))
            )

            if is_project_owner and platform_user is not None:
                # Обновляем БД, если тариф не business или стоит дата истечения
                if platform_user.get("tariff") != "business" or platform_user.get("tariff_until") is not None:
                    await db.execute(
                        """
                        UPDATE platform_users
                        SET tariff = 'business', tariff_until = NULL
                        WHERE user_id = $1
                        """,
                        user.id,
                    )
                # Всегда передаём актуальный тариф в data (даже если БД ещё не синхронизирована)
                platform_user.update({"tariff": "business", "tariff_until": None})

            data["platform_user"] = platform_user

        return await handler(event, data)
