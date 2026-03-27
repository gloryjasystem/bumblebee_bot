"""
middlewares/owner_check.py — Проверка: кто обращается к боту.
Регистрирует новых пользователей и проверяет роль (владелец/модератор/незнакомец).
"""
from typing import Any, Callable, Dict, Awaitable
from aiogram import BaseMiddleware
from aiogram.types import TelegramObject, Message, CallbackQuery

import db.pool as db
from config import settings

# ── Текст «прилипающего» сообщения о бане ────────────────────────────────────
def _ban_text(reason: str) -> str:
    return (
        "⛔️ <b>Ваш аккаунт заблокирован решением администрации.</b>\n\n"
        f"📝 Причина: <i>{reason}</i>\n\n"
        "Если вы считаете это ошибкой, обратитесь в поддержку."
    )


async def _send_sticky_ban(bot, user_id: int, reason: str,
                           old_ban_msg_id: int | None) -> int | None:
    """
    Удаляет старое прилипающее сообщение (если было) и отправляет новое.
    Возвращает message_id нового сообщения.
    """
    if old_ban_msg_id:
        try:
            await bot.delete_message(chat_id=user_id, message_id=old_ban_msg_id)
        except Exception:
            pass

    try:
        sent = await bot.send_message(
            chat_id=user_id,
            text=_ban_text(reason),
            parse_mode="HTML",
        )
        return sent.message_id
    except Exception:
        return None


class OwnerMiddleware(BaseMiddleware):
    """
    1. Если is_banned=True:
       - Message → удаляем сообщение пользователя, ротируем прилипающий бан-msg.
       - CallbackQuery → тихо гасим, удаляем сообщение с кнопками, ротируем бан-msg.
       - Прерываем обработку (return без вызова handler).
    2. Если is_banned=False И unban_msg_id != NULL:
       - Удаляем уведомление о разблокировке (оно висело внизу чата).
       - Обнуляем unban_msg_id в БД и пропускаем дальше штатно.
    3. Владелец / Совладелец — принудительно тариф business навсегда.
    """

    async def __call__(
        self,
        handler: Callable[[TelegramObject, Dict[str, Any]], Awaitable[Any]],
        event: TelegramObject,
        data: Dict[str, Any],
    ) -> Any:
        user = None
        if isinstance(event, (Message, CallbackQuery)):
            user = event.from_user

        if not user:
            return await handler(event, data)

        row = await db.fetchrow(
            "SELECT * FROM platform_users WHERE user_id=$1", user.id
        )
        platform_user = dict(row) if row else None

        # ── 1. Пользователь заблокирован ─────────────────────────────────────
        if platform_user and platform_user.get("is_banned"):
            reason     = platform_user.get("ban_reason") or "Нарушение правил"
            old_ban_id = platform_user.get("ban_msg_id")
            bot        = event.bot if hasattr(event, "bot") else data.get("bot")

            if isinstance(event, Message):
                # Удаляем сообщение пользователя
                try:
                    await event.delete()
                except Exception:
                    pass

                new_ban_id = await _send_sticky_ban(bot, user.id, reason, old_ban_id)

            elif isinstance(event, CallbackQuery):
                # Тихо гасим всплывашку
                try:
                    await event.answer()
                except Exception:
                    pass
                # Удаляем сообщение, к которому была привязана кнопка
                if event.message:
                    try:
                        await event.message.delete()
                    except Exception:
                        pass

                new_ban_id = await _send_sticky_ban(bot, user.id, reason, old_ban_id)
            else:
                return  # другие типы событий — просто отсекаем

            # Сохраняем новый ban_msg_id
            if new_ban_id:
                await db.execute(
                    "UPDATE platform_users SET ban_msg_id=$1 WHERE user_id=$2",
                    new_ban_id, user.id,
                )
            return  # ← НЕ вызываем handler

        # ── 2. Только что разблокирован — чистим уведомление о разбане ───────
        if platform_user and platform_user.get("unban_msg_id"):
            unban_msg_id = platform_user["unban_msg_id"]
            bot = event.bot if hasattr(event, "bot") else data.get("bot")
            try:
                await bot.delete_message(chat_id=user.id, message_id=unban_msg_id)
            except Exception:
                pass
            await db.execute(
                "UPDATE platform_users SET unban_msg_id=NULL WHERE user_id=$1",
                user.id,
            )
            # Пропускаем дальше штатно — не прерываем вызов handler

        # ── 3. Владелец проекта / Совладелец: принудительно business ─────────
        username = (user.username or "").lower().lstrip("@")
        is_project_owner = (
            user.id == settings.owner_telegram_id
            or (settings.owner_username and username == settings.owner_username.lower().lstrip("@"))
            or (settings.co_owner_telegram_id and user.id == settings.co_owner_telegram_id)
            or (settings.co_owner_username and username == settings.co_owner_username.lower().lstrip("@"))
        )

        if is_project_owner:
            # Гарантируем, что Главный Владелец существует в базе
            await db.execute(
                """
                INSERT INTO platform_users (user_id, username, first_name, tariff)
                VALUES ($1, $2, 'Owner', 'business')
                ON CONFLICT (user_id) DO UPDATE SET tariff = 'business', tariff_until = NULL
                """,
                settings.owner_telegram_id,
                settings.owner_username.lower().lstrip("@") if settings.owner_username else None,
            )

            if platform_user is not None:
                if platform_user.get("tariff") != "business" or platform_user.get("tariff_until") is not None:
                    await db.execute(
                        "UPDATE platform_users SET tariff='business', tariff_until=NULL WHERE user_id=$1",
                        user.id,
                    )
            if platform_user is not None:
                platform_user.update({"tariff": "business", "tariff_until": None})

        data["platform_user"] = platform_user

        return await handler(event, data)
