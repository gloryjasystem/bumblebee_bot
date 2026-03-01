"""
services/child_bot_service.py — Работа с дочерними ботами пользователей.
Валидация токена, получение информации о боте, проверка прав администратора.
"""
import logging
from aiogram import Bot
from aiogram.exceptions import TelegramUnauthorizedError, TelegramBadRequest

import db.pool as db
from services.security import encrypt_token, decrypt_token

logger = logging.getLogger(__name__)


async def validate_and_save_child_bot(owner_id: int, raw_token: str) -> dict:
    """
    Валидирует токен через Telegram API, сохраняет бота в child_bots.
    Возвращает dict с bot_id, bot_username, bot_name или выбрасывает ValueError.
    """
    raw_token = raw_token.strip()

    # Базовая проверка формата токена
    if ":" not in raw_token or len(raw_token) < 30:
        raise ValueError("Неверный формат токена. Скопируйте токен точно из @BotFather.")

    try:
        temp_bot = Bot(token=raw_token)
        me = await temp_bot.get_me()
        await temp_bot.session.close()
    except TelegramUnauthorizedError:
        raise ValueError("Токен недействителен. Проверьте токен и попробуйте снова.")
    except Exception as e:
        logger.error(f"validate token error: {e}")
        raise ValueError("Не удалось проверить токен. Попробуйте позже.")

    # Шифруем токен перед сохранением
    encrypted = encrypt_token(raw_token)

    # Сохраняем или обновляем
    row = await db.fetchrow(
        """
        INSERT INTO child_bots (owner_id, bot_id, bot_username, bot_name, token_encrypted)
        VALUES ($1, $2, $3, $4, $5)
        ON CONFLICT (owner_id, bot_id)
        DO UPDATE SET bot_username=EXCLUDED.bot_username,
                      bot_name=EXCLUDED.bot_name,
                      token_encrypted=EXCLUDED.token_encrypted
        RETURNING id, bot_id, bot_username, bot_name
        """,
        owner_id, me.id, me.username or "", me.full_name, encrypted,
    )
    return dict(row)


async def verify_bot_is_admin(owner_id: int, child_bot_id: int, chat_identifier: str) -> dict:
    """
    Проверяет, что дочерний бот является администратором в указанном канале/группе.
    chat_identifier — @username или -100xxxxxxxxxx
    Возвращает dict с chat_id и chat_title или выбрасывает ValueError.
    """
    bot_row = await db.fetchrow(
        "SELECT token_encrypted FROM child_bots WHERE id=$1 AND owner_id=$2",
        child_bot_id, owner_id,
    )
    if not bot_row:
        raise ValueError("Бот не найден.")

    raw_token = decrypt_token(bot_row["token_encrypted"])
    child = Bot(token=raw_token)

    try:
        # Пробуем получить информацию о чате
        chat_identifier = chat_identifier.strip()
        if chat_identifier.lstrip("-").isdigit():
            chat_id = int(chat_identifier)
        else:
            chat_id = chat_identifier if chat_identifier.startswith("@") else f"@{chat_identifier}"

        chat = await child.get_chat(chat_id)
        bot_me = await child.get_me()
        member = await child.get_chat_member(chat.id, bot_me.id)
    except TelegramBadRequest as e:
        raise ValueError(f"Канал не найден или бот не добавлен: {e}")
    except Exception as e:
        logger.error(f"verify_bot_is_admin error: {e}")
        raise ValueError("Не удалось проверить. Убедитесь что бот добавлен как администратор.")
    finally:
        await child.session.close()

    if member.status not in ("administrator", "creator"):
        raise ValueError(
            f"Бот @{bot_me.username} не является администратором в {chat.title}.\n"
            "Добавьте его как администратора с правами на управление участниками."
        )

    return {
        "chat_id":    chat.id,
        "chat_title": chat.title or chat_identifier,
        "chat_type":  chat.type,
    }
