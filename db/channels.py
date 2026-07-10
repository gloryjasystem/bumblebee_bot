"""
db/channels.py — Единая точка резолва настроек площадки (таблица bot_chats).

ИНВАРИАНТ (совпадает с ограничением схемы `UNIQUE (owner_id, chat_id)`):
    chat_id НЕ уникален сам по себе. Один и тот же канал/группа легитимно бывает
    подключён к ботам РАЗНЫХ владельцев — у каждого владельца своя строка bot_chats.
    Уникальный ключ строки — (owner_id, chat_id). Второй естественный ключ —
    (child_bot_id, chat_id): дочерний бот принадлежит одному владельцу.

Поэтому настройки площадки ВСЕГДА резолвим по (owner_id | child_bot_id, chat_id),
никогда — по одному chat_id. Именно «SELECT ... WHERE chat_id=$1» в одиночку
приводил к чтению ЧУЖОЙ строки (с чужими autoaccept/captcha) — см. историю бага
с капчей: канал у двух владельцев, а капча читала произвольную строку.

Единственное исключение — входные точки ГЛАВНОГО бота (ChatJoinRequest / сообщение
в группе), где известен только chat_id. Там выбираем строку ДЕТЕРМИНИРОВАННО
(старейшую) и логируем неоднозначность, чтобы она никогда не была «тихой».

ПРАВИЛО: загружать строку площадки ТОЛЬКО через get_channel(). Сырые запросы
«WHERE chat_id=...» в обработчиках запрещены.
"""
import logging

import db.pool as db

logger = logging.getLogger(__name__)


async def get_channel(
    chat_id: int,
    *,
    owner_id: int | None = None,
    child_bot_id: int | None = None,
):
    """Возвращает активную строку bot_chats (asyncpg.Record) или None.

    Приоритет резолва:
      1. (child_bot_id, chat_id) — точный ключ, если известен бот;
      2. (owner_id, chat_id)     — точный ключ, если известен владелец;
      3. только chat_id          — детерминированно старейшая строка + WARNING,
                                    если строк несколько (канал у нескольких владельцев).

    Точные ключи (1/2) гарантированно однозначны — их защищает UNIQUE(owner_id, chat_id).
    Ветка (3) — только для входов главного бота, где владелец из события неизвестен.
    """
    if child_bot_id is not None:
        row = await db.fetchrow(
            "SELECT * FROM bot_chats WHERE child_bot_id=$1 AND chat_id=$2::bigint AND is_active=true",
            child_bot_id, chat_id,
        )
        if row:
            return row

    if owner_id is not None:
        row = await db.fetchrow(
            "SELECT * FROM bot_chats WHERE owner_id=$1 AND chat_id=$2::bigint AND is_active=true",
            owner_id, chat_id,
        )
        if row:
            return row

    rows = await db.fetch(
        "SELECT * FROM bot_chats WHERE chat_id=$1::bigint AND is_active=true "
        "ORDER BY added_at ASC, id ASC",
        chat_id,
    )
    if len(rows) > 1:
        owners = ", ".join(str(r["owner_id"]) for r in rows)
        logger.warning(
            "[CHANNEL] chat_id=%s подключён к %d владельцам (%s) — беру старейшую строку. "
            "Вызов без owner_id/child_bot_id неоднозначен; передайте дизамбигуатор.",
            chat_id, len(rows), owners,
        )
    return rows[0] if rows else None
