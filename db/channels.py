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


async def resolve_chat_owner(user_id: int, chat_id: int) -> int:
    """Возвращает owner_id, от имени которого user_id управляет площадкой chat_id.

    Нужно для командного доступа: настройки площадки всегда живут под owner_id
    владельца бота, а редактировать их может и приглашённый член команды.

    Логика:
      1. Прямой владелец (у user_id есть своя строка bot_chats на этот чат) → сам user_id.
      2. Активный член команды бота, который держит этот чат (team_members.child_bot_id
         = bot_chats.child_bot_id, is_active) → реальный owner_id этого бота.
      3. Иначе → сам user_id.

    ВАЖНО: п.3 (fallback на самого user_id) гарантирует, что для владельца поведение
    НЕ меняется ни на йоту — в худшем случае это ровно старое `owner_id = user_id`.
    Доступ никому не выдаётся: если user_id не владелец и не член команды, запросы
    `WHERE owner_id=user_id` просто ничего не найдут, как и раньше.
    """
    row = await db.fetchrow(
        "SELECT 1 FROM bot_chats WHERE owner_id=$1 AND chat_id=$2::bigint",
        user_id, chat_id,
    )
    if row:
        return user_id

    row = await db.fetchrow(
        """
        SELECT bc.owner_id
        FROM bot_chats bc
        JOIN team_members tm ON tm.child_bot_id = bc.child_bot_id
                            AND tm.user_id = $1
                            AND tm.is_active = true
        WHERE bc.chat_id = $2::bigint
        ORDER BY bc.is_active DESC, bc.added_at ASC
        LIMIT 1
        """,
        user_id, chat_id,
    )
    if row:
        return row["owner_id"]

    return user_id


async def resolve_bot_chat_owner(user_id: int, bot_chat_id: int) -> int:
    """Как resolve_chat_owner, но по первичному ключу bot_chats.id (ch_id).

    Экраны деталей площадки грузят строку по bot_chats.id; чтобы командный доступ
    работал и там, резолвим владельца через chat_id этой строки.
    """
    row = await db.fetchrow("SELECT chat_id FROM bot_chats WHERE id=$1", bot_chat_id)
    if not row:
        return user_id
    return await resolve_chat_owner(user_id, row["chat_id"])
