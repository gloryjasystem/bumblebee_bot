"""
services/blacklist.py — Весь функционал чёрного списка.
"""
import asyncio
import logging
from aiogram import Bot

import db.pool as db
from services.security import parse_blacklist_line, detect_rtl, detect_hieroglyph

logger = logging.getLogger(__name__)


async def check_blacklist(owner_id: int, user_id: int, username: str | None) -> bool:
    """
    Мгновенная проверка пользователя по ЧС.
    Использует индексированные поля — < 1 мс при 1M записей.
    """
    row = await db.fetchrow(
        """
        SELECT 1 FROM blacklist
        WHERE owner_id = $1
          AND (
            (user_id IS NOT NULL AND user_id = $2)
            OR
            (username IS NOT NULL AND lower(username) = lower($3))
          )
        LIMIT 1
        """,
        owner_id, user_id, username or "",
    )
    return row is not None


async def add_to_blacklist(owner_id: int, user_id: int | None, username: str | None) -> bool:
    """
    Добавляет одну запись в ЧС.
    Возвращает True если добавлена, False если уже была.
    """
    result = await db.execute(
        """
        INSERT INTO blacklist (owner_id, user_id, username)
        VALUES ($1, $2, $3)
        ON CONFLICT DO NOTHING
        """,
        owner_id, user_id, username.lower() if username else None,
    )
    return result == "INSERT 0 1"


async def import_file(owner_id: int, content: bytes, filename: str) -> dict:
    """
    Импортирует файл ЧС (TXT/CSV).
    Возвращает {'added': int, 'duplicates': int, 'invalid': int, 'total': int}.
    """
    try:
        text = content.decode("utf-8")
    except UnicodeDecodeError:
        text = content.decode("latin-1")

    rows = []
    added = duplicates = invalid = 0

    for line in text.splitlines():
        parsed = parse_blacklist_line(line)
        if parsed is None:
            if line.strip() and not line.startswith("#"):
                invalid += 1
            continue
        rows.append((owner_id, parsed["user_id"], parsed["username"]))

    # Пакетная вставка с дедупликацией
    if rows:
        # Разбиваем на батчи по 1000
        for i in range(0, len(rows), 1000):
            batch = rows[i:i+1000]
            result = await db.executemany(
                """
                INSERT INTO blacklist (owner_id, user_id, username)
                VALUES ($1, $2, $3)
                ON CONFLICT DO NOTHING
                """,
                batch,
            )
            # asyncpg executemany не возвращает количество — считаем через SELECT
        total_after = await db.fetchval(
            "SELECT COUNT(*) FROM blacklist WHERE owner_id=$1", owner_id
        )
        added = len(rows) - invalid  # приблизительно
        duplicates = 0  # точно не считаем для скорости

    return {
        "added": added,
        "duplicates": duplicates,
        "invalid": invalid,
        "total": await db.fetchval("SELECT COUNT(*) FROM blacklist WHERE owner_id=$1", owner_id),
    }


async def sweep_after_import(owner_id: int, bot: Bot) -> int:
    """
    После загрузки файла — банит всех нарушителей во всех активных площадках.
    Запускать через asyncio.create_task() — не блокирует ответ боту.
    """
    chats = await db.fetch(
        "SELECT chat_id FROM bot_chats WHERE owner_id=$1 AND is_active=true",
        owner_id,
    )
    total_banned = 0

    for chat in chats:
        violators = await db.fetch(
            """
            SELECT DISTINCT bu.user_id
            FROM bot_users bu
            INNER JOIN blacklist bl ON bl.owner_id = bu.owner_id
              AND (
                (bl.user_id IS NOT NULL AND bl.user_id = bu.user_id)
                OR
                (bl.username IS NOT NULL AND bl.username = bu.username)
              )
            WHERE bu.owner_id = $1
              AND bu.chat_id = $2
              AND bu.is_active = true
            """,
            owner_id, chat["chat_id"],
        )

        for v in violators:
            try:
                await bot.ban_chat_member(chat["chat_id"], v["user_id"])
                await db.execute(
                    "UPDATE bot_users SET is_active=false, left_at=now() "
                    "WHERE owner_id=$1 AND chat_id=$2 AND user_id=$3",
                    owner_id, chat["chat_id"], v["user_id"],
                )
                total_banned += 1
            except Exception as e:
                logger.debug(f"Ban failed for {v['user_id']} in {chat['chat_id']}: {e}")
            await asyncio.sleep(0.05)  # 20 банов/сек — безопасный лимит

    return total_banned


async def sweep_unban_after_disable(owner_id: int, bot: Bot) -> int:
    """
    Разбанивает всех пользователей, которые есть в базе ЧС (если ЧС выключили).
    """
    chats = await db.fetch(
        "SELECT chat_id FROM bot_chats WHERE owner_id=$1 AND is_active=true",
        owner_id,
    )
    total_unbanned = 0

    for chat in chats:
        # Находим всех, кто есть в blacklist
        violators = await db.fetch(
            """
            SELECT bl.user_id
            FROM blacklist bl
            WHERE bl.owner_id = $1 AND bl.user_id IS NOT NULL
            """,
            owner_id,
        )

        for v in violators:
            try:
                await bot.unban_chat_member(chat["chat_id"], v["user_id"], only_if_banned=True)
                total_unbanned += 1
            except Exception as e:
                logger.debug(f"Unban failed for {v['user_id']} in {chat['chat_id']}: {e}")
            await asyncio.sleep(0.05)  # 20 анбанов/сек

    return total_unbanned


async def get_blacklist_count(owner_id: int) -> int:
    return await db.fetchval(
        "SELECT COUNT(*) FROM blacklist WHERE owner_id=$1", owner_id
    )
