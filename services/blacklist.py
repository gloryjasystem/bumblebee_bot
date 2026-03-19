"""
services/blacklist.py — Весь функционал чёрного списка.
"""
import asyncio
import logging

import db.pool as db
from services.security import parse_blacklist_line

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


async def _get_chats_with_tokens(owner_id: int) -> list:
    """
    Возвращает список активных чатов со статусом ЧС и расшифрованными токенами.
    """
    from services.security import decrypt_token
    rows = await db.fetch(
        """
        SELECT bc.chat_id, cb.token_encrypted, cb.blacklist_enabled
        FROM bot_chats bc
        JOIN child_bots cb ON cb.id = bc.child_bot_id
        WHERE bc.owner_id = $1 AND bc.is_active = true
        """,
        owner_id,
    )
    result = []
    for r in rows:
        token = decrypt_token(r["token_encrypted"])
        if token:
            result.append({
                "chat_id": r["chat_id"],
                "token": token,
                "blacklist_enabled": r.get("blacklist_enabled", True),
            })
    return result


async def _ban_user_in_chat(token: str, chat_id: int, user_id: int) -> bool:
    """
    Пытается забанить user_id в chat_id через дочернего бота.
    Возвращает True если бан успешно применён (пользователь был в чате).
    """
    from aiogram import Bot
    try:
        async with Bot(token=token).context() as child_bot:
            await child_bot.ban_chat_member(chat_id, user_id)
            logger.info(f"[BL KICK] Banned user={user_id} from chat={chat_id}")
            return True
    except Exception as e:
        err = str(e).lower()
        # Если пользователя нет в чате — это не ошибка, просто не нашли
        if "user not found" in err or "user_not_participant" in err or "member_not_found" in err:
            return False
        logger.debug(f"[BL KICK] ban failed user={user_id} chat={chat_id}: {e}")
        return False


async def kick_single_user(owner_id: int, user_id: int | None, username: str | None) -> int:
    """
    Кикает конкретного пользователя из всех активных площадок владельца.
    Работает для ВСЕХ участников чата — независимо от того, когда они вступили.

    - Если известен user_id: пробуем банить напрямую в каждом чате.
    - Если только username: резолвим через bot_users, потом баним.

    Возвращает количество чатов, откуда был выкинут пользователь.
    """
    chats = await _get_chats_with_tokens(owner_id)
    kicked = 0

    # Резолвим user_id через username если не задан явно
    resolved_user_id = user_id
    if not resolved_user_id and username:
        row = await db.fetchrow(
            "SELECT user_id FROM bot_users WHERE owner_id=$1 AND lower(username)=lower($2) LIMIT 1",
            owner_id, username,
        )
        if row:
            resolved_user_id = row["user_id"]

    if not resolved_user_id:
        # Без user_id забанить через Telegram API невозможно
        logger.info(f"[BL KICK] Cannot resolve user_id for username={username}, skip kick")
        return 0

    for chat in chats:
        success = await _ban_user_in_chat(chat["token"], chat["chat_id"], resolved_user_id)
        if success:
            # Помечаем в bot_users как неактивного
            await db.execute(
                "UPDATE bot_users SET is_active=false, left_at=now() "
                "WHERE owner_id=$1 AND chat_id=$2 AND user_id=$3",
                owner_id, chat["chat_id"], resolved_user_id,
            )
            kicked += 1
        await asyncio.sleep(0.05)  # 20 банов/сек — безопасный лимит

    # Инкрементируем счётчик заблокированных
    if kicked > 0:
        await db.execute(
            "UPDATE platform_users SET blocked_count = blocked_count + $1 WHERE user_id = $2",
            kicked, owner_id,
        )
        logger.info(f"[BL KICK] user={resolved_user_id} kicked from {kicked} chats for owner={owner_id}")

    return kicked


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
        for i in range(0, len(rows), 1000):
            batch = rows[i:i+1000]
            await db.executemany(
                """
                INSERT INTO blacklist (owner_id, user_id, username)
                VALUES ($1, $2, $3)
                ON CONFLICT DO NOTHING
                """,
                batch,
            )
        added = len(rows) - invalid  # приблизительно

    return {
        "added": added,
        "duplicates": duplicates,
        "invalid": invalid,
        "total": await db.fetchval("SELECT COUNT(*) FROM blacklist WHERE owner_id=$1", owner_id),
    }


async def sweep_after_import(owner_id: int) -> int:
    """
    После загрузки файла — банит всех нарушителей во всех активных площадках.

    Алгоритм:
    1. Для записей с явным user_id:
       - Пробуем ban_chat_member напрямую в каждом чате.
       - Telegram API ответит ошибкой если пользователя нет — это нормально.
    2. Для записей только с username:
       - Резолвим user_id через bot_users (если пользователь когда-либо взаимодействовал с ботом).
       - Баним найденных.
    3. Инкрементируем blocked_count для каждого успешного бана.
    """
    chats = await _get_chats_with_tokens(owner_id)
    if not chats:
        return 0

    # Берём все записи из ЧС
    bl_records = await db.fetch(
        "SELECT user_id, username FROM blacklist WHERE owner_id = $1",
        owner_id,
    )

    # Разделяем: с явным user_id и только username
    explicit_ids = [r["user_id"] for r in bl_records if r["user_id"] is not None]
    usernames_only = [r["username"].lower() for r in bl_records
                      if r["user_id"] is None and r["username"] is not None]

    # Резолвим username → user_id через bot_users
    resolved_ids_from_usernames = set()
    if usernames_only:
        rows = await db.fetch(
            """
            SELECT DISTINCT user_id FROM bot_users
            WHERE owner_id = $1 AND lower(username) = ANY($2::text[])
            """,
            owner_id, usernames_only,
        )
        resolved_ids_from_usernames = {r["user_id"] for r in rows}

    all_user_ids = list(set(explicit_ids) | resolved_ids_from_usernames)
    if not all_user_ids:
        return 0

    total_banned = 0

    for chat in chats:
        chat_id = chat["chat_id"]
        token = chat["token"]

        for uid in all_user_ids:
            success = await _ban_user_in_chat(token, chat_id, uid)
            if success:
                await db.execute(
                    "UPDATE bot_users SET is_active=false, left_at=now() "
                    "WHERE owner_id=$1 AND chat_id=$2 AND user_id=$3",
                    owner_id, chat_id, uid,
                )
                total_banned += 1
            await asyncio.sleep(0.05)

    # Инкрементируем счётчик
    if total_banned > 0:
        await db.execute(
            "UPDATE platform_users SET blocked_count = blocked_count + $1 WHERE user_id = $2",
            total_banned, owner_id,
        )
        logger.info(f"[BL SWEEP] owner={owner_id}: banned {total_banned} users across {len(chats)} chats")

    return total_banned


async def sweep_unban_after_disable(owner_id: int) -> int:
    """
    Разбанивает всех пользователей, которые есть в базе ЧС (если ЧС выключили).
    """
    from aiogram import Bot

    chats = await _get_chats_with_tokens(owner_id)
    total_unbanned = 0

    # Берём все user_id + резолвим username через bot_users
    bl_records = await db.fetch(
        "SELECT user_id, username FROM blacklist WHERE owner_id = $1",
        owner_id,
    )
    explicit_ids = {r["user_id"] for r in bl_records if r["user_id"] is not None}
    usernames_only = [r["username"].lower() for r in bl_records
                      if r["user_id"] is None and r["username"] is not None]

    resolved_from_usernames = set()
    if usernames_only:
        rows = await db.fetch(
            "SELECT DISTINCT user_id FROM bot_users WHERE owner_id=$1 AND lower(username)=ANY($2::text[])",
            owner_id, usernames_only,
        )
        resolved_from_usernames = {r["user_id"] for r in rows}

    all_ids = list(explicit_ids | resolved_from_usernames)
    if not all_ids:
        return 0

    for chat in chats:
        chat_id = chat["chat_id"]
        token = chat["token"]
        try:
            async with Bot(token=token).context() as child_bot:
                for uid in all_ids:
                    try:
                        await child_bot.unban_chat_member(chat_id, uid, only_if_banned=True)
                        total_unbanned += 1
                    except Exception as e:
                        logger.debug(f"Unban failed for {uid} in {chat_id}: {e}")
                    await asyncio.sleep(0.05)
        except Exception as e:
            logger.debug(f"Failed to use child_bot unban for chat {chat_id}: {e}")

    return total_unbanned


async def sweep_unban_records(owner_id: int, records: list) -> None:
    """
    Фоновая задача разбана после очистки базы ЧС.
    records: список строк из БД с полями user_id, username.
    """
    from aiogram import Bot

    chats = await _get_chats_with_tokens(owner_id)
    if not chats:
        return

    explicit_user_ids = {r["user_id"] for r in records if r["user_id"] is not None}
    usernames = [r["username"].lower() for r in records if r["username"] is not None]

    for chat in chats:
        chat_id = chat["chat_id"]
        token = chat["token"]

        username_user_ids = set()
        if usernames:
            resolved = await db.fetch(
                "SELECT user_id FROM bot_users WHERE owner_id=$1 AND chat_id=$2 AND lower(username)=ANY($3::text[])",
                owner_id, chat_id, usernames,
            )
            username_user_ids = {r["user_id"] for r in resolved}

        all_user_ids = explicit_user_ids | username_user_ids
        if not all_user_ids:
            continue

        try:
            async with Bot(token=token).context() as child_bot:
                for uid in all_user_ids:
                    try:
                        await child_bot.unban_chat_member(chat_id, uid, only_if_banned=True)
                    except Exception as e:
                        logger.debug(f"Unban failed for {uid} in {chat_id}: {e}")
                    await asyncio.sleep(0.05)
        except Exception as e:
            logger.debug(f"Failed to use child_bot unban for chat {chat_id}: {e}")


async def get_blacklist_count(owner_id: int) -> int:
    return await db.fetchval(
        "SELECT COUNT(*) FROM blacklist WHERE owner_id=$1", owner_id
    )


async def get_blocked_count(owner_id: int) -> int:
    """Возвращает общее количество сработавших блокировок ЧС."""
    val = await db.fetchval(
        "SELECT blocked_count FROM platform_users WHERE user_id=$1", owner_id
    )
    return val or 0
