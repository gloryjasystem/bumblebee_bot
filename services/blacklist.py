"""
services/blacklist.py — Весь функционал чёрного списка.
"""
import asyncio
import logging

import db.pool as db
from services.security import parse_blacklist_line

logger = logging.getLogger(__name__)


async def resolve_username_to_id(username: str) -> int | None:
    """
    Пробует резолвить @username → Telegram user_id через Bot API (get_chat).
    Работает для ПУБЛИЧНЫХ аккаунтов.
    Возвращает user_id (int) или None если не удалось.
    """
    from aiogram import Bot
    from config import settings
    try:
        master_bot = Bot(token=settings.bot_token)
        chat = await master_bot.get_chat(f"@{username.lstrip('@')}")
        await master_bot.session.close()
        if chat and chat.id:
            logger.info(f"[BL] Resolved @{username} → user_id={chat.id}")
            return chat.id
    except Exception as e:
        logger.debug(f"[BL] Cannot resolve @{username}: {e}")
    return None


async def check_blacklist(owner_id: int, user_id: int, username: str | None, child_bot_id: int | None = None) -> bool:
    """
    Проверка пользователя по ЧС.
    Если задан child_bot_id — ищет записи этого бота ИЛИ глобальные (child_bot_id IS NULL).
    """
    row = await db.fetchrow(
        """
        SELECT 1 FROM blacklist
        WHERE owner_id = $1
          AND (
            child_bot_id = $4
            OR child_bot_id IS NULL
          )
          AND (
            (user_id IS NOT NULL AND user_id = $2)
            OR
            (username IS NOT NULL AND lower(username) = lower($3))
          )
        LIMIT 1
        """,
        owner_id, user_id, username or "", child_bot_id,
    )
    return row is not None


async def add_to_blacklist(owner_id: int, user_id: int | None, username: str | None, child_bot_id: int | None = None) -> bool:
    """
    Добавляет одну запись в ЧС.
    child_bot_id=None — глобальная запись (global admin), иначе per-bot.
    Возвращает True если добавлена, False если уже была.
    """
    uname_lower = username.lower() if username else None

    # Явная проверка существования — не зависит от индексов
    if child_bot_id is not None:
        if user_id:
            exists = await db.fetchval(
                "SELECT 1 FROM blacklist WHERE owner_id=$1 AND child_bot_id=$2 AND user_id=$3",
                owner_id, child_bot_id, user_id,
            )
        else:
            exists = await db.fetchval(
                "SELECT 1 FROM blacklist WHERE owner_id=$1 AND child_bot_id=$2 AND lower(username)=lower($3)",
                owner_id, child_bot_id, uname_lower,
            )
    else:
        if user_id:
            exists = await db.fetchval(
                "SELECT 1 FROM blacklist WHERE owner_id=$1 AND child_bot_id IS NULL AND user_id=$2",
                owner_id, user_id,
            )
        else:
            exists = await db.fetchval(
                "SELECT 1 FROM blacklist WHERE owner_id=$1 AND child_bot_id IS NULL AND lower(username)=lower($2)",
                owner_id, uname_lower,
            )

    if exists:
        return False  # уже в базе

    result = await db.execute(
        """
        INSERT INTO blacklist (owner_id, user_id, username, child_bot_id)
        VALUES ($1, $2, $3, $4)
        """,
        owner_id, user_id, uname_lower, child_bot_id,
    )
    return result == "INSERT 0 1"


async def _get_chats_with_tokens(owner_id: int, child_bot_id: int | None = None) -> list:
    """
    Возвращает список активных чатов со статусом ЧС и расшифрованными токенами.
    """
    from services.security import decrypt_token
    if child_bot_id:
        rows = await db.fetch(
            """
            SELECT bc.chat_id, cb.token_encrypted, cb.blacklist_enabled
            FROM bot_chats bc
            JOIN child_bots cb ON cb.id = bc.child_bot_id
            WHERE bc.owner_id = $1 AND bc.child_bot_id = $2 AND bc.is_active = true
            """,
            owner_id, child_bot_id,
        )
    else:
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


async def kick_single_user(owner_id: int, user_id: int | None, username: str | None, child_bot_id: int | None = None) -> int:  # noqa
    """
    Кикает конкретного пользователя из всех активных площадок владельца.
    Работает для ВСЕХ участников чата — независимо от того, когда они вступили.

    - Если известен user_id: пробуем банить напрямую в каждом чате.
    - Если только username: резолвим через bot_users, потом баним.

    Возвращает количество чатов, откуда был выкинут пользователь.
    """
    chats = await _get_chats_with_tokens(owner_id, child_bot_id)
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
        if child_bot_id:
            await db.execute(
                "UPDATE child_bots SET blocked_count = blocked_count + $1 WHERE id = $2",
                kicked, child_bot_id,
            )
        logger.info(f"[BL KICK] user={resolved_user_id} kicked from {kicked} chats for owner={owner_id}")

    return kicked


async def import_file(owner_id: int, content: bytes, filename: str, child_bot_id: int | None = None) -> dict:
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
        rows.append((owner_id, parsed["user_id"], parsed["username"], child_bot_id))

    newly_added = []

    if rows:
        # Из-за возможных больших списков делаем это батчами
        for i in range(0, len(rows), 1000):
            batch = rows[i:i+1000]
            existing_uids = set()
            existing_unames = set()
            
            # Собираем всех user_id и username из батча
            batch_uids = [r[1] for r in batch if r[1] is not None]
            batch_unames = [r[2].lower() for r in batch if r[2] is not None]
            
            if batch_uids:
                if child_bot_id:
                    res = await db.fetch("SELECT user_id FROM blacklist WHERE owner_id=$1 AND child_bot_id=$2 AND user_id = ANY($3)", owner_id, child_bot_id, batch_uids)
                else:
                    res = await db.fetch("SELECT user_id FROM blacklist WHERE owner_id=$1 AND child_bot_id IS NULL AND user_id = ANY($2)", owner_id, batch_uids)
                existing_uids.update(r["user_id"] for r in res)
                
            if batch_unames:
                if child_bot_id:
                    res = await db.fetch("SELECT lower(username) as uname FROM blacklist WHERE owner_id=$1 AND child_bot_id=$2 AND lower(username) = ANY($3)", owner_id, child_bot_id, batch_unames)
                else:
                    res = await db.fetch("SELECT lower(username) as uname FROM blacklist WHERE owner_id=$1 AND child_bot_id IS NULL AND lower(username) = ANY($2)", owner_id, batch_unames)
                existing_unames.update(r["uname"] for r in res)
                
            # Фильтруем батч: оставляем только тех, кого нет в базе
            to_insert = []
            for r in batch:
                uid, uname = r[1], r[2]
                if uid and uid in existing_uids:
                    duplicates += 1
                    continue
                if uname and uname.lower() in existing_unames:
                    duplicates += 1
                    continue
                to_insert.append(r)
                newly_added.append({"user_id": uid, "username": uname})
                
                # Добавляем в локальный existing_*, чтобы не дублировать внутри одного файла
                if uid: existing_uids.add(uid)
                if uname: existing_unames.add(uname.lower())

            if to_insert:
                await db.executemany(
                    """
                    INSERT INTO blacklist (owner_id, user_id, username, child_bot_id)
                    VALUES ($1, $2, $3, $4)
                    ON CONFLICT DO NOTHING
                    """,
                    to_insert,
                )
                added += len(to_insert)

    if child_bot_id:
        total = await db.fetchval("SELECT COUNT(*) FROM blacklist WHERE owner_id=$1 AND child_bot_id=$2", owner_id, child_bot_id)
    else:
        total = await db.fetchval("SELECT COUNT(*) FROM blacklist WHERE owner_id=$1 AND child_bot_id IS NULL", owner_id)
        
    return {
        "added": added,
        "duplicates": duplicates,
        "invalid": invalid,
        "total": total or 0,
        "newly_added": newly_added,
    }


async def sweep_after_import(owner_id: int, child_bot_id: int | None = None, newly_added: list[dict] | None = None) -> int:
    """
    После загрузки файла — банит ТОЛЬКО (новых) нарушителей во всех активных площадках.
    Оптимизировано, чтобы не отправлять Telegram API "ban" для тех, кто уже в ЧС.
    """
    chats = await _get_chats_with_tokens(owner_id, child_bot_id)
    if not chats:
        return 0

    bl_records = []
    if newly_added is not None:
        bl_records = newly_added
    else:
        # Резервный механизм, если newly_added не передан
        if child_bot_id:
            bl_records = await db.fetch(
                "SELECT user_id, username FROM blacklist WHERE owner_id = $1 AND child_bot_id = $2",
                owner_id, child_bot_id,
            )
        else:
            bl_records = await db.fetch(
                "SELECT user_id, username FROM blacklist WHERE owner_id = $1 AND child_bot_id IS NULL",
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
        if child_bot_id:
            await db.execute(
                "UPDATE child_bots SET blocked_count = blocked_count + $1 WHERE id = $2",
                total_banned, child_bot_id,
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


async def sweep_unban_records(owner_id: int, records: list, child_bot_id: int | None = None) -> None:
    """
    Фоновая задача разбана после очистки базы ЧС или удаления конкретных пользователей.
    records: список словарей/строк из БД с полями user_id, username.
    """
    from aiogram import Bot

    chats = await _get_chats_with_tokens(owner_id, child_bot_id)
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
                        logger.info(f"[BL UNBAN] Attempting to unban user={uid} from chat={chat_id}")
                        await child_bot.unban_chat_member(chat_id, uid)
                        logger.info(f"[BL UNBAN] Successfully unbanned user={uid} in chat={chat_id}")
                    except Exception as e:
                        logger.error(f"[BL UNBAN ERROR] Unban failed for {uid} in {chat_id}: {e}")
                    await asyncio.sleep(0.05)
        except Exception as e:
            logger.error(f"[BL UNBAN FATAL] Failed to use child_bot unban for chat {chat_id}: {e}")


async def get_blacklist_count(owner_id: int, child_bot_id: int | None = None) -> int:
    if child_bot_id:
        return await db.fetchval(
            "SELECT COUNT(*) FROM blacklist WHERE owner_id=$1 AND child_bot_id=$2",
            owner_id, child_bot_id,
        ) or 0
    return await db.fetchval(
        "SELECT COUNT(*) FROM blacklist WHERE owner_id=$1 AND child_bot_id IS NULL", owner_id
    ) or 0


async def get_blocked_count(owner_id: int) -> int:
    """Возвращает общее количество сработавших блокировок ЧС."""
    val = await db.fetchval(
        "SELECT blocked_count FROM platform_users WHERE user_id=$1", owner_id
    )
    return val or 0
