"""
services/blacklist.py — Весь функционал чёрного списка.
"""
import asyncio
import logging

import db.pool as db
from services.security import parse_blacklist_line

logger = logging.getLogger(__name__)

# Максимальное время ожидания FloodWait в секундах (дальше просто пропускаем запись)
MAX_FLOOD_WAIT = 30


async def resolve_username_to_id(username: str, owner_id: int | None = None, child_bot_id: int | None = None) -> int | None:
    """
    Глобальный механизм резолва @username → Telegram user_id.
    1. Ищет юзернейм во всех таблицах БД (bot_users, platform_users, join_requests, blacklist).
    2. Если не находит — пробует get_chat через master-бота.
    3. Если не находит — пробует get_chat через нужного child_bot.
    """
    uname_clean = username.lower().lstrip("@")
    
    # 1. Супер-поиск по всей базе данных (т.к. юзернеймы могут быть где угодно)
    uid = await db.fetchval(
        """
        SELECT user_id FROM (
            SELECT user_id FROM bot_users WHERE lower(username) = $1 AND user_id IS NOT NULL
            UNION
            SELECT user_id FROM platform_users WHERE lower(username) = $1 AND user_id IS NOT NULL
            UNION
            SELECT user_id FROM blacklist WHERE lower(username) = $1 AND user_id IS NOT NULL
            UNION
            SELECT user_id FROM join_requests WHERE lower(username) = $1 AND user_id IS NOT NULL
        ) as t LIMIT 1
        """,
        uname_clean,
    )
    if uid:
        logger.info(f"[BL RESOLVE] @{uname_clean} → {uid} (found in DB)")
        return uid

    # 2. Пробуем через master-бота
    from aiogram import Bot
    from config import settings
    try:
        master_bot = Bot(token=settings.bot_token)
        chat = await master_bot.get_chat(f"@{uname_clean}")
        if chat and chat.id:
            logger.info(f"[BL RESOLVE] @{uname_clean} → {chat.id} (via master bot)")
            return chat.id
    except Exception as e:
        logger.warning(f"[BL RESOLVE] Master bot failed to resolve @{uname_clean}: {e}")
    finally:
        await master_bot.session.close()

    # 3. Пробуем через get_chat по юзернейму в каналах выбранных ботов
    #    (Telegram Bot API позволяет это только если пользователь когда-то пересекался с ботом)
    bot_ids_to_try = []
    if child_bot_id:
        bot_ids_to_try.append(child_bot_id)
    elif owner_id:
        rows = await db.fetch("SELECT child_bot_id FROM ga_selected_bots WHERE owner_id=$1", owner_id)
        bot_ids_to_try.extend(r['child_bot_id'] for r in rows)

    if bot_ids_to_try:
        from services.security import decrypt_token
        bot_rows = await db.fetch(
            "SELECT cb.id, cb.token_encrypted, bc.chat_id "
            "FROM child_bots cb "
            "JOIN bot_chats bc ON bc.child_bot_id = cb.id "
            "WHERE cb.id = ANY($1::int[]) AND bc.is_active = true",
            bot_ids_to_try
        )
        for bot_row in bot_rows:
            try:
                async with Bot(token=decrypt_token(bot_row["token_encrypted"])).context() as child_bot:
                    chat = await child_bot.get_chat(f"@{uname_clean}")
                    if chat and chat.id:
                        uid = chat.id
                        logger.info(f"[BL RESOLVE] @{uname_clean} → {uid} (via getChat in {bot_row['chat_id']})")
                        return uid
            except Exception as e:
                logger.warning(f"[BL RESOLVE] Child bot {bot_row['id']} failed to resolve @{uname_clean}: {e}")
                continue

    logger.debug(f"[BL RESOLVE] Cannot resolve @{uname_clean}")
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

    # Самовосстанавливающаяся база (Strict ID Resolution Policy)
    # Если спамер был добавлен только по username (например, через CSV) и мы его поймали,
    # мы гарантированно забираем его точный Телеграм ID навсегда:
    if row is not None and username:
        import asyncio
        async def _heal_blacklist_record():
            import asyncpg
            try:
                await db.execute(
                    """
                    UPDATE blacklist 
                    SET user_id = $2
                    WHERE owner_id = $1 
                      AND lower(username) = lower($3) 
                      AND user_id IS NULL
                    """,
                    owner_id, user_id, username
                )
            except asyncpg.UniqueViolationError:
                # Если такой user_id уже есть в базе (кто-то добавил его по ID),
                # то UPDATE упадёт с ошибкой. В этом случае "безымянную" запись можно просто удалить.
                await db.execute(
                    """
                    DELETE FROM blacklist
                    WHERE owner_id = $1 
                      AND lower(username) = lower($2) 
                      AND user_id IS NULL
                    """,
                    owner_id, username
                )
        asyncio.create_task(_heal_blacklist_record())

    return row is not None


async def add_to_blacklist(owner_id: int, user_id: int | None, username: str | None, child_bot_id: int | None = None) -> bool:
    """
    Добавляет одну запись в ЧС.
    child_bot_id=None — глобальная запись (global admin), иначе per-bot.
    Возвращает True если добавлена, False если уже была.
    """
    uname_lower = username.lower() if username else None

    # Step 1: Пытаемся вылечить пустой ID, если пришел и ID, и username
    if user_id and uname_lower:
        if child_bot_id is not None:
            updated = await db.execute(
                "UPDATE blacklist SET user_id=$1 WHERE owner_id=$2 AND child_bot_id=$3 AND lower(username)=$4 AND user_id IS NULL",
                user_id, owner_id, child_bot_id, uname_lower
            )
        else:
            updated = await db.execute(
                "UPDATE blacklist SET user_id=$1 WHERE owner_id=$2 AND child_bot_id IS NULL AND lower(username)=$3 AND user_id IS NULL",
                user_id, owner_id, uname_lower
            )
        # Если обновили — значит запись там уже была пустой, теперь мы её починили
        if updated == "UPDATE 1":
            return True

    # Step 2: Проверяем, существует ли уже такая запись (по ID или Юзернейму)
    if child_bot_id is not None:
        if user_id:
            exists = await db.fetchval("SELECT 1 FROM blacklist WHERE owner_id=$1 AND child_bot_id=$2 AND user_id=$3", owner_id, child_bot_id, user_id)
        else:
            exists = await db.fetchval("SELECT 1 FROM blacklist WHERE owner_id=$1 AND child_bot_id=$2 AND lower(username)=$3", owner_id, child_bot_id, uname_lower)
    else:
        if user_id:
            exists = await db.fetchval("SELECT 1 FROM blacklist WHERE owner_id=$1 AND child_bot_id IS NULL AND user_id=$2", owner_id, user_id)
        else:
            exists = await db.fetchval("SELECT 1 FROM blacklist WHERE owner_id=$1 AND child_bot_id IS NULL AND lower(username)=$3", owner_id, uname_lower)

    if exists:
        return False  # уже в базе полностью

    # Step 3: Вставляем
    import asyncpg
    try:
        result = await db.execute(
            """
            INSERT INTO blacklist (owner_id, user_id, username, child_bot_id)
            VALUES ($1, $2, $3, $4)
            """,
            owner_id, user_id, uname_lower, child_bot_id,
        )
        return result == "INSERT 0 1"
    except asyncpg.UniqueViolationError:
        return False


async def _get_chats_with_tokens(owner_id: int, child_bot_id: int | None = None) -> list:
    """
    Возвращает список активных чатов со статусом ЧС и расшифрованными токенами.
    Глобальный уровень (child_bot_id=None) берёт чаты ТОЛЬКО у ботов из ga_selected_bots.
    """
    from services.security import decrypt_token
    if child_bot_id:
        rows = await db.fetch(
            """
            SELECT bc.chat_id, cb.token_encrypted, cb.blacklist_enabled, cb.id as bot_id
            FROM bot_chats bc
            JOIN child_bots cb ON cb.id = bc.child_bot_id
            WHERE bc.owner_id = $1 AND bc.child_bot_id = $2 AND bc.is_active = true
            """,
            owner_id, child_bot_id,
        )
    else:
        rows = await db.fetch(
            """
            SELECT bc.chat_id, cb.token_encrypted, cb.blacklist_enabled, cb.id as bot_id
            FROM bot_chats bc
            JOIN child_bots cb ON cb.id = bc.child_bot_id
            JOIN ga_selected_bots gsb ON gsb.child_bot_id = cb.id AND gsb.owner_id = $1
            WHERE bc.is_active = true
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
                "child_bot_id": r["bot_id"],
            })
    return result


async def _ban_user_in_chat(token: str, chat_id: int, user_id: int) -> bool:
    """
    Пытается забанить user_id в chat_id через дочернего бота.
    Возвращает True если бан успешно применён (пользователь был в чате).
    """
    from aiogram import Bot
    from aiogram.exceptions import TelegramRetryAfter
    try:
        async with Bot(token=token).context() as child_bot:
            await child_bot.ban_chat_member(chat_id, user_id)
            logger.info(f"[BL KICK] Banned user={user_id} from chat={chat_id}")
            return True
    except TelegramRetryAfter as e:
        # Защита от FloodWait: если Telegram просит подождать и это разумно — ждём;
        # если ждать слишком долго — пропускаем запись без бана и продолжаем
        wait = e.retry_after
        if wait <= MAX_FLOOD_WAIT:
            logger.info(f"[BL KICK] FloodWait {wait}s for chat={chat_id}, waiting...")
            await asyncio.sleep(wait)
            try:
                async with Bot(token=token).context() as child_bot:
                    await child_bot.ban_chat_member(chat_id, user_id)
                    return True
            except Exception:
                pass
        else:
            logger.warning(f"[BL KICK] FloodWait too long ({wait}s) for chat={chat_id}, skipping ban for user={user_id}")
        return False
    except Exception as e:
        err = str(e).lower()
        # Защита 3: если пользователя нет в чате — бот его не видит, молча пропускаем
        if "user not found" in err or "user_not_participant" in err or "member_not_found" in err:
            return False
        # Любая другая ошибка (например, нет прав)
        logger.warning(f"[BL KICK] ban FAILED user={user_id} chat={chat_id}: {e}")
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

    # Резолвим user_id через наш новый мощный resolve_username_to_id
    resolved_user_id = user_id
    if not resolved_user_id and username:
        resolved_user_id = await resolve_username_to_id(username, owner_id=owner_id, child_bot_id=child_bot_id)

    if not resolved_user_id:
        # Защита 3: если бот не видит юзера — нельзя его кикнуть.
        # Запись остается в БД — бот забанит его, как только тот появится в чате.
        # Без warnings в логе (это нормальная ситуация для непубличных пользователей).
        logger.debug(f"[BL KICK] username={username} not seen by bot yet, record saved for deferred ban, skip kick")
        return 0

    for chat in chats:
        success = await _ban_user_in_chat(chat["token"], chat["chat_id"], resolved_user_id)
        
        # В ЛЮБОМ СЛУЧАЕ помечаем пользователя как неактивного в нашей БД.
        # Даже если бан через Telegram не удался (например, юзер уже сам вышел, 
        # или у бота временно нет прав) — по факту он в ЧС и не должен числиться активным.
        await db.execute(
            "UPDATE bot_users SET is_active=false, left_at=now() "
            "WHERE owner_id=$1 AND chat_id=$2 AND user_id=$3",
            owner_id, chat["chat_id"], resolved_user_id,
        )

        if success:
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

    import csv as _csv
    import io as _io

    is_csv = filename.lower().endswith(".csv")
    rows = []
    added = duplicates = invalid = 0

    if is_csv:
        reader = _csv.reader(_io.StringIO(text))
        lines_iter = list(reader)
    else:
        lines_iter = [[line] for line in text.splitlines()]

    for line_no, cells in enumerate(lines_iter, start=1):
        try:
            # Skip header rows (if first cell looks like a column name)
            if line_no == 1 and cells and not cells[0].strip().lstrip("@").isdigit():
                first = cells[0].strip().lower()
                if first in ("user_id", "id", "username", "user", "#"):
                    continue

            parsed = None
            for cell in cells:
                cell = cell.strip()
                if not cell:
                    continue
                result = parse_blacklist_line(cell)
                if result is not None:
                    parsed = result
                    break  # first valid token in the row wins

            if parsed is None:
                # Count as invalid only if the row has non-empty content
                if any(c.strip() for c in cells):
                    invalid += 1
                continue

            rows.append((owner_id, parsed["user_id"], parsed["username"], child_bot_id))
        except Exception as e:
            logger.error(f"[BL IMPORT] Parse error at line {line_no}: {e!r} | raw: {str(cells)[:80]!r}")
            invalid += 1
            continue


    newly_added = []

    if rows:
        from services.blacklist import get_total_blacklist_count
        pu = await db.fetchrow("SELECT tariff FROM platform_users WHERE user_id=$1", owner_id)
        tariff = pu["tariff"] if pu else "free"
        from config import TARIFFS
        limit = TARIFFS.get(tariff, TARIFFS["free"])["max_blacklist_users"]
        
        total_curr = await get_total_blacklist_count(owner_id)

        for i in range(0, len(rows), 1000):
            if total_curr >= limit:
                break
                
            # If batch would exceed limit, truncate it
            batch_size = min(1000, limit - total_curr)
            batch = rows[i:i+batch_size]

            existing_uids = set()
            existing_unames = set()
            
            # Step 1: Deep Resolve Missing IDs
            # Находим никнеймы, для которых нет ID 
            unames_to_resolve = [r[2].lower() for r in batch if r[1] is None and r[2] is not None]
            resolved_map = {}
            if unames_to_resolve:
                # Массовый резолв по всей базе
                res_rows = await db.fetch(
                    """
                    SELECT DISTINCT user_id, lower(username) as uname FROM (
                        SELECT user_id, username FROM bot_users WHERE lower(username) = ANY($1::text[]) AND user_id IS NOT NULL
                        UNION
                        SELECT user_id, username FROM platform_users WHERE lower(username) = ANY($1::text[]) AND user_id IS NOT NULL
                        UNION
                        SELECT user_id, username FROM blacklist WHERE lower(username) = ANY($1::text[]) AND user_id IS NOT NULL
                        UNION
                        SELECT user_id, username FROM join_requests WHERE lower(username) = ANY($1::text[]) AND user_id IS NOT NULL
                    ) t
                    """,
                    unames_to_resolve
                )
                for rr in res_rows:
                    resolved_map[rr["uname"]] = rr["user_id"]

            # Формируем итоговый батч с подставленными ID
            enriched_batch = []
            for r in batch:
                uid, uname = r[1], r[2]
                if not uid and uname and uname.lower() in resolved_map:
                    uid = resolved_map[uname.lower()]
                enriched_batch.append((owner_id, uid, uname, child_bot_id))

            # Step 2: Обновляем существующие записи (Лечим NULL user_ids)
            # Если юзернейм уже был в ЧС с пустым ID, мы его заполним, не удаляя запись!
            update_tuples = [(r[1], r[2].lower()) for r in enriched_batch if r[1] is not None and r[2] is not None]
            if update_tuples:
                if child_bot_id:
                    upd_query = """
                    UPDATE blacklist AS b
                    SET user_id = v.user_id::bigint
                    FROM (SELECT * FROM UNNEST($1::bigint[], $2::text[])) AS v(user_id, username)
                    WHERE b.owner_id = $3 AND b.child_bot_id = $4
                      AND lower(b.username) = v.username AND b.user_id IS NULL
                    """
                    await db.execute(upd_query, [t[0] for t in update_tuples], [t[1] for t in update_tuples], owner_id, child_bot_id)
                else:
                    upd_query = """
                    UPDATE blacklist AS b
                    SET user_id = v.user_id::bigint
                    FROM (SELECT * FROM UNNEST($1::bigint[], $2::text[])) AS v(user_id, username)
                    WHERE b.owner_id = $3 AND b.child_bot_id IS NULL
                      AND lower(b.username) = v.username AND b.user_id IS NULL
                    """
                    await db.execute(upd_query, [t[0] for t in update_tuples], [t[1] for t in update_tuples], owner_id)
            
            # Step 3: Собираем existing, чтобы отсеять полные дубликаты перед INSERT
            batch_uids = [r[1] for r in enriched_batch if r[1] is not None]
            batch_unames = [r[2].lower() for r in enriched_batch if r[2] is not None]
            
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
                
            # Фильтруем батч: добавляем только тех, кого СОВСЕМ нет в базе (ни по ID, ни по юзернейму)
            # Фильтруем батч: добавляем только тех, кого СОВСЕМ нет в базе (ни по ID, ни по юзернейму)
            to_insert = []
            for r in enriched_batch:
                uid, uname = r[1], r[2]
                
                # Если у нас есть ID, и в базе уже есть запись с таким ID - пропускаем
                if uid and uid in existing_uids:
                    duplicates += 1
                    # Мы всё равно передаем его для кика (с пушнутым ID)
                    newly_added.append({"user_id": uid, "username": uname})
                    continue
                    
                # Если у нас НЕТ ID (только юзернейм), и в базе есть запись с таким юзернеймом - пропускаем
                if uname and uname.lower() in existing_unames and not uid:
                    duplicates += 1
                    newly_added.append({"user_id": uid, "username": uname})
                    continue
                    
                to_insert.append(r)
                newly_added.append({"user_id": uid, "username": uname})  # С точным Resolved UID!
            
            # Применяем лимит к to_insert
            if total_curr + len(to_insert) > limit:
                # Обрезаем те элементы, которые не влезают в лимит
                allowed = limit - total_curr
                to_remove = to_insert[allowed:]
                to_insert = to_insert[:allowed]
                # Убираем их из newly_added, чтобы не кикать
                for r in to_remove:
                    r_uid, r_uname = r[1], r[2]
                    try:
                        newly_added.remove({"user_id": r_uid, "username": r_uname})
                    except ValueError:
                        pass
                        
            if to_insert:
                added += len(to_insert)
                total_curr += len(to_insert)
                
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

    # 1. Резолвим username → user_id через глобальную БД (массовый быстрый запрос)
    resolved_ids_from_usernames = set()
    if usernames_only:
        rows = await db.fetch(
            """
            SELECT DISTINCT user_id FROM (
                SELECT user_id FROM bot_users WHERE lower(username) = ANY($1::text[]) AND user_id IS NOT NULL
                UNION
                SELECT user_id FROM platform_users WHERE lower(username) = ANY($1::text[]) AND user_id IS NOT NULL
                UNION
                SELECT user_id FROM blacklist WHERE lower(username) = ANY($1::text[]) AND user_id IS NOT NULL
                UNION
                SELECT user_id FROM join_requests WHERE lower(username) = ANY($1::text[]) AND user_id IS NOT NULL
            ) t
            """,
            usernames_only,
        )
        resolved_ids_from_usernames = {r["user_id"] for r in rows}

        # Вычисляем, какие usernames мы НЕ нашли в БД массовым способом
        # Чтобы не дергать API на каждого уже найденного
        if len(resolved_ids_from_usernames) < len(usernames_only):
            # Найдем, кого именно мы не обнаружили
            found_users = await db.fetch(
                """
                SELECT lower(username) as un FROM (
                    SELECT username FROM bot_users WHERE user_id = ANY($1::bigint[]) AND username IS NOT NULL
                    UNION
                    SELECT username FROM blacklist WHERE user_id = ANY($1::bigint[]) AND username IS NOT NULL
                ) t
                """,
                list(resolved_ids_from_usernames)
            )
            found_unames_set = {r["un"] for r in found_users}
            
            # 2. Оставшиеся резолвим по одному через мощную функцию (Telegram API)
            for uname in usernames_only:
                if uname not in found_unames_set:
                    api_uid = await resolve_username_to_id(uname, owner_id=owner_id, child_bot_id=child_bot_id)
                    if api_uid:
                        resolved_ids_from_usernames.add(api_uid)

    all_user_ids = list(set(explicit_ids) | resolved_ids_from_usernames)
    if not all_user_ids:
        return 0

    total_banned = 0

    for chat in chats:
        chat_id = chat["chat_id"]
        token = chat["token"]

        for uid in all_user_ids:
            success = await _ban_user_in_chat(token, chat_id, uid)
            
            # В ЛЮБОМ СЛУЧАЕ помечаем пользователя как неактивного в нашей БД.
            # Даже если бан через Telegram не удался — этот юзер в ЧС.
            await db.execute(
                "UPDATE bot_users SET is_active=false, left_at=now() "
                "WHERE owner_id=$1 AND chat_id=$2 AND user_id=$3",
                owner_id, chat_id, uid,
            )

            if success:
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


# ── LAZY PASS: mass-unban functions removed ─────────────────────────────────
# sweep_unban_after_disable и sweep_unban_records удалены в рамках рефакторинга
# архитектуры на паттерн "Lazy Pass".
#
# Причина: массовый разбан с тысячами unban_chat_member вызывает:
#   1. Telegram FloodWait / бан токена по Rate Limits
#   2. Блокировку Event Loop на часы
#   3. Утечку памяти через лавину asyncio.Task
#
# Новая логика:
#   - Тумблеры ЧС (вкл/выкл) просто меняют флаг в БД — ничего не отправляют в API.
#   - Фильтр на входе (join_requests, member_update) проверяет флаг ПЕРЕД проверкой ЧС.
#   - Если ЧС выключен — пользователь проходит (Lazy Pass). Старые баны не трогаем.
#   - Если ЧС включён — новый входящий проверяется по таблице blacklist как обычно.

async def sweep_unban_after_disable(owner_id: int) -> int:  # noqa: dead-code
    """
    DEPRECATED (Lazy Pass refactoring). Функция оставлена как заглушка для обратной совместимости.
    Не выполняет никаких действий — возвращает 0 немедленно.
    """
    logger.debug("[BL] sweep_unban_after_disable called but is a no-op (Lazy Pass). owner=%d", owner_id)
    return 0


async def sweep_unban_records(owner_id: int, records: list, child_bot_id: int | None = None) -> None:  # noqa: dead-code
    """
    DEPRECATED (Lazy Pass refactoring). Функция оставлена как заглушка для обратной совместимости.
    Не выполняет никаких действий.
    """
    logger.debug(
        "[BL] sweep_unban_records called but is a no-op (Lazy Pass). owner=%d records=%d",
        owner_id, len(records) if records else 0,
    )


async def get_total_blacklist_count(owner_id: int) -> int:
    return await db.fetchval("SELECT COUNT(*) FROM blacklist WHERE owner_id=$1", owner_id) or 0


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
