"""
handlers/join_requests.py — Обработка ChatJoinRequest и ChatMemberUpdated.
Это центральный хендлер защиты каналов.
"""
import asyncio
import logging
from aiogram import Router, Bot
from aiogram.types import ChatJoinRequest, ChatMemberUpdated

import db.pool as db
from services.blacklist import check_blacklist
from services.security import detect_rtl, detect_hieroglyph

logger = logging.getLogger(__name__)
router = Router()


async def _get_owner(chat_id: int) -> dict | None:
    """Returns platform settings, owner_id, blacklist state, and whether this bot is in the global BL scope."""
    from config import settings as _cfg
    return await db.fetchrow(
        """
        SELECT bc.*, cb.blacklist_enabled,
               COALESCE(pu.blacklist_active, true) AS blacklist_active,
               EXISTS(
                   SELECT 1 FROM ga_selected_bots gsb
                   WHERE gsb.owner_id = $2
                     AND gsb.child_bot_id = bc.child_bot_id
               ) AS in_global_bl_scope
        FROM bot_chats bc
        JOIN child_bots cb ON bc.child_bot_id = cb.id
        LEFT JOIN platform_users pu ON pu.user_id = bc.owner_id
        WHERE bc.chat_id=$1 AND bc.is_active=true
        """,
        chat_id, _cfg.owner_telegram_id,
    )


async def _log_action(owner_id: int, chat_id: int, action: str, target_id: int, details: dict = None):
    """Пишет в audit_log (только для Про+ тарифов)."""
    puser = await db.fetchrow(
        "SELECT tariff FROM platform_users WHERE user_id=$1", owner_id
    )
    if puser and puser["tariff"] in ("pro", "business"):
        import json
        await db.execute(
            "INSERT INTO audit_log (owner_id, chat_id, action, target_id, details) "
            "VALUES ($1, $2, $3, $4, $5)",
            owner_id, chat_id, action, target_id,
            json.dumps(details or {}),
        )


# ── ЗАКРЫТЫЙ КАНАЛ: ChatJoinRequest ───────────────────────────
@router.chat_join_request()
async def on_join_request(event: ChatJoinRequest, bot: Bot):
    settings_row = await _get_owner(event.chat.id)
    if not settings_row:
        return

    owner_id = settings_row["owner_id"]
    user = event.from_user

    # 1. Проверка ЧС:
    #    a) Локальный ЧС бота (если включён у бота)
    #    b) Глобальный ЧС платформы (только если бот входит в выбранную выборку)
    if settings_row.get("blacklist_enabled", True):
        _cbi = settings_row.get("child_bot_id")
        
        is_global_block = False
        is_local_block = False
        
        if settings_row.get("blacklist_active", True) and settings_row.get("in_global_bl_scope", False):
            from config import settings as _cfg
            if await check_blacklist(_cfg.owner_telegram_id, user.id, user.username, child_bot_id=None):
                is_global_block = True
                
        if await check_blacklist(owner_id, user.id, user.username, child_bot_id=_cbi):
            is_local_block = True
            
        if is_global_block or is_local_block:
            await event.decline()
            try:
                await bot.ban_chat_member(event.chat.id, user.id)
            except Exception:
                pass
            # Счётчик заблокированных
            await db.execute(
                "UPDATE platform_users SET blocked_count = blocked_count + 1 WHERE user_id = $1",
                owner_id,
            )
            # Per-bot счётчик ("Заблокировано ботом")
            child_bot_id = settings_row.get("child_bot_id")
            if child_bot_id:
                if is_global_block:
                    await db.execute(
                        "UPDATE child_bots SET blocked_count = blocked_count + 1, global_blocked_count = global_blocked_count + 1 WHERE id = $1",
                        child_bot_id,
                    )
                else:
                    await db.execute(
                        "UPDATE child_bots SET blocked_count = blocked_count + 1 WHERE id = $1",
                        child_bot_id,
                    )
            await _log_action(owner_id, event.chat.id, "reject_bl", user.id)
            logger.info(f"[BL] Rejected {user.id} from {event.chat.id} (Global: {is_global_block}, Local: {is_local_block})")
            return

    # 2. Языковой фильтр
    # Нормализуем language_code: 'ru-RU' → 'ru', 'en-US' → 'en'
    user_lang = (user.language_code or "").split("-")[0].lower()
    if user_lang:
        lang_blocked = await db.fetchrow(
            "SELECT 1 FROM language_filters WHERE owner_id=$1 AND chat_id=$2 AND language_code=$3",
            owner_id, event.chat.id, user_lang,
        )
        if lang_blocked:
            await event.decline()
            await _log_action(owner_id, event.chat.id, "reject_lang", user.id,
                              {"lang": user_lang})
            return

    # 3. RTL-фильтр
    full_name = f"{user.first_name or ''} {user.last_name or ''}".strip()
    if settings_row["filter_rtl"] and detect_rtl(full_name):
        await event.decline()
        await _log_action(owner_id, event.chat.id, "reject_rtl", user.id)
        return

    # 4. Иероглифы
    if settings_row["filter_hieroglyph"] and detect_hieroglyph(full_name):
        await event.decline()
        await _log_action(owner_id, event.chat.id, "reject_hieroglyph", user.id)
        return

    # 5. Аккаунты без фото
    if settings_row.get("filter_no_photo"):
        try:
            photos = await bot.get_user_profile_photos(user.id, limit=1)
            if photos.total_count == 0:
                await event.decline()
                await _log_action(owner_id, event.chat.id, "reject_no_photo", user.id)
                return
        except Exception as e:
            logger.warning(f"[FILTER] get_user_profile_photos failed for {user.id}: {e}")

    # 6. Проверяем auto_accept конкретной ссылки (приоритет над настройками бота)
    raw_invite_url = event.invite_link.invite_link if event.invite_link else None
    link_auto_accept = None
    if raw_invite_url:
        link_row = await db.fetchrow(
            "SELECT auto_accept FROM invite_links WHERE link=$1",
            raw_invite_url,
        )
        if link_row:
            link_auto_accept = link_row["auto_accept"]  # "base" | "on" | "off"

    if link_auto_accept == "on":
        # Ссылка: автопринятие включено — принимаем немедленно
        await event.approve()
        await _save_pending(owner_id, event.chat.id, user)
        await db.execute(
            "UPDATE join_requests SET status='approved', resolved_at=now() "
            "WHERE owner_id=$1 AND chat_id=$2 AND user_id=$3",
            owner_id, event.chat.id, user.id,
        )
        await _register_user(owner_id, event.chat.id, user,
                             invite_link=event.invite_link)
        await _send_welcome(bot, event.chat.id, user, settings_row)
        await _log_action(owner_id, event.chat.id, "approve", user.id)
        logger.info(f"[LINK AUTO=on] Approved {user.id} via link {raw_invite_url}")
        return

    if link_auto_accept == "off":
        # Ссылка: автопринятие выключено — только ручная проверка администратором
        await _save_pending(owner_id, event.chat.id, user)
        logger.info(f"[LINK AUTO=off] Saved {user.id} for manual review, link={raw_invite_url}")
        return

    # link_auto_accept == "base" или ссылка неизвестна → стандартная логика бота
    # 7. Автопринятие / капча / отложенное / ручное
    if settings_row["autoaccept"]:
        delay = settings_row["autoaccept_delay"] or 0
        if delay > 0:
            # Сохраняем заявку и обрабатываем в фоне через asyncio.sleep
            await _save_pending(owner_id, event.chat.id, user)
            asyncio.create_task(_delayed_approve(event, owner_id, delay))
        else:
            await event.approve()
            await _save_pending(owner_id, event.chat.id, user)  # создаём запись если нет
            await db.execute(
                "UPDATE join_requests SET status='approved', resolved_at=now() "
                "WHERE owner_id=$1 AND chat_id=$2 AND user_id=$3",
                owner_id, event.chat.id, user.id,
            )
            await _register_user(owner_id, event.chat.id, user)
            await _send_welcome(bot, event.chat.id, user, settings_row)
            await _log_action(owner_id, event.chat.id, "approve", user.id)
    elif (settings_row.get("captcha_type") or "off") != "off":
        await _save_pending(owner_id, event.chat.id, user)
        from handlers.captcha import send_captcha
        await send_captcha(bot, event, dict(settings_row))
    else:
        # Ручной режим — сохраняем в очередь для ревью владельцем
        await _save_pending(owner_id, event.chat.id, user)
        logger.info(f"[JOIN] Request from {user.id} saved for manual review in {event.chat.id}")




# ── ОТКРЫТЫЙ КАНАЛ: ChatMemberUpdated ─────────────────────────
@router.chat_member()
async def on_member_update(event: ChatMemberUpdated, bot: Bot):
    new_status = event.new_chat_member.status
    old_status = event.old_chat_member.status if event.old_chat_member else None

    settings_row = await _get_owner(event.chat.id)
    if not settings_row:
        return

    owner_id = settings_row["owner_id"]
    user = event.new_chat_member.user

    # Пользователь вступил
    if new_status == "member" and old_status in (None, "left", "kicked"):
        # Если юзер уже зашел (по ссылке или админ добавил), аннулируем заявку в очереди
        await db.execute(
            "UPDATE join_requests SET status='expired', resolved_at=now() "
            "WHERE owner_id=$1 AND chat_id=$2::bigint AND user_id=$3 AND status='pending'",
            owner_id, event.chat.id, user.id,
        )

        if settings_row.get("blacklist_enabled", True):
            _cbi = settings_row.get("child_bot_id")
            
            is_global_block = False
            is_local_block = False
            
            if settings_row.get("blacklist_active", True) and settings_row.get("in_global_bl_scope", False):
                from config import settings as _cfg
                if await check_blacklist(_cfg.owner_telegram_id, user.id, user.username, child_bot_id=None):
                    is_global_block = True
                    
            if await check_blacklist(owner_id, user.id, user.username, child_bot_id=_cbi):
                is_local_block = True
                
            if is_global_block or is_local_block:
                try:
                    await bot.ban_chat_member(event.chat.id, user.id)
                except Exception:
                    pass
                # Счётчик заблокированных
                await db.execute(
                    "UPDATE platform_users SET blocked_count = blocked_count + 1 WHERE user_id = $1",
                    owner_id,
                )
                # Per-bot счётчик ("Заблокировано ботом")
                child_bot_id = settings_row.get("child_bot_id")
                if child_bot_id:
                    if is_global_block:
                        await db.execute(
                            "UPDATE child_bots SET blocked_count = blocked_count + 1, global_blocked_count = global_blocked_count + 1 WHERE id = $1",
                            child_bot_id,
                        )
                    else:
                        await db.execute(
                            "UPDATE child_bots SET blocked_count = blocked_count + 1 WHERE id = $1",
                            child_bot_id,
                        )
                await _log_action(owner_id, event.chat.id, "ban_on_join", user.id)
                logger.info(f"[BL] Banned {user.id} on join to {event.chat.id} (Global: {is_global_block}, Local: {is_local_block})")
                return

        await _register_user(owner_id, event.chat.id, user,
                             invite_link=event.invite_link)

        # Для обычных ссылок с auto_accept='off' — кикаем сразу после вступления
        raw_invite_url = event.invite_link.invite_link if event.invite_link else None
        if raw_invite_url:
            link_aa_row = await db.fetchrow(
                "SELECT auto_accept FROM invite_links WHERE link=$1",
                raw_invite_url,
            )
            link_aa = (link_aa_row["auto_accept"] if link_aa_row else None) or "base"
            if link_aa == "off":
                try:
                    await bot.ban_chat_member(event.chat.id, user.id)
                    await bot.unban_chat_member(event.chat.id, user.id, only_if_banned=True)
                except Exception as _e:
                    logger.warning(f"[AUTO_OFF] kick failed for user={user.id}: {_e}")
                logger.info(f"[AUTO_OFF] Kicked user={user.id} — link auto_accept=off")
                return

        await _send_welcome(bot, event.chat.id, user, settings_row)

    # Пользователь ушёл
    elif new_status in ("left", "kicked") and old_status == "member":
        # Если юзер отписался, аннулируем его забытую заявку
        await db.execute(
            "UPDATE join_requests SET status='expired', resolved_at=now() "
            "WHERE owner_id=$1 AND chat_id=$2::bigint AND user_id=$3 AND status='pending'",
            owner_id, event.chat.id, user.id,
        )
        await db.execute(
            "UPDATE bot_users SET is_active=false, left_at=now() "
            "WHERE owner_id=$1 AND chat_id=$2 AND user_id=$3",
            owner_id, event.chat.id, user.id,
        )
        # Обновляем счётчик отписок для ссылки (only if joined_counted=true — дедупликация)
        await db.execute(
            """
            UPDATE invite_links SET unsubscribed = unsubscribed + 1
            WHERE id = (
                SELECT ilm.link_id FROM invite_link_members ilm
                JOIN bot_users bu ON bu.joined_via_link_id = ilm.link_id
                WHERE bu.owner_id=$1 AND bu.chat_id=$2 AND bu.user_id=$3
                  AND ilm.user_id=$3 AND ilm.joined_counted = true
                LIMIT 1
            )
            """,
            owner_id, event.chat.id, user.id,
        )
        # Прощание — отправляем если пользователь когда-либо запускал бота
        await _send_farewell(bot, event.chat.id, user, settings_row)


async def _save_pending(owner_id: int, chat_id: int, user):
    """Сохраняет заявку в join_requests со статусом pending."""
    await db.execute(
        """
        INSERT INTO join_requests (owner_id, chat_id, user_id, username, first_name)
        VALUES ($1, $2, $3, $4, $5)
        ON CONFLICT (owner_id, chat_id, user_id)
        DO UPDATE SET status='pending', requested_at=now(), resolved_at=NULL
        """,
        owner_id, chat_id, user.id, user.username,
        user.first_name or "",
    )


async def _delayed_approve(event: ChatJoinRequest, owner_id: int, delay_minutes: int):
    """Принимает заявку через delay_minutes минут (в фоне)."""
    await asyncio.sleep(delay_minutes * 60)
    try:
        await event.approve()
        await db.execute(
            "UPDATE join_requests SET status='approved', resolved_at=now() "
            "WHERE owner_id=$1 AND chat_id=$2 AND user_id=$3",
            owner_id, event.chat.id, event.from_user.id,
        )
        settings_row = await _get_owner(event.chat.id)
        if settings_row:
            await _register_user(owner_id, event.chat.id, event.from_user)
    except Exception as e:
        logger.debug(f"[DELAYED] approve failed for {event.from_user.id}: {e}")
        await db.execute(
            "UPDATE join_requests SET status='expired', resolved_at=now() "
            "WHERE owner_id=$1 AND chat_id=$2 AND user_id=$3",
            owner_id, event.chat.id, event.from_user.id,
        )


# ── Вспомогательные функции ───────────────────────────────────
async def _register_user(owner_id: int, chat_id: int, user,
                          invite_link=None):
    """
    Сохраняет пользователя в bot_users и обновляет детальную статистику
    ссылки-приглашения в invite_links.
    """
    from services.security import detect_rtl, detect_hieroglyph
    import json

    full_name = f"{user.first_name or ''} {user.last_name or ''}".strip()
    has_rtl       = detect_rtl(full_name)
    has_hieroglyph = detect_hieroglyph(full_name)
    is_premium    = bool(getattr(user, "is_premium", False))

    # Страна по language_code (приблизительно)
    LANG_TO_COUNTRY = {
        "ru": "RU", "uk": "UA", "be": "BY", "kk": "KZ",
        "en": "US", "de": "DE", "fr": "FR", "es": "ES",
        "it": "IT", "pt": "BR", "zh": "CN", "ar": "AR",
        "tr": "TR", "pl": "PL", "nl": "NL", "sv": "SE",
        "da": "DK", "fi": "FI", "no": "NO", "cs": "CZ",
        "ro": "RO", "hu": "HU", "bg": "BG", "hr": "HR",
        "sk": "SK", "sl": "SL", "lt": "LT", "lv": "LV",
        "et": "EE", "fa": "IR", "he": "IL", "hi": "IN",
        "id": "ID", "ms": "MY", "th": "TH", "vi": "VN",
        "ko": "KR", "ja": "JP",
    }
    country_code = LANG_TO_COUNTRY.get(
        (user.language_code or "").split("-")[0].lower()
    )

    link_id = None
    if invite_link:
        row = await db.fetchrow(
            "SELECT id FROM invite_links WHERE link=$1",
            invite_link.invite_link,
        )
        if row:
            link_id = row["id"]

            # Проверяем дедупликацию
            already = await db.fetchrow(
                "SELECT joined_counted FROM invite_link_members WHERE link_id=$1 AND user_id=$2",
                link_id, user.id,
            )
            if not (already and already["joined_counted"]):
                from services.gender import guess_gender
                gender = guess_gender(user.first_name or "")
                males_inc   = 1 if gender == "M" else 0
                females_inc = 1 if gender == "F" else 0

                # Обновляем базовый счётчик + детальную статистику
                if country_code:
                    await db.execute(
                        """
                        UPDATE invite_links SET
                            joined      = joined + 1,
                            males       = males + $2::int,
                            females     = females + $3::int,
                            rtl_count   = rtl_count + $4::int,
                            hieroglyph_count = hieroglyph_count + $5::int,
                            premium_count    = premium_count + $6::int,
                            countries   = jsonb_set(
                                COALESCE(countries, '{}'),
                                ARRAY[$7],
                                (COALESCE(countries->$7, '0')::int + 1)::text::jsonb
                            )
                        WHERE id = $1
                        """,
                        link_id,
                        males_inc, females_inc,
                        int(has_rtl), int(has_hieroglyph), int(is_premium),
                        country_code,
                    )
                else:
                    await db.execute(
                        """
                        UPDATE invite_links SET
                            joined      = joined + 1,
                            males       = males + $2::int,
                            females     = females + $3::int,
                            rtl_count   = rtl_count + $4::int,
                            hieroglyph_count = hieroglyph_count + $5::int,
                            premium_count    = premium_count + $6::int
                        WHERE id = $1
                        """,
                        link_id,
                        males_inc, females_inc,
                        int(has_rtl), int(has_hieroglyph), int(is_premium),
                    )

                await db.execute(
                    """
                    INSERT INTO invite_link_members (link_id, user_id, joined_counted)
                    VALUES ($1, $2, true)
                    ON CONFLICT (link_id, user_id) DO UPDATE SET joined_counted = true
                    """,
                    link_id, user.id,
                )
                logger.info(f"[LINK] join_requests: Tracked user={user.id} link_id={link_id} gender={gender}")
            else:
                logger.info(f"[LINK] join_requests: User {user.id} already counted for link_id={link_id}")

    await db.execute(
        """
        INSERT INTO bot_users
          (owner_id, chat_id, user_id, username, first_name,
           language_code, is_premium, has_rtl, has_hieroglyph,
           joined_via_link_id)
        VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10)
        ON CONFLICT (owner_id, chat_id, user_id) DO UPDATE
          SET is_active=true, left_at=NULL,
              username=EXCLUDED.username,
              is_premium=EXCLUDED.is_premium,
              joined_via_link_id=COALESCE(EXCLUDED.joined_via_link_id, bot_users.joined_via_link_id),
              bot_activated = (bot_users.bot_activated OR EXCLUDED.bot_activated)
        """,
        owner_id, chat_id, user.id,
        user.username, user.first_name,
        user.language_code,
        is_premium,
        has_rtl, has_hieroglyph,
        link_id,
    )


async def _send_welcome(bot: Bot, chat_id: int, user, settings_row: dict):
    """Отправляет приветствие новому пользователю в личку."""
    text_tpl = settings_row.get("welcome_text") or ""
    media_fid = settings_row.get("welcome_media")
    media_type = settings_row.get("welcome_media_type")
    media_below = bool(settings_row.get("welcome_media_below", False))
    buttons_raw = settings_row.get("welcome_buttons")
    timer_val = int(settings_row.get("welcome_timer") or 0)

    if not text_tpl and not media_fid:
        return

    # Подставляем переменные
    text = (text_tpl
        .replace("{name}", user.first_name or "Пользователь")
        .replace("{allname}", f"{user.first_name or ''} {getattr(user, 'last_name', '') or ''}".strip())
        .replace("{username}", f"@{user.username}" if getattr(user, "username", None) else "")
        .replace("{chat}", settings_row.get("chat_title", ""))
        .replace("{day}", __import__("datetime").date.today().strftime("%d.%m.%Y"))
    ) if text_tpl else ""

    # Inline-кнопки
    from utils.keyboard import build_inline_keyboard
    user_kb = build_inline_keyboard(buttons_raw)

    try:
        sent_msgs = []

        if media_fid:
            kwargs = {
                "caption": text or None,
                "parse_mode": "HTML",
                "reply_markup": user_kb,
                "show_caption_above_media": media_below,
            }
            async def send_wl(fid):
                if media_type == "photo":
                    return await bot.send_photo(user.id, fid, **kwargs)
                elif media_type == "video":
                    return await bot.send_video(user.id, fid, **kwargs)
                elif media_type == "animation":
                    return await bot.send_animation(user.id, fid, **kwargs)
                else:
                    return await bot.send_document(user.id, fid, **kwargs)
            
            try:
                m = await send_wl(media_fid)
            except Exception as e:
                error_msg = str(e).lower()
                if "wrong file identifier" in error_msg or "file reference" in error_msg or "invalid file" in error_msg:
                    logger.info(f"Re-uploading welcome media {media_fid} for bot {bot.id}")
                    try:
                        from config import settings
                        from aiogram import Bot as AioBot
                        from aiogram.types import BufferedInputFile
                        main_bot = AioBot(token=settings.bot_token)
                        file_info = await main_bot.get_file(media_fid)
                        file_bytes = await main_bot.download_file(file_info.file_path)
                        await main_bot.session.close()
                        input_file = BufferedInputFile(file_bytes.read(), filename=f"wl.{media_type}")
                        m = await send_wl(input_file)
                    except Exception as inner_e:
                        logger.error(f"[WL REUPLOAD ERR] {inner_e}")
                        m = await bot.send_message(user.id, text, parse_mode="HTML", reply_markup=user_kb)
                else:
                    m = await bot.send_message(user.id, text, parse_mode="HTML", reply_markup=user_kb)
            sent_msgs.append(m)

        else:
            # Только текст (нет медиа)
            m = await bot.send_message(user.id, text, parse_mode="HTML", reply_markup=user_kb)
            sent_msgs.append(m)

        # Авто-удаление
        if timer_val > 0:
            for sent in sent_msgs:
                asyncio.create_task(_delete_later(bot, user.id, sent.message_id, timer_val))

    except Exception as e:
        logger.debug(f"[WELCOME] Failed to send to user {user.id}: {e}")


async def _delete_later(bot: Bot, chat_id: int, message_id: int, delay_sec: int):
    """Удаляет сообщение через delay_sec секунд."""
    await asyncio.sleep(delay_sec)
    try:
        await bot.delete_message(chat_id, message_id)
    except Exception:
        pass


async def _send_farewell(bot: Bot, chat_id: int, user, settings_row: dict):
    """Отправляет прощальное сообщение."""
    farewell_text_tpl = settings_row.get("farewell_text") or ""
    farewell_media_fid = settings_row.get("farewell_media")
    farewell_media_type = settings_row.get("farewell_media_type")
    farewell_media_below = bool(settings_row.get("farewell_media_below", False))
    farewell_buttons_raw = settings_row.get("farewell_buttons")
    farewell_timer = int(settings_row.get("farewell_timer") or 0)

    if not farewell_text_tpl and not farewell_media_fid:
        return

    import json as _json
    from aiogram.types import InlineKeyboardMarkup, InlineKeyboardButton
    farewell_text = (farewell_text_tpl
        .replace("{name}", user.first_name or "Пользователь")
        .replace("{allname}", f"{user.first_name or ''} {getattr(user, 'last_name', '') or ''}".strip())
        .replace("{username}", f"@{user.username}" if getattr(user, "username", None) else "")
        .replace("{chat}", settings_row.get("chat_title", ""))
        .replace("{day}", __import__("datetime").date.today().strftime("%d.%m.%Y"))
    ) if farewell_text_tpl else ""

    from utils.keyboard import build_inline_keyboard
    fw_kb = build_inline_keyboard(farewell_buttons_raw)

    try:
        fw_sent = []
        if farewell_media_fid:
            kwargs = {
                "caption": farewell_text or None,
                "parse_mode": "HTML",
                "reply_markup": fw_kb,
                "show_caption_above_media": farewell_media_below,
            }
            async def send_fw(fid):
                if farewell_media_type == "photo":
                    return await bot.send_photo(user.id, fid, **kwargs)
                elif farewell_media_type == "video":
                    return await bot.send_video(user.id, fid, **kwargs)
                elif farewell_media_type == "animation":
                    return await bot.send_animation(user.id, fid, **kwargs)
                else:
                    return await bot.send_document(user.id, fid, **kwargs)
                    
            try:
                m = await send_fw(farewell_media_fid)
            except Exception as e:
                error_msg = str(e).lower()
                if "wrong file identifier" in error_msg or "file reference" in error_msg or "invalid file" in error_msg:
                    logger.info(f"Re-uploading farewell media {farewell_media_fid} for bot {bot.id}")
                    try:
                        from config import settings
                        from aiogram import Bot as AioBot
                        from aiogram.types import BufferedInputFile
                        main_bot = AioBot(token=settings.bot_token)
                        file_info = await main_bot.get_file(farewell_media_fid)
                        file_bytes = await main_bot.download_file(file_info.file_path)
                        await main_bot.session.close()
                        input_file = BufferedInputFile(file_bytes.read(), filename=f"fw.{farewell_media_type}")
                        m = await send_fw(input_file)
                    except Exception as inner_e:
                        logger.error(f"[FW REUPLOAD ERR] {inner_e}")
                        m = await bot.send_message(user.id, farewell_text or "До свидания!", parse_mode="HTML", reply_markup=fw_kb)
                else:
                    m = await bot.send_message(user.id, farewell_text or "До свидания!", parse_mode="HTML", reply_markup=fw_kb)
            
            fw_sent.append(m)
        else:
            m = await bot.send_message(user.id, farewell_text, parse_mode="HTML", reply_markup=fw_kb)
            fw_sent.append(m)

        if farewell_timer > 0:
            for sent in fw_sent:
                asyncio.create_task(_delete_later(bot, user.id, sent.message_id, farewell_timer))
    except Exception as e:
        logger.debug(f"[FAREWELL] Failed to send to user {user.id}: {e}")

