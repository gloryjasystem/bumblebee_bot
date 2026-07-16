"""
handlers/join_requests.py — Обработка ChatJoinRequest и ChatMemberUpdated.
Это центральный хендлер защиты каналов.
"""
import asyncio
import logging
import time
from aiogram import Router, Bot
from aiogram.types import ChatJoinRequest, ChatMemberUpdated

import db.pool as db
from services.blacklist import check_blacklist
from services.security import detect_rtl, detect_hieroglyph

logger = logging.getLogger(__name__)
router = Router()

# ── Дедупликация приветствия ───────────────────────────────────
# Приветствие может быть вызвано из нескольких путей почти одновременно:
#   1) в момент одобрения заявки (_delayed_approve_join_request / on_req_confirm)
#   2) когда пользователь реально вступил (chat_member → _handle_chat_member)
# Оба пути ведут к _send_welcome. Чтобы пользователь получил РОВНО ОДНО
# приветствие, запоминаем (chat_id, user_id) на короткое окно и глушим повтор.
_recent_welcome: dict[tuple[int, int], float] = {}
_WELCOME_DEDUP_SEC = 180  # окно подавления дублей (сек); дольше задержки approve→join


def _welcome_already_sent(chat_id: int, user_id: int) -> bool:
    """True, если приветствие этому пользователю уже отправлялось в пределах окна.
    Побочный эффект: помечает пару как «отправлено сейчас» и чистит устаревшие записи."""
    now = time.monotonic()
    # Чистим старые записи, чтобы словарь не рос бесконечно
    if len(_recent_welcome) > 512:
        for k, ts in list(_recent_welcome.items()):
            if now - ts > _WELCOME_DEDUP_SEC:
                _recent_welcome.pop(k, None)
    key = (chat_id, user_id)
    last = _recent_welcome.get(key)
    if last is not None and now - last < _WELCOME_DEDUP_SEC:
        return True
    _recent_welcome[key] = now
    return False


def _welcome_release(chat_id: int, user_id: int) -> None:
    """Снимает пометку об отправке приветствия. Вызывается при НЕУДАЧНОЙ доставке,
    чтобы другой путь/бот мог повторить попытку (иначе упавшая попытка навсегда
    блокировала бы правильную в пределах окна дедупа)."""
    _recent_welcome.pop((chat_id, user_id), None)


def _welcome_recently_sent(chat_id: int, user_id: int) -> bool:
    """Read-only проверка (БЕЗ пометки): уходило ли приветствие этой паре в окне дедупа.
    Нужна режиму «до капчи»: если приветствие/цепочку сейчас подавит дедуп (быстрый
    ре-джойн тем же аккаунтом), капчу НЕ откладываем — свежей ленты не будет, шлём сразу."""
    ts = _recent_welcome.get((chat_id, user_id))
    return ts is not None and (time.monotonic() - ts) < _WELCOME_DEDUP_SEC


# ── Дедуп прощания: помечаем ТОЛЬКО по факту успешной доставки ──
# На общем канале событие 'left' приходит нескольким ботам-админам сразу; каждый
# пробует отправить прощание, но доходит лишь тот, у кого открыта личка с юзером.
# Пометка ставится ПОСЛЕ успешной отправки → повторная доставка подавляется, а
# упавшая попытка не мешает другому боту доставить.
_recent_farewell: dict[tuple[int, int], float] = {}
_FAREWELL_DEDUP_SEC = 180


def _farewell_delivered(chat_id: int, user_id: int) -> bool:
    """True, если прощание этому пользователю уже успешно доставлено в окне."""
    now = time.monotonic()
    if len(_recent_farewell) > 512:
        for k, ts in list(_recent_farewell.items()):
            if now - ts > _FAREWELL_DEDUP_SEC:
                _recent_farewell.pop(k, None)
    last = _recent_farewell.get((chat_id, user_id))
    return last is not None and now - last < _FAREWELL_DEDUP_SEC


def _mark_farewell_delivered(chat_id: int, user_id: int) -> None:
    _recent_farewell[(chat_id, user_id)] = time.monotonic()


def _farewell_release(chat_id: int, user_id: int) -> None:
    """Снимает пометку доставки прощания. Вызывается при ВСТУПЛЕНИИ юзера — чтобы его
    следующий выход снова отправил прощание (дедуп не должен перекрывать разные циклы
    вход→выход того же пользователя, иначе быстрый повторный тест «глотает» прощание)."""
    _recent_farewell.pop((chat_id, user_id), None)


def _dm_blocked_reason(err: Exception) -> str | None:
    """Если ошибка означает «бот не может писать этому юзеру» — вернуть краткую
    причину; иначе None. Нужно, чтобы не путать «юзер не запускал бота» с реальным сбоем."""
    low = str(err).lower()
    if any(s in low for s in (
        "bot can't initiate", "bot was blocked", "user is deactivated",
        "chat not found", "forbidden", "blocked by the user", "have no rights",
    )):
        return "юзер не запускал этого бота / заблокировал его"
    return None


async def _get_owner(chat_id: int) -> dict | None:
    """Returns platform settings, owner_id, blacklist state, and whether this bot is in the global BL scope."""
    from config import settings as _cfg
    row = await db.fetchrow(
        """
        WITH TargetBot AS (
            -- Только chat_id (главный бот как админ канала). Канал может быть у нескольких
            -- владельцев → берём ДЕТЕРМИНИРОВАННО старейшую строку, а не произвольную.
            SELECT child_bot_id FROM bot_chats WHERE chat_id=$1
            ORDER BY added_at ASC, id ASC LIMIT 1
        ),
        RankedChats AS (
            SELECT bc.*,
                   CASE WHEN bc.is_active THEN ROW_NUMBER() OVER(PARTITION BY bc.child_bot_id, bc.is_active ORDER BY bc.added_at ASC)
                        ELSE 9999 END as rn
            FROM bot_chats bc
            JOIN TargetBot tb ON bc.child_bot_id = tb.child_bot_id
        )
        SELECT bc.*, cb.blacklist_enabled, pu.tariff,
               COALESCE((SELECT blacklist_active FROM platform_users WHERE user_id = $2), true) AS blacklist_active,
               EXISTS(
                   SELECT 1 FROM ga_selected_bots gsb
                   WHERE gsb.owner_id = $2
                     AND gsb.child_bot_id = bc.child_bot_id
               ) AS in_global_bl_scope
        FROM RankedChats bc
        JOIN child_bots cb ON bc.child_bot_id = cb.id
        JOIN platform_users pu ON bc.owner_id = pu.user_id
        WHERE bc.chat_id=$1 AND bc.is_active=true
        """,
        chat_id, _cfg.owner_telegram_id,
    )
    if not row:
        return None
        
    from config import TARIFFS
    limit = TARIFFS.get(row["tariff"], TARIFFS["free"])["max_chats_per_bot"]
    if dict(row).get("rn", 1) > limit:
        return None  # Бот заморожен на этой площадке
        
    return dict(row)



# ── ЗАКРЫТЫЙ КАНАЛ: ChatJoinRequest ───────────────────────────
@router.chat_join_request()
async def on_join_request(event: ChatJoinRequest, bot: Bot):
    settings_row = await _get_owner(event.chat.id)
    if not settings_row:
        return

    owner_id = settings_row["owner_id"]
    user = event.from_user

    # 1. Проверка ЧС:
    #    ГЛОБАЛЬНЫЙ ЧС платформы — работает ВСЕГДА, независимо от локального тумблера бота.
    #    ЛОКАЛЬНЫЙ ЧС бота — только если у бота включён blacklist_enabled.
    _cbi = settings_row.get("child_bot_id")
    is_global_block = False
    is_local_block = False

    # Глобальный ЧС: проверяем всегда (если бот в выборке и мастер-тумблер включён)
    if settings_row.get("blacklist_active", True) and settings_row.get("in_global_bl_scope", False):
        from config import settings as _cfg
        if await check_blacklist(_cfg.owner_telegram_id, user.id, user.username, child_bot_id=None):
            is_global_block = True

    # Локальный ЧС: проверяем только если тумблер бота включён
    if settings_row.get("blacklist_enabled", True):
        if await check_blacklist(owner_id, user.id, user.username, child_bot_id=_cbi):
            is_local_block = True

    if is_global_block or is_local_block:
        await event.decline()
        try:
            await bot.ban_chat_member(event.chat.id, user.id)
        except Exception:
            pass
        # Счётчик заблокированных на платформе
        await db.execute(
            "UPDATE platform_users SET blocked_count = blocked_count + 1 WHERE user_id = $1",
            owner_id,
        )
        # Per-bot счётчик
        child_bot_id = settings_row.get("child_bot_id")
        if child_bot_id:
            if is_global_block:
                await db.execute(
                    "UPDATE child_bots SET global_blocked_count = global_blocked_count + 1 WHERE id = $1",
                    child_bot_id,
                )
            else:
                await db.execute(
                    "UPDATE child_bots SET blocked_count = blocked_count + 1 WHERE id = $1",
                    child_bot_id,
                )

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
            return

    # 3. RTL-фильтр
    full_name = f"{user.first_name or ''} {user.last_name or ''}".strip()
    if settings_row["filter_rtl"] and detect_rtl(full_name):
        await event.decline()

        return

    # 4. Иероглифы
    if settings_row["filter_hieroglyph"] and detect_hieroglyph(full_name):
        await event.decline()

        return

    # 5. Аккаунты без фото
    if settings_row.get("filter_no_photo"):
        try:
            photos = await bot.get_user_profile_photos(user.id, limit=1)
            if photos.total_count == 0:
                await event.decline()

                return
        except Exception as e:
            logger.warning(f"[FILTER] get_user_profile_photos failed for {user.id}: {e}")

    # ── «Сразу / по заявке»: базовое приветствие уходит В МОМЕНТ ЗАЯВКИ (до входа),
    #    пока открыто ЛС-окно от join-request. Дальнейший разбор (ссылка/автоприём/
    #    капча/ручной) идёт как обычно; на одобрении не задвоится — _send_welcome
    #    идемпотентна по (chat_id, user_id). Режим включается выбором «Сразу» в
    #    редакторе приветствия (welcome_delay_sec = -1); всем остальным (>=0) — как раньше.
    # settings_row из _get_owner может быть неоднозначным для каналов с несколькими ботами.
    # Для «Сразу» берём КАНОНИЧЕСКУЮ строку канала (как after-join путь в child_bot_runner),
    # иначе подхватывается чужая строка без медиа и приветствие уходит без картинки.
    from db.channels import get_channel
    _canon_wl = await get_channel(event.chat.id)
    _wl_row = dict(_canon_wl) if _canon_wl else settings_row
    # Капча-гейт: при включённой капче приветствие «по заявке» шлём ТОЛЬКО в режиме
    # «до капчи» (greet_mode=1). В «после капчи» (0, гейт) и «выкл» (2) приветствие
    # уходит после прохождения капчи (или не уходит) — чтобы капча не заваливалась.
    from handlers.captcha import greet_mode as _greet_mode
    _wl_captcha_on = (_wl_row.get("captcha_type") or "off") != "off"
    _wl_greet_mode = _greet_mode(_wl_row)
    _wl_greet_at_request = (not _wl_captcha_on) or (_wl_greet_mode == 1)
    _wl_welcome_fired = int(_wl_row.get("welcome_delay_sec") or 0) < 0 and _wl_greet_at_request
    _wl_welcome_deduped = _welcome_recently_sent(event.chat.id, user.id)  # свежую ленту подавит дедуп?
    if _wl_welcome_fired:
        try:
            await _send_welcome(bot, event.chat.id, user, _wl_row, contact_established=True, from_join_request=True)
        except Exception as _we:
            logger.warning(f"[WELCOME] «по заявке» не ушло user={user.id}: {_we}")

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

        logger.info(f"[LINK AUTO=on] Approved {user.id} via link {raw_invite_url}")
        return

    if link_auto_accept == "off":
        # Ссылка: автопринятие выключено — только ручная проверка администратором
        await _save_pending(owner_id, event.chat.id, user)
        logger.info(f"[LINK AUTO=off] Saved {user.id} for manual review, link={raw_invite_url}")
        return

    # link_auto_accept == "base" или ссылка неизвестна → стандартная логика бота
    # 7. Автопринятие / капча / отложенное / ручное
    # ВАЖНО: UI отложенного принятия пишет autoaccept=false + delay>0, поэтому
    # гейтить только по autoaccept нельзя — иначе задержка игнорируется.
    from utils.timing import effective_delay_sec
    delay = effective_delay_sec(settings_row)
    if settings_row["autoaccept"] or delay > 0:
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

    elif (settings_row.get("captcha_type") or "off") != "off":
        await _save_pending(owner_id, event.chat.id, user)
        from handlers.captcha import send_captcha, send_captcha_after
        # «До капчи» (greet_mode=1): №1 уже ушло, №2+ уходят отложенно. Капчу шлём ПОСЛЕ
        # последнего сообщения цепочки, иначе она выскочит между №1 и отложенными №2+.
        if _wl_greet_mode == 1 and _wl_welcome_fired and not _wl_welcome_deduped:
            _max_delay = await db.fetchval(
                "SELECT COALESCE(MAX(delay_sec), 0) FROM welcome_steps "
                "WHERE owner_id=$1 AND chat_id=$2::bigint AND (action IS NULL OR action <> 'delete')",
                owner_id, event.chat.id,
            )
            if int(_max_delay or 0) > 0:
                asyncio.create_task(send_captcha_after(bot, event, dict(settings_row), int(_max_delay) + 5))
                return
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

        _cbi = settings_row.get("child_bot_id")
        is_global_block = False
        is_local_block = False

        # Глобальный ЧС: проверяем всегда (если бот в выборке и мастер-тумблер включён)
        if settings_row.get("blacklist_active", True) and settings_row.get("in_global_bl_scope", False):
            from config import settings as _cfg
            if await check_blacklist(_cfg.owner_telegram_id, user.id, user.username, child_bot_id=None):
                is_global_block = True

        # Локальный ЧС: проверяем только если тумблер бота включён
        if settings_row.get("blacklist_enabled", True):
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
            child_bot_id = settings_row.get("child_bot_id")
            if child_bot_id:
                if is_global_block:
                    await db.execute(
                        "UPDATE child_bots SET global_blocked_count = global_blocked_count + 1 WHERE id = $1",
                        child_bot_id,
                    )
                else:
                    await db.execute(
                        "UPDATE child_bots SET blocked_count = blocked_count + 1 WHERE id = $1",
                        child_bot_id,
                    )

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


async def _delayed_approve(event: ChatJoinRequest, owner_id: int, delay_sec: int):
    """Принимает заявку через delay_sec секунд (в фоне)."""
    await asyncio.sleep(delay_sec)
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


async def _send_welcome(bot: Bot, chat_id: int, user, settings_row: dict, contact_established: bool = False, from_join_request: bool = False) -> bool:
    """Отправляет приветствие новому пользователю в личку.
    Возвращает True при успешной доставке (или если приветствие уже отправлено
    другим путём), False — если доставка не удалась.
    Идемпотентна: повторный вызов для той же пары (chat_id, user_id) в пределах
    короткого окна игнорируется — так гарантируется РОВНО ОДНО приветствие,
    даже если сработали оба пути (одобрение заявки и событие вступления).
    При неудаче пометка снимается — чтобы другой бот канала мог доставить."""
    # «Сразу» (welcome_delay_sec < 0): базовое приветствие уходит ТОЛЬКО в момент заявки
    # (from_join_request=True из on_join_request). Все прочие вызовы (авто-принятие/вход/
    # капча/отложенное/ручное) для такого канала — no-op: иначе при отложенном или ручном
    # принятии позже 180-сек дедапа пользователь получил бы ДУБЛЬ приветствия.
    if not from_join_request and int(settings_row.get("welcome_delay_sec") or 0) < 0:
        return True
    if _welcome_already_sent(chat_id, user.id):
        logger.info(f"[WELCOME] Пропуск дубля приветствия user={user.id} chat={chat_id}")
        return True
    try:
        from handlers.captcha import cleanup_captcha_and_send_welcome
        await cleanup_captcha_and_send_welcome(bot, chat_id, user.id)
    except Exception as e:
        import logging
        logging.getLogger(__name__).debug(f"[CAPTCHA CLEANUP] Failed before welcome: {e}")

    text_tpl = settings_row.get("welcome_text") or ""
    media_fid = settings_row.get("welcome_media")
    media_type = settings_row.get("welcome_media_type")
    media_below = bool(settings_row.get("welcome_media_below", False))
    buttons_raw = settings_row.get("welcome_buttons")
    timer_val = int(settings_row.get("welcome_timer") or 0)
    welcome_delay = int(settings_row.get("welcome_delay_sec") or 0)

    # Базовое приветствие (№1) шлём, только если оно ВКЛЮЧЕНО и ЗАДАНО. Если выключено
    # тумблером или пусто — базу пропускаем, но цепочка (№2+) всё равно уходит по своим
    # задержкам (расцепление: выключение №1 больше не гасит всю цепочку).
    send_base = (settings_row.get("welcome_enabled") is not False) and bool(text_tpl or media_fid)

    # Задержку №1 применяем ТОЛЬКО когда ЛС-окно гарантированно открыто: включённая капча
    # (inline-капчи в боте нет — капча всегда reply, юзер сам жмёт кнопку-ответ = пишет боту)
    # или вход по /start. Иначе (без капчи) первое сообщение шлём СРАЗУ — иначе Telegram не даст
    # его отправить (Forbidden). Так приветствие не теряется. Стиль кнопки (big/compact) на
    # установление контакта не влияет — важен только факт включённой капчи.
    _captcha_type = settings_row.get("captcha_type") or "off"
    dm_open = contact_established or (_captcha_type != "off")
    delay_base = send_base and welcome_delay > 0 and dm_open

    sent_msgs = []
    delayed_base_step = None
    if send_base and not delay_base:
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
                        # send_document не принимает show_caption_above_media — убираем только для него
                        doc_kwargs = {k: v for k, v in kwargs.items() if k != "show_caption_above_media"}
                        return await bot.send_document(user.id, fid, **doc_kwargs)

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
                        logger.warning(f"[WL MEDIA FAIL] медиа не ушло, откат в текст user={user.id} type={media_type}: {e}")
                        m = await bot.send_message(user.id, text, parse_mode="HTML", reply_markup=user_kb)
                sent_msgs.append(m)
            else:
                # Только текст (нет медиа)
                m = await bot.send_message(user.id, text, parse_mode="HTML", reply_markup=user_kb)
                sent_msgs.append(m)

            # Авто-удаление базы (своё, in-memory)
            if timer_val > 0:
                for sent in sent_msgs:
                    asyncio.create_task(_delete_later(bot, user.id, sent.message_id, timer_val))
        except Exception as e:
            # База должна была уйти, но упала (напр. ЛС закрыта) — снимаем пометку, чтобы
            # другой бот канала мог доставить. Цепочку тут НЕ планируем (тот же ЛС закрыт).
            _welcome_release(chat_id, user.id)
            reason = _dm_blocked_reason(e)
            bot_id = getattr(bot, "id", "?")
            if reason:
                logger.warning(f"[WELCOME] Не доставлено ({reason}) user={user.id} bot={bot_id} chat={chat_id}: {e}")
            else:
                logger.warning(f"[WELCOME] Ошибка отправки user={user.id} bot={bot_id} chat={chat_id}: {e}")
            return False
    elif delay_base:
        # №1 с задержкой — отправляем как отложенный шаг через проверенный движок цепочки
        # (_welcome_step_task: переотправка медиа, кнопки, авто-удаление, трекинг для очистки).
        # self_delete_sec = welcome_timer, чтобы авто-удаление базы сохранилось.
        delayed_base_step = {
            "delay_sec": welcome_delay,
            "action": "message",
            "text": text_tpl,           # сырой шаблон; переменные подставит _welcome_step_task
            "media": media_fid,
            "media_type": media_type,
            "media_below": media_below,
            "buttons": buttons_raw,
            "self_delete_sec": timer_val,
        }

    # Цепочка №2+ — планируем ВСЕГДА (сама пропустит, если шагов нет). Отложенную базу (если
    # есть) добавляем первым шагом. Возвращает число сообщений-шагов (для проверки «что-то ушло»).
    scheduled_msgs = 0
    try:
        base_ids = [m.message_id for m in sent_msgs if m]
        scheduled_msgs = await _schedule_welcome_sequence(
            bot, chat_id, user,
            settings_row.get("owner_id"),
            settings_row.get("chat_title", ""),
            base_ids,
            extra_steps=[delayed_base_step] if delayed_base_step else None,
            child_bot_id=settings_row.get("child_bot_id"),
            auto_delete_min=int(settings_row.get("auto_delete_min") or 0),
        )
    except Exception as _se:
        logger.debug(f"[WSEQ] schedule failed for user {user.id}: {_se}")

    if not send_base and not scheduled_msgs:
        # Ни базы, ни доп. сообщений — ничего не ушло, снимаем пометку дедупа
        _welcome_release(chat_id, user.id)
        return False

    return True


# ── Цепочка приветствий (доп. сообщения с задержками + шаг-удаление) ──
# Сообщения, отправленные в рамках цепочки для (chat_id, user_id) — чтобы
# шаг action='delete' мог их удалить («Удаляю ссылку»). Ключ авто-очищается.
_welcome_seq_msgs: dict[tuple[int, int], list[int]] = {}


def _fill_msg_vars(text: str, user, chat_title: str) -> str:
    """Подставляет переменные {name}/{allname}/{username}/{chat}/{day}."""
    if not text:
        return ""
    return (text
        .replace("{name}", user.first_name or "Пользователь")
        .replace("{allname}", f"{user.first_name or ''} {getattr(user, 'last_name', '') or ''}".strip())
        .replace("{username}", f"@{user.username}" if getattr(user, "username", None) else "")
        .replace("{chat}", chat_title or "")
        .replace("{day}", __import__("datetime").date.today().strftime("%d.%m.%Y"))
    )


async def _schedule_welcome_sequence(bot: Bot, chat_id: int, user, owner_id, chat_title: str,
                                     base_msg_ids: list[int], extra_steps: list | None = None,
                                     child_bot_id=None, auto_delete_min: int = 0) -> int:
    """Планирует отправку дополнительных шагов цепочки приветствий.
    extra_steps — синтетические шаги (напр. отложенное базовое приветствие №1); идут первыми.
    Возвращает число сообщений-шагов (без шага-очистки) — чтобы вызывающий понял,
    ушло ли что-то, когда базовое приветствие пропущено (выключено/пусто)."""
    steps = []
    if owner_id:
        try:
            steps = await db.fetch(
                "SELECT * FROM welcome_steps WHERE owner_id=$1 AND chat_id=$2::bigint "
                "ORDER BY step_order ASC, delay_sec ASC, id ASC",
                owner_id, chat_id,
            )
        except Exception as _e:
            logger.debug(f"[WSEQ] fetch steps failed: {_e}")
            steps = []
    # Все шаги как dict (extra_steps уже dict; записи из БД конвертируем) — для .get() ниже
    all_steps = list(extra_steps or []) + [dict(s) for s in steps]
    if not all_steps:
        return 0
    key = (chat_id, user.id)
    # База (№1) — первые сообщения цепочки, чтобы шаг-удаление мог их снять
    _welcome_seq_msgs[key] = list(base_msg_ids)
    max_delay = max(
        (int(s.get("delay_sec") or 0) + int(s.get("self_delete_sec") or 0)) for s in all_steps
    )
    # ПОРЯДОК: раньше на каждый шаг создавалась ОТДЕЛЬНАЯ задача с абсолютной задержкой
    # «от заявки». При равных задержках (напр. все +15с) шаги просыпались одновременно, и
    # порядок отправки был случайным (приходило вперемешку / задом наперёд). Теперь — один
    # прогон строго по порядку (см. _run_welcome_sequence).
    asyncio.create_task(_run_welcome_sequence(
        bot, chat_id, user, chat_title, all_steps,
        child_bot_id=child_bot_id, auto_delete_min=auto_delete_min,
    ))
    # Гарантированная очистка ключа из памяти после завершения цепочки
    asyncio.create_task(_prune_seq_key(key, max_delay + 300))
    return sum(1 for s in all_steps if (s.get("action") or "message") != "delete")


async def _run_welcome_sequence(bot: Bot, chat_id: int, user, chat_title: str, all_steps: list,
                                child_bot_id=None, auto_delete_min: int = 0):
    """Отправляет шаги цепочки СТРОГО ПО ПОРЯДКУ, одним прогоном.
    Сортировка по (абсолютная задержка «от заявки», исходный индекс) — база №1 идёт первой
    при равных задержках, дальше №2, №3, №4. Между шагами спим ИНКРЕМЕНТ и ЖДЁМ каждую
    отправку, поэтому порядок гарантирован даже когда у всех шагов одинаковая задержка."""
    ordered = sorted(enumerate(all_steps),
                     key=lambda t: (int(t[1].get("delay_sec") or 0), t[0]))
    elapsed = 0
    for _, step in ordered:
        target = int(step.get("delay_sec") or 0)
        wait = target - elapsed
        if wait > 0:
            await asyncio.sleep(wait)
            elapsed = target
        # Задержку уже отспали здесь → обнуляем, чтобы _welcome_step_task не спал повторно.
        step_now = dict(step)
        step_now["delay_sec"] = 0
        try:
            await _welcome_step_task(bot, chat_id, user, chat_title, step_now,
                                     child_bot_id=child_bot_id, auto_delete_min=auto_delete_min)
        except Exception as _e:
            logger.debug(f"[WSEQ] ordered step failed: {_e}")


async def _prune_seq_key(key: tuple[int, int], after_sec: int):
    await asyncio.sleep(max(1, after_sec))
    _welcome_seq_msgs.pop(key, None)


async def _welcome_step_task(bot: Bot, chat_id: int, user, chat_title: str, step: dict,
                             child_bot_id=None, auto_delete_min: int = 0):
    """Выполняет один шаг цепочки: сообщение или удаление ранее отправленных."""
    delay = int(step.get("delay_sec") or 0)
    if delay > 0:
        await asyncio.sleep(delay)
    key = (chat_id, user.id)

    # ── Шаг «удалить» — снимаем все ранее отправленные сообщения цепочки ──
    if (step.get("action") or "message") == "delete":
        for mid in list(_welcome_seq_msgs.get(key, [])):
            try:
                await bot.delete_message(user.id, mid)
            except Exception:
                pass
        _welcome_seq_msgs[key] = []
        return

    # ── Шаг «сообщение» ──
    text = _fill_msg_vars(step.get("text") or "", user, chat_title)
    media_fid = step.get("media")
    media_type = step.get("media_type")
    media_below = bool(step.get("media_below", False))
    self_del = int(step.get("self_delete_sec") or 0)

    if not text and not media_fid:
        return

    from utils.keyboard import build_inline_keyboard
    kb = build_inline_keyboard(step.get("buttons"))

    try:
        if media_fid:
            kwargs = {
                "caption": text or None,
                "parse_mode": "HTML",
                "reply_markup": kb,
            }
            async def send_step(fid):
                if media_type == "photo":
                    return await bot.send_photo(user.id, fid, show_caption_above_media=media_below, **kwargs)
                elif media_type == "video":
                    return await bot.send_video(user.id, fid, show_caption_above_media=media_below, **kwargs)
                elif media_type == "animation":
                    return await bot.send_animation(user.id, fid, show_caption_above_media=media_below, **kwargs)
                else:
                    return await bot.send_document(user.id, fid, **kwargs)
            try:
                m = await send_step(media_fid)
            except Exception as e:
                # file_id медиа шага выдан ГЛАВНЫМ ботом и невалиден для дочернего →
                # качаем байты главным ботом и переотправляем дочерним. Та же проверенная
                # логика, что в _send_welcome для базового приветствия.
                error_msg = str(e).lower()
                if "wrong file identifier" in error_msg or "file reference" in error_msg or "invalid file" in error_msg:
                    logger.info(f"[WSEQ REUPLOAD] Re-uploading step media {media_fid} for bot {bot.id}")
                    try:
                        from config import settings
                        from aiogram import Bot as AioBot
                        from aiogram.types import BufferedInputFile
                        main_bot = AioBot(token=settings.bot_token)
                        file_info = await main_bot.get_file(media_fid)
                        file_bytes = await main_bot.download_file(file_info.file_path)
                        await main_bot.session.close()
                        input_file = BufferedInputFile(file_bytes.read(), filename=f"wstep.{media_type}")
                        m = await send_step(input_file)
                    except Exception as inner_e:
                        logger.error(f"[WSEQ REUPLOAD ERR] {inner_e}")
                        m = await bot.send_message(user.id, text or "—", parse_mode="HTML", reply_markup=kb)
                else:
                    m = await bot.send_message(user.id, text or "—", parse_mode="HTML", reply_markup=kb)
        else:
            m = await bot.send_message(user.id, text, parse_mode="HTML", reply_markup=kb)

        # Трекинг для будущего шага-удаления
        _welcome_seq_msgs.setdefault(key, []).append(m.message_id)
        # Авто-удаление самого шага (своё, in-memory)
        if self_del > 0:
            asyncio.create_task(_delete_later(bot, user.id, m.message_id, self_del))
    except Exception as e:
        logger.debug(f"[WSEQ] step send failed for user {user.id}: {e}")


async def _delete_later(bot: Bot, chat_id: int, message_id: int, delay_sec: int):
    """Удаляет сообщение через delay_sec секунд."""
    await asyncio.sleep(delay_sec)
    try:
        await bot.delete_message(chat_id, message_id)
    except Exception:
        pass


async def _send_farewell(bot: Bot, chat_id: int, user, settings_row: dict) -> bool:
    """Отправляет прощальное сообщение уходящему пользователю в личку.
    Возвращает True при успешной доставке, False — если не удалось.
    Дедуп по факту доставки: на общем канале событие 'left' приходит нескольким
    ботам, каждый пробует отправить; доставит тот, у кого открыта личка, ровно раз."""
    if settings_row.get("farewell_enabled") is False:
        return False  # прощание выключено тумблером — не шлём (NULL/None = включено)
    if _farewell_delivered(chat_id, user.id):
        logger.info(f"[FAREWELL] Пропуск — уже доставлено user={user.id} chat={chat_id}")
        return True
    farewell_text_tpl = settings_row.get("farewell_text") or ""
    farewell_media_fid = settings_row.get("farewell_media")
    farewell_media_type = settings_row.get("farewell_media_type")
    farewell_media_below = bool(settings_row.get("farewell_media_below", False))
    farewell_buttons_raw = settings_row.get("farewell_buttons")
    farewell_timer = int(settings_row.get("farewell_timer") or 0)

    if not farewell_text_tpl and not farewell_media_fid:
        return False

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
                    # send_document не принимает show_caption_above_media — убираем только для него
                    doc_kwargs = {k: v for k, v in kwargs.items() if k != "show_caption_above_media"}
                    return await bot.send_document(user.id, fid, **doc_kwargs)
                    
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
        # Не помечаем доставленным — пусть другой бот канала попробует. Логируем ЯВНО.
        reason = _dm_blocked_reason(e)
        bot_id = getattr(bot, "id", "?")
        if reason:
            logger.warning(f"[FAREWELL] Не доставлено ({reason}) user={user.id} bot={bot_id} chat={chat_id}: {e}")
        else:
            logger.warning(f"[FAREWELL] Ошибка отправки user={user.id} bot={bot_id} chat={chat_id}: {e}")
        return False

    _mark_farewell_delivered(chat_id, user.id)
    return True

