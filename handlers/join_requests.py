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
    """Возвращает настройки площадки и owner_id по chat_id."""
    return await db.fetchrow(
        "SELECT * FROM bot_chats WHERE chat_id=$1 AND is_active=true",
        chat_id,
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

    # 1. Проверка ЧС
    in_bl = await check_blacklist(owner_id, user.id, user.username)
    if in_bl:
        await event.decline()
        try:
            await bot.ban_chat_member(event.chat.id, user.id)
        except Exception:
            pass
        await _log_action(owner_id, event.chat.id, "reject_bl", user.id)
        logger.info(f"[BL] Rejected {user.id} from {event.chat.id}")
        return

    # 2. Языковой фильтр
    if user.language_code:
        lang_blocked = await db.fetchrow(
            "SELECT 1 FROM language_filters WHERE owner_id=$1 AND chat_id=$2 AND language_code=$3",
            owner_id, event.chat.id, user.language_code,
        )
        if lang_blocked:
            await event.decline()
            await _log_action(owner_id, event.chat.id, "reject_lang", user.id,
                              {"lang": user.language_code})
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

    # 5. Автопринятие / капча / отложенное / ручное
    if settings_row["autoaccept"]:
        delay = settings_row["autoaccept_delay"] or 0
        if delay > 0:
            # Сохраняем заявку и обрабатываем в фоне через asyncio.sleep
            await _save_pending(owner_id, event.chat.id, user)
            asyncio.create_task(_delayed_approve(event, owner_id, delay))
        else:
            await event.approve()
            await _register_user(owner_id, event.chat.id, user)
            await _send_welcome(bot, event.chat.id, user, settings_row)
            await _log_action(owner_id, event.chat.id, "approve", user.id)
    elif settings_row["captcha_enabled"]:
        await _save_pending(owner_id, event.chat.id, user)
        await _send_captcha(bot, event, settings_row)
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
        in_bl = await check_blacklist(owner_id, user.id, user.username)
        if in_bl:
            try:
                await bot.ban_chat_member(event.chat.id, user.id)
            except Exception:
                pass
            await _log_action(owner_id, event.chat.id, "ban_on_join", user.id)
            return

        await _register_user(owner_id, event.chat.id, user,
                             invite_link=event.invite_link)
        await _send_welcome(bot, event.chat.id, user, settings_row)

    # Пользователь ушёл
    elif new_status in ("left", "kicked") and old_status == "member":
        await db.execute(
            "UPDATE bot_users SET is_active=false, left_at=now() "
            "WHERE owner_id=$1 AND chat_id=$2 AND user_id=$3",
            owner_id, event.chat.id, user.id,
        )
        # Обновляем счётчик отписок для ссылки
        await db.execute(
            """
            UPDATE invite_links SET unsubscribed = unsubscribed + 1
            WHERE id = (
                SELECT joined_via_link_id FROM bot_users
                WHERE owner_id=$1 AND chat_id=$2 AND user_id=$3
            )
            """,
            owner_id, event.chat.id, user.id,
        )
        # Прощание (только если bot_activated)
        if settings_row.get("farewell_text"):
            activated = await db.fetchval(
                "SELECT bot_activated FROM bot_users WHERE owner_id=$1 AND chat_id=$2 AND user_id=$3",
                owner_id, event.chat.id, user.id,
            )
            if activated:
                try:
                    await bot.send_message(user.id, settings_row["farewell_text"])
                except Exception:
                    pass


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
    """Сохраняет пользователя в bot_users."""
    full_name = f"{user.first_name or ''} {user.last_name or ''}".strip()
    link_id = None
    if invite_link:
        row = await db.fetchrow(
            "SELECT id FROM invite_links WHERE owner_id=$1 AND link=$2",
            owner_id, invite_link.invite_link,
        )
        if row:
            link_id = row["id"]
            await db.execute(
                "UPDATE invite_links SET joined=joined+1 WHERE id=$1", link_id
            )

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
              joined_via_link_id=EXCLUDED.joined_via_link_id
        """,
        owner_id, chat_id, user.id,
        user.username, user.first_name,
        user.language_code,
        getattr(user, "is_premium", False),
        detect_rtl(full_name), detect_hieroglyph(full_name),
        link_id,
    )


async def _send_welcome(bot: Bot, chat_id: int, user, settings_row: dict):
    """Отправляет приветствие новому пользователю в личку (если bot_activated)."""
    if not settings_row.get("welcome_text"):
        return
    # Шаблонные переменные
    text = settings_row["welcome_text"].replace(
        "{name}", user.first_name or "Пользователь"
    ).replace(
        "{channel}", settings_row.get("chat_title", "")
    )
    activated = await db.fetchval(
        "SELECT bot_activated FROM bot_users WHERE owner_id=$1 AND chat_id=$2 AND user_id=$3",
        settings_row["owner_id"], chat_id, user.id,
    )
    if activated:
        try:
            await bot.send_message(user.id, text)
        except Exception:
            pass


async def _send_captcha(bot: Bot, event: ChatJoinRequest, settings_row: dict):
    """Отправляет капчу (заглушка — полная реализация в handlers/captcha.py)."""
    from aiogram.types import InlineKeyboardMarkup, InlineKeyboardButton
    user = event.from_user
    text = (settings_row.get("captcha_text") or
            f"Привет {user.first_name}! Докажи что ты не робот — нажми кнопку ✅")
    kb = InlineKeyboardMarkup(inline_keyboard=[[
        InlineKeyboardButton(
            text="✅ Я не робот",
            callback_data=f"captcha:{event.chat.id}:{user.id}",
        )
    ]])
    try:
        await bot.send_message(user.id, text, reply_markup=kb)
    except Exception:
        # Пользователь не открыл диалог с ботом — авто-одобряем
        await event.approve()
        await _register_user(settings_row["owner_id"], event.chat.id, user)
