"""
scheduler/child_bot_runner.py — Запускает polling для каждого дочернего бота.

При старте приложения и при добавлении нового токена — стартует отдельную
asyncio задачу с long-polling для дочернего бота. Обрабатывает:
  - my_chat_member: бот добавлен в канал/группу → уведомляет владельца и сохраняет в БД
  - chat_join_request: новая заявка → принимает/откланяет + приветствует
"""
import asyncio
import logging
from typing import Dict

from aiogram import Bot
from aiogram.types import Update, ChatMemberUpdated, ChatJoinRequest
from aiogram.exceptions import TelegramUnauthorizedError

import db.pool as db
from services.security import decrypt_token

logger = logging.getLogger(__name__)

# owner_bot: задача polling
_running_bots: Dict[int, asyncio.Task] = {}  # child_bot_id → Task
_main_bot: Bot = None   # Bumblebee management bot для уведомлений


def init_runner(main_bot: Bot):
    global _main_bot
    _main_bot = main_bot


async def start_all_child_bots():
    """Вызывается при старте приложения — запускает polling для всех токенов из БД."""
    rows = await db.fetch(
        "SELECT id, owner_id, bot_username, token_encrypted FROM child_bots"
    )
    for row in rows:
        await start_child_bot(row["id"], row["owner_id"], row["bot_username"], row["token_encrypted"])
    logger.info(f"Started {len(rows)} child bot(s)")


async def start_child_bot(child_bot_id: int, owner_id: int, bot_username: str, token_encrypted: str):
    """Запускает polling для одного дочернего бота (если ещё не запущен)."""
    if child_bot_id in _running_bots and not _running_bots[child_bot_id].done():
        return  # Уже запущен

    raw_token = decrypt_token(token_encrypted)
    task = asyncio.create_task(
        _poll_child_bot(child_bot_id, owner_id, bot_username, raw_token),
        name=f"child_bot_{child_bot_id}",
    )
    _running_bots[child_bot_id] = task
    logger.info(f"Child bot @{bot_username} (id={child_bot_id}) polling started")


def stop_child_bot(child_bot_id: int):
    task = _running_bots.pop(child_bot_id, None)
    if task:
        task.cancel()


async def _poll_child_bot(child_bot_id: int, owner_id: int, bot_username: str, raw_token: str):
    """Long-polling цикл для дочернего бота."""
    bot = Bot(token=raw_token)
    offset = 0
    retry_delay = 5

    try:
        # Сбрасываем webhook — отдельный try чтобы невалидный токен не крашил таску
        try:
            await bot.delete_webhook(drop_pending_updates=True)
        except TelegramUnauthorizedError:
            logger.error(f"Child bot @{bot_username} (id={child_bot_id}): token revoked — deactivating")
            await db.execute(
                "UPDATE bot_chats SET is_active=false WHERE child_bot_id=$1", child_bot_id
            )
            return
        except Exception as e:
            logger.warning(f"Child bot @{bot_username} delete_webhook error: {e} — continuing anyway")

        # Ждём немного чтобы старый инстанс (при редеплое) успел умереть,
        # затем делаем быстрый вызов чтобы "захватить" сессию у старого polling-а
        await asyncio.sleep(3)
        try:
            await bot.get_updates(offset=0, timeout=0, allowed_updates=[])
        except Exception:
            pass
        retry_delay = 5  # сброс задержки при успешном подключении

        while True:
            try:
                updates = await bot.get_updates(
                    offset=offset,
                    timeout=30,
                    allowed_updates=["my_chat_member", "chat_join_request", "chat_member", "message"],
                )
                for update in updates:
                    offset = update.update_id + 1
                    await _handle_child_update(bot, child_bot_id, owner_id, bot_username, update)

            except TelegramUnauthorizedError:
                logger.error(f"Child bot @{bot_username}: token revoked — stopping")
                await db.execute(
                    "UPDATE bot_chats SET is_active=false WHERE child_bot_id=$1", child_bot_id
                )
                break
            except asyncio.CancelledError:
                raise
            except Exception as e:
                logger.warning(f"Child bot @{bot_username} poll error: {e}. Retry in {retry_delay}s")
                await asyncio.sleep(retry_delay)
                retry_delay = min(retry_delay * 2, 60)

    except asyncio.CancelledError:
        logger.info(f"Child bot @{bot_username} polling cancelled")
    finally:
        await bot.session.close()


async def _handle_child_update(
    bot: Bot, child_bot_id: int, owner_id: int, bot_username: str, update: Update
):
    """Обрабатывает одно событие от дочернего бота."""
    try:
        # ── Бот добавлен/удалён из чата ──────────────────────
        if update.my_chat_member:
            await _handle_my_chat_member(bot, child_bot_id, owner_id, bot_username, update.my_chat_member)

        # ── Заявка на вступление (закрытый канал) ─────────────
        elif update.chat_join_request:
            await _handle_join_request(bot, child_bot_id, update.chat_join_request)

        # ── Пользователь вступил/вышел (открытый канал/группа) ─
        elif update.chat_member:
            await _handle_chat_member(bot, child_bot_id, update.chat_member)

        # ── Сообщение пользователя в личку бота (например, /start) ──
        elif update.message and update.message.from_user:
            await _handle_message(bot, child_bot_id, owner_id, update.message)

    except Exception as e:
        logger.error(f"Child bot @{bot_username} update error: {e}")


async def _handle_message(bot: Bot, child_bot_id: int, owner_id: int, message):
    """
    Обрабатывает сообщения пользователя в личку дочернего бота.
    /start → устанавливает bot_activated=true. Работает в обоих порядках:
      - Если юзер уже в канале → UPDATE существующей записи
      - Если юзер ещё не в канале → INSERT с bot_activated=true (is_active=false)
        При вступлении в канал ON CONFLICT DO UPDATE не трогает bot_activated.
    """
    user = message.from_user
    if not user or user.is_bot:
        return

    text = message.text or ""

    if text.startswith("/start"):
        # UPSERT: создаём/обновляем запись по каждому каналу этого бота
        chats = await db.fetch(
            "SELECT chat_id, owner_id FROM bot_chats WHERE child_bot_id=$1 AND is_active=true",
            child_bot_id,
        )
        for ch in chats:
            await db.execute(
                """
                INSERT INTO bot_users (owner_id, chat_id, user_id, username, first_name,
                                       language_code, is_premium, bot_activated, is_active)
                VALUES ($1, $2, $3, $4, $5, $6, $7, true, false)
                ON CONFLICT (owner_id, chat_id, user_id) DO UPDATE
                    SET bot_activated = true
                """,
                ch["owner_id"], ch["chat_id"], user.id,
                user.username, user.first_name or "",
                user.language_code,
                bool(getattr(user, "is_premium", False)),
            )
        logger.info(f"[START] user {user.id} activated bot {child_bot_id} ({len(chats)} channels)")

        # Подтверждение пользователю
        try:
            await bot.send_message(
                user.id,
                "✅ Вы подписались на уведомления от канала. "
                "Теперь вы будете получать приветствие и рассылки.",
            )
        except Exception as e:
            logger.debug(f"[START] send reply failed for {user.id}: {e}")



async def _handle_my_chat_member(
    bot: Bot, child_bot_id: int, owner_id: int, bot_username: str, event: ChatMemberUpdated
):
    """Бот добавлен как администратор — сохраняем площадку и уведомляем владельца."""
    new_status = event.new_chat_member.status

    if new_status in ("administrator", "creator"):
        chat = event.chat
        chat_type = chat.type  # channel | supergroup | group

        # Сохраняем в bot_chats
        await db.execute(
            """
            INSERT INTO bot_chats (owner_id, child_bot_id, chat_id, chat_title, chat_type, is_active)
            VALUES ($1, $2, $3, $4, $5, true)
            ON CONFLICT (owner_id, chat_id)
            DO UPDATE SET chat_title=EXCLUDED.chat_title,
                          child_bot_id=EXCLUDED.child_bot_id,
                          is_active=true
            """,
            owner_id, child_bot_id, chat.id,
            chat.title or f"chat_{chat.id}", chat_type,
        )

        # Уведомляем владельца через главный бот
        if _main_bot:
            type_icon = "📢" if chat_type == "channel" else "👥"
            try:
                from aiogram.types import InlineKeyboardMarkup, InlineKeyboardButton
                await _main_bot.send_message(
                    owner_id,
                    f"✅ {type_icon} <b>{chat.title}</b> подключён!\n\n"
                    f"Бот @{bot_username} добавлен как администратор и готов к работе.",
                    reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                        [InlineKeyboardButton(text="📍 Площадки бота", callback_data=f"bot_chats_list:{child_bot_id}")],
                        [InlineKeyboardButton(text="⚙️ Настройки бота", callback_data=f"bot_settings:{child_bot_id}")],
                    ]),
                    parse_mode="HTML",
                )
            except Exception as e:
                logger.error(f"Failed to notify owner {owner_id}: {e}")

        logger.info(f"Child bot @{bot_username} added to {chat.title} ({chat.id}) as {new_status}")

    elif new_status in ("kicked", "left"):
        # Бот удалён — деактивируем площадку
        await db.execute(
            "UPDATE bot_chats SET is_active=false WHERE child_bot_id=$1 AND chat_id=$2",
            child_bot_id, event.chat.id,
        )
        if _main_bot:
            try:
                await _main_bot.send_message(
                    owner_id,
                    f"⚠️ Бот @{bot_username} удалён из <b>{event.chat.title}</b>.\n"
                    f"Площадка деактивирована.",
                    parse_mode="HTML",
                )
            except Exception:
                pass


async def _handle_join_request(bot: Bot, child_bot_id: int, event: ChatJoinRequest):
    """Заявка на вступление — проверяем настройки и обрабатываем."""
    chat_id = event.chat.id
    user = event.from_user

    # Получаем настройки площадки
    chat_settings = await db.fetchrow(
        """
        SELECT autoaccept, autoaccept_delay, welcome_text, captcha_enabled,
               captcha_type, captcha_text, captcha_timer, captcha_emoji_set, owner_id
        FROM bot_chats
        WHERE child_bot_id=$1 AND chat_id=$2 AND is_active=true
        """,
        child_bot_id, chat_id,
    )
    if not chat_settings:
        return

    owner_id = chat_settings["owner_id"]

    # Сохраняем заявку в join_requests (очередь)
    await db.execute(
        """
        INSERT INTO join_requests (owner_id, chat_id, user_id, username, first_name)
        VALUES ($1, $2, $3, $4, $5)
        ON CONFLICT (owner_id, chat_id, user_id)
        DO UPDATE SET status='pending', requested_at=now(), resolved_at=NULL
        """,
        owner_id, chat_id, user.id,
        user.username or "", user.first_name or "",
    )

    captcha_enabled = chat_settings.get("captcha_enabled") or False

    # ── КАПЧА (приоритет над авто-принятием) ──────────────────
    if captcha_enabled:
        # chat_join_request даёт боту временное право писать в личку —
        # /start от пользователя НЕ требуется.
        from handlers.captcha import send_captcha
        settings_for_captcha = {
            "captcha_type":      chat_settings.get("captcha_type") or "simple",
            "captcha_text":      chat_settings.get("captcha_text"),
            "captcha_timer_min": (chat_settings.get("captcha_timer") or 60) // 60,
            "captcha_emoji_set": chat_settings.get("captcha_emoji_set"),
            "captcha_delete":    chat_settings.get("captcha_delete") or False,
            "welcome_text":      chat_settings.get("welcome_text"),
            "owner_id":          owner_id,
            "captcha_greet":     chat_settings.get("captcha_greet") or False,
            "captcha_accept_immediately": chat_settings.get("captcha_accept_immediately") or False,
        }
        await send_captcha(bot, event, settings_for_captcha)
        return  # дальнейшая обработка — в captcha.py после нажатия кнопки

    # ── АВТО-ПРИНЯТИЕ (только если капча выключена) ────────────
    autoaccept = chat_settings["autoaccept"]
    delay      = chat_settings["autoaccept_delay"] or 0

    if autoaccept:
        # Авто-принятие
        if delay > 0:
            await asyncio.sleep(delay * 60)
        try:
            await bot.approve_chat_join_request(chat_id, user.id)
            # Регистрируем в bot_users после одобрения
            await db.execute(
                """
                INSERT INTO bot_users (owner_id, chat_id, user_id, username, first_name,
                                       language_code, is_premium, joined_at)
                VALUES ($1, $2, $3, $4, $5, $6, $7, now())
                ON CONFLICT (owner_id, chat_id, user_id) DO UPDATE
                    SET is_active=true, left_at=NULL,
                        username=EXCLUDED.username,
                        is_premium=EXCLUDED.is_premium,
                        bot_activated = (bot_users.bot_activated OR EXCLUDED.bot_activated)
                """,
                owner_id, chat_id, user.id,
                user.username, user.first_name, user.language_code,
                getattr(user, "is_premium", False),
            )
            await db.execute(
                "UPDATE join_requests SET status='approved', resolved_at=now() "
                "WHERE owner_id=$1 AND chat_id=$2 AND user_id=$3",
                owner_id, chat_id, user.id,
            )
            # Приветственное сообщение
            welcome = chat_settings.get("welcome_text")
            if welcome:
                try:
                    await bot.send_message(user.id, welcome, parse_mode="HTML")
                except Exception:
                    pass  # Пользователь не открыл диалог с ботом
        except Exception as e:
            logger.warning(f"approve join request error: {e}")
    # Если autoaccept=false — заявка остаётся в очереди со статусом pending


async def _handle_chat_member(bot: Bot, child_bot_id: int, event: ChatMemberUpdated):
    """Пользователь вступил или вышел из открытого канала/группы."""
    new_status = event.new_chat_member.status
    old_status = event.old_chat_member.status if event.old_chat_member else None
    user = event.new_chat_member.user
    chat_id = event.chat.id

    # Получаем настройки площадки
    chat_settings = await db.fetchrow(
        """
        SELECT owner_id, welcome_text
        FROM bot_chats
        WHERE child_bot_id=$1 AND chat_id=$2 AND is_active=true
        """,
        child_bot_id, chat_id,
    )
    if not chat_settings:
        return

    owner_id = chat_settings["owner_id"]

    # ── Пользователь вступил ──────────────────────────────────
    if new_status == "member" and old_status in (None, "left", "kicked"):
        from services.security import detect_rtl, detect_hieroglyph

        full_name = f"{user.first_name or ''} {user.last_name or ''}".strip()
        has_rtl        = detect_rtl(full_name)
        has_hieroglyph = detect_hieroglyph(full_name)
        is_premium     = bool(getattr(user, "is_premium", False))

        LANG_TO_COUNTRY = {
            "ru": "RU", "uk": "UA", "be": "BY", "kk": "KZ",
            "en": "US", "de": "DE", "fr": "FR", "es": "ES",
            "it": "IT", "pt": "BR", "zh": "CN", "ar": "AR",
            "tr": "TR", "pl": "PL", "nl": "NL", "sv": "SE",
            "da": "DK", "fi": "FI", "no": "NO", "cs": "CZ",
            "ro": "RO", "hu": "HU", "bg": "BG",
            "fa": "IR", "he": "IL", "hi": "IN",
            "id": "ID", "ms": "MY", "th": "TH", "vi": "VN",
            "ko": "KR", "ja": "JP",
        }
        country_code = LANG_TO_COUNTRY.get(
            (user.language_code or "").split("-")[0].lower()
        )

        # Ищем ссылку по invite_link из события
        link_id = None
        if event.invite_link:
            row = await db.fetchrow(
                "SELECT id FROM invite_links WHERE link=$1",
                event.invite_link.invite_link,
            )
            if row:
                link_id = row["id"]
                if country_code:
                    await db.execute(
                        """
                        UPDATE invite_links SET
                            joined           = joined + 1,
                            rtl_count        = rtl_count + $2::int,
                            hieroglyph_count = hieroglyph_count + $3::int,
                            premium_count    = premium_count + $4::int,
                            countries        = jsonb_set(
                                COALESCE(countries, '{}'),
                                ARRAY[$5],
                                (COALESCE(countries->$5, '0')::int + 1)::text::jsonb
                            )
                        WHERE id = $1
                        """,
                        link_id,
                        int(has_rtl), int(has_hieroglyph), int(is_premium),
                        country_code,
                    )
                else:
                    await db.execute(
                        """
                        UPDATE invite_links SET
                            joined           = joined + 1,
                            rtl_count        = rtl_count + $2::int,
                            hieroglyph_count = hieroglyph_count + $3::int,
                            premium_count    = premium_count + $4::int
                        WHERE id = $1
                        """,
                        link_id,
                        int(has_rtl), int(has_hieroglyph), int(is_premium),
                    )

        await db.execute(
            """
            INSERT INTO bot_users (owner_id, chat_id, user_id, username, first_name,
                                   language_code, is_premium, has_rtl, has_hieroglyph,
                                   joined_via_link_id, joined_at)
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, now())
            ON CONFLICT (owner_id, chat_id, user_id) DO UPDATE
                SET is_active=true, left_at=NULL,
                    username=EXCLUDED.username,
                    first_name=EXCLUDED.first_name,
                    is_premium=EXCLUDED.is_premium,
                    joined_via_link_id=COALESCE(EXCLUDED.joined_via_link_id, bot_users.joined_via_link_id),
                    bot_activated = (bot_users.bot_activated OR EXCLUDED.bot_activated)
            """,
            owner_id, chat_id, user.id,
            user.username, user.first_name, user.language_code,
            is_premium, has_rtl, has_hieroglyph, link_id,
        )
        logger.info(f"[MEMBER] User {user.id} joined chat {chat_id} (owner={owner_id})")

        # Приветственное сообщение (если юзер открыл бота)
        welcome = chat_settings.get("welcome_text")
        if welcome:
            try:
                await bot.send_message(user.id, welcome, parse_mode="HTML")
            except Exception:
                pass  # Пользователь не начал диалог с ботом

    # ── Пользователь вышел/забанен ────────────────────────────
    elif new_status in ("left", "kicked") and old_status == "member":
        # Обновляем счётчик отписок для ссылки (через joined_via_link_id)
        await db.execute(
            """
            UPDATE invite_links SET unsubscribed = unsubscribed + 1
            WHERE id = (
                SELECT joined_via_link_id FROM bot_users
                WHERE owner_id=$1 AND chat_id=$2 AND user_id=$3
                  AND joined_via_link_id IS NOT NULL
            )
            """,
            owner_id, chat_id, user.id,
        )
        await db.execute(
            "UPDATE bot_users SET is_active=false, left_at=now() "
            "WHERE owner_id=$1 AND chat_id=$2 AND user_id=$3",
            owner_id, chat_id, user.id,
        )
        logger.info(f"[MEMBER] User {user.id} left chat {chat_id} (owner={owner_id})")
