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
        # Сбрасываем webhook если был установлен
        await bot.delete_webhook(drop_pending_updates=True)
        retry_delay = 5  # сброс задержки при успешном подключении

        while True:
            try:
                updates = await bot.get_updates(
                    offset=offset,
                    timeout=30,
                    allowed_updates=["my_chat_member", "chat_join_request", "message"],
                )
                for update in updates:
                    offset = update.update_id + 1
                    await _handle_child_update(bot, child_bot_id, owner_id, bot_username, update)

            except TelegramUnauthorizedError:
                logger.error(f"Child bot @{bot_username}: token revoked — stopping")
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

        # ── Заявка на вступление ──────────────────────────────
        elif update.chat_join_request:
            await _handle_join_request(bot, child_bot_id, update.chat_join_request)

    except Exception as e:
        logger.error(f"Child bot @{bot_username} update error: {e}")


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
                        [InlineKeyboardButton(text="⚙️ Настройки", callback_data=f"channel_by_chat:{chat.id}")],
                        [InlineKeyboardButton(text="📡 Мои площадки", callback_data="menu:channels")],
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
               captcha_type, captcha_text, captcha_timer, owner_id
        FROM bot_chats
        WHERE child_bot_id=$1 AND chat_id=$2 AND is_active=true
        """,
        child_bot_id, chat_id,
    )
    if not chat_settings:
        return

    # Записываем участника в bot_users
    await db.execute(
        """
        INSERT INTO bot_users (owner_id, chat_id, user_id, username, first_name,
                               language_code, is_premium, joined_at)
        VALUES ($1, $2, $3, $4, $5, $6, $7, now())
        ON CONFLICT (owner_id, chat_id, user_id) DO UPDATE
            SET is_active=true, left_at=NULL
        """,
        chat_settings["owner_id"], chat_id, user.id,
        user.username, user.first_name, user.language_code,
        getattr(user, "is_premium", False),
    )

    autoaccept = chat_settings["autoaccept"]
    delay      = chat_settings["autoaccept_delay"] or 0

    if autoaccept:
        # Авто-принятие
        if delay > 0:
            await asyncio.sleep(delay * 60)
        try:
            await bot.approve_chat_join_request(chat_id, user.id)
            # Приветственное сообщение
            welcome = chat_settings.get("welcome_text")
            if welcome:
                try:
                    await bot.send_message(user.id, welcome, parse_mode="HTML")
                except Exception:
                    pass  # Пользователь не открыл диалог с ботом
        except Exception as e:
            logger.warning(f"approve join request error: {e}")
    # Если autoaccept=false — заявка ждёт ручного одобрения (в Telegram)
