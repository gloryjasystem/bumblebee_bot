"""
handlers/group_events.py — Обработчик событий в группах/каналах:
  - Автоответчик: бот отвечает на сообщения по ключевым словам
  - Реакции: бот ставит реакции на сообщения пользователей
"""
import asyncio
import logging
from aiogram import Router, Bot, F
from aiogram.types import Message, ReactionTypeEmoji

import db.pool as db

logger = logging.getLogger(__name__)
router = Router()


# ── Основной обработчик сообщений в группах ────────────────────

@router.message(F.chat.type.in_({"group", "supergroup"}))
async def on_group_message(message: Message, bot: Bot):
    """
    Обрабатывает входящие сообщения в группах:
    1. Автоответчик — ищет ключевые слова и отвечает
    2. Реакции — ставит реакцию на сообщение
    """
    if not message.from_user or message.from_user.is_bot:
        return

    chat_id = message.chat.id
    user_id = message.from_user.id

    # Получаем настройки площадки через единую точку резолва.
    # Здесь известен только chat_id (главный бот в группе) — get_channel выберет строку
    # детерминированно и залогирует, если группа заведена у нескольких владельцев.
    from db.channels import get_channel
    settings = await get_channel(chat_id)
    if not settings:
        return

    text = message.text or message.caption or ""

    # ── 1. Автоответчик ────────────────────────────────────────
    if text:
        owner_id = settings["owner_id"]
        # Загружаем все правила для этой площадки
        rules = await db.fetch(
            "SELECT keyword, reply_text FROM autoreplies "
            "WHERE owner_id=$1 AND chat_id=$2::bigint",
            owner_id, chat_id,
        )
        for rule in rules:
            kw = (rule["keyword"] or "").lower().strip()
            if kw and kw in text.lower():
                try:
                    reply = await message.reply(rule["reply_text"])
                    # Общий «срок жизни» ответа автоответчика (очередь, надёжно; шлёт главный бот → None)
                    delete_min = int(settings.get("auto_delete_min") or 0)
                    if delete_min > 0:
                        from services.deletions import enqueue_deletion
                        await enqueue_deletion(None, chat_id, reply.message_id, delete_min * 60)
                except Exception as e:
                    logger.debug(f"[AUTOREPLY] failed: {e}")
                break  # только первое совпадение

    # ── 2. Реакции (перемещены в личные сообщения бота) ──


# ── Вспомогательная: отложенное удаление сообщения ────────────

async def _delete_later(bot: Bot, chat_id: int, message_id: int, delay_min: int):
    """Удаляет сообщение через delay_min минут."""
    await asyncio.sleep(delay_min * 60)
    try:
        await bot.delete_message(chat_id, message_id)
    except Exception:
        pass
