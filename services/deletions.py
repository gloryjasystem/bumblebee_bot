"""
services/deletions.py — постановка сообщений бота в очередь отложенного удаления
(«срок жизни сообщения»). Одна запись = «удалить это сообщение в момент delete_at».

Безопасность:
- Любая ошибка записи ГЛОТАЕТСЯ (try/except) — постановка в очередь второстепенна,
  отправка сообщения пользователю никогда не должна из-за неё падать.
- Пишем только то, что бот реально отправил (свои chat_id+message_id). Воркер
  scheduler.jobs.cleanup_scheduled_deletions затем удаляет по наступлении delete_at.
"""
import logging

import db.pool as db

logger = logging.getLogger(__name__)


async def enqueue_deletion(child_bot_id, chat_id: int, message_id: int, delete_after_sec: int) -> None:
    """Ставит сообщение в очередь на удаление через delete_after_sec секунд.

    child_bot_id — id дочернего бота (None = главный бот).
    chat_id — куда отправлено (обычно user_id для лички).
    Никогда не бросает исключений: сбой записи логируется и игнорируется.
    """
    try:
        sec = int(delete_after_sec or 0)
        if sec <= 0:
            return
        await db.execute(
            "INSERT INTO scheduled_deletions(child_bot_id, chat_id, message_id, delete_at) "
            "VALUES ($1, $2, $3, now() + make_interval(secs => $4))",
            child_bot_id, int(chat_id), int(message_id), sec,
        )
    except Exception as e:  # отправка первична — очередь никогда не роняет её
        logger.debug(f"enqueue_deletion skipped: {e}")
