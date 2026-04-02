"""
db/pool.py — asyncpg connection pool.
Инициализируется один раз при старте, используется через get_pool().
"""
import asyncpg
from typing import Optional
from config import settings

_pool: Optional[asyncpg.Pool] = None


async def create_pool() -> asyncpg.Pool:
    global _pool
    _pool = await asyncpg.create_pool(
        settings.database_url,
        min_size=2,
        max_size=10,
        command_timeout=30,
        ssl="require" if "railway.app" in settings.database_url else None,
    )
    # Авто-миграции: безопасны при повторном запуске (IF NOT EXISTS)
    async with _pool.acquire() as conn:
        await conn.execute(
            "ALTER TABLE child_bots ADD COLUMN IF NOT EXISTS blocked_count BIGINT DEFAULT 0"
        )
        await conn.execute(
            "ALTER TABLE blacklist ADD COLUMN IF NOT EXISTS child_bot_id INTEGER REFERENCES child_bots(id) ON DELETE CASCADE"
        )
        # Пересоздаём уникальные индексы с учётом child_bot_id
        # Старые индексы (owner_id, user_id/username) блокируют per-bot добавление ON CONFLICT
        import logging
        logger = logging.getLogger(__name__)
        # Старые индексы (owner_id, user_id/username) блокируют per-bot добавление ON CONFLICT
        try:
            await conn.execute("ALTER TABLE blacklist DROP CONSTRAINT IF EXISTS idx_bl_user_id CASCADE")
        except Exception as e:
            logger.error(f"Failed DROP CONSTRAINT idx_bl_user_id: {e}")
        try:
            await conn.execute("DROP INDEX IF EXISTS idx_bl_user_id CASCADE")
        except Exception as e:
            logger.error(f"Failed DROP INDEX idx_bl_user_id: {e}")
            
        try:
            await conn.execute("ALTER TABLE blacklist DROP CONSTRAINT IF EXISTS idx_bl_username CASCADE")
        except Exception as e:
            logger.error(f"Failed DROP CONSTRAINT idx_bl_username: {e}")
        try:
            await conn.execute("DROP INDEX IF EXISTS idx_bl_username CASCADE")
        except Exception as e:
            logger.error(f"Failed DROP INDEX idx_bl_username: {e}")
        # Глобальные записи (global admin, child_bot_id IS NULL)
        await conn.execute(
            """CREATE UNIQUE INDEX IF NOT EXISTS idx_bl_uid_global
               ON blacklist(owner_id, user_id)
               WHERE user_id IS NOT NULL AND child_bot_id IS NULL"""
        )
        await conn.execute(
            """CREATE UNIQUE INDEX IF NOT EXISTS idx_bl_uname_global
               ON blacklist(owner_id, lower(username))
               WHERE username IS NOT NULL AND child_bot_id IS NULL"""
        )
        # Per-bot записи (child_bot_id IS NOT NULL)
        await conn.execute(
            """CREATE UNIQUE INDEX IF NOT EXISTS idx_bl_uid_bot
               ON blacklist(owner_id, child_bot_id, user_id)
               WHERE user_id IS NOT NULL AND child_bot_id IS NOT NULL"""
        )
        await conn.execute(
            """CREATE UNIQUE INDEX IF NOT EXISTS idx_bl_uname_bot
               ON blacklist(owner_id, child_bot_id, lower(username))
               WHERE username IS NOT NULL AND child_bot_id IS NOT NULL"""
        )

        # ── Новые колонки blacklist для трекинга RapidAPI-резолва ─────────────
        # source_username — оригинальный @username до конвертации в ID
        await conn.execute(
            "ALTER TABLE blacklist ADD COLUMN IF NOT EXISTS source_username TEXT"
        )
        # resolve_error — причина, по которой RapidAPI не смог найти пользователя
        await conn.execute(
            "ALTER TABLE blacklist ADD COLUMN IF NOT EXISTS resolve_error TEXT"
        )

        # ── Таблица platform_settings (key-value настройки платформы) ─────────
        # Используется для хранения RapidAPI ключей и остатка квоты без хардкода.
        # IF NOT EXISTS — безопасно запускать повторно при каждом старте бота.
        await conn.execute(
            """
            CREATE TABLE IF NOT EXISTS platform_settings (
                key        TEXT PRIMARY KEY,
                value      TEXT NOT NULL,
                updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
            )
            """
        )
        # Начальные значения RapidAPI — вставляем только если строки ещё нет.
        # Администратор заменит их через Admin UI без перезапуска бота.
        await conn.execute(
            """
            INSERT INTO platform_settings (key, value) VALUES
                ('rapidapi_key',              'YOUR_KEY_HERE'),
                ('rapidapi_host',             'telegram124.p.rapidapi.com'),
                ('rapidapi_url',              'https://telegram124.p.rapidapi.com/telegram/api/userInfo'),
                ('rapidapi_rpm',              '38'),
                ('rapidapi_quota_remaining',  '-1')
            ON CONFLICT (key) DO NOTHING
            """
        )
        logger.info("[DB] platform_settings table ready")

        # ── invoice_msg_id для удаления сообщения с кнопками после оплаты ──────
        await conn.execute(
            "ALTER TABLE payments ADD COLUMN IF NOT EXISTS invoice_msg_id BIGINT"
        )
        await conn.execute(
            "ALTER TABLE payments ADD COLUMN IF NOT EXISTS applied_discount SMALLINT DEFAULT 0"
        )

    return _pool



async def close_pool():
    global _pool
    if _pool:
        await _pool.close()
        _pool = None


def get_pool() -> asyncpg.Pool:
    if _pool is None:
        raise RuntimeError("DB pool not initialized. Call create_pool() first.")
    return _pool


# ── Хелперы для удобного использования ──────────────────────
async def fetchrow(query: str, *args):
    async with get_pool().acquire() as conn:
        return await conn.fetchrow(query, *args)


async def fetch(query: str, *args):
    async with get_pool().acquire() as conn:
        return await conn.fetch(query, *args)


async def fetchval(query: str, *args):
    async with get_pool().acquire() as conn:
        return await conn.fetchval(query, *args)


async def execute(query: str, *args):
    async with get_pool().acquire() as conn:
        return await conn.execute(query, *args)


async def executemany(query: str, args_list: list):
    async with get_pool().acquire() as conn:
        return await conn.executemany(query, args_list)
