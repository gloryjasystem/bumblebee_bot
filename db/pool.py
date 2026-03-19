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
