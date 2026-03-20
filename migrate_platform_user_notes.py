"""
Миграция: создание таблицы platform_user_notes
Запустить один раз: python migrate_platform_user_notes.py
"""
import asyncio
import os
import asyncpg


async def main():
    dsn = os.environ.get("DATABASE_URL")
    if not dsn:
        try:
            for line in open(".env").read().splitlines():
                if line.startswith("DATABASE_URL="):
                    dsn = line.split("=", 1)[1].strip().strip('"').strip("'")
        except FileNotFoundError:
            pass
    if not dsn:
        print("❌ DATABASE_URL не найден. Укажите переменную окружения или добавьте в .env")
        return
    ssl = "require" if "railway.app" in dsn else None
    conn = await asyncpg.connect(dsn, ssl=ssl)
    await conn.execute("""
        CREATE TABLE IF NOT EXISTS platform_user_notes (
            id              SERIAL PRIMARY KEY,
            owner_id        BIGINT NOT NULL,
            target_user_id  BIGINT NOT NULL,
            note            TEXT NOT NULL DEFAULT '',
            updated_at      TIMESTAMPTZ DEFAULT now(),
            UNIQUE (owner_id, target_user_id)
        )
    """)
    print("✅ Таблица platform_user_notes создана (или уже существовала).")
    await conn.close()


if __name__ == "__main__":
    asyncio.run(main())
