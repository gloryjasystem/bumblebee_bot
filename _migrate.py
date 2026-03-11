"""
Разовый скрипт миграции — добавляет новые колонки в bot_chats.
Запуск: python _migrate.py
Требует DATABASE_URL в переменных окружения или .env
"""
import asyncio, os, asyncpg

async def main():
    url = os.environ.get("DATABASE_URL")
    if not url:
        # попробуем прочитать .env
        try:
            for line in open(".env").read().splitlines():
                if line.startswith("DATABASE_URL="):
                    url = line.split("=", 1)[1].strip().strip('"').strip("'")
        except FileNotFoundError:
            pass
    if not url:
        print("❌ DATABASE_URL не найден. Укажите его как переменную окружения.")
        return

    ssl = "require" if "railway.app" in url else None
    conn = await asyncpg.connect(url, ssl=ssl)
    try:
        await conn.execute("""
            ALTER TABLE bot_chats ADD COLUMN IF NOT EXISTS captcha_media       TEXT;
            ALTER TABLE bot_chats ADD COLUMN IF NOT EXISTS captcha_buttons_raw TEXT;
        """)
        print("✅ Миграция применена успешно!")
    finally:
        await conn.close()

asyncio.run(main())
