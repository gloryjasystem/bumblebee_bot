import asyncio
import os
import asyncpg

async def main():
    url = os.environ.get("DATABASE_URL")
    if not url:
        try:
            for line in open(".env").read().splitlines():
                if line.startswith("DATABASE_URL="):
                    url = line.split("=", 1)[1].strip().strip('"').strip("'")
        except FileNotFoundError:
            pass

    if not url:
        print("❌ DATABASE_URL не найден. Установите переменную окружения или добавьте в .env")
        return

    ssl = "require" if "railway.app" in url else None

    try:
        conn = await asyncpg.connect(url, ssl=ssl)
        await conn.execute(
            "ALTER TABLE child_bots ADD COLUMN IF NOT EXISTS blocked_count BIGINT DEFAULT 0"
        )
        print("✅ Миграция применена: child_bots.blocked_count добавлен (или уже был).")
        await conn.close()
    except Exception as e:
        print(f"❌ Ошибка: {e}")

if __name__ == "__main__":
    asyncio.run(main())
