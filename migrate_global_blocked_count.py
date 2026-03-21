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

    print("Подключаемся к базе данных...")
    try:
        conn = await asyncpg.connect(url, ssl=ssl)
        
        print("Внедряем колонку global_blocked_count...")
        await conn.execute("ALTER TABLE child_bots ADD COLUMN IF NOT EXISTS global_blocked_count INTEGER DEFAULT 0;")
        print("✅ Колонка global_blocked_count добавлена (или уже была).")

        print("Начинаем миграцию существующих счетчиков...")
        res = await conn.execute("UPDATE child_bots SET global_blocked_count = blocked_count WHERE global_blocked_count = 0 AND blocked_count > 0;")
        print(f"✅ Миграция завершена. {res}")

        await conn.close()
    except Exception as e:
        print(f"❌ Ошибка миграции: {e}")

if __name__ == "__main__":
    asyncio.run(main())
