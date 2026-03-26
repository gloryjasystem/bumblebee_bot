import asyncio, os, asyncpg

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
        print("❌ DATABASE_URL не найден.")
        return

    ssl = "require" if "railway.app" in url else None
    conn = await asyncpg.connect(url, ssl=ssl)
    try:
        await conn.execute("""
            ALTER TABLE platform_users ADD COLUMN IF NOT EXISTS is_banned BOOLEAN DEFAULT false;
            ALTER TABLE platform_users ADD COLUMN IF NOT EXISTS ban_reason TEXT;
            ALTER TABLE platform_users ADD COLUMN IF NOT EXISTS banned_at TIMESTAMPTZ;
        """)
        print("✅ Миграция применена успешно!")
    finally:
        await conn.close()

asyncio.run(main())
