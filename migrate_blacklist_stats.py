import asyncio
import os
import asyncpg


async def main():
    url = os.environ.get('DATABASE_URL')
    if not url:
        try:
            for line in open('.env').read().splitlines():
                if line.startswith('DATABASE_URL='):
                    url = line.split('=', 1)[1].strip().strip('"').strip("'")
        except FileNotFoundError:
            pass

    if not url:
        print('❌ DATABASE_URL не найден. Укажите переменную окружения или добавьте в .env')
        return

    ssl = 'require' if 'railway.app' in url else None
    conn = await asyncpg.connect(url, ssl=ssl)
    try:
        await conn.execute("""
-- Счётчик сработавших блокировок ЧС (отклонённые заявки + кики из каналов)
ALTER TABLE platform_users ADD COLUMN IF NOT EXISTS blocked_count BIGINT DEFAULT 0;
        """)
        print('✅ Миграция применена: колонка blocked_count добавлена (или уже была).')
    finally:
        await conn.close()


asyncio.run(main())
