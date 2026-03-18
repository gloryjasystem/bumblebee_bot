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
        print('❌ DATABASE_URL не найден.')
        return

    ssl = 'require' if 'railway.app' in url else None
    conn = await asyncpg.connect(url, ssl=ssl)
    try:
        await conn.execute("""
-- Глобальные администраторы
CREATE TABLE IF NOT EXISTS global_admins (
    id              SERIAL PRIMARY KEY,
    owner_id        BIGINT REFERENCES platform_users(user_id) ON DELETE CASCADE,
    admin_id        BIGINT NOT NULL,
    admin_username  VARCHAR(64),
    added_at        TIMESTAMPTZ DEFAULT now(),
    UNIQUE(owner_id, admin_id)
);

-- Фиксация того, кто выдал бан
ALTER TABLE blacklist ADD COLUMN IF NOT EXISTS added_by BIGINT;

-- Использование глобального ЧС для конкретного бота
ALTER TABLE child_bots ADD COLUMN IF NOT EXISTS use_global_blacklist BOOLEAN DEFAULT false;

-- Настройка умного Авто-Бана на уровне владельца
ALTER TABLE platform_users ADD COLUMN IF NOT EXISTS global_auto_ban BOOLEAN DEFAULT false;
        """)
        print('✅ Миграция Экосистемы применена успешно!')
    finally:
        await conn.close()

asyncio.run(main())
