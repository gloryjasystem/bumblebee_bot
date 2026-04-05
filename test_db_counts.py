import asyncio
from dotenv import load_dotenv
load_dotenv()
from db.pool import get_pool, init_pool

async def test_db():
    await init_pool()
    async with get_pool().acquire() as conn:
        bots = await conn.fetch("SELECT child_bot_id, COUNT(*) FROM blacklist WHERE child_bot_id IS NOT NULL GROUP BY child_bot_id;")
        print("Bots in blacklist:")
        bot_ids = []
        for b in bots:
            print(dict(b))
            bot_ids.append(b['child_bot_id'])
            
        if bot_ids:
            res = await conn.fetchval("""
                SELECT COUNT(DISTINCT COALESCE(user_id::text, lower(username)))
                FROM blacklist
                WHERE child_bot_id = ANY($1::int[])
                  AND child_bot_id IS NOT NULL
            """, bot_ids)
            print("Result with ANY($1::int[]):", res)
            
            res2 = await conn.fetchval(f"""
                SELECT COUNT(DISTINCT COALESCE(user_id::text, lower(username)))
                FROM blacklist
                WHERE child_bot_id IN ({','.join(map(str, bot_ids))})
                  AND child_bot_id IS NOT NULL
            """)
            print("Result with IN (...):", res2)
            
asyncio.run(test_db())
