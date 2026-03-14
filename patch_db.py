import asyncio
import db.pool as db

async def main():
    await db.init()
    try:
        await db.execute('ALTER TABLE autoreplies ADD COLUMN IF NOT EXISTS media_bottom BOOLEAN DEFAULT false;')
        print('Added column media_bottom to autoreplies')
    except Exception as e:
        print(f"Error: {e}")
    finally:
        await db.close()

if __name__ == "__main__":
    asyncio.run(main())
