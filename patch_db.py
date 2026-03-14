import asyncio
import db.pool as db

async def main():
    await db.init()
    print("DB Initialized")
    try:
        await db.execute("ALTER TABLE bot_chats ADD COLUMN IF NOT EXISTS captcha_anim_file_id TEXT;")
        await db.execute("ALTER TABLE bot_chats ADD COLUMN IF NOT EXISTS captcha_anim_type VARCHAR(16);")
        print("Columns added successfully")
    except Exception as e:
        print(f"Error: {e}")
    finally:
        await db.close()

if __name__ == "__main__":
    asyncio.run(main())
