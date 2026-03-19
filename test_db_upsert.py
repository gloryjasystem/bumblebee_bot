import asyncio
import os
import asyncpg
import logging

logging.basicConfig(level=logging.INFO)

async def main():
    url = os.environ.get("DATABASE_URL")
    if not url:
        try:
            for line in open(".env").read().splitlines():
                if line.startswith("DATABASE_URL="):
                    url = line.split("=", 1)[1].strip().strip('"').strip("'")
        except FileNotFoundError:
            pass
            
    ssl = "require" if url and "railway.app" in url else None
    conn = await asyncpg.connect(url, ssl=ssl)
    
    try:
        # Simulate what _handle_my_chat_member does
        # Get the first child bot and owner
        bot = await conn.fetchrow("SELECT id, owner_id FROM child_bots LIMIT 1")
        if not bot:
            print("No bots found")
            return
            
        owner_id = bot['owner_id']
        child_bot_id = bot['id']
        chat_id = -1001234567890 # Fake ID
        chat_title = "Test Chat"
        chat_type = "channel"
        
        print(f"Testing INSERT for owner_id={owner_id}, child_bot_id={child_bot_id}")
        
        # First insert (like when bot is first added)
        await conn.execute(
            """
            INSERT INTO bot_chats (owner_id, child_bot_id, chat_id, chat_title, chat_type, is_active, captcha_type)
            VALUES ($1, $2, $3, $4, $5, true, 'off')
            ON CONFLICT (owner_id, chat_id)
            DO UPDATE SET chat_title=EXCLUDED.chat_title,
                          child_bot_id=EXCLUDED.child_bot_id,
                          is_active=true
            """,
            owner_id, child_bot_id, chat_id, chat_title, chat_type
        )
        print("First insert successful")
        
        # Update to deactivated (like when kicked)
        await conn.execute("UPDATE bot_chats SET is_active=false WHERE chat_id=$1", chat_id)
        print("Updated to inactive")
        
        # Second insert (like when added back)
        await conn.execute(
            """
            INSERT INTO bot_chats (owner_id, child_bot_id, chat_id, chat_title, chat_type, is_active, captcha_type)
            VALUES ($1, $2, $3, $4, $5, true, 'off')
            ON CONFLICT (owner_id, chat_id)
            DO UPDATE SET chat_title=EXCLUDED.chat_title,
                          child_bot_id=EXCLUDED.child_bot_id,
                          is_active=true
            """,
            owner_id, child_bot_id, chat_id, chat_title, chat_type
        )
        print("Second insert (reactivation) successful")
        
        # Cleanup
        await conn.execute("DELETE FROM bot_chats WHERE chat_id=$1", chat_id)
        
    except Exception as e:
        print(f"Error occurred: {e}")
    finally:
        await conn.close()

if __name__ == "__main__":
    asyncio.run(main())
