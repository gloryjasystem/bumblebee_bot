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
            
    # Connect directly with the provided credentials or rely on env
    ssl = "require" if url and "railway.app" in url else None
    
    try:
        conn = await asyncpg.connect(url, ssl=ssl)
        
        # Test if we can read child_bots
        bots = await conn.fetch("SELECT id, bot_username FROM child_bots")
        print(f"Found {len(bots)} child bots")
        for b in bots:
            print(f"- {b['bot_username']} (ID: {b['id']})")
            
        await conn.close()
    except Exception as e:
        print(f"Error: {e}")

if __name__ == "__main__":
    asyncio.run(main())
