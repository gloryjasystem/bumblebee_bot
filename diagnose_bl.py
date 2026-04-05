import asyncio
import os
import sys

# Ensure the parent directory is in the path so we can import db and handlers
sys.path.append(os.getcwd())

from db.pool import get_pool, init_pool

async def diagnose():
    await init_pool()
    async with get_pool().acquire() as conn:
        print("--- Global Admin Blacklist Diagnostic ---")
        
        # 1. Check recent blacklist entries
        print("\n[1] Recent Blacklist Entries (Last 10):")
        rows = await conn.fetch("SELECT id, owner_id, user_id, username, child_bot_id, added_at FROM blacklist ORDER BY added_at DESC LIMIT 10")
        for r in rows:
            print(f"ID: {r['id']}, Owner: {r['owner_id']}, UserID: {r['user_id']}, Unames: {r['username']}, BotID: {r['child_bot_id']}, Added: {r['added_at']}")

        # 2. Check counts by child_bot_id
        print("\n[2] Blacklist Entry Counts (Records by Bot):")
        counts = await conn.fetch("SELECT child_bot_id, COUNT(*) as cnt FROM blacklist GROUP BY child_bot_id")
        for c in counts:
            print(f"Bot ID: {c['child_bot_id']} | Count: {c['cnt']}")

        # 3. Check blocked_count from child_bots
        print("\n[3] Blocked Counts (Stats from child_bots table):")
        stats = await conn.fetch("SELECT id, bot_username, blocked_count, global_blocked_count FROM child_bots WHERE blocked_count > 0 OR global_blocked_count > 0 ORDER BY blocked_count DESC LIMIT 20")
        for s in stats:
            print(f"ID: {s['id']} | @{s['bot_username']} | Local Blocks: {s['blocked_count']} | Global Blocks: {s['global_blocked_count']}")

        # 4. Check who is the Global Admin (to see their ID)
        print("\n[4] Global Admins:")
        admins = await conn.fetch("SELECT owner_id, role FROM global_admins")
        for a in admins:
            print(f"Owner ID: {a['owner_id']} | Role: {a['role']}")

        # 5. Check ga_selected_bots (to see what's in the current selection)
        print("\n[5] Selected Bots (ga_selected_bots):")
        selected = await conn.fetch("SELECT owner_id, child_bot_id FROM ga_selected_bots")
        for s in selected:
            print(f"Owner: {s['owner_id']} | Selected Bot ID: {s['child_bot_id']}")

if __name__ == "__main__":
    asyncio.run(diagnose())
