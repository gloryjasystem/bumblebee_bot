import asyncio
import asyncpg
import sys
import os

# Read DATABASE_URL directly from environment — no need for full settings
DATABASE_URL = os.environ.get("DATABASE_URL")
if not DATABASE_URL:
    # Try reading from .env file if it exists
    env_file = os.path.join(os.path.dirname(__file__), ".env")
    if os.path.exists(env_file):
        for line in open(env_file):
            line = line.strip()
            if line.startswith("DATABASE_URL="):
                DATABASE_URL = line.split("=", 1)[1].strip().strip('"').strip("'")
                break

if not DATABASE_URL:
    print("❌ DATABASE_URL not found in environment or .env file")
    sys.exit(1)


async def main():
    conn = await asyncpg.connect(DATABASE_URL)
    try:
        print("=== migrate_ga_shared_selection ===")

        # Step 1: Convert sub-admin admin_ids to owner_ids via global_admins
        updated = await conn.execute("""
            UPDATE ga_selected_bots gsb
            SET admin_id = ga.owner_id
            FROM global_admins ga
            WHERE gsb.admin_id = ga.admin_id
        """)
        print(f"Step 1 — sub-admin rows converted: {updated}")

        # Step 2: Remove duplicates (same owner_id + child_bot_id after conversion)
        deleted = await conn.execute("""
            DELETE FROM ga_selected_bots a
            USING ga_selected_bots b
            WHERE a.ctid < b.ctid
              AND a.admin_id = b.admin_id
              AND a.child_bot_id = b.child_bot_id
        """)
        print(f"Step 2 — duplicate rows removed: {deleted}")

        # Step 3: Drop old PK, rename column, add new PK
        await conn.execute("ALTER TABLE ga_selected_bots DROP CONSTRAINT ga_selected_bots_pkey")
        print("Step 3a — old PRIMARY KEY dropped")

        await conn.execute("ALTER TABLE ga_selected_bots RENAME COLUMN admin_id TO owner_id")
        print("Step 3b — column renamed: admin_id → owner_id")

        await conn.execute("ALTER TABLE ga_selected_bots ADD PRIMARY KEY (owner_id, child_bot_id)")
        print("Step 3c — new PRIMARY KEY (owner_id, child_bot_id) added")

        # Step 4: Rebuild index
        await conn.execute("DROP INDEX IF EXISTS idx_ga_selected_bots_admin")
        await conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_ga_selected_bots_owner ON ga_selected_bots(owner_id)"
        )
        print("Step 4 — index rebuilt: idx_ga_selected_bots_owner")

        # Verify
        count = await conn.fetchval("SELECT COUNT(*) FROM ga_selected_bots")
        print(f"\n✅ Migration complete. ga_selected_bots rows remaining: {count}")

    except Exception as e:
        print(f"❌ Migration FAILED: {e}")
        raise
    finally:
        await conn.close()


if __name__ == "__main__":
    asyncio.run(main())
