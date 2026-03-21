import asyncio
import asyncpg
import sys
import os

# Read DATABASE_URL directly from environment — no need for full settings
DATABASE_URL = os.environ.get("DATABASE_URL")
if not DATABASE_URL:
    env_file = os.path.join(os.path.dirname(__file__), ".env")
    if os.path.exists(env_file):
        for line in open(env_file):
            line = line.strip()
            if line.startswith("DATABASE_URL="):
                DATABASE_URL = line.split("=", 1)[1].strip().strip('"').strip("'")
                break

if not DATABASE_URL:
    print("⚠️  DATABASE_URL not set — skipping migration")
    sys.exit(0)  # Exit 0 so bot.py still starts


async def _column_exists(conn, table: str, column: str) -> bool:
    return await conn.fetchval(
        """
        SELECT EXISTS (
            SELECT 1 FROM information_schema.columns
            WHERE table_name=$1 AND column_name=$2
        )
        """,
        table, column,
    )


async def main():
    try:
        conn = await asyncpg.connect(DATABASE_URL)
    except Exception as e:
        print(f"⚠️  Could not connect to DB for migration: {e}")
        return  # Don't crash — bot.py will fail with its own error

    try:
        print("=== migrate_ga_shared_selection ===")

        # Guard: if admin_id column no longer exists, migration was already applied
        if not await _column_exists(conn, "ga_selected_bots", "admin_id"):
            print("✅ Already migrated (admin_id column not found) — skipping.")
            return

        # Step 1: Convert sub-admin admin_ids to owner_ids
        updated = await conn.execute("""
            UPDATE ga_selected_bots gsb
            SET admin_id = ga.owner_id
            FROM global_admins ga
            WHERE gsb.admin_id = ga.admin_id
        """)
        print(f"Step 1 — sub-admin rows converted: {updated}")

        # Step 2: Remove duplicates
        deleted = await conn.execute("""
            DELETE FROM ga_selected_bots a
            USING ga_selected_bots b
            WHERE a.ctid < b.ctid
              AND a.admin_id = b.admin_id
              AND a.child_bot_id = b.child_bot_id
        """)
        print(f"Step 2 — duplicate rows removed: {deleted}")

        # Step 3: Drop old PK, rename column, add new PK
        try:
            await conn.execute("ALTER TABLE ga_selected_bots DROP CONSTRAINT ga_selected_bots_pkey")
            print("Step 3a — old PRIMARY KEY dropped")
        except Exception as e:
            print(f"Step 3a — skipped (PK already gone: {e})")

        await conn.execute("ALTER TABLE ga_selected_bots RENAME COLUMN admin_id TO owner_id")
        print("Step 3b — column renamed: admin_id → owner_id")

        try:
            await conn.execute("ALTER TABLE ga_selected_bots ADD PRIMARY KEY (owner_id, child_bot_id)")
            print("Step 3c — new PRIMARY KEY added")
        except Exception as e:
            print(f"Step 3c — skipped (PK already exists: {e})")

        # Step 4: Rebuild index
        await conn.execute("DROP INDEX IF EXISTS idx_ga_selected_bots_admin")
        await conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_ga_selected_bots_owner ON ga_selected_bots(owner_id)"
        )
        print("Step 4 — index rebuilt")

        count = await conn.fetchval("SELECT COUNT(*) FROM ga_selected_bots")
        print(f"\n✅ Migration complete. ga_selected_bots rows: {count}")

    except Exception as e:
        print(f"⚠️  Migration error (non-fatal): {e}")
        # Do NOT raise — bot must start regardless
    finally:
        await conn.close()


if __name__ == "__main__":
    asyncio.run(main())
