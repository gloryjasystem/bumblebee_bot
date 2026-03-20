import db.pool as db
from datetime import datetime

async def get_active_discount() -> tuple[int, datetime | None]:
    """
    Возвращает (процент_скидки, дата_окончания).
    Если скидка неактивна (процент 0 или дата истекла), возвращает (0, None).
    """
    row = await db.fetchrow(
        "SELECT discount_percent, discount_until FROM global_settings WHERE id = 1"
    )
    if not row:
        return 0, None
        
    percent = row["discount_percent"]
    until = row["discount_until"]
    
    if percent <= 0 or not until:
        return 0, None
        
    # Сравниваем naive/aware datetime, asyncpg возвращает TIMESTAMPTZ как aware datetime
    from datetime import timezone
    now = datetime.now(timezone.utc)
    if now > until:
        return 0, None
        
    return percent, until

async def set_discount(percent: int, duration_days: int = 0):
    """
    Устанавливает глобальную скидку. Если percent=0, скидка отключается.
    """
    if percent == 0:
        await db.execute(
            "UPDATE global_settings SET discount_percent = 0, discount_until = NULL WHERE id = 1"
        )
    else:
        await db.execute(
            f"UPDATE global_settings SET discount_percent = $1, discount_until = now() + interval '{duration_days} days' WHERE id = 1",
            percent
        )
