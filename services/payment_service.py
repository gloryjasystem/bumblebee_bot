"""
services/payment_service.py — NOWPayments API: создание инвойса, активация тарифа.
"""
import logging
import httpx
from datetime import timedelta

import db.pool as db
from config import settings

logger = logging.getLogger(__name__)

NOWPAYMENTS_API = "https://api.nowpayments.io/v1"


async def create_invoice(user_id: int, tariff: str, period: str, currency: str) -> dict:
    """
    Создаёт запись в payments и вызывает NOWPayments API.
    Возвращает {'payment_id': UUID, 'payment_url': str}.
    """
    key = f"{tariff}_{period}"
    amount = settings.tariff_prices.get(key)
    if not amount:
        raise ValueError(f"Unknown tariff/period: {key}")

    # Создаём pending-запись в БД
    payment_id = await db.fetchval(
        """
        INSERT INTO payments (user_id, tariff, period, amount_usd, currency)
        VALUES ($1, $2, $3, $4, $5) RETURNING id
        """,
        user_id, tariff, period, amount, currency,
    )

    # Вызываем NOWPayments
    payload = {
        "price_amount":      amount,
        "price_currency":    "usd",
        "pay_currency":      currency,
        "order_id":          str(payment_id),
        "order_description": f"Bumblebee Bot {tariff} {period}",
        "ipn_callback_url":  f"{settings.server_url}/nowpayments/webhook",
    }
    async with httpx.AsyncClient(timeout=15) as client:
        r = await client.post(
            f"{NOWPAYMENTS_API}/payment",
            json=payload,
            headers={"x-api-key": settings.nowpayments_api_key},
        )
        r.raise_for_status()
        np_data = r.json()

    # Сохраняем NOWPayments payment_id
    await db.execute(
        "UPDATE payments SET np_payment_id=$1 WHERE id=$2",
        str(np_data["payment_id"]), payment_id,
    )

    return {
        "payment_id":  str(payment_id),
        "payment_url": np_data.get("invoice_url") or np_data.get("payment_url"),
    }


async def activate_tariff(user_id: int, tariff: str, period: str):
    """
    Активирует тариф пользователю.
    Если тариф уже активен — продлевает от текущего конца.
    """
    days = settings.tariff_durations.get(period, 30)
    await db.execute(
        """
        UPDATE platform_users
        SET tariff       = $1,
            tariff_until = GREATEST(now(), COALESCE(tariff_until, now())) + ($2 || ' days')::interval
        WHERE user_id = $3
        """,
        tariff, str(days), user_id,
    )
    logger.info(f"Tariff {tariff}/{period} activated for user {user_id}")


async def get_payment_status(payment_id: str) -> dict:
    """Возвращает статус платежа из БД."""
    row = await db.fetchrow(
        "SELECT status FROM payments WHERE id=$1", payment_id
    )
    if not row:
        return {"status": "not_found", "paid": False}
    return {"status": row["status"], "paid": row["status"] == "paid"}


async def notify_user_paid(bot, user_id: int, tariff: str, period: str):
    """Уведомляет пользователя об успешной оплате."""
    from aiogram.types import InlineKeyboardMarkup, InlineKeyboardButton
    row = await db.fetchrow(
        "SELECT tariff_until FROM platform_users WHERE user_id=$1", user_id
    )
    until = row["tariff_until"].strftime("%d.%m.%Y") if row and row["tariff_until"] else "—"

    tariff_labels = {"start": "🌱 Старт", "pro": "⭐ Про", "business": "💼 Бизнес"}
    limits = settings.channel_limits
    bl_limits = settings.blacklist_limits

    try:
        await bot.send_message(
            user_id,
            f"━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
            f"✅ <b>Оплата успешно получена!</b>\n\n"
            f"Тариф {tariff_labels.get(tariff, tariff)} активирован.\n\n"
            f"📅 Активен до: {until}\n"
            f"📡 Площадок: до {limits.get(tariff, 1)}\n"
            f"🚫 Чёрный список: до {bl_limits.get(tariff, 0):,}\n"
            f"📨 Рассылка: ✅ доступна\n\n"
            f"Спасибо, что выбрал Bumblebee Bot 🙏\n"
            f"━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━",
            reply_markup=InlineKeyboardMarkup(inline_keyboard=[[
                InlineKeyboardButton(text="🚀 Открыть панель", callback_data="menu:main")
            ]]),
        )
    except Exception as e:
        logger.warning(f"Failed to notify user {user_id} about payment: {e}")


async def notify_owner_payment(bot, user_id: int, np_data: dict):
    """Уведомляет владельца платформы о новой оплате."""
    owner_id = settings.owner_telegram_id
    try:
        user = await db.fetchrow(
            "SELECT username, tariff FROM platform_users WHERE user_id=$1", user_id
        )
        username = f"@{user['username']}" if user and user.get("username") else str(user_id)
        tariff = user["tariff"] if user else "?"
        amount = np_data.get("price_amount", "?")
        currency = np_data.get("pay_currency", "?").upper()

        await bot.send_message(
            owner_id,
            f"━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
            f"💰 <b>Новая оплата</b>\n\n"
            f"👤 {username} (ID: {user_id})\n"
            f"💎 Тариф: {tariff}\n"
            f"💵 Сумма: ${amount} {currency}\n"
            f"🔑 NP ID: {np_data.get('payment_id', '?')}\n"
            f"✅ Тариф активирован автоматически\n"
            f"━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━",
        )
    except Exception:
        pass
