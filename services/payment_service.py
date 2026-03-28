"""
services/payment_service.py — NOWPayments API: создание инвойса, активация тарифа.
"""
import logging
import httpx
from datetime import timedelta

import db.pool as db
from config import settings
from services.discount import get_active_discount

logger = logging.getLogger(__name__)

NOWPAYMENTS_API = "https://api.nowpayments.io/v1"


async def create_invoice(user_id: int, tariff: str, period: str, currency: str = "usd", invoice_msg_id: int | None = None) -> dict:
    """
    Создаёт запись в payments и вызывает NOWPayments Invoice API.
    Возвращает {'payment_id': UUID, 'payment_url': str}.
    Используем /v1/invoice — пользователь сам выбирает валюту на странице NP.
    """
    key = f"{tariff}_{period}"
    base_amount = settings.tariff_prices.get(key)
    if not base_amount:
        raise ValueError(f"Unknown tariff/period: {key}")

    percent, _ = await get_active_discount()
    amount = base_amount
    if percent > 0:
        amount = round(base_amount * (1 - percent / 100.0), 2)

    # Создаём pending-запись в БД
    payment_id = await db.fetchval(
        """
        INSERT INTO payments (user_id, tariff, period, amount_usd, applied_discount, invoice_msg_id)
        VALUES ($1, $2, $3, $4, $5, $6) RETURNING id
        """,
        user_id, tariff, period, amount, percent, invoice_msg_id,
    )

    # Вызываем NOWPayments Invoice API (не /payment — там нужна конкретная валюта)
    payload = {
        "price_amount":      amount,
        "price_currency":    "usd",
        "order_id":          str(payment_id),
        "order_description": f"Bumblebee Bot {tariff} {period}",
        "ipn_callback_url":  f"{settings.server_url}/nowpayments/webhook",
    }
    async with httpx.AsyncClient(timeout=15) as client:
        r = await client.post(
            f"{NOWPAYMENTS_API}/invoice",
            json=payload,
            headers={"x-api-key": settings.nowpayments_api_key},
        )
        r.raise_for_status()
        np_data = r.json()

    # Сохраняем NOWPayments invoice id
    await db.execute(
        "UPDATE payments SET np_payment_id=$1 WHERE id=$2",
        str(np_data.get("id", "")), payment_id,
    )

    invoice_url = np_data.get("invoice_url")
    if not invoice_url:
        raise ValueError(f"NOWPayments returned no invoice_url: {np_data}")

    return {
        "payment_id":  str(payment_id),
        "payment_url": invoice_url,
    }


async def activate_tariff(user_id: int, tariff: str, period: str):
    """
    Активирует тариф пользователю. Три сценария:
      - Смена тарифа (Upgrade / Downgrade): новый тариф применяется немедленно. 
        Остаток дней текущего тарифа конвертируется в бонусные дни (пропорционально цене).
      - Продление: дни прибавляются к концу текущего периода.
    """
    from datetime import datetime, timezone, timedelta

    days = settings.tariff_durations.get(period, 30)

    # Иерархия тарифов
    TARIFF_RANK = {"free": 0, "start": 1, "pro": 2, "business": 3}

    now = datetime.now(timezone.utc)
    row = await db.fetchrow(
        "SELECT tariff, tariff_until FROM platform_users WHERE user_id=$1", user_id
    )

    current_tariff = (row["tariff"] if row else "free") or "free"
    current_until  = row["tariff_until"] if row else None

    current_rank = TARIFF_RANK.get(current_tariff, 0)
    new_rank     = TARIFF_RANK.get(tariff, 0)

    has_active_sub = current_until and current_until > now and current_tariff != "free"

    if not has_active_sub:
        # Нет активной подписки — простая активация от сейчас
        new_until = now + timedelta(days=days)
        final_tariff = tariff

    elif new_rank != current_rank:
        # ── СМЕНА ТАРИФА (Upgrade или Downgrade) ─────────────────────
        # Конвертируем оставшиеся дни текущего тарифа в бонусные дни нового
        remaining_days = max(0, (current_until - now).days)
        price_old = settings.tariff_prices.get(f"{current_tariff}_month", 0)
        price_new = settings.tariff_prices.get(f"{tariff}_month", 1)

        bonus_days = round(remaining_days * price_old / price_new) if price_new > 0 else 0
        new_until    = now + timedelta(days=days + bonus_days)
        final_tariff = tariff
        
        action = "Upgrade" if new_rank > current_rank else "Downgrade"
        logger.info(
            f"[{action}] {current_tariff}→{tariff} for {user_id}: "
            f"remaining {remaining_days}d → {bonus_days} bonus days"
        )

    else:
        # ── ПРОДЛЕНИЕ ТОГО ЖЕ ТАРИФА ─────────────────────────────────
        start_from   = max(now, current_until)
        new_until    = start_from + timedelta(days=days)
        final_tariff = tariff

    await db.execute(
        """
        UPDATE platform_users
        SET tariff       = $1,
            tariff_until = $2
        WHERE user_id = $3
        """,
        final_tariff, new_until, user_id,
    )

    from scheduler.child_bot_runner import sync_child_bots
    try:
        await sync_child_bots(user_id)
    except Exception as e:
        logger.error(f"Failed to sync child bots for {user_id} after payment: {e}")

    logger.info(f"Tariff {tariff}/{period} activated for user {user_id} until {new_until.strftime('%d.%m.%Y')}")


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
    from config import TARIFFS
    t_info = TARIFFS.get(tariff, TARIFFS["free"])

    try:
        await bot.send_message(
            user_id,
            f"✅ <b>Тариф {tariff_labels.get(tariff, tariff)} активирован!</b>\n\n"
            f"📅 Активен до: <b>{until}</b>\n"
            f"🤖 Ботов: до <b>{t_info['max_bots']}</b>\n"
            f"📍 Площадок на 1 бота: до <b>{t_info['max_chats_per_bot']}</b>\n"
            f"🚫 Чёрный список: до <b>{t_info['max_blacklist_users']:,}</b>\n"
            f"📨 Рассылки: <b>доступны</b>\n\n"
            f"Спасибо, что выбрал Bumblebee Bot 🙏",
            parse_mode="HTML",
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
