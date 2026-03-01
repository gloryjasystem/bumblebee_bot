"""
handlers/payment_handler.py — Экран тарифов и открытие WebApp оплаты.
"""
from aiogram import Router, F, Bot
from aiogram.types import (
    CallbackQuery, InlineKeyboardMarkup, InlineKeyboardButton, WebAppInfo
)
from config import settings
import db.pool as db

router = Router()

TARIFF_FEATURES = {
    "free": "1 площадка · Капча · Авто-бан · Trial 10 дней Про 🎁",
    "start": "3 площадки · ЧС до 10,000 · Рассылка",
    "pro": "10 площадок · ЧС до 100,000 · Лог действий",
    "business": "∞ площадок · ЧС до 1,000,000 · Персональный менеджер",
}


def _webapp_btn(text: str, tariff: str, period: str, amount: int) -> InlineKeyboardButton:
    url = f"{settings.webapp_url}?tariff={tariff}&period={period}"
    return InlineKeyboardButton(text=f"💳 ${amount} / {'мес' if period=='month' else 'год'} {'−29%' if period=='year' else ''}",
                                 web_app=WebAppInfo(url=url))


def kb_tariffs() -> InlineKeyboardMarkup:
    p = settings.tariff_prices
    return InlineKeyboardMarkup(inline_keyboard=[
        # Free
        [InlineKeyboardButton(text="🆓 Начать бесплатно", callback_data="tariff_activate:free")],
        # Старт
        [InlineKeyboardButton(text="─── 🌱 Старт ───", callback_data="noop")],
        [
            _webapp_btn("Старт мес", "start", "month", p["start_month"]),
            _webapp_btn("Старт год", "start", "year",  p["start_year"]),
        ],
        # Про
        [InlineKeyboardButton(text="─── ⭐ Про (ПОПУЛЯРНЫЙ) ───", callback_data="noop")],
        [
            _webapp_btn("Про мес",  "pro",  "month", p["pro_month"]),
            _webapp_btn("Про год",  "pro",  "year",  p["pro_year"]),
        ],
        # Бизнес
        [InlineKeyboardButton(text="─── 💼 Бизнес ───", callback_data="noop")],
        [
            _webapp_btn("Бизнес мес",  "business", "month", p["business_month"]),
            _webapp_btn("Бизнес год",  "business", "year",  p["business_year"]),
        ],
        [InlineKeyboardButton(text="◀️ Назад", callback_data="menu:main")],
    ])


def kb_trial() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="🎁 Активировать 10 дней Про бесплатно",
                               callback_data="tariff_activate:trial")],
        [InlineKeyboardButton(text="◀️ Назад", callback_data="menu:main")],
    ])


@router.callback_query(F.data == "menu:tariffs")
async def on_tariffs(callback: CallbackQuery, platform_user: dict | None):
    if not platform_user:
        return

    tariff = platform_user["tariff"]
    until  = ""
    if platform_user.get("tariff_until"):
        until = f"\n📅 До: {platform_user['tariff_until'].strftime('%d.%m.%Y')}"

    tariff_labels = {"free": "🆓 Free", "start": "🌱 Старт", "pro": "⭐ Про", "business": "💼 Бизнес"}

    # Показать кнопку триала если не использован
    trial_btn = ""
    kb = kb_tariffs()
    if not platform_user.get("trial_used") and tariff == "free":
        # Добавляем кнопку триала перед остальными
        trial_row = [InlineKeyboardButton(
            text="🎁 Попробовать 10 дней Про бесплатно",
            callback_data="tariff_activate:trial",
        )]
        kb.inline_keyboard.insert(0, trial_row)
        trial_btn = "\n\n🎁 <b>Trial 10 дней Про доступен!</b>"

    await callback.message.edit_text(
        f"💎 <b>Тарифы Bumblebee Bot</b>\n\n"
        f"Текущий: {tariff_labels.get(tariff, tariff)}{until}\n"
        f"{TARIFF_FEATURES.get(tariff, '')}"
        f"{trial_btn}\n\n"
        f"💳 Оплата: USDT / TON / ETH / BTC и другие",
        reply_markup=kb,
    )
    await callback.answer()


@router.callback_query(F.data.startswith("tariff_activate:"))
async def on_tariff_activate(callback: CallbackQuery, platform_user: dict | None):
    if not platform_user:
        return
    tariff_type = callback.data.split(":")[1]

    if tariff_type == "free":
        await callback.answer("Вы уже на Free тарифе!", show_alert=True)
        return

    if tariff_type == "trial":
        if platform_user.get("trial_used"):
            await callback.answer("Trial уже был использован", show_alert=True)
            return
        await db.execute(
            """
            UPDATE platform_users
            SET tariff='pro',
                tariff_until = now() + interval '10 days',
                trial_used = true
            WHERE user_id=$1
            """,
            platform_user["user_id"],
        )
        await callback.answer("✅ Trial 10 дней Про активирован!")
        await callback.message.edit_text(
            "🎁 <b>Trial активирован!</b>\n\n"
            "Тариф ⭐ Про активен на 10 дней.\n"
            "Используй все функции Pro и оцени бота!",
            reply_markup=InlineKeyboardMarkup(inline_keyboard=[[
                InlineKeyboardButton(text="▶ Начать работу", callback_data="menu:main")
            ]]),
        )
        return

    await callback.answer()


@router.callback_query(F.data == "noop")
async def on_noop(callback: CallbackQuery):
    await callback.answer()
