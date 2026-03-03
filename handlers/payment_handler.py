"""
handlers/payment_handler.py — Экран тарифов (2-уровневый) и оплата через WebApp.
"""
from aiogram import Router, F
from aiogram.types import (
    CallbackQuery, InlineKeyboardMarkup, InlineKeyboardButton, WebAppInfo
)
from config import settings
import db.pool as db

router = Router()


# ──────────────────────────────────────────────────────────────
# Описания тарифов
# ──────────────────────────────────────────────────────────────
TARIFF_INFO = {
    "start": {
        "icon":  "🌱",
        "name":  "Старт",
        "bots":  2,
        "desc": (
            "🌱 <b>Тариф Старт</b>\n\n"
            "• 🤖 До <b>2 ботов</b>\n"
            "• 📍 До <b>3 площадок</b> на бота\n"
            "• ⛔️ ЧС до <b>10,000</b> пользователей\n"
            "• 📨 Рассылки по базе\n"
            "• 🔗 Пригласительные ссылки\n"
            "• 🛡 Защита (капча, авто-бан)\n\n"
            "Идеально для старта.\n\n"
            "💳 Оплата: USDT / TON / ETH и другие"
        ),
    },
    "pro": {
        "icon":  "⭐",
        "name":  "Про",
        "bots":  4,
        "desc": (
            "⭐ <b>Тариф Про</b> — ПОПУЛЯРНЫЙ\n\n"
            "• 🤖 До <b>4 ботов</b>\n"
            "• 📍 До <b>10 площадок</b> на бота\n"
            "• ⛔️ ЧС до <b>100,000</b> пользователей\n"
            "• 📨 Рассылки по базе\n"
            "• 🔗 Пригласительные ссылки\n"
            "• 🛡 Полная защита\n"
            "• 📋 Лог действий\n"
            "• 👥 Команда (до 3 администраторов)\n"
            "• 📊 Расширенная аналитика\n\n"
            "Лучший выбор для активного роста.\n\n"
            "💳 Оплата: USDT / TON / ETH и другие"
        ),
    },
    "business": {
        "icon":  "💼",
        "name":  "Бизнес",
        "bots":  10,
        "desc": (
            "💼 <b>Тариф Бизнес</b>\n\n"
            "• 🤖 До <b>10 ботов</b>\n"
            "• 📍 <b>∞ площадок</b> на бота\n"
            "• ⛔️ ЧС до <b>1,000,000</b> пользователей\n"
            "• 📨 Приоритетные рассылки\n"
            "• 🔗 Неограниченные ссылки\n"
            "• 🛡 Полная защита\n"
            "• 📋 Лог действий\n"
            "• 👥 Команда (неограниченно)\n"
            "• 📊 Полная аналитика\n"
            "• 🎯 Персональный менеджер\n\n"
            "Для профессионалов и агентств.\n\n"
            "💳 Оплата: USDT / TON / ETH и другие"
        ),
    },
}

TARIFF_LABELS = {
    "free":     "🆓 Free",
    "start":    "🌱 Старт",
    "pro":      "⭐ Про",
    "business": "💼 Бизнес",
}


def _webapp_btn(text: str, tariff: str, period: str, amount: int) -> InlineKeyboardButton:
    url = f"{settings.webapp_url}?tariff={tariff}&period={period}"
    label = f"💳 ${amount} / {'мес' if period == 'month' else 'год −29%'}"
    return InlineKeyboardButton(text=label, web_app=WebAppInfo(url=url))


# ──────────────────────────────────────────────────────────────
# Экран 1: список тарифов
# ──────────────────────────────────────────────────────────────
@router.callback_query(F.data == "menu:tariffs")
async def on_tariffs(callback: CallbackQuery, platform_user: dict | None):
    if not platform_user:
        return

    tariff = platform_user["tariff"]
    until = ""
    if platform_user.get("tariff_until"):
        until = f"\n📅 До: {platform_user['tariff_until'].strftime('%d.%m.%Y')}"

    buttons = []

    # Trial / Free кнопка
    if not platform_user.get("trial_used") and tariff == "free":
        buttons.append([InlineKeyboardButton(
            text="🎁 Попробовать 10 дней Про бесплатно",
            callback_data="tariff_activate:trial",
        )])
    elif tariff == "free":
        buttons.append([InlineKeyboardButton(
            text="🆓 Начать бесплатно",
            callback_data="tariff_activate:free",
        )])

    # Кнопки тарифов
    for key in ("start", "pro", "business"):
        info = TARIFF_INFO[key]
        mark = " ✅" if tariff == key else ""
        buttons.append([InlineKeyboardButton(
            text=f"{info['icon']} {info['name']}{mark}",
            callback_data=f"tariff_detail:{key}",
        )])

    buttons.append([InlineKeyboardButton(text="◀️ Назад", callback_data="menu:main")])

    await callback.message.edit_text(
        f"💎 <b>Тарифы Bumblebee Bot</b>\n\n"
        f"Текущий: {TARIFF_LABELS.get(tariff, tariff)}{until}\n\n"
        f"Выберите тариф для подробной информации 👇",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=buttons),
    )
    await callback.answer()


# ──────────────────────────────────────────────────────────────
# Экран 2: детали тарифа + кнопки оплаты
# ──────────────────────────────────────────────────────────────
@router.callback_query(F.data.startswith("tariff_detail:"))
async def on_tariff_detail(callback: CallbackQuery, platform_user: dict | None):
    if not platform_user:
        return
    tariff_key = callback.data.split(":")[1]
    info = TARIFF_INFO.get(tariff_key)
    if not info:
        await callback.answer()
        return

    p = settings.tariff_prices
    month_price = p.get(f"{tariff_key}_month", 0)
    year_price  = p.get(f"{tariff_key}_year", 0)

    await callback.message.edit_text(
        info["desc"],
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [_webapp_btn("Месяц", tariff_key, "month", month_price)],
            [_webapp_btn("Год",   tariff_key, "year",  year_price)],
            [InlineKeyboardButton(text="◀️ Назад", callback_data="menu:tariffs")],
        ]),
    )
    await callback.answer()


# ──────────────────────────────────────────────────────────────
# Активация trial / free
# ──────────────────────────────────────────────────────────────
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
