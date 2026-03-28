"""
handlers/payment_handler.py — Тарифы: выбор → детали → NOWPayments invoice.
"""
import logging
from aiogram import Router, F
from aiogram.types import (
    CallbackQuery, InlineKeyboardMarkup, InlineKeyboardButton, Message, WebAppInfo
)
from config import settings
import db.pool as db
from services.discount import get_active_discount

from aiogram.fsm.context import FSMContext

logger = logging.getLogger(__name__)
router = Router()


# ──────────────────────────────────────────────────────────────
# Описания тарифов
# ──────────────────────────────────────────────────────────────
TARIFF_INFO = {
    "start": {
        "icon":  "🌱",
        "name":  "Старт",
        "bots":  3,
        "desc": (
            "🌱 <b>Тариф Старт</b>\n\n"
            "• 🤖 До <b>3 ботов</b>\n"
            "• 📍 До <b>3 площадок</b> на бота\n"
            "• ⛔️ ЧС до <b>1,000</b> пользователей\n"
            "• 📨 Рассылки по базе\n"
            "• 🔗 Пригласительные ссылки\n"
            "• 🛡 Защита (капча, авто-бан)\n\n"
            "Идеально для старта.\n\n"
            "💳 Оплата: USDT / TON / BTC / ETH и другие"
        ),
    },
    "pro": {
        "icon":  "⭐",
        "name":  "Про",
        "bots":  6,
        "desc": (
            "⭐ <b>Тариф Про</b> — ПОПУЛЯРНЫЙ\n\n"
            "• 🤖 До <b>6 ботов</b>\n"
            "• 📍 До <b>5 площадок</b> на бота\n"
            "• ⛔️ ЧС до <b>10,000</b> пользователей\n"
            "• 📨 Рассылки по базе\n"
            "• 🔗 Пригласительные ссылки\n"
            "• 🛡 Полная защита\n"
            "• 👥 Команда (до 3 администраторов)\n"
            "• 📊 Расширенная аналитика\n\n"
            "Лучший выбор для активного роста.\n\n"
            "💳 Оплата: USDT / TON / BTC / ETH и другие"
        ),
    },
    "business": {
        "icon":  "💼",
        "name":  "Бизнес",
        "bots":  12,
        "desc": (
            "💼 <b>Тариф Бизнес</b>\n\n"
            "• 🤖 До <b>12 ботов</b>\n"
            "• 📍 До <b>8 площадок</b> на бота\n"
            "• ⛔️ ЧС до <b>100,000</b> пользователей\n"
            "• 📨 Приоритетные рассылки\n"
            "• 🔗 Неограниченные ссылки\n"
            "• 🛡 Полная защита\n"
            "• 👥 Команда (неограниченно)\n"
            "• 📊 Полная аналитика\n"
            "• 🎯 Персональный менеджер\n\n"
            "Для профессионалов и агентств.\n\n"
            "💳 Оплата: USDT / TON / BTC / ETH и другие"
        ),
    },
}

TARIFF_LABELS = {
    "free":     "🆓 Free",
    "start":    "🌱 Старт",
    "pro":      "⭐ Про",
    "business": "💼 Бизнес",
}


# ──────────────────────────────────────────────────────────────
# Экран 1: список тарифов
# ──────────────────────────────────────────────────────────────
@router.callback_query(F.data.in_({"menu:tariffs", "menu:tariffs_back"}))
async def on_tariffs(callback: CallbackQuery, state: FSMContext, platform_user: dict | None):
    if not platform_user:
        return
        
    # Если зашли "снаружи" (главное меню), сбрасываем возврат к боту. Если из paywall — мы установим его сами.
    if callback.data == "menu:tariffs" and not getattr(callback, "came_from_paywall", False):
        await state.update_data(return_to_bot=None)

    tariff = platform_user["tariff"]
    until = ""
    if platform_user.get("tariff_until"):
        until = f"\n📅 До: {platform_user['tariff_until'].strftime('%d.%m.%Y')}"

    buttons = []

    # Trial / Free кнопка удалена для предотвращения злоупотреблений

    percent, until_date = await get_active_discount()
    discount_text = ""
    if percent > 0 and until_date:
        discount_text = f"\n🔥 <b>Текущая акция!</b> Скидка {percent}% действует до {until_date.strftime('%d.%m.%Y')} ⏳\n"

    # Кнопки тарифов
    for key in ("start", "pro", "business"):
        info = TARIFF_INFO[key]
        mark = " ✅" if tariff == key else ""
        btn_text = f"{info['icon']} {info['name']}{mark}"
        if percent > 0 and tariff != key:
            btn_text += f" (-{percent}%)"
        buttons.append([InlineKeyboardButton(
            text=btn_text,
            callback_data=f"tariff_detail:{key}",
        )])

    data = await state.get_data()
    return_bot = data.get("return_to_bot")
    back_cd = f"bot_settings:{return_bot}" if return_bot else "menu:main"
    buttons.append([InlineKeyboardButton(text="◀️ Назад", callback_data=back_cd)])

    await callback.message.edit_text(
        f"💎 <b>Тарифы Bumblebee Bot</b>\n\n"
        f"Текущий: {TARIFF_LABELS.get(tariff, tariff)}{until}\n"
        f"{discount_text}\n"
        f"Выберите тариф для подробной информации 👇",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=buttons),
    )
    await callback.answer()


# ──────────────────────────────────────────────────────────────
# Экран 2: детали тарифа + выбор периода
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

    percent, until_date = await get_active_discount()
    desc = info["desc"]

    if percent > 0 and until_date:
        new_month = round(month_price * (1 - percent / 100.0), 2)
        new_year = round(year_price * (1 - percent / 100.0), 2)
        
        btn_month_text = f"💳 ~${month_price}~ ${new_month} / месяц"
        btn_year_text  = f"💳 ~${year_price}~ ${new_year} / год  (−29%)"
        
        desc += f"\n\n🔥 <b>Внимание!</b> Успей купить со скидкой {percent}% до {until_date.strftime('%d.%m.%Y')} ⏳"
    else:
        btn_month_text = f"💳 ${month_price} / месяц"
        btn_year_text  = f"💳 ${year_price} / год  (−29%)"

    buttons = [
        [
            InlineKeyboardButton(text=btn_month_text, callback_data=f"tariff_buy:{tariff_key}:month")
        ],
        [
            InlineKeyboardButton(text=btn_year_text, callback_data=f"tariff_buy:{tariff_key}:year")
        ],
        [InlineKeyboardButton(text="◀️ Назад", callback_data="menu:tariffs_back")]
    ]

    await callback.message.edit_text(
        desc,
        reply_markup=InlineKeyboardMarkup(inline_keyboard=buttons),
    )
    await callback.answer()


# ──────────────────────────────────────────────────────────────
# Экран 3: создание NOWPayments инвойса
# ──────────────────────────────────────────────────────────────
@router.callback_query(F.data.startswith("tariff_buy:"))
async def on_tariff_buy(callback: CallbackQuery, platform_user: dict | None):
    if not platform_user:
        return

    parts      = callback.data.split(":")
    tariff_key = parts[1]
    period     = parts[2]

    if tariff_key not in TARIFF_INFO or period not in ("month", "year"):
        await callback.answer("Неверные параметры", show_alert=True)
        return

    if not settings.server_url:
        await callback.answer("Платёжный шлюз не настроен. Напишите @support.",
                               show_alert=True)
        return

    # Передаём message_id через URL, чтобы WebApp сохранил его при создании платежа в БД
    msg_id = callback.message.message_id
    webapp_url = (
        f"{settings.server_url}/webapp/payment.html"
        f"?tariff={tariff_key}&period={period}&msg_id={msg_id}"
    )

    await callback.message.edit_reply_markup(
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(
                text="💳 Оплатить",
                web_app=WebAppInfo(url=webapp_url),
            )],
            [InlineKeyboardButton(
                text="◀️ Назад к тарифу",
                callback_data=f"tariff_detail:{tariff_key}",
            )],
        ]),
    )
    await callback.answer()


# ──────────────────────────────────────────────────────────────
# Активация trial / free (Удалено для предотвращения абуза)
# ──────────────────────────────────────────────────────────────


@router.callback_query(F.data == "noop")
async def on_noop(callback: CallbackQuery):
    await callback.answer()

@router.callback_query(F.data.startswith("paywall:"))
async def on_paywall_click(callback: CallbackQuery, state: FSMContext, platform_user: dict | None):
    parts = callback.data.split(":")
    if len(parts) > 2:
        await state.update_data(return_to_bot=parts[2])
    else:
        await state.update_data(return_to_bot=None)
        
    # Показываем сообщение и перекидываем в тарифы
    await callback.answer(
        "🔒 Доступно только на платных тарифах!\nПерейдите в меню тарифов для прокачки бота.",
        show_alert=True
    )
    # Имитируем нажатие "menu:tariffs"
    callback.came_from_paywall = True
    await on_tariffs(callback, state, platform_user)

