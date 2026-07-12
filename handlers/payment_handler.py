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
            "🤖 До <b>3 ботов</b>\n"
            "📍 До <b>3 площадок</b> на бота\n"
            "⛔️ Чёрный список до <b>1 000</b>\n"
            "📨 Рассылки по базе\n"
            "🔗 Пригласительные ссылки\n"
            "🛡 Защита: капча и авто-бан\n\n"
            "🔓 <b>Что даёт:</b> канал под защитой 24/7 — спам и боты не проходят. "
            "Плюс рассылки по своей базе и ссылки со статистикой. На Free этого нет.\n"
            "👤 <b>Кому подходит:</b> первый канал или небольшой проект, который "
            "хочет навести порядок и начать расти.\n\n"
            "ℹ️ Что умеет каждая функция — /help\n"
            "💳 Оплата: USDT · TON · BTC · ETH"
        ),
    },
    "pro": {
        "icon":  "⭐",
        "name":  "Про",
        "bots":  6,
        "desc": (
            "⭐ <b>Тариф Про</b> — популярный выбор\n\n"
            "🤖 До <b>6 ботов</b>\n"
            "📍 До <b>5 площадок</b> на бота\n"
            "⛔️ Чёрный список до <b>10 000</b>\n"
            "📨 Рассылки по базе\n"
            "🔗 Пригласительные ссылки\n"
            "🛡 Полная защита\n"
            "👥 Команда — до 3 администраторов\n"
            "📊 Расширенная аналитика\n\n"
            "🔓 <b>Что даёт:</b> вдвое больше ботов и в 10 раз больше чёрный список, "
            "чем на Старте, плюс работа командой и аналитика — видно, какой трафик "
            "реально приносит подписчиков.\n"
            "👤 <b>Кому подходит:</b> тем, кто ведёт несколько каналов и активно "
            "растёт.\n\n"
            "ℹ️ Что умеет каждая функция — /help\n"
            "💳 Оплата: USDT · TON · BTC · ETH"
        ),
    },
    "business": {
        "icon":  "💼",
        "name":  "Бизнес",
        "bots":  12,
        "desc": (
            "💼 <b>Тариф Бизнес</b>\n\n"
            "🤖 До <b>12 ботов</b>\n"
            "📍 До <b>8 площадок</b> на бота\n"
            "⛔️ Чёрный список до <b>100 000</b>\n"
            "📨 Приоритетные рассылки\n"
            "🔗 Неограниченные ссылки\n"
            "🛡 Полная защита\n"
            "👥 Команда без ограничений\n"
            "📊 Полная аналитика\n"
            "🎯 Персональный менеджер\n\n"
            "🔓 <b>Что даёт:</b> максимум мощности — огромные лимиты, неограниченная "
            "команда и личный менеджер, который помогает всё настроить и не терять "
            "деньги.\n"
            "👤 <b>Кому подходит:</b> агентствам и сеткам каналов, где модерация — "
            "это бизнес.\n\n"
            "ℹ️ Что умеет каждая функция — /help\n"
            "💳 Оплата: USDT · TON · BTC · ETH"
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

    # Кнопки тарифов: английские названия, без иконок тарифа; серая ☑️ у текущего.
    # Названия берём локально — TARIFF_INFO/тексты не трогаем (остаются русскими).
    en_names = {"start": "Start", "pro": "Pro", "business": "Business"}
    for key in ("start", "pro", "business"):
        mark = " ☑️" if tariff == key else ""
        btn_text = f"{en_names[key]}{mark}"
        if percent > 0 and tariff != key:
            btn_text += f" (-{percent}%)"
        buttons.append([InlineKeyboardButton(
            text=btn_text,
            callback_data=f"tariff_detail:{key}",
        )])

    data = await state.get_data()
    return_bot = data.get("return_to_bot")
    back_cd = f"bot_settings:{return_bot}" if return_bot else "menu:main"
    buttons.append([InlineKeyboardButton(text="◄ Назад", callback_data=back_cd)])

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

