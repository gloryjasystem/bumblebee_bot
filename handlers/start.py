"""
handlers/start.py — /start, выбор языка, главное меню.
"""
from aiogram import Router, F
from aiogram.filters import CommandStart
from aiogram.types import Message, CallbackQuery, InlineKeyboardMarkup, InlineKeyboardButton

import db.pool as db
from config import settings

router = Router()


# ── Клавиатуры ───────────────────────────────────────────────
def kb_language() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="🇷🇺 Русский", callback_data="lang:ru")],
        [InlineKeyboardButton(text="🇺🇸 English", callback_data="lang:en")],
    ])


def kb_main_menu() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [
            InlineKeyboardButton(text="📡 Мои площадки", callback_data="menu:channels"),
            InlineKeyboardButton(text="📨 Рассылка",     callback_data="menu:mailing"),
        ],
        [
            InlineKeyboardButton(text="💳 Тарифы",       callback_data="menu:tariffs"),
            InlineKeyboardButton(text="🔑 Управление",   callback_data="menu:settings"),
        ],
    ])


# ── /start ───────────────────────────────────────────────────
@router.message(CommandStart())
async def cmd_start(message: Message, platform_user: dict | None):
    user = message.from_user

    if platform_user is None:
        # Новый пользователь — регистрируем и показываем выбор языка
        await db.execute(
            """
            INSERT INTO platform_users (user_id, username, first_name)
            VALUES ($1, $2, $3)
            ON CONFLICT (user_id) DO NOTHING
            """,
            user.id, user.username, user.first_name,
        )
        await message.answer(
            "👋 Добро пожаловать в <b>Bumblebee Bot</b>!\n\nВыберите язык / Choose language:",
            reply_markup=kb_language(),
        )
    else:
        # Существующий пользователь — сразу в меню
        await _show_main_menu(message, platform_user)


# ── Выбор языка ───────────────────────────────────────────────
@router.callback_query(F.data.startswith("lang:"))
async def on_language_select(callback: CallbackQuery, platform_user: dict | None):
    lang = callback.data.split(":")[1]  # ru | en
    await db.execute(
        "UPDATE platform_users SET language=$1 WHERE user_id=$2",
        lang, callback.from_user.id,
    )

    # Проверяем триал
    puser = await db.fetchrow(
        "SELECT trial_used FROM platform_users WHERE user_id=$1",
        callback.from_user.id,
    )
    if puser and not puser["trial_used"]:
        # Активируем Trial 10 дней (тариф Про)
        await db.execute(
            """
            UPDATE platform_users
            SET tariff='pro',
                tariff_until = now() + interval '10 days',
                trial_used = true
            WHERE user_id=$1
            """,
            callback.from_user.id,
        )
        trial_msg = "\n\n🎁 <b>10 дней Про-тарифа активированы бесплатно!</b>"
    else:
        trial_msg = ""

    text = "🇷🇺 Язык установлен: Русский" if lang == "ru" else "🇺🇸 Language set: English"
    await callback.message.edit_text(
        f"{text}{trial_msg}\n\n⇨ Главное меню",
        reply_markup=kb_main_menu(),
    )
    await callback.answer()


# ── Главное меню ──────────────────────────────────────────────
@router.callback_query(F.data == "menu:main")
async def on_main_menu(callback: CallbackQuery, platform_user: dict | None):
    await _show_main_menu(callback.message, platform_user)
    await callback.answer()


async def _show_main_menu(message: Message, platform_user: dict | None):
    tariff = platform_user["tariff"] if platform_user else "free"
    tariff_labels = {
        "free":     "🆓 Free",
        "start":    "🌱 Старт",
        "pro":      "⭐ Про",
        "business": "💼 Бизнес",
    }
    label = tariff_labels.get(tariff, "🆓 Free")
    until = ""
    if platform_user and platform_user.get("tariff_until"):
        until = f" · до {platform_user['tariff_until'].strftime('%d.%m.%Y')}"

    await message.answer(
        f"⚡ <b>Bumblebee Bot</b> — ваш главный помощник для работы с трафиком.\n\n"
        f"Тариф: {label}{until}\n\n"
        f"⇨ Главное меню",
        reply_markup=kb_main_menu(),
    )


# ── Управление аккаунтом ──────────────────────────────────────
@router.callback_query(F.data == "menu:settings")
async def on_settings_menu(callback: CallbackQuery, platform_user: dict | None):
    if not platform_user:
        await callback.answer("Сначала выполните /start", show_alert=True)
        return

    tariff = platform_user["tariff"]
    tariff_labels = {
        "free": "🆓 Free", "start": "🌱 Старт",
        "pro": "⭐ Про", "business": "💼 Бизнес",
    }
    label = tariff_labels.get(tariff, "🆓 Free")
    until = ""
    if platform_user.get("tariff_until"):
        until = f"\nДействует до: {platform_user['tariff_until'].strftime('%d.%m.%Y')}"

    lang_cur = "🇷🇺 Русский" if platform_user.get("language", "ru") == "ru" else "🇺🇸 English"

    await callback.message.edit_text(
        f"🔑 <b>Управление аккаунтом</b>\n\n"
        f"👤 ID: <code>{platform_user['user_id']}</code>\n"
        f"📦 Тариф: {label}{until}\n"
        f"🌍 Язык: {lang_cur}",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="🌍 Сменить язык",    callback_data="settings:lang")],
            [InlineKeyboardButton(text="💳 Тарифы и оплата", callback_data="menu:tariffs")],
            [InlineKeyboardButton(text="◀️ Назад",           callback_data="menu:main")],
        ]),
    )
    await callback.answer()


@router.callback_query(F.data == "settings:lang")
async def on_settings_lang(callback: CallbackQuery):
    await callback.message.edit_text(
        "🌍 <b>Выберите язык / Choose language:</b>",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="🇷🇺 Русский", callback_data="lang:ru")],
            [InlineKeyboardButton(text="🇺🇸 English",  callback_data="lang:en")],
            [InlineKeyboardButton(text="◀️ Назад",    callback_data="menu:settings")],
        ]),
    )
    await callback.answer()

