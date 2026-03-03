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
            InlineKeyboardButton(text="🤖 Мой список ботов", callback_data="menu:channels"),
            InlineKeyboardButton(text="📨 Рассылка",         callback_data="menu:mailing"),
        ],
        [
            InlineKeyboardButton(text="💳 Тарифы",           callback_data="menu:tariffs"),
            InlineKeyboardButton(text="🔑 Управление",       callback_data="menu:settings"),
        ],
    ])


# ── /start ───────────────────────────────────────────────────
@router.message(CommandStart())
async def cmd_start(message: Message, platform_user: dict | None):
    user = message.from_user
    args = message.text.split(" ", 1)[1] if " " in message.text else ""

    # ── Обработка team-invite deep link ──────────────────────
    if args.startswith("team-"):
        token = args[5:]   # убрать "team-"
        invite = await db.fetchrow(
            "SELECT * FROM team_invites WHERE token=$1", token,
        )
        if not invite:
            await message.answer("❌ Ссылка недействительна или уже использована.")
            return

        owner_id     = invite["owner_id"]
        child_bot_id = invite["child_bot_id"]
        role         = invite["role"]

        # Убеждаемся что приглашённый зарегистрирован
        if not platform_user:
            await db.execute(
                """INSERT INTO platform_users (user_id, username, first_name)
                   VALUES ($1, $2, $3) ON CONFLICT (user_id) DO NOTHING""",
                user.id, user.username, user.first_name,
            )

        # Нельзя принять приглашение в собственный бот
        bot_row = await db.fetchrow(
            "SELECT owner_id, bot_username FROM child_bots WHERE id=$1", child_bot_id,
        )
        if not bot_row or bot_row["owner_id"] == user.id:
            await message.answer("⚠️ Вы уже являетесь владельцем этого бота.")
            return

        bot_username = bot_row["bot_username"]

        if role == "admin":
            # Добавляем как администратора
            await db.execute(
                """INSERT INTO team_members
                       (owner_id, child_bot_id, user_id, username, role, is_active)
                   VALUES ($1, $2, $3, $4, 'admin', true)
                   ON CONFLICT (owner_id, user_id) DO UPDATE
                       SET is_active=true, child_bot_id=$2, role='admin'""",
                owner_id, child_bot_id, user.id, user.username,
            )
            # Удаляем токен (одноразовый)
            await db.execute("DELETE FROM team_invites WHERE token=$1", token)
            await message.answer(
                f"✅ <b>Вы добавлены как администратор</b>\n\n"
                f"Бот @{bot_username} появился в вашем списке ботов.\n"
                f"Вы можете управлять им через главное меню.",
                reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                    [InlineKeyboardButton(text="🤖 Мои боты", callback_data="menu:channels")],
                ]),
            )

        elif role == "owner":
            # Передача владения: меняем owner_id у child_bot
            old_owner_id = bot_row["owner_id"]
            await db.execute(
                "UPDATE child_bots SET owner_id=$1 WHERE id=$2",
                user.id, child_bot_id,
            )
            # Переносим смежные записи
            await db.execute(
                "UPDATE bot_chats SET owner_id=$1 WHERE child_bot_id=$2 AND owner_id=$3",
                user.id, child_bot_id, old_owner_id,
            )
            await db.execute(
                "UPDATE bot_users SET owner_id=$1 WHERE owner_id=$2",
                user.id, old_owner_id,
            )
            # Удаляем оба токена приглашения для этого бота (сброс)
            await db.execute(
                "DELETE FROM team_invites WHERE child_bot_id=$1", child_bot_id,
            )
            await message.answer(
                f"👑 <b>Вы стали владельцем бота @{bot_username}!</b>\n\n"
                f"Полный контроль над ботом теперь у вас.",
                reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                    [InlineKeyboardButton(text="🤖 Мои боты", callback_data="menu:channels")],
                ]),
            )
        return

    # ── Стандартный /start ────────────────────────────────────
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

