"""
handlers/start.py — /start, выбор языка, главное меню.
"""
from aiogram import Router, F
from aiogram.filters import CommandStart
from aiogram.types import Message, CallbackQuery, InlineKeyboardMarkup, InlineKeyboardButton

import db.pool as db
from config import settings
from utils.nav import navigate

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
        # Новый пользователь — регистрируем с языком ru по умолчанию
        await db.execute(
            """
            INSERT INTO platform_users (user_id, username, first_name, language)
            VALUES ($1, $2, $3, 'ru')
            ON CONFLICT (user_id) DO NOTHING
            """,
            user.id, user.username, user.first_name,
        )

        # Владелец/совладелец — business навсегда, без trial
        username_lower = (user.username or "").lower().lstrip("@")
        is_project_owner = (
            user.id == settings.owner_telegram_id
            or (settings.owner_username and username_lower == settings.owner_username.lower().lstrip("@"))
            or (settings.co_owner_telegram_id and user.id == settings.co_owner_telegram_id)
            or (settings.co_owner_username and username_lower == settings.co_owner_username.lower().lstrip("@"))
        )

        if is_project_owner:
            await db.execute(
                "UPDATE platform_users SET tariff='business', tariff_until=NULL, trial_used=true WHERE user_id=$1",
                user.id,
            )
            trial_msg = ""
        else:
            trial_msg = ""

        # Получаем свежую запись для _show_main_menu
        platform_user = await db.fetchrow(
            "SELECT * FROM platform_users WHERE user_id=$1", user.id
        )
        platform_user = dict(platform_user) if platform_user else {"user_id": user.id, "tariff": "free", "tariff_until": None}

        await _show_main_menu(message, platform_user, extra=trial_msg)
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

    # Владелец проекта и Совладелец — сразу business навсегда, без trial
    username = (callback.from_user.username or "").lower().lstrip("@")
    is_project_owner = (
        callback.from_user.id == settings.owner_telegram_id
        or (settings.owner_username and username == settings.owner_username.lower().lstrip("@"))
        or (settings.co_owner_telegram_id and callback.from_user.id == settings.co_owner_telegram_id)
        or (settings.co_owner_username and username == settings.co_owner_username.lower().lstrip("@"))
    )

    if is_project_owner:
        await db.execute(
            """
            UPDATE platform_users
            SET tariff = 'business', tariff_until = NULL, trial_used = true
            WHERE user_id=$1
            """,
            callback.from_user.id,
        )
        trial_msg = ""
    else:
        trial_msg = ""


    text = "🇷🇺 Язык установлен: Русский" if lang == "ru" else "🇺🇸 Language set: English"
    await navigate(
        callback,
        f"{text}{trial_msg}\n\n⇨ Главное меню",
        reply_markup=kb_main_menu(),
    )


# ── Главное меню ──────────────────────────────────────────────
@router.callback_query(F.data == "menu:main")
async def on_main_menu(callback: CallbackQuery, platform_user: dict | None):
    await callback.answer()
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

    await navigate(
        callback,
        f"⚡ <b>Bumblebee Bot</b> — ваш главный помощник для работы с трафиком.\n\n"
        f"Тариф: {label}{until}\n\n"
        f"⇨ Главное меню",
        reply_markup=kb_main_menu(),
    )


async def _show_main_menu(message: Message, platform_user: dict | None, extra: str = ""):
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
        f"⇨ Главное меню{extra}",
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
    
    # Расчет периода действия тарифа
    until_str = ""
    if tariff != "free":
        last_payment = await db.fetchrow(
            "SELECT paid_at FROM payments WHERE user_id=$1 AND status='paid' ORDER BY paid_at DESC LIMIT 1",
            platform_user["user_id"]
        )
        since_str = ""
        if last_payment and last_payment["paid_at"]:
            since_str = f"с {last_payment['paid_at'].strftime('%d.%m.%Y')} "
            
        if platform_user.get("tariff_until"):
            until_str = f" ({since_str}по {platform_user['tariff_until'].strftime('%d.%m.%Y')})"
        else:
            until_str = f" ({since_str}Бессрочно)"

    # Загружаем загруженность из БД
    async with db.get_pool().acquire() as conn:
        bots_count = await conn.fetchval("SELECT COUNT(*) FROM child_bots WHERE owner_id=$1", platform_user["user_id"])
        # Считаем только активные подключенные чаты
        chats_count = await conn.fetchval(
            "SELECT COUNT(c.id) FROM bot_chats c JOIN child_bots b ON c.child_bot_id = b.id WHERE c.owner_id=$1 AND c.is_active=true", 
            platform_user["user_id"]
        )
        bl_count = await conn.fetchval(
            "SELECT COUNT(*) FROM blacklist WHERE owner_id=$1",
            platform_user["user_id"]
        )

    from config import TARIFFS, settings
    t_cfg = TARIFFS.get(tariff, TARIFFS["free"])
    max_bots = t_cfg["max_bots"]
    
    max_chats_p_bot = t_cfg["max_chats_per_bot"]
    if max_chats_p_bot > 10000:
        chats_display = f"{chats_count} (Без ограничений)"
    else:
        chats_display = f"{chats_count} из {max_chats_p_bot * max_bots}"

    max_bl = t_cfg["max_blacklist_users"]
    
    # Получаем юзернейм саппорта из конфига
    support_username = settings.co_owner_username or settings.owner_username or "secvency"
    support_url = f"https://t.me/{support_username.strip('@')}"

    # Отформатируем большие числа с пробелами
    bl_str = f"{bl_count:,}".replace(",", " ")
    max_bl_str = f"{max_bl:,}".replace(",", " ")

    await navigate(
        callback,
        f"🔑 <b>Личный кабинет</b>\n\n"
        f"👤 Ваш ID: <code>{platform_user['user_id']}</code>\n"
        f"💎 Тариф: {label}{until_str}\n\n"
        f"📊 <b>Ваши лимиты:</b>\n"
        f"├ 🤖 Боты: <b>{bots_count}</b> из {max_bots}\n"
        f"├ 📍 Площадки: <b>{chats_display}</b>\n"
        f"└ ⛔️ Глобальный ЧС: <b>{bl_str}</b> из {max_bl_str}\n",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="💳 Тарифы и оплата", callback_data="menu:tariffs")],
            [InlineKeyboardButton(text="📜 История покупок",  callback_data="settings:history")],
            [InlineKeyboardButton(text="🎧 Служба поддержки", url=support_url)],
            [InlineKeyboardButton(text="◀️ Назад к меню",           callback_data="menu:main")],
        ]),
    )


@router.callback_query(F.data == "settings:history")
async def on_settings_history(callback: CallbackQuery, platform_user: dict | None):
    if not platform_user:
        return

    payments = await db.fetch(
        "SELECT * FROM payments WHERE user_id=$1 AND status='paid' ORDER BY paid_at DESC LIMIT 20",
        platform_user["user_id"]
    )
    
    if not payments:
        text = "📜 <b>История покупок</b>\n\nУ вас еще нет завершенных платежей."
    else:
        text = "📜 <b>История покупок</b>\n\n"
        tariff_labels = {"start": "🌱 Старт", "pro": "⭐ Про", "business": "💼 Бизнес"}
        period_labels = {"month": "1 мес", "year": "1 год"}
        
        for p in payments:
            dt = p["paid_at"].strftime("%d.%m.%Y %H:%M")
            t_label = tariff_labels.get(p["tariff"], p["tariff"].capitalize())
            p_label = period_labels.get(p["period"], p["period"])
            amount = float(p["amount_usd"])
            curr = (p["currency"] or "USD").upper()
            
            text += (
                f"📅 <b>{dt}</b>\n"
                f"📦 Тариф: {t_label} ({p_label})\n"
                f"💵 Сумма: {amount:.2f} $\n"
                f"────────────────\n"
            )

    await navigate(
        callback,
        text,
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="◀️ Назад", callback_data="menu:settings")],
        ]),
    )

