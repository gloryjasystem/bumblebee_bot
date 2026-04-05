"""
handlers/channels.py — Подключение площадок через дочерний бот.
Флоу: Виды ботов → Ввод токена → Валидация → Добавить в канал/группу → Проверка → Подключено!
"""
import logging
from aiogram import Router, F
from aiogram.types import (
    Message, CallbackQuery,
    InlineKeyboardMarkup, InlineKeyboardButton,
)
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup

import db.pool as db
from db.pool import get_pool
from services.child_bot_service import validate_and_save_child_bot, verify_bot_is_admin
from config import settings
from utils.nav import navigate

logger = logging.getLogger(__name__)
router = Router()


class ChannelFSM(StatesGroup):
    waiting_for_token        = State()   # Ввод токена
    waiting_for_chat_verify  = State()   # Ввод @username или ID канала для проверки
    waiting_for_retoken      = State()   # Ввод НОВОГО токена для нерабочего бота



# ══════════════════════════════════════════════════════════════
# 1. Список ботов — с пагинацией
# ══════════════════════════════════════════════════════════════
_BOTS_PER_PAGE = 5

@router.callback_query(F.data.startswith("menu:channels"))
async def on_channels_menu(callback: CallbackQuery, platform_user: dict | None):
    if not platform_user:
        await callback.answer("Выполните /start")
        return
    owner_id = platform_user["user_id"]

    # Парсим номер страницы из callback_data: "menu:channels" или "menu:channels:2"
    parts = callback.data.split(":")
    try:
        page = int(parts[2]) if len(parts) >= 3 and parts[2].isdigit() else 0
    except (IndexError, ValueError):
        page = 0

    # Мои боты (владелец) + подсчет RN для лимитов
    bots = await db.fetch(
        """
        WITH RankedBots AS (
            SELECT cb.id, cb.bot_username, 'owner' AS my_role, cb.owner_id,
                   ROW_NUMBER() OVER(PARTITION BY cb.owner_id ORDER BY cb.id ASC) as rn,
                   (SELECT COUNT(*) FROM bot_chats bc 
                    WHERE bc.child_bot_id = cb.id AND bc.owner_id = cb.owner_id) AS chat_count
            FROM child_bots cb
            WHERE cb.owner_id = $1
        )
        SELECT id, bot_username, my_role, chat_count, rn FROM RankedBots
        UNION
        -- Боты, к которым я добавлен как admin (для них своя квота идёт от их владельца, здесь просто показываем)
        SELECT cb.id, cb.bot_username, 'admin' AS my_role, 
               (SELECT COUNT(*) FROM bot_chats bc 
                WHERE bc.child_bot_id = cb.id AND bc.owner_id = cb.owner_id) AS chat_count,
               0 as rn
        FROM child_bots cb
        JOIN team_members tm ON tm.child_bot_id = cb.id AND tm.user_id = $1 AND tm.is_active = true
        WHERE cb.owner_id != $1
        ORDER BY rn ASC, id DESC
        """,
        owner_id,
    )

    from config import TARIFFS
    tariff = platform_user["tariff"]
    limit  = TARIFFS.get(tariff, TARIFFS["free"])["max_bots"]

    # Считаем статистику по ВСЕМ ботам (не только по текущей странице!)
    own_count  = sum(1 for b in bots if b["my_role"] == "owner")
    own_active = sum(1 for b in bots if b["my_role"] == "owner" and b["rn"] <= limit)
    own_frozen = sum(1 for b in bots if b["my_role"] == "owner" and b["rn"] > limit)

    # Пагинация
    total_pages = max(1, (len(bots) + _BOTS_PER_PAGE - 1) // _BOTS_PER_PAGE)
    page = max(0, min(page, total_pages - 1))
    page_bots = bots[page * _BOTS_PER_PAGE : (page + 1) * _BOTS_PER_PAGE]

    # Кнопки для текущей страницы
    buttons = []
    for b in page_bots:
        is_frozen = b["my_role"] == "owner" and b["rn"] > limit
        if is_frozen:
            status = "🔴"
            btn_text = f"{status} @{b['bot_username']} (Заморожен)"
            buttons.append([InlineKeyboardButton(text=btn_text, callback_data=f"bot_frozen:{b['id']}")])
        else:
            status = "🟢"
            role_icon = "" if b["my_role"] == "owner" else " 🛡"
            buttons.append([InlineKeyboardButton(
                text=f"{status} @{b['bot_username']}{role_icon}",
                callback_data=f"bot_settings:{b['id']}",
            )])

    # Навигация по страницам (только если страниц > 1)
    if total_pages > 1:
        nav_row = []
        if page > 0:
            nav_row.append(InlineKeyboardButton(text="◀️", callback_data=f"menu:channels:{page - 1}"))
        else:
            nav_row.append(InlineKeyboardButton(text="·", callback_data="ml_noop"))
        nav_row.append(InlineKeyboardButton(text=f"{page + 1} / {total_pages}", callback_data="ml_noop"))
        if page < total_pages - 1:
            nav_row.append(InlineKeyboardButton(text="▶️", callback_data=f"menu:channels:{page + 1}"))
        else:
            nav_row.append(InlineKeyboardButton(text="·", callback_data="ml_noop"))
        buttons.append(nav_row)

    if own_count < limit:
        buttons.append([InlineKeyboardButton(
            text="➕ Подключить новый бот",
            callback_data="channel:new",
        )])
    else:
        buttons.append([InlineKeyboardButton(
            text=f"🔒 Лимит ботов ({own_count}/{limit}) — улучшите тариф",
            callback_data="menu:tariffs",
        )])

    buttons.append([InlineKeyboardButton(text="◀️ Назад", callback_data="menu:main")])

    await navigate(
        callback,
        f"🤖 <b>Мои боты</b>\n\n"
        f"Подключено: {own_count}/{limit} (тариф {tariff.capitalize()})",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=buttons),
    )


@router.callback_query(F.data == "ml_noop")
async def on_ml_noop(callback: CallbackQuery):
    await callback.answer()


@router.callback_query(F.data.startswith("bot_frozen:"))
async def on_bot_frozen(callback: CallbackQuery):
    await callback.answer(
        "🔴 Бот заморожен!\n"
        "Ваш текущий лимит ботов исчерпан из-за понижения тарифа.\n"
        "Оплатите тариф для возвращения доступа к боту.",
        show_alert=True
    )

# ══════════════════════════════════════════════════════════════
# 1б. Настройки бота (уровень 2)
# ══════════════════════════════════════════════════════════════
@router.callback_query(F.data.startswith("bot_settings:"))
async def on_bot_settings(callback: CallbackQuery, state: FSMContext, platform_user: dict | None):
    if not platform_user:
        return
    child_bot_id = int(callback.data.split(":")[1])
    user_id = platform_user["user_id"]

    # Разрешаем доступ владельцу ИЛИ активному admin
    bot = await db.fetchrow(
        """
        SELECT cb.* FROM child_bots cb
        WHERE cb.id=$1::bigint AND (
            cb.owner_id=$2
            OR EXISTS (
                SELECT 1 FROM team_members tm
                WHERE tm.child_bot_id=cb.id AND tm.user_id=$2 AND tm.is_active=true
            )
        )
        """,
        child_bot_id, user_id,
    )
    if not bot:
        await callback.answer("Бот не найден", show_alert=True)
        return
    is_owner = bot["owner_id"] == user_id
    from utils.god_mode import get_target as _god_get
    is_god_mode = bool(_god_get(callback.from_user.id))
    # Все запросы к БД идут через реального владельца бота
    owner_id = bot["owner_id"]

    # --- ДОБАВЛЕНО: Проверка валидности токена бота ---
    from aiogram import Bot
    from aiogram.exceptions import TelegramUnauthorizedError
    from services.security import decrypt_token
    token_str = decrypt_token(bot["token_encrypted"])
    try:
        temp_bot = Bot(token=token_str)
        await temp_bot.get_me()
        await temp_bot.session.close()
    except TelegramUnauthorizedError:
        # Токен отозван или бот удален
        await temp_bot.session.close()
        await state.set_state(ChannelFSM.waiting_for_retoken)
        await state.update_data(child_bot_id=child_bot_id)
        msg = await navigate(
            callback,
            "🚫 Бот недоступен, перевыпустите и пришлите токен или удалите бота.\n\n"
            "<blockquote>ℹ️ Чтобы перевыпустить токен: нужно перейти в @BotFather ➔ my bots ➔ "
            "выбрать нужного бота ➔ API Token ➔ Revoke current token ➔ скопировать новый токен "
            "и отправить его сюда.</blockquote>\n\n"
            "Выберите действие 🔽",
            reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                [InlineKeyboardButton(text="🗑 Удалить бот", callback_data=f"bot_delete:{child_bot_id}")],
                [InlineKeyboardButton(text="◀️ Назад", callback_data="menu:channels")],
            ]),
        )
        if msg:
            await state.update_data(prompt_msg_id=msg.message_id)
        return
    except Exception as e:
        logger.error(f"Error checking bot token {child_bot_id}: {e}")
        await temp_bot.session.close()
    # --- КОНЕЦ ДОБАВЛЕНИЯ ---

    from datetime import date, timedelta
    today     = date.today()
    yesterday = today - timedelta(days=1)

    total_users = await db.fetchval(
        """SELECT COUNT(DISTINCT bu.user_id) FROM bot_users bu
           JOIN bot_chats bc ON bu.chat_id=bc.chat_id AND bu.owner_id=bc.owner_id
           WHERE bc.child_bot_id=$1 AND bc.owner_id=$2""",
        child_bot_id, owner_id,
    ) or 0
    today_users = await db.fetchval(
        """SELECT COUNT(DISTINCT bu.user_id) FROM bot_users bu
           JOIN bot_chats bc ON bu.chat_id=bc.chat_id AND bu.owner_id=bc.owner_id
           WHERE bc.child_bot_id=$1 AND bc.owner_id=$2 AND bu.joined_at::date=$3""",
        child_bot_id, owner_id, today,
    ) or 0
    yesterday_users = await db.fetchval(
        """SELECT COUNT(DISTINCT bu.user_id) FROM bot_users bu
           JOIN bot_chats bc ON bu.chat_id=bc.chat_id AND bu.owner_id=bc.owner_id
           WHERE bc.child_bot_id=$1 AND bc.owner_id=$2 AND bu.joined_at::date=$3""",
        child_bot_id, owner_id, yesterday,
    ) or 0
    pending = await db.fetchval(
        """SELECT COUNT(*) FROM join_requests jr
           JOIN bot_chats bc ON jr.chat_id=bc.chat_id AND jr.owner_id=bc.owner_id
           WHERE bc.child_bot_id=$1 AND bc.owner_id=$2 AND jr.status IN ('pending','captcha_verified')""",
        child_bot_id, owner_id,
    ) or 0
    # Статистика капчи (с защитой от ошибок, если таблица ещё не создана)
    captcha_total_today = captcha_passed_today = 0
    captcha_total_yest  = captcha_passed_yest  = 0
    captcha_total_all   = captcha_passed_all   = 0
    try:
        captcha_total_today = await db.fetchval(
            """SELECT COUNT(*) FROM captcha_events ce
               JOIN bot_chats bc ON ce.chat_id=bc.chat_id AND ce.owner_id=bc.owner_id
               WHERE bc.child_bot_id=$1 AND ce.owner_id=$2
                 AND ce.created_at::date=$3""",
            child_bot_id, owner_id, today,
        ) or 0
        captcha_passed_today = await db.fetchval(
            """SELECT COUNT(*) FROM captcha_events ce
               JOIN bot_chats bc ON ce.chat_id=bc.chat_id AND ce.owner_id=bc.owner_id
               WHERE bc.child_bot_id=$1 AND ce.owner_id=$2
                 AND ce.created_at::date=$3 AND ce.passed=true""",
            child_bot_id, owner_id, today,
        ) or 0
        captcha_total_yest = await db.fetchval(
            """SELECT COUNT(*) FROM captcha_events ce
               JOIN bot_chats bc ON ce.chat_id=bc.chat_id AND ce.owner_id=bc.owner_id
               WHERE bc.child_bot_id=$1 AND ce.owner_id=$2
                 AND ce.created_at::date=$3""",
            child_bot_id, owner_id, yesterday,
        ) or 0
        captcha_passed_yest = await db.fetchval(
            """SELECT COUNT(*) FROM captcha_events ce
               JOIN bot_chats bc ON ce.chat_id=bc.chat_id AND ce.owner_id=bc.owner_id
               WHERE bc.child_bot_id=$1 AND ce.owner_id=$2
                 AND ce.created_at::date=$3 AND ce.passed=true""",
            child_bot_id, owner_id, yesterday,
        ) or 0
        captcha_total_all = await db.fetchval(
            """SELECT COUNT(*) FROM captcha_events ce
               JOIN bot_chats bc ON ce.chat_id=bc.chat_id AND ce.owner_id=bc.owner_id
               WHERE bc.child_bot_id=$1 AND ce.owner_id=$2""",
            child_bot_id, owner_id,
        ) or 0
        captcha_passed_all = await db.fetchval(
            """SELECT COUNT(*) FROM captcha_events ce
               JOIN bot_chats bc ON ce.chat_id=bc.chat_id AND ce.owner_id=bc.owner_id
               WHERE bc.child_bot_id=$1 AND ce.owner_id=$2 AND ce.passed=true""",
            child_bot_id, owner_id,
        ) or 0
    except Exception as e:
        logger.debug(f"captcha_events query failed (bot_settings): {e}")

    def _pct(passed, total):
        return f"{round(passed/total*100)}%" if total > 0 else "0%"

    # Живые / Мёртвые
    alive_users = await db.fetchval(
        """SELECT COUNT(DISTINCT bu.user_id) FROM bot_users bu
           JOIN bot_chats bc ON bu.chat_id=bc.chat_id AND bu.owner_id=bc.owner_id
           WHERE bc.child_bot_id=$1 AND bc.owner_id=$2 AND bu.is_active=true
             AND bu.user_id IS NOT NULL""",
        child_bot_id, owner_id,
    ) or 0
    dead_users  = await db.fetchval(
        """SELECT COUNT(DISTINCT bu.user_id) FROM bot_users bu
           JOIN bot_chats bc ON bu.chat_id=bc.chat_id AND bu.owner_id=bc.owner_id
           WHERE bc.child_bot_id=$1 AND bc.owner_id=$2 AND bu.is_active=false
             AND bu.user_id IS NOT NULL""",
        child_bot_id, owner_id,
    ) or 0

    # Статистика сообщений (из message_events)
    msg_today = msg_yesterday = msg_total = 0
    try:
        msg_today = await db.fetchval(
            """SELECT COUNT(*) FROM message_events me
               JOIN bot_chats bc ON me.chat_id=bc.chat_id AND me.owner_id=bc.owner_id
               WHERE bc.child_bot_id=$1 AND me.owner_id=$2
                 AND me.created_at::date=$3""",
            child_bot_id, owner_id, today,
        ) or 0
        msg_yesterday = await db.fetchval(
            """SELECT COUNT(*) FROM message_events me
               JOIN bot_chats bc ON me.chat_id=bc.chat_id AND me.owner_id=bc.owner_id
               WHERE bc.child_bot_id=$1 AND me.owner_id=$2
                 AND me.created_at::date=$3""",
            child_bot_id, owner_id, yesterday,
        ) or 0
        msg_total = await db.fetchval(
            """SELECT COUNT(*) FROM message_events me
               JOIN bot_chats bc ON me.chat_id=bc.chat_id AND me.owner_id=bc.owner_id
               WHERE bc.child_bot_id=$1 AND me.owner_id=$2""",
            child_bot_id, owner_id,
        ) or 0
    except Exception as _e:
        logger.debug(f"message_events query failed (bot_settings): {_e}")

    username = bot["bot_username"]
    captcha_block = ""
    is_free = platform_user.get("tariff", "free") == "free"

    if is_free:
        captcha_block = ""
        msg_block = ""
    else:
        if captcha_total_all > 0:
            captcha_block = (
                f"\n\n<u>\U0001f512 Решений капч</u>\n"
                f"├ Сегодня ≈ {captcha_passed_today} | {_pct(captcha_passed_today, captcha_total_today)}\n"
                f"├ Вчера ≈ {captcha_passed_yest} | {_pct(captcha_passed_yest, captcha_total_yest)}\n"
                f"└ Всего ≈ {captcha_passed_all} | {_pct(captcha_passed_all, captcha_total_all)}"
            )
        msg_block = (
            f"\n\n<u>\U0001f4ac Сообщений</u>\n"
            f"├ Сегодня ≈ {msg_today}\n"
            f"├ Вчера ≈ {msg_yesterday}\n"
            f"└ Всего ≈ {msg_total}"
        )

    text = (
        f"🤖 Бот: @{username}\n\n"
        f"<u>\U0001f465 Пользователей</u>\n"
        f"├ Сегодня ≈ {today_users}\n"
        f"├ Вчера ≈ {yesterday_users}\n"
        f"├ Всего ≈ {total_users}\n"
        f"└ Заявок в очереди ≈ {pending}"
        f"{captcha_block}{msg_block}\n"
        f"\n"
        f"🟢 Живые ≈ {alive_users}\n"
        f"🔴 Мёртвые ≈ {dead_users}"
    )

    # Кнопки с учетом тарифа (Paywall)
    btn_mailing    = InlineKeyboardButton(text=("🔒 Рассылка" if is_free else "📨 Рассылка"), callback_data="paywall:mailing" if is_free else f"bs_mailing:{child_bot_id}")
    btn_links      = InlineKeyboardButton(text=("🔒 Ссылки" if is_free else "🔗 Ссылки"),   callback_data="paywall:links" if is_free else f"bs_links:{child_bot_id}")
    btn_protection = InlineKeyboardButton(text=("🔒 Защита" if is_free else "🛡 Защита"),   callback_data="paywall:protection" if is_free else f"bs_protection:{child_bot_id}")

    if is_owner:
        # Владелец: Защита + Управление в одной строке
        keyboard = [
            [InlineKeyboardButton(text="✅ Обработка заявок",  callback_data=f"bs_requests:{child_bot_id}")],
            [
                InlineKeyboardButton(text="💬 Сообщения",      callback_data=f"bs_messages:{child_bot_id}"),
                btn_mailing,
            ],
            [
                btn_links,
                InlineKeyboardButton(text="📍 Площадки",       callback_data=f"bot_chats_list:{child_bot_id}"),
            ],
            [
                btn_protection,
                InlineKeyboardButton(text="⚙️ Управление",     callback_data=f"bs_settings:{child_bot_id}"),
            ],
            [InlineKeyboardButton(text="📣 Обратная связь",    callback_data=f"bs_feedback:{child_bot_id}")],
        ]
        # Опасную кнопку показываем только если администратор НЕ в режиме управления
        if not is_god_mode:
            keyboard.append([InlineKeyboardButton(text="🗑 Удалить бот", callback_data=f"bot_delete:{child_bot_id}")])
            
        keyboard.append([InlineKeyboardButton(text="◀️ Назад", callback_data="menu:channels")])
    else:
        # Admin: без Управления и Удалить бот, Защита — отдельной строкой
        keyboard = [
            [InlineKeyboardButton(text="✅ Обработка заявок",  callback_data=f"bs_requests:{child_bot_id}")],
            [
                InlineKeyboardButton(text="💬 Сообщения",      callback_data=f"bs_messages:{child_bot_id}"),
                btn_mailing,
            ],
            [
                btn_links,
                InlineKeyboardButton(text="📍 Площадки",       callback_data=f"bot_chats_list:{child_bot_id}"),
            ],
            [btn_protection],
            [InlineKeyboardButton(text="📣 Обратная связь",    callback_data=f"bs_feedback:{child_bot_id}")],
            [InlineKeyboardButton(text="◀️ Назад",             callback_data="menu:channels")],
        ]
    msg = await navigate(
        callback,
        text,
        reply_markup=InlineKeyboardMarkup(inline_keyboard=keyboard),
    )
    # Сохраняем ID этого сообщения — уведомления о правах отредактируют его на месте
    if msg:
        await db.execute(
            "UPDATE platform_users SET last_channels_menu_id=$1 WHERE user_id=$2",
            msg.message_id, owner_id,
        )


# ══════════════════════════════════════════════════════════════
# 1в. Площадки бота (уровень 3)
# ══════════════════════════════════════════════════════════════
@router.callback_query(F.data.startswith("bot_chats_list:"))
async def on_bot_chats_list(callback: CallbackQuery, platform_user: dict | None):
    if not platform_user:
        return
    child_bot_id = int(callback.data.split(":")[1])
    from handlers.channel_settings import resolve_owner_id
    owner_id = await resolve_owner_id(platform_user["user_id"], child_bot_id)
    if owner_id is None:
        await callback.answer("Нет доступа", show_alert=True)
        return

    bot = await db.fetchrow(
        "SELECT bot_username, verify_only FROM child_bots WHERE id=$1 AND owner_id=$2",
        child_bot_id, owner_id,
    )
    if not bot:
        await callback.answer("Бот не найден", show_alert=True)
        return

    chats = await db.fetch(
        """
        WITH RankedChats AS (
            SELECT id, chat_title, chat_type, is_active,
                   CASE WHEN is_active THEN ROW_NUMBER() OVER(PARTITION BY child_bot_id, is_active ORDER BY added_at ASC)
                        ELSE 9999 END as rn
            FROM bot_chats
            WHERE child_bot_id=$1 AND owner_id=$2
        )
        SELECT id, chat_title, chat_type, is_active, rn
        FROM RankedChats
        ORDER BY rn ASC
        """,
        child_bot_id, owner_id,
    )

    from config import TARIFFS
    tariff = platform_user["tariff"]
    limit  = TARIFFS.get(tariff, TARIFFS["free"])["max_chats_per_bot"]

    buttons = []
    active_count = 0
    frozen_count = 0

    for ch in chats:
        # A chat is essentially soft-locked only if it's active AND its rank exceeds the limit
        is_frozen = ch["rn"] > limit and ch["is_active"]
        
        if is_frozen:
            frozen_count += 1
            icon = "🔴"
            title = ch["chat_title"] or "Без названия"
            btn_text = f"{icon} {title} (Заморожено)"
            buttons.append([InlineKeyboardButton(text=btn_text, callback_data=f"chat_frozen:{ch['id']}")])
        else:
            if ch["is_active"]:
                active_count += 1
            icon = "🟢" if ch["is_active"] else "⚪"
            type_icon = "📢" if ch["chat_type"] == "channel" else "👥"
            title = ch["chat_title"] or "Без названия"
            buttons.append([InlineKeyboardButton(
                text=f"{icon} {type_icon} {title}",
                callback_data=f"channel_in_bot:{ch['id']}:{child_bot_id}",
            )])

    verify_label = "✅ Проверка: вкл" if bot["verify_only"] else "❌ Проверка: выкл"
    buttons.append([InlineKeyboardButton(
        text="➕ Подключение",
        callback_data=f"bot_connect:{child_bot_id}",
    )])
    buttons.append([InlineKeyboardButton(
        text=verify_label,
        callback_data=f"bot_verify_toggle:{child_bot_id}",
    )])
    buttons.append([InlineKeyboardButton(text="◀️ Назад", callback_data=f"bot_settings:{child_bot_id}")])

    username = bot["bot_username"]
    hint = (
        "<blockquote>"
        "ℹ️ Площадка — это общее название для подключённых каналов и чатов.\n\n"
        "🔍 Если проверка включена, то только владелец бота сможет подключать площадки."
        "</blockquote>"
    )
    count = len(chats)
    status_text = f"Подключено площадок: {count}\n"
    if frozen_count > 0:
        status_text += f"Активно: {active_count} | Заморожено: {frozen_count} (лимит {limit})\n"

    msg = await navigate(
        callback,
        f"📍 <b>Площадки @{username}</b>\n\n"
        f"{hint}\n\n"
        f"{status_text}\n"
        "Выберите действие 👇" if count > 0 else
        f"📍 <b>Площадки @{username}</b>\n\n"
        f"{hint}\n\n"
        f"{status_text}\n"
        "Бот ещё не добавлен ни в один канал или группу.\n"
        "Нажмите <b>Подключение</b> чтобы добавить.",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=buttons),
    )

@router.callback_query(F.data.startswith("chat_frozen:"))
async def on_chat_frozen(callback: CallbackQuery):
    await callback.answer(
        "🔴 Площадка заморожена!\n"
        "Ваш лимит площадок на одного бота исчерпан.\n"
        "Улучшите тариф, чтобы бот снова начал работать в этой группе/канале.",
        show_alert=True
    )
    if msg:
        await db.execute(
            "UPDATE platform_users SET last_channels_menu_id=$1 WHERE user_id=$2",
            msg.message_id, owner_id
        )


# ══════════════════════════════════════════════════════════════
# 1г. Подключение (уровень 4)
# ══════════════════════════════════════════════════════════════
@router.callback_query(F.data.startswith("bot_connect:"))
async def on_bot_connect(callback: CallbackQuery, platform_user: dict | None):
    if not platform_user:
        return
    child_bot_id = int(callback.data.split(":")[1])
    owner_id = platform_user["user_id"]

    bot = await db.fetchrow(
        "SELECT bot_username FROM child_bots WHERE id=$1 AND owner_id=$2",
        child_bot_id, owner_id,
    )
    if not bot:
        await callback.answer("Бот не найден", show_alert=True)
        return

    username = bot["bot_username"]
    deep_channel = f"https://t.me/{username}?startchannel=true&admin=post_messages+delete_messages+invite_users+restrict_members+pin_messages"
    deep_group   = f"https://t.me/{username}?startgroup=true&admin=post_messages+delete_messages+invite_users+restrict_members+pin_messages"

    msg = await navigate(
        callback,
        f"➕ Добавьте <b>@{username}</b> в <b>канал или группу</b> "
        f"в качестве администратора с правами на "
        f"«Добавление участников» (ios) → «Пригласительные ссылки» (android).\n\n"
        "<blockquote>Он будет обрабатывать заявки, приветствовать "
        "пользователей и собирать их в базу для рассылок.</blockquote>\n\n"
        "Выберите действие 👇",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="→ Добавить в канал", url=deep_channel)],
            [InlineKeyboardButton(text="→ Добавить в группу", url=deep_group)],
            [InlineKeyboardButton(text="◀️ Назад", callback_data=f"bot_chats_list:{child_bot_id}")],
        ]),
    )
    if msg:
        await db.execute(
            "UPDATE platform_users SET last_channels_menu_id=$1 WHERE user_id=$2",
            msg.message_id, owner_id,
        )


# ══════════════════════════════════════════════════════════════
# 1д. Проверка toggle
# ══════════════════════════════════════════════════════════════
@router.callback_query(F.data.startswith("bot_verify_toggle:"))
async def on_bot_verify_toggle(callback: CallbackQuery, platform_user: dict | None):
    if not platform_user:
        return
    child_bot_id = int(callback.data.split(":")[1])
    owner_id = platform_user["user_id"]

    row = await db.fetchrow(
        "SELECT verify_only FROM child_bots WHERE id=$1 AND owner_id=$2",
        child_bot_id, owner_id,
    )
    if not row:
        return
    new_val = not row["verify_only"]
    await db.execute(
        "UPDATE child_bots SET verify_only=$1 WHERE id=$2 AND owner_id=$3",
        new_val, child_bot_id, owner_id,
    )
    label = "включена ✅" if new_val else "выключена ❌"
    await callback.answer(f"Проверка {label}")
    # Обновляем экран (не мутируем frozen объект, а создаём копию с нужным data)
    fake_cb = callback.model_copy(update={"data": f"bot_chats_list:{child_bot_id}"})
    await on_bot_chats_list(fake_cb, platform_user)


# ══════════════════════════════════════════════════════════════
# 1е. Удаление бота
# ══════════════════════════════════════════════════════════════
@router.callback_query(F.data.startswith("bot_delete:"))
async def on_bot_delete(callback: CallbackQuery, platform_user: dict | None):
    if not platform_user:
        return
    child_bot_id = int(callback.data.split(":")[1])
    await navigate(
        callback,
        "⚠️ <b>Удалить бота?</b>\n\n"
        "Все площадки, пользователи и настройки этого бота будут удалены.",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="✅ Да, удалить", callback_data=f"bot_delete_confirm:{child_bot_id}")],
            [InlineKeyboardButton(text="🚫 Отмена",     callback_data=f"bot_settings:{child_bot_id}")],
        ]),
    )


@router.callback_query(F.data.startswith("bot_delete_confirm:"))
async def on_bot_delete_confirm(callback: CallbackQuery, platform_user: dict | None):
    """Второй экран подтверждения — финальное предупреждение."""
    if not platform_user:
        return
    child_bot_id = int(callback.data.split(":")[1])
    await navigate(
        callback,
        "🚨 <b>Вы точно уверены?</b>\n\n"
        "После удаления <b>восстановить ничего будет невозможно</b>. Будут навсегда удалены:\n\n"
        "❌ Все настроенные параметры бота\n"
        "❌ База ваших пользователей и статистика\n"
        "❌ Ссылки, рассылки и обратная связь\n"
        "❌ Все подключённые площадки",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="❗️ Да, удалить навсегда", callback_data=f"bot_delete_final:{child_bot_id}")],
            [InlineKeyboardButton(text="🚧 Нет, оставить бот",          callback_data=f"bot_settings:{child_bot_id}")],
        ]),
    )


@router.callback_query(F.data.startswith("bot_delete_final:"))
async def on_bot_delete_final(callback: CallbackQuery, platform_user: dict | None):
    """Финальное удаление — после двойного подтверждения."""
    if not platform_user:
        return
    child_bot_id = int(callback.data.split(":")[1])
    await db.execute(
        "DELETE FROM child_bots WHERE id=$1 AND owner_id=$2",
        child_bot_id, platform_user["user_id"],
    )
    await callback.answer("✅ Бот удалён")
    await on_channels_menu(callback, platform_user)


# ══════════════════════════════════════════════════════════════
# 2. Выбор типа бота (Виды ботов)
# ══════════════════════════════════════════════════════════════
@router.callback_query(F.data == "channel:new")
async def on_channel_new(callback: CallbackQuery, state: FSMContext, platform_user: dict | None):
    if not platform_user:
        return
    await state.clear()
    await navigate(
        callback,
        "🐝 <b>Создай бота</b> — и получи полное управление "
        "каналом или группой в одном месте.\n\n"
        "Что сможет твой бот:\n\n"
        "<blockquote>"
        "🛡 принимать заявки автоматически\n"
        "и отсеивать спам через капчу\n\n"
        "👋 встречать каждого нового участника приветствием,\n"
        "а при отписке — прощанием\n\n"
        "📣 настраивать и планировать рассылки по всей базе\n"
        "подписчиков с нужной датой\n\n"
        "🔗 анализировать стоимость переходов по\n"
        "пригласительной ссылке и собирать статистику\n\n"
        "🚫 блокировать доступ ко входу и фильтровать заявки\n"
        "по языку, имени, фото и символам\n\n"
        "📊 видеть глубокую аналитику активности\n"
        "и настраивать обработку сообщений в боте\n\n"
        "👥 удобно управлять командой и многое другое"
        "</blockquote>\n\n"
        "🐝 Всё в одном боте — без лишних сервисов.",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="🚀 Создать бота", callback_data="bot_type:welcome")],
            [InlineKeyboardButton(text="🚫 Отменить",         callback_data="menu:channels")],
        ]),
    )




# ══════════════════════════════════════════════════════════════
# 3. Запрос токена
# ══════════════════════════════════════════════════════════════
@router.callback_query(F.data == "bot_type:welcome")
async def on_bot_type_welcome(callback: CallbackQuery, state: FSMContext, platform_user: dict | None):
    if not platform_user:
        return
    await state.set_state(ChannelFSM.waiting_for_token)
    await state.update_data(owner_id=platform_user["user_id"])

    # Убираем старый текст, отправляем новый вниз
    try:
        await callback.message.delete()
    except Exception:
        pass

    msg = await callback.message.answer(
        "⚡ Чтобы создать бота, который закроет все задачи по "
        "управлению каналом и возьмёт рутину на себя, мне нужен токен:\n\n"
        "① Перейдите в @BotFather\n\n"
        "② Отправьте @BotFather команду: <code>/newbot</code>\n\n"
        "③ Придумайте название и юзернейм для вашего бота,\n"
        '   например: "Новости" → <code>@newsbot</code>\n\n'
        "④ @BotFather выдаст вам токен бота. Пример токена:\n"
        "   <code>5827254996:AAEBu9108achvHoWvPmvr6kueDgmFpJMjHo</code>\n\n"
        "<b>Отправьте токен бота</b> 👇",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="🚫 Отменить", callback_data="menu:channels")],
        ]),
    )
    await state.update_data(prompt_msg_id=msg.message_id)
    await callback.answer()


@router.message(ChannelFSM.waiting_for_token)
async def on_token_received(message: Message, state: FSMContext, platform_user: dict | None):
    if not platform_user:
        return

    token = message.text.strip() if message.text else ""
    data = await state.get_data()
    prompt_msg_id = data.get("prompt_msg_id")

    msg = await message.answer("⏳ Проверяю токен...")

    try:
        bot_info = await validate_and_save_child_bot(platform_user["user_id"], token)
    except ValueError as e:
        # Удаляем сообщение пользователя (токен) и предыдущую инструкцию/ошибку
        try:
            await message.delete()
        except Exception:
            pass
        if prompt_msg_id:
            try:
                await message.bot.delete_message(chat_id=message.chat.id, message_id=prompt_msg_id)
            except Exception:
                pass
        # Удаляем "⏳ Проверяю токен..."
        try:
            await msg.delete()
        except Exception:
            pass

        # Отправляем новое сообщение об ошибке и запоминаем его id для следующей очистки
        error_msg = await message.answer(
            f"❌ {e}",
            reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                [InlineKeyboardButton(text="🔄 Попробовать снова", callback_data="bot_type:welcome")],
                [InlineKeyboardButton(text="🚫 Отменить",          callback_data="menu:channels")],
            ]),
        )
        # Обновляем prompt_msg_id — при следующей попытке это сообщение тоже будет удалено
        await state.update_data(prompt_msg_id=error_msg.message_id)
        return

    # Запускаем polling для нового дочернего бота (слушает my_chat_member и join requests)
    try:
        from scheduler.child_bot_runner import start_child_bot
        bot_row = await db.fetchrow(
            "SELECT token_encrypted FROM child_bots WHERE id=$1",
            bot_info["id"],
        )
        if bot_row:
            await start_child_bot(
                bot_info["id"],
                platform_user["user_id"],
                bot_info["bot_username"],
                bot_row["token_encrypted"],
            )
    except Exception as e:
        logger.warning(f"Could not start child bot runner: {e}")

    # Сохраняем child_bot_id в state для следующих шагов
    await state.update_data(
        child_bot_id=bot_info["id"],
        bot_username=bot_info["bot_username"],
    )
    await state.set_state(ChannelFSM.waiting_for_chat_verify)

    username = bot_info["bot_username"]

    deep_channel = f"https://t.me/{username}?startchannel=true&admin=post_messages+delete_messages+invite_users+restrict_members+pin_messages"
    deep_group   = f"https://t.me/{username}?startgroup=true&admin=post_messages+delete_messages+invite_users+restrict_members+pin_messages"

    # Удаляем сообщение с инструкцией (prompt_msg_id) и сообщение пользователя с токеном
    try:
        if prompt_msg_id:
            await message.bot.delete_message(chat_id=message.chat.id, message_id=prompt_msg_id)
        await message.delete()
    except Exception:
        pass

    # Запоминаем message_id ДО редактирования — edit_text возвращает bool в aiogram 3
    invite_msg_id = msg.message_id
    
    await msg.edit_text(
        f"✅ Бот: @{username} создан\n\n"
        f"➕ Чтобы начать настраивать созданного вами бота, добавьте сначала его "
        f"в <b>канал или группу</b> в качестве администратора "
        f"с правами на «Добавление участников» (ios) → «Пригласительные ссылки» (android).\n\n"
        f"🤖 Создай универсального помощника, который закроет все задачи по управлению, "
        f"защите и монетизации твоего сообщества.\n\n"
        f"Выберите действие 👇",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="→ Добавить в канал", url=deep_channel)],
            [InlineKeyboardButton(text="→ Добавить в группу", url=deep_group)],
            [InlineKeyboardButton(text="⊃ В меню", callback_data="menu:channels")],
        ]),
    )
    
    # Сохраняем ID сообщения — child_bot_runner перезапишет его когда бот успешно добавится в чат
    async with get_pool().acquire() as conn:
        await conn.execute(
            "UPDATE platform_users SET last_channels_menu_id=$1 WHERE user_id=$2",
            invite_msg_id, platform_user["user_id"]
        )



# ══════════════════════════════════════════════════════════════
# 3б. Перевыпуск сломанного токена (Удаление / Восстановление)
# ══════════════════════════════════════════════════════════════
@router.message(ChannelFSM.waiting_for_retoken)
async def on_retoken_received(message: Message, state: FSMContext, platform_user: dict | None):
    if not platform_user:
        return

    data = await state.get_data()
    child_bot_id = data.get("child_bot_id")
    prompt_msg_id = data.get("prompt_msg_id")

    if not child_bot_id:
        await state.clear()
        return

    raw_token = message.text.strip() if message.text else ""
    msg = await message.answer("⏳ Проверяю новый токен...")

    if ":" not in raw_token or len(raw_token) < 30:
        await msg.edit_text("❌ Неверный формат токена. Введите правильный токен из @BotFather.")
        return

    from aiogram import Bot
    from aiogram.exceptions import TelegramUnauthorizedError
    try:
        temp_bot = Bot(token=raw_token)
        me = await temp_bot.get_me()
        await temp_bot.session.close()
    except TelegramUnauthorizedError:
        await msg.edit_text("❌ Токен недействителен. Проверьте токен и попробуйте снова.")
        return
    except Exception as e:
        logger.error(f"Error checking new token: {e}")
        await msg.edit_text("❌ Временная ошибка проверки токена. Попробуйте позже.")
        return

    from services.security import encrypt_token
    encrypted = encrypt_token(raw_token)

    # Перезаписываем данные бота на новый токен (если юзернейм/ID сменились — тоже перезаписываем)
    await db.execute(
        """
        UPDATE child_bots
        SET bot_id = $1, bot_username = $2, bot_name = $3, token_encrypted = $4
        WHERE id = $5 AND owner_id = $6
        """,
        me.id, me.username or "", me.full_name, encrypted,
        child_bot_id, platform_user["user_id"]
    )

    # Запускаем поллинг заново
    try:
        from scheduler.child_bot_runner import start_child_bot
        await start_child_bot(child_bot_id, platform_user["user_id"], me.username or "", encrypted)
    except Exception as e:
        logger.warning(f"Could not restart child bot runner: {e}")

    await state.clear()

    # Удаляем служебные сообщения
    try:
        if prompt_msg_id:
            await message.bot.delete_message(chat_id=message.chat.id, message_id=prompt_msg_id)
        await message.delete()
        await msg.delete()
    except Exception:
        pass

    # Направляем пользователя в настройки бота через inline-кнопку (безопасно, без фейковых CallbackQuery)
    bot_username = me.username or "bot"
    await message.answer(
        f"✅ <b>Токен успешно обновлён!</b>\n\n"
        f"🤖 Бот @{bot_username} снова активен и готов к работе.",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="⚙️ Открыть настройки", callback_data=f"bot_settings:{child_bot_id}")],
            [InlineKeyboardButton(text="◀️ Мои боты", callback_data="menu:channels")],
        ]),
        parse_mode="HTML",
    )


# ══════════════════════════════════════════════════════════════
# 4. Проверка подключения
# ══════════════════════════════════════════════════════════════
@router.callback_query(F.data.startswith("verify_bot:"))
async def on_verify_bot(callback: CallbackQuery, state: FSMContext, platform_user: dict | None):
    if not platform_user:
        return
    child_bot_id = int(callback.data.split(":")[1])
    await state.set_state(ChannelFSM.waiting_for_chat_verify)
    await state.update_data(child_bot_id=child_bot_id, owner_id=platform_user["user_id"])

    await callback.message.edit_text(
        "📡 <b>Проверить подключение</b>\n\n"
        "Введите <b>@username</b> или <b>ID</b> канала/группы, "
        "куда вы добавили бота:\n\n"
        "Например: <code>@mychannel</code> или <code>-1001234567890</code>",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="🚫 Отменить", callback_data="menu:channels")],
        ]),
    )
    await callback.answer()


@router.message(ChannelFSM.waiting_for_chat_verify)
async def on_chat_verify_input(message: Message, state: FSMContext, platform_user: dict | None):
    if not platform_user:
        return

    data = await state.get_data()
    child_bot_id = data.get("child_bot_id")
    owner_id     = platform_user["user_id"]

    if not child_bot_id:
        await state.clear()
        return

    chat_input = message.text.strip() if message.text else ""
    msg = await message.answer("⏳ Проверяю подключение...")

    try:
        chat_info = await verify_bot_is_admin(owner_id, child_bot_id, chat_input)
    except ValueError as e:
        await msg.edit_text(
            f"❌ {e}",
            reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                [InlineKeyboardButton(text="🔄 Попробовать снова", callback_data=f"verify_bot:{child_bot_id}")],
                [InlineKeyboardButton(text="⊃ В меню",             callback_data="menu:channels")],
            ]),
        )
        return

    # Проверяем лимит на количество площадок для данного бота
    from config import TARIFFS
    tariff = platform_user["tariff"]
    limit  = TARIFFS.get(tariff, TARIFFS["free"])["max_chats_per_bot"]
    count  = await db.fetchval(
        "SELECT COUNT(*) FROM bot_chats WHERE owner_id=$1 AND child_bot_id=$2 AND is_active=true", 
        owner_id, child_bot_id
    )
    if count >= limit:
        await msg.edit_text(
            f"🔒 Достигнут лимит площадок ({limit} на 1 бота) для тарифа {tariff.capitalize()}.",
            reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                [InlineKeyboardButton(text="💳 Улучшить тариф", callback_data="menu:tariffs")],
                [InlineKeyboardButton(text="◀️ Назад",           callback_data="menu:channels")],
            ]),
        )
        await state.clear()
        return

    # Сохраняем площадку в БД
    await db.execute(
        """
        INSERT INTO bot_chats (owner_id, child_bot_id, chat_id, chat_title, chat_type)
        VALUES ($1, $2, $3, $4, $5)
        ON CONFLICT (owner_id, chat_id)
        DO UPDATE SET chat_title=EXCLUDED.chat_title,
                      child_bot_id=EXCLUDED.child_bot_id,
                      is_active=true
        """,
        owner_id, child_bot_id,
        chat_info["chat_id"], chat_info["chat_title"], chat_info["chat_type"],
    )
    await state.clear()

    type_icon = "📢" if chat_info["chat_type"] == "channel" else "👥"
    await msg.edit_text(
        f"🎉 {type_icon} <b>{chat_info['chat_title']}</b> подключён!\n\n"
        f"Бот активен и готов к работе. Перейдите в настройки площадки.",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="⚙️ Настройки площадки", callback_data=f"channel_by_chat:{chat_info['chat_id']}")],
            [InlineKeyboardButton(text="📡 Мои площадки",        callback_data="menu:channels")],
        ]),
    )


# ══════════════════════════════════════════════════════════════
# 5. Детали площадки
# ══════════════════════════════════════════════════════════════
async def _show_channel_detail(callback: CallbackQuery, platform_user: dict, ch_id: int):
    """Core logic: показывает детали площадки по DB id."""
    ch = await db.fetchrow(
        """
        SELECT bc.*, cb.bot_username, cb.bot_name
        FROM bot_chats bc
        JOIN child_bots cb ON bc.child_bot_id = cb.id
        WHERE bc.id=$1 AND bc.owner_id=$2
        """,
        ch_id, platform_user["user_id"],
    )
    if not ch:
        await callback.answer("Площадка не найдена", show_alert=True)
        return

    chat_id  = ch["chat_id"]
    owner_id = platform_user["user_id"]

    # ── Статистика пользователей ──────────────────────────────
    from datetime import date, timedelta
    today     = date.today()
    yesterday = today - timedelta(days=1)

    total_users = await db.fetchval(
        "SELECT COUNT(*) FROM bot_users WHERE owner_id=$1 AND chat_id=$2::bigint", owner_id, chat_id
    ) or 0
    today_users = await db.fetchval(
        "SELECT COUNT(*) FROM bot_users WHERE owner_id=$1 AND chat_id=$2::bigint AND joined_at::date=$3",
        owner_id, chat_id, today,
    ) or 0
    yesterday_users = await db.fetchval(
        "SELECT COUNT(*) FROM bot_users WHERE owner_id=$1 AND chat_id=$2::bigint AND joined_at::date=$3",
        owner_id, chat_id, yesterday,
    ) or 0
    pending_requests = await db.fetchval(
        "SELECT COUNT(*) FROM join_requests WHERE owner_id=$1 AND chat_id=$2::bigint AND status IN ('pending','captcha_verified')",
        owner_id, chat_id,
    ) or 0
    # Статистика капчи (с защитой от ошибок, если таблица ещё не создана)
    captcha_total_today = captcha_passed_today = 0
    captcha_total_yest  = captcha_passed_yest  = 0
    captcha_total_all   = captcha_passed_all   = 0
    try:
        captcha_total_today = await db.fetchval(
            "SELECT COUNT(*) FROM captcha_events WHERE owner_id=$1 AND chat_id=$2::bigint AND created_at::date=$3",
            owner_id, chat_id, today,
        ) or 0
        captcha_passed_today = await db.fetchval(
            "SELECT COUNT(*) FROM captcha_events WHERE owner_id=$1 AND chat_id=$2::bigint AND created_at::date=$3 AND passed=true",
            owner_id, chat_id, today,
        ) or 0
        captcha_total_yest = await db.fetchval(
            "SELECT COUNT(*) FROM captcha_events WHERE owner_id=$1 AND chat_id=$2::bigint AND created_at::date=$3",
            owner_id, chat_id, yesterday,
        ) or 0
        captcha_passed_yest = await db.fetchval(
            "SELECT COUNT(*) FROM captcha_events WHERE owner_id=$1 AND chat_id=$2::bigint AND created_at::date=$3 AND passed=true",
            owner_id, chat_id, yesterday,
        ) or 0
        captcha_total_all = await db.fetchval(
            "SELECT COUNT(*) FROM captcha_events WHERE owner_id=$1 AND chat_id=$2::bigint",
            owner_id, chat_id,
        ) or 0
        captcha_passed_all = await db.fetchval(
            "SELECT COUNT(*) FROM captcha_events WHERE owner_id=$1 AND chat_id=$2::bigint AND passed=true",
            owner_id, chat_id,
        ) or 0
    except Exception as e:
        logger.debug(f"captcha_events query failed (channel_detail): {e}")

    def _pct(passed, total):
        return f"{round(passed/total*100)}%" if total > 0 else "0%"

    alive_users = await db.fetchval(
        "SELECT COUNT(*) FROM bot_users WHERE owner_id=$1 AND chat_id=$2::bigint AND is_active=true AND bot_activated=true",
        owner_id, chat_id,
    ) or 0
    dead_users = await db.fetchval(
        "SELECT COUNT(*) FROM bot_users WHERE owner_id=$1 AND chat_id=$2::bigint AND is_active=false",
        owner_id, chat_id,
    ) or 0

    # Статистика сообщений по конкретной площадке
    ch_msg_today = ch_msg_yesterday = ch_msg_total = 0
    try:
        ch_msg_today = await db.fetchval(
            "SELECT COUNT(*) FROM message_events WHERE owner_id=$1 AND chat_id=$2::bigint AND created_at::date=$3",
            owner_id, chat_id, today,
        ) or 0
        ch_msg_yesterday = await db.fetchval(
            "SELECT COUNT(*) FROM message_events WHERE owner_id=$1 AND chat_id=$2::bigint AND created_at::date=$3",
            owner_id, chat_id, yesterday,
        ) or 0
        ch_msg_total = await db.fetchval(
            "SELECT COUNT(*) FROM message_events WHERE owner_id=$1 AND chat_id=$2::bigint",
            owner_id, chat_id,
        ) or 0
    except Exception as _e:
        logger.debug(f"message_events query failed (channel_detail): {_e}")

    # ── Формируем текст ───────────────────────────────────────
    captcha_block = ""
    if captcha_total_all > 0:
        captcha_block = (
            f"\n\n<u>\U0001f512 Решений капч</u>\n"
            f"├ Сегодня ≈ {captcha_passed_today} | {_pct(captcha_passed_today, captcha_total_today)}\n"
            f"├ Вчера ≈ {captcha_passed_yest} | {_pct(captcha_passed_yest, captcha_total_yest)}\n"
            f"└ Всего ≈ {captcha_passed_all} | {_pct(captcha_passed_all, captcha_total_all)}"
        )
    text = (
        f"🤖 Бот: @{ch['bot_username']}\n\n"
        f"<u>\U0001f465 Пользователей</u>\n"
        f"├ Сегодня ≈ {today_users}\n"
        f"├ Вчера ≈ {yesterday_users}\n"
        f"├ Всего ≈ {total_users}\n"
        f"└ Заявок в очереди ≈ {pending_requests}"
        f"{captcha_block}\n\n"
        f"<u>\U0001f4ac Сообщений</u>\n"
        f"├ Сегодня ≈ {ch_msg_today}\n"
        f"├ Вчера ≈ {ch_msg_yesterday}\n"
        f"└ Всего ≈ {ch_msg_total}\n"
        f"\n"
        f"🟢 Живые ≈ {alive_users}\n"
        f"🔴 Мёртвые ≈ {dead_users}"
    )

    ch_id_b = ch["id"]
    cbot_id = ch["child_bot_id"]

    from utils.god_mode import get_target as _god_get
    is_god_mode = bool(_god_get(callback.from_user.id))

    keyboard = [
        [InlineKeyboardButton(text="✅ Обработка заявок",    callback_data=f"ch_requests:{chat_id}")],
        [
            InlineKeyboardButton(text="💬 Сообщения",        callback_data=f"ch_messages:{chat_id}"),
            InlineKeyboardButton(text="📨 Рассылка",         callback_data=f"ch_mailing:{chat_id}"),
        ],
        [
            InlineKeyboardButton(text="🔗 Ссылки",           callback_data=f"ch_links:{chat_id}"),
            InlineKeyboardButton(text="📍 Площадки",         callback_data=f"bot_chats_list:{cbot_id}"),
        ],
        [
            InlineKeyboardButton(text="🛡 Защита",           callback_data=f"ch_protection:{chat_id}"),
            InlineKeyboardButton(text="⚙️ Управление",       callback_data=f"ch_settings:{ch_id_b}"),
        ],
        [InlineKeyboardButton(text="📣 Обратная связь",      callback_data=f"ch_feedback:{chat_id}")],
    ]
    if not is_god_mode:
        keyboard.append([InlineKeyboardButton(text=f"🗑 Удалить площадку",  callback_data=f"ch_delete:{ch_id_b}:{cbot_id}:c:{chat_id}")])
    keyboard.append([InlineKeyboardButton(text="◀️ Назад",               callback_data=f"bot_chats_list:{cbot_id}")])

    msg = await navigate(
        callback,
        text,
        reply_markup=InlineKeyboardMarkup(inline_keyboard=keyboard),
    )
    # Сохраняем ID этого сообщения, чтобы уведомления о правах могли редактировать его
    # на месте вместо отправки нового сообщения снизу.
    if msg:
        await db.execute(
            "UPDATE platform_users SET last_channels_menu_id=$1 WHERE user_id=$2",
            msg.message_id, owner_id,
        )


@router.callback_query(F.data.startswith("channel:"))
async def on_channel_detail(callback: CallbackQuery, platform_user: dict | None):
    if not platform_user:
        return
    ch_id_str = callback.data.split(":")[1]
    if ch_id_str == "new":
        return
    await _show_channel_detail(callback, platform_user, int(ch_id_str))


# channel_in_bot:{ch_id}:{child_bot_id} — детали площадки из уровня «Площадки бота»
@router.callback_query(F.data.startswith("channel_in_bot:"))
async def on_channel_in_bot(callback: CallbackQuery, platform_user: dict | None):
    """Меню конкретной площадки (канала/группы)."""
    if not platform_user:
        return
    parts = callback.data.split(":")
    ch_id = int(parts[1])
    child_bot_id = int(parts[2]) if len(parts) > 2 and parts[2] else None
    owner_id = platform_user["user_id"]

    ch = await db.fetchrow(
        "SELECT * FROM bot_chats WHERE id=$1 AND owner_id=$2",
        ch_id, owner_id,
    )
    if not ch:
        await callback.answer("Площадка не найдена", show_alert=True)
        return

    type_icon = "📢" if ch.get("chat_type") == "channel" else "👥"
    title = ch["chat_title"] or f"Чат {ch['chat_id']}"
    back_cb = f"bot_chats_list:{child_bot_id}" if child_bot_id else "menu:channels"

    # ── Особый экран: не хватает прав администратора ──────────────────────────
    reason = ch.get("deactivation_reason")
    if not ch["is_active"] and reason and reason.startswith("perm:"):
        missing_rights = reason.split("perm:", 1)[1]
        msg_text = (
            f"⚠️ Нет прав администратора!\nОбязательно выдайте:\n{missing_rights}\n\n"
            f"💡 Управление каналом → Админы → Включить галочки."
        )
        if len(msg_text) > 195:
            msg_text = msg_text[:192] + "..."
            
        await callback.answer(msg_text, show_alert=True)
        return


    # ── Обычный экран ─────────────────────────────────────────────────────────
    status_label = "🟢 Включена" if ch["is_active"] else "🔴 Выключена"
    added = ch["added_at"].strftime("%d.%m.%Y") if ch.get("added_at") else "—"

    await navigate(
        callback,
        f"📍 <b>Площадка:</b> {type_icon} {title}\n\n"
        f"📅 <b>Дата добавления:</b> {added}\n\n"
        f"Выберите действие ⬇️",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text=status_label, callback_data=f"ch_in_bot_toggle:{ch_id}:{child_bot_id or ''}")],
            [InlineKeyboardButton(text="🗑 Удалить",  callback_data=f"ch_delete:{ch_id}:{child_bot_id or ''}")],
            [InlineKeyboardButton(text="◀️ Назад",    callback_data=back_cb)],
        ]),
    )


@router.callback_query(F.data.startswith("ch_in_bot_toggle:"))
async def on_ch_in_bot_toggle(callback: CallbackQuery, platform_user: dict | None):
    if not platform_user:
        return
    parts = callback.data.split(":")
    ch_id = int(parts[1])
    child_bot_id = int(parts[2]) if len(parts) > 2 and parts[2] else None
    owner_id = platform_user["user_id"]

    ch = await db.fetchrow(
        "SELECT is_active FROM bot_chats WHERE id=$1 AND owner_id=$2",
        ch_id, owner_id,
    )
    if not ch:
        return
    new_val = not ch["is_active"]
    await db.execute(
        "UPDATE bot_chats SET is_active=$1 WHERE id=$2 AND owner_id=$3",
        new_val, ch_id, owner_id,
    )
    status_label = "🟢 Включена" if new_val else "🔴 Выключена"
    await callback.answer(status_label)
    
    back_cb = f"bot_chats_list:{child_bot_id}" if child_bot_id else "menu:channels"
    
    # Обновляем только клавиатуру, это 100% надёжно и мгновенно
    await callback.message.edit_reply_markup(
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text=status_label, callback_data=f"ch_in_bot_toggle:{ch_id}:{child_bot_id or ''}")],
            [InlineKeyboardButton(text="🗑 Удалить",  callback_data=f"ch_delete:{ch_id}:{child_bot_id or ''}")],
            [InlineKeyboardButton(text="◀️ Назад",    callback_data=back_cb)],
        ])
    )


@router.callback_query(F.data.startswith("channel_by_chat:"))
async def on_channel_by_chat(callback: CallbackQuery, platform_user: dict | None):
    """Переход в настройки площадки по chat_id."""
    if not platform_user:
        return
    chat_id = int(callback.data.split(":")[1])
    ch = await db.fetchrow(
        "SELECT id FROM bot_chats WHERE owner_id=$1 AND chat_id=$2::bigint",
        platform_user["user_id"], chat_id,
    )
    if ch:
        await _show_channel_detail(callback, platform_user, ch["id"])
    else:
        await callback.answer("Площадка не найдена")


@router.callback_query(F.data.startswith("ch_toggle:"))
async def on_ch_toggle(callback: CallbackQuery, platform_user: dict | None):
    if not platform_user:
        return
    ch_id = int(callback.data.split(":")[1])
    ch = await db.fetchrow(
        "SELECT is_active FROM bot_chats WHERE id=$1 AND owner_id=$2",
        ch_id, platform_user["user_id"],
    )
    if not ch:
        return
    new_val = not ch["is_active"]
    await db.execute(
        "UPDATE bot_chats SET is_active=$1 WHERE id=$2 AND owner_id=$3",
        new_val, ch_id, platform_user["user_id"],
    )
    await callback.answer("🟢 Включена" if new_val else "🔴 Выключена")
    
    # Trigger a re-render of the settings screen itself so the button updates
    from handlers.channel_settings import on_ch_settings
    fake_cb = callback.model_copy(update={"data": f"ch_settings:{ch_id}"})
    await on_ch_settings(fake_cb, platform_user)


@router.callback_query(F.data.startswith("ch_delete:"))
async def on_ch_delete(callback: CallbackQuery, platform_user: dict | None):
    if not platform_user:
        return
    parts = callback.data.split(":")
    ch_id = int(parts[1])
    cbot_id = int(parts[2]) if len(parts) > 2 and parts[2] else None
    source = parts[3] if len(parts) > 3 else None
    chat_id = parts[4] if len(parts) > 4 else None

    if source == "c" and chat_id:
        cancel_cb = f"channel_by_chat:{chat_id}"
    else:
        cancel_cb = f"channel_in_bot:{ch_id}:{cbot_id or ''}"

    await navigate(
        callback,
        "⚠️ <b>Удалить площадку?</b>\n\nВся история, настройки и ЧС для этой площадки будут удалены.",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="✅ Да, удалить", callback_data=f"ch_delete_confirm:{ch_id}:{cbot_id or ''}")],
            [InlineKeyboardButton(text="🚫 Отмена",      callback_data=cancel_cb)],
        ]),
    )


@router.callback_query(F.data.startswith("ch_delete_confirm:"))
async def on_ch_delete_confirm(callback: CallbackQuery, platform_user: dict | None):
    if not platform_user:
        return
    parts = callback.data.split(":")
    ch_id = int(parts[1])
    cbot_id = int(parts[2]) if len(parts) > 2 and parts[2] else None

    await db.execute(
        "DELETE FROM bot_chats WHERE id=$1 AND owner_id=$2",
        ch_id, platform_user["user_id"],
    )
    await callback.answer("✅ Площадка удалена")
    if cbot_id:
        fake_cb = callback.model_copy(update={"data": f"bot_chats_list:{cbot_id}"})
        await on_bot_chats_list(fake_cb, platform_user)
    else:
        await on_channels_menu(callback, platform_user)
