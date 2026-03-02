"""
handlers/links.py — Управление ссылками-приглашениями и их статистика.

Навигация:
  bot_settings → bs_links:{child_bot_id} → _bs_channel_picker
    → ch_links:{chat_id}                [Экран 1: список ссылок]
      ↓ + Создать         |  ◀ Назад → bs_links:{child_bot_id} (picker)
    link_create:{chat_id} [Экран 2: выбор типа]
      ↓ тип выбран        |  ◀ Назад → ch_links:{chat_id}
    [FSM: имя → лимит → бюджет → создание]
      ◀ Назад → ch_links:{chat_id}
"""
import logging
import re
from aiogram import Router, F, Bot
from aiogram.types import CallbackQuery, Message, InlineKeyboardMarkup, InlineKeyboardButton
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup

import db.pool as db

logger = logging.getLogger(__name__)
router = Router()


class LinkFSM(StatesGroup):
    waiting_for_name   = State()
    waiting_for_limit  = State()
    waiting_for_budget = State()


# ── Клавиатуры ───────────────────────────────────────────────────


def kb_links_list(links: list, chat_id: int, child_bot_id: int,
                  page: int = 0) -> InlineKeyboardMarkup:
    """
    Экран 1: постраничный список ссылок.
    Back → выбор площадки (bs_links:{child_bot_id}).
    """
    PAGE = 5
    total = len(links)
    start = page * PAGE
    chunk = links[start:start + PAGE]

    buttons = []

    # Пагинация (показываем только если ссылок > PAGE)
    if total > PAGE:
        nav = []
        if page > 0:
            nav.append(InlineKeyboardButton(text="◀",
                        callback_data=f"links_page:{chat_id}:{child_bot_id}:{page-1}"))
        nav.append(InlineKeyboardButton(
            text=f"{page+1}/{(total-1)//PAGE+1}", callback_data="noop"))
        if start + PAGE < total:
            nav.append(InlineKeyboardButton(text="▶",
                        callback_data=f"links_page:{chat_id}:{child_bot_id}:{page+1}"))
        buttons.append(nav)

    for link in chunk:
        type_icon = {"request": "✅", "regular": "🔗", "onetime": "🔢"}.get(
            link["link_type"], "🔗")
        buttons.append([InlineKeyboardButton(
            text=f"{type_icon} {link['name'][:30]}",
            callback_data=f"link_detail:{link['id']}:{chat_id}:{child_bot_id}",
        )])

    buttons.append([InlineKeyboardButton(
        text="➕ Создать ссылку",
        callback_data=f"link_create:{chat_id}:{child_bot_id}",
    )])
    buttons.append([InlineKeyboardButton(
        text="◀️ Назад",
        callback_data=f"bs_links:{child_bot_id}",
    )])
    return InlineKeyboardMarkup(inline_keyboard=buttons)


def kb_link_types(chat_id: int, child_bot_id: int) -> InlineKeyboardMarkup:
    """Экран 2: выбор типа ссылки. Back → Экран 1."""
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="✅ Ссылка с заявкой",
                              callback_data=f"link_type:{chat_id}:{child_bot_id}:request")],
        [InlineKeyboardButton(text="🔗 Обычная ссылка",
                              callback_data=f"link_type:{chat_id}:{child_bot_id}:regular")],
        [InlineKeyboardButton(text="◀️ Назад",
                              callback_data=f"ch_links:{chat_id}:{child_bot_id}")],
    ])


def kb_link_detail(link_id: int, chat_id: int, child_bot_id: int) -> InlineKeyboardMarkup:
    """Экран детали ссылки. Back → Экран 1."""
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="↗️ Поделиться",
                              callback_data=f"link_share:{link_id}")],
        [InlineKeyboardButton(text="🗑 Удалить",
                              callback_data=f"link_delete:{link_id}:{chat_id}:{child_bot_id}")],
        [InlineKeyboardButton(text="◀️ Назад",
                              callback_data=f"ch_links:{chat_id}:{child_bot_id}")],
    ])


# ── Вспомогательная: рендер Экрана 1 ──────────────────────────

async def _show_links_screen(callback: CallbackQuery, platform_user: dict,
                              chat_id: int, child_bot_id: int, page: int = 0):
    """Рендерит Экран 1 — список ссылок площадки."""
    ch = await db.fetchrow(
        "SELECT chat_title FROM bot_chats WHERE owner_id=$1 AND chat_id=$2",
        platform_user["user_id"], chat_id,
    )
    links = await db.fetch(
        "SELECT * FROM invite_links WHERE owner_id=$1 AND chat_id=$2::bigint AND is_active=true "
        "ORDER BY created_at DESC",
        platform_user["user_id"], chat_id,
    )
    title = ch["chat_title"] if ch else str(chat_id)
    count = len(links)
    if count == 0:
        body = "Ссылок пока нет. Создайте первую!"
    else:
        body = f"Активных ссылок: <b>{count}</b>"

    await callback.message.edit_text(
        f"🔗 <b>Ссылки — {title}</b>\n\n{body}",
        parse_mode="HTML",
        reply_markup=kb_links_list(list(links), chat_id, child_bot_id, page),
    )
    await callback.answer()


# ── Экран 1: список ссылок ────────────────────────────────────

@router.callback_query(F.data.startswith("ch_links:"))
async def on_links_list(callback: CallbackQuery, platform_user: dict | None):
    if not platform_user:
        return
    parts = callback.data.split(":")
    chat_id = int(parts[1])
    # child_bot_id может быть передан вторым параметром или нет
    child_bot_id = int(parts[2]) if len(parts) > 2 else None

    # Если child_bot_id не передан — ищем его в БД
    if child_bot_id is None:
        row = await db.fetchrow(
            "SELECT child_bot_id FROM bot_chats WHERE owner_id=$1 AND chat_id=$2::bigint",
            platform_user["user_id"], chat_id,
        )
        child_bot_id = row["child_bot_id"] if row else 0

    await _show_links_screen(callback, platform_user, chat_id, child_bot_id)


@router.callback_query(F.data.startswith("links_page:"))
async def on_links_page(callback: CallbackQuery, platform_user: dict | None):
    if not platform_user:
        return
    _, chat_id_s, child_bot_id_s, page_s = callback.data.split(":")
    await _show_links_screen(
        callback, platform_user,
        int(chat_id_s), int(child_bot_id_s), int(page_s),
    )


# ── Экран 2: выбор типа ссылки ────────────────────────────────

@router.callback_query(F.data.startswith("link_create:"))
async def on_link_create(callback: CallbackQuery, state: FSMContext,
                          platform_user: dict | None):
    if not platform_user:
        return
    parts = callback.data.split(":")
    chat_id = parts[1]
    child_bot_id = parts[2] if len(parts) > 2 else "0"

    await state.update_data(
        chat_id=chat_id,
        child_bot_id=child_bot_id,
        owner_id=platform_user["user_id"],
    )
    await callback.message.edit_text(
        "➕ <b>Какую ссылку необходимо создать?</b>",
        parse_mode="HTML",
        reply_markup=kb_link_types(chat_id, child_bot_id),
    )
    await callback.answer()


# ── Выбор типа → ввод имени ──────────────────────────────────

@router.callback_query(F.data.startswith("link_type:"))
async def on_link_type(callback: CallbackQuery, state: FSMContext):
    parts = callback.data.split(":")
    chat_id, child_bot_id, link_type = parts[1], parts[2], parts[3]
    await state.update_data(link_type=link_type)
    await state.set_state(LinkFSM.waiting_for_name)
    await callback.message.edit_text(
        "🔗 <b>Создание ссылки</b>\n\nОтправьте название ссылки:\n"
        "(Например: «Реклама Google» или «Инфлюенсер Иван»)",
        parse_mode="HTML",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="🚫 Отменить",
                                   callback_data=f"ch_links:{chat_id}:{child_bot_id}")]
        ]),
    )
    await callback.answer()


# ── FSM: имя ──────────────────────────────────────────────────

@router.message(LinkFSM.waiting_for_name)
async def on_link_name(message: Message, state: FSMContext):
    from services.security import sanitize
    name = sanitize(message.text, max_len=128)
    await state.update_data(name=name)
    await state.set_state(LinkFSM.waiting_for_limit)
    data = await state.get_data()
    chat_id = data.get("chat_id", "0")
    child_bot_id = data.get("child_bot_id", "0")
    await message.answer(
        "🔗 <b>Создание ссылки</b>\n\n"
        "💡 Укажите лимит переходов (или пропустите):\n"
        "Например: 100 — ссылка сработает только для 100 человек.",
        parse_mode="HTML",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="→ Пропустить",
                                   callback_data="link_skip_limit")]
        ]),
    )


# ── FSM: лимит ────────────────────────────────────────────────

@router.callback_query(F.data == "link_skip_limit")
@router.message(LinkFSM.waiting_for_limit)
async def on_link_limit(event, state: FSMContext):
    limit = None
    if isinstance(event, Message):
        try:
            limit = int(event.text.strip())
        except ValueError:
            pass
    await state.update_data(member_limit=limit)
    await state.set_state(LinkFSM.waiting_for_budget)

    respond = event.answer if isinstance(event, Message) else event.message.edit_text
    await respond(
        "🔗 <b>Создание ссылки</b>\n\n"
        "💡 Укажите бюджет этой ссылки (сколько потрачено на рекламу):\n"
        "Пример: 1000₽ или 50$\n\n"
        "🎯 Бот посчитает стоимость подписчика автоматически.",
        parse_mode="HTML",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="→ Пропустить",
                                   callback_data="link_skip_budget")]
        ]),
    )
    if isinstance(event, CallbackQuery):
        await event.answer()


# ── FSM: бюджет → создание ────────────────────────────────────

@router.callback_query(F.data == "link_skip_budget")
@router.message(LinkFSM.waiting_for_budget)
async def on_link_budget(event, state: FSMContext, bot: Bot):
    data = await state.get_data()
    budget = None
    budget_currency = None

    if isinstance(event, Message):
        raw = event.text.strip()
        m = re.match(r"([\d.]+)\s*([₽$€])?", raw)
        if m:
            budget = float(m.group(1))
            cur_map = {"₽": "RUB", "$": "USD", "€": "EUR"}
            budget_currency = cur_map.get(m.group(2), "USD")

    await state.clear()

    chat_id = int(data["chat_id"])
    child_bot_id = data.get("child_bot_id", "0")
    link_type = data.get("link_type", "request")
    member_limit = data.get("member_limit")
    owner_id = data.get("owner_id")

    # ── Получаем токен дочернего бота ──────────────────────────
    # Именно дочерний бот является администратором канала/группы,
    # поэтому только он может создавать ссылки-приглашения.
    bot_row = await db.fetchrow(
        """SELECT cb.token_encrypted
           FROM child_bots cb
           JOIN bot_chats bc ON bc.child_bot_id = cb.id
           WHERE bc.owner_id=$1 AND bc.chat_id=$2::bigint AND bc.is_active=true
           LIMIT 1""",
        owner_id, chat_id,
    )

    from services.security import decrypt_token
    from aiogram import Bot as AioBot

    if bot_row:
        child_bot_instance = AioBot(token=decrypt_token(bot_row["token_encrypted"]))
    else:
        child_bot_instance = bot  # Fallback — маловероятен, но не ломает

    respond = event.answer if isinstance(event, Message) else event.message.edit_text

    try:
        if link_type == "request":
            tg_link = await child_bot_instance.create_chat_invite_link(
                chat_id, creates_join_request=True, name=data["name"]
            )
        elif link_type == "onetime":
            tg_link = await child_bot_instance.create_chat_invite_link(
                chat_id, member_limit=1, name=data["name"]
            )
        else:
            kwargs = {"name": data["name"]}
            if member_limit:
                kwargs["member_limit"] = member_limit
            tg_link = await child_bot_instance.create_chat_invite_link(chat_id, **kwargs)
    except Exception as e:
        await respond(f"❌ Не удалось создать ссылку: {e}")
        return
    finally:
        if bot_row:
            await child_bot_instance.session.close()

    link_id = await db.fetchval(
        """
        INSERT INTO invite_links
          (owner_id, chat_id, name, link, link_type, member_limit, budget, budget_currency)
        VALUES ($1,$2,$3,$4,$5,$6,$7,$8) RETURNING id
        """,
        owner_id, chat_id, data["name"], tg_link.invite_link,
        link_type, member_limit, budget, budget_currency,
    )

    await respond(
        f"✅ <b>Ссылка создана!</b>\n\n"
        f"<code>{tg_link.invite_link}</code>",
        parse_mode="HTML",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="📊 Детали ссылки",
                                   callback_data=f"link_detail:{link_id}:{chat_id}:{child_bot_id}")],
            [InlineKeyboardButton(text="◀️ К списку ссылок",
                                   callback_data=f"ch_links:{chat_id}:{child_bot_id}")],
        ]),
    )
    if isinstance(event, CallbackQuery):
        await event.answer()


# ── Детали ссылки ─────────────────────────────────────────────

@router.callback_query(F.data.startswith("link_detail:"))
async def on_link_detail(callback: CallbackQuery, platform_user: dict | None):
    if not platform_user:
        return
    parts = callback.data.split(":")
    link_id = int(parts[1])
    chat_id = int(parts[2]) if len(parts) > 2 else 0
    child_bot_id = int(parts[3]) if len(parts) > 3 else 0

    link = await db.fetchrow(
        "SELECT * FROM invite_links WHERE id=$1 AND owner_id=$2",
        link_id, platform_user["user_id"],
    )
    if not link:
        await callback.answer("Ссылка не найдена", show_alert=True)
        return

    # Если chat_id не передан — берём из link
    if not chat_id:
        chat_id = link["chat_id"]

    # Стоимость подписчика
    cost_text = ""
    joined = link.get("joined") or 0
    unsub  = link.get("unsubscribed") or 0
    if link["budget"] and joined > 0:
        per_join = link["budget"] / joined
        remained = joined - unsub
        per_stayed = link["budget"] / remained if remained > 0 else 0
        cur = link["budget_currency"] or ""
        cost_text = (
            f"\n💰 <b>Стоимость подписчика:</b>\n"
            f"● Бюджет: {link['budget']:.2f}{cur}\n"
            f"● За вступившего: {per_join:.2f}{cur}\n"
            f"● За оставшегося: {per_stayed:.2f}{cur}"
        )

    type_map = {"request": "С заявкой", "regular": "Обычная", "onetime": "Одноразовая"}
    await callback.message.edit_text(
        f"📊 <b>Ссылка — «{link['name']}»</b>\n\n"
        f"🔗 <code>{link['link']}</code>\n"
        f"🔒 Тип: {type_map.get(link['link_type'], link['link_type'])}\n\n"
        f"👥 <b>Подписчики</b>\n"
        f"├ Подписалось: {joined}\n"
        f"├ Отписалось: {unsub}\n"
        f"└ Осталось: {joined - unsub}\n"
        f"{cost_text}\n"
        f"📅 Создана: {link['created_at'].strftime('%d.%m.%Y')}",
        parse_mode="HTML",
        reply_markup=kb_link_detail(link_id, chat_id, child_bot_id),
    )
    await callback.answer()


# ── Удаление ссылки ───────────────────────────────────────────

@router.callback_query(F.data.startswith("link_delete:"))
async def on_link_delete(callback: CallbackQuery, platform_user: dict | None):
    if not platform_user:
        return
    parts = callback.data.split(":")
    link_id = int(parts[1])
    chat_id = int(parts[2]) if len(parts) > 2 else 0
    child_bot_id = int(parts[3]) if len(parts) > 3 else 0

    await db.execute(
        "UPDATE invite_links SET is_active=false WHERE id=$1 AND owner_id=$2",
        link_id, platform_user["user_id"],
    )
    await callback.answer("✅ Ссылка удалена")
    # Возвращаемся к Экрану 1
    callback.data = f"ch_links:{chat_id}:{child_bot_id}"
    await on_links_list(callback, platform_user)
