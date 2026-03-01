"""
handlers/links.py — Управление ссылками-приглашениями и их статистика.
"""
import logging
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


def kb_links_list(links: list, chat_id: int) -> InlineKeyboardMarkup:
    buttons = []
    # Постраничный список
    for link in links[:8]:
        buttons.append([InlineKeyboardButton(
            text=link["name"][:30],
            callback_data=f"link_detail:{link['id']}",
        )])
    buttons.append([InlineKeyboardButton(text="➡️ Создать ссылку", callback_data=f"link_create:{chat_id}")])
    buttons.append([InlineKeyboardButton(text="◀️ Назад", callback_data=f"channel_by_chat:{chat_id}")])
    return InlineKeyboardMarkup(inline_keyboard=buttons)


def kb_link_types(chat_id: int) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="✅ Ссылка с заявкой",  callback_data=f"link_type:{chat_id}:request")],
        [InlineKeyboardButton(text="🔗 Обычная ссылка",   callback_data=f"link_type:{chat_id}:regular")],
        [InlineKeyboardButton(text="🔢 Одноразовая ссылка", callback_data=f"link_type:{chat_id}:onetime")],
        [InlineKeyboardButton(text="◀️ Назад",             callback_data=f"ch_links:{chat_id}")],
    ])


def kb_link_detail(link_id: int, is_active: bool) -> InlineKeyboardMarkup:
    autoaccept = "🔄 Автопринятие: базовое"
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="↗️ Поделиться",  callback_data=f"link_share:{link_id}")],
        [InlineKeyboardButton(text=autoaccept,        callback_data=f"link_autoaccept:{link_id}")],
        [InlineKeyboardButton(text="🗑 Удалить",      callback_data=f"link_delete:{link_id}")],
        [InlineKeyboardButton(text="◀️ Назад",        callback_data="menu:channels")],
    ])


# ── Список ссылок канала ──────────────────────────────────────
@router.callback_query(F.data.startswith("ch_links:"))
async def on_links_list(callback: CallbackQuery, platform_user: dict | None):
    if not platform_user:
        return
    chat_id = int(callback.data.split(":")[1])
    ch = await db.fetchrow(
        "SELECT chat_title FROM bot_chats WHERE owner_id=$1 AND chat_id=$2",
        platform_user["user_id"], chat_id,
    )
    links = await db.fetch(
        "SELECT * FROM invite_links WHERE owner_id=$1 AND chat_id=$2::bigint AND is_active=true "
        "ORDER BY created_at DESC",
        platform_user["user_id"], chat_id,
    )
    await callback.message.edit_text(
        f"🔗 <b>Ссылки — {ch['chat_title'] if ch else chat_id}</b>\n\n"
        f"Ссылок: {len(links)}",
        reply_markup=kb_links_list(list(links), chat_id),
    )
    await callback.answer()


# ── Выбор типа ссылки ─────────────────────────────────────────
@router.callback_query(F.data.startswith("link_create:"))
async def on_link_create(callback: CallbackQuery, state: FSMContext, platform_user: dict | None):
    if not platform_user:
        return
    chat_id = callback.data.split(":")[1]
    await state.update_data(chat_id=chat_id, owner_id=platform_user["user_id"])
    await callback.message.edit_text(
        "🔗 <b>Какую ссылку необходимо создать?</b>",
        reply_markup=kb_link_types(chat_id),
    )
    await callback.answer()


@router.callback_query(F.data.startswith("link_type:"))
async def on_link_type(callback: CallbackQuery, state: FSMContext):
    parts = callback.data.split(":")
    chat_id, link_type = parts[1], parts[2]
    await state.update_data(link_type=link_type)
    await state.set_state(LinkFSM.waiting_for_name)
    await callback.message.edit_text(
        "🔗 <b>Создание ссылки</b>\n\nОтправьте название ссылки:\n"
        "(Например: «Реклама Google» или «Инфлюенсер Иван»)",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="🚫 Отменить", callback_data=f"link_create:{chat_id}")]
        ]),
    )
    await callback.answer()


@router.message(LinkFSM.waiting_for_name)
async def on_link_name(message: Message, state: FSMContext):
    from services.security import sanitize
    name = sanitize(message.text, max_len=128)
    await state.update_data(name=name)
    await state.set_state(LinkFSM.waiting_for_limit)
    await message.answer(
        "🔗 <b>Создание ссылки</b>\n\n"
        "💡 Укажите лимит переходов (или пропустите):\n"
        "Например: 100 — ссылка сработает только для 100 человек.",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="→ Пропустить", callback_data="link_skip_limit")]
        ]),
    )


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
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="→ Пропустить", callback_data="link_skip_budget")]
        ]),
    )
    if isinstance(event, CallbackQuery):
        await event.answer()


@router.callback_query(F.data == "link_skip_budget")
@router.message(LinkFSM.waiting_for_budget)
async def on_link_budget(event, state: FSMContext, bot: Bot):
    data = await state.get_data()
    budget = None
    budget_currency = None

    if isinstance(event, Message):
        raw = event.text.strip()
        import re
        m = re.match(r"([\d.]+)\s*([₽$€])?", raw)
        if m:
            budget = float(m.group(1))
            cur_map = {"₽": "RUB", "$": "USD", "€": "EUR"}
            budget_currency = cur_map.get(m.group(2), "USD")

    await state.update_data(budget=budget, budget_currency=budget_currency)
    await state.clear()

    # Создаём ссылку в Telegram
    chat_id = int(data["chat_id"])
    link_type = data.get("link_type", "request")
    member_limit = data.get("member_limit")

    try:
        if link_type == "request":
            tg_link = await bot.create_chat_invite_link(
                chat_id, creates_join_request=True, name=data["name"]
            )
        elif link_type == "onetime":
            tg_link = await bot.create_chat_invite_link(
                chat_id, member_limit=1, name=data["name"]
            )
        else:
            kwargs = {"name": data["name"]}
            if member_limit:
                kwargs["member_limit"] = member_limit
            tg_link = await bot.create_chat_invite_link(chat_id, **kwargs)
    except Exception as e:
        respond = event.answer if isinstance(event, Message) else event.message.edit_text
        await respond(f"❌ Не удалось создать ссылку: {e}")
        return

    # Сохраняем в БД
    link_id = await db.fetchval(
        """
        INSERT INTO invite_links
          (owner_id, chat_id, name, link, link_type, member_limit, budget, budget_currency)
        VALUES ($1,$2,$3,$4,$5,$6,$7,$8) RETURNING id
        """,
        data["owner_id"], chat_id, data["name"], tg_link.invite_link,
        link_type, member_limit, budget, budget_currency,
    )

    respond = event.answer if isinstance(event, Message) else event.message.edit_text
    await respond(
        f"✅ <b>Ссылка создана!</b>\n\n"
        f"<code>{tg_link.invite_link}</code>",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="↗️ Поделиться",    callback_data=f"link_share:{link_id}")],
            [InlineKeyboardButton(text="➡️ Детали ссылки", callback_data=f"link_detail:{link_id}")],
        ]),
    )
    if isinstance(event, CallbackQuery):
        await event.answer()


# ── Статистика ссылки ─────────────────────────────────────────
@router.callback_query(F.data.startswith("link_detail:"))
async def on_link_detail(callback: CallbackQuery, platform_user: dict | None):
    if not platform_user:
        return
    link_id = int(callback.data.split(":")[1])
    link = await db.fetchrow(
        "SELECT * FROM invite_links WHERE id=$1 AND owner_id=$2",
        link_id, platform_user["user_id"],
    )
    if not link:
        await callback.answer("Ссылка не найдена", show_alert=True)
        return

    # Стоимость подписчика
    cost_text = ""
    if link["budget"] and link["joined"] > 0:
        per_join = link["budget"] / link["joined"]
        remained = link["joined"] - link["unsubscribed"]
        per_stayed = link["budget"] / remained if remained > 0 else 0
        cur = link["budget_currency"] or ""
        cost_text = (
            f"\n💰 <b>Стоимость подписчика:</b>\n"
            f"● Бюджет: {link['budget']:.2f}{cur}\n"
            f"● За вступившего: {per_join:.2f}{cur}\n"
            f"● За оставшегося: {per_stayed:.2f}{cur}"
        )

    await callback.message.edit_text(
        f"📊 <b>Статистика — «{link['name']}»</b>\n\n"
        f"🔗 <code>{link['link']}</code>\n"
        f"🔒 Тип: {link['link_type']}\n\n"
        f"👥 <b>Подписчики</b>\n"
        f"├ Подписалось: {link['joined']}\n"
        f"├ Отписалось: {link['unsubscribed']}\n"
        f"└ Осталось: {link['joined'] - link['unsubscribed']}\n"
        f"{cost_text}\n"
        f"📅 Создана: {link['created_at'].strftime('%d.%m.%Y')}",
        reply_markup=kb_link_detail(link_id, link["is_active"]),
    )
    await callback.answer()


# ── Удаление ссылки ───────────────────────────────────────────
@router.callback_query(F.data.startswith("link_delete:"))
async def on_link_delete(callback: CallbackQuery, platform_user: dict | None):
    if not platform_user:
        return
    link_id = int(callback.data.split(":")[1])
    await db.execute(
        "UPDATE invite_links SET is_active=false WHERE id=$1 AND owner_id=$2",
        link_id, platform_user["user_id"],
    )
    await callback.answer("✅ Ссылка удалена")
    # Возвращаемся в общее меню площадок
    await callback.message.edit_text(
        "✅ Ссылка удалена.",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="◀️ Назад", callback_data="menu:channels")],
        ]),
    )
