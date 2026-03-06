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
import html
import json
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
    Экран 1: постраничный список ссылок (3 на странице).
    Навигация [◄ | крайняя | ►] — показывается ВСЕГДА.
    Back → выбор площадки (bs_links:{child_bot_id}).
    """
    PAGE = 3
    total = len(links)
    last_page = max(0, (total - 1) // PAGE) if total > 0 else 0
    page = max(0, min(page, last_page))
    start = page * PAGE
    chunk = links[start:start + PAGE]

    buttons = []

    # Навигационная строка — ВСЕГДА
    nav = [
        InlineKeyboardButton(
            text="◄",
            callback_data=f"links_page:{chat_id}:{child_bot_id}:{page - 1}" if page > 0 else "noop",
        ),
        InlineKeyboardButton(
            text="крайняя",
            callback_data=f"links_page:{chat_id}:{child_bot_id}:{last_page}",
        ),
        InlineKeyboardButton(
            text="►",
            callback_data=f"links_page:{chat_id}:{child_bot_id}:{page + 1}" if page < last_page else "noop",
        ),
    ]
    buttons.append(nav)

    # Ссылки текущей страницы — каждая отдельной кнопкой
    AUTO_LABEL = {"on": " (включено)", "off": " (выключено)"}
    for link in chunk:
        lk_d = dict(link)
        type_icon = {"request": "✅", "regular": "🔗", "onetime": "🔢"}.get(
            lk_d.get("link_type", ""), "🔗")
        auto = lk_d.get("auto_accept") or "base"
        auto_suffix = AUTO_LABEL.get(auto, "")  # пусто если base
        name = (lk_d.get("name") or "")[:27]
        buttons.append([InlineKeyboardButton(
            text=f"{type_icon} {name}{auto_suffix}",
            callback_data=f"link_detail:{lk_d['id']}:{chat_id}:{child_bot_id}",
        )])

    buttons.append([InlineKeyboardButton(
        text="+ Создать ссылку",
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


def kb_link_detail(link_id: int, chat_id: int, child_bot_id: int,
                   auto_accept: str = "base") -> InlineKeyboardMarkup:
    """Кнопки экрана деталей ссылки."""
    auto_label = {
        "base":    "♓️ Автопринятие: базовое",
        "on":      "⚡ Автопринятие: включено",
        "off":     "❌ Автопринятие: выключено",
    }.get(auto_accept, "♓️ Автопринятие: базовое")
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="↗️ Поделиться",
                              callback_data=f"link_share:{link_id}")],
        [InlineKeyboardButton(text=auto_label,
                              callback_data=f"link_auto_accept:{link_id}:{chat_id}:{child_bot_id}")],
        [InlineKeyboardButton(text="🗑 Удалить",
                              callback_data=f"link_delete:{link_id}:{chat_id}:{child_bot_id}")],
        [InlineKeyboardButton(text="◀️ Назад",
                              callback_data=f"ch_links:{chat_id}:{child_bot_id}")],
    ])


# ── Вспомогательная: рендер Экрана 1 ──────────────────────────

async def _show_links_screen(callback: CallbackQuery, platform_user: dict,
                              chat_id: int, child_bot_id: int, page: int = 0):
    """Рендерит Экран 1 — список ссылок площадки."""
    links = await db.fetch(
        "SELECT * FROM invite_links WHERE owner_id=$1 AND chat_id=$2::bigint AND is_active=true "
        "ORDER BY created_at DESC",
        platform_user["user_id"], chat_id,
    )

    if len(links) == 0:
        body = "Ссылок пока нет. Создайте первую!"
    else:
        body = "Выберите ссылку или создайте новую:"

    await callback.message.edit_text(
        f"🔗 <b>Ссылки</b>\n\n{body}",
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
    await state.update_data(link_type=link_type, chat_id=chat_id, child_bot_id=child_bot_id)
    await state.set_state(LinkFSM.waiting_for_name)

    # Редактируем текущее сообщение и сохраняем его message_id как prompt
    edited = await callback.message.edit_text(
        "🔗 <b>Создание ссылки</b>\n\nОтправьте название ссылки:\n"
        "(Например: «Реклама Google» или «Инфлюенсер Иван»)",
        parse_mode="HTML",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="🚫 Отменить",
                                   callback_data=f"ch_links:{chat_id}:{child_bot_id}")]
        ]),
    )
    # Сохраняем ID и chat_id сообщения-инструкции в FSM
    await state.update_data(
        prompt_msg_id=callback.message.message_id,
        prompt_chat_id=callback.message.chat.id,
    )
    await callback.answer()


# ── FSM: имя ──────────────────────────────────────────────────

@router.message(LinkFSM.waiting_for_name)
async def on_link_name(message: Message, state: FSMContext, bot: Bot):
    from services.security import sanitize
    name = sanitize(message.text or "", max_len=128)
    data = await state.get_data()
    chat_id = data.get("chat_id", "0")
    child_bot_id = data.get("child_bot_id", "0")
    prompt_msg_id = data.get("prompt_msg_id")
    prompt_chat_id = data.get("prompt_chat_id", message.chat.id)

    await state.update_data(name=name)
    await state.set_state(LinkFSM.waiting_for_limit)

    # Удаляем сообщение пользователя
    try:
        await message.delete()
    except Exception:
        pass

    # Редактируем инструкцию в том же сообщении
    limit_kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="→ Пропустить", callback_data="link_skip_limit")]
    ])
    limit_text = (
        "🔗 <b>Создание ссылки</b>\n\n"
        f"✅ Название: <b>{name}</b>\n\n"
        "💡 Укажите лимит переходов (или пропустите):\n"
        "Например: 100 — ссылка сработает только для 100 человек."
    )
    if prompt_msg_id:
        try:
            await bot.edit_message_text(
                text=limit_text,
                chat_id=prompt_chat_id,
                message_id=prompt_msg_id,
                parse_mode="HTML",
                reply_markup=limit_kb,
            )
        except Exception:
            sent = await message.answer(limit_text, parse_mode="HTML", reply_markup=limit_kb)
            await state.update_data(prompt_msg_id=sent.message_id, prompt_chat_id=message.chat.id)
    else:
        sent = await message.answer(limit_text, parse_mode="HTML", reply_markup=limit_kb)
        await state.update_data(prompt_msg_id=sent.message_id, prompt_chat_id=message.chat.id)


# ── FSM: лимит ────────────────────────────────────────────────

@router.callback_query(F.data == "link_skip_limit")
@router.message(LinkFSM.waiting_for_limit)
async def on_link_limit(event, state: FSMContext, bot: Bot):
    limit = None
    if isinstance(event, Message):
        try:
            limit = int(event.text.strip())
        except ValueError:
            pass
        # Удаляем сообщение пользователя
        try:
            await event.delete()
        except Exception:
            pass

    await state.update_data(member_limit=limit)
    await state.set_state(LinkFSM.waiting_for_budget)

    data = await state.get_data()
    prompt_msg_id = data.get("prompt_msg_id")
    prompt_chat_id = data.get("prompt_chat_id")

    budget_text = (
        "🔗 <b>Создание ссылки</b>\n\n"
        f"✅ Название: <b>{data.get('name', '—')}</b>\n"
        f"✅ Лимит: <b>{limit if limit else 'без лимита'}</b>\n\n"
        "💡 Укажите бюджет этой ссылки (сколько потрачено на рекламу):\n"
        "Пример: 1000₽ или 50$\n\n"
        "🎯 Бот посчитает стоимость подписчика автоматически."
    )
    budget_kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="→ Пропустить", callback_data="link_skip_budget")]
    ])

    if isinstance(event, CallbackQuery):
        await event.message.edit_text(budget_text, parse_mode="HTML", reply_markup=budget_kb)
        await event.answer()
    else:
        if prompt_msg_id and prompt_chat_id:
            try:
                await bot.edit_message_text(
                    text=budget_text,
                    chat_id=prompt_chat_id,
                    message_id=prompt_msg_id,
                    parse_mode="HTML",
                    reply_markup=budget_kb,
                )
            except Exception:
                sent = await event.answer(budget_text, parse_mode="HTML", reply_markup=budget_kb)
                await state.update_data(prompt_msg_id=sent.message_id, prompt_chat_id=event.chat.id)
        else:
            sent = await event.answer(budget_text, parse_mode="HTML", reply_markup=budget_kb)
            await state.update_data(prompt_msg_id=sent.message_id, prompt_chat_id=event.chat.id)


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
        # Удаляем сообщение пользователя
        try:
            await event.delete()
        except Exception:
            pass

    prompt_msg_id = data.get("prompt_msg_id")
    prompt_chat_id = data.get("prompt_chat_id")

    await state.clear()

    chat_id = int(data["chat_id"])
    child_bot_id = data.get("child_bot_id", "0")
    link_type = data.get("link_type", "request")
    member_limit = data.get("member_limit")
    owner_id = data.get("owner_id")

    # ── Получаем токен дочернего бота ──────────────────────────
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
        child_bot_instance = bot

    # Функция для финального ответа — всегда редактируем prompt
    async def _edit_final(text: str, kb=None):
        if isinstance(event, CallbackQuery):
            await event.message.edit_text(text, parse_mode="HTML", reply_markup=kb)
            await event.answer()
        elif prompt_msg_id and prompt_chat_id:
            try:
                await bot.edit_message_text(
                    text=text, chat_id=prompt_chat_id,
                    message_id=prompt_msg_id, parse_mode="HTML", reply_markup=kb,
                )
            except Exception:
                await event.answer(text, parse_mode="HTML", reply_markup=kb)
        else:
            await event.answer(text, parse_mode="HTML", reply_markup=kb)

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
        await _edit_final(f"❌ Не удалось создать ссылку: {e}")
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

    await _edit_final(
        f"✅ <b>Ссылка создана!</b>\n\n"
        f"<code>{tg_link.invite_link}</code>",
        kb=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="📊 Детали ссылки",
                                   callback_data=f"link_detail:{link_id}:{chat_id}:{child_bot_id}")],
            [InlineKeyboardButton(text="◀️ К списку ссылок",
                                   callback_data=f"ch_links:{chat_id}:{child_bot_id}")],
        ]),
    )


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

    try:
        if not chat_id:
            chat_id = link["chat_id"]

        lk = dict(link)

        joined   = lk.get("joined") or 0
        unsub    = lk.get("unsubscribed") or 0
        remained = joined - unsub

        males        = lk.get("males") or 0
        females      = lk.get("females") or 0
        total_gender = males + females
        m_pct = f"{males / total_gender * 100:.2f}" if total_gender > 0 else "0.00"
        f_pct = f"{females / total_gender * 100:.2f}" if total_gender > 0 else "0.00"

        countries_raw = lk.get("countries") or {}
        if isinstance(countries_raw, str):
            try:
                countries_raw = json.loads(countries_raw)
            except Exception:
                countries_raw = {}
        if countries_raw:
            sorted_c = sorted(countries_raw.items(), key=lambda x: x[1], reverse=True)
            countries_text = "  " + ", ".join(f"{c}: {n}" for c, n in sorted_c[:3])
        else:
            countries_text = "  —"

        rtl        = lk.get("rtl_count") or 0
        hieroglyph = lk.get("hieroglyph_count") or 0
        premium    = lk.get("premium_count") or 0
        rtl_pct   = f"{rtl / joined * 100:.2f}"  if joined > 0 else "0.00"
        hier_pct  = f"{hieroglyph / joined * 100:.2f}" if joined > 0 else "0.00"
        prem_pct  = f"{premium / joined * 100:.2f}" if joined > 0 else "0.00"

        cost_text = ""
        if lk.get("budget"):
            budget   = float(lk["budget"])
            cur      = lk.get("budget_currency") or "RUB"
            cur_sym  = {"RUB": "₽", "USD": "$", "EUR": "€"}.get(cur, cur)
            per_all  = budget / joined   if joined   > 0 else budget
            per_stay = budget / remained if remained > 0 else budget
            cost_text = (
                f"\nСтоимость подписчика:\n"
                f"🔴 Общая: {per_all:.2f}{cur_sym}\n"
                f"🟢 Итоговая: {per_stay:.2f}{cur_sym}\n"
            )

        type_map   = {"request": "заявки", "regular": "обычная", "onetime": "одноразовая"}
        auto_accept = lk.get("auto_accept") or "base"
        safe_name   = html.escape(str(lk.get("name") or ""))
        safe_link   = html.escape(str(lk.get("link") or ""))
        link_type   = type_map.get(lk.get("link_type", ""), lk.get("link_type", ""))
        created_at  = lk["created_at"].strftime('%d.%m.%Y') if lk.get("created_at") else "—"

        text = (
            f"📊 Статистика по {safe_name}\n\n"
            f"🔗 Ссылка: <code>{safe_link}</code>\n"
            f"🔒 Вид: {link_type}\n\n"
            f"👤 <u>Подписчики</u>\n"
            f"👤 Подписалось: {joined}\n"
            f"👤 Отписалось: {unsub}\n"
            f"👤 Осталось: {remained}\n\n"
            f"🎯 <u>Пол аудитории</u>\n"
            f"М: {m_pct}% | Ж: {f_pct}%\n\n"
            f"🌍 <u>Страны</u>\n"
            f"{countries_text}\n\n"
            f"📋 <u>Аккаунты</u>\n"
            f"🌙 RTL-символы в имени: {rtl} | {rtl_pct}%\n"
            f"Иероглифы в имени: {hieroglyph} | {hier_pct}%\n"
            f"⭐ Telegram Premium: {premium} | {prem_pct}%\n"
            f"{cost_text}"
            f"📅 Дата создания: {created_at}"
        )

        await callback.message.edit_text(
            text,
            parse_mode="HTML",
            reply_markup=kb_link_detail(link_id, chat_id, child_bot_id, auto_accept),
        )
        await callback.answer()

    except Exception as e:
        logger.error(f"on_link_detail error: {e}")
        await callback.answer(f"Ошибка: {e}", show_alert=True)


# ── Поделиться ссылкой ────────────────────────────────────────

@router.callback_query(F.data.startswith("link_share:"))
async def on_link_share(callback: CallbackQuery, platform_user: dict | None):
    if not platform_user:
        return
    link_id = int(callback.data.split(":")[1])
    link = await db.fetchrow(
        "SELECT link, chat_id FROM invite_links WHERE id=$1 AND owner_id=$2",
        link_id, platform_user["user_id"],
    )
    if not link:
        await callback.answer("Ссылка не найдена", show_alert=True)
        return

    import urllib.parse
    link_url = link["link"]
    share_url = f"https://t.me/share/url?url={urllib.parse.quote(link_url)}"

    await callback.message.edit_text(
        f"↗️ <b>Поделиться ссылкой</b>\n\n"
        f"Скопируйте ссылку:\n<code>{link_url}</code>",
        parse_mode="HTML",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="↗️ Поделиться в Telegram", url=share_url)],
            [InlineKeyboardButton(text="◀️ Назад", callback_data=f"link_detail:{link_id}:0:0")],
        ]),
    )
    await callback.answer()


# ── Автопринятие: переключение ────────────────────────────────

@router.callback_query(F.data.startswith("link_auto_accept:"))
async def on_link_auto_accept(callback: CallbackQuery, platform_user: dict | None):
    if not platform_user:
        return
    parts = callback.data.split(":")
    link_id = int(parts[1])
    chat_id = int(parts[2]) if len(parts) > 2 else 0
    child_bot_id = int(parts[3]) if len(parts) > 3 else 0

    link = await db.fetchrow(
        "SELECT auto_accept FROM invite_links WHERE id=$1 AND owner_id=$2",
        link_id, platform_user["user_id"],
    )
    if not link:
        await callback.answer("Ссылка не найдена", show_alert=True)
        return

    # Циклично переключаем: base → on → off → base
    cycle   = {"base": "on", "on": "off", "off": "base"}
    new_val = cycle.get(link["auto_accept"] or "base", "base")
    await db.execute(
        "UPDATE invite_links SET auto_accept=$1 WHERE id=$2",
        new_val, link_id,
    )
    # Мгновенно обновляем только клавиатуру — текст остаётся неизменным
    await callback.message.edit_reply_markup(
        reply_markup=kb_link_detail(link_id, chat_id, child_bot_id, new_val)
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
