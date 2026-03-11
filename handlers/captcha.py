"""
handlers/captcha.py — Капча «Я не робот».
Поддерживает два режима:
  - simple:  пользователь нажимает любую кнопку
  - random:  пользователь должен нажать правильный эмодзи (из db captcha_emoji_set)
"""
import asyncio
import logging
import random
from datetime import datetime
from aiogram import Router, F, Bot
from aiogram.types import (
    CallbackQuery, ChatJoinRequest,
    InlineKeyboardMarkup, InlineKeyboardButton,
)

import db.pool as db

logger = logging.getLogger(__name__)
router = Router()


def _fill_captcha_text(template: str, user, chat_title: str) -> str:
    """Подставляет переменные {name}, {allname}, {username}, {chat}, {day} в текст капчи."""
    full_name = ((
        (user.first_name or "") + " " + (user.last_name or "")
    ).strip() or user.first_name or "")
    return (
        template
        .replace("{name}",    user.first_name or "")
        .replace("{allname}", full_name)
        .replace("{username}", f"@{user.username}" if user.username else user.first_name or "")
        .replace("{chat}",  chat_title)
        .replace("{day}",   datetime.now().strftime("%d.%m.%Y"))
    )

# Хранилище pending-заявок: {(chat_id, user_id): ChatJoinRequest}
_pending: dict[tuple[int, int], ChatJoinRequest] = {}

# Хранилище правильных ответов для рандомной капчи: {(chat_id, user_id): correct_emoji}
_expected: dict[tuple[int, int], str] = {}

# Хранилище invite_link_url для трекинга (fallback когда Telegram не шлёт invite_link)
_pending_link_urls: dict[tuple[int, int], str] = {}

# ── Групповой режим (join via regular link) ────────────────────────────────
# {(chat_id, user_id): {captcha_type, one_time_link, welcome_text, owner_id, ...}}
_pending_group: dict[tuple[int, int], dict] = {}
# Пользователи, успешно прошедшие капчу; chat_member-хендлер пропускает их
_passed_captcha_group: set[tuple[int, int]] = set()
# Пользователи, которых мы кикнули для капчи; chat_member (left) -хендлер пропускает их
_kicked_for_captcha: set[tuple[int, int]] = set()


# ══════════════════════════════════════════════════════════════
# Публичная точка входа — вызывается из join_requests.py
# ══════════════════════════════════════════════════════════════

async def send_captcha(bot: Bot, event: ChatJoinRequest, settings_row: dict):
    """
    Отправляет капчу пользователю в личку.
    Тип определяется по settings_row['captcha_type']:
      'simple'  — кнопка «Я не робот»
      'random'  — 4 эмодзи, один правильный
    Если пользователь не открыл бота — авто-одобряем.
    """
    user = event.from_user
    key  = (event.chat.id, user.id)
    _pending[key] = event
    # Сохраняем invite_link_url из settings (fallback если Telegram не шлёт в событии)
    inv_url = settings_row.get("invite_link_url")
    if inv_url:
        _pending_link_urls[key] = inv_url
    elif key in _pending_link_urls:
        del _pending_link_urls[key]

    captcha_type = settings_row.get("captcha_type") or "simple"
    timer_min    = int(settings_row.get("captcha_timer_min") or 1)

    # ── Простая капча ──────────────────────────────────────────
    if captcha_type == "simple":
        raw_text = (
            settings_row.get("captcha_text")
            or f"👋 Привет, <b>{{name}}</b>!\n\n"
               f"Прежде чем войти в <b>{{chat}}</b>,\n"
               f"докажи что ты не робот — нажми кнопку ниже ✅\n\n"
               f"⏱ У тебя {timer_min} мин."
        )
        text = _fill_captcha_text(raw_text, user, event.chat.title)
        kb = InlineKeyboardMarkup(inline_keyboard=[[
            InlineKeyboardButton(
                text="✅ Я не робот",
                callback_data=f"captcha_ok:{event.chat.id}:{user.id}",
            )
        ]])

    # ── Рандомная капча ────────────────────────────────────────
    elif captcha_type == "random":
        emoji_set_raw = settings_row.get("captcha_emoji_set") or "🍕🍔🌭🌮"
        # Разбиваем строку на отдельные эмоджи (каждый эмодзи = 1-2 символа Unicode)
        emojis = _split_emojis(emoji_set_raw)
        if len(emojis) < 2:
            emojis = ["🍕", "🍔", "🌭", "🌮"]

        # Выбираем правильный и перемешиваем
        correct = emojis[0]  # по конвенции первый = правильный
        options = emojis[:4] if len(emojis) >= 4 else (emojis * 2)[:4]
        random.shuffle(options)
        _expected[key] = correct

        raw_text = (
            settings_row.get("captcha_text")
            or f"👋 Привет, <b>{{name}}</b>!\n\n"
               f"Чтобы войти в <b>{{chat}}</b>, нажми на: <b>{correct}</b>\n\n"
               f"⏱ У тебя {timer_min} мин."
        )
        text = _fill_captcha_text(raw_text, user, event.chat.title)
        kb = InlineKeyboardMarkup(inline_keyboard=[[
            InlineKeyboardButton(
                text=e,
                callback_data=f"captcha_rnd:{event.chat.id}:{user.id}:{e}",
            ) for e in options
        ]])

    else:
        # Неизвестный тип капчи — безопасный fallback: не отправляем, заявка в очередь
        logger.warning(f"[CAPTCHA] Unknown captcha_type={settings_row.get('captcha_type')!r} for chat {event.chat.id} — skipping")
        _pending.pop(key, None)
        return

    media_id = settings_row.get("captcha_media")
    try:
        if media_id:
            msg = await bot.send_photo(
                user.id, photo=media_id, caption=text, parse_mode="HTML", reply_markup=kb,
            )
        else:
            msg = await bot.send_message(
                user.id, text, parse_mode="HTML", reply_markup=kb,
            )
        asyncio.create_task(
            _captcha_timeout(bot, event, settings_row, msg.message_id)
        )
    except Exception:
        # Пользователь не открыл диалог с ботом → авто-одобряем
        _pending.pop(key, None)
        _expected.pop(key, None)
        await event.approve()
        from handlers.join_requests import _register_user, _send_welcome
        await _register_user(settings_row["owner_id"], event.chat.id, user)
        await _send_welcome(bot, event.chat.id, user, settings_row)



# ══════════════════════════════════════════════════════════════
# Групповой режим: отправка капчи без ChatJoinRequest
# ══════════════════════════════════════════════════════════════

async def send_captcha_group(
    bot: Bot, chat_id: int, chat_title: str, user,
    settings_row: dict, one_time_link: str,
):
    """
    Отправляет капчу пользователю в личку в режиме «открытая группа».
    Пользователь уже был кикнут; one_time_link — одноразовая ссылка Telegram.
    При успехе — отправляем one_time_link; при провале — пользователь просто не получает ссылку.
    """
    key = (chat_id, user.id)
    captcha_type = settings_row.get("captcha_type") or "simple"
    timer_min    = int(settings_row.get("captcha_timer_min") or 1)

    _pending_group[key] = {
        "captcha_type":  captcha_type,
        "one_time_link": one_time_link,
        "welcome_text":  settings_row.get("welcome_text"),
        "owner_id":      settings_row.get("owner_id"),
        "chat_title":    chat_title,
    }

    # ── Простая капча ──────────────────────────────────────────
    if captcha_type == "simple":
        raw_text = (
            settings_row.get("captcha_text")
            or f"👋 Привет, <b>{{name}}</b>!\n\n"
               f"Чтобы войти в <b>{{chat}}</b>,\n"
               f"докажи что ты не робот — нажми кнопку ниже ✅\n\n"
               f"⏱ У тебя {timer_min} мин."
        )
        text = _fill_captcha_text(raw_text, user, chat_title)
        kb = InlineKeyboardMarkup(inline_keyboard=[[
            InlineKeyboardButton(
                text="✅ Я не робот",
                callback_data=f"captcha_ok:{chat_id}:{user.id}",
            )
        ]])

    # ── Рандомная капча ──────────────────────────────────────────
    elif captcha_type == "random":
        emoji_set_raw = settings_row.get("captcha_emoji_set") or "🍕🍔🌭🌮"
        emojis = _split_emojis(emoji_set_raw)
        if len(emojis) < 2:
            emojis = ["🍕", "🍔", "🌭", "🌮"]
        correct = emojis[0]
        options = emojis[:4] if len(emojis) >= 4 else (emojis * 2)[:4]
        random.shuffle(options)
        _expected[key] = correct

        raw_text = (
            settings_row.get("captcha_text")
            or f"👋 Привет, <b>{{name}}</b>!\n\n"
               f"Чтобы войти в <b>{{chat}}</b>, нажми на: <b>{correct}</b>\n\n"
               f"⏱ У тебя {timer_min} мин."
        )
        text = _fill_captcha_text(raw_text, user, chat_title)
        kb = InlineKeyboardMarkup(inline_keyboard=[[
            InlineKeyboardButton(
                text=e,
                callback_data=f"captcha_rnd:{chat_id}:{user.id}:{e}",
            ) for e in options
        ]])
    else:
        logger.warning(f"[GROUP CAPTCHA] Unknown captcha_type={captcha_type!r} — skipping")
        _pending_group.pop(key, None)
        return

    media_id = settings_row.get("captcha_media")
    try:
        if media_id:
            msg = await bot.send_photo(user.id, photo=media_id, caption=text, parse_mode="HTML", reply_markup=kb)
        else:
            msg = await bot.send_message(user.id, text, parse_mode="HTML", reply_markup=kb)
        asyncio.create_task(
            _captcha_timeout_group(bot, chat_id, user.id, timer_min, msg.message_id)
        )
        logger.info(f"[GROUP CAPTCHA] Sent to user={user.id} chat={chat_id} type={captcha_type}")
        return True
    except Exception as e:
        # Пользователь не открыл бота — не можем отправить DM.
        # Убираем pending, вызывающий код должен разбанить пользователя.
        logger.warning(f"[GROUP CAPTCHA] Cannot send DM to user={user.id}: {e}")
        _pending_group.pop(key, None)
        _expected.pop(key, None)
        return False


async def _captcha_timeout_group(
    bot: Bot, chat_id: int, user_id: int, timer_min: int, msg_id: int,
):
    """Истекло время капчи в групповом режиме — удаляем pending, сообщаем пользователю."""
    await asyncio.sleep(timer_min * 60)
    key = (chat_id, user_id)
    if key in _pending_group:
        _pending_group.pop(key, None)
        _expected.pop(key, None)
        try:
            await bot.delete_message(user_id, msg_id)
        except Exception:
            pass
        try:
            await bot.send_message(
                user_id,
                "⏱ Время вышло. Ссылка аннулирована.\n"
                "Вы можете войти в группу снова и пройти проверку.",
            )
        except Exception:
            pass


# ══════════════════════════════════════════════════════════════
# Обработчики нажатия на капчу
# ══════════════════════════════════════════════════════════════

@router.callback_query(F.data.startswith("captcha_ok:"))
async def on_captcha_simple_passed(callback: CallbackQuery, bot: Bot):
    """Простая капча — любая кнопка засчитывается."""
    parts   = callback.data.split(":")
    chat_id = int(parts[1])
    user_id = int(parts[2])

    if callback.from_user.id != user_id:
        await callback.answer("❌ Эта капча не для вас", show_alert=True)
        return

    await _approve_user(callback, bot, chat_id, user_id, success=True)


@router.callback_query(F.data.startswith("captcha_rnd:"))
async def on_captcha_random_press(callback: CallbackQuery, bot: Bot):
    """Рандомная капча — проверяем правильный ли эмодзи."""
    parts   = callback.data.split(":")
    chat_id = int(parts[1])
    user_id = int(parts[2])
    pressed = parts[3]

    if callback.from_user.id != user_id:
        await callback.answer("❌ Эта капча не для вас", show_alert=True)
        return

    key     = (chat_id, user_id)
    correct = _expected.get(key)
    success = (pressed == correct)

    # Удаляем ожидание вне зависимости от результата
    _expected.pop(key, None)
    await _approve_user(callback, bot, chat_id, user_id, success=success)


# ══════════════════════════════════════════════════════════════
# Вспомогательные функции
# ══════════════════════════════════════════════════════════════

async def _approve_user(
    callback: CallbackQuery, bot: Bot,
    chat_id: int, user_id: int, success: bool,
):
    key   = (chat_id, user_id)
    event = _pending.pop(key, None)

    if not event:
        # ── Групповой режим (пользователь вступил через обычную ссылку) ──
        group_data = _pending_group.pop(key, None)
        _expected.pop(key, None)
        if group_data:
            owner_id = group_data.get("owner_id")
            if success:
                _passed_captcha_group.add(key)
                # Записываем событие капчи (успех, групповой режим)
                if owner_id:
                    try:
                        await db.execute(
                            "INSERT INTO captcha_events (owner_id, chat_id, user_id, passed) VALUES ($1,$2,$3,true)",
                            owner_id, chat_id, user_id,
                        )
                    except Exception as ex:
                        logger.debug(f"captcha_events insert failed (group success): {ex}")
                # Приветствие НЕ отправляем здесь — пользователь ещё не вступил.
                # Оно будет отправлено в _handle_chat_member (child_bot_runner.py)
                # сразу после того как пользователь кликнет one_time_link и реально войдёт.
                # _passed_captcha_group — флаг, по которому _handle_chat_member это определит.
                # Одноразовая ссылка для вступления
                one_time_link = group_data.get("one_time_link")
                if one_time_link:
                    await callback.message.edit_text(
                        "✅ Капча пройдена! Нажмите кнопку ниже, чтобы войти:"
                    )
                    await bot.send_message(
                        user_id,
                        f'🔗 <a href="{one_time_link}">Войти в группу</a>\n\nСсылка одноразовая.',
                        parse_mode="HTML",
                    )
                else:
                    await callback.message.edit_text("✅ Капча пройдена! Добро пожаловать.")
                await callback.answer("✅ Отлично!")
                logger.info(f"[GROUP CAPTCHA] Passed: user={user_id} chat={chat_id} — welcome deferred to on-join event")
            else:
                # Записываем событие капчи (провал, групповой режим)
                if owner_id:
                    try:
                        await db.execute(
                            "INSERT INTO captcha_events (owner_id, chat_id, user_id, passed) VALUES ($1,$2,$3,false)",
                            owner_id, chat_id, user_id,
                        )
                    except Exception as ex:
                        logger.debug(f"captcha_events insert failed (group fail): {ex}")
                await callback.message.edit_text(
                    "❌ Неверный ответ.\n"
                    "Для вступления запросите доступ снова."
                )
                await callback.answer("❌ Неверно!", show_alert=True)
        else:
            await callback.answer("Капча уже обработана или истекла", show_alert=True)
        return

    if success:
        # ← Регистрируем ДО approve, чтобы chat_member-хендлер не кикнул пользователя повторно.
        # _handle_chat_member видит captcha_type != "off" и при отсутствии ключа кикает снова.
        _passed_captcha_group.add(key)
        try:
            await event.approve()
        except Exception as e:
            _passed_captcha_group.discard(key)  # откатываем при неудаче
            logger.warning(f"Approve failed: {e}")
            await callback.answer("❌ Не удалось одобрить заявку", show_alert=True)
            return

        settings_row = await db.fetchrow(
            "SELECT * FROM bot_chats WHERE chat_id=$1::bigint AND is_active=true", chat_id
        )
        if settings_row:
            from handlers.join_requests import _register_user, _send_welcome
            await _register_user(settings_row["owner_id"], chat_id, callback.from_user)

            # Записываем событие капчи (успех)
            try:
                await db.execute(
                    "INSERT INTO captcha_events (owner_id, chat_id, user_id, passed) VALUES ($1,$2,$3,true)",
                    settings_row["owner_id"], chat_id, callback.from_user.id,
                )
            except Exception as ex:
                logger.debug(f"captcha_events insert failed: {ex}")

            # Трекинг статистики ссылки-приглашения (event — ChatJoinRequest с invite_link)
            inv_url = event.invite_link.invite_link if getattr(event, "invite_link", None) and event.invite_link else None
            # Fallback: берём из сохранённого при получении join_request
            if not inv_url:
                inv_url = _pending_link_urls.pop(key, None)
            logger.info(f"[CAPTCHA APPROVED] user={callback.from_user.id} invite_link={inv_url}")
            if inv_url:
                try:
                    from scheduler.child_bot_runner import _track_invite_link
                    tracked = await _track_invite_link(inv_url, callback.from_user)
                    logger.info(f"[CAPTCHA TRACK] link_id={tracked}")
                except Exception as e:
                    logger.warning(f"[LINK TRACK] failed: {e}")

            # Приветствие отправляем сразу после event.approve(), пока DM-права
            # от ChatJoinRequest ещё активны — позже они истекают.
            welcome = settings_row.get("welcome_text")
            if welcome:
                from scheduler.child_bot_runner import _try_send_dm
                await _try_send_dm(
                    bot, callback.from_user.id, welcome,
                    show_typing=bool(settings_row.get("typing_action")),
                )

            if settings_row.get("captcha_delete"):
                try:
                    await callback.message.delete()
                except Exception:
                    pass
                # Всегда отвечаем на callback — иначе Telegram показывает «часики» на кнопке
                await callback.answer("✅ Отлично!")
                return

        await callback.message.edit_text("✅ Капча пройдена! Добро пожаловать.")
        await callback.answer("✅ Отлично!")

    else:
        # Неверный ответ — записываем событие (провал)
        try:
            await event.decline()
        except Exception:
            pass
        # Записываем событие капчи (провал)
        settings_row = await db.fetchrow(
            "SELECT owner_id FROM bot_chats WHERE chat_id=$1::bigint AND is_active=true", chat_id
        )
        if settings_row:
            try:
                await db.execute(
                    "INSERT INTO captcha_events (owner_id, chat_id, user_id, passed) VALUES ($1,$2,$3,false)",
                    settings_row["owner_id"], chat_id, callback.from_user.id,
                )
            except Exception as ex:
                logger.debug(f"captcha_events insert failed: {ex}")
        await callback.message.edit_text(
            "❌ Неверный ответ. Заявка отклонена.\n"
            "Вы можете подать заявку повторно."
        )
        await callback.answer("❌ Неверно!", show_alert=True)



async def _captcha_timeout(
    bot: Bot, event: ChatJoinRequest, settings_row: dict, msg_id: int,
):
    """Отклоняет заявку если капча не пройдена за timer_min минут."""
    timer = int(settings_row.get("captcha_timer_min") or 1)
    await asyncio.sleep(timer * 60)

    key = (event.chat.id, event.from_user.id)
    if key in _pending:
        _pending.pop(key)
        _expected.pop(key, None)
        try:
            await event.decline()
        except Exception:
            pass
        try:
            await bot.delete_message(event.from_user.id, msg_id)
        except Exception:
            pass
        try:
            await bot.send_message(
                event.from_user.id,
                "⏱ Время вышло. Заявка отклонена.\n"
                "Вы можете подать заявку повторно.",
            )
        except Exception:
            pass


# ── Старый колбэк "captcha:" — оставляем для совместимости ────

@router.callback_query(F.data.startswith("captcha:"))
async def on_captcha_passed(callback: CallbackQuery, bot: Bot):
    """Legacy: старый формат captcha:{chat_id}:{user_id}."""
    parts   = callback.data.split(":")
    chat_id = int(parts[1])
    user_id = int(parts[2])
    if callback.from_user.id != user_id:
        await callback.answer("❌ Эта капча не для вас", show_alert=True)
        return
    await _approve_user(callback, bot, chat_id, user_id, success=True)


def _split_emojis(s: str) -> list[str]:
    """Разбивает строку эмодзи на список отдельных эмодзи."""
    import unicodedata
    result = []
    i = 0
    while i < len(s):
        # Пропускаем пробелы
        if s[i] == ' ':
            i += 1
            continue
        # Берём символ + возможный variation selector или ZWJ
        char = s[i]
        i += 1
        while i < len(s) and (
            unicodedata.category(s[i]) in ('Mn', 'Cf') or
            ord(s[i]) in (0xFE0F, 0x200D, 0x20E3)
        ):
            char += s[i]
            i += 1
        if char.strip():
            result.append(char)
    return result
