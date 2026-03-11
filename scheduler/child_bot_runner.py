"""
scheduler/child_bot_runner.py — Запускает polling для каждого дочернего бота.

При старте приложения и при добавлении нового токена — стартует отдельную
asyncio задачу с long-polling для дочернего бота. Обрабатывает:
  - my_chat_member: бот добавлен в канал/группу → уведомляет владельца и сохраняет в БД
  - chat_join_request: новая заявка → принимает/откланяет + приветствует
"""
import asyncio
import logging
from typing import Dict

from aiogram import Bot
from aiogram.types import Update, ChatMemberUpdated, ChatJoinRequest
from aiogram.exceptions import TelegramUnauthorizedError

import db.pool as db
from services.security import decrypt_token

logger = logging.getLogger(__name__)

# owner_bot: задача polling
_running_bots: Dict[int, asyncio.Task] = {}  # child_bot_id → Task
_main_bot: Bot = None   # Bumblebee management bot для уведомлений

# Состояния ожидания ответа администратора: (child_bot_id, admin_id) → dict
# Храним в памяти: при редеплое теряется, но это допустимо — admin просто нажмёт повторно.
_reply_states: Dict[tuple, dict] = {}


async def _try_send_dm(child_bot: Bot, user_id: int, text: str,
                       parse_mode: str = "HTML", show_typing: bool = False) -> bool:
    """Отправляет сообщение пользователю в личку через дочернего бота.
    При неудаче — пробует через главного бота (_main_bot).
    Возвращает True если отправлено.
    """
    try:
        if show_typing:
            try:
                await child_bot.send_chat_action(user_id, "typing")
                await asyncio.sleep(1.5)
            except Exception:
                pass
        await child_bot.send_message(user_id, text, parse_mode=parse_mode)
        logger.info(f"[DM] Sent via child_bot to user {user_id} ✅")
        return True
    except Exception as e:
        logger.warning(f"[DM] child_bot failed for user {user_id}: {e}")

    # Fallback: пробуем через главного бота
    if _main_bot:
        try:
            await _main_bot.send_message(user_id, text, parse_mode=parse_mode)
            logger.info(f"[DM] Sent via main_bot (fallback) to user {user_id} ✅")
            return True
        except Exception as e2:
            logger.warning(f"[DM] main_bot fallback also failed for user {user_id}: {e2}")

    logger.warning(f"[DM] Не удалось отправить DM user {user_id} — пользователь не запустил ни одного бота")
    return False



async def _track_invite_link(invite_link_url: str, user) -> int | None:
    """Обновляет статистику ссылки-приглашения. Возвращает link_id или None."""
    if not invite_link_url:
        return None
    row = await db.fetchrow("SELECT id FROM invite_links WHERE link=$1", invite_link_url)
    if not row:
        return None
    link_id = row["id"]

    full_name = f"{user.first_name or ''} {user.last_name or ''}".strip()
    from services.security import detect_rtl, detect_hieroglyph
    has_rtl        = detect_rtl(full_name)
    has_hieroglyph = detect_hieroglyph(full_name)
    is_premium     = bool(getattr(user, "is_premium", False))
    LANG_TO_COUNTRY = {
        "ru": "RU", "uk": "UA", "be": "BY", "kk": "KZ",
        "en": "US", "de": "DE", "fr": "FR", "es": "ES",
        "it": "IT", "pt": "BR", "zh": "CN", "ar": "AR",
        "tr": "TR", "pl": "PL", "nl": "NL", "sv": "SE",
        "da": "DK", "fi": "FI", "no": "NO", "cs": "CZ",
        "ro": "RO", "hu": "HU", "bg": "BG",
        "fa": "IR", "he": "IL", "hi": "IN",
        "id": "ID", "ms": "MY", "th": "TH", "vi": "VN",
        "ko": "KR", "ja": "JP",
    }
    country_code = LANG_TO_COUNTRY.get(
        (getattr(user, "language_code", None) or "").split("-")[0].lower()
    )

    if country_code:
        await db.execute(
            """
            UPDATE invite_links SET
                joined           = joined + 1,
                rtl_count        = rtl_count + $2::int,
                hieroglyph_count = hieroglyph_count + $3::int,
                premium_count    = premium_count + $4::int,
                countries        = jsonb_set(
                    COALESCE(countries, '{}'),
                    ARRAY[$5],
                    (COALESCE(countries->$5, '0')::int + 1)::text::jsonb
                )
            WHERE id = $1
            """,
            link_id, int(has_rtl), int(has_hieroglyph), int(is_premium), country_code,
        )
    else:
        await db.execute(
            """
            UPDATE invite_links SET
                joined           = joined + 1,
                rtl_count        = rtl_count + $2::int,
                hieroglyph_count = hieroglyph_count + $3::int,
                premium_count    = premium_count + $4::int
            WHERE id = $1
            """,
            link_id, int(has_rtl), int(has_hieroglyph), int(is_premium),
        )
    logger.info(f"[LINK] Tracked join via link_id={link_id} for user {user.id}")
    return link_id


def init_runner(main_bot: Bot):
    global _main_bot
    _main_bot = main_bot


async def start_all_child_bots():
    """Вызывается при старте приложения — запускает polling для всех токенов из БД."""
    rows = await db.fetch(
        "SELECT id, owner_id, bot_username, token_encrypted FROM child_bots"
    )
    for row in rows:
        await start_child_bot(row["id"], row["owner_id"], row["bot_username"], row["token_encrypted"])
    logger.info(f"Started {len(rows)} child bot(s)")


async def start_child_bot(child_bot_id: int, owner_id: int, bot_username: str, token_encrypted: str):
    """Запускает polling для одного дочернего бота (если ещё не запущен)."""
    if child_bot_id in _running_bots and not _running_bots[child_bot_id].done():
        return  # Уже запущен

    raw_token = decrypt_token(token_encrypted)
    task = asyncio.create_task(
        _poll_child_bot(child_bot_id, owner_id, bot_username, raw_token),
        name=f"child_bot_{child_bot_id}",
    )
    _running_bots[child_bot_id] = task
    logger.info(f"Child bot @{bot_username} (id={child_bot_id}) polling started")


def stop_child_bot(child_bot_id: int):
    task = _running_bots.pop(child_bot_id, None)
    if task:
        task.cancel()


async def _poll_child_bot(child_bot_id: int, owner_id: int, bot_username: str, raw_token: str):
    """Long-polling цикл для дочернего бота."""
    bot = Bot(token=raw_token)
    offset = 0
    retry_delay = 5

    try:
        # Сбрасываем webhook — отдельный try чтобы невалидный токен не крашил таску
        try:
            await bot.delete_webhook(drop_pending_updates=True)
        except TelegramUnauthorizedError:
            logger.error(f"Child bot @{bot_username} (id={child_bot_id}): token revoked — deactivating")
            await db.execute(
                "UPDATE bot_chats SET is_active=false WHERE child_bot_id=$1", child_bot_id
            )
            return
        except Exception as e:
            logger.warning(f"Child bot @{bot_username} delete_webhook error: {e} — continuing anyway")

        # Ждём немного чтобы старый инстанс (при редеплое) успел умереть,
        # затем делаем быстрый вызов чтобы "захватить" сессию у старого polling-а
        await asyncio.sleep(3)
        try:
            await bot.get_updates(offset=0, timeout=0, allowed_updates=[])
        except Exception:
            pass
        retry_delay = 5  # сброс задержки при успешном подключении

        while True:
            try:
                updates = await bot.get_updates(
                    offset=offset,
                    timeout=30,
                    allowed_updates=["my_chat_member", "chat_join_request", "chat_member", "message", "callback_query"],
                )
                for update in updates:
                    offset = update.update_id + 1
                    await _handle_child_update(bot, child_bot_id, owner_id, bot_username, update)

            except TelegramUnauthorizedError:
                logger.error(f"Child bot @{bot_username}: token revoked — stopping")
                await db.execute(
                    "UPDATE bot_chats SET is_active=false WHERE child_bot_id=$1", child_bot_id
                )
                break
            except asyncio.CancelledError:
                raise
            except Exception as e:
                logger.warning(f"Child bot @{bot_username} poll error: {e}. Retry in {retry_delay}s")
                await asyncio.sleep(retry_delay)
                retry_delay = min(retry_delay * 2, 60)

    except asyncio.CancelledError:
        logger.info(f"Child bot @{bot_username} polling cancelled")
    finally:
        await bot.session.close()


async def _handle_child_update(
    bot: Bot, child_bot_id: int, owner_id: int, bot_username: str, update: Update
):
    """Обрабатывает одно событие от дочернего бота."""
    try:
        # ── Бот добавлен/удалён из чата ──────────────────────
        if update.my_chat_member:
            await _handle_my_chat_member(bot, child_bot_id, owner_id, bot_username, update.my_chat_member)

        # ── Заявка на вступление (закрытый канал) ─────────────
        elif update.chat_join_request:
            await _handle_join_request(bot, child_bot_id, update.chat_join_request)

        # ── Колбэк кнопки «Ответить» (обратная связь) ──────────
        elif update.callback_query and (update.callback_query.data or "").startswith("fb_reply:"):
            await _handle_fb_reply_callback(bot, child_bot_id, update.callback_query)

        # ── Колбэк от кнопки капчи ────────────────────────────
        elif update.callback_query:
            await _handle_captcha_callback(bot, update.callback_query)

        # ── Пользователь вступил/вышел (открытый канал/группа) ─
        elif update.chat_member:
            await _handle_chat_member(bot, child_bot_id, update.chat_member)

        # ── Сообщение пользователя ──────────────────────────────────
        elif update.message and update.message.from_user:
            chat_type = update.message.chat.type if update.message.chat else "private"
            if chat_type in ("group", "supergroup", "channel"):
                # Групповое сообщение — автоответчик + реакции
                await _handle_group_message(bot, child_bot_id, update.message)
            else:
                # Личное сообщение (/start и др.)
                await _handle_message(bot, child_bot_id, owner_id, update.message)

    except Exception as e:
        logger.error(f"Child bot @{bot_username} update error: {e}")


async def _handle_fb_reply_callback(bot: Bot, child_bot_id: int, callback):
    """
    Администратор нажал «Ответить» прямо в дочернем боте.
    Сохраняем состояние в _reply_states и просим написать ответ.
    """
    try:
        parts = (callback.data or "").split(":")
        # формат: fb_reply:{child_bot_id}:{user_id}:{owner_id}
        if len(parts) < 4:
            await callback.answer("⚠️ Устаревшая кнопка.", show_alert=True)
            return

        target_user_id = int(parts[2])
        owner_id       = int(parts[3])
        admin_id       = callback.from_user.id

        # Проверяем права: владелец или активный член команды
        if admin_id != owner_id:
            is_allowed = await db.fetchval(
                "SELECT 1 FROM team_members WHERE user_id=$1 AND owner_id=$2 AND is_active=true",
                admin_id, owner_id,
            )
            if not is_allowed:
                await callback.answer("❌ Нет доступа к этому действию.", show_alert=True)
                return

        # Извлекаем имя из текста уведомления (строка «От: Имя (@username)»)
        target_name, target_username = "пользователю", ""
        if callback.message and callback.message.text:
            for line in callback.message.text.splitlines():
                if line.startswith("От:"):
                    raw = line.replace("От:", "").strip()
                    if " (" in raw:
                        target_name     = raw.split(" (")[0].strip()
                        target_username = raw.split(" (")[1].rstrip(")")
                    else:
                        target_name = raw
                    break

        # Сохраняем исходный текст для восстановления при /cancel
        original_text = (callback.message.text or "") if callback.message else ""
        name_display = f"{target_name} ({target_username})" if target_username else target_name

        _reply_states[(child_bot_id, admin_id)] = {
            "target_user_id":      target_user_id,
            "target_name":         target_name,
            "target_username":     target_username,
            "notification_msg_id": callback.message.message_id if callback.message else None,
            "original_text":       original_text,
            "owner_id":            owner_id,
        }

        # Редактируем сообщение 2 в режим ввода (prompt, скриншот 3)
        try:
            await callback.message.edit_text(
                f"✉️ <b>Напишите ответ для {name_display}:</b>\n\n"
                f"Следующее сообщение, которое вы напишете в этот бот, будет отправлено пользователю.",
                parse_mode="HTML",
            )
        except Exception as edit_err:
            logger.debug(f"[FB_REPLY CB] Could not edit notification: {edit_err}")
        await callback.answer(f"✉️ Напишите ответ для {target_name} 👇")
        logger.info(f"[FB_REPLY CB] admin={admin_id} ready to reply to user={target_user_id} via bot={child_bot_id}")

    except Exception as e:
        logger.error(f"[FB_REPLY CB] Error: {e}", exc_info=True)
        try:
            await callback.answer(f"⚠️ Ошибка: {e}", show_alert=True)
        except Exception:
            pass


async def _handle_captcha_callback(bot: Bot, callback):
    """Обрабатывает нажатие кнопки капчи в дочернем боте."""
    data = callback.data or ""
    if (data.startswith("captcha_ok:") or data.startswith("captcha:") or data.startswith("captcha_rnd:")
            or data.startswith("fbr_more:") or data.startswith("fbr_cancel:")
            or data.startswith("fb_block:") or data.startswith("fb_unblock:")):
        from handlers.captcha import on_captcha_simple_passed, on_captcha_random_press, on_captcha_passed
        try:
            if data.startswith("captcha_ok:"):
                await on_captcha_simple_passed(callback, bot)
            elif data.startswith("captcha_rnd:"):
                await on_captcha_random_press(callback, bot)
            elif data.startswith("captcha:"):
                await on_captcha_passed(callback, bot)
            elif data.startswith("fb_block:") or data.startswith("fb_unblock:"):
                # Выполняем логику прямо здесь — callback приходит в дочернего бота
                parts          = data.split(":")
                fb_child_bot_id   = int(parts[1])
                fb_target_user_id = int(parts[2])
                fb_owner_id       = int(parts[3])
                clicker_id        = callback.from_user.id
                blocking          = data.startswith("fb_block:")

                # Проверяем права (владелец или команда)
                if clicker_id != fb_owner_id:
                    is_allowed = await db.fetchval(
                        "SELECT 1 FROM team_members WHERE user_id=$1 AND owner_id=$2 AND is_active=true",
                        clicker_id, fb_owner_id,
                    )
                    if not is_allowed:
                        await callback.answer("❌ Нет доступа.", show_alert=True)
                        return

                # Обновляем БД
                await db.execute(
                    "UPDATE bot_users SET feedback_blocked=$1 WHERE user_id=$2 AND owner_id=$3",
                    blocking, fb_target_user_id, fb_owner_id,
                )

                # Редактируем клавиатуру in-place
                from aiogram.types import InlineKeyboardMarkup, InlineKeyboardButton
                if blocking:
                    new_block_btn = InlineKeyboardButton(
                        text="🟢 Разблокировать",
                        callback_data=f"fb_unblock:{fb_child_bot_id}:{fb_target_user_id}:{fb_owner_id}",
                    )
                    toast = "🔴 Пользователь заблокирован"
                    logger.info(f"[FB_BLOCK] user={fb_target_user_id} blocked by owner={fb_owner_id}")
                else:
                    new_block_btn = InlineKeyboardButton(
                        text="🔴 Заблокировать",
                        callback_data=f"fb_block:{fb_child_bot_id}:{fb_target_user_id}:{fb_owner_id}",
                    )
                    toast = "🟢 Пользователь разблокирован"
                    logger.info(f"[FB_UNBLOCK] user={fb_target_user_id} unblocked by owner={fb_owner_id}")

                new_kb = InlineKeyboardMarkup(inline_keyboard=[
                    [InlineKeyboardButton(
                        text="💬 Ответить",
                        callback_data=f"fb_reply:{fb_child_bot_id}:{fb_target_user_id}:{fb_owner_id}",
                    )],
                    [new_block_btn],
                ])
                try:
                    await callback.message.edit_reply_markup(reply_markup=new_kb)
                except Exception:
                    pass
                await callback.answer(toast)

            elif data.startswith("fbr_more:"):
                # Редактируем то же сообщение в «режим ввода»
                parts             = data.split(":")
                fb_child_bot_id   = int(parts[1])
                fb_target_user_id = int(parts[2])
                fb_owner_id       = int(parts[3])
                admin_id          = callback.from_user.id
                work_msg_id       = callback.message.message_id
                urow = await db.fetchrow(
                    "SELECT first_name, username FROM bot_users WHERE user_id=$1 LIMIT 1",
                    fb_target_user_id,
                )
                if urow:
                    tname  = urow["first_name"] or "Пользователь"
                    tuname = f"@{urow['username']}" if urow["username"] else ""
                else:
                    tname, tuname = "Пользователь", ""
                ndisplay = f"{tname} ({tuname})" if tuname else tname
                _reply_states[(fb_child_bot_id, admin_id)] = {
                    "target_user_id":      fb_target_user_id,
                    "target_name":         tname,
                    "target_username":     tuname,
                    "notification_msg_id": None,
                    "work_msg_id":         work_msg_id,
                    "owner_id":            fb_owner_id,
                }
                from aiogram.types import InlineKeyboardMarkup, InlineKeyboardButton
                cancel_kb = InlineKeyboardMarkup(inline_keyboard=[[
                    InlineKeyboardButton(
                        text="❌ Отмена",
                        callback_data=f"fbr_cancel:{fb_child_bot_id}:{fb_target_user_id}:{fb_owner_id}",
                    )
                ]])
                try:
                    await callback.message.delete()
                except Exception:
                    pass
                new_prompt = await callback.message.answer(
                    f"✉️ <b>Напишите ответ для {ndisplay}:</b>\n\n"
                    "Следующее сообщение, которое вы напишете в этот бот, будет отправлено пользователю.\n"
                    "Для отмены — нажмите кнопку ниже или /cancel",
                    parse_mode="HTML",
                    reply_markup=cancel_kb,
                )
                _reply_states[(fb_child_bot_id, admin_id)]["work_msg_id"] = new_prompt.message_id
                await callback.answer("✍️ Напишите следующее 👇")
            elif data.startswith("fbr_cancel:"):
                # Восстанавливаем сообщение 2 (скриншот 2) — «У вас новое сообщение» с кнопками
                parts             = data.split(":")
                fb_child_bot_id   = int(parts[1])
                fb_target_user_id = int(parts[2])
                fb_owner_id       = int(parts[3])
                admin_id          = callback.from_user.id
                _reply_states.pop((fb_child_bot_id, admin_id), None)
                urow = await db.fetchrow(
                    "SELECT first_name, username FROM bot_users WHERE user_id=$1 LIMIT 1",
                    fb_target_user_id,
                )
                if urow:
                    tname  = urow["first_name"] or "Пользователь"
                    tuname = f"@{urow['username']}" if urow["username"] else ""
                else:
                    tname, tuname = "Пользователь", ""
                ndisplay = f"{tname} ({tuname})" if tuname else tname
                from aiogram.types import InlineKeyboardMarkup, InlineKeyboardButton
                restore_kb = InlineKeyboardMarkup(inline_keyboard=[
                    [InlineKeyboardButton(
                        text="💬 Ответить",
                        callback_data=f"fb_reply:{fb_child_bot_id}:{fb_target_user_id}:{fb_owner_id}",
                    )],
                    [InlineKeyboardButton(
                        text="🔴 Заблокировать",
                        callback_data=f"fb_block:{fb_child_bot_id}:{fb_target_user_id}:{fb_owner_id}",
                    )],
                ])
                try:
                    await callback.message.delete()
                except Exception:
                    pass
                await callback.message.answer(
                    f"ℹ️ <b>У вас новое сообщение от {ndisplay}!</b>\n"
                    f"💬 Свайпните сообщение для ответа.",
                    parse_mode="HTML",
                    reply_markup=restore_kb,
                )
                await callback.answer("❌ Отменено")
        except Exception as e:
            logger.error(f"Captcha callback error: {e}")
            try:
                await callback.answer("Ошибка, попробуйте ещё раз", show_alert=True)
            except Exception:
                pass
    else:
        try:
            await callback.answer()
        except Exception:
            pass


async def _handle_group_message(bot: Bot, child_bot_id: int, message):
    """Автоответчик и реакции для дочернего бота в группе."""
    from aiogram.types import ReactionTypeEmoji

    chat_id = message.chat.id
    user = message.from_user
    if not user or user.is_bot:
        return

    settings = await db.fetchrow(
        "SELECT * FROM bot_chats WHERE child_bot_id=$1 AND chat_id=$2 AND is_active=true",
        child_bot_id, chat_id,
    )
    if not settings:
        return

    text = message.text or message.caption or ""

    # ── 1. Автоответчик (ключевые слова) ────────────────────
    if text:
        owner_id = settings["owner_id"]
        rules = await db.fetch(
            "SELECT keyword, reply_text FROM autoreplies "
            "WHERE owner_id=$1 AND chat_id=$2::bigint",
            owner_id, chat_id,
        )
        for rule in rules:
            kw = (rule["keyword"] or "").lower().strip()
            if kw and kw in text.lower():
                try:
                    reply = await message.reply(rule["reply_text"])
                    # Авто-удаление ответа бота
                    delete_min = int(settings.get("auto_delete_min") or 0)
                    if delete_min > 0:
                        import asyncio as _asyncio
                        _asyncio.create_task(
                            _delete_later(bot, chat_id, reply.message_id, delete_min)
                        )
                    logger.info(f"[AUTOREPLY] keyword='{kw}' matched in chat {chat_id}")
                except Exception as e:
                    logger.warning(f"[AUTOREPLY] failed in chat {chat_id}: {e}")
                break  # только первое совпадение

    # ── 2. Реакции (только на сообщения с текстом, не системные) ───
    emoji = settings.get("reaction_emoji")
    if emoji and text:  # text='' для системных сообщений о вступлении/выходе
        try:
            await bot.set_message_reaction(
                chat_id=chat_id,
                message_id=message.message_id,
                reaction=[ReactionTypeEmoji(emoji=emoji)],
            )
        except Exception as e:
            logger.debug(f"[REACTION] failed for chat {chat_id}: {e}")


async def _delete_later(bot: Bot, chat_id: int, message_id: int, delay_min: int):
    """\u0423\u0434\u0430\u043b\u044f\u0435\u0442 \u0441\u043e\u043e\u0431\u0449\u0435\u043d\u0438\u0435 \u0447\u0435\u0440\u0435\u0437 delay_min \u043c\u0438\u043d\u0443\u0442."""
    await asyncio.sleep(delay_min * 60)
    try:
        await bot.delete_message(chat_id, message_id)
    except Exception:
        pass


async def _handle_message(bot: Bot, child_bot_id: int, owner_id: int, message):
    """
    Обрабатывает сообщения пользователя в личку дочернего бота.
    Порядок обработки:
      1) Если отправитель — администратор в состоянии ожидания ответа → отправляем ответ пользователю
      2) /start → устанавливает bot_activated=true
      3) Всё остальное → обратная связь (пересылаем владельцу через дочернего бота)
    """
    user = message.from_user
    if not user or user.is_bot:
        return

    text = message.text or ""


    # ── 1. Перехват: администратор в состоянии ожидания ответа ──
    state_key = (child_bot_id, user.id)
    if state_key in _reply_states:
        state = _reply_states.pop(state_key)  # изъять и очистить состояние

        # /cancel — отмена ответа
        if text.strip() == "/cancel":
            original_text_c = state.get("original_text", "")
            owner_id_c      = state.get("owner_id", 0)
            tv_user_id      = state["target_user_id"]
            tname_c         = state.get("target_name", "Пользователь")
            tuname_c        = state.get("target_username", "")
            notification_c  = state.get("notification_msg_id")
            work_msg_id_c   = state.get("work_msg_id")
            target_msg_c    = notification_c or work_msg_id_c
            from aiogram.types import InlineKeyboardMarkup, InlineKeyboardButton
            if target_msg_c:
                ndisplay_c = f"{tname_c} ({tuname_c})" if tuname_c else tname_c
                restore_kb = InlineKeyboardMarkup(inline_keyboard=[
                    [InlineKeyboardButton(
                        text="💬 Ответить",
                        callback_data=f"fb_reply:{child_bot_id}:{tv_user_id}:{owner_id_c}",
                    )],
                    [InlineKeyboardButton(
                        text="🔴 Заблокировать",
                        callback_data=f"fb_block:{child_bot_id}:{tv_user_id}:{owner_id_c}",
                    )],
                ])
                try:
                    await bot.delete_message(chat_id=user.id, message_id=target_msg_c)
                except Exception:
                    pass
                await bot.send_message(
                    user.id,
                    f"ℹ️ <b>У вас новое сообщение от {ndisplay_c}!</b>\n"
                    f"💬 Свайпните сообщение для ответа.",
                    parse_mode="HTML",
                    reply_markup=restore_kb,
                )
            else:
                await bot.send_message(user.id, "❌ Ответ отменён.")
            return

        target_user_id  = state["target_user_id"]
        target_name     = state.get("target_name", "пользователю")
        target_username = state.get("target_username", "")
        notification_id = state.get("notification_msg_id")
        name_display    = f"{target_name} ({target_username})" if target_username else target_name
        admin_user_id   = user.id

        work_msg_id   = state.get("work_msg_id")
        owner_id_4btn = state.get("owner_id", 0)

        try:
            # Отправляем ответ пользователю через тот же дочерний бот с заголовком
            header = "💬 <b>Ответ от поддержки</b>\n\n"
            reply_hint = "\n\n──────────────────\n💌 <i>Отправьте своё сообщение</i> 👇"
            sent_msg = None
            if message.text:
                sent_msg = await bot.send_message(
                    target_user_id,
                    header + message.text + reply_hint,
                    parse_mode="HTML",
                )
            elif message.photo:
                caption_text = (header + (message.caption or "")) + reply_hint
                sent_msg = await bot.send_photo(
                    target_user_id, message.photo[-1].file_id,
                    caption=caption_text,
                    parse_mode="HTML",
                )
            elif message.video:
                caption_text = (header + (message.caption or "")) + reply_hint
                sent_msg = await bot.send_video(
                    target_user_id, message.video.file_id,
                    caption=caption_text,
                    parse_mode="HTML",
                )
            elif message.document:
                caption_text = (header + (message.caption or "")) + reply_hint
                sent_msg = await bot.send_document(
                    target_user_id, message.document.file_id,
                    caption=caption_text,
                    parse_mode="HTML",
                )
            elif message.voice:
                sent_msg = await bot.send_voice(target_user_id, message.voice.file_id)
            elif message.audio:
                caption_text = (header + (message.caption or "")) + reply_hint
                sent_msg = await bot.send_audio(
                    target_user_id, message.audio.file_id,
                    caption=caption_text,
                    parse_mode="HTML",
                )
            elif message.sticker:
                await bot.send_message(
                    target_user_id,
                    header.strip() + reply_hint,
                    parse_mode="HTML",
                )
                sent_msg = await bot.send_sticker(target_user_id, message.sticker.file_id)
            elif message.video_note:
                sent_msg = await bot.send_video_note(target_user_id, message.video_note.file_id)
            else:
                await bot.send_message(user.id, "⚠️ Такой тип сообщения не поддерживается.")
                return

            # Удаляем prompt-сообщение и отправляем новое сообщение об успехе внизу (с кнопкой «Написать ещё»)
            from aiogram.types import InlineKeyboardMarkup, InlineKeyboardButton
            target_msg_id = notification_id or work_msg_id
            more_kb = InlineKeyboardMarkup(inline_keyboard=[[
                InlineKeyboardButton(
                    text="💬 Написать ещё",
                    callback_data=f"fbr_more:{child_bot_id}:{target_user_id}:{owner_id_4btn}",
                )
            ]])
            if target_msg_id:
                try:
                    await bot.delete_message(chat_id=user.id, message_id=target_msg_id)
                except Exception:
                    pass
            await bot.send_message(
                user.id,
                f"✅ <b>Ответ отправлен</b>\n\nПользователь <b>{name_display}</b> получил ваш ответ.",
                parse_mode="HTML",
                reply_markup=more_kb,
            )
            logger.info(f"[FEEDBACK REPLY via child] Sent to user {target_user_id} via bot {child_bot_id}")
        except Exception as e:
            await bot.send_message(
                user.id,
                f"⚠️ Не удалось отправить ответ: {e}\n\n"
                "Убедитесь что пользователь нажал /start этому боту.",
            )
            logger.warning(f"[FEEDBACK REPLY via child] Failed to send to {target_user_id}: {e}")
        return  # ← завершаем обработку, это был ответ от администратора

    # ── 2. /start → регистрация bot_activated ───────────────────
    if text.startswith("/start"):
        # UPSERT: создаём/обновляем запись по каждому каналу этого бота
        chats = await db.fetch(
            "SELECT chat_id, owner_id FROM bot_chats WHERE child_bot_id=$1 AND is_active=true",
            child_bot_id,
        )
        for ch in chats:
            await db.execute(
                """
                INSERT INTO bot_users (owner_id, chat_id, user_id, username, first_name,
                                       language_code, is_premium, bot_activated, is_active)
                VALUES ($1, $2, $3, $4, $5, $6, $7, true, false)
                ON CONFLICT (owner_id, chat_id, user_id) DO UPDATE
                    SET bot_activated = true
                """,
                ch["owner_id"], ch["chat_id"], user.id,
                user.username, user.first_name or "",
                user.language_code,
                bool(getattr(user, "is_premium", False)),
            )
        logger.info(f"[START] user {user.id} activated bot {child_bot_id} ({len(chats)} channels)")

        # Подтверждение пользователю
        try:
            await bot.send_message(
                user.id,
                "✅ Вы подписались на уведомления от канала. "
                "Теперь вы будете получать приветствие и рассылки.",
            )
        except Exception as e:
            logger.debug(f"[START] send reply failed for {user.id}: {e}")

    else:
        # ── 3. Обычное сообщение → обратная связь ───────────────
        # Если у пользователя активная reply-капча — обрабатываем её,
        # не пересылаем сообщение в обратную связь.
        from handlers.captcha import _pending, _pending_group, _approve_user_from_message
        _captcha_chat_id = None
        for (_cid, _uid) in list(_pending.keys()):
            if _uid == user.id:
                _captcha_chat_id = _cid
                break
        if _captcha_chat_id is None:
            for (_cid, _uid) in list(_pending_group.keys()):
                if _uid == user.id:
                    _captcha_chat_id = _cid
                    break
        if _captcha_chat_id is not None:
            logger.debug(f"[CAPTCHA REPLY] Processing reply captcha for user {user.id} chat {_captcha_chat_id}")
            await _approve_user_from_message(message, bot, _captcha_chat_id, user.id, success=True)
            return

        # JOIN с bot_users убран намеренно: feedback работает даже если
        # пользователь ещё не нажимал /start и нет записи в bot_users.
        chats = await db.fetch(
            """
            SELECT bc.owner_id, bc.chat_id
            FROM bot_chats bc
            LEFT JOIN child_bots cb ON cb.id = bc.child_bot_id
            WHERE bc.child_bot_id=$1
              AND bc.is_active=true
              AND (bc.feedback_enabled=true OR cb.feedback_enabled=true)
            """,
            child_bot_id,
        )
        if chats:
            from handlers.feedback import handle_feedback_message
            sent_owners: set = set()
            for ch in chats:
                oid = ch["owner_id"]
                if oid in sent_owners:
                    continue
                # Не пересылать если отправитель — владелец или его команда
                if user.id == oid:
                    logger.debug(f"[FEEDBACK] Skipping: user {user.id} is the owner")
                    continue
                is_team = await db.fetchval(
                    "SELECT 1 FROM team_members WHERE user_id=$1 AND owner_id=$2 AND is_active=true",
                    user.id, oid,
                )
                if is_team:
                    logger.debug(f"[FEEDBACK] Skipping: user {user.id} is a team member")
                    continue
                # Не пересылать если пользователь заблокирован по feedback
                is_blocked = await db.fetchval(
                    "SELECT feedback_blocked FROM bot_users WHERE user_id=$1 AND owner_id=$2 LIMIT 1",
                    user.id, oid,
                )
                if is_blocked:
                    logger.debug(f"[FEEDBACK] Skipping: user {user.id} is feedback_blocked for owner {oid}")
                    continue
                sent_owners.add(oid)
                # child_bot_instance=bot → уведомления идут через дочернего бота, не через основной
                await handle_feedback_message(
                    message, _main_bot, oid, ch["chat_id"], child_bot_id,
                    child_bot_instance=bot,
                )
            if sent_owners:
                logger.info(f"[FEEDBACK] Forwarded msg from user {user.id} to {len(sent_owners)} owner(s)")
                # Подтверждение пользователю что сообщение получено
                try:
                    await bot.send_message(
                        user.id,
                        "✅ <b>Сообщение отправлено!</b>\n"
                        "👍 Мы получили ваш запрос и скоро ответим.",
                        parse_mode="HTML",
                    )
                except Exception:
                    pass
        else:
            logger.debug(f"[FEEDBACK] No feedback-enabled chats for user {user.id}")




async def _handle_my_chat_member(
    bot: Bot, child_bot_id: int, owner_id: int, bot_username: str, event: ChatMemberUpdated
):
    """Бот добавлен как администратор — сохраняем площадку и уведомляем владельца."""
    new_status = event.new_chat_member.status

    if new_status in ("administrator", "creator"):
        chat = event.chat
        chat_type = chat.type  # channel | supergroup | group

        # Сохраняем в bot_chats
        await db.execute(
            """
            INSERT INTO bot_chats (owner_id, child_bot_id, chat_id, chat_title, chat_type, is_active, captcha_type)
            VALUES ($1, $2, $3, $4, $5, true, 'off')
            ON CONFLICT (owner_id, chat_id)
            DO UPDATE SET chat_title=EXCLUDED.chat_title,
                          child_bot_id=EXCLUDED.child_bot_id,
                          is_active=true
            """,
            owner_id, child_bot_id, chat.id,
            chat.title or f"chat_{chat.id}", chat_type,
        )


        # Уведомляем владельца через главный бот
        if _main_bot:
            type_icon = "📢" if chat_type == "channel" else "👥"
            try:
                from aiogram.types import InlineKeyboardMarkup, InlineKeyboardButton
                await _main_bot.send_message(
                    owner_id,
                    f"✅ {type_icon} <b>{chat.title}</b> подключён!\n\n"
                    f"Бот @{bot_username} добавлен как администратор и готов к работе.",
                    reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                        [InlineKeyboardButton(text="📍 Площадки бота", callback_data=f"bot_chats_list:{child_bot_id}")],
                        [InlineKeyboardButton(text="⚙️ Настройки бота", callback_data=f"bot_settings:{child_bot_id}")],
                    ]),
                    parse_mode="HTML",
                )
            except Exception as e:
                logger.error(f"Failed to notify owner {owner_id}: {e}")

        logger.info(f"Child bot @{bot_username} added to {chat.title} ({chat.id}) as {new_status}")

    elif new_status in ("kicked", "left"):
        # Бот удалён — деактивируем площадку
        await db.execute(
            "UPDATE bot_chats SET is_active=false WHERE child_bot_id=$1 AND chat_id=$2",
            child_bot_id, event.chat.id,
        )
        if _main_bot:
            try:
                await _main_bot.send_message(
                    owner_id,
                    f"⚠️ Бот @{bot_username} удалён из <b>{event.chat.title}</b>.\n"
                    f"Площадка деактивирована.",
                    parse_mode="HTML",
                )
            except Exception:
                pass


async def _check_join_limit(
    bot: Bot, owner_id: int, chat_id: int, settings: dict, user,
    chat_title: str = "", chat_username: str = "",
) -> bool:
    """Проверяет лимит вступлений за период. Возвращает True если пользователь заблокирован.
    Наказание применяется через дочернего бота (bot). Уведомление — через главного (_main_bot)."""
    enabled = settings.get("join_limit_enabled")
    logger.info(
        f"[LIMIT DBG] user={user.id} chat={chat_id} "
        f"enabled={enabled} count_cfg={settings.get('join_limit_count')} "
        f"period={settings.get('join_limit_period_min')}"
    )
    if not enabled:
        return False

    period_min = int(settings.get("join_limit_period_min") or 1)
    limit      = int(settings.get("join_limit_count") or 50)
    punishment = (settings.get("join_limit_punishment") or "kick")

    # Считаем вступления за последние N минут
    count = await db.fetchval(
        "SELECT COUNT(*) FROM bot_users "
        "WHERE owner_id=$1 AND chat_id=$2::bigint "
        "AND joined_at >= now() - ($3 * interval '1 minute')",
        owner_id, chat_id, period_min,
    )
    logger.info(f"[LIMIT DBG] count_now={count} limit={limit} period={period_min}min")

    if count is not None and count >= limit:
        logger.info(
            f"[LIMIT] owner={owner_id} chat={chat_id} "
            f"count={count}/{limit} per {period_min}min \u2192 {punishment} user={user.id}"
        )
        try:
            await bot.ban_chat_member(chat_id, user.id)
            if punishment == "kick":
                # Кик = бан + немедленный разбан
                await bot.unban_chat_member(chat_id, user.id, only_if_banned=True)
        except Exception as _e:
            logger.warning(f"[LIMIT] punishment failed: {_e}")

        # Формируем название чата
        if chat_username:
            chat_display = f'<a href="https://t.me/{chat_username}">{chat_title or chat_username}</a>'
        elif chat_title:
            chat_display = f"<b>{chat_title}</b>"
        else:
            chat_display = f"<code>{chat_id}</code>"

        # Уведомление владельцу через главного бота
        if _main_bot:
            try:
                pun_ru = "\U0001f9b5 Кик" if punishment == "kick" else "\U0001f528 Бан"
                await _main_bot.send_message(
                    owner_id,
                    f"\u26a0\ufe0f <b>Превышен лимит вступлений!</b>\n\n"
                    f"Чат: {chat_display}\n"
                    f"За <b>{period_min} мин.</b> вступило: <b>{count}</b> чел. "
                    f"(лимит \u2265{limit})\n"
                    f"Пользователь <a href='tg://user?id={user.id}'>"
                    f"{user.first_name or user.id}</a> — {pun_ru}",
                    parse_mode="HTML",
                )
            except Exception:
                pass
        return True
    return False


async def _handle_join_request(bot: Bot, child_bot_id: int, event: ChatJoinRequest):
    """Заявка на вступление — проверяем настройки и обрабатываем."""
    chat_id = event.chat.id
    user = event.from_user

    raw_invite = event.invite_link.invite_link if event.invite_link else None
    logger.info(f"[JOIN REQ] user={user.id} chat={chat_id} invite_link={raw_invite}")

    # RAW dump — чтобы увидеть всё что прислал Telegram
    try:
        import json
        raw = event.model_dump() if hasattr(event, "model_dump") else {}
        logger.info(f"[JOIN REQ RAW] {json.dumps(raw, ensure_ascii=False, default=str)[:600]}")
    except Exception as _e:
        logger.info(f"[JOIN REQ RAW err] {_e}")

    # Получаем настройки площадки
    chat_settings = await db.fetchrow(
        """
        SELECT autoaccept, autoaccept_delay, welcome_text,
               captcha_type, captcha_text, captcha_timer_min, captcha_emoji_set,
               captcha_buttons_raw, captcha_button_style,
               captcha_greet, captcha_accept_now, captcha_accept_all,
               filter_rtl, filter_hieroglyph, filter_no_photo,
               join_limit_enabled, join_limit_punishment,
               join_limit_period_min, join_limit_count,
               owner_id
        FROM bot_chats
        WHERE child_bot_id=$1 AND chat_id=$2 AND is_active=true
        """,
        child_bot_id, chat_id,
    )
    if not chat_settings:
        return

    owner_id = chat_settings["owner_id"]

    # Сохраняем заявку в join_requests (очередь)
    await db.execute(
        """
        INSERT INTO join_requests (owner_id, chat_id, user_id, username, first_name)
        VALUES ($1, $2, $3, $4, $5)
        ON CONFLICT (owner_id, chat_id, user_id)
        DO UPDATE SET status='pending', requested_at=now(), resolved_at=NULL
        """,
        owner_id, chat_id, user.id,
        user.username or "", user.first_name or "",
    )

    # ── ЗАЩИТНЫЕ ФИЛЬТРЫ (применяются до капчи/авто-принятия) ───
    # 1. Языковой фильтр
    # Нормализуем language_code: 'ru-RU' → 'ru', 'en-US' → 'en'
    user_lang = (user.language_code or "").split("-")[0].lower()
    if user_lang:
        lang_blocked = await db.fetchrow(
            "SELECT 1 FROM language_filters "
            "WHERE owner_id=$1 AND chat_id=$2::bigint AND language_code=$3",
            owner_id, chat_id, user_lang,
        )
        if lang_blocked:
            try:
                await event.decline()
            except Exception:
                pass
            logger.info(f"[FILTER-LANG] Declined {user.id} lang={user_lang}")
            return

    # 2. RTL-символы в имени
    full_name = f"{user.first_name or ''} {user.last_name or ''}".strip()
    if chat_settings.get("filter_rtl"):
        from services.security import detect_rtl
        if detect_rtl(full_name):
            try:
                await event.decline()
            except Exception:
                pass
            logger.info(f"[FILTER-RTL] Declined {user.id} name={full_name!r}")
            return

    # 3. Иероглифы в имени
    if chat_settings.get("filter_hieroglyph"):
        from services.security import detect_hieroglyph
        if detect_hieroglyph(full_name):
            try:
                await event.decline()
            except Exception:
                pass
            logger.info(f"[FILTER-HIER] Declined {user.id} name={full_name!r}")
            return

    # 4. Аккаунты без фото
    if chat_settings.get("filter_no_photo"):
        try:
            photos = await bot.get_user_profile_photos(user.id, limit=1)
            if photos.total_count == 0:
                await event.decline()
                logger.info(f"[FILTER-PHOTO] Declined {user.id} — no avatar")
                return
        except Exception as _e:
            logger.warning(f"[FILTER-PHOTO] get_user_profile_photos failed: {_e}")

    captcha_type = (chat_settings.get("captcha_type") or "off")

    # Определяем invite_link_url:
    # 1) Берём из события (если Telegram прислал)
    # 2) Fallback: если только одна активная request-ссылка в чате — атрибутируем к ней
    raw_invite = event.invite_link.invite_link if event.invite_link else None
    invite_link_url = raw_invite
    if not invite_link_url:
        link_rows = await db.fetch(
            "SELECT link FROM invite_links WHERE chat_id=$1::bigint AND link_type='request' AND is_active=true",
            chat_id,
        )
        if len(link_rows) == 1:
            invite_link_url = link_rows[0]["link"]
            logger.info(f"[JOIN REQ FALLBACK] Inferred link from single active request link: {invite_link_url}")
        elif len(link_rows) > 1:
            logger.info(f"[JOIN REQ FALLBACK] Multiple active links ({len(link_rows)}), can't infer")

    # ── КАПЧА (приоритет над авто-принятием) ──────────────────
    if captcha_type != "off":
        # chat_join_request даёт боту временное право писать в личку —
        # /start от пользователя НЕ требуется.
        from handlers.captcha import send_captcha
        settings_for_captcha = {
            "captcha_type":             captcha_type,
            "captcha_text":             chat_settings.get("captcha_text"),
            "captcha_timer_min":        chat_settings.get("captcha_timer_min") or 1,
            "captcha_emoji_set":        chat_settings.get("captcha_emoji_set"),
            "captcha_buttons_raw":      chat_settings.get("captcha_buttons_raw"),
            "captcha_button_style":     chat_settings.get("captcha_button_style") or "inline",
            "captcha_greet":            chat_settings.get("captcha_greet") or False,
            "captcha_accept_now":       chat_settings.get("captcha_accept_now") or False,
            "captcha_accept_all":       chat_settings.get("captcha_accept_all") or False,
            "welcome_text":             chat_settings.get("welcome_text"),
            "owner_id":                 owner_id,
            "invite_link_url":          invite_link_url,   # для трекинга статистики
        }
        await send_captcha(bot, event, settings_for_captcha)
        return  # дальнейшая обработка — в captcha.py после нажатия кнопки

    # ── АВТО-ПРИНЯТИЕ (только если капча выключена) ────────────
    autoaccept = chat_settings["autoaccept"]
    delay      = chat_settings["autoaccept_delay"] or 0

    if autoaccept:
        # Авто-принятие
        if delay > 0:
            await asyncio.sleep(delay * 60)
        try:
            await bot.approve_chat_join_request(chat_id, user.id)
            # Регистрируем в bot_users после одобрения
            await db.execute(
                """
                INSERT INTO bot_users (owner_id, chat_id, user_id, username, first_name,
                                       language_code, is_premium, joined_at)
                VALUES ($1, $2, $3, $4, $5, $6, $7, now())
                ON CONFLICT (owner_id, chat_id, user_id) DO UPDATE
                    SET is_active=true, left_at=NULL,
                        joined_at=now(),
                        username=EXCLUDED.username,
                        is_premium=EXCLUDED.is_premium,
                        bot_activated = (bot_users.bot_activated OR EXCLUDED.bot_activated)
                """,
                owner_id, chat_id, user.id,
                user.username, user.first_name, user.language_code,
                getattr(user, "is_premium", False),
            )
            await db.execute(
                "UPDATE join_requests SET status='approved', resolved_at=now() "
                "WHERE owner_id=$1 AND chat_id=$2 AND user_id=$3",
                owner_id, chat_id, user.id,
            )
            # Трекинг ссылки-приглашения (fallback уже вычислен выше в invite_link_url)
            if invite_link_url:
                await _track_invite_link(invite_link_url, user)
            # Приветственное сообщение
            welcome = chat_settings.get("welcome_text")
            if welcome:
                await _try_send_dm(bot, user.id, welcome)
        except Exception as e:
            logger.warning(f"approve join request error: {e}")
    # Если autoaccept=false — заявка остаётся в очереди со статусом pending


async def _handle_chat_member(bot: Bot, child_bot_id: int, event: ChatMemberUpdated):
    """Пользователь вступил или вышел из открытого канала/группы."""
    new_status = event.new_chat_member.status
    old_status = event.old_chat_member.status if event.old_chat_member else None
    user = event.new_chat_member.user
    chat_id = event.chat.id

    # Получаем настройки площадки
    chat_settings = await db.fetchrow(
        """
        SELECT owner_id, welcome_text, farewell_text,
               captcha_type, captcha_text, captcha_timer_min, captcha_emoji_set,
               join_limit_enabled, join_limit_punishment,
               join_limit_period_min, join_limit_count
        FROM bot_chats
        WHERE child_bot_id=$1 AND chat_id=$2 AND is_active=true
        """,
        child_bot_id, chat_id,
    )
    if not chat_settings:
        return

    owner_id = chat_settings["owner_id"]

    # ── Пользователь вступил ──────────────────────────────────
    # old_status может быть: None, "left", "kicked", "restricted" (одобрение заявки)
    if new_status == "member" and old_status not in ("member", "administrator", "creator"):
        from services.security import detect_rtl, detect_hieroglyph

        full_name = f"{user.first_name or ''} {user.last_name or ''}".strip()
        has_rtl        = detect_rtl(full_name)
        has_hieroglyph = detect_hieroglyph(full_name)
        is_premium     = bool(getattr(user, "is_premium", False))

        LANG_TO_COUNTRY = {
            "ru": "RU", "uk": "UA", "be": "BY", "kk": "KZ",
            "en": "US", "de": "DE", "fr": "FR", "es": "ES",
            "it": "IT", "pt": "BR", "zh": "CN", "ar": "AR",
            "tr": "TR", "pl": "PL", "nl": "NL", "sv": "SE",
            "da": "DK", "fi": "FI", "no": "NO", "cs": "CZ",
            "ro": "RO", "hu": "HU", "bg": "BG",
            "fa": "IR", "he": "IL", "hi": "IN",
            "id": "ID", "ms": "MY", "th": "TH", "vi": "VN",
            "ko": "KR", "ja": "JP",
        }
        country_code = LANG_TO_COUNTRY.get(
            (user.language_code or "").split("-")[0].lower()
        )

        captcha_type = (chat_settings.get("captcha_type") or "off")
        # Капча по обычной ссылке не применяется — пользователь пропускается.
        # Капча работает только через join request (см. _handle_join_request).

        # ── Языковой фильтр (применяется и для групп с обычными ссылками) ──
        # Нормализуем language_code: 'ru-RU' → 'ru', 'en-US' → 'en'
        user_lang = (user.language_code or "").split("-")[0].lower()
        if user_lang:
            lang_blocked = await db.fetchrow(
                "SELECT 1 FROM language_filters "
                "WHERE owner_id=$1 AND chat_id=$2::bigint AND language_code=$3",
                owner_id, chat_id, user_lang,
            )
            if lang_blocked:
                try:
                    await bot.ban_chat_member(chat_id, user.id)
                    await bot.unban_chat_member(chat_id, user.id, only_if_banned=True)
                except Exception as _e:
                    logger.warning(f"[FILTER-LANG-GROUP] kick failed for user={user.id}: {_e}")
                logger.info(f"[FILTER-LANG-GROUP] Kicked user={user.id} lang={user_lang} from chat={chat_id}")
                return


        # Ищем ссылку по invite_link из события
        link_id = None
        raw_invite = event.invite_link.invite_link if event.invite_link else None
        logger.info(f"[LINK DBG] user={user.id} chat={chat_id} invite_link={raw_invite}")
        if raw_invite:
            row = await db.fetchrow(
                "SELECT id FROM invite_links WHERE link=$1",
                raw_invite,
            )
            logger.info(f"[LINK DBG] db row found: {row is not None}")
            if row:
                link_id = row["id"]
                if country_code:
                    await db.execute(
                        """
                        UPDATE invite_links SET
                            joined           = joined + 1,
                            rtl_count        = rtl_count + $2::int,
                            hieroglyph_count = hieroglyph_count + $3::int,
                            premium_count    = premium_count + $4::int,
                            countries        = jsonb_set(
                                COALESCE(countries, '{}'),
                                ARRAY[$5],
                                (COALESCE(countries->$5, '0')::int + 1)::text::jsonb
                            )
                        WHERE id = $1
                        """,
                        link_id,
                        int(has_rtl), int(has_hieroglyph), int(is_premium),
                        country_code,
                    )
                else:
                    await db.execute(
                        """
                        UPDATE invite_links SET
                            joined           = joined + 1,
                            rtl_count        = rtl_count + $2::int,
                            hieroglyph_count = hieroglyph_count + $3::int,
                            premium_count    = premium_count + $4::int
                        WHERE id = $1
                        """,
                        link_id,
                        int(has_rtl), int(has_hieroglyph), int(is_premium),
                    )

        await db.execute(
            """
            INSERT INTO bot_users (owner_id, chat_id, user_id, username, first_name,
                                   language_code, is_premium, has_rtl, has_hieroglyph,
                                   joined_via_link_id, joined_at)
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, now())
            ON CONFLICT (owner_id, chat_id, user_id) DO UPDATE
                SET is_active=true, left_at=NULL,
                    joined_at=now(),
                    username=EXCLUDED.username,
                    first_name=EXCLUDED.first_name,
                    is_premium=EXCLUDED.is_premium,
                    joined_via_link_id=COALESCE(EXCLUDED.joined_via_link_id, bot_users.joined_via_link_id),
                    bot_activated = (bot_users.bot_activated OR EXCLUDED.bot_activated)
            """,
            owner_id, chat_id, user.id,
            user.username, user.first_name, user.language_code,
            is_premium, has_rtl, has_hieroglyph, link_id,
        )
        logger.info(f"[MEMBER] User {user.id} joined chat {chat_id} (owner={owner_id})")

        # Проверка лимита вступлений
        await _check_join_limit(
            bot, owner_id, chat_id, dict(chat_settings), user,
            chat_title=event.chat.title or "",
            chat_username=event.chat.username or "",
        )

        # Приветственное — два случая:
        # 1. Капча выключена → отправляем всем вступившим.
        # 2. Капча включена в групповом режиме → пользователь уже прошёл капчу
        #    (флаг _passed_captcha_group) и теперь реально вступил по one_time_link.
        #    В канальном режиме (ChatJoinRequest) приветствие уже отправлено в captcha.py.
        welcome = chat_settings.get("welcome_text")
        logger.info(f"[WELCOME] user={user.id} captcha={captcha_type} welcome_text={repr(welcome)[:60]}")
        if welcome:
            from handlers.captcha import _passed_captcha_group
            captcha_key = (chat_id, user.id)
            passed_group_captcha = captcha_key in _passed_captcha_group
            if passed_group_captcha:
                # Потребляем флаг сразу, чтобы не отправить приветствие дважды
                _passed_captcha_group.discard(captcha_key)
                logger.info(f"[WELCOME] group captcha passed — sending welcome to user={user.id}")
                await _try_send_dm(bot, user.id, welcome)
            elif captcha_type == "off":
                await _try_send_dm(bot, user.id, welcome)
    # ── Пользователь вышел/забанен ────────────────────────────
    elif new_status in ("left", "kicked") and old_status == "member":
        key = (chat_id, user.id)

        # Если это мы сами кикнули пользователя для капчи — пропускаем 'left'-событие
        from handlers.captcha import _kicked_for_captcha
        if key in _kicked_for_captcha:
            _kicked_for_captcha.discard(key)
            logger.info(f"[CAPTCHA KICK] Skip left-event for captcha-kicked user={user.id} chat={chat_id}")
            return

        # Обновляем счётчик отписок для ссылки (через joined_via_link_id)
        await db.execute(
            """
            UPDATE invite_links SET unsubscribed = unsubscribed + 1
            WHERE id = (
                SELECT joined_via_link_id FROM bot_users
                WHERE owner_id=$1 AND chat_id=$2 AND user_id=$3
                  AND joined_via_link_id IS NOT NULL
            )
            """,
            owner_id, chat_id, user.id,
        )
        await db.execute(
            "UPDATE bot_users SET is_active=false, left_at=now() "
            "WHERE owner_id=$1 AND chat_id=$2 AND user_id=$3",
            owner_id, chat_id, user.id,
        )
        logger.info(f"[MEMBER] User {user.id} left chat {chat_id} (owner={owner_id})")

        # Прощальное сообщение
        farewell = chat_settings.get("farewell_text")
        if farewell:
            await _try_send_dm(bot, user.id, farewell)
