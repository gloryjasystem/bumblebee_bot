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
    """Обновляет статистику ссылки-приглашения. Возвращает link_id или None.
    Дедупликация: один пользователь считается только один раз для каждой ссылки.
    """
    if not invite_link_url:
        return None
    row = await db.fetchrow("SELECT id FROM invite_links WHERE link=$1", invite_link_url)
    if not row:
        return None
    link_id = row["id"]

    # Проверяем, не учитывали ли мы уже этого пользователя по этой ссылке
    already = await db.fetchrow(
        "SELECT joined_counted FROM invite_link_members WHERE link_id=$1 AND user_id=$2",
        link_id, user.id,
    )
    if already and already["joined_counted"]:
        # Уже учтён — не считаем повторно, но обновляем joined_via_link_id
        logger.info(f"[LINK] User {user.id} already counted for link_id={link_id}, skipping")
        return link_id

    # Определяем пол
    from services.gender import guess_gender
    gender = guess_gender(user.first_name or "")
    males_inc   = 1 if gender == "M" else 0
    females_inc = 1 if gender == "F" else 0

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
                males            = males + $2::int,
                females          = females + $3::int,
                rtl_count        = rtl_count + $4::int,
                hieroglyph_count = hieroglyph_count + $5::int,
                premium_count    = premium_count + $6::int,
                countries        = jsonb_set(
                    COALESCE(countries, '{}'),
                    ARRAY[$7],
                    (COALESCE(countries->$7, '0')::int + 1)::text::jsonb
                )
            WHERE id = $1
            """,
            link_id, males_inc, females_inc,
            int(has_rtl), int(has_hieroglyph), int(is_premium), country_code,
        )
    else:
        await db.execute(
            """
            UPDATE invite_links SET
                joined           = joined + 1,
                males            = males + $2::int,
                females          = females + $3::int,
                rtl_count        = rtl_count + $4::int,
                hieroglyph_count = hieroglyph_count + $5::int,
                premium_count    = premium_count + $6::int
            WHERE id = $1
            """,
            link_id, males_inc, females_inc,
            int(has_rtl), int(has_hieroglyph), int(is_premium),
        )

    # Помечаем пользователя как учтённого (INSERT-or-UPDATE)
    await db.execute(
        """
        INSERT INTO invite_link_members (link_id, user_id, joined_counted)
        VALUES ($1, $2, true)
        ON CONFLICT (link_id, user_id) DO UPDATE SET joined_counted = true
        """,
        link_id, user.id,
    )
    logger.info(f"[LINK] Tracked join via link_id={link_id} for user {user.id} gender={gender}")
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


async def _global_blacklist_check(bot: Bot, child_bot_id: int, owner_id: int, update: Update) -> bool:
    """Проверяет любое действие юзера по базе ЧС (Глобальной или Локальной). Возвращает True, если юзер заблокирован."""
    user = None
    chat_id = None
    chat_type = None

    if update.message and update.message.from_user:
        user = update.message.from_user
        chat = update.message.chat
        chat_id = chat.id if chat else None
        chat_type = chat.type if chat else None
    elif update.callback_query and update.callback_query.from_user:
        user = update.callback_query.from_user
        if update.callback_query.message and update.callback_query.message.chat:
            chat_id = update.callback_query.message.chat.id
            chat_type = update.callback_query.message.chat.type
    elif update.chat_member and update.chat_member.new_chat_member:
        user = update.chat_member.new_chat_member.user
        chat_id = update.chat_member.chat.id
        chat_type = update.chat_member.chat.type
    elif update.chat_join_request:
        user = update.chat_join_request.from_user
        chat_id = update.chat_join_request.chat.id
        chat_type = update.chat_join_request.chat.type
    elif getattr(update, "message_reaction", None) and getattr(update.message_reaction, "user", None):
        # В Aiogram 3 MessageReactionUpdated
        user = update.message_reaction.user
        chat_id = update.message_reaction.chat.id
        chat_type = update.message_reaction.chat.type

    if not user:
        return False

    settings = await db.fetchrow(
        """
        SELECT cb.blacklist_enabled,
               COALESCE((SELECT blacklist_active FROM platform_users WHERE user_id = $1), true) AS blacklist_active,
               EXISTS(
                   SELECT 1 FROM ga_selected_bots gsb
                   WHERE gsb.owner_id = $1
                     AND gsb.child_bot_id = $2
               ) AS in_global_bl_scope
        FROM child_bots cb
        WHERE cb.id = $2
        """,
        owner_id, child_bot_id
    )
    if not settings:
        return False

    from services.blacklist import check_blacklist
    from config import settings as _cfg

    is_global_block = False
    is_local_block = False

    if settings.get("blacklist_active", True) and settings.get("in_global_bl_scope", False):
        if await check_blacklist(_cfg.owner_telegram_id, user.id, user.username, child_bot_id=None):
            is_global_block = True

    if not is_global_block and settings.get("blacklist_enabled", True):
        if await check_blacklist(owner_id, user.id, user.username, child_bot_id=child_bot_id):
            is_local_block = True

    if is_global_block or is_local_block:
        if chat_id and chat_type in ("group", "supergroup", "channel"):
            try:
                await bot.ban_chat_member(chat_id, user.id)
            except Exception:
                pass
            # Баним, а если это сообщение - оно обычно не удалится автоматически (или можно попробовать удалить)
            if update.message:
                try:
                    await bot.delete_message(chat_id, update.message.message_id)
                except Exception:
                    pass
        return True
    return False


async def _handle_child_update(
    bot: Bot, child_bot_id: int, owner_id: int, bot_username: str, update: Update
):
    """Обрабатывает одно событие от дочернего бота."""
    try:
        # Супер-перехват Капкана (Blacklist) на любое действие
        if await _global_blacklist_check(bot, child_bot_id, owner_id, update):
            return  # Игнорируем и блокируем всё дальнейшее выполнение
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
            if update.chat_member.new_chat_member.user.id == bot.id:
                # В редких случаях Telegram может прислать chat_member вместо my_chat_member для самого бота
                await _handle_my_chat_member(bot, child_bot_id, owner_id, bot_username, update.chat_member)
            else:
                await _handle_chat_member(bot, child_bot_id, update.chat_member)

        # ── Сообщение пользователя + миграция группы ────────────────────────────
        elif update.message and update.message.from_user:
            chat_type = update.message.chat.type if update.message.chat else "private"
            if chat_type in ("group", "supergroup", "channel"):
                # Групповое сообщение — автоответчик + реакции
                await _handle_group_message(bot, child_bot_id, update.message)
            else:
                # Личное сообщение (/start и др.)
                await _handle_message(bot, child_bot_id, owner_id, update.message)

        # ── Миграция группы → супергруппа ───────────────────────────────────────
        # Telegram присылает service-сообщение migrate_to_chat_id когда
        # обычная группа конвертируется в супергруппу. Старый chat_id становится
        # недействительным — обновляем запись в БД на новый chat_id «на лету».
        elif update.message and update.message.migrate_to_chat_id:
            old_chat_id = update.message.chat.id
            new_chat_id = update.message.migrate_to_chat_id
            await db.execute(
                """
                UPDATE bot_chats
                SET chat_id   = $1,
                    chat_type = 'supergroup'
                WHERE child_bot_id = $2
                  AND chat_id      = $3
                """,
                new_chat_id, child_bot_id, old_chat_id,
            )
            logger.info(
                f"[MIGRATE] child_bot={child_bot_id}: chat_id {old_chat_id} → {new_chat_id} (group→supergroup)"
            )

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

    # ── 0. Статистика сообщений ──────────────────────────────────
    try:
        await db.execute(
            "INSERT INTO message_events (owner_id, chat_id, user_id) VALUES ($1, $2, $3)",
            settings["owner_id"], chat_id, user.id,
        )
    except Exception as _me:
        logger.debug(f"[MSG_STAT] insert failed: {_me}")

    text = message.text or message.caption or ""

    # ── 1. Автоответчик и 2. Реакции (перемещены в личные сообщения бота) ──


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
            sent_msg = None
            if message.text:
                sent_msg = await bot.send_message(
                    target_user_id,
                    header + message.text,
                    parse_mode="HTML",
                )
            elif message.photo:
                caption_text = header + (message.caption or "")
                sent_msg = await bot.send_photo(
                    target_user_id, message.photo[-1].file_id,
                    caption=caption_text,
                    parse_mode="HTML",
                )
            elif message.video:
                caption_text = header + (message.caption or "")
                sent_msg = await bot.send_video(
                    target_user_id, message.video.file_id,
                    caption=caption_text,
                    parse_mode="HTML",
                )
            elif message.document:
                caption_text = header + (message.caption or "")
                sent_msg = await bot.send_document(
                    target_user_id, message.document.file_id,
                    caption=caption_text,
                    parse_mode="HTML",
                )
            elif message.voice:
                sent_msg = await bot.send_voice(target_user_id, message.voice.file_id)
            elif message.audio:
                caption_text = header + (message.caption or "")
                sent_msg = await bot.send_audio(
                    target_user_id, message.audio.file_id,
                    caption=caption_text,
                    parse_mode="HTML",
                )
            elif message.sticker:
                await bot.send_message(
                    target_user_id,
                    header.strip(),
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

        # ── 3a. Реакции (ставим на ЛЮБОЕ сообщение пользователя) ──
        reaction_row = await db.fetchrow(
            "SELECT reaction_emoji FROM bot_chats WHERE child_bot_id=$1 AND is_active=true AND reaction_emoji IS NOT NULL LIMIT 1",
            child_bot_id
        )
        if reaction_row and reaction_row["reaction_emoji"]:
            try:
                from aiogram.types import ReactionTypeEmoji
                await bot.set_message_reaction(
                    chat_id=message.chat.id,
                    message_id=message.message_id,
                    reaction=[ReactionTypeEmoji(emoji=reaction_row["reaction_emoji"])],
                )
            except Exception as e:
                logger.debug(f"[REACTION PM] failed for bot {child_bot_id}: {e}")

        # ── 3b. Автоответчик и обратная связь (только для текстовых) ────────
        # Флаг: если True — feedback не отправляется (автоответ уже обработал сообщение)
        suppress_feedback = False
        if text:

            # ── Автоответчик ──
            async def _send_ar(t, mid, mtype, mtop, prev, btns):
                if t:
                    # Подставляем переменные: {name}, {allname}, {username}, {chat}, {day}
                    first_name = user.first_name or "Пользователь"
                    last_name  = user.last_name or ""
                    full_name  = f"{first_name} {last_name}".strip()
                    username   = getattr(user, "username", "")
                    chat_title = "бот"
                    today_str  = __import__("datetime").date.today().strftime("%d.%m.%Y")
                    
                    t = (t.replace("{name}", first_name)
                          .replace("{allname}", full_name)
                          .replace("{username}", f"@{username}" if username else "")
                          .replace("{chat}", chat_title)
                          .replace("{day}", today_str))

                import json as _json
                from utils.keyboard import build_inline_keyboard
                
                kr = None
                poll_question = None
                poll_options = []
                
                if btns:
                    try:
                        parsed_btns = _json.loads(btns) if isinstance(btns, str) else btns
                        clean_btns = []
                        rows = parsed_btns if (parsed_btns and isinstance(parsed_btns[0], list)) else [[b] for b in parsed_btns]
                        for row in rows:
                            clean_row = []
                            for b in row:
                                url = str(b.get("url", "")).strip()
                                text_b = str(b.get("text", ""))
                                if url.endswith("(poll)"):
                                    if not poll_question:
                                        poll_question = text_b
                                    else:
                                        poll_options.append(text_b)
                                else:
                                    clean_row.append(b)
                            if clean_row:
                                clean_btns.append(clean_row)
                        kr = build_inline_keyboard(clean_btns) if clean_btns else None
                    except Exception:
                        try:
                            kr = build_inline_keyboard(btns)
                        except Exception:
                            pass

                main_msg = None
                has_content = bool(t) or bool(mid) or bool(kr)
                
                if has_content or not poll_question:
                    if mid:
                        kw_args = {
                            "caption": t or None,
                            "parse_mode": "HTML",
                            "reply_markup": kr,
                            "show_caption_above_media": not mtop
                        }
                        async def do_send(fid):
                            if mtype == "photo": return await message.reply_photo(fid, **kw_args)
                            elif mtype == "video": return await message.reply_video(fid, **kw_args)
                            elif mtype == "animation": return await message.reply_animation(fid, **kw_args)
                            else: return await message.reply_document(fid, **kw_args)
                        try:
                            main_msg = await do_send(mid)
                        except Exception as e:
                            error_msg = str(e).lower()
                            if "wrong file identifier" in error_msg or "file reference" in error_msg or "invalid file" in error_msg:
                                try:
                                    from config import settings as sys_settings
                                    from aiogram import Bot as AioBot
                                    from aiogram.types import BufferedInputFile
                                    mb = AioBot(token=sys_settings.bot_token)
                                    fi = await mb.get_file(mid)
                                    fb = await mb.download_file(fi.file_path)
                                    await mb.session.close()
                                    main_msg = await do_send(BufferedInputFile(fb.read(), filename=f"ar.{mtype}"))
                                except Exception:
                                    main_msg = await message.reply(t or "—", parse_mode="HTML", reply_markup=kr, disable_web_page_preview=not prev)
                            else:
                                main_msg = await message.reply(t or "—", parse_mode="HTML", reply_markup=kr, disable_web_page_preview=not prev)
                    else:
                        main_msg = await message.reply(t or "—", parse_mode="HTML", reply_markup=kr, disable_web_page_preview=not prev)

                if poll_question:
                    if len(poll_options) < 2:
                        poll_options.extend(["Да", "Нет"] if not poll_options else ["Нет"])
                    poll_msg = await message.answer_poll(
                        question=poll_question,
                        options=poll_options[:10],
                        is_anonymous=False
                    )
                    return main_msg or poll_msg
                    
                return main_msg

            # ── Получаем настройку Печати (typing_action) ──
            typing_row = await db.fetchrow(
                "SELECT typing_action FROM bot_chats WHERE child_bot_id=$1 AND is_active=true LIMIT 1",
                child_bot_id
            )
            typing_on = bool(typing_row["typing_action"]) if typing_row else False

            async def _typing_before_send():
                """Имитирует печать перед отправкой автоответа."""
                if typing_on:
                    try:
                        await bot.send_chat_action(chat_id=message.chat.id, action="typing")
                        await asyncio.sleep(3)
                    except Exception:
                        pass

            # ── Сначала проверяем Keyword-ответы ──
            matched_keyword = False
            rules = await db.fetch(
                """
                SELECT a.*, bc.auto_delete_min 
                FROM autoreplies a
                JOIN bot_chats bc ON a.chat_id = bc.chat_id
                WHERE bc.child_bot_id=$1 AND bc.is_active=true
                ORDER BY a.id ASC
                """,
                child_bot_id,
            )
            for rule in rules:
                kw = (rule["keyword"] or "").lower().strip()
                if kw and kw in text.lower():
                    matched_keyword = True
                    suppress_feedback = True  # кейворд сработал — feedback не нужен
                    try:
                        r_text  = rule["reply_text"] or ""
                        r_mid   = rule["reply_media"]
                        r_mtype = rule["reply_media_type"]
                        r_mtop  = rule["reply_media_top"] if rule["reply_media_top"] is not None else True
                        r_prev  = bool(rule["reply_preview"])
                        r_btns  = rule["reply_buttons"]
                        await _typing_before_send()
                        reply = await _send_ar(r_text, r_mid, r_mtype, r_mtop, r_prev, r_btns)
                        # Авто-удаление ответа бота в личке
                        delete_min = int(rule.get("auto_delete_min") or 0)
                        if delete_min > 0 and reply and hasattr(reply, 'message_id'):
                            import asyncio as _asyncio
                            _asyncio.create_task(
                                _delete_later(bot, user.id, reply.message_id, delete_min)
                            )
                        logger.info(f"[AUTOREPLY PM] keyword='{kw}' matched via bot {child_bot_id}")
                    except Exception as e:
                        logger.warning(f"[AUTOREPLY PM] failed via bot {child_bot_id}: {e}")
                    break  # только первое совпадение

            # ── Будем ли применять Общий ответ? ──
            if not matched_keyword:
                general_chat = await db.fetchrow(
                    """
                    SELECT * FROM bot_chats 
                    WHERE child_bot_id=$1 AND is_active=true AND general_reply_enabled=true 
                    LIMIT 1
                    """,
                    child_bot_id
                )
                if general_chat:
                    general_text  = (general_chat.get("general_reply_text") or "").strip()
                    general_media = general_chat.get("general_reply_media")

                    if general_text or general_media:
                        suppress_feedback = True  # общий ответ сработал — feedback не нужен
                        try:
                            g_mtype = general_chat.get("general_reply_media_type")
                            g_mtop  = general_chat.get("general_reply_media_top") if general_chat.get("general_reply_media_top") is not None else True
                            g_prev  = bool(general_chat.get("general_reply_preview"))
                            g_btns  = general_chat.get("general_reply_buttons")
                            await _typing_before_send()
                            reply = await _send_ar(general_text, general_media, g_mtype, g_mtop, g_prev, g_btns)
                            delete_min = int(general_chat.get("auto_delete_min") or 0)
                            if delete_min > 0 and reply and hasattr(reply, 'message_id'):
                                import asyncio as _asyncio
                                _asyncio.create_task(
                                    _delete_later(bot, user.id, reply.message_id, delete_min)
                                )
                            logger.info(f"[AUTOREPLY PM] general reply sent via bot {child_bot_id}")
                        except Exception as e:
                            logger.warning(f"[AUTOREPLY PM] general reply failed via bot {child_bot_id}: {e}")


        # JOIN с bot_users убран намеренно: feedback работает даже если
        # пользователь ещё не нажимал /start и нет записи в bot_users.
        # Но если автоответчик (keyword или общий) уже ответил — не пересылаем.
        if not suppress_feedback:
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
                            "Сообщение отправлено!",
                        )
                    except Exception:
                        pass
            else:
                logger.debug(f"[FEEDBACK] No feedback-enabled chats for user {user.id}")





async def _delete_main_bot_msg(chat_id: int, message_id: int, delay_sec: int):
    """Удаляет сообщение главного бота через delay_sec секунд (авто-чистка уведомлений)."""
    await asyncio.sleep(delay_sec)
    if _main_bot:
        try:
            await _main_bot.delete_message(chat_id, message_id)
        except Exception:
            pass


# ── КРИТИЧЕСКИЕ ПРАВА, необходимые для работы бота ────────────────────────────
# Минимально достаточный набор для: защиты, заявок, рассылок.
_REQUIRED_PERMISSIONS = {
    "can_restrict_members": "Ограничение участников",
    "can_invite_users":     "Приглашение участников",
    "can_delete_messages":  "Удаление сообщений",
}


def _check_permissions(member) -> list[str]:
    """
    Проверяет наличие критических прав у бота.
    Возвращает список ОТСУТСТВУЮЩИХ прав (пустой → всё OK).
    Для creator-статуса права не нужны — он главный.
    """
    if member.status == "creator":
        return []
    missing = []
    for attr, label in _REQUIRED_PERMISSIONS.items():
        if not getattr(member, attr, False):
            missing.append(label)
    return missing


async def _handle_my_chat_member(
    bot: Bot, child_bot_id: int, owner_id: int, bot_username: str, event: ChatMemberUpdated
):
    """
    Event-driven обработчик жизненного цикла бота в чате.

    Сценарии:
      1. ПЕРВИЧНОЕ ДОБАВЛЕНИЕ  — anti-spam guard → проверка прав → регистрация/уведомление.
      2. ИЗМЕНЕНИЕ ПРАВ        — ребот стал admin снова или потерял права → обновляем is_active.
      3. УДАЛЕНИЕ (kicked/left)— мягкая деактивация (is_active=False), запись сохраняется.
      4. PM-блокировка         — пользователь заблокировал бота в личке.
    """
    new_member = event.new_chat_member
    old_member = event.old_chat_member
    new_status  = new_member.status
    old_status  = old_member.status
    chat        = event.chat
    chat_type   = chat.type  # channel | supergroup | group | private
    chat_title  = chat.title or str(chat.id)
    from_user   = event.from_user  # кто изменил права

    # ── Личка: пользователь заблокировал бота ─────────────────────────────────
    if chat_type == "private":
        if new_status in ("kicked", "left"):
            await db.execute(
                "UPDATE bot_users SET is_active=false, left_at=now() "
                "WHERE owner_id=$1 AND user_id=$2",
                owner_id, event.from_user.id,
            )
            logger.info(f"[MCM] User {event.from_user.id} blocked child bot {child_bot_id}")
        return

    # ── Защита от дублей: проверяем, что этот polling-таск — правильный владелец ──
    # Если в системе один и тот же бот зарегистрирован у НЕСКОЛЬКИХ owner_id,
    # запускаются два параллельных polling-цикла и оба получают одни апдейты.
    # Условие: чат уже в нашей БД под ДРУГИМ owner_id → тихо выходим.
    existing_owner = await db.fetchval(
        "SELECT owner_id FROM bot_chats WHERE child_bot_id=$1 AND chat_id=$2",
        child_bot_id, chat.id,
    )
    if existing_owner is not None and existing_owner != owner_id:
        # Этот канал принадлежит другому владельцу — не наше событие.
        logger.debug(
            f"[MCM] Skipping: chat={chat.id} belongs to owner={existing_owner}, "
            f"but this polling task has owner={owner_id}"
        )
        return

    # ── Сценарий 3: бот удалён (kicked / left) ────────────────────────────────
    if new_status in ("kicked", "left"):
        # Мягкая деактивация — НЕ удаляем запись и настройки из БД.
        # При повторном добавлении бота все конфиги сохранятся (Seamless Flow).
        await db.execute(
            "UPDATE bot_chats SET is_active=false, deactivation_reason='kicked' WHERE child_bot_id=$1 AND chat_id=$2",
            child_bot_id, chat.id,
        )
        if _main_bot:
            try:
                await _main_bot.send_message(
                    owner_id,
                    f"⚠️ Бот @{bot_username} удалён из <b>{chat_title}</b>.\n"
                    f"Площадка деактивирована. Настройки сохранены — добавьте бота обратно, чтобы всё восстановить.",
                    parse_mode="HTML",
                )
            except Exception as _e:
                logger.debug(f"[MCM] notify owner failed (kicked): {_e}")
        logger.info(f"[MCM] Bot @{bot_username} removed from chat={chat.id} (soft-deactivated)")
        return

    # ── Сценарии 1 и 2: бот стал administrator / creator ─────────────────────
    if new_status not in ("administrator", "creator"):
        # member / restricted — права урезаны до уровня ниже admin
        was_active = await db.fetchval(
            "SELECT is_active FROM bot_chats WHERE child_bot_id=$1 AND chat_id=$2",
            child_bot_id, chat.id,
        )
        if was_active:
            await db.execute(
                "UPDATE bot_chats SET is_active=false, deactivation_reason='permissions' "
                "WHERE child_bot_id=$1 AND chat_id=$2",
                child_bot_id, chat.id,
            )
            # Строим ссылку на канал
            chat_username = getattr(chat, "username", None)
            if chat_username:
                chat_link = f'<a href="https://t.me/{chat_username}">{chat_title}</a>'
            else:
                chat_link = f"<b>{chat_title}</b>"
            if _main_bot:
                try:
                    sent = await _main_bot.send_message(
                        owner_id,
                        f"⚠️ В вашем канале/группе {chat_link} бот @{bot_username} "
                        f"лишился прав администратора.\n\n"
                        f"📍 Зайдите в {chat_link} → Управление → Администраторы → "
                        f"@{bot_username} и верните права.\n\n"
                        f"Бот возобновит работу автоматически, как только права появятся.",
                        parse_mode="HTML",
                        disable_web_page_preview=True,
                    )
                    asyncio.create_task(_delete_main_bot_msg(sent.chat.id, sent.message_id, 8))
                except Exception as _e:
                    logger.debug(f"[MCM] notify owner failed (rights stripped): {_e}")
            logger.info(f"[MCM] Bot @{bot_username} lost admin in chat={chat.id} — deactivated")
        return

    # ── Дальше new_status in ("administrator", "creator") ────────────────────

    # Шаг 1 (Anti-Spam): from_user добавил бота — проверяем, наш ли он клиент.
    # Исключение: если is_active уже false в БД (бот возвращается в знакомый чат),
    # то владелец уже прошёл проверку — пропускаем anti-spam.
    existing_chat = await db.fetchrow(
        "SELECT is_active FROM bot_chats WHERE child_bot_id=$1 AND chat_id=$2",
        child_bot_id, chat.id,
    )
    is_returning = existing_chat is not None  # чат уже был в БД ранее

    if not is_returning and from_user:
        # Первичное добавление: проверяем, есть ли from_user в нашей системе
        is_our_client = await db.fetchval(
            "SELECT 1 FROM platform_users WHERE user_id=$1", from_user.id,
        )
        if not is_our_client:
            # Посторонний добавил бота в свой канал — немедленно покидаем
            logger.warning(
                f"[MCM] ANTI-SPAM: bot @{bot_username} added to chat={chat.id} by unknown "
                f"user={from_user.id} (@{from_user.username}) — leaving immediately"
            )
            try:
                await bot.leave_chat(chat.id)
            except Exception as _e:
                logger.debug(f"[MCM] leave_chat failed: {_e}")
            return

    # Шаг 2: Проверка критических прав
    missing_perms = _check_permissions(new_member)

    if missing_perms:
        # Записываем в БД как НЕАКТИВНУЮ площадку — она появится в списке как 🔴.
        # При получении прав придёт новый my_chat_member → активируем автоматически.
        await db.execute(
            """
            INSERT INTO bot_chats (owner_id, child_bot_id, chat_id, chat_title, chat_type,
                                   is_active, captcha_type, deactivation_reason)
            VALUES ($1, $2, $3, $4, $5, false, 'off', 'permissions')
            ON CONFLICT (owner_id, chat_id)
            DO UPDATE SET
                chat_title         = EXCLUDED.chat_title,
                child_bot_id       = EXCLUDED.child_bot_id,
                is_active          = false,
                deactivation_reason = 'permissions'
            """,
            owner_id, child_bot_id, chat.id, chat_title, chat_type,
        )
        missing_text = "\n".join(f"  • {p}" for p in missing_perms)
        # Строим кликабельную ссылку на канал/группу
        chat_username = getattr(chat, "username", None)
        if chat_username:
            chat_link = f'<a href="https://t.me/{chat_username}">{chat_title}</a>'
        else:
            chat_link = f"<b>{chat_title}</b>"
        if _main_bot:
            try:
                sent = await _main_bot.send_message(
                    owner_id,
                    f"⚠️ В вашем канале/группе {chat_link} боту @{bot_username} "
                    f"не хватает прав администратора.\n\n"
                    f"Права, которые нужно выдать:\n{missing_text}\n\n"
                    f"📍 Как исправить: зайдите в {chat_link} → Управление → "
                    f"Администраторы → @{bot_username} → включите галочки выше.\n\n"
                    f"Бот подключится автоматически, как только вы сохраните изменения.",
                    parse_mode="HTML",
                    disable_web_page_preview=True,
                )
                asyncio.create_task(_delete_main_bot_msg(sent.chat.id, sent.message_id, 8))
            except Exception as _e:
                logger.debug(f"[MCM] notify owner failed (missing perms): {_e}")
        logger.warning(
            f"[MCM] Bot @{bot_username} in chat={chat.id} missing perms: {missing_perms}"
        )
        return

    # Шаг 3: Все права есть — регистрируем/активируем площадку атомарно.
    await db.execute(
        """
        INSERT INTO bot_chats (owner_id, child_bot_id, chat_id, chat_title, chat_type,
                               is_active, captcha_type, deactivation_reason)
        VALUES ($1, $2, $3, $4, $5, true, 'off', NULL)
        ON CONFLICT (owner_id, chat_id)
        DO UPDATE SET
            chat_title          = EXCLUDED.chat_title,
            child_bot_id        = EXCLUDED.child_bot_id,
            is_active           = true,
            deactivation_reason = NULL
        """,
        owner_id, child_bot_id,
        chat.id, chat_title, chat_type,
    )

    # Шаг 4: Уведомляем владельца с контекстом (первичное vs. возврат прав)
    if _main_bot:
        type_icon = "📢" if chat_type == "channel" else "👥"
        from aiogram.types import InlineKeyboardMarkup, InlineKeyboardButton
        kb = InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="📍 Площадки бота", callback_data=f"bot_chats_list:{child_bot_id}")],
            [InlineKeyboardButton(text="⚙️ Настройки бота", callback_data=f"bot_settings:{child_bot_id}")],
        ])
        if is_returning and existing_chat and not existing_chat["is_active"]:
            # Права возвращены — чат снова в работе
            msg_text = (
                f"✅ Права обновлены! {type_icon} <b>{chat_title}</b> снова в работе.\n\n"
                f"Бот @{bot_username} восстановил защиту автоматически."
            )
        else:
            # Первичное подключение
            msg_text = (
                f"✅ {type_icon} <b>{chat_title}</b> успешно подключён!\n\n"
                f"Бот @{bot_username} готов к работе."
            )
        try:
            await _main_bot.send_message(
                owner_id, msg_text, parse_mode="HTML", reply_markup=kb,
            )
        except Exception as _e:
            logger.debug(f"[MCM] notify owner failed (success): {_e}")

    logger.info(
        f"[MCM] Bot @{bot_username} {'reactivated' if is_returning else 'registered'} "
        f"in chat={chat.id} ({chat_type}) by user={getattr(from_user, 'id', '?')}"
    )


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


async def _delayed_approve_join_request(bot: Bot, owner_id: int, chat_id: int, user, delay_min: int, invite_link_url: str | None, welcome: str | None):
    """Фоновая задача для отложенного (или мгновенного) принятия заявки, чтобы не блокировать webhook."""
    if delay_min > 0:
        await asyncio.sleep(delay_min * 60)
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
        # Трекинг ссылки-приглашения
        if invite_link_url:
            await _track_invite_link(invite_link_url, user)
        # Приветственное сообщение
        if welcome:
            await _try_send_dm(bot, user.id, welcome)
    except Exception as e:
        logger.warning(f"delayed approve join request error: {e}")


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
               captcha_animation, captcha_anim_file_id, captcha_anim_type,
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

    # 5. Языковой фильтр
    blocked_langs_rows = await db.fetch(
        "SELECT language_code FROM language_filters WHERE owner_id=$1 AND chat_id=$2::bigint",
        owner_id, chat_id,
    )
    if blocked_langs_rows:
        blocked_codes = {r["language_code"] for r in blocked_langs_rows}
        from services.security import detect_user_language
        user_langs = detect_user_language(
            getattr(user, "language_code", None),
            user.first_name or "",
            getattr(user, "last_name", "") or "",
        )
        if user_langs and user_langs.intersection(blocked_codes):
            try:
                await event.decline()
            except Exception:
                pass
            logger.info(f"[LANG-FILTER] Declined join req {user.id} (langs={user_langs} blocked={blocked_codes})")
            return

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
            "captcha_animation":        chat_settings.get("captcha_animation") or False,
            "captcha_anim_file_id":     chat_settings.get("captcha_anim_file_id"),
            "captcha_anim_type":        chat_settings.get("captcha_anim_type"),
            "welcome_text":             chat_settings.get("welcome_text"),
            "owner_id":                 owner_id,
            "invite_link_url":          invite_link_url,   # для трекинга статистики
        }
        await send_captcha(bot, event, settings_for_captcha)
        return  # дальнейшая обработка — в captcha.py после нажатия кнопки

    # ── АВТО-ПРИНЯТИЕ (только если капча выключена) ────────────
    autoaccept = chat_settings["autoaccept"]
    delay      = chat_settings["autoaccept_delay"] or 0

    if autoaccept or delay > 0:
        # Авто-принятие или отложенное принятие запускаем в фоне
        asyncio.create_task(
            _delayed_approve_join_request(
                bot=bot,
                owner_id=owner_id,
                chat_id=chat_id,
                user=user,
                delay_min=delay if delay > 0 else 0,
                invite_link_url=invite_link_url,
                welcome=chat_settings.get("welcome_text"),
            )
        )
    # Если autoaccept=false и delay=0 — заявка остаётся в очереди со статусом pending


async def _handle_chat_member(bot: Bot, child_bot_id: int, event: ChatMemberUpdated):
    """Пользователь вступил или вышел из открытого канала/группы."""
    new_status = event.new_chat_member.status
    old_status = event.old_chat_member.status if event.old_chat_member else None
    user = event.new_chat_member.user
    chat_id = event.chat.id

    # Получаем настройки площадки + blacklist_enabled
    from config import settings as _cfg
    chat_settings = await db.fetchrow(
        """
        SELECT bc.*, cb.blacklist_enabled,
               COALESCE((SELECT blacklist_active FROM platform_users WHERE user_id = $3), true) AS blacklist_active,
               EXISTS(
                   SELECT 1 FROM ga_selected_bots gsb
                   WHERE gsb.owner_id = $3
                     AND gsb.child_bot_id = bc.child_bot_id
               ) AS in_global_bl_scope
        FROM bot_chats bc
        JOIN child_bots cb ON bc.child_bot_id = cb.id
        WHERE bc.child_bot_id=$1 AND bc.chat_id=$2 AND bc.is_active=true
        """,
        child_bot_id, chat_id, _cfg.owner_telegram_id,
    )
    if not chat_settings:
        return

    owner_id = chat_settings["owner_id"]

    # ── Пользователь вступил ──────────────────────────────────
    # old_status может быть: None, "left", "kicked", "restricted" (одобрение заявки)
    if new_status == "member" and old_status not in ("member", "administrator", "creator"):
        # ── 1. Проверка ЧС ──────────────────────────────
        from services.blacklist import check_blacklist
        
        is_global_block = False
        is_local_block = False

        if chat_settings.get("blacklist_active", True) and chat_settings.get("in_global_bl_scope", False):
            if await check_blacklist(_cfg.owner_telegram_id, user.id, user.username, child_bot_id=None):
                is_global_block = True

        if chat_settings.get("blacklist_enabled", True):
            if await check_blacklist(owner_id, user.id, user.username, child_bot_id=child_bot_id):
                is_local_block = True

        if is_global_block or is_local_block:
            try:
                await bot.ban_chat_member(chat_id, user.id)
            except Exception:
                pass
            
            await db.execute(
                "UPDATE platform_users SET blocked_count = blocked_count + 1 WHERE user_id = $1",
                owner_id,
            )
            
            if is_global_block:
                await db.execute(
                    "UPDATE child_bots SET blocked_count = blocked_count + 1, global_blocked_count = global_blocked_count + 1 WHERE id = $1",
                    child_bot_id,
                )
            else:
                await db.execute(
                    "UPDATE child_bots SET blocked_count = blocked_count + 1 WHERE id = $1",
                    child_bot_id,
                )

            from handlers.join_requests import _log_action
            await _log_action(owner_id, chat_id, "ban_on_join", user.id)
            logger.info(f"[BL] Kicked {user.id} from open chat {chat_id} (blacklisted. global={is_global_block}, local={is_local_block})")
            return

        # ── 2. Языковой фильтр ──────────────────────────
        blocked_langs = await db.fetch(
            "SELECT language_code FROM language_filters WHERE owner_id=$1 AND chat_id=$2::bigint",
            owner_id, chat_id,
        )
        if blocked_langs:
            blocked_codes = {r["language_code"] for r in blocked_langs}
            from services.security import detect_user_language
            user_langs = detect_user_language(
                getattr(user, "language_code", None),
                user.first_name or "",
                getattr(user, "last_name", "") or "",
            )
            # Кикаем только если пересечение с заблокированными языками
            # Если user_langs пустой (не определить) — пропускаем (вариант A)
            if user_langs and user_langs.intersection(blocked_codes):
                try:
                    await bot.ban_chat_member(chat_id, user.id)
                    await bot.unban_chat_member(chat_id, user.id, only_if_banned=True)
                except Exception:
                    pass
                logger.info(f"[LANG] Kicked {user.id} from {chat_id} (langs={user_langs} blocked={blocked_codes})")
                return

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

        # ── Проверяем auto_accept конкретной ссылки ─────────────────────────
        # Для обычных ссылок (не creates_join_request): если auto_accept='off' —
        # кикаем пользователя сразу после вступления (ban+unban).
        raw_invite = event.invite_link.invite_link if event.invite_link else None
        if raw_invite:
            link_aa_row = await db.fetchrow(
                "SELECT auto_accept FROM invite_links WHERE link=$1",
                raw_invite,
            )
            link_aa = (link_aa_row["auto_accept"] if link_aa_row else None) or "base"
            if link_aa == "off":
                try:
                    await bot.ban_chat_member(chat_id, user.id)
                    await bot.unban_chat_member(chat_id, user.id, only_if_banned=True)
                except Exception as _e:
                    logger.warning(f"[AUTO_OFF] kick failed for user={user.id}: {_e}")
                logger.info(f"[AUTO_OFF] Kicked user={user.id} — link auto_accept=off")
                return

        # Ищем ссылку по invite_link из события
        link_id = None
        logger.info(f"[LINK DBG] user={user.id} chat={chat_id} invite_link={raw_invite}")
        if raw_invite:
            row = await db.fetchrow(
                "SELECT id FROM invite_links WHERE link=$1",
                raw_invite,
            )
            logger.info(f"[LINK DBG] db row found: {row is not None}")
            if row:
                link_id = row["id"]

                # Проверяем дедупликацию
                already = await db.fetchrow(
                    "SELECT joined_counted FROM invite_link_members WHERE link_id=$1 AND user_id=$2",
                    link_id, user.id,
                )
                if not (already and already["joined_counted"]):
                    from services.gender import guess_gender
                    gender = guess_gender(user.first_name or "")
                    males_inc   = 1 if gender == "M" else 0
                    females_inc = 1 if gender == "F" else 0

                    if country_code:
                        await db.execute(
                            """
                            UPDATE invite_links SET
                                joined           = joined + 1,
                                males            = males + $2::int,
                                females          = females + $3::int,
                                rtl_count        = rtl_count + $4::int,
                                hieroglyph_count = hieroglyph_count + $5::int,
                                premium_count    = premium_count + $6::int,
                                countries        = jsonb_set(
                                    COALESCE(countries, '{}'),
                                    ARRAY[$7],
                                    (COALESCE(countries->$7, '0')::int + 1)::text::jsonb
                                )
                            WHERE id = $1
                            """,
                            link_id,
                            males_inc, females_inc,
                            int(has_rtl), int(has_hieroglyph), int(is_premium),
                            country_code,
                        )
                    else:
                        await db.execute(
                            """
                            UPDATE invite_links SET
                                joined           = joined + 1,
                                males            = males + $2::int,
                                females          = females + $3::int,
                                rtl_count        = rtl_count + $4::int,
                                hieroglyph_count = hieroglyph_count + $5::int,
                                premium_count    = premium_count + $6::int
                            WHERE id = $1
                            """,
                            link_id,
                            males_inc, females_inc,
                            int(has_rtl), int(has_hieroglyph), int(is_premium),
                        )

                    await db.execute(
                        """
                        INSERT INTO invite_link_members (link_id, user_id, joined_counted)
                        VALUES ($1, $2, true)
                        ON CONFLICT (link_id, user_id) DO UPDATE SET joined_counted = true
                        """,
                        link_id, user.id,
                    )
                    logger.info(f"[LINK] Tracked join via link_id={link_id} user={user.id} gender={gender}")
                else:
                    logger.info(f"[LINK] User {user.id} already counted for link_id={link_id}, skipping")

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
        # 2. Капча пройдена (через ChatJoinRequest или групповой one_time_link)
        #    флаг _passed_captcha_group снимается, и отправляем полный welcome
        welcome_text = chat_settings.get("welcome_text")
        welcome_media = chat_settings.get("welcome_media")
        
        try:
            from handlers.captcha import cleanup_captcha_and_send_welcome
            await cleanup_captcha_and_send_welcome(bot, chat_id, user.id)
        except Exception as _ce:
            logger.debug(f"[CAPTCHA CLEANUP] Failed: {_ce}")

        logger.info(f"[WELCOME] user={user.id} captcha={captcha_type} has_text={bool(welcome_text)} has_media={bool(welcome_media)}")
        if welcome_text or welcome_media:
            from handlers.join_requests import _send_welcome
            from handlers.captcha import _passed_captcha_group
            captcha_key = (chat_id, user.id)
            passed_group_captcha = captcha_key in _passed_captcha_group
            if passed_group_captcha:
                # Потребляем флаг сразу, чтобы не отправить приветствие дважды
                _passed_captcha_group.discard(captcha_key)
                logger.info(f"[WELCOME] captcha passed — sending welcome to user={user.id}")
                await _send_welcome(bot, chat_id, user, dict(chat_settings))
            elif captcha_type == "off":
                await _send_welcome(bot, chat_id, user, dict(chat_settings))
    # ── Пользователь вышел/забанен ────────────────────────────
    elif new_status in ("left", "kicked") and old_status == "member":
        key = (chat_id, user.id)

        # Если это мы сами кикнули пользователя для капчи — пропускаем 'left'-событие
        from handlers.captcha import _kicked_for_captcha
        if key in _kicked_for_captcha:
            _kicked_for_captcha.discard(key)
            logger.info(f"[CAPTCHA KICK] Skip left-event for captcha-kicked user={user.id} chat={chat_id}")
            return

        # Обновляем счётчик отписок для ссылки (только если этот пользователь был учтён в joined)
        await db.execute(
            """
            UPDATE invite_links SET unsubscribed = unsubscribed + 1
            WHERE id = (
                SELECT ilm.link_id FROM invite_link_members ilm
                JOIN bot_users bu ON bu.joined_via_link_id = ilm.link_id
                WHERE bu.owner_id=$1 AND bu.chat_id=$2 AND bu.user_id=$3
                  AND ilm.user_id=$3 AND ilm.joined_counted = true
                LIMIT 1
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
        farewell_text = chat_settings.get("farewell_text")
        farewell_media = chat_settings.get("farewell_media")
        
        logger.info(f"[FAREWELL] user={user.id} has_text={bool(farewell_text)} has_media={bool(farewell_media)}")
        if farewell_text or farewell_media:
            from handlers.join_requests import _send_farewell
            await _send_farewell(bot, chat_id, user, dict(chat_settings))
