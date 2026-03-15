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
from aiogram.filters import StateFilter
from aiogram.types import (
    CallbackQuery, ChatJoinRequest,
    InlineKeyboardMarkup, InlineKeyboardButton,
    ReplyKeyboardMarkup, KeyboardButton, ReplyKeyboardRemove, Message,
    BufferedInputFile,
)
from io import BytesIO

import db.pool as db
from config import settings

logger = logging.getLogger(__name__)
router = Router()

# Cache for child bot file IDs mapping: (original_file_id, bot_id) -> new_file_id
_child_file_ids_cache = {}


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


# Маппинг цвет-emoji → style (Bot API 9.4)
_CAPTCHA_EMOJI_STYLE_MAP = {
    "🟦": "primary",   # синяя
    "🟩": "success",   # зелёная
    "🟥": "danger",    # красная
}


def _parse_captcha_buttons(raw: str) -> list[tuple[str, str | None]]:
    """Парсит captcha_buttons_raw в список (текст, style|None).
    Поддерживает разделитель новая строка и запятая.
    Если строка начинается с цветного эмодзи (🟩/🟦/🟥) — возвращает соответствующий style,
    эмодзи убирается из текста кнопки (цвет применяется через Bot API 9.4).
    """
    if not raw:
        return []
    parts = []
    for line in raw.splitlines():
        line = line.strip()
        if not line:
            continue
        for btn in line.split(","):
            btn = btn.strip()
            if not btn:
                continue
            # Определяем цвет по ведущему emoji
            btn_style = None
            for emoji, style in _CAPTCHA_EMOJI_STYLE_MAP.items():
                if btn.startswith(emoji):
                    btn_style = style
                    btn = btn[len(emoji):].strip()
                    break
            parts.append((btn, btn_style))
    return parts[:10]  # не больше 10 кнопок

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
        raw_caption = (
            settings_row.get("captcha_text")
            or f"👋 Привет, <b>{{name}}</b>!\n\n"
               f"Прежде чем войти в <b>{{chat}}</b>,\n"
               f"докажи что ты не робот — нажми кнопку ниже ✅\n\n"
               f"⏱ У тебя {timer_min} мин."
        )
        caption = _fill_captcha_text(raw_caption, user, event.chat.title)
        # Применяем пользовательские кнопки если заданы, иначе — кнопка по умолчанию
        custom_btns = _parse_captcha_buttons(settings_row.get("captcha_buttons_raw") or "")
        btn_style_placement = settings_row.get("captcha_button_style") or "inline"

        if btn_style_placement == "reply":
            # Reply-клавиатура: кнопки в панели ввода
            if custom_btns:
                btn_texts = [label for label, _ in custom_btns]
            else:
                btn_texts = ["✅ Я не робот"]
            kb = ReplyKeyboardMarkup(
                keyboard=[[KeyboardButton(text=t)] for t in btn_texts],
                resize_keyboard=True,
                one_time_keyboard=True,
            )
        else:
            if custom_btns:
                kb = InlineKeyboardMarkup(inline_keyboard=[
                    [InlineKeyboardButton(
                        text=btn_label,
                        callback_data=f"captcha_ok:{event.chat.id}:{user.id}",
                        **({"style": btn_style} if btn_style else {}),
                    )] for btn_label, btn_style in custom_btns
                ])
            else:
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

        raw_caption = (
            settings_row.get("captcha_text")
            or f"👋 Привет, <b>{{name}}</b>!\n\n"
               f"Чтобы войти в <b>{{chat}}</b>, нажми на: <b>{correct}</b>\n\n"
               f"⏱ У тебя {timer_min} мин."
        )
        caption = _fill_captcha_text(raw_caption, user, event.chat.title)
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

    captcha_media = settings_row.get("captcha_media")
    anim_file_id = settings_row.get("captcha_anim_file_id")
    anim_type = settings_row.get("captcha_anim_type")
    try:
        msg = None
        if anim_file_id:
            actual_file_id = _child_file_ids_cache.get((anim_file_id, bot.id), anim_file_id)
            try:
                if anim_type == "video":
                    msg = await bot.send_video(
                        user.id,
                        actual_file_id,
                        caption=caption,
                        parse_mode="HTML",
                        reply_markup=kb,
                    )
                else:
                    msg = await bot.send_animation(
                        user.id,
                        actual_file_id,
                        caption=caption,
                        parse_mode="HTML",
                        reply_markup=kb,
                    )
            except Exception as e:
                error_msg = str(e).lower()
                if "wrong file identifier" in error_msg or "file reference" in error_msg or "invalid file" in error_msg:
                    logger.info(f"Re-uploading animation {anim_file_id} for bot {bot.id}")
                    try:
                        main_bot = Bot(token=settings.bot_token)
                        file_info = await main_bot.get_file(anim_file_id)
                        file_bytes = await main_bot.download_file(file_info.file_path)
                        await main_bot.session.close()

                        input_file = BufferedInputFile(file_bytes.read(), filename="anim.mp4")

                        if anim_type == "video":
                            msg = await bot.send_video(user.id, input_file, caption=caption, parse_mode="HTML", reply_markup=kb)
                            _child_file_ids_cache[(anim_file_id, bot.id)] = msg.video.file_id
                        else:
                            msg = await bot.send_animation(user.id, input_file, caption=caption, parse_mode="HTML", reply_markup=kb)
                            _child_file_ids_cache[(anim_file_id, bot.id)] = msg.animation.file_id
                    except Exception as inner_e:
                        logger.error(f"[CAPTCHA REUPLOAD ERR] {inner_e}")
                else:
                    logger.error(f"[CAPTCHA SEND ERR] {e}")

        # Fallback to standard message or photo if animation fails or was not provided
        if not msg:
            if captcha_media:
                msg = await bot.send_photo(
                    user.id,
                    captcha_media,
                    caption=caption,
                    parse_mode="HTML",
                    reply_markup=kb,
                )
            else:
                msg = await bot.send_message(
                    user.id,
                    caption,
                    parse_mode="HTML",
                    reply_markup=kb,
                )
        asyncio.create_task(
            _captcha_timeout(bot, event, settings_row, msg.message_id)
        )
    except Exception as e:
        logger.error(f"[CAPTCHA SEND ERROR] Could not send to {user.id}: {e}")
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
               f"Прежде чем войти в <b>{{chat}}</b>,\n"
               f"докажи что ты не робот — нажми кнопку ниже ✅\n\n"
               f"⏱ У тебя {timer_min} мин."
        )
        text = _fill_captcha_text(raw_text, user, chat_title)

        custom_btns = _parse_captcha_buttons(settings_row.get("captcha_buttons_raw") or "")
        btn_style_placement = settings_row.get("captcha_button_style") or "inline"

        if btn_style_placement == "reply":
            if custom_btns:
                btn_texts = [label for label, _ in custom_btns]
            else:
                btn_texts = ["✅ Я не робот"]
            kb = ReplyKeyboardMarkup(
                keyboard=[[KeyboardButton(text=t)] for t in btn_texts],
                resize_keyboard=True,
                one_time_keyboard=True,
            )
        else:
            if custom_btns:
                kb = InlineKeyboardMarkup(inline_keyboard=[
                    [InlineKeyboardButton(
                        text=btn_label,
                        callback_data=f"captcha_ok:{chat_id}:{user.id}",
                    )] for btn_label, _ in custom_btns
                ])
            else:
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
        # Рандомная капча всегда inline (нужно знать какие эмодзи выбраны)
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
    anim_file_id = settings_row.get("captcha_anim_file_id")
    anim_type = settings_row.get("captcha_anim_type")
    try:
        if anim_file_id:
            if anim_type == "video":
                msg = await bot.send_video(user.id, video=anim_file_id, caption=text, parse_mode="HTML", reply_markup=kb)
            else:
                msg = await bot.send_animation(user.id, animation=anim_file_id, caption=text, parse_mode="HTML", reply_markup=kb)
        elif media_id:
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
    """Простая капча — любая кнопка засчитывается (inline-режим)."""
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


@router.message(F.chat.type == "private", StateFilter(None))
async def on_captcha_reply_message(message: Message, bot: Bot):
    """Reply-капча — пользователь нажал кнопку Reply-клавиатуры в личке.
    Ищем активный pending для данного пользователя. Этот хендлер срабатывает
    только если у пользователя есть незакрытая капча (captcha_button_style=reply).
    """
    user_id = message.from_user.id

    # Ищем pending-заявку (join_request режим)
    for key, event in list(_pending.items()):
        chat_id, uid = key
        if uid == user_id:
            await _approve_user_from_message(message, bot, chat_id, user_id, success=True)
            return

    # Ищем pending-заявку (групповой режим)
    for key in list(_pending_group.keys()):
        chat_id, uid = key
        if uid == user_id:
            await _approve_user_from_message(message, bot, chat_id, user_id, success=True)
            return


# ══════════════════════════════════════════════════════════════
# Вспомогательные функции
# ══════════════════════════════════════════════════════════════

async def _approve_user(
    callback: CallbackQuery, bot: Bot,
    chat_id: int, user_id: int, success: bool,
):
    """Обработка результата капчи при нажатии inline-кнопки (CallbackQuery)."""
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
        _passed_captcha_group.add(key)
        try:
            await event.approve()
        except Exception as e:
            _passed_captcha_group.discard(key)
            logger.warning(f"Approve failed: {e}")
            await callback.answer("❌ Не удалось одобрить заявку", show_alert=True)
            return

        settings_row = await db.fetchrow(
            "SELECT * FROM bot_chats WHERE chat_id=$1::bigint AND is_active=true", chat_id
        )
        if settings_row:
            from handlers.join_requests import _register_user, _send_welcome
            await _register_user(settings_row["owner_id"], chat_id, callback.from_user)

            try:
                await db.execute(
                    "INSERT INTO captcha_events (owner_id, chat_id, user_id, passed) VALUES ($1,$2,$3,true)",
                    settings_row["owner_id"], chat_id, callback.from_user.id,
                )
            except Exception as ex:
                logger.debug(f"captcha_events insert failed: {ex}")

            inv_url = event.invite_link.invite_link if getattr(event, "invite_link", None) and event.invite_link else None
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

            await _send_welcome(bot, chat_id, callback.from_user, settings_row)

            if settings_row.get("captcha_delete"):
                try:
                    await callback.message.delete()
                except Exception:
                    pass
                await callback.answer("✅ Отлично!")
                return

        await callback.message.edit_text("✅ Капча пройдена! Добро пожаловать.")
        await callback.answer("✅ Отлично!")

    else:
        try:
            await event.decline()
        except Exception:
            pass
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


async def _approve_user_from_message(
    message: Message, bot: Bot,
    chat_id: int, user_id: int, success: bool,
):
    """Обработка результата капчи при нажатии Reply-кнопки (Message)."""
    key   = (chat_id, user_id)
    event = _pending.pop(key, None)

    # Убираем Reply-клавиатуру
    try:
        await bot.send_message(
            user_id,
            "✅ Капча пройдена! Добро пожаловать.",
            reply_markup=ReplyKeyboardRemove(),
        )
    except Exception:
        pass

    if not event:
        # ── Групповой режим ──
        group_data = _pending_group.pop(key, None)
        _expected.pop(key, None)
        if group_data:
            owner_id = group_data.get("owner_id")
            _passed_captcha_group.add(key)
            if owner_id:
                try:
                    await db.execute(
                        "INSERT INTO captcha_events (owner_id, chat_id, user_id, passed) VALUES ($1,$2,$3,true)",
                        owner_id, chat_id, user_id,
                    )
                except Exception as ex:
                    logger.debug(f"captcha_events insert failed (group reply): {ex}")
            one_time_link = group_data.get("one_time_link")
            if one_time_link:
                await bot.send_message(
                    user_id,
                    f'🔗 <a href="{one_time_link}">Войти в группу</a>\n\nСсылка одноразовая.',
                    parse_mode="HTML",
                )
            logger.info(f"[GROUP CAPTCHA REPLY] Passed: user={user_id} chat={chat_id}")
        else:
            logger.debug(f"[CAPTCHA REPLY] No pending for user={user_id} — ignored")
        return

    # ── Join-request режим ──
    _passed_captcha_group.add(key)
    try:
        await event.approve()
    except Exception as e:
        _passed_captcha_group.discard(key)
        logger.warning(f"[CAPTCHA REPLY] Approve failed: {e}")
        return

    settings_row = await db.fetchrow(
        "SELECT * FROM bot_chats WHERE chat_id=$1::bigint AND is_active=true", chat_id
    )
    if settings_row:
        from handlers.join_requests import _register_user, _send_welcome
        await _register_user(settings_row["owner_id"], chat_id, message.from_user)

        try:
            await db.execute(
                "INSERT INTO captcha_events (owner_id, chat_id, user_id, passed) VALUES ($1,$2,$3,true)",
                settings_row["owner_id"], chat_id, message.from_user.id,
            )
        except Exception as ex:
            logger.debug(f"captcha_events insert failed (reply): {ex}")

        inv_url = event.invite_link.invite_link if getattr(event, "invite_link", None) and event.invite_link else None
        if not inv_url:
            inv_url = _pending_link_urls.pop(key, None)
        if inv_url:
            try:
                from scheduler.child_bot_runner import _track_invite_link
                await _track_invite_link(inv_url, message.from_user)
            except Exception as e:
                logger.warning(f"[LINK TRACK REPLY] failed: {e}")

        await _send_welcome(bot, chat_id, message.from_user, settings_row)

    logger.info(f"[CAPTCHA REPLY] Passed join_request: user={user_id} chat={chat_id}")



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
