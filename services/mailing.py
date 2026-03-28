"""
services/mailing.py — Рассылка сообщений с rate limiting, паузой,
поддержкой URL-кнопок (парсинг url_buttons_raw), protect_content,
disable_preview, notify_users.
"""
import asyncio
import logging
import re
from aiogram import Bot
from aiogram.exceptions import TelegramForbiddenError, TelegramRetryAfter
from aiogram.types import (
    InlineKeyboardMarkup, InlineKeyboardButton, WebAppInfo
)

import db.pool as db

logger = logging.getLogger(__name__)

# {mailing_id: {"paused": bool, "cancelled": bool, "speed": str}}
_active_mailings: dict[int, dict] = {}

SPEEDS = {
    "low":    0.08,   # ~12 msg/s
    "medium": 0.04,   # ~25 msg/s
    "high":   0.02,   # ~50 msg/s
}


# Маппинг цвет-emoji → style (Bot API 9.4)
_EMOJI_STYLE_MAP = {
    "🟦": "primary",   # синяя
    "🟩": "success",   # зелёная
    "🟥": "danger",    # красная
}

def _parse_buttons(raw: str, color: str = "blue") -> InlineKeyboardMarkup | None:
    """
    Парсит сырой текст кнопок в InlineKeyboardMarkup.

    Форматы:
      Текст — ссылка                        → одна кнопка в ряду (стиль по умолчанию)
      🟦 Текст — ссылка                     → синяя кнопка (primary)
      🟩 Текст — ссылка                     → зелёная кнопка (success)
      🟥 Текст — ссылка                     → красная кнопка (danger)
      Текст 1 — ссылка | Текст 2 — ссылка  → два в ряду
      Текст — ссылка (webapp)               → WebApp-кнопка
    """
    if not raw or not raw.strip():
        return None

    rows = []
    for line in raw.strip().splitlines():
        line = line.strip()
        if not line:
            continue
        row = []
        for chunk in line.split("|"):
            chunk = chunk.strip()

            # Определяем цвет по ведущему emoji
            btn_style = None
            for emoji, style in _EMOJI_STYLE_MAP.items():
                if chunk.startswith(emoji):
                    btn_style = style
                    chunk = chunk[len(emoji):].strip()
                    break

            # Проверяем формат "Текст — ссылка" (поддерживаем — и -)
            match = re.match(r"^(.+?)\s*[—\-]{1,2}\s*(https?://\S+?)(\s+\(webapp\))?$", chunk)
            if match:
                text = match.group(1).strip()
                url  = match.group(2).strip()
                is_webapp = bool(match.group(3))
                if is_webapp:
                    btn = InlineKeyboardButton(text=text, web_app=WebAppInfo(url=url))
                else:
                    kwargs = dict(text=text, url=url)
                    if btn_style:
                        kwargs["style"] = btn_style
                    btn = InlineKeyboardButton(**kwargs)
                row.append(btn)
        if row:
            rows.append(row)

    return InlineKeyboardMarkup(inline_keyboard=rows) if rows else None


def _substitute_vars(text: str, user: dict) -> str:
    """Подставляет переменные {name}, {allname}, {username}, {chat}, {day}."""
    from datetime import datetime, timezone
    now = datetime.now(timezone.utc)
    fname    = user.get("first_name") or ""
    lname    = user.get("last_name") or ""
    username = user.get("username") or ""
    channel  = user.get("_chat_title") or ""
    text = text.replace("{name}",    fname)
    text = text.replace("{allname}", f"{fname} {lname}".strip())
    text = text.replace("{username}", f"@{username}" if username else fname)
    text = text.replace("{chat}",    channel)
    text = text.replace("{day}",     now.strftime("%d.%m.%Y"))
    return text


# ── Основной цикл рассылки ───────────────────────────────────

async def run_mailing(mailing_id: int, bot: Bot,
                      progress_callback=None):
    """
    Запускает рассылку для mailing_id через дочерний бот (child bot).
    Fallback на основной бот если токен дочернего не найден.
    progress_callback(mailing_id, sent, total, errors, status) вызывается
    каждые 5 сек и по завершении для обновления экрана прогресса.
    """
    import time
    from services.security import decrypt_token
    mailing = await db.fetchrow("SELECT * FROM mailings WHERE id=$1", mailing_id)
    if not mailing or mailing["status"] != "pending":
        return

    owner_id     = mailing["owner_id"]
    chat_id      = mailing["chat_id"]       # None → bot-level
    child_bot_id = mailing["child_bot_id"]

    logger.info(
        f"[MAILING {mailing_id}] owner={owner_id} chat_id={chat_id} "
        f"child_bot_id={child_bot_id} status={mailing['status']}"
    )

    # ── Если child_bot_id не сохранён — пробуем достать по chat_id ─
    if not child_bot_id and chat_id:
        row = await db.fetchrow(
            "SELECT child_bot_id FROM bot_chats WHERE owner_id=$1 AND chat_id=$2::bigint",
            owner_id, chat_id,
        )
        if row:
            child_bot_id = row["child_bot_id"]
        logger.info(f"[MAILING {mailing_id}] resolved child_bot_id={child_bot_id} from chat_id")

    # ── Проверка Soft-Lock (тариф и заморозка бота) ─────────────
    user = await db.fetchrow("SELECT tariff FROM platform_users WHERE user_id=$1", owner_id)
    tariff = user["tariff"] if user else "free"
    from config import TARIFFS
    t_info = TARIFFS.get(tariff, TARIFFS["free"])

    if not t_info["features"]["mailings"]:
        logger.info(f"[MAILING {mailing_id}] cancelled: mailings disabled for {tariff}")
        await db.execute("UPDATE mailings SET status='cancelled' WHERE id=$1", mailing_id)
        return

    if child_bot_id:
        rn = await db.fetchval(
            """
            SELECT rn FROM (
                SELECT id, ROW_NUMBER() OVER(PARTITION BY owner_id ORDER BY id ASC) as rn
                FROM child_bots WHERE owner_id=$1
            ) t WHERE id=$2
            """,
            owner_id, child_bot_id
        )
        if rn and rn > t_info["max_bots"]:
            logger.info(f"[MAILING {mailing_id}] cancelled: child_bot_id={child_bot_id} is frozen (limit {t_info['max_bots']})")
            await db.execute("UPDATE mailings SET status='cancelled' WHERE id=$1", mailing_id)
            return

    # ── Получаем токен дочернего бота для отправки ──────────────
    send_bot = bot   # fallback на основной
    child_bot_instance = None
    if child_bot_id:
        bot_row = await db.fetchrow(
            "SELECT token_encrypted FROM child_bots WHERE id=$1",
            child_bot_id,
        )
        if bot_row and bot_row.get("token_encrypted"):
            try:
                child_bot_instance = Bot(token=decrypt_token(bot_row["token_encrypted"]))
                send_bot = child_bot_instance
                logger.info(f"[MAILING {mailing_id}] using child bot id={child_bot_id} for sending")
            except Exception as e:
                logger.warning(f"[MAILING {mailing_id}] failed to init child bot: {e}, falling back to main bot")


    # ── Получатели ──────────────────────────────────────────────
    if chat_id:
        ch_row = await db.fetchrow(
            "SELECT chat_title FROM bot_chats WHERE owner_id=$1 AND chat_id=$2::bigint",
            owner_id, chat_id,
        )
        chat_title = ch_row["chat_title"] if ch_row else ""

        recipients = await db.fetch(
            """SELECT bu.user_id, bu.username, bu.first_name
               FROM bot_users bu
               WHERE bu.owner_id=$1 AND bu.chat_id=$2
                 AND bu.user_id IS NOT NULL
               ORDER BY bu.joined_at""",
            owner_id, chat_id,
        )
        logger.info(f"[MAILING {mailing_id}] chat-level: found {len(recipients)} recipients for chat {chat_id}")
    elif child_bot_id:
        # Bot-level: уникальные пользователи всех площадок бота
        chat_title = ""
        recipients = await db.fetch(
            """SELECT DISTINCT ON (bu.user_id)
                      bu.user_id, bu.username, bu.first_name
               FROM bot_users bu
               JOIN bot_chats bc ON bu.chat_id = bc.chat_id
                                AND bu.owner_id = bc.owner_id
               WHERE bc.child_bot_id = $1
                 AND bu.owner_id = $2
                 AND bu.user_id IS NOT NULL
               ORDER BY bu.user_id, bu.joined_at""",
            child_bot_id, owner_id,
        )
        logger.info(f"[MAILING {mailing_id}] bot-level: child_bot_id={child_bot_id} found {len(recipients)} recipients")
    else:
        # Нет ни chat_id, ни child_bot_id — рассылаем всем пользователям владельца
        chat_title = ""
        recipients = await db.fetch(
            """SELECT DISTINCT ON (bu.user_id)
                      bu.user_id, bu.username, bu.first_name
               FROM bot_users bu
               WHERE bu.owner_id=$1 AND bu.user_id IS NOT NULL
               ORDER BY bu.user_id, bu.joined_at""",
            owner_id,
        )
        logger.info(f"[MAILING {mailing_id}] owner-level fallback: found {len(recipients)} recipients")

    total = len(recipients)
    await db.execute(
        "UPDATE mailings SET status='running', started_at=now(), total_count=$1 WHERE id=$2",
        total, mailing_id,
    )

    _active_mailings[mailing_id] = {"paused": False, "cancelled": False, "speed": "low"}
    sent = errors = 0
    last_notify_ts = 0.0

    kb = _parse_buttons(
        mailing.get("url_buttons_raw") or "",
        mailing.get("button_color") or "blue",
    )

    # ── Если используем дочернего бота и есть медиа: скачиваем файл ──
    # Telegram file_id привязан к боту — file_id главного бота не работает
    # у дочернего. Решение: скачать байты через главный бот один раз,
    # отправить первому получателю через дочернего (он вернёт свой file_id),
    # затем использовать child_file_id для всех остальных.
    _media_bytes: bytes | None = None
    _child_media_id: str | None = None
    media_file_id = mailing.get("media_file_id")

    if media_file_id and child_bot_instance:
        try:
            file_info = await bot.get_file(media_file_id)
            _bio = await bot.download_file(file_info.file_path)
            _bio.seek(0)
            _media_bytes = _bio.read()
            logger.info(
                f"[MAILING {mailing_id}] media downloaded via main bot, "
                f"size={len(_media_bytes)} bytes"
            )
        except Exception as e:
            logger.warning(
                f"[MAILING {mailing_id}] failed to download media: {e}. "
                f"Mailing will fail for all recipients."
            )

    for rec in recipients:
        control = _active_mailings.get(mailing_id, {})

        if control.get("cancelled"):
            break

        while control.get("paused"):
            await asyncio.sleep(1)
            control = _active_mailings.get(mailing_id, {})
            if control.get("cancelled"):
                break

        user_dict = dict(rec)
        user_dict["_chat_title"] = chat_title

        try:
            sent_file_id = None
            sent_msg_id = None
            if _media_bytes is not None and _child_media_id is None:
                # Первая медиа-отправка: загружаем файл на серверы дочернего бота
                from aiogram.types import BufferedInputFile
                filename = f"media.{mailing.get('media_type', 'bin')}"
                override = BufferedInputFile(_media_bytes, filename=filename)
                sent_file_id, sent_msg_id = await _send_message(
                    send_bot, rec["user_id"], mailing, kb, user_dict,
                    media_override=override,
                )
                if sent_file_id:
                    _child_media_id = sent_file_id
                    logger.info(
                        f"[MAILING {mailing_id}] child_file_id obtained: "
                        f"{sent_file_id[:20]}..."
                    )
            elif _child_media_id is not None:
                # Все последующие: мгновенно через child file_id
                sent_file_id, sent_msg_id = await _send_message(
                    send_bot, rec["user_id"], mailing, kb, user_dict,
                    media_override=_child_media_id,
                )
            else:
                # Текстовая рассылка или fallback на main bot
                sent_file_id, sent_msg_id = await _send_message(send_bot, rec["user_id"], mailing, kb, user_dict)
            sent += 1
            await db.execute("UPDATE mailings SET sent_count=$1 WHERE id=$2", sent, mailing_id)
            # Сохраняем message_id для будущего открепления/удаления
            if sent_msg_id and (mailing.get("pin_message") or mailing.get("delete_after_send")):
                from datetime import timedelta
                _pin_until = None
                if mailing.get("pin_message"):
                    from datetime import datetime, timezone
                    _pin_until = datetime.now(timezone.utc) + timedelta(hours=24)
                try:
                    await db.execute(
                        """INSERT INTO mailing_sent_messages
                           (mailing_id, child_bot_id, tg_user_id, tg_message_id, pin_until, delete_after)
                           VALUES ($1, $2, $3, $4, $5, $6)""",
                        mailing_id,
                        mailing.get("child_bot_id"),
                        rec["user_id"],
                        sent_msg_id,
                        _pin_until,
                        bool(mailing.get("delete_after_send", False)),
                    )
                except Exception as _db_err:
                    logger.debug(f"[MAILING {mailing_id}] msm insert error: {_db_err}")
        except TelegramForbiddenError:
            errors += 1
            await db.execute(
                "UPDATE bot_users SET is_active=false WHERE owner_id=$1 AND user_id=$2",
                owner_id, rec["user_id"],
            )
        except TelegramRetryAfter as e:
            logger.warning(f"Mailing {mailing_id}: rate limit {e.retry_after}s")
            await asyncio.sleep(e.retry_after)
            try:
                # При retry тоже используем child_media_id если уже есть
                override = _child_media_id if _child_media_id else None
                _, retry_msg_id = await _send_message(send_bot, rec["user_id"], mailing, kb, user_dict,
                                    media_override=override)
                sent += 1
                if retry_msg_id and (mailing.get("pin_message") or mailing.get("delete_after_send")):
                    from datetime import timedelta
                    _pin_until = None
                    if mailing.get("pin_message"):
                        from datetime import datetime, timezone
                        _pin_until = datetime.now(timezone.utc) + timedelta(hours=24)
                    try:
                        await db.execute(
                            """INSERT INTO mailing_sent_messages
                               (mailing_id, child_bot_id, tg_user_id, tg_message_id, pin_until, delete_after)
                               VALUES ($1, $2, $3, $4, $5, $6)""",
                            mailing_id,
                            mailing.get("child_bot_id"),
                            rec["user_id"],
                            retry_msg_id,
                            _pin_until,
                            bool(mailing.get("delete_after_send", False)),
                        )
                    except Exception:
                        pass
            except Exception:
                errors += 1
        except Exception as e:
            errors += 1
            logger.warning(f"Mailing {mailing_id}: failed to send to {rec['user_id']}: {e}")

        speed = _active_mailings.get(mailing_id, {}).get("speed", "low")
        await asyncio.sleep(SPEEDS.get(speed, 0.08))

        # Прогресс-колбэк каждые 5 секунд
        now_ts = time.monotonic()
        if progress_callback and (now_ts - last_notify_ts) >= 5:
            last_notify_ts = now_ts
            try:
                await progress_callback(mailing_id, sent, total, errors, "running")
            except Exception as cb_err:
                logger.debug(f"Progress callback error: {cb_err}")

    cancelled = _active_mailings.get(mailing_id, {}).get("cancelled", False)
    final_status = "cancelled" if cancelled else "done"
    await db.execute(
        "UPDATE mailings SET status=$1, finished_at=now(), sent_count=$2, error_count=$3 WHERE id=$4",
        final_status, sent, errors, mailing_id,
    )
    _active_mailings.pop(mailing_id, None)
    logger.info(f"Mailing {mailing_id} done: {sent}/{total}, errors={errors}")

    # Закрываем сессию дочернего бота если использовали его
    if child_bot_instance:
        try:
            await child_bot_instance.session.close()
        except Exception:
            pass

    # Финальный колбэк
    if progress_callback:
        try:
            await progress_callback(mailing_id, sent, total, errors, final_status)
        except Exception as cb_err:
            logger.debug(f"Final progress callback error: {cb_err}")




async def _send_message(
    bot: Bot,
    user_id: int,
    mailing: dict,
    kb: InlineKeyboardMarkup | None,
    user_dict: dict,
    media_override=None,
) -> tuple[str | None, int | None]:
    """
    Отправляет одно сообщение рассылки пользователю.

    media_override — если передан, используется вместо mailing["media_file_id"].
    Может быть BufferedInputFile (первая отправка) или str (child file_id, все последующие).

    Возвращает (file_id медиа, message_id) — file_id для переиспользования,
    message_id для закрепления/удаления через 24 часа. Оба могут быть None.
    """
    raw_text    = mailing.get("text") or ""
    media_type  = mailing.get("media_type")
    media_below = bool(mailing.get("media_below", False))

    # Что отправляем: override имеет приоритет над сохранённым file_id
    actual_media = media_override if media_override is not None else mailing.get("media_file_id")

    # Подстановка переменных
    text = _substitute_vars(raw_text, user_dict) if raw_text else ""

    protect = bool(mailing.get("protect_content", False))
    notify  = bool(mailing.get("notify_users", True))
    no_prev = bool(mailing.get("disable_preview", False))
    do_pin  = bool(mailing.get("pin_message", False))

    common = dict(
        reply_markup=kb,
        protect_content=protect,
        disable_notification=not notify,
    )

    sent_msg = None

    if actual_media and not media_below:
        # ⬆ — текст сверху (show_caption_above_media), только для photo/video
        if media_type == "photo":
            sent_msg = await bot.send_photo(
                user_id, actual_media,
                caption=text or None, parse_mode="HTML",
                show_caption_above_media=True,
                **common,
            )
        elif media_type == "video":
            sent_msg = await bot.send_video(
                user_id, actual_media,
                caption=text or None, parse_mode="HTML",
                show_caption_above_media=True,
                **common,
            )
        elif media_type == "document":
            # document не поддерживает show_caption_above_media — игнорируем флаг
            sent_msg = await bot.send_document(
                user_id, actual_media,
                caption=text or None, parse_mode="HTML",
                **common,
            )

    elif actual_media:
        # ⬇ — стандарт: медиа сверху, текст caption'ом снизу
        if media_type == "photo":
            sent_msg = await bot.send_photo(
                user_id, actual_media,
                caption=text or None, parse_mode="HTML",
                **common,
            )
        elif media_type == "video":
            sent_msg = await bot.send_video(
                user_id, actual_media,
                caption=text or None, parse_mode="HTML",
                **common,
            )
        elif media_type == "document":
            sent_msg = await bot.send_document(
                user_id, actual_media,
                caption=text or None, parse_mode="HTML",
                **common,
            )

    else:
        from aiogram.types import LinkPreviewOptions
        sent_msg = await bot.send_message(
            user_id, text,
            parse_mode="HTML",
            link_preview_options=LinkPreviewOptions(is_disabled=no_prev),
            **common,
        )
        msg_id = sent_msg.message_id if sent_msg else None
        # Закрепить текстовое сообщение если нужно
        if do_pin and msg_id:
            try:
                await bot.pin_chat_message(user_id, msg_id, disable_notification=True)
            except Exception:
                pass
        return None, msg_id

    msg_id = sent_msg.message_id if sent_msg else None

    # Закрепить медиа-сообщение если нужно
    if do_pin and msg_id:
        try:
            await bot.pin_chat_message(user_id, msg_id, disable_notification=True)
        except Exception:
            pass

    # Возвращаем (file_id, message_id) из ответа дочернего бота
    if sent_msg:
        if media_type == "photo" and sent_msg.photo:
            return sent_msg.photo[-1].file_id, msg_id
        if media_type == "video" and sent_msg.video:
            return sent_msg.video.file_id, msg_id
        if media_type == "document" and sent_msg.document:
            return sent_msg.document.file_id, msg_id
    return None, msg_id




# ── Runtime controls ─────────────────────────────────────────

def pause_mailing(mailing_id: int):
    if mailing_id in _active_mailings:
        _active_mailings[mailing_id]["paused"] = True


def resume_mailing(mailing_id: int):
    if mailing_id in _active_mailings:
        _active_mailings[mailing_id]["paused"] = False


def cancel_mailing(mailing_id: int):
    if mailing_id in _active_mailings:
        _active_mailings[mailing_id]["cancelled"] = True


def set_speed(mailing_id: int, speed: str):
    if mailing_id in _active_mailings and speed in SPEEDS:
        _active_mailings[mailing_id]["speed"] = speed


def get_status(mailing_id: int) -> dict | None:
    return _active_mailings.get(mailing_id)
