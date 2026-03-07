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


# ── Парсинг url_buttons_raw ──────────────────────────────────

def _parse_buttons(raw: str, color: str = "blue") -> InlineKeyboardMarkup | None:
    """
    Парсит сырой текст кнопок в InlineKeyboardMarkup.

    Форматы:
      Текст — ссылка                  → одна кнопка в ряду
      Текст 1 — ссылка | Текст 2 — ссылка  → два в ряду
      Текст — ссылка (webapp)         → WebApp-кнопка
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
            # Проверяем формат "Текст — ссылка" (поддерживаем — и -)
            match = re.match(r"^(.+?)\s*[—\-]{1,2}\s*(https?://\S+?)(\s+\(webapp\))?$", chunk)
            if match:
                text = match.group(1).strip()
                url  = match.group(2).strip()
                is_webapp = bool(match.group(3))
                if is_webapp:
                    btn = InlineKeyboardButton(text=text, web_app=WebAppInfo(url=url))
                else:
                    btn = InlineKeyboardButton(text=text, url=url)
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
    Запускает рассылку для mailing_id через основной Bumblebee бот.
    progress_callback(mailing_id, sent, total, errors, status) вызывается
    каждые 5 сек и по завершении для обновления экрана прогресса.
    """
    import time
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
            await _send_message(bot, rec["user_id"], mailing, kb, user_dict)
            sent += 1
            await db.execute("UPDATE mailings SET sent_count=$1 WHERE id=$2", sent, mailing_id)
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
                await _send_message(bot, rec["user_id"], mailing, kb, user_dict)
                sent += 1
            except Exception:
                errors += 1
        except Exception as e:
            errors += 1
            logger.debug(f"Mailing {mailing_id}: failed to {rec['user_id']}: {e}")

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
):
    """Отправляет одно сообщение рассылки пользователю."""
    raw_text   = mailing.get("text") or ""
    media_id   = mailing.get("media_file_id")
    media_type = mailing.get("media_type")

    # Подстановка переменных
    text = _substitute_vars(raw_text, user_dict) if raw_text else ""

    protect  = bool(mailing.get("protect_content", False))
    notify   = bool(mailing.get("notify_users", True))
    no_prev  = bool(mailing.get("disable_preview", False))

    common = dict(
        reply_markup=kb,
        protect_content=protect,
        disable_notification=not notify,
    )

    if media_type == "photo":
        await bot.send_photo(
            user_id, media_id,
            caption=text, parse_mode="HTML",
            has_spoiler=False, **common,
        )
    elif media_type == "video":
        await bot.send_video(
            user_id, media_id,
            caption=text, parse_mode="HTML", **common,
        )
    elif media_type == "document":
        await bot.send_document(
            user_id, media_id,
            caption=text, parse_mode="HTML", **common,
        )
    else:
        from aiogram.types import LinkPreviewOptions
        await bot.send_message(
            user_id, text,
            parse_mode="HTML",
            link_preview_options=LinkPreviewOptions(is_disabled=no_prev),
            **common,
        )


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
