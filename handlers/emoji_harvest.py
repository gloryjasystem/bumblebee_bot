"""
handlers/emoji_harvest.py — ВРЕМЕННЫЙ сборщик custom_emoji_id.

Команда /emojiid: пришли её одним сообщением вместе с премиум-эмодзи из паков —
бот ответит списком «эмодзи → custom_emoji_id» для словаря замен.

Не требует прав владельца: читает entities входящего сообщения (custom_emoji_id
универсален и одинаков для всех — тот же, что отрисуется в проде). Ничего в
продукте не меняет. Роутер подключается ДО catch-all в messages_router.

УДАЛИТЬ после сбора ID: убрать этот файл и его include в bot.py.
"""
import logging
from aiogram import Router
from aiogram.types import Message
from aiogram.filters import Command

from config import settings

logger = logging.getLogger(__name__)
router = Router()


@router.message(Command("emojiid"))
async def cmd_emojiid(message: Message):
    # Только владелец/совладелец: у владельца есть Premium — им и добирать id.
    uid = message.from_user.id if message.from_user else None
    if uid != settings.owner_telegram_id and not (
        settings.co_owner_telegram_id is not None and uid == settings.co_owner_telegram_id
    ):
        return
    text = message.text or ""
    found = []
    for e in (message.entities or []):
        if e.type == "custom_emoji":
            try:
                ch = e.extract_from(text)
            except Exception:
                try:
                    u16 = text.encode("utf-16-le")
                    ch = u16[e.offset * 2:(e.offset + e.length) * 2].decode("utf-16-le")
                except Exception:
                    ch = "?"
            found.append((ch, e.custom_emoji_id))

    if not found:
        uid = message.from_user.id if message.from_user else "?"
        await message.reply(
            "Я получил сообщение, но <b>премиум-эмодзи в нём нет</b> — пришли обычные символы.\n\n"
            "Так бывает по двум причинам:\n"
            "1) аккаунт <b>без Telegram Premium</b> — тогда премиум-иконки при вставке "
            "заменяются на обычные;\n"
            "2) эмодзи <b>скопированы-вставлены</b> или взяты со страницы пака, "
            "а не выбраны из эмодзи-клавиатуры.\n\n"
            "Как надо: набери <code>/emojiid</code>, открой эмодзи-клавиатуру (значок у поля ввода), "
            "найди добавленный пак и <b>тапни иконки</b> — они встанут в это же поле. Потом отправь.\n\n"
            f"<i>Ваш ID: <code>{uid}</code></i>"
        )
        return

    # Пишем в лог, чтобы забрать ID с сервера точь-в-точь (без копипаста цифр).
    for ch, cid in found:
        logger.info(f"[EMOJI_HARVEST] {ch} {cid}")
    logger.info(f"[EMOJI_HARVEST] total={len(found)}")

    lines = [f"{ch}  →  <code>{cid}</code>" for ch, cid in found]
    await message.reply(f"Собрал {len(found)} шт. — записал на сервере ✅\n\n" + "\n".join(lines))
