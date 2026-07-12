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

logger = logging.getLogger(__name__)
router = Router()


@router.message(Command("emojiid"))
async def cmd_emojiid(message: Message):
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
            "Пришлите <b>одним сообщением</b>:\n"
            "<code>/emojiid</code> и сразу премиум-эмодзи из паков "
            "(можно много подряд).\n\n"
            "Я отвечу списком «эмодзи → id» — это и есть custom_emoji_id для словаря.\n\n"
            f"<i>Ваш ID: <code>{uid}</code></i>"
        )
        return

    lines = [f"{ch}  →  <code>{cid}</code>" for ch, cid in found]
    await message.reply(f"Собрал {len(found)} шт.:\n\n" + "\n".join(lines))
