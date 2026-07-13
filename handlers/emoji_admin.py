"""
handlers/emoji_admin.py — owner-only инструменты премиум-эмодзи (custom_emoji).

  /emoji_check — валидирует ВСЕ custom_emoji_id из карты против Telegram
                 (getCustomEmojiStickers): показывает невалидные id и несовпадения
                 картинки (базовый глиф стикера ≠ ключ карты). Обязательный шаг
                 перед раскаткой иконок на всех — битый id уронил бы иконки на экране.
  /emoji_off   — МГНОВЕННО гасит премиум-иконки в текущем процессе
                 (settings.custom_emoji_enabled=False; middleware читает флаг на каждом
                 запросе). Аварийный выключатель без редеплоя.
  /emoji_on    — вернуть иконки в процессе.

Только владелец / совладелец. Регистрируется в bot.py ДО catch-all роутеров.
Диагностические ответы шлём parse_mode=None — чтобы их НЕ трогала сама emoji-мидлварь.
"""
import logging

from aiogram import Router, Bot
from aiogram.types import Message
from aiogram.filters import Command

from config import settings
from utils.custom_emoji_map import CUSTOM_EMOJI

logger = logging.getLogger(__name__)
router = Router()


def _is_owner(message: Message) -> bool:
    uid = message.from_user.id if message.from_user else None
    if uid is None:
        return False
    return uid == settings.owner_telegram_id or (
        settings.co_owner_telegram_id is not None
        and uid == settings.co_owner_telegram_id
    )


@router.message(Command("emoji_check"))
async def cmd_emoji_check(message: Message, bot: Bot):
    if not _is_owner(message):
        return

    # id → список emoji-ключей (у одного id может быть несколько ключей: VS16/синонимы)
    id_to_keys: dict[str, list[str]] = {}
    for ch, cid in CUSTOM_EMOJI.items():
        id_to_keys.setdefault(cid, []).append(ch)
    unique_ids = list(id_to_keys.keys())  # ≤200 за раз; у нас ~56 → один вызов

    try:
        stickers = await bot.get_custom_emoji_stickers(custom_emoji_ids=unique_ids)
    except Exception as e:
        logger.warning(f"[EMOJI_CHECK] api error: {e}")
        await message.reply(f"Ошибка getCustomEmojiStickers: {e}", parse_mode=None)
        return

    returned = {s.custom_emoji_id: s for s in stickers}
    invalid = [cid for cid in unique_ids if cid not in returned]

    # Несовпадение картинки: базовый emoji стикера не совпадает ни с одним ключом id.
    # Сравниваем по «голому» глифу (без VS16 U+FE0F), т.к. ключи бывают с/без него.
    mismatches: list[tuple[str, list[str], str]] = []
    for cid, s in returned.items():
        keys = id_to_keys.get(cid, [])
        base_keys = {k.replace("️", "") for k in keys}
        s_emoji = (getattr(s, "emoji", "") or "").replace("️", "")
        if s_emoji and s_emoji not in base_keys:
            mismatches.append((cid, keys, getattr(s, "emoji", "") or "?"))

    lines = [f"valid {len(returned)}/{len(unique_ids)}"]
    if invalid:
        lines.append(f"\n❌ невалидные ({len(invalid)}):")
        for cid in invalid:
            lines.append(f"  {' '.join(id_to_keys.get(cid, []))} -> {cid}")
    if mismatches:
        lines.append(f"\n⚠ несовпадение картинки ({len(mismatches)}):")
        for cid, keys, semoji in mismatches:
            lines.append(f"  {' '.join(keys)} -> {cid} (стикер: {semoji})")
    if not invalid and not mismatches:
        lines.append("\n✅ все id валидны, картинки совпадают — можно раскатывать")

    # Дублируем в лог (переживает флейки Telegram-ответа)
    logger.info(
        f"[EMOJI_CHECK] valid={len(returned)}/{len(unique_ids)} "
        f"invalid={len(invalid)} mismatch={len(mismatches)}"
    )
    for cid in invalid:
        logger.warning(f"[EMOJI_CHECK] INVALID {' '.join(id_to_keys.get(cid, []))} {cid}")
    for cid, keys, semoji in mismatches:
        logger.warning(f"[EMOJI_CHECK] MISMATCH {' '.join(keys)} {cid} sticker={semoji}")

    await message.reply("\n".join(lines), parse_mode=None)


@router.message(Command("emoji_off"))
async def cmd_emoji_off(message: Message):
    if not _is_owner(message):
        return
    settings.custom_emoji_enabled = False
    logger.warning("[EMOJI_KILL] custom_emoji_enabled=False (in-memory, by owner)")
    await message.reply(
        "Премиум-иконки ВЫКЛючены в этом процессе. Вернуть — /emoji_on.\n"
        "После рестарта/редеплоя вернётся значение из env (CUSTOM_EMOJI_ENABLED).",
        parse_mode=None,
    )


@router.message(Command("emoji_on"))
async def cmd_emoji_on(message: Message):
    if not _is_owner(message):
        return
    settings.custom_emoji_enabled = True
    logger.warning("[EMOJI_KILL] custom_emoji_enabled=True (in-memory, by owner)")
    await message.reply("Премиум-иконки ВКЛючены в этом процессе.", parse_mode=None)
