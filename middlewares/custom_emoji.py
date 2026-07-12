"""
middlewares/custom_emoji.py — session-level перехватчик исходящих запросов ГЛАВНОГО
бота: оборачивает известные эмодзи в тексте/подписи в <tg-emoji> (премиум-иконки).

Безопасность (см. план):
- Только сессия главного бота (дочерние боты — отдельные сессии, не затрагиваются).
- По умолчанию ВЫКЛючено (settings.custom_emoji_enabled). Пока OFF — мгновенный passthrough.
- Тест-гейт: при заданном CUSTOM_EMOJI_TEST_UIDS иконки применяются ТОЛЬКО к этим чатам.
- Только parse_mode=HTML (омитнутый резолвится в дефолт бота; явный None пропускается).
- Entity-aware rewrite: не трогает содержимое <code>/<pre>/<tg-emoji> и атрибуты тегов.
- Фолбэк: любой emoji/entities/parse-BadRequest → повторная отправка ИСХОДНОГО (без иконок).
- Любая ошибка препроцессинга → отправляем как есть. Ничего не падает.
"""
import re
import logging

from aiogram.client.session.middlewares.base import BaseRequestMiddleware
from aiogram.client.default import Default
from aiogram.enums import ParseMode
from aiogram.exceptions import TelegramBadRequest

from config import settings
from utils.custom_emoji_map import CUSTOM_EMOJI, SKIP_IF_CONTAINS

logger = logging.getLogger(__name__)

# ── подготовка (один раз при импорте) ────────────────────────────────────────
_TOKENS = sorted(CUSTOM_EMOJI.keys(), key=len, reverse=True)  # длинные (с VS16) раньше
_EMOJI_RE = re.compile("|".join(re.escape(t) for t in _TOKENS)) if _TOKENS else None
_TAG_RE = re.compile(r"(<[^>]+>)")
_SKIP_TAGS = ("code", "pre", "tg-emoji")  # внутри них кастом не вставляем


def _parse_uids(raw: str) -> set[int]:
    out: set[int] = set()
    for p in (raw or "").replace(";", ",").split(","):
        p = p.strip()
        if p.lstrip("-").isdigit():
            out.add(int(p))
    return out


_TEST_UIDS = _parse_uids(getattr(settings, "custom_emoji_test_uids", "") or "")


def _make_wrap(exclude):
    def _wrap(m: "re.Match") -> str:
        e = m.group(0)
        if exclude and e in exclude:   # такой значок есть на кнопке этого экрана → оставляем обычным
            return e
        cid = CUSTOM_EMOJI.get(e)
        if not cid:
            return e
        return f'<tg-emoji emoji-id="{cid}">{e}</tg-emoji>'
    return _wrap


def rewrite(text: str, exclude: "set[str] | None" = None) -> str:
    """Оборачивает известные эмодзи в <tg-emoji>, НЕ трогая содержимое
    <code>/<pre>/<tg-emoji> и атрибуты тегов. Значки из `exclude` (те, что уже
    есть на кнопках этого сообщения) не премиумим — чтобы на одном экране один и
    тот же значок не оказался в двух стилях. Идемпотентно и безопасно."""
    if not text or _EMOJI_RE is None:
        return text
    if not _EMOJI_RE.search(text):
        return text
    wrap = _make_wrap(exclude)
    parts = _TAG_RE.split(text)
    skip = 0
    out = []
    for part in parts:
        if not part:
            continue
        if part[0] == "<" and part[-1] == ">":
            inner = part[1:-1].strip()
            toks = inner.lstrip("/").split()
            name = toks[0].lower() if toks else ""
            if name in _SKIP_TAGS and not inner.endswith("/"):
                if inner.startswith("/"):
                    skip = max(0, skip - 1)
                else:
                    skip += 1
            out.append(part)
        else:
            out.append(_EMOJI_RE.sub(wrap, part) if skip == 0 else part)
    return "".join(out)


def _button_tokens(reply_markup) -> "set[str]":
    """Мапленные эмодзи, встречающиеся на кнопках этого сообщения (inline или reply)."""
    if reply_markup is None or not _TOKENS:
        return set()
    rows = getattr(reply_markup, "inline_keyboard", None) or getattr(reply_markup, "keyboard", None)
    if not rows:
        return set()
    found: set[str] = set()
    for row in rows:
        for btn in row:
            t = getattr(btn, "text", "") or ""
            if not t:
                continue
            for tok in _TOKENS:
                if tok in t:
                    found.add(tok)
    return found


class CustomEmojiMiddleware(BaseRequestMiddleware):
    """Оборачивается на bot.session главного бота (bot.py)."""

    async def __call__(self, make_request, bot, method):
        # 0) фича выключена → мгновенный passthrough (обычный режим прода)
        if not getattr(settings, "custom_emoji_enabled", False):
            return await make_request(bot, method)

        try:
            # 1) есть ли текст/подпись
            if getattr(method, "text", None):
                field = "text"
            elif getattr(method, "caption", None):
                field = "caption"
            else:
                return await make_request(bot, method)

            # 1b) экраны-исключения (легенды кнопок, напр. /help) — целиком без иконок
            value = getattr(method, field)
            if SKIP_IF_CONTAINS and isinstance(value, str) and any(
                sig in value for sig in SKIP_IF_CONTAINS
            ):
                return await make_request(bot, method)

            # 2) тест-гейт по получателю (иконки только для указанных чатов)
            if _TEST_UIDS:
                if getattr(method, "chat_id", None) not in _TEST_UIDS:
                    return await make_request(bot, method)

            # 3) только HTML (омитнутый → дефолт бота; явный None/Markdown → пропуск)
            pm = getattr(method, "parse_mode", None)
            if isinstance(pm, Default):
                pm = bot.default[pm.name]
            if pm not in (ParseMode.HTML, "HTML"):
                return await make_request(bot, method)

            # 4) преобразование (не премиумим значки, уже стоящие на кнопках экрана)
            original = getattr(method, field)
            exclude = _button_tokens(getattr(method, "reply_markup", None))
            transformed = rewrite(original, exclude=exclude)
            if transformed == original:
                return await make_request(bot, method)
            new_method = method.model_copy(update={field: transformed})
        except Exception as e:  # любой сбой препроцессинга — шлём как есть
            logger.debug(f"custom_emoji preprocess skipped: {e}")
            return await make_request(bot, method)

        # 5) отправка с иконками + защитный фолбэк на обычный текст
        try:
            return await make_request(bot, new_method)
        except TelegramBadRequest as e:
            msg = str(e).lower()
            if any(k in msg for k in ("emoji", "entit", "parse", "tag", "unsupported")):
                logger.warning(f"custom_emoji fallback to plain: {e}")
                return await make_request(bot, method)  # исходный, без иконок
            raise
