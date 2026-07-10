"""
utils/timing.py — Работа с задержкой автопринятия заявок.

Задержка хранится в СЕКУНДАХ (колонка bot_chats.autoaccept_delay_sec), что
позволяет указывать значения меньше минуты. Для обратной совместимости со
старыми записями (autoaccept_delay в минутах) используется fallback ×60.
"""
import re

# Быстрые пресеты (в секундах) — оставлены для совместимости, регулятор их не использует
DELAY_PRESETS_SEC = [0, 15, 30, 60, 300, 900, 1800, 3600]

_MAX_DELAY_SEC = 7 * 24 * 3600  # потолок — 7 суток (регулятор ±, единицы до дней)


def effective_delay_sec(row) -> int:
    """Эффективная задержка автопринятия в СЕКУНДАХ.

    Приоритет — новое поле autoaccept_delay_sec. Если его нет/NULL (старые
    записи или SELECT без этой колонки) — берём autoaccept_delay (минуты) × 60.
    Принимает asyncpg.Record или dict.
    """
    def _get(key):
        try:
            return row.get(key)
        except AttributeError:
            return row[key] if key in row else None

    v = _get("autoaccept_delay_sec")
    if v is not None:
        try:
            return max(0, int(v))
        except (TypeError, ValueError):
            return 0
    try:
        return max(0, int(_get("autoaccept_delay") or 0)) * 60
    except (TypeError, ValueError):
        return 0


def format_delay(sec: int) -> str:
    """Человекочитаемая метка задержки: 'ВЫКЛ 🔴' / '15 сек 🟡' / '5 мин 🟡' / '1 ч 🟡' / '2 дн 🟡'."""
    sec = int(sec or 0)
    if sec <= 0:
        return "ВЫКЛ 🔴"
    if sec < 60:
        return f"{sec} сек 🟡"
    if sec < 3600:
        m, s = divmod(sec, 60)
        return (f"{m} мин 🟡" if s == 0 else f"{m} мин {s} сек 🟡")
    if sec < 86400:
        h, rem = divmod(sec, 3600)
        m = rem // 60
        return (f"{h} ч 🟡" if m == 0 else f"{h} ч {m} мин 🟡")
    d, rem = divmod(sec, 86400)
    h = rem // 3600
    return (f"{d} дн 🟡" if h == 0 else f"{d} дн {h} ч 🟡")


def format_delay_short(sec: int) -> str:
    """Короткая метка без эмодзи — для кнопок пресетов: 'Выкл' / '15с' / '5м' / '1ч'."""
    sec = int(sec or 0)
    if sec <= 0:
        return "Выкл"
    if sec < 60:
        return f"{sec}с"
    if sec < 3600:
        m, s = divmod(sec, 60)
        return (f"{m}м" if s == 0 else f"{m}м {s}с")
    h, rem = divmod(sec, 3600)
    m = rem // 60
    return (f"{h}ч" if m == 0 else f"{h}ч {m}м")


def parse_delay_input(text: str) -> int | None:
    """Парсит пользовательский ввод задержки в СЕКУНДЫ.

    Поддерживает:
      «15», «90»            → секунды (значение по умолчанию — секунды)
      «15с», «15 сек»       → секунды
      «5м», «5 мин»         → минуты
      «1ч», «1 час»         → часы
      «2д», «2 дня»         → дни
      «0», «выкл», «off»    → 0 (выключить)
    Возвращает секунды (0.._MAX_DELAY_SEC) или None, если распознать не удалось.
    """
    if not text:
        return None
    t = text.strip().lower().replace(",", ".")
    if t in ("0", "выкл", "off", "нет", "-"):
        return 0

    m = re.match(r"^(\d+(?:\.\d+)?)\s*([а-яa-z]*)$", t)
    if not m:
        return None
    try:
        num = float(m.group(1))
    except ValueError:
        return None
    unit = m.group(2)

    if unit in ("", "с", "сек", "секунд", "секунды", "s", "sec"):
        mult = 1
    elif unit in ("м", "мин", "минут", "минуты", "m", "min"):
        mult = 60
    elif unit in ("ч", "час", "часа", "часов", "h", "hour"):
        mult = 3600
    elif unit in ("д", "дн", "день", "дня", "дней", "d", "day"):
        mult = 86400
    else:
        return None

    sec = int(round(num * mult))
    if sec < 0:
        return 0
    return min(sec, _MAX_DELAY_SEC)
