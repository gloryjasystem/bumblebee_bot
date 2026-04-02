"""
utils/username_parser.py — Парсинг и валидация входных данных для ЧС-пайплайна.

Умеет разбирать:
  - Одиночные @username и username без @
  - Числовые Telegram ID (положительные и отрицательные)
  - Ссылки t.me/username (с исключением технических путей через negative lookahead)
  - Произвольный текст с перемешанными форматами
  - Содержимое .txt и .csv файлов

Возвращает два дедуплицированных списка: usernames и numeric_ids.
"""
import csv
import io
import logging
import re
from typing import Optional

logger = logging.getLogger(__name__)

# ── Регулярные выражения ──────────────────────────────────────────────────────

# Числовой Telegram ID: опциональный минус (каналы) + минимум 5 цифр
# Это защищает от парсинга порядковых номеров строк (например, "1. username") как ID
_NUMERIC_RE = re.compile(r"^-?\d{5,}$")

# Валидный @username по правилам Telegram:
#   - Начинается с буквы (не цифры/подчёркивания)
#   - Содержит буквы, цифры, подчёркивания
#   - Длина от 5 до 32 символов (включая первый символ)
#   - @ опционален
_USERNAME_RE = re.compile(r"(?:^|(?<=\s)|(?<=,)|(?<=;))@?([a-zA-Z][a-zA-Z0-9_]{4,31})(?=\s|,|;|$)")

# Ссылка t.me/username с negative lookahead для технических путей:
#   - joinchat/ — групповые инвайт-ссылки
#   - +          — приватные инвайт-ссылки (t.me/+AbCdEf)
#   - c/         — приватные каналы по ID (t.me/c/123456)
#   - s/         — превью публичных каналов
#   - addstickers/ — стикерпаки
#   - m/         — мини-апп пути
# Без этих исключений парсер ошибочно вытаскивал бы "joinchat" как юзернейм.
_TG_LINK_RE = re.compile(
    r"(?:https?://)?(?:www\.)?t(?:elegram)?\.me/"
    r"(?!joinchat/|[+]|c/|s/|addstickers/|m/)"  # negative lookahead
    r"([a-zA-Z][a-zA-Z0-9_]{4,31})"             # группа захвата: юзернейм
)

# Разделители токенов в строке: пробелы, запятые, точки с запятой, переносы
_SPLIT_RE = re.compile(r"[\s,;\r\n]+")


# ── Основные функции ──────────────────────────────────────────────────────────

def parse_usernames_and_ids(
    raw: str,
) -> tuple[list[str], list[int]]:
    """
    Разбирает произвольный текст и возвращает два дедуплицированных списка.

    Порядок приоритетов при разборе каждого токена:
      1. Числовой ID — сразу в numeric_ids (без API-запроса)
      2. t.me/username ссылка — извлекаем username, технические пути игнорируем
      3. @username или username — валидируем по правилам Telegram

    Args:
        raw: Произвольный текст с юзернеймами, ID, ссылками в любом порядке.

    Returns:
        Кортеж (usernames, numeric_ids):
          - usernames   — список строк в нижнем регистре без @
          - numeric_ids — список целых чисел
    """
    usernames:   list[str] = []
    numeric_ids: list[int] = []

    seen_usernames: set[str] = set()
    seen_ids:       set[int] = set()

    for token in _SPLIT_RE.split(raw):
        token = token.strip()
        if not token:
            continue

        _process_token(token, usernames, numeric_ids, seen_usernames, seen_ids)

    logger.debug(
        "[PARSER] Parsed: %d usernames, %d numeric IDs",
        len(usernames), len(numeric_ids),
    )
    return usernames, numeric_ids


def parse_file_content(
    content:  bytes,
    filename: str,
) -> tuple[list[str], list[int]]:
    """
    Разбирает содержимое загруженного .txt или .csv файла.

    Для CSV файлов объединяет все ячейки всех строк в единый поток токенов
    и затем передаёт в parse_usernames_and_ids.

    Args:
        content:  Байтовое содержимое файла.
        filename: Имя файла (для определения формата по расширению).

    Returns:
        Кортеж (usernames, numeric_ids) — см. parse_usernames_and_ids.
    """
    # Пробуем UTF-8, затем Latin-1 как fallback (как в существующем import_file)
    try:
        text = content.decode("utf-8")
    except UnicodeDecodeError:
        text = content.decode("latin-1", errors="replace")

    if filename.lower().endswith(".csv"):
        text = _flatten_csv(text)

    return parse_usernames_and_ids(text)


# ── Вспомогательные функции ───────────────────────────────────────────────────

def _process_token(
    token:          str,
    usernames:      list[str],
    numeric_ids:    list[int],
    seen_usernames: set[str],
    seen_ids:       set[int],
) -> None:
    """Разбирает один токен и добавляет в нужный список (с дедупликацией)."""

    # 1. Числовой ID — наивысший приоритет, без API
    if _NUMERIC_RE.match(token):
        try:
            uid = int(token)
            if uid not in seen_ids:
                seen_ids.add(uid)
                numeric_ids.append(uid)
        except ValueError:
            pass
        return

    # 2. Ссылка t.me/... — проверяем до @username, т.к. токен может начинаться с https
    link_match = _TG_LINK_RE.search(token)
    if link_match:
        uname = link_match.group(1).lower()
        if uname not in seen_usernames:
            seen_usernames.add(uname)
            usernames.append(uname)
        return

    # 3. @username или username (без ссылки)
    clean = token.lstrip("@")
    if _is_valid_username(clean):
        uname = clean.lower()
        if uname not in seen_usernames:
            seen_usernames.add(uname)
            usernames.append(uname)


def _is_valid_username(username: str) -> bool:
    """
    Проверяет, соответствует ли строка правилам Telegram @username:
      - Только a-z, A-Z, 0-9, _
      - Начинается с буквы (не цифра/подчёркивание)
      - Длина от 5 до 32 символов
    """
    if not username:
        return False
    if len(username) < 5 or len(username) > 32:
        return False
    if not username[0].isalpha():
        return False
    return bool(re.match(r"^[a-zA-Z][a-zA-Z0-9_]{4,31}$", username))


def _flatten_csv(text: str) -> str:
    """
    Разворачивает CSV в плоскую строку токенов через пробел.
    Умный парсинг: если в первой строке есть заголовки 'username' или 'id',
    извлекает данные ТОЛЬКО из этих колонок (защита от случайных совпадений в First Name).
    """
    reader = csv.reader(io.StringIO(text))
    rows   = list(reader)
    if not rows:
        return ""

    header = [str(col).strip().lower() for col in rows[0]]
    username_col = -1
    id_col = -1

    # Поиск нужных столбцов в заголовке
    for i, col in enumerate(header):
        if col in ("username", "user_name", "юзернейм"):
            username_col = i
        elif col in ("id", "user_id", "userid", "tg_id"):
            id_col = i

    tokens = []

    if username_col != -1 or id_col != -1:
        # Нашли заголовки — берём данные строго из них
        for row in rows[1:]:
            if username_col != -1 and username_col < len(row):
                val = row[username_col].strip()
                if val: tokens.append(val)
            if id_col != -1 and id_col < len(row):
                val = row[id_col].strip()
                if val: tokens.append(val)
    else:
        # Fallback: заголовков нет, извлекаем всё (пропускаем столбец №, если есть)
        start = 1 if header and header[0] in ("#", "n", "number", "№") else 0
        for row in rows[start:]:
            for cell in row:
                cell = cell.strip()
                if cell: tokens.append(cell)

    return " ".join(tokens)
