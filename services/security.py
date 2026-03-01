"""
services/security.py — Шифрование токенов, валидация tg.initData.
"""
import hashlib
import hmac
import html
import json
import re
import time
from urllib.parse import unquote, parse_qsl

from cryptography.fernet import Fernet
from config import settings

# ── Fernet для шифрования токенов ────────────────────────────
_fernet = Fernet(settings.token_encryption_key.encode()
                 if isinstance(settings.token_encryption_key, str)
                 else settings.token_encryption_key)


def encrypt_token(token: str) -> str:
    """Шифрование токена бота перед сохранением в БД."""
    return _fernet.encrypt(token.encode()).decode()


def decrypt_token(encrypted: str) -> str:
    """Расшифровка токена бота из БД."""
    return _fernet.decrypt(encrypted.encode()).decode()


# ── Валидация tg.initData (WebApp) ────────────────────────────
def verify_init_data(init_data: str) -> dict | None:
    """
    Верифицирует HMAC-SHA256 подпись Telegram WebApp.
    Возвращает dict с user data если OK, None если подпись неверна
    или данные старше 1 часа.
    """
    params = dict(parse_qsl(unquote(init_data), keep_blank_values=True))
    received_hash = params.pop("hash", None)
    if not received_hash:
        return None

    data_check = "\n".join(f"{k}={v}" for k, v in sorted(params.items()))
    secret = hmac.new(b"WebAppData", settings.bot_token.encode(), hashlib.sha256).digest()
    expected = hmac.new(secret, data_check.encode(), hashlib.sha256).hexdigest()

    if not hmac.compare_digest(expected, received_hash):
        return None
    if abs(time.time() - int(params.get("auth_date", 0))) > 3600:
        return None

    return json.loads(params.get("user", "{}"))


# ── Верификация NOWPayments webhook ───────────────────────────
def verify_nowpayments_sig(payload_bytes: bytes, signature: str) -> bool:
    """
    HMAC-SHA512 верификация вебхука NOWPayments.
    payload_bytes — сырые байты тела запроса (до json.loads).
    """
    data = json.loads(payload_bytes)
    sorted_str = json.dumps(data, sort_keys=True, separators=(",", ":"))
    expected = hmac.new(
        settings.nowpayments_ipn_secret.encode(),
        sorted_str.encode(),
        hashlib.sha512,
    ).hexdigest()
    return hmac.compare_digest(expected, signature)


# ── Валидация пользовательского ввода ─────────────────────────
ALLOWED_USERNAME_RE = re.compile(r'^@?[a-zA-Z][a-zA-Z0-9_]{4,31}$')
ALLOWED_USER_ID_RE  = re.compile(r'^\d{5,12}$')
ALLOWED_TOKEN_RE    = re.compile(r'^\d{8,12}:[A-Za-z0-9_-]{35,}$')

MAX_FILE_SIZE = 20 * 1024 * 1024  # 20 MB


def sanitize(text: str, max_len: int = 256) -> str:
    """HTML-escape + обрезка — для всего пользовательского ввода."""
    return html.escape(text.strip()[:max_len])


def validate_bot_token(token: str) -> bool:
    return bool(ALLOWED_TOKEN_RE.match(token.strip()))


def validate_bl_file(content: bytes, filename: str) -> tuple[bool, str]:
    """
    Проверяет файл ЧС перед импортом.
    Возвращает (ok, error_message).
    """
    if len(content) > MAX_FILE_SIZE:
        return False, f"Файл слишком большой (макс. 20 MB)"
    if not filename.lower().endswith((".txt", ".csv")):
        return False, "Поддерживаются только TXT и CSV файлы"
    if b"\x00" in content:
        return False, "Файл содержит бинарные данные"
    try:
        content.decode("utf-8")
    except UnicodeDecodeError:
        try:
            content.decode("latin-1")
        except UnicodeDecodeError:
            return False, "Не удалось прочитать файл (неверная кодировка)"
    return True, ""


def parse_blacklist_line(line: str) -> dict | None:
    """
    Парсит одну строку файла ЧС.
    Возвращает {"user_id": int, "username": None} или {"user_id": None, "username": str}.
    """
    line = line.strip()
    if not line or line.startswith("#"):
        return None
    if ALLOWED_USER_ID_RE.match(line):
        return {"user_id": int(line), "username": None}
    if ALLOWED_USERNAME_RE.match(line):
        return {"user_id": None, "username": line.lstrip("@").lower()}
    return None


def detect_rtl(text: str) -> bool:
    """Проверяет наличие RTL-символов (арабский, иврит) в тексте."""
    for ch in text:
        if "\u0600" <= ch <= "\u06FF" or "\u0590" <= ch <= "\u05FF":
            return True
    return False


def detect_hieroglyph(text: str) -> bool:
    """Проверяет наличие иероглифов (китайский, японский) в тексте."""
    for ch in text:
        if "\u4E00" <= ch <= "\u9FFF" or "\u3040" <= ch <= "\u30FF":
            return True
    return False
