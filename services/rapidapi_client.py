"""
services/rapidapi_client.py — Асинхронный клиент RapidAPI для конвертации
@username → Telegram numeric ID.

Архитектурные принципы:
  - Один aiohttp.ClientSession создаётся в ban_pipeline и передаётся сюда;
    создание сессии внутри каждого вызова — антипаттерн.
  - Настройки (key, host, url) берутся из services.settings.get_api_config()
    с TTL-кэшем 60 сек, чтобы не долбить БД при тысячах запросов.
  - Функция возвращает кортеж (tg_id, quota_remaining), где quota_remaining
    читается из заголовков ответа и сохраняется ОДИН РАЗ в конце пайплайна.
  - Все ошибки API выбрасываются как кастомные исключения — вызывающий код
    (воркер пайплайна) решает, что с ними делать.
"""
import logging
from typing import Optional

import aiohttp

from services.settings import get_api_config

logger = logging.getLogger(__name__)

# ── Имена заголовков с остатком квоты (RapidAPI использует разные имена) ──────
# Проверяем их по приоритету: первый непустой выигрывает.
_QUOTA_HEADERS = (
    "x-ratelimit-requests-remaining",
    "X-RateLimit-Requests-Remaining",
    "x-rapidapi-ratelimit-remaining",
    "X-RateLimit-Remaining",
)


# ── Кастомные исключения ──────────────────────────────────────────────────────

class UserNotFoundError(Exception):
    """
    Пользователь не найден в Telegram (HTTP 404 или невалидное тело ответа).
    Воркер должен записать resolve_error='not_found' и продолжить обработку.
    """
    pass


class RateLimitError(Exception):
    """
    RapidAPI вернул 429 Too Many Requests.
    Атрибут retry_after содержит рекомендованное время ожидания в секундах.
    Воркер должен вернуть задание в очередь и заснуть на retry_after секунд.
    """
    def __init__(self, retry_after: int = 5) -> None:
        self.retry_after = retry_after
        super().__init__(f"Rate limit exceeded, retry after {retry_after}s")


class InvalidApiKeyError(Exception):
    """
    RapidAPI вернул 403 Forbidden — ключ недействителен или истёк.
    Воркер должен остановить весь пайплайн и уведомить администратора.
    """
    pass


# ── Основная функция конвертации ─────────────────────────────────────────────

async def username_to_id(
    session:  aiohttp.ClientSession,
    username: str,
    timeout:  int = 10,
) -> tuple[int, Optional[int]]:
    """
    Конвертирует @username в числовой Telegram ID через RapidAPI.

    Настройки (key, host, url) подтягиваются автоматически через TTL-кэш;
    при необходимости смены ключей достаточно вызвать invalidate_api_cache().

    Args:
        session:  Переиспользуемый aiohttp.ClientSession из ban_pipeline.
        username: Юзернейм без @ (очищается внутри).
        timeout:  Таймаут одного запроса в секундах.

    Returns:
        Кортеж (tg_id, quota_remaining):
          - tg_id           — числовой Telegram ID пользователя
          - quota_remaining — остаток квоты из заголовков ответа,
                              None если заголовок отсутствует в ответе

    Raises:
        UserNotFoundError  — HTTP 404 или пустой/невалидный ответ от API
        RateLimitError     — HTTP 429, содержит retry_after
        InvalidApiKeyError — HTTP 403, ключ невалиден
        aiohttp.ClientError — сетевые ошибки, таймауты
    """
    api_key, api_host, api_url, api_param = await get_api_config()

    # Защита от случайно сохранённого placeholder-значения
    if not api_key or api_key == "YOUR_KEY_HERE":
        raise InvalidApiKeyError("RapidAPI key is not configured (value is placeholder)")

    clean_username = username.lstrip("@").strip()
    if not clean_username:
        raise UserNotFoundError("Empty username after cleanup")

    headers = {
        "x-rapidapi-key":  api_key,
        "x-rapidapi-host": api_host,
    }
    # api_param — динамическое имя параметра ('username', 'peer', и т.д.)
    params = {api_param: clean_username}

    logger.debug("[RAPIDAPI] Resolving @%s via %s (param=%s)", clean_username, api_host, api_param)

    async with session.get(
        api_url,
        headers=headers,
        params=params,
        timeout=aiohttp.ClientTimeout(total=timeout),
    ) as resp:

        # ── Читаем остаток квоты из заголовков ЛЮБОГО ответа ─────────────────
        quota_remaining: Optional[int] = _parse_quota(resp.headers)

        # ── Обработка HTTP-статусов ───────────────────────────────────────────
        if resp.status == 404:
            logger.info("[RAPIDAPI] @%s not found (404)", clean_username)
            raise UserNotFoundError(f"@{clean_username} not found in Telegram")

        if resp.status == 429:
            retry_after = _parse_retry_after(resp.headers)
            logger.warning("[RAPIDAPI] Rate limit 429, retry after %ds", retry_after)
            raise RateLimitError(retry_after)

        if resp.status == 403:
            logger.error("[RAPIDAPI] 403 Forbidden — invalid or expired API key")
            raise InvalidApiKeyError("Invalid or expired RapidAPI key (403)")

        # Любые другие 4xx/5xx — пробрасываем как aiohttp.ClientResponseError
        resp.raise_for_status()

        # ── Парсинг тела ответа ───────────────────────────────────────────────
        try:
            data = await resp.json(content_type=None)  # допускаем text/plain
        except Exception as exc:
            raise UserNotFoundError(
                f"Failed to parse JSON for @{clean_username}"
            ) from exc

        # Ожидаемая структура: {"result": {"id": 123456789, ...}}
        tg_id = _extract_id(data, clean_username)

        logger.info(
            "[RAPIDAPI] @%s → %d (quota_remaining=%s)",
            clean_username, tg_id,
            quota_remaining if quota_remaining is not None else "unknown",
        )
        return tg_id, quota_remaining


# ── Вспомогательные функции ───────────────────────────────────────────────────

def _parse_quota(headers: "aiohttp.typedefs.CIMultiDictProxy") -> Optional[int]:
    """
    Ищет заголовок с остатком квоты среди известных имён.
    Возвращает int или None, если ни один заголовок не найден/невалиден.
    """
    for header_name in _QUOTA_HEADERS:
        raw = headers.get(header_name)
        if raw is not None:
            try:
                return int(raw)
            except ValueError:
                logger.debug("[RAPIDAPI] Cannot parse quota header %r=%r", header_name, raw)
    return None


def _parse_retry_after(headers: "aiohttp.typedefs.CIMultiDictProxy") -> int:
    """
    Читает Retry-After из заголовков ответа 429.
    Если заголовок отсутствует или невалиден — возвращает 5 секунд по умолчанию.
    """
    raw = headers.get("Retry-After") or headers.get("retry-after")
    if raw:
        try:
            return max(1, int(raw))
        except ValueError:
            pass
    return 5  # безопасный дефолт


def _extract_id(data: dict, username: str) -> int:
    """
    Извлекает числовой Telegram ID из тела ответа RapidAPI.

    Поддерживаемые форматы:
      - {"result": {"id": 123456789, "username": "..."}}         — основной формат
      - {"id": 123456789, "username": "..."}                     — упрощённый формат
      - {"user": {"id": 123456789, "username": "..."}}           — альтернативный формат
    """
    
    # ── Проверка на поддельный/ошибочный ответ от API ─────────────────────────
    # Защита от багов провайдера (когда он шлёт всем один и тот же ID с HTTP 200)
    resp_username = (
        (data.get("result") if isinstance(data.get("result"), dict) else {}).get("username") or
        data.get("username") or
        (data.get("user") if isinstance(data.get("user"), dict) else {}).get("username")
    )
    if resp_username and str(resp_username).lower() != username.lower():
        raise UserNotFoundError(
            f"API mismatch: requested @{username}, but API returned @{resp_username}"
        )

    # Пробуем все известные пути в порядке приоритета
    candidates = [
        data.get("result", {}).get("id") if isinstance(data.get("result"), dict) else None,
        data.get("id"),
        data.get("user", {}).get("id") if isinstance(data.get("user"), dict) else None,
    ]

    for raw_id in candidates:
        if raw_id is not None:
            try:
                return int(raw_id)
            except (ValueError, TypeError):
                continue

    raise UserNotFoundError(
        f"Unexpected API response for @{username}: id field not found. "
        f"Response keys: {list(data.keys())}"
    )
