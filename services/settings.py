"""
services/settings.py — Управление key-value настройками платформы.

Таблица: platform_settings (key TEXT PK, value TEXT, updated_at TIMESTAMPTZ)

Используется для динамических настроек, которые администратор меняет через
Admin UI «на лету» без перезапуска бота. Основные ключи:

    rapidapi_key              — API-ключ RapidAPI
    rapidapi_host             — хост RapidAPI (x-rapidapi-host)
    rapidapi_url              — полный URL эндпоинта
    rapidapi_quota_remaining  — остаток запросов квоты (сохраняется после пайплайна)
"""
import logging
import time
from typing import Optional

import db.pool as db

logger = logging.getLogger(__name__)

# ── In-memory TTL-кэш для настроек RapidAPI ──────────────────────────────────
# Позволяет избежать запроса в БД при каждом из тысяч вызовов username_to_id().
# Кэш хранит все три ключа одновременно; инвалидируется при обновлении любого.

class _ApiConfigCache:
    """Простой TTL-контейнер для трёх RapidAPI-настроек."""

    TTL: float = 60.0  # секунды актуальности кэша

    def __init__(self) -> None:
        self.key:  str = ""
        self.host: str = ""
        self.url:  str = ""
        self._ts:  float = 0.0  # monotonic timestamp последнего обновления

    def is_fresh(self) -> bool:
        """Возвращает True, если кэш актуален (не истёк TTL)."""
        return (time.monotonic() - self._ts) < self.TTL

    def update(self, key: str, host: str, url: str) -> None:
        """Обновляет значения и сбрасывает таймер TTL."""
        self.key  = key
        self.host = host
        self.url  = url
        self._ts  = time.monotonic()

    def invalidate(self) -> None:
        """
        Принудительно инвалидирует кэш.
        Вызывается сразу после сохранения новых настроек через set_setting(),
        чтобы следующий запрос username_to_id() гарантированно перечитал
        свежие данные из БД, не дожидаясь истечения TTL.
        """
        self._ts = 0.0


# Единственный экземпляр кэша на весь процесс (модульный синглтон).
# Python импортирует модуль один раз — объект живёт до завершения процесса.
api_config_cache = _ApiConfigCache()


# ── Базовые CRUD-операции ─────────────────────────────────────────────────────

async def get_setting(key: str, default: Optional[str] = None) -> Optional[str]:
    """
    Читает значение по ключу из таблицы platform_settings.

    Args:
        key:     Строковый ключ настройки.
        default: Значение по умолчанию, если ключ не найден.

    Returns:
        Строковое значение из БД или default.
    """
    row = await db.fetchrow(
        "SELECT value FROM platform_settings WHERE key = $1",
        key,
    )
    return row["value"] if row else default


async def set_setting(key: str, value: str) -> None:
    """
    Сохраняет (или обновляет) значение в platform_settings.

    Использует UPSERT: если ключ уже существует — обновляет value и updated_at,
    иначе — вставляет новую строку.

    Args:
        key:   Строковый ключ настройки.
        value: Новое значение.
    """
    await db.execute(
        """
        INSERT INTO platform_settings (key, value, updated_at)
        VALUES ($1, $2, now())
        ON CONFLICT (key) DO UPDATE
            SET value      = EXCLUDED.value,
                updated_at = now()
        """,
        key,
        value,
    )
    logger.info("[SETTINGS] Updated key=%r", key)


# ── Специализированные функции для RapidAPI ───────────────────────────────────

async def get_api_config() -> tuple[str, str, str]:
    """
    Возвращает актуальные настройки RapidAPI: (api_key, api_host, api_url).

    Сначала проверяет TTL-кэш; если кэш устарел или был инвалидирован —
    запрашивает все три значения из БД одним запросом и обновляет кэш.

    Использовать из services/rapidapi_client.py вместо прямых запросов к БД.
    """
    if api_config_cache.is_fresh():
        return api_config_cache.key, api_config_cache.host, api_config_cache.url

    # Читаем все три ключа одним запросом для минимизации round-trips к БД
    rows = await db.fetch(
        """
        SELECT key, value FROM platform_settings
        WHERE key IN ('rapidapi_key', 'rapidapi_host', 'rapidapi_url')
        """,
    )
    cfg = {r["key"]: r["value"] for r in rows}

    key  = cfg.get("rapidapi_key",  "")
    host = cfg.get("rapidapi_host", "telegram124.p.rapidapi.com")
    url  = cfg.get("rapidapi_url",
                   "https://telegram124.p.rapidapi.com/telegram/api/userInfo")

    api_config_cache.update(key, host, url)
    logger.debug("[SETTINGS] API config cache refreshed (key masked)")
    return key, host, url


def invalidate_api_cache() -> None:
    """
    Немедленно инвалидирует TTL-кэш RapidAPI-настроек.

    Вызывать сразу ПОСЛЕ вызова set_setting() для rapidapi_key или rapidapi_host,
    чтобы пайплайн подхватил новые ключи без ожидания 60 секунд.

    Пример:
        await set_setting("rapidapi_key", new_key)
        invalidate_api_cache()   # ← следующий вызов get_api_config() пойдёт в БД
    """
    api_config_cache.invalidate()
    logger.info("[SETTINGS] API config cache invalidated")


async def save_quota(remaining: int) -> None:
    """
    Сохраняет остаток квоты RapidAPI в platform_settings.

    Вызывается ОДИН РАЗ в блоке finally функции start_ban_pipeline(),
    чтобы не создавать лишних UPDATE-запросов при обработке тысяч записей.

    Args:
        remaining: Число оставшихся запросов (из заголовка x-ratelimit-...).
    """
    await set_setting("rapidapi_quota_remaining", str(remaining))
    logger.info("[SETTINGS] RapidAPI quota saved: %d remaining", remaining)


async def get_quota() -> int:
    """
    Читает последний известный остаток квоты RapidAPI из БД.

    Returns:
        Количество оставшихся запросов, или -1 если данных ещё нет.
    """
    raw = await get_setting("rapidapi_quota_remaining", "-1")
    try:
        return int(raw)  # type: ignore[arg-type]
    except (ValueError, TypeError):
        return -1
