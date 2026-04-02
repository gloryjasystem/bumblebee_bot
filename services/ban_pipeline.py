"""
services/ban_pipeline.py — Enterprise-grade dual-lane async ban pipeline.

Architecture (Producer-Consumer + Strategy + Circuit Breaker):

  ┌──────────────────────────────────────────────────────────────────────┐
  │  start_ban_pipeline()                                                │
  │  ├─ Splits input into FastQueue (IDs) and SlowQueue (usernames)     │
  │  ├─ FastWorkers: use TokenBucket for Telegram (≤20 req/s)           │
  │  ├─ SlowWorkers: use provider's RPM limiter + local DB first        │
  │  ├─ Global FloodWait event: pauses ALL Telegram calls               │
  │  └─ Circuit Breaker: disables SlowQueue on API key error            │
  │                                                                      │
  │  BaseUsernameResolver (Strategy Pattern)                             │
  │  └─ RapidApiResolver (concrete): dynamic rpm_limit, host, key       │
  │                                                                      │
  │  TokenBucket: thread-safe, async-native rate limiter                │
  └──────────────────────────────────────────────────────────────────────┘

Rate Limits (configurable):
  - Telegram Ban API:  TG_BAN_RPS = 20 req/s  (stays under 30 req/s hard cap)
  - RapidAPI:          rpm_limit from DB config (e.g. 15 req/min)
  - Jitter:            ±20% on all sleeps to avoid thundering herd

Reliability:
  - TelegramRetryAfter: global pause, respects exact timeout + jitter
  - RateLimitError:     returns item to SlowQueue, pauses that worker
  - InvalidApiKeyError: circuit breaker opens, stops all slow workers
  - Empty task guard:   queue.task_done() always called (deadlock-free)
"""
from __future__ import annotations

import asyncio
import logging
import random
import time
from abc import ABC, abstractmethod
from typing import Optional

import aiohttp
from aiogram import Bot as AioBot
from aiogram.exceptions import TelegramBadRequest, TelegramForbiddenError, TelegramRetryAfter

import db.pool as db
from services.blacklist import _get_chats_with_tokens
from services.rapidapi_client import (
    InvalidApiKeyError,
    RateLimitError,
    UserNotFoundError,
    username_to_id,
)
from services.settings import get_api_config, save_quota

logger = logging.getLogger(__name__)


# ── Глобальные настройки ───────────────────────────────────────────────────────

# Telegram
TG_BAN_RPS: float = 20.0          # Макс. банов/сек (ниже лимита Telegram 30 req/s)
TG_BAN_JITTER: float = 0.2        # ±20% джиттер для каждого sleep

# Воркеры
FAST_WORKERS: int = 4             # Для числовых ID (без API)
SLOW_WORKERS: int = 5             # 5 Воркеров: золотой баланс скорости и лимитов параллельности RapidAPI

# Максимальное количество попыток API на один username (защита от вечного цикла)
MAX_API_RETRIES: int = 5

# Прогресс-бар
PROGRESS_INTERVAL: float = 3.0   # Минимальный интервал обновления (сек)

# Реестр активных пайплайнов для Graceful Shutdown
active_pipelines: dict[int, asyncio.Event] = {}
_last_report: dict[int, float] = {}

# Глобальный Exponential Backoff для провайдера API (защита от 429 и таймаутов)
_api_flood_wait_until: float = 0.0
_api_backoff_time: float = 5.0


# ══════════════════════════════════════════════════════════════════════════════
# TOKEN BUCKET — async-native rate limiter с джиттером
# ══════════════════════════════════════════════════════════════════════════════

class TokenBucket:
    """
    Классический Token Bucket для ограничения throughput.

    Потокобезопасен для asyncio (использует asyncio.Lock).
    Поддерживает динамическое обновление rate без перезапуска.
    """

    def __init__(self, rate: float, capacity: float | None = None) -> None:
        """
        Args:
            rate:     Токенов в секунду (максимальный throughput).
            capacity: Максимальный «бурст» токенов (default = max(1.0, rate * 2)).
        """
        self._rate: float = rate
        # Если rate < 0.5 (например, RPM=15 -> rate=0.25), capacity будет < 1.0.
        # В этом случае acquire() зависнет в бесконечном цикле, так как ждёт >= 1.0 токенов.
        self._capacity: float = capacity or max(1.0, rate * 2.0)
        self._tokens: float = self._capacity
        self._last_refill: float = time.monotonic()
        self._lock: asyncio.Lock = asyncio.Lock()

    def update_rate(self, new_rate: float) -> None:
        """Динамически обновить лимит без пересоздания объекта."""
        self._rate = new_rate
        self._capacity = max(1.0, new_rate * 2.0)

    async def acquire(self, jitter: float = TG_BAN_JITTER) -> None:
        """
        Ждёт, пока не появится доступный токен.
        Применяет джиттер ±jitter к паузам, чтобы избежать синхронных волн.
        """
        while True:
            async with self._lock:
                now = time.monotonic()
                elapsed = now - self._last_refill
                refill = elapsed * self._rate
                self._tokens = min(self._capacity, self._tokens + refill)
                self._last_refill = now

                if self._tokens >= 1.0:
                    self._tokens -= 1.0
                    return

                wait = (1.0 - self._tokens) / self._rate

            # Добавляем джиттер: случайное смещение ±jitter*wait
            jitter_wait = wait * (1.0 + random.uniform(-jitter, jitter))
            await asyncio.sleep(max(0.001, jitter_wait))


# ══════════════════════════════════════════════════════════════════════════════
# ABSTRACT RESOLVER — Strategy Pattern
# ══════════════════════════════════════════════════════════════════════════════

class BaseUsernameResolver(ABC):
    """
    Абстрактный провайдер конвертации @username → Telegram ID.

    Все провайдеры реализуют этот интерфейс — swap без изменения воркеров.
    """

    @abstractmethod
    async def resolve(
        self,
        session: aiohttp.ClientSession,
        username: str,
    ) -> tuple[int, Optional[int]]:
        """
        Returns:
            (tg_id, quota_remaining)

        Raises:
            UserNotFoundError, RateLimitError, InvalidApiKeyError, aiohttp.ClientError
        """
        ...

    @property
    @abstractmethod
    def rpm_limit(self) -> float:
        """Максимальное количество запросов в минуту для этого провайдера."""
        ...


class RapidApiResolver(BaseUsernameResolver):
    """
    Конкретный провайдер через RapidAPI.

    Параметры (key, host, url, param, rpm_limit) подтягиваются из БД
    через get_api_config() с TTL-кэшем — смена ключей/хостов без перезапуска.
    """

    _DEFAULT_RPM: float = 38.0  # 38/min — безопасный запас (5% ниже лимита тарифа 40/min)
                                  # Переопределяется из platform_settings.rapidapi_rpm при старте пайплайна

    async def resolve(
        self,
        session: aiohttp.ClientSession,
        username: str,
    ) -> tuple[int, Optional[int]]:
        from services.rapidapi_client import username_to_id
        # Жестко обрываем поиск мертвых душ через 14 секунд
        return await username_to_id(session, username, timeout=14.0)

    @property
    def rpm_limit(self) -> float:
        return self._DEFAULT_RPM

    def update_rpm(self, new_rpm: float) -> None:
        """Обновить лимит в runtime (например, после обнаружения нового тарифа)."""
        self._DEFAULT_RPM = new_rpm


# Синглтон провайдера — создаётся один раз, доступен для обновления извне
_default_resolver: RapidApiResolver = RapidApiResolver()


# ══════════════════════════════════════════════════════════════════════════════
# ГЛОБАЛЬНЫЕ ОБЪЕКТЫ RATE LIMITING И CIRCUIT BREAKER
# ══════════════════════════════════════════════════════════════════════════════

# Telegram Token Bucket — общий для ВСЕХ воркеров (FastQueue + SlowQueue),
# чтобы суммарный RPS никогда не превышал TG_BAN_RPS.
_tg_bucket: TokenBucket = TokenBucket(rate=TG_BAN_RPS)

# Global FloodWait event:
# Когда Telegram возвращает RetryAfter, этот event устанавливается.
# ВСЕ воркеры проверяют его перед каждым ban_chat_member.
# После истечения таймаута — сбрасывается.
_tg_flood_wait_until: float = 0.0  # monotonic timestamp, когда снова можно


# ══════════════════════════════════════════════════════════════════════════════
# ТОЧКА ВХОДА
# ══════════════════════════════════════════════════════════════════════════════

async def start_ban_pipeline(
    bot:             AioBot,
    owner_id:        int,
    usernames:       list[str],
    numeric_ids:     list[int],
    notify_chat_id:  int,
    status_msg_id:   int,
    child_bot_id:    Optional[int] = None,
    resolver:        BaseUsernameResolver | None = None,
    action:          str = "ban",
    use_api:         bool = True,
) -> None:
    """
    Запускает enterprise-grade dual-lane конвейер бана.

    FastLane: numeric_ids → сразу в _ban_in_all_chats (без API, пейсинг TokenBucket)
    SlowLane: usernames → резолв (локальная БД → провайдер) → _ban_in_all_chats

    Args:
        bot:            Главный bot (для прогресс-бара).
        owner_id:       ID владельца/администратора сети.
        usernames:      @username-список.
        numeric_ids:    Цифровые Telegram ID (минуют API полностью).
        notify_chat_id: Чат для прогресс-бара.
        status_msg_id:  ID сообщения прогресс-бара (ключ graceful shutdown).
        child_bot_id:   None → глобальный ЧС. int → локальный per-bot.
        resolver:       Провайдер резолва (default: RapidApiResolver).
    """
    if resolver is None:
        resolver = _default_resolver

    # ── Сброс глобального backoff от предыдущего пайплайна ────────────
    # BUG FIX: _api_backoff_time и _api_flood_wait_until — глобальные переменные.
    # Если предыдущий пайплайн завершился с exponential backoff (например, 60s),
    # следующий старт не должен начинать с этим «раздутым» состоянием.
    global _api_backoff_time, _api_flood_wait_until
    _api_backoff_time = 5.0
    _api_flood_wait_until = 0.0

    # ── Динамическое чтение RPM из platform_settings ─────────────────
    # Позволяет менять лимит без перезапуска бота (смена тарифа провайдера).
    # Значение хранится в таблице platform_settings под ключом 'rapidapi_rpm'.
    # Если ключ отсутствует — используется _DEFAULT_RPM резолвера.
    if use_api:
        try:
            from services.settings import get_setting as _gs
            _rpm_raw = await _gs("rapidapi_rpm", None)
            if _rpm_raw:
                _rpm_val = float(_rpm_raw)
                if 1.0 <= _rpm_val <= 500.0:  # Защита от несуразных значений
                    resolver.update_rpm(_rpm_val)
                    logger.info("[PIPELINE] RPM loaded from DB: %.1f/min", _rpm_val)
        except Exception:
            pass  # Любая ошибка → fallback на _DEFAULT_RPM (уже 38.0)

    # ── Дедупликация ───────────────────────────────────────────────────────
    unique_usernames: list[str] = list(dict.fromkeys(
        u.lower().lstrip("@") for u in usernames
    ))
    unique_ids: list[int] = list(dict.fromkeys(numeric_ids))
    total = len(unique_usernames) + len(unique_ids)

    # ── Результаты (разделяемый dict, защищён GIL) ───────────────────────
    results: dict[str, int] = {
        "ok": 0, "not_found": 0, "error": 0,
        "already_in_bl": 0, "total": total,
    }
    quota_box: dict[str, Optional[int]] = {"remaining": None}

    # ── Очереди ───────────────────────────────────────────────────────────
    fast_queue: asyncio.Queue[tuple[None, int]] = asyncio.Queue()
    slow_queue: asyncio.Queue[tuple[str, None]] = asyncio.Queue()

    for uid in unique_ids:
        await fast_queue.put((None, uid))
    for uname in unique_usernames:
        await slow_queue.put((uname, None, 0))  # (username, resolved_id, retries)

    # ── Stop / Circuit Breaker events ────────────────────────────────────
    stop_event     = asyncio.Event()
    api_dead_event = asyncio.Event()   # Circuit Breaker: API ключ мёртв
    active_pipelines[status_msg_id] = stop_event

    logger.info(
        "[PIPELINE] Started: owner=%d fast=%d slow=%d (dupes_skipped=%d) rpm=%.1f",
        owner_id, len(unique_ids), len(unique_usernames),
        (len(usernames) + len(numeric_ids)) - total,
        resolver.rpm_limit,
    )

    connector = aiohttp.TCPConnector(limit=10, ttl_dns_cache=300, ssl=True)
    try:
        async with aiohttp.ClientSession(connector=connector) as session:
            # ── RPM лимитер для SlowQueue (ОДИН shared bucket на ВСЕХ slow-воркеров)
            # Гарантирует: суммарный поток к RapidAPI ≤ rpm_limit независимо от
            # количества воркеров (SLOW_WORKERS=2 конкурируют за один bucket).
            rpm = resolver.rpm_limit
            slow_bucket = TokenBucket(rate=rpm / 60.0)   # rpm → rps

            workers: list[asyncio.Task] = []

            # Fast workers — цифровые ID
            for i in range(FAST_WORKERS):
                workers.append(asyncio.create_task(
                    _fast_worker(
                        worker_id=i,
                        queue=fast_queue,
                        owner_id=owner_id,
                        child_bot_id=child_bot_id,
                        stop_event=stop_event,
                        results=results,
                        bot=bot,
                        notify_chat_id=notify_chat_id,
                        status_msg_id=status_msg_id,
                        action=action,
                    )
                ))

            # Slow workers — @usernames
            for i in range(SLOW_WORKERS):
                workers.append(asyncio.create_task(
                    _slow_worker(
                        worker_id=i,
                        queue=slow_queue,
                        session=session,
                        resolver=resolver,
                        slow_bucket=slow_bucket,
                        owner_id=owner_id,
                        child_bot_id=child_bot_id,
                        stop_event=stop_event,
                        api_dead_event=api_dead_event,
                        results=results,
                        quota_box=quota_box,
                        bot=bot,
                        notify_chat_id=notify_chat_id,
                        status_msg_id=status_msg_id,
                        action=action,
                        use_api=use_api,
                    )
                ))

            await asyncio.gather(fast_queue.join(), slow_queue.join())
            for w in workers:
                w.cancel()

    finally:
        if quota_box["remaining"] is not None:
            await save_quota(quota_box["remaining"])

        active_pipelines.pop(status_msg_id, None)
        _last_report.pop(status_msg_id, None)

        logger.info(
            "[PIPELINE] Done: owner=%d ok=%d not_found=%d error=%d already=%d quota=%s",
            owner_id, results["ok"], results["not_found"],
            results["error"], results["already_in_bl"], quota_box["remaining"],
        )

    # ── Финальное сообщение ───────────────────────────────────────────────
    from aiogram.types import InlineKeyboardMarkup, InlineKeyboardButton
    back_cb = f"bs_blacklist:{child_bot_id}" if child_bot_id else f"ga_bl:{owner_id}"
    back_markup = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="◀️ Назад в ЧС", callback_data=back_cb)]
    ])
    if stop_event.is_set():
        done = sum(results[k] for k in ("ok", "not_found", "error", "already_in_bl"))
        text = (
            f"⚠️ <b>Процесс прерван</b> вручную.\n"
            f"Успело обработаться: <b>{done}/{total}</b>\n\n"
            f"✅ {'Забанено' if action=='ban' else 'Разбанено'}: <b>{results['ok']}</b>\n"
            f"🔄 {'Уже в базе' if action=='ban' else 'Уже удалены'}: <b>{results['already_in_bl']}</b>\n"
            f"❓ Не найдено: <b>{results['not_found']}</b>\n"
            f"⚠️ Ошибки: <b>{results['error']}</b>"
        )
    else:
        text = (
            f"✅ <b>Готово!</b>\n\n"
            f"├ {'Забанено' if action=='ban' else 'Разбанено'}: <b>{results['ok']}</b>\n"
            f"├ {'Уже в базе' if action=='ban' else 'Уже удалены'}: <b>{results['already_in_bl']}</b>\n"
            f"├ Не найдено: <b>{results['not_found']}</b>\n"
            f"└ Ошибки: <b>{results['error']}</b>"
        )
    await _edit_status(bot, notify_chat_id, status_msg_id, text, show_stop=False, markup=back_markup)


# ══════════════════════════════════════════════════════════════════════════════
# FAST WORKER — для числовых ID (без API)
# ══════════════════════════════════════════════════════════════════════════════

async def _fast_worker(
    worker_id:      int,
    queue:          asyncio.Queue,
    owner_id:       int,
    child_bot_id:   Optional[int],
    stop_event:     asyncio.Event,
    results:        dict[str, int],
    bot:            AioBot,
    notify_chat_id: int,
    status_msg_id:  int,
    action:         str = "ban",
) -> None:
    while True:
        if stop_event.is_set():
            _drain_queue(queue)
            return

        # ── 1. Получение задачи вне блока try...finally ──
        try:
            # Используем wait_for для периодической проверки stop_event
            _, numeric_id = await asyncio.wait_for(queue.get(), timeout=1.0)
        except asyncio.TimeoutError:
            continue
        except asyncio.CancelledError:
            return

        # ── 2. Процесс задачи с гарантированным task_done() ──
        try:
            already, local_id = await _smart_resolve(
                owner_id=owner_id,
                username=None,
                numeric_id=numeric_id,
                child_bot_id=child_bot_id,
            )
            if action == "ban":
                # ОПТИМИЗАЦИЯ: пропускаем повторный бан тех, кто уже в ЧС.
                # Раньше система делала ban API-вызовы во ВСЕХ чатах для каждой из
                # «уже заблокированных» записей — это и было главной причиной торможения
                # при файлах с большим количеством дубликатов (>3000 «Уже в ЧС»).
                if already:
                    results["already_in_bl"] += 1
                    logger.debug("[FAST %d] id=%d already in BL — skip re-ban", worker_id, numeric_id)
                    continue  # finally: queue.task_done() отработает

                banned = await _process_in_all_chats(owner_id, numeric_id, child_bot_id, action=action)
                total_banned = sum(banned.values())
                if total_banned > 0:
                    await _increment_blocked_count(owner_id, banned, is_global=(child_bot_id is None))
                results["ok"] += 1
                await _save_to_blacklist(owner_id, numeric_id, None, child_bot_id)
                logger.info("[FAST %d] Banned user=%d in %d chats", worker_id, numeric_id, total_banned)
            else:
                if not already:
                    logger.info("[FAST %d] id=%d not in BL, enforcing unban anyway...", worker_id, numeric_id)
                    results["already_in_bl"] += 1
                
                banned = await _process_in_all_chats(owner_id, numeric_id, child_bot_id, action=action)
                total_banned = sum(banned.values())
                if total_banned > 0:
                    await _increment_blocked_count(owner_id, banned, is_global=(child_bot_id is None), is_unban=True)
                if already:
                    results["ok"] += 1
                    await _remove_from_blacklist(owner_id, numeric_id, child_bot_id)
                logger.info("[FAST %d] Unbanned user=%d in %d chats", worker_id, numeric_id, total_banned)

        except Exception:
            logger.exception("[FAST %d] Unexpected error for id=%s", worker_id, numeric_id)
            results["error"] += 1
        finally:
            queue.task_done()
            _report_progress(bot, results, notify_chat_id, status_msg_id)


# ══════════════════════════════════════════════════════════════════════════════
# SLOW WORKER — для @username (с API, RPM-лимитером и circuit breaker)
# ══════════════════════════════════════════════════════════════════════════════

async def _slow_worker(
    worker_id:      int,
    queue:          asyncio.Queue,
    session:        aiohttp.ClientSession,
    resolver:       BaseUsernameResolver,
    slow_bucket:    TokenBucket,
    owner_id:       int,
    child_bot_id:   Optional[int],
    stop_event:     asyncio.Event,
    api_dead_event: asyncio.Event,
    results:        dict[str, int],
    quota_box:      dict[str, Optional[int]],
    bot:            AioBot,
    notify_chat_id: int,
    status_msg_id:  int,
    action:         str,
    use_api:        bool = True,
) -> None:
    # Объявляем global в начале функции — Python требует это до первого использования
    global _api_flood_wait_until, _api_backoff_time
    while True:
        if stop_event.is_set():
            _drain_queue(queue)
            return

        if api_dead_event.is_set():
            _drain_queue(queue)
            return

        # ── Глобальный flood wait: ждём ПЕРЕД получением задачи ──────────
        # Это предотвращает немедленную повторную обработку юзера из очереди
        # после 429/timeout, когда он был возвращён через queue.put().
        _now = time.monotonic()
        if _api_flood_wait_until > _now:
            await asyncio.sleep(_api_flood_wait_until - _now)

        # ── 1. Получение задачи вне блока try...finally ──
        try:
            item = await asyncio.wait_for(queue.get(), timeout=1.0)
            username, _, retries = item if len(item) == 3 else (*item, 0)
        except asyncio.TimeoutError:
            continue
        except asyncio.CancelledError:
            return

        # ── 2. Процесс задачи с гарантированным task_done() ──
        try:
            already, local_id = await _smart_resolve(
                owner_id=owner_id,
                username=username,
                numeric_id=None,
                child_bot_id=child_bot_id,
            )
            # ОПТИМИЗАЦИЯ: пропускаем запись уже в ЧС — не тратим RPM-квоту RapidAPI
            # и не делаем повторный бан во всех чатах (самое затратное место).
            # До этой правки 3560 «Уже в ЧС» записей провоцировали одинаковое число
            # API-вызовов — отсюда торможение к 5000+ записям при повторной загрузке файла.
            if already and action == "ban":
                results["already_in_bl"] += 1
                logger.debug("[SLOW %d] @%s already in BL — skip API + re-ban", worker_id, username)
                continue  # finally: queue.task_done() отработает

            if not already and action == "unban":
                logger.info("[SLOW %d] @%s not in BL, enforcing unban anyway...", worker_id, username)

            resolved_id: Optional[int] = local_id

            # Если ID не нашли локально — идём в внешний API (только если разрешено)
            if resolved_id is None and not use_api:
                if action == "ban":
                    await _save_to_blacklist(owner_id, 0, username, child_bot_id)
                    await db.execute(
                        "UPDATE blacklist SET user_id = NULL WHERE owner_id=$1 "
                        "AND LOWER(username)=$2 AND child_bot_id IS NOT DISTINCT FROM $3 AND user_id=0",
                        owner_id, username.lower(), child_bot_id,
                    )
                    if already:
                        results["already_in_bl"] += 1
                    else:
                        results["ok"] += 1
                    logger.info("[SLOW %d] @%s queued for passive detection (no API)", worker_id, username)
                else:
                    if not already:
                        results["already_in_bl"] += 1
                    else:
                        results["ok"] += 1
                    logger.info("[SLOW %d] @%s not in BL locally, skipping unban (no API)", worker_id, username)
                continue  # finally: task_done() сработает

            if resolved_id is None:
                # Арендуем токен RPM bucket перед запросом к API
                await slow_bucket.acquire(jitter=TG_BAN_JITTER)
                try:
                    tg_id, quota = await asyncio.wait_for(
                        resolver.resolve(session, username),
                        timeout=15.0,  # Запас для внутреннего таймаута (14с)
                    )
                    resolved_id = tg_id

                    if quota is not None:
                        quota_box["remaining"] = quota
                    logger.info("[SLOW %d] @%s → %d (via provider)", worker_id, username, tg_id)
                    _api_backoff_time = 5.0  # Успех — сбрасываем кулдаун

                except asyncio.TimeoutError:
                    # Если юзер не резолвится за 14 секунд, он мертв/забанен Телеграмом.
                    # Не делаем повторов. Сбрасываем и идем дальше.
                    logger.info("[SLOW %d] @%s: RapidAPI timeout (14s) — skipping dead account", worker_id, username)
                    results["not_found"] += 1
                    await _save_resolve_error(owner_id, username, child_bot_id, "timeout")
                    continue

                except UserNotFoundError:
                    logger.info("[SLOW %d] @%s not found", worker_id, username)
                    await _save_resolve_error(owner_id, username, child_bot_id, "not_found")
                    results["not_found"] += 1
                    _api_backoff_time = 5.0  # 404 — это успешный и быстрый ответ от API
                    continue

                except RateLimitError as e:
                    if retries >= MAX_API_RETRIES:
                        logger.error("[SLOW %d] @%s: max retries (%d) exceeded on 429 — marking as error", worker_id, username, MAX_API_RETRIES)
                        results["error"] += 1
                        continue
                    # Exponential Backoff для 429 ошибки
                    wait = max(float(e.retry_after), _api_backoff_time) * (1.0 + random.uniform(0.0, 0.3))
                    logger.warning("[SLOW %d] Rate limit 429, sleeping %.1fs + jitter (attempt %d/%d)", worker_id, wait, retries + 1, MAX_API_RETRIES)
                    _api_flood_wait_until = time.monotonic() + wait
                    _api_backoff_time = min(60.0, _api_backoff_time * 2.0)
                    await queue.put((username, None, retries + 1))
                    continue

                except InvalidApiKeyError:
                    logger.error("[SLOW %d] Invalid API key — opening circuit breaker!", worker_id)
                    results["error"] += 1
                    api_dead_event.set()
                    stop_event.set()
                    asyncio.create_task(_send_key_error(bot, notify_chat_id, status_msg_id))
                    return # finally: task_done() сработает перед выходом из функции

                except aiohttp.ClientError as e:
                    logger.warning("[SLOW %d] Network error for @%s: %s", worker_id, username, e)
                    results["error"] += 1
                    continue

            if resolved_id:
                banned = await _process_in_all_chats(owner_id, resolved_id, child_bot_id, action=action)
                total_banned = sum(banned.values())
                
                if action == "ban":
                    if total_banned > 0:
                        await _increment_blocked_count(owner_id, banned, is_global=(child_bot_id is None))
                    await _save_to_blacklist(owner_id, resolved_id, username, child_bot_id)
                    if already:
                        results["already_in_bl"] += 1
                    else:
                        results["ok"] += 1
                    logger.info("[SLOW %d] Banned user=%d in %d chats", worker_id, resolved_id, total_banned)
                else:
                    if total_banned > 0:
                        await _increment_blocked_count(owner_id, banned, is_global=(child_bot_id is None), is_unban=True)
                    await _remove_from_blacklist(owner_id, resolved_id, child_bot_id)
                    if not already:
                        results["already_in_bl"] += 1
                    else:
                        results["ok"] += 1
                    logger.info("[SLOW %d] Unbanned user=%d in %d chats", worker_id, resolved_id, total_banned)
            else:
                results["error"] += 1

        except Exception:
            logger.exception("[SLOW %d] Unexpected error for @%s", worker_id, username)
            results["error"] += 1
        finally:
            queue.task_done()
            _report_progress(bot, results, notify_chat_id, status_msg_id)


# ══════════════════════════════════════════════════════════════════════════════
# BAN ENGINE — с Global FloodWait и TokenBucket
# ══════════════════════════════════════════════════════════════════════════════

async def _process_in_all_chats(
    owner_id:     int,
    tg_id:        int,
    child_bot_id: Optional[int],
    max_retries:  int = 3,
    action:       str = "ban",
) -> dict[int, int]:
    """
    Выполняет ban_chat_member или unban_chat_member во всех активных чатах с соблюдением:
    - Глобального TokenBucket (TG_BAN_RPS)
    - Глобального FloodWait event (пауза для ВСЕХ воркеров)
    - Exponential backoff при RetryAfter
    """
    from collections import defaultdict
    global _tg_flood_wait_until

    chats = await _get_chats_with_tokens(owner_id, child_bot_id)
    banned_by_bot: dict[int, int] = defaultdict(int)

    for chat in chats:
        chat_id: int = chat["chat_id"]
        token: str   = chat["token"]
        bot_id: int  = chat.get("child_bot_id")

        for attempt in range(max_retries):
            # ── Глобальный FloodWait: ждём если Telegram просил паузу ─────
            now = time.monotonic()
            if _tg_flood_wait_until > now:
                wait = _tg_flood_wait_until - now
                logger.warning("[BAN] Global FloodWait active, sleeping %.1fs", wait)
                await asyncio.sleep(wait)

            # ── TokenBucket: арендуем слот перед каждым API-вызовом ───────
            await _tg_bucket.acquire()

            try:
                async with AioBot(token=token).context() as child_bot:
                    if action == "ban":
                        await child_bot.ban_chat_member(
                            chat_id=chat_id,
                            user_id=tg_id,
                            revoke_messages=False,
                        )
                    else:
                        await child_bot.unban_chat_member(
                            chat_id=chat_id,
                            user_id=tg_id,
                            only_if_banned=True,
                        )
                if bot_id:
                    banned_by_bot[bot_id] += 1
                logger.debug("[PIPELINE] user=%d → chat=%d ✓ (%s)", tg_id, chat_id, action)
                break  # Успех

            except TelegramRetryAfter as e:
                # Устанавливаем глобальную паузу (защищает ВСЕ воркеры)
                jitter = random.uniform(1.0, 3.0)
                _tg_flood_wait_until = time.monotonic() + e.retry_after + jitter
                logger.warning(
                    "[BAN] FloodWait %ds+%.1f jitter for user=%d chat=%d (attempt %d/%d)",
                    e.retry_after, jitter, tg_id, chat_id, attempt + 1, max_retries,
                )
                await asyncio.sleep(e.retry_after + jitter)

            except TelegramForbiddenError:
                logger.warning("[BAN] Not admin in chat=%d, skipping", chat_id)
                break

            except TelegramBadRequest as e:
                err = str(e).lower()
                if any(x in err for x in ("user not found", "user_not_participant", "participant_id_invalid")):
                    pass  # Не в чате — нормально, запись в BL всё равно сохраняется
                else:
                    logger.error("[BAN] BadRequest user=%d chat=%d: %s", tg_id, chat_id, e)
                break

            except Exception as e:
                logger.exception("[BAN] Error user=%d chat=%d: %s", tg_id, chat_id, e)
                break

    return dict(banned_by_bot)


# ══════════════════════════════════════════════════════════════════════════════
# SMART RESOLVER — локальная БД перед API (экономим квоту)
# ══════════════════════════════════════════════════════════════════════════════

async def _smart_resolve(
    owner_id:     int,
    username:     Optional[str],
    numeric_id:   Optional[int],
    child_bot_id: Optional[int],
) -> tuple[bool, Optional[int]]:
    """
    Возвращает (already_in_bl, local_id).
    already_in_bl = True  → уже в ЧС (но ban sweep всё равно запустится)
    local_id      = int   → ID найден в локальных таблицах, API не нужен
    """
    clean_username = username.lower().lstrip("@") if username else None
    already_in_bl = False

    async with db.get_pool().acquire() as conn:
        # Шаг 1: есть ли уже в blacklist?
        if numeric_id:
            dup = await conn.fetchrow(
                "SELECT 1 FROM blacklist WHERE owner_id=$1 AND user_id=$2 "
                "AND child_bot_id IS NOT DISTINCT FROM $3 LIMIT 1",
                owner_id, numeric_id, child_bot_id,
            )
        elif clean_username:
            dup = await conn.fetchrow(
                "SELECT 1 FROM blacklist WHERE owner_id=$1 AND LOWER(username)=$2 "
                "AND child_bot_id IS NOT DISTINCT FROM $3 LIMIT 1",
                owner_id, clean_username, child_bot_id,
            )
        else:
            return False, None

        if dup:
            already_in_bl = True

        # Шаг 2: если ID уже есть — нет смысла искать дальше
        if not clean_username or numeric_id:
            return already_in_bl, numeric_id

        # Шаг 3: ищем ID в локальных таблицах (бесплатно, без API)
        local = await conn.fetchrow(
            """
            SELECT user_id FROM (
                SELECT user_id FROM bot_users
                    WHERE LOWER(username)=$1 AND user_id IS NOT NULL
                UNION ALL
                SELECT user_id FROM platform_users
                    WHERE LOWER(username)=$1 AND user_id IS NOT NULL
                UNION ALL
                SELECT user_id FROM blacklist
                    WHERE LOWER(username)=$1 AND user_id IS NOT NULL
            ) src LIMIT 1
            """,
            clean_username,
        )
        if local:
            return already_in_bl, int(local["user_id"])

    return already_in_bl, None


# ══════════════════════════════════════════════════════════════════════════════
# DB HELPERS
# ══════════════════════════════════════════════════════════════════════════════

async def _save_to_blacklist(
    owner_id: int, tg_id: int, source_username: Optional[str], child_bot_id: Optional[int],
) -> None:
    # Сначала пытаемся обновить существующую запись (вставленную при импорте файла),
    # у которой есть только username, но user_id = NULL
    if source_username:
        try:
            await db.execute(
                """
                UPDATE blacklist 
                SET user_id = $1 
                WHERE owner_id = $2 
                  AND lower(username) = $3
                  AND child_bot_id IS NOT DISTINCT FROM $4
                  AND user_id IS NULL
                """,
                tg_id, owner_id, source_username.lower().lstrip("@"), child_bot_id
            )
        except Exception as _upd_err:
            # Редкий случай: unique constraint — user_id уже есть в другой записи.
            # Безопасно игнорируем; INSERT ниже тоже сделает ON CONFLICT DO NOTHING.
            logger.debug("_save_to_blacklist UPDATE skipped (constraint): %s", _upd_err)

    # Затем пытаемся вставить новую запись (если её вообще не было)
    await db.execute(
        """
        INSERT INTO blacklist (owner_id, user_id, username, source_username, child_bot_id)
        VALUES ($1, $2, $3, $4, $5)
        ON CONFLICT DO NOTHING
        """,
        owner_id, tg_id, source_username, source_username, child_bot_id,
    )


async def _save_resolve_error(
    owner_id: int, username: str, child_bot_id: Optional[int], error: str,
) -> None:
    await db.execute(
        """
        INSERT INTO blacklist (owner_id, username, source_username, resolve_error, child_bot_id)
        VALUES ($1, $2, $3, $4, $5)
        ON CONFLICT DO NOTHING
        """,
        owner_id, username.lower().lstrip("@"), username, error, child_bot_id,
    )

async def _remove_from_blacklist(
    owner_id: int, tg_id: int, child_bot_id: Optional[int],
) -> None:
    await db.execute(
        "DELETE FROM blacklist WHERE owner_id=$1 AND user_id=$2 AND child_bot_id IS NOT DISTINCT FROM $3",
        owner_id, tg_id, child_bot_id,
    )


async def _increment_blocked_count(
    owner_id:      int,
    banned_by_bot: dict[int, int],
    is_global:     bool,
    is_unban:      bool = False,
) -> None:
    total_count = sum(banned_by_bot.values())
    if total_count == 0:
        return

    mult = -1 if is_unban else 1
    total_change = total_count * mult

    await db.execute(
        "UPDATE platform_users SET blocked_count = GREATEST(0, blocked_count + $1) WHERE user_id = $2",
        total_change, owner_id,
    )

    updates = [(mult * count, bot_id) for bot_id, count in banned_by_bot.items() if bot_id]
    if not updates:
        return

    if is_global:
        # Глобальный ЧС: только global_blocked_count, NOT local blocked_count
        await db.executemany(
            "UPDATE child_bots SET global_blocked_count = GREATEST(0, global_blocked_count + $1) WHERE id = $2",
            updates,
        )
    else:
        await db.executemany(
            "UPDATE child_bots SET blocked_count = GREATEST(0, blocked_count + $1) WHERE id = $2",
            updates,
        )


# ══════════════════════════════════════════════════════════════════════════════
# PROGRESS BAR & STATUS EDIT
# ══════════════════════════════════════════════════════════════════════════════

def _report_progress(
    bot: AioBot, results: dict, notify_chat_id: int, status_msg_id: int,
) -> None:
    now  = time.monotonic()
    done = sum(results[k] for k in ("ok", "not_found", "error", "already_in_bl"))
    last = _last_report.get(status_msg_id, 0.0)
    if (now - last) < PROGRESS_INTERVAL and done < results["total"]:
        return
    _last_report[status_msg_id] = now
    asyncio.create_task(_edit_status(
        bot, notify_chat_id, status_msg_id,
        f"⏳ Обработано <b>{done}/{results['total']}</b>\n"
        f"✅ Забанено: {results['ok']} | "
        f"🔄 Уже в ЧС: {results['already_in_bl']}\n"
        f"❓ Не найдено: {results['not_found']} | "
        f"⚠️ Ошибки: {results['error']}",
        show_stop=True,
    ))


async def _edit_status(
    bot:       AioBot,
    chat_id:   int,
    msg_id:    int,
    text:      str,
    show_stop: bool = True,
    markup:    Optional["InlineKeyboardMarkup"] = None,
) -> None:
    from keyboards.stop_pipeline import stop_keyboard
    from aiogram.types import InlineKeyboardMarkup
    final_markup = stop_keyboard(msg_id) if show_stop else markup
    try:
        await bot.edit_message_text(
            chat_id=chat_id, message_id=msg_id,
            text=text, parse_mode="HTML", reply_markup=final_markup,
        )
    except Exception:
        pass


async def _send_key_error(bot: AioBot, chat_id: int, msg_id: int) -> None:
    await _edit_status(
        bot, chat_id, msg_id,
        "❌ <b>Ошибка API-ключа</b>\n"
        "RapidAPI вернул 403 Forbidden.\n"
        "Обновите ключ в <b>Настройки → RapidAPI</b>.",
        show_stop=False,
    )


def _drain_queue(queue: asyncio.Queue) -> None:
    """Быстро сливает очередь без deadlock на queue.join()."""
    while not queue.empty():
        try:
            queue.get_nowait()
            queue.task_done()
        except asyncio.QueueEmpty:
            break
