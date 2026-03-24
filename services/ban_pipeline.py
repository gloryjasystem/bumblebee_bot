"""
services/ban_pipeline.py — Главный асинхронный конвейер бана пользователей.

Архитектура (Producer-Consumer pattern + Smart Resolver):
  ┌──────────────────────────────────────────────────────┐
  │  start_ban_pipeline()                                │
  │  ├─ Дедупликация входных данных (set())              │
  │  ├─ Наполняет asyncio.Queue парами (username, id)   │
  │  ├─ Запускает N воркеров (_worker)                  │
  │  ├─ Регистрирует stop_event в active_pipelines       │
  │  └─ В finally: сохраняет квоту, чистит реестр        │
  │                                                      │
  │  _worker() × CONCURRENCY — 4-шаговый Smart Resolver │
  │  ├─ Шаг 1: Deduplication — уже есть в ЧС? → skip   │
  │  ├─ Шаг 2: Local DB Resolution — UNION ALL query    │
  │  ├─ Шаг 3: External API (RapidAPI) — только fallback│
  │  └─ Шаг 4: Background Sweep — бан + сохранение      │
  │                                                      │
  │  _ban_in_all_chats()                                │
  │  ├─ До 3 попыток при TelegramRetryAfter             │
  │  └─ Микро-пауза BAN_DELAY между каждым баном        │
  └──────────────────────────────────────────────────────┘

Graceful Shutdown:
  active_pipelines[status_msg_id] = asyncio.Event()
  Handler кнопки «Стоп» вызывает event.set() и сливает очередь.
  Воркеры проверяют is_set() и выходят без deadlock на queue.join().

Rate Limiting:
  - RapidAPI: DELAY_BETWEEN = 0.3s между запросами внутри воркера.
  - Telegram:  BAN_DELAY    = 0.05s между ban_chat_member + retry при 429.
  - Прогресс-бар: не чаще PROGRESS_INTERVAL = 3.0s.
"""
import asyncio
import logging
import time
from typing import Optional

import aiohttp
from aiogram import Bot
from aiogram.exceptions import (
    TelegramBadRequest,
    TelegramForbiddenError,
    TelegramRetryAfter,
)

import db.pool as db
from services.rapidapi_client import (
    InvalidApiKeyError,
    RateLimitError,
    UserNotFoundError,
    username_to_id,
)
from services.settings import save_quota
from services.blacklist import _get_chats_with_tokens  # переиспользуем существующую логику

logger = logging.getLogger(__name__)

# ── Настраиваемые константы ───────────────────────────────────────────────────
CONCURRENCY      = 5    # параллельных воркеров (ограничивает нагрузку на RapidAPI)
DELAY_BETWEEN    = 0.3  # секунд между запросами к RapidAPI внутри одного воркера
BAN_DELAY        = 0.05 # секунд между отдельными вызовами ban_chat_member
BAN_MAX_RETRIES  = 3    # максимум попыток при TelegramRetryAfter
PROGRESS_INTERVAL = 3.0 # минимальный интервал обновления прогресс-бара (секунды)

# ── Реестр активных пайплайнов для Graceful Shutdown ─────────────────────────
# Ключ: message_id сообщения с прогресс-баром (уникален в пределах чата).
# Значение: asyncio.Event — handler кнопки «Стоп» взведёт его через .set().
active_pipelines: dict[int, asyncio.Event] = {}


# ── Точка входа ───────────────────────────────────────────────────────────────

async def start_ban_pipeline(
    bot:             Bot,
    owner_id:        int,
    usernames:       list[str],
    numeric_ids:     list[int],
    notify_chat_id:  int,
    status_msg_id:   int,
    child_bot_id:    Optional[int] = None,
) -> None:
    """
    Запускает асинхронный конвейер бана.

    Вызывать через asyncio.create_task() из handler'а, чтобы не блокировать
    event loop бота на время обработки тысяч записей.

    Args:
        bot:            Экземпляр главного бота (для редактирования статус-сообщения).
        owner_id:       Telegram ID владельца / администратора.
        usernames:      Список @username для конвертации через RapidAPI.
        numeric_ids:    Список числовых ID (идут в обход API напрямую к бану).
        notify_chat_id: ID чата, в котором обновляем прогресс-бар.
        status_msg_id:  ID сообщения с прогресс-баром (привязка Graceful Shutdown).
        child_bot_id:   None → глобальный ЧС (все боты), int → конкретный бот.
    """
    # ── Шаг 0: Дедупликация на входе (устраняет Race Condition внутри батча) ────
    # set() убирает дубли прямо в памяти, до попадания в очередь.
    # Если 5 воркеров одновременно взяли бы одинаковый username — было бы 5 API-вызовов.
    # Теперь каждый уникальный username войдёт в очередь РОВНО 1 РАЗ.
    unique_usernames: list[str] = list(dict.fromkeys(u.lower().lstrip("@") for u in usernames))
    unique_ids:       list[int] = list(dict.fromkeys(numeric_ids))

    total = len(unique_usernames) + len(unique_ids)
    queue: asyncio.Queue[tuple[Optional[str], Optional[int]]] = asyncio.Queue()
    stop_event = asyncio.Event()

    # Счётчики прогресса (разделяемый dict между воркерами без локов — GIL защищает)
    results: dict[str, int] = {
        "ok":         0,
        "not_found":  0,
        "error":      0,
        "already_in_bl": 0,
        "total":      total,
    }
    # Последний известный остаток квоты RapidAPI — обновляется из каждого ответа
    quota_box: dict[str, Optional[int]] = {"remaining": None}

    # ── Регистрируем пайплайн в реестре graceful shutdown ─────────────────────
    active_pipelines[status_msg_id] = stop_event

    # Наполняем очередь: (username, None) или (None, numeric_id)
    for u in unique_usernames:
        await queue.put((u, None))
    for uid in unique_ids:
        await queue.put((None, uid))

    logger.info(
        "[PIPELINE] Started: owner=%d total=%d (usernames=%d, ids=%d, dupes_skipped=%d)",
        owner_id, total, len(unique_usernames), len(unique_ids),
        (len(usernames) + len(numeric_ids)) - total,
    )

    connector = aiohttp.TCPConnector(limit=10, ttl_dns_cache=300, ssl=True)
    try:
        async with aiohttp.ClientSession(connector=connector) as session:
            workers = [
                asyncio.create_task(
                    _worker(
                        worker_id=i,
                        queue=queue,
                        session=session,
                        bot=bot,
                        owner_id=owner_id,
                        child_bot_id=child_bot_id,
                        stop_event=stop_event,
                        results=results,
                        quota_box=quota_box,
                        notify_chat_id=notify_chat_id,
                        status_msg_id=status_msg_id,
                    )
                )
                for i in range(CONCURRENCY)
            ]
            await queue.join()
            for w in workers:
                w.cancel()

    finally:
        # ── Сохраняем квоту ОДИН РАЗ — не при каждом API-запросе ─────────────
        if quota_box["remaining"] is not None:
            await save_quota(quota_box["remaining"])

        # ── Чистим реестр — защита от утечки памяти ───────────────────────────
        active_pipelines.pop(status_msg_id, None)
        # Чистим таймштамп прогресс-бара — он больше не нужен
        _last_report.pop(status_msg_id, None)

        logger.info(
            "[PIPELINE] Done: owner=%d ok=%d not_found=%d error=%d quota=%s",
            owner_id,
            results["ok"], results["not_found"], results["error"],
            quota_box["remaining"],
        )


    # ── Финальное сообщение ───────────────────────────────────────────────────
    from aiogram.types import InlineKeyboardMarkup, InlineKeyboardButton
    
    # Формируем кнопку возврата: в локальный ЧС бота или в глобальный ЧС владельца
    back_cb = f"bs_blacklist:{child_bot_id}" if child_bot_id else f"ga_bl:{owner_id}"
    back_markup = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="◀️ Назад в ЧС", callback_data=back_cb)]
    ])

    if stop_event.is_set():
        done = results["ok"] + results["not_found"] + results["error"] + results["already_in_bl"]
        await _edit_status(
            bot, notify_chat_id, status_msg_id,
            f"⚠️ <b>Процесс прерван</b> вручную.\n"
            f"Успело обработаться: <b>{done}/{total}</b>\n\n"
            f"✅ Новых забанено: <b>{results['ok']}</b>\n"
            f"🔄 Уже в базе: <b>{results['already_in_bl']}</b>\n"
            f"❓ Не найдено: <b>{results['not_found']}</b>\n"
            f"⚠️ Ошибки: <b>{results['error']}</b>",
            show_stop=False,
            markup=back_markup,
        )
    else:
        await _edit_status(
            bot, notify_chat_id, status_msg_id,
            f"✅ <b>Готово!</b>\n\n"
            f"├ Новых забанено: <b>{results['ok']}</b>\n"
            f"├ Уже в базе: <b>{results['already_in_bl']}</b>\n"
            f"├ Не найдено: <b>{results['not_found']}</b>\n"
            f"└ Ошибки: <b>{results['error']}</b>",
            show_stop=False,
            markup=back_markup,
        )


# ── Воркер ────────────────────────────────────────────────────────────────────

async def _worker(
    worker_id:      int,
    queue:          asyncio.Queue,
    session:        aiohttp.ClientSession,
    bot:            Bot,
    owner_id:       int,
    child_bot_id:   Optional[int],
    stop_event:     asyncio.Event,
    results:        dict[str, int],
    quota_box:      dict[str, Optional[int]],
    notify_chat_id: int,
    status_msg_id:  int,
) -> None:
    """
    Один воркер конвейера. Работает в бесконечном цикле до опустошения очереди.

    На каждой итерации:
      1. Проверяет stop_event — если взведён, сливает очередь и выходит.
      2. Берёт задание из очереди.
      3. Если задание — username: конвертирует через RapidAPI.
      4. Банит в все активные чаты владельца.
      5. Записывает результат в БД (blacklist).
    """
    while True:
        # ── Проверка сигнала остановки ─────────────────────────────────────────
        if stop_event.is_set():
            # Сливаем оставшиеся задания, чтобы queue.join() не завис
            while not queue.empty():
                try:
                    queue.get_nowait()
                    queue.task_done()
                except asyncio.QueueEmpty:
                    break
            logger.info("[WORKER %d] Stop signal received, exiting", worker_id)
            return

        # Берём следующее задание (блокирующий await)
        try:
            username, numeric_id = await asyncio.wait_for(queue.get(), timeout=1.0)
        except asyncio.TimeoutError:
            # Очередь временно пуста, проверим stop_event снова
            continue

        # task_done() вызывается ВСЕГДА через finally — защита от deadlock queue.join()
        _handled = False
        try:
            resolved_id: Optional[int] = numeric_id

            # ── Шаг 1 & 2: Smart Resolve (Dedup + Local DB) ───────────────────
            # Проверяем: юзер уже в ЧС? Знаем его ID без API?
            if username or resolved_id:
                already, local_id = await _smart_resolve(
                    owner_id=owner_id,
                    username=username,
                    numeric_id=resolved_id,
                    child_bot_id=child_bot_id,
                )
                if already:
                    # Шаг 1 сработал: дубликат — пропускаем без API
                    logger.info(
                        "[WORKER %d] %s/%s already in blacklist, skipping",
                        worker_id, username, resolved_id,
                    )
                    results["already_in_bl"] += 1
                    _handled = True
                elif local_id is not None:
                    # Шаг 2 сработал: ID найден в локальных таблицах, API не трогаем
                    logger.info(
                        "[WORKER %d] @%s → %d (resolved locally, no API call)",
                        worker_id, username, local_id,
                    )
                    resolved_id = local_id

            # ── Шаг 3: External API (только если не удалось разрешить локально) ─
            if not _handled and username and resolved_id is None:
                try:
                    tg_id, quota = await username_to_id(session, username)
                    resolved_id = tg_id

                    # Обновляем последнее известное значение квоты
                    if quota is not None:
                        quota_box["remaining"] = quota

                    logger.info("[WORKER %d] @%s → %d (via RapidAPI)", worker_id, username, tg_id)

                except UserNotFoundError:
                    logger.info("[WORKER %d] @%s not found", worker_id, username)
                    await _save_resolve_error(owner_id, username, child_bot_id, "not_found")
                    results["not_found"] += 1
                    _report_progress(bot, results, notify_chat_id, status_msg_id)
                    _handled = True

                except RateLimitError as e:
                    logger.warning("[WORKER %d] Rate limit 429, sleep %ds", worker_id, e.retry_after)
                    await asyncio.sleep(e.retry_after)
                    # Возвращаем задание в очередь для повторной попытки
                    await queue.put((username, None))
                    _handled = True

                except InvalidApiKeyError:
                    logger.error("[WORKER %d] Invalid API key — stopping pipeline", worker_id)
                    results["error"] += 1
                    stop_event.set()  # Останавливаем весь пайплайн
                    asyncio.create_task(_send_key_error(bot, notify_chat_id, status_msg_id))
                    _handled = True

                except aiohttp.ClientError as e:
                    logger.warning("[WORKER %d] Network error for @%s: %s", worker_id, username, e)
                    results["error"] += 1
                    _handled = True

            # ── Шаг 4: Ban Sweep + сохранение в БД ────────────────────────────
            if not _handled:
                if resolved_id:
                    banned_by_bot = await _ban_in_all_chats(owner_id, resolved_id, child_bot_id)

                    # Сохраняем запись в blacklist (ON CONFLICT DO NOTHING — безопасно)
                    await _save_to_blacklist(
                        owner_id, resolved_id, username, child_bot_id
                    )

                    total_banned = sum(banned_by_bot.values())
                    # Обновляем счётчик blocked_count для аналитики
                    if total_banned > 0:
                        await _increment_blocked_count(owner_id, banned_by_bot, is_global=(child_bot_id is None))

                    results["ok"] += 1
                    logger.info(
                        "[WORKER %d] Banned user=%d in %d chats",
                        worker_id, resolved_id, total_banned,
                    )
                else:
                    # Пришёл None numeric_id и None username — не должно случиться
                    logger.warning("[WORKER %d] Empty task received, skipping", worker_id)
                    results["error"] += 1

        except Exception as e:
            logger.exception(
                "[WORKER %d] Unexpected error for %s/%s: %s",
                worker_id, username, numeric_id, e,
            )
            results["error"] += 1

        finally:
            # КРИТИЧНО: task_done() должен вызываться ВСЕГДА для каждого queue.get()
            # Без этого queue.join() зависнет при любом continue/break выше.
            queue.task_done()

        # Микро-пауза и обновление прогресс-бара — вне try/finally
        await asyncio.sleep(DELAY_BETWEEN)
        _report_progress(bot, results, notify_chat_id, status_msg_id)


# ── Smart Resolver — 4-шаговое разрешение без API ────────────────────────────

async def _smart_resolve(
    owner_id:     int,
    username:     Optional[str],
    numeric_id:   Optional[int],
    child_bot_id: Optional[int],
) -> tuple[bool, Optional[int]]:
    """
    Шаг 1 + Шаг 2 умного резолвера.

    Returns:
        (already_in_bl, local_id)
        - already_in_bl: True  → запись уже есть в blacklist → пропустить обработку
        - local_id:      int   → ID найден в локальных таблицах → API не нужен
                         None  → нужен внешний RapidAPI вызов
    """
    clean_username = username.lower().lstrip("@") if username else None

    async with db.get_pool().acquire() as conn:
        # ── Шаг 1: Deduplication Check ────────────────────────────────────────
        # Проверяем, есть ли user_id ИЛИ username в целевом ЧС.
        # child_bot_id IS NOT DISTINCT FROM $3 корректно сравнивает NULL = NULL (ANSI SQL).
        if numeric_id:
            dup_row = await conn.fetchrow(
                """
                SELECT 1 FROM blacklist
                WHERE owner_id = $1
                  AND user_id = $2
                  AND child_bot_id IS NOT DISTINCT FROM $3
                LIMIT 1
                """,
                owner_id, numeric_id, child_bot_id,
            )
        elif clean_username:
            dup_row = await conn.fetchrow(
                """
                SELECT 1 FROM blacklist
                WHERE owner_id = $1
                  AND LOWER(username) = $2
                  AND child_bot_id IS NOT DISTINCT FROM $3
                LIMIT 1
                """,
                owner_id, clean_username, child_bot_id,
            )
        else:
            return False, None

        if dup_row:
            return True, None  # Дубликат — пропускаем

        # ── Шаг 2: Local DB Resolution (только если username, без ID) ────────
        # Одно обращение к БД: UNION ALL по трём таблицам + LIMIT 1.
        # ВАЖНО: для оптимальной производительности на больших данных необходимы
        # функциональные индексы: CREATE INDEX ON bot_users (LOWER(username));
        if not clean_username or numeric_id:
            # ID уже известен или username не предоставлен — пропускаем локальный поиск
            return False, numeric_id

        local_row = await conn.fetchrow(
            """
            SELECT user_id FROM (
                SELECT user_id FROM bot_users
                WHERE LOWER(username) = $1 AND user_id IS NOT NULL
                UNION ALL
                SELECT user_id FROM platform_users
                WHERE LOWER(username) = $1 AND user_id IS NOT NULL
                UNION ALL
                SELECT user_id FROM blacklist
                WHERE LOWER(username) = $1 AND user_id IS NOT NULL
            ) src
            LIMIT 1
            """,
            clean_username,
        )

        if local_row:
            return False, int(local_row["user_id"])

    return False, None  # Нужен внешний API-запрос



# ── Бан в чатах ───────────────────────────────────────────────────────────────

async def _ban_in_all_chats(
    owner_id:     int,
    tg_id:        int,
    child_bot_id: Optional[int],
) -> dict[int, int]:
    """
    Банит tg_id во всех активных чатах владельца (согласно scope).

    Переиспользует _get_chats_with_tokens из services.blacklist — там уже
    реализована вся логика получения токенов дочерних ботов и соблюдения ga_selected_bots.

    Защита от Telegram flood:
      - Микро-пауза BAN_DELAY после каждого бана.
      - TelegramRetryAfter перехватывается с повтором до BAN_MAX_RETRIES раз.

    Returns:
        Словарь { child_bot_id: количество_успешных_банов }
    """
    from aiogram import Bot as AioBot
    from collections import defaultdict

    chats = await _get_chats_with_tokens(owner_id, child_bot_id)
    banned_by_bot = defaultdict(int)

    for chat in chats:
        chat_id: int   = chat["chat_id"]
        token:   str   = chat["token"]
        bot_id:  int   = chat.get("child_bot_id")

        attempt = 0
        while attempt < BAN_MAX_RETRIES:
            try:
                async with AioBot(token=token).context() as child_bot:
                    await child_bot.ban_chat_member(
                        chat_id=chat_id,
                        user_id=tg_id,
                        revoke_messages=False,
                    )
                if bot_id:
                    banned_by_bot[bot_id] += 1
                logger.debug("[BAN] user=%d → chat=%d ✓", tg_id, chat_id)
                break  # успех — следующий чат

            except TelegramRetryAfter as e:
                wait = e.retry_after + 1
                logger.warning(
                    "[BAN] RetryAfter %ds for user=%d chat=%d (attempt %d/%d)",
                    wait, tg_id, chat_id, attempt + 1, BAN_MAX_RETRIES,
                )
                await asyncio.sleep(wait)
                attempt += 1

            except TelegramForbiddenError:
                # Бот более не является администратором — пропускаем
                logger.warning("[BAN] Bot is not admin in chat=%d, skipping", chat_id)
                break

            except TelegramBadRequest as e:
                err = str(e).lower()
                if any(x in err for x in ("user not found", "user_not_participant", "participant_id_invalid")):
                    pass  # Пользователь не в чате / никогда не был в чате — ок, запись в ЧС всё равно есть
                else:
                    logger.error("[BAN] BadRequest user=%d chat=%d: %s", tg_id, chat_id, e)
                break

            except Exception as e:
                logger.exception("[BAN] Error user=%d chat=%d: %s", tg_id, chat_id, e)
                break

        # Микро-пауза между банами: 5 воркеров × 20 чатов × 0.05s = 5 req/s — безопасно
        await asyncio.sleep(BAN_DELAY)

    return dict(banned_by_bot)


# ── Вспомогательные DB-функции ────────────────────────────────────────────────

async def _save_to_blacklist(
    owner_id:     int,
    tg_id:        int,
    source_username: Optional[str],
    child_bot_id: Optional[int],
) -> None:
    """
    Сохраняет или обновляет запись в таблице blacklist.
    Если запись с user_id уже существует — дополняем source_username.
    """
    await db.execute(
        """
        INSERT INTO blacklist (owner_id, user_id, username, source_username, child_bot_id)
        VALUES ($1, $2, $3, $4, $5)
        ON CONFLICT DO NOTHING
        """,
        owner_id,
        tg_id,
        source_username,
        source_username,
        child_bot_id,
    )


async def _save_resolve_error(
    owner_id:     int,
    username:     str,
    child_bot_id: Optional[int],
    error:        str,
) -> None:
    """Сохраняет запись о неудачном резолве (username не найден в Telegram)."""
    await db.execute(
        """
        INSERT INTO blacklist (owner_id, username, source_username, resolve_error, child_bot_id)
        VALUES ($1, $2, $3, $4, $5)
        ON CONFLICT DO NOTHING
        """,
        owner_id,
        username.lower().lstrip("@"),  # username (VARCHAR)
        username,                      # source_username (TEXT)
        error,
        child_bot_id,
    )


async def _increment_blocked_count(
    owner_id:      int,
    banned_by_bot: dict[int, int],
    is_global:     bool,
) -> None:
    """Инкрементирует счётчики blocked_count для аналитики (с учётом выборки ботов)."""
    total_count = sum(banned_by_bot.values())
    if total_count == 0:
        return

    # Обновляем общую статистику владельца на платформе
    await db.execute(
        "UPDATE platform_users SET blocked_count = blocked_count + $1 WHERE user_id = $2",
        total_count, owner_id,
    )

    # Массово обновляем статистику всех участвовавших в бане ботов
    # Если это глобальный ЧС (is_global=True), то обновляем И global_blocked_count тоже
    updates = [(count, bot_id) for bot_id, count in banned_by_bot.items() if bot_id]
    if not updates:
        return

    if is_global:
        await db.executemany(
            "UPDATE child_bots SET blocked_count = blocked_count + $1, global_blocked_count = global_blocked_count + $1 WHERE id = $2",
            updates,
        )
    else:
        await db.executemany(
            "UPDATE child_bots SET blocked_count = blocked_count + $1 WHERE id = $2",
            updates,
        )


# ── Прогресс-бар ──────────────────────────────────────────────────────────────

# Таймштамп последнего обновления прогресс-бара (per message_id)
_last_report: dict[int, float] = {}


def _report_progress(
    bot:            Bot,
    results:        dict[str, int],
    notify_chat_id: int,
    status_msg_id:  int,
) -> None:
    """
    Планирует обновление прогресс-бара не чаще PROGRESS_INTERVAL секунд.

    Используется time.monotonic() вместо счётчика итераций — защита от
    TelegramRetryAfter при быстрой обработке числовых ID (без API-вызовов).
    """
    now  = time.monotonic()
    done = results["ok"] + results["not_found"] + results["error"] + results["already_in_bl"]
    total = results["total"]

    last = _last_report.get(status_msg_id, 0.0)
    if (now - last) < PROGRESS_INTERVAL and done < total:
        return

    _last_report[status_msg_id] = now
    asyncio.create_task(
        _edit_status(
            bot, notify_chat_id, status_msg_id,
            f"⏳ Обработано <b>{done}/{total}</b>\n"
            f"✅ Забанено: {results['ok']} | "
            f"🔄 Уже в ЧС: {results['already_in_bl']}\n"
            f"❓ Не найдено: {results['not_found']} | "
            f"⚠️ Ошибки: {results['error']}",
            show_stop=True,
        )
    )


async def _edit_status(
    bot:       Bot,
    chat_id:   int,
    msg_id:    int,
    text:      str,
    show_stop: bool = True,
    markup:    Optional['InlineKeyboardMarkup'] = None,
) -> None:
    """
    Редактирует сообщение-статус с прогрессом.
    При show_stop=True прикрепляет кнопку «Остановить».
    Если show_stop=False и передан markup, использует его.
    Ошибки редактирования игнорируются (сообщение могло быть удалено).
    """
    from keyboards.stop_pipeline import stop_keyboard  # импорт здесь избегает цикла
    from aiogram.types import InlineKeyboardMarkup

    final_markup = stop_keyboard(msg_id) if show_stop else markup

    try:
        await bot.edit_message_text(
            chat_id=chat_id,
            message_id=msg_id,
            text=text,
            parse_mode="HTML",
            reply_markup=final_markup,
        )
    except Exception:
        pass  # Сообщение удалено или не изменилось — тихо игнорируем


async def _send_key_error(bot: Bot, chat_id: int, msg_id: int) -> None:
    """Отправляет уведомление об ошибке API-ключа и снимает кнопку Стоп."""
    await _edit_status(
        bot, chat_id, msg_id,
        "❌ <b>Ошибка API-ключа</b>\n"
        "RapidAPI вернул 403 Forbidden.\n"
        "Обновите ключ в <b>Настройки платформы → RapidAPI</b>.",
        show_stop=False,
    )
