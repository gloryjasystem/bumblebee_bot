"""
api/server.py — FastAPI сервер с lifespan для Railway.
/health отвечает сразу после старта — ДО инициализации бота.
"""
import json
import logging
from contextlib import asynccontextmanager

from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse
from fastapi.staticfiles import StaticFiles
from aiogram import Bot, Dispatcher
from aiogram.types import Update

import db.pool as db
from services.security import verify_init_data, verify_nowpayments_sig
from services.payment_service import (
    create_invoice, activate_tariff, get_payment_status,
    notify_user_paid, notify_owner_payment,
)

logger = logging.getLogger(__name__)

_bot: Bot = None
_dp: Dispatcher = None


def create_app(bot: Bot, dp: Dispatcher) -> FastAPI:
    global _bot, _dp
    _bot = bot
    _dp = dp

    @asynccontextmanager
    async def lifespan(app: FastAPI):
        """
        Запускается ВНУТРИ уже стартовавшего uvicorn сервера.
        /health уже доступен до этого момента — Railway healthcheck пройдёт.
        """
        # ── Startup ──────────────────────────────────────────
        logger.info("Lifespan startup...")
        await db.create_pool()
        logger.info("DB pool created")

        # Применяем схему
        with open("db/init.sql", "r", encoding="utf-8") as f:
            sql = f.read()
        async with db.get_pool().acquire() as conn:
            await conn.execute(sql)
            # Идемпотентные миграции для новых колонок
            await conn.execute(
                "ALTER TABLE child_bots ADD COLUMN IF NOT EXISTS verify_only BOOLEAN DEFAULT false"
            )
            # Обратная связь на уровне бота
            for migration in [
                "ALTER TABLE child_bots ADD COLUMN IF NOT EXISTS feedback_enabled BOOLEAN DEFAULT false",
                "ALTER TABLE child_bots ADD COLUMN IF NOT EXISTS feedback_target  TEXT    DEFAULT 'owner'",
                "ALTER TABLE child_bots ADD COLUMN IF NOT EXISTS feedback_lang    TEXT    DEFAULT 'ru'",
            ]:
                await conn.execute(migration)
            # Новые колонки рассылки
            for migration in [
                "ALTER TABLE mailings ADD COLUMN IF NOT EXISTS notify_users       BOOLEAN DEFAULT true",
                "ALTER TABLE mailings ADD COLUMN IF NOT EXISTS protect_content     BOOLEAN DEFAULT false",
                "ALTER TABLE mailings ADD COLUMN IF NOT EXISTS pin_message         BOOLEAN DEFAULT false",
                "ALTER TABLE mailings ADD COLUMN IF NOT EXISTS delete_after_send   BOOLEAN DEFAULT false",
                "ALTER TABLE mailings ADD COLUMN IF NOT EXISTS disable_preview     BOOLEAN DEFAULT false",
                "ALTER TABLE mailings ADD COLUMN IF NOT EXISTS url_buttons_raw     TEXT",
                "ALTER TABLE mailings ADD COLUMN IF NOT EXISTS button_color        TEXT DEFAULT 'blue'",
                "ALTER TABLE mailings ADD COLUMN IF NOT EXISTS media_below         BOOLEAN DEFAULT false",
                # Bot-level mailing support
                "ALTER TABLE mailings ADD COLUMN IF NOT EXISTS child_bot_id        INTEGER REFERENCES child_bots(id) ON DELETE CASCADE",
                "ALTER TABLE mailings ALTER COLUMN chat_id DROP NOT NULL",
            ]:
                await conn.execute(migration)
            # Новые колонки bot_chats (раздел Сообщения)
            for migration in [
                "ALTER TABLE bot_chats ADD COLUMN IF NOT EXISTS captcha_type         TEXT    DEFAULT 'off'",
                "ALTER TABLE bot_chats ADD COLUMN IF NOT EXISTS captcha_text         TEXT",
                "ALTER TABLE bot_chats ADD COLUMN IF NOT EXISTS captcha_buttons_raw  TEXT",
                "ALTER TABLE bot_chats ADD COLUMN IF NOT EXISTS captcha_lang         TEXT    DEFAULT 'off'",
                "ALTER TABLE bot_chats ADD COLUMN IF NOT EXISTS captcha_animation    BOOLEAN DEFAULT false",
                "ALTER TABLE bot_chats ADD COLUMN IF NOT EXISTS captcha_button_style TEXT    DEFAULT 'inline'",
                "ALTER TABLE bot_chats ADD COLUMN IF NOT EXISTS captcha_timer_min    INT     DEFAULT 1",
                "ALTER TABLE bot_chats ADD COLUMN IF NOT EXISTS captcha_emoji_set    TEXT    DEFAULT '🍕🍔🌭🌮'",
                "ALTER TABLE bot_chats ADD COLUMN IF NOT EXISTS captcha_greet        BOOLEAN DEFAULT false",
                "ALTER TABLE bot_chats ADD COLUMN IF NOT EXISTS captcha_accept_now   BOOLEAN DEFAULT false",
                "ALTER TABLE bot_chats ADD COLUMN IF NOT EXISTS captcha_accept_all   BOOLEAN DEFAULT false",
                "ALTER TABLE bot_chats ADD COLUMN IF NOT EXISTS captcha_anim_file_id TEXT",
                "ALTER TABLE bot_chats ADD COLUMN IF NOT EXISTS captcha_anim_type    VARCHAR(16)",
                "ALTER TABLE bot_chats ADD COLUMN IF NOT EXISTS typing_action        BOOLEAN DEFAULT false",
                "ALTER TABLE bot_chats ADD COLUMN IF NOT EXISTS reaction_emoji       TEXT    DEFAULT '👍'",
                "ALTER TABLE bot_chats ADD COLUMN IF NOT EXISTS auto_delete_min      INT     DEFAULT 0",
                "ALTER TABLE bot_chats ADD COLUMN IF NOT EXISTS feedback_lang        TEXT    DEFAULT 'ru'",
                "ALTER TABLE bot_chats ADD COLUMN IF NOT EXISTS edit_welcome_mid     INTEGER",
                "ALTER TABLE bot_chats ADD COLUMN IF NOT EXISTS edit_farewell_mid    INTEGER",
                # Миграция captcha_lang: если колонка была BOOLEAN — конвертируем в TEXT
                "ALTER TABLE bot_chats ALTER COLUMN captcha_lang TYPE TEXT USING CASE WHEN captcha_lang::text = 'true' THEN 'ru' ELSE 'off' END",
                # Миграция captcha_button_style: старые значения 1x1/1x2/2x2 → inline
                "UPDATE bot_chats SET captcha_button_style = 'inline' WHERE captcha_button_style IN ('1x1', '1x2', '2x2')",
            ]:
                await conn.execute(migration)
            # Таблица автоответчика
            await conn.execute("""
                CREATE TABLE IF NOT EXISTS autoreplies (
                    id         SERIAL PRIMARY KEY,
                    owner_id   BIGINT  NOT NULL,
                    chat_id    BIGINT  NOT NULL,
                    keyword    TEXT    NOT NULL,
                    reply_text TEXT    NOT NULL,
                    created_at TIMESTAMPTZ DEFAULT now(),
                    UNIQUE (owner_id, chat_id, keyword)
                )
            """)
            # Новые колонки autoreplies (медиа, кнопки, превью, позиция медиа)
            for migration in [
                "ALTER TABLE autoreplies ADD COLUMN IF NOT EXISTS reply_media      TEXT",
                "ALTER TABLE autoreplies ADD COLUMN IF NOT EXISTS reply_media_type TEXT",
                "ALTER TABLE autoreplies ADD COLUMN IF NOT EXISTS reply_buttons    TEXT",
                "ALTER TABLE autoreplies ADD COLUMN IF NOT EXISTS reply_preview    BOOLEAN DEFAULT false",
                "ALTER TABLE autoreplies ADD COLUMN IF NOT EXISTS reply_media_below BOOLEAN DEFAULT false",
                "ALTER TABLE autoreplies ADD COLUMN IF NOT EXISTS reply_media_top  BOOLEAN DEFAULT true",
                "ALTER TABLE bot_chats   ADD COLUMN IF NOT EXISTS general_reply_media_top BOOLEAN DEFAULT true",
            ]:
                await conn.execute(migration)

            # Миграция статистики Глобального ЧС (Бесшовная изоляция)
            await conn.execute("ALTER TABLE child_bots ADD COLUMN IF NOT EXISTS global_blocked_count INTEGER DEFAULT 0")
            await conn.execute("UPDATE child_bots SET global_blocked_count = blocked_count WHERE global_blocked_count = 0 AND blocked_count > 0")

            # Таблица событий капчи
            await conn.execute("""
                CREATE TABLE IF NOT EXISTS captcha_events (
                    id         BIGSERIAL PRIMARY KEY,
                    owner_id   BIGINT  NOT NULL,
                    chat_id    BIGINT  NOT NULL,
                    user_id    BIGINT  NOT NULL,
                    passed     BOOLEAN NOT NULL DEFAULT false,
                    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
                )
            """)
            await conn.execute(
                "CREATE INDEX IF NOT EXISTS idx_captcha_events_lookup "
                "ON captcha_events (owner_id, chat_id, created_at)"
            )
            # Заметки администратора о пользователях платформы
            await conn.execute("""
                CREATE TABLE IF NOT EXISTS platform_user_notes (
                    id              SERIAL PRIMARY KEY,
                    owner_id        BIGINT NOT NULL,
                    target_user_id  BIGINT NOT NULL,
                    note            TEXT NOT NULL DEFAULT '',
                    updated_at      TIMESTAMPTZ DEFAULT now(),
                    UNIQUE (owner_id, target_user_id)
                )
            """)
            # ── Миграция ga_selected_bots: admin_id → owner_id (общая выборка) ──
            try:
                col_exists = await conn.fetchval(
                    """
                    SELECT EXISTS (
                        SELECT 1 FROM information_schema.columns
                        WHERE table_name='ga_selected_bots' AND column_name='admin_id'
                    )
                    """
                )
                if col_exists:
                    # Конвертируем sub-admin записи → owner записи
                    await conn.execute("""
                        UPDATE ga_selected_bots gsb
                        SET admin_id = ga.owner_id
                        FROM global_admins ga
                        WHERE gsb.admin_id = ga.admin_id
                    """)
                    # Удаляем дубликаты
                    await conn.execute("""
                        DELETE FROM ga_selected_bots a
                        USING ga_selected_bots b
                        WHERE a.ctid < b.ctid
                          AND a.admin_id = b.admin_id
                          AND a.child_bot_id = b.child_bot_id
                    """)
                    # Пересоздаём PK и переименовываем колонку
                    await conn.execute(
                        "ALTER TABLE ga_selected_bots DROP CONSTRAINT IF EXISTS ga_selected_bots_pkey"
                    )
                    await conn.execute(
                        "ALTER TABLE ga_selected_bots RENAME COLUMN admin_id TO owner_id"
                    )
                    await conn.execute(
                        "ALTER TABLE ga_selected_bots ADD PRIMARY KEY (owner_id, child_bot_id)"
                    )
                    await conn.execute("DROP INDEX IF EXISTS idx_ga_selected_bots_admin")
                    await conn.execute(
                        "CREATE INDEX IF NOT EXISTS idx_ga_selected_bots_owner "
                        "ON ga_selected_bots(owner_id)"
                    )
                    logger.info("Migration ga_selected_bots: admin_id → owner_id ✅")
                else:
                    logger.info("Migration ga_selected_bots: already applied, skipping")
            except Exception as _mig_err:
                logger.warning(f"Migration ga_selected_bots non-fatal error: {_mig_err}")

        logger.info("DB schema applied")


        # Планировщик
        from scheduler.jobs import setup_scheduler
        setup_scheduler(bot).start()
        logger.info("Scheduler started")

        # Дочерние боты — запускаем polling для каждого
        from scheduler.child_bot_runner import init_runner, start_all_child_bots, stop_all_child_bots
        init_runner(bot)
        await start_all_child_bots()
        logger.info("Child bot runner started")

        # Устанавливаем webhook (SERVER_URL уже известен)
        from config import settings
        if settings.server_url:
            await bot.set_webhook(
                url=f"{settings.server_url}/bot/webhook",
                drop_pending_updates=True,
                allowed_updates=["message", "callback_query", "chat_join_request", "chat_member"],
            )
            logger.info(f"Webhook set: {settings.server_url}/bot/webhook")
        else:
            logger.warning("SERVER_URL not set — webhook not configured!")

        logger.info("Bot started successfully ✅")
        yield
        # ── Shutdown ─────────────────────────────────────────
        logger.info("Shutting down...")
        await stop_all_child_bots()  # Gracefully cancel all child bot polling tasks
        await db.close_pool()
        await bot.session.close()

    app = FastAPI(
        title="Bumblebee Bot API",
        docs_url=None,
        redoc_url=None,
        lifespan=lifespan,
    )

    # Статические файлы WebApp
    app.mount("/webapp", StaticFiles(directory="webapp", html=True), name="webapp")

    # ── Health check (отвечает СРАЗУ при старте сервера) ──────
    @app.get("/health")
    async def health():
        return {"status": "ok"}

    # ── Telegram Bot Webhook ───────────────────────────────────
    @app.post("/bot/webhook")
    async def bot_webhook(request: Request):
        try:
            data = await request.json()
            update = Update(**data)
            await _dp.feed_update(_bot, update)
        except Exception as e:
            # Логируем ошибку, но всегда возвращаем 200 — иначе Telegram будет повторять
            logger.exception(f"Error processing update: {e}")
        return {"ok": True}

    # ── WebApp API: создать платёж ─────────────────────────────
    @app.post("/api/create-crypto-payment")
    async def create_crypto_payment(request: Request):
        try:
            data = await request.json()
        except Exception:
            return JSONResponse({"error": "Invalid JSON"}, status_code=400)

        init_data = data.get("init_data", "")
        user_info = verify_init_data(init_data)
        if not user_info:
            return JSONResponse({"error": "Unauthorized"}, status_code=401)

        user_id  = user_info["id"]
        tariff   = data.get("tariff")
        period   = data.get("period")
        currency = data.get("currency")
        msg_id   = data.get("msg_id")  # message_id сообщения с кнопкой Оплатить
        if msg_id:
            try:
                msg_id = int(msg_id)
            except (TypeError, ValueError):
                msg_id = None

        if not all([tariff, period, currency]):
            return JSONResponse({"error": "Missing fields"}, status_code=400)

        try:
            result = await create_invoice(user_id, tariff, period, currency, invoice_msg_id=msg_id)
            return JSONResponse({"success": True, **result})
        except Exception as e:
            logger.error(f"create_invoice error: {e}")
            return JSONResponse({"error": str(e)}, status_code=500)

    # ── WebApp API: статус платежа ─────────────────────────────
    @app.get("/api/payment-status")
    async def payment_status(payment_id: str):
        return await get_payment_status(payment_id)

    # ── NOWPayments Webhook ────────────────────────────────────
    @app.post("/nowpayments/webhook")
    async def nowpayments_webhook(request: Request):
        payload   = await request.body()
        signature = request.headers.get("x-nowpayments-sig", "")

        if not verify_nowpayments_sig(payload, signature):
            logger.warning("NOWPayments webhook: invalid signature")
            return JSONResponse({"status": "forbidden"}, status_code=403)

        data = json.loads(payload)
        logger.info(f"NOWPayments: status={data.get('payment_status')}, order={data.get('order_id')}")

        if data.get("payment_status") == "finished":
            payment_id = data.get("order_id")
            result = await db.execute(
                """
                UPDATE payments
                SET status='paid', paid_at=now(), np_payment_id=$2
                WHERE id=$1 AND status='pending'
                """,
                payment_id, str(data.get("payment_id", "")),
            )
            if result == "UPDATE 1":
                row = await db.fetchrow(
                    "SELECT user_id, tariff, period, invoice_msg_id FROM payments WHERE id=$1",
                    payment_id,
                )
                if row:
                    await activate_tariff(row["user_id"], row["tariff"], row["period"])
                    # Удаляем старое сообщение с кнопками "Оплатить / Назад"
                    if row["invoice_msg_id"]:
                        try:
                            await _bot.delete_message(
                                chat_id=row["user_id"],
                                message_id=row["invoice_msg_id"],
                            )
                        except Exception:
                            pass
                    await notify_user_paid(_bot, row["user_id"], row["tariff"], row["period"])

        return {"status": "ok"}

    return app
