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
        logger.info("DB schema applied")

        # Планировщик
        from scheduler.jobs import setup_scheduler
        setup_scheduler(bot).start()
        logger.info("Scheduler started")

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

        if not all([tariff, period, currency]):
            return JSONResponse({"error": "Missing fields"}, status_code=400)

        try:
            result = await create_invoice(user_id, tariff, period, currency)
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
                    "SELECT user_id, tariff, period FROM payments WHERE id=$1",
                    payment_id,
                )
                if row:
                    await activate_tariff(row["user_id"], row["tariff"], row["period"])
                    await notify_user_paid(_bot, row["user_id"], row["tariff"], row["period"])
                    await notify_owner_payment(_bot, row["user_id"], data)

        return {"status": "ok"}

    return app
