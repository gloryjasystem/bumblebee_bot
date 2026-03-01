"""
api/server.py — FastAPI сервер: webhook бота, NOWPayments webhook, WebApp API.
"""
import json
import logging
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

    app = FastAPI(title="Bumblebee Bot API", docs_url=None, redoc_url=None)

    # WebApp статические файлы
    app.mount("/webapp", StaticFiles(directory="webapp", html=True), name="webapp")

    # ── Telegram Bot Webhook ───────────────────────────────────
    @app.post("/bot/webhook")
    async def bot_webhook(request: Request):
        data = await request.json()
        update = Update(**data)
        await dp.feed_update(bot, update)
        return {"ok": True}

    # ── WebApp API: создать платёж ─────────────────────────────
    @app.post("/api/create-crypto-payment")
    async def create_crypto_payment(request: Request):
        try:
            data = await request.json()
        except Exception:
            return JSONResponse({"error": "Invalid JSON"}, status_code=400)

        # Валидация tg.initData (КРИТИЧНО — защита от подмены user_id)
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

    # ── WebApp API: статус платежа (polling каждые 4 сек) ─────
    @app.get("/api/payment-status")
    async def payment_status(payment_id: str):
        return await get_payment_status(payment_id)

    # ── NOWPayments Webhook ────────────────────────────────────
    @app.post("/nowpayments/webhook")
    async def nowpayments_webhook(request: Request):
        payload = await request.body()  # сырые байты — ВАЖНО для HMAC
        signature = request.headers.get("x-nowpayments-sig", "")

        if not verify_nowpayments_sig(payload, signature):
            logger.warning("NOWPayments webhook: invalid signature")
            return JSONResponse({"status": "forbidden"}, status_code=403)

        data = json.loads(payload)
        logger.info(f"NOWPayments webhook: status={data.get('payment_status')}, order={data.get('order_id')}")

        if data.get("payment_status") == "finished":
            payment_id = data.get("order_id")

            # Idempotency: обновляем ТОЛЬКО если статус pending
            result = await db.execute(
                """
                UPDATE payments
                SET status='paid', paid_at=now(), np_payment_id=$2
                WHERE id=$1 AND status='pending'
                """,
                payment_id, str(data.get("payment_id", "")),
            )

            if result == "UPDATE 1":
                # Только первый вебхук активирует тариф (защита от дублей)
                row = await db.fetchrow(
                    "SELECT user_id, tariff, period FROM payments WHERE id=$1",
                    payment_id,
                )
                if row:
                    await activate_tariff(row["user_id"], row["tariff"], row["period"])
                    await notify_user_paid(_bot, row["user_id"], row["tariff"], row["period"])
                    await notify_owner_payment(_bot, row["user_id"], data)

        # Всегда 200 — иначе NOWPayments будет слать снова
        return {"status": "ok"}

    # ── Health check ───────────────────────────────────────────
    @app.get("/health")
    async def health():
        return {"status": "ok"}

    return app
