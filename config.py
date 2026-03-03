"""
config.py — Все переменные окружения проекта.
Используется через: from config import settings
"""
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(
        env_file=".env",          # для локальной разработки (игнорируется если нет файла)
        env_file_encoding="utf-8",
        env_ignore_empty=True,
        extra="ignore",
        case_sensitive=False,     # BOT_TOKEN == bot_token
    )

    # Telegram
    bot_token: str
    owner_telegram_id: int

    # Database
    database_url: str

    # Encryption
    token_encryption_key: str

    # NOWPayments
    nowpayments_api_key: str = ""
    nowpayments_ipn_secret: str = ""

    # URLs
    server_url: str = ""
    webapp_url: str = ""

    # Mode
    bot_mode: str = "polling"  # polling | webhook

    # ── Тарифные цены (USD) ──────────────────────────
    tariff_prices: dict = {
        "start_month":    9,
        "pro_month":     24,
        "business_month": 59,
        "start_year":    79,
        "pro_year":     199,
        "business_year": 499,
    }

    tariff_durations: dict = {
        "month": 30,
        "year":  365,
    }

    # ── Лимиты ЧС по тарифам ──────────────────────────
    blacklist_limits: dict = {
        "free":     0,
        "start":    10_000,
        "pro":      100_000,
        "business": 1_000_000,
    }

    # ── Лимиты площадок по тарифам ────────────────────
    channel_limits: dict = {
        "free":     1,
        "start":    2,
        "pro":      4,
        "business": 10,
    }

    # ── Trial ──────────────────────────────────────────
    trial_days: int = 10


settings = Settings()
