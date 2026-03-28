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
    # Юзернейм владельца проекта — ему всегда принудительно выдаётся тариф business навсегда
    owner_username: str = "alextgads"
    
    # Co-Owner (Second Owner with 100% equivalent access)
    co_owner_telegram_id: int | None = None
    co_owner_username: str | None = None

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


# ── Тарифы и Лимиты ───────────────────────────────────────────
TARIFFS = {
    "free": {
        "max_bots": 1,
        "max_chats_per_bot": 1,
        "max_blacklist_users": 100,
        "features": {
            "analytics_full": False,
            "protection": False,
            "mailings": False,
            "custom_links": False,
            "add_admins": False
        }
    },
    "start": {
        "max_bots": 1,
        "max_chats_per_bot": 3,
        "max_blacklist_users": 1000,
        "features": {
            "analytics_full": True,
            "protection": True,
            "mailings": True,
            "custom_links": True,
            "add_admins": True
        }
    },
    "pro": {
        "max_bots": 3,
        "max_chats_per_bot": 10,
        "max_blacklist_users": 10000,
        "features": {
            "analytics_full": True,
            "protection": True,
            "mailings": True,
            "custom_links": True,
            "add_admins": True
        }
    },
    "business": {
        "max_bots": 5,
        "max_chats_per_bot": 25,
        "max_blacklist_users": 100000,
        "features": {
            "analytics_full": True,
            "protection": True,
            "mailings": True,
            "custom_links": True,
            "add_admins": True
        }
    }
}

settings = Settings()
