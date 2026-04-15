"""
Central configuration module.
All values are loaded from environment variables / .env file.
"""
from __future__ import annotations
import os
from dotenv import load_dotenv

load_dotenv()

# ---- Bot ----
BOT_TOKEN: str = os.getenv("BOT_TOKEN", "")

SUPER_ADMIN_IDS: list[int] = [
    int(x.strip())
    for x in os.getenv("SUPER_ADMIN_IDS", "").split(",")
    if x.strip().lstrip("-").isdigit()
]

TARGET_CHANNEL_ID: str = os.getenv("TARGET_CHANNEL_ID", "")

_review_group = os.getenv("REVIEW_GROUP_ID", "").strip()
REVIEW_GROUP_ID: int | None = int(_review_group) if _review_group else None

# ---- Database ----
DATABASE_PATH: str = os.getenv("DATABASE_PATH", "data/bot.db")

# ---- Pyrogram (personal account API) ----
_api_id = os.getenv("API_ID", "").strip()
API_ID: int = int(_api_id) if _api_id.isdigit() else 0
API_HASH: str = os.getenv("API_HASH", "")
PHONE_NUMBER: str = os.getenv("PHONE_NUMBER", "")
SESSION_NAME: str = os.getenv("SESSION_NAME", "collector_session")

# ---- Webhook ----
WEBHOOK_URL: str = os.getenv("WEBHOOK_URL", "")
WEBHOOK_PORT: int = int(os.getenv("WEBHOOK_PORT", "8443"))
WEBHOOK_SECRET: str = os.getenv("WEBHOOK_SECRET", "")

# ---- Business logic defaults ----
DEFAULT_DAILY_LIMIT: int = 5          # per-user daily submission cap
RATE_LIMIT_COUNT: int = 3             # submissions within window triggers cooldown
RATE_LIMIT_WINDOW_SECONDS: int = 60  # window length (seconds)
COOLDOWN_HOURS: int = 24             # cooldown duration after rate-limit trigger

# MediaGroup aggregation delay (seconds)
MEDIA_GROUP_DELAY: float = 0.5

# Collector reconnect settings
RECONNECT_MAX_RETRIES: int = 3
RECONNECT_DELAY_SECONDS: int = 10

# Queue consumer interval (seconds between batch polls)
QUEUE_POLL_INTERVAL: int = 2
QUEUE_BATCH_SIZE: int = 3
