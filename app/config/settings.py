from __future__ import annotations

import os
from dataclasses import dataclass

from dotenv import load_dotenv


load_dotenv()


def _getenv(name: str, default: str | None = None) -> str:
    v = os.getenv(name, default)
    if v is None or v == "":
        raise RuntimeError(f"Missing required env var: {name}")
    return v


@dataclass(frozen=True)
class Settings:
    postgres_host: str = os.getenv("POSTGRES_HOST", "postgres")
    postgres_port: int = int(os.getenv("POSTGRES_PORT", "5432"))
    postgres_db: str = os.getenv("POSTGRES_DB", "jobs")
    postgres_user: str = os.getenv("POSTGRES_USER", "postgres")
    postgres_password: str = os.getenv("POSTGRES_PASSWORD", "postgres")

    kafka_bootstrap_servers: str = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
    kafka_topic: str = os.getenv("KAFKA_TOPIC", "jobs_live")

    poll_seconds: int = int(os.getenv("POLL_SECONDS", "120"))
    producer_state_path: str = os.getenv("PRODUCER_STATE_PATH", "producer_state.json")

    remotive_api_url: str = os.getenv("REMOTIVE_API_URL", "https://remotive.com/api/remote-jobs")
    # Remotive generally works without an API key; kept for PDF/checklist completeness.
    remotive_api_key: str | None = os.getenv("REMOTIVE_API_KEY") or None


settings = Settings()


def require_settings() -> Settings:
    # Keep this function to enforce required vars if you later choose stricter behavior.
    return settings

