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

    # Adzuna API (replaces Remotive).
    # Only credentials live in the .env files; everything else is configured here.
    adzuna_api_base_url: str = "https://api.adzuna.com/v1/api/jobs"
    adzuna_app_id: str = os.getenv("ADZUNA_APP_ID", "")
    adzuna_app_key: str = os.getenv("ADZUNA_APP_KEY", "")
    adzuna_country: str = "us"

    # Search query for "mostly tech".
    adzuna_what: str = "software engineer"
    # Adzuna's `where` supports global search.
    # Note: Adzuna's endpoint is country-scoped via the URL path; we keep `us`
    # here and use `where=world` to broaden results.
    adzuna_where: str = "world"

    adzuna_results_per_page: int = 100
    # Batch should fetch a lot, but not run forever on every execution.
    adzuna_batch_max_pages: int = 20
    # Producer fetches a small number of pages per poll.
    adzuna_pages_per_poll: int = 1

    # Unused by default (batch uses max_pages); left for backward compatibility.
    adzuna_max_jobs: int = 0

    # Use Spark for core dataframe operations (e.g., deduplication) when available.
    use_spark_dedupe: bool = os.getenv("USE_SPARK_DEDUPE", "1").lower() in ("1", "true", "yes")


settings = Settings()


def require_settings() -> Settings:
    # Keep this function to enforce required vars if you later choose stricter behavior.
    return settings

