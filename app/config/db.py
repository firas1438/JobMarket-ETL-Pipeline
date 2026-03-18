from __future__ import annotations

from dataclasses import dataclass

from sqlalchemy import create_engine
from sqlalchemy.engine import Engine

from app.config.settings import settings


@dataclass(frozen=True)
class DbConfig:
    host: str
    port: int
    db: str
    user: str
    password: str

    @property
    def sqlalchemy_url(self) -> str:
        return (
            f"postgresql+psycopg2://{self.user}:{self.password}"
            f"@{self.host}:{self.port}/{self.db}"
        )


def get_db_config() -> DbConfig:
    return DbConfig(
        host=settings.postgres_host,
        port=settings.postgres_port,
        db=settings.postgres_db,
        user=settings.postgres_user,
        password=settings.postgres_password,
    )


def get_engine() -> Engine:
    cfg = get_db_config()
    return create_engine(cfg.sqlalchemy_url, pool_pre_ping=True, future=True)

