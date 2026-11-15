"""SQLAlchemy models. SQLite for local and tests, Postgres in Compose."""

from __future__ import annotations

from typing import Any

from sqlalchemy import BigInteger, Boolean, Float, ForeignKey, Integer, String, Text, UniqueConstraint, event
from sqlalchemy.ext.asyncio import AsyncEngine, AsyncSession, async_sessionmaker, create_async_engine
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column
from sqlalchemy.pool import NullPool


class Base(DeclarativeBase):
    pass


class APIKey(Base):
    __tablename__ = "api_keys"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    name: Mapped[str] = mapped_column(String(120))
    key_hash: Mapped[str] = mapped_column(String(64), unique=True, index=True)
    key_prefix: Mapped[str] = mapped_column(String(24))
    rpm_limit: Mapped[int] = mapped_column(Integer)
    tpm_limit: Mapped[int] = mapped_column(Integer)
    token_budget: Mapped[int | None] = mapped_column(BigInteger, nullable=True)
    cost_budget_micros: Mapped[int | None] = mapped_column(BigInteger, nullable=True)
    tokens_used: Mapped[int] = mapped_column(BigInteger, default=0)
    cost_used_micros: Mapped[int] = mapped_column(BigInteger, default=0)
    disabled: Mapped[bool] = mapped_column(Boolean, default=False)
    created_at_ms: Mapped[int] = mapped_column(BigInteger)


class UsageRecord(Base):
    __tablename__ = "usage_records"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    request_id: Mapped[str] = mapped_column(String(64), unique=True)
    api_key_id: Mapped[int] = mapped_column(ForeignKey("api_keys.id"), index=True)
    idempotency_key: Mapped[str | None] = mapped_column(String(200), nullable=True)
    model_alias: Mapped[str] = mapped_column(String(200))
    provider: Mapped[str] = mapped_column(String(80), default="")
    upstream_model: Mapped[str] = mapped_column(String(200), default="")
    prompt_tokens: Mapped[int] = mapped_column(Integer, default=0)
    completion_tokens: Mapped[int] = mapped_column(Integer, default=0)
    total_tokens: Mapped[int] = mapped_column(Integer, default=0)
    cost_micros: Mapped[int] = mapped_column(BigInteger, default=0)
    latency_ms: Mapped[float] = mapped_column(Float, default=0)
    ttft_ms: Mapped[float | None] = mapped_column(Float, nullable=True)
    cache_status: Mapped[str] = mapped_column(String(16), default="miss")
    failover_count: Mapped[int] = mapped_column(Integer, default=0)
    attempt_log: Mapped[str] = mapped_column(Text, default="[]")
    status: Mapped[str] = mapped_column(String(32))
    error_type: Mapped[str | None] = mapped_column(String(40), nullable=True)
    streamed: Mapped[bool] = mapped_column(Boolean, default=False)
    created_at_ms: Mapped[int] = mapped_column(BigInteger, index=True)


class IdempotencyRecord(Base):
    __tablename__ = "idempotency_records"
    __table_args__ = (UniqueConstraint("api_key_id", "idempotency_key", name="uq_idempotency"),)

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    api_key_id: Mapped[int] = mapped_column(ForeignKey("api_keys.id"), index=True)
    idempotency_key: Mapped[str] = mapped_column(String(200))
    request_hash: Mapped[str] = mapped_column(String(64))
    status: Mapped[str] = mapped_column(String(20))
    response_json: Mapped[str | None] = mapped_column(Text, nullable=True)
    status_code: Mapped[int | None] = mapped_column(Integer, nullable=True)
    created_at_ms: Mapped[int] = mapped_column(BigInteger)


def make_engine(url: str) -> AsyncEngine:
    if url.startswith("sqlite"):
        engine = create_async_engine(
            url,
            connect_args={"check_same_thread": False, "timeout": 30},
            poolclass=NullPool,
        )

        @event.listens_for(engine.sync_engine, "connect")
        def _sqlite_pragmas(dbapi_connection: Any, _: Any) -> None:
            cursor = dbapi_connection.cursor()
            cursor.execute("PRAGMA journal_mode=WAL")
            cursor.execute("PRAGMA busy_timeout=5000")
            cursor.execute("PRAGMA foreign_keys=ON")
            cursor.close()

        return engine
    return create_async_engine(url, pool_size=10, max_overflow=20)


def make_sessionmaker(engine: AsyncEngine) -> async_sessionmaker[AsyncSession]:
    return async_sessionmaker(engine, expire_on_commit=False)
