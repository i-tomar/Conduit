"""Process settings. Route tables and prices live in the YAML config."""

from __future__ import annotations

from pydantic import field_validator
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(
        env_prefix="CONDUIT_",
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore",
    )

    database_url: str = "sqlite+aiosqlite:///./conduit.db"
    redis_url: str = "redis://localhost:6379/0"
    config_path: str = "config/gateway.yaml"
    admin_token: str = ""
    dev_api_key: str = ""
    dev_key_name: str = "dev"
    dev_rpm: int = 600
    dev_tpm: int = 200_000
    dev_token_budget: int | None = None
    dev_cost_budget_usd: float | None = None
    allow_fault_injection: bool = False
    # None keeps the YAML flag. Set CONDUIT_SEMANTIC_CACHE=true to override it.
    semantic_cache: bool | None = None
    log_prompts: bool = False
    log_level: str = "INFO"
    otel_enabled: bool = True
    otlp_endpoint: str = ""
    key_pepper: str = "dev-pepper-change-me"
    host: str = "0.0.0.0"
    port: int = 8080
    idempotency_stale_ms: int = 60_000

    @field_validator("semantic_cache", mode="before")
    @classmethod
    def _empty_semantic_cache(cls, value: object) -> object:
        if value == "":
            return None
        return value
