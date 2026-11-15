"""YAML route table. Validated once at startup."""

from __future__ import annotations

from decimal import Decimal
from pathlib import Path
from typing import Any, Literal

import yaml
from pydantic import BaseModel, Field, field_validator


class Target(BaseModel):
    provider: str
    model: str


class Alias(BaseModel):
    targets: list[Target]


class ProviderConfig(BaseModel):
    type: Literal["openai_compat", "mock"]
    base_url: str | None = None
    api_key_env: str | None = None
    api_key: str | None = None


class Price(BaseModel):
    input_per_million: Decimal
    output_per_million: Decimal

    @field_validator("input_per_million", "output_per_million", mode="before")
    @classmethod
    def _as_decimal(cls, value: object) -> Decimal:
        if isinstance(value, Decimal):
            return value
        return Decimal(str(value))


class RetryConfig(BaseModel):
    max_attempts: int = Field(default=2, ge=1, le=6)
    base_delay_seconds: float = Field(default=0.2, ge=0)
    max_delay_seconds: float = Field(default=2.0, ge=0)


class CircuitConfig(BaseModel):
    failure_threshold: int = Field(default=5, ge=1)
    recovery_seconds: float = Field(default=30.0, ge=0)


class CacheConfig(BaseModel):
    exact_ttl_seconds: int = Field(default=3600, ge=1)
    semantic_enabled: bool = False
    semantic_threshold: float = Field(default=0.92, ge=0, le=1)
    semantic_max_entries: int = Field(default=200, ge=1, le=5000)
    semantic_dims: int = Field(default=64, ge=8, le=512)
    scope: Literal["tenant", "global"] = "tenant"


class TimeoutConfig(BaseModel):
    request_seconds: float = Field(default=30.0, gt=0)
    connect_seconds: float = Field(default=5.0, gt=0)


class GatewayConfig(BaseModel):
    providers: dict[str, ProviderConfig]
    aliases: dict[str, Alias]
    pricing: dict[str, dict[str, Price]] = Field(default_factory=dict)
    retry: RetryConfig = Field(default_factory=RetryConfig)
    circuit_breaker: CircuitConfig = Field(default_factory=CircuitConfig)
    cache: CacheConfig = Field(default_factory=CacheConfig)
    timeouts: TimeoutConfig = Field(default_factory=TimeoutConfig)


def load_gateway_config(path: str) -> GatewayConfig:
    raw = Path(path).read_text(encoding="utf-8")
    data: dict[str, Any] = yaml.safe_load(raw) or {}
    config = GatewayConfig.model_validate(data)
    validate_gateway(config)
    return config


def validate_gateway(config: GatewayConfig) -> None:
    if not config.aliases:
        raise ValueError("gateway config has no aliases")
    for alias, spec in config.aliases.items():
        if not spec.targets:
            raise ValueError(f"alias '{alias}' has no targets")
        for target in spec.targets:
            if target.provider not in config.providers:
                raise ValueError(f"alias '{alias}' references unknown provider '{target.provider}'")
