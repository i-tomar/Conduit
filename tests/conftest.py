"""App fixtures. Everything here is offline: SQLite, fakeredis, and the mock provider."""

from __future__ import annotations

from collections.abc import AsyncIterator, Iterator
from contextlib import asynccontextmanager
from decimal import Decimal
from pathlib import Path

import pytest
from httpx import ASGITransport, AsyncClient

from conduit.app import create_app
from conduit.config_loader import (
    Alias,
    CacheConfig,
    CircuitConfig,
    GatewayConfig,
    Price,
    ProviderConfig,
    RetryConfig,
    Target,
    TimeoutConfig,
)
from conduit.settings import Settings

AUTH = {"Authorization": "Bearer cdt_live_test_key"}
ADMIN = {"X-Admin-Token": "test-admin"}


def gateway_config(*, semantic: bool = False, failure_threshold: int = 100) -> GatewayConfig:
    price = Price(input_per_million=Decimal("1"), output_per_million=Decimal("1"))
    return GatewayConfig(
        providers={
            "primary": ProviderConfig(type="mock"),
            "backup": ProviderConfig(type="mock"),
            "openai": ProviderConfig(
                type="openai_compat",
                base_url="https://example.invalid/v1",
                api_key_env="CONDUIT_TEST_OPENAI_KEY",
            ),
        },
        aliases={
            "mock": Alias(targets=[Target(provider="primary", model="echo")]),
            "resilient": Alias(
                targets=[
                    Target(provider="primary", model="echo"),
                    Target(provider="backup", model="echo"),
                ]
            ),
            "live": Alias(
                targets=[
                    Target(provider="openai", model="gpt-4o-mini"),
                    Target(provider="primary", model="echo"),
                ]
            ),
            "openai_only": Alias(targets=[Target(provider="openai", model="gpt-4o-mini")]),
        },
        pricing={"primary": {"echo": price}, "backup": {"echo": price}, "openai": {"gpt-4o-mini": price}},
        retry=RetryConfig(max_attempts=2, base_delay_seconds=0, max_delay_seconds=0),
        circuit_breaker=CircuitConfig(failure_threshold=failure_threshold, recovery_seconds=30),
        cache=CacheConfig(
            exact_ttl_seconds=120,
            semantic_enabled=semantic,
            semantic_threshold=0.9,
            semantic_max_entries=50,
            scope="tenant",
        ),
        timeouts=TimeoutConfig(request_seconds=2, connect_seconds=1),
    )


def chat_body(content: str = "hello", model: str = "mock", stream: bool = False, **extra: object) -> dict[str, object]:
    body: dict[str, object] = {
        "model": model,
        "messages": [{"role": "user", "content": content}],
        "stream": stream,
    }
    body.update(extra)
    return body


@asynccontextmanager
async def open_gateway(
    tmp_path: Path,
    *,
    semantic: bool = False,
    failure_threshold: int = 100,
    **setting_overrides: object,
) -> AsyncIterator[tuple[AsyncClient, object]]:
    database = tmp_path / "conduit.db"
    settings = Settings(
        _env_file=None,
        database_url=f"sqlite+aiosqlite:///{database}",
        redis_url="memory://",
        admin_token="test-admin",
        dev_api_key="cdt_live_test_key",
        dev_rpm=10_000,
        dev_tpm=5_000_000,
        allow_fault_injection=True,
        otel_enabled=False,
        key_pepper="test-pepper",
        log_level="INFO",
        **setting_overrides,
    )
    application = create_app(
        settings,
        gateway_config=gateway_config(semantic=semantic, failure_threshold=failure_threshold),
    )
    async with application.router.lifespan_context(application):
        transport = ASGITransport(app=application)
        async with AsyncClient(transport=transport, base_url="http://conduit.test") as client:
            yield client, application


@pytest.fixture
async def gateway(tmp_path: Path) -> AsyncIterator[tuple[AsyncClient, object]]:
    async with open_gateway(tmp_path) as pair:
        yield pair


@pytest.fixture
def cap_conduit(caplog: pytest.LogCaptureFixture) -> Iterator[pytest.LogCaptureFixture]:
    log = __import__("logging").getLogger("conduit")
    log.addHandler(caplog.handler)
    caplog.set_level("INFO", logger="conduit")
    yield caplog
    log.removeHandler(caplog.handler)
