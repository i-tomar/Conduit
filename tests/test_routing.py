from __future__ import annotations

import pytest

from conduit.errors import ConduitError
from conduit.providers import build_providers
from conduit.routing import CircuitBreaker, RoutingEngine
from conduit.schemas import ChatCompletionRequest
from tests.conftest import gateway_config

REQUEST = ChatCompletionRequest(model="resilient", messages=[{"role": "user", "content": "route me"}])


def engine(sleeps: list[float] | None = None) -> tuple[RoutingEngine, dict]:
    config = gateway_config()
    providers = build_providers(config)
    breaker = CircuitBreaker(failure_threshold=3, recovery_seconds=30)

    async def sleeper(seconds: float) -> None:
        if sleeps is not None:
            sleeps.append(seconds)

    return RoutingEngine(config=config, providers=providers, breaker=breaker, sleeper=sleeper), providers


async def test_failover_retries_then_uses_the_backup() -> None:
    sleeps: list[float] = []
    router, providers = engine(sleeps)
    result = await router.complete(REQUEST, "resilient", {"primary": "429"})
    assert result.provider == "backup"
    assert providers["primary"].calls == 2
    assert providers["backup"].calls == 1
    assert [item.status for item in result.attempts] == ["rate_limited", "rate_limited", "success"]
    assert sleeps == [0.0]


async def test_bad_request_does_not_fail_over() -> None:
    router, providers = engine()
    with pytest.raises(ConduitError) as exc:
        await router.complete(REQUEST, "resilient", {"primary": "400"})
    assert exc.value.status_code == 400
    assert providers["backup"].calls == 0


async def test_unconfigured_provider_is_skipped() -> None:
    router, providers = engine()
    result = await router.complete(REQUEST, "live", {})
    assert result.provider == "primary"
    assert providers["openai"].calls == 0
    assert result.attempts[0].status == "unconfigured"


async def test_all_providers_down_is_503() -> None:
    router, _providers = engine()
    with pytest.raises(ConduitError) as exc:
        await router.complete(REQUEST, "openai_only", {})
    assert exc.value.status_code == 503
    assert exc.value.code == "all_providers_failed"


async def test_open_circuit_skips_the_provider() -> None:
    router, providers = engine()
    for _ in range(2):
        await router.complete(REQUEST, "resilient", {"primary": "500"})
    calls = providers["primary"].calls
    result = await router.complete(REQUEST, "resilient", {"primary": "500"})
    assert result.provider == "backup"
    assert providers["primary"].calls == calls
    assert result.attempts[0].status == "circuit_open"
