from __future__ import annotations

import time

import pytest

from conduit.cache import cosine, embed, exact_cache_key
from conduit.costing import cost_micros, micros_to_usd, usd_to_micros
from conduit.keys import generate_api_key, hash_api_key, key_prefix
from conduit.limits import TokenBucket
from conduit.logging_config import redact
from conduit.routing import CircuitBreaker
from conduit.schemas import ChatCompletionRequest
from tests.conftest import gateway_config


class Clock:
    def __init__(self, start: float = 10_000.0) -> None:
        self.now = start

    def __call__(self) -> float:
        return self.now


async def test_circuit_opens_and_half_open_probe_recovers() -> None:
    clock = Clock()
    breaker = CircuitBreaker(failure_threshold=2, recovery_seconds=10, clock=clock)
    assert await breaker.allow("groq")
    await breaker.record_failure("groq")
    assert breaker.state_of("groq") == "closed"
    await breaker.record_failure("groq")
    assert breaker.state_of("groq") == "open"
    assert not await breaker.allow("groq")
    clock.now += 10
    assert await breaker.allow("groq")
    assert breaker.state_of("groq") == "half_open"
    assert not await breaker.allow("groq")
    await breaker.record_success("groq")
    assert breaker.state_of("groq") == "closed"
    assert await breaker.allow("groq")


async def test_half_open_failure_reopens() -> None:
    clock = Clock()
    breaker = CircuitBreaker(failure_threshold=1, recovery_seconds=5, clock=clock)
    await breaker.record_failure("openai")
    assert breaker.state_of("openai") == "open"
    clock.now += 5
    assert await breaker.allow("openai")
    await breaker.record_failure("openai")
    assert breaker.state_of("openai") == "open"
    assert not await breaker.allow("openai")


async def test_token_bucket_refills_and_refunds() -> None:
    from fakeredis.aioredis import FakeRedis

    clock = Clock()
    bucket = TokenBucket(FakeRedis(decode_responses=True), clock=clock)
    allowed, _ = await bucket.take("rpm", 10, 10)
    assert allowed
    denied, remaining = await bucket.take("rpm", 10, 1)
    assert not denied
    assert remaining < 1
    await bucket.refund("rpm", 10, 4)
    allowed, _ = await bucket.take("rpm", 10, 4)
    assert allowed
    denied, _ = await bucket.take("rpm", 10, 1)
    assert not denied
    clock.now += 60
    allowed, _ = await bucket.take("rpm", 10, 10)
    assert allowed


async def test_force_take_can_drive_the_bucket_negative() -> None:
    from fakeredis.aioredis import FakeRedis

    bucket = TokenBucket(FakeRedis(decode_responses=True))
    await bucket.force_take("tpm", 5, 8)
    allowed, remaining = await bucket.take("tpm", 5, 1)
    assert not allowed
    assert remaining < 0


def test_exact_cache_key_ignores_stream_and_isolates_tenants() -> None:
    request = ChatCompletionRequest(model="mock", messages=[{"role": "user", "content": "Hello"}], stream=True)
    other = request.model_copy(update={"stream": False, "temperature": 0.2})
    same = request.model_copy(update={"stream": False})
    assert exact_cache_key("tenant:1", request) == exact_cache_key("tenant:1", same)
    assert exact_cache_key("tenant:1", request) != exact_cache_key("tenant:1", other)
    assert exact_cache_key("tenant:1", request) != exact_cache_key("tenant:2", request)


def test_embedder_matches_normalized_text_and_rejects_unrelated_text() -> None:
    left = embed("Hello, world. How are you today?", 64)
    right = embed("hello world how are you today", 64)
    unrelated = embed("quantum pasta folding manual", 64)
    assert cosine(left, right) == pytest.approx(1.0)
    assert cosine(left, unrelated) < 0.9


def test_cost_micros_match_price_times_tokens() -> None:
    config = gateway_config()
    assert cost_micros(config, "primary", "echo", 1000, 0) == 1000
    assert cost_micros(config, "primary", "missing", 1000, 5) == 0
    assert usd_to_micros("0.000050") == 50
    assert micros_to_usd(50) == "0.000050"


def test_api_keys_are_hashed_with_a_pepper() -> None:
    raw = generate_api_key()
    assert raw.startswith("cdt_live_")
    digest = hash_api_key(raw, "pepper")
    assert digest != hash_api_key(raw, "other-pepper")
    assert raw not in digest
    assert len(digest) == 64
    assert key_prefix(raw) == raw[:16]


def test_redact_strips_bearer_tokens_and_gateway_keys() -> None:
    raw = "Authorization: Bearer cdt_live_supersecret token"
    cleaned = redact(raw)
    assert "supersecret" not in cleaned
    assert "cdt_live_" not in cleaned
    assert "Bearer [redacted]" in cleaned


def test_shipped_gateway_config_loads() -> None:
    from conduit.config_loader import load_gateway_config

    config = load_gateway_config("config/gateway.yaml")
    assert "mock" in config.aliases
    assert "fast" in config.aliases
    assert config.providers["groq"].type == "openai_compat"
    assert config.cache.scope == "tenant"


def test_clock_moves() -> None:
    clock = Clock()
    before = clock()
    clock.now += 1
    assert clock() == before + 1
    assert time.time() > 0
