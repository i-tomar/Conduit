"""Process resources created during application startup."""

from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import cast

from redis.asyncio import Redis as RedisClient
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncEngine, AsyncSession, async_sessionmaker

from conduit.cache import ExactCache, SemanticCache
from conduit.config_loader import GatewayConfig
from conduit.costing import usd_to_micros
from conduit.db import APIKey, make_engine, make_sessionmaker
from conduit.keys import hash_api_key, key_prefix
from conduit.limits import TokenBucket
from conduit.logging_config import log_event
from conduit.metrics import Metrics
from conduit.providers import Provider, build_providers
from conduit.routing import CircuitBreaker, RoutingEngine
from conduit.settings import Settings

logger = logging.getLogger("conduit")


@dataclass(slots=True)
class Runtime:
    settings: Settings
    config: GatewayConfig
    db_engine: AsyncEngine
    sessionmaker: async_sessionmaker[AsyncSession]
    redis: RedisClient
    providers: dict[str, Provider]
    breaker: CircuitBreaker
    router: RoutingEngine
    buckets: TokenBucket
    exact_cache: ExactCache
    semantic_cache: SemanticCache
    metrics: Metrics

    async def aclose(self) -> None:
        for provider in self.providers.values():
            await provider.aclose()
        closer = getattr(self.redis, "aclose", None)
        if closer is not None:
            await closer()
        await self.db_engine.dispose()


def _redis(settings: Settings) -> RedisClient:
    if settings.redis_url == "memory://":
        try:
            from fakeredis.aioredis import FakeRedis
        except ImportError as exc:
            raise RuntimeError(
                "CONDUIT_REDIS_URL=memory:// needs the fakeredis package from the dev dependency group"
            ) from exc
        return cast(RedisClient, FakeRedis(decode_responses=True))
    return RedisClient.from_url(settings.redis_url, decode_responses=True)


async def build_runtime(settings: Settings, config: GatewayConfig) -> Runtime:
    if settings.semantic_cache is not None:
        config = config.model_copy(
            update={"cache": config.cache.model_copy(update={"semantic_enabled": settings.semantic_cache})}
        )
    engine = make_engine(settings.database_url)
    sessions = make_sessionmaker(engine)
    async with engine.begin() as connection:
        await connection.run_sync(APIKey.metadata.create_all)
    redis = _redis(settings)
    metrics = Metrics()
    for name in config.providers:
        metrics.set_circuit(name, "closed")
    breaker = CircuitBreaker(
        failure_threshold=config.circuit_breaker.failure_threshold,
        recovery_seconds=config.circuit_breaker.recovery_seconds,
        on_change=metrics.set_circuit,
    )
    providers = build_providers(config)
    runtime = Runtime(
        settings=settings,
        config=config,
        db_engine=engine,
        sessionmaker=sessions,
        redis=redis,
        providers=providers,
        breaker=breaker,
        router=RoutingEngine(config=config, providers=providers, breaker=breaker),
        buckets=TokenBucket(redis),
        exact_cache=ExactCache(redis, config.cache.exact_ttl_seconds),
        semantic_cache=SemanticCache(
            redis,
            ttl_seconds=config.cache.exact_ttl_seconds,
            threshold=config.cache.semantic_threshold,
            max_entries=config.cache.semantic_max_entries,
            dims=config.cache.semantic_dims,
        ),
        metrics=metrics,
    )
    try:
        await redis.ping()
    except Exception:
        logger.exception("redis_unavailable_at_startup")
    await _seed_dev_key(runtime)
    return runtime


async def _seed_dev_key(runtime: Runtime) -> None:
    raw = runtime.settings.dev_api_key
    if not raw:
        return
    if not raw.startswith("cdt_"):
        logger.warning("dev_api_key_ignored")
        return
    digest = hash_api_key(raw, runtime.settings.key_pepper)
    async with runtime.sessionmaker() as session:
        existing = await session.scalar(select(APIKey).where(APIKey.key_hash == digest))
        if existing is not None:
            return
        budget = runtime.settings.dev_cost_budget_usd
        session.add(
            APIKey(
                name=runtime.settings.dev_key_name,
                key_hash=digest,
                key_prefix=key_prefix(raw),
                rpm_limit=runtime.settings.dev_rpm,
                tpm_limit=runtime.settings.dev_tpm,
                token_budget=runtime.settings.dev_token_budget,
                cost_budget_micros=None if budget is None else usd_to_micros(budget),
                tokens_used=0,
                cost_used_micros=0,
                disabled=False,
                created_at_ms=_now_ms(),
            )
        )
        await session.commit()
    log_event(logger, logging.INFO, "dev_api_key_ready", prefix=key_prefix(raw))


def _now_ms() -> int:
    import time

    return int(time.time() * 1000)
