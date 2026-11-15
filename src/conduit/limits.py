"""Per-key token buckets stored in Redis.

Capacity is the per-minute limit and refills continuously, so a key may burst
up to its full minute allowance and then settle at the average rate. The
read-modify-write is guarded by a short Redis lock so it stays correct without
a Lua script, which keeps the offline fakeredis tests on the same code path.
"""

from __future__ import annotations

import asyncio
import contextlib
import time
from collections.abc import Callable

from redis.asyncio import Redis
from redis.exceptions import RedisError

from conduit.errors import ConduitError

_WINDOW_MS = 60_000


class TokenBucket:
    def __init__(self, redis: Redis, clock: Callable[[], float] | None = None) -> None:
        self._redis = redis
        self._clock = clock or time.time

    async def take(self, key: str, capacity: int, cost: int) -> tuple[bool, float]:
        return await self._update(key, capacity, cost, "take")

    async def refund(self, key: str, capacity: int, cost: int) -> None:
        if cost <= 0:
            return
        await self._update(key, capacity, cost, "refund")

    async def force_take(self, key: str, capacity: int, cost: int) -> None:
        if cost <= 0:
            return
        await self._update(key, capacity, cost, "force")

    async def _update(self, key: str, capacity: int, cost: int, mode: str) -> tuple[bool, float]:
        if capacity <= 0:
            return False, 0.0
        lock = f"{key}:lock"
        last_error: Exception | None = None
        for _ in range(200):
            try:
                acquired = await self._redis.set(lock, "1", nx=True, px=1000)
            except RedisError as exc:
                raise ConduitError(
                    "Rate limiter is unavailable",
                    status_code=503,
                    error_type="rate_limiter_unavailable",
                    code="rate_limiter_unavailable",
                ) from exc
            if not acquired:
                await asyncio.sleep(0.001)
                continue
            try:
                return await self._mutate(key, capacity, cost, mode)
            except RedisError as exc:
                last_error = exc
                raise ConduitError(
                    "Rate limiter is unavailable",
                    status_code=503,
                    error_type="rate_limiter_unavailable",
                    code="rate_limiter_unavailable",
                ) from exc
            finally:
                with contextlib.suppress(RedisError):
                    await self._redis.delete(lock)
        raise ConduitError(
            "Rate limiter is unavailable",
            status_code=503,
            error_type="rate_limiter_unavailable",
            code="rate_limiter_unavailable",
        ) from last_error

    async def _mutate(self, key: str, capacity: int, cost: int, mode: str) -> tuple[bool, float]:
        raw_tokens, raw_ts = await self._redis.hmget(key, "tokens", "ts")
        now = int(self._clock() * 1000)
        if raw_tokens is None or raw_ts is None:
            tokens = float(capacity)
        else:
            tokens = float(raw_tokens)
            elapsed = max(0, now - int(float(raw_ts)))
            tokens = min(float(capacity), tokens + elapsed * (capacity / _WINDOW_MS))
        allowed = False
        if mode == "refund":
            tokens = min(float(capacity), tokens + cost)
            allowed = True
        elif mode == "force" or cost <= capacity and tokens + 1e-9 >= cost:
            tokens -= cost
            allowed = True
        await self._redis.hset(key, mapping={"tokens": repr(tokens), "ts": str(now)})
        await self._redis.pexpire(key, _WINDOW_MS * 2)
        return allowed, tokens
