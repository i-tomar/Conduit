"""Ordered failover, retries, and an in-process circuit breaker.

The breaker is per process. A replica that has opened the circuit stops calling
that provider; other replicas keep their own view. A Redis-backed breaker is
the multi-replica version of the same state machine.
"""

from __future__ import annotations

import asyncio
import logging
import time
from collections.abc import AsyncIterator, Awaitable, Callable
from dataclasses import dataclass
from typing import NoReturn

from conduit.config_loader import GatewayConfig, Target
from conduit.errors import ConduitError, ProviderError
from conduit.providers.base import Provider, ProviderResult, StreamChunk
from conduit.schemas import ChatCompletionRequest

logger = logging.getLogger("conduit")


@dataclass(slots=True)
class _BreakerState:
    failures: int = 0
    state: str = "closed"
    opened_at: float = 0.0
    half_open_in_flight: bool = False


class CircuitBreaker:
    def __init__(
        self,
        *,
        failure_threshold: int,
        recovery_seconds: float,
        clock: Callable[[], float] | None = None,
        on_change: Callable[[str, str], None] | None = None,
    ) -> None:
        self._threshold = failure_threshold
        self._recovery = recovery_seconds
        self._clock = clock or time.monotonic
        self._on_change = on_change
        self._states: dict[str, _BreakerState] = {}
        self._lock = asyncio.Lock()

    def state_of(self, provider: str) -> str:
        state = self._states.get(provider)
        if state is None:
            return "closed"
        return state.state

    async def allow(self, provider: str) -> bool:
        async with self._lock:
            state = self._states.get(provider)
            if state is None or state.state == "closed":
                return True
            now = self._clock()
            if state.state == "open":
                if now - state.opened_at >= self._recovery:
                    self._set(provider, state, "half_open")
                    state.half_open_in_flight = True
                    return True
                return False
            if state.half_open_in_flight:
                return False
            state.half_open_in_flight = True
            return True

    async def record_success(self, provider: str) -> None:
        async with self._lock:
            state = self._states.setdefault(provider, _BreakerState())
            state.failures = 0
            state.half_open_in_flight = False
            self._set(provider, state, "closed")

    async def record_failure(self, provider: str) -> None:
        async with self._lock:
            state = self._states.setdefault(provider, _BreakerState())
            state.half_open_in_flight = False
            if state.state == "half_open":
                state.opened_at = self._clock()
                self._set(provider, state, "open")
                return
            state.failures += 1
            if state.failures >= self._threshold:
                state.opened_at = self._clock()
                self._set(provider, state, "open")

    def _set(self, provider: str, state: _BreakerState, new_state: str) -> None:
        if state.state == new_state:
            return
        state.state = new_state
        if self._on_change is not None:
            self._on_change(provider, new_state)


@dataclass(slots=True)
class AttemptRecord:
    provider: str
    model: str
    status: str
    latency_ms: float
    error_type: str | None = None

    def as_dict(self) -> dict[str, object]:
        return {
            "provider": self.provider,
            "model": self.model,
            "status": self.status,
            "latency_ms": round(self.latency_ms, 3),
            "error_type": self.error_type,
        }


@dataclass(slots=True)
class CompletedRoute:
    result: ProviderResult
    provider: str
    attempts: list[AttemptRecord]
    ttft_ms: float


@dataclass(slots=True)
class StreamingRoute:
    provider: str
    upstream_model: str
    attempts: list[AttemptRecord]
    iterator: AsyncIterator[StreamChunk]
    ttft_ms: float


def failover_count(attempts: list[AttemptRecord]) -> int:
    failed: list[str] = []
    for attempt in attempts:
        if attempt.status in {"success", "circuit_open", "unconfigured"}:
            continue
        if not failed or failed[-1] != attempt.provider:
            failed.append(attempt.provider)
    return len(failed)


def attempt_header(attempts: list[AttemptRecord]) -> str:
    return ",".join(attempt.status for attempt in attempts[:20])


class RoutingEngine:
    def __init__(
        self,
        *,
        config: GatewayConfig,
        providers: dict[str, Provider],
        breaker: CircuitBreaker,
        sleeper: Callable[[float], Awaitable[None]] | None = None,
    ) -> None:
        self._config = config
        self._providers = providers
        self._breaker = breaker
        self._sleep = sleeper or _sleep

    async def complete(
        self,
        request: ChatCompletionRequest,
        alias: str,
        faults: dict[str, str],
    ) -> CompletedRoute:
        attempts: list[AttemptRecord] = []

        async def run(provider: Provider, target: Target) -> ProviderResult:
            return await provider.complete(
                request,
                target.model,
                self._config.timeouts.request_seconds,
                faults.get(provider.name),
            )

        result, provider_name, ttft = await self._execute(alias, faults, attempts, run)
        return CompletedRoute(result=result, provider=provider_name, attempts=attempts, ttft_ms=ttft)

    async def stream(
        self,
        request: ChatCompletionRequest,
        alias: str,
        faults: dict[str, str],
    ) -> StreamingRoute:
        attempts: list[AttemptRecord] = []

        async def run(provider: Provider, target: Target) -> tuple[AsyncIterator[StreamChunk], str]:
            agen = provider.stream(
                request,
                target.model,
                self._config.timeouts.request_seconds,
                faults.get(provider.name),
            )
            try:
                first = await agen.__anext__()
            except StopAsyncIteration as exc:
                await _close_quietly(agen)
                raise ProviderError(
                    "upstream stream ended before the first event",
                    status_code=502,
                    retryable=True,
                    failover=True,
                    error_type="malformed_stream",
                ) from exc
            except BaseException:
                await _close_quietly(agen)
                raise

            async def iterate() -> AsyncIterator[StreamChunk]:
                try:
                    yield first
                    async for chunk in agen:
                        yield chunk
                finally:
                    await _close_quietly(agen)

            return iterate(), target.model

        streamed, provider_name, ttft = await self._execute(alias, faults, attempts, run)
        iterator, upstream_model = streamed
        return StreamingRoute(
            provider=provider_name,
            upstream_model=upstream_model,
            attempts=attempts,
            iterator=iterator,
            ttft_ms=ttft,
        )

    async def _execute[T](
        self,
        alias: str,
        faults: dict[str, str],
        attempts: list[AttemptRecord],
        call: Callable[[Provider, Target], Awaitable[T]],
    ) -> tuple[T, str, float]:
        del faults
        spec = self._config.aliases.get(alias)
        if spec is None:
            raise ConduitError(
                f"Model '{alias}' is not configured",
                status_code=404,
                error_type="invalid_request",
                code="model_not_found",
            )
        for target in spec.targets:
            provider = self._providers.get(target.provider)
            if provider is None or not provider.configured:
                attempts.append(
                    AttemptRecord(
                        provider=target.provider,
                        model=target.model,
                        status="unconfigured",
                        latency_ms=0,
                    )
                )
                continue
            outcome = await self._attempt_target(provider, target, attempts, call)
            if outcome is not None:
                value, latency = outcome
                return value, provider.name, latency
        self._raise_exhausted(attempts)

    async def _attempt_target[T](
        self,
        provider: Provider,
        target: Target,
        attempts: list[AttemptRecord],
        call: Callable[[Provider, Target], Awaitable[T]],
    ) -> tuple[T, float] | None:
        max_attempts = self._config.retry.max_attempts
        for attempt in range(max_attempts):
            if not await self._breaker.allow(provider.name):
                attempts.append(
                    AttemptRecord(
                        provider=provider.name,
                        model=target.model,
                        status="circuit_open",
                        latency_ms=0,
                        error_type="circuit_open",
                    )
                )
                return None
            started = time.perf_counter()
            try:
                value = await call(provider, target)
            except ProviderError as exc:
                latency = (time.perf_counter() - started) * 1000
                attempts.append(
                    AttemptRecord(
                        provider=provider.name,
                        model=target.model,
                        status=exc.error_type,
                        latency_ms=latency,
                        error_type=exc.error_type,
                    )
                )
                if not exc.failover:
                    # The provider answered, so it is healthy. The request itself is not.
                    await self._breaker.record_success(provider.name)
                    raise ConduitError(
                        exc.message,
                        status_code=exc.status_code if 400 <= exc.status_code < 500 else 502,
                        error_type="invalid_request",
                        code="upstream_rejected",
                        headers={"x-conduit-attempt-statuses": attempt_header(attempts)},
                        extra={"attempts": [item.as_dict() for item in attempts]},
                    ) from exc
                await self._breaker.record_failure(provider.name)
                if exc.retryable and attempt + 1 < max_attempts:
                    await self._sleep(self._delay(attempt))
                    continue
                return None
            except Exception:
                latency = (time.perf_counter() - started) * 1000
                logger.exception(
                    "provider_attempt_failed",
                    extra={"conduit_fields": {"provider": provider.name, "model": target.model}},
                )
                attempts.append(
                    AttemptRecord(
                        provider=provider.name,
                        model=target.model,
                        status="error",
                        latency_ms=latency,
                        error_type="error",
                    )
                )
                await self._breaker.record_failure(provider.name)
                if attempt + 1 < max_attempts:
                    await self._sleep(self._delay(attempt))
                    continue
                return None
            else:
                latency = (time.perf_counter() - started) * 1000
                await self._breaker.record_success(provider.name)
                attempts.append(
                    AttemptRecord(
                        provider=provider.name,
                        model=target.model,
                        status="success",
                        latency_ms=latency,
                    )
                )
                return value, latency
        return None

    def _delay(self, attempt: int) -> float:
        delay = self._config.retry.base_delay_seconds * (2**attempt)
        return float(min(delay, self._config.retry.max_delay_seconds))

    def _raise_exhausted(self, attempts: list[AttemptRecord]) -> NoReturn:
        raise ConduitError(
            "No provider could serve this request",
            status_code=503,
            error_type="provider_unavailable",
            code="all_providers_failed",
            headers={"x-conduit-attempt-statuses": attempt_header(attempts)},
            extra={"attempts": [item.as_dict() for item in attempts]},
        )


async def _sleep(seconds: float) -> None:
    if seconds > 0:
        await asyncio.sleep(seconds)


async def _close_quietly(agen: AsyncIterator[StreamChunk]) -> None:
    closer = getattr(agen, "aclose", None)
    if closer is None:
        return
    try:
        await closer()
    except Exception:
        return
