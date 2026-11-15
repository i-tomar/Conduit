"""One chat completion, from API key to usage row.

The request path is: authenticate, replay an idempotent success if there is
one, rate-limit, cache, reserve budget, route, then reconcile the reservation
to the measured token and cost totals.
"""

from __future__ import annotations

import asyncio
import json
import logging
import math
import time
from collections.abc import AsyncIterator
from dataclasses import dataclass, field
from typing import Any

from fastapi import Request
from fastapi.responses import JSONResponse, Response, StreamingResponse
from sqlalchemy import delete, select, update
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession

from conduit.budgets import credit, reconcile, try_charge
from conduit.cache import CachedCompletion, exact_cache_key
from conduit.costing import cost_micros, max_alias_cost_micros, micros_to_usd
from conduit.db import IdempotencyRecord, UsageRecord
from conduit.errors import ConduitError
from conduit.keys import authenticate, bearer_token
from conduit.logging_config import log_event
from conduit.routing import AttemptRecord, StreamingRoute, attempt_header, failover_count
from conduit.runtime import Runtime
from conduit.schemas import ChatCompletionRequest
from conduit.tokens import estimate_message_tokens, estimate_text_tokens, message_text
from conduit.tracing import tracer

logger = logging.getLogger("conduit")


@dataclass(slots=True)
class IdempotencyGate:
    action: str
    record_id: int | None = None
    response_json: str | None = None
    status_code: int = 200


@dataclass(slots=True)
class Hold:
    rpm_taken: bool = False
    keep_rpm: bool = False
    tpm_taken: int = 0
    reserved_tokens: int = 0
    reserved_cost: int = 0
    idempotency_id: int | None = None
    rpm_capacity: int = 0
    settled: bool = False
    released: bool = False
    failure_status: str = "error"
    failure_type: str | None = None
    attempt_log: list[dict[str, Any]] = field(default_factory=list)
    cache_status: str = "miss"
    provider: str = ""
    upstream_model: str = ""


def parse_faults(header: str | None, allowed: bool) -> dict[str, str]:
    if not header or not allowed:
        return {}
    faults: dict[str, str] = {}
    for part in header.split(","):
        name, sep, fault = part.partition("=")
        if sep and name.strip() and fault.strip():
            faults[name.strip()] = fault.strip()
    return faults


def scope_for(runtime: Runtime, key_id: int) -> str:
    if runtime.config.cache.scope == "tenant":
        return f"tenant:{key_id}"
    return "global"


def completion_payload(
    request_id: str,
    model: str,
    message: dict[str, Any],
    finish_reason: str,
    prompt_tokens: int,
    completion_tokens: int,
) -> dict[str, Any]:
    return {
        "id": f"chatcmpl_{request_id}",
        "object": "chat.completion",
        "created": int(time.time()),
        "model": model,
        "choices": [
            {
                "index": 0,
                "message": message,
                "finish_reason": finish_reason,
            }
        ],
        "usage": {
            "prompt_tokens": prompt_tokens,
            "completion_tokens": completion_tokens,
            "total_tokens": prompt_tokens + completion_tokens,
        },
    }


def sse(payload: dict[str, Any]) -> str:
    return f"data: {json.dumps(payload, separators=(',', ':'))}\n\n"


def chunk_payload(
    request_id: str,
    model: str,
    delta: dict[str, Any] | None = None,
    finish_reason: str | None = None,
    usage: dict[str, int] | None = None,
) -> dict[str, Any]:
    body: dict[str, Any] = {
        "id": f"chatcmpl_{request_id}",
        "object": "chat.completion.chunk",
        "created": int(time.time()),
        "model": model,
    }
    if usage is not None:
        body["choices"] = []
        body["usage"] = usage
    else:
        body["choices"] = [{"index": 0, "delta": delta or {}, "finish_reason": finish_reason}]
    return body


def gateway_headers(
    request_id: str,
    *,
    provider: str,
    upstream_model: str,
    cache_status: str,
    attempts: list[AttemptRecord] | None = None,
    extra: dict[str, str] | None = None,
) -> dict[str, str]:
    attempt_list = attempts or []
    headers = {
        "x-request-id": request_id,
        "x-conduit-provider": provider,
        "x-conduit-upstream-model": upstream_model,
        "x-conduit-cache": cache_status,
        "x-conduit-attempts": str(len(attempt_list)),
        "x-conduit-attempt-statuses": attempt_header(attempt_list),
        "x-conduit-failover": str(failover_count(attempt_list)),
    }
    if extra:
        headers.update(extra)
    return headers


def rate_limit_error(kind: str, capacity: int, remaining: float, cost: int) -> ConduitError:
    per_second = capacity / 60 if capacity else 0
    deficit = max(cost - remaining, 1)
    retry_after = 1 if per_second <= 0 else max(1, math.ceil(deficit / per_second))
    return ConduitError(
        f"{kind} rate limit exceeded",
        status_code=429,
        error_type="rate_limit_exceeded",
        code="rate_limit_exceeded",
        headers={"retry-after": str(retry_after)},
    )


def budget_error() -> ConduitError:
    return ConduitError(
        "API key budget exhausted",
        status_code=402,
        error_type="budget_exceeded",
        code="budget_exceeded",
    )


async def handle_chat(request: Request, body: ChatCompletionRequest) -> Response:
    runtime: Runtime = request.app.state.runtime
    request_id = str(request.state.request_id)
    started = time.perf_counter()
    if not body.messages:
        raise ConduitError(
            "messages must not be empty",
            status_code=400,
            error_type="invalid_request",
            code="empty_messages",
        )
    if body.n not in (None, 1):
        raise ConduitError(
            "Conduit currently supports n=1",
            status_code=400,
            error_type="invalid_request",
            code="unsupported_n",
        )
    if body.model not in runtime.config.aliases:
        raise ConduitError(
            f"Model '{body.model}' is not configured",
            status_code=404,
            error_type="invalid_request",
            code="model_not_found",
        )

    with tracer().start_as_current_span("conduit.chat") as span:
        span.set_attribute("conduit.model", body.model)
        span.set_attribute("conduit.stream", body.stream)
        span.set_attribute("conduit.request_id", request_id)
        try:
            response = await _handle_chat(runtime, request, body, request_id, started, span)
        except ConduitError as exc:
            span.set_attribute("conduit.error", exc.code)
            raise
        return response


async def _handle_chat(
    runtime: Runtime,
    request: Request,
    body: ChatCompletionRequest,
    request_id: str,
    started: float,
    span: Any,
) -> Response:
    idem_key = request.headers.get("idempotency-key")
    async with runtime.sessionmaker() as session:
        raw_key = bearer_token(request.headers.get("authorization"))
        principal = await authenticate(session, raw_key, runtime.settings.key_pepper)
        gate = await _begin_idempotency(session, runtime, principal.id, body, idem_key)

    if gate.action == "replay" and gate.response_json:
        payload = json.loads(gate.response_json)
        headers = {
            "x-request-id": request_id,
            "x-conduit-idempotent-replay": "true",
            "x-conduit-cache": "replay",
        }
        span.set_attribute("conduit.cache", "replay")
        if body.stream:
            return StreamingResponse(
                _replay_stream(payload, body.model, request_id),
                status_code=gate.status_code,
                media_type="text/event-stream",
                headers=headers,
            )
        return JSONResponse(payload, status_code=gate.status_code, headers=headers)

    hold = Hold(idempotency_id=gate.record_id, rpm_capacity=principal.rpm)
    handed_off = False
    bypass = (request.headers.get("x-conduit-cache") or "").lower() == "bypass"
    if bypass:
        hold.cache_status = "bypass"
    faults = parse_faults(request.headers.get("x-conduit-fault"), runtime.settings.allow_fault_injection)
    rpm_key = f"conduit:rl:rpm:{principal.id}"
    tpm_key = f"conduit:rl:tpm:{principal.id}"
    try:
        try:
            allowed, remaining = await runtime.buckets.take(rpm_key, principal.rpm, 1)
        except Exception as exc:
            raise ConduitError(
                "Rate limiter is unavailable",
                status_code=503,
                error_type="rate_limiter_unavailable",
                code="rate_limiter_unavailable",
            ) from exc
        if not allowed:
            runtime.metrics.rate_limited.labels("requests").inc()
            raise rate_limit_error("request", principal.rpm, remaining, 1)
        hold.rpm_taken = True

        cached = await _read_cache(runtime, principal.id, body, bypass)
        if cached is not None:
            response = await _serve_cache(
                runtime,
                hold,
                principal_id=principal.id,
                principal_tpm=principal.tpm,
                tpm_key=tpm_key,
                body=body,
                cached=cached,
                request_id=request_id,
                idem_key=idem_key,
                started=started,
                span=span,
            )
            hold.settled = True
            return response

        prompt_est = estimate_message_tokens(body.messages)
        completion_reserve = body.completion_cap() or 0
        projected = max(prompt_est + completion_reserve, 1)
        try:
            allowed, remaining = await runtime.buckets.take(tpm_key, principal.tpm, projected)
        except Exception as exc:
            raise ConduitError(
                "Rate limiter is unavailable",
                status_code=503,
                error_type="rate_limiter_unavailable",
                code="rate_limiter_unavailable",
            ) from exc
        if not allowed:
            runtime.metrics.rate_limited.labels("tokens").inc()
            raise rate_limit_error("token", principal.tpm, remaining, projected)
        hold.tpm_taken = projected
        cost_reserve = max_alias_cost_micros(runtime.config, body.model, prompt_est, completion_reserve)
        if not await _reserve(runtime, principal.id, projected, cost_reserve):
            runtime.metrics.budget_rejected.inc()
            raise budget_error()
        hold.reserved_tokens = projected
        hold.reserved_cost = cost_reserve
        hold.keep_rpm = True

        if body.stream:
            route = await runtime.router.stream(body, body.model, faults)
            hold.provider = route.provider
            hold.upstream_model = route.upstream_model
            hold.attempt_log = [item.as_dict() for item in route.attempts]
            headers = gateway_headers(
                request_id,
                provider=route.provider,
                upstream_model=route.upstream_model,
                cache_status=hold.cache_status,
                attempts=route.attempts,
            )
            response = StreamingResponse(
                _stream_events(
                    runtime,
                    hold,
                    route,
                    body=body,
                    request_id=request_id,
                    idem_key=idem_key,
                    started=started,
                    tpm_key=tpm_key,
                    tpm_capacity=principal.tpm,
                    principal_id=principal.id,
                    span=span,
                ),
                media_type="text/event-stream",
                headers=headers,
            )
            handed_off = True
            return response

        completed = await runtime.router.complete(body, body.model, faults)
        payload = completion_payload(
            request_id,
            body.model,
            completed.result.message,
            completed.result.finish_reason,
            completed.result.prompt_tokens,
            completed.result.completion_tokens,
        )
        await _persist(
            runtime,
            hold,
            principal_id=principal.id,
            request_id=request_id,
            idem_key=idem_key,
            body=body,
            payload=payload,
            provider=completed.provider,
            upstream_model=completed.result.upstream_model,
            prompt_tokens=completed.result.prompt_tokens,
            completion_tokens=completed.result.completion_tokens,
            cost=cost_micros(
                runtime.config,
                completed.provider,
                completed.result.upstream_model,
                completed.result.prompt_tokens,
                completed.result.completion_tokens,
            ),
            started=started,
            ttft_ms=(time.perf_counter() - started) * 1000,
            attempts=completed.attempts,
            streamed=False,
            status="success",
            tpm_key=tpm_key,
            tpm_capacity=principal.tpm,
            span=span,
        )
        await _store_cache(
            runtime,
            principal.id,
            body,
            payload,
            completed.provider,
            completed.result.upstream_model,
        )
        headers = gateway_headers(
            request_id,
            provider=completed.provider,
            upstream_model=completed.result.upstream_model,
            cache_status=hold.cache_status,
            attempts=completed.attempts,
        )
        hold.settled = True
        return JSONResponse(payload, headers=headers)
    except ConduitError as exc:
        hold.failure_status = exc.code
        hold.failure_type = exc.error_type
        attempts = exc.extra.get("attempts")
        if isinstance(attempts, list):
            hold.attempt_log = [item for item in attempts if isinstance(item, dict)]
        raise
    finally:
        if not hold.settled and not handed_off:
            await _release(
                runtime,
                hold,
                principal_id=principal.id,
                request_id=request_id,
                idem_key=idem_key,
                body=body,
                started=started,
                tpm_key=tpm_key,
                tpm_capacity=principal.tpm,
            )


async def _begin_idempotency(
    session: AsyncSession,
    runtime: Runtime,
    key_id: int,
    body: ChatCompletionRequest,
    idem_key: str | None,
) -> IdempotencyGate:
    if not idem_key:
        return IdempotencyGate("proceed")
    if len(idem_key) > 200:
        raise ConduitError(
            "Idempotency-Key must be 200 characters or fewer",
            status_code=400,
            error_type="invalid_request",
            code="invalid_idempotency_key",
        )
    fingerprint = body.fingerprint()
    for _ in range(40):
        existing = await session.scalar(
            select(IdempotencyRecord).where(
                IdempotencyRecord.api_key_id == key_id,
                IdempotencyRecord.idempotency_key == idem_key,
            )
        )
        now = int(time.time() * 1000)
        if existing is None:
            record = IdempotencyRecord(
                api_key_id=key_id,
                idempotency_key=idem_key,
                request_hash=fingerprint,
                status="in_progress",
                created_at_ms=now,
            )
            session.add(record)
            try:
                await session.commit()
            except IntegrityError:
                await session.rollback()
                continue
            return IdempotencyGate("proceed", record_id=record.id)
        if existing.request_hash != fingerprint:
            raise ConduitError(
                "Idempotency-Key was already used for a different request",
                status_code=409,
                error_type="idempotency_conflict",
                code="idempotency_key_reused",
            )
        if existing.status == "complete" and existing.response_json:
            return IdempotencyGate(
                "replay",
                record_id=existing.id,
                response_json=existing.response_json,
                status_code=existing.status_code or 200,
            )
        if now - existing.created_at_ms > runtime.settings.idempotency_stale_ms:
            await session.delete(existing)
            await session.commit()
            continue
        await session.rollback()
        await asyncio.sleep(0.05)
    raise ConduitError(
        "A request with this idempotency key is still in progress",
        status_code=409,
        error_type="idempotency_conflict",
        code="idempotency_in_progress",
    )


async def _read_cache(
    runtime: Runtime,
    key_id: int,
    body: ChatCompletionRequest,
    bypass: bool,
) -> CachedCompletion | None:
    if bypass or not body.cacheable():
        return None
    scope = scope_for(runtime, key_id)
    try:
        hit = await runtime.exact_cache.get(exact_cache_key(scope, body))
        if hit is not None:
            runtime.metrics.cache_lookups.labels("exact_hit").inc()
            return hit
        if runtime.config.cache.semantic_enabled:
            found = await runtime.semantic_cache.lookup(scope, body.model, message_text(body.messages))
            if found is not None:
                cached, _score = found
                runtime.metrics.cache_lookups.labels("semantic_hit").inc()
                return cached
        runtime.metrics.cache_lookups.labels("miss").inc()
    except Exception:
        logger.exception("cache_read_failed")
    return None


async def _store_cache(
    runtime: Runtime,
    key_id: int,
    body: ChatCompletionRequest,
    payload: dict[str, Any],
    provider: str,
    upstream_model: str,
) -> None:
    if not body.cacheable():
        return
    usage = payload["usage"]
    cached = CachedCompletion(
        message=payload["choices"][0]["message"],
        finish_reason=payload["choices"][0]["finish_reason"],
        prompt_tokens=int(usage["prompt_tokens"]),
        completion_tokens=int(usage["completion_tokens"]),
        provider=provider,
        upstream_model=upstream_model,
        kind="exact",
    )
    scope = scope_for(runtime, key_id)
    try:
        await runtime.exact_cache.set(exact_cache_key(scope, body), cached)
        if runtime.config.cache.semantic_enabled:
            await runtime.semantic_cache.store(scope, body.model, message_text(body.messages), cached)
    except Exception:
        logger.exception("cache_write_failed")


async def _reserve(runtime: Runtime, key_id: int, tokens: int, cost: int) -> bool:
    async with runtime.sessionmaker() as session:
        ok = await try_charge(session, key_id, tokens, cost)
        await session.commit()
        return ok


async def _serve_cache(
    runtime: Runtime,
    hold: Hold,
    *,
    principal_id: int,
    principal_tpm: int,
    tpm_key: str,
    body: ChatCompletionRequest,
    cached: CachedCompletion,
    request_id: str,
    idem_key: str | None,
    started: float,
    span: Any,
) -> Response:
    total = cached.prompt_tokens + cached.completion_tokens
    try:
        allowed, remaining = await runtime.buckets.take(tpm_key, principal_tpm, max(total, 1))
    except Exception as exc:
        raise ConduitError(
            "Rate limiter is unavailable",
            status_code=503,
            error_type="rate_limiter_unavailable",
            code="rate_limiter_unavailable",
        ) from exc
    if not allowed:
        runtime.metrics.rate_limited.labels("tokens").inc()
        raise rate_limit_error("token", principal_tpm, remaining, max(total, 1))
    hold.tpm_taken = max(total, 1)
    if not await _reserve(runtime, principal_id, total, 0):
        runtime.metrics.budget_rejected.inc()
        raise budget_error()
    hold.reserved_tokens = total
    hold.cache_status = cached.kind
    payload = completion_payload(
        request_id,
        body.model,
        cached.message,
        cached.finish_reason,
        cached.prompt_tokens,
        cached.completion_tokens,
    )
    await _persist(
        runtime,
        hold,
        principal_id=principal_id,
        request_id=request_id,
        idem_key=idem_key,
        body=body,
        payload=payload,
        provider=cached.provider,
        upstream_model=cached.upstream_model,
        prompt_tokens=cached.prompt_tokens,
        completion_tokens=cached.completion_tokens,
        cost=0,
        started=started,
        ttft_ms=(time.perf_counter() - started) * 1000,
        attempts=[],
        streamed=body.stream,
        status="success",
        tpm_key=tpm_key,
        tpm_capacity=principal_tpm,
        span=span,
    )
    if cached.kind == "semantic":
        await _store_cache(runtime, principal_id, body, payload, cached.provider, cached.upstream_model)
    headers = gateway_headers(
        request_id,
        provider=cached.provider,
        upstream_model=cached.upstream_model,
        cache_status=cached.kind,
    )
    if body.stream:
        return StreamingResponse(
            _replay_stream(payload, body.model, request_id),
            media_type="text/event-stream",
            headers=headers,
        )
    return JSONResponse(payload, headers=headers)


async def _persist(
    runtime: Runtime,
    hold: Hold,
    *,
    principal_id: int,
    request_id: str,
    idem_key: str | None,
    body: ChatCompletionRequest,
    payload: dict[str, Any],
    provider: str,
    upstream_model: str,
    prompt_tokens: int,
    completion_tokens: int,
    cost: int,
    started: float,
    ttft_ms: float | None,
    attempts: list[AttemptRecord],
    streamed: bool,
    status: str,
    tpm_key: str,
    tpm_capacity: int,
    span: Any,
) -> None:
    latency_ms = (time.perf_counter() - started) * 1000
    total = prompt_tokens + completion_tokens
    async with runtime.sessionmaker() as session:
        await reconcile(
            session,
            principal_id,
            reserved_tokens=hold.reserved_tokens,
            reserved_cost=hold.reserved_cost,
            actual_tokens=total,
            actual_cost=cost,
        )
        session.add(
            _usage_row(
                request_id=request_id,
                principal_id=principal_id,
                idem_key=idem_key,
                body=body,
                provider=provider,
                upstream_model=upstream_model,
                prompt_tokens=prompt_tokens,
                completion_tokens=completion_tokens,
                cost=cost,
                latency_ms=latency_ms,
                ttft_ms=ttft_ms,
                cache_status=hold.cache_status,
                attempts=attempts,
                attempt_log=hold.attempt_log,
                status=status,
                error_type=None,
                streamed=streamed,
            )
        )
        if hold.idempotency_id is not None:
            await session.execute(
                update(IdempotencyRecord)
                .where(IdempotencyRecord.id == hold.idempotency_id)
                .values(status="complete", response_json=json.dumps(payload), status_code=200)
            )
        await session.commit()
    hold.settled = True
    await _align_tokens(runtime, tpm_key, tpm_capacity, hold.tpm_taken, total)
    hold.reserved_tokens = total
    hold.reserved_cost = cost
    hold.provider = provider
    hold.upstream_model = upstream_model
    _note_outcome(
        runtime,
        provider=provider,
        model=body.model,
        status=status,
        cache_status=hold.cache_status,
        latency_ms=latency_ms,
        ttft_ms=ttft_ms,
        prompt_tokens=prompt_tokens,
        completion_tokens=completion_tokens,
        cost_micros=cost,
        attempts=attempts,
        request_id=request_id,
        prompt_chars=len(message_text(body.messages)) if runtime.settings.log_prompts else None,
    )
    span.set_attribute("conduit.provider", provider)
    span.set_attribute("conduit.cache", hold.cache_status)
    span.set_attribute("conduit.status", status)


def _usage_row(
    *,
    request_id: str,
    principal_id: int,
    idem_key: str | None,
    body: ChatCompletionRequest,
    provider: str,
    upstream_model: str,
    prompt_tokens: int,
    completion_tokens: int,
    cost: int,
    latency_ms: float,
    ttft_ms: float | None,
    cache_status: str,
    attempts: list[AttemptRecord],
    attempt_log: list[dict[str, Any]],
    status: str,
    error_type: str | None,
    streamed: bool,
) -> UsageRecord:
    log = [item.as_dict() for item in attempts] if attempts else attempt_log
    return UsageRecord(
        request_id=request_id,
        api_key_id=principal_id,
        idempotency_key=idem_key,
        model_alias=body.model,
        provider=provider,
        upstream_model=upstream_model,
        prompt_tokens=prompt_tokens,
        completion_tokens=completion_tokens,
        total_tokens=prompt_tokens + completion_tokens,
        cost_micros=cost,
        latency_ms=latency_ms,
        ttft_ms=ttft_ms,
        cache_status=cache_status,
        failover_count=failover_count(attempts) if attempts else _failover_from_log(log),
        attempt_log=json.dumps(log),
        status=status,
        error_type=error_type,
        streamed=streamed,
        created_at_ms=int(time.time() * 1000),
    )


def _failover_from_log(log: list[dict[str, Any]]) -> int:
    failed: list[str] = []
    for item in log:
        status = str(item.get("status", ""))
        provider = str(item.get("provider", ""))
        if status in {"success", "circuit_open", "unconfigured"}:
            continue
        if provider and (not failed or failed[-1] != provider):
            failed.append(provider)
    return len(failed)


async def _align_tokens(runtime: Runtime, key: str, capacity: int, reserved: int, actual: int) -> None:
    delta = actual - reserved
    try:
        if delta > 0:
            await runtime.buckets.force_take(key, capacity, delta)
        elif delta < 0:
            await runtime.buckets.refund(key, capacity, -delta)
    except Exception:
        logger.exception("tpm_reconcile_failed")


def _note_outcome(
    runtime: Runtime,
    *,
    provider: str,
    model: str,
    status: str,
    cache_status: str,
    latency_ms: float,
    ttft_ms: float | None,
    prompt_tokens: int,
    completion_tokens: int,
    cost_micros: int,
    attempts: list[AttemptRecord],
    request_id: str,
    prompt_chars: int | None,
) -> None:
    label = provider or "none"
    runtime.metrics.llm_requests.labels(label, model, status, cache_status).inc()
    runtime.metrics.latency.labels(label, model, cache_status).observe(max(latency_ms, 0) / 1000)
    if ttft_ms is not None:
        runtime.metrics.ttft.labels(label, model).observe(max(ttft_ms, 0) / 1000)
    if prompt_tokens:
        runtime.metrics.tokens.labels(label, model, "prompt").inc(prompt_tokens)
    if completion_tokens:
        runtime.metrics.tokens.labels(label, model, "completion").inc(completion_tokens)
    if cost_micros:
        runtime.metrics.cost.labels(label, model).inc(cost_micros / 1_000_000)
    runtime.metrics.observe_attempts(attempts)
    fields: dict[str, object] = {
        "request_id": request_id,
        "provider": provider,
        "model": model,
        "status": status,
        "latency_ms": round(latency_ms, 3),
        "cache": cache_status,
        "tokens": prompt_tokens + completion_tokens,
        "cost_usd": micros_to_usd(cost_micros),
    }
    if prompt_chars is not None:
        fields["prompt_chars"] = prompt_chars
    log_event(logger, logging.INFO, "chat_completion", **fields)


async def _release(
    runtime: Runtime,
    hold: Hold,
    *,
    principal_id: int,
    request_id: str,
    idem_key: str | None,
    body: ChatCompletionRequest,
    started: float,
    tpm_key: str,
    tpm_capacity: int,
) -> None:
    if hold.settled or hold.released:
        return
    hold.released = True
    try:
        if hold.tpm_taken:
            await runtime.buckets.refund(tpm_key, tpm_capacity, hold.tpm_taken)
            hold.tpm_taken = 0
        if hold.rpm_taken and not hold.keep_rpm and hold.rpm_capacity > 0:
            await runtime.buckets.refund(f"conduit:rl:rpm:{principal_id}", hold.rpm_capacity, 1)
    except Exception:
        logger.exception("rate_limit_refund_failed")
    try:
        async with runtime.sessionmaker() as session:
            if hold.reserved_tokens or hold.reserved_cost:
                await credit(session, principal_id, hold.reserved_tokens, hold.reserved_cost)
            if hold.idempotency_id is not None:
                await session.execute(delete(IdempotencyRecord).where(IdempotencyRecord.id == hold.idempotency_id))
            latency_ms = (time.perf_counter() - started) * 1000
            provider = str(hold.attempt_log[-1]["provider"]) if hold.attempt_log else ""
            upstream = str(hold.attempt_log[-1]["model"]) if hold.attempt_log else ""
            session.add(
                _usage_row(
                    request_id=request_id,
                    principal_id=principal_id,
                    idem_key=idem_key,
                    body=body,
                    provider=provider,
                    upstream_model=upstream,
                    prompt_tokens=0,
                    completion_tokens=0,
                    cost=0,
                    latency_ms=latency_ms,
                    ttft_ms=None,
                    cache_status=hold.cache_status,
                    attempts=[],
                    attempt_log=hold.attempt_log,
                    status=hold.failure_status,
                    error_type=hold.failure_type,
                    streamed=body.stream,
                )
            )
            await session.commit()
        _note_outcome(
            runtime,
            provider=provider if hold.attempt_log else "",
            model=body.model,
            status=hold.failure_status,
            cache_status=hold.cache_status,
            latency_ms=(time.perf_counter() - started) * 1000,
            ttft_ms=None,
            prompt_tokens=0,
            completion_tokens=0,
            cost_micros=0,
            attempts=[],
            request_id=request_id,
            prompt_chars=len(message_text(body.messages)) if runtime.settings.log_prompts else None,
        )
    except Exception:
        logger.exception("failure_accounting_failed")


async def _stream_events(
    runtime: Runtime,
    hold: Hold,
    route: StreamingRoute,
    *,
    body: ChatCompletionRequest,
    request_id: str,
    idem_key: str | None,
    started: float,
    tpm_key: str,
    tpm_capacity: int,
    principal_id: int,
    span: Any,
) -> AsyncIterator[str]:
    parts: list[str] = []
    prompt_tokens: int | None = None
    completion_tokens: int | None = None
    finish = "stop"
    ttft_ms: float | None = None
    try:
        async for chunk in route.iterator:
            if ttft_ms is None:
                ttft_ms = (time.perf_counter() - started) * 1000
            content = chunk.delta.get("content")
            if isinstance(content, str):
                parts.append(content)
            if chunk.prompt_tokens is not None:
                prompt_tokens = chunk.prompt_tokens
            if chunk.completion_tokens is not None:
                completion_tokens = chunk.completion_tokens
            if chunk.finish_reason:
                finish = chunk.finish_reason
            yield sse(chunk_payload(request_id, body.model, chunk.delta, chunk.finish_reason))
    except Exception:
        logger.exception("stream_interrupted")
        yield sse(
            {
                "error": {
                    "message": "The upstream stream ended before a complete response",
                    "type": "stream_error",
                    "code": "stream_error",
                }
            }
        )
        text = "".join(parts)
        if text:
            prompt = prompt_tokens if prompt_tokens is not None else estimate_message_tokens(body.messages)
            completion = completion_tokens if completion_tokens is not None else (estimate_text_tokens(text) or 1)
            payload = completion_payload(
                request_id,
                body.model,
                {"role": "assistant", "content": text},
                "error",
                prompt,
                completion,
            )
            try:
                await _persist(
                    runtime,
                    hold,
                    principal_id=principal_id,
                    request_id=request_id,
                    idem_key=idem_key,
                    body=body,
                    payload=payload,
                    provider=route.provider,
                    upstream_model=route.upstream_model,
                    prompt_tokens=prompt,
                    completion_tokens=completion,
                    cost=cost_micros(runtime.config, route.provider, route.upstream_model, prompt, completion),
                    started=started,
                    ttft_ms=ttft_ms,
                    attempts=route.attempts,
                    streamed=True,
                    status="stream_error",
                    tpm_key=tpm_key,
                    tpm_capacity=tpm_capacity,
                    span=span,
                )
                hold.settled = True
            except Exception:
                logger.exception("partial_stream_accounting_failed")
                await _release(
                    runtime,
                    hold,
                    principal_id=principal_id,
                    request_id=request_id,
                    idem_key=idem_key,
                    body=body,
                    started=started,
                    tpm_key=tpm_key,
                    tpm_capacity=tpm_capacity,
                )
        else:
            hold.failure_status = "stream_error"
            hold.failure_type = "stream_error"
            await _release(
                runtime,
                hold,
                principal_id=principal_id,
                request_id=request_id,
                idem_key=idem_key,
                body=body,
                started=started,
                tpm_key=tpm_key,
                tpm_capacity=tpm_capacity,
            )
        return

    text = "".join(parts)
    prompt = prompt_tokens if prompt_tokens is not None else estimate_message_tokens(body.messages)
    completion = completion_tokens if completion_tokens is not None else (estimate_text_tokens(text) or 1)
    payload = completion_payload(
        request_id,
        body.model,
        {"role": "assistant", "content": text},
        finish,
        prompt,
        completion,
    )
    usage = payload["usage"]
    try:
        yield sse(chunk_payload(request_id, body.model, usage=usage))
        await _persist(
            runtime,
            hold,
            principal_id=principal_id,
            request_id=request_id,
            idem_key=idem_key,
            body=body,
            payload=payload,
            provider=route.provider,
            upstream_model=route.upstream_model,
            prompt_tokens=prompt,
            completion_tokens=completion,
            cost=cost_micros(runtime.config, route.provider, route.upstream_model, prompt, completion),
            started=started,
            ttft_ms=ttft_ms if ttft_ms is not None else (time.perf_counter() - started) * 1000,
            attempts=route.attempts,
            streamed=True,
            status="success",
            tpm_key=tpm_key,
            tpm_capacity=tpm_capacity,
            span=span,
        )
        hold.settled = True
        await _store_cache(runtime, principal_id, body, payload, route.provider, route.upstream_model)
    except Exception:
        logger.exception("stream_accounting_failed")
        if not hold.settled:
            await _release(
                runtime,
                hold,
                principal_id=principal_id,
                request_id=request_id,
                idem_key=idem_key,
                body=body,
                started=started,
                tpm_key=tpm_key,
                tpm_capacity=tpm_capacity,
            )
        return
    yield "data: [DONE]\n\n"


async def _replay_stream(payload: dict[str, Any], model: str, request_id: str) -> AsyncIterator[str]:
    message = payload["choices"][0]["message"]
    content = message.get("content") or ""
    if not isinstance(content, str):
        content = str(content)
    yield sse(chunk_payload(request_id, model, {"role": "assistant"}))
    for index in range(0, len(content), 24):
        yield sse(chunk_payload(request_id, model, {"content": content[index : index + 24]}))
    yield sse(chunk_payload(request_id, model, {}, "stop"))
    yield sse(chunk_payload(request_id, model, usage=payload["usage"]))
    yield "data: [DONE]\n\n"
