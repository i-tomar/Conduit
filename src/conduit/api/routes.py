"""Public, OpenAI-compatible, and admin routes."""

from __future__ import annotations

import time
from datetime import UTC, datetime
from typing import Any

from fastapi import APIRouter, Query, Request
from fastapi.responses import HTMLResponse, JSONResponse, Response
from prometheus_client import CONTENT_TYPE_LATEST
from sqlalchemy import func, select, text, update

from conduit import __version__
from conduit.api.pages import admin_page, home_page
from conduit.costing import micros_to_usd, usd_to_micros
from conduit.db import APIKey, UsageRecord
from conduit.errors import ConduitError
from conduit.keys import admin_authorized, authenticate, bearer_token, generate_api_key, hash_api_key, key_prefix
from conduit.runtime import Runtime
from conduit.schemas import ChatCompletionRequest, CreateKeyRequest
from conduit.service import handle_chat

router = APIRouter()


def _runtime(request: Request) -> Runtime:
    runtime: Runtime | None = getattr(request.app.state, "runtime", None)
    if runtime is None:
        raise ConduitError(
            "Gateway is not ready",
            status_code=503,
            error_type="server_error",
            code="not_ready",
        )
    return runtime


def _require_admin(request: Request, runtime: Runtime) -> None:
    presented = request.headers.get("x-admin-token", "")
    if not admin_authorized(presented, runtime.settings.admin_token):
        raise ConduitError(
            "Admin token rejected",
            status_code=401,
            error_type="authentication_error",
            code="invalid_admin_token",
        )


@router.get("/", response_class=HTMLResponse)
async def home(request: Request) -> HTMLResponse:
    return HTMLResponse(home_page(_runtime(request).config))


@router.get("/admin", response_class=HTMLResponse)
async def admin() -> HTMLResponse:
    return HTMLResponse(admin_page())


@router.get("/health")
async def health(request: Request) -> JSONResponse:
    runtime = _runtime(request)
    redis_ok = False
    database_ok = False
    try:
        await runtime.redis.ping()
        redis_ok = True
    except Exception:
        redis_ok = False
    try:
        async with runtime.sessionmaker() as session:
            await session.execute(text("SELECT 1"))
        database_ok = True
    except Exception:
        database_ok = False
    status = "ok" if redis_ok and database_ok else "degraded"
    return JSONResponse(
        {"status": status, "redis": redis_ok, "database": database_ok, "version": __version__},
        status_code=200 if status == "ok" else 503,
    )


@router.get("/metrics")
async def metrics(request: Request) -> Response:
    runtime = _runtime(request)
    return Response(runtime.metrics.render(), media_type=CONTENT_TYPE_LATEST)


@router.get("/v1/models")
async def models(request: Request) -> dict[str, Any]:
    runtime = _runtime(request)
    async with runtime.sessionmaker() as session:
        await authenticate(
            session,
            bearer_token(request.headers.get("authorization")),
            runtime.settings.key_pepper,
        )
    created = int(time.time())
    return {
        "object": "list",
        "data": [
            {"id": name, "object": "model", "created": created, "owned_by": "conduit"}
            for name in runtime.config.aliases
        ],
    }


@router.post("/v1/chat/completions")
async def chat_completions(request: Request, body: ChatCompletionRequest) -> Response:
    return await handle_chat(request, body)


@router.post("/admin/keys")
async def create_key(request: Request, body: CreateKeyRequest) -> dict[str, Any]:
    runtime = _runtime(request)
    _require_admin(request, runtime)
    raw = generate_api_key()
    now = int(time.time() * 1000)
    record = APIKey(
        name=body.name,
        key_hash=hash_api_key(raw, runtime.settings.key_pepper),
        key_prefix=key_prefix(raw),
        rpm_limit=body.rpm,
        tpm_limit=body.tpm,
        token_budget=body.token_budget,
        cost_budget_micros=None if body.cost_budget_usd is None else usd_to_micros(body.cost_budget_usd),
        tokens_used=0,
        cost_used_micros=0,
        disabled=False,
        created_at_ms=now,
    )
    async with runtime.sessionmaker() as session:
        session.add(record)
        await session.commit()
        await session.refresh(record)
        key_id = record.id
    return {
        "id": key_id,
        "name": body.name,
        "prefix": key_prefix(raw),
        "api_key": raw,
        "rpm": body.rpm,
        "tpm": body.tpm,
        "token_budget": body.token_budget,
        "cost_budget_usd": None if body.cost_budget_usd is None else micros_to_usd(usd_to_micros(body.cost_budget_usd)),
    }


@router.get("/admin/keys")
async def list_keys(request: Request) -> dict[str, Any]:
    runtime = _runtime(request)
    _require_admin(request, runtime)
    async with runtime.sessionmaker() as session:
        rows = (await session.scalars(select(APIKey).order_by(APIKey.id))).all()
    return {
        "data": [
            {
                "id": row.id,
                "name": row.name,
                "prefix": row.key_prefix,
                "rpm": row.rpm_limit,
                "tpm": row.tpm_limit,
                "token_budget": row.token_budget,
                "cost_budget_usd": None if row.cost_budget_micros is None else micros_to_usd(row.cost_budget_micros),
                "tokens_used": row.tokens_used,
                "cost_used_usd": micros_to_usd(row.cost_used_micros),
                "disabled": row.disabled,
                "created_at": _iso(row.created_at_ms),
            }
            for row in rows
        ]
    }


@router.post("/admin/keys/{key_id}/disable")
async def disable_key(key_id: int, request: Request) -> dict[str, Any]:
    runtime = _runtime(request)
    _require_admin(request, runtime)
    async with runtime.sessionmaker() as session:
        result = await session.execute(
            update(APIKey).where(APIKey.id == key_id).values(disabled=True).returning(APIKey.id)
        )
        if result.first() is None:
            raise ConduitError(
                "API key not found",
                status_code=404,
                error_type="invalid_request",
                code="key_not_found",
            )
        await session.commit()
    return {"id": key_id, "disabled": True}


@router.get("/admin/usage")
async def usage(request: Request, limit: int = Query(default=50, ge=1, le=200)) -> dict[str, Any]:
    runtime = _runtime(request)
    _require_admin(request, runtime)
    async with runtime.sessionmaker() as session:
        rows = (await session.scalars(select(UsageRecord).order_by(UsageRecord.id.desc()).limit(limit))).all()
    return {
        "data": [
            {
                "request_id": row.request_id,
                "api_key_id": row.api_key_id,
                "model": row.model_alias,
                "provider": row.provider,
                "upstream_model": row.upstream_model,
                "prompt_tokens": row.prompt_tokens,
                "completion_tokens": row.completion_tokens,
                "total_tokens": row.total_tokens,
                "cost_usd": micros_to_usd(row.cost_micros),
                "latency_ms": row.latency_ms,
                "ttft_ms": row.ttft_ms,
                "cache": row.cache_status,
                "failover_count": row.failover_count,
                "attempts": row.attempt_log,
                "status": row.status,
                "error_type": row.error_type,
                "stream": row.streamed,
                "created_at": _iso(row.created_at_ms),
            }
            for row in rows
        ]
    }


@router.get("/admin/usage/summary")
async def usage_summary(request: Request) -> dict[str, Any]:
    runtime = _runtime(request)
    _require_admin(request, runtime)
    stmt = (
        select(
            UsageRecord.model_alias,
            UsageRecord.provider,
            func.count(),
            func.coalesce(func.sum(UsageRecord.total_tokens), 0),
            func.coalesce(func.sum(UsageRecord.cost_micros), 0),
            func.coalesce(func.avg(UsageRecord.latency_ms), 0.0),
        )
        .group_by(UsageRecord.model_alias, UsageRecord.provider)
        .order_by(UsageRecord.model_alias, UsageRecord.provider)
    )
    async with runtime.sessionmaker() as session:
        rows = (await session.execute(stmt)).all()
    return {
        "data": [
            {
                "model": model,
                "provider": provider,
                "requests": int(count),
                "tokens": int(tokens),
                "cost_usd": micros_to_usd(int(cost)),
                "avg_latency_ms": float(latency),
            }
            for model, provider, count, tokens, cost, latency in rows
        ]
    }


def _iso(epoch_ms: int) -> str:
    return datetime.fromtimestamp(epoch_ms / 1000, tz=UTC).isoformat()
