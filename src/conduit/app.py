"""FastAPI application factory."""

from __future__ import annotations

import logging
import time
import uuid
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager

from fastapi import FastAPI, Request
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse, Response

from conduit import __version__
from conduit.api.routes import router
from conduit.config_loader import GatewayConfig, load_gateway_config, validate_gateway
from conduit.errors import ConduitError
from conduit.logging_config import setup_logging
from conduit.runtime import Runtime, build_runtime
from conduit.settings import Settings
from conduit.tracing import setup_tracing

logger = logging.getLogger("conduit")


def create_app(settings: Settings | None = None, gateway_config: GatewayConfig | None = None) -> FastAPI:
    settings = settings or Settings()
    setup_logging(settings.log_level)
    config = gateway_config or load_gateway_config(settings.config_path)
    validate_gateway(config)

    @asynccontextmanager
    async def lifespan(app: FastAPI) -> AsyncIterator[None]:
        runtime = await build_runtime(settings, config)
        app.state.runtime = runtime
        try:
            yield
        finally:
            await runtime.aclose()

    app = FastAPI(
        title="Conduit",
        version=__version__,
        summary="OpenAI-compatible LLM gateway",
        lifespan=lifespan,
    )
    app.state.settings = settings
    app.state.config = config

    @app.middleware("http")
    async def request_context(request: Request, call_next):  # type: ignore[no-untyped-def]
        request_id = request.headers.get("x-request-id") or uuid.uuid4().hex
        request.state.request_id = request_id
        started = time.perf_counter()
        try:
            response: Response = await call_next(request)
        except Exception:
            logger.exception("unhandled_request", extra={"conduit_fields": {"request_id": request_id}})
            raise
        response.headers["x-request-id"] = request_id
        runtime: Runtime | None = getattr(request.app.state, "runtime", None)
        if runtime is not None:
            path = _route_label(request.url.path)
            runtime.metrics.http_requests.labels(request.method, path, str(response.status_code)).inc()
            runtime.metrics.http_latency.labels(path).observe(time.perf_counter() - started)
        return response

    @app.exception_handler(ConduitError)
    async def conduit_error(_request: Request, exc: ConduitError) -> JSONResponse:
        error: dict[str, object] = {
            "message": exc.message,
            "type": exc.error_type,
            "code": exc.code,
        }
        error.update(exc.extra)
        return JSONResponse({"error": error}, status_code=exc.status_code, headers=exc.headers)

    @app.exception_handler(RequestValidationError)
    async def validation_error(_request: Request, exc: RequestValidationError) -> JSONResponse:
        return JSONResponse(
            {
                "error": {
                    "message": "Invalid request body",
                    "type": "invalid_request",
                    "code": "invalid_request",
                    "details": exc.errors(),
                }
            },
            status_code=400,
        )

    app.include_router(router)
    setup_tracing(app, settings)
    return app


def _route_label(path: str) -> str:
    if path.startswith("/admin/keys/") and path.endswith("/disable"):
        return "/admin/keys/{id}/disable"
    return path
