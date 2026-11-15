"""OpenTelemetry setup. Export is a no-op until CONDUIT_OTLP_ENDPOINT is set."""

from __future__ import annotations

from fastapi import FastAPI
from opentelemetry import trace
from opentelemetry.exporter.otlp.proto.http.trace_exporter import OTLPSpanExporter
from opentelemetry.instrumentation.fastapi import FastAPIInstrumentor
from opentelemetry.instrumentation.httpx import HTTPXClientInstrumentor
from opentelemetry.sdk.resources import Resource
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor

from conduit.settings import Settings

_installed = False


def setup_tracing(app: FastAPI, settings: Settings) -> None:
    global _installed
    if not settings.otel_enabled or _installed:
        return
    provider = TracerProvider(resource=Resource.create({"service.name": "conduit"}))
    if settings.otlp_endpoint:
        exporter = OTLPSpanExporter(endpoint=settings.otlp_endpoint)
        provider.add_span_processor(BatchSpanProcessor(exporter))
    trace.set_tracer_provider(provider)
    FastAPIInstrumentor.instrument_app(app, tracer_provider=provider)
    HTTPXClientInstrumentor().instrument(tracer_provider=provider)
    _installed = True


def tracer() -> trace.Tracer:
    return trace.get_tracer("conduit")
