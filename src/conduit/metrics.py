"""Prometheus collectors. One registry per process so tests do not collide."""

from __future__ import annotations

from collections.abc import Sequence
from typing import Any

from prometheus_client import CollectorRegistry, Counter, Gauge, Histogram, ProcessCollector, generate_latest

_LATENCY_BUCKETS = (0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2, 5, 10, 30, 60)
_CIRCUIT = {"closed": 0, "half_open": 1, "open": 2}


class Metrics:
    def __init__(self, registry: CollectorRegistry | None = None) -> None:
        self.registry = registry or CollectorRegistry()
        ProcessCollector(registry=self.registry)
        self.http_requests = Counter(
            "conduit_http_requests_total",
            "HTTP requests handled by the gateway",
            ["method", "path", "status"],
            registry=self.registry,
        )
        self.http_latency = Histogram(
            "conduit_http_latency_seconds",
            "HTTP request latency",
            ["path"],
            buckets=_LATENCY_BUCKETS,
            registry=self.registry,
        )
        self.llm_requests = Counter(
            "conduit_llm_requests_total",
            "Chat completion outcomes",
            ["provider", "model", "status", "cache"],
            registry=self.registry,
        )
        self.latency = Histogram(
            "conduit_llm_latency_seconds",
            "End-to-end chat completion latency",
            ["provider", "model", "cache"],
            buckets=_LATENCY_BUCKETS,
            registry=self.registry,
        )
        self.ttft = Histogram(
            "conduit_llm_time_to_first_token_seconds",
            "Time to first token. Non-streaming calls use full upstream latency.",
            ["provider", "model"],
            buckets=_LATENCY_BUCKETS,
            registry=self.registry,
        )
        self.tokens = Counter(
            "conduit_llm_tokens_total",
            "Tokens reported for completed calls",
            ["provider", "model", "direction"],
            registry=self.registry,
        )
        self.cost = Counter(
            "conduit_llm_cost_usd_total",
            "Estimated USD cost of upstream calls",
            ["provider", "model"],
            registry=self.registry,
        )
        self.cache_lookups = Counter(
            "conduit_cache_lookups_total",
            "Cache lookups by result",
            ["result"],
            registry=self.registry,
        )
        self.provider_errors = Counter(
            "conduit_provider_errors_total",
            "Failed provider attempts",
            ["provider", "error_type"],
            registry=self.registry,
        )
        self.failovers = Counter(
            "conduit_failover_total",
            "Times the router moved from one provider to another",
            ["from_provider", "to_provider"],
            registry=self.registry,
        )
        self.circuit = Gauge(
            "conduit_circuit_breaker_state",
            "Circuit state: 0 closed, 1 half-open, 2 open",
            ["provider"],
            registry=self.registry,
        )
        self.rate_limited = Counter(
            "conduit_rate_limited_total",
            "Requests rejected by a per-key rate limit",
            ["kind"],
            registry=self.registry,
        )
        self.budget_rejected = Counter(
            "conduit_budget_rejected_total",
            "Requests rejected by a hard token or cost budget",
            registry=self.registry,
        )

    def render(self) -> bytes:
        return generate_latest(self.registry)

    def set_circuit(self, provider: str, state: str) -> None:
        self.circuit.labels(provider).set(_CIRCUIT.get(state, 0))

    def observe_attempts(self, attempts: Sequence[Any]) -> None:
        visited: list[str] = []
        for attempt in attempts:
            provider = str(getattr(attempt, "provider", ""))
            status = str(getattr(attempt, "status", ""))
            error_type = getattr(attempt, "error_type", None)
            if status not in {"success", "circuit_open", "unconfigured"}:
                self.provider_errors.labels(provider, str(error_type or status)).inc()
            if status in {"circuit_open", "unconfigured"}:
                continue
            if not visited or visited[-1] != provider:
                visited.append(provider)
        for source, dest in zip(visited, visited[1:], strict=False):
            self.failovers.labels(source, dest).inc()
