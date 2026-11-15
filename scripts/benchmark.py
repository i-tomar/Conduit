"""Measure the mock-provider path through a real uvicorn loopback server.

Numbers printed by this script are the only benchmark figures that belong in
the README. They are single-process, in-memory Redis, SQLite, and the mock
provider. They are not provider-network latency.
"""

from __future__ import annotations

import asyncio
import json
import os
import platform
import statistics
import tempfile
import time
from decimal import Decimal
from pathlib import Path

import httpx
import uvicorn

from conduit.app import create_app
from conduit.config_loader import (
    Alias,
    CacheConfig,
    CircuitConfig,
    GatewayConfig,
    Price,
    ProviderConfig,
    RetryConfig,
    Target,
    TimeoutConfig,
)
from conduit.providers.mock import mock_completion
from conduit.schemas import ChatCompletionRequest
from conduit.settings import Settings


def percentile(samples: list[float], pct: float) -> float:
    if not samples:
        return 0.0
    ordered = sorted(samples)
    index = min(len(ordered) - 1, max(0, round((pct / 100) * (len(ordered) - 1))))
    return ordered[index]


def summarize(samples: list[float]) -> dict[str, float]:
    return {
        "n": float(len(samples)),
        "p50_ms": percentile(samples, 50),
        "p95_ms": percentile(samples, 95),
        "p99_ms": percentile(samples, 99),
        "mean_ms": statistics.fmean(samples) if samples else 0.0,
    }


async def _timed_posts(
    client: httpx.AsyncClient,
    *,
    count: int,
    concurrency: int,
    payload: dict[str, object] | None,
    vary: bool,
    headers: dict[str, str],
) -> tuple[list[float], float, int]:
    semaphore = asyncio.Semaphore(concurrency)
    samples: list[float] = []
    errors = 0

    async def one(index: int) -> None:
        nonlocal errors
        body = dict(payload or {})
        if vary:
            body = {
                "model": "mock",
                "messages": [{"role": "user", "content": f"bench miss {index}"}],
                "max_tokens": 16,
            }
        async with semaphore:
            started = time.perf_counter()
            response = await client.post("/v1/chat/completions", headers=headers, json=body)
            elapsed = (time.perf_counter() - started) * 1000
        if response.status_code != 200:
            errors += 1
            return
        samples.append(elapsed)

    started = time.perf_counter()
    await asyncio.gather(*(one(index) for index in range(count)))
    elapsed = time.perf_counter() - started
    return samples, elapsed, errors


async def _stream_ttft(client: httpx.AsyncClient, headers: dict[str, str], count: int) -> list[float]:
    samples: list[float] = []
    payload = {"model": "mock", "messages": [{"role": "user", "content": "stream bench"}], "stream": True}
    for _ in range(count):
        started = time.perf_counter()
        async with client.stream("POST", "/v1/chat/completions", headers=headers, json=payload) as response:
            response.raise_for_status()
            async for line in response.aiter_lines():
                if line.startswith("data:"):
                    samples.append((time.perf_counter() - started) * 1000)
                    break
    return samples


def _direct(count: int) -> list[float]:
    request = ChatCompletionRequest(model="mock", messages=[{"role": "user", "content": "bench"}], max_tokens=16)
    samples: list[float] = []
    for _ in range(count):
        started = time.perf_counter()
        mock_completion(request, "echo")
        samples.append((time.perf_counter() - started) * 1000)
    return samples


def _config() -> GatewayConfig:
    return GatewayConfig(
        providers={"mock": ProviderConfig(type="mock")},
        aliases={"mock": Alias(targets=[Target(provider="mock", model="echo")])},
        pricing={"mock": {"echo": Price(input_per_million=Decimal("0"), output_per_million=Decimal("0"))}},
        retry=RetryConfig(max_attempts=1, base_delay_seconds=0, max_delay_seconds=0),
        circuit_breaker=CircuitConfig(failure_threshold=100, recovery_seconds=30),
        cache=CacheConfig(exact_ttl_seconds=600, semantic_enabled=False, scope="tenant"),
        timeouts=TimeoutConfig(request_seconds=5, connect_seconds=1),
    )


async def run() -> dict[str, object]:
    serial = 120
    loaded = 240
    concurrency = 20
    stream_count = 40
    tmp = tempfile.mkdtemp(prefix="conduit-bench-")
    settings = Settings(
        _env_file=None,
        database_url=f"sqlite+aiosqlite:///{tmp}/bench.db",
        redis_url="memory://",
        dev_api_key="cdt_live_bench_key",
        dev_rpm=2_000_000,
        dev_tpm=50_000_000,
        admin_token="bench-admin",
        allow_fault_injection=False,
        otel_enabled=True,
        otlp_endpoint="",
        key_pepper="bench-pepper",
        log_level="WARNING",
    )
    application = create_app(settings, gateway_config=_config())
    port = int(os.environ.get("CONDUIT_BENCH_PORT", "8766"))
    server = uvicorn.Server(
        uvicorn.Config(application, host="127.0.0.1", port=port, log_level="warning", access_log=False)
    )
    task = asyncio.create_task(server.serve())
    headers = {"Authorization": "Bearer cdt_live_bench_key"}
    miss_headers = {**headers, "X-Conduit-Cache": "bypass"}
    hit_body = {"model": "mock", "messages": [{"role": "user", "content": "bench hit"}], "max_tokens": 16}
    try:
        async with httpx.AsyncClient(base_url=f"http://127.0.0.1:{port}", timeout=30) as client:
            for _ in range(50):
                try:
                    health = await client.get("/health")
                    if health.status_code == 200:
                        break
                except httpx.HTTPError:
                    await asyncio.sleep(0.05)
            else:
                raise RuntimeError("benchmark server did not become healthy")
            await client.post("/v1/chat/completions", headers=headers, json=hit_body)
            direct = _direct(serial)
            health_samples: list[float] = []
            health_errors = 0
            for _ in range(serial):
                started = time.perf_counter()
                response = await client.get("/health")
                if response.status_code != 200:
                    health_errors += 1
                    continue
                health_samples.append((time.perf_counter() - started) * 1000)
            miss_serial, _, miss_serial_errors = await _timed_posts(
                client, count=serial, concurrency=1, payload=hit_body, vary=True, headers=miss_headers
            )
            hit_serial, _, hit_serial_errors = await _timed_posts(
                client, count=serial, concurrency=1, payload=hit_body, vary=False, headers=headers
            )
            miss_loaded, miss_elapsed, miss_loaded_errors = await _timed_posts(
                client, count=loaded, concurrency=concurrency, payload=hit_body, vary=True, headers=miss_headers
            )
            hit_loaded, hit_elapsed, hit_loaded_errors = await _timed_posts(
                client, count=loaded, concurrency=concurrency, payload=hit_body, vary=False, headers=headers
            )
            ttft = await _stream_ttft(client, miss_headers, stream_count)
    finally:
        server.should_exit = True
        await task

    miss_p95 = summarize(miss_serial)["p95_ms"]
    hit_p95 = summarize(hit_serial)["p95_ms"]
    direct_p95 = summarize(direct)["p95_ms"]
    report: dict[str, object] = {
        "label": "mock-provider, single process, loopback HTTP, in-memory Redis, SQLite",
        "python": platform.python_version(),
        "platform": platform.platform(),
        "cpu_count": os.cpu_count(),
        "serial_requests": serial,
        "loaded_requests": loaded,
        "loaded_concurrency": concurrency,
        "direct_mock": summarize(direct),
        "health_serial": summarize(health_samples),
        "gateway_miss_serial": summarize(miss_serial),
        "gateway_hit_serial": summarize(hit_serial),
        "gateway_miss_loaded": summarize(miss_loaded),
        "gateway_hit_loaded": summarize(hit_loaded),
        "stream_ttft_serial": summarize(ttft),
        "gateway_overhead_p95_ms": miss_p95 - direct_p95,
        "cache_hit_p95_savings_ms": miss_p95 - hit_p95,
        "cache_hit_p95_savings_pct": (100 * (miss_p95 - hit_p95) / miss_p95) if miss_p95 else 0,
        "miss_throughput_rps": loaded / miss_elapsed if miss_elapsed else 0,
        "hit_throughput_rps": loaded / hit_elapsed if hit_elapsed else 0,
        "errors": {
            "health": health_errors,
            "miss_serial": miss_serial_errors,
            "hit_serial": hit_serial_errors,
            "miss_loaded": miss_loaded_errors,
            "hit_loaded": hit_loaded_errors,
        },
    }
    return report


if __name__ == "__main__":
    report = asyncio.run(run())
    destination = Path("benchmarks/latest.json")
    destination.parent.mkdir(parents=True, exist_ok=True)
    destination.write_text(json.dumps(report, indent=2) + "\n", encoding="utf-8")
    print(json.dumps(report, indent=2))
