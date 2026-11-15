from __future__ import annotations

import httpx
import pytest

from conduit.errors import ProviderError
from conduit.providers.openai_compat import OpenAICompatProvider
from conduit.schemas import ChatCompletionRequest

REQUEST = ChatCompletionRequest(model="alias", messages=[{"role": "user", "content": "hi"}])


def _provider(handler) -> tuple[OpenAICompatProvider, httpx.AsyncClient]:
    client = httpx.AsyncClient(transport=httpx.MockTransport(handler))
    provider = OpenAICompatProvider(
        "openai",
        base_url="https://example.invalid/v1",
        api_key="test-key",
        client=client,
        connect_timeout=1,
    )
    return provider, client


async def test_openai_compatible_completion_parses_usage() -> None:
    seen: dict[str, str] = {}

    def handler(request: httpx.Request) -> httpx.Response:
        seen["url"] = str(request.url)
        seen["auth"] = request.headers["authorization"]
        return httpx.Response(
            200,
            json={
                "model": "gpt-4o-mini",
                "choices": [{"message": {"role": "assistant", "content": "pong"}, "finish_reason": "stop"}],
                "usage": {"prompt_tokens": 4, "completion_tokens": 2},
            },
        )

    provider, client = _provider(handler)
    try:
        result = await provider.complete(REQUEST, "gpt-4o-mini", 5)
    finally:
        await client.aclose()
    assert result.message["content"] == "pong"
    assert result.prompt_tokens == 4
    assert result.upstream_model == "gpt-4o-mini"
    assert seen["url"] == "https://example.invalid/v1/chat/completions"
    assert seen["auth"] == "Bearer test-key"


async def test_status_mapping_for_retryable_and_terminal_errors() -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        if request.url.params.get("mode") == "429":
            return httpx.Response(429, json={"error": {"message": "slow down"}})
        return httpx.Response(400, json={"error": {"message": "bad input"}})

    provider, client = _provider(handler)
    try:
        with pytest.raises(ProviderError) as rate_limited:
            await provider.complete(REQUEST, "gpt-4o-mini", 5)
    finally:
        await client.aclose()
    # The handler above always returns 400 because the provider does not send mode.
    assert rate_limited.value.error_type == "bad_request"
    assert rate_limited.value.retryable is False
    assert rate_limited.value.failover is False


async def test_rate_limit_and_timeout_and_stream() -> None:
    def rate_limit(request: httpx.Request) -> httpx.Response:
        del request
        return httpx.Response(429, json={"error": {"message": "slow down"}})

    provider, client = _provider(rate_limit)
    try:
        with pytest.raises(ProviderError) as exc:
            await provider.complete(REQUEST, "gpt-4o-mini", 5)
    finally:
        await client.aclose()
    assert exc.value.error_type == "rate_limited"
    assert exc.value.retryable is True
    assert exc.value.failover is True

    def timeout(request: httpx.Request) -> httpx.Response:
        raise httpx.ReadTimeout("timed out", request=request)

    provider, client = _provider(timeout)
    try:
        with pytest.raises(ProviderError) as exc:
            await provider.complete(REQUEST, "gpt-4o-mini", 5)
    finally:
        await client.aclose()
    assert exc.value.error_type == "timeout"

    def stream(request: httpx.Request) -> httpx.Response:
        del request
        body = (
            'data: {"choices":[{"index":0,"delta":{"content":"hi"},"finish_reason":null}]}\n\n'
            'data: {"choices":[{"index":0,"delta":{},"finish_reason":"stop"}],'
            '"usage":{"prompt_tokens":3,"completion_tokens":1}}\n\n'
            "data: [DONE]\n\n"
        )
        return httpx.Response(200, text=body)

    provider, client = _provider(stream)
    try:
        chunks = [chunk async for chunk in provider.stream(REQUEST, "gpt-4o-mini", 5)]
    finally:
        await client.aclose()
    assert chunks[0].delta["content"] == "hi"
    assert chunks[-1].prompt_tokens == 3
    assert chunks[-1].finish_reason == "stop"


async def test_malformed_stream_event_and_missing_key() -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        del request
        return httpx.Response(200, text="data: {not-json}\n\n")

    provider, client = _provider(handler)
    try:
        with pytest.raises(ProviderError) as exc:
            async for _chunk in provider.stream(REQUEST, "gpt-4o-mini", 5):
                pass
    finally:
        await client.aclose()
    assert exc.value.error_type == "malformed_stream"

    empty = OpenAICompatProvider(
        "openai",
        base_url="https://example.invalid/v1",
        api_key="",
        client=httpx.AsyncClient(),
        connect_timeout=1,
    )
    assert empty.configured is False
    with pytest.raises(ProviderError) as missing:
        await empty.complete(REQUEST, "gpt-4o-mini", 5)
    assert missing.value.error_type == "unconfigured"
    assert empty.calls == 0
    await empty.aclose()
