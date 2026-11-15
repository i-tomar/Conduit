"""Deterministic offline provider.

The same prompt always produces the same completion, which makes cache tests
and benchmarks repeatable without API keys.
"""

from __future__ import annotations

import hashlib
from collections.abc import AsyncIterator

from conduit.errors import ProviderError
from conduit.providers.base import Provider, ProviderResult, StreamChunk
from conduit.schemas import ChatCompletionRequest
from conduit.tokens import estimate_message_tokens, estimate_text_tokens, message_text


def mock_completion(
    request: ChatCompletionRequest,
    model: str,
) -> tuple[str, int, int]:
    text = message_text(request.messages)
    digest = hashlib.sha256(text.encode()).hexdigest()[:12]
    content = f"conduit-mock:{digest}"
    prompt_tokens = estimate_message_tokens(request.messages)
    completion_tokens = estimate_text_tokens(content) or 1
    cap = request.completion_cap()
    if cap is not None and cap < completion_tokens:
        content = content[: max(1, cap * 4)]
        completion_tokens = estimate_text_tokens(content) or 1
    return content, prompt_tokens, completion_tokens


class MockProvider(Provider):
    def __init__(self, name: str) -> None:
        super().__init__(name, configured=True)

    async def complete(
        self,
        request: ChatCompletionRequest,
        model: str,
        timeout: float,
        fault: str | None = None,
    ) -> ProviderResult:
        del timeout
        self._tick()
        await self.before_call(fault)
        content, prompt_tokens, completion_tokens = mock_completion(request, model)
        return ProviderResult(
            message={"role": "assistant", "content": content},
            prompt_tokens=prompt_tokens,
            completion_tokens=completion_tokens,
            finish_reason="stop",
            upstream_model=model,
        )

    async def stream(
        self,
        request: ChatCompletionRequest,
        model: str,
        timeout: float,
        fault: str | None = None,
    ) -> AsyncIterator[StreamChunk]:
        del timeout
        self._tick()
        if fault == "malformed_mid":
            content, prompt_tokens, completion_tokens = mock_completion(request, model)
            del prompt_tokens, completion_tokens
            yield StreamChunk(delta={"role": "assistant", "content": content[:1]})
            raise ProviderError(
                "stream broke after the first token",
                status_code=502,
                retryable=True,
                failover=True,
                error_type="malformed_stream",
            )
        await self.before_call(fault)
        content, prompt_tokens, completion_tokens = mock_completion(request, model)
        yield StreamChunk(delta={"role": "assistant"})
        yield StreamChunk(delta={"content": content})
        yield StreamChunk(
            delta={},
            finish_reason="stop",
            prompt_tokens=prompt_tokens,
            completion_tokens=completion_tokens,
        )
