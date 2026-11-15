"""One adapter for OpenAI, Groq, and Gemini's OpenAI-compatible endpoint."""

from __future__ import annotations

import json
from collections.abc import AsyncIterator
from typing import Any

import httpx

from conduit.errors import ProviderError
from conduit.providers.base import Provider, ProviderResult, StreamChunk
from conduit.schemas import ChatCompletionRequest
from conduit.tokens import estimate_message_tokens, estimate_text_tokens


class OpenAICompatProvider(Provider):
    def __init__(
        self,
        name: str,
        *,
        base_url: str,
        api_key: str,
        client: httpx.AsyncClient,
        connect_timeout: float,
    ) -> None:
        super().__init__(name, configured=bool(api_key))
        self._base_url = base_url.rstrip("/")
        self._api_key = api_key
        self._client = client
        self._connect_timeout = connect_timeout

    async def aclose(self) -> None:
        await self._client.aclose()

    def _headers(self) -> dict[str, str]:
        return {
            "Authorization": f"Bearer {self._api_key}",
            "Content-Type": "application/json",
        }

    def _timeout(self, read_seconds: float) -> httpx.Timeout:
        return httpx.Timeout(connect=self._connect_timeout, read=read_seconds, write=10.0, pool=5.0)

    def _unconfigured(self) -> ProviderError:
        return ProviderError(
            f"provider '{self.name}' has no API key configured",
            status_code=503,
            retryable=False,
            failover=True,
            error_type="unconfigured",
        )

    async def complete(
        self,
        request: ChatCompletionRequest,
        model: str,
        timeout: float,
        fault: str | None = None,
    ) -> ProviderResult:
        if not self.configured:
            raise self._unconfigured()
        self._tick()
        await self.before_call(fault)
        try:
            response = await self._client.post(
                f"{self._base_url}/chat/completions",
                headers=self._headers(),
                json=request.upstream_body(model, stream=False),
                timeout=self._timeout(timeout),
            )
        except httpx.TimeoutException as exc:
            raise self._timeout_error() from exc
        except httpx.HTTPError as exc:
            raise self._connection_error() from exc
        if response.status_code >= 400:
            self._raise_status(response.status_code, response.content)
        try:
            payload = response.json()
        except json.JSONDecodeError as exc:
            raise ProviderError(
                "upstream returned a non-JSON body",
                status_code=502,
                retryable=True,
                failover=True,
                error_type="malformed_response",
            ) from exc
        return self._parse_completion(payload, request, model)

    async def stream(
        self,
        request: ChatCompletionRequest,
        model: str,
        timeout: float,
        fault: str | None = None,
    ) -> AsyncIterator[StreamChunk]:
        if not self.configured:
            raise self._unconfigured()
        self._tick()
        if fault == "malformed_mid":
            yield StreamChunk(delta={"role": "assistant", "content": "x"})
            raise ProviderError(
                "stream broke after the first token",
                status_code=502,
                retryable=True,
                failover=True,
                error_type="malformed_stream",
            )
        await self.before_call(fault)
        try:
            async with self._client.stream(
                "POST",
                f"{self._base_url}/chat/completions",
                headers=self._headers(),
                json=request.upstream_body(model, stream=True),
                timeout=self._timeout(timeout),
            ) as response:
                if response.status_code >= 400:
                    body = await response.aread()
                    self._raise_status(response.status_code, body)
                async for line in response.aiter_lines():
                    chunk = self._parse_sse_line(line)
                    if chunk is not None:
                        yield chunk
        except ProviderError:
            raise
        except httpx.TimeoutException as exc:
            raise self._timeout_error() from exc
        except httpx.HTTPError as exc:
            raise self._connection_error() from exc

    def _parse_completion(
        self,
        payload: dict[str, Any],
        request: ChatCompletionRequest,
        model: str,
    ) -> ProviderResult:
        choices = payload.get("choices") or []
        if not choices:
            raise ProviderError(
                "upstream returned no choices",
                status_code=502,
                retryable=True,
                failover=True,
                error_type="malformed_response",
            )
        choice = choices[0]
        message = choice.get("message") or {}
        content = message.get("content")
        if content is None:
            content = ""
        elif not isinstance(content, str):
            content = json.dumps(content)
        assistant: dict[str, Any] = {"role": message.get("role") or "assistant", "content": content}
        if message.get("tool_calls"):
            assistant["tool_calls"] = message["tool_calls"]
        usage = payload.get("usage") or {}
        prompt_tokens = int(usage.get("prompt_tokens") or estimate_message_tokens(request.messages))
        completion_tokens = int(usage.get("completion_tokens") or estimate_text_tokens(content) or 1)
        return ProviderResult(
            message=assistant,
            prompt_tokens=prompt_tokens,
            completion_tokens=completion_tokens,
            finish_reason=str(choice.get("finish_reason") or "stop"),
            upstream_model=str(payload.get("model") or model),
        )

    def _parse_sse_line(self, line: str) -> StreamChunk | None:
        if not line or line.startswith(":"):
            return None
        if not line.startswith("data:"):
            return None
        data = line[5:].strip()
        if not data or data == "[DONE]":
            return None
        try:
            payload = json.loads(data)
        except json.JSONDecodeError as exc:
            raise ProviderError(
                "malformed upstream stream event",
                status_code=502,
                retryable=True,
                failover=True,
                error_type="malformed_stream",
            ) from exc
        usage = payload.get("usage") or {}
        choices = payload.get("choices") or []
        prompt_tokens = usage.get("prompt_tokens")
        completion_tokens = usage.get("completion_tokens")
        if not choices:
            if prompt_tokens is None and completion_tokens is None:
                return None
            return StreamChunk(
                delta={},
                prompt_tokens=int(prompt_tokens or 0),
                completion_tokens=int(completion_tokens or 0),
            )
        choice = choices[0]
        return StreamChunk(
            delta=dict(choice.get("delta") or {}),
            finish_reason=choice.get("finish_reason"),
            prompt_tokens=int(prompt_tokens) if prompt_tokens is not None else None,
            completion_tokens=int(completion_tokens) if completion_tokens is not None else None,
        )

    def _raise_status(self, status: int, body: bytes) -> None:
        message = body.decode("utf-8", errors="replace")[:500]
        try:
            parsed = json.loads(body)
        except json.JSONDecodeError:
            parsed = None
        if isinstance(parsed, dict) and isinstance(parsed.get("error"), dict):
            upstream = parsed["error"].get("message")
            if upstream:
                message = str(upstream)[:500]
        retryable = status in {408, 409, 425, 429} or status >= 500
        failover = retryable or status in {401, 403, 404}
        if status == 429:
            error_type = "rate_limited"
        elif status in {408, 504}:
            error_type = "timeout"
        elif status >= 500:
            error_type = "server_error"
        elif status in {401, 403}:
            error_type = "upstream_auth"
        elif status == 404:
            error_type = "model_not_found"
        else:
            error_type = "bad_request"
        raise ProviderError(
            message or f"upstream status {status}",
            status_code=status,
            retryable=retryable,
            failover=failover,
            error_type=error_type,
        )

    def _timeout_error(self) -> ProviderError:
        return ProviderError(
            "upstream timed out",
            status_code=504,
            retryable=True,
            failover=True,
            error_type="timeout",
        )

    def _connection_error(self) -> ProviderError:
        return ProviderError(
            "upstream connection failed",
            status_code=502,
            retryable=True,
            failover=True,
            error_type="connection_error",
        )
