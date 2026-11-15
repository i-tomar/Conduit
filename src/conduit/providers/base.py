"""Provider results and the fault-injection hook used by drills and tests."""

from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator
from dataclasses import dataclass
from typing import Any

from conduit.errors import fault_to_error
from conduit.schemas import ChatCompletionRequest


@dataclass(slots=True)
class ProviderResult:
    message: dict[str, Any]
    prompt_tokens: int
    completion_tokens: int
    finish_reason: str
    upstream_model: str


@dataclass(slots=True)
class StreamChunk:
    delta: dict[str, Any]
    finish_reason: str | None = None
    prompt_tokens: int | None = None
    completion_tokens: int | None = None


class Provider:
    def __init__(self, name: str, configured: bool) -> None:
        self.name = name
        self.configured = configured
        self.calls = 0

    def _tick(self) -> None:
        self.calls += 1

    async def before_call(self, fault: str | None) -> None:
        if fault == "slow":
            await asyncio.sleep(0.02)
            return
        if fault is None:
            return
        error = fault_to_error(fault)
        if error is not None:
            raise error

    async def complete(
        self,
        request: ChatCompletionRequest,
        model: str,
        timeout: float,
        fault: str | None = None,
    ) -> ProviderResult:
        raise NotImplementedError

    async def stream(
        self,
        request: ChatCompletionRequest,
        model: str,
        timeout: float,
        fault: str | None = None,
    ) -> AsyncIterator[StreamChunk]:
        del request, model, timeout, fault
        raise NotImplementedError
        yield StreamChunk(delta={})

    async def aclose(self) -> None:
        return None
