"""OpenAI-compatible request models. Unknown fields are kept and forwarded."""

from __future__ import annotations

import hashlib
import json
from typing import Any

from pydantic import BaseModel, ConfigDict, Field


class ChatMessage(BaseModel):
    model_config = ConfigDict(extra="allow")

    role: str
    content: str | list[dict[str, Any]] | None = None
    name: str | None = None
    tool_calls: list[dict[str, Any]] | None = None
    tool_call_id: str | None = None


class ChatCompletionRequest(BaseModel):
    model_config = ConfigDict(extra="allow")

    model: str
    messages: list[ChatMessage]
    temperature: float | None = None
    top_p: float | None = None
    max_tokens: int | None = None
    max_completion_tokens: int | None = None
    stream: bool = False
    stop: str | list[str] | None = None
    presence_penalty: float | None = None
    frequency_penalty: float | None = None
    user: str | None = None
    n: int | None = 1
    tools: list[dict[str, Any]] | None = None
    tool_choice: Any | None = None
    response_format: dict[str, Any] | None = None
    stream_options: dict[str, Any] | None = None
    seed: int | None = None

    def completion_cap(self) -> int | None:
        if self.max_tokens is not None:
            return self.max_tokens
        return self.max_completion_tokens

    def cacheable(self) -> bool:
        if self.tools:
            return False
        return self.n in (None, 1)

    def fingerprint(self) -> str:
        raw = json.dumps(self.model_dump(mode="json"), sort_keys=True, separators=(",", ":"), default=str)
        return hashlib.sha256(raw.encode()).hexdigest()

    def upstream_body(self, upstream_model: str, *, stream: bool) -> dict[str, Any]:
        data = self.model_dump(mode="json", exclude_none=True)
        data["model"] = upstream_model
        data["stream"] = stream
        if stream:
            options = dict(data.get("stream_options") or {})
            options["include_usage"] = True
            data["stream_options"] = options
        else:
            data.pop("stream_options", None)
        return data


class CreateKeyRequest(BaseModel):
    name: str = Field(min_length=1, max_length=120)
    rpm: int = Field(default=60, ge=1, le=1_000_000)
    tpm: int = Field(default=60_000, ge=1, le=100_000_000)
    token_budget: int | None = Field(default=None, ge=1)
    cost_budget_usd: float | None = Field(default=None, ge=0)
