"""Exact-match cache and an optional local semantic cache.

The semantic cache embeds prompts with a deterministic feature hash so it runs
offline. It is a recall aid, not a substitute for a hosted embedding model.
"""

from __future__ import annotations

import hashlib
import json
import math
import re
import uuid
from dataclasses import dataclass
from typing import Any

from redis.asyncio import Redis as RedisClient

from conduit.schemas import ChatCompletionRequest

_TOKEN = re.compile(r"[a-z0-9]+")


@dataclass(slots=True)
class CachedCompletion:
    message: dict[str, Any]
    finish_reason: str
    prompt_tokens: int
    completion_tokens: int
    provider: str
    upstream_model: str
    kind: str


def embed(text: str, dims: int) -> list[float]:
    tokens = _TOKEN.findall(text.lower())
    vector = [0.0] * dims
    if not tokens:
        return vector
    features = list(tokens)
    features.extend(f"{left}_{right}" for left, right in zip(tokens, tokens[1:], strict=False))
    for feature in features:
        digest = hashlib.blake2b(feature.encode(), digest_size=8).digest()
        hashed = int.from_bytes(digest, "little")
        index = hashed % dims
        sign = 1.0 if (hashed >> 8) & 1 else -1.0
        vector[index] += sign
    norm = math.sqrt(sum(value * value for value in vector)) or 1.0
    return [value / norm for value in vector]


def cosine(left: list[float], right: list[float]) -> float:
    return float(sum(a * b for a, b in zip(left, right, strict=False)))


def exact_cache_key(scope_id: str, body: ChatCompletionRequest) -> str:
    payload = body.model_dump(mode="json", exclude_none=True)
    payload.pop("stream", None)
    payload.pop("stream_options", None)
    raw = json.dumps({"scope": scope_id, "body": payload}, sort_keys=True, separators=(",", ":"), default=str)
    digest = hashlib.sha256(raw.encode()).hexdigest()
    return f"conduit:exact:{digest}"


def _dump(cached: CachedCompletion) -> str:
    return json.dumps(
        {
            "message": cached.message,
            "finish_reason": cached.finish_reason,
            "prompt_tokens": cached.prompt_tokens,
            "completion_tokens": cached.completion_tokens,
            "provider": cached.provider,
            "upstream_model": cached.upstream_model,
        },
        separators=(",", ":"),
        default=str,
    )


def _as_str(value: str | bytes) -> str:
    if isinstance(value, bytes):
        return value.decode()
    return value


def _load(raw: str, kind: str) -> CachedCompletion:
    data = json.loads(raw)
    return CachedCompletion(
        message=data["message"],
        finish_reason=data["finish_reason"],
        prompt_tokens=int(data["prompt_tokens"]),
        completion_tokens=int(data["completion_tokens"]),
        provider=str(data["provider"]),
        upstream_model=str(data["upstream_model"]),
        kind=kind,
    )


class ExactCache:
    def __init__(self, redis: RedisClient, ttl_seconds: int) -> None:
        self._redis = redis
        self._ttl = ttl_seconds

    async def get(self, key: str) -> CachedCompletion | None:
        raw = await self._redis.get(key)
        if raw is None:
            return None
        return _load(_as_str(raw), "exact")

    async def set(self, key: str, cached: CachedCompletion) -> None:
        await self._redis.set(key, _dump(cached), ex=self._ttl)


class SemanticCache:
    def __init__(
        self,
        redis: RedisClient,
        *,
        ttl_seconds: int,
        threshold: float,
        max_entries: int,
        dims: int,
    ) -> None:
        self._redis = redis
        self._ttl = ttl_seconds
        self._threshold = threshold
        self._max_entries = max_entries
        self._dims = dims

    def _index_key(self, scope_id: str, alias: str) -> str:
        return f"conduit:semidx:{scope_id}:{alias}"

    async def lookup(self, scope_id: str, alias: str, text: str) -> tuple[CachedCompletion, float] | None:
        if not text.strip():
            return None
        query = embed(text, self._dims)
        if not any(query):
            return None
        ids = await self._redis.lrange(self._index_key(scope_id, alias), 0, self._max_entries - 1)
        best: CachedCompletion | None = None
        best_score = -1.0
        for entry_id in ids:
            entry = _as_str(entry_id)
            raw = await self._redis.get(f"conduit:sement:{entry}")
            if raw is None:
                await self._redis.lrem(self._index_key(scope_id, alias), 0, entry)
                continue
            data = json.loads(_as_str(raw))
            score = cosine(query, list(data["embedding"]))
            if score > best_score:
                best_score = score
                best = _load(json.dumps(data["payload"]), "semantic")
        if best is None or best_score < self._threshold:
            return None
        return best, best_score

    async def store(self, scope_id: str, alias: str, text: str, cached: CachedCompletion) -> None:
        if not text.strip():
            return
        entry_id = uuid.uuid4().hex
        body = json.dumps(
            {"embedding": embed(text, self._dims), "payload": json.loads(_dump(cached))},
            separators=(",", ":"),
        )
        index = self._index_key(scope_id, alias)
        await self._redis.set(f"conduit:sement:{entry_id}", body, ex=self._ttl)
        await self._redis.lpush(index, entry_id)
        await self._redis.ltrim(index, 0, self._max_entries - 1)
        await self._redis.expire(index, self._ttl)
