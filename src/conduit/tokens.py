"""Approximate token counts.

Provider-reported usage replaces this whenever the upstream response includes it.
The estimator exists so rate limits and budgets can reject a call before it is sent.
"""

from __future__ import annotations

from collections.abc import Sequence
from typing import Any


def estimate_text_tokens(text: str) -> int:
    stripped = text.strip()
    if not stripped:
        return 0
    return max(1, (len(stripped) + 3) // 4)


def content_text(content: Any) -> str:
    if content is None:
        return ""
    if isinstance(content, str):
        return content
    if isinstance(content, list):
        parts: list[str] = []
        for item in content:
            if isinstance(item, str):
                parts.append(item)
            elif isinstance(item, dict) and item.get("type") in (None, "text"):
                text = item.get("text")
                if isinstance(text, str):
                    parts.append(text)
        return "\n".join(parts)
    return str(content)


def message_text(messages: Sequence[Any]) -> str:
    parts: list[str] = []
    for message in messages:
        content = message.get("content") if isinstance(message, dict) else getattr(message, "content", None)
        text = content_text(content)
        if text:
            parts.append(text)
    return "\n".join(parts)


def estimate_message_tokens(messages: Sequence[Any]) -> int:
    tokens = estimate_text_tokens(message_text(messages))
    return tokens if tokens > 0 else 1
