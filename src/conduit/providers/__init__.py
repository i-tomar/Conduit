"""Build the configured provider set. Missing API keys stay in the table and are skipped."""

from __future__ import annotations

import os

import httpx

from conduit.config_loader import GatewayConfig
from conduit.providers.base import Provider
from conduit.providers.mock import MockProvider
from conduit.providers.openai_compat import OpenAICompatProvider

__all__ = ["Provider", "build_providers"]


def build_providers(config: GatewayConfig) -> dict[str, Provider]:
    providers: dict[str, Provider] = {}
    for name, spec in config.providers.items():
        if spec.type == "mock":
            providers[name] = MockProvider(name)
            continue
        if spec.type != "openai_compat":
            raise ValueError(f"provider '{name}' has unknown type '{spec.type}'")
        if not spec.base_url:
            raise ValueError(f"provider '{name}' is missing base_url")
        api_key = spec.api_key or os.environ.get(spec.api_key_env or "", "")
        client = httpx.AsyncClient(
            timeout=None,
            limits=httpx.Limits(max_connections=100, max_keepalive_connections=20),
        )
        providers[name] = OpenAICompatProvider(
            name=name,
            base_url=spec.base_url,
            api_key=api_key,
            client=client,
            connect_timeout=config.timeouts.connect_seconds,
        )
    return providers
