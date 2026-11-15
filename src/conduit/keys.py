"""Gateway API keys. Only a peppered SHA-256 hash is stored."""

from __future__ import annotations

import hashlib
import secrets
from dataclasses import dataclass

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from conduit.db import APIKey
from conduit.errors import ConduitError


@dataclass(frozen=True, slots=True)
class Principal:
    id: int
    name: str
    rpm: int
    tpm: int
    token_budget: int | None
    cost_budget_micros: int | None
    disabled: bool


def generate_api_key() -> str:
    return "cdt_live_" + secrets.token_urlsafe(32)


def key_prefix(raw: str) -> str:
    return raw[:16]


def hash_api_key(raw: str, pepper: str) -> str:
    material = f"{pepper}:{raw}".encode()
    return hashlib.sha256(material).hexdigest()


def bearer_token(header: str | None) -> str | None:
    if not header:
        return None
    scheme, _, value = header.partition(" ")
    if scheme.lower() != "bearer" or not value.strip():
        return None
    return value.strip()


def admin_authorized(presented: str, expected: str) -> bool:
    if not expected or not presented:
        return False
    return secrets.compare_digest(presented, expected)


async def authenticate(session: AsyncSession, raw_key: str | None, pepper: str) -> Principal:
    if not raw_key:
        raise ConduitError(
            "Missing API key. Send Authorization: Bearer cdt_live_...",
            status_code=401,
            error_type="authentication_error",
            code="missing_api_key",
        )
    row = await session.scalar(select(APIKey).where(APIKey.key_hash == hash_api_key(raw_key, pepper)))
    if row is None or row.disabled:
        raise ConduitError(
            "Invalid API key",
            status_code=401,
            error_type="authentication_error",
            code="invalid_api_key",
        )
    return Principal(
        id=row.id,
        name=row.name,
        rpm=row.rpm_limit,
        tpm=row.tpm_limit,
        token_budget=row.token_budget,
        cost_budget_micros=row.cost_budget_micros,
        disabled=row.disabled,
    )
