"""Hard token and cost budgets.

A request reserves its projected spend with a single conditional UPDATE, calls
the provider, then reconciles to the measured usage. Concurrent requests cannot
both pass a budget that only one of them fits.
"""

from __future__ import annotations

from sqlalchemy import case, update
from sqlalchemy.ext.asyncio import AsyncSession

from conduit.db import APIKey


async def try_charge(session: AsyncSession, key_id: int, tokens: int, cost_micros: int) -> bool:
    if tokens < 0 or cost_micros < 0:
        raise ValueError("try_charge amounts must be non-negative")
    stmt = (
        update(APIKey)
        .where(APIKey.id == key_id)
        .where(APIKey.disabled.is_(False))
        .where((APIKey.token_budget.is_(None)) | (APIKey.tokens_used + tokens <= APIKey.token_budget))
        .where(
            (APIKey.cost_budget_micros.is_(None)) | (APIKey.cost_used_micros + cost_micros <= APIKey.cost_budget_micros)
        )
        .values(
            tokens_used=APIKey.tokens_used + tokens,
            cost_used_micros=APIKey.cost_used_micros + cost_micros,
        )
        .returning(APIKey.id)
    )
    result = await session.execute(stmt)
    return result.first() is not None


async def credit(session: AsyncSession, key_id: int, tokens: int, cost_micros: int) -> None:
    if tokens < 0 or cost_micros < 0:
        raise ValueError("credit amounts must be non-negative")
    if tokens == 0 and cost_micros == 0:
        return
    values: dict[str, object] = {}
    if tokens:
        values["tokens_used"] = case(
            (APIKey.tokens_used <= tokens, 0),
            else_=APIKey.tokens_used - tokens,
        )
    if cost_micros:
        values["cost_used_micros"] = case(
            (APIKey.cost_used_micros <= cost_micros, 0),
            else_=APIKey.cost_used_micros - cost_micros,
        )
    await session.execute(update(APIKey).where(APIKey.id == key_id).values(**values))


async def reconcile(
    session: AsyncSession,
    key_id: int,
    *,
    reserved_tokens: int,
    reserved_cost: int,
    actual_tokens: int,
    actual_cost: int,
) -> None:
    """Replace a reservation with the measured spend.

    If the provider returns more than the reservation, the extra is recorded
    even when it crosses the cap. That keeps the ledger honest; the next call
    is rejected.
    """

    token_delta = actual_tokens - reserved_tokens
    cost_delta = actual_cost - reserved_cost
    if token_delta < 0 or cost_delta < 0:
        await credit(session, key_id, max(0, -token_delta), max(0, -cost_delta))
        await session.flush()
    extra_tokens = max(0, token_delta)
    extra_cost = max(0, cost_delta)
    if extra_tokens == 0 and extra_cost == 0:
        return
    applied = await try_charge(session, key_id, extra_tokens, extra_cost)
    if applied:
        return
    await session.execute(
        update(APIKey)
        .where(APIKey.id == key_id)
        .values(
            tokens_used=APIKey.tokens_used + extra_tokens,
            cost_used_micros=APIKey.cost_used_micros + extra_cost,
        )
    )
