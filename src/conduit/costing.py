"""Integer microdollar costs. A microdollar is one millionth of a USD."""

from __future__ import annotations

from decimal import ROUND_HALF_UP, Decimal

from conduit.config_loader import GatewayConfig


def usd_to_micros(usd: Decimal | float | str) -> int:
    amount = usd if isinstance(usd, Decimal) else Decimal(str(usd))
    return int((amount * Decimal(1_000_000)).quantize(Decimal("1"), rounding=ROUND_HALF_UP))


def micros_to_usd(micros: int) -> str:
    amount = (Decimal(micros) / Decimal(1_000_000)).quantize(Decimal("0.000001"), rounding=ROUND_HALF_UP)
    return format(amount, "f")


def cost_micros(
    config: GatewayConfig,
    provider: str,
    model: str,
    prompt_tokens: int,
    completion_tokens: int,
) -> int:
    """Return the cost of a call.

    ``cost_usd = tokens / 1_000_000 * price_per_million``, so microdollars
    equal ``tokens * price_per_million`` after rounding.
    """

    provider_prices = config.pricing.get(provider, {})
    price = provider_prices.get(model)
    if price is None:
        return 0
    raw = Decimal(prompt_tokens) * price.input_per_million + Decimal(completion_tokens) * price.output_per_million
    return int(raw.quantize(Decimal("1"), rounding=ROUND_HALF_UP))


def max_alias_cost_micros(
    config: GatewayConfig,
    alias: str,
    prompt_tokens: int,
    completion_tokens: int,
) -> int:
    targets = config.aliases[alias].targets
    if not targets:
        return 0
    return max(
        cost_micros(config, target.provider, target.model, prompt_tokens, completion_tokens) for target in targets
    )
