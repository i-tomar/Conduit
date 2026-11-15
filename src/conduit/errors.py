"""Errors that map onto the OpenAI HTTP error shape."""

from __future__ import annotations

from typing import Any


class ConduitError(Exception):
    """A failure the gateway owns and can explain to the client."""

    def __init__(
        self,
        message: str,
        *,
        status_code: int,
        error_type: str,
        code: str,
        headers: dict[str, str] | None = None,
        extra: dict[str, Any] | None = None,
    ) -> None:
        super().__init__(message)
        self.message = message
        self.status_code = status_code
        self.error_type = error_type
        self.code = code
        self.headers = headers or {}
        self.extra = extra or {}


class ProviderError(Exception):
    """A failure from one upstream target. The router decides whether to continue."""

    def __init__(
        self,
        message: str,
        *,
        status_code: int,
        retryable: bool,
        failover: bool,
        error_type: str,
    ) -> None:
        super().__init__(message)
        self.message = message
        self.status_code = status_code
        self.retryable = retryable
        self.failover = failover
        self.error_type = error_type


def fault_to_error(fault: str) -> ProviderError | None:
    """Turn a drill fault name into the provider error it simulates.

    ``slow`` and ``malformed_mid`` are handled by the provider because they
    change timing or happen after the first streamed token.
    """

    table: dict[str, ProviderError] = {
        "timeout": ProviderError(
            "upstream timed out",
            status_code=504,
            retryable=True,
            failover=True,
            error_type="timeout",
        ),
        "429": ProviderError(
            "upstream rate limited",
            status_code=429,
            retryable=True,
            failover=True,
            error_type="rate_limited",
        ),
        "500": ProviderError(
            "upstream server error",
            status_code=500,
            retryable=True,
            failover=True,
            error_type="server_error",
        ),
        "400": ProviderError(
            "upstream rejected the request",
            status_code=400,
            retryable=False,
            failover=False,
            error_type="bad_request",
        ),
        "malformed": ProviderError(
            "malformed upstream stream",
            status_code=502,
            retryable=True,
            failover=True,
            error_type="malformed_stream",
        ),
    }
    if fault in {"slow", "malformed_mid"}:
        return None
    if fault not in table:
        return ProviderError(
            f"unknown fault injection '{fault}'",
            status_code=400,
            retryable=False,
            failover=False,
            error_type="bad_request",
        )
    return table[fault]
