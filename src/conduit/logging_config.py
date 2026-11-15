"""JSON logs with a last-line redaction pass for keys and bearer tokens."""

from __future__ import annotations

import json
import logging
import re
from datetime import UTC, datetime
from typing import Any

_BEARER = re.compile(r"(?i)(bearer\s+)[A-Za-z0-9._\-+/=]+")
_GATEWAY_KEY = re.compile(r"cdt_(?:live|test)_[A-Za-z0-9_\-]+")
_SKIPPED_FIELDS = {"authorization", "api_key", "prompt", "messages", "content"}

_configured = False


def redact(text: str) -> str:
    text = _BEARER.sub(r"\1[redacted]", text)
    return _GATEWAY_KEY.sub("cdt_[redacted]", text)


class RedactFilter(logging.Filter):
    def filter(self, record: logging.LogRecord) -> bool:
        if isinstance(record.msg, str):
            record.msg = redact(record.msg)
        if record.args:
            record.args = _redact_args(record.args)
        return True


def _redact_args(args: Any) -> Any:
    if isinstance(args, dict):
        return {key: redact(value) if isinstance(value, str) else value for key, value in args.items()}
    if isinstance(args, tuple):
        return tuple(redact(value) if isinstance(value, str) else value for value in args)
    return args


class JsonFormatter(logging.Formatter):
    def format(self, record: logging.LogRecord) -> str:
        payload: dict[str, Any] = {
            "ts": datetime.now(UTC).isoformat(),
            "level": record.levelname,
            "logger": record.name,
            "event": record.getMessage(),
        }
        fields = getattr(record, "conduit_fields", None)
        if isinstance(fields, dict):
            for key, value in fields.items():
                if key in _SKIPPED_FIELDS:
                    continue
                payload[str(key)] = value
        if record.exc_info:
            payload["exception"] = self.formatException(record.exc_info)
        return redact(json.dumps(payload, default=str, separators=(",", ":")))


def setup_logging(level: str) -> None:
    global _configured
    logger = logging.getLogger("conduit")
    logger.setLevel(level.upper())
    logger.propagate = False
    if _configured and logger.handlers:
        return
    handler = logging.StreamHandler()
    handler.setFormatter(JsonFormatter())
    handler.addFilter(RedactFilter())
    logger.addHandler(handler)
    _configured = True


def log_event(logger: logging.Logger, level: int, event: str, **fields: object) -> None:
    logger.log(level, event, extra={"conduit_fields": fields})
