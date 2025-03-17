"""Structured logging setup for the ETL pipeline.

Centralised configuration so every module logs the same way: a single
``PipelineLogger`` factory, a JSON-friendly formatter that emits structured
fields (``pipeline_name``, ``step``, ``run_id``, ``duration_ms``, ...), and a
sensible console handler for local dev.

Usage::

    from src.logging_config import get_logger
    logger = get_logger(__name__)
    logger.info("pipeline started", extra={"pipeline_name": "orders", "run_id": "abc"})

The formatter writes a single line per record with key=value pairs after the
message, which is easy to grep in dev and easy to ship to Loki/ELK in prod.
"""
from __future__ import annotations

import json
import logging
import sys
from datetime import datetime, timezone
from typing import Any

# Module-level flag so ``configure_logging`` is idempotent.
_CONFIGURED: bool = False

# Default format that reads well on a terminal.
DEFAULT_FORMAT = "%(asctime)s %(levelname)-8s %(name)s :: %(message)s"


class StructuredFormatter(logging.Formatter):
    """Formatter that appends structured fields from ``record.extra`` as
    ``key=value`` pairs (with JSON-safe values). Falls back to a plain line
    when no extra fields are present."""

    def format(self, record: logging.LogRecord) -> str:
        base = super().format(record)
        extra: dict[str, Any] = getattr(record, "extra", {}) or {}
        if not extra:
            return base
        pairs = " ".join(f"{k}={json.dumps(v, default=str, ensure_ascii=False)}" for k, v in extra.items())
        return f"{base} | {pairs}"


class _ExtraLoggerAdapter(logging.LoggerAdapter):
    """LoggerAdapter that merges ``extra`` kwargs into the log record's
    ``extra`` attribute, so downstream formatters see a flat dict."""

    def process(self, msg: str, kwargs: Any) -> tuple[str, Any]:
        extra = dict(self.extra or {})
        extra.update(kwargs.pop("extra", {}) or {})
        kwargs["extra"] = {"extra": extra}
        return msg, kwargs


def configure_logging(
    level: int = logging.INFO,
    *,
    json_format: bool = False,
    stream: Any = None,
    force: bool = False,
) -> None:
    """Configure the root logger once. Safe to call multiple times.

    Args:
        level: log level for the root logger.
        json_format: if True, emit one JSON object per record (recommended
            for production / log shippers). If False, use the readable
            ``key=value`` format.
        stream: stream for the handler (defaults to ``sys.stderr``).
        force: re-configure even if already configured.
    """
    global _CONFIGURED
    if _CONFIGURED and not force:
        return

    root = logging.getLogger()
    # Remove pre-existing handlers we may have added; keep external ones.
    for h in list(root.handlers):
        if getattr(h, "_pipeline_handler", False):
            root.removeHandler(h)

    handler = logging.StreamHandler(stream or sys.stderr)
    handler._pipeline_handler = True  # type: ignore[attr-defined]
    if json_format:
        handler.setFormatter(_JsonFormatter(DEFAULT_FORMAT))
    else:
        handler.setFormatter(StructuredFormatter(DEFAULT_FORMAT))
    root.addHandler(handler)
    root.setLevel(level)
    _CONFIGURED = True


class _JsonFormatter(logging.Formatter):
    """Emit each record as a single JSON line."""

    def format(self, record: logging.LogRecord) -> str:
        payload: dict[str, Any] = {
            "ts": datetime.now(timezone.utc).isoformat(),
            "level": record.levelname,
            "logger": record.name,
            "message": record.getMessage(),
        }
        extra: dict[str, Any] = getattr(record, "extra", {}) or {}
        payload.update(extra)
        if record.exc_info:
            payload["exc_info"] = self.formatException(record.exc_info)
        return json.dumps(payload, default=str, ensure_ascii=False)


def get_logger(name: str, **default_extra: Any) -> logging.LoggerAdapter:
    """Return a logger that auto-injects ``default_extra`` on every record.

    Example::

        logger = get_logger("src.engine", component="engine")
        logger.info("started", extra={"pipeline_name": "orders"})
        # -> 2026-... INFO src.engine :: started | component="engine" pipeline_name="orders"
    """
