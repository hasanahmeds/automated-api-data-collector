"""Domain-specific exception hierarchy for the ETL pipeline.

A small, well-typed exception tree lets the engine surface failures with
context (which source, which transform, which quality gate failed) instead of
catching bare ``Exception`` instances. Every error carries a ``context`` dict
that callers can forward to logs, lineage, and API responses.
"""
from __future__ import annotations

from typing import Any


class PipelineError(Exception):
    """Base class for every failure raised by the pipeline runtime.

    Attributes:
        message: human-readable description.
        context: structured fields (pipeline, source, transform, gate, ...)
            that callers can log or return to API clients.
    """

    def __init__(self, message: str, *, context: dict[str, Any] | None = None) -> None:
        super().__init__(message)
        self.message = message
        self.context: dict[str, Any] = dict(context or {})

    def to_dict(self) -> dict[str, Any]:
        """Serialise the error to a JSON-friendly dict (for API responses)."""
        return {
            "error": type(self).__name__,
            "message": self.message,
            "context": self.context,
        }


class SourceError(PipelineError):
    """Raised when a connector cannot read its source (file missing, HTTP
    5xx, SQL error, malformed payload, ...)."""


class TransformError(PipelineError):
    """Raised when a cleaning rule fails (unknown rule, bad column, coerce
    overflow, invalid filter expression, ...)."""


class QualityGateError(PipelineError):
    """Raised when a quality expectation is misconfigured or cannot be
    evaluated (unknown check, missing column for a required field, ...)."""


class SinkError(PipelineError):
    """Raised when the loading layer fails (DDL, upsert, file write, ...)."""


class ConfigValidationError(PipelineError):
    """Raised when a pipeline spec fails dry-run validation (missing fields,
    unreachable source, unknown rule/check, ...)."""
