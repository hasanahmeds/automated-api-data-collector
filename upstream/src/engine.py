"""Pipeline engine: connect -> clean -> validate -> load, with lineage + idempotency.

Idempotency model:
  * every run has a ``run_id`` and an input *fingerprint* (pipeline name +
    source identity, e.g. path+mtime+size for files, URL/query for others);
  * a previously successful run with the same fingerprint is skipped unless
    ``force=True`` (re-runs never duplicate work);
  * when a primary key is configured, loading is a true upsert
    (``ON CONFLICT DO UPDATE``), so even forced re-runs cannot duplicate rows.

This module also exposes a :class:`MetricsCollector` (counters for
``rows_processed``, ``rows_rejected``, ``quality_score``, ``pipeline_duration``)
that the engine updates on every run and the API exposes under ``/api/metrics``.
"""
from __future__ import annotations

import hashlib
import time
import uuid
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import pandas as pd
import sqlalchemy as sa

from src.config import Settings
from src.config_loader import PipelineSpec, SinkSpec
from src.connectors import build_connector
from src.exceptions import (
    PipelineError,
    QualityGateError,
    SinkError,
    SourceError,
    TransformError,
)
from src.lineage import LineageTracker, load_lineage
from src.logging_config import get_logger, log_duration_ms
from src.quality import evaluate
from src.quality.reporter import build_report, write_html, write_json
from src.storage import create_table_from_df, upsert_rows, write_warehouse
from src.transforms import apply_all

logger = get_logger("src.engine", component="engine")

RUN_MARKER_DDL = """
CREATE TABLE IF NOT EXISTS pipeline_runs (
    run_id TEXT PRIMARY KEY,
    pipeline TEXT NOT NULL,
    fingerprint TEXT,
    status TEXT NOT NULL,
    rows_in INTEGER DEFAULT 0,
    rows_out INTEGER DEFAULT 0,
    quality_score REAL,
    started_at TEXT,
    finished_at TEXT
)
"""


@dataclass
class PipelineResult:
    run_id: str
    pipeline: str
    status: str                 # success | failed | skipped | quality_failed
    rows_in: int = 0
    rows_out: int = 0
    dropped: int = 0
    quality_score: float = 100.0
    gate_passed: bool = True
    checks: list[dict] = field(default_factory=list)
    duration_ms: float = 0.0
    lineage: dict = field(default_factory=dict)
    report_path: str | None = None
    error: str | None = None

    def to_dict(self) -> dict[str, Any]:
        return {
            "run_id": self.run_id,
