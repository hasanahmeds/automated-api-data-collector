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
            "pipeline": self.pipeline,
            "status": self.status,
            "rows_in": self.rows_in,
            "rows_out": self.rows_out,
            "dropped": self.dropped,
            "quality_score": self.quality_score,
            "gate_passed": self.gate_passed,
            "duration_ms": self.duration_ms,
            "report_path": self.report_path,
            "error": self.error,
        }


class MetricsCollector:
    """In-memory counters aggregated across all engine runs in this process.

    The API layer reads these to expose ``/api/metrics`` (alongside the
    Prometheus text format derived from the ``pipeline_runs`` table). Counters
    are intentionally simple — no atomicity needed since the engine is single
    threaded per process; the durable source of truth stays in the DB.
    """

    def __init__(self) -> None:
        self.rows_processed: int = 0
        self.rows_rejected: int = 0
        self.rows_written: int = 0
        self.quality_score_sum: float = 0.0
        self.quality_score_count: int = 0
        self.pipeline_duration_ms: float = 0.0
        self.runs_total: int = 0
        self.runs_by_status: dict[str, int] = {}

    def record(self, result: PipelineResult) -> None:
        self.runs_total += 1
        self.runs_by_status[result.status] = self.runs_by_status.get(result.status, 0) + 1
        self.rows_processed += result.rows_in
        self.rows_rejected += result.dropped
        self.rows_written += result.rows_out
        self.pipeline_duration_ms += result.duration_ms
        if result.quality_score is not None:
            self.quality_score_sum += result.quality_score
            self.quality_score_count += 1

    @property
    def quality_score(self) -> float:
        if not self.quality_score_count:
            return 0.0
        return round(self.quality_score_sum / self.quality_score_count, 2)

    def to_dict(self) -> dict[str, Any]:
        return {
            "rows_processed": self.rows_processed,
            "rows_rejected": self.rows_rejected,
            "rows_written": self.rows_written,
            "quality_score": self.quality_score,
            "pipeline_duration_ms": round(self.pipeline_duration_ms, 2),
            "runs_total": self.runs_total,
            "runs_by_status": dict(self.runs_by_status),
        }


def compute_fingerprint(spec: PipelineSpec, df: pd.DataFrame) -> str:
    """Stable hash of pipeline name + source identity + content hash.

    Used for idempotency: two runs with the same fingerprint produce
    identical output, so the engine skips re-processing on the second run.
    """
    src = spec.source
    if src.type in ("csv", "json") and src.path:
        path = Path(src.path)
        stat = path.stat() if path.exists() else None
        identity = f"{src.type}:{path}:{stat.st_mtime_ns if stat else '?'}:{stat.st_size if stat else '?'}"
    elif src.type == "api":
        identity = f"api:{src.url}:{sorted(src.params.items())}"
    elif src.type == "sql":
        identity = f"sql:{src.dsn}:{src.query}"
    else:
        identity = f"{src.type}:{src.path or src.url or src.query}"
    content_hash = hashlib.sha256(pd.util.hash_pandas_object(df, index=True).values.tobytes()).hexdigest()[:16]
    return hashlib.sha256(f"{spec.name}|{identity}|{content_hash}".encode()).hexdigest()


class Engine:
    """ETL runtime: orchestrates source -> transform -> quality -> sink.

    The engine is stateless between runs: every state lives either on disk
    (lineage files, quality reports, sink tables) or in the ``pipeline_runs``
    marker table (used for the idempotency check). The optional
    :class:`MetricsCollector` holds in-process counters for the API.
    """

    def __init__(self, settings: Settings) -> None:
        self.settings = settings
        settings.output_dir.mkdir(parents=True, exist_ok=True)
        settings.lineage_dir.mkdir(parents=True, exist_ok=True)
        settings.reports_dir.mkdir(parents=True, exist_ok=True)
        self._engine = sa.create_engine(settings.database_url)
        with self._engine.begin() as conn:
            conn.execute(sa.text(RUN_MARKER_DDL))
        self.metrics = MetricsCollector()
        logger.info("engine initialised",
                    extra={"database_url": settings.database_url})

    # ------------------------------------------------------------------ #
    # Internal helpers
    # ------------------------------------------------------------------ #
    def _marker_db(self, sink: SinkSpec) -> sa.Engine:
        url = sink.database_url or self.settings.database_url
        if url == self.settings.database_url:
            return self._engine
        engine = sa.create_engine(url)
        with engine.begin() as conn:
            conn.execute(sa.text(RUN_MARKER_DDL))
        return engine

    def _last_fingerprint(self, engine: sa.Engine, spec: PipelineSpec) -> tuple[str, str, int, float] | None:
        with engine.connect() as conn:
            row = conn.execute(
                sa.text("SELECT run_id, fingerprint, rows_out, quality_score FROM pipeline_runs "
                        "WHERE pipeline=:p AND status='success' ORDER BY started_at DESC LIMIT 1"),
