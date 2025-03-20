"""Lineage tracking: record where every row came from and what happened to it.

Every pipeline run writes a lineage file (JSON) and a run record: source,
each transformation applied (rows in/out, affected), the quality score, the
sink and timing. This answers the interview-favourite question: "where does
this number come from, and how was it derived?"
"""
from __future__ import annotations

import json
import time
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from src.logging_config import get_logger
from src.transforms import RuleEvent

logger = get_logger("src.lineage", component="lineage")


class LineageTracker:
    """Collect per-step lineage for a single run; persist on :meth:`finish`."""

    def __init__(self, lineage_dir: Path) -> None:
        self.lineage_dir = lineage_dir
        self.lineage_dir.mkdir(parents=True, exist_ok=True)
        self.run_id: str = uuid.uuid4().hex[:12]
        self._steps: list[dict[str, Any]] = []
        self.started_at: datetime = datetime.now(timezone.utc)
        self._sw: float = time.perf_counter()

    def source(self, source: dict[str, Any], rows: int) -> None:
        self._steps.append({
            "step": "source",
            "type": source.get("type"),
            "detail": source.get("path") or source.get("url") or source.get("query"),
            "rows": rows,
        })

    def transform(self, event: RuleEvent) -> None:
        self._steps.append({
            "step": "transform",
            "rule": event.rule,
            "detail": event.detail,
            "rows_in": event.rows_in,
            "rows_out": event.rows_out,
            "affected": event.affected,
        })

    def quality(self, score: float, checks: int, passed: bool) -> None:
        self._steps.append({
            "step": "quality",
            "score": score,
            "checks": checks,
            "passed": passed,
        })

    def sink(self, sink: dict[str, Any], rows: int, mode: str) -> None:
        self._steps.append({
            "step": "sink",
            "type": sink.get("type"),
            "target": sink.get("table") or sink.get("path"),
            "rows": rows,
            "mode": mode,
        })

    def finish(self, pipeline: str, status: str) -> dict[str, Any]:
        duration_ms = round((time.perf_counter() - self._sw) * 1000, 2)
        record: dict[str, Any] = {
            "run_id": self.run_id,
            "pipeline": pipeline,
            "status": status,
            "started_at": self.started_at.isoformat(),
            "duration_ms": duration_ms,
            "steps": self._steps,
        }
        path = self.lineage_dir / f"{self.run_id}.json"
        try:
            path.write_text(
                json.dumps(record, ensure_ascii=False, indent=2),
                encoding="utf-8",
            )
        except OSError as exc:
            logger.error("failed to persist lineage",
                         extra={"run_id": self.run_id, "path": str(path), "error": str(exc)})
        logger.info("lineage finished",
                    extra={"run_id": self.run_id, "pipeline": pipeline,
                           "status": status, "steps": len(self._steps),
                           "duration_ms": duration_ms})
        return record


def load_lineage(lineage_dir: Path, run_id: str) -> dict[str, Any] | None:
    """Return the lineage JSON for a run, or ``None`` if not found."""
    path = lineage_dir / f"{run_id}.json"
    if not path.exists():
        return None
    return json.loads(path.read_text(encoding="utf-8"))


def list_runs(lineage_dir: Path, limit: int = 50) -> list[dict[str, Any]]:
    """List recent runs (newest first) by scanning lineage files."""
    runs: list[dict[str, Any]] = []
    for path in sorted(lineage_dir.glob("*.json"), reverse=True):
        try:
            data = json.loads(path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError) as exc:
            logger.warning("skipping malformed lineage file",
                           extra={"path": str(path), "error": str(exc)})
            continue
        runs.append({
            "run_id": data["run_id"],
            "pipeline": data["pipeline"],
            "status": data["status"],
            "started_at": data["started_at"],
            "duration_ms": data["duration_ms"],
        })
        if len(runs) >= limit:
