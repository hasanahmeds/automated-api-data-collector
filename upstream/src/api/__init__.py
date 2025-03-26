"""REST API + Grafana-friendly metrics.

Endpoints:
    GET  /api/health
    GET  /api/pipelines
    POST /api/pipelines/{name}/run
    GET  /api/pipelines/{name}/runs         — run history for a pipeline
    GET  /api/pipelines/{name}/lineage      — most-recent lineage for a pipeline
    POST /api/pipelines/{name}/validate     — dry-run config validation
    GET  /api/runs                          — global run history
    GET  /api/runs/{run_id}/lineage         — lineage for a specific run
    GET  /api/metrics                        — Prometheus + in-memory counters
    GET  /api/metrics/json                   — structured metrics for dashboards
"""
from __future__ import annotations

from contextlib import asynccontextmanager
from pathlib import Path
from typing import Any

from fastapi import FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware

from src import __version__
from src.config import get_settings
from src.config_loader import load_all_pipelines
from src.engine import Engine
from src.exceptions import PipelineError
from src.logging_config import get_logger

logger = get_logger("src.api", component="api")


@asynccontextmanager
async def lifespan(app: FastAPI):
    settings = get_settings()
    app.state.settings = settings
    app.state.engine = Engine(settings)
    app.state.pipelines = load_all_pipelines(settings.pipelines_dir)
    logger.info("api initialised",
                extra={"pipelines": list(app.state.pipelines),
                       "version": __version__})
    yield


def _get_pipeline(app: FastAPI, name: str):
    spec = app.state.pipelines.get(name)
    if spec is None:
        raise HTTPException(404, f"Unknown pipeline '{name}'")
    return spec


def _validate_spec(spec) -> dict[str, Any]:
    """Dry-run validation: load the spec, check it resolves, and walk the
    rules / expectations for obvious misconfigurations. Does NOT execute
    the pipeline."""
    issues: list[dict[str, Any]] = []

    # Source: file paths must exist for csv/json.
    src = spec.source
    if src.type in ("csv", "json") and src.path:
        if not Path(src.path).exists():
            issues.append({"severity": "error", "field": "source.path",
                           "message": f"file not found: {src.path}"})
    if src.type == "api" and not src.url:
        issues.append({"severity": "error", "field": "source.url",
                       "message": "api source requires a url"})
    if src.type == "sql" and not (src.dsn and src.query):
        issues.append({"severity": "error", "field": "source.sql",
                       "message": "sql source requires dsn + query"})

    # Sink: file/db requirements.
    sink = spec.sink
    if sink.type in ("csv", "parquet"):
        if not (sink.path or sink.table):
            issues.append({"severity": "error", "field": "sink",
                           "message": "file sink requires path or table name"})
    if sink.type in ("sqlite", "postgres") and not sink.table:
        issues.append({"severity": "error", "field": "sink.table",
                       "message": "db sink requires a table name"})
