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

    # Quality: known checks, columns referenced where required.
    from src.quality import CHECKERS  # local import to avoid cycle at import time
    for i, exp in enumerate(spec.quality.expectations):
        if exp.check not in CHECKERS:
            issues.append({"severity": "error", "field": f"quality.expectations[{i}].check",
                           "message": f"unknown check: {exp.check}"})
        if exp.check in {"freshness", "schema", "referential"} and exp.check != "schema" and not exp.column:
            issues.append({"severity": "error", "field": f"quality.expectations[{i}].column",
                           "message": f"{exp.check} requires a column"})
        if exp.check == "freshness" and exp.max_age_hours is None:
            issues.append({"severity": "error", "field": f"quality.expectations[{i}].max_age_hours",
                           "message": "freshness requires max_age_hours"})
        if exp.check == "schema" and not exp.schema:
            issues.append({"severity": "error", "field": f"quality.expectations[{i}].schema",
                           "message": "schema check requires a schema map"})
        if exp.check == "referential" and not exp.reference_path:
            issues.append({"severity": "error", "field": f"quality.expectations[{i}].reference_path",
                           "message": "referential requires reference_path"})

    # Transforms: known rules.
    from src.transforms import _HANDLERS  # local import
    for i, rule in enumerate(spec.transforms):
        if rule.rule not in _HANDLERS:
            issues.append({"severity": "error", "field": f"transforms[{i}].rule",
                           "message": f"unknown rule: {rule.rule}"})
        if rule.rule == "filter" and not rule.condition:
            issues.append({"severity": "error", "field": f"transforms[{i}].condition",
                           "message": "filter requires a condition"})

    return {
        "pipeline": spec.name,
        "valid": not any(i["severity"] == "error" for i in issues),
        "issues": issues,
        "summary": {
            "transforms": len(spec.transforms),
            "expectations": len(spec.quality.expectations),
            "min_score": spec.quality.min_score,
        },
    }


def create_app() -> FastAPI:
    settings = get_settings()
    app = FastAPI(
        title="data-pipeline-etl",
        version=__version__,
        description=(
            "Config-driven ETL & data-governance pipeline: connect -> clean -> "
            "validate -> load, with lineage, quality scoring and idempotent runs."
        ),
        lifespan=lifespan,
    )
    app.add_middleware(
        CORSMiddleware,
        allow_origins=["*"],
        allow_methods=["*"],
        allow_headers=["*"],
    )

    # ------------------------------------------------------------------ #
    # Health & catalogue
    # ------------------------------------------------------------------ #
    @app.get("/api/health")
    def health() -> dict[str, Any]:
        return {"status": "ok", "version": __version__,
                "pipelines": list(app.state.pipelines)}

    @app.get("/api/pipelines")
    def pipelines() -> list[dict[str, Any]]:
        return [
            {
                "name": spec.name,
                "description": spec.description,
                "source_type": spec.source.type,
                "sink_type": spec.sink.type,
                "sink_table": spec.sink.table,
                "rules": [r.rule for r in spec.transforms],
                "expectations": [e.check for e in spec.quality.expectations],
                "schedule": spec.schedule,
            }
            for spec in app.state.pipelines.values()
        ]

    # ------------------------------------------------------------------ #
    # Run management
    # ------------------------------------------------------------------ #
    @app.post("/api/pipelines/{name}/run")
    def run_pipeline(name: str, force: bool = Query(False)) -> dict[str, Any]:
        spec = _get_pipeline(app, name)
        result = app.state.engine.run(spec, force=force)
        if result.status == "failed":
            raise HTTPException(500, getattr(result, "error", "pipeline failed"))
        return result.to_dict()

    @app.get("/api/pipelines/{name}/runs")
    def pipeline_runs(name: str, limit: int = Query(50, ge=1, le=500)) -> list[dict[str, Any]]:
        """Run history for a single pipeline."""
        _get_pipeline(app, name)
        return app.state.engine.list_runs(pipeline=name, limit=limit)

    @app.get("/api/pipelines/{name}/lineage")
    def pipeline_lineage(name: str) -> dict[str, Any]:
        """Most-recent lineage record for a pipeline."""
        _get_pipeline(app, name)
        runs = app.state.engine.list_runs(pipeline=name, limit=1)
        if not runs:
            raise HTTPException(404, f"No runs found for pipeline '{name}'")
        run_id = runs[0]["run_id"]
        data = app.state.engine.get_lineage(run_id)
        if data is None:
            raise HTTPException(404, f"No lineage file for run {run_id}")
        return data

    @app.post("/api/pipelines/{name}/validate")
    def validate_pipeline(name: str) -> dict[str, Any]:
        """Dry-run config validation: no source reading, no writing."""
        spec = _get_pipeline(app, name)
        try:
            return _validate_spec(spec)
