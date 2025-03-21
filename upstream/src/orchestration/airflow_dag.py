"""Example Airflow DAG wrapping the same engine.

Place this file in your Airflow ``dags/`` directory. It runs the exact same
`data-pipeline-etl` engine, so behaviour is identical whether you use the
built-in scheduler or Airflow.
"""
from __future__ import annotations

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago


def _run_pipeline(pipeline: str) -> dict:
    import sys
    from pathlib import Path

    repo = Path(__file__).resolve().parents[1]
    if str(repo) not in sys.path:
        sys.path.insert(0, str(repo))

    from src.config import get_settings
    from src.config_loader import load_pipeline
    from src.engine import Engine

    settings = get_settings()
    spec = load_pipeline(settings.pipelines_dir / f"{pipeline}.yaml")
    result = Engine(settings).run(spec)
    if result.status in ("failed", "quality_failed"):
        raise RuntimeError(f"pipeline {pipeline} {result.status}")
    return result.to_dict()


DEFAULT_ARGS = {
    "owner": "data-platform",
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
    "start_date": days_ago(1),
