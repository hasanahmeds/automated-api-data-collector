"""Built-in scheduler + example Airflow DAG.

The built-in scheduler is intentionally simple (interval / daily-at / cron-lite
parsing) so the repo runs standalone. For production, ``airflow_dag.py`` shows
the exact same engine wrapped as an Airflow PythonOperator DAG.
"""
from __future__ import annotations

import re
import time

from src.config import get_settings
from src.config_loader import load_all_pipelines
from src.engine import Engine
from src.logging_config import get_logger

logger = get_logger("src.orchestration.scheduler", component="scheduler")


def _parse_schedule(schedule: str) -> int | None:
    """Return interval seconds for simple schedules; None for daily/cron."""
    m = re.match(r"every\s+(\d+)\s*s", schedule)
    if m:
        return int(m.group(1))
    return None


def _is_due_daily(hhmm: str) -> bool:
    import datetime

    now = datetime.datetime.now()
    target = datetime.datetime.combine(now.date(), datetime.datetime.strptime(hhmm, "%H:%M").time())
    return now.hour == target.hour and now.minute == target.minute


def run_scheduler() -> None:
    settings = get_settings()
    engine = Engine(settings)
    specs = load_all_pipelines(settings.pipelines_dir)
    logger.info("scheduler started",
