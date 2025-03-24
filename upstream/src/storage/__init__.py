"""Storage: idempotent DB sinks + file-based warehouse output.

Two sink families are supported:

    * **Database sinks** (SQLite/PostgreSQL) — true upserts via
      ``ON CONFLICT DO UPDATE`` when a primary key is configured, otherwise
      append (the engine de-duplicates via the run-marker table).
    * **File sinks** (CSV/Parquet) — single-file writes under the configured
      output directory, namespaced by ``run_id``.
"""
from __future__ import annotations

from typing import Any

import pandas as pd
import sqlalchemy as sa

from src.config_loader import SinkSpec
from src.exceptions import SinkError
from src.logging_config import get_logger

logger = get_logger("src.storage", component="storage")


def _sa_type(dtype: Any) -> sa.types.TypeEngine:
    """Map a pandas dtype to the closest SQLAlchemy column type."""
    if pd.api.types.is_integer_dtype(dtype):
        return sa.BigInteger()
    if pd.api.types.is_float_dtype(dtype):
        return sa.Float()
    if pd.api.types.is_datetime64_any_dtype(dtype):
        return sa.DateTime(timezone=True)
    if pd.api.types.is_bool_dtype(dtype):
        return sa.Boolean()
    return sa.Text()


def create_table_from_df(
    engine: sa.Engine,
    table_name: str,
