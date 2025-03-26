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
    df: pd.DataFrame,
    primary_key: list[str] | None = None,
) -> sa.Table:
    """Create ``table_name`` from ``df`` dtypes if it doesn't already exist."""
    metadata = sa.MetaData()
    cols: list[sa.Column[Any]] = []
    pk = primary_key or []
    for name, dtype in df.dtypes.items():
        cols.append(
            sa.Column(str(name), _sa_type(dtype), primary_key=(name in pk))
        )
    table = sa.Table(table_name, metadata, *cols)
    try:
        table.create(engine, checkfirst=True)
    except Exception as exc:  # noqa: BLE001
        raise SinkError(
            f"failed to create table '{table_name}': {exc}",
            context={"table": table_name, "primary_key": pk},
        ) from exc
    logger.info("table ready", extra={"table": table_name, "primary_key": pk})
    return table


def _clean(value: Any) -> Any:
    """Convert pandas NaN/NaT to SQL NULL before binding."""
    import numpy as np

    if value is None:
        return None
    if isinstance(value, float) and np.isnan(value):
        return None
    if isinstance(value, np.floating) and np.isnan(value):
        return None
    try:
        if pd.isna(value):
            return None
    except (TypeError, ValueError):
        pass
    return value


def upsert_rows(
    engine: sa.Engine,
    table: sa.Table,
    df: pd.DataFrame,
    primary_key: list[str] | None,
) -> int:
    """Insert rows idempotently.

    * with a primary key -> dialect-specific upsert (ON CONFLICT DO UPDATE);
    * without a primary key -> append (the engine handles run-level dedupe
      via the run-marker table).
    """
    records = [{k: _clean(v) for k, v in r.items()} for r in df.to_dict("records")]
    if not records:
        return 0
    pk = primary_key or []
    try:
        if pk and all(p in table.columns for p in pk):
            dialect = engine.dialect.name
            if dialect == "postgresql":
                from sqlalchemy.dialects.postgresql import insert as dialect_insert
            else:
                from sqlalchemy.dialects.sqlite import insert as dialect_insert
            stmt = dialect_insert(table).values(records)
            update = {c.name: getattr(stmt.excluded, c.name)
                      for c in table.columns if c.name not in pk}
            stmt = stmt.on_conflict_do_update(index_elements=pk, set_=update)
            with engine.begin() as conn:
                conn.execute(stmt)
        else:
            with engine.begin() as conn:
                conn.execute(table.insert(), records)
    except Exception as exc:  # noqa: BLE001
        raise SinkError(
            f"failed to write {len(records)} records: {exc}",
            context={"table": table.name, "primary_key": pk},
        ) from exc
    logger.info("rows written",
                extra={"table": table.name, "rows": len(records), "mode": "upsert" if pk else "append"})
    return len(records)


def write_warehouse(
    df: pd.DataFrame,
    sink: SinkSpec,
    run_id: str,
    output_dir: Any,
) -> str:
    """Write to CSV or Parquet under the output dir; returns the file path."""
    path = sink.path or str(output_dir / f"{sink.table or 'export'}_{run_id}")
    try:
        if sink.type == "parquet":
            if not path.endswith(".parquet"):
                path += ".parquet"
            df.to_parquet(path, index=False)
        else:
            if not path.endswith(".csv"):
                path += ".csv"
            df.to_csv(path, index=False)
    except Exception as exc:  # noqa: BLE001
        raise SinkError(
            f"failed to write file sink: {exc}",
            context={"path": path, "type": sink.type},
        ) from exc
    logger.info("warehouse written", extra={"path": path, "rows": len(df), "type": sink.type})
    return path
