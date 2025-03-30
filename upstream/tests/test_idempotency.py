"""Idempotency tests: re-runs never duplicate data."""
from __future__ import annotations

import sqlalchemy as sa

from src.engine import Engine


def _count(db, table: str) -> int:
    with db.connect() as conn:
        return conn.execute(sa.text(f"SELECT count(*) FROM {table}")).scalar()


def test_same_fingerprint_skips(settings, orders_spec):
    engine = Engine(settings)
    first = engine.run(orders_spec)
    assert first.status == "success"
    assert first.rows_out == 5

    second = engine.run(orders_spec)
    assert second.status == "skipped"  # fingerprint matched -> idempotent skip
    assert second.rows_out == 5  # reported rows unchanged, nothing loaded

    db = sa.create_engine(settings.database_url)
    assert _count(db, "orders") == 5  # still 5 rows — no duplicates


def test_forced_rerun_upserts_not_duplicates(settings, orders_spec):
    engine = Engine(settings)
    engine.run(orders_spec)
    result = engine.run(orders_spec, force=True)  # bypass fingerprint skip
    assert result.status == "success"

    db = sa.create_engine(settings.database_url)
    # Upsert on primary key: same 5 rows, updated in place
    assert _count(db, "orders") == 5


def test_failed_run_can_rerun_safely(settings, orders_spec):
    engine = Engine(settings)
    # Force a failure: point the source at a missing file
    bad = orders_spec.model_copy(deep=True)
    bad.source.path = "data/sample/does_not_exist.csv"
    failed = engine.run(bad)
    assert failed.status == "failed"

    # The real pipeline still runs cleanly afterwards (no poisoned state)
    ok = engine.run(orders_spec)
    assert ok.status == "success"
    db = sa.create_engine(settings.database_url)
    assert _count(db, "orders") == 5


def test_quality_failed_does_not_partially_load(settings, orders_spec):
    engine = Engine(settings)
    spec = orders_spec.model_copy(deep=True)
    spec.quality.min_score = 99.0
    result = engine.run(spec)
    assert result.status == "quality_failed"
    db = sa.create_engine(settings.database_url)
    # Gate aborted before the sink -> no table, no partial load
    assert not sa.inspect(db).has_table("orders")
