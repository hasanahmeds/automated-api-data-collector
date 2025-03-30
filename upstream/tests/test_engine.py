"""End-to-end engine tests using the real sample data + configs."""
from __future__ import annotations

from pathlib import Path

import sqlalchemy as sa

from src.config_loader import load_pipeline
from src.engine import Engine


def test_orders_pipeline_end_to_end(settings, orders_spec):
    engine = Engine(settings)
    result = engine.run(orders_spec)

    assert result.status == "success", getattr(result, "error", result.status)
    assert result.rows_in == 9
    # 9 rows: 1 duplicate dropped, 2 null emails dropped, 1 bad email invalidated
    # then 1 negative-amount row filtered -> order: dedup(8) drop_null(6) validate->null
    # filter(amount>=0 drops 1) -> 5 rows out
    assert result.rows_out == 5
    assert result.quality_score >= 80
    assert result.gate_passed is True
    assert len(result.checks) == 5

    # Lineage must contain source + transforms + quality + sink steps
    steps = [s["step"] for s in result.lineage["steps"]]
    assert steps == ["source", "transform", "transform", "transform", "transform", "transform", "transform", "quality", "sink"]

    # Report files exist
    assert Path(result.report_path).exists()

    # Rows actually landed in the DB
    db = sa.create_engine(settings.database_url)
    with db.connect() as conn:
        count = conn.execute(sa.text("SELECT count(*) FROM orders")).scalar()
        assert count == 5
        # Upsert by primary key: ids unique
        ids = conn.execute(sa.text("SELECT order_id FROM orders")).scalars().all()
        assert len(set(ids)) == 5


def test_quality_gate_aborts(settings):
    engine = Engine(settings)
    spec = load_pipeline(Path(__file__).resolve().parents[1] / "config" / "pipelines" / "example_orders.yaml")
    spec.sink.database_url = settings.database_url
    spec.quality.min_score = 99.0  # impossible to pass
    result = engine.run(spec)
    assert result.status == "quality_failed"
    assert result.gate_passed is False
    db = sa.create_engine(settings.database_url)
    # No partial load: the orders table must not exist (gate aborts before sink)
    assert not sa.inspect(db).has_table("orders")


def test_users_pipeline_csv_sink(settings):
    engine = Engine(settings)
    spec = load_pipeline(Path(__file__).resolve().parents[1] / "config" / "pipelines" / "example_users.yaml")
    spec.sink.database_url = settings.database_url
    spec.sink.path = str(settings.output_dir / "users")  # isolate output
    result = engine.run(spec)
    assert result.status == "success"
    # 6 rows -> dedup on user_id -> 5 rows. 'validate' nulls the invalid country
    # (XX) instead of dropping the row, so rows_out stays 5; the consistency
    # check still scores it.
    assert result.rows_out == 5
    assert result.quality_score >= 80
    out = settings.output_dir / "users.csv"
    assert out.exists()
