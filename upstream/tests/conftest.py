"""Shared fixtures."""
from __future__ import annotations

import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))


@pytest.fixture()
def settings(tmp_path):
    from src.config import Settings

    return Settings(
        database_url=f"sqlite:///{tmp_path / 'pipeline.db'}",
        pipelines_dir=tmp_path / "config" / "pipelines",
        quality_dir=tmp_path / "config" / "quality",
        lineage_dir=tmp_path / "lineage",
        reports_dir=tmp_path / "reports",
        output_dir=tmp_path / "output",
        default_quality_threshold=80.0,
    )


@pytest.fixture()
def engine(settings):
    from src.engine import Engine

    return Engine(settings)


@pytest.fixture()
def orders_spec(settings):
    from src.config_loader import load_pipeline

    ROOT = Path(__file__).resolve().parents[1]
