"""Command-line interface.

Usage::

    python -m src.cli run --pipeline example_orders [--force]
    python -m src.cli runs [--pipeline example_orders]
    python -m src.cli lineage --run-id <id>
    python -m src.cli serve
    python -m src.cli schedule
"""
from __future__ import annotations

import argparse
import json
import sys

from src.config import get_settings
from src.config_loader import load_all_pipelines, load_pipeline
from src.engine import Engine


def cmd_run(args) -> None:
    settings = get_settings()
    engine = Engine(settings)
    spec = load_pipeline(settings.pipelines_dir / f"{args.pipeline}.yaml")
    result = engine.run(spec, force=args.force)
    print(json.dumps(result.to_dict(), indent=2, ensure_ascii=False))
    if result.status == "failed":
        print(f"ERROR: {getattr(result, 'error', 'unknown')}", file=sys.stderr)
        sys.exit(1)
    if result.status == "quality_failed":
        print(f"QUALITY GATE FAILED — no rows loaded (score {result.quality_score})", file=sys.stderr)
        sys.exit(2)


def cmd_runs(args) -> None:
    settings = get_settings()
    engine = Engine(settings)
    print(json.dumps(engine.list_runs(pipeline=args.pipeline), indent=2, ensure_ascii=False))

