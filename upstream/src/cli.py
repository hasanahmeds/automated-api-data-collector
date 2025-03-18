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


def cmd_lineage(args) -> None:
    settings = get_settings()
    engine = Engine(settings)
    lineage = engine.get_lineage(args.run_id)
    if lineage is None:
        print(f"No lineage for run {args.run_id}", file=sys.stderr)
        sys.exit(1)
    print(json.dumps(lineage, indent=2, ensure_ascii=False))


def cmd_serve(args) -> None:
    import uvicorn

    uvicorn.run("src.api.main:app", host=args.host, port=args.port, reload=False)


def cmd_schedule(args) -> None:
    from src.orchestration.scheduler import run_scheduler

    run_scheduler()


def main() -> None:
    parser = argparse.ArgumentParser(prog="data-pipeline-etl", description=__doc__)
    sub = parser.add_subparsers(dest="command", required=True)

    p_run = sub.add_parser("run", help="run a pipeline")
    p_run.add_argument("--pipeline", required=True)
    p_run.add_argument("--force", action="store_true", help="re-run even if fingerprint matches")
    p_run.set_defaults(func=cmd_run)

    p_runs = sub.add_parser("runs", help="list runs")
    p_runs.add_argument("--pipeline", default=None)
    p_runs.set_defaults(func=cmd_runs)

    p_lin = sub.add_parser("lineage", help="show lineage for a run")
    p_lin.add_argument("--run-id", required=True)
    p_lin.set_defaults(func=cmd_lineage)

    p_serve = sub.add_parser("serve", help="run REST API")
    p_serve.add_argument("--host", default="127.0.0.1")
    p_serve.add_argument("--port", type=int, default=8000)
    p_serve.set_defaults(func=cmd_serve)

    p_sched = sub.add_parser("schedule", help="run the built-in scheduler")
    p_sched.set_defaults(func=cmd_schedule)

    args = parser.parse_args()
    args.func(args)


if __name__ == "__main__":
    main()
