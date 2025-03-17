"""Run both sample pipelines end-to-end and print the results.

Usage: python scripts/demo.py
"""
from __future__ import annotations

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from src.config import get_settings  # noqa: E402
from src.config_loader import load_all_pipelines  # noqa: E402
from src.engine import Engine  # noqa: E402


def main() -> None:
    settings = get_settings()
    engine = Engine(settings)
    specs = load_all_pipelines(settings.pipelines_dir)
    for name, spec in specs.items():
        result = engine.run(spec)
        print(f"\n=== {name}: {result.status} ===")
        print(f"  rows_in={result.rows_in} rows_out={result.rows_out} dropped={result.dropped}")
        print(f"  quality_score={result.quality_score} gate_passed={result.gate_passed}")
        print(f"  duration_ms={result.duration_ms} run_id={result.run_id}")
        for c in result.checks:
            print(f"    - {c['check']:<14} {c['column'] or '-':<12} {'PASS' if c['passed'] else 'FAIL'}  score={c['score']:.1f}  {c['detail']}")
        print(f"  report: {result.report_path}")
        steps = [s['step'] for s in result.lineage['steps']]
        print(f"  lineage: {' -> '.join(steps)}")

    print("\nAll reports written under data/reports/. Query runs with: python -m src.cli runs")


if __name__ == "__main__":
    main()
