"""Quality report generation: JSON + self-contained HTML.

The HTML report is a single file you can open in a browser or attach to a
Slack/email — no server needed.
"""
from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path

from src.quality import CheckResult


def build_report(pipeline: str, run_id: str, score: float, passed: bool,
                 checks: list[CheckResult], rows_in: int, rows_out: int,
                 duration_ms: float) -> dict:
    return {
        "pipeline": pipeline,
        "run_id": run_id,
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "quality_score": score,
        "gate_passed": passed,
        "rows_in": rows_in,
        "rows_out": rows_out,
        "duration_ms": round(duration_ms, 2),
        "checks": [c.to_dict() for c in checks],
    }


def write_json(report: dict, path: Path) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8")
    return path


def write_html(report: dict, path: Path) -> Path:
    checks_html = "".join(
        f"""
        <tr class="{'pass' if c['passed'] else 'fail'}">
          <td>{c['check']}</td><td>{c['column'] or '—'}</td>
          <td>{'✅ pass' if c['passed'] else '❌ fail'}</td>
          <td>{c['score']:.1f}</td><td>{c['detail']}</td>
        </tr>"""
        for c in report["checks"]
    )
    samples_html = "".join(
        f"<li><code>{c['check']}</code>: {str(c.get('samples', []))}</li>"
        for c in report["checks"] if c.get("samples")
    )
    html = f"""<!doctype html>
<html lang="en"><head><meta charset="utf-8">
<title>Quality report — {report['pipeline']}</title>
<style>
  body {{ font-family: -apple-system, 'Segoe UI', Roboto, sans-serif; margin: 32px; color: #1a202c; }}
  h1 {{ font-size: 22px; }} .meta {{ color: #718096; font-size: 13px; }}
  .score {{ font-size: 44px; font-weight: 700; }}
  .score.pass {{ color: #2f855a; }} .score.fail {{ color: #c53030; }}
  table {{ border-collapse: collapse; margin-top: 16px; width: 100%; }}
  th, td {{ border: 1px solid #e2e8f0; padding: 8px 10px; text-align: left; font-size: 14px; }}
  tr.pass td:first-child {{ border-left: 4px solid #38a169; }}
  tr.fail td:first-child {{ border-left: 4px solid #e53e3e; }}
  ul {{ font-size: 13px; color: #4a5568; }}
</style></head><body>
<h1>Data quality report — {report['pipeline']}</h1>
<div class="meta">run <code>{report['run_id']}</code> · {report['generated_at']} ·
{report['rows_in']} rows in → {report['rows_out']} rows out · {report['duration_ms']} ms</div>
<div class="score {'pass' if report['gate_passed'] else 'fail'}">{report['quality_score']:.1f}/100</div>
<div class="meta">{'Gate passed — ready to load.' if report['gate_passed'] else 'Gate FAILED — pipeline aborted before load.'}</div>
<table><tr><th>check</th><th>column</th><th>result</th><th>score</th><th>detail</th></tr>{checks_html}</table>
{"<h3>Failure samples</h3><ul>" + samples_html + "</ul>" if samples_html else ""}
</body></html>"""
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(html, encoding="utf-8")
    return path
