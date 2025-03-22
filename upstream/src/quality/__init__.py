"""Data-quality checks (Great-Expectations-style) and scoring.

Each expectation is checked against the DataFrame; every check yields a
pass/fail verdict, a 0-100 score, and a failure sample. The stream score is a
weighted average across checks — the number you can gate on or alert on.

Available checks:
    * ``completeness``  — non-null rate of a column (threshold via ``min``).
    * ``consistency``   — values match a regex and/or allowed set.
    * ``uniqueness``    — no duplicates on a key (or set of columns).
    * ``range``         — numeric values fall within ``[min, max]``.
    * ``volume``        — row count >= ``min_rows``.
    * ``freshness``     — newest record in ``column`` is within ``max_age_hours``.
    * ``schema``        — required columns are present with the expected dtype
                          (declared in ``schema`` as ``{column: dtype}``).
    * ``referential``   — every non-null value in ``column`` exists in the
                          first column of the CSV at ``reference_path``.
"""
from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import pandas as pd

from src.config_loader import ExpectationSpec
from src.logging_config import get_logger

logger = get_logger("src.quality", component="quality")

# Map the dtype strings accepted in YAML/``schema`` to pandas-compatible
# dtype checkers. ``date`` is checked separately because ``to_datetime`` is
# the canonical coercion.
_DTYPE_CHECKS: dict[str, str] = {
    "int": "int",
    "integer": "int",
    "float": "float",
    "str": "str",
    "string": "str",
    "bool": "bool",
    "boolean": "bool",
    "date": "datetime",
    "datetime": "datetime",
}


@dataclass
class CheckResult:
    check: str
    column: str | None
    passed: bool
    score: float          # 0-100
    detail: str
    samples: list[Any] = field(default_factory=list)

    def to_dict(self) -> dict[str, Any]:
        return {
            "check": self.check,
            "column": self.column,
            "passed": self.passed,
            "score": round(self.score, 2),
            "detail": self.detail,
            "samples": self.samples[:5],
        }


def _non_null_rate(s: pd.Series) -> float:
    return 0.0 if len(s) == 0 else float(s.notna().mean())


# --------------------------------------------------------------------------- #
# Existing checks
# --------------------------------------------------------------------------- #
def check_completeness(df: pd.DataFrame, exp: ExpectationSpec) -> CheckResult:
    col = exp.column
    if not col or col not in df.columns:
        return CheckResult("completeness", col, False, 0.0,
                            f"column '{col}' missing")
    rate = _non_null_rate(df[col])
