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
    threshold = exp.min if exp.min is not None else 0.95
    score = round(min(100.0, rate * 100), 2)
    samples = df.loc[df[col].isna(), col].head(5).tolist()
    return CheckResult("completeness", col, score >= threshold * 100, score,
                       f"{rate:.1%} non-null (threshold {threshold:.0%})", samples)


def check_consistency(df: pd.DataFrame, exp: ExpectationSpec) -> CheckResult:
    col = exp.column
    if col not in df.columns or len(df) == 0:
        return CheckResult("consistency", col, False, 0.0, f"column '{col}' missing or empty")
    s = df[col]
    mask = pd.Series(True, index=df.index)
    if exp.pattern:
        mask = mask & s.astype(str).str.match(exp.pattern, na=False)
    if exp.values is not None:
        mask = mask & s.isin(exp.values)
    ratio = float(mask.mean())
    samples = s[~mask].head(5).tolist()
    rule = f"pattern={exp.pattern}" if exp.pattern else f"values={exp.values}"
    return CheckResult("consistency", col, ratio >= 0.95, round(ratio * 100, 2),
                       f"{ratio:.1%} conform to {rule}", samples)


def check_uniqueness(df: pd.DataFrame, exp: ExpectationSpec) -> CheckResult:
    cols = [exp.column] if exp.column else list(df.columns)
    if not cols or len(df) == 0:
        return CheckResult("uniqueness", None, True, 100.0, "no columns to check")
    dupes = int(df.duplicated(subset=cols).sum())
    ratio = 1.0 - dupes / len(df)
    samples = df[df.duplicated(subset=cols)].head(5).to_dict("records")
    return CheckResult("uniqueness", cols[0] if len(cols) == 1 else None,
                       ratio >= 0.99, round(ratio * 100, 2),
                       f"{dupes} duplicate row(s) on {cols} ({ratio:.1%} unique)", samples)


def check_range(df: pd.DataFrame, exp: ExpectationSpec) -> CheckResult:
    col = exp.column
    if not col or col not in df.columns:
        return CheckResult("range", col, False, 0.0, f"column '{col}' missing")
    s = pd.to_numeric(df[col], errors="coerce")
    mask = pd.Series(True, index=df.index)
    if exp.min is not None:
        mask = mask & (s >= exp.min)
    if exp.max is not None:
        mask = mask & (s <= exp.max)
    ratio = float(mask.mean())
    samples = df.loc[~mask, col].head(5).tolist()
    bounds = (f"[{exp.min}, {exp.max}]"
              if exp.min is not None and exp.max is not None
              else f"min={exp.min}, max={exp.max}")
    return CheckResult("range", col, ratio >= 0.95, round(ratio * 100, 2),
                       f"{ratio:.1%} within {bounds}", samples)


def check_volume(df: pd.DataFrame, exp: ExpectationSpec) -> CheckResult:
    n = len(df)
    min_rows = exp.min_rows or 1
    return CheckResult("volume", None, n >= min_rows, min(100.0, n / min_rows * 100),
                       f"{n} rows (min {min_rows})")


# --------------------------------------------------------------------------- #
# New checks: freshness, schema, referential integrity
# --------------------------------------------------------------------------- #
def check_freshness(df: pd.DataFrame, exp: ExpectationSpec) -> CheckResult:
    """Pass when the newest timestamp in ``column`` is within ``max_age_hours``
    of "now". Useful for SLAs on incremental sources (APIs, CDC feeds)."""
    col = exp.column
    if not col or col not in df.columns:
        return CheckResult("freshness", col, False, 0.0, f"column '{col}' missing")
    max_age = exp.max_age_hours
    if max_age is None:
        return CheckResult("freshness", col, False, 0.0,
                           "max_age_hours not configured")
    if len(df) == 0:
        return CheckResult("freshness", col, False, 0.0, "empty dataframe")
    ts = pd.to_datetime(df[col], errors="coerce")
    if ts.isna().all():
        return CheckResult("freshness", col, False, 0.0,
                           f"column '{col}' has no parseable timestamps")
    newest = ts.max()
    if pd.isna(newest):
        return CheckResult("freshness", col, False, 0.0,
                           f"newest value in '{col}' is null")
    # Make naive vs aware comparable: a naive newest is assumed to be UTC.
    if getattr(newest, "tzinfo", None) is None:
        newest_aware = pd.Timestamp(newest).tz_localize("UTC")
    else:
        newest_aware = pd.Timestamp(newest)
    now = pd.Timestamp.now(tz="UTC")
    age_hours = max(0.0, float((now - newest_aware).total_seconds()) / 3600.0)
    ratio = max(0.0, 1.0 - (age_hours / max_age)) if max_age > 0 else 1.0
    score = round(min(100.0, ratio * 100), 2)
    passed = age_hours <= max_age
    return CheckResult("freshness", col, passed, score,
                       f"newest record {age_hours:.1f}h old (max {max_age}h)",
                       samples=[str(newest)])


def check_schema(df: pd.DataFrame, exp: ExpectationSpec) -> CheckResult:
    """Pass when every column declared in ``schema`` is present and has a
    compatible dtype. Returns a 0-100 score based on the share of columns
    that match (column presence counts as half; correct dtype as full)."""
    expected = exp.schema or {}
    if not expected:
        return CheckResult("schema", None, False, 0.0,
                           "no schema declared in expectation")
    missing: list[str] = []
    wrong_type: list[str] = []
    partial = 0.0
    for name, expected_dtype in expected.items():
        if name not in df.columns:
            missing.append(name)
            continue
        partial += 0.5  # column present
        actual = str(df[name].dtype)
        expected_key = _DTYPE_CHECKS.get(str(expected_dtype).lower())
        if expected_key is None:
            logger.warning("schema check: unknown expected dtype",
                           extra={"column": name, "expected_dtype": expected_dtype})
            continue
        ok = (
            (expected_key == "int" and pd.api.types.is_integer_dtype(df[name]))
            or (expected_key == "float" and pd.api.types.is_float_dtype(df[name]))
            or (expected_key == "str" and df[name].dtype == object)
            or (expected_key == "bool" and pd.api.types.is_bool_dtype(df[name]))
            or (expected_key == "datetime" and pd.api.types.is_datetime64_any_dtype(df[name]))
        )
        if ok:
            partial += 0.5
        else:
            wrong_type.append(f"{name}: expected {expected_dtype}, got {actual}")
    score = round(partial / len(expected) * 100, 2)
    passed = not missing and not wrong_type
    samples = (missing + wrong_type)[:5]
    detail = (
        f"{len(expected) - len(missing) - len(wrong_type)}/{len(expected)} columns match"
        + (f"; missing={missing}" if missing else "")
        + (f"; wrong_type={wrong_type}" if wrong_type else "")
    )
    return CheckResult("schema", None, passed, score, detail, samples)


def check_referential(df: pd.DataFrame, exp: ExpectationSpec) -> CheckResult:
    """Pass when every non-null value in ``column`` exists in the first
    column of the CSV at ``reference_path``. Used for foreign-key integrity
    checks (e.g. ``orders.customer_id`` must exist in ``customers.csv``)."""
    col = exp.column
    if not col or col not in df.columns:
        return CheckResult("referential", col, False, 0.0, f"column '{col}' missing")
    if not exp.reference_path:
        return CheckResult("referential", col, False, 0.0,
                           "reference_path not configured")
    ref_path = Path(exp.reference_path)
    if not ref_path.exists():
        return CheckResult("referential", col, False, 0.0,
                           f"reference file not found: {ref_path}")
    try:
        ref_df = pd.read_csv(ref_path)
        valid_set = set(ref_df.iloc[:, 0].dropna().tolist())
    except Exception as exc:  # noqa: BLE001
        logger.error("referential check failed to load reference",
                     extra={"reference_path": str(ref_path), "error": str(exc)})
        return CheckResult("referential", col, False, 0.0,
                           f"failed to load reference: {exc}")
    s = df[col]
    non_null = s.dropna()
    if len(non_null) == 0:
        return CheckResult("referential", col, False, 0.0,
                           f"column '{col}' has no non-null values")
    matches = non_null.isin(valid_set)
    ratio = float(matches.mean())
    samples = non_null[~matches].head(5).tolist()
    return CheckResult("referential", col, ratio >= 0.99, round(ratio * 100, 2),
                       f"{ratio:.1%} of '{col}' values exist in reference "
                       f"({len(valid_set)} keys)", samples)


CHECKERS: dict[str, Any] = {
    "completeness": check_completeness,
    "consistency": check_consistency,
    "uniqueness": check_uniqueness,
    "range": check_range,
    "volume": check_volume,
    "freshness": check_freshness,
    "schema": check_schema,
    "referential": check_referential,
}


def evaluate(df: pd.DataFrame, expectations: list[ExpectationSpec]) -> tuple[list[CheckResult], float]:
    """Run every expectation against ``df`` and return (results, weighted_score).

    Unknown check types raise ``KeyError`` (caught and surfaced by the engine
    as a :class:`QualityGateError`).
    """
    results: list[CheckResult] = []
    total_w, weighted = 0.0, 0.0
    for exp in expectations:
        fn = CHECKERS.get(exp.check)
        if fn is None:
            raise KeyError(f"unknown quality check: {exp.check!r}")
        result = fn(df, exp)
        results.append(result)
        total_w += exp.weight
        weighted += result.score * exp.weight
    score = round(weighted / total_w, 2) if total_w else 100.0
    return results, score
