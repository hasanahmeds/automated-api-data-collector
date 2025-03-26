"""Rule-based data cleaning.

Every rule is declared in YAML and executed by a small, explicit engine
(pandas). Each rule returns the transformed DataFrame plus a :class:`RuleEvent`
describing what happened (rows in/out, affected) — which feeds the lineage
tracker.

Available rules:
    * ``dedup``      — drop duplicates on a key subset (``keep`` first/last).
    * ``drop_null``  — drop rows with nulls in given columns (or any column).
    * ``fill_null``  — fill nulls per-column (``values``), with a scalar
                       (``value``), or forward-fill.
    * ``normalize``  — apply string/date transforms declared in ``format``
                       (``lower``, ``upper``, ``strip``, ``date``).
    * ``coerce``     — cast columns to ``int``/``float``/``date``/``str``.
    * ``map``        — apply a value mapping (``values``).
    * ``filter``     — keep rows matching a pandas query expression.
    * ``validate``   — invalidate (set to null) values that fail a regex or
                       are not in the ``allowed`` set.
"""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

import pandas as pd

from src.config_loader import RuleSpec
from src.exceptions import TransformError
from src.logging_config import get_logger

logger = get_logger("src.transforms", component="transforms")


@dataclass
class RuleEvent:
    rule: str
    detail: str
    rows_in: int
    rows_out: int
    affected: int
    meta: dict[str, Any] = field(default_factory=dict)


def _cols(rule: RuleSpec) -> list[str]:
    cols = rule.columns
    return [cols] if isinstance(cols, str) else list(cols)


def apply_dedup(df: pd.DataFrame, rule: RuleSpec) -> tuple[pd.DataFrame, RuleEvent]:
    cols = _cols(rule) or list(df.columns)
    before = len(df)
    keep = rule.keep or "first"
    out = df.drop_duplicates(subset=cols, keep=keep)
    logger.info("rule=dedup", extra={"cols": cols, "keep": keep,
                                     "in": before, "out": len(out)})
    return out, RuleEvent("dedup", f"dedup on {cols} (keep={keep})",
                          before, len(out), before - len(out))


def apply_drop_null(df: pd.DataFrame, rule: RuleSpec) -> tuple[pd.DataFrame, RuleEvent]:
    cols = _cols(rule)
    before = len(df)
    out = df.dropna(subset=cols) if cols else df.dropna()
    logger.info("rule=drop_null", extra={"cols": cols or "any",
                                         "in": before, "out": len(out)})
    return out, RuleEvent(
        "drop_null",
        f"drop rows with nulls in {cols or 'any column'}",
        before, len(out), before - len(out),
    )


def apply_fill_null(df: pd.DataFrame, rule: RuleSpec) -> tuple[pd.DataFrame, RuleEvent]:
    out = df.copy()
    if isinstance(rule.values, dict) and rule.values:
        out = out.fillna(rule.values)
        detail = f"fill per-column: {list(rule.values)}"
    elif rule.value is not None:
        cols = _cols(rule) or list(df.columns)
