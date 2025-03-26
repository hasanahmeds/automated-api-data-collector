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
        out[cols] = out[cols].fillna(rule.value)
        detail = f"fill {cols} with {rule.value!r}"
    else:
        cols = _cols(rule) or list(df.columns)
        out[cols] = out[cols].ffill()
        detail = f"forward-fill {cols}"
    filled = int(df.isna().sum().sum() - out.isna().sum().sum())
    logger.info("rule=fill_null", extra={"filled": filled, "detail": detail})
    return out, RuleEvent("fill_null", detail, len(df), len(out), filled)


def apply_normalize(df: pd.DataFrame, rule: RuleSpec) -> tuple[pd.DataFrame, RuleEvent]:
    out = df.copy()
    cols = _cols(rule) or list(df.columns)
    ops: list[str] = []
    fmt = rule.format or ""
    for c in cols:
        if "lower" in fmt:
            out[c] = out[c].astype(str).str.lower()
            ops.append("lower")
        elif "upper" in fmt:
            out[c] = out[c].astype(str).str.upper()
            ops.append("upper")
        if "strip" in fmt:
            out[c] = out[c].astype(str).str.strip()
            ops.append("strip")
        if "date" in fmt:
            out[c] = pd.to_datetime(out[c], errors="coerce")
            ops.append("date")
    logger.info("rule=normalize", extra={"cols": cols, "ops": ops})
    return out, RuleEvent("normalize",
                          f"normalize {cols} [{', '.join(ops) or 'strip'}]",
                          len(df), len(out), len(df))


def apply_coerce(df: pd.DataFrame, rule: RuleSpec) -> tuple[pd.DataFrame, RuleEvent]:
    out = df.copy()
    cols = _cols(rule) or list(df.columns)
    target = rule.type or "str"
    affected = 0
    try:
        for c in cols:
            before = out[c].count()
            if target == "int":
                out[c] = pd.to_numeric(out[c], errors="coerce").astype("Int64")
            elif target == "float":
                out[c] = pd.to_numeric(out[c], errors="coerce")
            elif target == "date":
                out[c] = pd.to_datetime(out[c], errors="coerce")
            elif target == "str":
                out[c] = out[c].astype(str)
            else:
                raise TransformError(f"unknown coerce type: {target}",
                                     context={"column": c, "type": target})
            affected += int(before - out[c].count())
    except TransformError:
        raise
    except Exception as exc:  # noqa: BLE001
        raise TransformError(f"coerce failed: {exc}",
                             context={"columns": cols, "type": target}) from exc
    logger.info("rule=coerce", extra={"cols": cols, "type": target, "affected": affected})
    return out, RuleEvent("coerce", f"coerce {cols} to {target}",
                          len(df), len(out), affected)


def apply_map(df: pd.DataFrame, rule: RuleSpec) -> tuple[pd.DataFrame, RuleEvent]:
    out = df.copy()
    cols = _cols(rule)
    mapping = rule.values
    before = len(df)
    for c in cols:
        out[c] = out[c].map(mapping).fillna(out[c])
    logger.info("rule=map", extra={"cols": cols, "mapping_entries": len(mapping)})
    return out, RuleEvent("map", f"map {cols} via {len(mapping)} entries",
                          before, len(out), 0)


def apply_filter(df: pd.DataFrame, rule: RuleSpec) -> tuple[pd.DataFrame, RuleEvent]:
    before = len(df)
    try:
        out = df.query(rule.condition or "True")
    except Exception as exc:  # noqa: BLE001
        raise TransformError(f"filter failed: {exc}",
                             context={"condition": rule.condition}) from exc
    logger.info("rule=filter", extra={"condition": rule.condition,
                                      "in": before, "out": len(out)})
    return out, RuleEvent("filter", f"keep rows where {rule.condition}",
                          before, len(out), before - len(out))


def apply_validate(df: pd.DataFrame, rule: RuleSpec) -> tuple[pd.DataFrame, RuleEvent]:
    """Validate values against a regex or allowed set; invalid -> NaN."""
    out = df.copy()
    cols = _cols(rule)
    affected = 0
    for c in cols:
        valid = pd.Series(True, index=df.index)
        if rule.pattern:
            valid = valid & out[c].astype(str).str.match(rule.pattern, na=False)
        if rule.allowed is not None:
            valid = valid & out[c].isin(rule.allowed)
        affected += int((~valid).sum())
        out.loc[~valid, c] = None
    logger.info("rule=validate", extra={"cols": cols, "affected": affected,
                                        "pattern": rule.pattern})
    return out, RuleEvent("validate", f"validate {cols} (pattern/allowed)",
                          len(df), len(out), affected)


_HANDLERS: dict[str, Any] = {
    "dedup": apply_dedup,
    "drop_null": apply_drop_null,
    "fill_null": apply_fill_null,
    "normalize": apply_normalize,
    "coerce": apply_coerce,
    "map": apply_map,
    "filter": apply_filter,
    "validate": apply_validate,
}


def apply_rule(df: pd.DataFrame, rule: RuleSpec) -> tuple[pd.DataFrame, RuleEvent]:
    fn = _HANDLERS.get(rule.rule)
    if fn is None:
        raise TransformError(f"unknown rule: {rule.rule}",
                             context={"rule": rule.rule})
    return fn(df, rule)


def apply_all(df: pd.DataFrame, rules: list[RuleSpec]) -> tuple[pd.DataFrame, list[RuleEvent]]:
    """Apply each rule in order; collect events for lineage."""
    events: list[RuleEvent] = []
    for idx, rule in enumerate(rules):
        df, event = apply_rule(df, rule)
        events.append(event)
    return df, events
