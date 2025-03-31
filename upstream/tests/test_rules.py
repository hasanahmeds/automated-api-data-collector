"""Tests for cleaning rules."""
from __future__ import annotations

import pandas as pd

from src.config_loader import RuleSpec
from src.transforms import apply_all


def _df():
    return pd.DataFrame({
        "id": [1, 1, 2, 3, 4],
        "email": ["a@x.com", "a@x.com", None, "bad", "c@x.com"],
        "amount": ["10.5", "10.5", "0", "-3", "7"],
    })


def test_dedup():
    df = _df()
    out, events = apply_all(df, [RuleSpec(rule="dedup", columns=["id"])])
    assert len(out) == 4
    assert events[0].rule == "dedup"
    assert events[0].affected == 1


def test_drop_null():
    df = _df()
    out, _ = apply_all(df, [RuleSpec(rule="drop_null", columns=["email"])])
    assert len(out) == 4  # only the NaN email row is dropped ('bad' is a string)
    assert out["email"].isna().sum() == 0


def test_fill_null_constant():
    df = _df()
    out, _ = apply_all(df, [RuleSpec(rule="fill_null", columns=["email"], value="unknown@x.com")])
    assert out["email"].isna().sum() == 0
    assert "unknown@x.com" in out["email"].values


def test_coerce_float():
    df = _df()
    out, _ = apply_all(df, [RuleSpec(rule="coerce", columns=["amount"], type="float")])
    assert out["amount"].dtype == float


def test_validate_pattern_sets_invalid_to_null():
    df = _df()
    out, events = apply_all(df, [RuleSpec(rule="validate", columns=["email"], pattern=r"^[^@\s]+@[^@\s]+\.[^@\s]+$")])
    assert out["email"].isna().sum() == 2  # original NaN + invalidated 'bad'
    assert events[0].affected == 2  # NaN stringifies to 'nan' and fails the regex


def test_filter_keeps_condition():
    df = _df()
    out, _ = apply_all(df, [
        RuleSpec(rule="coerce", columns=["amount"], type="float"),
        RuleSpec(rule="filter", condition="amount >= 0"),
    ])
    assert (pd.to_numeric(out["amount"]) >= 0).all()


def test_map_values():
    df = pd.DataFrame({"country": ["US", "GB", "ZZ"]})
    out, _ = apply_all(df, [RuleSpec(rule="map", columns=["country"], values={"ZZ": "XX"})])
    assert out.loc[2, "country"] == "XX"


def test_unknown_rule_raises():
    import pytest

    with pytest.raises(ValueError):
        apply_all(_df(), [RuleSpec(rule="explode")])
