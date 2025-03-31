"""Tests for quality expectations and scoring."""
from __future__ import annotations

import pandas as pd

from src.config_loader import ExpectationSpec
from src.quality import evaluate


def test_completeness_pass_and_fail():
    df = pd.DataFrame({"email": ["a@x.com", None, "b@x.com"]})
    checks, score = evaluate(df, [ExpectationSpec(check="completeness", column="email", min=0.95)])
    assert not checks[0].passed
    assert score < 95


def test_consistency_regex():
    df = pd.DataFrame({"email": ["a@x.com", "bad", "c@x.com"]})
    checks, score = evaluate(df, [ExpectationSpec(check="consistency", column="email", pattern=r"^[^@\s]+@[^@\s]+\.[^@\s]+$")])
    assert not checks[0].passed
    assert "bad" in checks[0].samples


def test_uniqueness_detects_duplicates():
    df = pd.DataFrame({"id": [1, 1, 2]})
    checks, _ = evaluate(df, [ExpectationSpec(check="uniqueness", column="id")])
    assert not checks[0].passed


def test_range_bounds():
    df = pd.DataFrame({"amount": [-5, 10, 100]})
    checks, _ = evaluate(df, [ExpectationSpec(check="range", column="amount", min=0)])
    assert not checks[0].passed


def test_volume():
    df = pd.DataFrame({"a": [1]})
    checks, _ = evaluate(df, [ExpectationSpec(check="volume", min_rows=10)])
    assert not checks[0].passed


def test_weighted_score():
    df = pd.DataFrame({"a": [1, 2, 3]})
    checks, score = evaluate(df, [
        ExpectationSpec(check="volume", min_rows=1, weight=1.0),
        ExpectationSpec(check="uniqueness", column="a", weight=3.0),
    ])
    assert score == 100.0
