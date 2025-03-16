"""Configuration loading: YAML pipeline specs -> validated pydantic models.

Pipelines are *declared*, not coded. Adding a new pipeline = adding a YAML
file: source, cleaning rules, quality expectations, sink. No code changes.
"""
from __future__ import annotations

from pathlib import Path
from typing import Any, Literal

import yaml
from pydantic import BaseModel, Field

SourceType = Literal["csv", "json", "api", "sql"]
SinkType = Literal["sqlite", "postgres", "csv", "parquet"]


class SourceSpec(BaseModel):
    type: SourceType
    path: str | None = None            # csv / json
    url: str | None = None             # api
    method: str = "GET"                # api
    params: dict[str, Any] = {}        # api
    headers: dict[str, Any] = {}       # api
    pagination: bool = False           # api: auto-follow next page
    page_size: int = 1000              # api
    dsn: str | None = None             # sql
    query: str | None = None           # sql
    encoding: str = "utf-8"            # csv/json
    delimiter: str = ","               # csv
    json_orient: str = "records"       # json


class RuleSpec(BaseModel):
    rule: Literal["dedup", "drop_null", "fill_null", "normalize", "coerce", "map", "filter", "validate"]
    columns: list[str] | str = []
    keep: Literal["first", "last"] = "first"
    value: Any = None
    values: dict[str, Any] = {}
    type: str | None = None            # coerce target type
    format: str | None = None          # normalize/coerce format hint
    condition: str | None = None       # filter expression (pandas query syntax)
    pattern: str | None = None         # validate regex
    allowed: list[Any] | None = None   # validate allowed values


class ExpectationSpec(BaseModel):
    check: Literal[
        "completeness",
        "consistency",
        "uniqueness",
        "range",
        "volume",
        "freshness",
        "schema",
        "referential",
    ]
    column: str | None = None
    min: float | None = None
    max: float | None = None
    pattern: str | None = None
    values: list[Any] | None = None
    min_rows: int | None = None
    weight: float = 1.0
    # freshness: max acceptable age of the newest record, in hours.
    max_age_hours: float | None = None
    # schema: map of column name -> expected dtype string
    #   ("int", "float", "str", "bool", "date").
    schema: dict[str, str] | None = None
    # referential: path to a CSV whose first column holds the allowed FK
    # values. ``column`` is the FK column in the dataset being checked.
    reference_path: str | None = None


class QualitySpec(BaseModel):
    expectations: list[ExpectationSpec] = []
    min_score: float | None = None
    abort_on_fail: bool = True


class SinkSpec(BaseModel):
    type: SinkType
    table: str | None = None
    path: str | None = None            # csv / parquet sink
    database_url: str | None = None    # overrides default
    primary_key: list[str] | None = None   # upsert key; None => append + run_id dedupe


class PipelineSpec(BaseModel):
    name: str
    description: str = ""
    source: SourceSpec
    transforms: list[RuleSpec] = Field(default_factory=list)
    quality: QualitySpec = Field(default_factory=QualitySpec)
    sink: SinkSpec
    schedule: str | None = None        # "every 30s" | "@daily" | "0 9 * * *"


def load_pipeline(path: str | Path) -> PipelineSpec:
    data = yaml.safe_load(Path(path).read_text(encoding="utf-8"))
    return PipelineSpec.model_validate(data)


def load_all_pipelines(directory: str | Path) -> dict[str, PipelineSpec]:
    specs: dict[str, PipelineSpec] = {}
    for path in sorted(Path(directory).glob("*.yaml")):
        spec = load_pipeline(path)
        specs[spec.name] = spec
    return specs
