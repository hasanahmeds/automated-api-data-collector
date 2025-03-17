# 🔄 Data Pipeline ETL — Config-driven ETL & Data Governance

**Production-grade ETL pipeline: extract data from sources → clean and
transform → validate against quality gates → load into a warehouse.** A
maintained, monitored, idempotent pipeline — not a notebook.

> Pipelines are *declared*, not coded. Add a YAML file: source, cleaning
> rules, quality expectations, sink. No code changes.

---

## 🚀 What It Does

Raw data (CSV, JSON, REST API, SQL) → rule-based cleaning → Great-Expectations-
style quality gate with a weighted score → idempotent upsert (PostgreSQL/SQLite)
or warehouse export (CSV/Parquet). Every run writes lineage, a quality report
(JSON + self-contained HTML), and is observable through a REST API and
Prometheus-style metrics.

### Highlights

- **Multi-source ingestion** — CSV, JSON, REST API (with pagination), SQL.
- **Rule-based cleaning** — dedup, drop_null, fill_null, normalize, coerce,
  map, filter, validate — all driven by YAML.
- **Quality gates** — completeness, consistency, uniqueness, range, volume,
  **freshness**, **schema**, and **referential integrity** checks with a
  weighted score and an abort threshold.
- **Idempotent loading** — content fingerprinting skips unchanged runs; primary
  keys trigger true `ON CONFLICT DO UPDATE` upserts.
- **Lineage** — every run persists a JSON lineage file (source → transforms →
  quality → sink → timing).
- **Structured logging** — single-line structured records with
  `pipeline_name`, `step`, `run_id`, `duration_ms`, etc. — grep-friendly in
  dev, Loki/ELK-ready in prod (JSON toggle).
- **Typed exceptions** — `PipelineError`, `SourceError`, `TransformError`,
  `QualityGateError`, `SinkError`, `ConfigValidationError` carry context.
- **Metrics collector** — in-process counters (`rows_processed`,
  `rows_rejected`, `quality_score`, `pipeline_duration`) plus durable
  per-run records in the `pipeline_runs` marker table.
- **REST API** — list pipelines, run, view run history, view lineage, dry-run
  config validation, and scrape Prometheus metrics.
- **Orchestration** — built-in scheduler and a ready-to-use Airflow DAG.

---

## 🧠 Why ETL Matters

Every data team spends 80% of their time cleaning data. A proper ETL pipeline:

- 🧹 **Cleans** — removes duplicates, handles nulls, fixes types.
- ✅ **Validates** — checks constraints (no negative ages, email format, FK
  integrity, freshness SLA, schema invariants).
- 🔄 **Transforms** — derives features, normalises, maps, filters.
- 📊 **Loads** — writes to the warehouse, logs row counts, alerts on failure.

---

## ⚙️ Architecture

```
📥 Extract          🧹 Clean & Transform         ✅ Quality Gate        📤 Load
┌──────────┐       ┌──────────────────┐       ┌─────────────────┐    ┌──────────┐
│ CSV files │──→   │ dedup / drop_null │  ─→   │ completeness    │ ─→ │ PostgreSQL│
│ REST API  │──→   │ fill / normalize  │       │ uniqueness      │    │ SQLite    │
│ JSON      │──→   │ coerce / map      │       │ range / volume  │    │ CSV       │
│ SQL       │──→   │ filter / validate │       │ freshness       │    │ Parquet   │
└──────────┘       └──────────────────┘       │ schema          │    └──────────┘
                                                │ referential     │
                                                └─────────────────┘
                                                       │
                                        ━━━━━━━━━━━━━━━┿━━━━━━━━━━━━━━━
                                                       ▼
                            📊 lineage + quality report (JSON/HTML)
                            📈 metrics /api/metrics  ·  🗂  pipeline_runs marker table
```

---

## 📁 Project Structure

```
data-pipeline-etl/
├── src/
│   ├── __init__.py
│   ├── cli.py                # CLI: run / runs / lineage / serve / schedule
│   ├── config.py             # Settings (env-driven, .env-aware)
│   ├── config_loader.py      # YAML → pydantic models (SourceSpec, RuleSpec, ...)
│   ├── engine.py             # ETL runtime + MetricsCollector + idempotency
│   ├── exceptions.py         # PipelineError / SourceError / TransformError / ...
│   ├── logging_config.py    # Structured logging (text or JSON formatter)
│   ├── pipeline.py           # Standalone reference ETL (teaching sample)
│   ├── connectors/           # CSV / JSON / REST API / SQL readers
│   ├── transforms/           # Rule-based cleaning (dedup, coerce, ...)
│   ├── quality/              # Quality checks + scored gate + HTML report
│   ├── storage/               # Idempotent DB upsert + file warehouse
│   ├── lineage/               # Per-run lineage tracking
│   ├── api/                   # FastAPI control & monitoring surface
│   └── orchestration/         # Built-in scheduler + Airflow DAG
├── config/pipelines/          # YAML pipeline specs
├── config/quality/            # Reusable expectations library
├── tests/                     # pytest end-to-end + idempotency + rules
├── scripts/demo.py            # Run both sample pipelines end-to-end
├── pyproject.toml             # Build + ruff + black + mypy + coverage
└── README.md
```

---

## 🎯 Pipeline Stages

| Stage | What it does |
|-------|-------------|
| 📥 Extract | Read CSV/JSON/API/SQL into a DataFrame via a typed connector. |
| 🧹 Clean | Apply declared rules: dedup, drop_null, fill_null, normalize, coerce, map, filter, validate. |
| ✅ Quality Gate | Score the DataFrame on weighted expectations; abort before load if below threshold. |
| 📤 Load | Idempotent upsert (DB) or warehouse export (CSV/Parquet). |
| 📊 Report | Self-contained HTML + JSON quality report. |
| 📈 Lineage | JSON lineage file per run: every step with rows in/out + duration. |

---

## ✅ Quality Checks

| Check | Purpose |
|-------|---------|
| `completeness` | Non-null rate of a column ≥ threshold (`min`). |
| `consistency` | Values match a regex (`pattern`) and/or allowed set (`values`). |
| `uniqueness` | No duplicates on the key column(s). |
| `range` | Numeric values within `[min, max]`. |
| `volume` | Row count ≥ `min_rows`. |
| `freshness` | Newest timestamp in `column` is within `max_age_hours` of now (SLA). |
| `schema` | Required columns are present with the expected dtype (`schema: {col: dtype}`). |
| `referential` | Every non-null value in `column` exists in the first column of `reference_path` CSV (FK integrity). |

Each expectation has a `weight` (default `1.0`); the pipeline score is the
weighted average, gated against `quality.min_score` (or the global
`DEFAULT_QUALITY_THRESHOLD`).

---

## 🚀 Quickstart

```bash
# 1. Install (dev extras pull in ruff / mypy / black / pytest)
pip install -e ".[dev]"

# 2. Run the demo
python scripts/demo.py

# 3. Run a single pipeline via the CLI
python -m src.cli run --pipeline example_orders

# 4. Inspect run history
python -m src.cli runs --pipeline example_orders

# 5. Serve the API + scrape metrics
python -m src.cli serve
curl http://127.0.0.1:8000/api/metrics
```

### Environment

Copy `.env.example` to `.env` and adjust. Defaults (SQLite, local files) work
out of the box.

| Variable | Default | Description |
|----------|---------|-------------|
| `DATABASE_URL` | `sqlite:///data/pipeline.db` | Default sink DB (per-pipeline YAML can override). |
| `PIPELINES_DIR` | `config/pipelines` | Where pipeline YAMLs live. |
| `QUALITY_DIR` | `config/quality` | Reusable expectations library. |
| `DEFAULT_QUALITY_THRESHOLD` | `80` | Quality gate threshold when not set per-pipeline. |
| `SCHEDULE_ENABLED` | `true` | Whether the built-in scheduler runs. |

---

## 📡 REST API

| Method | Endpoint | Purpose |
|--------|----------|---------|
| `GET`  | `/api/health` | Health check. |
| `GET`  | `/api/pipelines` | Catalogue of declared pipelines. |
| `POST` | `/api/pipelines/{name}/run?force=true` | Run a pipeline (idempotent by default; `force` re-runs). |
| `GET`  | `/api/pipelines/{name}/runs` | Run history for one pipeline. |
| `GET`  | `/api/pipelines/{name}/lineage` | Most-recent lineage record for a pipeline. |
| `POST` | `/api/pipelines/{name}/validate` | Dry-run config validation (no IO). |
| `GET`  | `/api/runs` | Global run history. |
| `GET`  | `/api/runs/{run_id}/lineage` | Lineage for a specific run. |
| `GET`  | `/api/metrics` | Prometheus text format + in-memory counters. |
| `GET`  | `/api/metrics/json` | Structured metrics for dashboards. |

---

## 📊 Example Pipeline (YAML)

```yaml
name: example_orders
description: Orders from a CSV, deduplicated, cleaned and loaded with a quality gate.

source:
  type: csv
  path: data/sample/orders.csv

transforms:
  - rule: dedup
    columns: [order_id]
    keep: first
  - rule: drop_null
    columns: [email]
  - rule: normalize
    columns: [order_date]
    format: date
  - rule: coerce
    columns: [amount]
    type: float
  - rule: validate
    columns: [email]
    pattern: '^[^@\s]+@[^@\s]+\.[^@\s]+$'
  - rule: filter
    condition: 'amount >= 0'

quality:
  expectations:
    - check: completeness
      column: email
      min: 0.95
    - check: consistency
      column: email
      pattern: '^[^@\s]+@[^@\s]+\.[^@\s]+$'
    - check: uniqueness
      column: order_id
    - check: range
      column: amount
      min: 0
    - check: volume
      min_rows: 5
    - check: freshness      # SLA: data must be < 48h old
      column: order_date
      max_age_hours: 48
    - check: schema          # columns must be present with correct types
      schema:
        order_id: int
        email: str
        amount: float
        order_date: date
    - check: referential     # FK: every customer_id must exist in customers.csv
      column: customer_id
      reference_path: data/sample/customers.csv
  min_score: 80
  abort_on_fail: true

sink:
  type: sqlite
  table: orders
  database_url: sqlite:///data/pipeline.db
  primary_key: [order_id]

schedule: every 60s
```

---

## 🛠 Development

```bash
pip install -e ".[dev]"

# Lint
ruff check src tests
# Format
black src tests
# Type-check
mypy src
# Test with coverage
pytest
```

CI (`.github/workflows/ci.yml`) runs the test suite and the demo pipeline on
Python 3.11 and 3.12.

---

## 📜 License

MIT — see `LICENSE`.
