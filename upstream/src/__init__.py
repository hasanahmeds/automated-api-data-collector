"""data-pipeline-etl — config-driven ETL & data-governance pipelines.

Multi-source ingestion (CSV/JSON/API/SQL) -> rule-based cleaning ->
Great-Expectations-style quality checks with a scored gate -> idempotent
loading (PostgreSQL/SQLite/warehouse) -> REST API + lineage + quality reports.
"""

__version__ = "1.0.0"
