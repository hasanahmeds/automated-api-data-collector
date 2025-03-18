"""Connectors: read data from CSV / JSON / HTTP API / SQL into DataFrames.

Each connector implements the :class:`Connector` protocol — a single ``read``
method that returns a populated ``DataFrame``. Connectors are constructed by
:func:`build_connector` from a :class:`~src.config_loader.SourceSpec` so the
engine never branches on source type.

Failures (file missing, HTTP 5xx, SQL errors) are wrapped in
:class:`~src.exceptions.SourceError` so the engine can attach context.
"""
from __future__ import annotations

from pathlib import Path
from typing import Any, Protocol

import pandas as pd

from src.exceptions import SourceError
from src.logging_config import get_logger
from src.config_loader import SourceSpec

logger = get_logger("src.connectors", component="connectors")


class Connector(Protocol):
    """Minimal interface every connector must implement."""

    def read(self) -> pd.DataFrame: ...


class CsvConnector:
    """Read a CSV file into a DataFrame."""

    def __init__(self, path: str, encoding: str = "utf-8", delimiter: str = ",") -> None:
        self.path = path
        self.encoding = encoding
        self.delimiter = delimiter

    def read(self) -> pd.DataFrame:
        if not Path(self.path).exists():
            raise SourceError(f"CSV file not found: {self.path}",
                              context={"path": self.path, "type": "csv"})
        try:
            df = pd.read_csv(self.path, encoding=self.encoding, delimiter=self.delimiter)
        except Exception as exc:  # noqa: BLE001
            raise SourceError(
                f"failed to read CSV: {exc}",
                context={"path": self.path, "encoding": self.encoding},
            ) from exc
        logger.info("csv read", extra={"path": self.path, "rows": len(df)})
        return df


class JsonConnector:
    """Read a JSON file into a DataFrame."""

    def __init__(self, path: str, orient: str = "records") -> None:
        self.path = path
        self.orient = orient

    def read(self) -> pd.DataFrame:
        if not Path(self.path).exists():
            raise SourceError(f"JSON file not found: {self.path}",
                              context={"path": self.path, "type": "json"})
        try:
            df = pd.read_json(self.path, orient=self.orient)
        except Exception as exc:  # noqa: BLE001
            raise SourceError(
                f"failed to read JSON: {exc}",
                context={"path": self.path, "orient": self.orient},
            ) from exc
        logger.info("json read", extra={"path": self.path, "rows": len(df)})
        return df


class ApiConnector:
    """REST API reader with optional simple pagination (``page`` parameter)."""

    def __init__(
        self,
        url: str,
        method: str = "GET",
        params: dict[str, Any] | None = None,
        headers: dict[str, Any] | None = None,
        pagination: bool = False,
        page_size: int = 1000,
    ) -> None:
        self.url = url
        self.method = method
        self.params = params or {}
        self.headers = headers or {}
        self.pagination = pagination
        self.page_size = page_size

    def read(self) -> pd.DataFrame:
        import httpx

        rows: list[dict[str, Any]] = []
        params = dict(self.params)
        page = 1
        try:
            while True:
                if self.pagination:
                    params["page"] = page
                    params["page_size"] = self.page_size
                resp = httpx.request(
                    self.method,
                    self.url,
                    params=params,
                    headers=self.headers,
                    timeout=30.0,
                )
                resp.raise_for_status()
                data = resp.json()
                batch = (
                    data.get("data")
                    or data.get("results")
                    or (data if isinstance(data, list) else [])
                )
                rows.extend(batch)
                if not self.pagination or len(batch) < self.page_size:
                    break
                page += 1
        except Exception as exc:  # noqa: BLE001
            raise SourceError(
                f"API request failed: {exc}",
                context={"url": self.url, "method": self.method, "page": page},
            ) from exc
        logger.info("api read",
                    extra={"url": self.url, "rows": len(rows),
                           "pages": page if self.pagination else 1})
        return pd.DataFrame(rows)


class SqlConnector:
    """Read the result of a SQL query into a DataFrame."""

    def __init__(self, dsn: str, query: str) -> None:
        self.dsn = dsn
        self.query = query

    def read(self) -> pd.DataFrame:
        import sqlalchemy as sa

        try:
            engine = sa.create_engine(self.dsn)
            with engine.connect() as conn:
                df = pd.read_sql_query(self.query, conn)
        except Exception as exc:  # noqa: BLE001
            raise SourceError(
                f"SQL query failed: {exc}",
                context={"dsn": self.dsn, "query": self.query[:200]},
            ) from exc
        logger.info("sql read",
                    extra={"dsn": self.dsn, "rows": len(df),
                           "query_prefix": self.query[:80]})
        return df


def build_connector(spec: SourceSpec) -> Connector:
    """Factory mapping a :class:`SourceSpec` to the right connector class."""
    logger.info("building connector", extra={"source_type": spec.type})
    if spec.type == "csv":
        return CsvConnector(spec.path or "", encoding=spec.encoding, delimiter=spec.delimiter)
    if spec.type == "json":
        return JsonConnector(spec.path or "", orient=spec.json_orient)
    if spec.type == "api":
        return ApiConnector(
            spec.url or "",
            method=spec.method,
            params=spec.params,
            headers=spec.headers,
            pagination=spec.pagination,
            page_size=spec.page_size,
        )
    if spec.type == "sql":
        return SqlConnector(spec.dsn or "", spec.query or "")
    raise SourceError(f"unsupported source type: {spec.type}",
                      context={"source_type": spec.type})
