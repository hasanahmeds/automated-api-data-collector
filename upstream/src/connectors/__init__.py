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
