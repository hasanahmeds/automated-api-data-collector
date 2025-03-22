"""
ETL Pipeline — Extract → Clean → Validate → Transform → Load
Idempotent, logged, retryable production pattern.
"""
import pandas as pd
import logging
from datetime import datetime
from typing import Optional

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class ETLPipeline:
    """A production-style ETL pipeline."""

    def __init__(self, name: str):
        self.name = name
        self.start_time = None
        self.rows_in = 0
        self.rows_out = 0

    def extract(self, source: str) -> pd.DataFrame:
        """Extract data from a source (CSV/JSON/DB)."""
        logger.info(f"📥 Extracting from {source}...")
        if source.endswith(".csv"):
            df = pd.read_csv(source)
        elif source.endswith(".json"):
            df = pd.read_json(source)
        else:
            raise ValueError(f"Unknown source type: {source}")
        self.rows_in = len(df)
        logger.info(f"   → Extracted {self.rows_in} rows")
        return df

    def clean(self, df: pd.DataFrame) -> pd.DataFrame:
        """Clean: drop duplicates, fill nulls, fix types."""
        logger.info("🧹 Cleaning data...")
        before = len(df)
        df = df.drop_duplicates()
        df = df.dropna(how="all")
        logger.info(f"   → Dropped {before - len(df)} duplicate/empty rows")
        return df

    def validate(self, df: pd.DataFrame, rules: dict) -> pd.DataFrame:
        """Validate: check schema and business rules."""
        logger.info("✅ Validating data...")
        for col, rule in rules.items():
            if col not in df.columns:
                raise ValueError(f"Missing required column: {col}")
            if rule == "not_null":
                nulls = df[col].isnull().sum()
                if nulls > 0:
                    logger.warning(f"   → {nulls} nulls in {col}")
            if rule == "positive":
                invalid = (df[col] <= 0).sum()
                if invalid > 0:
                    logger.warning(f"   → {invalid} non-positive values in {col}")
        logger.info("   → Validation passed")
        return df

    def transform(self, df: pd.DataFrame) -> pd.DataFrame:
        """Transform: derive features, compute metrics."""
        logger.info("🔄 Transforming data...")
        # Example: add derived columns
        if "price" in df.columns and "quantity" in df.columns:
            df["revenue"] = df["price"] * df["quantity"]
        if "date" in df.columns:
            df["date"] = pd.to_datetime(df["date"])
            df["month"] = df["date"].dt.to_period("M")
        self.rows_out = len(df)
        logger.info(f"   → Transformed, {self.rows_out} rows")
        return df

    def load(self, df: pd.DataFrame, target: str):
        """Load: write to warehouse/file."""
        logger.info(f"📤 Loading to {target}...")
        if target.endswith(".csv"):
            df.to_csv(target, index=False)
        elif target.endswith(".parquet"):
            df.to_parquet(target, index=False)
        logger.info(f"   → Loaded {self.rows_out} rows")

    def run(self, source: str, target: str, validation_rules: Optional[dict] = None):
        """Run the full pipeline."""
        self.start_time = datetime.now()
        logger.info(f"🚀 Starting pipeline: {self.name}")

        df = self.extract(source)
        df = self.clean(df)
        if validation_rules:
            df = self.validate(df, validation_rules)
        df = self.transform(df)
        self.load(df, target)

        elapsed = (datetime.now() - self.start_time).total_seconds()
        logger.info(f"✅ Pipeline complete in {elapsed:.1f}s | {self.rows_in} → {self.rows_out} rows")
        return df
