"""Application configuration."""
from functools import lru_cache
from pathlib import Path

from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", env_file_encoding="utf-8", extra="ignore")

    app_name: str = "data-pipeline-etl"
    api_prefix: str = "/api"

    # Where pipeline YAML configs live
    pipelines_dir: Path = Path("config/pipelines")
    quality_dir: Path = Path("config/quality")

    # Default sink database (override per-pipeline in YAML)
    database_url: str = "sqlite:///data/pipeline.db"

    # Lineage store
    lineage_dir: Path = Path("data/lineage")
    reports_dir: Path = Path("data/reports")
    output_dir: Path = Path("data/output")

    # Quality gate: pipelines with score below this abort before load
    default_quality_threshold: float = 80.0

    # Scheduler
    schedule_enabled: bool = True


@lru_cache
def get_settings() -> Settings:
    return Settings()
