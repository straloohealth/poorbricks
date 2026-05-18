"""Settings loaded from .env using pydantic."""

from __future__ import annotations

from pathlib import Path

from pydantic_settings import BaseSettings


class Settings(BaseSettings):
    """Configuration for MongoDB, contracts store, PostgreSQL, and local paths.

    Load from .env in the repository root.
    """

    mongo_uri: str = "mongodb://localhost:27017"
    contracts_mongo_uri: str | None = None
    contracts_db: str = "poorbricks"
    contracts_collection: str = "data_contracts"
    delta_output_dir: str = "artifacts/delta"
    tables_root: Path = Path("tables")
    postgres_host: str = "postgresql-rw.storage.svc.cluster.local"
    postgres_port: int = 5432
    postgres_db: str = "poorbricks"
    postgres_user: str = "analytics"
    postgres_password: str = "analytics"

    model_config = {"env_file": ".env", "extra": "ignore"}


settings = Settings()

__all__ = ["Settings", "settings"]
