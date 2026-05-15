"""Settings loaded from .env using pydantic."""

from __future__ import annotations

from pydantic_settings import BaseSettings


class Settings(BaseSettings):
    """Configuration for MongoDB, contracts store, PostgreSQL, and local paths.

    Load from .env in the repository root.
    """

    mongo_uri: str = "mongodb://localhost:27017"
    contracts_db: str = "poorbricks_contracts"
    contracts_collection: str = "data_contracts"
    delta_output_dir: str = "artifacts/delta"
    postgres_host: str = "localhost"
    postgres_port: int = 5432
    postgres_db: str = "analytics"
    postgres_user: str = "analytics"
    postgres_password: str = "analytics"

    model_config = {"env_file": ".env"}


settings = Settings()

__all__ = ["Settings", "settings"]
