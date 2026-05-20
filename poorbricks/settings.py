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
    postgres_host: str = "localhost"
    postgres_port: int = 5432
    postgres_db: str = "analytics"
    postgres_user: str = "analytics"
    postgres_password: str = "analytics"

    # Out-of-core execution tuning. The framework processes datasets larger
    # than RAM by partitioning reads, spilling shuffles to disk, and streaming
    # writes — never collecting a whole dataset into the driver.
    spark_master: str = "local[*]"  # all cores; the test suite bounds this
    spark_driver_memory: str = "2g"
    spark_local_dir: str | None = None  # scratch dir for shuffle/sort spill
    read_partitions: int = 16  # parallelism for partitioned Mongo / JDBC reads

    model_config = {"env_file": ".env", "extra": "ignore"}


settings = Settings()

__all__ = ["Settings", "settings"]
